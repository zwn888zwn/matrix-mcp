package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/crypto/backup"
	"maunium.net/go/mautrix/crypto/verificationhelper"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func (app *App) verifyAndRestore(ctx context.Context, input io.Reader, output io.Writer) error {
	ctx, timeout := context.WithTimeout(ctx, 10*time.Minute)
	defer timeout()
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	if app.cli.UserID != id.UserID(app.cfg.UserID) {
		return fmt.Errorf("logged in as %s, expected %s", app.cli.UserID, app.cfg.UserID)
	}
	mach := app.crypto.Machine()
	devices, err := mach.FetchKeys(ctx, []id.UserID{app.cli.UserID}, true)
	if err != nil {
		return fmt.Errorf("fetch Matrix devices: %w", err)
	}
	masterTrusted, _, _, err := mach.GetOwnCrossSigningVerificationStatus(ctx)
	if err != nil {
		return fmt.Errorf("check Matrix identity: %w", err)
	}
	hasIdentity, signed, err := mach.GetOwnVerificationStatus(ctx)
	if err != nil {
		return fmt.Errorf("check Matrix device: %w", err)
	}
	if !hasIdentity {
		return errors.New("no Matrix cross-signing identity found. Set up encryption on an Element device first")
	}

	var txn id.VerificationTransactionID
	var verifier *verificationhelper.VerificationHelper
	updates := make(verificationUpdates, 8)
	syncer := app.cli.Syncer.(*mautrix.DefaultSyncer)
	if !masterTrusted || !signed {
		if len(devices[app.cli.UserID]) < 2 {
			return errors.New("no other Matrix device found. Open a signed-in, verified Element device first")
		}
		verifier = verificationhelper.NewVerificationHelper(app.cli, mach, nil, updates, false, false, true)
		if err := verifier.Init(ctx); err != nil {
			return fmt.Errorf("initialize verification: %w", err)
		}
		// Start before polling /sync so a fast reply cannot beat the SDK saving the transaction.
		txn, err = verifier.StartVerification(ctx, app.cli.UserID)
		if err != nil {
			return fmt.Errorf("start verification: %w", err)
		}
		// Run after the SDK's ready handler has saved the transaction and released its lock.
		syncer.OnEventType(event.ToDeviceVerificationReady, func(ctx context.Context, evt *event.Event) {
			ready := evt.Content.AsVerificationReady()
			if evt.Sender != app.cli.UserID || ready.TransactionID != txn || !slices.Contains(ready.Methods, event.VerificationMethodSAS) {
				return
			}
			if err := verifier.StartSAS(ctx, txn); err != nil {
				updates.publish(verificationUpdate{txn: txn, err: fmt.Errorf("start emoji verification: %w", err)})
			}
		})
		fmt.Fprintf(output, "Accept the verification request for %s (%s) in Element.\n", app.cfg.DeviceName, app.cli.DeviceID)
	}

	// Pairing sync must not advance the server's persistent room sync cursor.
	app.cli.Store = mautrix.NewMemorySyncStore()
	omit := &mautrix.FilterPart{NotTypes: []event.Type{{Type: "*"}}}
	syncer.FilterJSON = &mautrix.Filter{
		Presence: omit, AccountData: omit,
		Room: &mautrix.RoomFilter{State: omit, Timeline: omit, Ephemeral: omit, AccountData: omit},
	}
	syncDone := make(chan struct{})
	go func() {
		defer close(syncDone)
		err := app.cli.SyncWithContext(ctx)
		if ctx.Err() == nil {
			cancel(fmt.Errorf("Matrix sync ended during device setup: %v", err))
		}
	}()
	defer func() {
		cancel(nil)
		<-syncDone
	}()

	if txn != "" {
		if err := updates.confirm(ctx, txn, verifier.ConfirmSAS, input, output); err != nil {
			cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			code := event.VerificationCancelCodeUser
			if errors.Is(err, errVerificationMismatch) {
				code = event.VerificationCancelCodeSASMismatch
			}
			_ = verifier.CancelVerification(cleanupCtx, txn, code, "Verification was not completed locally")
			return err
		}
		if err := app.waitForDeviceSignature(ctx); err != nil {
			return err
		}
	}
	if err := app.store.SetKV(ctx, "verified_device", app.cli.DeviceID.String()); err != nil {
		return fmt.Errorf("save verified device: %w", err)
	}
	fmt.Fprintln(output, "Device verified. Keep the verified Element device online while importing the key backup.")
	if err := app.recoverKeyBackup(ctx); err != nil {
		return err
	}
	if err := app.store.resetBackfill(ctx); err != nil {
		return fmt.Errorf("schedule indexing with restored keys: %w", err)
	}
	fmt.Fprintln(output, "Key backup imported. Start matrix-mcp with the same data directory.")
	return nil
}

func (app *App) waitForDeviceSignature(ctx context.Context) error {
	ctx, stop := context.WithTimeout(ctx, 20*time.Second)
	defer stop()
	mach := app.crypto.Machine()
	for {
		if _, err := mach.FetchKeys(ctx, []id.UserID{app.cli.UserID}, true); err != nil {
			return fmt.Errorf("refresh devices after verification: %w", err)
		}
		_, signed, err := mach.GetOwnVerificationStatus(ctx)
		if err != nil {
			return fmt.Errorf("check new device signature: %w", err)
		}
		if signed {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("Element did not cross-sign the MCP device: %w", context.Cause(ctx))
		case <-time.After(time.Second):
		}
	}
}

func (app *App) recoverKeyBackup(ctx context.Context) error {
	version, err := app.cli.GetKeyBackupLatestVersion(ctx)
	if err != nil {
		return fmt.Errorf("find the server-side key backup: %w", err)
	}
	if version.Algorithm != id.KeyBackupAlgorithmMegolmBackupV1 {
		return fmt.Errorf("unsupported key backup algorithm %q", version.Algorithm)
	}
	var key *backup.MegolmBackupKey
	mach := app.crypto.Machine()
	err = mach.GetOrRequestSecret(ctx, id.SecretMegolmBackupV1, func(secret string) (bool, error) {
		candidate, err := decodeBackupKey(secret)
		if err != nil || backupPublicKey(candidate) != version.AuthData.PublicKey {
			return false, nil
		}
		key = candidate
		return true, nil
	}, time.Minute)
	if err != nil {
		return fmt.Errorf("receive the matching backup key from a verified device: %w", err)
	}
	app.backupKey = key
	return app.refreshKeyBackup(ctx, true)
}

func (app *App) loadStoredKeyBackup(ctx context.Context) error {
	verified, err := app.store.GetKV(ctx, "verified_device")
	if err != nil {
		return fmt.Errorf("load verified device: %w", err)
	}
	if verified != app.cli.DeviceID.String() {
		app.backupStatus.Store(ptr("Device verification required. Run matrix-mcp verify or set MATRIX_RECOVERY_KEY."))
		return nil
	}
	app.verified.Store(true)
	secret, err := app.crypto.Machine().CryptoStore.GetSecret(ctx, id.SecretMegolmBackupV1)
	if err != nil {
		return fmt.Errorf("load the backup key from the crypto store: %w", err)
	}
	if secret == "" {
		app.backupStatus.Store(ptr("Device verified. Run matrix-mcp verify to import the key backup."))
		return nil
	}
	app.backupKey, err = decodeBackupKey(secret)
	if err != nil {
		return fmt.Errorf("decode the stored backup key: %w", err)
	}
	return app.refreshKeyBackup(ctx, true)
}

func decodeBackupKey(secret string) (*backup.MegolmBackupKey, error) {
	raw, err := base64.RawStdEncoding.DecodeString(strings.TrimRight(secret, "="))
	if err != nil {
		return nil, err
	}
	return backup.MegolmBackupKeyFromBytes(raw)
}

type verificationUpdate struct {
	txn  id.VerificationTransactionID
	sas  string
	done bool
	err  error
}

type verificationUpdates chan verificationUpdate

func (updates verificationUpdates) publish(update verificationUpdate) {
	select {
	case updates <- update:
	default:
	}
}

func (updates verificationUpdates) VerificationRequested(context.Context, id.VerificationTransactionID, id.UserID, id.DeviceID) {
	// This command only participates in the request it initiated.
}

func (updates verificationUpdates) VerificationReady(_ context.Context, txn id.VerificationTransactionID, _ id.DeviceID, sas, _ bool, _ *verificationhelper.QRCode) {
	if !sas {
		updates.publish(verificationUpdate{txn: txn, err: errors.New("the Element device does not support emoji verification")})
	}
}

func (updates verificationUpdates) ShowSAS(_ context.Context, txn id.VerificationTransactionID, emojis []rune, _ []string, decimals []int) {
	sas := fmt.Sprint(decimals)
	if len(emojis) > 0 {
		var symbols []string
		for _, emoji := range emojis {
			symbols = append(symbols, string(emoji))
		}
		sas = strings.Join(symbols, "  ")
	}
	updates.publish(verificationUpdate{txn: txn, sas: sas})
}

func (updates verificationUpdates) VerificationCancelled(_ context.Context, txn id.VerificationTransactionID, code event.VerificationCancelCode, reason string) {
	updates.publish(verificationUpdate{txn: txn, err: fmt.Errorf("verification cancelled (%s): %s", code, reason)})
}

func (updates verificationUpdates) VerificationDone(_ context.Context, txn id.VerificationTransactionID, _ event.VerificationMethod) {
	updates.publish(verificationUpdate{txn: txn, done: true})
}

var errVerificationMismatch = errors.New("verification code was not confirmed")

func (updates verificationUpdates) confirm(ctx context.Context, txn id.VerificationTransactionID, confirmSAS func(context.Context, id.VerificationTransactionID) error, input io.Reader, output io.Writer) error {
	var confirmation <-chan bool
	confirmed := false
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case ok := <-confirmation:
			confirmation = nil
			if !ok {
				return errVerificationMismatch
			}
			if err := confirmSAS(ctx, txn); err != nil {
				return fmt.Errorf("confirm verification code: %w", err)
			}
			confirmed = true
			fmt.Fprintln(output, "Waiting for Element to finish verification.")
		case update := <-updates:
			if update.txn != txn {
				continue
			}
			switch {
			case update.err != nil:
				return update.err
			case update.done:
				if !confirmed {
					return errVerificationMismatch
				}
				return nil
			case update.sas != "" && confirmation == nil && !confirmed:
				fmt.Fprintf(output, "Compare with Element: %s\nType yes only if the codes match: ", update.sas)
				answer := make(chan bool, 1)
				confirmation = answer
				go func() {
					line, _ := bufio.NewReader(input).ReadString('\n')
					answer <- strings.EqualFold(strings.TrimSpace(line), "yes")
				}()
			}
		}
	}
}
