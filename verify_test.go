package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/crypto/backup"
	"maunium.net/go/mautrix/crypto/olm"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func TestVerificationRequiresLocalConfirmation(t *testing.T) {
	for _, tc := range []struct {
		name       string
		input      string
		remoteDone bool
		wantOK     bool
	}{
		{name: "matching codes confirmed", input: "yes\n", wantOK: true},
		{name: "mismatch", input: "no\n"},
		{name: "stdin closed"},
		{name: "remote alone cannot confirm", remoteDone: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			const txn id.VerificationTransactionID = "own-transaction"
			updates := make(verificationUpdates, 4)
			updates.VerificationCancelled(ctx, "unrelated", event.VerificationCancelCodeUser, "ignore this")
			if tc.remoteDone {
				updates.VerificationDone(ctx, txn, event.VerificationMethodSAS)
			} else {
				updates.ShowSAS(ctx, txn, []rune{'🐶', '🍎'}, nil, nil)
			}
			confirmed := false
			err := updates.confirm(ctx, txn, func(ctx context.Context, got id.VerificationTransactionID) error {
				if got != txn {
					t.Fatalf("unexpected transaction %q", got)
				}
				confirmed = true
				updates.VerificationDone(ctx, txn, event.VerificationMethodSAS)
				return nil
			}, strings.NewReader(tc.input), io.Discard)
			if tc.wantOK && err != nil || !tc.wantOK && !errors.Is(err, errVerificationMismatch) {
				t.Fatalf("unexpected verification result: %v", err)
			}
			if confirmed != tc.wantOK {
				t.Fatalf("sent confirmation = %t, want %t", confirmed, tc.wantOK)
			}
		})
	}
}

func TestDecodeSharedBackupKey(t *testing.T) {
	key, err := backup.NewMegolmBackupKey()
	if err != nil {
		t.Fatal(err)
	}
	for _, encoding := range []*base64.Encoding{base64.RawStdEncoding, base64.StdEncoding} {
		decoded, err := decodeBackupKey(encoding.EncodeToString(key.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		if backupPublicKey(decoded) != backupPublicKey(key) {
			t.Fatal("decoded a different backup key")
		}
	}
	if _, err := decodeBackupKey("not a backup key"); err == nil {
		t.Fatal("accepted an invalid backup key")
	}
}

func TestDataDirCannotBeSharedByServerAndVerification(t *testing.T) {
	dir := t.TempDir()
	unlock, err := lockDataDir(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	if second, err := lockDataDir(t.Context(), dir); err == nil {
		second()
		t.Fatal("locked a data directory twice")
	}
	unlock()
	reopened, err := lockDataDir(t.Context(), dir)
	if err != nil {
		t.Fatalf("could not relock after release: %v", err)
	}
	reopened()
}

func TestServerRestoresKeyBackupFromSavedDevice(t *testing.T) {
	ctx := t.Context()
	const user id.UserID = "@test:example.org"
	const device id.DeviceID = "MCP_TEST"
	const room id.RoomID = "!test:example.org"
	key, err := backup.NewMegolmBackupKey()
	if err != nil {
		t.Fatal(err)
	}
	group, err := olm.NewOutboundGroupSession()
	if err != nil {
		t.Fatal(err)
	}
	inbound, err := olm.NewInboundGroupSession([]byte(group.Key()))
	if err != nil {
		t.Fatal(err)
	}
	exported, err := inbound.Export(0)
	if err != nil {
		t.Fatal(err)
	}
	sessionData, err := backup.EncryptSessionData(key, backup.MegolmSessionData{
		Algorithm: id.AlgorithmMegolmV1, SessionKey: string(exported),
	})
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := group.Encrypt([]byte("message before MCP logged in"))
	if err != nil {
		t.Fatal(err)
	}

	var uploadedKeys atomic.Pointer[mautrix.DeviceKeys]
	mux := http.NewServeMux()
	respond := func(w http.ResponseWriter, value any) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(value); err != nil {
			t.Errorf("encode Matrix response: %v", err)
		}
	}
	mux.HandleFunc("POST /_matrix/client/v3/login", func(w http.ResponseWriter, _ *http.Request) {
		respond(w, mautrix.RespLogin{UserID: user, DeviceID: device, AccessToken: "test-token"})
	})
	mux.HandleFunc("POST /_matrix/client/v3/keys/query", func(w http.ResponseWriter, _ *http.Request) {
		var result mautrix.RespQueryKeys
		if keys := uploadedKeys.Load(); keys != nil {
			result.DeviceKeys = map[id.UserID]map[id.DeviceID]mautrix.DeviceKeys{user: {device: *keys}}
		}
		respond(w, result)
	})
	mux.HandleFunc("POST /_matrix/client/v3/keys/upload", func(w http.ResponseWriter, r *http.Request) {
		var req mautrix.ReqUploadKeys
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode Matrix request: %v", err)
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if req.DeviceKeys != nil {
			uploadedKeys.Store(req.DeviceKeys)
		}
		respond(w, mautrix.RespUploadKeys{})
	})
	mux.HandleFunc("GET /_matrix/client/v3/room_keys/version", func(w http.ResponseWriter, _ *http.Request) {
		respond(w, mautrix.RespRoomKeysVersion[backup.MegolmAuthData]{
			Algorithm: id.KeyBackupAlgorithmMegolmBackupV1,
			AuthData:  backup.MegolmAuthData{PublicKey: backupPublicKey(key)},
			Version:   "1",
			Count:     1,
		})
	})
	mux.HandleFunc("GET /_matrix/client/v3/room_keys/keys", func(w http.ResponseWriter, r *http.Request) {
		if version := r.URL.Query().Get("version"); version != "1" {
			t.Errorf("requested backup version %q", version)
		}
		respond(w, map[string]any{"rooms": map[id.RoomID]any{
			room: map[string]any{"sessions": map[id.SessionID]any{
				group.ID(): map[string]any{"session_data": sessionData},
			}},
		}})
	})
	homeserver := httptest.NewServer(mux)
	defer homeserver.Close()
	cfg := Config{
		Homeserver: homeserver.URL, UserID: user.String(), Password: "test-password",
		PickleKey: "test-pickle-key", DataDir: t.TempDir(),
	}
	app, err := NewApp(ctx, cfg, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	// Simulate the persisted verification and secret received from the old device.
	err = app.crypto.Machine().CryptoStore.PutSecret(ctx, id.SecretMegolmBackupV1, base64.RawStdEncoding.EncodeToString(key.Bytes()))
	if err == nil {
		err = app.store.SetKV(ctx, "verified_device", device.String())
	}
	app.Close()
	if err != nil {
		t.Fatal(err)
	}

	app, err = NewApp(ctx, cfg, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	app.loadKeyBackup(ctx)
	if !app.verified.Load() {
		t.Fatal("verified device state did not survive restart")
	}
	session, err := app.crypto.Machine().CryptoStore.GetGroupSession(ctx, room, group.ID())
	if err != nil {
		t.Fatal(err)
	}
	if session == nil {
		t.Fatalf("room key missing after restore, backup status: %v", *app.backupStatus.Load())
	}
	plaintext, _, err := session.Internal.Decrypt(ciphertext)
	if err != nil || string(plaintext) != "message before MCP logged in" {
		t.Fatalf("decrypt with restored room key: %q, %v", plaintext, err)
	}
}
