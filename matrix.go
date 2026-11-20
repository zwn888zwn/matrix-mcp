package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	_ "go.mau.fi/util/dbutil/litestream"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/crypto/backup"
	"maunium.net/go/mautrix/crypto/cryptohelper"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

type App struct {
	cfg    Config
	log    zerolog.Logger
	cli    *mautrix.Client
	crypto *cryptohelper.CryptoHelper
	db     *dbutil.Database
	store  *Store
	rooms  *RoomCache

	ready      atomic.Bool
	verified   atomic.Bool
	lastSyncOK atomic.Int64

	backupMu        sync.Mutex
	backupKey       *backup.MegolmBackupKey
	lastBackupFetch time.Time
	backupStatus    atomic.Pointer[string]

	waitMu  sync.Mutex
	waiters map[int]*waiter
	waitSeq int
}

type waiter struct {
	room id.RoomID // empty = any room
	ch   chan *event.Event
}

func NewApp(ctx context.Context, cfg Config, log zerolog.Logger) (*App, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, err
	}
	userID := id.UserID(cfg.UserID)
	localpart, _, err := userID.Parse()
	if err != nil {
		return nil, fmt.Errorf("invalid MATRIX_USER_ID: %w", err)
	}
	cli, err := mautrix.NewClient(cfg.Homeserver, "", "")
	if err != nil {
		return nil, err
	}
	cli.Log = log.With().Str("component", "matrix").Logger()
	dbPath := filepath.Join(cfg.DataDir, "matrix-mcp.db")
	db, err := dbutil.NewWithDialect(fmt.Sprintf("file:%s?_txlock=immediate", dbPath), "sqlite3-fk-wal")
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	helper, err := cryptohelper.NewCryptoHelper(cli, []byte(cfg.PickleKey), db)
	if err != nil {
		return nil, err
	}
	helper.LoginAs = &mautrix.ReqLogin{
		Type:                     mautrix.AuthTypePassword,
		Identifier:               mautrix.UserIdentifier{Type: mautrix.IdentifierTypeUser, User: localpart},
		Password:                 cfg.Password,
		InitialDeviceDisplayName: cfg.DeviceName,
	}
	app := &App{cfg: cfg, log: log, cli: cli, crypto: helper, db: db, waiters: map[int]*waiter{}}
	helper.DecryptErrorCallback = func(evt *event.Event, err error) {
		app.log.Debug().Err(err).Stringer("event_id", evt.ID).Msg("live event could not be decrypted")
	}
	if err = helper.Init(ctx); err != nil {
		return nil, fmt.Errorf("crypto init / login: %w", err)
	}
	cli.Crypto = helper
	app.store = &Store{db: db}
	if err = app.store.Init(ctx); err != nil {
		return nil, fmt.Errorf("init store: %w", err)
	}
	app.rooms = NewRoomCache(app)
	app.log.Info().Stringer("user_id", cli.UserID).Stringer("device_id", cli.DeviceID).Msg("logged in")

	return app, nil
}

// lockDataDir keeps separate processes from operating the same Matrix device concurrently.
func lockDataDir(ctx context.Context, dataDir string) (func(), error) {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(dataDir, ".matrix-mcp-lock.db")
	lockDB, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_txlock=immediate&_busy_timeout=0", path))
	if err != nil {
		return nil, err
	}
	// The transaction is on a separate database so it does not block crypto or index writes.
	// It stays open until release and SQLite frees the lock even if the process exits abruptly.
	txn, err := lockDB.BeginTx(context.WithoutCancel(ctx), nil)
	if err != nil {
		_ = lockDB.Close()
		return nil, fmt.Errorf("lock %s (stop any other matrix-mcp process using this data directory): %w", dataDir, err)
	}
	return func() {
		_ = txn.Rollback()
		_ = lockDB.Close()
	}, nil
}

func (app *App) loadKeyBackup(ctx context.Context) {
	var err error
	if app.cfg.RecoveryKey != "" {
		err = app.setupRecovery(ctx)
	} else {
		err = app.loadStoredKeyBackup(ctx)
	}
	if err != nil {
		app.log.Error().Err(err).Msg("key backup setup failed")
		app.backupStatus.Store(ptr("key backup setup failed: " + err.Error()))
	}
}

func (app *App) setupRecovery(ctx context.Context) error {
	mach := app.crypto.Machine()
	verifiedFor, _ := app.store.GetKV(ctx, "verified_device")
	if verifiedFor != app.cli.DeviceID.String() {
		if err := mach.VerifyWithRecoveryKey(ctx, app.cfg.RecoveryKey); err != nil {
			return fmt.Errorf("verify device with recovery key: %w", err)
		}
		_ = app.store.SetKV(ctx, "verified_device", app.cli.DeviceID.String())
		app.log.Info().Msg("device cross-signed with recovery key")
	}
	app.verified.Store(true)

	keyID, keyData, err := mach.SSSS.GetDefaultKeyData(ctx)
	if err != nil {
		return fmt.Errorf("get secret storage key: %w", err)
	}
	key, err := keyData.VerifyRecoveryKey(keyID, app.cfg.RecoveryKey)
	if err != nil {
		return fmt.Errorf("check recovery key: %w", err)
	}
	raw, err := mach.SSSS.GetDecryptedAccountData(ctx, event.AccountDataMegolmBackupKey, key)
	if err != nil {
		return fmt.Errorf("read key backup secret: %w", err)
	}
	bk, err := backup.MegolmBackupKeyFromBytes(raw)
	if err != nil {
		return fmt.Errorf("parse key backup secret: %w", err)
	}
	app.backupKey = bk
	return app.refreshKeyBackup(ctx, true)
}

// refreshKeyBackup downloads room keys from the server-side key backup. Throttled unless forced.
func (app *App) refreshKeyBackup(ctx context.Context, force bool) error {
	app.backupMu.Lock()
	defer app.backupMu.Unlock()
	if app.backupKey == nil {
		return errors.New("key backup not available")
	}
	if !force && time.Since(app.lastBackupFetch) < 2*time.Minute {
		return nil
	}
	app.lastBackupFetch = time.Now()
	info, err := app.cli.GetKeyBackupLatestVersion(ctx)
	if err == nil {
		switch {
		case info.Algorithm != id.KeyBackupAlgorithmMegolmBackupV1:
			err = fmt.Errorf("unsupported key backup algorithm %q", info.Algorithm)
		case info.Version == "" || info.AuthData.PublicKey != backupPublicKey(app.backupKey):
			err = errors.New("the saved key does not match the current backup")
		default:
			err = app.crypto.Machine().GetAndStoreKeyBackup(ctx, info.Version, app.backupKey)
		}
	}
	if err != nil {
		app.backupStatus.Store(ptr("key backup download failed: " + err.Error()))
		return err
	}
	app.backupStatus.Store(ptr(fmt.Sprintf("key backup version %s imported at %s", info.Version, time.Now().UTC().Format(time.RFC3339))))
	app.log.Info().Str("version", info.Version.String()).Msg("imported room keys from key backup")
	return nil
}

func backupPublicKey(key *backup.MegolmBackupKey) id.Ed25519 {
	return id.Ed25519(base64.RawStdEncoding.EncodeToString(key.PublicKey().Bytes()))
}

func (app *App) Run(ctx context.Context) {
	syncer := app.cli.Syncer.(*mautrix.DefaultSyncer)
	syncer.FilterJSON = &mautrix.Filter{
		Room: &mautrix.RoomFilter{Timeline: &mautrix.FilterPart{Limit: 30}},
	}
	syncer.OnSync(app.onSync)
	syncer.OnEvent(app.onEvent)

	go app.periodic(ctx)
	for ctx.Err() == nil {
		err := app.cli.SyncWithContext(ctx)
		if ctx.Err() != nil {
			return
		}
		app.log.Error().Err(err).Msg("sync stopped, retrying in 10s")
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
		}
	}
}

func (app *App) periodic(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if app.backupKey != nil {
				_ = app.refreshKeyBackup(ctx, true)
			}
		}
	}
}

func (app *App) Close() {
	_ = app.crypto.Close()
}

func (app *App) onSync(ctx context.Context, resp *mautrix.RespSync, since string) bool {
	app.lastSyncOK.Store(time.Now().Unix())
	for roomID, room := range resp.Rooms.Join {
		if app.rooms.NoteJoined(roomID, app.ready.Load()) {
			go app.loadJoinedRoom(context.WithoutCancel(ctx), roomID)
		}
		if room.UnreadNotifications != nil {
			app.rooms.SetUnread(ctx, roomID, room.UnreadNotifications.NotificationCount, room.UnreadNotifications.HighlightCount)
		}
		for _, evt := range room.State.Events {
			_ = evt
			app.rooms.MarkDirty(roomID)
		}
	}
	for roomID := range resp.Rooms.Leave {
		app.rooms.Forget(roomID)
	}
	for roomID := range resp.Rooms.Invite {
		app.rooms.AddInvite(roomID)
	}
	if !app.ready.Load() {
		// First sync finished: load the room list and start history indexing in the background.
		go func() {
			app.rooms.LoadAll(context.WithoutCancel(ctx))
			app.ready.Store(true)
			app.log.Info().Int("rooms", app.rooms.Count()).Msg("ready")
			go app.backfillAll(context.WithoutCancel(ctx))
		}()
	}
	return true
}

// loadJoinedRoom adds a room joined while the server is running to the room list and indexes its history.
func (app *App) loadJoinedRoom(ctx context.Context, roomID id.RoomID) {
	defer app.rooms.FinishLoading(roomID)
	app.rooms.loadSem <- struct{}{}
	defer func() { <-app.rooms.loadSem }()
	if _, err := app.rooms.load(ctx, roomID); err != nil {
		app.log.Warn().Err(err).Stringer("room_id", roomID).Msg("failed to load joined room")
		return
	}
	app.backfillOnce(ctx, roomID)
}

func (app *App) onEvent(ctx context.Context, evt *event.Event) {
	if evt.RoomID == "" {
		return
	}
	if evt.Type == event.AccountDataRoomTags {
		app.rooms.SetTags(evt.RoomID, evt)
		return
	}
	if evt.Type.Class == event.StateEventType || evt.StateKey != nil {
		app.rooms.MarkDirty(evt.RoomID)
		return
	}
	if evt.Type == event.EventEncrypted {
		return // handled after decryption
	}
	if evt.Type.Class != event.MessageEventType {
		return
	}
	app.ingest(ctx, evt)
	app.notifyWaiters(evt)
}

// ingest updates the search index and room activity for a timeline event.
func (app *App) ingest(ctx context.Context, evt *event.Event) {
	msg := app.toMessage(ctx, evt)
	if msg == nil {
		return
	}
	switch msg.Kind {
	case "redaction":
		_ = app.store.Delete(ctx, msg.Redacts)
	case "edit":
		_ = app.store.UpdateBody(ctx, msg.Edits, msg.Body)
	case "reaction", "undecryptable", "membership", "room_name", "room_topic", "deleted":
	default:
		_ = app.store.Index(ctx, msg)
		app.rooms.Touch(ctx, evt.RoomID, msg)
	}
}

func (app *App) addWaiter(room id.RoomID) (int, chan *event.Event) {
	app.waitMu.Lock()
	defer app.waitMu.Unlock()
	app.waitSeq++
	ch := make(chan *event.Event, 64)
	app.waiters[app.waitSeq] = &waiter{room: room, ch: ch}
	return app.waitSeq, ch
}

func (app *App) removeWaiter(n int) {
	app.waitMu.Lock()
	defer app.waitMu.Unlock()
	delete(app.waiters, n)
}

func (app *App) notifyWaiters(evt *event.Event) {
	app.waitMu.Lock()
	defer app.waitMu.Unlock()
	for _, w := range app.waiters {
		if w.room == "" || w.room == evt.RoomID {
			select {
			case w.ch <- evt:
			default:
			}
		}
	}
}

// decryptIfNeeded parses the content and decrypts history events, pulling keys from backup if missing.
func (app *App) decryptIfNeeded(ctx context.Context, evt *event.Event) *event.Event {
	if err := evt.Content.ParseRaw(evt.Type); err != nil && !errors.Is(err, event.ErrContentAlreadyParsed) {
		app.log.Debug().Err(err).Stringer("event_id", evt.ID).Msg("failed to parse event content")
	}
	if evt.Type != event.EventEncrypted {
		return evt
	}
	dec, err := app.crypto.Decrypt(ctx, evt)
	if err == nil {
		return dec
	}
	if app.backupKey != nil && strings.Contains(err.Error(), "no session") {
		if app.refreshKeyBackup(ctx, false) == nil {
			if dec, err = app.crypto.Decrypt(ctx, evt); err == nil {
				return dec
			}
		}
	}
	return evt
}

// fetchHistory pages backwards from `from` (empty = latest) and returns decrypted events oldest-first.
func (app *App) fetchHistory(ctx context.Context, roomID id.RoomID, from string, limit int) ([]*event.Event, string, error) {
	resp, err := app.cli.Messages(ctx, roomID, from, "", mautrix.DirectionBackward, nil, limit)
	if err != nil {
		return nil, "", err
	}
	events := make([]*event.Event, 0, len(resp.Chunk))
	for i := len(resp.Chunk) - 1; i >= 0; i-- {
		evt := resp.Chunk[i]
		evt.RoomID = roomID
		events = append(events, app.decryptIfNeeded(ctx, evt))
	}
	next := resp.End
	if len(resp.Chunk) == 0 {
		next = ""
	}
	return events, next, nil
}

func (app *App) backfillAll(ctx context.Context) {
	if app.cfg.BackfillPerRoom <= 0 {
		return
	}
	for _, room := range app.rooms.List() {
		if ctx.Err() != nil {
			return
		}
		if app.backfillOnce(ctx, room.ID) {
			time.Sleep(300 * time.Millisecond)
		}
	}
	app.log.Info().Msg("initial history indexing finished")
}

// backfillOnce indexes a room's history unless that already happened. It reports whether it did the work.
func (app *App) backfillOnce(ctx context.Context, roomID id.RoomID) bool {
	if app.cfg.BackfillPerRoom <= 0 {
		return false
	}
	if done, _ := app.store.GetKV(ctx, "backfilled:"+roomID.String()); done != "" {
		return false
	}
	n, err := app.backfillRoom(ctx, roomID, app.cfg.BackfillPerRoom)
	if err != nil {
		app.log.Warn().Err(err).Stringer("room_id", roomID).Msg("backfill failed")
		return false
	}
	_ = app.store.SetKV(ctx, "backfilled:"+roomID.String(), time.Now().UTC().Format(time.RFC3339))
	app.log.Debug().Stringer("room_id", roomID).Int("events", n).Msg("backfilled room")
	return true
}

func (app *App) backfillRoom(ctx context.Context, roomID id.RoomID, max int) (int, error) {
	from, total := "", 0
	for total < max {
		page := min(100, max-total)
		events, next, err := app.fetchHistory(ctx, roomID, from, page)
		if err != nil {
			return total, err
		}
		for _, evt := range events {
			app.ingest(ctx, evt)
		}
		total += len(events)
		if next == "" || len(events) == 0 {
			break
		}
		from = next
	}
	return total, nil
}

// verifyLogin checks a username and password against the homeserver by logging in and
// immediately logging out again. Only the account this server runs as may sign in.
func (app *App) verifyLogin(ctx context.Context, user, password string) (bool, error) {
	user = strings.TrimSpace(user)
	uid := id.UserID(user)
	if !strings.HasPrefix(user, "@") {
		uid = id.NewUserID(strings.ToLower(user), app.cli.UserID.Homeserver())
	}
	if uid != app.cli.UserID {
		return false, nil
	}
	tmp, err := mautrix.NewClient(app.cfg.Homeserver, "", "")
	if err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	resp, err := tmp.Login(ctx, &mautrix.ReqLogin{
		Type:                     mautrix.AuthTypePassword,
		Identifier:               mautrix.UserIdentifier{Type: mautrix.IdentifierTypeUser, User: uid.String()},
		Password:                 password,
		InitialDeviceDisplayName: "Matrix MCP sign-in check",
		StoreCredentials:         true,
	})
	if err != nil {
		if errors.Is(err, mautrix.MForbidden) {
			return false, nil
		}
		if errors.Is(err, mautrix.MLimitExceeded) {
			return false, errors.New("the homeserver is rate limiting logins, try again in a minute")
		}
		return false, err
	}
	if _, err := tmp.Logout(context.WithoutCancel(ctx)); err != nil {
		app.log.Warn().Err(err).Stringer("device_id", resp.DeviceID).Msg("could not log out the sign-in check session")
	}
	return resp.UserID == app.cli.UserID, nil
}
