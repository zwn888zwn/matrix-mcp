package main

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
	_ "time/tzdata"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/rs/zerolog"
)

const version = "0.1.0"

type Config struct {
	Homeserver      string
	UserID          string
	Password        string
	RecoveryKey     string
	PickleKey       string
	BearerToken     string
	AllowWrites     bool
	DataDir         string
	ListenAddr      string
	DeviceName      string
	BackfillPerRoom int
	PublicURL       string
}

func env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func loadConfig() (Config, error) {
	cfg := Config{
		Homeserver:  env("MATRIX_HOMESERVER", "http://matrix-synapse:8008"),
		UserID:      env("MATRIX_USER_ID", ""),
		Password:    os.Getenv("MATRIX_PASSWORD"),
		RecoveryKey: strings.TrimSpace(os.Getenv("MATRIX_RECOVERY_KEY")),
		PickleKey:   os.Getenv("PICKLE_KEY"),
		BearerToken: os.Getenv("MCP_BEARER_TOKEN"),
		DataDir:     env("DATA_DIR", "/data"),
		ListenAddr:  env("LISTEN_ADDR", ":8080"),
		DeviceName:  env("DEVICE_NAME", "Matrix MCP"),
		PublicURL:   strings.TrimRight(env("PUBLIC_URL", ""), "/"),
	}
	cfg.BackfillPerRoom, _ = strconv.Atoi(env("BACKFILL_PER_ROOM", "200"))
	var err error
	cfg.AllowWrites, err = strconv.ParseBool(env("MCP_ALLOW_WRITES", "false"))
	if err != nil {
		return cfg, errors.New("MCP_ALLOW_WRITES must be a boolean")
	}
	var missing []string
	for name, v := range map[string]string{
		"MATRIX_USER_ID": cfg.UserID, "MATRIX_PASSWORD": cfg.Password,
		"PICKLE_KEY": cfg.PickleKey, "MCP_BEARER_TOKEN": cfg.BearerToken,
	} {
		if v == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return cfg, errors.New("missing required env: " + strings.Join(missing, ", "))
	}
	if len(cfg.BearerToken) < 32 {
		return cfg, errors.New("MCP_BEARER_TOKEN must be at least 32 characters")
	}
	return cfg, nil
}

// requireAuth accepts the static MCP_BEARER_TOKEN or, when OAuth is enabled, an OAuth access token.
func requireAuth(token string, oauth *OAuth, next http.Handler) http.Handler {
	want := []byte(token)
	challenge := `Bearer realm="matrix-mcp"`
	if oauth != nil {
		challenge = `Bearer realm="matrix-mcp", resource_metadata="` + oauth.resourceMetadataURL() + `"`
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if ok && (subtle.ConstantTimeCompare([]byte(got), want) == 1 || oauth != nil && oauth.ValidAccessToken(r.Context(), got)) {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("WWW-Authenticate", challenge)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	})
}

func main() {
	log := zerolog.New(os.Stdout).With().Timestamp().Logger().Level(zerolog.InfoLevel)
	if os.Getenv("DEBUG") != "" {
		log = log.Level(zerolog.DebugLevel)
	}
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal().Err(err).Msg("invalid configuration")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	app, err := NewApp(ctx, cfg, log)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to start Matrix client")
	}
	go app.Run(ctx)

	instructions := fmt.Sprintf(serverInstructions, cfg.UserID)
	if cfg.AllowWrites {
		instructions += writeInstructions
	} else {
		instructions += "\n- This MCP connection provides read-only access to Matrix chats."
	}
	server := mcp.NewServer(&mcp.Implementation{Name: "matrix-mcp", Title: "Matrix", Version: version}, &mcp.ServerOptions{
		Instructions: instructions,
	})
	registerTools(server, app)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if !app.ready.Load() {
			http.Error(w, "starting", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	})
	mcpHandler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{Stateless: true})
	var oauth *OAuth
	if cfg.PublicURL != "" {
		if oauth, err = NewOAuth(ctx, app.db, cfg.PublicURL, app.cli.UserID.String(), app.verifyLogin); err != nil {
			log.Fatal().Err(err).Msg("failed to set up OAuth")
		}
		oauth.Register(mux)
		log.Info().Str("issuer", cfg.PublicURL).Msg("OAuth enabled for custom connectors")
	}
	mux.Handle("/mcp", requireAuth(cfg.BearerToken, oauth, mcpHandler))

	httpServer := &http.Server{Addr: cfg.ListenAddr, Handler: mux, ReadHeaderTimeout: 15 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()
	log.Info().Str("addr", cfg.ListenAddr).Bool("allow_writes", cfg.AllowWrites).Msg("MCP server listening on /mcp")
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal().Err(err).Msg("HTTP server failed")
	}
	app.Close()
}

const serverInstructions = `Access to the user's Matrix account (%s), including chats bridged from Messenger, Instagram, WhatsApp, Discord, iMessage and other networks.
- Rooms can be referenced by room ID (!abc:server), alias (#name:server) or a unique part of their name.
- Start with list_rooms (sorted by recent activity) or search_messages, then read_messages for a room.
- list_rooms only matches room names. To find a chat by what someone said, use search_messages.
- Use list_rooms with awaiting_reply for chats the user has not answered yet.
- Messages are identified by message_id. Pass it to get_message_context or download_media.`

const writeInstructions = `
- Use message_id for send_message reply_to and send_reaction.
- Messages you send are delivered to real people on the bridged networks. Only send when the user asked for it.
- Use bridge_command to talk to a bridge bot (e.g. network "messenger", command "help", "list-logins", "start-chat <id>").`
