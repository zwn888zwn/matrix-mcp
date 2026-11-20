# matrix-mcp

A remote [MCP](https://modelcontextprotocol.io) server that gives AI agents read-only access by default to a Matrix account, including **end-to-end encrypted** rooms and chats bridged from Messenger, Instagram, WhatsApp, Discord, iMessage and other [mautrix](https://github.com/mautrix) bridges.

It logs in as its own Matrix device, decrypts messages, and keeps a local full-text index so an agent can search and read your chats. Enable write access to let clients send messages and manage rooms and bridges.

- **E2EE.** Built on [mautrix-go](https://github.com/mautrix/go). Verify its device from an existing Element session or with your recovery key, then import the server-side key backup to decrypt older history.
- **Search.** SQLite FTS5 index that survives restarts and folds diacritics, so `zolc` matches `żółć`. It backfills a configurable amount of history per room, and more on demand.
- **Bridge-aware.** Detects each room's network (`m.bridge`) and the bridge management rooms, and can run bridge bot commands.
- **Remote.** Streamable HTTP. Authenticate with a static bearer token (Claude Code, Codex, scripts) or OAuth 2.1 with dynamic client registration, which claude.ai and ChatGPT custom connectors need. OAuth sign-in uses the Matrix account itself: the consent page checks your username and password with the homeserver.

## Tools

| Tool | What it does |
| --- | --- |
| `status` | Sync, encryption, key backup and index status |
| `list_rooms` | Chats by recent activity. Filter by name, network, kind, unread, `awaiting_reply` (last message not from you) and an activity date range |
| `get_room` | Room details and members |
| `read_messages` | Decrypted history, oldest first. Edits, reactions and deletions are folded in; page back with `next_before` |
| `get_message_context` | Messages around a `message_id`, e.g. a search hit |
| `search_messages` | Full-text search. Filter by sender (`me` works), room, `after`/`before` dates or the last N days |
| `backfill_room` | Index older history of a room |
| `wait_for_messages` | Long-poll for new incoming messages |
| `send_message` | Markdown or plain text, with replies and threads |
| `send_file`, `send_reaction`, `edit_message`, `delete_message`, `mark_read` | Messaging |
| `download_media` | Decrypt and return attachments. Images come inline as a 768 px thumbnail by default (`variant: full` for the original); audio also inline |
| `join_room`, `leave_room`, `invite_user`, `remove_user`, `create_room`, `update_room` | Room management, including tags |
| `bridge_command` | Send a command to a bridge bot (`help`, `list-logins`, `search`, `start-chat`, …) and collect its replies |
| `matrix_api` | Raw client-server or Synapse admin API call |

Rooms can be referenced by ID, alias or a unique part of their name.

The server exposes only `status`, `list_rooms`, `get_room`, `read_messages`, `get_message_context`, `search_messages`, `backfill_room`, `wait_for_messages`, and `download_media` by default. `backfill_room` fetches Matrix history and updates the local search index. The server rejects calls to tools outside this list, including `mark_read`, `bridge_command`, and `matrix_api`, unless `MCP_ALLOW_WRITES=true`. The setting applies to both bearer-token and OAuth clients.

## Running

The server needs a Matrix account it can log into with a password, ideally on the same network as the homeserver.

| Variable | Required | Notes |
| --- | --- | --- |
| `MATRIX_HOMESERVER` | | Homeserver URL. Default `http://matrix-synapse:8008` |
| `MATRIX_USER_ID` | yes | e.g. `@me:example.com` |
| `MATRIX_PASSWORD` | yes | Used once to create the device; the session is stored afterwards |
| `MATRIX_RECOVERY_KEY` | | Recovery (SSSS) key. Verifies the device and imports the key backup so old messages decrypt |
| `PICKLE_KEY` | yes | Encrypts the crypto store. Never change it, or the device has to be recreated |
| `MCP_BEARER_TOKEN` | yes | Static client token, at least 32 characters |
| `MCP_ALLOW_WRITES` | | Default `false`. Set to `true` and restart to enable all tools for authenticated clients |
| `PUBLIC_URL` | for OAuth | Public base URL, e.g. `https://matrix-mcp.example.com`. Setting it enables OAuth for custom connectors |
| `TZ` | | Time zone for date filters without an explicit offset, e.g. `Europe/Warsaw` |
| `BACKFILL_PER_ROOM` | | Messages indexed per room on first start. Default 200 |
| `DATA_DIR`, `LISTEN_ADDR`, `DEVICE_NAME` | | Defaults `/data`, `:8080`, `Matrix MCP` |

```sh
docker build -t matrix-mcp .
docker run -d --name matrix-mcp -p 8080:8080 -v matrix-mcp-data:/data \
  -e MATRIX_HOMESERVER=https://matrix.example.com -e MATRIX_USER_ID=@me:example.com \
  -e MATRIX_PASSWORD=... -e PICKLE_KEY="$(openssl rand -hex 32)" -e MCP_BEARER_TOKEN="$(openssl rand -hex 32)" \
  matrix-mcp
```

Put it behind HTTPS. Keep the `/data` volume: it holds the device keys, the crypto store and the search index.

### Verify with an existing Element device

Use the local `verify` command to pair with a signed-in, verified Element device and recover the server-side key backup. This works without setting `MATRIX_RECOVERY_KEY` when the Element device can share the backup key.

If the server container is already running, stop it first with `docker stop matrix-mcp`. Run the command in a terminal with the same configuration and data volume as the server.

```sh
docker build -t matrix-mcp .
docker run --rm -it --env-file ~/.matrix-mcp.env \
  -v matrix-mcp-data:/data matrix-mcp verify
```

Open Element, accept the request for the displayed Matrix MCP device, and compare the emoji codes. Confirm on both sides if they match. Keep Element online until the terminal reports that the key backup was imported.

Start the server from the rebuilt image with the same data volume and `PICKLE_KEY`. Recreate a server container that uses an older image. The server loads the backup key from the encrypted crypto store and reindexes the configured recent history. Run `verify` again if device verification succeeded but backup recovery timed out.

## Connecting clients

**Claude Code**

```sh
claude mcp add --transport http -s user matrix https://matrix-mcp.example.com/mcp \
  --header "Authorization: Bearer $MATRIX_MCP_TOKEN"
```

**Codex** (`~/.codex/config.toml`)

```toml
[mcp_servers.matrix]
url = "https://matrix-mcp.example.com/mcp"
bearer_token_env_var = "MATRIX_MCP_TOKEN"
```

**claude.ai / Claude apps.** Set `PUBLIC_URL`, then go to Settings → Connectors → Add custom connector and enter `https://matrix-mcp.example.com/mcp`. Claude registers itself and opens the consent page, where you sign in with your Matrix username and password.

**ChatGPT.** Enable developer mode (Settings → Apps → Advanced settings), then create an app with the same URL and OAuth authentication.

Only the account the server runs as can sign in. The password is checked by logging in to the homeserver and immediately logging out again, so the homeserver's own rate limits apply. OAuth access tokens last an hour and refresh tokens 60 days (they rotate on use). Tokens are stored hashed.

## Development

```sh
CGO_ENABLED=1 go build -tags "goolm sqlite_fts5" .
CGO_ENABLED=1 go test -tags "goolm sqlite_fts5" ./...
```

The `goolm` tag selects the pure-Go Olm implementation; `sqlite_fts5` enables full-text search.

## Evals

`evals/` measures how well an agent answers real questions through the MCP. Each task runs through Claude Code headless (`claude -p`), connected only to this server and limited to read-only tools. Each trial's final `ANSWER:` line is graded against the expected answers. A task passes only if every trial passes (pass^k).

```sh
cp evals/tasks.example.json evals/tasks.local.json   # write questions about your own chats
MATRIX_MCP_URL=https://matrix-mcp.example.com/mcp MATRIX_MCP_TOKEN=... \
  go run ./evals -tasks evals/tasks.local.json -trials 3 -json evals/report.json
```

Files matching `*.local.json` are gitignored, so questions about your own chats stay on your machine. Use failing transcripts (`-v`) to improve tool descriptions.

## License

MIT
