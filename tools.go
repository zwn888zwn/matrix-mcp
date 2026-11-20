package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/crypto/attachment"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/format"
	"maunium.net/go/mautrix/id"
)

const maxUploadBytes = 50 << 20
const maxInlineMediaBytes = 8 << 20

func ptr[T any](v T) *T { return &v }

func jsonResult(v any) (*mcp.CallToolResult, any, error) {
	data, err := json.MarshalIndent(v, "", " ")
	if err != nil {
		return nil, nil, err
	}
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(data)}}}, nil, nil
}

func errResult(err error) (*mcp.CallToolResult, any, error) {
	return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: err.Error()}}}, nil, nil
}

// tool registers a handler returning (value, error); errors become tool errors visible to the model.
func tool[In any](s *mcp.Server, app *App, t *mcp.Tool, fn func(ctx context.Context, in In) (any, error)) {
	if !app.cfg.AllowWrites && !allowedReadOnlyTool(t.Name) {
		return
	}
	mcp.AddTool(s, t, func(ctx context.Context, _ *mcp.CallToolRequest, in In) (*mcp.CallToolResult, any, error) {
		if !app.ready.Load() && t.Name != "status" {
			return errResult(errors.New("the Matrix client is still starting up (initial sync); try again in a few seconds"))
		}
		out, err := fn(ctx, in)
		if err != nil {
			return errResult(err)
		}
		if r, ok := out.(*mcp.CallToolResult); ok {
			return r, nil, nil
		}
		return jsonResult(out)
	})
}

func allowedReadOnlyTool(name string) bool {
	switch name {
	case "status", "list_rooms", "get_room", "read_messages", "get_message_context",
		"search_messages", "backfill_room", "wait_for_messages", "download_media":
		return true
	default:
		return false
	}
}

func readOnly(title string) *mcp.ToolAnnotations {
	return &mcp.ToolAnnotations{Title: title, ReadOnlyHint: true, OpenWorldHint: ptr(false)}
}

func writes(title string, destructive bool) *mcp.ToolAnnotations {
	return &mcp.ToolAnnotations{Title: title, DestructiveHint: ptr(destructive), OpenWorldHint: ptr(true)}
}

func clamp(v, def, max int) int {
	if v <= 0 {
		return def
	}
	if v > max {
		return max
	}
	return v
}

// ---- inputs ----

type RoomArg struct {
	Room string `json:"room" jsonschema:"room ID (!id:server), alias (#alias:server) or unique part of the room name"`
}

type ListRoomsIn struct {
	Query         string `json:"query,omitempty" jsonschema:"filter by room name (case-insensitive substring)"`
	Network       string `json:"network,omitempty" jsonschema:"messenger, instagram, whatsapp, discord, imessage, or 'matrix' for native rooms"`
	Kind          string `json:"kind,omitempty" jsonschema:"dm, group, bridge_bot or space"`
	UnreadOnly    bool   `json:"unread_only,omitempty" jsonschema:"only rooms with unread messages"`
	AwaitingReply bool   `json:"awaiting_reply,omitempty" jsonschema:"only chats (dm or group) whose last message is from someone else, i.e. not answered yet"`
	ActiveAfter   string `json:"active_after,omitempty" jsonschema:"only rooms with a message at or after this date (YYYY-MM-DD or RFC 3339)"`
	ActiveBefore  string `json:"active_before,omitempty" jsonschema:"only rooms whose last message is before this date"`
	Limit         int    `json:"limit,omitempty" jsonschema:"max rooms (default 50, max 500)"`
	Offset        int    `json:"offset,omitempty" jsonschema:"skip this many matching rooms (pagination)"`
}

type ReadMessagesIn struct {
	Room         string `json:"room" jsonschema:"room ID, alias or unique part of the room name"`
	Limit        int    `json:"limit,omitempty" jsonschema:"number of events to fetch (default 30, max 200)"`
	Before       string `json:"before,omitempty" jsonschema:"pagination token from a previous call's next_before, to read older messages"`
	IncludeState bool   `json:"include_state,omitempty" jsonschema:"also include membership and room name/topic changes"`
}

type ContextIn struct {
	Room    string `json:"room" jsonschema:"room ID, alias or unique part of the room name"`
	EventID string `json:"message_id" jsonschema:"message to show context around, e.g. a search result"`
	Limit   int    `json:"limit,omitempty" jsonschema:"events before and after (default 10, max 50)"`
}

type SearchIn struct {
	Query  string `json:"query,omitempty" jsonschema:"words to search for; all must match, last word is a prefix. Optional when sender is set"`
	Room   string `json:"room,omitempty" jsonschema:"restrict to one room"`
	Sender string `json:"sender,omitempty" jsonschema:"restrict to a sender: Matrix ID, part of the display name, or 'me'"`
	Days   int    `json:"days,omitempty" jsonschema:"only messages from the last N days"`
	After  string `json:"after,omitempty" jsonschema:"only messages at or after this date (YYYY-MM-DD or RFC 3339)"`
	Before string `json:"before,omitempty" jsonschema:"only messages before this date (YYYY-MM-DD or RFC 3339)"`
	Limit  int    `json:"limit,omitempty" jsonschema:"max results (default 30, max 200)"`
}

type BackfillIn struct {
	Room  string `json:"room" jsonschema:"room ID, alias or unique part of the room name"`
	Limit int    `json:"limit,omitempty" jsonschema:"how many events back to index (default 1000, max 20000)"`
}

type SendIn struct {
	Room       string `json:"room" jsonschema:"room ID, alias or unique part of the room name"`
	Text       string `json:"text" jsonschema:"message text; Markdown is rendered unless format is plain"`
	Format     string `json:"format,omitempty" jsonschema:"markdown (default) or plain"`
	MsgType    string `json:"msgtype,omitempty" jsonschema:"text (default), notice or emote"`
	ReplyTo    string `json:"reply_to,omitempty" jsonschema:"message_id to reply to"`
	ThreadRoot string `json:"thread_root,omitempty" jsonschema:"message_id of a thread root to post into"`
}

type ReactIn struct {
	Room    string `json:"room" jsonschema:"room ID, alias or unique part of the room name"`
	EventID string `json:"message_id" jsonschema:"message to react to"`
	Emoji   string `json:"emoji" jsonschema:"reaction key, usually a single emoji"`
}

type EditIn struct {
	Room    string `json:"room" jsonschema:"room ID, alias or unique part of the room name"`
	EventID string `json:"message_id" jsonschema:"your own message to edit"`
	Text    string `json:"text" jsonschema:"new text (Markdown)"`
}

type DeleteIn struct {
	Room    string `json:"room" jsonschema:"room ID, alias or unique part of the room name"`
	EventID string `json:"message_id" jsonschema:"message to delete"`
	Reason  string `json:"reason,omitempty"`
}

type MarkReadIn struct {
	Room    string `json:"room" jsonschema:"room ID, alias or unique part of the room name"`
	EventID string `json:"message_id,omitempty" jsonschema:"mark read up to this message (default: latest)"`
}

type SendFileIn struct {
	Room       string `json:"room" jsonschema:"room ID, alias or unique part of the room name"`
	URL        string `json:"url,omitempty" jsonschema:"http(s) URL to download the file from"`
	Base64Data string `json:"base64_data,omitempty" jsonschema:"file contents as base64 (alternative to url)"`
	Filename   string `json:"filename,omitempty" jsonschema:"file name shown to recipients"`
	MimeType   string `json:"mime_type,omitempty" jsonschema:"MIME type; detected if empty"`
	Caption    string `json:"caption,omitempty" jsonschema:"optional caption text"`
	ReplyTo    string `json:"reply_to,omitempty" jsonschema:"message_id to reply to"`
}

type DownloadIn struct {
	Room    string `json:"room" jsonschema:"room ID, alias or unique part of the room name"`
	EventID string `json:"message_id" jsonschema:"image/video/audio/file message to download"`
	Variant string `json:"variant,omitempty" jsonschema:"for images: thumbnail (default, longest side 768 px, cheap) or full (original resolution)"`
}

type JoinIn struct {
	Room string `json:"room" jsonschema:"room ID or alias to join, or a pending invite's room ID"`
}

type InviteIn struct {
	Room   string `json:"room" jsonschema:"room ID, alias or unique part of the room name"`
	UserID string `json:"user_id" jsonschema:"Matrix user ID to invite"`
}

type KickIn struct {
	Room   string `json:"room" jsonschema:"room ID, alias or unique part of the room name"`
	UserID string `json:"user_id" jsonschema:"Matrix user ID to remove"`
	Reason string `json:"reason,omitempty"`
	Ban    bool   `json:"ban,omitempty" jsonschema:"ban instead of kick"`
}

type CreateRoomIn struct {
	Name      string   `json:"name,omitempty"`
	Topic     string   `json:"topic,omitempty"`
	Invite    []string `json:"invite,omitempty" jsonschema:"Matrix user IDs to invite"`
	Direct    bool     `json:"direct,omitempty" jsonschema:"mark as a direct chat"`
	Encrypted *bool    `json:"encrypted,omitempty" jsonschema:"enable end-to-end encryption (default true)"`
	Public    bool     `json:"public,omitempty" jsonschema:"public room listed in the directory"`
}

type UpdateRoomIn struct {
	Room    string   `json:"room" jsonschema:"room ID, alias or unique part of the room name"`
	Name    *string  `json:"name,omitempty" jsonschema:"new room name"`
	Topic   *string  `json:"topic,omitempty" jsonschema:"new topic"`
	AddTags []string `json:"add_tags,omitempty" jsonschema:"tags to add, e.g. favourite, lowpriority or u.custom"`
	DelTags []string `json:"remove_tags,omitempty" jsonschema:"tags to remove"`
}

type BridgeCommandIn struct {
	Network     string `json:"network" jsonschema:"messenger, instagram, whatsapp, discord or imessage"`
	Command     string `json:"command" jsonschema:"bridge bot command, e.g. help, list-logins, search <name>, start-chat <identifier>, sync"`
	WaitSeconds int    `json:"wait_seconds,omitempty" jsonschema:"how long to collect bot replies (default 8, max 60)"`
}

type WaitIn struct {
	Room           string `json:"room,omitempty" jsonschema:"only wait for messages in this room"`
	TimeoutSeconds int    `json:"timeout_seconds,omitempty" jsonschema:"max wait (default 30, max 300)"`
	IncludeOwn     bool   `json:"include_own,omitempty" jsonschema:"also return messages sent by the account itself"`
}

type APIIn struct {
	Method string `json:"method" jsonschema:"GET, POST, PUT or DELETE"`
	Path   string `json:"path" jsonschema:"path starting with /_matrix/ or /_synapse/, including query string"`
	Body   any    `json:"body,omitempty" jsonschema:"JSON request body (usually an object)"`
}

type NoArgs struct{}

// ---- registration ----

func registerTools(s *mcp.Server, app *App) {
	tool(s, app, &mcp.Tool{Name: "status", Description: "Connection, encryption and indexing status of the Matrix MCP.", Annotations: readOnly("Status")},
		func(ctx context.Context, _ NoArgs) (any, error) {
			indexed, _ := app.store.IndexedCount(ctx)
			var lastSync string
			if ts := app.lastSyncOK.Load(); ts > 0 {
				lastSync = time.Unix(ts, 0).UTC().Format(time.RFC3339)
			}
			return map[string]any{
				"user_id": app.cli.UserID, "device_id": app.cli.DeviceID, "ready": app.ready.Load(),
				"device_verified": app.verified.Load(), "key_backup": app.backupStatus,
				"last_sync": lastSync, "rooms": app.rooms.Count(), "pending_invites": app.rooms.Invites(),
				"indexed_messages": indexed, "version": version,
			}, nil
		})

	tool(s, app, &mcp.Tool{Name: "list_rooms", Description: "List joined rooms (chats) sorted by most recent activity, with network (messenger/instagram/whatsapp/discord/imessage), kind, unread counts, last message preview and last_sender_is_me. query matches room names only; to find a chat by its content use search_messages. Use awaiting_reply to find chats you have not answered.", Annotations: readOnly("List rooms")},
		func(ctx context.Context, in ListRoomsIn) (any, error) {
			limit := clamp(in.Limit, 50, 500)
			after, before, err := timeRange(in.ActiveAfter, in.ActiveBefore)
			if err != nil {
				return nil, err
			}
			q := strings.ToLower(in.Query)
			var out []*RoomInfo
			skipped, total := 0, 0
			for _, r := range app.rooms.List() {
				if q != "" && !strings.Contains(strings.ToLower(r.Name), q) {
					continue
				}
				if in.Network != "" {
					want := strings.ToLower(in.Network)
					if want == "matrix" && r.Network != "" || want != "matrix" && r.Network != want {
						continue
					}
				}
				if in.Kind != "" && r.Kind != in.Kind {
					continue
				}
				if in.UnreadOnly && r.Unread == 0 {
					continue
				}
				if in.AwaitingReply && (r.Kind != "dm" && r.Kind != "group" || r.lastTS == 0 || r.LastFromMe) {
					continue
				}
				if after > 0 && r.lastTS < after || before > 0 && (r.lastTS == 0 || r.lastTS >= before) {
					continue
				}
				total++
				if skipped < in.Offset {
					skipped++
					continue
				}
				if len(out) < limit {
					out = append(out, r)
				}
			}
			res := map[string]any{"rooms": out, "total": total, "pending_invites": app.rooms.Invites()}
			if in.Offset+len(out) < total {
				res["next_offset"] = in.Offset + len(out)
			}
			return res, nil
		})

	tool(s, app, &mcp.Tool{Name: "get_room", Description: "Details of a room including members.", Annotations: readOnly("Get room")},
		func(ctx context.Context, in RoomArg) (any, error) {
			room, err := app.resolveRoom(ctx, in.Room)
			if err != nil {
				return nil, err
			}
			members, err := app.cli.JoinedMembers(ctx, room.ID)
			if err != nil {
				return nil, err
			}
			type member struct {
				UserID  id.UserID `json:"user_id"`
				Name    string    `json:"name,omitempty"`
				Network string    `json:"network,omitempty"`
				Bot     bool      `json:"bridge_bot,omitempty"`
			}
			var list []member
			for uid, m := range members.Joined {
				n, bot := networkOf(uid)
				list = append(list, member{UserID: uid, Name: m.DisplayName, Network: n, Bot: bot})
			}
			return map[string]any{"room": room, "members": list}, nil
		})

	tool(s, app, &mcp.Tool{Name: "read_messages", Description: "Read recent messages of a room (oldest first, decrypted). Edits, reactions and deletions are folded into the messages. Use next_before to page further back.", Annotations: readOnly("Read messages")},
		func(ctx context.Context, in ReadMessagesIn) (any, error) {
			room, err := app.resolveRoom(ctx, in.Room)
			if err != nil {
				return nil, err
			}
			events, next, err := app.fetchHistory(ctx, room.ID, in.Before, clamp(in.Limit, 30, 200))
			if err != nil {
				return nil, err
			}
			var msgs []*Message
			undecryptable := 0
			for _, evt := range events {
				app.ingest(ctx, evt)
				if m := app.toMessage(ctx, evt); m != nil {
					if m.Kind == "undecryptable" {
						undecryptable++
					}
					msgs = append(msgs, m)
				}
			}
			res := map[string]any{"room": room.Name, "room_id": room.ID, "network": room.Network, "messages": aggregate(msgs, in.IncludeState), "next_before": next}
			if undecryptable > 0 {
				res["note"] = fmt.Sprintf("%d messages could not be decrypted (keys missing from this device and the key backup)", undecryptable)
			}
			return res, nil
		})

	tool(s, app, &mcp.Tool{Name: "get_message_context", Description: "Show messages around a specific event, e.g. a search hit.", Annotations: readOnly("Message context")},
		func(ctx context.Context, in ContextIn) (any, error) {
			room, err := app.resolveRoom(ctx, in.Room)
			if err != nil {
				return nil, err
			}
			resp, err := app.cli.Context(ctx, room.ID, id.EventID(in.EventID), nil, clamp(in.Limit, 10, 50)*2)
			if err != nil {
				return nil, err
			}
			var raw []*event.Event
			for i := len(resp.EventsBefore) - 1; i >= 0; i-- {
				raw = append(raw, resp.EventsBefore[i])
			}
			raw = append(raw, resp.Event)
			raw = append(raw, resp.EventsAfter...)
			var msgs []*Message
			for _, evt := range raw {
				if evt == nil {
					continue
				}
				evt.RoomID = room.ID
				if m := app.toMessage(ctx, app.decryptIfNeeded(ctx, evt)); m != nil {
					msgs = append(msgs, m)
				}
			}
			return map[string]any{"room": room.Name, "room_id": room.ID, "target": in.EventID, "messages": aggregate(msgs, false)}, nil
		})

	tool(s, app, &mcp.Tool{Name: "search_messages", Description: "Full-text search over decrypted message bodies of all rooms (the latest messages of every room plus anything read or backfilled), newest first. Use it to find a chat by something that was said (a link, name, keyword). Filter by sender, room and date range; with only sender set it lists that person's messages. Use backfill_room to index older history first.", Annotations: readOnly("Search messages")},
		func(ctx context.Context, in SearchIn) (any, error) {
			if strings.TrimSpace(in.Query) == "" && strings.TrimSpace(in.Sender) == "" {
				return nil, errors.New("query or sender is required")
			}
			after, before, err := timeRange(in.After, in.Before)
			if err != nil {
				return nil, err
			}
			var roomID id.RoomID
			if in.Room != "" {
				room, err := app.resolveRoom(ctx, in.Room)
				if err != nil {
					return nil, err
				}
				roomID = room.ID
			}
			if in.Days > 0 {
				after = max(after, time.Now().Add(-time.Duration(in.Days)*24*time.Hour).UnixMilli())
			}
			sender := strings.TrimSpace(in.Sender)
			if strings.EqualFold(sender, "me") {
				sender = app.cli.UserID.String()
			}
			hits, err := app.store.Search(ctx, SearchQuery{Text: in.Query, RoomID: roomID, Sender: sender, After: after, Before: before, Limit: clamp(in.Limit, 30, 200)})
			if err != nil {
				return nil, err
			}
			for i := range hits {
				hits[i].FromMe = hits[i].Sender == app.cli.UserID.String()
				if r, err := app.rooms.Get(ctx, id.RoomID(hits[i].RoomID)); err == nil {
					hits[i].RoomName = r.Name
				}
			}
			return map[string]any{"results": hits}, nil
		})

	tool(s, app, &mcp.Tool{Name: "backfill_room", Description: "Fetch and index older history of a room so search_messages can find it.", Annotations: &mcp.ToolAnnotations{Title: "Index room history", IdempotentHint: true, OpenWorldHint: ptr(false)}},
		func(ctx context.Context, in BackfillIn) (any, error) {
			room, err := app.resolveRoom(ctx, in.Room)
			if err != nil {
				return nil, err
			}
			n, err := app.backfillRoom(ctx, room.ID, clamp(in.Limit, 1000, 20000))
			if err != nil {
				return nil, err
			}
			return map[string]any{"room": room.Name, "events_processed": n}, nil
		})

	tool(s, app, &mcp.Tool{Name: "send_message", Description: "Send a text message to a room. In bridged rooms this is delivered to the real person/group on Messenger, WhatsApp, etc.", Annotations: writes("Send message", false)},
		func(ctx context.Context, in SendIn) (any, error) {
			room, err := app.resolveRoom(ctx, in.Room)
			if err != nil {
				return nil, err
			}
			if strings.TrimSpace(in.Text) == "" {
				return nil, errors.New("text is empty")
			}
			var content event.MessageEventContent
			if in.Format == "plain" {
				content = event.MessageEventContent{MsgType: event.MsgText, Body: in.Text}
			} else {
				content = format.RenderMarkdown(in.Text, true, false)
			}
			switch in.MsgType {
			case "notice":
				content.MsgType = event.MsgNotice
			case "emote":
				content.MsgType = event.MsgEmote
			}
			app.addRelations(&content, room.ID, in.ReplyTo, in.ThreadRoot)
			resp, err := app.cli.SendMessageEvent(ctx, room.ID, event.EventMessage, &content)
			if err != nil {
				return nil, err
			}
			return map[string]any{"message_id": resp.EventID, "room": room.Name, "network": room.Network}, nil
		})

	tool(s, app, &mcp.Tool{Name: "send_reaction", Description: "React to a message with an emoji.", Annotations: writes("React", false)},
		func(ctx context.Context, in ReactIn) (any, error) {
			room, err := app.resolveRoom(ctx, in.Room)
			if err != nil {
				return nil, err
			}
			resp, err := app.cli.SendReaction(ctx, room.ID, id.EventID(in.EventID), in.Emoji)
			if err != nil {
				return nil, err
			}
			return map[string]any{"message_id": resp.EventID}, nil
		})

	tool(s, app, &mcp.Tool{Name: "edit_message", Description: "Edit a message previously sent by this account.", Annotations: writes("Edit message", false)},
		func(ctx context.Context, in EditIn) (any, error) {
			room, err := app.resolveRoom(ctx, in.Room)
			if err != nil {
				return nil, err
			}
			newContent := format.RenderMarkdown(in.Text, true, false)
			content := newContent
			content.Body = "* " + newContent.Body
			if content.FormattedBody != "" {
				content.FormattedBody = "* " + content.FormattedBody
			}
			content.SetEdit(id.EventID(in.EventID))
			content.NewContent = &newContent
			resp, err := app.cli.SendMessageEvent(ctx, room.ID, event.EventMessage, &content)
			if err != nil {
				return nil, err
			}
			return map[string]any{"message_id": resp.EventID}, nil
		})

	tool(s, app, &mcp.Tool{Name: "delete_message", Description: "Delete (redact) a message. On bridges this unsends it where supported.", Annotations: writes("Delete message", true)},
		func(ctx context.Context, in DeleteIn) (any, error) {
			room, err := app.resolveRoom(ctx, in.Room)
			if err != nil {
				return nil, err
			}
			resp, err := app.cli.RedactEvent(ctx, room.ID, id.EventID(in.EventID), mautrix.ReqRedact{Reason: in.Reason})
			if err != nil {
				return nil, err
			}
			return map[string]any{"deleted": in.EventID, "redaction_id": resp.EventID}, nil
		})

	tool(s, app, &mcp.Tool{Name: "mark_read", Description: "Mark a room as read (sends a read receipt, which bridges forward as 'seen').", Annotations: writes("Mark read", false)},
		func(ctx context.Context, in MarkReadIn) (any, error) {
			room, err := app.resolveRoom(ctx, in.Room)
			if err != nil {
				return nil, err
			}
			target := id.EventID(in.EventID)
			if target == "" {
				resp, err := app.cli.Messages(ctx, room.ID, "", "", mautrix.DirectionBackward, nil, 1)
				if err != nil {
					return nil, err
				}
				if len(resp.Chunk) == 0 {
					return nil, errors.New("room has no events")
				}
				target = resp.Chunk[0].ID
			}
			if err := app.cli.SetReadMarkers(ctx, room.ID, &mautrix.ReqSetReadMarkers{Read: target, FullyRead: target}); err != nil {
				return nil, err
			}
			app.rooms.SetUnread(ctx, room.ID, 0, 0)
			return map[string]any{"marked_read_up_to": target}, nil
		})

	tool(s, app, &mcp.Tool{Name: "send_file", Description: "Send an image, video, audio or other file to a room, from a URL or base64 data. Encrypted automatically in encrypted rooms.", Annotations: writes("Send file", false)},
		func(ctx context.Context, in SendFileIn) (any, error) {
			room, err := app.resolveRoom(ctx, in.Room)
			if err != nil {
				return nil, err
			}
			data, filename, mime, err := loadFile(ctx, in)
			if err != nil {
				return nil, err
			}
			content := event.MessageEventContent{MsgType: msgTypeFor(mime), Body: filename, FileName: filename,
				Info: &event.FileInfo{MimeType: mime, Size: len(data)}}
			if in.Caption != "" {
				content.Body = in.Caption
			}
			uploadMime := mime
			if room.Encrypted {
				file := attachment.NewEncryptedFile()
				file.EncryptInPlace(data)
				uploadMime = "application/octet-stream"
				up, err := app.cli.UploadBytes(ctx, data, uploadMime)
				if err != nil {
					return nil, err
				}
				content.File = &event.EncryptedFileInfo{EncryptedFile: *file, URL: up.ContentURI.CUString()}
			} else {
				up, err := app.cli.UploadBytes(ctx, data, uploadMime)
				if err != nil {
					return nil, err
				}
				content.URL = up.ContentURI.CUString()
			}
			app.addRelations(&content, room.ID, in.ReplyTo, "")
			resp, err := app.cli.SendMessageEvent(ctx, room.ID, event.EventMessage, &content)
			if err != nil {
				return nil, err
			}
			return map[string]any{"message_id": resp.EventID, "filename": filename, "size": content.Info.Size}, nil
		})

	tool(s, app, &mcp.Tool{Name: "download_media", Description: "Download (and decrypt) the attachment of a message. Images are returned inline so you can look at them, as a 768 px thumbnail unless variant is full; audio inline; other files as embedded resources up to 8 MB.", Annotations: readOnly("Download media")},
		func(ctx context.Context, in DownloadIn) (any, error) {
			room, err := app.resolveRoom(ctx, in.Room)
			if err != nil {
				return nil, err
			}
			evt, err := app.cli.GetEvent(ctx, room.ID, id.EventID(in.EventID))
			if err != nil {
				return nil, err
			}
			evt.RoomID = room.ID
			evt = app.decryptIfNeeded(ctx, evt)
			content, ok := evt.Content.Parsed.(*event.MessageEventContent)
			if !ok {
				return nil, errors.New("event is not a message (or could not be decrypted)")
			}
			var data []byte
			if content.File != nil {
				uri, err := content.File.URL.Parse()
				if err != nil {
					return nil, err
				}
				if data, err = app.cli.DownloadBytes(ctx, uri); err != nil {
					return nil, err
				}
				if err = content.File.DecryptInPlace(data); err != nil {
					return nil, fmt.Errorf("decrypt attachment: %w", err)
				}
			} else if content.URL != "" {
				uri, err := content.URL.Parse()
				if err != nil {
					return nil, err
				}
				if data, err = app.cli.DownloadBytes(ctx, uri); err != nil {
					return nil, err
				}
			} else {
				return nil, errors.New("message has no attachment")
			}
			mime := ""
			if content.Info != nil {
				mime = content.Info.MimeType
			}
			if mime == "" {
				mime = http.DetectContentType(data)
			}
			name := content.FileName
			if name == "" {
				name = content.Body
			}
			if strings.HasPrefix(mime, "image/") && in.Variant != "full" {
				if thumb, w, h, ok := thumbnail(data, thumbnailMaxDim); ok {
					return &mcp.CallToolResult{Content: []mcp.Content{
						&mcp.TextContent{Text: fmt.Sprintf("%s (%s, %d bytes; thumbnail %dx%d, use variant full for the original)", name, mime, len(data), w, h)},
						&mcp.ImageContent{Data: thumb, MIMEType: "image/jpeg"},
					}}, nil
				}
			}
			if len(data) > maxInlineMediaBytes {
				return nil, fmt.Errorf("attachment is %d bytes, larger than the %d byte limit", len(data), maxInlineMediaBytes)
			}
			if strings.HasPrefix(mime, "image/") {
				return &mcp.CallToolResult{Content: []mcp.Content{
					&mcp.TextContent{Text: fmt.Sprintf("%s (%s, %d bytes)", name, mime, len(data))},
					&mcp.ImageContent{Data: data, MIMEType: mime},
				}}, nil
			}
			if strings.HasPrefix(mime, "audio/") {
				return &mcp.CallToolResult{Content: []mcp.Content{
					&mcp.TextContent{Text: fmt.Sprintf("%s (%s, %d bytes)", name, mime, len(data))},
					&mcp.AudioContent{Data: data, MIMEType: mime},
				}}, nil
			}
			res := &mcp.ResourceContents{URI: "matrix://" + room.ID.String() + "/" + in.EventID + "/" + url.PathEscape(name), MIMEType: mime}
			if strings.HasPrefix(mime, "text/") || mime == "application/json" {
				res.Text = string(data)
			} else {
				res.Blob = data
			}
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.EmbeddedResource{Resource: res}}}, nil
		})

	tool(s, app, &mcp.Tool{Name: "join_room", Description: "Join a room by ID or alias, or accept a pending invite.", Annotations: writes("Join room", false)},
		func(ctx context.Context, in JoinIn) (any, error) {
			resp, err := app.cli.JoinRoom(ctx, in.Room, nil)
			if err != nil {
				return nil, err
			}
			info, _ := app.rooms.Get(ctx, resp.RoomID)
			return map[string]any{"room_id": resp.RoomID, "room": info}, nil
		})

	tool(s, app, &mcp.Tool{Name: "leave_room", Description: "Leave a room or reject an invite. Leaving a bridged chat may delete it on the bridge side.", Annotations: writes("Leave room", true)},
		func(ctx context.Context, in RoomArg) (any, error) {
			roomID := id.RoomID(in.Room)
			if !strings.HasPrefix(in.Room, "!") {
				room, err := app.resolveRoom(ctx, in.Room)
				if err != nil {
					return nil, err
				}
				roomID = room.ID
			}
			if _, err := app.cli.LeaveRoom(ctx, roomID); err != nil {
				return nil, err
			}
			app.rooms.Forget(roomID)
			return map[string]any{"left": roomID}, nil
		})

	tool(s, app, &mcp.Tool{Name: "invite_user", Description: "Invite a Matrix user to a room.", Annotations: writes("Invite user", false)},
		func(ctx context.Context, in InviteIn) (any, error) {
			room, err := app.resolveRoom(ctx, in.Room)
			if err != nil {
				return nil, err
			}
			_, err = app.cli.InviteUser(ctx, room.ID, &mautrix.ReqInviteUser{UserID: id.UserID(in.UserID)})
			if err != nil {
				return nil, err
			}
			return map[string]any{"invited": in.UserID, "room": room.Name}, nil
		})

	tool(s, app, &mcp.Tool{Name: "remove_user", Description: "Kick or ban a user from a room.", Annotations: writes("Remove user", true)},
		func(ctx context.Context, in KickIn) (any, error) {
			room, err := app.resolveRoom(ctx, in.Room)
			if err != nil {
				return nil, err
			}
			if in.Ban {
				_, err = app.cli.BanUser(ctx, room.ID, &mautrix.ReqBanUser{UserID: id.UserID(in.UserID), Reason: in.Reason})
			} else {
				_, err = app.cli.KickUser(ctx, room.ID, &mautrix.ReqKickUser{UserID: id.UserID(in.UserID), Reason: in.Reason})
			}
			if err != nil {
				return nil, err
			}
			return map[string]any{"removed": in.UserID, "banned": in.Ban}, nil
		})

	tool(s, app, &mcp.Tool{Name: "create_room", Description: "Create a new Matrix room (encrypted by default).", Annotations: writes("Create room", false)},
		func(ctx context.Context, in CreateRoomIn) (any, error) {
			req := &mautrix.ReqCreateRoom{Name: in.Name, Topic: in.Topic, IsDirect: in.Direct, Preset: "private_chat"}
			if in.Public {
				req.Preset = "public_chat"
				req.Visibility = "public"
			}
			for _, u := range in.Invite {
				req.Invite = append(req.Invite, id.UserID(u))
			}
			if in.Encrypted == nil || *in.Encrypted {
				req.InitialState = append(req.InitialState, &event.Event{
					Type:    event.StateEncryption,
					Content: event.Content{Parsed: &event.EncryptionEventContent{Algorithm: id.AlgorithmMegolmV1}},
				})
			}
			resp, err := app.cli.CreateRoom(ctx, req)
			if err != nil {
				return nil, err
			}
			return map[string]any{"room_id": resp.RoomID}, nil
		})

	tool(s, app, &mcp.Tool{Name: "update_room", Description: "Rename a room, change its topic, or add/remove tags like favourite and lowpriority.", Annotations: writes("Update room", false)},
		func(ctx context.Context, in UpdateRoomIn) (any, error) {
			room, err := app.resolveRoom(ctx, in.Room)
			if err != nil {
				return nil, err
			}
			done := []string{}
			if in.Name != nil {
				if _, err := app.cli.SendStateEvent(ctx, room.ID, event.StateRoomName, "", &event.RoomNameEventContent{Name: *in.Name}); err != nil {
					return nil, err
				}
				done = append(done, "name")
			}
			if in.Topic != nil {
				if _, err := app.cli.SendStateEvent(ctx, room.ID, event.StateTopic, "", &event.TopicEventContent{Topic: *in.Topic}); err != nil {
					return nil, err
				}
				done = append(done, "topic")
			}
			for _, t := range in.AddTags {
				if err := app.cli.AddTag(ctx, room.ID, event.RoomTag(normalizeTag(t)), 0.5); err != nil {
					return nil, err
				}
				done = append(done, "+"+t)
			}
			for _, t := range in.DelTags {
				if err := app.cli.RemoveTag(ctx, room.ID, event.RoomTag(normalizeTag(t))); err != nil {
					return nil, err
				}
				done = append(done, "-"+t)
			}
			app.rooms.MarkDirty(room.ID)
			return map[string]any{"updated": done}, nil
		})

	tool(s, app, &mcp.Tool{Name: "bridge_command", Description: "Send a command to a bridge bot (messenger, instagram, whatsapp, discord, imessage) in its management room and return the bot's replies. Useful commands: help, list-logins, search <name>, start-chat <identifier>, resolve-identifier <id>, sync-chats.", Annotations: writes("Bridge command", false)},
		func(ctx context.Context, in BridgeCommandIn) (any, error) {
			network := strings.ToLower(strings.TrimSpace(in.Network))
			var mgmt *RoomInfo
			for _, r := range app.rooms.List() {
				if r.Kind == "bridge_bot" && r.Network == network {
					mgmt = r
					break
				}
			}
			if mgmt == nil {
				return nil, fmt.Errorf("no management room with the %s bridge bot found", network)
			}
			n, ch := app.addWaiter(mgmt.ID)
			defer app.removeWaiter(n)
			content := event.MessageEventContent{MsgType: event.MsgText, Body: in.Command}
			sent, err := app.cli.SendMessageEvent(ctx, mgmt.ID, event.EventMessage, &content)
			if err != nil {
				return nil, err
			}
			wait := time.Duration(clamp(in.WaitSeconds, 8, 60)) * time.Second
			deadline := time.After(wait)
			var replies []string
			var quiet <-chan time.Time
			for {
				select {
				case evt := <-ch:
					if evt.Sender != mgmt.BotUser || evt.ID == sent.EventID {
						continue
					}
					if m := app.toMessage(ctx, evt); m != nil && m.Body != "" {
						replies = append(replies, m.Body)
					}
					quiet = time.After(2 * time.Second)
				case <-quiet:
					return map[string]any{"network": network, "replies": replies}, nil
				case <-deadline:
					return map[string]any{"network": network, "replies": replies, "note": "stopped waiting; more replies may arrive later (read_messages on the management room)"}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
		})

	tool(s, app, &mcp.Tool{Name: "wait_for_messages", Description: "Wait for new incoming messages (in one room or any room) and return them as soon as they arrive, or an empty list on timeout.", Annotations: readOnly("Wait for messages")},
		func(ctx context.Context, in WaitIn) (any, error) {
			var roomID id.RoomID
			if in.Room != "" {
				room, err := app.resolveRoom(ctx, in.Room)
				if err != nil {
					return nil, err
				}
				roomID = room.ID
			}
			n, ch := app.addWaiter(roomID)
			defer app.removeWaiter(n)
			timeout := time.After(time.Duration(clamp(in.TimeoutSeconds, 30, 300)) * time.Second)
			var msgs []map[string]any
			var settle <-chan time.Time
			for {
				select {
				case evt := <-ch:
					if !in.IncludeOwn && evt.Sender == app.cli.UserID {
						continue
					}
					m := app.toMessage(ctx, evt)
					if m == nil || m.Kind == "redaction" {
						continue
					}
					name := ""
					if r, err := app.rooms.Get(ctx, evt.RoomID); err == nil {
						name = r.Name
					}
					msgs = append(msgs, map[string]any{"room": name, "room_id": evt.RoomID, "message": m})
					settle = time.After(1500 * time.Millisecond)
				case <-settle:
					return map[string]any{"messages": msgs}, nil
				case <-timeout:
					return map[string]any{"messages": msgs}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
		})

	tool(s, app, &mcp.Tool{Name: "matrix_api", Description: "Escape hatch: call any Matrix client-server or Synapse admin API endpoint as this user and return the raw JSON. Note: events read this way are not decrypted.", Annotations: writes("Raw Matrix API", true)},
		func(ctx context.Context, in APIIn) (any, error) {
			method := strings.ToUpper(in.Method)
			switch method {
			case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete:
			default:
				return nil, errors.New("method must be GET, POST, PUT or DELETE")
			}
			if !strings.HasPrefix(in.Path, "/_matrix/") && !strings.HasPrefix(in.Path, "/_synapse/") {
				return nil, errors.New("path must start with /_matrix/ or /_synapse/")
			}
			if strings.Contains(path.Clean(strings.SplitN(in.Path, "?", 2)[0]), "..") {
				return nil, errors.New("invalid path")
			}
			full := strings.TrimRight(app.cli.HomeserverURL.String(), "/") + in.Path
			raw, err := app.cli.MakeRequest(ctx, method, full, in.Body, nil)
			if err != nil {
				return nil, err
			}
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(raw)}}}, nil
		})
}

func normalizeTag(t string) string {
	switch t {
	case "favourite", "favorite", "lowpriority", "low_priority", "server_notice":
		if t == "favorite" {
			t = "favourite"
		}
		if t == "low_priority" {
			t = "lowpriority"
		}
		return "m." + t
	}
	return t
}

func (app *App) addRelations(content *event.MessageEventContent, roomID id.RoomID, replyTo, threadRoot string) {
	if threadRoot != "" {
		content.RelatesTo = &event.RelatesTo{Type: event.RelThread, EventID: id.EventID(threadRoot)}
		if replyTo != "" {
			content.RelatesTo.InReplyTo = &event.InReplyTo{EventID: id.EventID(replyTo)}
		} else {
			content.RelatesTo.IsFallingBack = true
			content.RelatesTo.InReplyTo = &event.InReplyTo{EventID: id.EventID(threadRoot)}
		}
		return
	}
	if replyTo != "" {
		content.RelatesTo = &event.RelatesTo{InReplyTo: &event.InReplyTo{EventID: id.EventID(replyTo)}}
	}
}

func msgTypeFor(mime string) event.MessageType {
	switch {
	case strings.HasPrefix(mime, "image/"):
		return event.MsgImage
	case strings.HasPrefix(mime, "video/"):
		return event.MsgVideo
	case strings.HasPrefix(mime, "audio/"):
		return event.MsgAudio
	}
	return event.MsgFile
}

func loadFile(ctx context.Context, in SendFileIn) (data []byte, filename, mime string, err error) {
	filename, mime = in.Filename, in.MimeType
	switch {
	case in.Base64Data != "":
		data, err = base64.StdEncoding.DecodeString(strings.TrimSpace(in.Base64Data))
		if err != nil {
			return nil, "", "", fmt.Errorf("invalid base64: %w", err)
		}
	case in.URL != "":
		u, perr := url.Parse(in.URL)
		if perr != nil || (u.Scheme != "http" && u.Scheme != "https") {
			return nil, "", "", errors.New("url must be http or https")
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, in.URL, nil)
		resp, derr := (&http.Client{Timeout: 60 * time.Second}).Do(req)
		if derr != nil {
			return nil, "", "", derr
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			return nil, "", "", fmt.Errorf("download failed: HTTP %d", resp.StatusCode)
		}
		data, err = io.ReadAll(io.LimitReader(resp.Body, maxUploadBytes+1))
		if err != nil {
			return nil, "", "", err
		}
		if mime == "" {
			mime = strings.TrimSpace(strings.SplitN(resp.Header.Get("Content-Type"), ";", 2)[0])
		}
		if filename == "" {
			filename = path.Base(u.Path)
		}
	default:
		return nil, "", "", errors.New("provide url or base64_data")
	}
	if len(data) > maxUploadBytes {
		return nil, "", "", fmt.Errorf("file larger than %d bytes", maxUploadBytes)
	}
	if mime == "" || mime == "application/octet-stream" {
		mime = http.DetectContentType(data)
		mime = strings.TrimSpace(strings.SplitN(mime, ";", 2)[0])
	}
	if filename == "" || filename == "/" || filename == "." {
		filename = "file"
	}
	return data, filename, mime, nil
}
