package main

import (
	"slices"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestToolAllowlist(t *testing.T) {
	t.Setenv("MATRIX_USER_ID", "@test:example.com")
	t.Setenv("MATRIX_PASSWORD", "test-password")
	t.Setenv("PICKLE_KEY", "test-key")
	t.Setenv("MCP_BEARER_TOKEN", strings.Repeat("t", 32))

	readTools := []string{
		"backfill_room", "download_media", "get_message_context", "get_room", "list_rooms",
		"read_messages", "search_messages", "status", "wait_for_messages",
	}
	writeTools := []string{
		"bridge_command", "create_room", "delete_message", "edit_message", "invite_user",
		"join_room", "leave_room", "mark_read", "matrix_api", "remove_user", "send_file",
		"send_message", "send_reaction", "update_room",
	}
	t.Run("read-only by default", func(t *testing.T) {
		t.Setenv("MCP_ALLOW_WRITES", "")
		session := connectToolTestClient(t)
		checkListedTools(t, session, readTools)
		result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "list_rooms"})
		if err != nil || result.IsError {
			t.Fatalf("list_rooms failed: result=%v, err=%v", result, err)
		}
		for _, name := range writeTools {
			_, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: name})
			if err == nil || !strings.Contains(err.Error(), "unknown tool") {
				t.Errorf("calling %s: got %v, want unknown tool", name, err)
			}
		}
	})
	t.Run("writes explicitly enabled", func(t *testing.T) {
		t.Setenv("MCP_ALLOW_WRITES", "true")
		checkListedTools(t, connectToolTestClient(t), slices.Concat(readTools, writeTools))
	})
}

func connectToolTestClient(t *testing.T) *mcp.ClientSession {
	t.Helper()
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	app := &App{cfg: cfg, rooms: &RoomCache{}}
	app.ready.Store(true)
	server := mcp.NewServer(&mcp.Implementation{Name: "test-server", Version: version}, nil)
	registerTools(server, app)
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(t.Context(), serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = serverSession.Close() })
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: version}, nil)
	session, err := client.Connect(t.Context(), clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func checkListedTools(t *testing.T, session *mcp.ClientSession, want []string) {
	t.Helper()
	result, err := session.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(result.Tools))
	for _, tool := range result.Tools {
		got = append(got, tool.Name)
	}
	slices.Sort(got)
	want = slices.Clone(want)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("tools/list returned %v, want %v", got, want)
	}
}
