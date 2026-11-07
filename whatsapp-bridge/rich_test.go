package main

import (
	"database/sql"
	"io"
	"os"
	"path/filepath"
	"testing"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

func TestParseRichTypes(t *testing.T) {
	cases := []struct {
		name    string
		msg     *waE2E.Message
		typ     string
		content string
		media   string
	}{
		{"text", &waE2E.Message{Conversation: proto.String("hi")}, "text", "hi", ""},
		{"image caption", &waE2E.Message{ImageMessage: &waE2E.ImageMessage{Caption: proto.String("look"), Mimetype: proto.String("image/png")}}, "image", "look", "image"},
		{"voice", &waE2E.Message{AudioMessage: &waE2E.AudioMessage{PTT: proto.Bool(true)}}, "voice", "", "audio"},
		{"sticker", &waE2E.Message{StickerMessage: &waE2E.StickerMessage{}}, "sticker", "", "sticker"},
		{"location", &waE2E.Message{LocationMessage: &waE2E.LocationMessage{Name: proto.String("Azhar"), DegreesLatitude: proto.Float64(30)}}, "location", "Azhar", ""},
		{"poll", &waE2E.Message{PollCreationMessageV3: &waE2E.PollCreationMessage{Name: proto.String("Q?"),
			Options: []*waE2E.PollCreationMessage_Option{{OptionName: proto.String("a")}, {OptionName: proto.String("b")}}}}, "poll", "Q?", ""},
		{"document", &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{FileName: proto.String("../x.pdf"), Caption: proto.String("c")}}, "document", "c", "document"},
	}
	for _, c := range cases {
		r, ok := ParseRich(c.msg, "ID1")
		if !ok || r.Type != c.typ || r.Content != c.content || r.MediaType != c.media {
			t.Errorf("%s: got ok=%v type=%q content=%q media=%q", c.name, ok, r.Type, r.Content, r.MediaType)
		}
		if c.name == "document" && r.Filename != ".._x.pdf" {
			t.Errorf("document filename not sanitized: %q", r.Filename)
		}
	}
	if _, ok := ParseRich(&waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{}}, "x"); ok {
		t.Error("reactions must not be stored as messages")
	}
}

func TestReplyAndMentionsRoundTrip(t *testing.T) {
	m := &waE2E.Message{Conversation: proto.String("hello @201000000000")}
	ok := setContextInfo(m, &waE2E.ContextInfo{StanzaID: proto.String("Q1"), Participant: proto.String("1@s.whatsapp.net"),
		QuotedMessage: &waE2E.Message{Conversation: proto.String("original")}, MentionedJID: []string{"201000000000@s.whatsapp.net"}})
	if !ok || m.Conversation != nil || m.ExtendedTextMessage == nil {
		t.Fatal("text should become ExtendedTextMessage with context")
	}
	r, _ := ParseRich(m, "ID")
	if r.QuotedID != "Q1" || r.QuotedText != "original" || len(r.Mentions) != 1 || r.Content != "hello @201000000000" {
		t.Fatalf("bad parse: %+v", r)
	}
	img := &waE2E.Message{ImageMessage: &waE2E.ImageMessage{}}
	setContextInfo(img, &waE2E.ContextInfo{IsForwarded: proto.Bool(true)})
	if r, _ := ParseRich(img, "I"); !r.IsForwarded {
		t.Fatal("forward flag lost on image")
	}
}

// TestMigrateRealCopy runs the migration on a copy of a real messages.db (WA_TEST_DB).
func TestMigrateRealCopy(t *testing.T) {
	src := os.Getenv("WA_TEST_DB")
	if src == "" {
		t.Skip("set WA_TEST_DB to a messages.db copy")
	}
	dst := filepath.Join(t.TempDir(), "m.db")
	in, _ := os.Open(src)
	out, _ := os.Create(dst)
	io.Copy(out, in)
	in.Close()
	out.Close()
	db, err := sql.Open("sqlite3", "file:"+dst+"?_foreign_keys=on")
	if err != nil {
		t.Fatal(err)
	}
	ms := &MessageStore{db: db}
	var before int
	db.QueryRow("SELECT COUNT(*) FROM messages").Scan(&before)
	for i := 0; i < 2; i++ { // idempotent
		if err := ms.Migrate(); err != nil {
			t.Fatal(err)
		}
	}
	var after int
	db.QueryRow("SELECT COUNT(*) FROM messages").Scan(&after)
	if before != after || before == 0 {
		t.Fatalf("row count changed: %d -> %d", before, after)
	}
	if _, err := db.Exec("SELECT msg_type, raw, quoted_id FROM messages LIMIT 1"); err != nil {
		t.Fatal(err)
	}
	t.Logf("migrated %d messages", after)
}
