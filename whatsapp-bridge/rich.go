package main

// Rich message storage: every message type, replies, mentions, reactions, edits,
// deletions, poll votes, receipts, calls and group events.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var richColumns = map[string]map[string]string{
	"messages": {
		"sender_jid": "TEXT", "sender_pn": "TEXT", "push_name": "TEXT", "msg_type": "TEXT",
		"mimetype": "TEXT", "quoted_id": "TEXT", "quoted_sender": "TEXT", "quoted_text": "TEXT",
		"mentions": "TEXT", "is_forwarded": "INTEGER DEFAULT 0", "forwarding_score": "INTEGER DEFAULT 0",
		"is_ephemeral": "INTEGER DEFAULT 0", "is_view_once": "INTEGER DEFAULT 0",
		"edited_at": "TIMESTAMP", "original_content": "TEXT",
		"is_revoked": "INTEGER DEFAULT 0", "revoked_by": "TEXT", "revoked_at": "TIMESTAMP",
		"starred": "INTEGER DEFAULT 0", "extra": "TEXT", "raw": "BLOB", "server_id": "INTEGER",
	},
	"chats": {"unread_count": "INTEGER", "is_group": "INTEGER DEFAULT 0"},
}

// Migrate adds the rich columns and side tables; safe to run on every start.
func (store *MessageStore) Migrate() error {
	for table, cols := range richColumns {
		existing := map[string]bool{}
		rows, err := store.db.Query("SELECT name FROM pragma_table_info(?)", table)
		if err != nil {
			return err
		}
		for rows.Next() {
			var n string
			rows.Scan(&n)
			existing[n] = true
		}
		rows.Close()
		for col, typ := range cols {
			if !existing[col] {
				if _, err := store.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, col, typ)); err != nil {
					return fmt.Errorf("add %s.%s: %w", table, col, err)
				}
			}
		}
	}
	_, err := store.db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_messages_chat_ts ON messages(chat_jid, timestamp);
		CREATE INDEX IF NOT EXISTS idx_messages_id ON messages(id);
		CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(sender);
		CREATE TABLE IF NOT EXISTS reactions (
			chat_jid TEXT, message_id TEXT, sender_jid TEXT, emoji TEXT, timestamp TIMESTAMP,
			PRIMARY KEY (chat_jid, message_id, sender_jid));
		CREATE TABLE IF NOT EXISTS receipts (
			chat_jid TEXT, message_id TEXT, reader_jid TEXT, type TEXT, timestamp TIMESTAMP,
			PRIMARY KEY (chat_jid, message_id, reader_jid, type));
		CREATE TABLE IF NOT EXISTS poll_votes (
			chat_jid TEXT, poll_id TEXT, voter_jid TEXT, options TEXT, timestamp TIMESTAMP,
			PRIMARY KEY (chat_jid, poll_id, voter_jid));
		CREATE TABLE IF NOT EXISTS calls (
			call_id TEXT PRIMARY KEY, from_jid TEXT, creator_jid TEXT, group_jid TEXT,
			is_video INTEGER, status TEXT, reason TEXT, started_at TIMESTAMP, ended_at TIMESTAMP);
		CREATE TABLE IF NOT EXISTS group_events (
			group_jid TEXT, timestamp TIMESTAMP, actor_jid TEXT, kind TEXT, targets TEXT, detail TEXT);
		CREATE INDEX IF NOT EXISTS idx_group_events ON group_events(group_jid, timestamp);
	`)
	return err
}

// RichMessage is everything we keep about one stored message.
type RichMessage struct {
	Type, Content                       string
	MediaType, Filename, URL, Mimetype  string
	MediaKey, FileSHA256, FileEncSHA256 []byte
	FileLength                          uint64
	QuotedID, QuotedSender, QuotedText  string
	Mentions                            []string
	IsForwarded                         bool
	ForwardingScore                     uint32
	Extra                               map[string]any
}

type mediaFields interface {
	GetURL() string
	GetDirectPath() string
	GetMediaKey() []byte
	GetFileSHA256() []byte
	GetFileEncSHA256() []byte
	GetFileLength() uint64
	GetMimetype() string
}

func (r *RichMessage) setMedia(kind, name string, m mediaFields) {
	r.MediaType = kind
	r.Filename = name
	r.URL = firstNonEmpty(m.GetURL(), m.GetDirectPath())
	r.MediaKey, r.FileSHA256, r.FileEncSHA256 = m.GetMediaKey(), m.GetFileSHA256(), m.GetFileEncSHA256()
	r.FileLength = m.GetFileLength()
	r.Mimetype = m.GetMimetype()
}

func (r *RichMessage) extra(k string, v any) {
	if r.Extra == nil {
		r.Extra = map[string]any{}
	}
	r.Extra[k] = v
}

func safeName(s string) string {
	s = strings.Map(func(c rune) rune {
		if strings.ContainsRune(`/\:*?"<>|`, c) || c < 32 {
			return '_'
		}
		return c
	}, s)
	if s == "." || s == ".." {
		return "_"
	}
	return s
}

func extFromMime(mt, fallback string) string {
	mt = strings.SplitN(mt, ";", 2)[0]
	switch mt {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/webp":
		return ".webp"
	case "image/gif":
		return ".gif"
	case "video/mp4":
		return ".mp4"
	case "audio/ogg":
		return ".ogg"
	case "audio/mpeg":
		return ".mp3"
	case "audio/mp4":
		return ".m4a"
	}
	return fallback
}

func pollOf(m *waE2E.Message) *waE2E.PollCreationMessage {
	for _, p := range []*waE2E.PollCreationMessage{m.GetPollCreationMessage(), m.GetPollCreationMessageV2(),
		m.GetPollCreationMessageV3(), m.GetPollCreationMessageV5(), m.GetPollCreationMessageV6()} {
		if p != nil {
			return p
		}
	}
	return nil
}

// contextInfoOf finds the ContextInfo of whichever message type is set.
func contextInfoOf(m *waE2E.Message) *waE2E.ContextInfo {
	var ci *waE2E.ContextInfo
	m.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Kind() != protoreflect.MessageKind || fd.IsList() || fd.IsMap() {
			return true
		}
		sub := v.Message()
		if cf := sub.Descriptor().Fields().ByName("contextInfo"); cf != nil && sub.Has(cf) {
			ci, _ = sub.Get(cf).Message().Interface().(*waE2E.ContextInfo)
			return ci == nil
		}
		return true
	})
	return ci
}

// setContextInfo attaches ci to the message; plain text becomes an ExtendedTextMessage.
func setContextInfo(m *waE2E.Message, ci *waE2E.ContextInfo) bool {
	if m.Conversation != nil {
		m.ExtendedTextMessage = &waE2E.ExtendedTextMessage{Text: m.Conversation}
		m.Conversation = nil
	}
	done := false
	m.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Kind() != protoreflect.MessageKind || fd.IsList() || fd.IsMap() {
			return true
		}
		sub := v.Message()
		if cf := sub.Descriptor().Fields().ByName("contextInfo"); cf != nil {
			sub.Set(cf, protoreflect.ValueOfMessage(ci.ProtoReflect()))
			done = true
			return false
		}
		return true
	})
	return done
}

// firstSetField names the message type for anything we don't parse specially.
func firstSetField(m *waE2E.Message) string {
	name := ""
	m.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if n := string(fd.Name()); n != "messageContextInfo" {
			name = n
			return false
		}
		return true
	})
	return name
}

// ParseRich turns a decrypted message into storable fields. ok=false means nothing to store.
func ParseRich(m *waE2E.Message, id string) (r RichMessage, ok bool) {
	if m == nil {
		return r, false
	}
	switch {
	case m.Conversation != nil:
		r.Type, r.Content = "text", m.GetConversation()
	case m.ExtendedTextMessage != nil:
		e := m.ExtendedTextMessage
		r.Type, r.Content = "text", e.GetText()
		if e.GetMatchedText() != "" || e.GetTitle() != "" {
			r.extra("link_preview", map[string]any{"url": e.GetMatchedText(), "title": e.GetTitle(), "description": e.GetDescription()})
		}
	case m.ImageMessage != nil:
		i := m.ImageMessage
		r.Type, r.Content = "image", i.GetCaption()
		r.setMedia("image", "image_"+id+extFromMime(i.GetMimetype(), ".jpg"), i)
		r.extra("width", i.GetWidth())
		r.extra("height", i.GetHeight())
	case m.VideoMessage != nil || m.PtvMessage != nil:
		v := m.VideoMessage
		r.Type = "video"
		if v == nil {
			v, r.Type = m.PtvMessage, "video_note"
		}
		r.Content = v.GetCaption()
		r.setMedia("video", "video_"+id+".mp4", v)
		r.extra("seconds", v.GetSeconds())
		if v.GetGifPlayback() {
			r.Type = "gif"
		}
	case m.AudioMessage != nil:
		a := m.AudioMessage
		r.Type = "audio"
		if a.GetPTT() {
			r.Type = "voice"
		}
		r.setMedia("audio", "audio_"+id+extFromMime(a.GetMimetype(), ".ogg"), a)
		r.extra("seconds", a.GetSeconds())
	case m.DocumentMessage != nil:
		d := m.DocumentMessage
		r.Type, r.Content = "document", d.GetCaption()
		name := safeName(d.GetFileName())
		if name == "" || name == "_" {
			name = "document_" + id + extFromMime(d.GetMimetype(), "")
		}
		r.setMedia("document", name, d)
		r.extra("file_name", d.GetFileName())
		if d.GetPageCount() > 0 {
			r.extra("pages", d.GetPageCount())
		}
	case m.StickerMessage != nil:
		s := m.StickerMessage
		r.Type = "sticker"
		r.setMedia("sticker", "sticker_"+id+".webp", s)
		r.extra("animated", s.GetIsAnimated())
	case m.LocationMessage != nil:
		l := m.LocationMessage
		r.Type, r.Content = "location", strings.TrimSpace(l.GetName()+" "+l.GetAddress())
		r.extra("latitude", l.GetDegreesLatitude())
		r.extra("longitude", l.GetDegreesLongitude())
		r.extra("maps_url", fmt.Sprintf("https://maps.google.com/?q=%f,%f", l.GetDegreesLatitude(), l.GetDegreesLongitude()))
	case m.LiveLocationMessage != nil:
		l := m.LiveLocationMessage
		r.Type, r.Content = "live_location", l.GetCaption()
		r.extra("latitude", l.GetDegreesLatitude())
		r.extra("longitude", l.GetDegreesLongitude())
	case m.ContactMessage != nil:
		r.Type, r.Content = "contact", m.ContactMessage.GetDisplayName()
		r.extra("vcards", []string{m.ContactMessage.GetVcard()})
	case m.ContactsArrayMessage != nil:
		var names, cards []string
		for _, c := range m.ContactsArrayMessage.GetContacts() {
			names, cards = append(names, c.GetDisplayName()), append(cards, c.GetVcard())
		}
		r.Type, r.Content = "contacts", strings.Join(names, ", ")
		r.extra("vcards", cards)
	case pollOf(m) != nil:
		p := pollOf(m)
		r.Type, r.Content = "poll", p.GetName()
		var opts []string
		for _, o := range p.GetOptions() {
			opts = append(opts, o.GetOptionName())
		}
		r.extra("options", opts)
		r.extra("selectable_count", p.GetSelectableOptionsCount())
	case m.EventMessage != nil:
		e := m.EventMessage
		r.Type, r.Content = "event", strings.TrimSpace(e.GetName()+"\n"+e.GetDescription())
		r.extra("start_time", e.GetStartTime())
		r.extra("end_time", e.GetEndTime())
		if loc := e.GetLocation(); loc != nil {
			r.extra("location", loc.GetName())
		}
		r.extra("is_canceled", e.GetIsCanceled())
	case m.GroupInviteMessage != nil:
		g := m.GroupInviteMessage
		r.Type, r.Content = "group_invite", strings.TrimSpace(g.GetGroupName()+"\n"+g.GetCaption())
		r.extra("group_jid", g.GetGroupJID())
		r.extra("invite_code", g.GetInviteCode())
		r.extra("expiration", g.GetInviteExpiration())
	case m.ButtonsResponseMessage != nil:
		r.Type, r.Content = "button_reply", m.ButtonsResponseMessage.GetSelectedDisplayText()
	case m.TemplateButtonReplyMessage != nil:
		r.Type, r.Content = "button_reply", m.TemplateButtonReplyMessage.GetSelectedDisplayText()
	case m.ListResponseMessage != nil:
		r.Type, r.Content = "list_reply", m.ListResponseMessage.GetTitle()
	case m.ButtonsMessage != nil:
		r.Type, r.Content = "buttons", m.ButtonsMessage.GetContentText()
	case m.ListMessage != nil:
		r.Type, r.Content = "list", strings.TrimSpace(m.ListMessage.GetTitle()+"\n"+m.ListMessage.GetDescription())
	case m.InteractiveMessage != nil:
		r.Type, r.Content = "interactive", m.InteractiveMessage.GetBody().GetText()
	case m.CallLogMesssage != nil:
		c := m.CallLogMesssage
		r.Type = "call_log"
		r.Content = fmt.Sprintf("%s call, %ds, %s", map[bool]string{true: "video", false: "voice"}[c.GetIsVideo()], c.GetDurationSecs(), c.GetCallOutcome().String())
	case m.ProtocolMessage != nil, m.ReactionMessage != nil, m.EncReactionMessage != nil,
		m.PollUpdateMessage != nil, m.SenderKeyDistributionMessage != nil, m.KeepInChatMessage != nil,
		m.PinInChatMessage != nil:
		return r, false
	default:
		name := firstSetField(m)
		if name == "" {
			return r, false
		}
		r.Type, r.Content = "other:"+name, ""
	}
	if ci := contextInfoOf(m); ci != nil {
		r.QuotedID, r.QuotedSender = ci.GetStanzaID(), ci.GetParticipant()
		if q := ci.GetQuotedMessage(); q != nil {
			if qr, ok := ParseRich(q, ci.GetStanzaID()); ok {
				r.QuotedText = qr.Content
				if r.QuotedText == "" {
					r.QuotedText = "[" + qr.Type + "]"
				}
			}
		}
		r.Mentions = ci.GetMentionedJID()
		r.IsForwarded, r.ForwardingScore = ci.GetIsForwarded(), ci.GetForwardingScore()
	}
	return r, true
}

// Rich storage of events.Message ------------------------------------------------

type RichStore struct {
	client *whatsmeow.Client
	ms     *MessageStore
	log    waLog.Logger
	mu     sync.Mutex
}

func ts(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func jsonOrNil(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case []string:
		if len(x) == 0 {
			return nil
		}
	case map[string]any:
		if len(x) == 0 {
			return nil
		}
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// phoneFor resolves a LID to a phone-number JID when WhatsApp told us the mapping.
func (rs *RichStore) phoneFor(j, alt types.JID) types.JID {
	j = j.ToNonAD()
	if j.Server == types.DefaultUserServer {
		return j
	}
	if alt.Server == types.DefaultUserServer {
		return alt.ToNonAD()
	}
	if j.Server == types.HiddenUserServer {
		if pn, err := rs.client.Store.LIDs.GetPNForLID(context.Background(), j); err == nil && !pn.IsEmpty() {
			return pn.ToNonAD()
		}
	}
	return types.EmptyJID
}

func (rs *RichStore) ensureChat(chat types.JID, t time.Time, sender string) {
	chatJID := chat.String()
	if chat.Server != types.GroupServer {
		sender = chat.User // a DM is named after the other person, never after our own number
	}
	name := GetChatName(rs.client, rs.ms, chat, chatJID, nil, sender, rs.log)
	var last sql.NullTime
	rs.ms.db.QueryRow("SELECT last_message_time FROM chats WHERE jid = ?", chatJID).Scan(&last)
	if last.Valid && last.Time.After(t) {
		t = last.Time
	}
	rs.ms.db.Exec(`INSERT INTO chats (jid, name, last_message_time, is_group) VALUES (?, ?, ?, ?)
		ON CONFLICT(jid) DO UPDATE SET name = COALESCE(NULLIF(chats.name, ''), excluded.name),
		last_message_time = excluded.last_message_time, is_group = excluded.is_group`,
		chatJID, name, t, chat.Server == types.GroupServer)
}

// Handle stores any message event; history=true for messages from history sync.
func (rs *RichStore) Handle(evt *events.Message, history bool) {
	if evt == nil || evt.Message == nil {
		return
	}
	info := evt.Info
	chatJID := info.Chat.String()
	senderJID := info.Sender.ToNonAD()
	if info.IsFromMe && rs.client.Store.ID != nil {
		senderJID = rs.client.Store.ID.ToNonAD()
	}
	m := evt.Message
	if inner := m.GetEditedMessage().GetMessage(); inner != nil {
		m = inner // our own sent edits arrive wrapped
	}

	// Side effects that modify other messages.
	if r := m.GetReactionMessage(); r != nil {
		rs.storeReaction(chatJID, r.GetKey().GetID(), senderJID.String(), r.GetText(), info.Timestamp)
		return
	}
	if m.GetEncReactionMessage() != nil && !history {
		if r, err := rs.client.DecryptReaction(context.Background(), evt); err == nil {
			rs.storeReaction(chatJID, r.GetKey().GetID(), senderJID.String(), r.GetText(), info.Timestamp)
		}
		return
	}
	if pu := m.GetPollUpdateMessage(); pu != nil {
		if vote, err := rs.client.DecryptPollVote(context.Background(), evt); err == nil {
			rs.storePollVote(chatJID, pu.GetPollCreationMessageKey().GetID(), senderJID.String(), vote.GetSelectedOptions(), info.Timestamp)
		} else {
			rs.log.Warnf("poll vote decrypt failed: %v", err)
		}
		return
	}
	if pm := m.GetProtocolMessage(); pm != nil {
		target := pm.GetKey().GetID()
		switch pm.GetType() {
		case waE2E.ProtocolMessage_REVOKE:
			rs.ms.db.Exec(`UPDATE messages SET is_revoked = 1, revoked_by = ?, revoked_at = ? WHERE id = ? AND chat_jid = ?`,
				senderJID.String(), info.Timestamp, target, chatJID)
		case waE2E.ProtocolMessage_MESSAGE_EDIT:
			if nr, ok := ParseRich(pm.GetEditedMessage(), target); ok {
				rs.ms.db.Exec(`UPDATE messages SET original_content = COALESCE(original_content, content),
					content = ?, edited_at = ? WHERE id = ? AND chat_jid = ?`, nr.Content, info.Timestamp, target, chatJID)
			}
		}
		return
	}

	r, ok := ParseRich(m, info.ID)
	if !ok {
		return
	}
	rs.ensureChat(info.Chat, info.Timestamp, senderJID.User)

	pn := rs.phoneFor(senderJID, info.SenderAlt)
	legacySender := senderJID.User
	if !pn.IsEmpty() {
		legacySender = pn.User
	}
	var raw []byte
	if r.MediaType != "" || r.Type == "poll" || !history {
		raw, _ = proto.Marshal(m)
	}
	pushName := info.PushName
	if info.IsFromMe {
		pushName = ""
	}
	_, err := rs.ms.db.Exec(`INSERT INTO messages
		(id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256,
		 file_enc_sha256, file_length, sender_jid, sender_pn, push_name, msg_type, mimetype, quoted_id, quoted_sender,
		 quoted_text, mentions, is_forwarded, forwarding_score, is_ephemeral, is_view_once, extra, raw, server_id)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(id, chat_jid) DO UPDATE SET
		 sender = excluded.sender, content = CASE WHEN messages.edited_at IS NULL THEN excluded.content ELSE messages.content END,
		 media_type = excluded.media_type, filename = COALESCE(messages.filename, excluded.filename), url = excluded.url,
		 media_key = excluded.media_key, file_sha256 = excluded.file_sha256, file_enc_sha256 = excluded.file_enc_sha256,
		 file_length = excluded.file_length, sender_jid = excluded.sender_jid,
		 sender_pn = COALESCE(excluded.sender_pn, messages.sender_pn), push_name = COALESCE(excluded.push_name, messages.push_name),
		 msg_type = excluded.msg_type, mimetype = excluded.mimetype, quoted_id = excluded.quoted_id,
		 quoted_sender = excluded.quoted_sender, quoted_text = excluded.quoted_text, mentions = excluded.mentions,
		 is_forwarded = excluded.is_forwarded, forwarding_score = excluded.forwarding_score,
		 is_ephemeral = excluded.is_ephemeral, is_view_once = excluded.is_view_once, extra = excluded.extra,
		 raw = COALESCE(excluded.raw, messages.raw), server_id = COALESCE(excluded.server_id, messages.server_id)`,
		info.ID, chatJID, legacySender, r.Content, info.Timestamp, info.IsFromMe, nullStr(r.MediaType), nullStr(r.Filename),
		nullStr(r.URL), r.MediaKey, r.FileSHA256, r.FileEncSHA256, r.FileLength, senderJID.String(), nullStr(pn.User),
		nullStr(pushName), r.Type, nullStr(r.Mimetype), nullStr(r.QuotedID), nullStr(r.QuotedSender), nullStr(r.QuotedText),
		jsonOrNil(r.Mentions), r.IsForwarded, r.ForwardingScore, evt.IsEphemeral, evt.IsViewOnce, jsonOrNil(r.Extra), raw,
		func() any {
			if info.ServerID == 0 {
				return nil
			}
			return int64(info.ServerID)
		}())
	if err != nil {
		rs.log.Warnf("store message %s: %v", info.ID, err)
		return
	}
	if !history {
		dir := "←"
		if info.IsFromMe {
			dir = "→"
		}
		fmt.Printf("[%s] %s %s: [%s] %s\n", info.Timestamp.Format("2006-01-02 15:04:05"), dir, legacySender, r.Type, r.Content)
	}
}

func (rs *RichStore) storeReaction(chat, msgID, sender, emoji string, t time.Time) {
	if emoji == "" {
		rs.ms.db.Exec("DELETE FROM reactions WHERE chat_jid = ? AND message_id = ? AND sender_jid = ?", chat, msgID, sender)
		return
	}
	rs.ms.db.Exec(`INSERT OR REPLACE INTO reactions (chat_jid, message_id, sender_jid, emoji, timestamp) VALUES (?,?,?,?,?)`,
		chat, msgID, sender, emoji, t)
}

// storePollVote maps option hashes back to option names using the stored poll.
func (rs *RichStore) storePollVote(chat, pollID, voter string, hashes [][]byte, t time.Time) {
	var extra sql.NullString
	rs.ms.db.QueryRow("SELECT extra FROM messages WHERE id = ? AND chat_jid = ?", pollID, chat).Scan(&extra)
	var poll struct {
		Options []string `json:"options"`
	}
	json.Unmarshal([]byte(extra.String), &poll)
	byHash := map[string]string{}
	for _, o := range poll.Options {
		h := sha256.Sum256([]byte(o))
		byHash[string(h[:])] = o
	}
	chosen := []string{}
	for _, h := range hashes {
		if name, ok := byHash[string(h)]; ok {
			chosen = append(chosen, name)
		} else {
			chosen = append(chosen, fmt.Sprintf("hash:%x", h))
		}
	}
	b, _ := json.Marshal(chosen)
	rs.ms.db.Exec(`INSERT OR REPLACE INTO poll_votes (chat_jid, poll_id, voter_jid, options, timestamp) VALUES (?,?,?,?,?)`,
		chat, pollID, voter, string(b), t)
}

// HandleHistory replaces the old history handler: same parser as live messages,
// plus the reactions, receipts, poll votes and stars that history sync carries.
func (rs *RichStore) HandleHistory(hs *events.HistorySync) {
	stored := 0
	for _, conv := range hs.Data.GetConversations() {
		jid, err := types.ParseJID(conv.GetID())
		if err != nil {
			continue
		}
		chatJID := jid.String()
		name := GetChatName(rs.client, rs.ms, jid, chatJID, conv, "", rs.log)
		var t time.Time
		if cts := conv.GetConversationTimestamp(); cts > 0 {
			t = time.Unix(int64(cts), 0)
		}
		for _, hm := range conv.GetMessages() {
			if wm := hm.GetMessage(); wm != nil && wm.GetMessageTimestamp() > 0 {
				if mt := time.Unix(int64(wm.GetMessageTimestamp()), 0); mt.After(t) {
					t = mt
				}
			}
		}
		if !t.IsZero() {
			rs.ms.db.Exec(`INSERT INTO chats (jid, name, last_message_time, is_group, unread_count) VALUES (?,?,?,?,?)
				ON CONFLICT(jid) DO UPDATE SET name = COALESCE(NULLIF(chats.name, ''), excluded.name),
				last_message_time = MAX(COALESCE(chats.last_message_time, excluded.last_message_time), excluded.last_message_time),
				is_group = excluded.is_group, unread_count = excluded.unread_count`,
				chatJID, name, t, jid.Server == types.GroupServer, conv.GetUnreadCount())
		}
		for _, hm := range conv.GetMessages() {
			if rs.storeWeb(jid, hm.GetMessage()) {
				stored++
			}
		}
	}
	fmt.Printf("History sync (%s): stored %d messages from %d conversations\n",
		hs.Data.GetSyncType().String(), stored, len(hs.Data.GetConversations()))
}

func (rs *RichStore) storeWeb(chat types.JID, wm *waWeb.WebMessageInfo) bool {
	if wm == nil || wm.GetKey() == nil {
		return false
	}
	chatJID := chat.String()
	id := wm.GetKey().GetID()
	if st := wm.GetMessageStubType(); st != 0 && wm.GetMessage() == nil {
		// System messages: "X added Y", "group created", "number changed", missed calls...
		t := time.Unix(int64(wm.GetMessageTimestamp()), 0)
		content := strings.TrimSpace(st.String() + " " + strings.Join(wm.GetMessageStubParameters(), ", "))
		sender := wm.GetParticipant()
		if sender == "" {
			sender = wm.GetKey().GetParticipant()
		}
		_, err := rs.ms.db.Exec(`INSERT OR IGNORE INTO messages (id, chat_jid, sender, content, timestamp, is_from_me, msg_type, sender_jid)
			VALUES (?,?,?,?,?,?,?,?)`, id, chatJID, strings.SplitN(sender, "@", 2)[0], content, t, wm.GetKey().GetFromMe(), "system", nullStr(sender))
		return err == nil
	}
	evt, err := rs.client.ParseWebMessage(chat, wm)
	if err != nil {
		return false
	}
	rs.Handle(evt, true)
	if wm.GetStarred() {
		rs.ms.db.Exec("UPDATE messages SET starred = 1 WHERE id = ? AND chat_jid = ?", id, chatJID)
	}
	own := ""
	if rs.client.Store.ID != nil {
		own = rs.client.Store.ID.ToNonAD().String()
	}
	for _, re := range wm.GetReactions() {
		sender := re.GetKey().GetParticipant()
		if re.GetKey().GetFromMe() {
			sender = own
		} else if sender == "" {
			sender = re.GetKey().GetRemoteJID()
		}
		rs.storeReaction(chatJID, id, sender, re.GetText(), time.UnixMilli(re.GetSenderTimestampMS()))
	}
	for _, ur := range wm.GetUserReceipt() {
		for typ, sec := range map[string]int64{"delivered": ur.GetReceiptTimestamp(), "read": ur.GetReadTimestamp(), "played": ur.GetPlayedTimestamp()} {
			if sec > 0 {
				rs.ms.db.Exec(`INSERT OR REPLACE INTO receipts (chat_jid, message_id, reader_jid, type, timestamp) VALUES (?,?,?,?,?)`,
					chatJID, id, ur.GetUserJID(), typ, time.Unix(sec, 0))
			}
		}
	}
	for _, pu := range wm.GetPollUpdates() {
		voter := pu.GetPollUpdateMessageKey().GetParticipant()
		if pu.GetPollUpdateMessageKey().GetFromMe() {
			voter = own
		} else if voter == "" {
			voter = pu.GetPollUpdateMessageKey().GetRemoteJID()
		}
		rs.storePollVote(chatJID, id, voter, pu.GetVote().GetSelectedOptions(), time.UnixMilli(pu.GetSenderTimestampMS()))
	}
	return true
}

// Other events --------------------------------------------------------------

type presenceInfo struct {
	Online   bool      `json:"online"`
	LastSeen time.Time `json:"last_seen,omitempty"`
	Typing   string    `json:"typing,omitempty"`
	TypingIn string    `json:"typing_in,omitempty"`
	Updated  time.Time `json:"updated"`
}

var (
	presenceMu sync.Mutex
	presences  = map[string]*presenceInfo{}
)

func presenceFor(jid string) *presenceInfo {
	p := presences[jid]
	if p == nil {
		p = &presenceInfo{}
		presences[jid] = p
	}
	return p
}

func (rs *RichStore) HandleEvent(evt any) {
	db := rs.ms.db
	switch v := evt.(type) {
	case *events.Receipt:
		typ := string(v.Type)
		if typ == "" {
			typ = "delivered"
		}
		if typ == "retry" {
			return
		}
		for _, id := range v.MessageIDs {
			db.Exec(`INSERT OR REPLACE INTO receipts (chat_jid, message_id, reader_jid, type, timestamp) VALUES (?,?,?,?,?)`,
				v.Chat.String(), id, v.Sender.ToNonAD().String(), typ, v.Timestamp)
		}
	case *events.CallOffer:
		video := false
		if v.Data != nil {
			_, video = v.Data.GetOptionalChildByTag("video")
		}
		db.Exec(`INSERT OR IGNORE INTO calls (call_id, from_jid, creator_jid, group_jid, is_video, status, started_at) VALUES (?,?,?,?,?,?,?)`,
			v.CallID, v.From.ToNonAD().String(), v.CallCreator.ToNonAD().String(), nullStr(v.GroupJID.String()), video, "ringing", v.Timestamp)
		fmt.Printf("[call] incoming %s call from %s (id %s)\n", map[bool]string{true: "video", false: "voice"}[video], v.From, v.CallID)
	case *events.CallAccept:
		db.Exec("UPDATE calls SET status = 'accepted' WHERE call_id = ?", v.CallID)
	case *events.CallReject:
		db.Exec("UPDATE calls SET status = 'rejected', ended_at = ? WHERE call_id = ?", v.Timestamp, v.CallID)
	case *events.CallTerminate:
		db.Exec(`UPDATE calls SET status = CASE WHEN status = 'ringing' THEN 'missed' ELSE status END,
			reason = ?, ended_at = ? WHERE call_id = ?`, v.Reason, v.Timestamp, v.CallID)
	case *events.Star:
		db.Exec("UPDATE messages SET starred = ? WHERE id = ? AND chat_jid = ?", v.Action.GetStarred(), v.MessageID, v.ChatJID.String())
	case *events.DeleteForMe:
		db.Exec("UPDATE messages SET extra = json_set(COALESCE(extra, '{}'), '$.deleted_for_me', 1) WHERE id = ? AND chat_jid = ?",
			v.MessageID, v.ChatJID.String())
	case *events.JoinedGroup:
		rs.ms.db.Exec(`INSERT INTO chats (jid, name, last_message_time, is_group) VALUES (?,?,?,1)
			ON CONFLICT(jid) DO UPDATE SET name = excluded.name`, v.JID.String(), v.Name, time.Now())
		rs.groupEvent(v.JID, time.Now(), v.Sender, "joined_group", nil, v.Reason)
	case *events.GroupInfo:
		if v.Name != nil {
			db.Exec("UPDATE chats SET name = ? WHERE jid = ?", v.Name.Name, v.JID.String())
			rs.groupEvent(v.JID, v.Timestamp, v.Sender, "name", nil, v.Name.Name)
		}
		if v.Topic != nil {
			rs.groupEvent(v.JID, v.Timestamp, v.Sender, "description", nil, v.Topic.Topic)
		}
		if v.Announce != nil {
			rs.groupEvent(v.JID, v.Timestamp, v.Sender, "announce", nil, fmt.Sprint(v.Announce.IsAnnounce))
		}
		if v.Locked != nil {
			rs.groupEvent(v.JID, v.Timestamp, v.Sender, "locked", nil, fmt.Sprint(v.Locked.IsLocked))
		}
		if v.Ephemeral != nil {
			rs.groupEvent(v.JID, v.Timestamp, v.Sender, "disappearing", nil, fmt.Sprint(v.Ephemeral.DisappearingTimer))
		}
		if v.NewInviteLink != nil {
			rs.groupEvent(v.JID, v.Timestamp, v.Sender, "invite_link_reset", nil, "")
		}
		for kind, list := range map[string][]types.JID{"join": v.Join, "leave": v.Leave, "promote": v.Promote, "demote": v.Demote} {
			if len(list) > 0 {
				rs.groupEvent(v.JID, v.Timestamp, v.Sender, kind, list, v.JoinReason)
			}
		}
		if v.Link != nil {
			rs.groupEvent(v.JID, v.Timestamp, v.Sender, "community_link", []types.JID{v.Link.Group.JID}, v.Link.Group.Name)
		}
		if v.Unlink != nil {
			rs.groupEvent(v.JID, v.Timestamp, v.Sender, "community_unlink", []types.JID{v.Unlink.Group.JID}, v.Unlink.Group.Name)
		}
		if v.Delete != nil {
			rs.groupEvent(v.JID, v.Timestamp, v.Sender, "deleted", nil, v.Delete.DeleteReason)
		}
	case *events.Presence:
		presenceMu.Lock()
		p := presenceFor(v.From.ToNonAD().String())
		p.Online, p.LastSeen, p.Updated = !v.Unavailable, v.LastSeen, time.Now()
		presenceMu.Unlock()
	case *events.ChatPresence:
		presenceMu.Lock()
		p := presenceFor(v.Sender.ToNonAD().String())
		p.Typing, p.TypingIn, p.Updated = "", "", time.Now()
		if v.State == types.ChatPresenceComposing {
			p.Typing, p.TypingIn = "typing", v.Chat.String()
			if v.Media == types.ChatPresenceMediaAudio {
				p.Typing = "recording"
			}
		}
		presenceMu.Unlock()
	}
}

func (rs *RichStore) groupEvent(group types.JID, t time.Time, actor *types.JID, kind string, targets []types.JID, detail string) {
	a := ""
	if actor != nil {
		a = actor.ToNonAD().String()
	}
	var tj any
	if len(targets) > 0 {
		s := make([]string, len(targets))
		for i, j := range targets {
			s[i] = j.ToNonAD().String()
		}
		tj = jsonOrNil(s)
	}
	rs.ms.db.Exec("INSERT INTO group_events (group_jid, timestamp, actor_jid, kind, targets, detail) VALUES (?,?,?,?,?,?)",
		group.String(), t, nullStr(a), kind, tj, nullStr(detail))
}

// StoreSent records a message this device just sent (whatsmeow emits no event for it).
func (rs *RichStore) StoreSent(to types.JID, msg *waE2E.Message, resp whatsmeow.SendResponse) {
	if rs.client.Store.ID == nil {
		return
	}
	evt := &events.Message{Message: msg, RawMessage: msg}
	evt.Info.Chat = to
	evt.Info.Sender = rs.client.Store.ID.ToNonAD()
	evt.Info.IsFromMe = true
	evt.Info.IsGroup = to.Server == types.GroupServer
	evt.Info.ID = resp.ID
	evt.Info.Timestamp = resp.Timestamp
	evt.Info.ServerID = resp.ServerID
	if evt.Info.Timestamp.IsZero() {
		evt.Info.Timestamp = time.Now()
	}
	rs.Handle(evt, false)
}
