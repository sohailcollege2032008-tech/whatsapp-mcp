package main

// Full REST API: messaging actions, groups, communities, contacts, profile,
// privacy, status, channels, calls. All routes are POST + JSON on loopback.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

type API struct {
	cli *whatsmeow.Client
	ms  *MessageStore
	rs  *RichStore
}

type apiResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Result  any    `json:"result,omitempty"`
	Path    string `json:"path,omitempty"` // old stdio clients read the download path here
}

// handle registers a POST route that decodes T and encodes fn's result.
func handle[T any](path string, fn func(ctx context.Context, req T) (any, error)) {
	http.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			json.NewEncoder(w).Encode(apiResponse{Message: "POST only"})
			return
		}
		var req T
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(apiResponse{Message: "invalid JSON: " + err.Error()})
				return
			}
		}
		ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
		defer cancel()
		res, err := fn(ctx, req)
		if err != nil {
			fmt.Printf("API %s error: %v\n", path, err)
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(apiResponse{Message: err.Error()})
			return
		}
		msg := "ok"
		if s, ok := res.(string); ok {
			msg, res = s, nil
		}
		out := apiResponse{Success: true, Message: msg, Result: res}
		if m, ok := res.(map[string]any); ok {
			out.Path, _ = m["path"].(string)
		}
		json.NewEncoder(w).Encode(out)
	})
}

var nonDigits = regexp.MustCompile(`\D`)

// parseJID accepts a full JID or a phone number in any format.
func parseJID(s string) (types.JID, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return types.EmptyJID, errors.New("empty JID/phone")
	}
	if strings.Contains(s, "@") {
		return types.ParseJID(s)
	}
	d := nonDigits.ReplaceAllString(s, "")
	if len(d) < 7 {
		return types.EmptyJID, fmt.Errorf("invalid phone number %q", s)
	}
	return types.NewJID(d, types.DefaultUserServer), nil
}

func parseJIDs(list []string) ([]types.JID, error) {
	out := make([]types.JID, 0, len(list))
	for _, s := range list {
		j, err := parseJID(s)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, nil
}

func (a *API) own() types.JID {
	if a.cli.Store.ID == nil {
		return types.EmptyJID
	}
	return a.cli.Store.ID.ToNonAD()
}

func (a *API) connected() error {
	if !a.cli.IsConnected() {
		return errors.New("not connected to WhatsApp")
	}
	return nil
}

// Stored message lookup ---------------------------------------------------------

type storedMsg struct {
	ID        string
	Chat      types.JID
	Sender    types.JID
	FromMe    bool
	Timestamp time.Time
	Content   string
	Raw       []byte
	Extra     string
}

func (a *API) lookup(chat, id string) (*storedMsg, error) {
	cj, err := parseJID(chat)
	if err != nil {
		return nil, err
	}
	var s storedMsg
	var senderJID, sender, content, extra sql.NullString
	err = a.ms.db.QueryRow(`SELECT id, sender_jid, sender, is_from_me, timestamp, content, raw, extra
		FROM messages WHERE id = ? AND chat_jid = ?`, id, cj.String()).
		Scan(&s.ID, &senderJID, &sender, &s.FromMe, &s.Timestamp, &content, &s.Raw, &extra)
	if err != nil {
		return nil, fmt.Errorf("message %s not found in chat %s", id, cj)
	}
	s.Chat, s.Content, s.Extra = cj, content.String, extra.String
	switch {
	case s.FromMe:
		s.Sender = a.own()
	case senderJID.String != "":
		s.Sender, _ = types.ParseJID(senderJID.String)
	case strings.Contains(sender.String, "@"):
		s.Sender, _ = types.ParseJID(sender.String)
	case cj.Server != types.GroupServer:
		s.Sender = cj
	case len(sender.String) >= 15:
		s.Sender = types.NewJID(sender.String, types.HiddenUserServer)
	default:
		s.Sender = types.NewJID(sender.String, types.DefaultUserServer)
	}
	return &s, nil
}

func (s *storedMsg) message() *waE2E.Message {
	if len(s.Raw) > 0 {
		m := &waE2E.Message{}
		if proto.Unmarshal(s.Raw, m) == nil {
			return m
		}
	}
	return &waE2E.Message{Conversation: proto.String(s.Content)}
}

func (s *storedMsg) info() *types.MessageInfo {
	return &types.MessageInfo{
		MessageSource: types.MessageSource{Chat: s.Chat, Sender: s.Sender, IsFromMe: s.FromMe, IsGroup: s.Chat.Server == types.GroupServer},
		ID:            s.ID, Timestamp: s.Timestamp,
	}
}

func (a *API) latest(chat types.JID) (*storedMsg, error) {
	var id string
	if err := a.ms.db.QueryRow("SELECT id FROM messages WHERE chat_jid = ? ORDER BY timestamp DESC LIMIT 1", chat.String()).Scan(&id); err != nil {
		return nil, errors.New("no stored messages in this chat")
	}
	return a.lookup(chat.String(), id)
}

// Sending ----------------------------------------------------------------------

func (a *API) send(ctx context.Context, to types.JID, msg *waE2E.Message) (map[string]any, error) {
	if err := a.connected(); err != nil {
		return nil, err
	}
	resp, err := a.cli.SendMessage(ctx, to, msg)
	if err != nil {
		return nil, err
	}
	a.rs.StoreSent(to, msg, resp)
	return map[string]any{"id": resp.ID, "chat_jid": to.String(), "timestamp": resp.Timestamp}, nil
}

type sendReq struct {
	Recipient  string   `json:"recipient"`
	Message    string   `json:"message"`
	MediaPath  string   `json:"media_path"`
	ReplyTo    string   `json:"reply_to"`
	Mentions   []string `json:"mentions"`
	ViewOnce   bool     `json:"view_once"`
	AsSticker  bool     `json:"as_sticker"`
	AsDocument bool     `json:"as_document"`
	FileName   string   `json:"file_name"`
}

func (a *API) buildMedia(ctx context.Context, r sendReq) (*waE2E.Message, error) {
	data, err := os.ReadFile(r.MediaPath)
	if err != nil {
		return nil, fmt.Errorf("read media: %w", err)
	}
	ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(r.MediaPath), "."))
	kind := "document"
	mimeType := ""
	switch ext {
	case "jpg", "jpeg":
		kind, mimeType = "image", "image/jpeg"
	case "png":
		kind, mimeType = "image", "image/png"
	case "gif":
		kind, mimeType = "image", "image/gif"
	case "webp":
		kind, mimeType = "image", "image/webp"
	case "ogg", "opus":
		kind, mimeType = "voice", "audio/ogg; codecs=opus"
	case "mp3":
		kind, mimeType = "audio", "audio/mpeg"
	case "m4a", "aac":
		kind, mimeType = "audio", "audio/mp4"
	case "mp4":
		kind, mimeType = "video", "video/mp4"
	case "mov":
		kind, mimeType = "video", "video/quicktime"
	}
	if r.AsSticker {
		if ext != "webp" {
			return nil, errors.New("stickers must be .webp (512x512)")
		}
		kind = "sticker"
	}
	if r.AsDocument {
		kind = "document"
	}
	mt := map[string]whatsmeow.MediaType{"image": whatsmeow.MediaImage, "sticker": whatsmeow.MediaImage,
		"video": whatsmeow.MediaVideo, "audio": whatsmeow.MediaAudio, "voice": whatsmeow.MediaAudio}[kind]
	if kind == "document" {
		mt = whatsmeow.MediaDocument
	}
	up, err := a.cli.Upload(ctx, data, mt)
	if err != nil {
		return nil, fmt.Errorf("upload: %w", err)
	}
	m := &waE2E.Message{}
	caption := proto.String(r.Message)
	switch kind {
	case "image":
		m.ImageMessage = &waE2E.ImageMessage{Caption: caption, Mimetype: proto.String(mimeType), URL: &up.URL, DirectPath: &up.DirectPath,
			MediaKey: up.MediaKey, FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256, FileLength: &up.FileLength}
		if r.ViewOnce {
			m.ImageMessage.ViewOnce = proto.Bool(true)
		}
	case "sticker":
		m.StickerMessage = &waE2E.StickerMessage{Mimetype: proto.String("image/webp"), URL: &up.URL, DirectPath: &up.DirectPath,
			MediaKey: up.MediaKey, FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256, FileLength: &up.FileLength}
	case "video":
		m.VideoMessage = &waE2E.VideoMessage{Caption: caption, Mimetype: proto.String(mimeType), URL: &up.URL, DirectPath: &up.DirectPath,
			MediaKey: up.MediaKey, FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256, FileLength: &up.FileLength}
		if r.ViewOnce {
			m.VideoMessage.ViewOnce = proto.Bool(true)
		}
	case "voice", "audio":
		am := &waE2E.AudioMessage{Mimetype: proto.String(mimeType), URL: &up.URL, DirectPath: &up.DirectPath,
			MediaKey: up.MediaKey, FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256, FileLength: &up.FileLength}
		if kind == "voice" {
			secs, wave, err := analyzeOggOpus(data)
			if err != nil {
				return nil, fmt.Errorf("voice notes need Ogg Opus: %w", err)
			}
			am.Seconds, am.Waveform, am.PTT = proto.Uint32(secs), wave, proto.Bool(true)
		}
		m.AudioMessage = am
	default:
		name := r.MediaPath
		if r.FileName != "" {
			name = r.FileName
		}
		m.DocumentMessage = buildDocumentMessage(name, r.Message, up)
	}
	return m, nil
}

func (a *API) withContext(ctx context.Context, chat types.JID, m *waE2E.Message, replyTo string, mentions []string) error {
	if replyTo == "" && len(mentions) == 0 {
		return nil
	}
	ci := &waE2E.ContextInfo{}
	if replyTo != "" {
		q, err := a.lookup(chat.String(), replyTo)
		if err != nil {
			return err
		}
		ci.StanzaID = proto.String(q.ID)
		ci.Participant = proto.String(q.Sender.String())
		qm := q.message()
		qm.MessageContextInfo = nil
		ci.QuotedMessage = qm
	}
	for _, s := range mentions {
		j, err := parseJID(s)
		if err != nil {
			return err
		}
		ci.MentionedJID = append(ci.MentionedJID, j.String())
	}
	if !setContextInfo(m, ci) {
		return errors.New("this message type can't carry a reply/mentions")
	}
	return nil
}

func (a *API) handleSend(ctx context.Context, r sendReq) (any, error) {
	to, err := parseJID(r.Recipient)
	if err != nil {
		return nil, err
	}
	var m *waE2E.Message
	if r.MediaPath != "" {
		if m, err = a.buildMedia(ctx, r); err != nil {
			return nil, err
		}
	} else if r.Message != "" {
		m = &waE2E.Message{Conversation: proto.String(r.Message)}
	} else {
		return nil, errors.New("message or media_path is required")
	}
	if err := a.withContext(ctx, to, m, r.ReplyTo, r.Mentions); err != nil {
		return nil, err
	}
	return a.send(ctx, to, m)
}

type msgRef struct {
	ChatJID   string `json:"chat_jid"`
	MessageID string `json:"message_id"`
}

// Routes -----------------------------------------------------------------------

func (a *API) Register() {
	cli := a.cli
	db := a.ms.db

	handle("/api/send", a.handleSend)

	handle("/api/status", func(ctx context.Context, _ struct{}) (any, error) {
		res := map[string]any{"connected": cli.IsConnected(), "logged_in": cli.IsLoggedIn(), "push_name": cli.Store.PushName,
			"platform": cli.Store.Platform, "business_name": cli.Store.BusinessName}
		if cli.Store.ID != nil {
			res["jid"], res["lid"] = cli.Store.ID.ToNonAD().String(), cli.Store.LID.ToNonAD().String()
		}
		var n, c int
		db.QueryRow("SELECT COUNT(*) FROM messages").Scan(&n)
		db.QueryRow("SELECT COUNT(*) FROM chats").Scan(&c)
		res["stored_messages"], res["stored_chats"] = n, c
		return res, nil
	})

	handle("/api/react", func(ctx context.Context, r struct {
		msgRef
		Emoji string `json:"emoji"`
	}) (any, error) {
		s, err := a.lookup(r.ChatJID, r.MessageID)
		if err != nil {
			return nil, err
		}
		return a.send(ctx, s.Chat, cli.BuildReaction(s.Chat, s.Sender, s.ID, r.Emoji))
	})

	handle("/api/edit", func(ctx context.Context, r struct {
		msgRef
		Text string `json:"text"`
	}) (any, error) {
		s, err := a.lookup(r.ChatJID, r.MessageID)
		if err != nil {
			return nil, err
		}
		if !s.FromMe {
			return nil, errors.New("only your own messages can be edited (within ~15 minutes)")
		}
		return a.send(ctx, s.Chat, cli.BuildEdit(s.Chat, s.ID, &waE2E.Message{Conversation: proto.String(r.Text)}))
	})

	handle("/api/revoke", func(ctx context.Context, r msgRef) (any, error) {
		s, err := a.lookup(r.ChatJID, r.MessageID)
		if err != nil {
			return nil, err
		}
		sender := s.Sender
		if s.FromMe {
			sender = types.EmptyJID
		}
		return a.send(ctx, s.Chat, cli.BuildRevoke(s.Chat, sender, s.ID))
	})

	handle("/api/forward", func(ctx context.Context, r struct {
		msgRef
		To []string `json:"to"`
	}) (any, error) {
		s, err := a.lookup(r.ChatJID, r.MessageID)
		if err != nil {
			return nil, err
		}
		targets, err := parseJIDs(r.To)
		if err != nil {
			return nil, err
		}
		var out []any
		for _, to := range targets {
			m := s.message()
			m.MessageContextInfo = nil
			score := uint32(1)
			if ci := contextInfoOf(m); ci != nil {
				score = ci.GetForwardingScore() + 1
			}
			setContextInfo(m, &waE2E.ContextInfo{IsForwarded: proto.Bool(true), ForwardingScore: proto.Uint32(score)})
			res, err := a.send(ctx, to, m)
			if err != nil {
				out = append(out, map[string]any{"to": to.String(), "error": err.Error()})
			} else {
				out = append(out, res)
			}
		}
		return out, nil
	})

	handle("/api/poll/send", func(ctx context.Context, r struct {
		Recipient       string   `json:"recipient"`
		Question        string   `json:"question"`
		Options         []string `json:"options"`
		SelectableCount int      `json:"selectable_count"`
	}) (any, error) {
		to, err := parseJID(r.Recipient)
		if err != nil {
			return nil, err
		}
		if len(r.Options) < 2 || len(r.Options) > 12 {
			return nil, errors.New("polls need 2-12 options")
		}
		return a.send(ctx, to, cli.BuildPollCreation(r.Question, r.Options, r.SelectableCount))
	})

	handle("/api/poll/vote", func(ctx context.Context, r struct {
		msgRef
		Options []string `json:"options"`
	}) (any, error) {
		s, err := a.lookup(r.ChatJID, r.MessageID)
		if err != nil {
			return nil, err
		}
		m, err := cli.BuildPollVote(ctx, s.info(), r.Options)
		if err != nil {
			return nil, err
		}
		return a.send(ctx, s.Chat, m)
	})

	handle("/api/location/send", func(ctx context.Context, r struct {
		Recipient string  `json:"recipient"`
		Latitude  float64 `json:"latitude"`
		Longitude float64 `json:"longitude"`
		Name      string  `json:"name"`
		Address   string  `json:"address"`
	}) (any, error) {
		to, err := parseJID(r.Recipient)
		if err != nil {
			return nil, err
		}
		return a.send(ctx, to, &waE2E.Message{LocationMessage: &waE2E.LocationMessage{
			DegreesLatitude: proto.Float64(r.Latitude), DegreesLongitude: proto.Float64(r.Longitude),
			Name: proto.String(r.Name), Address: proto.String(r.Address)}})
	})

	handle("/api/contact/send", func(ctx context.Context, r struct {
		Recipient string `json:"recipient"`
		Name      string `json:"name"`
		Phone     string `json:"phone"`
	}) (any, error) {
		to, err := parseJID(r.Recipient)
		if err != nil {
			return nil, err
		}
		d := nonDigits.ReplaceAllString(r.Phone, "")
		vcard := fmt.Sprintf("BEGIN:VCARD\nVERSION:3.0\nFN:%s\nTEL;type=CELL;waid=%s:+%s\nEND:VCARD", r.Name, d, d)
		return a.send(ctx, to, &waE2E.Message{ContactMessage: &waE2E.ContactMessage{DisplayName: proto.String(r.Name), Vcard: proto.String(vcard)}})
	})

	handle("/api/read", func(ctx context.Context, r struct {
		ChatJID string `json:"chat_jid"`
	}) (any, error) {
		chat, err := parseJID(r.ChatJID)
		if err != nil {
			return nil, err
		}
		rows, err := db.Query(`SELECT id FROM messages WHERE chat_jid = ? AND is_from_me = 0 ORDER BY timestamp DESC LIMIT 30`, chat.String())
		if err != nil {
			return nil, err
		}
		var ids []string
		for rows.Next() {
			var id string
			rows.Scan(&id)
			ids = append(ids, id)
		}
		rows.Close()
		bySender := map[types.JID][]types.MessageID{}
		for _, id := range ids {
			if s, err := a.lookup(chat.String(), id); err == nil {
				bySender[s.Sender] = append(bySender[s.Sender], id)
			}
		}
		for sender, list := range bySender {
			if err := cli.MarkRead(ctx, list, time.Now(), chat, sender); err != nil {
				return nil, err
			}
		}
		if last, err := a.latest(chat); err == nil {
			cli.SendAppState(ctx, appstate.BuildMarkChatAsRead(chat, true, last.Timestamp, cli.BuildMessageKey(chat, last.Sender, last.ID)))
		}
		db.Exec("UPDATE chats SET unread_count = 0 WHERE jid = ?", chat.String())
		return fmt.Sprintf("marked %d messages read", len(ids)), nil
	})

	handle("/api/typing", func(ctx context.Context, r struct {
		ChatJID string `json:"chat_jid"`
		State   string `json:"state"`
	}) (any, error) {
		chat, err := parseJID(r.ChatJID)
		if err != nil {
			return nil, err
		}
		state, media := types.ChatPresenceComposing, types.ChatPresenceMediaText
		switch r.State {
		case "recording":
			media = types.ChatPresenceMediaAudio
		case "paused", "stop":
			state = types.ChatPresencePaused
		}
		return "ok", cli.SendChatPresence(ctx, chat, state, media)
	})

	handle("/api/chat/action", func(ctx context.Context, r struct {
		ChatJID   string  `json:"chat_jid"`
		Action    string  `json:"action"`
		MuteHours float64 `json:"mute_hours"`
		Seconds   int     `json:"seconds"`
	}) (any, error) {
		chat, err := parseJID(r.ChatJID)
		if err != nil {
			return nil, err
		}
		var lastTS time.Time
		var lastKey = cli.BuildMessageKey(chat, chat, "")
		if last, err := a.latest(chat); err == nil {
			lastTS, lastKey = last.Timestamp, cli.BuildMessageKey(chat, last.Sender, last.ID)
		}
		var patch appstate.PatchInfo
		switch r.Action {
		case "archive", "unarchive":
			patch = appstate.BuildArchive(chat, r.Action == "archive", lastTS, lastKey)
		case "pin", "unpin":
			patch = appstate.BuildPin(chat, r.Action == "pin")
		case "mute":
			d := time.Duration(r.MuteHours * float64(time.Hour))
			if r.MuteHours <= 0 {
				d = 0 // forever
			}
			patch = appstate.BuildMute(chat, true, d)
		case "unmute":
			patch = appstate.BuildMute(chat, false, 0)
		case "mark_read", "mark_unread":
			patch = appstate.BuildMarkChatAsRead(chat, r.Action == "mark_read", lastTS, lastKey)
		case "delete":
			patch = appstate.BuildDeleteChat(chat, lastTS, lastKey, true)
		case "disappearing":
			return "ok", cli.SetDisappearingTimer(ctx, chat, time.Duration(r.Seconds)*time.Second, time.Now())
		default:
			return nil, fmt.Errorf("unknown action %q", r.Action)
		}
		return "ok", cli.SendAppState(ctx, patch)
	})

	handle("/api/star", func(ctx context.Context, r struct {
		msgRef
		Starred bool `json:"starred"`
	}) (any, error) {
		s, err := a.lookup(r.ChatJID, r.MessageID)
		if err != nil {
			return nil, err
		}
		if err := cli.SendAppState(ctx, appstate.BuildStar(s.Chat, s.Sender, s.ID, s.FromMe, r.Starred)); err != nil {
			return nil, err
		}
		db.Exec("UPDATE messages SET starred = ? WHERE id = ? AND chat_jid = ?", r.Starred, s.ID, s.Chat.String())
		return "ok", nil
	})

	handle("/api/download", func(ctx context.Context, r msgRef) (any, error) {
		return a.download(ctx, r.ChatJID, r.MessageID)
	})

	handle("/api/history/request", func(ctx context.Context, r struct {
		ChatJID string `json:"chat_jid"`
		Count   int    `json:"count"`
	}) (any, error) {
		chat, err := parseJID(r.ChatJID)
		if err != nil {
			return nil, err
		}
		var id string
		if err := db.QueryRow("SELECT id FROM messages WHERE chat_jid = ? AND msg_type IS NOT 'system' ORDER BY timestamp ASC LIMIT 1", chat.String()).Scan(&id); err != nil {
			return nil, errors.New("need at least one stored message in the chat to page back from")
		}
		oldest, err := a.lookup(chat.String(), id)
		if err != nil {
			return nil, err
		}
		if r.Count <= 0 || r.Count > 500 {
			r.Count = 100
		}
		if _, err := cli.SendPeerMessage(ctx, cli.BuildHistorySyncRequest(oldest.info(), r.Count)); err != nil {
			return nil, err
		}
		return map[string]any{"requested_before": oldest.Timestamp, "count": r.Count,
			"note": "the phone sends older messages asynchronously (needs to be online); re-query in ~30s"}, nil
	})

	a.registerGroups()
	a.registerUsers()
}

func (a *API) download(ctx context.Context, chat, id string) (any, error) {
	s, err := a.lookup(chat, id)
	if err != nil {
		return nil, err
	}
	var filename, mediaType sql.NullString
	a.ms.db.QueryRow("SELECT filename, media_type FROM messages WHERE id = ? AND chat_jid = ?", id, s.Chat.String()).Scan(&filename, &mediaType)
	if mediaType.String == "" || filename.String == "" {
		return nil, errors.New("not a media message")
	}
	dir := filepath.Join("store", strings.ReplaceAll(s.Chat.String(), ":", "_"))
	os.MkdirAll(dir, 0755)
	path, _ := filepath.Abs(filepath.Join(dir, safeName(filename.String)))
	res := map[string]any{"path": path, "filename": filepath.Base(path), "media_type": mediaType.String}
	if _, err := os.Stat(path); err == nil {
		return res, nil
	}
	if len(s.Raw) > 0 {
		data, err := a.cli.DownloadAny(ctx, s.message())
		if err == nil {
			if err := os.WriteFile(path, data, 0644); err != nil {
				return nil, err
			}
			return res, nil
		}
		fmt.Printf("DownloadAny failed for %s, trying stored path: %v\n", id, err)
	}
	ok, _, _, p, err := downloadMedia(a.cli, a.ms, id, s.Chat.String())
	if !ok || err != nil {
		return nil, fmt.Errorf("download failed (media may have expired on WhatsApp servers): %v", err)
	}
	res["path"] = p
	return res, nil
}

// Groups & communities ---------------------------------------------------------

func participantsOut(list []types.GroupParticipant) []map[string]any {
	out := make([]map[string]any, 0, len(list))
	for _, p := range list {
		m := map[string]any{"jid": p.JID.String(), "is_admin": p.IsAdmin, "is_super_admin": p.IsSuperAdmin}
		if !p.PhoneNumber.IsEmpty() {
			m["phone"] = p.PhoneNumber.User
		}
		if !p.LID.IsEmpty() {
			m["lid"] = p.LID.String()
		}
		if p.DisplayName != "" {
			m["display_name"] = p.DisplayName
		}
		if p.Error != 0 {
			m["error"] = p.Error
			m["error_meaning"] = map[int]string{403: "privacy settings block adding; send an invite instead",
				408: "recently left; can't be re-added yet", 409: "already in the group", 401: "you blocked them or they blocked you",
				404: "not on WhatsApp"}[p.Error]
		}
		if p.AddRequest != nil {
			m["invite_code"] = p.AddRequest.Code
			m["invite_expiration"] = p.AddRequest.Expiration
		}
		out = append(out, m)
	}
	return out
}

func (a *API) groupOut(ctx context.Context, g *types.GroupInfo, withNames bool) map[string]any {
	out := map[string]any{
		"jid": g.JID.String(), "name": g.Name, "description": g.Topic, "created": g.GroupCreated,
		"owner": g.OwnerJID.String(), "owner_phone": g.OwnerPN.User,
		"announce_only_admins_send": g.IsAnnounce, "locked_only_admins_edit": g.IsLocked,
		"join_approval_required": g.IsJoinApprovalRequired, "member_add_mode": g.MemberAddMode,
		"disappearing_seconds": g.DisappearingTimer, "is_community": g.IsParent,
		"is_community_announcement_group": g.IsDefaultSubGroup, "participant_count": len(g.Participants),
		"addressing_mode": g.AddressingMode, "suspended": g.Suspended,
	}
	if g.ParticipantCount > len(g.Participants) {
		out["participant_count"] = g.ParticipantCount
	}
	if !g.LinkedParentJID.IsEmpty() {
		out["community_jid"] = g.LinkedParentJID.String()
	}
	if g.Participants != nil {
		ps := participantsOut(g.Participants)
		own, ownLID := a.own(), a.cli.Store.LID.ToNonAD()
		admins := 0
		for i, p := range g.Participants {
			if p.IsAdmin {
				admins++
			}
			if p.JID.ToNonAD() == own || p.JID.ToNonAD() == ownLID || p.PhoneNumber.ToNonAD() == own {
				ps[i]["is_me"] = true
				out["i_am_admin"] = p.IsAdmin
			}
			if withNames {
				for _, j := range []types.JID{p.PhoneNumber, p.JID} {
					if c, err := a.cli.Store.Contacts.GetContact(ctx, j); err == nil && c.Found {
						if n := firstNonEmpty(c.FullName, firstNonEmpty(c.PushName, c.BusinessName)); n != "" {
							ps[i]["name"] = n
							break
						}
					}
				}
			}
		}
		out["admin_count"] = admins
		out["participants"] = ps
	}
	return out
}

var inviteCode = regexp.MustCompile(`(?:chat\.whatsapp\.com/(?:invite/)?)?([A-Za-z0-9]{18,30})$`)

func parseInvite(s string) (string, error) {
	m := inviteCode.FindStringSubmatch(strings.TrimSpace(strings.TrimRight(s, "/")))
	if m == nil {
		return "", errors.New("not a WhatsApp invite link/code")
	}
	return m[1], nil
}

func (a *API) registerGroups() {
	cli := a.cli

	handle("/api/group/list", func(ctx context.Context, r struct {
		Query string `json:"query"`
	}) (any, error) {
		groups, err := cli.GetJoinedGroups(ctx)
		if err != nil {
			return nil, err
		}
		q := strings.ToLower(r.Query)
		out := []map[string]any{}
		for _, g := range groups {
			if q != "" && !strings.Contains(strings.ToLower(g.Name), q) && !strings.Contains(g.JID.String(), q) {
				continue
			}
			o := a.groupOut(ctx, g, false)
			delete(o, "participants")
			out = append(out, o)
		}
		sort.Slice(out, func(i, j int) bool { return fmt.Sprint(out[i]["name"]) < fmt.Sprint(out[j]["name"]) })
		return out, nil
	})

	handle("/api/group/info", func(ctx context.Context, r struct {
		JID        string `json:"jid"`
		InviteLink bool   `json:"invite_link"`
	}) (any, error) {
		jid, err := parseJID(r.JID)
		if err != nil {
			return nil, err
		}
		g, err := cli.GetGroupInfo(ctx, jid)
		if err != nil {
			return nil, err
		}
		out := a.groupOut(ctx, g, true)
		if g.IsParent {
			if subs, err := cli.GetSubGroups(ctx, jid); err == nil {
				out["subgroups"] = subs
			}
		}
		if r.InviteLink {
			if link, err := cli.GetGroupInviteLink(ctx, jid, false); err == nil {
				out["invite_link"] = link
			} else {
				out["invite_link_error"] = err.Error()
			}
		}
		if reqs, err := cli.GetGroupRequestParticipants(ctx, jid); err == nil && len(reqs) > 0 {
			out["pending_join_requests"] = reqs
		}
		return out, nil
	})

	handle("/api/group/create", func(ctx context.Context, r struct {
		Name         string   `json:"name"`
		Participants []string `json:"participants"`
		IsCommunity  bool     `json:"is_community"`
		CommunityJID string   `json:"community_jid"`
		Description  string   `json:"description"`
		AnnounceOnly bool     `json:"announce_only"`
		Locked       bool     `json:"locked"`
		JoinApproval bool     `json:"join_approval"`
		Disappearing int      `json:"disappearing_seconds"`
	}) (any, error) {
		if err := a.connected(); err != nil {
			return nil, err
		}
		ps, err := parseJIDs(r.Participants)
		if err != nil {
			return nil, err
		}
		req := whatsmeow.ReqCreateGroup{Name: r.Name, Participants: ps}
		req.IsParent = r.IsCommunity
		req.IsAnnounce = r.AnnounceOnly
		req.IsLocked = r.Locked
		req.IsJoinApprovalRequired = r.JoinApproval
		if r.Disappearing > 0 {
			req.IsEphemeral, req.DisappearingTimer = true, uint32(r.Disappearing)
		}
		if r.CommunityJID != "" {
			if req.LinkedParentJID, err = parseJID(r.CommunityJID); err != nil {
				return nil, err
			}
		}
		g, err := cli.CreateGroup(ctx, req)
		if err != nil {
			return nil, err
		}
		if r.Description != "" {
			if err := cli.SetGroupDescription(ctx, g.JID, r.Description); err != nil {
				fmt.Printf("set description: %v\n", err)
			}
		}
		out := a.groupOut(ctx, g, false)
		if link, err := cli.GetGroupInviteLink(ctx, g.JID, false); err == nil {
			out["invite_link"] = link
		}
		return out, nil
	})

	handle("/api/group/participants", func(ctx context.Context, r struct {
		JID          string   `json:"jid"`
		Action       string   `json:"action"`
		Participants []string `json:"participants"`
		SendInvites  bool     `json:"send_invites"`
	}) (any, error) {
		jid, err := parseJID(r.JID)
		if err != nil {
			return nil, err
		}
		ps, err := parseJIDs(r.Participants)
		if err != nil {
			return nil, err
		}
		var res []types.GroupParticipant
		switch r.Action {
		case "add", "remove", "promote", "demote":
			res, err = cli.UpdateGroupParticipants(ctx, jid, ps, whatsmeow.ParticipantChange(r.Action))
		case "approve", "reject":
			res, err = cli.UpdateGroupRequestParticipants(ctx, jid, ps, whatsmeow.ParticipantRequestChange(r.Action))
		default:
			return nil, fmt.Errorf("action must be add/remove/promote/demote/approve/reject")
		}
		if err != nil {
			return nil, err
		}
		out := participantsOut(res)
		if r.SendInvites {
			name := ""
			if g, err := cli.GetGroupInfo(ctx, jid); err == nil {
				name = g.Name
			}
			for i, p := range res {
				if p.AddRequest == nil {
					continue
				}
				to := p.PhoneNumber
				if to.IsEmpty() {
					to = p.JID
				}
				_, err := a.send(ctx, to, &waE2E.Message{GroupInviteMessage: &waE2E.GroupInviteMessage{
					GroupJID: proto.String(jid.String()), InviteCode: proto.String(p.AddRequest.Code),
					InviteExpiration: proto.Int64(p.AddRequest.Expiration.Unix()), GroupName: proto.String(name),
					Caption: proto.String("Invitation to join my WhatsApp group")}})
				out[i]["invite_sent"] = err == nil
				if err != nil {
					out[i]["invite_error"] = err.Error()
				}
			}
		}
		return out, nil
	})

	handle("/api/group/settings", func(ctx context.Context, r struct {
		JID           string  `json:"jid"`
		Name          *string `json:"name"`
		Description   *string `json:"description"`
		AnnounceOnly  *bool   `json:"announce_only"`
		Locked        *bool   `json:"locked"`
		JoinApproval  *bool   `json:"join_approval"`
		AdminsAddOnly *bool   `json:"admins_add_only"`
		Disappearing  *int    `json:"disappearing_seconds"`
		PhotoPath     *string `json:"photo_path"`
	}) (any, error) {
		jid, err := parseJID(r.JID)
		if err != nil {
			return nil, err
		}
		done := map[string]any{}
		try := func(k string, err error) {
			if err != nil {
				done[k] = "error: " + err.Error()
			} else {
				done[k] = "ok"
			}
		}
		if r.Name != nil {
			try("name", cli.SetGroupName(ctx, jid, *r.Name))
		}
		if r.Description != nil {
			try("description", cli.SetGroupDescription(ctx, jid, *r.Description))
		}
		if r.AnnounceOnly != nil {
			try("announce_only", cli.SetGroupAnnounce(ctx, jid, *r.AnnounceOnly))
		}
		if r.Locked != nil {
			try("locked", cli.SetGroupLocked(ctx, jid, *r.Locked))
		}
		if r.JoinApproval != nil {
			try("join_approval", cli.SetGroupJoinApprovalMode(ctx, jid, *r.JoinApproval))
		}
		if r.AdminsAddOnly != nil {
			mode := types.GroupMemberAddModeAllMember
			if *r.AdminsAddOnly {
				mode = types.GroupMemberAddModeAdmin
			}
			try("admins_add_only", cli.SetGroupMemberAddMode(ctx, jid, mode))
		}
		if r.Disappearing != nil {
			try("disappearing_seconds", cli.SetDisappearingTimer(ctx, jid, time.Duration(*r.Disappearing)*time.Second, time.Now()))
		}
		if r.PhotoPath != nil {
			var data []byte
			if *r.PhotoPath != "" {
				if data, err = os.ReadFile(*r.PhotoPath); err != nil {
					return nil, err
				}
			}
			_, err := cli.SetGroupPhoto(ctx, jid, data)
			try("photo", err)
		}
		if len(done) == 0 {
			return nil, errors.New("nothing to change")
		}
		return done, nil
	})

	handle("/api/group/invite_link", func(ctx context.Context, r struct {
		JID   string `json:"jid"`
		Reset bool   `json:"reset"`
	}) (any, error) {
		jid, err := parseJID(r.JID)
		if err != nil {
			return nil, err
		}
		link, err := cli.GetGroupInviteLink(ctx, jid, r.Reset)
		return map[string]any{"invite_link": link}, err
	})

	handle("/api/group/invite_info", func(ctx context.Context, r struct {
		Link string `json:"link"`
	}) (any, error) {
		code, err := parseInvite(r.Link)
		if err != nil {
			return nil, err
		}
		g, err := cli.GetGroupInfoFromLink(ctx, code)
		if err != nil {
			return nil, err
		}
		return a.groupOut(ctx, g, false), nil
	})

	handle("/api/group/join", func(ctx context.Context, r struct {
		Link string `json:"link"`
	}) (any, error) {
		code, err := parseInvite(r.Link)
		if err != nil {
			return nil, err
		}
		jid, err := cli.JoinGroupWithLink(ctx, code)
		return map[string]any{"jid": jid.String()}, err
	})

	handle("/api/group/leave", func(ctx context.Context, r struct {
		JID string `json:"jid"`
	}) (any, error) {
		jid, err := parseJID(r.JID)
		if err != nil {
			return nil, err
		}
		return "left group", cli.LeaveGroup(ctx, jid)
	})

	handle("/api/group/requests", func(ctx context.Context, r struct {
		JID string `json:"jid"`
	}) (any, error) {
		jid, err := parseJID(r.JID)
		if err != nil {
			return nil, err
		}
		return cli.GetGroupRequestParticipants(ctx, jid)
	})

	handle("/api/community/link", func(ctx context.Context, r struct {
		CommunityJID string `json:"community_jid"`
		GroupJID     string `json:"group_jid"`
		Unlink       bool   `json:"unlink"`
	}) (any, error) {
		parent, err := parseJID(r.CommunityJID)
		if err != nil {
			return nil, err
		}
		child, err := parseJID(r.GroupJID)
		if err != nil {
			return nil, err
		}
		if r.Unlink {
			return "unlinked", cli.UnlinkGroup(ctx, parent, child)
		}
		return "linked", cli.LinkGroup(ctx, parent, child)
	})

	handle("/api/community/members", func(ctx context.Context, r struct {
		JID string `json:"jid"`
	}) (any, error) {
		jid, err := parseJID(r.JID)
		if err != nil {
			return nil, err
		}
		return cli.GetLinkedGroupsParticipants(ctx, jid)
	})
}

// Users, contacts, profile, privacy, status, channels, calls -------------------

func (a *API) contactName(ctx context.Context, j types.JID) map[string]any {
	c, err := a.cli.Store.Contacts.GetContact(ctx, j)
	if err != nil || !c.Found {
		return nil
	}
	return map[string]any{"full_name": c.FullName, "first_name": c.FirstName, "push_name": c.PushName, "business_name": c.BusinessName}
}

func (a *API) registerUsers() {
	cli := a.cli

	handle("/api/user/info", func(ctx context.Context, r struct {
		Users   []string `json:"users"`
		Picture bool     `json:"picture"`
	}) (any, error) {
		if err := a.connected(); err != nil {
			return nil, err
		}
		out := []map[string]any{}
		var jids []types.JID
		var phones []string
		for _, u := range r.Users {
			if strings.Contains(u, "@") {
				j, err := types.ParseJID(u)
				if err != nil {
					return nil, err
				}
				jids = append(jids, j.ToNonAD())
			} else {
				phones = append(phones, "+"+nonDigits.ReplaceAllString(u, ""))
			}
		}
		if len(phones) > 0 {
			resp, err := cli.IsOnWhatsApp(ctx, phones)
			if err != nil {
				return nil, err
			}
			for _, p := range resp {
				if !p.IsIn {
					out = append(out, map[string]any{"query": p.Query, "on_whatsapp": false})
					continue
				}
				jids = append(jids, p.JID)
			}
		}
		if len(jids) == 0 {
			return out, nil
		}
		infos, err := cli.GetUserInfo(ctx, jids)
		if err != nil {
			return nil, err
		}
		for _, j := range jids {
			info := infos[j]
			m := map[string]any{"jid": j.String(), "on_whatsapp": true, "about": info.Status, "picture_id": info.PictureID,
				"device_count": len(info.Devices)}
			if !info.LID.IsEmpty() {
				m["lid"] = info.LID.String()
			}
			if pn := a.rs.phoneFor(j, types.EmptyJID); !pn.IsEmpty() {
				m["phone"] = pn.User
			}
			if c := a.contactName(ctx, j); c != nil {
				m["contact"] = c
			}
			if info.VerifiedName != nil && info.VerifiedName.Details != nil {
				m["business_verified_name"] = info.VerifiedName.Details.GetVerifiedName()
				if bp, err := cli.GetBusinessProfile(ctx, j); err == nil {
					m["business_profile"] = bp
				}
			}
			if r.Picture {
				if pic, err := cli.GetProfilePictureInfo(ctx, j, &whatsmeow.GetProfilePictureParams{}); err == nil && pic != nil {
					m["picture_url"] = pic.URL
					if path, err := savePicture(pic.URL, j); err == nil {
						m["picture_path"] = path
					}
				}
			}
			presenceMu.Lock()
			if p, ok := presences[j.String()]; ok {
				m["presence"] = *p
			}
			presenceMu.Unlock()
			out = append(out, m)
		}
		return out, nil
	})

	handle("/api/contacts", func(ctx context.Context, r struct {
		Query string `json:"query"`
		Limit int    `json:"limit"`
	}) (any, error) {
		all, err := cli.Store.Contacts.GetAllContacts(ctx)
		if err != nil {
			return nil, err
		}
		q := strings.ToLower(r.Query)
		qd := nonDigits.ReplaceAllString(r.Query, "")
		out := []map[string]any{}
		for j, c := range all {
			names := strings.ToLower(c.FullName + " " + c.FirstName + " " + c.PushName + " " + c.BusinessName)
			pn := a.rs.phoneFor(j, types.EmptyJID)
			if q != "" && !strings.Contains(names, q) && !(qd != "" && (strings.Contains(j.User, qd) || strings.Contains(pn.User, qd))) {
				continue
			}
			m := map[string]any{"jid": j.String(), "full_name": c.FullName, "push_name": c.PushName, "business_name": c.BusinessName,
				"saved_in_phone": c.FullName != ""}
			if !pn.IsEmpty() {
				m["phone"] = pn.User
			}
			out = append(out, m)
		}
		sort.Slice(out, func(i, j int) bool {
			return fmt.Sprint(out[i]["full_name"], out[i]["push_name"]) < fmt.Sprint(out[j]["full_name"], out[j]["push_name"])
		})
		if r.Limit <= 0 {
			r.Limit = 50
		}
		if len(out) > r.Limit {
			out = out[:r.Limit]
		}
		return out, nil
	})

	handle("/api/blocklist", func(ctx context.Context, _ struct{}) (any, error) {
		bl, err := cli.GetBlocklist(ctx, "")
		if err != nil {
			return nil, err
		}
		return bl.Items, nil
	})

	handle("/api/block", func(ctx context.Context, r struct {
		JID   string `json:"jid"`
		Block bool   `json:"block"`
	}) (any, error) {
		j, err := parseJID(r.JID)
		if err != nil {
			return nil, err
		}
		action := events.BlocklistChangeActionUnblock
		if r.Block {
			action = events.BlocklistChangeActionBlock
		}
		bl, err := cli.UpdateBlocklist(ctx, j, action, "")
		if err != nil {
			return nil, err
		}
		return map[string]any{"blocked_count": len(bl.Items)}, nil
	})

	handle("/api/privacy", func(ctx context.Context, r struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}) (any, error) {
		if r.Name != "" {
			return cli.SetPrivacySetting(ctx, types.PrivacySettingType(r.Name), types.PrivacySetting(r.Value))
		}
		return cli.TryFetchPrivacySettings(ctx, true)
	})

	handle("/api/profile", func(ctx context.Context, r struct {
		About     *string `json:"about"`
		PushName  *string `json:"push_name"`
		PhotoPath *string `json:"photo_path"`
	}) (any, error) {
		done := map[string]string{}
		mark := func(k string, err error) {
			done[k] = "ok"
			if err != nil {
				done[k] = "error: " + err.Error()
			}
		}
		if r.About != nil {
			mark("about", cli.SetStatusMessage(ctx, types.SetStatusInput{Text: r.About}))
		}
		if r.PushName != nil {
			mark("push_name", cli.SendAppState(ctx, appstate.BuildSettingPushName(*r.PushName)))
		}
		if r.PhotoPath != nil {
			var data []byte
			var err error
			if *r.PhotoPath != "" {
				if data, err = os.ReadFile(*r.PhotoPath); err != nil {
					return nil, err
				}
			}
			_, err = cli.SetGroupPhoto(ctx, types.EmptyJID, data)
			mark("photo", err)
		}
		return done, nil
	})

	handle("/api/status/post", func(ctx context.Context, r sendReq) (any, error) {
		var m *waE2E.Message
		var err error
		if r.MediaPath != "" {
			if m, err = a.buildMedia(ctx, r); err != nil {
				return nil, err
			}
		} else {
			m = &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String(r.Message),
				BackgroundArgb: proto.Uint32(0xFF075E54), TextArgb: proto.Uint32(0xFFFFFFFF)}}
		}
		return a.send(ctx, types.StatusBroadcastJID, m)
	})

	handle("/api/presence/subscribe", func(ctx context.Context, r struct {
		JID string `json:"jid"`
	}) (any, error) {
		j, err := parseJID(r.JID)
		if err != nil {
			return nil, err
		}
		cli.SendPresence(ctx, types.PresenceAvailable)
		return "subscribed; presence appears in get_user_info after the contact's next update", cli.SubscribePresence(ctx, j)
	})

	handle("/api/call/reject", func(ctx context.Context, r struct {
		CallID string `json:"call_id"`
		From   string `json:"from"`
	}) (any, error) {
		j, err := parseJID(r.From)
		if err != nil {
			return nil, err
		}
		return "rejected", cli.RejectCall(ctx, j, r.CallID)
	})

	handle("/api/newsletter/list", func(ctx context.Context, _ struct{}) (any, error) {
		return cli.GetSubscribedNewsletters(ctx)
	})

	handle("/api/newsletter/info", func(ctx context.Context, r struct {
		JID    string `json:"jid"`
		Invite string `json:"invite"`
	}) (any, error) {
		if r.Invite != "" {
			key := r.Invite
			if i := strings.LastIndex(key, "/"); i >= 0 {
				key = key[i+1:]
			}
			return cli.GetNewsletterInfoWithInvite(ctx, key)
		}
		j, err := parseJID(r.JID)
		if err != nil {
			return nil, err
		}
		return cli.GetNewsletterInfo(ctx, j)
	})

	handle("/api/newsletter/messages", func(ctx context.Context, r struct {
		JID    string `json:"jid"`
		Count  int    `json:"count"`
		Before int    `json:"before_server_id"`
	}) (any, error) {
		j, err := parseJID(r.JID)
		if err != nil {
			return nil, err
		}
		if r.Count <= 0 {
			r.Count = 20
		}
		msgs, err := cli.GetNewsletterMessages(ctx, j, &whatsmeow.GetNewsletterMessagesParams{Count: r.Count, Before: types.MessageServerID(r.Before)})
		if err != nil {
			return nil, err
		}
		out := []map[string]any{}
		for _, m := range msgs {
			o := map[string]any{"server_id": m.MessageServerID, "id": m.MessageID, "timestamp": m.Timestamp,
				"views": m.ViewsCount, "reactions": m.ReactionCounts}
			if rm, ok := ParseRich(m.Message, m.MessageID); ok {
				o["type"], o["content"] = rm.Type, rm.Content
				if rm.Extra != nil {
					o["extra"] = rm.Extra
				}
			}
			out = append(out, o)
		}
		return out, nil
	})

	handle("/api/newsletter/follow", func(ctx context.Context, r struct {
		JID    string `json:"jid"`
		Follow bool   `json:"follow"`
	}) (any, error) {
		j, err := parseJID(r.JID)
		if err != nil {
			return nil, err
		}
		if r.Follow {
			return "followed", cli.FollowNewsletter(ctx, j)
		}
		return "unfollowed", cli.UnfollowNewsletter(ctx, j)
	})
}

func savePicture(url string, j types.JID) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return "", err
	}
	os.MkdirAll("store/profile_pictures", 0755)
	path, _ := filepath.Abs(filepath.Join("store/profile_pictures", safeName(j.User)+".jpg"))
	return path, os.WriteFile(path, data, 0644)
}
