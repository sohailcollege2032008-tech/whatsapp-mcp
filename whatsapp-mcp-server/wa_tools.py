"""Full WhatsApp toolset shared by the stdio (main.py) and HTTP (http_server.py) servers.

Read tools query the bridge's SQLite stores directly (rich, offline, fast).
Live tools and actions call the Go bridge REST API on loopback.
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import tempfile
from collections import Counter
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import requests

import whatsapp as legacy

STORE = os.path.dirname(os.path.abspath(legacy.MESSAGES_DB_PATH))
MESSAGES_DB = os.path.join(STORE, "messages.db")
DEVICE_DB = os.path.join(STORE, "whatsapp.db")
API = legacy.WHATSAPP_API_BASE_URL

# http_server.py swaps this for a guard that only allows safe paths.
PATH_GUARD: Callable[[str], str] = lambda p: p
# ...and this one to validate the stored filename before the bridge writes a download.
DOWNLOAD_GUARD: Callable[[str, str], Any] = lambda message_id, chat_jid: None


class BridgeError(RuntimeError):
    pass


def _api(path: str, payload: Optional[dict] = None, timeout: int = 120) -> Any:
    try:
        r = requests.post(f"{API}/{path}", json=payload or {}, timeout=timeout)
    except requests.RequestException as e:
        raise BridgeError(f"WhatsApp bridge unreachable: {e}") from None
    try:
        data = r.json()
    except ValueError:
        raise BridgeError(f"HTTP {r.status_code}: {r.text[:300]}") from None
    if not data.get("success"):
        raise BridgeError(data.get("message") or f"HTTP {r.status_code}")
    return data.get("result", data.get("message"))


def _clean(d: dict) -> dict:
    return {k: v for k, v in d.items() if v not in (None, "", [], {}, 0, False) or k in ("success",)}


# --------------------------------------------------------------------------- names

class _Names:
    """Resolves LIDs/phones to the best display name (saved name > push name > business)."""

    def __init__(self, conn: sqlite3.Connection):
        self.c = conn
        self.cache: Dict[str, Dict[str, Optional[str]]] = {}

    def who(self, user: str, push: Optional[str] = None) -> Dict[str, Optional[str]]:
        if not user:
            return {"name": push, "phone": None}
        user_part = user.split("@")[0].split(":")[0]
        key = user_part + "|" + (push or "")
        if key in self.cache:
            return self.cache[key]
        pn = lid = None
        if user.endswith("@lid"):
            lid = user_part
        elif user.endswith("@s.whatsapp.net"):
            pn = user_part
        elif self.c.execute("SELECT 1 FROM dev.whatsmeow_lid_map WHERE lid = ?", (user_part,)).fetchone() or \
                self.c.execute("SELECT 1 FROM dev.whatsmeow_contacts WHERE their_jid = ?", (user_part + "@lid",)).fetchone():
            lid = user_part
        else:
            pn = user_part
        if lid and not pn:
            row = self.c.execute("SELECT pn FROM dev.whatsmeow_lid_map WHERE lid = ?", (lid,)).fetchone()
            pn = row[0] if row else None
        elif pn and not lid:
            row = self.c.execute("SELECT lid FROM dev.whatsmeow_lid_map WHERE pn = ?", (pn,)).fetchone()
            lid = row[0] if row else None
        cands = [j for j in ((pn and pn + "@s.whatsapp.net"), (lid and lid + "@lid")) if j]
        full = pushn = biz = None
        for j in cands:
            row = self.c.execute(
                "SELECT full_name, push_name, business_name FROM dev.whatsmeow_contacts WHERE their_jid = ?", (j,)
            ).fetchone()
            if row:
                full, pushn, biz = full or row[0] or None, pushn or row[1] or None, biz or row[2] or None
        name = full or push or pushn or biz
        if not name and pn:
            row = self.c.execute("SELECT name FROM chats WHERE jid = ?", (pn + "@s.whatsapp.net",)).fetchone()
            name = row[0] if row and row[0] and row[0] != pn else None
        res = {"name": name, "phone": pn, "lid": lid, "saved_name": full, "push_name": push or pushn}
        self.cache[key] = res
        return res

    def label(self, user: str, push: Optional[str] = None) -> str:
        w = self.who(user, push)
        if w["name"] and w["phone"]:
            return f"{w['name']} (+{w['phone']})"
        return w["name"] or (f"+{w['phone']}" if w["phone"] else user)


def _db() -> sqlite3.Connection:
    c = sqlite3.connect(f"file:{MESSAGES_DB}?mode=ro", uri=True, timeout=10)
    c.execute(f"ATTACH DATABASE 'file:{DEVICE_DB}?mode=ro' AS dev")
    c.row_factory = sqlite3.Row
    return c


MSG_COLS = """m.id, m.chat_jid, c.name AS chat_name, m.sender, m.sender_jid, m.sender_pn, m.push_name, m.content,
    m.timestamp, m.is_from_me, m.media_type, m.filename, m.msg_type, m.quoted_id, m.quoted_sender, m.quoted_text,
    m.mentions, m.is_forwarded, m.forwarding_score, m.is_view_once, m.is_ephemeral, m.edited_at, m.original_content,
    m.is_revoked, m.starred, m.extra, m.file_length, m.mimetype"""


def _reactions(c, chat: str, mid: str, names: _Names) -> List[dict]:
    rows = c.execute("SELECT sender_jid, emoji, timestamp FROM reactions WHERE chat_jid = ? AND message_id = ?", (chat, mid))
    return [{"emoji": r[1], "by": names.label(r[0]), "at": r[2]} for r in rows]


def _msg_dict(c, r: sqlite3.Row, names: _Names, full: bool = False) -> dict:
    if r["is_from_me"]:
        sender = "Me"
    elif not r["sender_jid"] and r["chat_jid"].endswith("@g.us") and r["sender"] == r["chat_jid"].split("@")[0]:
        sender = "Unknown member (old imported history)"
    else:
        sender = names.label(r["sender_jid"] or r["sender"], r["push_name"])
    d = {
        "id": r["id"], "chat_jid": r["chat_jid"], "chat_name": r["chat_name"], "timestamp": r["timestamp"],
        "from": sender, "is_from_me": bool(r["is_from_me"]), "type": r["msg_type"] or r["media_type"] or "text",
        "content": r["content"],
    }
    if r["media_type"]:
        d["media"] = {"kind": r["media_type"], "filename": r["filename"], "size": r["file_length"], "mimetype": r["mimetype"],
                      "download": "call download_media(message_id, chat_jid)"}
    if r["quoted_id"]:
        d["reply_to"] = {"id": r["quoted_id"], "from": names.label(r["quoted_sender"]) if r["quoted_sender"] else None,
                         "text": r["quoted_text"]}
    if r["mentions"]:
        d["mentions"] = [names.label(j) for j in json.loads(r["mentions"])]
    if r["is_forwarded"]:
        d["forwarded"] = "many times" if (r["forwarding_score"] or 0) >= 5 else True
    if r["edited_at"]:
        d["edited_at"] = r["edited_at"]
        d["original_content"] = r["original_content"]
    if r["is_revoked"]:
        d["deleted_for_everyone"] = True
    for k in ("starred", "is_view_once", "is_ephemeral"):
        if r[k]:
            d[k] = True
    if r["extra"]:
        d["details"] = json.loads(r["extra"])
    reacts = _reactions(c, r["chat_jid"], r["id"], names)
    if reacts:
        d["reactions"] = reacts if full else dict(Counter(x["emoji"] for x in reacts))
    if full:
        rec = c.execute("SELECT reader_jid, type, timestamp FROM receipts WHERE chat_jid = ? AND message_id = ? ORDER BY timestamp",
                        (r["chat_jid"], r["id"])).fetchall()
        if rec:
            d["receipts"] = [{"by": names.label(x[0]), "type": x[1], "at": x[2]} for x in rec]
        if d["type"] == "poll":
            votes = c.execute("SELECT voter_jid, options, timestamp FROM poll_votes WHERE chat_jid = ? AND poll_id = ?",
                              (r["chat_jid"], r["id"])).fetchall()
            tally: Counter = Counter()
            for v in votes:
                tally.update(json.loads(v[1]))
            d["poll_results"] = {"tally": dict(tally), "votes": [{"by": names.label(v[0]), "options": json.loads(v[1]), "at": v[2]} for v in votes]}
    return d


def _line(d: dict) -> str:
    bits = [f"[{str(d['timestamp'])[:19]}]"]
    if d.get("chat_name"):
        bits.append(f"{d['chat_name']} |")
    bits.append(f"{d['from']}:")
    tags = []
    if d["type"] not in ("text",):
        tags.append(d["type"])
    if "reply_to" in d:
        q = (d["reply_to"].get("text") or "")[:60]
        tags.append(f"reply to {d['reply_to'].get('from') or ''} \"{q}\"")
    if d.get("forwarded"):
        tags.append("forwarded")
    if d.get("edited_at"):
        tags.append("edited")
    if d.get("deleted_for_everyone"):
        tags.append("DELETED for everyone")
    if d.get("mentions"):
        tags.append("mentions " + ", ".join(d["mentions"]))
    if d.get("reactions"):
        tags.append("reactions " + " ".join(f"{e}x{n}" for e, n in d["reactions"].items()))
    if d.get("details", {}).get("options"):
        tags.append("options: " + " / ".join(d["details"]["options"]))
    if d.get("details", {}).get("maps_url"):
        tags.append(d["details"]["maps_url"])
    tag = f"[{'; '.join(tags)}] " if tags else ""
    return f"{' '.join(bits)} {tag}{d['content'] or ''}  (id={d['id']}, chat={d['chat_jid']})"


# --------------------------------------------------------------------------- read tools (stored data)

def list_messages(
    after: Optional[str] = None,
    before: Optional[str] = None,
    sender_phone_number: Optional[str] = None,
    chat_jid: Optional[str] = None,
    query: Optional[str] = None,
    limit: int = 20,
    page: int = 0,
    include_context: bool = True,
    context_before: int = 1,
    context_after: int = 1,
    message_type: Optional[str] = None,
    only_starred: bool = False,
    include_deleted: bool = True,
) -> str:
    """Search stored WhatsApp messages. Each line shows sender name (+phone), type, reply/forward/edit/deleted
    flags, reactions and the message id. Newest first.

    Args:
        after / before: ISO-8601 datetime bounds
        sender_phone_number: sender's phone digits (or LID)
        chat_jid: limit to one chat (use list_chats to find it)
        query: text to find in message text/captions
        limit, page: pagination
        include_context: add surrounding messages for each match (default True)
        context_before / context_after: how many surrounding messages
        message_type: text, image, video, voice, audio, document, sticker, location, contact, poll, event, system...
        only_starred: only starred messages
        include_deleted: include messages the sender deleted for everyone (default True)
    """
    where, params = [], []
    if after:
        where.append("m.timestamp > ?"); params.append(datetime.fromisoformat(after).isoformat(" "))
    if before:
        where.append("m.timestamp < ?"); params.append(datetime.fromisoformat(before).isoformat(" "))
    if sender_phone_number:
        s = "".join(ch for ch in sender_phone_number if ch.isdigit()) or sender_phone_number
        where.append("(m.sender = ? OR m.sender_pn = ? OR m.sender = (SELECT lid FROM dev.whatsmeow_lid_map WHERE pn = ?))")
        params += [s, s, s]
    if chat_jid:
        where.append("m.chat_jid = ?"); params.append(chat_jid)
    if query:
        where.append("LOWER(m.content) LIKE LOWER(?)"); params.append(f"%{query}%")
    if message_type:
        where.append("COALESCE(m.msg_type, NULLIF(m.media_type, ''), 'text') = ?"); params.append(message_type)
    if only_starred:
        where.append("m.starred = 1")
    if not include_deleted:
        where.append("COALESCE(m.is_revoked, 0) = 0")
    sql = f"SELECT {MSG_COLS} FROM messages m LEFT JOIN chats c ON c.jid = m.chat_jid"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY m.timestamp DESC LIMIT ? OFFSET ?"
    params += [limit, page * limit]
    with _db() as c:
        names = _Names(c)
        rows = c.execute(sql, params).fetchall()
        if not rows:
            return "No messages found."
        out, seen = [], set()
        for r in rows:
            group = [r]
            if include_context:
                b = c.execute(f"SELECT {MSG_COLS} FROM messages m LEFT JOIN chats c ON c.jid = m.chat_jid WHERE m.chat_jid = ? AND m.timestamp < ? ORDER BY m.timestamp DESC LIMIT ?",
                              (r["chat_jid"], r["timestamp"], context_before)).fetchall()
                a = c.execute(f"SELECT {MSG_COLS} FROM messages m LEFT JOIN chats c ON c.jid = m.chat_jid WHERE m.chat_jid = ? AND m.timestamp > ? ORDER BY m.timestamp ASC LIMIT ?",
                              (r["chat_jid"], r["timestamp"], context_after)).fetchall()
                group = list(reversed(b)) + [r] + list(a)
            for g in group:
                if (g["id"], g["chat_jid"]) in seen:
                    continue
                seen.add((g["id"], g["chat_jid"]))
                prefix = ">> " if include_context and g is r else ""
                out.append(prefix + _line(_msg_dict(c, g, names)))
            if include_context:
                out.append("---")
        return "\n".join(out)


def get_message_context(message_id: str, before: int = 5, after: int = 5) -> str:
    """Show the messages around one message (by id), oldest first, the target marked with >>."""
    with _db() as c:
        r = c.execute("SELECT chat_jid, timestamp FROM messages WHERE id = ?", (message_id,)).fetchone()
        if not r:
            return f"Message {message_id} not found"
        names = _Names(c)
        q = f"SELECT {MSG_COLS} FROM messages m LEFT JOIN chats c ON c.jid = m.chat_jid WHERE m.chat_jid = ? AND "
        rows = list(reversed(c.execute(q + "m.timestamp <= ? ORDER BY m.timestamp DESC LIMIT ?", (r[0], r[1], before + 1)).fetchall()))
        rows += c.execute(q + "m.timestamp > ? ORDER BY m.timestamp ASC LIMIT ?", (r[0], r[1], after)).fetchall()
        return "\n".join((">> " if x["id"] == message_id else "") + _line(_msg_dict(c, x, names)) for x in rows)


def get_message_details(chat_jid: str, message_id: str) -> dict:
    """Everything about one message: full text, original text if edited, reply/forward info, mentions,
    each reaction with who reacted, delivery/read/played receipts (who read it and when), poll votes and
    results, location/contact/link-preview details."""
    with _db() as c:
        r = c.execute(f"SELECT {MSG_COLS} FROM messages m LEFT JOIN chats c ON c.jid = m.chat_jid WHERE m.id = ? AND m.chat_jid = ?",
                      (message_id, chat_jid)).fetchone()
        if not r:
            raise ValueError("message not found")
        names = _Names(c)
        d = _msg_dict(c, r, names, full=True)
        if not r["is_from_me"]:
            d["sender"] = names.who(r["sender_jid"] or r["sender"], r["push_name"])
        return d


def list_chats(
    query: Optional[str] = None,
    limit: int = 20,
    page: int = 0,
    include_last_message: bool = True,
    sort_by: str = "last_active",
    chat_type: str = "all",
) -> List[dict]:
    """List WhatsApp chats with name, type, unread count, archived/pinned/muted state and last message.

    Args:
        query: match chat name, JID or phone
        limit, page: pagination
        include_last_message: include a one-line preview of the last message
        sort_by: "last_active" or "name"
        chat_type: all, groups, direct, unread, archived, pinned, muted
    """
    where, params = [], []
    if query:
        where.append("(LOWER(c.name) LIKE LOWER(?) OR c.jid LIKE ?)"); params += [f"%{query}%", f"%{query}%"]
    filters = {
        "groups": "c.jid LIKE '%@g.us'", "direct": "(c.jid LIKE '%@s.whatsapp.net' OR c.jid LIKE '%@lid')",
        "unread": "COALESCE(c.unread_count, 0) > 0", "archived": "s.archived = 1", "pinned": "s.pinned = 1",
        "muted": "s.muted_until > strftime('%s','now') OR s.muted_until = -1",
    }
    if chat_type in filters:
        where.append(filters[chat_type])
    sql = """SELECT c.jid, c.name, c.last_message_time, c.unread_count, s.archived, s.pinned, s.muted_until
        FROM chats c LEFT JOIN dev.whatsmeow_chat_settings s ON s.chat_jid = c.jid"""
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY " + ("c.name" if sort_by == "name" else "s.pinned DESC, c.last_message_time DESC") + " LIMIT ? OFFSET ?"
    params += [limit, page * limit]
    out = []
    with _db() as c:
        names = _Names(c)
        for r in c.execute(sql, params).fetchall():
            jid = r["jid"]
            d = {"jid": jid, "name": r["name"], "is_group": jid.endswith("@g.us"), "last_active": r["last_message_time"],
                 "unread": r["unread_count"], "archived": bool(r["archived"]), "pinned": bool(r["pinned"])}
            if r["muted_until"]:
                d["muted"] = "forever" if r["muted_until"] < 0 else datetime.fromtimestamp(r["muted_until"]).isoformat(" ")
            if not d["is_group"] and jid.endswith("@lid"):
                d.update({k: v for k, v in names.who(jid).items() if k in ("phone",) and v})
            if include_last_message:
                m = c.execute(f"SELECT {MSG_COLS} FROM messages m LEFT JOIN chats c ON c.jid = m.chat_jid WHERE m.chat_jid = ? ORDER BY m.timestamp DESC LIMIT 1",
                              (jid,)).fetchone()
                if m:
                    md = _msg_dict(c, m, names)
                    d["last_message"] = f"{md['from']}: " + (f"[{md['type']}] " if md["type"] != "text" else "") + (md["content"] or "")[:120]
            out.append(_clean(d) | {"jid": jid})
    return out


def search_contacts(query: str, limit: int = 30) -> List[dict]:
    """Find people by name (saved contact name, their WhatsApp profile name, business name) or phone digits.
    Returns phone, JID, LID and whether you have a chat with them."""
    q = f"%{query}%"
    digits = "".join(ch for ch in query if ch.isdigit())
    with _db() as c:
        rows = c.execute(
            """SELECT their_jid, full_name, push_name, business_name FROM dev.whatsmeow_contacts
               WHERE LOWER(full_name) LIKE LOWER(?) OR LOWER(push_name) LIKE LOWER(?) OR LOWER(business_name) LIKE LOWER(?)
                  OR (? != '' AND their_jid LIKE ?)
               ORDER BY full_name = '', full_name, push_name LIMIT ?""",
            (q, q, q, digits, f"%{digits}%", limit * 2)).fetchall()
        names, out, seen = _Names(c), [], set()
        for r in rows:
            w = names.who(r[0])
            key = w["phone"] or w["lid"]
            if key in seen:
                continue
            seen.add(key)
            jid = f"{w['phone']}@s.whatsapp.net" if w["phone"] else r[0]
            has_chat = c.execute("SELECT 1 FROM chats WHERE jid IN (?, ?)", (jid, f"{w['lid']}@lid")).fetchone() is not None
            out.append(_clean({"name": w["name"], "saved_name": r[1], "push_name": r[2], "business_name": r[3],
                               "phone": w["phone"], "jid": jid, "lid": w["lid"], "has_chat": has_chat}))
        if len(out) < limit:  # chats whose names never reached the contact store
            for r in c.execute("SELECT jid, name FROM chats WHERE (LOWER(name) LIKE LOWER(?) OR jid LIKE ?) AND jid NOT LIKE '%@g.us' LIMIT ?",
                               (q, q, limit)).fetchall():
                if r[0].split("@")[0] not in seen:
                    out.append({"name": r[1], "jid": r[0], "phone": r[0].split("@")[0], "has_chat": True})
        return out[:limit]


def list_calls(limit: int = 30, missed_only: bool = False) -> List[dict]:
    """Recent incoming calls seen by the linked device (voice/video, group, missed/answered/rejected)."""
    sql = "SELECT * FROM calls" + (" WHERE status = 'missed'" if missed_only else "") + " ORDER BY started_at DESC LIMIT ?"
    with _db() as c:
        names = _Names(c)
        return [_clean({"call_id": r["call_id"], "from": names.label(r["from_jid"]), "from_jid": r["from_jid"],
                        "video": bool(r["is_video"]), "group_jid": r["group_jid"], "status": r["status"],
                        "reason": r["reason"], "started_at": r["started_at"], "ended_at": r["ended_at"]})
                for r in c.execute(sql, (limit,)).fetchall()]


def get_group_activity(group_jid: str, limit: int = 50) -> List[dict]:
    """Membership and settings history of a group seen since the upgrade: who joined, left, was added/removed,
    promoted/demoted, name/description/settings changes, with who did it."""
    with _db() as c:
        names = _Names(c)
        out = []
        for r in c.execute("SELECT * FROM group_events WHERE group_jid = ? ORDER BY timestamp DESC LIMIT ?", (group_jid, limit)):
            out.append(_clean({"at": r["timestamp"], "event": r["kind"], "by": names.label(r["actor_jid"]) if r["actor_jid"] else None,
                               "who": [names.label(j) for j in json.loads(r["targets"])] if r["targets"] else None,
                               "detail": r["detail"]}))
        return out


def get_chat_stats(chat_jid: str, after: Optional[str] = None, top: int = 15) -> dict:
    """Statistics for one chat from stored messages: totals, messages per type, most active senders,
    first/last stored message, busiest days."""
    cond, params = "chat_jid = ?", [chat_jid]
    if after:
        cond += " AND timestamp > ?"; params.append(datetime.fromisoformat(after).isoformat(" "))
    with _db() as c:
        names = _Names(c)
        total, first, last = c.execute(f"SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM messages WHERE {cond}", params).fetchone()
        types = dict(c.execute(f"SELECT COALESCE(msg_type, NULLIF(media_type, ''), 'text'), COUNT(*) FROM messages WHERE {cond} GROUP BY 1 ORDER BY 2 DESC", params).fetchall())
        senders = c.execute(f"""SELECT CASE WHEN is_from_me THEN 'me' ELSE COALESCE(sender_jid, sender) END s, MAX(push_name), COUNT(*)
            FROM messages WHERE {cond} GROUP BY s ORDER BY 3 DESC LIMIT ?""", params + [top]).fetchall()
        days = c.execute(f"SELECT substr(timestamp,1,10) d, COUNT(*) FROM messages WHERE {cond} GROUP BY d ORDER BY 2 DESC LIMIT 5", params).fetchall()
        return {"total_messages": total, "first": first, "last": last, "by_type": types,
                "top_senders": [{"who": "Me" if s[0] == "me" else ("Unknown (old imported history)" if s[0] == chat_jid.split("@")[0]
                                 else names.label(s[0], s[1])), "messages": s[2]} for s in senders],
                "busiest_days": dict(days)}


# Legacy readers kept as-is.
get_chat = legacy.get_chat
get_direct_chat_by_contact = legacy.get_direct_chat_by_contact
get_contact_chats = legacy.get_contact_chats
get_last_interaction = legacy.get_last_interaction


# --------------------------------------------------------------------------- live read tools

def whatsapp_status() -> dict:
    """Connection state of the linked WhatsApp device, own number/name and how much is stored."""
    return _api("status")


def get_user_info(users: List[str], include_picture: bool = False) -> List[dict]:
    """Live profile info for phone numbers or JIDs: on WhatsApp or not, About text, LID/phone, saved contact
    names, business profile, last-seen/online if subscribed. include_picture also downloads the profile photo."""
    return _api("user/info", {"users": users, "picture": include_picture})


def list_groups(query: Optional[str] = None) -> List[dict]:
    """All groups and communities you're in (live from WhatsApp): name, description, member/admin counts,
    whether you're admin, settings, community links."""
    return _api("group/list", {"query": query or ""})


def get_group_info(group_jid: str, include_invite_link: bool = False) -> dict:
    """Full live info for a group/community: every participant with name, phone, admin/super-admin, you,
    settings (announce/locked/approval/add mode/disappearing), subgroups, pending join requests, invite link."""
    return _api("group/info", {"jid": group_jid, "invite_link": include_invite_link})


def get_invite_link_info(invite_link: str) -> dict:
    """Preview a group from its chat.whatsapp.com invite link without joining."""
    return _api("group/invite_info", {"link": invite_link})


def get_account_settings() -> dict:
    """Your privacy settings (last seen, online, profile photo, about, status, read receipts, groups add,
    calls) and blocked contacts."""
    return {"privacy": _api("privacy"), "blocked": _api("blocklist")}


def list_channels() -> List[dict]:
    """WhatsApp Channels (newsletters) you follow or own."""
    return _api("newsletter/list")


def get_channel(jid: Optional[str] = None, invite_link: Optional[str] = None, messages: int = 10,
                before_server_id: int = 0) -> dict:
    """A WhatsApp Channel's info and its latest posts (with views and reaction counts).
    Give its JID (…@newsletter) or invite link (whatsapp.com/channel/…)."""
    info = _api("newsletter/info", {"jid": jid or "", "invite": invite_link or ""})
    cj = jid or (info or {}).get("id")
    posts = _api("newsletter/messages", {"jid": cj, "count": messages, "before_server_id": before_server_id}) if cj and messages else []
    return {"info": info, "posts": posts}


def request_older_messages(chat_jid: str, count: int = 100) -> dict:
    """Ask the phone for older history of a chat (before the oldest stored message). Arrives within a minute
    if the phone is online; then use list_messages again."""
    return _api("history/request", {"chat_jid": chat_jid, "count": count})


def download_media(message_id: str, chat_jid: str) -> dict:
    """Download a message's image/video/voice/audio/document/sticker; returns the local file path."""
    DOWNLOAD_GUARD(message_id, chat_jid)
    return _api("download", {"message_id": message_id, "chat_jid": chat_jid})


# --------------------------------------------------------------------------- media helpers

def _ffmpeg(src: str, suffix: str, args: List[str]) -> str:
    fd, out = tempfile.mkstemp(prefix="wa-", suffix=suffix)
    os.close(fd)
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", src, *args, out], check=True, timeout=120)
    return out


def _as_jpeg(path: str) -> str:
    return _ffmpeg(path, ".jpg", ["-vf", "scale=640:640:force_original_aspect_ratio=increase,crop=640:640", "-frames:v", "1", "-q:v", "3"])


# --------------------------------------------------------------------------- actions
SEND_NOTE = "Acts AS THE ACCOUNT OWNER: only when the user explicitly asked for this exact action."


def send_message(recipient: str, message: str, reply_to_message_id: Optional[str] = None,
                 mentions: Optional[List[str]] = None) -> dict:
    """Send a text message. Acts AS THE ACCOUNT OWNER: only when the user explicitly asked to send this exact
    message to this recipient. Never on your own initiative.

    Args:
        recipient: phone number with country code (digits) or a JID (person …@s.whatsapp.net, group …@g.us)
        message: text (WhatsApp formatting: *bold* _italic_ ~strike~ ```mono```)
        reply_to_message_id: quote-reply to this message id (must be in the same chat)
        mentions: phones/JIDs to @mention; also write "@<phone digits>" in the text where the tag should appear
    """
    return _api("send", {"recipient": recipient, "message": message, "reply_to": reply_to_message_id or "",
                         "mentions": mentions or []})


def send_file(recipient: str, media_path: str, caption: str = "", reply_to_message_id: Optional[str] = None,
              view_once: bool = False, as_sticker: bool = False, as_document: bool = False,
              file_name: Optional[str] = None) -> dict:
    """Send an image, video, audio, document or sticker. Acts AS THE ACCOUNT OWNER: only on the user's explicit
    request for this recipient and file.

    Args:
        recipient: phone digits or JID
        media_path: absolute path of the file
        caption: caption for image/video/document
        reply_to_message_id: quote-reply to a message in the same chat
        view_once: send image/video as view-once
        as_sticker: send an image as a sticker (converted to 512x512 webp)
        as_document: send an image/video uncompressed as a document
        file_name: file name shown for documents
    """
    path = PATH_GUARD(media_path)
    if as_sticker and not path.lower().endswith(".webp"):
        path = _ffmpeg(path, ".webp", ["-vf", "scale=512:512:force_original_aspect_ratio=decrease,format=rgba,pad=512:512:-1:-1:color=0x00000000", "-frames:v", "1"])
    return _api("send", {"recipient": recipient, "message": caption, "media_path": path, "reply_to": reply_to_message_id or "",
                         "view_once": view_once, "as_sticker": as_sticker, "as_document": as_document, "file_name": file_name or ""})


def send_audio_message(recipient: str, media_path: str, reply_to_message_id: Optional[str] = None) -> dict:
    """Send any audio file as a voice note (converted to Opus .ogg). Acts AS THE ACCOUNT OWNER: only on explicit request."""
    path = PATH_GUARD(media_path)
    if not path.endswith(".ogg"):
        path = legacy.audio.convert_to_opus_ogg_temp(path)
    return _api("send", {"recipient": recipient, "media_path": path, "reply_to": reply_to_message_id or ""})


def send_poll(recipient: str, question: str, options: List[str], allow_multiple_answers: bool = False) -> dict:
    """Send a poll (2-12 options). Acts AS THE ACCOUNT OWNER: only on explicit request. Votes appear in
    get_message_details."""
    return _api("poll/send", {"recipient": recipient, "question": question, "options": options,
                              "selectable_count": 0 if allow_multiple_answers else 1})


def vote_poll(chat_jid: str, message_id: str, options: List[str]) -> dict:
    """Vote in a poll with the exact option names (empty list removes your vote). Only on explicit request."""
    return _api("poll/vote", {"chat_jid": chat_jid, "message_id": message_id, "options": options})


def send_location(recipient: str, latitude: float, longitude: float, name: str = "", address: str = "") -> dict:
    """Send a location pin. Acts AS THE ACCOUNT OWNER: only on explicit request."""
    return _api("location/send", {"recipient": recipient, "latitude": latitude, "longitude": longitude, "name": name, "address": address})


def send_contact_card(recipient: str, contact_name: str, contact_phone: str) -> dict:
    """Share a contact card (vCard). Acts AS THE ACCOUNT OWNER: only on explicit request."""
    return _api("contact/send", {"recipient": recipient, "name": contact_name, "phone": contact_phone})


def react_to_message(chat_jid: str, message_id: str, emoji: str) -> dict:
    """React to a message with an emoji; empty emoji removes your reaction. Only on explicit request."""
    return _api("react", {"chat_jid": chat_jid, "message_id": message_id, "emoji": emoji})


def edit_message(chat_jid: str, message_id: str, new_text: str) -> dict:
    """Edit one of your own text messages (WhatsApp allows ~15 minutes). Only on explicit request."""
    return _api("edit", {"chat_jid": chat_jid, "message_id": message_id, "text": new_text})


def delete_message_for_everyone(chat_jid: str, message_id: str) -> dict:
    """Delete a message for everyone: your own, or anyone's in a group where you're admin. Irreversible;
    only on explicit request."""
    return _api("revoke", {"chat_jid": chat_jid, "message_id": message_id})


def forward_message(chat_jid: str, message_id: str, to: List[str]) -> List[dict]:
    """Forward a stored message (text or media) to one or more chats, marked as forwarded. Only on explicit request."""
    return _api("forward", {"chat_jid": chat_jid, "message_id": message_id, "to": to})


def mark_chat_read(chat_jid: str) -> str:
    """Mark a chat's latest messages as read (sends blue ticks if your read receipts are on)."""
    return _api("read", {"chat_jid": chat_jid})


def set_typing(chat_jid: str, state: str = "typing") -> str:
    """Show typing… / recording… in a chat, or stop it. state: typing, recording, paused."""
    return _api("typing", {"chat_jid": chat_jid, "state": state})


def manage_chat(chat_jid: str, action: str, mute_hours: float = 0, disappearing_seconds: int = 0) -> str:
    """Chat-list actions, synced to your phone.

    Args:
        action: archive, unarchive, pin, unpin, mute, unmute, mark_read, mark_unread, delete (deletes the chat
                for you, irreversible), disappearing (set disappearing_seconds: 0 off, 86400, 604800, 7776000)
        mute_hours: for mute; 0 = always
    """
    return _api("chat/action", {"chat_jid": chat_jid, "action": action, "mute_hours": mute_hours, "seconds": disappearing_seconds})


def star_message(chat_jid: str, message_id: str, starred: bool = True) -> str:
    """Star or unstar a message."""
    return _api("star", {"chat_jid": chat_jid, "message_id": message_id, "starred": starred})


def create_group(name: str, participants: List[str], description: str = "", is_community: bool = False,
                 community_jid: Optional[str] = None, announce_only: bool = False, locked: bool = False,
                 join_approval: bool = False, disappearing_seconds: int = 0) -> dict:
    """Create a group or a community. Only on the owner's explicit request.

    Args:
        name: up to 25 characters
        participants: phone numbers/JIDs to add (can be empty). People whose privacy blocks adding come back
                      with error 403; add them later with update_group_participants(send_invites_if_blocked=True)
        description: group description
        is_community: create a Community (WhatsApp also creates its announcement group)
        community_jid: create the group inside this community
        announce_only: only admins can send messages
        locked: only admins can edit group info
        join_approval: admins must approve people joining by link
        disappearing_seconds: 0, 86400, 604800 or 7776000
    Returns the group (JID, participants with per-person add result) and its invite link.
    """
    return _api("group/create", {"name": name, "participants": participants, "description": description,
                                 "is_community": is_community, "community_jid": community_jid or "",
                                 "announce_only": announce_only, "locked": locked, "join_approval": join_approval,
                                 "disappearing_seconds": disappearing_seconds})


def update_group_participants(group_jid: str, action: str, participants: List[str],
                              send_invites_if_blocked: bool = False) -> List[dict]:
    """Add, remove, promote to admin, demote, or approve/reject join requests. Needs admin. Only on explicit request.
    Add people in small batches; adding many unknown numbers quickly can get the account restricted.

    Args:
        action: add, remove, promote, demote, approve, reject
        participants: phone numbers/JIDs
        send_invites_if_blocked: when someone's privacy blocks adding (error 403), send them a private group invite
    Returns per-person result (error codes explained in error_meaning).
    """
    return _api("group/participants", {"jid": group_jid, "action": action, "participants": participants,
                                       "send_invites": send_invites_if_blocked})


def update_group_settings(group_jid: str, name: Optional[str] = None, description: Optional[str] = None,
                          announce_only: Optional[bool] = None, locked: Optional[bool] = None,
                          join_approval: Optional[bool] = None, admins_add_only: Optional[bool] = None,
                          disappearing_seconds: Optional[int] = None, photo_path: Optional[str] = None) -> dict:
    """Change group/community settings; pass only what should change. Needs admin. Only on explicit request.

    Args:
        name, description: new texts
        announce_only: only admins can send messages
        locked: only admins can edit info
        join_approval: approve new members joining by link
        admins_add_only: only admins can add members
        disappearing_seconds: 0 off, 86400, 604800, 7776000
        photo_path: image file for the group photo ("" removes it)
    """
    payload: Dict[str, Any] = {"jid": group_jid}
    for k, v in (("name", name), ("description", description), ("announce_only", announce_only), ("locked", locked),
                 ("join_approval", join_approval), ("admins_add_only", admins_add_only),
                 ("disappearing_seconds", disappearing_seconds)):
        if v is not None:
            payload[k] = v
    if photo_path is not None:
        payload["photo_path"] = _as_jpeg(PATH_GUARD(photo_path)) if photo_path else ""
    return _api("group/settings", payload)


def get_group_invite_link(group_jid: str, reset: bool = False) -> dict:
    """Get the group's invite link; reset=True revokes the old link and makes a new one."""
    return _api("group/invite_link", {"jid": group_jid, "reset": reset})


def join_group(invite_link: str) -> dict:
    """Join a group by invite link. Only on explicit request."""
    return _api("group/join", {"link": invite_link})


def leave_group(group_jid: str) -> str:
    """Leave a group. Only on explicit request."""
    return _api("group/leave", {"jid": group_jid})


def link_group_to_community(community_jid: str, group_jid: str, unlink: bool = False) -> str:
    """Add an existing group to a community you admin, or remove it (unlink=True). Only on explicit request."""
    return _api("community/link", {"community_jid": community_jid, "group_jid": group_jid, "unlink": unlink})


def block_contact(jid_or_phone: str, block: bool = True) -> dict:
    """Block or unblock someone. Only on explicit request."""
    return _api("block", {"jid": jid_or_phone, "block": block})


def set_privacy_setting(setting: str, value: str) -> dict:
    """Change one privacy setting. Only on explicit request.
    setting/value: last (all/contacts/contact_blacklist/none), online (all/match_last_seen),
    profile, status (all/contacts/contact_blacklist/none), readreceipts (all/none),
    groupadd (all/contacts/contact_blacklist/none), calladd (all/known), messages (all/contacts)."""
    return _api("privacy", {"name": setting, "value": value})


def update_my_profile(about: Optional[str] = None, push_name: Optional[str] = None,
                      photo_path: Optional[str] = None) -> dict:
    """Change your About text, display name or profile photo ("" removes it). Only on explicit request."""
    payload: Dict[str, Any] = {}
    if about is not None:
        payload["about"] = about
    if push_name is not None:
        payload["push_name"] = push_name
    if photo_path is not None:
        payload["photo_path"] = _as_jpeg(PATH_GUARD(photo_path)) if photo_path else ""
    return _api("profile", payload)


def post_status(text: str = "", media_path: Optional[str] = None) -> dict:
    """Post a Status (story): text, or an image/video with text as caption. Only on explicit request."""
    payload = {"message": text}
    if media_path:
        payload["media_path"] = PATH_GUARD(media_path)
    return _api("status/post", payload)


def reject_call(call_id: str, caller_jid: str) -> str:
    """Reject a ringing incoming call (ids from list_calls)."""
    return _api("call/reject", {"call_id": call_id, "from": caller_jid})


def subscribe_presence(jid_or_phone: str) -> str:
    """Start tracking someone's online/last-seen and typing (shown later in get_user_info)."""
    return _api("presence/subscribe", {"jid": jid_or_phone})


def follow_channel(jid: str, follow: bool = True) -> str:
    """Follow or unfollow a WhatsApp Channel. Only on explicit request."""
    return _api("newsletter/follow", {"jid": jid, "follow": follow})


# --------------------------------------------------------------------------- registry
READ = [
    whatsapp_status, list_chats, list_messages, get_message_context, get_message_details, search_contacts,
    get_chat, get_direct_chat_by_contact, get_contact_chats, get_last_interaction, get_chat_stats, list_calls,
    get_group_activity, get_user_info, list_groups, get_group_info, get_invite_link_info, get_account_settings,
    list_channels, get_channel,
]
WRITE = [  # changes local/remote state but not visible to other people
    download_media, request_older_messages, mark_chat_read, set_typing, manage_chat, star_message,
    subscribe_presence, get_group_invite_link,
]
DESTRUCTIVE = [  # visible to others or hard to undo
    send_message, send_file, send_audio_message, send_poll, vote_poll, send_location, send_contact_card,
    react_to_message, edit_message, delete_message_for_everyone, forward_message, create_group,
    update_group_participants, update_group_settings, join_group, leave_group, link_group_to_community,
    block_contact, set_privacy_setting, update_my_profile, post_status, reject_call, follow_channel,
]


def register(server, annotations_factory: Callable[[str], Any]) -> None:
    """Register every tool on a FastMCP-style server. annotations_factory(kind) builds the hint object."""
    for kind, fns in (("read", READ), ("write", WRITE), ("destructive", DESTRUCTIVE)):
        for fn in fns:
            server.tool(name=fn.__name__, annotations=annotations_factory(kind))(fn)
