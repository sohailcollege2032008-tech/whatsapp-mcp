"""Private remote transport; the existing stdio server stays unchanged."""
from __future__ import annotations

import os
import re
import sqlite3
from pathlib import Path

from fastmcp import FastMCP
import whatsapp

MEDIA_ROOT = Path(whatsapp.MESSAGES_DB_PATH).resolve().parent
READ_TOOLS = (
    "search_contacts", "list_messages", "list_chats", "get_chat",
    "get_direct_chat_by_contact", "get_contact_chats", "get_last_interaction",
    "get_message_context",
)


def media_file(media_path: str) -> str:
    """Remote callers may send downloaded media, never arbitrary server files."""
    path = Path(media_path).resolve()
    try:
        relative = path.relative_to(MEDIA_ROOT)
    except ValueError:
        raise ValueError("Only downloaded WhatsApp media files may be sent") from None
    if len(relative.parts) < 2 or not path.is_file():
        raise ValueError("A downloaded WhatsApp media file is required")
    return str(path)


def build_server() -> FastMCP:
    server = FastMCP("WhatsApp personal account")
    for name in READ_TOOLS:
        server.tool(getattr(whatsapp, name), annotations={"readOnlyHint": True})

    @server.tool(annotations={"readOnlyHint": False, "destructiveHint": True})
    def send_message(recipient: str, message: str) -> dict:
        """Send as the account owner ONLY on their explicit request for this recipient and message."""
        success, status = whatsapp.send_message(recipient, message)
        return {"success": success, "message": status}

    @server.tool(annotations={"readOnlyHint": False, "destructiveHint": True})
    def send_file(recipient: str, media_path: str) -> dict:
        """Send downloaded WhatsApp media ONLY on the owner's explicit request for this recipient and file."""
        success, status = whatsapp.send_file(recipient, media_file(media_path))
        return {"success": success, "message": status}

    @server.tool(annotations={"readOnlyHint": False, "destructiveHint": True})
    def send_audio_message(recipient: str, media_path: str) -> dict:
        """Send downloaded .ogg WhatsApp audio ONLY on the owner's explicit request. Use send_file for other formats."""
        path = media_file(media_path)
        if not path.endswith(".ogg"):
            raise ValueError("Remote voice messages require .ogg; use send_file for other formats")
        success, status = whatsapp.send_audio_message(recipient, path)
        return {"success": success, "message": status}

    @server.tool(annotations={"readOnlyHint": False})
    def download_media(message_id: str, chat_jid: str) -> dict:
        """Download media from a WhatsApp message and return its server media path."""
        expected = download_target(message_id, chat_jid)
        path = whatsapp.download_media(message_id, chat_jid)
        if path:
            if Path(path).resolve() != expected:
                raise ValueError("Unexpected media download path")
            return {"success": True, "file_path": path}
        return {"success": False, "message": "Media download failed"}

    return server


def download_target(message_id: str, chat_jid: str) -> Path:
    """Validate stored WhatsApp filenames before the bridge can write them."""
    if not re.fullmatch(r"[A-Za-z0-9_:@.-]+", chat_jid) or chat_jid in (".", ".."):
        raise ValueError("Invalid chat identifier")
    with sqlite3.connect(Path(whatsapp.MESSAGES_DB_PATH).resolve().as_uri() + "?mode=ro", uri=True) as db:
        row = db.execute(
            "SELECT filename FROM messages WHERE id = ? AND chat_jid = ?",
            (message_id, chat_jid),
        ).fetchone()
    if not row or not row[0] or row[0] in (".", "..") or any(c in row[0] for c in ("/", "\\", "\x00")):
        raise ValueError("Missing or unsafe media filename")
    chat = (MEDIA_ROOT / chat_jid.replace(":", "_")).resolve()
    target = (chat / row[0]).resolve()
    if chat.parent != MEDIA_ROOT or target.parent != chat:
        raise ValueError("Media download must stay inside its chat directory")
    return target


def main() -> None:
    token = Path(os.environ["WHATSAPP_MCP_TOKEN_FILE"]).read_text().strip()
    if not re.fullmatch(r"[a-f0-9]{64}", token):
        raise SystemExit("Invalid WhatsApp MCP token file")
    build_server().run(
        transport="http", host="127.0.0.1", port=9450,
        path=f"/whatsapp/{token}", stateless_http=True, json_response=True,
        allowed_hosts=["tgu.sohail-remote.top"],
        show_banner=False, log_level="warning",
        uvicorn_config={"access_log": False},
    )


if __name__ == "__main__":
    main()
