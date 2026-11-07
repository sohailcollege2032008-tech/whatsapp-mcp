"""Private HTTP transport for the WhatsApp MCP; same tools as main.py with file-path guards."""
from __future__ import annotations

import os
import re
import sqlite3
from pathlib import Path

from fastmcp import FastMCP
from fastmcp.server.dependencies import get_http_request
import whatsapp
import wa_tools
from mcp.types import ToolAnnotations

MEDIA_ROOT = Path(whatsapp.MESSAGES_DB_PATH).resolve().parent
LOCAL_ROOTS = (Path.home() / "workspace", Path("/tmp"))


def local_agent() -> bool:
    """True for agents on this box hitting loopback; Caddy always adds X-Forwarded-For."""
    try:
        headers = get_http_request().headers
    except RuntimeError:
        return False
    return "x-forwarded-for" not in headers and headers.get("host", "").split(":")[0] == "127.0.0.1"


def media_file(media_path: str) -> str:
    """Remote callers may send downloaded media, never arbitrary server files."""
    path = Path(media_path).resolve()
    if local_agent() and path.is_file() and any(path.is_relative_to(root) for root in LOCAL_ROOTS):
        return str(path)
    try:
        relative = path.relative_to(MEDIA_ROOT)
    except ValueError:
        raise ValueError("Only downloaded WhatsApp media files may be sent") from None
    if len(relative.parts) < 2 or not path.is_file():
        raise ValueError("A downloaded WhatsApp media file is required")
    return str(path)


def build_server() -> FastMCP:
    server = FastMCP("WhatsApp personal account")
    wa_tools.PATH_GUARD = media_file
    wa_tools.DOWNLOAD_GUARD = download_target
    wa_tools.register(server, lambda kind: ToolAnnotations(
        readOnlyHint=kind == "read", destructiveHint=kind == "destructive", openWorldHint=True))
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
        allowed_hosts=["tgu.sohail-remote.top", "127.0.0.1:*"],
        show_banner=False, log_level="warning",
        uvicorn_config={"access_log": False},
    )


if __name__ == "__main__":
    main()
