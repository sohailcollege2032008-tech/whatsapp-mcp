"""Transport and file-scope regression checks; never send real messages."""
import tempfile
import unittest
import sqlite3
from pathlib import Path
from unittest.mock import patch

from starlette.testclient import TestClient
import http_server


class HttpTransportTest(unittest.TestCase):
    def test_private_transport(self):
        app = http_server.build_server().http_app(
            path="/whatsapp/test-token", stateless_http=True, json_response=True,
        )
        headers = {"Accept": "application/json, text/event-stream"}
        with TestClient(app) as client:
            def rpc(method, params=None):
                return client.post("/whatsapp/test-token", headers=headers, json={
                    "jsonrpc": "2.0", "id": 1, "method": method,
                    "params": params or {},
                })

            response = rpc("initialize", {
                "protocolVersion": "2024-11-05", "capabilities": {},
                "clientInfo": {"name": "transport-test", "version": "1"},
            })
            self.assertEqual(response.status_code, 200)
            self.assertIn("serverInfo", response.json()["result"])
            tools = rpc("tools/list").json()["result"]["tools"]
            self.assertEqual({t["name"] for t in tools}, set(http_server.READ_TOOLS) | {
                "send_message", "send_file", "send_audio_message", "download_media",
            })
            for tool in tools:
                self.assertIn("inputSchema", tool)
            with patch.object(http_server.whatsapp, "send_file") as sender:
                result = rpc("tools/call", {
                    "name": "send_file", "arguments": {
                        "recipient": "test", "media_path": "/etc/passwd",
                    },
                }).json()["result"]
                self.assertTrue(result["isError"])
                sender.assert_not_called()
            for path in ("/whatsapp/wrong-token", "/whatsapp", "/mcp", "/unrelated"):
                self.assertEqual(client.post(path, headers=headers, json={}).status_code, 404)

    def test_media_scope(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "store"
            chat = root / "chat"
            chat.mkdir(parents=True)
            media = chat / "document.pdf"
            media.write_bytes(b"test")
            database = root / "whatsapp.db"
            database.write_bytes(b"private")
            outside = Path(tmp) / "secret.env"
            outside.write_bytes(b"private")
            link = chat / "escape.pdf"
            link.symlink_to(outside)
            with patch.object(http_server, "MEDIA_ROOT", root):
                self.assertEqual(http_server.media_file(str(media)), str(media))
                for path in (database, outside, link, chat, chat / "missing.pdf"):
                    with self.subTest(path=path.name), self.assertRaises(ValueError):
                        http_server.media_file(str(path))

    def test_download_paths_before_bridge_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            database = root / "messages.db"
            with sqlite3.connect(database) as db:
                db.execute("CREATE TABLE messages (id TEXT, chat_jid TEXT, filename TEXT)")
                for key, name in (("safe", "document.pdf"), ("traversal", "../secret"), ("absolute", "/secret"), ("windows", "..\\secret")):
                    db.execute("INSERT INTO messages VALUES (?, ?, ?)", (key, "chat", name))
            with patch.object(http_server, "MEDIA_ROOT", root), patch.object(http_server.whatsapp, "MESSAGES_DB_PATH", str(database)):
                self.assertEqual(http_server.download_target("safe", "chat"), root / "chat/document.pdf")
                for key in ("traversal", "absolute", "windows", "missing"):
                    with self.subTest(key=key), self.assertRaises(ValueError):
                        http_server.download_target(key, "chat")
                for chat in ("..", "../escape", "x/y", "x\\y"):
                    with self.subTest(chat=chat), self.assertRaises(ValueError):
                        http_server.download_target("safe", chat)


if __name__ == "__main__":
    unittest.main()
