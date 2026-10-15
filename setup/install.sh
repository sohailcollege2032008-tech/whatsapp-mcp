#!/usr/bin/env bash
# install.sh — build the WhatsApp bridge + MCP server, optionally install a systemd service.
#   ./setup/install.sh                 build only (run the bridge yourself: cd whatsapp-bridge && ./whatsapp-bridge)
#   ./setup/install.sh --systemd       build + install/enable whatsapp-bridge.service for the current user (Linux, needs sudo)
#   PORT=8090 ./setup/install.sh ...   REST port of the bridge (loopback only), default 8090
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PORT=${PORT:-8090}

need() { command -v "$1" >/dev/null || { echo "missing: $1 — $2"; exit 1; }; }
need go  "install Go ≥1.21 (https://go.dev/dl). A newer toolchain is fetched automatically."
need gcc "install a C compiler (Debian/Ubuntu: sudo apt install build-essential) — sqlite needs cgo."
need uv  "install uv: curl -LsSf https://astral.sh/uv/install.sh | sh"
command -v ffmpeg >/dev/null || echo "note: ffmpeg not found — only needed to send non-.ogg audio as voice notes."

echo "== building bridge (1–3 min, ~400MB RAM)"
cd "$ROOT/whatsapp-bridge"
CGO_ENABLED=1 GOTOOLCHAIN=auto nice -n 15 go build -o whatsapp-bridge.new .
mv whatsapp-bridge.new whatsapp-bridge
mkdir -p store && chmod 700 store

echo "== installing MCP server deps"
cd "$ROOT/whatsapp-mcp-server" && uv sync -q

if [[ "${1:-}" == --systemd ]]; then
  echo "== installing systemd service"
  sed -e "s|@USER@|$(id -un)|g" -e "s|@DIR@|$ROOT/whatsapp-bridge|g" -e "s|@PORT@|$PORT|g" \
    "$ROOT/setup/whatsapp-bridge.service" | sudo tee /etc/systemd/system/whatsapp-bridge.service >/dev/null
  sudo systemctl daemon-reload
  sudo systemctl enable --now whatsapp-bridge
  echo "service started. Link your phone: ./setup/pair.sh   (or ./setup/pair.sh --phone <international number>)"
else
  echo "built. Start it and scan the QR shown in the terminal:"
  echo "  cd $ROOT/whatsapp-bridge && WA_BRIDGE_PORT=$PORT ./whatsapp-bridge"
  echo "  (no camera? WA_PAIR_PHONE=<international number> ./whatsapp-bridge prints an 8-char pairing code)"
fi

cat <<EOF

== MCP config (add to your client — Claude Code: ~/.claude.json "mcpServers", Claude Desktop, Cursor, OpenCode...)
"whatsapp": {
  "command": "$ROOT/whatsapp-mcp-server/.venv/bin/python",
  "args": ["$ROOT/whatsapp-mcp-server/main.py"],
  "env": {"WA_BRIDGE_PORT": "$PORT"}
}
Claude Code one-liner:
  claude mcp add whatsapp -s user -e WA_BRIDGE_PORT=$PORT -- $ROOT/whatsapp-mcp-server/.venv/bin/python $ROOT/whatsapp-mcp-server/main.py
EOF
