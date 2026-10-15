#!/usr/bin/env bash
# pair.sh — link your WhatsApp to the bridge running as a systemd service (see install.sh --systemd).
#   ./setup/pair.sh                    print the QR in this terminal (refreshes every ~20s) until linked
#   ./setup/pair.sh --phone 2010XXXXXXXX   print an 8-char code instead: WhatsApp → Linked devices →
#                                       Link a device → "Link with phone number instead"
#   ./setup/pair.sh status             linked or not
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
STORE=$ROOT/whatsapp-bridge/store
DROPIN=/etc/systemd/system/whatsapp-bridge.service.d/pair.conf

# linked = whatsmeow saved a device identity
linked() { python3 -c "import sqlite3,sys; c=sqlite3.connect('file:$STORE/whatsapp.db?mode=ro',uri=True); sys.exit(0 if c.execute('select count(*) from whatsmeow_device').fetchone()[0] else 1)" 2>/dev/null; }

if [[ "${1:-}" == status ]]; then linked && echo linked || { echo "NOT linked"; exit 1; }; exit 0; fi
linked && { echo "already linked (to re-link: log the device out from your phone, then rerun)"; exit 0; }

rm -f "$STORE/qr.txt" "$STORE/pair.txt"
if [[ "${1:-}" == --phone ]]; then
  phone=${2//[^0-9]/}
  [[ -n "$phone" ]] || { echo "usage: $0 --phone <international number, digits only>"; exit 2; }
  sudo mkdir -p "$(dirname $DROPIN)"
  printf '[Service]\nEnvironment=WA_PAIR_PHONE=%s\n' "$phone" | sudo tee $DROPIN >/dev/null
fi
cleanup() { sudo rm -f $DROPIN; sudo systemctl daemon-reload; }
trap cleanup EXIT
sudo systemctl daemon-reload; sudo systemctl restart whatsapp-bridge

if [[ "${1:-}" == --phone ]]; then
  for _ in $(seq 30); do [[ -s $STORE/pair.txt ]] && break; sleep 1; done
  [[ -s $STORE/pair.txt ]] || { echo "no pairing code — see: journalctl -u whatsapp-bridge -n 50"; exit 1; }
  echo "Pairing code: $(cat $STORE/pair.txt)   (valid ~2 minutes)"
else
  last=""
  for _ in $(seq 180); do
    linked && break
    if [[ -s $STORE/qr.txt && "$(cat $STORE/qr.txt)" != "$last" ]]; then
      last=$(cat $STORE/qr.txt); clear
      uv run -q --with qrcode python -c "import qrcode,sys; q=qrcode.QRCode(border=2); q.add_data(sys.argv[1]); q.print_ascii(invert=True)" "$last"
      echo "Scan with WhatsApp → Linked devices → Link a device"
    fi
    sleep 1
  done
fi

for _ in $(seq 180); do linked && break; sleep 1; done
linked && echo "LINKED ✓" || { echo "not linked yet — rerun for a fresh code"; exit 1; }
