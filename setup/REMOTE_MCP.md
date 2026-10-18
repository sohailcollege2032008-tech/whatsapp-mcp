# Private remote WhatsApp MCP

The HTTP companion leaves the existing stdio server and linked-device bridge
unchanged. Install `whatsapp-mcp-server/requirements-http.txt` in a separate
environment; `setup/whatsapp-mcp-http.service` records the deployed locations.

The companion listens only on loopback. Caddy forwards `/whatsapp/*` on
`tgu.sohail-remote.top` to it. The only MCP endpoint is
`https://tgu.sohail-remote.top/whatsapp/<private-token>`; missing or incorrect
tokens return 404. The credential is read from a mode-600 file named by
`WHATSAPP_MCP_TOKEN_FILE`. Never commit, print, or log the complete URL.
The unit contains only the credential file path, and access logs are disabled.

The interface exposes the eight existing query tools, message sending, media
download, file sending, and audio sending. Remote file sending accepts only
files inside a downloaded-media chat folder. Remote voice messages accept
`.ogg` files only; other audio can be sent using `send_file`. Download validates
the stored filename and chat path before calling the bridge. Existing local
tools retain their original behavior. Sending always requires an explicit
instruction from the account owner.

Run `python -m unittest -v test_http_server` from `whatsapp-mcp-server` with
the HTTP environment. Tests cover the MCP handshake and schemas, denial of
incorrect routes, file/symlink escapes, and malicious download filenames.
All sending is mocked. Live acceptance uses public HTTPS initialization,
tool enumeration, one read, and negative authentication checks. It does not
send real messages or download personal media.

This Contabo service is registered as a seven-day trial under the server
policy. A permanent deployment belongs on med-vps; the existing linked-device
session must not be copied or moved as part of that migration without approval.
