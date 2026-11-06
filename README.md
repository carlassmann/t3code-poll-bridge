# t3code-poll-bridge

Use [T3 Code](https://github.com/pingdotgg/t3code)'s web UI on networks that block WebSockets.

The bridge proxies a running T3 Code server and injects a script that carries the UI's WebSocket over HTTP long-polling. No fork, no rebuild. It signs in to the server once and shares that session, so it only answers requests signed by Cloudflare Access.

```sh
t3 pair --base-dir ~/.t3                      # on the T3 Code machine
node bridge.ts pair '<pairing url or token>'
PUBLIC_HOSTS=t3.example.com PORT=3781 \
  CF_ACCESS_TEAM_DOMAIN=<team>.cloudflareaccess.com CF_ACCESS_AUD=<application audience tag> \
  node bridge.ts
```

The audience tag is on the Access application's overview page (or the `kid` in its login redirect).

Node 24+, no dependencies. Also: `T3_UPSTREAM` (default `http://127.0.0.1:3773`), `T3_BRIDGE_SESSION_FILE`. Without the `CF_ACCESS_*` variables, anyone who reaches the port gets T3 Code. `deploy/` has a cloudflared config and systemd user units.

Not bridged: other linked machines, browser previews, device streams.
