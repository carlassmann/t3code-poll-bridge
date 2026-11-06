# t3code-poll-bridge

Use [T3 Code](https://github.com/pingdotgg/t3code)'s web UI on networks that block WebSockets.

The bridge proxies a running T3 Code server and injects a script that carries the UI's WebSocket over HTTP long-polling. No fork, no rebuild. It signs in to the server once, so put it behind Cloudflare Access (or similar).

```sh
t3 pair --base-dir ~/.t3                      # on the T3 Code machine
node bridge.ts pair '<pairing url or token>'
PUBLIC_HOSTS=t3.example.com PORT=3781 node bridge.ts
```

Node 24+, no dependencies. Env: `T3_UPSTREAM` (default `http://127.0.0.1:3773`), `PORT`, `PUBLIC_HOSTS`, `T3_BRIDGE_SESSION_FILE`. `deploy/` has a cloudflared config and systemd user units.

Not bridged: other linked machines, browser previews, device streams.
