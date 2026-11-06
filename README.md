# t3code-poll-bridge

Use [T3 Code](https://github.com/pingdotgg/t3code) in a browser on networks that block WebSockets and streamed responses.

T3 Code's web UI talks to its server over one WebSocket. On a network with a proxy that blocks upgrades, the UI loads and then never connects. This bridge sits in front of a running T3 Code server and carries that socket over plain HTTP long-polling instead. Each request is short and completes normally, so buffering proxies let it through.

```
browser ── HTTPS (long-poll) ──▶ bridge ── WebSocket ──▶ T3 Code server (127.0.0.1:3773)
```

- **No fork, no rebuild.** The bridge serves the server's own web UI and injects `poll-websocket.js` into the page. That script swaps T3 Code's RPC socket (`new WebSocket("/ws")`) for long-polling. Other WebSockets are left alone.
- **Same instance.** You see the same threads as the desktop app and your other devices.
- **No pairing in the browser.** The bridge signs in to the server once and adds that session to every request. Whatever protects the bridge (Cloudflare Access below) is what protects T3 Code. Never expose it without that.

## Setup

Requires Node 24+ on the machine running T3 Code. No dependencies.

1. Make a one-time pairing token on that machine and give it to the bridge:

   ```sh
   npx t3 pair --base-dir ~/.t3 --label "t3code-poll-bridge"
   node bridge.ts pair '<pairing url or token>'
   ```

   The session is saved to `~/.config/t3-bridge/session.json` (mode 600). It shows up in T3 Code under Settings → Connections, where you can revoke it.

2. Run it:

   ```sh
   PUBLIC_HOSTS=t3.example.com PORT=3781 node bridge.ts
   ```

   | Variable | Default | |
   | --- | --- | --- |
   | `T3_UPSTREAM` | `http://127.0.0.1:3773` | T3 Code server |
   | `PORT` | `3780` | Listens on 127.0.0.1 only |
   | `PUBLIC_HOSTS` | | Comma-separated hostnames to accept besides localhost |
   | `T3_BRIDGE_SESSION_FILE` | `~/.config/t3-bridge/session.json` | |

3. Publish it through a Cloudflare tunnel and put a Cloudflare Access application with an email allowlist on the hostname. `deploy/` has a cloudflared config and systemd user units:

   ```sh
   cloudflared tunnel create t3code-poll-bridge
   cloudflared tunnel route dns t3code-poll-bridge t3.example.com
   cp deploy/cloudflared.yml ~/.cloudflared/t3code-poll-bridge.yml   # fill in the tunnel id
   cp deploy/*.service ~/.config/systemd/user/
   systemctl --user enable --now t3code-poll-bridge t3code-poll-bridge-tunnel
   loginctl enable-linger "$USER"
   ```

   Then check that `https://t3.example.com` redirects to your Access login.

## What doesn't work

- **Threads on other machines.** The browser connects to linked machines directly, and those connections are WebSockets. Only the bridged machine's threads show up.
- **Browser previews and device streams.** These use their own WebSockets.
- **An expired Access session** stops the polling. Reload the page.

## Protocol

| Request | |
| --- | --- |
| `GET /ws/poll/open?<same query as /ws>` | Opens the upstream WebSocket and returns `{ id }` |
| `GET /ws/poll/:id?ack=N` | Waits up to 20 s for frames and returns `{ seq, frames, closed? }`. Frames before `ack` are dropped. |
| `POST /ws/poll/:id` `{ seq, frames }` | Sends frames. Frames already seen (by `seq`) are skipped, so retries are safe. |
| `DELETE /ws/poll/:id` | Closes the connection. A connection with no polls for 50 s closes on its own. |
