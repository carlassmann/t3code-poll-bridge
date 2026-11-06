// Serves a running T3 Code server's web UI to browsers that cannot open
// WebSockets or streamed responses. Pages get poll-websocket.js, which carries
// the RPC socket over HTTP long-polling; the bridge relays it to the server's
// real WebSocket. Only the bridge talks to that server; it signs in once and
// adds the session cookie to every request, so whatever guards this bridge
// (Cloudflare Access) guards T3 Code.
import { randomUUID } from "node:crypto";
import { chmodSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { Readable } from "node:stream";

const UPSTREAM = new URL(process.env.T3_UPSTREAM ?? "http://127.0.0.1:3773");
const PORT = Number(process.env.PORT ?? 3780);
const SESSION_FILE =
  process.env.T3_BRIDGE_SESSION_FILE ?? join(homedir(), ".config", "t3-bridge", "session.json");
const PUBLIC_HOSTS = new Set(
  (process.env.PUBLIC_HOSTS ?? "")
    .split(",")
    .map((host) => host.trim().toLowerCase())
    .filter(Boolean),
);

const SHIM_PATH = "/__t3code-poll-bridge/poll-websocket.js";
const SHIM_SOURCE = readFileSync(new URL("./poll-websocket.js", import.meta.url));
const POLL_PATH = "/ws/poll";
const POLL_WAIT_MS = 20_000;
// A client that stops polling for this long is gone; its upstream socket closes.
const POLL_IDLE_CLOSE_MS = POLL_WAIT_MS + 30_000;
const UPSTREAM_OPEN_TIMEOUT_MS = 15_000;

// ---------- session ----------

function readSessionCookie(): string {
  try {
    return (JSON.parse(readFileSync(SESSION_FILE, "utf8")) as { cookie: string }).cookie;
  } catch {
    console.error(`No session at ${SESSION_FILE}. Run: node bridge.ts pair <pairing-url>`);
    process.exit(1);
  }
}

async function pair(pairingUrlOrToken: string) {
  const credential = pairingUrlOrToken.includes("token=")
    ? new URL(pairingUrlOrToken).hash.replace(/^#/, "").split("&").find((part) => part.startsWith("token="))!.slice(6)
    : pairingUrlOrToken;
  const response = await fetch(new URL("/api/auth/browser-session", UPSTREAM), {
    method: "POST",
    headers: { "content-type": "application/json", origin: UPSTREAM.origin, "user-agent": "t3-bridge" },
    body: JSON.stringify({ credential }),
  });
  if (!response.ok) throw new Error(`Pairing failed: ${response.status} ${await response.text()}`);
  const cookie = response.headers
    .getSetCookie()
    .map((header) => header.split(";")[0]!)
    .find((pair) => pair.startsWith("t3_session") && !pair.endsWith("="));
  if (cookie === undefined) throw new Error("Server answered without a session cookie.");
  mkdirSync(dirname(SESSION_FILE), { recursive: true });
  writeFileSync(SESSION_FILE, JSON.stringify({ cookie }), { mode: 0o600 });
  chmodSync(SESSION_FILE, 0o600);
  const result = (await response.json()) as { expiresAt: string };
  console.log(`Signed in. Session saved to ${SESSION_FILE}, expires ${result.expiresAt}.`);
}

// ---------- request guards ----------

function isAllowedHost(request: IncomingMessage): boolean {
  const host = (request.headers.host ?? "").toLowerCase().replace(/:\d+$/, "");
  return host === "localhost" || host === "127.0.0.1" || host === "[::1]" || PUBLIC_HOSTS.has(host);
}

// Blocks cross-site requests and DNS rebinding; authentication is Cloudflare Access's job.
function hasSameOrigin(request: IncomingMessage): boolean {
  const origin = request.headers.origin;
  if (origin === undefined) return true;
  try {
    return new URL(origin).host === request.headers.host;
  } catch {
    return false;
  }
}

// ---------- poll shim ----------

function serveShim(response: ServerResponse) {
  response.writeHead(200, { "content-type": "text/javascript; charset=utf-8", "cache-control": "no-cache" });
  response.end(SHIM_SOURCE);
}

// A classic script in <head> runs before the app's module scripts create the socket.
function withShim(html: string): string {
  return html.replace(/<head[^>]*>/i, (head) => `${head}<script src="${SHIM_PATH}"></script>`);
}

// ---------- HTTP proxy ----------

function upstreamHeaders(request: IncomingMessage, sessionCookie: string): Headers {
  const headers = new Headers();
  for (const [name, value] of Object.entries(request.headers)) {
    if (value === undefined || ["host", "cookie", "connection", "content-length"].includes(name)) continue;
    headers.set(name, Array.isArray(value) ? value.join(", ") : value);
  }
  headers.set("cookie", sessionCookie);
  // The server checks cookie sessions against its own origin.
  if (headers.has("origin")) headers.set("origin", UPSTREAM.origin);
  headers.delete("referer");
  return headers;
}

async function proxy(request: IncomingMessage, response: ServerResponse, sessionCookie: string) {
  const hasBody = request.method !== "GET" && request.method !== "HEAD";
  const upstream = await fetch(new URL(request.url ?? "/", UPSTREAM), {
    method: request.method,
    headers: upstreamHeaders(request, sessionCookie),
    ...(hasBody ? { body: Readable.toWeb(request) as ReadableStream, duplex: "half" } : {}),
    redirect: "manual",
  });
  const headers: Record<string, string | Array<string>> = {};
  upstream.headers.forEach((value, name) => {
    if (["content-encoding", "content-length", "transfer-encoding", "connection", "set-cookie"].includes(name)) return;
    headers[name] = value;
  });
  // The bridge owns the T3 session; the browser keeps only non-session cookies.
  const cookies = upstream.headers.getSetCookie().filter((cookie) => !cookie.startsWith("t3_session"));
  if (cookies.length > 0) headers["set-cookie"] = cookies;
  if (upstream.headers.get("content-type")?.startsWith("text/html")) {
    response.writeHead(upstream.status, headers);
    return response.end(withShim(await upstream.text()));
  }
  response.writeHead(upstream.status, headers);
  if (upstream.body === null) return response.end();
  Readable.fromWeb(upstream.body as never).pipe(response);
}

// ---------- long-poll ⇄ WebSocket ----------

class PollConnection {
  private outbound: Array<string> = [];
  private outboundSeq = 0;
  private nextInboundSeq = 0;
  private closed: { code: number; reason: string } | undefined;
  private readonly waiters = new Set<() => void>();
  private idleTimer: ReturnType<typeof setTimeout> | undefined;
  private readonly socket: WebSocket;
  private readonly onGone: () => void;

  constructor(socket: WebSocket, onGone: () => void) {
    this.socket = socket;
    this.onGone = onGone;
    socket.addEventListener("message", (event) => {
      this.outbound.push(String(event.data));
      this.wake();
    });
    socket.addEventListener("close", (event) => {
      this.closed ??= { code: event.code === 1005 ? 1000 : event.code, reason: event.reason };
      this.wake();
    });
    this.touch();
  }

  send(seq: number, frames: ReadonlyArray<string>) {
    this.touch();
    frames.forEach((frame, index) => {
      if (seq + index !== this.nextInboundSeq) return;
      this.nextInboundSeq += 1;
      if (this.socket.readyState === WebSocket.OPEN) this.socket.send(frame);
    });
  }

  async poll(ack: number, signal: AbortSignal) {
    this.touch();
    const acked = Math.min(Math.max(ack - this.outboundSeq, 0), this.outbound.length);
    this.outbound = this.outbound.slice(acked);
    this.outboundSeq += acked;
    if (this.outbound.length === 0 && this.closed === undefined) {
      await new Promise<void>((resolve) => {
        const done = () => {
          clearTimeout(timer);
          this.waiters.delete(done);
          signal.removeEventListener("abort", done);
          resolve();
        };
        const timer = setTimeout(done, POLL_WAIT_MS);
        this.waiters.add(done);
        signal.addEventListener("abort", done);
      });
    }
    this.touch();
    return {
      seq: this.outboundSeq,
      frames: this.outbound,
      ...(this.closed === undefined ? {} : { closed: this.closed }),
    };
  }

  close() {
    clearTimeout(this.idleTimer);
    this.socket.close();
    this.closed ??= { code: 1000, reason: "client closed" };
    this.wake();
    this.onGone();
  }

  private wake() {
    for (const waiter of [...this.waiters]) waiter();
  }

  private touch() {
    clearTimeout(this.idleTimer);
    this.idleTimer = setTimeout(() => this.close(), POLL_IDLE_CLOSE_MS);
  }
}

const connections = new Map<string, PollConnection>();

function openUpstreamSocket(search: string, sessionCookie: string): Promise<WebSocket> {
  const url = new URL(`/ws${search}`, UPSTREAM);
  url.protocol = UPSTREAM.protocol === "https:" ? "wss:" : "ws:";
  const socket = new WebSocket(url, {
    headers: { cookie: sessionCookie, origin: UPSTREAM.origin },
  } as never);
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      socket.close();
      reject(new Error("upstream open timed out"));
    }, UPSTREAM_OPEN_TIMEOUT_MS);
    socket.addEventListener("open", () => {
      clearTimeout(timer);
      resolve(socket);
    });
    socket.addEventListener("close", (event) => {
      clearTimeout(timer);
      reject(new Error(`upstream closed: ${event.code} ${event.reason}`));
    });
  });
}

function json(response: ServerResponse, status: number, body: unknown) {
  response.writeHead(status, { "content-type": "application/json", "cache-control": "no-store" });
  response.end(JSON.stringify(body));
}

async function readJson(request: IncomingMessage): Promise<unknown> {
  const chunks: Array<Buffer> = [];
  for await (const chunk of request) chunks.push(chunk as Buffer);
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

async function handlePoll(request: IncomingMessage, response: ServerResponse, url: URL, sessionCookie: string) {
  if (url.pathname === `${POLL_PATH}/open` && request.method === "GET") {
    try {
      const socket = await openUpstreamSocket(url.search, sessionCookie);
      const id = randomUUID();
      connections.set(id, new PollConnection(socket, () => connections.delete(id)));
      return json(response, 200, { id });
    } catch (error) {
      console.warn("[bridge] upstream socket failed:", (error as Error).message);
      return json(response, 502, { code: "upstream_socket_failed" });
    }
  }
  const connection = connections.get(decodeURIComponent(url.pathname.slice(POLL_PATH.length + 1)));
  if (connection === undefined) return json(response, 410, { code: "poll_socket_gone" });
  switch (request.method) {
    case "GET": {
      const abort = new AbortController();
      response.on("close", () => abort.abort());
      return json(response, 200, await connection.poll(Number(url.searchParams.get("ack") ?? 0) || 0, abort.signal));
    }
    case "POST": {
      const body = (await readJson(request)) as { seq: number; frames: Array<string> };
      if (!Number.isInteger(body.seq) || !Array.isArray(body.frames)) return json(response, 400, {});
      connection.send(body.seq, body.frames.map(String));
      response.writeHead(204).end();
      return;
    }
    case "DELETE":
      connection.close();
      response.writeHead(204).end();
      return;
  }
  json(response, 405, {});
}

// ---------- server ----------

function serve() {
  const sessionCookie = readSessionCookie();
  const server = createServer(async (request, response) => {
    try {
      if (!isAllowedHost(request)) return json(response, 421, { code: "host_not_allowed" });
      if (!hasSameOrigin(request)) return json(response, 403, { code: "cross_origin" });
      const url = new URL(request.url ?? "/", "http://bridge");
      if (url.pathname === POLL_PATH || url.pathname.startsWith(`${POLL_PATH}/`)) {
        return await handlePoll(request, response, url, sessionCookie);
      }
      if (url.pathname === SHIM_PATH) return serveShim(response);
      await proxy(request, response, sessionCookie);
    } catch (error) {
      console.error("[bridge]", request.method, request.url, error);
      if (!response.headersSent) json(response, 502, { code: "bridge_error" });
      else response.destroy();
    }
  });
  // Upgrades are refused: this bridge exists for clients that cannot use them.
  server.on("upgrade", (_request, socket) => socket.destroy());
  server.listen(PORT, "127.0.0.1", () => {
    console.log(`[bridge] http://127.0.0.1:${PORT} → ${UPSTREAM.origin}`);
    if (PUBLIC_HOSTS.size > 0) console.log(`[bridge] public hosts: ${[...PUBLIC_HOSTS].join(", ")}`);
  });
}

const [command, argument] = process.argv.slice(2);
if (command === "pair") {
  if (argument === undefined) {
    console.error("Usage: node bridge.ts pair <pairing-url-or-token>");
    process.exit(1);
  }
  await pair(argument);
} else {
  serve();
}
