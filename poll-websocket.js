// Loaded by the bridge before T3 Code's own scripts. T3 Code opens its RPC
// socket with `new globalThis.WebSocket(".../ws")`; this swaps that one socket
// for HTTP long-polling, which networks that block WebSocket upgrades and
// streamed responses still let through. Every other WebSocket is untouched.
(() => {
  const POLL_PATH = "/ws/poll";
  const RETRY_DELAY_MS = 1_000;
  const MAX_CONSECUTIVE_FAILURES = 5;
  const NativeWebSocket = globalThis.WebSocket;

  const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  class PollWebSocket extends EventTarget {
    static CONNECTING = 0;
    static OPEN = 1;
    static CLOSING = 2;
    static CLOSED = 3;

    readyState = 0;
    binaryType = "blob";
    protocol = "";
    extensions = "";
    bufferedAmount = 0;
    onopen = null;
    onmessage = null;
    onerror = null;
    onclose = null;
    #id;
    #pending = [];
    #sentSeq = 0;
    #receivedSeq = 0;
    #sending = false;
    #abort = new AbortController();

    constructor(url) {
      super();
      this.url = url.toString();
      const openUrl = new URL(this.url);
      openUrl.protocol = location.protocol;
      openUrl.pathname = `${POLL_PATH}/open`;
      void this.#open(openUrl);
    }

    send(data) {
      if (this.readyState !== 1) return;
      this.#pending.push(typeof data === "string" ? data : new TextDecoder().decode(data));
      void this.#flush();
    }

    close(code = 1000, reason = "") {
      if (this.readyState === 3) return;
      if (this.#id !== undefined) {
        fetch(this.#connectionUrl(), { method: "DELETE", keepalive: true }).catch(() => {});
      }
      this.#finish(code, reason);
    }

    async #open(openUrl) {
      try {
        const response = await fetch(openUrl, { signal: this.#abort.signal, cache: "no-store" });
        if (!response.ok) throw new Error(`poll open failed: ${response.status}`);
        const { id } = await response.json();
        if (this.readyState !== 0) return;
        this.#id = id;
        this.readyState = 1;
        this.#emit(new Event("open"));
        void this.#receiveLoop();
      } catch {
        this.#fail();
      }
    }

    async #receiveLoop() {
      let failures = 0;
      while (this.readyState === 1) {
        try {
          const response = await fetch(`${this.#connectionUrl()}?ack=${this.#receivedSeq}`, {
            signal: this.#abort.signal,
            cache: "no-store",
          });
          if (response.status === 410) return this.#finish(1006, "poll socket gone");
          if (!response.ok) throw new Error(`poll receive failed: ${response.status}`);
          const body = await response.json();
          failures = 0;
          const fresh = body.frames.slice(Math.max(this.#receivedSeq - body.seq, 0));
          this.#receivedSeq = Math.max(this.#receivedSeq, body.seq + body.frames.length);
          for (const data of fresh) {
            if (this.readyState !== 1) return;
            this.#emit(new MessageEvent("message", { data }));
          }
          if (body.closed) return this.#finish(body.closed.code, body.closed.reason);
        } catch {
          if (this.readyState !== 1) return;
          if (++failures >= MAX_CONSECUTIVE_FAILURES) return this.#fail();
          await delay(RETRY_DELAY_MS);
        }
      }
    }

    // One POST in flight keeps frames ordered; a failed batch is resent under the same seq.
    async #flush() {
      if (this.#sending) return;
      this.#sending = true;
      let failures = 0;
      try {
        while (this.readyState === 1 && this.#pending.length > 0) {
          const frames = this.#pending.slice();
          try {
            const response = await fetch(this.#connectionUrl(), {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({ seq: this.#sentSeq, frames }),
              signal: this.#abort.signal,
            });
            if (response.status === 410) return this.#finish(1006, "poll socket gone");
            if (!response.ok) throw new Error(`poll send failed: ${response.status}`);
            failures = 0;
            this.#pending = this.#pending.slice(frames.length);
            this.#sentSeq += frames.length;
          } catch {
            if (this.readyState !== 1) return;
            if (++failures >= MAX_CONSECUTIVE_FAILURES) return this.#fail();
            await delay(RETRY_DELAY_MS);
          }
        }
      } finally {
        this.#sending = false;
      }
    }

    #connectionUrl() {
      return `${location.origin}${POLL_PATH}/${encodeURIComponent(this.#id)}`;
    }

    #fail() {
      if (this.readyState === 3) return;
      this.#emit(new Event("error"));
      this.#finish(1006, "poll socket failed");
    }

    #finish(code, reason) {
      if (this.readyState === 3) return;
      this.readyState = 3;
      this.#abort.abort();
      this.#emit(new CloseEvent("close", { code, reason, wasClean: code === 1000 }));
    }

    #emit(event) {
      this.dispatchEvent(event);
      this[`on${event.type}`]?.(event);
    }
  }

  function isThisServersRpcSocket(url) {
    const target = new URL(url, location.href);
    return target.pathname === "/ws" && target.host === location.host;
  }

  function WebSocket(url, protocols) {
    if (!new.target) throw new TypeError("Failed to construct 'WebSocket': Please use the 'new' operator");
    if (isThisServersRpcSocket(url)) return new PollWebSocket(url);
    return protocols === undefined ? new NativeWebSocket(url) : new NativeWebSocket(url, protocols);
  }
  WebSocket.prototype = NativeWebSocket.prototype;
  Object.assign(WebSocket, { CONNECTING: 0, OPEN: 1, CLOSING: 2, CLOSED: 3 });
  globalThis.WebSocket = WebSocket;
})();
