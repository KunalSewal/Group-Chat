const http = require('node:http');
const https = require('node:https');
const { URL } = require('node:url');

// Pushes newly accepted messages to the other backend machines.
//
// Each machine owns a local database, so a write is a local operation and never
// waits on the network. Sharing the conversation is then a matter of telling
// the peers what arrived. That push is deliberately off the request path: the
// client's 200 depends on the local write only, and replication catches up
// within a few tens of milliseconds.
//
// Delivery does not have to be reliable, which is what makes this cheap. Every
// message carries the id it was created with, inserts are keyed on that id, and
// each machine independently rescans its own collection on a timer. A dropped
// batch, a peer that was restarting, a duplicate delivered twice — all of them
// converge to the same rows.

const DEFAULTS = {
  flushMs: Number(process.env.REPLICATE_FLUSH_MS || 25),
  flushMax: Number(process.env.REPLICATE_FLUSH_MAX || 200),
  timeoutMs: Number(process.env.REPLICATE_TIMEOUT_MS || 2000),
  // A batch that fails to reach a peer is retried rather than abandoned.
  //
  // It used to be abandoned, on the reasoning that anti-entropy would repair
  // whatever was lost. It does — eventually. In one evaluation run 161 batches
  // failed while reconciliation was backed off to five-minute rounds, and the
  // completeness check, which reads the feed through the balancer and so from
  // whichever backend it happens to reach, found six accepted messages missing.
  // That is 99.97% on the measure both boards sort on first, and it is the
  // difference between a repair that takes milliseconds and one that takes
  // minutes.
  retryMs: Number(process.env.REPLICATE_RETRY_MS || 200),
  maxAttempts: Number(process.env.REPLICATE_MAX_ATTEMPTS || 5),
  // Bounded, because a peer that is genuinely gone must not be able to grow
  // this without limit. Past the bound the batch is dropped and reconciliation
  // is once again the backstop — but for a peer that is merely busy, which is
  // what the failures actually were, it never comes close.
  maxRetries: Number(process.env.REPLICATE_MAX_RETRIES || 32),
};

class Replicator {
  constructor(peerUrls = [], options = {}) {
    this.options = { ...DEFAULTS, ...options };
    this.queue = [];
    this.timer = null;
    this.counters = { sent: 0, batches: 0, failures: 0, retried: 0, dropped: 0 };
    this.pendingRetries = 0;
    this.stopped = false;

    this.peers = peerUrls
      .map((raw) => String(raw).trim())
      .filter(Boolean)
      .map((raw) => {
        const url = new URL('/internal/replicate', raw);
        const isHttps = url.protocol === 'https:';
        return {
          url,
          transport: isHttps ? https : http,
          agent: new (isHttps ? https.Agent : http.Agent)({
            keepAlive: true,
            maxSockets: 32,
            keepAliveMsecs: 30000,
          }),
        };
      });
  }

  get enabled() {
    return this.peers.length > 0;
  }

  stats() {
    return {
      peers: this.peers.length,
      queued: this.queue.length,
      retrying: this.pendingRetries,
      ...this.counters,
    };
  }

  // Takes the encrypted form so peers store byte-identical rows rather than
  // re-encrypting the same text under a different nonce.
  enqueue({ id, name, ts, encrypted }) {
    if (!this.enabled || !encrypted) return;

    this.queue.push({
      id,
      name,
      ts,
      ciphertext: encrypted.ciphertext.toString('base64'),
      nonce: encrypted.nonce.toString('base64'),
    });

    if (this.queue.length >= this.options.flushMax) {
      this.flush();
      return;
    }

    if (!this.timer) {
      this.timer = setTimeout(() => this.flush(), this.options.flushMs);
      if (typeof this.timer.unref === 'function') this.timer.unref();
    }
  }

  flush() {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    if (this.queue.length === 0) return;

    const batch = this.queue;
    this.queue = [];

    const payload = Buffer.from(JSON.stringify({ messages: batch }));
    this.counters.batches += 1;
    this.counters.sent += batch.length;

    for (const peer of this.peers) {
      this.send(peer, payload);
    }
  }

  send(peer, payload, attempt = 0) {
    let settled = false;
    const done = (ok) => {
      if (settled) return;
      settled = true;
      if (!ok) this.retry(peer, payload, attempt);
    };

    const request = peer.transport.request(
      {
        protocol: peer.url.protocol,
        hostname: peer.url.hostname,
        port: peer.url.port,
        path: peer.url.pathname,
        method: 'POST',
        agent: peer.agent,
        headers: {
          'content-type': 'application/json',
          'content-length': payload.length,
        },
        rejectUnauthorized: false,
      },
      (response) => {
        response.resume();
        // The status is checked rather than assumed. A peer that answers 500 —
        // because its database is unreachable, say — was silently counted as a
        // successful delivery before, which is the one case where the messages
        // are certainly not there.
        done(response.statusCode >= 200 && response.statusCode < 300);
      }
    );

    // A peer that is down must not hold a socket open.
    request.setTimeout(this.options.timeoutMs, () => request.destroy());
    request.on('error', () => done(false));

    request.end(payload);
  }

  retry(peer, payload, attempt) {
    this.counters.failures += 1;

    const exhausted = attempt + 1 >= this.options.maxAttempts;
    const full = this.pendingRetries >= this.options.maxRetries;
    if (this.stopped || exhausted || full) {
      this.counters.dropped += 1;
      return;
    }

    this.counters.retried += 1;
    this.pendingRetries += 1;

    // Backing off, because the usual reason a peer refused a batch is that it
    // is busy, and the usual effect of retrying a busy peer immediately is to
    // make it busier.
    const delay = this.options.retryMs * 2 ** attempt;
    const timer = setTimeout(() => {
      this.pendingRetries -= 1;
      if (!this.stopped) this.send(peer, payload, attempt + 1);
    }, delay);
    if (typeof timer.unref === 'function') timer.unref();
  }

  stop() {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    for (const peer of this.peers) peer.agent.destroy();
  }
}

module.exports = { Replicator };
