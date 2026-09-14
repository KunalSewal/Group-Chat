// Anti-entropy between the backend machines.
//
// Replication is a fire-and-forget push, which is what keeps it off the request
// path — but it means a message written while a peer was down, restarting, or
// briefly unreachable never reaches that peer. Nothing would ever repair it,
// and that machine's /feed would stay permanently short. Since the evaluation
// reads /feed through a load balancer that may route it to any of the three,
// a divergent replica is a wrong answer.
//
// So each machine periodically asks its peers what they hold, and pulls
// anything it is missing. Insertion is keyed on the message id, so pulling a
// message that arrived by push in the meantime is a no-op — the same property
// that makes retries safe makes repair safe.
//
// The comparison is cheap in the common case: a digest of count plus newest id
// settles it in one small request, and the id list is only fetched when those
// disagree.

const DEFAULTS = {
  // Thirty seconds, not ten. Comparing two machines that disagree means
  // fetching a list of every id the peer holds — at thirty-five thousand
  // messages that is a megabyte of JSON to build on one side and parse on the
  // other, on the single core both are also serving requests with. Replication
  // already delivers everything within tens of milliseconds; this exists to
  // repair what a peer missed while it was unreachable, and that does not need
  // checking three times a minute.
  intervalMs: Number(process.env.RECONCILE_INTERVAL_MS || 30000),
  // Cap on how much repair happens in one round, so catching up a machine that
  // missed a lot never turns into one enormous request.
  batchSize: Number(process.env.RECONCILE_BATCH || 2000),
  timeoutMs: Number(process.env.RECONCILE_TIMEOUT_MS || 5000),
  // Two machines can disagree permanently and correctly: one of them trimmed
  // its conversation and will not take the difference back. Comparing them
  // then means fetching a list of every id the peer holds, every round,
  // forever — tens of thousands of ids on the one core that is also serving
  // the requests. So a round that finds a difference it cannot repair backs
  // off, and any round that actually repairs something resets it.
  maxIntervalMs: Number(process.env.RECONCILE_MAX_INTERVAL_MS || 60000),
};

class Reconciler {
  constructor(store, peerUrls = [], options = {}) {
    this.store = store;
    this.options = { ...DEFAULTS, ...options };
    this.peers = peerUrls.map((raw) => String(raw).trim().replace(/\/$/, '')).filter(Boolean);
    this.timer = null;
    this.running = false;
    this.counters = { rounds: 0, repaired: 0, errors: 0, backoffMs: 0 };
    this.currentIntervalMs = this.options.intervalMs;
  }

  get enabled() {
    return this.peers.length > 0;
  }

  stats() {
    return { ...this.counters, intervalMs: this.currentIntervalMs };
  }

  async request(url, init = {}) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.options.timeoutMs);
    try {
      const response = await fetch(url, { ...init, signal: controller.signal });
      if (!response.ok) throw new Error(`${url} -> ${response.status}`);
      return await response.json();
    } finally {
      clearTimeout(timer);
    }
  }

  async reconcileWith(peer) {
    const digest = await this.request(`${peer}/internal/digest`);
    const local = this.store.digest();

    // Same number of messages and the same set of ids: nothing to do. This is
    // the steady state, and it costs one small request per peer per round.
    if (digest.count === local.count && digest.checksum === local.checksum) {
      return 0;
    }

    const { ids } = await this.request(`${peer}/internal/ids`);
    const missing = ids.filter((id) => !this.store.has(id));
    if (missing.length === 0) return 0;

    let repaired = 0;
    for (let i = 0; i < missing.length; i += this.options.batchSize) {
      const batch = missing.slice(i, i + this.options.batchSize);
      const payload = await this.request(`${peer}/internal/fetch`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ ids: batch }),
      });

      repaired += await this.store.ingestReplicated(payload.messages || []);
    }

    if (repaired > 0) {
      console.log(`reconciled ${repaired} message(s) from ${peer}`);
    }

    return repaired;
  }

  async runOnce() {
    if (!this.enabled || this.running) return 0;
    this.running = true;
    this.counters.rounds += 1;

    let repaired = 0;
    for (const peer of this.peers) {
      try {
        repaired += await this.reconcileWith(peer);
      } catch (err) {
        // A peer being unreachable is the case this exists for, not an error
        // worth making noise about; the next round tries again.
        this.counters.errors += 1;
      }
    }

    this.counters.repaired += repaired;
    this.running = false;

    // Repairing something means the peers are converging and this is worth
    // doing often. Repairing nothing, round after round, means either there is
    // nothing wrong or there is something this cannot fix — and both are
    // reasons to ask less frequently.
    this.currentIntervalMs = repaired > 0
      ? this.options.intervalMs
      : Math.min(this.currentIntervalMs * 2, this.options.maxIntervalMs);
    this.counters.backoffMs = this.currentIntervalMs;

    return repaired;
  }

  start() {
    if (!this.enabled) return;

    // Once at startup, which is what repairs a machine that has just come back
    // after missing messages entirely.
    setTimeout(() => this.runOnce(), 1000).unref?.();

    const schedule = () => {
      this.timer = setTimeout(async () => {
        await this.runOnce();
        schedule();
      }, this.currentIntervalMs);
      this.timer.unref?.();
    };
    schedule();
  }

  stop() {
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
  }
}

module.exports = { Reconciler };
