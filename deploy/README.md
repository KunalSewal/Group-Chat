# Running the cluster

Four machines. `sys1` runs the load balancer and is the only address anything
outside the cluster talks to; `sys2`, `sys3` and `sys4` each run a copy of the
backend with its own MongoDB alongside it.

Everything is started through `up.sh`. Nothing is started by hand, for two
reasons that are easy to confuse and need separate answers:

- **tmux** keeps a process alive when the ssh connection drops.
- **`supervise.sh`** keeps it alive when the process itself dies — which here
  has meant the OOM killer, every runtime in this system having been killed by
  it at least once. tmux does nothing about that: the session stays, and the
  machine sits there serving nothing.

A machine that is down is not just slower. If it held the newest messages, the
feed served by the survivors is wrong until it comes back.

## First time on a machine

```bash
git clone https://<token>@github.com/KunalSewal/Group-Chat ~/csd-group-chat
cd ~/csd-group-chat/server
npm install --omit=dev
cp .env.example .env     # then fill it in; see below
```

`.env` needs, at minimum:

- `MONGODB_URI` — `mongodb://127.0.0.1:27017/groupchat` for the local database
- `CHAT_ENCRYPTION_KEY` — **identical on sys2, sys3 and sys4.** Messages are
  stored encrypted and replicated in that form, so a machine with a different
  key cannot read what its peers wrote, and the feeds diverge silently.
- `PEERS` — the other two backends, so replication and reconciliation have
  somewhere to go.

## Every time

On sys2, sys3, sys4:

```bash
cd ~/csd-group-chat && git pull
bash deploy/up.sh backend
```

Running them through `bash` rather than as `./deploy/up.sh` works whether or
not the execute bit survived the trip to this machine.

On sys1:

```bash
cd ~/csd-group-chat && git pull
BACKENDS=http://<sys2>:3000,http://<sys3>:3000,http://<sys4>:3000 bash deploy/up.sh lb
```

`up.sh` is idempotent: a session already running is left alone. After pulling
new code, replace it:

```bash
bash deploy/up.sh backend --restart
```

## Checking

```bash
bash deploy/status.sh
```

One screen: the tmux sessions, the container's memory against its limit,
`memory.events` (a non-zero `oom_kill` is how you find out a machine was killed
rather than idle), the processes, `/health`, `/stats`, and the last few
restarts the supervisor logged.

From outside, only the balancer's address answers:

```bash
curl http://<sys1>:<mapped port>/health
curl http://<sys1>:<mapped port>/lb/status
```

The lab maps each container's port 3000 to the host's *system port + 1000*.

## When a machine will not come up

Run `bash deploy/status.sh`; it prints the last lines of any log that recorded
a failure, which is where the reason is.

- **`backend exited with 9`** — node did not recognise a flag, which means the
  supervisor found an older node than the interactive shell does. The scripts
  source `~/.nvm/nvm.sh`, `~/.profile` and `~/.bashrc` to get the same one; if
  node is installed somewhere none of those put on the PATH, the log now says
  which node it used and what version.
- **`lb exited with 1`, `set BACKENDS=...`** — the balancer was restarted
  without being told which machines it fronts. Give them once and they are
  remembered in `~/.lb-backends`; later restarts need nothing.
- **`mongo exited with 48`** — something already holds port 27017, almost
  always an unsupervised `mongod --fork` left from an earlier session. Stop it
  with `pkill -x mongod` and run `bash deploy/up.sh backend --restart`. Check
  its `--dbpath` first: the supervised one uses `~/mongo-data`, so a forked one
  on `/data/db` was writing somewhere else.
- **`oom_kill` climbing during a run** — the container is out of memory. The
  backend sheds its own buffers from 80% (`/stats` reports `shedding`), so a
  kill above that usually means mongod, not the backend: lower
  `MONGO_CACHE_GB`.

## Stopping

```bash
bash deploy/down.sh         # this machine's sessions
bash deploy/down.sh backend # just one
```

Killing the process does not stop it — the supervisor starts it again. The
session is what has to go.

## Tuning without redeploying

- The backend runs as `node` directly rather than through `npm start`: npm is
  itself a Node process and stays resident for the life of the run, holding
  around 51MB of a 512MB budget to do nothing but wait.
- `MONGO_CACHE_GB` (default 0.25) — WiredTiger sizes its cache from the *host's*
  memory unless told otherwise, and the host has 128GB while the container is
  allowed 512MB. Leaving this unset is what killed mongod and the backend
  together.
- `FEED_MAX_BYTES` (default 16MB) — the cap that actually bounds this process's
  memory. `FEED_MAX_MESSAGES` bounds how much work a rebuild does, not how much
  it allocates: message length is the client's choice.
- `FEED_SHED_AT` / `FEED_SHED_HARD_AT` (0.80 / 0.90) — where the backend starts
  handing memory back, and where it starts shortening the conversation to do it.
- The balancer's `-load-threshold` (default 4) can be changed at runtime:
  `curl -X POST '<lb>/lb/config?threshold=3'`.
