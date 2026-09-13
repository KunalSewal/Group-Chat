#!/usr/bin/env bash
# The database, sized for the container it is actually in rather than for the
# host it can see.
set -eu
source "$(dirname "$0")/env.sh"
mkdir -p "$MONGO_DBPATH"

# An instance started outside this supervisor — a --fork left over from an
# earlier session, most likely — still holds the port, and mongod's answer to
# that is to exit 48 immediately. Supervised, that becomes a restart loop that
# says "exited with 48" and nothing about the cause, while the machine looks
# like it has a database because something is in fact listening on 27017.
#
# It is not enough to leave that one running: an unsupervised mongod is the
# thing this deployment exists to stop having. Say plainly what to do about it.
if (exec 3<>"/dev/tcp/$MONGO_BIND/$MONGO_PORT") 2>/dev/null; then
  exec 3<&- 2>/dev/null || true
  echo "[mongod] FATAL: something already holds $MONGO_BIND:$MONGO_PORT, so this one cannot start."
  echo "[mongod] It is almost certainly an unsupervised mongod from an earlier session:"
  ps -o pid,etime,cmd -C mongod 2>/dev/null | sed 's/^/[mongod]   /' || true
  echo "[mongod] Stop it and let the supervisor own it:  pkill -x mongod"
  echo "[mongod] Note its --dbpath first if the data there matters; this one uses $MONGO_DBPATH."
  exit 48
fi

args=(
  --dbpath "$MONGO_DBPATH"
  --bind_ip "$MONGO_BIND"
  --port "$MONGO_PORT"
  --wiredTigerCacheSizeGB "$MONGO_CACHE_GB"
)

# The diagnostic data collector writes a few megabytes and keeps a thread busy
# for telemetry nobody here reads. Only passed if this build accepts it, so the
# scripts work across the versions on the three machines.
if mongod --help 2>/dev/null | grep -q diagnosticDataCollectionEnabled ||
   mongod --setParameter diagnosticDataCollectionEnabled=false --version >/dev/null 2>&1; then
  args+=(--setParameter diagnosticDataCollectionEnabled=false)
fi

exec mongod "${args[@]}"
