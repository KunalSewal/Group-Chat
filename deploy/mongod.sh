#!/usr/bin/env bash
# The database, sized for the container it is actually in rather than for the
# host it can see.
set -eu
source "$(dirname "$0")/env.sh"
mkdir -p "$MONGO_DBPATH"

# Wait for the port before deciding anything is wrong with it. Replacing a
# session kills the old mongod and starts this one immediately, and the socket
# is still held for a moment afterwards — that race produced an "exited with
# 48" on every restart, which is noise in the one place the logs need to be
# trustworthy.
held=""
for _ in $(seq 1 15); do
  if (exec 3<>"/dev/tcp/$MONGO_BIND/$MONGO_PORT") 2>/dev/null; then
    exec 3<&- 2>/dev/null || true
    held="yes"
    sleep 1
  else
    held=""
    break
  fi
done

# Still held after fifteen seconds: this is not a restart racing itself but an
# instance started outside this supervisor — a --fork left from an earlier
# session, most likely. It is worth naming, because the machine does not look
# broken: something is listening on 27017, so the database appears to be up,
# while the instance meant to be supervised restarts forever. And leaving that
# one running is not an option either: an unsupervised mongod is the thing
# this deployment exists to stop having.
if [ -n "$held" ]; then
  echo "[mongod] FATAL: something already holds $MONGO_BIND:$MONGO_PORT, so this one cannot start."
  ps -o pid,etime,cmd -C mongod 2>/dev/null | sed "s/^/[mongod]   /" || true
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
