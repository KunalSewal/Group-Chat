#!/usr/bin/env bash
# The database, sized for the container it is actually in rather than for the
# host it can see.
set -eu
source "$(dirname "$0")/env.sh"
mkdir -p "$MONGO_DBPATH"

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
