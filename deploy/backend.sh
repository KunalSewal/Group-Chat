#!/usr/bin/env bash
# The chat backend. Waits for its database rather than racing it: on a restart
# after a reboot both come up at once, and a backend that gives up because
# mongod was two seconds behind it would sit there doing nothing.
set -eu
source "$(dirname "$0")/env.sh"

for _ in $(seq 1 60); do
  if (exec 3<>"/dev/tcp/$MONGO_BIND/$MONGO_PORT") 2>/dev/null; then
    exec 3<&- 2>/dev/null || true
    break
  fi
  echo "[backend] waiting for mongod on $MONGO_BIND:$MONGO_PORT"
  sleep 1
done

cd "$SERVER_DIR"
exec npm start
