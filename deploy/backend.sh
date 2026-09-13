#!/usr/bin/env bash
# The chat backend.
set -eu
source "$(dirname "$0")/env.sh"

# The shell a person gets over ssh sources ~/.bashrc, and on these machines that
# is where the current node ends up on the PATH. A tmux session started
# detached sources nothing, so the supervisor would run whichever node happens
# to be in the default PATH — which is a different, older one, and which exits
# immediately with code 9 because it does not recognise the --env-file-if-exists
# flag that npm start uses. The process then looks like it is crash-looping for
# no reason, with nothing in the log to say why.
#
# So load the same environment the person deploying has.
for profile in "$HOME/.nvm/nvm.sh" "$HOME/.profile" "$HOME/.bashrc"; do
  # shellcheck disable=SC1090
  [ -r "$profile" ] && . "$profile" >/dev/null 2>&1 || true
done
set -eu

node_bin="$(command -v node || true)"
if [ -z "$node_bin" ]; then
  echo "[backend] FATAL: no node on PATH. PATH=$PATH"
  exit 127
fi

version="$(node -v)"
echo "[backend] using $node_bin ($version)"

# --env-file-if-exists landed in 20.12. Below that the process would exit 9
# before printing anything, so say so here instead.
major="$(echo "${version#v}" | cut -d. -f1)"
minor="$(echo "${version#v}" | cut -d. -f2)"
if [ "$major" -lt 20 ] || { [ "$major" -eq 20 ] && [ "$minor" -lt 12 ]; }; then
  echo "[backend] FATAL: node $version is too old; 20.12 or newer is required."
  echo "[backend] The interactive shell may have a newer one: check 'node -v' over ssh,"
  echo "[backend] and make sure nvm is loaded from ~/.bashrc or ~/.profile."
  exit 1
fi

# Wait for the database rather than racing it: on a restart both come up at
# once, and a backend that gave up because mongod was two seconds behind it
# would sit there doing nothing.
for _ in $(seq 1 60); do
  if (exec 3<>"/dev/tcp/$MONGO_BIND/$MONGO_PORT") 2>/dev/null; then
    exec 3<&- 2>/dev/null || true
    break
  fi
  echo "[backend] waiting for mongod on $MONGO_BIND:$MONGO_PORT"
  sleep 1
done

cd "$SERVER_DIR"

# node directly, not through npm.
#
# npm is itself a Node process: "npm start" forks a shell, which forks node,
# and then npm sits there for the life of the run holding around 51MB with the
# shell holding another 2. On a container allowed 512MB that is a tenth of the
# budget spent on a process whose only remaining job is to wait — and it is
# charged against the same limit that decides which process the kernel kills.
#
# The flags match what package.json does, so running it by hand still behaves
# the same way.
export NODE_ENV="${NODE_ENV:-production}"
exec node --env-file-if-exists=.env server.js
