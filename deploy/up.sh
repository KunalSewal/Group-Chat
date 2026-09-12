#!/usr/bin/env bash
# Brings a machine up, and leaves it up.
#
#   ./deploy/up.sh backend     # sys2, sys3, sys4: mongod + the chat backend
#   ./deploy/up.sh lb          # sys1: the load balancer
#
# Everything runs under tmux (so it survives the ssh connection dropping) and
# under supervise.sh inside tmux (so it survives the process dying, which the
# ssh session alone does nothing about). Both have happened; each needs its own
# answer.
#
# Safe to run repeatedly: a session that is already running is left alone. Pass
# --restart to replace it.
set -eu

here="$(cd "$(dirname "$0")" && pwd)"
source "$here/env.sh"

role="${1:-backend}"
restart=""
[ "${2:-}" = "--restart" ] && restart="yes"
[ "${1:-}" = "--restart" ] && { restart="yes"; role="${2:-backend}"; }

start_session() {
  local session="$1"
  shift

  if tmux has-session -t "$session" 2>/dev/null; then
    if [ -n "$restart" ]; then
      echo "[up] replacing session '$session'"
      tmux kill-session -t "$session"
    else
      echo "[up] session '$session' already running (--restart to replace)"
      return
    fi
  fi

  echo "[up] starting session '$session'"
  tmux new-session -d -s "$session" "$here/supervise.sh" "$session" "$@"
}

case "$role" in
  backend)
    start_session mongo "$here/mongod.sh"
    start_session backend "$here/backend.sh"
    ;;
  lb)
    start_session lb "$here/lb.sh"
    ;;
  *)
    echo "usage: $0 [backend|lb] [--restart]" >&2
    exit 2
    ;;
esac

echo
tmux ls || true
echo
echo "logs: $LOG_DIR    attach: tmux attach -t <session>    detach: Ctrl-b then d"
