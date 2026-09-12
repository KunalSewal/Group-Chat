#!/usr/bin/env bash
# Stops the supervised sessions on this machine. The supervisor restarts a
# process that dies, so a process cannot be stopped by killing it — the session
# has to go.
set -u
for s in "$@"; do tmux kill-session -t "$s" 2>/dev/null && echo "stopped $s"; done
[ $# -eq 0 ] && for s in backend mongo lb; do
  tmux kill-session -t "$s" 2>/dev/null && echo "stopped $s"
done
exit 0
