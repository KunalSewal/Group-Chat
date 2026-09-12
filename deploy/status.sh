#!/usr/bin/env bash
# What this machine is actually doing. One screen, no interpretation needed.
set -u
source "$(dirname "$0")/env.sh"

port="${PORT:-3000}"

echo "=== sessions ==="
tmux ls 2>/dev/null || echo "(no tmux sessions)"

echo
echo "=== container limits and use ==="
if [ -r /sys/fs/cgroup/memory.max ]; then
  used=$(cat /sys/fs/cgroup/memory.current)
  max=$(cat /sys/fs/cgroup/memory.max)
  echo "memory: $((used / 1048576))MB of $((max / 1048576))MB"
  # The number that says whether this machine has been killed, and how often.
  # It is the first thing to look at after a machine has "gone quiet".
  grep -E 'oom|high' /sys/fs/cgroup/memory.events 2>/dev/null | sed 's/^/memory.events: /'
else
  echo "(not in a cgroup v2 container)"
fi
[ -r /sys/fs/cgroup/cpu.max ] && echo "cpu.max: $(cat /sys/fs/cgroup/cpu.max)"

echo
echo "=== processes ==="
ps -o pid,rss,etime,cmd -C mongod 2>/dev/null | sed 's/^/  /' || true
ps -o pid,rss,etime,cmd -C node 2>/dev/null | sed 's/^/  /' || true

echo
echo "=== health ==="
curl -sS --max-time 3 "http://127.0.0.1:$port/health" || echo "(no answer on $port)"
echo
curl -sS --max-time 3 "http://127.0.0.1:$port/stats" 2>/dev/null |
  head -c 600 || true
echo

echo
echo "=== recent restarts ==="
grep -h "exited with" "$LOG_DIR"/*.log 2>/dev/null | tail -5 || echo "(none logged)"
