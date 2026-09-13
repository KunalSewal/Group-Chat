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
echo "=== processes (largest first) ==="
# Everything resident, not a fixed list of names: on the balancer there is no
# node and no mongod, and a status screen that showed nothing there was worse
# than useless — it looked like the machine was empty.
ps -eo pid,rss,etime,cmd --sort=-rss 2>/dev/null | head -8 | sed 's/^/  /' || true

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

# A restart count says a process is failing; only the last lines before it say
# why, and that is the whole reason to look.
for f in "$LOG_DIR"/*.log; do
  [ -r "$f" ] || continue
  if grep -q "FATAL|exited with" "$f" 2>/dev/null; then
    echo
    echo "--- $(basename "$f") (last 12 lines) ---"
    tail -12 "$f"
  fi
done
