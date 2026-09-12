#!/usr/bin/env bash
# Runs a command, and keeps running it.
#
# Usage: supervise.sh <name> <command...>
#
# Everything in this deployment has been killed at least once — the backend and
# the balancer by the OOM killer, mongod by the same, and all of them by a
# dropped ssh connection before tmux. A process that is not restarted takes the
# machine out of the pool for the rest of the run, and on a three-machine
# cluster that is a third of the capacity and, if it is the machine holding the
# newest messages, a wrong feed as well.
#
# So nothing here is started directly. This is deliberately not systemd: there
# is no init inside these containers and no privilege to add one, so the
# supervisor is a shell loop, and a shell loop is enough.

set -u

name="$1"
shift

source "$(dirname "$0")/env.sh"
mkdir -p "$LOG_DIR"

log="$LOG_DIR/$name.log"
delay="$RESTART_DELAY_MIN"

echo "[supervise] $name -> $log"

while true; do
  started=$(date +%s)
  echo "[supervise] $(date -Is) starting $name: $*" | tee -a "$log"

  "$@" >>"$log" 2>&1
  code=$?

  ran=$(( $(date +%s) - started ))
  echo "[supervise] $(date -Is) $name exited with $code after ${ran}s" | tee -a "$log"

  # A process that ran for a while and then died is a transient failure: restart
  # it immediately, because every second it is down is a machine out of the
  # pool. One that dies immediately is misconfigured or being killed on
  # startup, and retrying it in a tight loop would burn the single core it
  # shares with everything else.
  if [ "$ran" -ge "$RESTART_MIN_UPTIME" ]; then
    delay="$RESTART_DELAY_MIN"
  else
    delay=$(( delay * 2 ))
    [ "$delay" -gt "$RESTART_DELAY_MAX" ] && delay="$RESTART_DELAY_MAX"
  fi

  echo "[supervise] restarting $name in ${delay}s" | tee -a "$log"
  sleep "$delay"
done
