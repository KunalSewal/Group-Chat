#!/usr/bin/env bash
# The load balancer, on sys1. BACKENDS must be set — there is no sensible
# default, and a balancer pointed at the wrong machines is worse than one that
# refuses to start.
set -eu
source "$(dirname "$0")/env.sh"

# The balancer has to remember which machines it fronts.
#
# BACKENDS lives in the environment of whoever started it, which is fine for
# the first start and useless for every one after: the supervisor restarts this
# script with its own environment, so a crash at three in the morning, or a
# plain --restart, brought it back with no backends and it refused to start —
# then refused again, every thirty seconds, with the cluster unreachable the
# whole time. Given once, it is remembered.
: "${LB_BACKENDS_FILE:=$HOME/.lb-backends}"

if [ -n "${BACKENDS:-}" ]; then
  printf "%s
" "$BACKENDS" > "$LB_BACKENDS_FILE"
elif [ -r "$LB_BACKENDS_FILE" ]; then
  BACKENDS="$(cat "$LB_BACKENDS_FILE")"
  echo "[lb] using remembered backends: $BACKENDS"
fi

if [ -z "${BACKENDS:-}" ]; then
  echo "[lb] FATAL: no backends. Start it once with them and they are remembered:"
  echo "[lb]   BACKENDS=http://host:3000,http://host:3000 bash deploy/up.sh lb --restart"
  exit 78
fi
: "${LB_LISTEN:=:3000}"
: "${LB_THRESHOLD:=4}"

cd "$APP_DIR/loadbalancer"

# Built here rather than assumed: a stale binary from a previous session is
# indistinguishable from a current one until it behaves differently.
if [ ! -x ./lb ] || [ main.go -nt ./lb ]; then
  echo "[lb] building"
  go build -o lb .
fi

exec ./lb -backends "$BACKENDS" -listen "$LB_LISTEN" -load-threshold "$LB_THRESHOLD" ${LB_EXTRA_ARGS:-}
