#!/usr/bin/env bash
# The load balancer, on sys1. BACKENDS must be set — there is no sensible
# default, and a balancer pointed at the wrong machines is worse than one that
# refuses to start.
set -eu
source "$(dirname "$0")/env.sh"

: "${BACKENDS:?set BACKENDS=http://host:port,http://host:port,...}"
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
