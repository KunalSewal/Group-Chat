#!/usr/bin/env bash
# Does the cluster actually hold one conversation?
#
#   bash deploy/smoke.sh <lb-url> <backend-url>...
#   bash deploy/smoke.sh http://127.0.0.1:3000 \
#     http://172.17.0.67:3000 http://172.17.0.68:3000 http://172.17.0.69:3000
#
# Posts through the balancer, which spreads the messages across the backends,
# then asks every backend for the whole feed. Each one must return all of them:
# a message is accepted by whichever machine the balancer picked, and the others
# only have it because replication put it there. This is the property the
# evaluation checks and the one most likely to be quietly wrong — a backend can
# serve fast, healthy, correct-looking responses that are missing a third of the
# conversation.
#
# Also posts one message twice with the same id, which must be stored once.
set -u

lb="${1:?usage: smoke.sh <lb-url> <backend-url>...}"
shift
backends=("$@")

count="${SMOKE_MESSAGES:-60}"
run="smoke-$(date +%s)"
echo "posting $count messages through $lb"

for i in $(seq 1 "$count"); do
  curl -s -o /dev/null -X POST "$lb/message" \
    -H 'content-type: application/json' \
    -d "{\"client-name\":\"smoke$((i % 7))\",\"msg\":\"$run message $i\"}"
done

# The same id twice: the second must resolve to the message already stored.
dup="$run-duplicate"
for _ in 1 2; do
  curl -s -o /dev/null -X POST "$lb/message" \
    -H 'content-type: application/json' \
    -d "{\"id\":\"$dup\",\"client-name\":\"smoke\",\"msg\":\"sent twice\"}"
done

# Replication is asynchronous by design — the poster does not wait for its peers
# — so give it a moment before asking whether it worked.
sleep "${SMOKE_SETTLE:-5}"

expected=$((count + 1))
echo
printf '%-34s %8s %8s %s\n' "backend" "mine" "dupes" "verdict"
failed=0

for backend in "${backends[@]}"; do
  body="$(curl -s -m 20 "$backend/feed" || true)"
  if [ -z "$body" ]; then
    printf '%-34s %8s %8s %s\n' "$backend" "-" "-" "NO ANSWER"
    failed=1
    continue
  fi

  mine="$(printf '%s' "$body" | grep -o "$run message" | wc -l)"
  dupes="$(printf '%s' "$body" | grep -o "\"$dup\"" | wc -l)"

  verdict="ok"
  [ "$mine" -ne "$count" ] && { verdict="MISSING $((count - mine))"; failed=1; }
  [ "$dupes" -ne 1 ] && { verdict="$verdict; DUPLICATED x$dupes"; failed=1; }

  printf '%-34s %8s %8s %s\n' "$backend" "$mine" "$dupes" "$verdict"
done

echo
if [ "$failed" -eq 0 ]; then
  echo "PASS: every backend holds all $expected messages, the repeated id stored once."
else
  echo "FAIL: the backends do not hold the same conversation. Check replication"
  echo "      (PEERS on each machine) and that CHAT_ENCRYPTION_KEY is identical."
fi
exit "$failed"
