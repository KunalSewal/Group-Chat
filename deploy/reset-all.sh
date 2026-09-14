#!/usr/bin/env bash
# Empties the conversation on every backend at once.
#
#   bash deploy/reset-all.sh <token> <backend-url>...
#
# Resetting the machines one at a time does not work, and the reason is the
# system working as designed: the moment one empties itself it disagrees with
# its peers, and anti-entropy repairs the disagreement by pulling everything
# back. Done in sequence with a pause between, a cluster ends up exactly where
# it started — measured here as three machines all holding 52,385 messages that
# had each just been told to delete them.
#
# So all of them are reset together, inside the reconciliation interval, and
# then checked twice: once immediately, and again after long enough for a repair
# round to have happened. The second check is the one that matters.
set -u

token="${1:?usage: reset-all.sh <token> <backend-url>...}"
shift
backends=("$@")
[ "${#backends[@]}" -gt 0 ] || { echo "give at least one backend URL" >&2; exit 2; }

echo "resetting ${#backends[@]} backends together"

# Each answer is kept and shown. Throwing them away meant a run where every
# reset was refused looked identical to one where every reset worked — the
# only clue being that the counts had not moved, which is exactly the thing a
# person running this is trusting the script to notice for them.
tmp="$(mktemp -d)"
i=0
for backend in "${backends[@]}"; do
  curl -s -m 10 -X POST "$backend/admin/reset?token=$token" -o "$tmp/$i" &
  i=$((i + 1))
done
wait

i=0
refused=0
for backend in "${backends[@]}"; do
  answer="$(cat "$tmp/$i" 2>/dev/null)"
  printf "  %-32s %s
" "$backend" "${answer:-(no answer)}"
  case "$answer" in
    *"ok"*) ;;
    *forbidden*) refused=1 ;;
    *"not enabled"*) refused=1 ;;
    *) refused=1 ;;
  esac
  i=$((i + 1))
done
rm -rf "$tmp"

if [ "$refused" -ne 0 ]; then
  echo
  echo "FAIL: at least one backend refused the reset. "forbidden" means the"
  echo "      token did not match — pass the real value of ADMIN_TOKEN from"
  echo "      server/.env, not a placeholder. "not enabled" means that machine"
  echo "      has no ADMIN_TOKEN set at all."
  exit 1
fi

count_of() {
  curl -s -m 10 "$1/stats" |
    sed -n 's/.*"messages":\([0-9]*\).*/\1/p' |
    head -1
}

report() {
  local when="$1"
  local total=0
  echo
  echo "$when:"
  for backend in "${backends[@]}"; do
    local n
    n="$(count_of "$backend")"
    n="${n:--1}"
    printf '  %-32s %s messages\n' "$backend" "$n"
    [ "$n" -gt 0 ] 2>/dev/null && total=$((total + n))
  done
  return "$((total > 0 ? 1 : 0))"
}

report "immediately after"
sleep "${RESET_SETTLE:-20}"

if report "after a repair round"; then
  echo
  echo "PASS: the cluster is empty and stayed empty."
  exit 0
fi

echo
echo "FAIL: messages came back. Something still holds them — check that every"
echo "      backend in the cluster was included in this command."
exit 1
