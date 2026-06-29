#!/usr/bin/env bash
# End-to-end test against a real Kafka cluster.
# Usage: BROKERS="b1:9092,b2:9092" ./test/e2e.sh
set -uo pipefail

BROKERS="${BROKERS:?set BROKERS=host:9092,host:9092}"
BIN="${BIN:-./syncup}"
CH="e2e-$$"                                  # unique per run, prefixed syncup.e2e-<pid>
WORK="$(mktemp -d)"
ALICE="$WORK/alice.json"
BOB="$WORK/bob.json"

pass=0; fail=0
ok()   { echo "  ✅ $1"; pass=$((pass+1)); }
bad()  { echo "  ❌ $1"; fail=$((fail+1)); }
a() { SYNCUP_CONFIG="$ALICE" "$BIN" "$@"; }   # alice
b() { SYNCUP_CONFIG="$BOB"   "$BIN" "$@"; }   # bob

cleanup() {
  echo "── cleanup"
  SYNCUP_CONFIG="$ALICE" "$BIN" delete "$CH" >/dev/null 2>&1 && echo "  deleted syncup.$CH" || true
  rm -rf "$WORK"
}
trap cleanup EXIT

echo "── setup (channel: syncup.$CH)"
a init --brokers "$BROKERS" --user alice >/dev/null && ok "alice configured" || bad "alice init"
b init --brokers "$BROKERS" --user bob   >/dev/null && ok "bob configured"   || bad "bob init"

echo "── create + discover"
a create "$CH" "e2e temp channel" >/dev/null && ok "alice created channel" || bad "create"
a list | grep -q "$CH" && ok "alice sees channel in list" || bad "alice list"
b list | grep -q "$CH" && ok "bob discovers channel (shared registry)" || bad "bob list"

echo "── from-now semantics"
a publish "$CH" "BEFORE join — must NOT be seen" >/dev/null && ok "alice published pre-join msg" || bad "publish pre"
b join "$CH" >/dev/null && ok "bob joined" || bad "join"
b list | grep -E "✓ +$CH" >/dev/null && ok "bob shows ✓ on channel" || bad "join mark"

echo "── publish + inbox"
a publish "$CH" "AFTER-1 schema changed" >/dev/null
a publish "$CH" "AFTER-2 redeploy needed" >/dev/null && ok "alice published 2 post-join msgs" || bad "publish post"

OUT="$(b inbox)"
echo "$OUT" | grep -q "AFTER-1" && echo "$OUT" | grep -q "AFTER-2" && ok "bob inbox shows both post-join msgs" || bad "inbox missing msgs"
echo "$OUT" | grep -q "BEFORE join" && bad "inbox LEAKED pre-join msg" || ok "pre-join msg correctly hidden"
echo "$OUT" | grep -q "alice," && ok "inbox shows author" || bad "inbox author"

echo "── offset commit (no re-delivery)"
OUT2="$(b inbox)"
echo "$OUT2" | grep -q "no new updates" && ok "second inbox is empty (offsets committed)" || bad "re-delivered: $OUT2"

echo "── incremental delivery"
a publish "$CH" "AFTER-3 follow-up" >/dev/null
OUT3="$(b inbox)"
echo "$OUT3" | grep -q "AFTER-3" && ! echo "$OUT3" | grep -q "AFTER-1" && ok "third msg delivered alone" || bad "incremental: $OUT3"

echo "── leave"
b leave "$CH" >/dev/null && ok "bob left" || bad "leave"
b list | grep -E "✓ +$CH" >/dev/null && bad "still marked joined" || ok "no ✓ after leave"

echo "── delete"
a delete "$CH" >/dev/null && ok "alice deleted channel" || bad "delete"
a list | grep -q "$CH" && bad "channel still in registry" || ok "channel gone from registry (tombstone)"
trap 'rm -rf "$WORK"' EXIT   # channel already deleted; skip re-delete

echo
echo "── results: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
