#!/usr/bin/env bash
# End-to-end test against a real Kafka cluster.
# Usage: BROKERS="b1:9092,b2:9092" ./test/e2e.sh
set -uo pipefail

BROKERS="${BROKERS:?set BROKERS=host:9092,host:9092}"
BIN="${BIN:-./syncup}"
CH="e2e-$$"                                  # unique per run, prefixed syncup.e2e-<pid>
CH2="e2e2-$$"                                 # second channel, for per-channel inbox test
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
  SYNCUP_CONFIG="$ALICE" "$BIN" delete "$CH2" >/dev/null 2>&1 && echo "  deleted syncup.$CH2" || true
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

# Re-joining an already-followed channel must NOT fast-forward past unread msgs.
b join "$CH" >/dev/null && ok "bob re-joined (offset preserved)" || bad "re-join"

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

echo "── per-channel inbox"
a create "$CH2" "second channel" >/dev/null
b join "$CH2" >/dev/null
a publish "$CH"  "msg for channel one" >/dev/null
a publish "$CH2" "msg for channel two" >/dev/null
OUT4="$(b inbox "$CH2")"
echo "$OUT4" | grep -q "channel two" && ! echo "$OUT4" | grep -q "channel one" && ok "inbox <channel> reads only that channel" || bad "per-channel: $OUT4"
OUT5="$(b inbox)"
echo "$OUT5" | grep -q "channel one" && ok "other channel still unread after scoped read" || bad "scoped read drained wrong channel: $OUT5"
OUT6="$(b inbox nonexistent-chan 2>&1 || true)"
echo "$OUT6" | grep -qi "not joined" && ok "inbox rejects unsubscribed channel" || bad "no guard on unsubscribed channel: $OUT6"

echo "── per-session cursors (same user, independent catch-up)"
SYNCUP_SESSION=s1 b inbox >/dev/null   # anchor session s1 to now
SYNCUP_SESSION=s2 b inbox >/dev/null   # anchor session s2 to now
a publish "$CH" "PERSESSION broadcast" >/dev/null
O1="$(SYNCUP_SESSION=s1 b inbox)"
O2="$(SYNCUP_SESSION=s2 b inbox)"
echo "$O1" | grep -q "PERSESSION" && ok "session s1 saw broadcast" || bad "s1 missed it: $O1"
echo "$O2" | grep -q "PERSESSION" && ok "session s2 saw it independently" || bad "s2 missed it: $O2"
O1b="$(SYNCUP_SESSION=s1 b inbox)"
echo "$O1b" | grep -q "no new updates" && ok "s1 cursor advanced (no re-delivery)" || bad "s1 re-delivered: $O1b"

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
