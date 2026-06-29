#!/usr/bin/env bash
# Claude Code SessionStart hook — print the channel catalog and anchor this
# session's read position to "now". Fails open: never blocks a session.
export SYNCUP_TIMEOUT="${SYNCUP_TIMEOUT:-5}"
command -v syncup >/dev/null 2>&1 || exit 0

# Claude passes hook JSON (incl. session_id) on stdin. Give each session its own
# inbox cursor so every open session catches up independently. Skip reading if
# stdin is a terminal (manual run) so we don't block.
input=""
[ -t 0 ] || input="$(cat 2>/dev/null)"
if [ -n "$input" ]; then
  sid="$(printf '%s' "$input" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("session_id",""))' 2>/dev/null)"
  [ -z "$sid" ] && sid="$(printf '%s' "$input" | grep -o '"session_id"[^,]*' | head -1 | sed -E 's/.*:[[:space:]]*"([^"]*)".*/\1/')"
  [ -n "$sid" ] && export SYNCUP_SESSION="$sid"
fi

syncup list 2>/dev/null || true
syncup inbox --quiet 2>/dev/null || true   # anchor this session's cursor to now
cat <<'HINT'

syncup is available for cross-team updates. When the user asks to post/share
an update, run: syncup publish <channel> "<message>". To follow a channel, run:
syncup join <channel>. Channel names are shown above (omit the "syncup." prefix).
HINT
exit 0
