#!/usr/bin/env bash
# Claude Code UserPromptSubmit hook — drain this session's unread updates into
# context before the agent sees your prompt. Fails open: never blocks a prompt.
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

syncup inbox --quiet 2>/dev/null || true
exit 0
