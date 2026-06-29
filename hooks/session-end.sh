#!/usr/bin/env bash
# Claude Code SessionEnd hook — stop this session's push watcher. Fails open.
# (The watcher also self-exits when its tmux pane disappears, so this is belt-and-suspenders.)
input=""
sid=""
[ -t 0 ] || input="$(cat 2>/dev/null)"
if [ -n "$input" ]; then
  sid="$(printf '%s' "$input" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("session_id",""))' 2>/dev/null)"
  [ -z "$sid" ] && sid="$(printf '%s' "$input" | grep -o '"session_id"[^,]*' | head -1 | sed -E 's/.*:[[:space:]]*"([^"]*)".*/\1/')"
fi
[ -z "$sid" ] && exit 0
pidf="/tmp/syncup-watch-$sid.pid"
if [ -f "$pidf" ]; then
  kill "$(cat "$pidf" 2>/dev/null)" 2>/dev/null
  rm -f "$pidf"
fi
exit 0
