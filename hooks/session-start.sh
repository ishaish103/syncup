#!/usr/bin/env bash
# Claude Code SessionStart hook — print the channel catalog into context.
# Fails open: never blocks a session if Kafka or the CLI is unavailable.
export SYNCUP_TIMEOUT="${SYNCUP_TIMEOUT:-5}"
command -v syncup >/dev/null 2>&1 || exit 0
syncup list 2>/dev/null || true
cat <<'HINT'

syncup is available for cross-team updates. When the user asks to post/share
an update, run: syncup publish <channel> "<message>". To follow a channel, run:
syncup join <channel>. Channel names are shown above (omit the "syncup." prefix).
HINT
exit 0
