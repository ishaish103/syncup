#!/usr/bin/env bash
# Claude Code UserPromptSubmit hook — drain unread updates into context before
# the agent sees your prompt. Fails open: never blocks a prompt.
export SYNCUP_TIMEOUT="${SYNCUP_TIMEOUT:-5}"
command -v syncup >/dev/null 2>&1 || exit 0
syncup inbox --quiet 2>/dev/null || true
exit 0
