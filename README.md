# syncup

[![CI](https://github.com/ishaish103/syncup/actions/workflows/ci.yml/badge.svg)](https://github.com/ishaish103/syncup/actions/workflows/ci.yml)

Topic-based updates between AI agent sessions, over Kafka.

When teammates work alongside fast-moving coding agents, a daily standup can't keep up. `syncup` lets each person tell their agent to **publish** a short update to a channel, and every teammate's agent **automatically catches up** on the channels it follows — injected as context on their next prompt. No new infrastructure beyond a Kafka cluster you already run.

```
you ──"post to api: auth endpoint moved to /v2, update clients before deploy"──▶ agent
                                                                            │
                                                       syncup publish api "..."
                                                                            ▼
                                                            Kafka topic  syncup.api
                                                                            │
teammate's next prompt ◀── hook injects: "📬 New on api: auth endpoint moved to /v2…"
```

## How it works

- **Publish** — your agent runs `syncup publish <channel> <msg>` when you tell it to. You decide what and when; nothing is automatic on the send side.
- **Consume** — a Claude Code `UserPromptSubmit` hook runs `syncup inbox` before each prompt and prepends any unread messages from your subscribed channels. Zero effort to stay current.
- **Discover** — a `SessionStart` hook lists available channels so you can `join` the ones you care about.
- **Storage** — one Kafka topic per channel (`syncup.<name>`), plus a compacted `syncup._registry` topic that acts as the channel catalog. "Unread" is tracked per session via a Kafka consumer group — no database, no server.

---

# Setup

**Requirements:** Go 1.25+, and network access to your Kafka brokers.

Follow these steps in order. The `syncup` command does not exist until step 2
(`make install` is what builds and installs it).

**1. Clone and enter the repo**
```sh
git clone https://github.com/ishaish103/syncup.git
cd syncup
```

**2. Install**

Homebrew (recommended):
```sh
brew install ishaish103/tap/syncup
```
Or build from source: `make install` (compiles `syncup` to `~/.local/bin`).

**3. Configure**
```sh
syncup init --brokers b-1:9092,b-2:9092,b-3:9092 --user alice
```
- `--brokers` — your broker list, comma-separated (**required**).
- `--user` — the name teammates see on your updates (defaults to `$USER`).

This writes `~/.config/syncup/config.json`. Brokers and config path can also be
set via the `SYNCUP_BROKERS` and `SYNCUP_CONFIG` environment variables.

**4. Verify** (open a new shell, or run `hash -r` to refresh PATH first)
```sh
syncup list
```
If you get `command not found`, `~/.local/bin` isn't on your `PATH` — add
`export PATH="$HOME/.local/bin:$PATH"` to your `~/.zshrc` and reopen the shell.

**5. Integrate with your agent** — for Claude Code, just run `syncup hooks install` (details below). Other agents: see [Agent integration](#agent-integration).

## Agent integration

`syncup` is just a CLI, so **any agent that can run shell commands can use it** — to publish, run `syncup publish …`; to catch up, run `syncup inbox`. The only agent-specific part is how updates get surfaced *automatically*. Pick the section for your tool.

### Claude Code (automatic, via hooks)

One command writes the hook scripts and wires them into `~/.claude/settings.json`:

```sh
syncup hooks install
```

It's idempotent and leaves any non-syncup hooks untouched; `syncup hooks path` prints where they were written. It installs three hooks:

- `SessionStart` — shows the channel catalog and anchors this session's cursor
- `UserPromptSubmit` — injects unread updates before each prompt
- `SessionEnd` — stops the push watcher (below)

All **fail open** — if Kafka or the CLI is unavailable they print nothing and never block your session, bounded by `SYNCUP_TIMEOUT` (default 5s).

<details>
<summary>Configure manually instead</summary>

Add to `~/.claude/settings.json` (absolute paths to the installed scripts — see `syncup hooks path`):

```json
{
  "hooks": {
    "SessionStart":     [ { "hooks": [ { "type": "command", "command": "/abs/path/hooks/session-start.sh" } ] } ],
    "UserPromptSubmit": [ { "hooks": [ { "type": "command", "command": "/abs/path/hooks/user-prompt-submit.sh" } ] } ],
    "SessionEnd":       [ { "hooks": [ { "type": "command", "command": "/abs/path/hooks/session-end.sh" } ] } ]
  }
}
```
</details>

### Push mode (tmux or herdr) — receive updates without typing

By default updates are *pulled* on each prompt, so a teammate's message only appears when **you** type. To have updates **pushed** into a session even while you're not typing, **run your agent inside a multiplexer** — tmux or [herdr](https://github.com/ogulcancelik/herdr):

```sh
tmux new -s work && claude    # tmux …
herdr                         # … or herdr (then launch your agent in a pane)
```

That's the whole requirement. When the agent starts inside one, the `SessionStart` hook launches `syncup watch` in the background, which live-tails your channels and types each new update straight into your pane:

```
[syncup] amir on api: deploy is green, ship it
```

so the agent reacts hands-free. `syncup watch` **auto-detects** the multiplexer from its env (`TMUX_PANE` → `tmux send-keys`; `HERDR_PANE_ID` → `herdr pane send-text`). It shares the session's consumer group with the pull hook, so every message is delivered exactly once, and it stops on `SessionEnd`.

- **Not in a multiplexer?** Nothing breaks — you silently fall back to pull-on-prompt.
- **Manual control:** `syncup watch --tmux <pane>` or `syncup watch --herdr <pane>`.
- **Caveat:** every update becomes an agent turn, so a chatty channel will interrupt you and spend tokens.

### Codex CLI

Codex has no per-prompt hook, so instruct the agent via `AGENTS.md` (global `~/.codex/AGENTS.md`, or per-repo). The agent then runs the CLI itself:

```md
## syncup — team updates
- At the start of a task, run `syncup inbox` and surface anything new.
- When I ask to share/post an update, run `syncup publish <channel> "<message>"`.
- Discover channels with `syncup list`; follow one with `syncup join <channel>`.
```

### Other agents (Cursor, Cline, Aider, …) / manual

- **With a rules/instructions file** (e.g. Cursor rules, `CLAUDE.md`, a system prompt): paste the same instruction block as above.
- **With a pre-prompt/pre-run hook**: wire `syncup inbox` into it, the same way the Claude Code hook does.
- **No automation**: just run `syncup inbox` yourself whenever you want to catch up, and `syncup publish …` to post.

---

# Usage

Day-to-day you just talk to your agent ("post to api: …", "anything new?") and the hooks do the rest. Under the hood it's these commands — your agent runs them, or you can run them directly:

| Command | Description |
|---|---|
| `syncup create <channel> [description]` | Create a channel |
| `syncup list` | List channels (`✓` = joined) |
| `syncup join <channel>` | Subscribe — you'll see updates posted *from now on* |
| `syncup leave <channel>` | Unsubscribe |
| `syncup publish <channel> <message...>` | Post an update |
| `syncup inbox [channel] [--quiet]` | Read unread updates (all channels, or one) |
| `syncup watch [--tmux <pane>]` | Daemon: push new updates into a tmux pane |
| `syncup delete <channel>` | Retire a channel |

```sh
syncup create api "backend API work"
syncup join api
syncup publish api "auth endpoint moved to /v2, update clients before deploy (PR #482)"
syncup inbox
# 📬 New on api (1):
#   • alice, 2m ago: auth endpoint moved to /v2, update clients before deploy (PR #482)
```

## Message schema

Each update is one Kafka record: headers carry `type`, `author`, `schema` for cheap filtering; the JSON value carries the payload.

```jsonc
{
  "id":     "0192f...",            // sortable id, for dedup
  "topic":  "syncup.api",
  "author": "alice",
  "ts":     "2026-06-29T14:02:11Z",
  "type":   "update",
  "body":   "auth endpoint moved to /v2…",
  "refs":   { "pr": "482" }         // optional structured links
}
```

## Design notes

- **From-now semantics.** On *first* join, `syncup` records the channel's current end offset, so you only ever see messages posted after you joined. Re-joining a channel you already follow leaves your position untouched (it never skips unread). History replay is a deliberate non-feature for now.
- **Per-session offsets.** Each agent session gets its own cursor (consumer group `syncup.<user>.<session-id>`, with the session id supplied by the hooks), so **every open session catches up independently** — an update shows up in all of them, not just whichever reads first. Without a session id (manual CLI use), it falls back to a per-user cursor (`syncup.<user>`). Per-session groups self-expire on the broker, so there's nothing to clean up.
- **Namespacing.** Every topic is prefixed `syncup.`, so the tool coexists safely on a shared cluster and `list` only ever shows channels from its own registry.
- **Resilience.** Group-coordinator hiccups (fresh cluster, failover) are retried; the Claude Code hooks fail open so a Kafka outage never blocks your session.

## Development

```sh
go test ./...                       # unit tests (no Kafka needed)
BROKERS=localhost:9092 make test    # end-to-end against a real broker
```

CI runs unit tests, `go vet`, and a `gofmt` check on every push, plus the full
end-to-end suite against a throwaway Kafka container.

## License

MIT
