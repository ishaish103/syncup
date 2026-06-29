# syncup

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
- **Storage** — one Kafka topic per channel (`syncup.<name>`), plus a compacted `syncup._registry` topic that acts as the channel catalog. "Unread" is tracked per user via a Kafka consumer group (`syncup.<user>`) — no database, no server.

## Setup

**Requirements:** Go 1.25+, and network access to your Kafka brokers.

Follow these steps in order. The `syncup` command does not exist until step 2
(`make install` is what builds and installs it).

**1. Clone and enter the repo**
```sh
git clone https://github.com/ishaish103/syncup.git
cd syncup
```

**2. Build and install**
```sh
make install
```
This compiles `syncup` and installs it to `~/.local/bin`.

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

**5. Wire up the Claude Code hooks** — see [Claude Code integration](#claude-code-integration) below.

## Usage

| Command | Description |
|---|---|
| `syncup create <channel> [description]` | Create a channel |
| `syncup list` | List channels (`✓` = joined) |
| `syncup join <channel>` | Subscribe — you'll see updates posted *from now on* |
| `syncup leave <channel>` | Unsubscribe |
| `syncup publish <channel> <message...>` | Post an update |
| `syncup inbox [--quiet]` | Read unread updates |
| `syncup delete <channel>` | Retire a channel |

```sh
syncup create api "backend API work"
syncup join api
syncup publish api "auth endpoint moved to /v2, update clients before deploy (PR #482)"
syncup inbox
# 📬 New on api (1):
#   • alice, 2m ago: auth endpoint moved to /v2, update clients before deploy (PR #482)
```

## Claude Code integration

Add the hooks to `~/.claude/settings.json` (use absolute paths):

```json
{
  "hooks": {
    "SessionStart": [
      { "hooks": [ { "type": "command", "command": "/abs/path/syncup/hooks/session-start.sh" } ] }
    ],
    "UserPromptSubmit": [
      { "hooks": [ { "type": "command", "command": "/abs/path/syncup/hooks/user-prompt-submit.sh" } ] }
    ]
  }
}
```

Both hooks **fail open** — if Kafka or the CLI is unavailable they print nothing and never block your session. They bound their runtime with `SYNCUP_TIMEOUT` (default 5s in the hooks).

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

- **From-now semantics.** `join` commits the channel's current end offset, so you only ever see messages posted after you joined. (History replay is a deliberate non-feature for now.)
- **Per-user offsets.** All your sessions share the consumer group `syncup.<user>`, so "caught up" follows you across machines. Two *simultaneous* sessions can race a commit — rare and low-stakes.
- **Namespacing.** Every topic is prefixed `syncup.`, so the tool coexists safely on a shared cluster and `list` only ever shows channels from its own registry.

## License

MIT
