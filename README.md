# syncup

Topic-based updates between AI agent sessions, over Kafka.

When teammates work alongside fast-moving coding agents, a daily standup can't keep up. `syncup` lets each person tell their agent to **publish** a short update to a channel, and every teammate's agent **automatically catches up** on the channels it follows — injected as context on their next prompt. No new infrastructure beyond a Kafka cluster you already run.

```
you ──"post to collector: retry schema changed, re-read before deploy"──▶ agent
                                                                            │
                                                       syncup publish collector "..."
                                                                            ▼
                                                            Kafka topic  syncup.collector
                                                                            │
teammate's next prompt ◀── hook injects: "📬 New on collector: retry schema changed…"
```

## How it works

- **Publish** — your agent runs `syncup publish <channel> <msg>` when you tell it to. You decide what and when; nothing is automatic on the send side.
- **Consume** — a Claude Code `UserPromptSubmit` hook runs `syncup inbox` before each prompt and prepends any unread messages from your subscribed channels. Zero effort to stay current.
- **Discover** — a `SessionStart` hook lists available channels so you can `join` the ones you care about.
- **Storage** — one Kafka topic per channel (`syncup.<name>`), plus a compacted `syncup._registry` topic that acts as the channel catalog. "Unread" is tracked per user via a Kafka consumer group (`syncup.<user>`) — no database, no server.

## Setup

Share a `.env` with your team's brokers, then each person runs one command:

```sh
cp .env.example .env          # set SYNCUP_BROKERS (do this once, share with the team)
make bootstrap                # builds, installs to ~/.local/bin, runs `syncup init`
```

`bootstrap` reads brokers from `.env` and your username from `$USER` (override with
`SYNCUP_USER`). To configure manually instead:

```sh
syncup init --brokers b-1:9092,b-2:9092,b-3:9092 --user alice
```

Brokers and config path can also be set via `SYNCUP_BROKERS` and `SYNCUP_CONFIG`.

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
syncup create collector "collector pipeline + HPA work"
syncup join collector
syncup publish collector "retry schema changed: max_retries now under backoff (PR #482)"
syncup inbox
# 📬 New on collector (1):
#   • alice, 2m ago: retry schema changed: max_retries now under backoff (PR #482)
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
  "topic":  "syncup.collector",
  "author": "alice",
  "ts":     "2026-06-29T14:02:11Z",
  "type":   "update",
  "body":   "retry schema changed…",
  "refs":   { "pr": "482" }         // optional structured links
}
```

## Design notes

- **From-now semantics.** `join` commits the channel's current end offset, so you only ever see messages posted after you joined. (History replay is a deliberate non-feature for now.)
- **Per-user offsets.** All your sessions share the consumer group `syncup.<user>`, so "caught up" follows you across machines. Two *simultaneous* sessions can race a commit — rare and low-stakes.
- **Namespacing.** Every topic is prefixed `syncup.`, so the tool coexists safely on a shared cluster and `list` only ever shows channels from its own registry.

## License

MIT
