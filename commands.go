package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	topicPrefix   = "syncup."          // namespaces our topics on a shared cluster
	registryTopic = "syncup._registry" // compacted catalog of channels
	groupPrefix   = "syncup."          // consumer group is syncup.<user>
	schemaVersion = "1"
)

// Message is the payload of one update, stored as JSON in a topic record.
type Message struct {
	ID      string            `json:"id"`
	Topic   string            `json:"topic"`
	Author  string            `json:"author"`
	TS      string            `json:"ts"`
	Type    string            `json:"type"`
	Body    string            `json:"body"`
	Refs    map[string]string `json:"refs,omitempty"`
	ReplyTo string            `json:"reply_to,omitempty"`
}

// TopicMeta is a registry record describing one channel.
type TopicMeta struct {
	Topic       string `json:"topic"`
	Description string `json:"description"`
	Creator     string `json:"creator"`
	CreatedAt   string `json:"created_at"`
}

// canon returns the fully-qualified topic name (adds the prefix if missing).
func canon(name string) string {
	if strings.HasPrefix(name, topicPrefix) {
		return name
	}
	return topicPrefix + name
}

// short strips the prefix for display.
func short(topic string) string { return strings.TrimPrefix(topic, topicPrefix) }

func newID() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%013x%x", time.Now().UnixMilli(), b)
}

func now() string { return time.Now().UTC().Format(time.RFC3339) }

func since(ts string) string {
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return ts
	}
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}

// readRegistry replays the compacted registry topic into a current view of channels.
func readRegistry(ctx context.Context, cfg *Config) (map[string]TopicMeta, error) {
	adm, closeAdm, err := admin(cfg)
	if err != nil {
		return nil, err
	}
	start, end, err := bounds(ctx, adm, registryTopic)
	closeAdm()
	if errors.Is(err, errNoTopic) {
		return map[string]TopicMeta{}, nil
	}
	if err != nil {
		return nil, err
	}
	recs, err := fetchFrom(ctx, cfg, registryTopic, start, end)
	if err != nil {
		return nil, err
	}
	out := map[string]TopicMeta{}
	for _, r := range recs {
		key := string(r.Key)
		if len(r.Value) == 0 { // tombstone -> channel retired
			delete(out, key)
			continue
		}
		var m TopicMeta
		if json.Unmarshal(r.Value, &m) == nil {
			out[key] = m
		}
	}
	return out, nil
}

func cmdInit(args []string) error {
	fs := newFlagSet("init")
	brokers := fs.String("brokers", "", "comma-separated Kafka bootstrap brokers")
	user := fs.String("user", "", "your username (defaults to $USER)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg := &Config{}
	if existing, err := loadConfig(); err == nil {
		cfg = existing
	}
	b := *brokers
	if b == "" {
		b = os.Getenv("SYNCUP_BROKERS") // shared .env / make bootstrap
	}
	if b != "" {
		cfg.Brokers = strings.Split(b, ",")
	}
	if *user != "" {
		cfg.User = *user
	}
	if cfg.User == "" {
		cfg.User = defaultUser()
	}
	if len(cfg.Brokers) == 0 || cfg.User == "" {
		return errors.New("need --brokers and --user (or $USER)")
	}
	if err := cfg.save(); err != nil {
		return err
	}
	fmt.Printf("configured: user=%s, brokers=%d, config=%s\n", cfg.User, len(cfg.Brokers), configPath())
	return nil
}

func cmdCreate(ctx context.Context, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: syncup create <channel> [description]")
	}
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	topic := canon(args[0])
	desc := strings.Join(args[1:], " ")

	adm, closeAdm, err := admin(cfg)
	if err != nil {
		return err
	}
	defer closeAdm()
	compact := "compact"
	if err := ensureTopic(ctx, adm, cfg, registryTopic, map[string]*string{"cleanup.policy": &compact}); err != nil {
		return err
	}
	if err := ensureTopic(ctx, adm, cfg, topic, nil); err != nil {
		return err
	}
	meta := TopicMeta{Topic: topic, Description: desc, Creator: cfg.User, CreatedAt: now()}
	val, _ := json.Marshal(meta)
	if err := produce(ctx, cfg, &kgo.Record{Topic: registryTopic, Key: []byte(topic), Value: val}); err != nil {
		return err
	}
	fmt.Printf("created channel %q\n", short(topic))
	return nil
}

func cmdList(ctx context.Context, args []string) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	metas, err := readRegistry(ctx, cfg)
	if err != nil {
		return err
	}
	if len(metas) == 0 {
		fmt.Println("no channels yet — create one: syncup create <name> \"description\"")
		return nil
	}
	names := make([]string, 0, len(metas))
	for k := range metas {
		names = append(names, k)
	}
	sort.Strings(names)

	fmt.Printf("📡 syncup channels (you: %s)\n", cfg.User)
	for _, t := range names {
		mark := " "
		if cfg.subscribed(t) {
			mark = "✓"
		}
		m := metas[t]
		fmt.Printf("  %s %-18s %-34s (%s)\n", mark, short(t), m.Description, m.Creator)
	}
	return nil
}

func cmdJoin(ctx context.Context, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: syncup join <channel>")
	}
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	topic := canon(args[0])

	adm, closeAdm, err := admin(cfg)
	if err != nil {
		return err
	}
	defer closeAdm()
	_, end, err := bounds(ctx, adm, topic)
	if errors.Is(err, errNoTopic) {
		return fmt.Errorf("channel %q does not exist (create it first)", short(topic))
	}
	if err != nil {
		return err
	}
	// On first join, start from now. If we already track an offset for this
	// channel, leave it untouched so re-joining never skips unread messages.
	off, err := committedOffset(ctx, adm, cfg.group(), topic)
	if err != nil {
		return err
	}
	if off < 0 {
		if err := commit(ctx, adm, cfg.group(), topic, end); err != nil {
			return err
		}
	}
	if !cfg.subscribed(topic) {
		cfg.Subscriptions = append(cfg.Subscriptions, topic)
		if err := cfg.save(); err != nil {
			return err
		}
	}
	fmt.Printf("joined %q — you'll see new updates from now on\n", short(topic))
	return nil
}

func cmdLeave(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: syncup leave <channel>")
	}
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	topic := canon(args[0])
	kept := cfg.Subscriptions[:0]
	for _, t := range cfg.Subscriptions {
		if t != topic {
			kept = append(kept, t)
		}
	}
	cfg.Subscriptions = kept
	if err := cfg.save(); err != nil {
		return err
	}
	fmt.Printf("left %q\n", short(topic))
	return nil
}

func cmdPublish(ctx context.Context, args []string) error {
	if len(args) < 2 {
		return errors.New("usage: syncup publish <channel> <message...>")
	}
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	topic := canon(args[0])
	body := strings.Join(args[1:], " ")
	msg := Message{ID: newID(), Topic: topic, Author: cfg.User, TS: now(), Type: "update", Body: body}
	val, _ := json.Marshal(msg)
	rec := &kgo.Record{
		Topic: topic,
		Key:   []byte(cfg.User),
		Value: val,
		Headers: []kgo.RecordHeader{
			{Key: "type", Value: []byte(msg.Type)},
			{Key: "author", Value: []byte(cfg.User)},
			{Key: "schema", Value: []byte(schemaVersion)},
		},
	}
	if err := produce(ctx, cfg, rec); err != nil {
		return err
	}
	fmt.Printf("posted to %q\n", short(topic))
	return nil
}

func cmdInbox(ctx context.Context, args []string) error {
	quiet := false
	only := ""
	for _, a := range args {
		switch {
		case a == "--quiet" || a == "-q":
			quiet = true
		case strings.HasPrefix(a, "-"):
			// ignore unknown flags
		default:
			only = a
		}
	}
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	// Read all subscribed channels, or just one if named.
	topics := cfg.Subscriptions
	if only != "" {
		t := canon(only)
		if !cfg.subscribed(t) {
			return fmt.Errorf("not joined to %q (run: syncup join %s)", short(t), short(t))
		}
		topics = []string{t}
	}

	adm, closeAdm, err := admin(cfg)
	if err != nil {
		return err
	}
	defer closeAdm()

	type block struct {
		topic string
		msgs  []Message
	}
	var blocks []block
	commits := map[string]int64{}

	for _, topic := range topics {
		start, end, err := bounds(ctx, adm, topic)
		if errors.Is(err, errNoTopic) {
			continue
		}
		if err != nil {
			return err
		}
		off, err := committedOffset(ctx, adm, cfg.group(), topic)
		if err != nil {
			return err
		}
		if off < 0 {
			// First time this group (e.g. a new session) sees the channel:
			// anchor to "now" so we never replay history, and show nothing yet.
			if err := commit(ctx, adm, cfg.group(), topic, end); err != nil {
				return err
			}
			continue
		}
		if off < start {
			off = start // committed offset fell behind retention; resume from earliest
		}
		recs, err := fetchFrom(ctx, cfg, topic, off, end)
		if err != nil {
			return err
		}
		if len(recs) == 0 {
			continue
		}
		b := block{topic: topic}
		for _, r := range recs {
			var m Message
			if json.Unmarshal(r.Value, &m) == nil {
				b.msgs = append(b.msgs, m)
			}
		}
		blocks = append(blocks, b)
		commits[topic] = end
	}

	if len(blocks) == 0 {
		if !quiet {
			fmt.Println("no new updates")
		}
		return nil
	}

	for _, b := range blocks {
		fmt.Printf("📬 New on %s (%d):\n", short(b.topic), len(b.msgs))
		for _, m := range b.msgs {
			fmt.Printf("  • %s, %s: %s\n", m.Author, since(m.TS), m.Body)
		}
	}
	// Commit only after printing (delivery into context = read).
	for topic, off := range commits {
		if err := commit(ctx, adm, cfg.group(), topic, off); err != nil {
			return err
		}
	}
	return nil
}

func cmdDelete(ctx context.Context, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: syncup delete <channel>")
	}
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	topic := canon(args[0])
	adm, closeAdm, err := admin(cfg)
	if err != nil {
		return err
	}
	defer closeAdm()
	if _, err := adm.DeleteTopics(ctx, topic); err != nil {
		return err
	}
	// Tombstone the registry entry.
	if err := produce(ctx, cfg, &kgo.Record{Topic: registryTopic, Key: []byte(topic), Value: nil}); err != nil {
		return err
	}
	fmt.Printf("deleted channel %q\n", short(topic))
	return nil
}

// muxer abstracts the terminal multiplexer we inject into (tmux or herdr).
type muxer struct {
	name   string
	pane   string
	exists func(pane string) bool // nil if liveness can't be checked
	inject func(pane, text string) error
}

// tmux backend: `tmux send-keys`.
func tmuxMuxer(pane string) *muxer {
	return &muxer{
		name: "tmux", pane: pane,
		exists: func(p string) bool {
			return exec.Command("tmux", "display-message", "-p", "-t", p, "#{pane_id}").Run() == nil
		},
		inject: func(p, text string) error {
			if err := exec.Command("tmux", "send-keys", "-t", p, "-l", "--", text).Run(); err != nil {
				return err
			}
			return exec.Command("tmux", "send-keys", "-t", p, "Enter").Run()
		},
	}
}

// herdr backend: `herdr pane send-text` + `send-keys enter`.
func herdrMuxer(pane string) *muxer {
	return &muxer{
		name: "herdr", pane: pane,
		exists: nil, // rely on the SessionEnd hook / inject failures
		inject: func(p, text string) error {
			if err := exec.Command("herdr", "pane", "send-text", p, text).Run(); err != nil {
				return err
			}
			return exec.Command("herdr", "pane", "send-keys", p, "enter").Run()
		},
	}
}

// detectMuxer picks the injection backend: explicit flags win, then env vars
// (herdr's HERDR_PANE_ID or tmux's TMUX_PANE) set inside the session.
func detectMuxer(tmuxFlag, herdrFlag string) *muxer {
	switch {
	case herdrFlag != "":
		return herdrMuxer(herdrFlag)
	case tmuxFlag != "":
		return tmuxMuxer(tmuxFlag)
	case os.Getenv("HERDR_PANE_ID") != "":
		return herdrMuxer(os.Getenv("HERDR_PANE_ID"))
	case os.Getenv("SYNCUP_TMUX") != "":
		return tmuxMuxer(os.Getenv("SYNCUP_TMUX"))
	case os.Getenv("TMUX_PANE") != "":
		return tmuxMuxer(os.Getenv("TMUX_PANE"))
	}
	return nil
}

// cmdWatch runs as a daemon: it live-tails the subscribed channels and types each
// new message into the session's pane (tmux or herdr), so updates reach the agent
// without you prompting. It shares the session consumer group with the inbox hook,
// so every message is delivered exactly once (whichever path reads it first).
func cmdWatch(args []string) error {
	var tmuxFlag, herdrFlag string
	interval := 2 * time.Second
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--tmux":
			if i+1 < len(args) {
				tmuxFlag = args[i+1]
				i++
			}
		case "--herdr":
			if i+1 < len(args) {
				herdrFlag = args[i+1]
				i++
			}
		case "--interval":
			if i+1 < len(args) {
				if d, err := time.ParseDuration(args[i+1]); err == nil {
					interval = d
				}
				i++
			}
		}
	}
	m := detectMuxer(tmuxFlag, herdrFlag)
	if m == nil {
		return errors.New("no tmux or herdr pane detected; run inside one, or pass --tmux/--herdr <pane>")
	}
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	for {
		if m.exists != nil && !m.exists(m.pane) {
			return nil // pane closed — the session is gone, so stop
		}
		if err := watchOnce(cfg, m); err != nil {
			fmt.Fprintln(os.Stderr, "watch:", err)
		}
		time.Sleep(interval)
	}
}

// watchOnce delivers any new messages on subscribed channels into the pane.
func watchOnce(cfg *Config, m *muxer) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	adm, closeAdm, err := admin(cfg)
	if err != nil {
		return err
	}
	defer closeAdm()

	for _, topic := range cfg.Subscriptions {
		start, end, err := bounds(ctx, adm, topic)
		if errors.Is(err, errNoTopic) {
			continue
		}
		if err != nil {
			return err
		}
		off, err := committedOffset(ctx, adm, cfg.group(), topic)
		if err != nil {
			return err
		}
		if off < 0 {
			if err := commit(ctx, adm, cfg.group(), topic, end); err != nil {
				return err
			}
			continue
		}
		if off < start {
			off = start
		}
		recs, err := fetchFrom(ctx, cfg, topic, off, end)
		if err != nil {
			return err
		}
		for _, r := range recs {
			var msg Message
			if json.Unmarshal(r.Value, &msg) != nil {
				continue
			}
			if err := m.inject(m.pane, fmt.Sprintf("[syncup] %s on %s: %s", msg.Author, short(topic), oneLine(msg.Body))); err != nil {
				return err
			}
		}
		if len(recs) > 0 {
			if err := commit(ctx, adm, cfg.group(), topic, end); err != nil {
				return err
			}
		}
	}
	return nil
}

// oneLine collapses whitespace so an injected message can't submit early.
func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }
