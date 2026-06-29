package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
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
		if off < 0 || off < start {
			off = start
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
