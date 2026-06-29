package main

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestCanonShort(t *testing.T) {
	cases := []struct{ in, canon, short string }{
		{"api", "syncup.api", "api"},
		{"syncup.api", "syncup.api", "api"},
		{"a.b", "syncup.a.b", "a.b"},
	}
	for _, c := range cases {
		if got := canon(c.in); got != c.canon {
			t.Errorf("canon(%q) = %q, want %q", c.in, got, c.canon)
		}
		if got := short(c.canon); got != c.short {
			t.Errorf("short(%q) = %q, want %q", c.canon, got, c.short)
		}
	}
}

func TestSince(t *testing.T) {
	mk := func(d time.Duration) string { return time.Now().Add(-d).UTC().Format(time.RFC3339) }
	cases := []struct {
		ts, want string
	}{
		{mk(10 * time.Second), "just now"},
		{mk(5 * time.Minute), "5m ago"},
		{mk(2 * time.Hour), "2h ago"},
		{mk(3 * 24 * time.Hour), "3d ago"},
		{"not-a-timestamp", "not-a-timestamp"},
	}
	for _, c := range cases {
		if got := since(c.ts); got != c.want {
			t.Errorf("since(%q) = %q, want %q", c.ts, got, c.want)
		}
	}
}

func TestNewIDUniqueAndSortable(t *testing.T) {
	a := newID()
	if len(a) < 13 {
		t.Fatalf("newID too short: %q", a)
	}
	// Ids carry a millisecond time prefix; one made >1ms later must sort after.
	time.Sleep(2 * time.Millisecond)
	b := newID()
	if a == b {
		t.Fatalf("newID returned duplicate: %q", a)
	}
	if a >= b {
		t.Errorf("expected time-ordered ids, got %q then %q", a, b)
	}
}

func TestRF(t *testing.T) {
	cases := []struct {
		brokers int
		want    int16
	}{{1, 1}, {2, 2}, {3, 3}, {5, 3}}
	for _, c := range cases {
		cfg := &Config{Brokers: make([]string, c.brokers)}
		if got := rf(cfg); got != c.want {
			t.Errorf("rf(%d brokers) = %d, want %d", c.brokers, got, c.want)
		}
	}
}

func TestConfigRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("SYNCUP_CONFIG", path)
	t.Setenv("SYNCUP_BROKERS", "") // don't let an env override leak in

	want := &Config{Brokers: []string{"b1:9092", "b2:9092"}, User: "alice", Subscriptions: []string{"syncup.api"}}
	if err := want.save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if got.User != want.User || len(got.Brokers) != 2 || len(got.Subscriptions) != 1 {
		t.Errorf("round trip mismatch: %+v", got)
	}
}

func TestConfigBrokersEnvOverride(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("SYNCUP_CONFIG", path)
	t.Setenv("SYNCUP_BROKERS", "")

	base := &Config{Brokers: []string{"file:9092"}, User: "alice"}
	if err := base.save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	t.Setenv("SYNCUP_BROKERS", "env1:9092,env2:9092")
	got, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if len(got.Brokers) != 2 || got.Brokers[0] != "env1:9092" {
		t.Errorf("env override not applied: %+v", got.Brokers)
	}
}

func TestConfigRequiresBrokersAndUser(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("SYNCUP_CONFIG", path)
	t.Setenv("SYNCUP_BROKERS", "")

	(&Config{User: "alice"}).save() // no brokers
	if _, err := loadConfig(); err == nil {
		t.Error("expected error when brokers missing")
	}
}

func TestSubscribedAndGroup(t *testing.T) {
	cfg := &Config{User: "bob", Subscriptions: []string{"syncup.api", "syncup.web"}}
	if !cfg.subscribed("syncup.api") {
		t.Error("expected subscribed to syncup.api")
	}
	if cfg.subscribed("syncup.other") {
		t.Error("did not expect subscription to syncup.other")
	}
	if cfg.group() != "syncup.bob" {
		t.Errorf("group() = %q, want syncup.bob", cfg.group())
	}
}

func TestMessageJSONRoundTrip(t *testing.T) {
	in := Message{ID: "x1", Topic: "syncup.api", Author: "alice", TS: now(), Type: "update", Body: "hi", Refs: map[string]string{"pr": "42"}}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out Message
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Body != "hi" || out.Author != "alice" || out.Refs["pr"] != "42" {
		t.Errorf("round trip mismatch: %+v", out)
	}
}
