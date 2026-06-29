package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// Config is the per-user local state, stored as JSON in ~/.config/syncup/config.json.
type Config struct {
	Brokers       []string `json:"brokers"`
	User          string   `json:"user"`
	Subscriptions []string `json:"subscriptions"`
}

func configPath() string {
	if p := os.Getenv("SYNCUP_CONFIG"); p != "" {
		return p
	}
	dir, _ := os.UserConfigDir()
	return filepath.Join(dir, "syncup", "config.json")
}

func loadConfig() (*Config, error) {
	b, err := os.ReadFile(configPath())
	if err != nil {
		return nil, errors.New("not configured; run: syncup init --brokers <b1,b2> --user <name>")
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	if v := os.Getenv("SYNCUP_BROKERS"); v != "" {
		c.Brokers = strings.Split(v, ",")
	}
	if len(c.Brokers) == 0 || c.User == "" {
		return nil, errors.New("config missing brokers or user; run: syncup init")
	}
	return &c, nil
}

func (c *Config) save() error {
	p := configPath()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(c, "", "  ")
	return os.WriteFile(p, b, 0o644)
}

func (c *Config) subscribed(topic string) bool {
	for _, t := range c.Subscriptions {
		if t == topic {
			return true
		}
	}
	return false
}

// group is the stable per-user Kafka consumer group used for offset tracking.
func (c *Config) group() string { return groupPrefix + c.User }
