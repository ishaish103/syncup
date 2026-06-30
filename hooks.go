package main

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Hook scripts are embedded so the binary is self-contained — `syncup hooks
// install` writes them out, regardless of how syncup itself was installed
// (brew, go install, make install).
//
//go:embed hooks/session-start.sh hooks/user-prompt-submit.sh hooks/session-end.sh
var hooksFS embed.FS

// hookFiles maps each Claude Code hook event to its embedded script.
var hookFiles = []struct{ event, file string }{
	{"SessionStart", "session-start.sh"},
	{"UserPromptSubmit", "user-prompt-submit.sh"},
	{"SessionEnd", "session-end.sh"},
}

func hooksDir() string {
	return filepath.Join(filepath.Dir(configPath()), "hooks")
}

func claudeSettingsPath() string {
	if p := os.Getenv("SYNCUP_CLAUDE_SETTINGS"); p != "" {
		return p
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".claude", "settings.json")
}

func cmdHooks(args []string) error {
	sub := ""
	if len(args) > 0 {
		sub = args[0]
	}
	switch sub {
	case "path":
		fmt.Println(hooksDir())
		return nil
	case "install":
		return hooksInstall()
	default:
		return errors.New("usage: syncup hooks <install|path>")
	}
}

// hooksInstall writes the embedded hook scripts and wires them into Claude Code's
// settings.json. Idempotent: re-running replaces syncup's own hook entries and
// leaves any other hooks untouched.
func hooksInstall() error {
	dir := hooksDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for _, h := range hookFiles {
		data, err := hooksFS.ReadFile("hooks/" + h.file)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, h.file), data, 0o755); err != nil {
			return err
		}
	}
	if err := patchClaudeSettings(dir); err != nil {
		return err
	}
	fmt.Printf("installed hooks to %s\nwired into %s\n", dir, claudeSettingsPath())
	return nil
}

func patchClaudeSettings(dir string) error {
	path := claudeSettingsPath()
	settings := map[string]any{}
	if b, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(b, &settings); err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}
		_ = os.WriteFile(path+".bak", b, 0o644) // backup before editing
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	hooks, _ := settings["hooks"].(map[string]any)
	if hooks == nil {
		hooks = map[string]any{}
	}
	for _, h := range hookFiles {
		entry := map[string]any{"hooks": []any{
			map[string]any{"type": "command", "command": filepath.Join(dir, h.file)},
		}}
		var kept []any
		if existing, ok := hooks[h.event].([]any); ok {
			for _, e := range existing {
				if !entryIsOurs(e) { // drop our old entries; keep everyone else's
					kept = append(kept, e)
				}
			}
		}
		hooks[h.event] = append(kept, entry)
	}
	settings["hooks"] = hooks

	out, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(out, '\n'), 0o644)
}

// entryIsOurs reports whether a settings hook entry was installed by syncup —
// matched by our distinctive script filenames (or a "syncup" path), so re-install
// is idempotent regardless of where hooks live.
func entryIsOurs(e any) bool {
	m, ok := e.(map[string]any)
	if !ok {
		return false
	}
	hs, ok := m["hooks"].([]any)
	if !ok {
		return false
	}
	for _, h := range hs {
		hm, ok := h.(map[string]any)
		if !ok {
			continue
		}
		c, ok := hm["command"].(string)
		if !ok {
			continue
		}
		if strings.Contains(c, "syncup") {
			return true
		}
		for _, hf := range hookFiles {
			if filepath.Base(c) == hf.file {
				return true
			}
		}
	}
	return false
}
