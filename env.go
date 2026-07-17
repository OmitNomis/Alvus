package main

// Reading and writing the .env file.
//
// The writer deliberately does a surgical update rather than regenerating the
// file: Alvus only knows about a handful of variables, but the user's .env may
// hold comments, blank-line grouping and settings this build has no opinion
// about. Rewriting from a fixed template silently discards all of that.

import (
	"fmt"
	"os"
	"sort"
	"strings"
)

// envKeys are the variables Alvus itself reads. reloadConfig clears these
// before re-reading .env so removed lines actually take effect.
var envKeys = []string{
	"API_KEYS",
	"TARGET_BASE_URL",
	"GENAI_BASE_URL",
	"PORT",
	"COOLDOWN_SEC",
	"MAX_RETRIES",
	"OVERRIDE_MODEL",
	"ADMIN_TOKEN",
}

func loadDotEnv(filename string) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		k, v := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		if os.Getenv(k) == "" {
			os.Setenv(k, v)
		}
	}
}

// splitEnvLine reports the key of a KEY=value line, or "" for comments, blanks
// and anything unparseable — those are passed through untouched.
func splitEnvLine(line string) string {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, "#") {
		return ""
	}
	k, _, ok := strings.Cut(trimmed, "=")
	if !ok {
		return ""
	}
	return strings.TrimSpace(k)
}

// updateDotEnv rewrites only the keys in updates, in place, leaving every
// other line — comments, spacing, unrelated variables — exactly as it found
// them. Keys not already present are appended.
func updateDotEnv(filename string, updates map[string]string) error {
	existing, err := os.ReadFile(filename)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	var lines []string
	if len(existing) > 0 {
		lines = strings.Split(strings.ReplaceAll(string(existing), "\r\n", "\n"), "\n")
		// A trailing newline yields a final empty element; drop it so we don't
		// accumulate a blank line on every save, and re-add it at the end.
		if len(lines) > 0 && lines[len(lines)-1] == "" {
			lines = lines[:len(lines)-1]
		}
	}

	seen := make(map[string]bool, len(updates))
	for i, line := range lines {
		key := splitEnvLine(line)
		if key == "" {
			continue
		}
		if val, ok := updates[key]; ok && !seen[key] {
			lines[i] = fmt.Sprintf("%s=%s", key, val)
			seen[key] = true
		}
	}

	// Append anything that wasn't already in the file, in a stable order.
	var missing []string
	for k := range updates {
		if !seen[k] {
			missing = append(missing, k)
		}
	}
	sort.Strings(missing)
	for _, k := range missing {
		lines = append(lines, fmt.Sprintf("%s=%s", k, updates[k]))
	}

	out := strings.Join(lines, "\n") + "\n"
	return os.WriteFile(filename, []byte(out), 0600)
}
