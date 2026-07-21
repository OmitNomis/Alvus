package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeEnv drops a .env in a temp dir and returns its path.
func writeEnv(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatalf("seed .env: %v", err)
	}
	return path
}

func readEnv(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back .env: %v", err)
	}
	return string(data)
}

func TestLoadDotEnv(t *testing.T) {
	path := writeEnv(t, strings.Join([]string{
		"# a comment",
		"",
		"API_KEYS=one,two",
		"  PORT = 4000  ",
		"MALFORMED",
		"TARGET_BASE_URL=https://example.test/v1",
	}, "\n"))

	for _, k := range []string{"API_KEYS", "PORT", "TARGET_BASE_URL"} {
		t.Setenv(k, "")
		os.Unsetenv(k)
	}

	loadDotEnv(path)

	if got := os.Getenv("API_KEYS"); got != "one,two" {
		t.Errorf("API_KEYS = %q, want \"one,two\"", got)
	}
	if got := os.Getenv("PORT"); got != "4000" {
		t.Errorf("PORT = %q, want \"4000\" — surrounding space should be trimmed", got)
	}
	if got := os.Getenv("TARGET_BASE_URL"); got != "https://example.test/v1" {
		t.Errorf("TARGET_BASE_URL = %q", got)
	}
}

func TestLoadDotEnvDoesNotOverrideRealEnv(t *testing.T) {
	path := writeEnv(t, "API_KEYS=from-file\n")
	t.Setenv("API_KEYS", "from-environment")

	loadDotEnv(path)

	if got := os.Getenv("API_KEYS"); got != "from-environment" {
		t.Errorf("API_KEYS = %q, want the real environment to win", got)
	}
}

// reload mimics what reloadConfig does to the environment, against an
// arbitrary .env path.
func reloadEnv(path string) {
	resetEnvToBaseline()
	loadDotEnv(path)
}

func TestReloadKeepsTheRealEnvironmentWinning(t *testing.T) {
	// systemd or Docker supplied this; .env must never override it, not even
	// after a reload.
	t.Setenv("API_KEYS", "from-environment")
	snapshotEnv()

	path := writeEnv(t, "API_KEYS=from-file\n")
	loadDotEnv(path)
	if got := os.Getenv("API_KEYS"); got != "from-environment" {
		t.Fatalf("after first load: %q", got)
	}

	for i := 0; i < 3; i++ {
		reloadEnv(path)
		if got := os.Getenv("API_KEYS"); got != "from-environment" {
			t.Fatalf("after reload %d: API_KEYS = %q, want the real environment to still win", i, got)
		}
	}
}

func TestReloadAppliesValuesRemovedFromTheFile(t *testing.T) {
	os.Unsetenv("OVERRIDE_MODEL")
	snapshotEnv()

	path := writeEnv(t, "API_KEYS=one\nOVERRIDE_MODEL=some/model\n")
	loadDotEnv(path)
	if got := os.Getenv("OVERRIDE_MODEL"); got != "some/model" {
		t.Fatalf("OVERRIDE_MODEL = %q", got)
	}

	// Drop the line and reload: it has to actually disappear, not linger.
	if err := os.WriteFile(path, []byte("API_KEYS=one\n"), 0600); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	reloadEnv(path)

	if got := os.Getenv("OVERRIDE_MODEL"); got != "" {
		t.Errorf("OVERRIDE_MODEL = %q, want it gone once removed from .env", got)
	}
}

func TestReloadPicksUpEditedValues(t *testing.T) {
	os.Unsetenv("API_KEYS")
	snapshotEnv()

	path := writeEnv(t, "API_KEYS=one\n")
	loadDotEnv(path)

	if err := os.WriteFile(path, []byte("API_KEYS=one,two\n"), 0600); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	reloadEnv(path)

	if got := os.Getenv("API_KEYS"); got != "one,two" {
		t.Errorf("API_KEYS = %q, want the edited value", got)
	}
}

func TestUpdateDotEnvPreservesEverythingElse(t *testing.T) {
	path := writeEnv(t, strings.Join([]string{
		"# Alvus configuration",
		"API_KEYS=old-one,old-two",
		"",
		"# Something Alvus has no opinion about",
		"MY_CUSTOM_VAR=keep-me",
		"OVERRIDE_MODEL=some/model",
		"",
	}, "\n"))

	err := updateDotEnv(path, map[string]string{"API_KEYS": "new-one,new-two"})
	if err != nil {
		t.Fatalf("updateDotEnv: %v", err)
	}

	got := readEnv(t, path)
	for _, want := range []string{
		"# Alvus configuration",
		"API_KEYS=new-one,new-two",
		"# Something Alvus has no opinion about",
		"MY_CUSTOM_VAR=keep-me",
		"OVERRIDE_MODEL=some/model",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q from:\n%s", want, got)
		}
	}
	if strings.Contains(got, "old-one") {
		t.Errorf("stale value survived:\n%s", got)
	}
}

func TestUpdateDotEnvAppendsMissingKeys(t *testing.T) {
	path := writeEnv(t, "API_KEYS=one\n")

	err := updateDotEnv(path, map[string]string{
		"API_KEYS":        "one",
		"TARGET_BASE_URL": "https://example.test/v1",
		"GENAI_BASE_URL":  "https://genai.example.test",
	})
	if err != nil {
		t.Fatalf("updateDotEnv: %v", err)
	}

	got := readEnv(t, path)
	if !strings.Contains(got, "TARGET_BASE_URL=https://example.test/v1") {
		t.Errorf("appended key missing:\n%s", got)
	}
	// Appended keys are sorted, so the file is stable across saves.
	if strings.Index(got, "GENAI_BASE_URL") > strings.Index(got, "TARGET_BASE_URL") {
		t.Errorf("appended keys are not in sorted order:\n%s", got)
	}
}

func TestUpdateDotEnvIsIdempotent(t *testing.T) {
	path := writeEnv(t, "API_KEYS=one\n")
	updates := map[string]string{"API_KEYS": "one", "PORT": "3000"}

	if err := updateDotEnv(path, updates); err != nil {
		t.Fatalf("first write: %v", err)
	}
	first := readEnv(t, path)

	for i := 0; i < 3; i++ {
		if err := updateDotEnv(path, updates); err != nil {
			t.Fatalf("rewrite %d: %v", i, err)
		}
	}
	if got := readEnv(t, path); got != first {
		t.Errorf("repeated saves drifted:\n--- first ---\n%s\n--- later ---\n%s", first, got)
	}
}

func TestUpdateDotEnvRewritesOnlyTheFirstDuplicate(t *testing.T) {
	// A hand-edited file can hold the same key twice. Rewriting both would be
	// surprising; the first occurrence is the one .env parsers honour.
	path := writeEnv(t, "API_KEYS=first\nAPI_KEYS=second\n")

	if err := updateDotEnv(path, map[string]string{"API_KEYS": "new"}); err != nil {
		t.Fatalf("updateDotEnv: %v", err)
	}

	got := readEnv(t, path)
	if !strings.Contains(got, "API_KEYS=new") {
		t.Errorf("first occurrence not rewritten:\n%s", got)
	}
	if !strings.Contains(got, "API_KEYS=second") {
		t.Errorf("second occurrence should have been left alone:\n%s", got)
	}
}

func TestUpdateDotEnvCreatesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")

	if err := updateDotEnv(path, map[string]string{"API_KEYS": "one"}); err != nil {
		t.Fatalf("updateDotEnv: %v", err)
	}
	if got := readEnv(t, path); got != "API_KEYS=one\n" {
		t.Errorf("got %q", got)
	}
}

func TestSplitEnvLine(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"API_KEYS=one", "API_KEYS"},
		{"  PORT = 3000", "PORT"},
		{"# comment", ""},
		{"", ""},
		{"   ", ""},
		{"NOEQUALS", ""},
		{"EMPTY=", "EMPTY"},
	}
	for _, tc := range tests {
		if got := splitEnvLine(tc.in); got != tc.want {
			t.Errorf("splitEnvLine(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestParseKeysFromEnv(t *testing.T) {
	t.Run("trims and drops blanks", func(t *testing.T) {
		t.Setenv("API_KEYS", " one , ,two,  ")
		keys, err := parseKeysFromEnv()
		if err != nil {
			t.Fatalf("parseKeysFromEnv: %v", err)
		}
		if len(keys) != 2 || keys[0] != "one" || keys[1] != "two" {
			t.Errorf("got %q, want [one two]", keys)
		}
	})

	t.Run("rejects an empty pool", func(t *testing.T) {
		t.Setenv("API_KEYS", " , , ")
		if _, err := parseKeysFromEnv(); err == nil {
			t.Error("want an error when every entry is blank")
		}
	})

	t.Run("rejects a missing variable", func(t *testing.T) {
		t.Setenv("API_KEYS", "")
		if _, err := parseKeysFromEnv(); err == nil {
			t.Error("want an error when API_KEYS is unset")
		}
	})
}
