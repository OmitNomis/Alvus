package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// GET /api/config feeds the settings form. It has to hand back every tuning
// knob so a non-technical user can edit them in the browser instead of the
// .env file — but it must never echo the admin token that guards this very
// surface, and keys must come back masked.
//
// Only the read path is exercised on purpose: POST rewrites the .env in the
// working directory, which during `go test` is the repo itself.
func TestConfigGetExposesKnobsButNotAdminToken(t *testing.T) {
	s := &ServerState{
		auth: &AdminAuth{},
		pool: NewKeyPool([]string{"nvapi-aaaaaaaaaaaa"}, 0),
		cfg: Config{
			TargetBase:    "https://example.test/v1",
			GenaiBase:     "https://genai.example.test",
			Port:          "3000",
			CooldownSec:   45,
			RPMLimit:      40,
			MaxRetries:    7,
			MaxBodyMB:     16,
			OverrideModel: "some/model",
		},
	}

	rec := httptest.NewRecorder()
	s.configHandler(rec, httptest.NewRequest(http.MethodGet, "/api/config", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/config = %d", rec.Code)
	}

	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("undecodable JSON: %v (%q)", err, rec.Body.String())
	}

	want := map[string]any{
		"cooldownSec":   float64(45),
		"rpmLimit":      float64(40),
		"maxRetries":    float64(7),
		"maxBodyMB":     float64(16),
		"port":          "3000",
		"overrideModel": "some/model",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}

	if _, ok := got["adminToken"]; ok {
		t.Error("GET /api/config must never echo the admin token")
	}

	keys, ok := got["keys"].([]any)
	if !ok || len(keys) != 1 {
		t.Fatalf("keys = %v", got["keys"])
	}
	if keys[0] == "nvapi-aaaaaaaaaaaa" {
		t.Error("keys must be masked in the config payload")
	}
}
