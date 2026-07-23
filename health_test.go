package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newHealthState(auth *AdminAuth) *ServerState {
	pool := NewKeyPool([]string{"nvapi-aaaaaaaaaaaa", "nvapi-bbbbbbbbbbbb"}, 0)
	return &ServerState{pool: pool, auth: auth}
}

func healthBody(t *testing.T, s *ServerState, r *http.Request) map[string]any {
	t.Helper()
	rec := httptest.NewRecorder()
	s.healthHandler(rec, r)
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("health returned undecodable JSON: %v (%q)", err, rec.Body.String())
	}
	return body
}

func TestHealthOnLoopbackIncludesKeyDetail(t *testing.T) {
	// Loopback bind: no token, the admin surface is open, so /health is fully
	// detailed — same trust boundary as the .env file next to the binary.
	s := newHealthState(&AdminAuth{})
	body := healthBody(t, s, httptest.NewRequest(http.MethodGet, "/health", nil))

	if body["status"] != "ok" || body["keys"] != float64(2) {
		t.Errorf("summary = %v", body)
	}
	if _, ok := body["details"]; !ok {
		t.Error("loopback /health should carry per-key details")
	}
}

func TestHealthOffBoxHidesKeyDetailWithoutToken(t *testing.T) {
	// Non-loopback bind: a token guards the admin surface, so an unauthenticated
	// caller gets liveness only — no per-key operational detail leaks to the LAN.
	s := newHealthState(&AdminAuth{token: "secret"})
	body := healthBody(t, s, httptest.NewRequest(http.MethodGet, "/health", nil))

	if body["status"] != "ok" || body["keys"] != float64(2) {
		t.Errorf("summary = %v", body)
	}
	if _, ok := body["details"]; ok {
		t.Error("off-box /health must not expose per-key details without the token")
	}
}

func TestHealthOffBoxRevealsDetailWithToken(t *testing.T) {
	// A caller that proves it holds the admin token still gets the full picture.
	s := newHealthState(&AdminAuth{token: "secret"})
	r := httptest.NewRequest(http.MethodGet, "/health", nil)
	r.Header.Set("X-Alvus-Token", "secret")
	body := healthBody(t, s, r)

	if _, ok := body["details"]; !ok {
		t.Error("a caller with the admin token should still get full detail")
	}
}
