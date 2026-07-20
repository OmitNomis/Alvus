package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// upstreamStub is a fake upstream that replies from a scripted list of statuses
// and records which key each attempt presented.
type upstreamStub struct {
	*httptest.Server
	mu       sync.Mutex
	statuses []int
	seen     []string
}

func newUpstreamStub(statuses ...int) *upstreamStub {
	u := &upstreamStub{statuses: statuses}
	u.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.mu.Lock()
		u.seen = append(u.seen, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		status := http.StatusOK
		if len(u.statuses) > 0 {
			status = u.statuses[0]
			u.statuses = u.statuses[1:]
		}
		u.mu.Unlock()

		if ra := r.Header.Get("X-Test-Retry-After"); ra != "" {
			w.Header().Set("Retry-After", ra)
		}
		w.WriteHeader(status)
		io.WriteString(w, "stub body")
	}))
	return u
}

func (u *upstreamStub) keysSeen() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.seen...)
}

// rotate drives withKeyRotation against the stub with a sane test config.
func rotate(t *testing.T, u *upstreamStub, pool *KeyPool, cfg Config) (*rotateResult, *rotateError) {
	t.Helper()
	return withKeyRotation(context.Background(), cfg, pool, rotateOpts{}, "", func(key string) (*http.Request, error) {
		req, err := http.NewRequest(http.MethodPost, u.URL, strings.NewReader("{}"))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		return req, nil
	})
}

func testConfig() Config {
	return Config{MaxRetries: 5, CooldownSec: 60}
}

func TestRotationReturnsFirstGoodResponse(t *testing.T) {
	u := newUpstreamStub(http.StatusOK)
	defer u.Close()
	pool := NewKeyPool([]string{"key-a", "key-b"}, 0)

	out, rerr := rotate(t, u, pool, testConfig())
	if rerr != nil {
		t.Fatalf("unexpected error: %v", rerr.msg)
	}
	defer out.release()
	defer out.resp.Body.Close()

	if out.resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d", out.resp.StatusCode)
	}
	if out.attempt != 1 {
		t.Errorf("attempt = %d, want 1", out.attempt)
	}
}

func TestRotationRetriesNextKeyOn429(t *testing.T) {
	u := newUpstreamStub(http.StatusTooManyRequests, http.StatusOK)
	defer u.Close()
	pool := NewKeyPool([]string{"key-a", "key-b"}, 0)

	out, rerr := rotate(t, u, pool, testConfig())
	if rerr != nil {
		t.Fatalf("unexpected error: %v", rerr.msg)
	}
	defer out.release()
	defer out.resp.Body.Close()

	seen := u.keysSeen()
	if len(seen) != 2 {
		t.Fatalf("upstream saw %d attempts, want 2", len(seen))
	}
	if seen[0] == seen[1] {
		t.Errorf("retry reused the rate-limited key %q", seen[0])
	}
	if out.attempt != 2 {
		t.Errorf("attempt = %d, want 2", out.attempt)
	}

	// The 429'd key must now be cooling.
	if n := pool.ActiveCount(); n != 2 {
		t.Errorf("ActiveCount = %d — a 429 should cool a key, not disable it", n)
	}
	if d := pool.TimeUntilAvailable(); d != 0 {
		t.Errorf("the surviving key should still be ready, got %v", d)
	}
}

func TestRotationDisablesKeyOn401(t *testing.T) {
	u := newUpstreamStub(http.StatusUnauthorized, http.StatusOK)
	defer u.Close()
	pool := NewKeyPool([]string{"key-a", "key-b"}, 0)

	out, rerr := rotate(t, u, pool, testConfig())
	if rerr != nil {
		t.Fatalf("unexpected error: %v", rerr.msg)
	}
	defer out.release()
	defer out.resp.Body.Close()

	if n := pool.ActiveCount(); n != 1 {
		t.Errorf("ActiveCount = %d, want 1 — a 401 key should be dropped", n)
	}
}

func TestRotationFailsFastWhenEveryKeyIsRevoked(t *testing.T) {
	u := newUpstreamStub(http.StatusUnauthorized, http.StatusUnauthorized)
	defer u.Close()
	pool := NewKeyPool([]string{"key-a", "key-b"}, 0)

	start := time.Now()
	_, rerr := rotate(t, u, pool, Config{MaxRetries: 10, CooldownSec: 60})
	if rerr == nil {
		t.Fatal("want an error once every key is revoked")
	}
	if rerr.status != http.StatusServiceUnavailable {
		t.Errorf("status = %d", rerr.status)
	}
	if !strings.Contains(rerr.msg, "invalid or revoked") {
		t.Errorf("msg = %q", rerr.msg)
	}
	// Nothing is going to recover, so it must not sit through the retry budget.
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("took %v — should fail fast rather than wait out cooldowns", elapsed)
	}
}

func TestRotationPassesTerminalErrorsThrough(t *testing.T) {
	// A 400 is the client's problem; retrying on another key cannot help.
	u := newUpstreamStub(http.StatusBadRequest)
	defer u.Close()
	pool := NewKeyPool([]string{"key-a", "key-b"}, 0)

	out, rerr := rotate(t, u, pool, testConfig())
	if rerr != nil {
		t.Fatalf("unexpected error: %v", rerr.msg)
	}
	defer out.release()
	defer out.resp.Body.Close()

	if out.resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 handed back as-is", out.resp.StatusCode)
	}
	if got := len(u.keysSeen()); got != 1 {
		t.Errorf("upstream saw %d attempts, want 1 — a 4xx must not be retried", got)
	}
}

func TestRotationExhaustsRetries(t *testing.T) {
	u := newUpstreamStub(http.StatusTooManyRequests, http.StatusTooManyRequests, http.StatusTooManyRequests)
	defer u.Close()
	pool := NewKeyPool([]string{"key-a"}, 0)

	// One key, cooled on every 429, with a budget of 3 and a cooldown short
	// enough that the loop keeps trying rather than giving up on the wait.
	_, rerr := rotate(t, u, pool, Config{MaxRetries: 3, CooldownSec: 0})
	if rerr == nil {
		t.Fatal("want an error once the retry budget is spent")
	}
	if !strings.Contains(rerr.msg, "exhausted") {
		t.Errorf("msg = %q, want an exhausted-retries error", rerr.msg)
	}
}

func TestRotationStopsWhenClientGoesAway(t *testing.T) {
	u := newUpstreamStub(http.StatusTooManyRequests, http.StatusOK)
	defer u.Close()
	pool := NewKeyPool([]string{"key-a"}, 0)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // client hung up before we even started

	_, rerr := withKeyRotation(ctx, testConfig(), pool, rotateOpts{}, "", func(key string) (*http.Request, error) {
		return http.NewRequest(http.MethodPost, u.URL, strings.NewReader("{}"))
	})
	if rerr == nil {
		t.Fatal("want an error when the client is gone")
	}
	if !strings.Contains(rerr.msg, "client went away") {
		t.Errorf("msg = %q", rerr.msg)
	}
	if got := len(u.keysSeen()); got != 0 {
		t.Errorf("upstream saw %d attempts, want 0 — nothing should be sent for a dead client", got)
	}
}

func TestRotationCountsEveryAttemptNotJustTheWinner(t *testing.T) {
	// The upstream's rate limit counts what we send. A retried request has
	// really been sent, so it has to show up against the key that sent it.
	u := newUpstreamStub(http.StatusTooManyRequests, http.StatusOK)
	defer u.Close()
	pool := NewKeyPool([]string{"key-a", "key-b"}, 0)

	out, rerr := rotate(t, u, pool, testConfig())
	if rerr != nil {
		t.Fatalf("unexpected error: %v", rerr.msg)
	}
	defer out.release()
	defer out.resp.Body.Close()

	total := 0
	pool.mu.Lock()
	for i := range pool.keys {
		total += pool.requestsInLastMinute(i)
	}
	pool.mu.Unlock()

	if total != 2 {
		t.Errorf("counted %d requests, want 2 — the 429'd attempt was not recorded", total)
	}
}

func TestRPMLimitKeepsUsUnderTheUpstreamCeiling(t *testing.T) {
	// A provider that allows two requests per key and 429s the third — the
	// situation RPM_LIMIT exists to avoid walking into.
	const ceiling = 2
	var mu sync.Mutex
	perKey := map[string]int{}
	refused := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		mu.Lock()
		perKey[key]++
		over := perKey[key] > ceiling
		if over {
			refused++
		}
		mu.Unlock()

		if over {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	pool := NewKeyPool([]string{"key-a", "key-b", "key-c"}, ceiling)
	cfg := Config{MaxRetries: 5, CooldownSec: 60, RPMLimit: ceiling}

	for i := 0; i < len(pool.keys)*ceiling; i++ {
		out, rerr := withKeyRotation(context.Background(), cfg, pool, rotateOpts{}, "", func(key string) (*http.Request, error) {
			req, err := http.NewRequest(http.MethodPost, srv.URL, strings.NewReader("{}"))
			if err != nil {
				return nil, err
			}
			req.Header.Set("Authorization", "Bearer "+key)
			return req, nil
		})
		if rerr != nil {
			t.Fatalf("request %d failed: %v", i, rerr.msg)
		}
		out.resp.Body.Close()
		out.release()
	}

	mu.Lock()
	defer mu.Unlock()
	if refused != 0 {
		t.Errorf("upstream refused %d requests — the throttle should have paced us under its limit", refused)
	}
	for key, n := range perKey {
		if n > ceiling {
			t.Errorf("key %s took %d requests, over the upstream ceiling of %d", key, n, ceiling)
		}
	}
}

func TestClassify(t *testing.T) {
	tests := []struct {
		status int
		want   disposition
	}{
		{http.StatusOK, dispTerminal},
		{http.StatusBadRequest, dispTerminal},
		{http.StatusNotFound, dispTerminal},
		{http.StatusTooManyRequests, dispRetryCooldown},
		{http.StatusBadGateway, dispRetryCooldown},
		{http.StatusServiceUnavailable, dispRetryCooldown},
		{http.StatusUnauthorized, dispDisable},
		{http.StatusForbidden, dispDisable},
		{http.StatusInternalServerError, dispRetryPlain},
		{http.StatusGatewayTimeout, dispRetryPlain},
	}
	for _, tc := range tests {
		if got := classify(tc.status); got != tc.want {
			t.Errorf("classify(%d) = %v, want %v", tc.status, got, tc.want)
		}
	}
}

func TestCooldownFor(t *testing.T) {
	cfg := Config{CooldownSec: 45}

	t.Run("falls back to the configured cooldown", func(t *testing.T) {
		resp := &http.Response{Header: http.Header{}}
		if got := cooldownFor(resp, cfg); got != 45*time.Second {
			t.Errorf("got %v, want 45s", got)
		}
	})

	t.Run("honours Retry-After with a margin", func(t *testing.T) {
		resp := &http.Response{Header: http.Header{"Retry-After": []string{"10"}}}
		if got := cooldownFor(resp, cfg); got != 12*time.Second {
			t.Errorf("got %v, want 12s (10 + 2s margin)", got)
		}
	})

	t.Run("ignores an unparseable Retry-After", func(t *testing.T) {
		// The HTTP-date form is legal but we do not parse it; fall back rather
		// than treating it as zero.
		resp := &http.Response{Header: http.Header{"Retry-After": []string{"Wed, 21 Oct 2026 07:28:00 GMT"}}}
		if got := cooldownFor(resp, cfg); got != 45*time.Second {
			t.Errorf("got %v, want the 45s fallback", got)
		}
	})
}

func TestBackoffFor(t *testing.T) {
	prev := time.Duration(0)
	for attempt := 0; attempt < 10; attempt++ {
		d := backoffFor(attempt)
		if d < prev {
			t.Errorf("attempt %d: backoff went backwards (%v after %v)", attempt, d, prev)
		}
		if d > 2*time.Second {
			t.Errorf("attempt %d: backoff %v exceeds the 2s cap", attempt, d)
		}
		prev = d
	}
	if got := backoffFor(0); got != 100*time.Millisecond {
		t.Errorf("first backoff = %v, want 100ms", got)
	}
}

func TestSleepCtx(t *testing.T) {
	t.Run("completes", func(t *testing.T) {
		if !sleepCtx(context.Background(), time.Millisecond) {
			t.Error("want true when the sleep finishes")
		}
	})
	t.Run("zero duration is a no-op", func(t *testing.T) {
		if !sleepCtx(context.Background(), 0) {
			t.Error("want true for a zero sleep")
		}
	})
	t.Run("bails out when the client goes away", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if sleepCtx(ctx, time.Hour) {
			t.Error("want false once the context is cancelled")
		}
	})
}
