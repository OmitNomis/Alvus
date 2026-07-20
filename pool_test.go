package main

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNextRoundRobins(t *testing.T) {
	p := NewKeyPool([]string{"a", "b", "c"}, 0)

	var got []string
	for i := 0; i < 6; i++ {
		_, key, ok := p.Next()
		if !ok {
			t.Fatalf("call %d: no key available", i)
		}
		got = append(got, key)
	}

	// The starting offset is an implementation detail; what matters is that
	// every key is handed out once per cycle of three.
	for _, cycle := range [][]string{got[:3], got[3:]} {
		seen := map[string]bool{}
		for _, k := range cycle {
			seen[k] = true
		}
		if len(seen) != 3 {
			t.Errorf("cycle %v did not cover all three keys", cycle)
		}
	}
}

func TestCooldownSkipsKey(t *testing.T) {
	p := NewKeyPool([]string{"a", "b"}, 0)
	p.Cooldown(0, time.Minute)

	for i := 0; i < 4; i++ {
		idx, key, ok := p.Next()
		if !ok {
			t.Fatalf("call %d: no key available", i)
		}
		if idx == 0 || key == "a" {
			t.Fatalf("call %d: handed out a cooling key", i)
		}
	}
}

func TestCooldownOnlyExtends(t *testing.T) {
	p := NewKeyPool([]string{"a"}, 0)
	p.Cooldown(0, time.Minute)
	first := p.cooldowns[0]

	// A shorter cooldown must not pull the deadline back in.
	p.Cooldown(0, time.Second)
	if !p.cooldowns[0].Equal(first) {
		t.Errorf("short cooldown shortened the deadline: %v → %v", first, p.cooldowns[0])
	}
}

func TestCooldownExpires(t *testing.T) {
	p := NewKeyPool([]string{"a"}, 0)
	p.Cooldown(0, 30*time.Millisecond)

	if _, _, ok := p.Next(); ok {
		t.Fatal("key handed out while still cooling")
	}
	time.Sleep(50 * time.Millisecond)
	if _, _, ok := p.Next(); !ok {
		t.Fatal("key still withheld after its cooldown expired")
	}
}

func TestDisableRemovesFromPool(t *testing.T) {
	p := NewKeyPool([]string{"a", "b"}, 0)
	p.Disable(0)

	if n := p.ActiveCount(); n != 1 {
		t.Errorf("ActiveCount = %d, want 1", n)
	}
	for i := 0; i < 4; i++ {
		idx, _, ok := p.Next()
		if !ok {
			t.Fatalf("call %d: no key available", i)
		}
		if idx == 0 {
			t.Fatalf("call %d: handed out a disabled key", i)
		}
	}

	p.Disable(1)
	if n := p.ActiveCount(); n != 0 {
		t.Errorf("ActiveCount = %d, want 0", n)
	}
	if _, _, ok := p.Next(); ok {
		t.Error("Next succeeded with every key disabled")
	}
}

func TestTimeUntilAvailable(t *testing.T) {
	p := NewKeyPool([]string{"a", "b"}, 0)

	if d := p.TimeUntilAvailable(); d != 0 {
		t.Errorf("fresh pool: got %v, want 0", d)
	}

	p.Cooldown(0, time.Minute)
	if d := p.TimeUntilAvailable(); d != 0 {
		t.Errorf("one key ready: got %v, want 0", d)
	}

	// Both cooling: report the soonest, not the furthest.
	p.Cooldown(1, 10*time.Second)
	d := p.TimeUntilAvailable()
	if d <= 0 || d > 10*time.Second {
		t.Errorf("both cooling: got %v, want just under 10s", d)
	}
}

func TestTimeUntilAvailableIgnoresDisabled(t *testing.T) {
	p := NewKeyPool([]string{"a", "b"}, 0)
	p.Cooldown(0, time.Minute)
	p.Disable(1)

	// Key 1 is disabled and will never come back, so it must not be reported
	// as the soonest-available key.
	if d := p.TimeUntilAvailable(); d <= time.Second {
		t.Errorf("got %v, want roughly a minute — a disabled key was counted", d)
	}
}

func TestRequestsInLastMinute(t *testing.T) {
	p := NewKeyPool([]string{"a"}, 0)

	for i := 0; i < 3; i++ {
		p.IncrementRequestCount(0)
	}
	if n := p.requestsInLastMinute(0); n != 3 {
		t.Errorf("got %d, want 3", n)
	}

	// Anything outside the 60s window drops out.
	p.requestHistory[0] = append(p.requestHistory[0], time.Now().Add(-2*time.Minute))
	if n := p.requestsInLastMinute(0); n != 3 {
		t.Errorf("got %d, want 3 — a stale timestamp was counted", n)
	}

	p.cleanupOldRequests(0)
	if n := len(p.requestHistory[0]); n != 3 {
		t.Errorf("after cleanup, history holds %d entries, want 3", n)
	}
}

func TestKeyStatusLabel(t *testing.T) {
	p := NewKeyPool([]string{"a", "b", "c"}, 0)
	p.Cooldown(1, 30*time.Second)
	p.Disable(2)

	now := time.Now()
	if got := p.keyStatusLabel(0, now); got != "ready" {
		t.Errorf("key 0: got %q, want \"ready\"", got)
	}
	if got := p.keyStatusLabel(1, now); got != "cooling(30s)" {
		t.Errorf("key 1: got %q, want \"cooling(30s)\"", got)
	}
	if got := p.keyStatusLabel(2, now); !strings.HasPrefix(got, "quarantined(") {
		t.Errorf("key 2: got %q, want a quarantined(...) label", got)
	}
}

// ── Quarantine and recovery ─────────────────────────────────────────

func TestQuarantinedKeyIsWithheldUntilItsProbeIsDue(t *testing.T) {
	p := NewKeyPool([]string{"a"}, 0)
	p.Disable(0)

	if _, _, ok := p.Next(); ok {
		t.Fatal("a freshly quarantined key was handed straight back out")
	}
	if n := p.ActiveCount(); n != 0 {
		t.Errorf("ActiveCount = %d, want 0 while quarantined", n)
	}

	// Once the wait is up the key gets exactly one chance to prove itself.
	p.quarantineUntil[0] = time.Now().Add(-time.Second)
	if _, _, ok := p.Next(); !ok {
		t.Error("key was not probed after its quarantine expired")
	}
	if got := p.keyStatusLabel(0, time.Now()); got != "probing" {
		t.Errorf("status = %q, want \"probing\"", got)
	}
}

func TestMarkHealthyLiftsTheQuarantine(t *testing.T) {
	p := NewKeyPool([]string{"a"}, 0)
	p.Disable(0)
	p.MarkHealthy(0)

	if n := p.ActiveCount(); n != 1 {
		t.Errorf("ActiveCount = %d, want 1", n)
	}
	if got := p.keyStatusLabel(0, time.Now()); got != "ready" {
		t.Errorf("status = %q, want \"ready\"", got)
	}
}

func TestRepeatOffencesBackOffFurther(t *testing.T) {
	p := NewKeyPool([]string{"a"}, 0)

	var prev time.Duration
	for strike := 1; strike <= 3; strike++ {
		p.Disable(0)
		wait := time.Until(p.quarantineUntil[0])
		if wait <= prev {
			t.Errorf("strike %d waited %v, not longer than the previous %v", strike, wait, prev)
		}
		prev = wait
		// A successful probe clears the quarantine but not the strike count,
		// so a flapping key keeps earning longer waits.
		p.MarkHealthy(0)
	}

	if prev > quarantineMax {
		t.Errorf("backoff reached %v, over the %v cap", prev, quarantineMax)
	}
}

func TestQuarantineBackoffIsCapped(t *testing.T) {
	p := NewKeyPool([]string{"a"}, 0)
	for i := 0; i < 20; i++ {
		p.Disable(0)
	}
	if wait := time.Until(p.quarantineUntil[0]); wait > quarantineMax {
		t.Errorf("backoff = %v, over the %v cap", wait, quarantineMax)
	}
}

func TestEnableForgivesStrikes(t *testing.T) {
	p := NewKeyPool([]string{"a"}, 0)
	p.Disable(0)
	p.Disable(0)

	if !p.Enable(0) {
		t.Fatal("Enable reported failure for a valid index")
	}
	if n := p.ActiveCount(); n != 1 {
		t.Errorf("ActiveCount = %d, want 1", n)
	}
	// The operator says it is fixed, so the next failure starts from scratch.
	p.Disable(0)
	if wait := time.Until(p.quarantineUntil[0]); wait > quarantineBase+time.Minute {
		t.Errorf("backoff = %v, want the base %v — strikes should have been forgiven", wait, quarantineBase)
	}
}

func TestEnableRejectsAnUnknownIndex(t *testing.T) {
	p := NewKeyPool([]string{"a"}, 0)
	for _, idx := range []int{-1, 1, 99} {
		if p.Enable(idx) {
			t.Errorf("Enable(%d) succeeded for an out-of-range index", idx)
		}
	}
}

// ── Proactive RPM throttling ────────────────────────────────────────

func TestRPMLimitZeroIsOff(t *testing.T) {
	p := NewKeyPool([]string{"a"}, 0)
	for i := 0; i < 500; i++ {
		p.IncrementRequestCount(0)
	}
	if _, _, ok := p.Next(); !ok {
		t.Error("an unlimited pool withheld a key")
	}
}

func TestRPMLimitWithholdsAnExhaustedKey(t *testing.T) {
	p := NewKeyPool([]string{"a"}, 3)

	for i := 0; i < 3; i++ {
		if _, _, ok := p.Next(); !ok {
			t.Fatalf("request %d refused while still inside the budget", i)
		}
		p.IncrementRequestCount(0)
	}
	if _, _, ok := p.Next(); ok {
		t.Error("key handed out after its per-minute budget was spent")
	}
}

func TestRPMLimitSpreadsAcrossKeys(t *testing.T) {
	// The whole point: with three keys at 2 rpm each, six requests should go
	// through without a single one being refused.
	p := NewKeyPool([]string{"a", "b", "c"}, 2)

	counts := map[int]int{}
	for i := 0; i < 6; i++ {
		idx, _, ok := p.Next()
		if !ok {
			t.Fatalf("request %d refused, but the pool still had budget", i)
		}
		p.IncrementRequestCount(idx)
		counts[idx]++
	}

	if len(counts) != 3 {
		t.Errorf("requests landed on %d keys, want all 3: %v", len(counts), counts)
	}
	for idx, n := range counts {
		if n > 2 {
			t.Errorf("key %d took %d requests, over its limit of 2", idx, n)
		}
	}

	// Seventh request has nowhere to go.
	if _, _, ok := p.Next(); ok {
		t.Error("a seventh request was allowed past the pool's total budget")
	}
}

func TestRPMLimitFreesUpAsRequestsAgeOut(t *testing.T) {
	p := NewKeyPool([]string{"a"}, 2)

	// Two requests, backdated so one is about to leave the window.
	now := time.Now()
	p.requestHistory[0] = []time.Time{
		now.Add(-rpmWindow + 40*time.Millisecond),
		now.Add(-time.Second),
	}

	if _, _, ok := p.Next(); ok {
		t.Fatal("key handed out while at its limit")
	}

	// TimeUntilAvailable must point at the moment the oldest entry expires,
	// not at some fixed cooldown.
	wait := p.TimeUntilAvailable()
	if wait <= 0 || wait > 200*time.Millisecond {
		t.Fatalf("TimeUntilAvailable = %v, want just under 40ms", wait)
	}

	time.Sleep(wait + 20*time.Millisecond)
	if _, _, ok := p.Next(); !ok {
		t.Error("key still withheld after a slot should have freed up")
	}
}

func TestRPMLimitAndCooldownTakeTheLater(t *testing.T) {
	p := NewKeyPool([]string{"a"}, 1)
	p.IncrementRequestCount(0) // throttled for ~60s
	p.Cooldown(0, 5*time.Second)

	// Both apply; the key is not usable until the later of the two.
	wait := p.TimeUntilAvailable()
	if wait <= 10*time.Second {
		t.Errorf("TimeUntilAvailable = %v, want the ~60s throttle to win over the 5s cooldown", wait)
	}
}

func TestThrottledKeyReportsItsOwnStatus(t *testing.T) {
	p := NewKeyPool([]string{"a"}, 1)
	p.IncrementRequestCount(0)

	// "throttled" is not "cooling" — nothing went wrong, we are just pacing.
	got := p.keyStatusLabel(0, time.Now())
	if !strings.HasPrefix(got, "throttled(") {
		t.Errorf("status = %q, want a throttled(...) label", got)
	}
}

// ── Reload ──────────────────────────────────────────────────────────

func TestAdoptStateFromCarriesSurvivingKeys(t *testing.T) {
	old := NewKeyPool([]string{"a", "b", "c"}, 0)
	old.Cooldown(0, time.Minute)
	old.Disable(1)
	old.IncrementRequestCount(2)
	old.IncrementRequestCount(2)

	// "b" was removed, "d" is new, and the rest survive the edit.
	fresh := NewKeyPool([]string{"a", "c", "d"}, 0)
	fresh.adoptStateFrom(old)

	if fresh.keyStatusLabel(0, time.Now()) == "ready" {
		t.Error("key \"a\" came back ready — its cooldown was dropped")
	}
	if n := fresh.requestsInLastMinute(1); n != 2 {
		t.Errorf("key \"c\" carried %d requests, want 2", n)
	}
	if fresh.disabled[2] || !fresh.lastUsed[2].IsZero() {
		t.Error("newly added key \"d\" did not start clean")
	}
	if n := fresh.ActiveCount(); n != 3 {
		t.Errorf("ActiveCount = %d — the disabled key was dropped from .env, so nothing should be disabled", n)
	}
}

func TestAdoptStateFromKeepsDisabledKeysDisabled(t *testing.T) {
	old := NewKeyPool([]string{"a", "b"}, 0)
	old.Disable(0)

	fresh := NewKeyPool([]string{"a", "b"}, 0)
	fresh.adoptStateFrom(old)

	// The provider revoked this key; editing an unrelated setting in .env is
	// not a reason to start sending to it again.
	if !fresh.disabled[0] {
		t.Error("a revoked key was resurrected by a reload")
	}
	if n := fresh.ActiveCount(); n != 1 {
		t.Errorf("ActiveCount = %d, want 1", n)
	}
}

func TestAdoptStateFromHandlesNoPriorPool(t *testing.T) {
	fresh := NewKeyPool([]string{"a"}, 0)
	fresh.adoptStateFrom(nil) // first startup
	if _, _, ok := fresh.Next(); !ok {
		t.Error("a pool with no predecessor should be immediately usable")
	}
}

func TestAdoptStateFromCopiesHistory(t *testing.T) {
	old := NewKeyPool([]string{"a"}, 0)
	old.IncrementRequestCount(0)

	fresh := NewKeyPool([]string{"a"}, 0)
	fresh.adoptStateFrom(old)

	// The two pools must not share a backing array, or later requests against
	// one would show up in the other.
	fresh.IncrementRequestCount(0)
	if n := old.requestsInLastMinute(0); n != 1 {
		t.Errorf("old pool now reports %d requests, want 1 — history was aliased", n)
	}
}

func TestPoolIsSafeUnderConcurrentUse(t *testing.T) {
	// Every in-flight request touches the pool at once, so exercise all of it
	// together and let -race adjudicate.
	p := NewKeyPool([]string{"a", "b", "c", "d"}, 0)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				idx, _, ok := p.Next()
				if !ok {
					continue
				}
				p.IncrementRequestCount(idx)
				switch (worker + j) % 5 {
				case 0:
					p.Cooldown(idx, time.Millisecond)
				case 1:
					p.Status()
				case 2:
					p.GetKeyDetails()
				case 3:
					p.TimeUntilAvailable()
				case 4:
					p.ActiveCount()
				}
			}
		}(i)
	}
	wg.Wait()

	if n := p.ActiveCount(); n != 4 {
		t.Errorf("ActiveCount = %d, want 4 — nothing here should disable a key", n)
	}
}

func TestMaskKey(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"", "****"},
		{"short", "****"},
		{"exactly12chr", "****"},
		{"nvapi-abcdefghijklmnop", "nvapi-ab...mnop"},
	}
	for _, tc := range tests {
		if got := maskKey(tc.in); got != tc.want {
			t.Errorf("maskKey(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestMaskKeyNeverLeaksTheMiddle(t *testing.T) {
	const key = "nvapi-thisisaverylongsecretkeyvalue"
	masked := maskKey(key)
	if len(masked) >= len(key) {
		t.Fatalf("mask %q is not shorter than the key", masked)
	}
	if got := len(masked); got != 15 {
		t.Errorf("mask is %d chars (%q); want a fixed 15", got, masked)
	}
}
