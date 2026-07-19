package main

import (
	"sync"
	"testing"
	"time"
)

func TestNextRoundRobins(t *testing.T) {
	p := NewKeyPool([]string{"a", "b", "c"})

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
	p := NewKeyPool([]string{"a", "b"})
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
	p := NewKeyPool([]string{"a"})
	p.Cooldown(0, time.Minute)
	first := p.cooldowns[0]

	// A shorter cooldown must not pull the deadline back in.
	p.Cooldown(0, time.Second)
	if !p.cooldowns[0].Equal(first) {
		t.Errorf("short cooldown shortened the deadline: %v → %v", first, p.cooldowns[0])
	}
}

func TestCooldownExpires(t *testing.T) {
	p := NewKeyPool([]string{"a"})
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
	p := NewKeyPool([]string{"a", "b"})
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
	p := NewKeyPool([]string{"a", "b"})

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
	p := NewKeyPool([]string{"a", "b"})
	p.Cooldown(0, time.Minute)
	p.Disable(1)

	// Key 1 is disabled and will never come back, so it must not be reported
	// as the soonest-available key.
	if d := p.TimeUntilAvailable(); d <= time.Second {
		t.Errorf("got %v, want roughly a minute — a disabled key was counted", d)
	}
}

func TestRequestsInLastMinute(t *testing.T) {
	p := NewKeyPool([]string{"a"})

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
	p := NewKeyPool([]string{"a", "b", "c"})
	p.Cooldown(1, 30*time.Second)
	p.Disable(2)

	now := time.Now()
	if got := p.keyStatusLabel(0, now); got != "ready" {
		t.Errorf("key 0: got %q, want \"ready\"", got)
	}
	if got := p.keyStatusLabel(1, now); got != "cooling(30s)" {
		t.Errorf("key 1: got %q, want \"cooling(30s)\"", got)
	}
	if got := p.keyStatusLabel(2, now); got != "disabled" {
		t.Errorf("key 2: got %q, want \"disabled\"", got)
	}
}

func TestPoolIsSafeUnderConcurrentUse(t *testing.T) {
	// Every in-flight request touches the pool at once, so exercise all of it
	// together and let -race adjudicate.
	p := NewKeyPool([]string{"a", "b", "c", "d"})

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
