package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func maskKey(key string) string {
	if len(key) <= 12 {
		return "****"
	}
	return key[:8] + "..." + key[len(key)-4:]
}

type LogEntry struct {
	Timestamp       string `json:"timestamp"`
	Key             string `json:"key"`
	KeyIndex        int    `json:"key_index"`
	Method          string `json:"method"`
	URL             string `json:"url"`
	Status          int    `json:"status"`
	RequestBodySize int    `json:"request_body_size"`
}

const maxUsageLogs = 1000

var (
	usageLogs = []LogEntry{}
	usageMu   sync.Mutex
)

// logUsage records one completed upstream request for the dashboard's activity
// feed, trimming the buffer to the most recent maxUsageLogs entries.
func logUsage(key string, idx int, method, target string, status, bodySize int) {
	usageMu.Lock()
	defer usageMu.Unlock()
	usageLogs = append(usageLogs, LogEntry{
		Timestamp:       time.Now().Format(time.RFC3339),
		Key:             key,
		KeyIndex:        idx + 1,
		Method:          method,
		URL:             target,
		Status:          status,
		RequestBodySize: bodySize,
	})
	if len(usageLogs) > maxUsageLogs {
		usageLogs = usageLogs[len(usageLogs)-maxUsageLogs:]
	}
}

// ── Key Pool ──────────────────────────────────

// rpmWindow is the trailing window request rates are measured over. Providers
// quote their limits per minute, so this is not really tunable.
const rpmWindow = 60 * time.Second

// Quarantine bounds for a key the upstream rejected with 401/403. That usually
// means revoked, but several providers also return 403 for an exhausted quota,
// so writing the key off for the rest of the process is too strong. Probe it
// again later instead, backing off further each time it fails.
const (
	quarantineBase = 15 * time.Minute
	quarantineMax  = 4 * time.Hour
)

type KeyPool struct {
	counter         uint64
	keys            []string
	cooldowns       []time.Time
	disabled        []bool
	quarantineUntil []time.Time   // when a disabled key earns its next probe
	strikes         []int         // how many times it has been quarantined
	requestHistory  [][]time.Time // timestamps of requests in the last rpmWindow, per key
	lastUsed        []time.Time
	rpmLimit        int // requests per key per minute; 0 disables the throttle
	mu              sync.Mutex
}

func NewKeyPool(keys []string, rpmLimit int) *KeyPool {
	return &KeyPool{
		keys:            keys,
		cooldowns:       make([]time.Time, len(keys)),
		disabled:        make([]bool, len(keys)),
		quarantineUntil: make([]time.Time, len(keys)),
		strikes:         make([]int, len(keys)),
		requestHistory:  make([][]time.Time, len(keys)),
		lastUsed:        make([]time.Time, len(keys)),
		rpmLimit:        rpmLimit,
	}
}

// rpmBlockedUntil reports when a key that has spent its per-minute budget will
// have room again. A zero time means it has room now.
//
// requestHistory is appended in order, so the entries still inside the window
// are a suffix of the slice, and the moment a slot frees is simply when the
// oldest surplus entry ages out.
//
// Caller must hold p.mu.
func (p *KeyPool) rpmBlockedUntil(idx int, now time.Time) time.Time {
	if p.rpmLimit <= 0 {
		return time.Time{}
	}
	hist := p.requestHistory[idx]
	cutoff := now.Add(-rpmWindow)

	first := len(hist)
	for i, t := range hist {
		if t.After(cutoff) {
			first = i
			break
		}
	}
	inWindow := len(hist) - first
	if inWindow < p.rpmLimit {
		return time.Time{}
	}
	// Wait for enough of the oldest in-window requests to expire that one slot
	// opens up.
	return hist[first+(inWindow-p.rpmLimit)].Add(rpmWindow)
}

// nextFreeAt reports when key idx is next usable, and whether it will ever be.
// A zero or past time means it is usable now; ok is false for a disabled key,
// which is never coming back.
//
// Caller must hold p.mu.
func (p *KeyPool) nextFreeAt(idx int, now time.Time) (time.Time, bool) {
	if p.disabled[idx] && !p.probeDue(idx, now) {
		return time.Time{}, false
	}
	free := p.cooldowns[idx]
	if throttled := p.rpmBlockedUntil(idx, now); throttled.After(free) {
		free = throttled
	}
	return free, true
}

func (p *KeyPool) TimeUntilAvailable() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	var soonest time.Duration = -1
	for i := range p.keys {
		free, ok := p.nextFreeAt(i, now)
		if !ok {
			continue
		}
		if now.After(free) {
			return 0
		}
		if wait := free.Sub(now); soonest < 0 || wait < soonest {
			soonest = wait
		}
	}
	return soonest
}

func (p *KeyPool) Next() (int, string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := len(p.keys)
	// Mask before narrowing: on a 32-bit build a plain int() conversion goes
	// negative once the counter passes 2^31, and a negative index panics.
	start := int(atomic.AddUint64(&p.counter, 1)-1&0x7fffffff) % n
	now := time.Now()
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		if free, ok := p.nextFreeAt(idx, now); ok && now.After(free) {
			return idx, p.keys[idx], true
		}
	}
	return -1, "", false
}

// requestsInLastMinute returns the number of requests made by a key in the last
// rpmWindow.
func (p *KeyPool) requestsInLastMinute(idx int) int {
	cutoff := time.Now().Add(-rpmWindow)
	count := 0
	for _, t := range p.requestHistory[idx] {
		if t.After(cutoff) {
			count++
		}
	}
	return count
}

// cleanupOldRequests removes request timestamps that have left the window.
func (p *KeyPool) cleanupOldRequests(idx int) {
	cutoff := time.Now().Add(-rpmWindow)
	var filtered []time.Time
	for _, t := range p.requestHistory[idx] {
		if t.After(cutoff) {
			filtered = append(filtered, t)
		}
	}
	p.requestHistory[idx] = filtered
}

func (p *KeyPool) Cooldown(idx int, d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if until := time.Now().Add(d); p.cooldowns[idx].Before(until) {
		p.cooldowns[idx] = until
	}
	log.Printf("🧊 Key [%d] on cooldown for %s", idx, d)
}

// probeDue reports whether a quarantined key has waited long enough to be
// tried once more. Caller must hold p.mu.
func (p *KeyPool) probeDue(idx int, now time.Time) bool {
	return p.disabled[idx] && now.After(p.quarantineUntil[idx])
}

// Disable quarantines a key the upstream refused to authenticate. Each repeat
// offence doubles the wait, so a genuinely revoked key quickly stops costing
// requests while a merely throttled one comes back on its own.
func (p *KeyPool) Disable(idx int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.disabled[idx] = true
	p.strikes[idx]++

	backoff := quarantineBase << uint(min(p.strikes[idx]-1, 4))
	if backoff > quarantineMax {
		backoff = quarantineMax
	}
	p.quarantineUntil[idx] = time.Now().Add(backoff)
	log.Printf("🔑 Key [%d] quarantined for %s (strike %d)", idx, backoff, p.strikes[idx])
}

// MarkHealthy clears a quarantine once the key has answered. The strike count
// is deliberately kept: a key that keeps flapping earns a longer wait each
// time rather than probing every 15 minutes forever.
func (p *KeyPool) MarkHealthy(idx int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.disabled[idx] {
		return
	}
	p.disabled[idx] = false
	p.quarantineUntil[idx] = time.Time{}
	log.Printf("🔑 Key [%d] is answering again — back in the pool", idx)
}

// Enable puts a quarantined key back immediately and forgives its strikes.
// This is the operator saying "I fixed it", so take them at their word.
func (p *KeyPool) Enable(idx int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if idx < 0 || idx >= len(p.keys) {
		return false
	}
	p.disabled[idx] = false
	p.quarantineUntil[idx] = time.Time{}
	p.strikes[idx] = 0
	return true
}

func (p *KeyPool) ActiveCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for i := range p.keys {
		if !p.disabled[i] {
			n++
		}
	}
	return n
}

func (p *KeyPool) keyStatusLabel(i int, now time.Time) string {
	if p.disabled[i] {
		if p.probeDue(i, now) {
			return "probing"
		}
		return fmt.Sprintf("quarantined(%.0fm)", p.quarantineUntil[i].Sub(now).Minutes())
	}
	if cd := p.cooldowns[i]; now.Before(cd) {
		return fmt.Sprintf("cooling(%.0fs)", cd.Sub(now).Seconds())
	}
	// Distinct from cooling: nothing went wrong, we are just pacing the key to
	// stay under its quota.
	if t := p.rpmBlockedUntil(i, now); now.Before(t) {
		return fmt.Sprintf("throttled(%.0fs)", t.Sub(now).Seconds())
	}
	return "ready"
}

func (p *KeyPool) Status() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	parts := make([]string, len(p.keys))
	for i := range p.keys {
		parts[i] = fmt.Sprintf("[%d]:%s", i, p.keyStatusLabel(i, now))
	}
	return strings.Join(parts, " ")
}

// GetKeyDetails returns detailed status for each key in the pool
func (p *KeyPool) GetKeyDetails() []map[string]interface{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	details := make([]map[string]interface{}, len(p.keys))
	for i := range p.keys {
		p.cleanupOldRequests(i)
		keyDetail := map[string]interface{}{
			"index":               i,
			"key":                 maskKey(p.keys[i]),
			"disabled":            p.disabled[i],
			"requests_per_minute": p.requestsInLastMinute(i),
			"rpm_limit":           p.rpmLimit,
			"last_used":           p.lastUsed[i].Format(time.RFC3339),
			"cooldown_until":      p.cooldowns[i].Format(time.RFC3339),
			"quarantined_until":   p.quarantineUntil[i].Format(time.RFC3339),
		}
		keyDetail["status"] = p.keyStatusLabel(i, now)
		details[i] = keyDetail
	}
	return details
}

// adoptStateFrom carries per-key runtime state across a reload for every key
// that appears in both pools.
//
// A reload builds a whole new pool, which used to mean every cooldown, every
// disabled flag and the entire request history were silently discarded — so
// saving an unrelated setting from the dashboard made a pool of rate-limited
// keys look uniformly ready, and the next burst walked straight back into the
// provider's limit. Keys that are new in this pool start clean.
func (p *KeyPool) adoptStateFrom(old *KeyPool) {
	if old == nil {
		return
	}
	// Always old-then-new; the new pool is unpublished at this point, so this
	// cannot contend with anything.
	old.mu.Lock()
	defer old.mu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()

	prev := make(map[string]int, len(old.keys))
	for i, k := range old.keys {
		prev[k] = i
	}
	for i, k := range p.keys {
		j, ok := prev[k]
		if !ok {
			continue
		}
		p.cooldowns[i] = old.cooldowns[j]
		p.disabled[i] = old.disabled[j]
		p.quarantineUntil[i] = old.quarantineUntil[j]
		p.strikes[i] = old.strikes[j]
		p.lastUsed[i] = old.lastUsed[j]
		p.requestHistory[i] = append([]time.Time(nil), old.requestHistory[j]...)
	}
	// Keep the round-robin position too, so a reload doesn't dogpile whichever
	// key happens to sit at index 0.
	atomic.StoreUint64(&p.counter, atomic.LoadUint64(&old.counter))
}

// IncrementRequestCount records a request timestamp for a key and updates its lastUsed timestamp
func (p *KeyPool) IncrementRequestCount(idx int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cleanupOldRequests(idx)
	p.requestHistory[idx] = append(p.requestHistory[idx], time.Now())
	p.lastUsed[idx] = time.Now()
}

// ── Config ────────────────────────────────────

type Config struct {
	TargetBase    string
	GenaiBase     string
	Port          string
	MaxRetries    int
	CooldownSec   int
	RPMLimit      int
	MaxBodyMB     int
	OverrideModel string
}

// maxBodyBytes is the ceiling for a buffered request body.
func (c Config) maxBodyBytes() int64 { return int64(c.MaxBodyMB) << 20 }

// readLimitedBody buffers a request body, refusing anything past the ceiling.
//
// Both entry points have to hold the whole body in memory so a retry can
// replay it against the next key, so an unbounded read is an unbounded
// allocation — one large upload on a --network-only bind is enough.
func readLimitedBody(w http.ResponseWriter, r *http.Request, max int64) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	defer r.Body.Close()
	return io.ReadAll(http.MaxBytesReader(w, r.Body, max))
}

// bodyTooLarge distinguishes "you sent too much" from a read that failed.
func bodyTooLarge(err error) bool {
	var mbe *http.MaxBytesError
	return errors.As(err, &mbe)
}

func parseKeysFromEnv() ([]string, error) {
	raw := os.Getenv("API_KEYS")
	if raw == "" {
		return nil, fmt.Errorf("API_KEYS is required")
	}
	var keys []string
	for _, k := range strings.Split(raw, ",") {
		if k = strings.TrimSpace(k); k != "" {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("no valid API keys found in API_KEYS")
	}
	return keys, nil
}

func buildConfig() (Config, *KeyPool, error) {
	keys, err := parseKeysFromEnv()
	if err != nil {
		return Config{}, nil, err
	}
	cfg := Config{
		TargetBase:    strings.TrimRight(getenv("TARGET_BASE_URL", "https://integrate.api.nvidia.com/v1"), "/"),
		GenaiBase:     strings.TrimRight(getenv("GENAI_BASE_URL", "https://ai.api.nvidia.com"), "/"),
		Port:          getenv("PORT", "3000"),
		MaxRetries:    getenvInt("MAX_RETRIES", 10),
		CooldownSec:   getenvInt("COOLDOWN_SEC", 60),
		RPMLimit:      getenvInt("RPM_LIMIT", 0),
		MaxBodyMB:     getenvInt("MAX_BODY_MB", 32),
		OverrideModel: getenv("OVERRIDE_MODEL", ""),
	}
	return cfg, NewKeyPool(keys, cfg.RPMLimit), nil
}

func loadConfig() (Config, *KeyPool) {
	cfg, pool, err := buildConfig()
	if err != nil {
		log.Fatalf("❌ %v", err)
	}
	return cfg, pool
}

// reloadConfig re-reads .env and builds a fresh config and pool, carrying the
// runtime state of any surviving key over from old.
func reloadConfig(old *KeyPool) (Config, *KeyPool, error) {
	resetEnvToBaseline()
	loadDotEnv(".env")
	cfg, pool, err := buildConfig()
	if err != nil {
		return Config{}, nil, err
	}
	pool.adoptStateFrom(old)
	return cfg, pool, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// ── Server ────────────────────────────────────

type ServerState struct {
	mu   sync.RWMutex
	cfg  Config
	pool *KeyPool
	auth *AdminAuth
	mux  *http.ServeMux
}

func newServerState(cfg Config, pool *KeyPool, auth *AdminAuth) *ServerState {
	s := &ServerState{cfg: cfg, pool: pool, auth: auth, mux: http.NewServeMux()}
	s.mux.HandleFunc("/health", s.healthHandler)
	// Claude Code support: Anthropic Messages API translated to OpenAI upstream
	s.mux.HandleFunc("/v1/messages", s.anthropicHandler)
	s.mux.HandleFunc("/v1/messages/count_tokens", s.anthropicCountTokensHandler)
	// Admin surface — exposes masked keys and rewrites config, so it is gated
	// whenever the listener is reachable from off-box. See auth.go.
	s.mux.HandleFunc("/logs", s.guard(s.logsHandler))
	s.mux.HandleFunc("/dashboard", s.guard(s.dashboardHandler))
	s.mux.HandleFunc("/clear", s.guardWrite(s.clearHandler))
	s.mux.HandleFunc("/api/config", s.guardWrite(s.configHandler))
	s.mux.HandleFunc("/api/keys/enable", s.guardWrite(s.enableKeyHandler))
	// Requests a browser makes on its own. Without these they fall through to
	// the catch-all proxy, which spends a pooled key forwarding them upstream
	// — opening the dashboard was enough to do it.
	s.mux.HandleFunc("/sw.js", s.noContentHandler)
	s.mux.HandleFunc("/favicon.ico", s.noContentHandler)
	s.mux.HandleFunc("/robots.txt", s.noContentHandler)
	s.mux.HandleFunc("/", s.proxyHandler)
	return s
}

func (s *ServerState) noContentHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

type ConfigPayload struct {
	TargetBase string   `json:"targetBase"`
	GenaiBase  string   `json:"genaiBase"`
	Keys       []string `json:"keys"`
}

func (s *ServerState) configHandler(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	cfg := s.cfg
	keys := s.pool.keys
	s.mu.RUnlock()

	if r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		maskedKeys := make([]string, len(keys))
		for i, k := range keys {
			maskedKeys[i] = maskKey(k)
		}
		json.NewEncoder(w).Encode(ConfigPayload{
			TargetBase: cfg.TargetBase,
			GenaiBase:  cfg.GenaiBase,
			Keys:       maskedKeys,
		})
		return
	}

	if r.Method == http.MethodPost {
		var payload ConfigPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}

		payload.TargetBase = strings.TrimSpace(payload.TargetBase)
		payload.GenaiBase = strings.TrimSpace(payload.GenaiBase)

		s.mu.RLock()
		oldPool := s.pool
		currentKeys := oldPool.keys
		s.mu.RUnlock()

		reclaimed := make(map[int]bool)
		for i := range payload.Keys {
			k := strings.TrimSpace(payload.Keys[i])
			if k == "" {
				continue
			}
			// If the key is masked (contains "..." or is "****"), try to restore it from the current pool
			if strings.Contains(k, "...") || k == "****" {
				for j, ck := range currentKeys {
					if !reclaimed[j] && maskKey(ck) == k {
						k = ck
						reclaimed[j] = true
						break
					}
				}
			}
			payload.Keys[i] = k
		}
		payload.Keys = filterEmpty(payload.Keys)

		if payload.TargetBase == "" {
			http.Error(w, "targetBase is required", http.StatusBadRequest)
			return
		}
		if payload.GenaiBase == "" {
			http.Error(w, "genaiBase is required", http.StatusBadRequest)
			return
		}
		if len(payload.Keys) == 0 {
			http.Error(w, "at least one API key is required", http.StatusBadRequest)
			return
		}

		// Only touch what the form actually owns. Anything else in the file
		// (OVERRIDE_MODEL, ADMIN_TOKEN, comments, custom vars) is preserved.
		if err := updateDotEnv(".env", map[string]string{
			"TARGET_BASE_URL": payload.TargetBase,
			"GENAI_BASE_URL":  payload.GenaiBase,
			"API_KEYS":        strings.Join(payload.Keys, ","),
		}); err != nil {
			log.Printf("❌ Failed to write .env: %v", err)
			http.Error(w, "failed to save config", http.StatusInternalServerError)
			return
		}

		log.Printf("📝 Configuration updated via API")

		newCfg, newPool, err := reloadConfig(oldPool)
		if err != nil {
			log.Printf("⚠️ Immediate reload failed: %v", err)
			w.WriteHeader(http.StatusAccepted)
			return
		}

		s.mu.Lock()
		s.cfg = newCfg
		s.pool = newPool
		s.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "reloaded"})
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}

func filterEmpty(ss []string) []string {
	filtered := make([]string, 0, len(ss))
	for _, s := range ss {
		if s != "" {
			filtered = append(filtered, s)
		}
	}
	return filtered
}

// enableKeyHandler lifts a quarantine on demand, for when you have fixed
// whatever the provider was unhappy about and don't want to wait out the
// backoff.
func (s *ServerState) enableKeyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var payload struct {
		Index int `json:"index"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	s.mu.RLock()
	pool := s.pool
	s.mu.RUnlock()

	if !pool.Enable(payload.Index) {
		http.Error(w, "no such key", http.StatusNotFound)
		return
	}
	log.Printf("🔑 Key [%d] re-enabled by hand", payload.Index)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "enabled"})
}

func (s *ServerState) healthHandler(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	pool := s.pool
	s.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")

	details := pool.GetKeyDetails()
	jsonDetails, err := json.Marshal(details)
	if err != nil {
		http.Error(w, "failed to marshal key details", http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, `{"status":"ok","keys":%d,"details":%s}`, len(pool.keys), jsonDetails)
}

func (s *ServerState) proxyHandler(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	cfg := s.cfg
	pool := s.pool
	s.mu.RUnlock()

	bodyBytes, err := readLimitedBody(w, r, cfg.maxBodyBytes())
	if err != nil {
		if bodyTooLarge(err) {
			http.Error(w, fmt.Sprintf("alvus: request body over the %d MB limit", cfg.MaxBodyMB), http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}

	// Route /genai/ paths to GenaiBase, everything else to TargetBase
	var target string
	if strings.Contains(r.URL.Path, "/genai/") {
		target = cfg.GenaiBase + r.URL.Path
		if r.URL.RawQuery != "" {
			target += "?" + r.URL.RawQuery
		}
	} else {
		path := r.URL.Path
		if strings.HasSuffix(cfg.TargetBase, "/v1") && strings.HasPrefix(path, "/v1") {
			path = path[3:]
		}
		if r.URL.RawQuery != "" {
			path += "?" + r.URL.RawQuery
		}
		target = cfg.TargetBase + path
	}

	log.Printf("→ %s %s (%d bytes)", r.Method, target, len(bodyBytes))

	// A streaming response must not carry a deadline: the old 120s client
	// timeout counted body reads, so any SSE turn longer than two minutes was
	// cut off mid-stream. Non-streaming calls keep a per-attempt cap.
	opts := rotateOpts{PerAttemptTimeout: nonStreamTimeout}
	if wantsStream(bodyBytes, r.Header) {
		opts.PerAttemptTimeout = 0
	}

	out, rerr := withKeyRotation(r.Context(), cfg, pool, opts, "", func(key string) (*http.Request, error) {
		req, err := http.NewRequest(r.Method, target, bytes.NewReader(bodyBytes))
		if err != nil {
			return nil, err
		}
		for k, vals := range r.Header {
			for _, v := range vals {
				req.Header.Add(k, v)
			}
		}
		req.Header.Set("Authorization", "Bearer "+key)
		return req, nil
	})
	if rerr != nil {
		http.Error(w, "alvus: "+rerr.msg, rerr.status)
		return
	}
	// LIFO: body closes first, then the attempt context is released.
	defer out.release()
	defer out.resp.Body.Close()

	resp := out.resp
	for k, vals := range resp.Header {
		for _, v := range vals {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)

	if f, ok := w.(http.Flusher); ok {
		buf := make([]byte, 4096)
		for {
			n, readErr := resp.Body.Read(buf)
			if n > 0 {
				w.Write(buf[:n])
				f.Flush()
			}
			if readErr != nil {
				break
			}
		}
	} else {
		io.Copy(w, resp.Body)
	}

	logUsage(out.key, out.idx, r.Method, target, resp.StatusCode, len(bodyBytes))

	if resp.StatusCode >= 400 {
		log.Printf("⚠️ %s %s → %d (Terminal Client Error, no retry)", r.Method, target, resp.StatusCode)
		return
	}
	log.Printf("✅ %s %s → %d (key[%d], attempt %d)", r.Method, target, resp.StatusCode, out.idx, out.attempt)
}

func (s *ServerState) logsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	usageMu.Lock()
	masked := make([]LogEntry, len(usageLogs))
	for i, entry := range usageLogs {
		masked[i] = LogEntry{
			Timestamp:       entry.Timestamp,
			Key:             maskKey(entry.Key),
			KeyIndex:        entry.KeyIndex,
			Method:          entry.Method,
			URL:             entry.URL,
			Status:          entry.Status,
			RequestBodySize: entry.RequestBodySize,
		}
	}
	data, _ := json.Marshal(masked)
	usageMu.Unlock()
	w.Write(data)
}

func (s *ServerState) clearHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	usageMu.Lock()
	usageLogs = []LogEntry{}
	usageMu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "cleared"})
}

// ── .env Watcher ──────────────────────────────

func watchEnvFile(state *ServerState, stop <-chan struct{}) {
	var lastMod time.Time
	if info, err := os.Stat(".env"); err == nil {
		lastMod = info.ModTime()
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			info, err := os.Stat(".env")
			if err != nil {
				if os.IsNotExist(err) {
					log.Printf("⚠️ .env file deleted — keeping current config")
				}
				continue
			}
			if !info.ModTime().After(lastMod) {
				continue
			}
			lastMod = info.ModTime()
			time.Sleep(100 * time.Millisecond) // debounce

			log.Printf("🔄 .env changed — reloading...")
			state.mu.RLock()
			oldPool := state.pool
			state.mu.RUnlock()

			newCfg, newPool, err := reloadConfig(oldPool)
			if err != nil {
				log.Printf("❌ Reload failed: %v", err)
				continue
			}
			state.mu.Lock()
			state.cfg = newCfg
			state.pool = newPool
			state.mu.Unlock()
			log.Printf("✅ Reloaded — %d keys, target: %s, genai: %s", len(newPool.keys), newCfg.TargetBase, newCfg.GenaiBase)
		}
	}
}

// ── Main ──────────────────────────────────────

func main() {
	isLocal := flag.Bool("local", false, "Bind to 127.0.0.1 (default; kept for compatibility)")
	isNetwork := flag.Bool("network-only", false, "Bind to 0.0.0.0 — reachable over the LAN (requires an admin token)")
	flag.Parse()

	// Loopback by default. Alvus holds live API keys and serves an admin
	// surface that can rewrite the upstream URL, so exposing it to the network
	// has to be a deliberate act rather than the default.
	host := "127.0.0.1"
	if *isNetwork {
		host = "0.0.0.0"
	}
	if *isLocal {
		host = "127.0.0.1"
	}

	// Record the real environment before .env can pollute it, so reloads can
	// tell the two apart.
	snapshotEnv()
	loadDotEnv(".env")
	cfg, pool := loadConfig()
	auth := newAdminAuth(host, os.Getenv("ADMIN_TOKEN"))
	state := newServerState(cfg, pool, auth)

	stop := make(chan struct{})
	go watchEnvFile(state, stop)

	// Graceful shutdown on SIGINT/SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	server := &http.Server{
		Addr:    host + ":" + cfg.Port,
		Handler: state.mux,
		// Only the header deadline is safe to set here. ReadTimeout would cap
		// how long a client has to upload a large body, and WriteTimeout would
		// cut off exactly the long SSE streams this proxy exists to carry —
		// the same trap the upstream client avoids. This bounds a connection
		// that opens and then never sends a complete request line.
		ReadHeaderTimeout: 20 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		<-sigCh
		log.Printf("🛑 Shutting down gracefully...")
		close(stop)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("❌ Shutdown error: %v", err)
		}
	}()

	log.Printf("⚡ Alvus %s:%s → %s | genai → %s (%d keys)", host, cfg.Port, cfg.TargetBase, cfg.GenaiBase, len(pool.keys))
	if isLoopbackHost(host) {
		log.Printf("   Dashboard: http://127.0.0.1:%s/dashboard (loopback only — use --network-only for LAN)", cfg.Port)
	}
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("❌ Server error: %v", err)
	}
}
