package main

import (
	"bytes"
	"context"
	"encoding/json"
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

type KeyPool struct {
	counter        uint64
	keys           []string
	cooldowns      []time.Time
	disabled       []bool
	requestHistory [][]time.Time // timestamps of requests in the last 60s per key
	lastUsed       []time.Time
	mu             sync.Mutex
}

func NewKeyPool(keys []string) *KeyPool {
	return &KeyPool{
		keys:           keys,
		cooldowns:      make([]time.Time, len(keys)),
		disabled:       make([]bool, len(keys)),
		requestHistory: make([][]time.Time, len(keys)),
		lastUsed:       make([]time.Time, len(keys)),
	}
}

func (p *KeyPool) TimeUntilAvailable() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	var soonest time.Duration = -1
	for i, cd := range p.cooldowns {
		if p.disabled[i] {
			continue
		}
		if now.After(cd) {
			return 0
		}
		if wait := cd.Sub(now); soonest < 0 || wait < soonest {
			soonest = wait
		}
	}
	return soonest
}

func (p *KeyPool) Next() (int, string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := len(p.keys)
	start := int(atomic.AddUint64(&p.counter, 1)-1) % n
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		if !p.disabled[idx] && time.Now().After(p.cooldowns[idx]) {
			return idx, p.keys[idx], true
		}
	}
	return -1, "", false
}

// requestsInLastMinute returns the number of requests made by a key in the last 60 seconds
func (p *KeyPool) requestsInLastMinute(idx int) int {
	cutoff := time.Now().Add(-60 * time.Second)
	count := 0
	for _, t := range p.requestHistory[idx] {
		if t.After(cutoff) {
			count++
		}
	}
	return count
}

// cleanupOldRequests removes request timestamps older than 60 seconds
func (p *KeyPool) cleanupOldRequests(idx int) {
	cutoff := time.Now().Add(-60 * time.Second)
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

func (p *KeyPool) Disable(idx int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.disabled[idx] = true
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
	cd := p.cooldowns[i]
	switch {
	case p.disabled[i]:
		return "disabled"
	case now.After(cd):
		return "ready"
	default:
		return fmt.Sprintf("cooling(%.0fs)", cd.Sub(now).Seconds())
	}
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
			"last_used":           p.lastUsed[i].Format(time.RFC3339),
			"cooldown_until":      p.cooldowns[i].Format(time.RFC3339),
		}
		keyDetail["status"] = p.keyStatusLabel(i, now)
		details[i] = keyDetail
	}
	return details
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
	OverrideModel string
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
		OverrideModel: getenv("OVERRIDE_MODEL", ""),
	}
	return cfg, NewKeyPool(keys), nil
}

func loadConfig() (Config, *KeyPool) {
	cfg, pool, err := buildConfig()
	if err != nil {
		log.Fatalf("❌ %v", err)
	}
	return cfg, pool
}

func reloadConfig() (Config, *KeyPool, error) {
	for _, k := range envKeys {
		os.Unsetenv(k)
	}
	loadDotEnv(".env")
	return buildConfig()
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
	// Block service worker requests to prevent 404s and unnecessary upstream proxying
	s.mux.HandleFunc("/sw.js", s.swHandler)
	s.mux.HandleFunc("/", s.proxyHandler)
	return s
}

func (s *ServerState) swHandler(w http.ResponseWriter, r *http.Request) {
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
		currentKeys := s.pool.keys
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

		newCfg, newPool, err := reloadConfig()
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

	var bodyBytes []byte
	if r.Body != nil {
		var err error
		bodyBytes, err = io.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			http.Error(w, "failed to read request body", http.StatusBadRequest)
			return
		}
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

	pool.IncrementRequestCount(out.idx)
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
			newCfg, newPool, err := reloadConfig()
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

	loadDotEnv(".env")
	cfg, pool := loadConfig()
	auth := newAdminAuth(host, os.Getenv("ADMIN_TOKEN"))
	state := newServerState(cfg, pool, auth)

	stop := make(chan struct{})
	go watchEnvFile(state, stop)

	// Graceful shutdown on SIGINT/SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	server := &http.Server{Addr: host + ":" + cfg.Port, Handler: state.mux}

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
