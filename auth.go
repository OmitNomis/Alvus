package main

// Access control for the admin surface (/dashboard, /logs, /clear, /api/config).
//
// These routes expose masked keys and — via POST /api/config — the ability to
// rewrite TARGET_BASE_URL and the key pool. That makes them a credential
// exfiltration path if reachable by anyone else, so they are never served
// unauthenticated to a non-loopback listener.
//
// Policy:
//
//	loopback bind (the default)  → open, no token. Same trust boundary as the
//	                               .env file sitting next to the binary.
//	non-loopback bind            → ADMIN_TOKEN required. If unset, one is
//	                               generated at startup and logged once.
//
// The proxy routes themselves are deliberately NOT gated: agents point at them
// with a dummy key and gating would break every existing config.

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"log"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
)

const adminCookie = "alvus_token"

// AdminAuth holds the token guarding the admin routes. An empty token means
// the admin surface is open (loopback-only binds).
type AdminAuth struct {
	token string
}

func newAdminAuth(host, envToken string) *AdminAuth {
	if isLoopbackHost(host) {
		if envToken == "" {
			return &AdminAuth{}
		}
		// Explicit token on a loopback bind: honour it anyway.
		return &AdminAuth{token: envToken}
	}

	token := envToken
	if token == "" {
		token = randomToken()
		log.Printf("🔐 Admin token (set ADMIN_TOKEN in .env to pin it): %s", token)
		log.Printf("   Dashboard: http://<this-host>:PORT/dashboard?token=%s", token)
	} else {
		log.Printf("🔐 Admin routes protected by ADMIN_TOKEN from the environment")
	}
	return &AdminAuth{token: token}
}

func isLoopbackHost(host string) bool {
	if host == "" {
		return false // empty means "all interfaces"
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func randomToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failing is not something we can paper over: an
		// unauthenticated admin surface on a LAN is exactly what this guards.
		log.Fatalf("❌ Failed to generate admin token: %v", err)
	}
	return hex.EncodeToString(b)
}

// enabled reports whether a token is actually required.
func (a *AdminAuth) enabled() bool { return a != nil && a.token != "" }

// presented pulls a candidate token off the request: header, bearer, cookie,
// or query string. The query form exists so the dashboard can be opened from a
// browser; it hands back a cookie so later fetch() calls carry it implicitly.
func presentedToken(r *http.Request) string {
	if t := r.Header.Get("X-Alvus-Token"); t != "" {
		return t
	}
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		return strings.TrimPrefix(h, "Bearer ")
	}
	if c, err := r.Cookie(adminCookie); err == nil && c.Value != "" {
		return c.Value
	}
	return r.URL.Query().Get("token")
}

func (a *AdminAuth) valid(r *http.Request) bool {
	if !a.enabled() {
		return true
	}
	got := presentedToken(r)
	if got == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(a.token)) == 1
}

// guard wraps an admin handler with the token check.
func (s *ServerState) guard(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.auth.valid(r) {
			http.Error(w, "alvus: unauthorized — supply the admin token", http.StatusUnauthorized)
			return
		}
		h(w, r)
	}
}

// sameOrigin reports whether a state-changing request came from the dashboard
// itself rather than some other page in the user's browser.
//
// Cookie auth alone is forgeable cross-site, and a plain <form> POST needs no
// preflight — so without this, any site you visit could rewrite Alvus's config
// (and therefore its upstream URL) using your cookie. Two independent checks:
//
//	Content-Type must be application/json — forms can only send text/plain,
//	urlencoded or multipart, and a JSON body forces a CORS preflight the
//	attacker's origin cannot pass.
//
//	Origin, when present, must match the Host we were reached on.
//
// Non-browser callers (curl, scripts) send no Origin and set the JSON type, so
// they are unaffected.
func sameOrigin(r *http.Request) bool {
	ct := r.Header.Get("Content-Type")
	if mt, _, err := mime.ParseMediaType(ct); err != nil || mt != "application/json" {
		return false
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true // not a browser-initiated cross-site request
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	return u.Host == r.Host
}

// guardWrite wraps a mutating admin handler with the token check plus the
// cross-origin check.
func (s *ServerState) guardWrite(h http.HandlerFunc) http.HandlerFunc {
	return s.guard(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && !sameOrigin(r) {
			http.Error(w, "alvus: cross-origin or non-JSON write rejected", http.StatusForbidden)
			return
		}
		h(w, r)
	})
}

// issueCookie mints the admin cookie after a successful ?token= handoff so the
// dashboard's XHRs authenticate without putting the token in every URL.
func (s *ServerState) issueCookie(w http.ResponseWriter, r *http.Request) {
	if !s.auth.enabled() || r.URL.Query().Get("token") == "" {
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     adminCookie,
		Value:    s.auth.token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	})
}
