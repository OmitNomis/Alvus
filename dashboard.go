package main

// The dashboard UI lives in dashboard.html and is embedded into the binary at
// build time, so the single-static-binary promise is unaffected. embed is
// stdlib, so the zero-dependency promise holds too — and it works under
// `go build *.go` with no go.mod.

import (
	_ "embed"
	"net/http"
)

//go:embed dashboard.html
var dashboardHTML string

func (s *ServerState) dashboardHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(dashboardHTML))
}
