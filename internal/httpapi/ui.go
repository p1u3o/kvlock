package httpapi

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed ui/index.html ui/app.js ui/style.css
var uiFiles embed.FS

// uiCSP keeps the page self-contained: no inline script, no external
// origins.
const uiCSP = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'"

func uiHandler() http.Handler {
	sub, _ := fs.Sub(uiFiles, "ui")
	files := http.StripPrefix("/ui/", http.FileServer(http.FS(sub)))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		h := w.Header()
		h.Set("Content-Security-Policy", uiCSP)
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Cache-Control", "no-cache")
		switch r.URL.Path {
		case "/", "/ui", "/ui/", "/ui/index.html":
			b, _ := uiFiles.ReadFile("ui/index.html")
			h.Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write(b)
		default:
			files.ServeHTTP(w, r)
		}
	})
}
