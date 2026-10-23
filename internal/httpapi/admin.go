package httpapi

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// HandoverRequest asks the leader to hand the lock to a replica.
type HandoverRequest struct {
	// To optionally names the replica as "ip:port", as the leader's INFO
	// replication reports it. Empty picks the least-lagged online replica.
	To string `json:"to,omitempty"`
	// MaxLag overrides --handover-max-lag when >= 0 (nil = default).
	MaxLag *int64 `json:"max_lag,omitempty"`
	// Timeout overrides --handover-timeout when > 0 (Go duration string).
	Timeout string `json:"timeout,omitempty"`
}

// HandoverResult describes a handover outcome.
type HandoverResult struct {
	OK bool `json:"ok"`
	// Stage reached: "checks", "waiting", "committed", "done".
	Stage     string `json:"stage"`
	Target    string `json:"target,omitempty"`
	Lag       int64  `json:"lag"`
	NewLeader string `json:"new_leader,omitempty"`
	Error     string `json:"error,omitempty"`
	// StillLeader is true when the handover was aborted before this node
	// gave anything up.
	StillLeader bool `json:"still_leader"`
}

// Handoverer runs planned handovers.
type Handoverer interface {
	Handover(ctx context.Context, req HandoverRequest) (HandoverResult, error)
}

// Admin configures admin endpoints.
type Admin struct {
	// Token, when set, must be sent as "Authorization: Bearer <token>".
	// When empty, admin endpoints accept loopback clients only.
	Token    string
	Handover Handoverer
}

func (a Admin) authorized(r *http.Request) bool {
	if a.Token != "" {
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		return ok && subtle.ConstantTimeCompare([]byte(got), []byte(a.Token)) == 1
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (a Admin) handleHandover(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, HandoverResult{Error: "use POST"})
		return
	}
	if !a.authorized(r) {
		writeJSON(w, http.StatusForbidden, HandoverResult{Error: "forbidden: send the admin token, or connect from localhost when no token is configured"})
		return
	}
	var req HandoverRequest
	body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if len(bytes.TrimSpace(body)) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, HandoverResult{Error: "bad JSON: " + err.Error()})
			return
		}
	}
	if req.Timeout != "" {
		if _, err := time.ParseDuration(req.Timeout); err != nil {
			writeJSON(w, http.StatusBadRequest, HandoverResult{Error: "bad timeout: " + err.Error()})
			return
		}
	}
	res, err := a.Handover.Handover(r.Context(), req)
	if err != nil {
		res.OK = false
		res.Error = err.Error()
		code := http.StatusConflict
		if !res.StillLeader && res.Stage == "committed" {
			// We gave up the lock but did not see a successor in time.
			code = http.StatusAccepted
		}
		writeJSON(w, code, res)
		return
	}
	res.OK = true
	writeJSON(w, http.StatusOK, res)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// RequestHandover is the client side used by `kvrocks-leader handover`.
func RequestHandover(ctx context.Context, baseURL, token string, req HandoverRequest) (HandoverResult, error) {
	var res HandoverResult
	body, _ := json.Marshal(req)
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/handover", bytes.NewReader(body))
	if err != nil {
		return res, err
	}
	hr.Header.Set("Content-Type", "application/json")
	if token != "" {
		hr.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(hr)
	if err != nil {
		return res, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err := json.Unmarshal(b, &res); err != nil {
		return res, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if resp.StatusCode != http.StatusOK {
		if res.Error == "" {
			res.Error = resp.Status
		}
		return res, errors.New(res.Error)
	}
	return res, nil
}
