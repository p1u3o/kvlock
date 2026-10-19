package logx

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// Logger emits structured JSON lines. Secrets must never be passed in fields.
type Logger struct {
	mu     sync.Mutex
	node   string
	mode   string
	fields map[string]string
}

func New(node, mode string) *Logger {
	return &Logger{node: node, mode: mode}
}

func (l *Logger) log(level, msg string, fields map[string]any) {
	e := map[string]any{
		"ts":    time.Now().UTC().Format(time.RFC3339Nano),
		"level": level,
		"msg":   msg,
		"node":  l.node,
		"mode":  l.mode,
	}
	for k, v := range fields {
		e[k] = v
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_ = json.NewEncoder(os.Stderr).Encode(e)
}

// Info etc. keep signatures small; fields is optional.
func (l *Logger) Info(msg string, fields map[string]any)  { l.log("info", msg, fields) }
func (l *Logger) Warn(msg string, fields map[string]any)  { l.log("warn", msg, fields) }
func (l *Logger) Error(msg string, fields map[string]any) { l.log("error", msg, fields) }
