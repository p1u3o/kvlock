// Package resp is a minimal RESP2 reader/writer for the sidecar's own
// small servers (the Sentinel-compatible listener and test fakes).
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// MaxArgs and MaxBulk bound what a client may send.
const (
	MaxArgs = 1024
	MaxBulk = 64 * 1024
)

// ErrProtocol is returned for malformed input.
var ErrProtocol = errors.New("resp: protocol error")

// ReadCommand reads one command: a RESP array of bulk strings, or an
// inline command line.
func ReadCommand(r *bufio.Reader) ([]string, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return []string{}, nil
	}
	if line[0] != '*' {
		return strings.Fields(line), nil
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil || n < 0 || n > MaxArgs {
		return nil, ErrProtocol
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		h, err := readLine(r)
		if err != nil {
			return nil, err
		}
		if len(h) == 0 || h[0] != '$' {
			return nil, ErrProtocol
		}
		l, err := strconv.Atoi(h[1:])
		if err != nil || l < 0 || l > MaxBulk {
			return nil, ErrProtocol
		}
		buf := make([]byte, l+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		if buf[l] != '\r' || buf[l+1] != '\n' {
			return nil, ErrProtocol
		}
		args = append(args, string(buf[:l]))
	}
	return args, nil
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) > MaxBulk {
		return "", ErrProtocol
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// Value types for Write.
type (
	// Simple is a +status reply.
	Simple string
	// Error is a -error reply; the text should start with a code such as ERR.
	Error string
	// Null is a null bulk string ($-1).
	Null struct{}
	// NullArray is a null array (*-1).
	NullArray struct{}
)

// Write encodes v: Simple, Error, int, int64, string (bulk), []byte (bulk),
// Null, NullArray, []string and []any (arrays, nested).
func Write(w *bufio.Writer, v any) error {
	switch x := v.(type) {
	case Simple:
		fmt.Fprintf(w, "+%s\r\n", clean(string(x)))
	case Error:
		fmt.Fprintf(w, "-%s\r\n", clean(string(x)))
	case int:
		fmt.Fprintf(w, ":%d\r\n", x)
	case int64:
		fmt.Fprintf(w, ":%d\r\n", x)
	case string:
		fmt.Fprintf(w, "$%d\r\n%s\r\n", len(x), x)
	case []byte:
		fmt.Fprintf(w, "$%d\r\n%s\r\n", len(x), x)
	case Null:
		w.WriteString("$-1\r\n")
	case NullArray:
		w.WriteString("*-1\r\n")
	case []string:
		fmt.Fprintf(w, "*%d\r\n", len(x))
		for _, s := range x {
			fmt.Fprintf(w, "$%d\r\n%s\r\n", len(s), s)
		}
	case []any:
		fmt.Fprintf(w, "*%d\r\n", len(x))
		for _, e := range x {
			if err := Write(w, e); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("resp: cannot encode %T", v)
	}
	return nil
}

func clean(s string) string {
	return strings.NewReplacer("\r", " ", "\n", " ").Replace(s)
}
