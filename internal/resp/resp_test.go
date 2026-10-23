package resp_test

import (
	"bufio"
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/p1u3o/kvlock/internal/resp"
)

func TestReadCommand(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("*2\r\n$4\r\nPING\r\n$3\r\nabc\r\nPING x\r\n*1\r\n$99999999\r\n"))
	got, err := resp.ReadCommand(r)
	if err != nil || !reflect.DeepEqual(got, []string{"PING", "abc"}) {
		t.Fatalf("array: %v %v", got, err)
	}
	got, err = resp.ReadCommand(r)
	if err != nil || !reflect.DeepEqual(got, []string{"PING", "x"}) {
		t.Fatalf("inline: %v %v", got, err)
	}
	if _, err = resp.ReadCommand(r); err != resp.ErrProtocol {
		t.Fatalf("oversized bulk: want protocol error, got %v", err)
	}
}

func TestWrite(t *testing.T) {
	var b bytes.Buffer
	w := bufio.NewWriter(&b)
	for _, v := range []any{resp.Simple("OK"), resp.Error("ERR bad\r\nx"), 7, "hi", resp.Null{}, resp.NullArray{}, []any{"a", []string{"b"}}} {
		if err := resp.Write(w, v); err != nil {
			t.Fatal(err)
		}
	}
	w.Flush()
	want := "+OK\r\n-ERR bad  x\r\n:7\r\n$2\r\nhi\r\n$-1\r\n*-1\r\n*2\r\n$1\r\na\r\n*1\r\n$1\r\nb\r\n"
	if b.String() != want {
		t.Fatalf("got %q", b.String())
	}
}
