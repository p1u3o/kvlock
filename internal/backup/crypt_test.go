package backup

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"io"
	"strings"
	"testing"
)

func testKey() []byte { return bytes.Repeat([]byte{7}, 32) }

func encrypt(t *testing.T, pt []byte, chunk int) []byte {
	t.Helper()
	var out bytes.Buffer
	w, err := newEncryptWriter(&out, testKey(), chunk)
	if err != nil {
		t.Fatal(err)
	}
	// Uneven writes to exercise buffering.
	for i := 0; i < len(pt); {
		n := 1 + i%37
		if i+n > len(pt) {
			n = len(pt) - i
		}
		if _, err := w.Write(pt[i : i+n]); err != nil {
			t.Fatal(err)
		}
		i += n
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func decrypt(ct []byte, key []byte) ([]byte, error) {
	r, err := NewDecryptReader(bytes.NewReader(ct), key)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(r)
}

func TestRoundTripSizes(t *testing.T) {
	for _, n := range []int{0, 1, 63, 64, 65, 128, 1000} {
		pt := make([]byte, n)
		_, _ = rand.Read(pt)
		ct := encrypt(t, pt, 64)
		got, err := decrypt(ct, testKey())
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if !bytes.Equal(got, pt) {
			t.Fatalf("n=%d: mismatch", n)
		}
	}
}

func TestTamperTruncateWrongKey(t *testing.T) {
	pt := bytes.Repeat([]byte("kvrocks"), 100)
	ct := encrypt(t, pt, 64)
	bad := append([]byte(nil), ct...)
	bad[len(bad)/2] ^= 1
	if _, err := decrypt(bad, testKey()); err == nil {
		t.Fatal("tampered stream decrypted")
	}
	// Drop the final chunk: cut at a chunk boundary.
	full := 4 + 64 + 16
	cut := ct[:headerLen+2*full]
	if _, err := decrypt(cut, testKey()); err == nil || !strings.Contains(err.Error(), "truncated") {
		t.Fatalf("truncated stream: %v", err)
	}
	if _, err := decrypt(append(append([]byte(nil), ct...), 0), testKey()); err == nil {
		t.Fatal("trailing data accepted")
	}
	other := bytes.Repeat([]byte{8}, 32)
	if _, err := decrypt(ct, other); err == nil {
		t.Fatal("wrong key accepted")
	}
	if _, err := decrypt([]byte("not a backup at all......"), testKey()); err == nil {
		t.Fatal("bad magic accepted")
	}
}

func TestParseKey(t *testing.T) {
	k := testKey()
	for _, in := range [][]byte{k, []byte(hex.EncodeToString(k) + "\n"), []byte(base64.StdEncoding.EncodeToString(k))} {
		got, err := ParseKey(in)
		if err != nil || !bytes.Equal(got, k) {
			t.Fatalf("ParseKey(%q) = %v", in, err)
		}
	}
	if _, err := ParseKey([]byte("short")); err == nil {
		t.Fatal("short key accepted")
	}
}
