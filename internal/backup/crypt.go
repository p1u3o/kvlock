// Package backup implements optional encrypted kvrocks backups to
// S3-compatible object storage, and the matching restore.
package backup

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Encrypted stream format ("KVLB1"), all integers big-endian:
//
//	header  = magic "KVLKBAK1" (8 bytes) || chunk_size uint32 || nonce_prefix (7 random bytes)
//	chunk_i = sealed_len uint32 || AES-256-GCM-Seal(key, nonce_i, plaintext_i, aad=header)
//	nonce_i = nonce_prefix (7) || i uint32 || final (1 byte: 1 on the last chunk, else 0)
//
// Every chunk except the last carries exactly chunk_size plaintext bytes;
// the last one carries 0..chunk_size bytes and has final=1. Decryption
// fails on any tampering, reordering, truncation (no final chunk) or
// trailing data. The plaintext is a gzip-compressed tar of the kvrocks
// backup directory.
const (
	magic            = "KVLKBAK1"
	headerLen        = len(magic) + 4 + 7
	DefaultChunkSize = 64 * 1024
	maxChunkSize     = 16 * 1024 * 1024
	gcmOverhead      = 16
)

// ParseKey accepts a 32-byte key as raw bytes, 64 hex characters or
// standard/URL base64 (surrounding whitespace ignored).
func ParseKey(b []byte) ([]byte, error) {
	if len(b) == 32 {
		return append([]byte(nil), b...), nil
	}
	s := strings.TrimSpace(string(b))
	if len(s) == 64 {
		if k, err := hex.DecodeString(s); err == nil {
			return k, nil
		}
	}
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if k, err := enc.DecodeString(s); err == nil && len(k) == 32 {
			return k, nil
		}
	}
	if len(s) == 32 {
		return []byte(s), nil
	}
	return nil, errors.New("backup key must be 32 bytes (raw, 64 hex chars or base64)")
}

type encWriter struct {
	w      io.Writer
	aead   cipher.AEAD
	header []byte
	prefix [7]byte
	buf    []byte
	size   int
	n      uint32
	closed bool
	err    error
}

// NewEncryptWriter returns a writer that encrypts into w. Close must be
// called to write the final chunk; it does not close w.
func NewEncryptWriter(w io.Writer, key []byte) (io.WriteCloser, error) {
	return newEncryptWriter(w, key, DefaultChunkSize)
}

func newEncryptWriter(w io.Writer, key []byte, chunk int) (*encWriter, error) {
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	e := &encWriter{w: w, aead: aead, size: chunk, buf: make([]byte, 0, chunk)}
	if _, err := rand.Read(e.prefix[:]); err != nil {
		return nil, err
	}
	h := make([]byte, 0, headerLen)
	h = append(h, magic...)
	h = binary.BigEndian.AppendUint32(h, uint32(chunk))
	h = append(h, e.prefix[:]...)
	e.header = h
	if _, err := w.Write(h); err != nil {
		return nil, err
	}
	return e, nil
}

func newAEAD(key []byte) (cipher.AEAD, error) {
	if len(key) != 32 {
		return nil, errors.New("backup key must be 32 bytes")
	}
	blk, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(blk)
}

func nonce(prefix [7]byte, i uint32, final bool) []byte {
	n := make([]byte, 12)
	copy(n, prefix[:])
	binary.BigEndian.PutUint32(n[7:], i)
	if final {
		n[11] = 1
	}
	return n
}

func (e *encWriter) seal(final bool) error {
	if e.n == ^uint32(0) {
		return errors.New("backup stream too long")
	}
	ct := e.aead.Seal(nil, nonce(e.prefix, e.n, final), e.buf, e.header)
	e.n++
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(ct)))
	if _, err := e.w.Write(l[:]); err != nil {
		return err
	}
	_, err := e.w.Write(ct)
	e.buf = e.buf[:0]
	return err
}

func (e *encWriter) Write(p []byte) (int, error) {
	if e.err != nil {
		return 0, e.err
	}
	if e.closed {
		return 0, errors.New("write after close")
	}
	total := len(p)
	for len(p) > 0 {
		// Only seal a full chunk once more data arrives, so the last chunk
		// is always sealed as final by Close.
		if len(e.buf) == e.size {
			if e.err = e.seal(false); e.err != nil {
				return total - len(p), e.err
			}
		}
		k := copy(e.buf[len(e.buf):e.size], p)
		e.buf = e.buf[:len(e.buf)+k]
		p = p[k:]
	}
	return total, nil
}

func (e *encWriter) Close() error {
	if e.closed {
		return e.err
	}
	e.closed = true
	if e.err != nil {
		return e.err
	}
	e.err = e.seal(true)
	return e.err
}

type decReader struct {
	r      io.Reader
	aead   cipher.AEAD
	header []byte
	prefix [7]byte
	size   int
	n      uint32
	buf    []byte
	done   bool
	err    error
}

// NewDecryptReader authenticates and decrypts a KVLB1 stream.
func NewDecryptReader(r io.Reader, key []byte) (io.Reader, error) {
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	h := make([]byte, headerLen)
	if _, err := io.ReadFull(r, h); err != nil {
		return nil, fmt.Errorf("read backup header: %w", err)
	}
	if !bytes.Equal(h[:len(magic)], []byte(magic)) {
		return nil, errors.New("not a kvlock backup (bad magic)")
	}
	size := int(binary.BigEndian.Uint32(h[len(magic):]))
	if size <= 0 || size > maxChunkSize {
		return nil, fmt.Errorf("invalid chunk size %d", size)
	}
	d := &decReader{r: r, aead: aead, header: h, size: size}
	copy(d.prefix[:], h[len(magic)+4:])
	return d, nil
}

func (d *decReader) next() error {
	var l [4]byte
	if _, err := io.ReadFull(d.r, l[:]); err != nil {
		if err == io.EOF {
			return errors.New("backup truncated: final chunk missing")
		}
		return fmt.Errorf("read chunk: %w", err)
	}
	n := int(binary.BigEndian.Uint32(l[:]))
	if n < gcmOverhead || n > d.size+gcmOverhead {
		return fmt.Errorf("invalid chunk length %d", n)
	}
	ct := make([]byte, n)
	if _, err := io.ReadFull(d.r, ct); err != nil {
		return fmt.Errorf("read chunk: %w", err)
	}
	// Try as a middle chunk first (only full-size ones can be), then final.
	var pt []byte
	var err error
	if n == d.size+gcmOverhead {
		pt, err = d.aead.Open(nil, nonce(d.prefix, d.n, false), ct, d.header)
	}
	if pt == nil {
		pt, err = d.aead.Open(nil, nonce(d.prefix, d.n, true), ct, d.header)
		if err != nil {
			return errors.New("backup authentication failed (wrong key or corrupted data)")
		}
		d.done = true
		var one [1]byte
		if k, _ := io.ReadFull(d.r, one[:]); k != 0 {
			return errors.New("trailing data after final chunk")
		}
	}
	d.n++
	d.buf = pt
	return nil
}

func (d *decReader) Read(p []byte) (int, error) {
	for len(d.buf) == 0 {
		if d.err != nil {
			return 0, d.err
		}
		if d.done {
			return 0, io.EOF
		}
		if err := d.next(); err != nil {
			d.err = err
			return 0, err
		}
	}
	k := copy(p, d.buf)
	d.buf = d.buf[k:]
	return k, nil
}
