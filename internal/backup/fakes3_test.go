package backup

import (
	"bufio"
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeS3 is a minimal path-style S3 server: single and multipart PUT,
// GET/HEAD, ListObjectsV2, DELETE, GetBucketLocation. It decodes
// aws-chunked bodies and records the SSE header of each upload.
type fakeS3 struct {
	mu      sync.Mutex
	bucket  string
	objs    map[string][]byte
	mod     map[string]time.Time
	uploads map[string]map[int][]byte
	sse     map[string]string
	seq     int
	failPut bool
}

func newFakeS3(t *testing.T, bucket string) (*fakeS3, *httptest.Server) {
	f := &fakeS3{bucket: bucket, objs: map[string][]byte{}, mod: map[string]time.Time{}, uploads: map[string]map[int][]byte{}, sse: map[string]string{}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakeS3) store(t *testing.T, srv *httptest.Server) *S3Store {
	u, _ := url.Parse(srv.URL)
	s, err := NewS3Store(S3Config{Endpoint: u.Host, Region: "fr-par", Bucket: f.bucket, AccessKey: "AK", SecretKey: "SK", Insecure: true, SSE: true, PartSize: 5 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func (f *fakeS3) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var k []string
	for x := range f.objs {
		k = append(k, x)
	}
	sort.Strings(k)
	return k
}

func readBody(r *http.Request) ([]byte, error) {
	b, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-") || strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") {
		return decodeAWSChunked(b)
	}
	return b, nil
}

func decodeAWSChunked(b []byte) ([]byte, error) {
	br := bufio.NewReader(bytes.NewReader(b))
	var out bytes.Buffer
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("chunk header: %w", err)
		}
		hexSize, _, _ := strings.Cut(strings.TrimSpace(line), ";")
		n, err := strconv.ParseInt(hexSize, 16, 64)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return out.Bytes(), nil // trailers, if any, are ignored
		}
		if _, err := io.CopyN(&out, br, n); err != nil {
			return nil, err
		}
		if _, err := br.ReadString('\n'); err != nil {
			return nil, err
		}
	}
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
	if parts[0] != f.bucket {
		http.Error(w, "NoSuchBucket", 404)
		return
	}
	key := ""
	if len(parts) == 2 {
		key = parts[1]
	}
	q := r.URL.Query()
	switch {
	case key == "" && q.Has("location"):
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">fr-par</LocationConstraint>`)
	case key == "" && r.Method == http.MethodGet:
		f.list(w, q.Get("prefix"))
	case r.Method == http.MethodPost && q.Has("uploads"):
		f.seq++
		id := fmt.Sprintf("up%d", f.seq)
		f.uploads[id] = map[int][]byte{}
		f.sse[key] = r.Header.Get("X-Amz-Server-Side-Encryption")
		fmt.Fprintf(w, `<InitiateMultipartUploadResult><Bucket>%s</Bucket><Key>%s</Key><UploadId>%s</UploadId></InitiateMultipartUploadResult>`, f.bucket, key, id)
	case r.Method == http.MethodPut && q.Has("uploadId"):
		up, ok := f.uploads[q.Get("uploadId")]
		if !ok || f.failPut {
			http.Error(w, "NoSuchUpload", 500)
			return
		}
		b, err := readBody(r)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		n, _ := strconv.Atoi(q.Get("partNumber"))
		up[n] = b
		w.Header().Set("ETag", fmt.Sprintf(`"etag%d"`, n))
	case r.Method == http.MethodPost && q.Has("uploadId"):
		up, ok := f.uploads[q.Get("uploadId")]
		if !ok {
			http.Error(w, "NoSuchUpload", 404)
			return
		}
		io.Copy(io.Discard, r.Body)
		var nums []int
		for n := range up {
			nums = append(nums, n)
		}
		sort.Ints(nums)
		var all []byte
		for _, n := range nums {
			all = append(all, up[n]...)
		}
		f.objs[key], f.mod[key] = all, time.Now()
		delete(f.uploads, q.Get("uploadId"))
		fmt.Fprintf(w, `<CompleteMultipartUploadResult><Bucket>%s</Bucket><Key>%s</Key><ETag>"x"</ETag></CompleteMultipartUploadResult>`, f.bucket, key)
	case r.Method == http.MethodDelete && q.Has("uploadId"):
		delete(f.uploads, q.Get("uploadId"))
		w.WriteHeader(204)
	case r.Method == http.MethodPut:
		if f.failPut {
			http.Error(w, "InternalError", 500)
			return
		}
		b, err := readBody(r)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		f.objs[key], f.mod[key] = b, time.Now()
		f.sse[key] = r.Header.Get("X-Amz-Server-Side-Encryption")
		w.Header().Set("ETag", `"x"`)
	case r.Method == http.MethodGet || r.Method == http.MethodHead:
		b, ok := f.objs[key]
		if !ok {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(404)
			if r.Method == http.MethodGet {
				fmt.Fprint(w, `<Error><Code>NoSuchKey</Code><Message>no</Message></Error>`)
			}
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(b)))
		w.Header().Set("Last-Modified", f.mod[key].UTC().Format(http.TimeFormat))
		w.Header().Set("ETag", `"x"`)
		if rg := r.Header.Get("Range"); rg != "" && r.Method == http.MethodGet {
			http.ServeContent(w, r, key, f.mod[key], bytes.NewReader(b))
			return
		}
		if r.Method == http.MethodGet {
			w.Write(b)
		}
	case r.Method == http.MethodDelete:
		delete(f.objs, key)
		w.WriteHeader(204)
	default:
		http.Error(w, "unsupported", 400)
	}
}

func (f *fakeS3) list(w http.ResponseWriter, prefix string) {
	type content struct {
		Key          string
		Size         int
		LastModified string
		ETag         string
	}
	type result struct {
		XMLName     xml.Name `xml:"ListBucketResult"`
		Name        string
		Prefix      string
		KeyCount    int
		IsTruncated bool
		Contents    []content
	}
	res := result{Name: f.bucket, Prefix: prefix}
	var keys []string
	for k := range f.objs {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		res.Contents = append(res.Contents, content{Key: k, Size: len(f.objs[k]), LastModified: f.mod[k].UTC().Format(time.RFC3339), ETag: `"x"`})
	}
	res.KeyCount = len(keys)
	w.Header().Set("Content-Type", "application/xml")
	xml.NewEncoder(w).Encode(res)
}
