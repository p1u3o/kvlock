package backup

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"testing"
)

func evilArchive(t *testing.T, h *tar.Header) []byte {
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(h); err != nil {
		t.Fatal(err)
	}
	tw.Close()
	gz.Close()
	return b.Bytes()
}

func TestExtractRejectsUnsafe(t *testing.T) {
	for _, h := range []*tar.Header{
		{Name: "../x", Typeflag: tar.TypeReg},
		{Name: "/etc/x", Typeflag: tar.TypeReg},
		{Name: "a/../../x", Typeflag: tar.TypeReg},
		{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "/etc"},
	} {
		if err := ExtractArchive(bytes.NewReader(evilArchive(t, h)), t.TempDir()+"/d"); err == nil {
			t.Errorf("%q accepted", h.Name)
		}
	}
}
