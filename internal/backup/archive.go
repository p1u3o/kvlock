package backup

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// WriteArchive writes a gzip-compressed tar of dir (directories and
// regular files only, paths relative to dir) to w.
func WriteArchive(w io.Writer, dir string) error {
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		switch {
		case info.IsDir():
			return tw.WriteHeader(&tar.Header{Typeflag: tar.TypeDir, Name: name + "/", Mode: 0o755, ModTime: info.ModTime()})
		case info.Mode().IsRegular():
			f, err := os.Open(p)
			if err != nil {
				return err
			}
			defer f.Close()
			if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: name, Mode: 0o644, Size: info.Size(), ModTime: info.ModTime()}); err != nil {
				return err
			}
			n, err := io.Copy(tw, f)
			if err != nil {
				return err
			}
			if n != info.Size() {
				return fmt.Errorf("%s changed size while archiving", name)
			}
			return nil
		default:
			return fmt.Errorf("unsupported file type in backup dir: %s", name)
		}
	})
	if err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
}

// ExtractArchive unpacks a WriteArchive stream into dir, which must be
// empty or not exist. Absolute paths, ".." and non-regular entries are
// rejected.
func ExtractArchive(r io.Reader, dir string) error {
	if ents, err := os.ReadDir(dir); err == nil && len(ents) > 0 {
		return fmt.Errorf("target dir %s is not empty", dir)
	} else if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	gz, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		clean := path.Clean(h.Name)
		if path.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, "../") || clean == "." {
			return fmt.Errorf("unsafe path in archive: %q", h.Name)
		}
		target := filepath.Join(dir, filepath.FromSlash(clean))
		switch h.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported entry type %q for %s", h.Typeflag, h.Name)
		}
	}
	// Drain so the decrypting reader verifies the final chunk.
	if _, err := io.Copy(io.Discard, gz); err != nil {
		return err
	}
	if err := gz.Close(); err != nil {
		return err
	}
	_, err = io.Copy(io.Discard, r)
	return err
}
