package backup

import (
	"context"
	"errors"
	"io"
)

// Restore downloads key (or the newest backup of cluster when key is ""),
// decrypts it and unpacks it into dir, which must be empty or missing.
// It returns the key that was restored.
func Restore(ctx context.Context, s Store, prefix, cluster, key string, encKey []byte, dir string) (string, error) {
	if key == "" {
		objs, err := ListBackups(ctx, s, prefix, cluster)
		if err != nil {
			return "", err
		}
		if len(objs) == 0 {
			return "", errors.New("no backups found")
		}
		key = objs[len(objs)-1].Key
	}
	rc, err := s.Get(ctx, key)
	if err != nil {
		return key, err
	}
	defer rc.Close()
	return key, Unpack(rc, encKey, dir)
}

// Unpack decrypts a KVLB1 stream and extracts it into dir.
func Unpack(r io.Reader, encKey []byte, dir string) error {
	dr, err := NewDecryptReader(r, encKey)
	if err != nil {
		return err
	}
	return ExtractArchive(dr, dir)
}
