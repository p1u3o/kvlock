package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/config"
)

func TestBackupDisabledByDefault(t *testing.T) {
	c, err := config.Parse(nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Backup.Enabled || c.Backup.Interval != 6*time.Hour || c.Backup.SSE != "AES256" {
		t.Fatalf("%+v", c.Backup)
	}
}

func TestBackupConfig(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "key")
	os.WriteFile(keyFile, []byte(strings.Repeat("ab", 32)+"\n"), 0o600)
	skFile := filepath.Join(dir, "sk")
	os.WriteFile(skFile, []byte("secret\n"), 0o600)
	t.Setenv("AWS_ACCESS_KEY_ID", "AKID")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	base := []string{"--backup", "--backup-dir=/b", "--backup-s3-endpoint=s3.fr-par.scw.cloud", "--backup-s3-bucket=bk", "--backup-s3-prefix=p", "--backup-key-file=" + keyFile, "--backup-s3-secret-key-file=" + skFile, "--backup-s3-sse=none"}
	c, err := config.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	b := c.Backup
	if len(b.Key) != 32 || b.Key[0] != 0xab || b.AccessKey != "AKID" || b.SecretKey != "secret" || b.Prefix != "p/" || b.SSE != "none" {
		t.Fatalf("resolved wrongly (prefix %q sse %q)", b.Prefix, b.SSE)
	}
	bad := map[string][]string{
		"no dir":      {"--backup", "--backup-s3-endpoint=e", "--backup-s3-bucket=b", "--backup-key-file=" + keyFile, "--backup-s3-secret-key-file=" + skFile},
		"scheme":      append(append([]string{}, base...), "--backup-s3-endpoint=https://x"),
		"bad sse":     append(append([]string{}, base...), "--backup-s3-sse=kms"),
		"short":       append(append([]string{}, base...), "--backup-interval=10s"),
		"missing key": append(append([]string{}, base...), "--backup-key-file="),
	}
	t.Setenv("KVLOCK_BACKUP_KEY", "")
	for name, args := range bad {
		if _, err := config.Parse(args); err == nil {
			t.Errorf("%s: accepted", name)
		} else if strings.Contains(err.Error(), "secret") && strings.Contains(err.Error(), "abab") {
			t.Errorf("%s: error leaks key", name)
		}
	}
}
