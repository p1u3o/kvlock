package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/p1u3o/kvlock/internal/backup"
)

func TestBackupDecrypt(t *testing.T) {
	hexKey := strings.Repeat("0f", 32)
	key, _ := backup.ParseKey([]byte(hexKey))
	var enc bytes.Buffer
	w, _ := backup.NewEncryptWriter(&enc, key)
	w.Write([]byte("hello archive"))
	w.Close()
	t.Setenv("KVLOCK_BACKUP_KEY", hexKey)
	var out, errb bytes.Buffer
	if rc := runDecrypt(nil, bytes.NewReader(enc.Bytes()), &out, &errb); rc != 0 || out.String() != "hello archive" {
		t.Fatalf("rc=%d out=%q err=%q", rc, out.String(), errb.String())
	}
	t.Setenv("KVLOCK_BACKUP_KEY", strings.Repeat("1f", 32))
	out.Reset()
	if rc := runDecrypt(nil, bytes.NewReader(enc.Bytes()), &out, &errb); rc == 0 || out.Len() != 0 {
		t.Fatal("wrong key accepted")
	}
}

func TestRestoreNeedsTarget(t *testing.T) {
	var out, errb bytes.Buffer
	if rc := runRestore([]string{"--backup-s3-endpoint=x", "--backup-s3-bucket=y"}, &out, &errb); rc != 2 {
		t.Fatalf("rc=%d", rc)
	}
}
