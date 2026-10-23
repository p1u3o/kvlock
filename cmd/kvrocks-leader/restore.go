package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/p1u3o/kvlock/internal/backup"
	"github.com/p1u3o/kvlock/internal/config"
)

func newStore(b *config.Backup) (*backup.S3Store, error) {
	return backup.NewS3Store(backup.S3Config{Endpoint: b.Endpoint, Region: b.Region, Bucket: b.Bucket,
		AccessKey: b.AccessKey, SecretKey: b.SecretKey, Insecure: b.Insecure, SSE: b.SSE == "AES256"})
}

// runRestore: kvlock restore [--object KEY|--list] --target DIR + S3 flags.
func runRestore(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("restore", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var b config.Backup
	b.RegisterStoreFlags(fs)
	cluster := fs.String("cluster", os.Getenv("CLUSTER_NAME"), "Cluster whose latest backup to restore ($CLUSTER_NAME)")
	object := fs.String("object", "", "Exact object key to restore (default: latest backup of --cluster)")
	target := fs.String("target", "", "Directory to unpack into; must be empty or missing")
	list := fs.Bool("list", false, "List the cluster's backups and exit")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *cluster == "" {
		*cluster = "main"
	}
	if !*list && *target == "" {
		fmt.Fprintln(stderr, "restore: --target is required")
		return 2
	}
	if err := b.ResolveStore(); err != nil {
		fmt.Fprintln(stderr, "restore:", err)
		return 2
	}
	st, err := newStore(&b)
	if err != nil {
		fmt.Fprintln(stderr, "restore:", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *list {
		objs, err := backup.ListBackups(ctx, st, b.Prefix, *cluster)
		if err != nil {
			fmt.Fprintln(stderr, "restore:", err)
			return 1
		}
		for _, o := range objs {
			fmt.Fprintf(stdout, "%s\t%d\t%s\n", o.Key, o.Size, o.LastModified.UTC().Format("2006-01-02T15:04:05Z"))
		}
		return 0
	}
	key, err := backup.Restore(ctx, st, b.Prefix, *cluster, *object, b.Key, *target)
	if err != nil {
		fmt.Fprintf(stderr, "restore %s: %v\n", key, err)
		return 1
	}
	fmt.Fprintf(stdout, "restored %s into %s\n", key, *target)
	return 0
}

// runDecrypt: kvlock backup-decrypt < obj.tar.gz.kvlb > obj.tar.gz
func runDecrypt(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("backup-decrypt", flag.ContinueOnError)
	fs.SetOutput(stderr)
	keyFile := fs.String("backup-key-file", os.Getenv("KVLOCK_BACKUP_KEY_FILE"), "Encryption key file; or $KVLOCK_BACKUP_KEY")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	var raw []byte
	var err error
	if *keyFile != "" {
		if raw, err = os.ReadFile(*keyFile); err != nil {
			fmt.Fprintln(stderr, "backup-decrypt: read key file:", err)
			return 2
		}
	} else {
		raw = []byte(os.Getenv("KVLOCK_BACKUP_KEY"))
	}
	key, err := backup.ParseKey(raw)
	if err != nil {
		fmt.Fprintln(stderr, "backup-decrypt:", err)
		return 2
	}
	dr, err := backup.NewDecryptReader(stdin, key)
	if err == nil {
		_, err = io.Copy(stdout, dr)
	}
	if err != nil {
		fmt.Fprintln(stderr, "backup-decrypt:", err)
		return 1
	}
	return 0
}
