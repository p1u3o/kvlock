package backup

import (
	"context"
	"io"
	"sort"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	sse "github.com/minio/minio-go/v7/pkg/encrypt"
)

// Object is one stored backup.
type Object struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// Store is the object storage the backups go to.
type Store interface {
	Put(ctx context.Context, key string, r io.Reader) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	// List returns the objects under prefix sorted by key.
	List(ctx context.Context, prefix string) ([]Object, error)
	Delete(ctx context.Context, key string) error
}

// S3Config configures an S3-compatible store.
type S3Config struct {
	Endpoint  string // host[:port], no scheme
	Region    string
	Bucket    string
	AccessKey string
	SecretKey string
	Insecure  bool // plain HTTP
	// SSE requests x-amz-server-side-encryption: AES256 on upload.
	SSE bool
	// PartSize is the multipart part size (memory used per upload).
	PartSize uint64
}

// S3Store implements Store with minio-go.
type S3Store struct {
	c   *minio.Client
	cfg S3Config
}

func NewS3Store(cfg S3Config) (*S3Store, error) {
	c, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: !cfg.Insecure,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, err
	}
	if cfg.PartSize == 0 {
		cfg.PartSize = 16 << 20
	}
	return &S3Store{c: c, cfg: cfg}, nil
}

func (s *S3Store) Put(ctx context.Context, key string, r io.Reader) error {
	opts := minio.PutObjectOptions{ContentType: "application/octet-stream", PartSize: s.cfg.PartSize}
	if s.cfg.SSE {
		opts.ServerSideEncryption = sse.NewSSE()
	}
	_, err := s.c.PutObject(ctx, s.cfg.Bucket, key, r, -1, opts)
	return err
}

func (s *S3Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	o, err := s.c.GetObject(ctx, s.cfg.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	// GetObject is lazy; Stat surfaces missing objects/auth errors now.
	if _, err := o.Stat(); err != nil {
		o.Close()
		return nil, err
	}
	return o, nil
}

func (s *S3Store) List(ctx context.Context, prefix string) ([]Object, error) {
	var out []Object
	for o := range s.c.ListObjects(ctx, s.cfg.Bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if o.Err != nil {
			return nil, o.Err
		}
		out = append(out, Object{Key: o.Key, Size: o.Size, LastModified: o.LastModified})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (s *S3Store) Delete(ctx context.Context, key string) error {
	return s.c.RemoveObject(ctx, s.cfg.Bucket, key, minio.RemoveObjectOptions{})
}
