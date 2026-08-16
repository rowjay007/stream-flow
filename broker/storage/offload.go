package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Offloader uploads and downloads snapshot blobs to/from an external store.
type Offloader interface {
	Upload(ctx context.Context, bucket, key string, r io.Reader, size int64) (string, error)
	Download(ctx context.Context, bucket, key string) (io.ReadCloser, error)
	List(ctx context.Context, bucket, prefix string) ([]string, error)
}

// LocalOffloader stores blobs in a local directory (useful for testing).
type LocalOffloader struct {
	Dir string
}

func NewLocalOffloader(dir string) *LocalOffloader {
	os.MkdirAll(dir, 0o755)
	return &LocalOffloader{Dir: dir}
}

func (l *LocalOffloader) Upload(ctx context.Context, bucket, key string, r io.Reader, size int64) (string, error) {
	bucketDir := filepath.Join(l.Dir, bucket)
	if err := os.MkdirAll(bucketDir, 0o755); err != nil {
		return "", err
	}
	dst := filepath.Join(bucketDir, key)
	f, err := os.Create(dst)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if _, err := io.Copy(f, r); err != nil {
		return "", err
	}
	return dst, nil
}

func (l *LocalOffloader) Download(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	p := filepath.Join(l.Dir, bucket, key)
	return os.Open(p)
}

func (l *LocalOffloader) List(ctx context.Context, bucket, prefix string) ([]string, error) {
	dir := filepath.Join(l.Dir, bucket)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if prefix == "" || (len(name) >= len(prefix) && name[:len(prefix)] == prefix) {
			out = append(out, name)
		}
	}
	return out, nil
}

// S3Offloader uploads to an S3-compatible endpoint using MinIO client.
type S3Offloader struct {
	client *minio.Client
}

func NewS3Offloader(endpoint, accessKey, secretKey string, useSSL bool) (*S3Offloader, error) {
	mc, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, err
	}
	return &S3Offloader{client: mc}, nil
}

func (s *S3Offloader) Upload(ctx context.Context, bucket, key string, r io.Reader, size int64) (string, error) {
	// Ensure bucket exists
	exists, err := s.client.BucketExists(ctx, bucket)
	if err != nil {
		return "", err
	}
	if !exists {
		if err := s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return "", err
		}
	}
	info, err := s.client.PutObject(ctx, bucket, key, r, size, minio.PutObjectOptions{})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("s3://%s/%s (etag=%s)", bucket, info.Key, info.ETag), nil
}

func (s *S3Offloader) Download(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	obj, err := s.client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *S3Offloader) List(ctx context.Context, bucket, prefix string) ([]string, error) {
	var out []string
	// use ListObjects to collect keys
	opts := minio.ListObjectsOptions{Prefix: prefix, Recursive: true}
	for obj := range s.client.ListObjects(ctx, bucket, opts) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		out = append(out, obj.Key)
	}
	return out, nil
}
