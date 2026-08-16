package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"streamflow/broker/storage"
	"time"
)

func main() {
	backend := flag.String("backend", "local", "offload backend: local or s3")
	dir := flag.String("dir", "./backups", "local dir for local backend")
	endpoint := flag.String("endpoint", "", "s3 endpoint")
	access := flag.String("access", "", "s3 access key")
	secret := flag.String("secret", "", "s3 secret key")
	bucket := flag.String("bucket", "snapshots", "bucket name")
	key := flag.String("key", "snapshot.snap", "object key")
	download := flag.Bool("download", false, "download object instead of upload")
	flag.Parse()
	if flag.NArg() < 1 {
		fmt.Println("usage: backup [flags] <file-to-upload>")
		os.Exit(2)
	}
	file := flag.Arg(0)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var off storage.Offloader
	if *backend == "s3" {
		s, err := storage.NewS3Offloader(*endpoint, *access, *secret, false)
		if err != nil {
			fmt.Println("new s3 offloader:", err)
			os.Exit(1)
		}
		off = s
	} else {
		off = storage.NewLocalOffloader(*dir)
	}
	if *download {
		rc, err := off.Download(ctx, *bucket, *key)
		if err != nil {
			fmt.Println("download:", err)
			os.Exit(1)
		}
		defer rc.Close()
		out, err := os.Create(file)
		if err != nil {
			fmt.Println("create out:", err)
			os.Exit(1)
		}
		defer out.Close()
		if _, err := io.Copy(out, rc); err != nil {
			fmt.Println("write out:", err)
			os.Exit(1)
		}
		fmt.Println("downloaded to:", file)
		return
	}

	f, err := os.Open(file)
	if err != nil {
		fmt.Println("open file:", err)
		os.Exit(1)
	}
	defer f.Close()

	fi, _ := f.Stat()
	info, err := off.Upload(ctx, *bucket, *key, f, fi.Size())
	if err != nil {
		fmt.Println("upload:", err)
		os.Exit(1)
	}
	fmt.Println("uploaded:", info)
}
