package main

import (
	"context"
	"io"
	"log"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

const CREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS"

func isGsPath(path string) bool {
	return len(path) > 5 && path[:5] == "gs://"
}

func splitGsPath(path string) (string, string) {
	bits := strings.SplitN(path[5:], "/", 2)
	return bits[0], bits[1]
}

func readerForPath(path string, client *storage.Client, ctx context.Context) (io.ReadCloser, error) {
	if !isGsPath(path) {
		return os.Open(path)
	}
	bucket, object := splitGsPath(path)
	return client.Bucket(bucket).Object(object).NewReader(ctx)
}

func writerForPath(path string, client *storage.Client, ctx context.Context) (io.WriteCloser, error) {
	if !isGsPath(path) {
		return os.Create(path)
	}
	bucket, object := splitGsPath(path)
	w := client.Bucket(bucket).Object(object).NewWriter(ctx)
	// Explicitly disable chunking to send in one request
	w.ChunkSize = 0
	return w, nil
}

func main() {
	if len(os.Args) != 3 {
		log.Fatal("!! wrong number of arguments")
		os.Exit(1)
	}

	keyfile := os.Getenv(CREDENTIALS)
	if keyfile == "" {
		log.Fatal("!! missing ", CREDENTIALS)
	}

	ctx := context.Background()
	opt := option.WithServiceAccountFile(keyfile)
	client, err := storage.NewClient(ctx, opt)
	if err != nil {
		log.Fatal(err)
	}

	src, err := readerForPath(os.Args[1], client, ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer src.Close()

	dest, err := writerForPath(os.Args[2], client, ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer dest.Close()

	if _, err := io.Copy(dest, src); err != nil {
		log.Fatal(err)
	}
}
