package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

const Version = "1.0.0"
const CREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS"

func isGsPath(path string) bool {
	return len(path) > 5 && path[:5] == "gs://"
}

func splitGsPath(path string) (string, string) {
	bits := strings.SplitN(path[5:], "/", 2)
	return bits[0], bits[1]
}

func readerForPath(path string, client *storage.Client, ctx context.Context) (io.ReadCloser, error) {
	if path == "-" {
		return os.Stdin, nil
	}
	if !isGsPath(path) {
		return os.Open(path)
	}
	bucket, object := splitGsPath(path)
	return client.Bucket(bucket).Object(object).NewReader(ctx)
}

func writerForPath(path string, client *storage.Client, ctx context.Context) (io.WriteCloser, error) {
	if path == "-" {
		return os.Stdout, nil
	}
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
	if len(os.Args) == 2 {
		if os.Args[1] == "--version" || os.Args[1] == "-v" {
			fmt.Fprintf(os.Stdout, "%s version: %s (%s on %s/%s; %s)\n", os.Args[0], Version, runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.Compiler)
			os.Exit(0)
		}
	}

	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "!! wrong number of arguments")
		os.Exit(1)
	}

	keyfile := os.Getenv(CREDENTIALS)
	if keyfile == "" {
		fmt.Fprintln(os.Stderr, "!! missing", CREDENTIALS)
		os.Exit(1)
	}

	ctx := context.Background()
	opt := option.WithServiceAccountFile(keyfile)
	client, err := storage.NewClient(ctx, opt)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	src, err := readerForPath(os.Args[1], client, ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	defer src.Close()

	dest, err := writerForPath(os.Args[2], client, ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	defer dest.Close()

	if _, err := io.Copy(dest, src); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
