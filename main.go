package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

const Version = "1.0.0"

func newStorageClient(ctx context.Context) (*storage.Client, error) {
	o := []option.ClientOption{}
	keyfile := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if keyfile != "" {
		o = append(o, option.WithServiceAccountFile(keyfile))
	}
	return storage.NewClient(ctx, o...)
}

func getGoogleUploadChunkSize() (int, error) {
	size := os.Getenv("GOOGLE_UPLOAD_CHUNK_SIZE")
	if size == "" {
		// Default to a chunk size of 0, which forces a single chunk
		// and a single request. This is highly optimized
		// for small files.
		return 0, nil
	}
	return strconv.Atoi(size)
}

func isGsPath(path string) bool {
	return len(path) > 5 && path[:5] == "gs://"
}

func splitGsPath(path string) (string, string) {
	bits := strings.SplitN(path[5:], "/", 2)
	if len(bits) == 1 {
		return bits[0], ""
	}
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
	chunkSize, err := getGoogleUploadChunkSize()
	if err != nil {
		return nil, err
	}
	bucket, object := splitGsPath(path)
	w := client.Bucket(bucket).Object(object).NewWriter(ctx)
	w.ChunkSize = chunkSize
	return w, nil
}

func printVersion(exitcode int) {
	prog := path.Base(os.Args[0])
	fmt.Fprintf(
		os.Stdout,
		"%s version: %s (%s on %s/%s; %s)\n",
		prog, Version, runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.Compiler,
	)
	os.Exit(exitcode)
}

func printHelp(exitcode int) {
	prog := path.Base(os.Args[0])
	fmt.Fprintf(
		os.Stdout,
		"usage: %s source_file target_file\n",
		prog,
	)
	os.Exit(exitcode)
}

func main() {
	if len(os.Args) == 2 {
		switch os.Args[1] {
		case "--version", "-v":
			printVersion(0)
		case "--help", "-h":
			printHelp(0)
		}
	}

	if len(os.Args) != 3 {
		printHelp(1)
	}

	ctx := context.Background()
	client, err := newStorageClient(ctx)
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
