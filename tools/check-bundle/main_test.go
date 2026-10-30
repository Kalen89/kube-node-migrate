package main

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBundleVerificationRejectsIncompleteOrChangedPayload(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	source := filepath.Join(directory, "source")
	if err := os.WriteFile(source, []byte("payload"), 0o755); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("payload"))
	input := bundleFile{source: source, size: 7, mode: 0o755, digest: hex.EncodeToString(digest[:])}
	for _, test := range []struct {
		name    string
		entries []string
		content string
		mode    int64
		want    string
	}{
		{name: "valid", entries: []string{"app"}, content: "payload", mode: 0o755},
		{name: "missing", want: "incomplete"},
		{name: "extra", entries: []string{"other"}, content: "payload", mode: 0o755, want: "unexpected"},
		{name: "duplicate", entries: []string{"app", "app"}, content: "payload", mode: 0o755, want: "duplicate"},
		{name: "changed content", entries: []string{"app"}, content: "changed", mode: 0o755, want: "content changed"},
		{name: "lost execute bit", entries: []string{"app"}, content: "payload", mode: 0o644, want: "mismatched"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bundle.tar.gz")
			output, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			compressed := gzip.NewWriter(output)
			archive := tar.NewWriter(compressed)
			for _, name := range test.entries {
				if err := archive.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeReg, Size: int64(len(test.content)), Mode: test.mode}); err != nil {
					t.Fatal(err)
				}
				if _, err := archive.Write([]byte(test.content)); err != nil {
					t.Fatal(err)
				}
			}
			for _, close := range []func() error{archive.Close, compressed.Close, output.Close} {
				if err := close(); err != nil {
					t.Fatal(err)
				}
			}
			err = verifyBundle(path, map[string]bundleFile{"app": input})
			if test.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("got %v, want error containing %q", err, test.want)
			}
		})
	}
	path := filepath.Join(directory, "created.tar.gz")
	files := map[string]bundleFile{"examples/demo.yaml": input}
	if err := createBundle(path, files); err != nil {
		t.Fatal(err)
	}
	if err := verifyBundle(path, files); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-8] ^= 1 // Corrupt gzip CRC after the valid tar payload.
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := verifyBundle(path, files); err == nil {
		t.Fatal("accepted bundle with a corrupted gzip trailer")
	}
}
