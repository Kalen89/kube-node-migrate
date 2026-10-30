package nodemigrate

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCopyArchiveChecksDigestAndExecutableEntry(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, entry, want string
		typeflag          byte
		wrongDigest       bool
	}{
		{name: "regular executable", entry: "pv-migrate", typeflag: tar.TypeReg},
		{name: "wrong digest", entry: "pv-migrate", typeflag: tar.TypeReg, wrongDigest: true, want: "SHA-256"},
		{name: "missing executable", entry: "README", typeflag: tar.TypeReg, want: "缺少"},
		{name: "symlink executable", entry: "pv-migrate", typeflag: tar.TypeSymlink, want: "类型或大小"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var data bytes.Buffer
			compressed := gzip.NewWriter(&data)
			archive := tar.NewWriter(compressed)
			header := &tar.Header{Name: test.entry, Typeflag: test.typeflag, Mode: 0o755, Linkname: "other"}
			if test.typeflag == tar.TypeReg {
				header.Size = 3
			}
			if err := archive.WriteHeader(header); err != nil {
				t.Fatal(err)
			}
			if test.typeflag == tar.TypeReg {
				if _, err := archive.Write([]byte("bin")); err != nil {
					t.Fatal(err)
				}
			}
			if err := archive.Close(); err != nil {
				t.Fatal(err)
			}
			if err := compressed.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "pv-migrate.tar.gz")
			if err := os.WriteFile(path, data.Bytes(), 0o600); err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(data.Bytes())
			expected := hex.EncodeToString(digest[:])
			if test.wrongDigest {
				expected = strings.Repeat("0", 64)
			}
			err := readCopyArchive(path, expected, func(reader io.Reader, size int64) error {
				_, err := io.CopyN(io.Discard, reader, size)
				return err
			})
			if test.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("got %v, want error containing %q", err, test.want)
			}
		})
	}
	if err := ValidateCopyArchive("unused", "mips"); err == nil {
		t.Fatal("accepted unsupported architecture")
	}
}
