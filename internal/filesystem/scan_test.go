//go:build linux

package filesystem

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func digest(t *testing.T, path string) Digest {
	t.Helper()
	s, err := Scan(t.Context(), path, "digest")
	if err != nil {
		t.Fatal(err)
	}
	if s.Version != Version || len(s.SHA256) != 64 {
		t.Fatalf("invalid summary: %+v", s)
	}
	return s.Digest
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestContentAndPermissionsAffectDigest(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data")
	writeFile(t, path, "original")
	first := digest(t, dir)
	if first.Entries != 2 || first.Bytes != 8 {
		t.Fatalf("unexpected summary: %+v", first)
	}
	if got := digest(t, dir); got != first {
		t.Fatal("unchanged tree produced a different digest")
	}
	writeFile(t, path, "modified")
	second := digest(t, dir)
	if second.SHA256 == first.SHA256 {
		t.Fatal("same-sized content change was missed")
	}
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	if digest(t, dir).SHA256 == second.SHA256 {
		t.Fatal("permission change was missed")
	}
	probe, err := Scan(t.Context(), dir, "probe")
	if err != nil {
		t.Fatal(err)
	}
	if probe.SHA256 != "" || probe.Bytes != 8 || probe.Entries != 2 {
		t.Fatalf("invalid probe: %+v", probe)
	}
	free, err := Scan(t.Context(), dir, "free")
	if err != nil {
		t.Fatal(err)
	}
	if free.Entries != 0 || free.SHA256 != "" || free.FreeBytes == 0 {
		t.Fatalf("invalid free summary: %+v", free)
	}
}

func TestSymlinksAreRecordedWithoutReadingTheirTargets(t *testing.T) {
	dir, outside := t.TempDir(), t.TempDir()
	file := filepath.Join(outside, "secret")
	writeFile(t, file, "not part of the volume")
	link := filepath.Join(dir, "link")
	if err := os.Symlink(file, link); err != nil {
		t.Fatal(err)
	}
	first := digest(t, dir)
	if first.Entries != 2 || first.Bytes != 0 {
		t.Fatalf("followed a symlink: %+v", first)
	}
	writeFile(t, file, "changed outside content")
	if digest(t, dir) != first {
		t.Fatal("symlink target content entered the digest")
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(file+"-other", link); err != nil {
		t.Fatal(err)
	}
	if digest(t, dir).SHA256 == first.SHA256 {
		t.Fatal("symlink target change was missed")
	}
}

func TestUnsupportedFilesystemObjectsAreRejected(t *testing.T) {
	for _, name := range []string{"hard link", "extended attribute", "fifo"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "data")
			writeFile(t, path, "data")
			switch name {
			case "hard link":
				if err := os.Link(path, filepath.Join(dir, "alias")); err != nil {
					t.Fatal(err)
				}
			case "extended attribute":
				if err := unix.Lsetxattr(path, "user.k3s-migrate-test", []byte("value"), 0); err != nil {
					if errors.Is(err, unix.ENOTSUP) {
						t.Skip(err)
					}
					t.Fatal(err)
				}
			case "fifo":
				if err := unix.Mkfifo(filepath.Join(dir, "pipe"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			for _, mode := range []string{"probe", "digest"} {
				if _, err := Scan(t.Context(), dir, mode); err == nil {
					t.Fatalf("accepted %s in mode %s", name, mode)
				}
			}
		})
	}
}

func TestOwnershipChangesAffectDigest(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("chown verification requires root")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "data")
	writeFile(t, path, "data")
	first := digest(t, dir)
	if err := os.Chown(path, 12345, 12346); err != nil {
		t.Fatal(err)
	}
	if digest(t, dir).SHA256 == first.SHA256 {
		t.Fatal("ownership change was missed")
	}
}

func TestNonUTF8NamesRemainDistinct(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, string([]byte{'a', 0xfe}))
	b := filepath.Join(dir, string([]byte{'a', 0xff}))
	if err := os.WriteFile(a, []byte("data"), 0o600); err != nil {
		t.Skipf("filesystem does not support raw filenames: %v", err)
	}
	first := digest(t, dir)
	if err := os.Rename(a, b); err != nil {
		t.Skipf("filesystem does not support these filenames: %v", err)
	}
	if digest(t, dir).SHA256 == first.SHA256 {
		t.Fatal("distinct raw filenames were collapsed")
	}
}

func TestValidationAndCancellation(t *testing.T) {
	dir := t.TempDir()
	for _, path := range []string{"relative", dir + "/../", "/"} {
		if _, err := Scan(t.Context(), path, "digest"); err == nil {
			t.Fatalf("accepted %q", path)
		}
	}
	if _, err := Scan(t.Context(), dir, "unknown"); err == nil {
		t.Fatal("accepted unknown mode")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := Scan(ctx, dir, "digest"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	link := filepath.Join(t.TempDir(), "root")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Scan(t.Context(), link, "digest"); err == nil {
		t.Fatal("accepted symlink as volume root")
	}
}

func TestFileReplacementBetweenInspectionAndOpenIsRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data")
	writeFile(t, path, "old")
	var expected unix.Stat_t
	if err := unix.Lstat(path, &expected); err != nil {
		t.Fatal(err)
	}
	newPath := filepath.Join(dir, "new")
	writeFile(t, newPath, "new")
	if err := os.Rename(newPath, path); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if _, err := hashFile(t.Context(), root, "data", &expected, make([]byte, 128)); err == nil {
		t.Fatal("accepted a replaced file")
	}
}
