//go:build linux || darwin

// Package filesystem implements read-only volume inspection for the node helper.
package filesystem

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// Version distinguishes the canonical Go digest from the previous Python format.
const Version = 2

type Digest struct {
	SHA256  string `json:"sha256"`
	Entries uint64 `json:"entries"`
	Bytes   uint64 `json:"bytes"`
}

type Summary struct {
	Digest
	Version    int            `json:"version"`
	FreeBytes  uint64         `json:"freeBytes"`
	FreeInodes uint64         `json:"freeInodes"`
	Device     uint64         `json:"device"`
	BlockSize  uint64         `json:"blockSize"`
	Inventory  []FileMetadata `json:"inventory,omitempty"`
}

// Byte slices encode paths/links as base64, preserving even non-UTF-8 filenames.
type entry struct {
	Path   []byte `json:"path"`
	Type   uint32 `json:"type"`
	Mode   uint32 `json:"mode"`
	UID    uint32 `json:"uid"`
	GID    uint32 `json:"gid"`
	Size   int64  `json:"size,omitempty"`
	SHA256 string `json:"sha256,omitempty"`
	Link   []byte `json:"link,omitempty"`
}

// Scan never writes or follows descendant symlinks. Content reads honor ctx.
func Scan(ctx context.Context, path, mode string) (*Summary, error) {
	validMode := mode == "probe" || mode == "digest" || mode == "free" || mode == "inventory"
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || !validMode {
		return nil, errors.New("expected a normalized absolute path and probe/digest/free/inventory mode")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var st unix.Stat_t
	if err := unix.Lstat(path, &st); err != nil {
		return nil, err
	}
	if uint32(st.Mode)&unix.S_IFMT != unix.S_IFDIR || (path == "/" && mode != "free") {
		return nil, errors.New("expected an existing non-root volume directory (not a symlink)")
	}
	var disk unix.Statfs_t
	if err := unix.Statfs(path, &disk); err != nil {
		return nil, err
	}
	if disk.Bsize <= 0 || uint64(disk.Bavail) > ^uint64(0)/uint64(disk.Bsize) {
		return nil, errors.New("invalid or overflowing filesystem capacity")
	}
	result := &Summary{Version: Version, Device: uint64(st.Dev), FreeBytes: uint64(disk.Bavail) * uint64(disk.Bsize), FreeInodes: uint64(disk.Ffree), BlockSize: uint64(disk.Bsize)}
	if mode == "free" {
		return result, nil
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	chain := sha256.New()
	encoder := json.NewEncoder(chain)
	encoder.SetEscapeHTML(false)
	var buffer []byte
	if mode == "digest" {
		buffer = make([]byte, 1024*1024)
	}
	err = fs.WalkDir(root.FS(), ".", func(name string, _ fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		absolute := filepath.Join(path, name)
		var metadata unix.Stat_t
		if err := unix.Lstat(absolute, &metadata); err != nil {
			return err
		}
		attrs, err := unix.Llistxattr(absolute, nil)
		if err != nil {
			return fmt.Errorf("list attributes %q: %w", name, err)
		}
		if attrs != 0 {
			return fmt.Errorf("%q: extended attributes require a separate copy policy", name)
		}
		e := entry{Path: []byte(name), Type: uint32(metadata.Mode) & unix.S_IFMT, Mode: uint32(metadata.Mode) & 0o7777, UID: metadata.Uid, GID: metadata.Gid}
		switch e.Type {
		case unix.S_IFREG:
			if metadata.Nlink != 1 {
				return fmt.Errorf("%q: hard links require a separate copy policy", name)
			}
			if metadata.Size < 0 || ^uint64(0)-result.Bytes < uint64(metadata.Size) {
				return errors.New("invalid or overflowing volume size")
			}
			e.Size = metadata.Size
			result.Bytes += uint64(metadata.Size)
			if mode == "digest" {
				e.SHA256, err = hashFile(ctx, root, name, &metadata, buffer)
				if err != nil {
					return err
				}
			}
		case unix.S_IFLNK:
			link, err := root.Readlink(name)
			if err != nil {
				return err
			}
			e.Link = []byte(link)
		case unix.S_IFDIR:
		default:
			return fmt.Errorf("%q: special files require a separate copy policy", name)
		}
		if result.Entries == ^uint64(0) {
			return errors.New("overflowing filesystem entry count")
		}
		result.Entries++
		if mode == "inventory" {
			if metadata.Size < 0 || metadata.Blocks < 0 || uint64(metadata.Blocks) > ^uint64(0)/512 {
				return errors.New("invalid or overflowing file allocation")
			}
			result.Inventory = append(result.Inventory, FileMetadata{
				Path: []byte(name), Type: e.Type, Size: uint64(metadata.Size), AllocatedBytes: uint64(metadata.Blocks) * 512,
			})
		}
		if mode == "digest" {
			return encoder.Encode(e)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if mode == "digest" {
		result.SHA256 = hex.EncodeToString(chain.Sum(nil))
	}
	return result, nil
}

func hashFile(ctx context.Context, root *os.Root, name string, expected *unix.Stat_t, buffer []byte) (string, error) {
	file, err := root.OpenFile(name, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return "", err
	}
	defer file.Close()
	var before, after unix.Stat_t
	if err = unix.Fstat(int(file.Fd()), &before); err != nil {
		return "", err
	}
	if !sameFile(expected, &before) || uint32(before.Mode)&unix.S_IFMT != unix.S_IFREG {
		return "", fmt.Errorf("file %q changed before reading", name)
	}
	info, err := file.Stat()
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	var count int64
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		n, readErr := file.Read(buffer)
		if n > 0 {
			_, _ = hash.Write(buffer[:n])
			count += int64(n)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return "", readErr
		}
	}
	if err = unix.Fstat(int(file.Fd()), &after); err != nil {
		return "", err
	}
	final, err := file.Stat()
	if err != nil {
		return "", err
	}
	if !sameFile(&before, &after) || count != before.Size || !info.ModTime().Equal(final.ModTime()) {
		return "", fmt.Errorf("file %q changed while reading", name)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func sameFile(a, b *unix.Stat_t) bool {
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Mode == b.Mode && a.Uid == b.Uid && a.Gid == b.Gid && a.Size == b.Size && a.Nlink == b.Nlink
}
