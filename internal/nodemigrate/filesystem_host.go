package nodemigrate

import (
	"context"
	"crypto/sha256"
	"debug/elf"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const helperCache = "/var/lib/kube-node-migrate"

type filesystemHost interface {
	Filesystem(context.Context, Host, string, string, string) (string, error)
}

func defaultFilesystemHelper(arch string) (string, error) {
	if !oneOf(arch, "amd64", "arm64") {
		return "", fmt.Errorf("不支持文件系统助手架构 %q", arch)
	}
	program, err := os.Executable()
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(program), "kube-node-fs-linux-"+arch), nil
}

func openFilesystemHelper(path, arch string) (*os.File, string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, "", fmt.Errorf("缺少 Go 文件系统助手，请随主程序放置对应架构助手或指定 --filesystem-helper: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 8<<20 {
		return nil, "", errors.New("文件系统助手须为不超过 8 MiB 的普通文件")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, "", err
	}
	ok := false
	defer func() {
		if !ok {
			closeReadOnly(file)
		}
	}()
	header, err := elf.NewFile(file)
	if err != nil {
		return nil, "", fmt.Errorf("文件系统助手须为 Linux ELF: %w", err)
	}
	wanted := map[string]elf.Machine{"amd64": elf.EM_X86_64, "arm64": elf.EM_AARCH64}[arch]
	if wanted == elf.EM_NONE || header.Machine != wanted || header.Class != elf.ELFCLASS64 {
		return nil, "", errors.New("文件系统助手与节点架构不匹配")
	}
	hash := sha256.New()
	if _, err = io.Copy(hash, file); err != nil {
		return nil, "", err
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return nil, "", err
	}
	ok = true
	return file, hex.EncodeToString(hash.Sum(nil)), nil
}

func helperCacheCheck(digest string) string {
	return "set -eu; umask 077; base=" + shellQuote(helperCache) + "; " +
		`if ! test -e "$base" && ! test -L "$base"; then mkdir -m 700 "$base" 2>/dev/null || test -d "$base"; fi; ` +
		`test ! -L "$base"; test -d "$base"; test "$(stat -c '%u:%a' "$base")" = "$(id -u):700"; ` +
		"binary=" + shellQuote(helperCache+"/fs-"+digest) + "; " +
		`test ! -L "$binary"; if test -e "$binary"; then test -f "$binary"; ` +
		`test "$(stat -c '%u:%a' "$binary")" = "$(id -u):500"; actual=$(sha256sum "$binary"); ` +
		"test \"${actual%% *}\" = " + shellQuote(digest) + `; printf ready; else printf missing; fi`
}

func helperUpload(digest string) string {
	return "set -eu; umask 077; base=" + shellQuote(helperCache) + "; binary=" + shellQuote(helperCache+"/fs-"+digest) + "; " +
		`test ! -L "$base"; test "$(stat -c '%u:%a' "$base")" = "$(id -u):700"; ` +
		`tmp=$(mktemp "$base/.upload.XXXXXX"); trap 'rm -f -- "$tmp"' 0; cat > "$tmp"; ` +
		`actual=$(sha256sum "$tmp"); ` + "test \"${actual%% *}\" = " + shellQuote(digest) + "; " +
		`chmod 500 "$tmp"; ln "$tmp" "$binary" 2>/dev/null || test -f "$binary"; ` +
		`test ! -L "$binary"; test "$(stat -c '%u:%a' "$binary")" = "$(id -u):500"; ` +
		`actual=$(sha256sum "$binary"); ` + "test \"${actual%% *}\" = " + shellQuote(digest)
}

// Filesystem stages a checksummed Go helper once, then performs a read-only scan.
// The root-owned, private, content-addressed cache never contains business data.
func (h SSHHosts) Filesystem(ctx context.Context, node Host, helper, path, mode string) (string, error) {
	file, digest, err := openFilesystemHelper(helper, node.Architecture)
	if err != nil {
		return "", err
	}
	defer closeReadOnly(file)
	status, err := h.Run(ctx, node, helperCacheCheck(digest))
	if err != nil {
		return "", err
	}
	switch strings.TrimSpace(status) {
	case "missing":
		if _, err = h.runInput(ctx, node, helperUpload(digest), file); err != nil {
			return "", fmt.Errorf("部署 Go 文件系统助手: %w", err)
		}
	case "ready":
	default:
		return "", errors.New("文件系统助手缓存检查返回异常")
	}
	limit := 12 * time.Hour
	if deadline, ok := ctx.Deadline(); ok {
		limit = time.Until(deadline)
	}
	if limit <= 0 {
		return "", context.DeadlineExceeded
	}
	command := "exec " + shellQuote(helperCache+"/fs-"+digest) + " --path " + shellQuote(path) + " --mode " + shellQuote(mode) + " --timeout " + shellQuote(limit.String())
	return h.Run(ctx, node, command)
}
