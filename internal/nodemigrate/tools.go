package nodemigrate

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
)

const maxToolSize = 128 << 20

var archiveDigests = map[string]string{
	"amd64": "5e33342777786f07161d8629772cc46baf8a696de641d2d1537f4b9adb8bcdf1",
	"arm64": "996ea63b4f91b4750e10aab6604803c6beadab9d2ca97a283cd7f39e299f1d3d",
}

// PrepareCopyTool uses an explicit executable or a checksummed offline archive.
func PrepareCopyTool(ctx context.Context, options *Options) error {
	if options.CopyBinary != "" {
		return CheckCopyTool(ctx, *options)
	}
	if runtime.GOOS != linuxOS {
		return errors.New("卷复制执行端必须是 Linux")
	}
	expected, ok := archiveDigests[runtime.GOARCH]
	if !ok {
		return errors.New("不支持该执行端架构")
	}
	if options.Archive == "" {
		program, err := os.Executable()
		if err != nil {
			return err
		}
		options.CopyBinary, options.Archive = discoverCopyTool(filepath.Dir(program), runtime.GOARCH)
		if options.CopyBinary != "" {
			return CheckCopyTool(ctx, *options)
		}
		if options.Archive == "" {
			return errors.New("未找到离线复制工具；请使用完整发布包，或指定 --pv-migrate / --pv-migrate-archive")
		}
	}
	path, err := extractTool(options.Archive, filepath.Dir(options.StatePath), expected)
	if err != nil {
		return err
	}
	options.CopyBinary = path
	return CheckCopyTool(ctx, *options)
}

func discoverCopyTool(directory, arch string) (binary, archive string) {
	name := "pv-migrate_v3.7.0_linux_arm64.tar.gz"
	if arch == "amd64" {
		name = "pv-migrate_v3.7.0_linux_x86_64.tar.gz"
	}
	for _, entry := range []string{copyToolName, name} {
		path := filepath.Join(directory, entry)
		if info, err := os.Lstat(path); err == nil && info.Mode().IsRegular() {
			if entry == copyToolName {
				return path, ""
			}
			return "", path
		}
	}
	return "", ""
}
func extractTool(archive, directory, expected string) (string, error) {
	var destination string
	err := readCopyArchive(archive, expected, func(reader io.Reader, size int64) error {
		var err error
		destination, err = writeTool(directory, reader, size)
		return err
	})
	return destination, err
}

// ValidateCopyArchive checks the original release digest and executable entry
// without extracting files. Release packaging uses the same contract as runtime.
func ValidateCopyArchive(path, arch string) error {
	expected, ok := archiveDigests[arch]
	if !ok {
		return fmt.Errorf("不支持复制工具架构 %q", arch)
	}
	return readCopyArchive(path, expected, func(reader io.Reader, size int64) error {
		_, err := io.CopyN(io.Discard, reader, size)
		return err
	})
}

func readCopyArchive(archive, expected string, consume func(io.Reader, int64) error) error {
	f, err := os.Open(archive)
	if err != nil {
		return err
	}
	defer closeReadOnly(f)
	hash := sha256.New()
	if _, err = io.Copy(hash, f); err != nil {
		return err
	}
	if hex.EncodeToString(hash.Sum(nil)) != expected {
		return errors.New("pv-migrate 离线归档 SHA-256 不匹配")
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer closeReadOnly(gz)
	reader := tar.NewReader(gz)
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return errors.New("归档缺少 pv-migrate 普通文件")
		}
		if err != nil {
			return err
		}
		if header.Name != copyToolName {
			continue
		}
		if header.Typeflag != tar.TypeReg || header.Size <= 0 || header.Size > maxToolSize {
			return errors.New("归档中的复制工具类型或大小无效")
		}
		return consume(reader, header.Size)
	}
}

func writeTool(directory string, reader io.Reader, size int64) (string, error) {
	f, err := os.CreateTemp(directory, ".pv-migrate-")
	if err != nil {
		return "", err
	}
	defer removeTemporary(f.Name())
	_, copyErr := io.CopyN(f, reader, size)
	err = errors.Join(copyErr, f.Sync(), f.Chmod(privateDirMode), f.Close())
	if err != nil {
		return "", err
	}
	destination := filepath.Join(directory, "pv-migrate-"+copierVersion)
	if err = os.Rename(f.Name(), destination); err != nil {
		return "", fmt.Errorf("保存离线复制工具: %w", err)
	}
	return destination, nil
}
