// Command check-bundle builds and verifies offline bundles without platform-specific
// tar or checksum commands. It must run from the repository root.
package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"debug/elf"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/Kalen89/kube-node-migrate/internal/nodemigrate"
)

type bundleFile struct {
	source string
	size   int64
	mode   int64
	digest string
}

func main() {
	dependencies := flag.String("pv-migrate-dir", "bin", "Directory containing both original pv-migrate archives")
	version := flag.String("version", "0.1.0", "Expected application version")
	commit := flag.String("commit", "unknown", "Expected source commit")
	built := flag.String("built", "unknown", "Expected UTC build time")
	flag.Parse()
	if err := run(*dependencies, *version, *commit, *built); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func archiveName(arch string) string {
	platform := arch
	if arch == "amd64" {
		platform = "x86_64"
	}
	return "pv-migrate_v3.7.0_linux_" + platform + ".tar.gz"
}

func run(dependencies, version, commit, built string) error {
	if version == "" || strings.ContainsAny(version, "/\\ \t\r\n") {
		return errors.New("invalid release version")
	}
	// Validate every dependency before producing either architecture's bundle.
	for _, arch := range []string{"amd64", "arm64"} {
		if err := nodemigrate.ValidateCopyArchive(filepath.Join(dependencies, archiveName(arch)), arch); err != nil {
			return fmt.Errorf("%s offline dependency: %w", arch, err)
		}
		if err := validateBinary(filepath.Join("bin", "kube-node-fs-linux-"+arch), arch, ""); err != nil {
			return err
		}
		metadata := fmt.Sprintf("%s (commit=%s built=%s)", version, commit, built)
		if err := validateBinary(filepath.Join("bin", "kube-node-migrate-linux-"+arch), arch, metadata); err != nil {
			return err
		}
	}
	for _, arch := range []string{"amd64", "arm64"} {
		files, err := collectFiles(dependencies, arch)
		if err != nil {
			return err
		}
		path := filepath.Join("bin", "kube-node-migrate-"+version+"-linux-"+arch+".tar.gz")
		if err := publishBundle(path, files); err != nil {
			return err
		}
		fmt.Printf("Created and verified %s (%d files), checksum %s.sha256\n", path, len(files), path)
	}
	return nil
}

func publishBundle(path string, files map[string]bundleFile) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".bundle-*.tar.gz")
	if err != nil {
		return err
	}
	defer os.Remove(temporary.Name())
	if err := errors.Join(temporary.Chmod(0o644), temporary.Close()); err != nil {
		return err
	}
	if err := createBundle(temporary.Name(), files); err != nil {
		return err
	}
	if err := verifyBundle(temporary.Name(), files); err != nil {
		return fmt.Errorf("verify %s: %w", path, err)
	}
	digest, err := fileDigest(temporary.Name())
	if err != nil {
		return err
	}
	if err := os.Rename(temporary.Name(), path); err != nil {
		return err
	}
	return os.WriteFile(path+".sha256", []byte(digest+"  "+filepath.Base(path)+"\n"), 0o644)
}

func validateBinary(path, arch, expected string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return fmt.Errorf("%s: expected an executable regular file", path)
	}
	binary, err := elf.Open(path)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	defer binary.Close()
	machine := map[string]elf.Machine{"amd64": elf.EM_X86_64, "arm64": elf.EM_AARCH64}[arch]
	if binary.Class != elf.ELFCLASS64 || binary.Machine != machine {
		return fmt.Errorf("%s: wrong Linux executable architecture", path)
	}
	if expected == "" {
		return nil
	}
	// -trimpath omits linker flags from Go buildinfo. The single version string
	// used by --version is injected into read-only data even in stripped binaries.
	section := binary.Section(".rodata")
	if section == nil || section.Size > 128<<20 {
		return fmt.Errorf("%s: missing or oversized read-only data", path)
	}
	data, err := section.Data()
	if err != nil {
		return err
	}
	if !bytes.Contains(data, []byte(expected)) {
		return fmt.Errorf("%s: missing expected build metadata %q", path, expected)
	}
	return nil
}

func collectFiles(dependencies, arch string) (map[string]bundleFile, error) {
	paths := map[string]string{
		"kube-node-migrate-linux-" + arch: filepath.Join("bin", "kube-node-migrate-linux-"+arch),
		"kube-node-fs-linux-amd64":        "bin/kube-node-fs-linux-amd64",
		"kube-node-fs-linux-arm64":        "bin/kube-node-fs-linux-arm64",
		archiveName(arch):                 filepath.Join(dependencies, archiveName(arch)),
		"README.md":                       "README.md",
		"LICENSE":                         "LICENSE",
		"CHANGELOG.md":                    "CHANGELOG.md",
		"README.zh-CN.md":                 "README.zh-CN.md",
		"CHANGELOG.zh-CN.md":              "CHANGELOG.zh-CN.md",
	}
	for _, directory := range []string{"examples", "docs"} {
		if err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
			if err == nil && !entry.IsDir() {
				paths[filepath.ToSlash(path)] = path
			}
			return err
		}); err != nil {
			return nil, err
		}
	}
	files := make(map[string]bundleFile, len(paths))
	for name, path := range paths {
		info, err := os.Lstat(path)
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("%s: bundle input must be a regular file", path)
		}
		digest, err := fileDigest(path)
		if err != nil {
			return nil, err
		}
		files[name] = bundleFile{source: path, size: info.Size(), mode: int64(info.Mode().Perm()), digest: digest}
	}
	return files, nil
}

func fileDigest(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	_, err = io.Copy(hash, file)
	return hex.EncodeToString(hash.Sum(nil)), err
}

func createBundle(path string, files map[string]bundleFile) (result error) {
	output, err := os.Create(path)
	if err != nil {
		return err
	}
	compressed := gzip.NewWriter(output)
	archive := tar.NewWriter(compressed)
	defer func() { result = errors.Join(result, archive.Close(), compressed.Close(), output.Close()) }()
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		input := files[name]
		if err := archive.WriteHeader(&tar.Header{Name: name, Mode: input.mode, Size: input.size, Typeflag: tar.TypeReg}); err != nil {
			return err
		}
		file, err := os.Open(input.source)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(archive, file)
		if err := errors.Join(copyErr, file.Close()); err != nil {
			return err
		}
	}
	return nil
}

func verifyBundle(path string, expected map[string]bundleFile) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	compressed, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer compressed.Close()
	archive := tar.NewReader(compressed)
	seen := make(map[string]bool, len(expected))
	for {
		header, err := archive.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		input, exists := expected[header.Name]
		if !exists || seen[header.Name] || header.Typeflag != tar.TypeReg || header.Size != input.size || header.Mode != input.mode {
			return fmt.Errorf("unexpected, duplicate or mismatched bundle entry %q", header.Name)
		}
		hash := sha256.New()
		if _, err := io.Copy(hash, archive); err != nil {
			return err
		}
		if hex.EncodeToString(hash.Sum(nil)) != input.digest {
			return fmt.Errorf("bundle entry content changed: %s", header.Name)
		}
		seen[header.Name] = true
	}
	if len(seen) != len(expected) {
		return errors.New("bundle manifest is incomplete")
	}
	// Consume the gzip trailer too: tar EOF alone does not validate its checksum.
	_, err = io.Copy(io.Discard, compressed)
	return err
}
