package filesystem

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

const (
	TypeDirectory uint32 = 0o040000
	TypeRegular   uint32 = 0o100000
	TypeSymlink   uint32 = 0o120000
)

// FileMetadata records raw relative paths without reading file contents. Blocks
// are reported in bytes, separately from logical size, so sparse files cannot
// provide fictitious free-space credit.
type FileMetadata struct {
	Path           []byte `json:"path"`
	Type           uint32 `json:"type"`
	Size           uint64 `json:"size"`
	AllocatedBytes uint64 `json:"allocatedBytes"`
}

// SpaceRequirements keeps persistent growth separate from one serial rsync
// replacement. Shared-device callers sum growth and take the largest scratch.
type SpaceRequirements struct {
	GrowthBytes   uint64
	ScratchBytes  uint64
	GrowthInodes  uint64
	ScratchInodes uint64
}

// Add combines volumes copied serially on one filesystem.
func (s *SpaceRequirements) Add(next SpaceRequirements) error {
	combined := *s
	var err error
	combined.GrowthBytes, err = checkedAdd(combined.GrowthBytes, next.GrowthBytes)
	if err != nil {
		return err
	}
	combined.GrowthInodes, err = checkedAdd(combined.GrowthInodes, next.GrowthInodes)
	if err != nil {
		return err
	}
	combined.ScratchBytes = max(combined.ScratchBytes, next.ScratchBytes)
	combined.ScratchInodes = max(combined.ScratchInodes, next.ScratchInodes)
	if _, _, err := combined.Total(); err != nil {
		return err
	}
	*s = combined
	return nil
}

func (s SpaceRequirements) Total() (uint64, uint64, error) {
	bytes, err := checkedAdd(s.GrowthBytes, s.ScratchBytes)
	if err != nil {
		return 0, 0, err
	}
	inodes, err := checkedAdd(s.GrowthInodes, s.ScratchInodes)
	return bytes, inodes, err
}

func checkedAdd(a, b uint64) (uint64, error) {
	if ^uint64(0)-a < b {
		return 0, errors.New("filesystem capacity arithmetic overflow")
	}
	return a + b, nil
}

func roundedAllocation(size, blockSize uint64) (uint64, error) {
	if size == 0 {
		return 0, nil
	}
	if remainder := size % blockSize; remainder != 0 {
		return checkedAdd(size, blockSize-remainder)
	}
	return size, nil
}

// ValidateInventory fails closed if an older helper ignores the new mode, or
// malformed paths/duplicates could make unrelated allocations appear reusable.
func ValidateInventory(summary *Summary) error {
	if summary.Version != Version || summary.BlockSize < 512 || summary.BlockSize%512 != 0 || summary.Entries != uint64(len(summary.Inventory)) || summary.Entries == 0 {
		return errors.New("invalid filesystem inventory header or allocation block size")
	}
	entries := make(map[string]FileMetadata, len(summary.Inventory))
	var bytes uint64
	for _, entry := range summary.Inventory {
		name := string(entry.Path)
		unsafePath := name == "" || name == ".." || filepath.IsAbs(name) || filepath.Clean(name) != name || strings.HasPrefix(name, "../") || strings.ContainsRune(name, 0)
		if unsafePath || entry.AllocatedBytes%512 != 0 {
			return fmt.Errorf("invalid inventory path or allocation: %q", name)
		}
		if _, duplicate := entries[name]; duplicate {
			return fmt.Errorf("duplicate inventory path: %q", name)
		}
		switch entry.Type {
		case TypeRegular:
			var err error
			bytes, err = checkedAdd(bytes, entry.Size)
			if err != nil {
				return err
			}
		case TypeDirectory, TypeSymlink:
		default:
			return fmt.Errorf("unsupported inventory type: %q", name)
		}
		entries[name] = entry
	}
	if root, ok := entries["."]; !ok || root.Type != TypeDirectory || bytes != summary.Bytes {
		return errors.New("inventory root or logical size does not match summary")
	}
	for name := range entries {
		if name != "." && entries[filepath.Dir(name)].Type != TypeDirectory {
			return fmt.Errorf("inventory parent is not a directory: %q", name)
		}
	}
	return nil
}

// RequiredSpace bounds rsync's default temporary-file-and-rename behavior. No
// credit is taken for extraneous destination files or for future deletions.
// Ordinary files use logical size because rsync is not configured with --sparse.
func RequiredSpace(source, target *Summary) (SpaceRequirements, error) {
	var result SpaceRequirements
	for _, summary := range []*Summary{source, target} {
		if summary == nil {
			return result, errors.New("missing filesystem inventory")
		}
		if err := ValidateInventory(summary); err != nil {
			return result, err
		}
	}
	existing := make(map[string]FileMetadata, len(target.Inventory))
	for _, entry := range target.Inventory {
		existing[string(entry.Path)] = entry
	}
	for _, entry := range source.Inventory {
		allocation := entry.AllocatedBytes
		switch entry.Type {
		case TypeRegular:
			allocation = entry.Size
		case TypeDirectory:
			allocation = max(allocation, entry.Size, target.BlockSize)
		case TypeSymlink:
			allocation = max(allocation, entry.Size)
		}
		allocation, err := roundedAllocation(allocation, target.BlockSize)
		if err != nil {
			return result, err
		}
		old, matched := existing[string(entry.Path)]
		matched = matched && old.Type == entry.Type
		var reusable uint64
		if matched {
			reusable = min(allocation, old.AllocatedBytes)
		}
		result.GrowthBytes, err = checkedAdd(result.GrowthBytes, allocation-reusable)
		if err != nil {
			return result, err
		}
		if !matched {
			result.GrowthInodes, err = checkedAdd(result.GrowthInodes, 1)
			if err != nil {
				return result, err
			}
		}
		if entry.Type != TypeDirectory && matched {
			// Growth already accounts for the excess of the new file over the
			// old allocation. Only its overlapping part is additional scratch.
			result.ScratchBytes = max(result.ScratchBytes, reusable)
			result.ScratchInodes = 1
		}
	}
	if _, _, err := result.Total(); err != nil {
		return result, err
	}
	return result, nil
}
