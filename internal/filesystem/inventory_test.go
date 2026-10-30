//go:build linux

package filesystem

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInventoryRecordsSparseAllocationAndDirectoryInodes(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(filepath.Join(dir, "nested", "sparse"))
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(1 << 30); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("nested/sparse", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	summary, err := Scan(t.Context(), dir, "inventory")
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateInventory(summary); err != nil {
		t.Fatal(err)
	}
	if summary.Entries != 4 || len(summary.Inventory) != 4 || summary.Bytes != 1<<30 || summary.SHA256 != "" {
		t.Fatalf("invalid inventory: %+v", summary)
	}
	for _, entry := range summary.Inventory {
		if string(entry.Path) == "nested/sparse" && (entry.Type != TypeRegular || entry.Size != 1<<30 || entry.AllocatedBytes >= entry.Size) {
			t.Fatalf("sparse file allocation was lost: %+v", entry)
		}
	}
	needs, err := RequiredSpace(summary, inventory())
	if err != nil || needs.GrowthBytes < 1<<30 || needs.GrowthInodes != 3 {
		t.Fatalf("new rsync copy must expand sparse source and include directories: %+v / %v", needs, err)
	}
}
