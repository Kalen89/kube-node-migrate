package filesystem

import (
	"testing"
)

const testBlock uint64 = 4096

func inventory(entries ...FileMetadata) *Summary {
	root := FileMetadata{Path: []byte("."), Type: TypeDirectory, Size: testBlock, AllocatedBytes: testBlock}
	result := &Summary{Version: Version, BlockSize: testBlock, Digest: Digest{Entries: uint64(len(entries) + 1)}, Inventory: []FileMetadata{root}}
	for _, e := range entries {
		result.Inventory = append(result.Inventory, e)
		if e.Type == TypeRegular {
			result.Bytes += e.Size
		}
	}
	return result
}

func regular(name string, size, allocated uint64) FileMetadata {
	return FileMetadata{Path: []byte(name), Type: TypeRegular, Size: size, AllocatedBytes: allocated}
}

func TestRequiredSpaceUsesMatchedAllocationsAndSerialScratch(t *testing.T) {
	const gib uint64 = 1 << 30
	for _, tc := range []struct {
		name                      string
		source, target            []FileMetadata
		growth, scratch           uint64
		growthInode, scratchInode uint64
	}{
		{name: "empty-target-does-not-double-new-file-temp", source: []FileMetadata{regular("a", 10*gib, 10*gib), regular("b", 20*gib, 20*gib)}, growth: 30 * gib, growthInode: 2},
		{name: "precopied-files-only-need-largest-replacement", source: []FileMetadata{regular("a", 10*gib, 10*gib), regular("b", 20*gib, 20*gib)}, target: []FileMetadata{regular("a", 10*gib, 10*gib), regular("b", 20*gib, 20*gib)}, scratch: 20 * gib, scratchInode: 1},
		{name: "single-giant-file-still-needs-whole-temp", source: []FileMetadata{regular("a", 90*gib, 90*gib)}, target: []FileMetadata{regular("a", 90*gib, 90*gib)}, scratch: 90 * gib, scratchInode: 1},
		{name: "file-growth-plus-overlap-equals-temp", source: []FileMetadata{regular("a", 10*gib, 10*gib)}, target: []FileMetadata{regular("a", 8*gib, 8*gib)}, growth: 2 * gib, scratch: 8 * gib, scratchInode: 1},
		{name: "shrinking-file-never-credits-other-files", source: []FileMetadata{regular("a", 8*gib, 8*gib)}, target: []FileMetadata{regular("a", 16*gib, 16*gib)}, scratch: 8 * gib, scratchInode: 1},
		{name: "extraneous-target-is-not-credit", source: []FileMetadata{regular("a", 2*gib, 2*gib)}, target: []FileMetadata{regular("unrelated", 64*gib, 64*gib)}, growth: 2 * gib, growthInode: 1},
		{name: "sparse-target-does-not-credit-logical-size", source: []FileMetadata{regular("a", 16*gib, 16*gib)}, target: []FileMetadata{regular("a", 16*gib, testBlock)}, growth: 16*gib - testBlock, scratch: testBlock, scratchInode: 1},
		{name: "sparse-source-expands-without-rsync-sparse", source: []FileMetadata{regular("a", 16*gib, testBlock)}, growth: 16 * gib, growthInode: 1},
		{name: "small-file-rounds-to-target-block", source: []FileMetadata{regular("a", 1, testBlock)}, growth: testBlock, growthInode: 1},
		{name: "directory-and-child-need-inodes", source: []FileMetadata{{Path: []byte("dir"), Type: TypeDirectory, AllocatedBytes: testBlock}, regular("dir/a", 1, testBlock)}, growth: 2 * testBlock, growthInode: 2},
		{name: "type-change-does-not-reuse-unrelated-allocation", source: []FileMetadata{regular("a", testBlock, testBlock)}, target: []FileMetadata{{Path: []byte("a"), Type: TypeDirectory, AllocatedBytes: 8 * testBlock}}, growth: testBlock, growthInode: 1},
		{name: "binary-paths-remain-matchable", source: []FileMetadata{regular(string([]byte{'a', 0xff}), 1, testBlock)}, target: []FileMetadata{regular(string([]byte{'a', 0xff}), 1, testBlock)}, scratch: testBlock, scratchInode: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := RequiredSpace(inventory(tc.source...), inventory(tc.target...))
			if err != nil {
				t.Fatal(err)
			}
			want := SpaceRequirements{GrowthBytes: tc.growth, ScratchBytes: tc.scratch, GrowthInodes: tc.growthInode, ScratchInodes: tc.scratchInode}
			if got != want {
				t.Fatalf("space requirements = %+v; want %+v", got, want)
			}
		})
	}
}

func TestInventoryRejectsMalformedOrOverflowingInput(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Summary)
	}{
		{"missing-inventory", func(s *Summary) { s.Inventory = nil }},
		{"duplicate", func(s *Summary) { s.Inventory[1].Path = []byte(".") }},
		{"traversal", func(s *Summary) { s.Inventory[1].Path = []byte("../outside") }},
		{"absolute", func(s *Summary) { s.Inventory[1].Path = []byte("/outside") }},
		{"nul", func(s *Summary) { s.Inventory[1].Path = []byte("a\x00b") }},
		{"missing-parent", func(s *Summary) { s.Inventory[1].Path = []byte("missing/a") }},
		{"unsupported-type", func(s *Summary) { s.Inventory[1].Type = 0o010000 }},
		{"missing-root", func(s *Summary) { s.Inventory[0].Path = []byte("root") }},
		{"invalid-allocation", func(s *Summary) { s.Inventory[1].AllocatedBytes = 1 }},
		{"invalid-block-size", func(s *Summary) { s.BlockSize = 1000 }},
		{"wrong-byte-total", func(s *Summary) { s.Bytes++ }},
		{"logical-size-overflow", func(s *Summary) { s.Inventory = append(s.Inventory, regular("b", ^uint64(0), 0)); s.Entries++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := inventory(regular("a", 1, testBlock))
			tc.change(s)
			if err := ValidateInventory(s); err == nil {
				t.Fatal("accepted malformed inventory")
			}
		})
	}
	source := inventory(regular("a", ^uint64(0), 0))
	if _, err := RequiredSpace(source, inventory()); err == nil {
		t.Fatal("accepted overflowing allocation round-up")
	}
}

func TestSharedDeviceRequirementsSumGrowthAndMaxScratch(t *testing.T) {
	needs := SpaceRequirements{GrowthBytes: 3 * testBlock, ScratchBytes: 7 * testBlock, GrowthInodes: 3, ScratchInodes: 1}
	if err := needs.Add(SpaceRequirements{GrowthBytes: 5 * testBlock, ScratchBytes: 11 * testBlock, GrowthInodes: 5, ScratchInodes: 1}); err != nil {
		t.Fatal(err)
	}
	bytes, inodes, err := needs.Total()
	if err != nil || bytes != 19*testBlock || inodes != 9 {
		t.Fatalf("combined capacity = %d bytes / %d inodes / %v", bytes, inodes, err)
	}
	before := needs
	if err := needs.Add(SpaceRequirements{GrowthBytes: ^uint64(0)}); err == nil || needs != before {
		t.Fatal("overflow must fail without partially updating capacity")
	}
	if _, _, err := (SpaceRequirements{GrowthInodes: ^uint64(0), ScratchInodes: 1}).Total(); err == nil {
		t.Fatal("accepted overflowing inode count")
	}
}
