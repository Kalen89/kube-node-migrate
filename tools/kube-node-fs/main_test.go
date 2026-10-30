package main

import (
	"bytes"
	"flag"
	"strings"
	"testing"
)

func TestHelpListsInventoryMode(t *testing.T) {
	var output, stderr bytes.Buffer
	if err := run(t.Context(), []string{"-h"}, &output, &stderr); err != flag.ErrHelp {
		t.Fatalf("help returned %v", err)
	}
	if !strings.Contains(stderr.String(), "inventory") {
		t.Fatalf("help omitted metadata inventory mode: %s", stderr.String())
	}
}
