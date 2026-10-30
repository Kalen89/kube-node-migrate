package nodemigrate

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"time"
)

// NewCheckpointWriter coalesces observation-only updates for one locked task.
// Transaction changes remain synchronous and durable before a report is published.
// The caller serializes state mutations and calls; no background writes occur.
func NewCheckpointWriter(path string) func(*State) error {
	var previous [sha256.Size]byte
	var persisted bool
	return func(state *State) error {
		if state == nil {
			return errors.New("迁移状态未初始化")
		}
		transaction := *state
		transaction.ActiveStage = nil
		transaction.Events = nil
		transaction.UpdatedAt = time.Time{}
		data, err := json.Marshal(&transaction)
		if err != nil {
			return err
		}
		fingerprint := sha256.Sum256(data)
		if !persisted || fingerprint != previous {
			if err := SaveState(path, state); err != nil {
				// The private write may have succeeded before report publication failed.
				// Re-persist next time rather than trusting any earlier cached version.
				persisted = false
				return err
			}
			previous, persisted = fingerprint, true
			return nil
		}
		state.UpdatedAt = time.Now().UTC()
		return SaveReport(path, state)
	}
}
