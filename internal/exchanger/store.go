package exchanger

import (
	"fmt"
	"path/filepath"

	"heatpulse/internal/store"
)

// SwitchoverStore persists switchover orchestrations through an atomic JSON
// document so an in-flight operation stays queryable after a restart.
type SwitchoverStore struct {
	file *store.File[map[string]Switchover]
}

func NewSwitchoverStore(directory string) *SwitchoverStore {
	return &SwitchoverStore{file: store.NewFile[map[string]Switchover](filepath.Join(directory, "switchovers.json"))}
}

// Load returns all persisted switchovers. A missing or empty state file yields
// an empty map without an error.
func (s *SwitchoverStore) Load() (map[string]Switchover, error) {
	all, err := s.file.Load()
	if err != nil {
		return nil, fmt.Errorf("load switchovers: %w", err)
	}
	if all == nil {
		all = make(map[string]Switchover)
	}
	return all, nil
}

// Put upserts a single switchover, synchronising the document to disk.
func (s *SwitchoverStore) Put(item Switchover) error {
	if item.ID == "" {
		return fmt.Errorf("switchover id is required")
	}
	_, err := s.file.Update(func(all *map[string]Switchover) error {
		if *all == nil {
			*all = make(map[string]Switchover)
		}
		copy := item.Clone()
		(*all)[item.ID] = copy
		return nil
	})
	if err != nil {
		return fmt.Errorf("persist switchover: %w", err)
	}
	return nil
}

// Delete removes a terminal switchover from the document.
func (s *SwitchoverStore) Delete(id string) error {
	_, err := s.file.Update(func(all *map[string]Switchover) error {
		delete(*all, id)
		return nil
	})
	if err != nil {
		return fmt.Errorf("delete switchover: %w", err)
	}
	return nil
}
