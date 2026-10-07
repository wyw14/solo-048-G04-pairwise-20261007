package switchover

import (
	"path/filepath"

	"heatpulse/internal/store"
)

// State is the persisted set of switchover operations, keyed by operation ID.
type State struct {
	Operations map[string]Operation `json:"operations"`
}

type operationStore struct {
	file *store.File[State]
}

func newOperationStore(directory string) *operationStore {
	return &operationStore{file: store.NewFile[State](filepath.Join(directory, "switchovers.json"))}
}

func (s *operationStore) load() (map[string]Operation, error) {
	state, err := s.file.Load()
	if err != nil {
		return nil, err
	}
	if state.Operations == nil {
		state.Operations = make(map[string]Operation)
	}
	return state.Operations, nil
}

func (s *operationStore) save(operations map[string]Operation) error {
	return s.file.Save(State{Operations: operations})
}
