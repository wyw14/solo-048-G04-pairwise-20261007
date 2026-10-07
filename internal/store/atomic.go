package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// File serializes a JSON document through an atomic rename.
type File[T any] struct {
	path string
	mu   sync.Mutex
}

func NewFile[T any](path string) *File[T] {
	return &File[T]{path: path}
}

func (f *File[T]) Load() (T, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loadLocked()
}

func (f *File[T]) loadLocked() (T, error) {
	var value T
	data, err := os.ReadFile(f.path)
	if errors.Is(err, os.ErrNotExist) {
		return value, nil
	}
	if err != nil {
		return value, fmt.Errorf("read %s: %w", f.path, err)
	}
	if len(data) == 0 {
		return value, nil
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return value, fmt.Errorf("decode %s: %w", f.path, err)
	}
	return value, nil
}

func (f *File[T]) saveLocked(value T) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", f.path, err)
	}
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(f.path), 0o755); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(f.path), filepath.Base(f.path)+"-*")
	if err != nil {
		return fmt.Errorf("create temporary state: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write temporary state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync temporary state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary state: %w", err)
	}
	if err := os.Rename(tmpName, f.path); err != nil {
		return fmt.Errorf("replace state file: %w", err)
	}
	return nil
}

func (f *File[T]) Save(value T) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.saveLocked(value)
}

func (f *File[T]) Update(change func(*T) error) (T, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	value, err := f.loadLocked()
	if err != nil {
		return value, err
	}
	if err := change(&value); err != nil {
		return value, err
	}
	if err := f.saveLocked(value); err != nil {
		return value, err
	}
	return value, nil
}
