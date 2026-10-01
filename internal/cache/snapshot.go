package cache

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"time"
)

type snapshotState struct {
	Version  int                  `json:"version"`
	Sequence uint64               `json:"sequence"`
	Entries  map[string]walRecord `json:"entries"`
}

type snapshotEnvelope struct {
	State    json.RawMessage `json:"state"`
	Checksum uint32          `json:"checksum"`
}

func (s *DurableStore) loadSnapshot() error {
	data, err := os.ReadFile(s.snapshotPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var envelope snapshotEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}
	if crc32.ChecksumIEEE(envelope.State) != envelope.Checksum {
		return fmt.Errorf("snapshot checksum mismatch")
	}
	var state snapshotState
	if err := json.Unmarshal(envelope.State, &state); err != nil {
		return fmt.Errorf("decode snapshot state: %w", err)
	}
	if state.Version != 1 || state.Entries == nil {
		return fmt.Errorf("unsupported snapshot state")
	}
	for key, record := range state.Entries {
		if !validRecord(record) || record.Op != "put" || key != record.Key || record.Sequence == 0 || record.Sequence > state.Sequence {
			return fmt.Errorf("invalid snapshot entry %q", key)
		}
	}
	s.entries = state.Entries
	s.sequence = state.Sequence
	return nil
}

// Snapshot saves current live entries and compacts the WAL. All store operations
// pause while it runs. The WAL inode is retained so its exclusive lock survives.
func (s *DurableStore) Snapshot() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	if s.failed != nil {
		return s.failed
	}
	s.purgeExpired(time.Now())
	if err := s.publishSnapshot(); err != nil {
		return err
	}
	// Publication (including directory sync) must finish before truncation.
	if err := s.trimTail(0); err != nil {
		s.failed = fmt.Errorf("compact WAL; reopen required: %w", err)
		return s.failed
	}
	if err := s.file.Sync(); err != nil {
		s.failed = fmt.Errorf("sync compacted WAL; reopen required: %w", err)
		return s.failed
	}
	return nil
}

// publishSnapshot requires s.mu. Separating publication from compaction also
// lets tests exercise recovery with the old WAL still present.
func (s *DurableStore) publishSnapshot() error {
	state, err := json.Marshal(snapshotState{Version: 1, Sequence: s.sequence, Entries: s.entries})
	if err != nil {
		return err
	}
	data, err := json.Marshal(snapshotEnvelope{State: state, Checksum: crc32.ChecksumIEEE(state)})
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.snapshotPath)
	tmp, err := os.CreateTemp(dir, ".snapshot-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), s.snapshotPath); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
