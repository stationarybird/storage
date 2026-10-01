package cache

import (
	"distributedcache/internal/version"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// Each WAL frame is a length, CRC32 checksum, and JSON payload. Bound frame
// allocation during recovery and reject oversized writes before touching disk.
const maxRecordBytes = 16 << 20

type walRecord struct {
	Siblings []version.Record `json:"siblings,omitempty"`
	Sequence uint64           `json:"sequence,omitempty"`
	Version  int              `json:"version"`
	Op       string           `json:"op"`
	Key      string           `json:"key"`
	Value    []byte           `json:"value,omitempty"`
	Expires  time.Time        `json:"expires,omitempty"`
}

// DurableStore keeps live values in RAM and synchronously persists mutations.
// Capacity counts keys; <= 0 means unlimited. It never evicts live keys.
// A WAL file has one owner. File locking currently supports Unix platforms.
type DurableStore struct {
	mu           sync.Mutex
	file         *os.File
	entries      map[string]walRecord
	capacity     int
	failed       error
	closed       bool
	sequence     uint64
	snapshotPath string
}

func OpenDurableStore(path string, capacity int) (*DurableStore, error) {
	// The parent directory must exist, so its own creation/durability remains
	// the responsibility of the caller or deployment.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("lock WAL: %w", err)
	}
	s := &DurableStore{file: f, capacity: capacity, entries: make(map[string]walRecord), snapshotPath: path + ".snapshot"}
	if err := s.loadSnapshot(); err != nil {
		f.Close()
		return nil, err
	}
	if err := s.replay(); err != nil {
		f.Close()
		return nil, err
	}
	// Persist both the file and its directory entry before accepting writes.
	err = f.Sync()
	if err == nil {
		var dir *os.File
		dir, err = os.Open(filepath.Dir(path))
		if err == nil {
			err = dir.Sync()
			dir.Close()
		}
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	s.purgeExpired(time.Now())
	// Preserve recovered keys even if the configured capacity was reduced.
	return s, nil
}

func (s *DurableStore) replay() error {
	var offset int64
	var previous uint64
	boundary := s.sequence
	for {
		var header [8]byte
		_, err := io.ReadFull(s.file, header[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return s.trimTail(offset)
		}
		if err != nil {
			return err
		}
		size := binary.BigEndian.Uint32(header[:4])
		if size == 0 || size > maxRecordBytes {
			return fmt.Errorf("invalid WAL frame length at %d", offset)
		}
		payload := make([]byte, int(size))
		if _, err := io.ReadFull(s.file, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return s.trimTail(offset)
			}
			return err
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(header[4:]) {
			return fmt.Errorf("WAL checksum mismatch at %d", offset)
		}
		var record walRecord
		if err := json.Unmarshal(payload, &record); err != nil {
			return fmt.Errorf("decode WAL at %d: %w", offset, err)
		}
		if !validRecord(record) {
			return fmt.Errorf("unsupported WAL record at %d", offset)
		}
		// Old WALs had no sequences. Assign their original physical order.
		if record.Sequence == 0 {
			record.Sequence = previous + 1
		}
		if record.Sequence <= previous || (previous != 0 && record.Sequence != previous+1) {
			return fmt.Errorf("invalid WAL sequence at %d", offset)
		}
		previous = record.Sequence
		if record.Sequence > boundary {
			if record.Sequence != s.sequence+1 {
				return fmt.Errorf("missing WAL sequence at %d", offset)
			}
			s.apply(record)
			s.sequence = record.Sequence
		}
		offset += 8 + int64(size)
	}
}

func validRecord(r walRecord) bool {
	if r.Version == 1 {
		return len(r.Siblings) == 0 && (r.Op == "put" || r.Op == "delete")
	}
	if r.Version != 2 || r.Op != "put" || len(r.Siblings) == 0 || !r.Expires.IsZero() || len(r.Value) != 0 {
		return false
	}
	_, err := version.Merge(r.Siblings)
	return err == nil
}

func (s *DurableStore) trimTail(offset int64) error {
	if err := s.file.Truncate(offset); err != nil {
		return err
	}
	_, err := s.file.Seek(offset, io.SeekStart)
	return err
}

func (s *DurableStore) apply(r walRecord) {
	if r.Op == "delete" {
		delete(s.entries, r.Key)
	} else {
		s.entries[r.Key] = r
	}
}

func (s *DurableStore) append(r walRecord) error {
	if s.closed {
		return os.ErrClosed
	}
	if s.failed != nil {
		return s.failed
	}
	if s.sequence == ^uint64(0) {
		return errors.New("WAL sequence exhausted")
	}
	r.Sequence = s.sequence + 1
	payload, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(payload) > maxRecordBytes {
		return errors.New("WAL record exceeds 16 MiB")
	}
	frame := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[8:], payload)
	n, err := s.file.Write(frame)
	if err == nil && n != len(frame) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = s.file.Sync()
	}
	if err != nil {
		// A failed sync has an uncertain outcome. Stop subsequent writes until
		// reopen/recovery rather than append behind a potentially partial frame.
		s.failed = fmt.Errorf("WAL write failed; reopen required: %w", err)
		return s.failed
	}
	s.apply(r)
	s.sequence = r.Sequence
	return nil
}

func (s *DurableStore) Put(key string, value []byte, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.purgeExpired(now)
	if _, exists := s.entries[key]; !exists && s.capacity > 0 && len(s.entries) >= s.capacity {
		return ErrCapacity
	}
	r := walRecord{Version: 1, Op: "put", Key: key, Value: append([]byte(nil), value...)}
	if ttl > 0 {
		r.Expires = now.Add(ttl)
	}
	return s.append(r)
}

func (s *DurableStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.append(walRecord{Version: 1, Op: "delete", Key: key})
}

func (s *DurableStore) Get(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, found := s.entries[key]
	if !found || s.closed {
		return nil, false
	}
	if !r.Expires.IsZero() && !time.Now().Before(r.Expires) {
		delete(s.entries, key)
		return nil, false
	}
	return append([]byte(nil), r.Value...), true
}

func (s *DurableStore) purgeExpired(now time.Time) {
	for key, r := range s.entries {
		if !r.Expires.IsZero() && !now.Before(r.Expires) {
			delete(s.entries, key)
		}
	}
}

func (s *DurableStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.file.Close()
}
