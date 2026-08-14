package broker

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"
)

// Record models a broker record with the fields required for single-node
// append-only log storage and consumer fetches.
type Record struct {
	Key       []byte            `json:"key"`
	Value     []byte            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
	Offset    int64             `json:"offset"`
}

// Topic represents a named topic and its underlying segment files.
type Topic struct {
	Name       string
	Partition  int
	Dir        string
	Segments   []*Segment
	NextOffset int64
	mu         sync.Mutex
}

// Segment is an append-only log segment with an offset index and optional time index.
type Segment struct {
	Path        string
	LogPath     string
	IndexPath   string
	TimeIndex   string
	LogFile     *os.File
	IndexFile   *os.File
	position    int64
	nextOffset  int64
	offsets     map[int64]int64
	timeOffsets map[int64]int64
	mu          sync.Mutex
}

func newSegment(path string, startOffset int64) (*Segment, error) {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}

	seg := &Segment{
		Path:        path,
		LogPath:     filepath.Join(path, "segment.log"),
		IndexPath:   filepath.Join(path, "segment.index"),
		TimeIndex:   filepath.Join(path, "segment.timeindex"),
		offsets:     make(map[int64]int64),
		timeOffsets: make(map[int64]int64),
		nextOffset:  startOffset,
	}

	logFile, err := os.OpenFile(seg.LogPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log: %w", err)
	}
	seg.LogFile = logFile

	fileInfo, err := logFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat log: %w", err)
	}
	seg.position = fileInfo.Size()

	// Load existing index file into memory for O(1) lookups in the
	// single-node implementation. This provides equivalent behavior
	// to a memory-mapped sparse index for Phase 1.
	if err := seg.loadIndex(); err != nil {
		return nil, fmt.Errorf("load index: %w", err)
	}

	idx, err := os.OpenFile(seg.IndexPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open index: %w", err)
	}
	seg.IndexFile = idx

	return seg, nil
}

func (s *Segment) Append(record Record) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	payload, err := json.Marshal(record)
	if err != nil {
		return 0, fmt.Errorf("marshal record: %w", err)
	}

	length := int64(len(payload))
	if length > 0xFFFFFFFF {
		return 0, fmt.Errorf("record exceeds max payload size")
	}

	offset := s.nextOffset
	pos := s.position
	prefix := make([]byte, 4)
	binary.BigEndian.PutUint32(prefix, uint32(length))
	if _, err = s.LogFile.WriteAt(prefix, pos); err != nil {
		return 0, fmt.Errorf("write length prefix: %w", err)
	}
	if _, err = s.LogFile.WriteAt(payload, pos+4); err != nil {
		return 0, fmt.Errorf("write payload: %w", err)
	}

	s.offsets[offset] = pos
	s.timeOffsets[record.Timestamp.UnixNano()] = offset
	s.position += 4 + length
	s.nextOffset++

	if err = s.persistIndexes(); err != nil {
		return 0, fmt.Errorf("persist index: %w", err)
	}

	return offset, nil
}

func (s *Segment) ReadRange(fromOffset int64, max int) ([]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if max <= 0 {
		max = 100
	}

	keys := make([]int64, 0, len(s.offsets))
	for offset := range s.offsets {
		if offset >= fromOffset {
			keys = append(keys, offset)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	result := make([]Record, 0, min(max, len(keys)))
	for _, offset := range keys {
		if len(result) >= max {
			break
		}
		rec, err := s.readByOffset(offset)
		if err != nil {
			return nil, err
		}
		result = append(result, rec)
	}
	return result, nil
}

func (s *Segment) readByOffset(offset int64) (Record, error) {
	pos, ok := s.offsets[offset]
	if !ok {
		return Record{}, fmt.Errorf("offset %d not found in segment", offset)
	}

	var sizeBuf [4]byte
	if _, err := s.LogFile.ReadAt(sizeBuf[:], pos); err != nil {
		return Record{}, fmt.Errorf("read payload length: %w", err)
	}
	length := int64(binary.BigEndian.Uint32(sizeBuf[:]))
	payload := make([]byte, length)
	if _, err := s.LogFile.ReadAt(payload, pos+4); err != nil {
		return Record{}, fmt.Errorf("read payload: %w", err)
	}

	var rec Record
	if err := json.Unmarshal(payload, &rec); err != nil {
		return Record{}, fmt.Errorf("unmarshal record: %w", err)
	}
	return rec, nil
}

func (s *Segment) persistIndexes() error {
	entries := make([]indexEntry, 0, len(s.offsets))
	for offset, pos := range s.offsets {
		entries = append(entries, indexEntry{Offset: offset, Position: pos})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Offset < entries[j].Offset })

	buf := make([]byte, 0, len(entries)*16)
	for _, entry := range entries {
		buf = append(buf, encodeIndexEntry(entry)...)
	}
	if err := os.WriteFile(s.IndexPath, buf, 0o644); err != nil {
		return err
	}

	timeEntries := make([]timeIndexEntry, 0, len(s.timeOffsets))
	for ts, offset := range s.timeOffsets {
		timeEntries = append(timeEntries, timeIndexEntry{Timestamp: ts, Offset: offset})
	}
	sort.Slice(timeEntries, func(i, j int) bool { return timeEntries[i].Timestamp < timeEntries[j].Timestamp })
	timeBuf := make([]byte, 0, len(timeEntries)*16)
	for _, entry := range timeEntries {
		timeBuf = append(timeBuf, encodeTimeEntry(entry)...)
	}
	return os.WriteFile(s.TimeIndex, timeBuf, 0o644)
}

func (s *Segment) loadIndex() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Try memory-mapping the index file for fast lookups.
	f, err := os.Open(s.IndexPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return err
	}
	size := stat.Size()
	if size == 0 {
		return nil
	}
	// Ensure size is multiple of 16
	if size%16 != 0 {
		return fmt.Errorf("corrupt index file")
	}

	// Map the file into memory.
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err == nil {
		defer syscall.Munmap(data)
		s.offsets = make(map[int64]int64, size/16)
		maxOffset := s.nextOffset
		for i := 0; i < len(data); i += 16 {
			off := bytesToInt64(data[i : i+8])
			pos := bytesToInt64(data[i+8 : i+16])
			s.offsets[off] = pos
			if off >= maxOffset {
				maxOffset = off + 1
			}
		}
		s.nextOffset = maxOffset
		return nil
	}

	// Fallback to reading the file into memory.
	data2, err := os.ReadFile(s.IndexPath)
	if err != nil {
		return err
	}
	s.offsets = make(map[int64]int64, len(data2)/16)
	maxOffset := s.nextOffset
	for i := 0; i < len(data2); i += 16 {
		off := bytesToInt64(data2[i : i+8])
		pos := bytesToInt64(data2[i+8 : i+16])
		s.offsets[off] = pos
		if off >= maxOffset {
			maxOffset = off + 1
		}
	}
	s.nextOffset = maxOffset
	return nil
}

func bytesToInt64(b []byte) int64 {
	var v int64
	for i := 0; i < 8; i++ {
		v = (v << 8) | int64(b[i])
	}
	return v
}

type indexEntry struct {
	Offset   int64
	Position int64
}

type timeIndexEntry struct {
	Timestamp int64
	Offset    int64
}

func encodeIndexEntry(entry indexEntry) []byte {
	buf := make([]byte, 16)
	copy(buf[0:8], int64ToBytes(entry.Offset))
	copy(buf[8:16], int64ToBytes(entry.Position))
	return buf
}

func encodeTimeEntry(entry timeIndexEntry) []byte {
	buf := make([]byte, 16)
	copy(buf[0:8], int64ToBytes(entry.Timestamp))
	copy(buf[8:16], int64ToBytes(entry.Offset))
	return buf
}

func int64ToBytes(v int64) []byte {
	b := make([]byte, 8)
	for i := 0; i < 8; i++ {
		b[i] = byte(v >> (8 * (7 - i)))
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func init() {
	_ = binary.BigEndian
}
