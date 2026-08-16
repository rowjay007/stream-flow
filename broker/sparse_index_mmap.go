package broker

import (
	"encoding/binary"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
)

// SparseMMapIndex stores offset->position entries using a fixed-width mmap file.
type SparseMMapIndex struct {
	file    *os.File
	data    []byte
	entries int
}

func OpenSparseMMapIndex(path string, maxEntries int) (*SparseMMapIndex, error) {
	if maxEntries <= 0 {
		maxEntries = 1024
	}
	sz := maxEntries * 16
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(int64(sz)); err != nil {
		_ = f.Close()
		return nil, err
	}
	mapped, err := unix.Mmap(int(f.Fd()), 0, sz, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	idx := &SparseMMapIndex{file: f, data: mapped}
	for i := 0; i < maxEntries; i++ {
		base := i * 16
		off := int64(binary.BigEndian.Uint64(mapped[base : base+8]))
		pos := int64(binary.BigEndian.Uint64(mapped[base+8 : base+16]))
		if off == 0 && pos == 0 {
			break
		}
		idx.entries++
	}
	return idx, nil
}

func (s *SparseMMapIndex) Append(offset, position int64) error {
	base := s.entries * 16
	if base+16 > len(s.data) {
		return fmt.Errorf("sparse mmap index full")
	}
	binary.BigEndian.PutUint64(s.data[base:base+8], uint64(offset))
	binary.BigEndian.PutUint64(s.data[base+8:base+16], uint64(position))
	s.entries++
	return nil
}

func (s *SparseMMapIndex) Find(offset int64) (int64, bool) {
	for i := 0; i < s.entries; i++ {
		base := i * 16
		off := int64(binary.BigEndian.Uint64(s.data[base : base+8]))
		if off == offset {
			pos := int64(binary.BigEndian.Uint64(s.data[base+8 : base+16]))
			return pos, true
		}
	}
	return 0, false
}

func (s *SparseMMapIndex) Close() error {
	if s.data != nil {
		if err := unix.Munmap(s.data); err != nil {
			return err
		}
		s.data = nil
	}
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}
