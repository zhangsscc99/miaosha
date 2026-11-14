package main

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sort"
	"syscall"
)

const (
	messageMagic   int32 = -626843481
	messageMagicV2 int32 = -626843477
	blankMagic     int32 = -875286124
)

type walked struct {
	files     int
	logical   int64
	alloc     int64
	messages  int64
	bytes     int64
	blankSkip int64
}

func walkCommitLog(dir string) (walked, error) {
	var w walked
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return w, nil
		}
		return w, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)
	for _, name := range names {
		path := filepath.Join(dir, name)
		info, err := os.Stat(path)
		if err != nil {
			return w, err
		}
		w.files++
		w.logical += info.Size()
		w.alloc += allocBytes(info)
		n, b, blank, err := walkFile(path)
		if err != nil {
			return w, err
		}
		w.messages += n
		w.bytes += b
		w.blankSkip += blank
	}
	return w, nil
}

func walkFile(path string) (count, bytes, blank int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, 0, 0, err
	}
	size := info.Size()
	var pos int64
	hdr := make([]byte, 8)
	for pos+8 <= size {
		if _, err := f.ReadAt(hdr, pos); err != nil {
			return count, bytes, blank, err
		}
		total := int32(binary.BigEndian.Uint32(hdr[0:4]))
		magic := int32(binary.BigEndian.Uint32(hdr[4:8]))
		if total <= 0 || total > 4<<20 {
			break
		}
		if magic == blankMagic {
			blank += int64(total)
			break
		}
		if magic != messageMagic && magic != messageMagicV2 {
			break
		}
		if pos+int64(total) > size {
			break
		}
		count++
		bytes += int64(total)
		pos += int64(total)
	}
	return count, bytes, blank, nil
}

func dirUsage(root string) (logical, alloc int64, err error) {
	err = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if info.IsDir() {
			return nil
		}
		logical += info.Size()
		alloc += allocBytes(info)
		return nil
	})
	return logical, alloc, err
}

func allocBytes(info os.FileInfo) int64 {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return info.Size()
	}
	return st.Blocks * 512
}
