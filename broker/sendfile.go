package broker

import (
	"io"
	"os"
)

func sendFileFallback(w io.Writer, f *os.File, off int64, count int64) (int64, error) {
	if _, err := f.Seek(off, 0); err != nil {
		return 0, err
	}
	var copied int64
	buf := make([]byte, 32*1024)
	remain := count
	for remain > 0 {
		toRead := int64(len(buf))
		if remain < toRead {
			toRead = remain
		}
		n, err := f.Read(buf[:toRead])
		if n > 0 {
			wn, werr := w.Write(buf[:n])
			copied += int64(wn)
			if werr != nil {
				return copied, werr
			}
			if int64(n) != int64(wn) {
				return copied, io.ErrShortWrite
			}
			remain -= int64(n)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return copied, err
		}
	}
	return copied, nil
}
