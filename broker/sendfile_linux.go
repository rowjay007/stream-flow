//go:build linux
// +build linux

package broker

import (
	"io"
	"os"
	"syscall"
)

func sendFile(w io.Writer, f *os.File, off int64, count int64) (int64, error) {

	wf, ok := w.(*os.File)
	if !ok {
		return 0, io.ErrInvalid
	}
	var total int64

	for total < count {
		toSend := int(count - total)
		n, err := syscall.Sendfile(int(wf.Fd()), int(f.Fd()), &off, toSend)
		if n > 0 {
			total += int64(n)
		}
		if err != nil {
			if err == syscall.EAGAIN || err == syscall.EINTR {
				continue
			}
			return total, err
		}
		if n == 0 {
			break
		}
	}
	return total, nil
}
