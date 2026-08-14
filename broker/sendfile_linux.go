//go:build linux
// +build linux

package broker

import (
	"io"
	"os"
	"syscall"
)

// sendFile uses the linux syscall.Sendfile to perform zero-copy transfer
// from file descriptor of f to the file descriptor of w, if w is an *os.File
// backed by a socket. If it fails, caller may fallback to other means.
func sendFile(w io.Writer, f *os.File, off int64, count int64) (int64, error) {
	// Attempt to get file descriptor from writer
	wf, ok := w.(*os.File)
	if !ok {
		return 0, io.ErrInvalid
	}
	var total int64
	// Loop until all bytes sent
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
