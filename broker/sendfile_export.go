package broker

import (
	"io"
	"os"
)

func SendFile(w io.Writer, f *os.File, off int64, count int64) (int64, error) {
	return sendFile(w, f, off, count)
}
