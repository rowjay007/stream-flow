package snapshot

import (
	"io"
	"log"
)

func CreateDelta(r io.Reader) (io.ReadCloser, error) {
	log.Printf("CreateDelta: placeholder returning full snapshot")

	pr, pw := io.Pipe()
	go func() {
		_, err := io.Copy(pw, r)
		_ = pw.CloseWithError(err)
	}()
	return pr, nil
}
