package processor

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"strings"
	"sync"
)

// StartProcessorServer starts a simple TCP server that accepts newline-delimited JSON records
// and echoes them back (for now). Returns the listener so tests can obtain the bound address.
func StartProcessorServer(listenAddr, serverCert, serverKey string) (interface {
	Stop()
	Addr() net.Addr
}, net.Listener, error) {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, nil, err
	}
	srv := &tcpProcessorServer{ln: ln, wg: &sync.WaitGroup{}}
	srv.wg.Add(1)
	go srv.serve()
	return srv, ln, nil
}

type tcpProcessorServer struct {
	ln net.Listener
	wg *sync.WaitGroup
}

func (s *tcpProcessorServer) serve() {
	defer s.wg.Done()
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "use of closed network connection") {
				return
			}
			continue
		}
		go s.handle(conn)
	}
}

func (s *tcpProcessorServer) handle(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				return
			}
			return
		}
		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			// ignore
			continue
		}
		// echo back
		b, _ := json.Marshal(rec)
		b = append(b, '\n')
		if _, err := w.Write(b); err != nil {
			return
		}
		w.Flush()
	}
}

func (s *tcpProcessorServer) Stop() {
	s.ln.Close()
	s.wg.Wait()
}

func (s *tcpProcessorServer) Addr() net.Addr { return s.ln.Addr() }

// ClientStream is a simple TCP client that communicates newline-delimited JSON.
type ClientStream struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
}

func NewClientStream(ctx interface{}, addr string) (*ClientStream, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &ClientStream{conn: conn, r: bufio.NewReader(conn), w: bufio.NewWriter(conn)}, nil
}

func (c *ClientStream) Send(rec Record) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if _, err := c.w.Write(b); err != nil {
		return err
	}
	return c.w.Flush()
}

func (c *ClientStream) Recv() (Record, error) {
	line, err := c.r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	var rec Record
	if err := json.Unmarshal(line, &rec); err != nil {
		return nil, err
	}
	return rec, nil
}

func (c *ClientStream) Close() error {
	return c.conn.Close()
}
