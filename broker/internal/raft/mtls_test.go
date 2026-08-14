package raft

import (
	"testing"
)

func TestRotateCAAndIssueCert(t *testing.T) {
	caPEM, caKey, err := generateCA()
	if err != nil {
		t.Fatal(err)
	}
	certPEM, keyPEM, err := generateCertForHost(caPEM, caKey, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	if len(certPEM) == 0 || len(keyPEM) == 0 {
		t.Fatalf("expected cert and key PEM bytes")
	}
}
