package raft

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
)

func RotateCA() ([]byte, *rsa.PrivateKey, error) {
	return generateCA()
}

func IssueNodeCert(caCertPEM []byte, caKey *rsa.PrivateKey, host string) ([]byte, []byte, error) {
	return generateCertForHost(caCertPEM, caKey, host)
}

func EncodePrivateKeyPEM(key *rsa.PrivateKey) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}

func DecodeCertPEM(b []byte) ([]byte, error) {
	block, _ := pem.Decode(b)
	if block == nil {
		return nil, x509.CertificateInvalidError{Cert: nil, Reason: x509.Expired}
	}
	return block.Bytes, nil
}
