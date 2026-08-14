package raft

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
)

// RotateCA generates a new CA cert and key.
func RotateCA() ([]byte, *rsa.PrivateKey, error) {
	return generateCA()
}

// IssueNodeCert issues a cert signed by the provided CA for the given host.
func IssueNodeCert(caCertPEM []byte, caKey *rsa.PrivateKey, host string) ([]byte, []byte, error) {
	return generateCertForHost(caCertPEM, caKey, host)
}

// EncodePrivateKeyPEM encodes an RSA private key to PEM format.
func EncodePrivateKeyPEM(key *rsa.PrivateKey) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}

// DecodeCertPEM decodes a PEM certificate to DER bytes.
func DecodeCertPEM(b []byte) ([]byte, error) {
	block, _ := pem.Decode(b)
	if block == nil {
		return nil, x509.CertificateInvalidError{Cert: nil, Reason: x509.Expired}
	}
	return block.Bytes, nil
}
