package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// GenerateDevCert creates a self-signed ECDSA P-256 certificate suitable
// for development. Writes <outDir>/cert.pem and <outDir>/key.pem and also
// returns a ready-to-use tls.Certificate.
//
// DO NOT USE IN PRODUCTION. This is for `prmd admin generate-cert` and
// for tests.
func GenerateDevCert(host string, outDir string) (tls.Certificate, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate serial: %w", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: host},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{host},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create cert: %w", err)
	}
	keyBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("marshal key: %w", err)
	}

	if outDir != "" {
		if err := os.MkdirAll(outDir, 0o700); err != nil {
			return tls.Certificate{}, fmt.Errorf("mkdir: %w", err)
		}
		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes})
		if err := os.WriteFile(filepath.Join(outDir, "cert.pem"), certPEM, 0o600); err != nil {
			return tls.Certificate{}, fmt.Errorf("write cert: %w", err)
		}
		if err := os.WriteFile(filepath.Join(outDir, "key.pem"), keyPEM, 0o600); err != nil {
			return tls.Certificate{}, fmt.Errorf("write key: %w", err)
		}
	}

	cert := tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  priv,
	}
	return cert, nil
}

// DevTLSConfig returns a TLS config wrapping a single self-signed cert.
// Intended for tests and for `prmd serve --dev`.
func DevTLSConfig(host string) (*tls.Config, error) {
	cert, err := GenerateDevCert(host, "")
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}
