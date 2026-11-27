package hurricache

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTestCA(t *testing.T) (caFile string, caCert *x509.Certificate, caKey *rsa.PrivateKey) {
	t.Helper()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	caFile = filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		t.Fatal(err)
	}
	return caFile, caCert, caKey
}

func signWithCA(t *testing.T, caCert *x509.Certificate, caKey *rsa.PrivateKey, cn string) (certFile, keyFile string, der []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err = x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile, der
}

func TestTLSConfigLoadEmpty(t *testing.T) {
	cfg, err := TLSConfig{}.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.InsecureSkipVerify {
		t.Error("empty TLSConfig must produce encryption-only config (InsecureSkipVerify)")
	}
}

func TestTLSConfigLoadCertWithoutKey(t *testing.T) {
	if _, err := (TLSConfig{CertFile: "cert.pem"}).Load(); err == nil {
		t.Error("expected error when cert is set without key")
	}
}

func TestTLSConfigLoadMissingCA(t *testing.T) {
	if _, err := (TLSConfig{CAFile: "/nonexistent/ca.pem"}).Load(); err == nil {
		t.Error("expected error for missing CA file")
	}
}

func TestTLSConfigLoadCAChainOnlyVerification(t *testing.T) {
	caFile, caCert, caKey := writeTestCA(t)
	certFile, keyFile, serverDER := signWithCA(t, caCert, caKey, "grpc.hurricache")

	cfg, err := TLSConfig{CAFile: caFile, CertFile: certFile, KeyFile: keyFile}.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.VerifyPeerCertificate == nil {
		t.Fatal("expected custom VerifyPeerCertificate for chain-only verification")
	}
	// Цепочка от нашего CA принимается без проверки hostname
	if err := cfg.VerifyPeerCertificate([][]byte{serverDER}, nil); err != nil {
		t.Errorf("valid chain rejected: %v", err)
	}
	// Чужой самоподписанный сертификат отвергается
	_, _, foreignDER := signWithCA(t, func() *x509.Certificate {
		c := *caCert
		return &c
	}(), caKey, "evil")
	_ = foreignDER
	otherKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	selfTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "evil"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	foreign, err := x509.CreateCertificate(rand.Reader, selfTmpl, selfTmpl, &otherKey.PublicKey, otherKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.VerifyPeerCertificate([][]byte{foreign}, nil); err == nil {
		t.Error("foreign self-signed certificate must be rejected")
	}
}

func TestTLSConfigLoadServerName(t *testing.T) {
	caFile, _, _ := writeTestCA(t)
	cfg, err := TLSConfig{CAFile: caFile, ServerName: "grpc.hurricache"}.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServerName != "grpc.hurricache" || cfg.InsecureSkipVerify {
		t.Error("ServerName mode must keep standard verification")
	}
}
