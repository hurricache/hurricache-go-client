package hurricache

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
)

// TLSConfig describes client TLS settings from PEM files, mirroring the
// server-side `tls` section semantics:
//   - zero TLSConfig / TLS == nil in Config -> plaintext, as before;
//   - CAFile set -> the server chain is verified against this CA; the
//     hostname (CN/SAN) is NOT verified unless ServerName is set;
//   - CAFile empty -> encryption only, the server is not verified;
//   - CertFile+KeyFile -> client certificate for mTLS (mandatory when the
//     server has ca configured in its tls section).
type TLSConfig struct {
	CAFile     string // root CA PEM used to verify the server
	CertFile   string // client certificate PEM (mTLS)
	KeyFile    string // client private key PEM (mTLS)
	ServerName string // optional: expected server certificate name (SNI + hostname check)
}

// Load builds a *tls.Config for Config.TLS. It reads and validates the
// referenced PEM files eagerly, so misconfiguration surfaces at client
// construction time rather than on the first RPC.
func (c TLSConfig) Load() (*tls.Config, error) {
	cfg := &tls.Config{}

	if c.CertFile != "" || c.KeyFile != "" {
		if c.CertFile == "" || c.KeyFile == "" {
			return nil, errors.New("hurricache: TLS CertFile and KeyFile must be set together")
		}
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("hurricache: TLS client cert/key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	if c.CAFile == "" {
		// No CA: encryption only, the server is not verified.
		cfg.InsecureSkipVerify = true
		return cfg, nil
	}

	pem, err := os.ReadFile(c.CAFile)
	if err != nil {
		return nil, fmt.Errorf("hurricache: TLS CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("hurricache: TLS CA file %q: no valid PEM certificates", c.CAFile)
	}
	cfg.RootCAs = pool

	if c.ServerName != "" {
		// Standard verification: chain against CA + hostname against ServerName.
		cfg.ServerName = c.ServerName
		return cfg, nil
	}

	// Chain verification against the CA without hostname verification:
	// InsecureSkipVerify disables the built-in checks, VerifyPeerCertificate
	// re-runs the chain check with an empty DNSName.
	cfg.InsecureSkipVerify = true
	cfg.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("hurricache: TLS server presented no certificates")
		}
		certs := make([]*x509.Certificate, 0, len(rawCerts))
		for _, raw := range rawCerts {
			cert, err := x509.ParseCertificate(raw)
			if err != nil {
				return fmt.Errorf("hurricache: TLS server certificate parse: %w", err)
			}
			certs = append(certs, cert)
		}
		intermediates := x509.NewCertPool()
		for _, cert := range certs[1:] {
			intermediates.AddCert(cert)
		}
		_, err := certs[0].Verify(x509.VerifyOptions{
			Roots:         pool,
			Intermediates: intermediates,
			DNSName:       "",
		})
		if err != nil {
			return fmt.Errorf("hurricache: TLS server chain verification: %w", err)
		}
		return nil
	}
	return cfg, nil
}
