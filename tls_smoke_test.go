package hurricache

import (
	"context"
	"os"
	"testing"
	"time"
)

// Live smoke test against a TLS-enabled standalone server.
// Skipped unless HURRICACHE_TLS_SMOKE=1 and the server listens on
// HURRICACHE_TLS_TARGET (default 127.0.0.1:40000) with mTLS enforced.
func TestTLSSmoke(t *testing.T) {
	if os.Getenv("HURRICACHE_TLS_SMOKE") != "1" {
		t.Skip("set HURRICACHE_TLS_SMOKE=1 to run the live TLS smoke test")
	}
	target := os.Getenv("HURRICACHE_TLS_TARGET")
	if target == "" {
		target = "127.0.0.1:40000"
	}
	certsDir := os.Getenv("HURRICACHE_TLS_CERTS")
	if certsDir == "" {
		certsDir = "/ext/fastcache/tls-test-certs"
	}

	tlsCfg, err := TLSConfig{
		CAFile:   certsDir + "/ca.pem",
		CertFile: certsDir + "/client-cert.pem",
		KeyFile:  certsDir + "/client-key.pem",
	}.Load()
	if err != nil {
		t.Fatal(err)
	}

	client, err := NewClient(target, Config{TLS: tlsCfg})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	key := []byte("smoke-go")
	hint, err := client.Create(ctx, key, Bytes([]byte("go-tls-ok")), Options{})
	if err != nil {
		t.Fatalf("create over mTLS: %v", err)
	}
	value, err := client.Get(ctx, key, Options{Hint: hint})
	if err != nil {
		t.Fatalf("get over mTLS: %v", err)
	}
	raw, err := value.Bytes()
	if err != nil {
												t.Fatal(err)
											}
	if string(raw) != "go-tls-ok" {
		t.Fatalf("unexpected value: %v", value)
	}

	// Клиент без сертификата должен быть отклонён (на сервере mTLS)
	tlsNoCert, err := TLSConfig{CAFile: certsDir + "/ca.pem"}.Load()
	if err != nil {
		t.Fatal(err)
	}
	bad, err := NewClient(target, Config{TLS: tlsNoCert})
	if err != nil {
		t.Fatal(err)
	}
	defer bad.Close()
	badCtx, badCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer badCancel()
	if _, err = bad.Get(badCtx, key, Options{}); err == nil {
		t.Error("client without certificate must be rejected when mTLS is enforced")
	}
}
