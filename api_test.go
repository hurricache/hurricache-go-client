package hurricache

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestPublicContractCoverage(t *testing.T) {
	covered := map[string]bool{}
	for _, tc := range operationCases() {
		covered[tc.name] = true
	}
	for _, name := range []string{"Close", "Target", "DefaultClientID", "DefaultTimeout", "DefaultTTL"} {
		covered[name] = true
	}
	contract := reflect.TypeFor[Operations]()
	for i := 0; i < contract.NumMethod(); i++ {
		method := contract.Method(i)
		if !covered[method.Name] {
			t.Errorf("Operations.%s lacks operation matrix coverage", method.Name)
		}
		delete(covered, method.Name)
	}
	for name := range covered {
		t.Errorf("matrix method %s missing from Operations interface", name)
	}
	conn := transport(t, nil, nil)
	c, e := NewClientWithConnection(conn, Config{ClientID: 12, Timeout: 2 * time.Second, DefaultTTL: 3 * time.Minute})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if c.DefaultClientID() != 12 || c.DefaultTimeout() != 2*time.Second || c.DefaultTTL() != 3*time.Minute || c.Target() != "borrowed" {
		t.Fatal("accessor defaults")
	}
	if _, e := c.GetValue(context.Background(), []byte("k"), Options{TTL: Ptr(-time.Second)}); e == nil {
		t.Fatal("negative TTL accepted")
	}
}
