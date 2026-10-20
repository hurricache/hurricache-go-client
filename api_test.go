package hurricache

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestPublicContractCoverage(t *testing.T) {
	covered := map[string]bool{}
	for _, tc := range operationCases() {
		covered[tc.name] = true
	}
	for _, name := range []string{"Close", "Target", "DefaultClientID", "DefaultTimeout", "DefaultTTL", "DefaultCompressionThreshold"} {
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
	// The inventory generator consumes real reflected methods and matrix cases.
	// Keep this in the coverage test so nonexistent subtests cannot be documented.
	methods := map[string]string{}
	for i := 0; i < contract.NumMethod(); i++ {
		m := contract.Method(i)
		methods[m.Name] = m.Type.String()
	}
	for _, name := range []string{"IsReady"} {
		m, ok := reflect.TypeFor[*SmartClient]().MethodByName(name)
		if !ok {
			t.Fatalf("missing smart method %s", name)
		}
		methods[name] = m.Type.String()
	}
	cases := []string{}
	for _, tc := range operationCases() {
		cases = append(cases, tc.name)
	}
	b, err := json.Marshal(struct {
		Methods    map[string]string
		Cases      []string
		Configured []string
	}{methods, cases, configuredOperations()})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("PARITY_CONTRACT %s", b)
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
