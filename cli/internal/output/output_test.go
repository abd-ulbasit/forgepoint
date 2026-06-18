package output

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// TestTableHuman checks aligned columns and that every header + cell appears.
func TestTableHuman(t *testing.T) {
	var buf bytes.Buffer
	p := New(&buf, false)
	err := p.Table(
		[]string{"ID", "NAME"},
		[][]string{{"1", "alpha"}, {"22", "beta"}},
	)
	if err != nil {
		t.Fatalf("Table: %v", err)
	}
	out := buf.String()
	for _, want := range []string{"ID", "NAME", "alpha", "beta", "22"} {
		if !strings.Contains(out, want) {
			t.Errorf("table output missing %q:\n%s", want, out)
		}
	}
}

// TestTableJSON: --json mode emits a parseable array of objects keyed by header.
func TestTableJSON(t *testing.T) {
	var buf bytes.Buffer
	p := New(&buf, true)
	if err := p.Table([]string{"ID", "NAME"}, [][]string{{"1", "alpha"}}); err != nil {
		t.Fatalf("Table: %v", err)
	}

	var got []map[string]string
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, buf.String())
	}
	if len(got) != 1 || got[0]["ID"] != "1" || got[0]["NAME"] != "alpha" {
		t.Errorf("unexpected JSON: %#v", got)
	}
}

// TestKeyValsJSON: detail view as a JSON object under --json.
func TestKeyValsJSON(t *testing.T) {
	var buf bytes.Buffer
	p := New(&buf, true)
	if err := p.KeyVals([][2]string{{"ID", "x"}, {"Name", "y"}}); err != nil {
		t.Fatalf("KeyVals: %v", err)
	}
	var got map[string]string
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("not JSON: %v", err)
	}
	if got["ID"] != "x" || got["Name"] != "y" {
		t.Errorf("unexpected: %#v", got)
	}
}

// TestKeyValsHuman: "Key: value" lines.
func TestKeyValsHuman(t *testing.T) {
	var buf bytes.Buffer
	p := New(&buf, false)
	if err := p.KeyVals([][2]string{{"ID", "x"}}); err != nil {
		t.Fatalf("KeyVals: %v", err)
	}
	if !strings.Contains(buf.String(), "ID:") || !strings.Contains(buf.String(), "x") {
		t.Errorf("missing key/value: %s", buf.String())
	}
}

// TestLineJSON: even a plain status line is structured under --json.
func TestLineJSON(t *testing.T) {
	var buf bytes.Buffer
	p := New(&buf, true)
	if err := p.Line("hello %s", "world"); err != nil {
		t.Fatalf("Line: %v", err)
	}
	var got map[string]string
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("not JSON: %v", err)
	}
	if got["message"] != "hello world" {
		t.Errorf("message = %q", got["message"])
	}
}

// TestJSONFlagAccessor sanity-checks the mode getter.
func TestJSONFlagAccessor(t *testing.T) {
	if !New(&bytes.Buffer{}, true).JSON() {
		t.Error("JSON() should be true")
	}
	if New(&bytes.Buffer{}, false).JSON() {
		t.Error("JSON() should be false")
	}
}
