package redis

import (
	"encoding/json"
	"fmt"
)

// json.go — the tiny JSON (de)serialization used to pack the tag/metric MAPS into a
// single Redis hash FIELD.
//
// WHY JSON for the map fields (and not a hand-rolled key=value join):
//
//	A model's tags or a version's metrics are themselves a map, but a Redis hash field
//	holds a single string. We could flatten "k1=v1,k2=v2", but a tag VALUE may legally
//	contain '=' or ',' (free-form labels), which would corrupt a naive split. JSON does
//	the quoting/escaping correctly and round-trips losslessly. It is the same encoding
//	the Postgres side uses for its JSONB columns, so the two stores agree on the wire
//	shape of a tag/metric map — handy when a projection rebuild reads Postgres and writes
//	Redis.
//
// These wrap encoding/json with the package's error-wrapping convention so a malformed
// stored value surfaces a clear, attributable error rather than a bare json error.

// jsonString marshals v to a compact JSON string.
func jsonString(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("registry/redis: marshal hash field: %w", err)
	}
	return string(b), nil
}

// jsonParse unmarshals a JSON string into dest.
func jsonParse(s string, dest any) error {
	if err := json.Unmarshal([]byte(s), dest); err != nil {
		return fmt.Errorf("registry/redis: unmarshal hash field: %w", err)
	}
	return nil
}
