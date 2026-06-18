// cursor_codec.go — the base64 helpers behind the opaque list cursor.
//
// Kept in their own file so prompt_store.go's cursor logic reads cleanly. We use
// URL-safe base64 (no '+' or '/') so a cursor is safe to put in a URL query string
// or a gRPC metadata value without escaping. RawURLEncoding omits '=' padding, so the
// token is a tad shorter and has no character that ever needs percent-encoding.
package postgres

import "encoding/base64"

// base64Encode renders raw bytes-as-string into a URL-safe, unpadded base64 token.
func base64Encode(raw string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// base64Decode parses a URL-safe, unpadded base64 token back to its string. A
// malformed token returns an error the caller turns into a validation error.
func base64Decode(token string) (string, error) {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
