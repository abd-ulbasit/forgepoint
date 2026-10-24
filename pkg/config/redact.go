package config

import (
	"net/url"
	"strings"
)

// RedactURL returns a connection URL that is safe to log: any password in the
// userinfo is replaced by "xxxxx" (net/url's own convention), so
// "redis://:s3cret@host:6379" logs as "redis://:xxxxx@host:6379".
//
// WHY this exists: the services log their NATS and Redis URLs at startup. In
// AWS those URLs come from Secrets Manager via External Secrets Operator and
// embed the ElastiCache auth token (rediss://:<token>@host), so logging them
// verbatim copies a credential into every log sink.
//
// Values without a scheme (a bare "host:port", as the registry and billing
// Redis config allow) carry no userinfo and are returned unchanged. A value
// that does not parse as a URL is NOT echoed back, because it may still
// contain a secret; a fixed placeholder is returned instead.
func RedactURL(raw string) string {
	if !strings.Contains(raw, "://") {
		return raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "<unparseable url>"
	}
	return u.Redacted()
}
