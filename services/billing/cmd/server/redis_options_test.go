package main

import (
	"net/url"
	"testing"
)

func TestRedisOptions(t *testing.T) {
	t.Run("bare host:port is used as the address", func(t *testing.T) {
		opts, err := redisOptions("redis.fp-infra.svc.cluster.local:6379")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if opts.Addr != "redis.fp-infra.svc.cluster.local:6379" || opts.Password != "" || opts.TLSConfig != nil {
			t.Fatalf("got Addr=%q Password set=%t TLS=%t", opts.Addr, opts.Password != "", opts.TLSConfig != nil)
		}
	})
	t.Run("redis URL carries the password", func(t *testing.T) {
		opts, err := redisOptions("redis://:s3cret@localhost:6379/2")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if opts.Addr != "localhost:6379" || opts.Password != "s3cret" || opts.DB != 2 {
			t.Fatalf("got Addr=%q DB=%d password match=%t", opts.Addr, opts.DB, opts.Password == "s3cret")
		}
	})
	t.Run("rediss URL enables TLS", func(t *testing.T) {
		opts, err := redisOptions("rediss://:tok@master.example.cache.amazonaws.com:6379")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if opts.TLSConfig == nil {
			t.Fatal("expected TLS config for rediss://")
		}
	})
	// The ElastiCache module generates the token from [A-Za-z0-9] plus
	// "!&#$^<>-". Go's net/url rejects #, ^, < and > raw in the userinfo, so
	// Terraform writes the URL with urlencode(token). This pins both halves:
	// the raw form fails, and the encoded form round-trips to the exact token.
	t.Run("ElastiCache token must be percent-encoded in the URL", func(t *testing.T) {
		const token = "Ab1!&#$^<>-z9Ab1!&#$^<>-z9"
		const host = "master.example.cache.amazonaws.com:6379"
		if _, err := redisOptions("rediss://:" + token + "@" + host); err == nil {
			t.Fatal("expected the raw token to be rejected by net/url")
		}
		opts, err := redisOptions("rediss://:" + url.QueryEscape(token) + "@" + host)
		if err != nil {
			t.Fatalf("encoded token should parse: %v", err)
		}
		if opts.Password != token || opts.Addr != host || opts.TLSConfig == nil {
			t.Fatalf("got Addr=%q password match=%t TLS=%t", opts.Addr, opts.Password == token, opts.TLSConfig != nil)
		}
	})
	t.Run("bad scheme is an error", func(t *testing.T) {
		if _, err := redisOptions("http://localhost:6379"); err == nil {
			t.Fatal("expected an error for a non-redis scheme")
		}
	})
}
