package main

import "testing"

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
	t.Run("bad scheme is an error", func(t *testing.T) {
		if _, err := redisOptions("http://localhost:6379"); err == nil {
			t.Fatal("expected an error for a non-redis scheme")
		}
	})
}
