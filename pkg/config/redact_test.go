package config

import "testing"

func TestRedactURL(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"redis password only", "redis://:s3cret@localhost:6379", "redis://:xxxxx@localhost:6379"},
		{"rediss with token", "rediss://:tok3n@master.example.cache.amazonaws.com:6379", "rediss://:xxxxx@master.example.cache.amazonaws.com:6379"},
		{"nats user and password", "nats://fp:s3cret@localhost:4222", "nats://fp:xxxxx@localhost:4222"},
		{"no credentials", "nats://nats.fp-infra.svc.cluster.local:4222", "nats://nats.fp-infra.svc.cluster.local:4222"},
		{"bare host:port", "localhost:6379", "localhost:6379"},
		{"empty", "", ""},
		{"unparseable is not echoed", "redis://:pa ss@%zz", "<unparseable url>"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RedactURL(tt.in); got != tt.want {
				t.Errorf("RedactURL(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
