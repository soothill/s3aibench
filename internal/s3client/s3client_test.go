package s3client

import "testing"

func TestJoinPrefix(t *testing.T) {
	cases := []struct {
		prefix, key, want string
	}{
		{"", "k", "k"},
		{"", "/k", "k"},
		{"p", "k", "p/k"},
		{"p/", "k", "p/k"},
		{"p/", "/k", "p/k"},
		{"p/", "p/k", "p/k"}, // already prefixed, not doubled
	}
	for _, c := range cases {
		if got := joinPrefix(c.prefix, c.key); got != c.want {
			t.Errorf("joinPrefix(%q,%q)=%q want %q", c.prefix, c.key, got, c.want)
		}
	}
}

func TestNewTransportDefaults(t *testing.T) {
	tr := NewTransport(TransportOptions{})
	if tr.MaxIdleConnsPerHost != 4096 {
		t.Fatalf("default max idle: %d", tr.MaxIdleConnsPerHost)
	}
	if tr.ForceAttemptHTTP2 {
		t.Fatal("HTTP2 should be off by default")
	}
	if tr.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("TLS verify should be on by default")
	}
}

func TestNewTransportOverrides(t *testing.T) {
	tr := NewTransport(TransportOptions{
		MaxIdleConnsPerHost: 128,
		TLSSkipVerify:       true,
		ForceHTTP2:          true,
	})
	if tr.MaxIdleConnsPerHost != 128 {
		t.Fatalf("max idle: %d", tr.MaxIdleConnsPerHost)
	}
	if !tr.ForceAttemptHTTP2 || !tr.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("overrides ignored")
	}
}
