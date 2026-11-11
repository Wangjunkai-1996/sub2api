package kkaiattribution

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOriginSetMatchesOnlyExactSchemeHostAndPort(t *testing.T) {
	origins, err := newOriginSet([]string{
		"https://guard.internal.example:8443",
		"http://127.0.0.1:8080",
		"https://[2001:db8::1]:9443",
		"https://default-port.example",
	})
	require.NoError(t, err)

	tests := []struct {
		name    string
		target  string
		allowed bool
	}{
		{name: "exact dns", target: "https://guard.internal.example:8443/v1/chat", allowed: true},
		{name: "dns case and trailing dot", target: "https://GUARD.INTERNAL.EXAMPLE.:8443/v1/chat", allowed: true},
		{name: "dns suffix impersonation", target: "https://guard.internal.example.attacker.test:8443/v1/chat", allowed: false},
		{name: "dns subdomain", target: "https://api.guard.internal.example:8443/v1/chat", allowed: false},
		{name: "wrong scheme", target: "http://guard.internal.example:8443/v1/chat", allowed: false},
		{name: "wrong port", target: "https://guard.internal.example:443/v1/chat", allowed: false},
		{name: "userinfo", target: "https://guard.internal.example@attacker.test:8443/v1/chat", allowed: false},
		{name: "exact ipv4", target: "http://127.0.0.1:8080/v1/chat", allowed: true},
		{name: "different ipv4", target: "http://127.0.0.2:8080/v1/chat", allowed: false},
		{name: "integer ipv4 alias", target: "http://2130706433:8080/v1/chat", allowed: false},
		{name: "canonical ipv6", target: "https://[2001:0db8:0:0:0:0:0:1]:9443/v1/chat", allowed: true},
		{name: "different ipv6", target: "https://[2001:db8::2]:9443/v1/chat", allowed: false},
		{name: "ipv6 zone", target: "https://[fe80::1%25eth0]:9443/v1/chat", allowed: false},
		{name: "implicit default port", target: "https://default-port.example/v1/chat", allowed: true},
		{name: "explicit default port", target: "https://default-port.example:443/v1/chat", allowed: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, allowed := origins.match(test.target)
			require.Equal(t, test.allowed, allowed)
		})
	}
}

func TestOriginSetRejectsAmbiguousAllowlistEntries(t *testing.T) {
	invalid := []string{
		"guard.internal.example:8443",
		"ftp://guard.internal.example:8443",
		"https://user@guard.internal.example:8443",
		"https://guard.internal.example:8443/path",
		"https://guard.internal.example:8443?query=1",
		"https://guard.internal.example:8443#fragment",
		"https://guard_internal.example:8443",
		"https://[fe80::1%25eth0]:8443",
		"https://guard.internal.example:70000",
	}

	for _, entry := range invalid {
		t.Run(entry, func(t *testing.T) {
			_, err := newOriginSet([]string{entry})
			require.ErrorIs(t, err, ErrInvalidConfiguration)
		})
	}
}
