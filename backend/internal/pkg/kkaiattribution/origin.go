package kkaiattribution

import (
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"

	"golang.org/x/net/idna"
)

type exactOrigin struct {
	scheme string
	host   string
	port   string
}

type parsedTarget struct {
	origin        exactOrigin
	requestTarget string
}

type originSet map[exactOrigin]struct{}

func newOriginSet(entries []string) (originSet, error) {
	if len(entries) == 0 {
		return nil, ErrInvalidConfiguration
	}
	result := make(originSet, len(entries))
	for _, entry := range entries {
		target, err := parseTarget(entry, true)
		if err != nil {
			return nil, fmt.Errorf("%w: %q", ErrInvalidConfiguration, entry)
		}
		result[target.origin] = struct{}{}
	}
	return result, nil
}

func (s originSet) match(rawURL string) (parsedTarget, bool) {
	target, err := parseTarget(rawURL, false)
	if err != nil {
		return parsedTarget{}, false
	}
	_, ok := s[target.origin]
	return target, ok
}

func parseTarget(rawURL string, originOnly bool) (parsedTarget, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed.Opaque != "" || parsed.User != nil || parsed.Host == "" || parsed.Fragment != "" {
		return parsedTarget{}, ErrInvalidOrigin
	}
	scheme := strings.ToLower(parsed.Scheme)
	if !supportedScheme(scheme) {
		return parsedTarget{}, ErrInvalidOrigin
	}
	if originOnly && ((parsed.EscapedPath() != "" && parsed.EscapedPath() != "/") || parsed.RawQuery != "") {
		return parsedTarget{}, ErrInvalidOrigin
	}
	if strings.HasSuffix(parsed.Host, ":") {
		return parsedTarget{}, ErrInvalidOrigin
	}
	host, err := normalizeHost(parsed.Hostname())
	if err != nil {
		return parsedTarget{}, err
	}
	port, err := normalizePort(scheme, parsed.Port())
	if err != nil {
		return parsedTarget{}, err
	}
	requestTarget := parsed.EscapedPath()
	if requestTarget == "" {
		requestTarget = "/"
	}
	if parsed.ForceQuery || parsed.RawQuery != "" {
		requestTarget += "?" + parsed.RawQuery
	}
	return parsedTarget{
		origin:        exactOrigin{scheme: scheme, host: host, port: port},
		requestTarget: requestTarget,
	}, nil
}

func normalizeHost(host string) (string, error) {
	host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	if host == "" || strings.Contains(host, "%") {
		return "", ErrInvalidOrigin
	}
	if address, err := netip.ParseAddr(host); err == nil {
		return address.String(), nil
	}
	if strings.Contains(host, ":") {
		return "", ErrInvalidOrigin
	}
	ascii, err := idna.Lookup.ToASCII(host)
	if err != nil || !validDNSName(ascii) {
		return "", ErrInvalidOrigin
	}
	return strings.ToLower(ascii), nil
}

func validDNSName(host string) bool {
	if len(host) == 0 || len(host) > 253 {
		return false
	}
	allDigits := true
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, char := range label {
			if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' {
				if char < '0' || char > '9' {
					allDigits = false
				}
				continue
			}
			if char != '-' {
				return false
			}
		}
	}
	return !allDigits
}

func normalizePort(scheme string, rawPort string) (string, error) {
	if rawPort == "" {
		switch scheme {
		case "http", "ws":
			return "80", nil
		case "https", "wss":
			return "443", nil
		default:
			return "", ErrInvalidOrigin
		}
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil || port < 1 || port > 65535 {
		return "", ErrInvalidOrigin
	}
	return strconv.Itoa(port), nil
}

func supportedScheme(scheme string) bool {
	switch scheme {
	case "http", "https", "ws", "wss":
		return true
	default:
		return false
	}
}

func (o exactOrigin) String() string {
	return o.scheme + "://" + net.JoinHostPort(o.host, o.port)
}

func sameAuthority(target exactOrigin, authority string) bool {
	parsed, err := parseTarget(target.scheme+"://"+strings.TrimSpace(authority), true)
	return err == nil && parsed.origin == target
}
