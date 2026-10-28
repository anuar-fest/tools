package httpserver

import (
	"net"
	"net/http"
	"net/netip"
	"strings"
)

// RealIP replaces RemoteAddr with the client address from X-Forwarded-For
// or X-Real-IP, but only when the peer is one of the trusted proxies: a
// forwarding header from anyone else is caller-supplied and would let a
// scanner pick its own address for rate limits and logs. The rightmost
// untrusted entry of X-Forwarded-For wins. With no trusted prefixes the
// middleware is a no-op.
func RealIP(trusted []netip.Prefix) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if len(trusted) == 0 {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			peer, ok := parseAddr(r.RemoteAddr)
			if !ok || !inPrefixes(peer, trusted) {
				next.ServeHTTP(w, r)
				return
			}
			client := forwardedClient(r.Header.Values("X-Forwarded-For"), trusted)
			if client == "" {
				if ip, ok := parseAddr(r.Header.Get("X-Real-IP")); ok {
					client = ip.String()
				}
			}
			if client == "" {
				next.ServeHTTP(w, r)
				return
			}
			r2 := new(http.Request)
			*r2 = *r
			r2.RemoteAddr = client
			next.ServeHTTP(w, r2)
		})
	}
}

func forwardedClient(headers []string, trusted []netip.Prefix) string {
	hops := make([]string, 0, len(headers))
	for _, h := range headers {
		hops = append(hops, strings.Split(h, ",")...)
	}
	for i := len(hops) - 1; i >= 0; i-- {
		ip, ok := parseAddr(strings.TrimSpace(hops[i]))
		if !ok {
			return "" // a malformed chain is not trusted at all
		}
		if !inPrefixes(ip, trusted) {
			return ip.String()
		}
	}
	return ""
}

func parseAddr(s string) (netip.Addr, bool) {
	if host, _, err := net.SplitHostPort(s); err == nil {
		s = host
	}
	ip, err := netip.ParseAddr(strings.Trim(s, "[]"))
	if err != nil {
		return netip.Addr{}, false
	}
	return ip.Unmap(), true
}

func inPrefixes(ip netip.Addr, prefixes []netip.Prefix) bool {
	for _, p := range prefixes {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// ClientIP is the host part of RemoteAddr (after RealIP, the client).
func ClientIP(r *http.Request) string {
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

// ParsePrefixes parses CIDRs and bare addresses ("10.0.0.0/8", "::1").
func ParsePrefixes(cidrs ...string) ([]netip.Prefix, error) {
	out := make([]netip.Prefix, 0, len(cidrs))
	for _, c := range cidrs {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		if p, err := netip.ParsePrefix(c); err == nil {
			out = append(out, p.Masked())
			continue
		}
		ip, err := netip.ParseAddr(c)
		if err != nil {
			return nil, err
		}
		out = append(out, netip.PrefixFrom(ip, ip.BitLen()))
	}
	return out, nil
}

// PrivateNetworks are the RFC 1918, loopback and unique-local ranges: the
// usual pod and node networks in front of a service inside a cluster.
func PrivateNetworks() []netip.Prefix {
	ps, _ := ParsePrefixes("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "127.0.0.0/8", "::1/128", "fc00::/7") // constants parse
	return ps
}
