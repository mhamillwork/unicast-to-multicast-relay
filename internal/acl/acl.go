// Package acl matches UDP source addresses against an allow list of
// IPv4 addresses and CIDRs. An empty list allows every source.
package acl

import (
	"fmt"
	"net"
)

// List is a compiled source allow-list.
type List struct {
	allowAll bool
	nets     []*net.IPNet
}

// New compiles CIDRs and host addresses. A host without a mask is a /32.
// An empty input allows every IPv4 source.
func New(sources []string) (*List, error) {
	if len(sources) == 0 {
		return &List{allowAll: true}, nil
	}
	nets := make([]*net.IPNet, 0, len(sources))
	for _, src := range sources {
		n, err := Parse(src)
		if err != nil {
			return nil, err
		}
		nets = append(nets, n)
	}
	return &List{nets: nets}, nil
}

// Parse parses one IPv4 address or CIDR. Hosts become /32s.
func Parse(src string) (*net.IPNet, error) {
	if src == "" {
		return nil, fmt.Errorf("empty source")
	}
	if _, ipnet, err := net.ParseCIDR(src); err == nil {
		if ipnet.IP.To4() == nil {
			return nil, fmt.Errorf("source %q is not IPv4", src)
		}
		ipnet.IP = ipnet.IP.To4()
		return ipnet, nil
	}
	ip := net.ParseIP(src)
	if ip == nil || ip.To4() == nil {
		return nil, fmt.Errorf("source %q is not an IPv4 address or CIDR", src)
	}
	ip = ip.To4()
	return &net.IPNet{IP: ip, Mask: net.CIDRMask(32, 32)}, nil
}

// Allow reports whether ip may send to the stream.
func (l *List) Allow(ip net.IP) bool {
	if l == nil || l.allowAll {
		return true
	}
	v4 := ip.To4()
	if v4 == nil {
		return false
	}
	for _, n := range l.nets {
		if n.Contains(v4) {
			return true
		}
	}
	return false
}

// Canonical returns the network form of src ("10.1.0.1/32", "10.0.0.0/8").
func Canonical(src string) (string, error) {
	n, err := Parse(src)
	if err != nil {
		return "", err
	}
	return n.String(), nil
}
