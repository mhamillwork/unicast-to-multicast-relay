package acl

import (
	"net"
	"testing"
)

func TestParseAndAllow(t *testing.T) {
	cases := []struct {
		name    string
		sources []string
		ip      string
		want    bool
		bad     bool
	}{
		{name: "empty allows", ip: "8.8.8.8", want: true},
		{name: "host", sources: []string{"10.1.2.3"}, ip: "10.1.2.3", want: true},
		{name: "host miss", sources: []string{"10.1.2.3"}, ip: "10.1.2.4", want: false},
		{name: "cidr hit", sources: []string{"10.0.0.0/8"}, ip: "10.9.1.1", want: true},
		{name: "cidr miss", sources: []string{"10.0.0.0/8"}, ip: "11.0.0.1", want: false},
		{name: "second cidr", sources: []string{"192.168.1.0/24", "172.16.0.0/12"}, ip: "172.16.5.5", want: true},
		{name: "ipv6 rejected", ip: "::1", sources: []string{"10.0.0.0/8"}, want: false},
		{name: "bad cidr", sources: []string{"10.0.0.0/33"}, bad: true},
		{name: "bad token", sources: []string{"not-an-ip"}, bad: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			list, err := New(tc.sources)
			if tc.bad {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := list.Allow(net.ParseIP(tc.ip)); got != tc.want {
				t.Fatalf("Allow(%s)=%v want %v", tc.ip, got, tc.want)
			}
		})
	}
}

func TestCanonical(t *testing.T) {
	got, err := Canonical("10.1.2.3")
	if err != nil {
		t.Fatal(err)
	}
	if got != "10.1.2.3/32" {
		t.Fatalf("got %s", got)
	}
	got, err = Canonical("10.1.2.0/24")
	if err != nil {
		t.Fatal(err)
	}
	if got != "10.1.2.0/24" {
		t.Fatalf("got %s", got)
	}
}
