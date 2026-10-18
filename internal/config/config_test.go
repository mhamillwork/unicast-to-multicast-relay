package config

import (
	"os"
	"strings"
	"testing"
)

func TestParseSpecExampleOnLoopback(t *testing.T) {
	in := []byte(strings.ReplaceAll(specExample, "eth1", "lo"))
	cfg, err := Parse(in)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.EgressInterface != "lo" {
		t.Fatalf("iface %s", cfg.EgressInterface)
	}
	if cfg.MetricsListen != "0.0.0.0:9100" {
		t.Fatalf("metrics %s", cfg.MetricsListen)
	}
	if cfg.LogLevel != "info" || cfg.BatchSize != DefaultBatchSize {
		t.Fatalf("defaults log=%s batch=%d", cfg.LogLevel, cfg.BatchSize)
	}
	if len(cfg.Streams) != 2 || len(cfg.ReverseStreams) != 1 {
		t.Fatalf("streams %+v reverse %+v", cfg.Streams, cfg.ReverseStreams)
	}
	feed := cfg.Streams[0]
	if feed.Name != "market-feed-a" || feed.TTL != 4 || feed.Loop || feed.Group != "239.1.1.10" {
		t.Fatalf("feed %+v", feed)
	}
	if len(feed.AllowedSources) != 1 || feed.AllowedSources[0] != "10.0.0.0/8" {
		t.Fatalf("acl %+v", feed.AllowedSources)
	}
	disc := cfg.Streams[1]
	if disc.TTL != 1 || disc.Port != 1900 || disc.Listen != "0.0.0.0:5001" {
		t.Fatalf("discovery %+v", disc)
	}
	rev := cfg.ReverseStreams[0]
	if rev.Interface != "lo" || rev.JoinGroup != "239.2.2.2" || rev.Targets[0] != "10.96.12.34:6000" {
		t.Fatalf("reverse %+v", rev)
	}
	if cfg.MaxDatagramBytes < 9000 {
		t.Fatalf("max datagram %d", cfg.MaxDatagramBytes)
	}
}

func TestValidationErrors(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "bad cidr",
			yaml: `
egress_interface: lo
streams:
  - name: a
    listen: 0.0.0.0:5000
    group: 239.1.1.1
    port: 5000
    allowed_sources: ["10.0.0.0/33"]
`,
			want: "allowed_sources",
		},
		{
			name: "port conflict",
			yaml: `
egress_interface: lo
streams:
  - name: a
    listen: 0.0.0.0:5000
    group: 239.1.1.1
    port: 5000
  - name: b
    listen: 127.0.0.1:5000
    group: 239.1.1.2
    port: 5001
`,
			want: "port conflict",
		},
		{
			name: "missing interface",
			yaml: `
egress_interface: no-such-iface-mcast
streams:
  - name: a
    listen: 0.0.0.0:5000
    group: 239.1.1.1
    port: 5000
`,
			want: "no-such-iface-mcast",
		},
		{
			name: "group not multicast",
			yaml: `
egress_interface: lo
streams:
  - name: a
    listen: 0.0.0.0:5000
    group: 10.1.1.1
    port: 5000
`,
			want: "multicast range",
		},
		{
			name: "ttl",
			yaml: `
egress_interface: lo
streams:
  - name: a
    listen: 0.0.0.0:5000
    group: 239.1.1.1
    port: 5000
    ttl: 300
`,
			want: "ttl",
		},
		{
			name: "duplicate name",
			yaml: `
egress_interface: lo
streams:
  - name: a
    listen: 0.0.0.0:5000
    group: 239.1.1.1
    port: 5000
reverse_streams:
  - name: a
    join_group: 239.2.2.2
    port: 6000
    targets: ["10.0.0.1:6000"]
`,
			want: "duplicate name",
		},
		{
			name: "preserve and fixed port",
			yaml: `
egress_interface: lo
streams:
  - name: a
    listen: 0.0.0.0:5000
    group: 239.1.1.1
    port: 5000
    source_port: 4000
    preserve_source_port: true
`,
			want: "mutually exclusive",
		},
		{
			name: "unknown field",
			yaml: `
egress_interface: lo
not_a_field: true
`,
			want: "not_a_field",
		},
		{
			name: "reverse target multicast",
			yaml: `
egress_interface: lo
reverse_streams:
  - name: a
    join_group: 239.2.2.2
    port: 6000
    targets: ["239.1.1.1:6000"]
`,
			want: "unicast",
		},
		{
			name: "explicit ttl zero kept and still valid",
			yaml: `
egress_interface: lo
streams:
  - name: a
    listen: 0.0.0.0:5000
    group: 239.1.1.1
    port: 5000
    ttl: 0
`,
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := Parse([]byte(tc.yaml))
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				if cfg.Streams[0].TTL != 0 {
					t.Fatalf("ttl %d", cfg.Streams[0].TTL)
				}
				return
			}
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}

func TestEnvOverrideAndExpansion(t *testing.T) {
	t.Setenv("MCAST_EGRESS_INTERFACE", "lo")
	t.Setenv("MCAST_LOG_LEVEL", "debug")
	t.Setenv("FEED_PORT", "5151")
	t.Setenv("MCAST_BATCH_SIZE", "32")
	cfg, err := Parse([]byte(`
egress_interface: no-such-iface-mcast
metrics_listen: ${MCAST_METRICS_ADDR:-127.0.0.1:19100}
streams:
  - name: a
    listen: 127.0.0.1:${FEED_PORT}
    group: 239.1.1.1
    port: 5151
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.EgressInterface != "lo" || cfg.LogLevel != "debug" || cfg.BatchSize != 32 {
		t.Fatalf("%+v", cfg)
	}
	if cfg.MetricsListen != "127.0.0.1:19100" || cfg.Streams[0].Listen != "127.0.0.1:5151" {
		t.Fatalf("%+v", cfg)
	}
}

func TestUnresolvedEnv(t *testing.T) {
	os.Unsetenv("DEFINITELY_MISSING_MCAST_VAR")
	_, err := Parse([]byte(`
egress_interface: ${DEFINITELY_MISSING_MCAST_VAR}
`))
	if err == nil || !strings.Contains(err.Error(), "DEFINITELY_MISSING_MCAST_VAR") {
		t.Fatalf("got %v", err)
	}
}

func TestSignatureStableForSameStream(t *testing.T) {
	cfg, err := Parse([]byte(`
egress_interface: lo
streams:
  - name: a
    listen: 0.0.0.0:5000
    group: 239.1.1.1
    port: 5000
    allowed_sources: ["10.2.0.0/16", "10.1.0.0/16"]
`))
	if err != nil {
		t.Fatal(err)
	}
	again, err := Parse([]byte(`
egress_interface: lo
streams:
  - name: a
    listen: 0.0.0.0:5000
    group: 239.1.1.1
    port: 5000
    allowed_sources: ["10.1.0.0/16", "10.2.0.0/16"]
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ForwardSignature(cfg.Streams[0]) != again.ForwardSignature(again.Streams[0]) {
		t.Fatal("source order changed the signature")
	}
	bumped := *cfg
	bumped.Streams = append([]Stream(nil), cfg.Streams...)
	bumped.Streams[0].TTL = 8
	if cfg.ForwardSignature(cfg.Streams[0]) == bumped.ForwardSignature(bumped.Streams[0]) {
		t.Fatal("ttl change was ignored")
	}
}

const specExample = `
egress_interface: eth1
metrics_listen: 0.0.0.0:9100
streams:
  - name: market-feed-a
    listen: 0.0.0.0:5000
    group: 239.1.1.10
    port: 5000
    ttl: 4
    allowed_sources: ["10.0.0.0/8"]
  - name: discovery
    listen: 0.0.0.0:5001
    group: 239.255.255.250
    port: 1900
    ttl: 1
reverse_streams:
  - name: ext-status
    join_group: 239.2.2.2
    port: 6000
    interface: eth1
    targets: ["10.96.12.34:6000"]
`
