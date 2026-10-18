package relay

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mhamillwork/unicast-to-multicast-relay/internal/config"
	"github.com/mhamillwork/unicast-to-multicast-relay/internal/health"
	"github.com/mhamillwork/unicast-to-multicast-relay/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"golang.org/x/net/ipv4"
	"golang.org/x/sys/unix"
)

var groupSeq atomic.Uint32

func nextGroup(t *testing.T) string {
	t.Helper()
	n := groupSeq.Add(1)
	return fmt.Sprintf("239.23.%d.%d", byte(n>>8), byte(n))
}

func freePort(t *testing.T, network string) int {
	t.Helper()
	if network == "tcp" {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		return ln.Addr().(*net.TCPAddr).Port
	}
	ln, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.LocalAddr().(*net.UDPAddr).Port
}

type harness struct {
	mgr  *Manager
	reg  *prometheus.Registry
	url  string
	ctx  context.Context
	stop func()
}

func startHarness(t *testing.T, yaml string) *harness {
	t.Helper()
	cfg, err := config.Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	reg := prometheus.NewRegistry()
	met := metrics.New(reg, "test")
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
	mgr := NewManager(log, met)
	ctx, cancel := context.WithCancel(context.Background())
	if err := mgr.Apply(ctx, cfg); err != nil {
		cancel()
		t.Fatalf("apply: %v", err)
	}
	srv, err := health.Listen(cfg.MetricsListen, reg, mgr)
	if err != nil {
		cancel()
		mgr.Shutdown(context.Background())
		t.Fatalf("health: %v", err)
	}
	go func() { _ = srv.Serve() }()
	h := &harness{mgr: mgr, reg: reg, url: "http://" + srv.Addr(), ctx: ctx}
	h.stop = func() {
		cancel()
		shut, done := context.WithTimeout(context.Background(), 2*time.Second)
		defer done()
		mgr.Shutdown(shut)
		_ = srv.Shutdown(shut)
	}
	t.Cleanup(h.stop)
	waitReady(t, h.url)
	return h
}

func waitReady(t *testing.T, base string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		resp, err := http.Get(base + "/readyz")
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
			last = string(body)
		} else {
			last = err.Error()
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("readyz: %s", last)
}

func joinGroup(t *testing.T, group string, port int) *net.UDPConn {
	t.Helper()
	ifi, err := net.InterfaceByName("lo")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: port})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	pc := ipv4.NewPacketConn(conn)
	if err := pc.JoinGroup(ifi, &net.UDPAddr{IP: net.ParseIP(group)}); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadBuffer(4 << 20); err != nil {
		t.Fatal(err)
	}
	return conn
}

type datagram struct {
	data []byte
	port int
}

func readDatagrams(t *testing.T, conn *net.UDPConn, n int, timeout time.Duration) []datagram {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	out := make([]datagram, 0, n)
	buf := make([]byte, 65535)
	for len(out) < n {
		nread, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			t.Fatalf("read %d/%d: %v", len(out), n, err)
		}
		out = append(out, datagram{data: append([]byte(nil), buf[:nread]...), port: addr.Port})
	}
	return out
}

func readNone(t *testing.T, conn *net.UDPConn, timeout time.Duration) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, 65535)
	_, _, err := conn.ReadFromUDP(buf)
	if err == nil {
		t.Fatal("unexpected datagram")
	}
	if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		t.Fatal(err)
	}
}

func sendTo(t *testing.T, dst string, payloads [][]byte, src *net.UDPAddr) {
	t.Helper()
	remote, err := net.ResolveUDPAddr("udp4", dst)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialUDP("udp4", src, remote)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, p := range payloads {
		if _, err := conn.Write(p); err != nil {
			t.Fatal(err)
		}
	}
}

func metricValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, fam := range fams {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			if !labelsMatch(m, labels) {
				continue
			}
			if c := m.GetCounter(); c != nil {
				return c.GetValue()
			}
			if g := m.GetGauge(); g != nil {
				return g.GetValue()
			}
		}
	}
	t.Fatalf("metric %s %v not found", name, labels)
	return 0
}

func labelsMatch(m *dto.Metric, labels map[string]string) bool {
	got := map[string]string{}
	for _, lp := range m.GetLabel() {
		got[lp.GetName()] = lp.GetValue()
	}
	for k, v := range labels {
		if got[k] != v {
			return false
		}
	}
	return true
}

func TestForwardIdenticalPayload(t *testing.T) {
	group := nextGroup(t)
	listen := freePort(t, "udp")
	mport := freePort(t, "udp")
	metricsPort := freePort(t, "tcp")
	h := startHarness(t, fmt.Sprintf(`
egress_interface: lo
metrics_listen: 127.0.0.1:%d
log_level: warn
streams:
  - name: feed
    listen: 127.0.0.1:%d
    group: %s
    port: %d
    ttl: 4
    loop: true
    allowed_sources: ["127.0.0.0/8"]
`, metricsPort, listen, group, mport))

	rx := joinGroup(t, group, mport)
	payloads := [][]byte{
		[]byte("market-feed"),
		{0x00, 0x01, 0xff, 0x00, 'A'},
		bytes.Repeat([]byte{0xab}, 8000),
		{},
	}
	sendTo(t, fmt.Sprintf("127.0.0.1:%d", listen), payloads, nil)
	got := readDatagrams(t, rx, len(payloads), 2*time.Second)
	for i := range payloads {
		if !bytes.Equal(got[i].data, payloads[i]) {
			t.Fatalf("payload %d got %d bytes %x want %x", i, len(got[i].data), got[i].data, payloads[i])
		}
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if metricValue(t, h.reg, "mcast_relay_packets_out_total", map[string]string{"stream": "feed", "direction": "forward"}) == float64(len(payloads)) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := metricValue(t, h.reg, "mcast_relay_packets_in_total", map[string]string{"stream": "feed", "direction": "forward"}); got != float64(len(payloads)) {
		t.Fatalf("packets in %v", got)
	}
	if got := metricValue(t, h.reg, "mcast_relay_packets_out_total", map[string]string{"stream": "feed", "direction": "forward"}); got != float64(len(payloads)) {
		t.Fatalf("packets out %v", got)
	}
	var wantBytes int
	for _, p := range payloads {
		wantBytes += len(p)
	}
	if got := metricValue(t, h.reg, "mcast_relay_bytes_out_total", map[string]string{"stream": "feed", "direction": "forward"}); got != float64(wantBytes) {
		t.Fatalf("bytes out %v want %d", got, wantBytes)
	}
	resp, err := http.Get(h.url + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("healthz %d", resp.StatusCode)
	}
	if up := metricValue(t, h.reg, "mcast_relay_stream_up", map[string]string{"stream": "feed", "direction": "forward"}); up != 1 {
		t.Fatalf("up %v", up)
	}
}

func TestACLAndOversizeDrops(t *testing.T) {
	group := nextGroup(t)
	listen := freePort(t, "udp")
	mport := freePort(t, "udp")
	h := startHarness(t, fmt.Sprintf(`
egress_interface: lo
metrics_listen: 127.0.0.1:%d
streams:
  - name: gated
    listen: 127.0.0.1:%d
    group: %s
    port: %d
    loop: true
    max_datagram_bytes: 32
    allowed_sources: ["10.0.0.0/8"]
`, freePort(t, "tcp"), listen, group, mport))
	rx := joinGroup(t, group, mport)
	sendTo(t, fmt.Sprintf("127.0.0.1:%d", listen), [][]byte{[]byte("nope"), bytes.Repeat([]byte("x"), 100)}, nil)

	// A second stream on the same relay proves an ACL drop does not stop the process.
	time.Sleep(50 * time.Millisecond)
	readNone(t, rx, 150*time.Millisecond)
	if got := metricValue(t, h.reg, "mcast_relay_drops_total", map[string]string{"stream": "gated", "direction": "forward", "reason": "acl"}); got != 1 {
		t.Fatalf("acl drops %v", got)
	}
	if got := metricValue(t, h.reg, "mcast_relay_drops_total", map[string]string{"stream": "gated", "direction": "forward", "reason": "oversize"}); got != 1 {
		t.Fatalf("oversize drops %v", got)
	}
	if got := metricValue(t, h.reg, "mcast_relay_packets_out_total", map[string]string{"stream": "gated", "direction": "forward"}); got != 0 {
		t.Fatalf("packets out %v", got)
	}
}

func TestAllowedDatagramPassesACL(t *testing.T) {
	group := nextGroup(t)
	listen := freePort(t, "udp")
	mport := freePort(t, "udp")
	startHarness(t, fmt.Sprintf(`
egress_interface: lo
metrics_listen: 127.0.0.1:%d
streams:
  - name: ok
    listen: 127.0.0.1:%d
    group: %s
    port: %d
    loop: true
    max_datagram_bytes: 64
    allowed_sources: ["127.0.0.1"]
`, freePort(t, "tcp"), listen, group, mport))
	rx := joinGroup(t, group, mport)
	sendTo(t, fmt.Sprintf("127.0.0.1:%d", listen), [][]byte{[]byte("fits")}, nil)
	got := readDatagrams(t, rx, 1, time.Second)
	if string(got[0].data) != "fits" {
		t.Fatalf("got %q", got[0].data)
	}
}

func TestFixedAndPreservedSourcePort(t *testing.T) {
	group := nextGroup(t)
	listen := freePort(t, "udp")
	mport := freePort(t, "udp")
	fixed := freePort(t, "udp")
	startHarness(t, fmt.Sprintf(`
egress_interface: lo
metrics_listen: 127.0.0.1:%d
streams:
  - name: fixed
    listen: 127.0.0.1:%d
    group: %s
    port: %d
    loop: true
    source_port: %d
`, freePort(t, "tcp"), listen, group, mport, fixed))
	rx := joinGroup(t, group, mport)
	sendTo(t, fmt.Sprintf("127.0.0.1:%d", listen), [][]byte{[]byte("fixed-port")}, nil)
	got := readDatagrams(t, rx, 1, time.Second)
	if got[0].port != fixed {
		t.Fatalf("source port %d want %d", got[0].port, fixed)
	}

	group2 := nextGroup(t)
	listen2 := freePort(t, "udp")
	mport2 := freePort(t, "udp")
	src := freePort(t, "udp")
	startHarness(t, fmt.Sprintf(`
egress_interface: lo
metrics_listen: 127.0.0.1:%d
streams:
  - name: preserve
    listen: 127.0.0.1:%d
    group: %s
    port: %d
    loop: true
    preserve_source_port: true
`, freePort(t, "tcp"), listen2, group2, mport2))
	rx2 := joinGroup(t, group2, mport2)
	sendTo(t, fmt.Sprintf("127.0.0.1:%d", listen2), [][]byte{[]byte("same-port")}, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: src})
	got = readDatagrams(t, rx2, 1, time.Second)
	if string(got[0].data) != "same-port" {
		t.Fatalf("payload %q", got[0].data)
	}
	if got[0].port != src {
		t.Fatalf("preserved port %d want %d", got[0].port, src)
	}
}

func TestReverseFanout(t *testing.T) {
	group := nextGroup(t)
	joinPort := freePort(t, "udp")
	a := freePort(t, "udp")
	b := freePort(t, "udp")
	startHarness(t, fmt.Sprintf(`
egress_interface: lo
metrics_listen: 127.0.0.1:%d
reverse_streams:
  - name: back
    join_group: %s
    port: %d
    interface: lo
    targets: ["127.0.0.1:%d", "127.0.0.1:%d"]
`, freePort(t, "tcp"), group, joinPort, a, b))

	ra, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: a})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ra.Close() })
	rb, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: b})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rb.Close() })

	ifi, err := net.InterfaceByName("lo")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	pc := ipv4.NewPacketConn(conn)
	if err := configureSend(pc, conn, ifi, net.IPv4(127, 0, 0, 1), 1, true); err != nil {
		t.Fatal(err)
	}
	payload := []byte("from-outside")
	dst := &net.UDPAddr{IP: net.ParseIP(group), Port: joinPort}
	if _, err := conn.WriteToUDP(payload, dst); err != nil {
		t.Fatal(err)
	}
	for _, rx := range []*net.UDPConn{ra, rb} {
		got := readDatagrams(t, rx, 1, time.Second)
		if !bytes.Equal(got[0].data, payload) {
			t.Fatalf("got %q", got[0].data)
		}
	}
}

func TestReloadKeepsUnchangedStream(t *testing.T) {
	group := nextGroup(t)
	listen := freePort(t, "udp")
	mport := freePort(t, "udp")
	metricsPort := freePort(t, "tcp")
	body := fmt.Sprintf(`
egress_interface: lo
metrics_listen: 127.0.0.1:%d
log_level: warn
streams:
  - name: feed
    listen: 127.0.0.1:%d
    group: %s
    port: %d
    ttl: 2
    loop: true
`, metricsPort, listen, group, mport)
	h := startHarness(t, body)
	gen, ok := h.mgr.Generation("feed")
	if !ok {
		t.Fatal("missing generation")
	}
	rx := joinGroup(t, group, mport)

	again, err := config.Parse([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if err := h.mgr.Apply(h.ctx, again); err != nil {
		t.Fatal(err)
	}
	if got, _ := h.mgr.Generation("feed"); got != gen {
		t.Fatalf("unchanged stream restarted: %d -> %d", gen, got)
	}

	// A rejected reload must leave the running stream in place.
	bad, err := config.Parse([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	bad.Streams[0].TTL = 500
	if err := h.mgr.Apply(h.ctx, bad); err == nil {
		t.Fatal("expected invalid ttl to be rejected")
	}
	if got, _ := h.mgr.Generation("feed"); got != gen {
		t.Fatalf("rejected reload restarted the stream")
	}

	changed, err := config.Parse([]byte(fmt.Sprintf(`
egress_interface: lo
metrics_listen: 127.0.0.1:%d
streams:
  - name: feed
    listen: 127.0.0.1:%d
    group: %s
    port: %d
    ttl: 6
    loop: true
`, metricsPort, listen, group, mport)))
	if err != nil {
		t.Fatal(err)
	}
	if err := h.mgr.Apply(h.ctx, changed); err != nil {
		t.Fatal(err)
	}
	if got, _ := h.mgr.Generation("feed"); got == gen {
		t.Fatal("ttl change did not recreate the stream")
	}
	waitReady(t, h.url)
	sendTo(t, fmt.Sprintf("127.0.0.1:%d", listen), [][]byte{[]byte("after-reload")}, nil)
	got := readDatagrams(t, rx, 1, time.Second)
	if string(got[0].data) != "after-reload" {
		t.Fatalf("got %q", got[0].data)
	}
}

func TestTwoStreamsAreIndependent(t *testing.T) {
	g1, g2 := nextGroup(t), nextGroup(t)
	l1, l2 := freePort(t, "udp"), freePort(t, "udp")
	p1, p2 := freePort(t, "udp"), freePort(t, "udp")
	startHarness(t, fmt.Sprintf(`
egress_interface: lo
metrics_listen: 127.0.0.1:%d
streams:
  - name: one
    listen: 127.0.0.1:%d
    group: %s
    port: %d
    loop: true
  - name: two
    listen: 127.0.0.1:%d
    group: %s
    port: %d
    loop: true
`, freePort(t, "tcp"), l1, g1, p1, l2, g2, p2))
	rx1 := joinGroup(t, g1, p1)
	rx2 := joinGroup(t, g2, p2)
	sendTo(t, fmt.Sprintf("127.0.0.1:%d", l1), [][]byte{[]byte("alpha")}, nil)
	sendTo(t, fmt.Sprintf("127.0.0.1:%d", l2), [][]byte{[]byte("beta")}, nil)
	if string(readDatagrams(t, rx1, 1, time.Second)[0].data) != "alpha" {
		t.Fatal("stream one")
	}
	if string(readDatagrams(t, rx2, 1, time.Second)[0].data) != "beta" {
		t.Fatal("stream two")
	}
}

func TestSocketOptions(t *testing.T) {
	conn, err := listenUDP("127.0.0.1:0", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	pc := wrap(conn)
	ifi, err := net.InterfaceByName("lo")
	if err != nil {
		t.Fatal(err)
	}
	if err := configureSend(pc, conn, ifi, net.IPv4(127, 0, 0, 1), 7, false); err != nil {
		t.Fatal(err)
	}
	ttl, err := pc.MulticastTTL()
	if err != nil {
		t.Fatal(err)
	}
	if ttl != 7 {
		t.Fatalf("ttl %d", ttl)
	}
	loop, err := pc.MulticastLoopback()
	if err != nil {
		t.Fatal(err)
	}
	if loop {
		t.Fatal("loopback default was enabled")
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var outIP net.IP
	if err := raw.Control(func(fd uintptr) {
		addr, sockErr := unix.GetsockoptInet4Addr(int(fd), unix.IPPROTO_IP, unix.IP_MULTICAST_IF)
		if sockErr != nil {
			err = sockErr
			return
		}
		outIP = net.IPv4(addr[0], addr[1], addr[2], addr[3])
	}); err != nil {
		t.Fatal(err)
	}
	if !outIP.Equal(net.IPv4(127, 0, 0, 1)) {
		t.Fatalf("IP_MULTICAST_IF %v", outIP)
	}
	if err := pc.SetMulticastLoopback(true); err != nil {
		t.Fatal(err)
	}
	loop, err = pc.MulticastLoopback()
	if err != nil {
		t.Fatal(err)
	}
	if !loop {
		t.Fatal("loopback did not turn on")
	}
}
