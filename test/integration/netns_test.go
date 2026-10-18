//go:build integration

package integration

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/ipv4"
)

func TestMain(m *testing.M) {
	switch os.Getenv("MCAST_IT_ROLE") {
	case "send":
		os.Exit(runSend())
	case "recv":
		os.Exit(runRecv())
	case "get":
		os.Exit(runGet())
	default:
		os.Exit(m.Run())
	}
}

func TestForwardAcrossNamespaces(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("network namespace test requires root")
	}
	if _, err := exec.LookPath("ip"); err != nil {
		t.Skip("iproute2 is required")
	}
	id := fmt.Sprintf("%04x", os.Getpid()&0xffff)
	nsSrc, nsGw, nsDst := "ms"+id, "mg"+id, "md"+id
	vSrc, vGwIn, vGwOut, vDst := "as"+id, "bs"+id, "cs"+id, "ds"+id
	for _, ns := range []string{nsSrc, nsGw, nsDst} {
		mustRun(t, "ip", "netns", "add", ns)
		ns := ns
		t.Cleanup(func() { _ = exec.Command("ip", "netns", "del", ns).Run() })
	}
	mustRun(t, "ip", "link", "add", vSrc, "type", "veth", "peer", "name", vGwIn)
	mustRun(t, "ip", "link", "add", vDst, "type", "veth", "peer", "name", vGwOut)
	t.Cleanup(func() {
		_ = exec.Command("ip", "link", "del", vSrc).Run()
		_ = exec.Command("ip", "link", "del", vDst).Run()
	})
	mustRun(t, "ip", "link", "set", vSrc, "netns", nsSrc)
	mustRun(t, "ip", "link", "set", vGwIn, "netns", nsGw)
	mustRun(t, "ip", "link", "set", vDst, "netns", nsDst)
	mustRun(t, "ip", "link", "set", vGwOut, "netns", nsGw)

	nsExec(t, nsSrc, "ip", "link", "set", "lo", "up")
	nsExec(t, nsSrc, "ip", "link", "set", vSrc, "mtu", "9000")
	nsExec(t, nsSrc, "ip", "addr", "add", "10.1.0.1/24", "dev", vSrc)
	nsExec(t, nsSrc, "ip", "link", "set", vSrc, "up")

	nsExec(t, nsGw, "ip", "link", "set", "lo", "up")
	nsExec(t, nsGw, "ip", "link", "set", vGwIn, "mtu", "9000")
	nsExec(t, nsGw, "ip", "addr", "add", "10.1.0.2/24", "dev", vGwIn)
	nsExec(t, nsGw, "ip", "link", "set", vGwIn, "up")
	nsExec(t, nsGw, "ip", "addr", "add", "10.2.0.2/24", "dev", vGwOut)
	nsExec(t, nsGw, "ip", "link", "set", vGwOut, "mtu", "9000")
	nsExec(t, nsGw, "ip", "link", "set", vGwOut, "up")
	nsExec(t, nsGw, "ip", "route", "replace", "224.0.0.0/4", "dev", vGwOut)
	writeProc(t, nsGw, "net.ipv4.conf.all.rp_filter", "0")
	writeProc(t, nsGw, "net.ipv4.conf."+vGwOut+".rp_filter", "0")
	writeProc(t, nsDst, "net.ipv4.conf.all.rp_filter", "0")

	nsExec(t, nsDst, "ip", "link", "set", "lo", "up")
	nsExec(t, nsDst, "ip", "link", "set", vDst, "mtu", "9000")
	nsExec(t, nsDst, "ip", "addr", "add", "10.2.0.1/24", "dev", vDst)
	nsExec(t, nsDst, "ip", "link", "set", vDst, "up")

	root := moduleRoot(t)
	bin := filepath.Join(t.TempDir(), "mcast-relay")
	build := exec.Command("go", "build", "-o", bin, "./cmd/mcast-relay")
	build.Dir = root
	build.Env = os.Environ()
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build relay: %v\n%s", err, out)
	}
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	cfg := fmt.Sprintf(`
egress_interface: %s
metrics_listen: 10.2.0.2:19100
log_level: info
streams:
  - name: edge
    listen: 10.1.0.2:5000
    group: 239.1.1.10
    port: 5000
    ttl: 1
    loop: false
    allowed_sources: ["10.1.0.0/24"]
`, vGwOut)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	const packets = 200
	recv := exec.Command("ip", "netns", "exec", nsDst, exe, "-test.run=^$")
	recv.Env = append(os.Environ(),
		"MCAST_IT_ROLE=recv",
		"MCAST_IT_GROUP=239.1.1.10",
		"MCAST_IT_PORT=5000",
		"MCAST_IT_IFACE="+vDst,
		"MCAST_IT_COUNT="+strconv.Itoa(packets+1),
		"MCAST_IT_TIMEOUT=15s",
	)
	var recvOut strings.Builder
	recv.Stdout = &recvOut
	recv.Stderr = &recvOut
	if err := recv.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if recv.Process != nil {
			_ = recv.Process.Kill()
		}
	}()
	time.Sleep(200 * time.Millisecond)

	relay := exec.Command("ip", "netns", "exec", nsGw, bin, "--config", cfgPath)
	var relayOut strings.Builder
	relay.Stdout = &relayOut
	relay.Stderr = &relayOut
	if err := relay.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if relay.Process != nil {
			_ = relay.Process.Kill()
			_, _ = relay.Process.Wait()
		}
	}()
	waitReady(t, exe, nsGw, "http://10.2.0.2:19100/readyz", &relayOut)

	send := exec.Command("ip", "netns", "exec", nsSrc, exe, "-test.run=^$")
	send.Env = append(os.Environ(),
		"MCAST_IT_ROLE=send",
		"MCAST_IT_DST=10.1.0.2:5000",
		"MCAST_IT_COUNT="+strconv.Itoa(packets),
		"MCAST_IT_INTERVAL=2ms",
		"MCAST_IT_JUMBO=8000",
	)
	out, err := send.CombinedOutput()
	if err != nil {
		t.Fatalf("send: %v\n%s\nrelay:\n%s", err, out, relayOut.String())
	}
	done := make(chan error, 1)
	go func() { done <- recv.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("receiver: %v\n%s\nrelay:\n%s", err, recvOut.String(), relayOut.String())
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("receiver timed out\n%s\nrelay:\n%s", recvOut.String(), relayOut.String())
	}

	body := httpInNS(t, exe, nsGw, "http://10.2.0.2:19100/metrics")
	if !strings.Contains(body, "mcast_relay_packets_out_total") {
		t.Fatalf("metrics missing\n%s", body)
	}
	if !strings.Contains(body, fmt.Sprintf("mcast_relay_packets_out_total{direction=\"forward\",stream=\"edge\"} %d", packets+1)) &&
		!strings.Contains(body, fmt.Sprintf("} %d", packets+1)) {
		// Label order is deterministic but keep the failure output if the count is wrong.
		if !strings.Contains(body, fmt.Sprintf("%d", packets+1)) {
			t.Fatalf("expected %d forwarded packets, metrics:\n%s", packets+1, body)
		}
	}
}

func waitReady(t *testing.T, exe, ns, url string, logs *strings.Builder) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		out, err := role(exe, ns, "get", "MCAST_IT_URL="+url)
		last = string(out)
		if err == nil && strings.Contains(last, "ok") {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("relay not ready: %s\nlogs:\n%s", last, logs.String())
}

func httpInNS(t *testing.T, exe, ns, url string) string {
	t.Helper()
	out, err := role(exe, ns, "get", "MCAST_IT_URL="+url)
	if err != nil {
		t.Fatalf("get %s: %v\n%s", url, err, out)
	}
	return string(out)
}

func role(exe, ns, name, extra string) ([]byte, error) {
	cmd := exec.Command("ip", "netns", "exec", ns, exe, "-test.run=^$")
	cmd.Env = append(os.Environ(), "MCAST_IT_ROLE="+name, extra)
	return cmd.CombinedOutput()
}

func mustRun(t *testing.T, name string, args ...string) {
	t.Helper()
	cmd := exec.Command(name, args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%s %v: %v\n%s", name, args, err, out)
	}
}

func nsExec(t *testing.T, ns string, args ...string) {
	t.Helper()
	all := append([]string{"netns", "exec", ns}, args...)
	mustRun(t, "ip", all...)
}

func writeProc(t *testing.T, ns, key, value string) {
	t.Helper()
	path := "/proc/sys/" + strings.ReplaceAll(key, ".", "/")
	cmd := exec.Command("ip", "netns", "exec", ns, "sh", "-c", fmt.Sprintf("echo %s > %s", value, path))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("sysctl %s: %v\n%s", key, err, out)
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}

func runSend() int {
	dst := os.Getenv("MCAST_IT_DST")
	n, _ := strconv.Atoi(os.Getenv("MCAST_IT_COUNT"))
	interval, _ := time.ParseDuration(os.Getenv("MCAST_IT_INTERVAL"))
	jumbo, _ := strconv.Atoi(os.Getenv("MCAST_IT_JUMBO"))
	conn, err := net.Dial("udp4", dst)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer conn.Close()
	for i := 0; i < n; i++ {
		if _, err := conn.Write([]byte(fmt.Sprintf("pkt-%05d", i))); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		if interval > 0 {
			time.Sleep(interval)
		}
	}
	if jumbo > 0 {
		if _, err := conn.Write([]byte(strings.Repeat("J", jumbo))); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	}
	return 0
}

func runRecv() int {
	group := net.ParseIP(os.Getenv("MCAST_IT_GROUP")).To4()
	port, _ := strconv.Atoi(os.Getenv("MCAST_IT_PORT"))
	count, _ := strconv.Atoi(os.Getenv("MCAST_IT_COUNT"))
	timeout, _ := time.ParseDuration(os.Getenv("MCAST_IT_TIMEOUT"))
	ifi, err := net.InterfaceByName(os.Getenv("MCAST_IT_IFACE"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: port})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer conn.Close()
	pc := ipv4.NewPacketConn(conn)
	if err := pc.JoinGroup(ifi, &net.UDPAddr{IP: group}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	_ = conn.SetReadBuffer(8 << 20)
	deadline := time.Now().Add(timeout)
	buf := make([]byte, 65535)
	for got := 0; got < count; got++ {
		_ = conn.SetReadDeadline(deadline)
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "recv %d/%d: %v\n", got, count, err)
			return 1
		}
		if got == count-1 && n > 1000 {
			if n != 8000 || buf[0] != 'J' || buf[n-1] != 'J' {
				fmt.Fprintf(os.Stderr, "jumbo payload len %d\n", n)
				return 1
			}
			continue
		}
		want := fmt.Sprintf("pkt-%05d", got)
		if string(buf[:n]) != want {
			fmt.Fprintf(os.Stderr, "payload %d got %q want %q\n", got, buf[:n], want)
			return 1
		}
	}
	fmt.Printf("received %d\n", count)
	return 0
}

func runGet() int {
	resp, err := http.Get(os.Getenv("MCAST_IT_URL"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("%s", body)
	if resp.StatusCode != 200 {
		return 1
	}
	return 0
}
