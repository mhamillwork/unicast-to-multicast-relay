package main

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/net/ipv4"
)

// safeBuf is the child process log, written by os/exec and read by the test.
type safeBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *safeBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func (s *safeBuf) Contains(needle string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return bytes.Contains(s.buf.Bytes(), []byte(needle))
}

func TestHelperProcess(t *testing.T) {
	if os.Getenv("MCAST_HELPER") != "1" {
		return
	}
	cfg := os.Getenv("MCAST_CFG")
	if err := run([]string{"--config", cfg}); err != nil {
		fmt.Fprintf(os.Stderr, "helper: %v\n", err)
		os.Exit(1)
	}
	os.Exit(0)
}

func TestVersion(t *testing.T) {
	if err := run([]string{"--version"}); err != nil {
		t.Fatal(err)
	}
}

func TestSIGHUPKeepsRunningStream(t *testing.T) {
	if os.Getenv("MCAST_HELPER") == "1" {
		t.Skip()
	}
	listen := freeLocal(t, "udp")
	mport := freeLocal(t, "udp")
	metricsPort := freeLocal(t, "tcp")
	group := "239.23.200.7"
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	write := func(extra string) {
		body := fmt.Sprintf(`
egress_interface: lo
metrics_listen: 127.0.0.1:%d
log_level: info
streams:
  - name: feed
    listen: 127.0.0.1:%d
    group: %s
    port: %d
    ttl: 2
    loop: true
%s`, metricsPort, listen, group, mport, extra)
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("")

	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$")
	cmd.Env = append(os.Environ(), "MCAST_HELPER=1", "MCAST_CFG="+path)
	var stdout, stderr safeBuf
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGTERM)
			_, _ = cmd.Process.Wait()
		}
	})

	base := fmt.Sprintf("http://127.0.0.1:%d", metricsPort)
	waitHTTP(t, base+"/readyz")
	rx := joinLocal(t, group, mport)
	mustSend(t, fmt.Sprintf("127.0.0.1:%d", listen), "before")
	if got := readOne(t, rx, time.Second); string(got) != "before" {
		t.Fatalf("before reload got %q stderr=%s stdout=%s", got, stderr.String(), stdout.String())
	}

	extraListen := freeLocal(t, "udp")
	extraPort := freeLocal(t, "udp")
	extraGroup := "239.23.200.8"
	write(fmt.Sprintf(`
  - name: extra
    listen: 127.0.0.1:%d
    group: %s
    port: %d
    loop: true
`, extraListen, extraGroup, extraPort))
	if err := cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	waitFor(t, &stdout, "configuration reloaded", 3*time.Second)

	mustSend(t, fmt.Sprintf("127.0.0.1:%d", listen), "after")
	if got := readOne(t, rx, time.Second); string(got) != "after" {
		t.Fatalf("after reload got %q logs=%s", got, stdout.String())
	}
	rx2 := joinLocal(t, extraGroup, extraPort)
	waitHTTP(t, base+"/readyz")
	mustSend(t, fmt.Sprintf("127.0.0.1:%d", extraListen), "extra")
	if got := readOne(t, rx2, time.Second); string(got) != "extra" {
		t.Fatalf("extra stream got %q logs=%s", got, stdout.String())
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("exit: %v stderr=%s", err, stderr.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("process did not exit")
	}
}

func waitFor(t *testing.T, buf *safeBuf, needle string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if buf.Contains(needle) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q in %s", needle, buf.String())
}

func waitHTTP(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
			last = fmt.Errorf("status %d", resp.StatusCode)
		} else {
			last = err
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("wait %s: %v", url, last)
}

func freeLocal(t *testing.T, network string) int {
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

func joinLocal(t *testing.T, group string, port int) *net.UDPConn {
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
	return conn
}

func mustSend(t *testing.T, dst, payload string) {
	t.Helper()
	conn, err := net.Dial("udp4", dst)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte(payload)); err != nil {
		t.Fatal(err)
	}
}

func readOne(t *testing.T, conn *net.UDPConn, timeout time.Duration) []byte {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, 2048)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	return append([]byte(nil), buf[:n]...)
}
