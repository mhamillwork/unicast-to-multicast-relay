// Command load drives the relay on the loopback interface and prints
// throughput and forwarding latency. Pass -strict to fail unless the run
// reaches the NFR-1 target (100k packets/s, p99 under 1ms).
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sort"
	"time"

	"github.com/mhamillwork/unicast-to-multicast-relay/internal/config"
	"github.com/mhamillwork/unicast-to-multicast-relay/internal/metrics"
	"github.com/mhamillwork/unicast-to-multicast-relay/internal/relay"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/ipv4"
	"golang.org/x/sys/unix"
)

func main() {
	duration := flag.Duration("duration", 3*time.Second, "how long to send")
	payload := flag.Int("payload", 64, "UDP payload size, at least 8")
	pps := flag.Int("pps", 0, "target packets per second; 0 sends as fast as possible")
	strict := flag.Bool("strict", false, "exit 1 unless NFR-1 thresholds are met")
	minPPS := flag.Float64("min-pps", 100000, "strict-mode minimum relay output rate")
	maxP99 := flag.Duration("max-p99", time.Millisecond, "strict-mode maximum relay p99 latency")
	ifaceName := flag.String("iface", "lo", "egress interface")
	flag.Parse()
	if *payload < 8 {
		fatalf("payload must be at least 8 bytes")
	}
	ifi, err := net.InterfaceByName(*ifaceName)
	if err != nil {
		fatalf("interface: %v", err)
	}
	if ifi.Flags&net.FlagMulticast == 0 {
		fatalf("interface %s does not support multicast (ip link set %s multicast on)", *ifaceName, *ifaceName)
	}

	listen := freeUDP()
	mport := freeUDP()
	group := "239.23.255.1"
	cfg, err := config.Parse([]byte(fmt.Sprintf(`
egress_interface: %s
metrics_listen: 127.0.0.1:9
log_level: error
batch_size: 64
streams:
  - name: load
    listen: 127.0.0.1:%d
    group: %s
    port: %d
    ttl: 1
    loop: true
`, *ifaceName, listen, group, mport)))
	if err != nil {
		fatalf("config: %v", err)
	}
	reg := prometheus.NewRegistry()
	met := metrics.New(reg, "load")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := relay.NewManager(logger, met)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Apply(ctx, cfg); err != nil {
		fatalf("start: %v", err)
	}
	defer mgr.Shutdown(context.Background())

	recvConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: mport})
	if err != nil {
		fatalf("recv: %v", err)
	}
	defer recvConn.Close()
	rpc := ipv4.NewPacketConn(recvConn)
	if err := rpc.JoinGroup(ifi, &net.UDPAddr{IP: net.ParseIP(group)}); err != nil {
		fatalf("join: %v", err)
	}
	_ = recvConn.SetReadBuffer(16 << 20)

	type sample struct {
		latency time.Duration
	}
	latencies := make(chan sample, 1<<16)
	recvDone := make(chan struct{})
	var recvCount int
	go func() {
		defer close(recvDone)
		msgs := make([]ipv4.Message, 64)
		bufs := make([][]byte, 64)
		for i := range msgs {
			bufs[i] = make([]byte, 2048)
			msgs[i].Buffers = [][]byte{bufs[i]}
		}
		deadline := time.Now().Add(*duration + 2*time.Second)
		_ = recvConn.SetReadDeadline(deadline)
		for {
			n, err := rpc.ReadBatch(msgs, unix.MSG_WAITFORONE)
			now := time.Now()
			for i := 0; i < n; i++ {
				recvCount++
				if msgs[i].N >= 8 {
					ts := int64(binary.BigEndian.Uint64(bufs[i][:8]))
					d := now.Sub(time.Unix(0, ts))
					select {
					case latencies <- sample{latency: d}:
					default:
					}
				}
			}
			if err != nil {
				return
			}
			if time.Now().After(deadline) {
				return
			}
		}
	}()

	sendConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		fatalf("dial: %v", err)
	}
	defer sendConn.Close()
	spc := ipv4.NewPacketConn(sendConn)
	batch := 64
	msgs := make([]ipv4.Message, batch)
	bufs := make([][]byte, batch)
	dst := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: listen}
	for i := range msgs {
		bufs[i] = make([]byte, *payload)
		for j := 8; j < *payload; j++ {
			bufs[i][j] = byte(j)
		}
		msgs[i].Buffers = [][]byte{bufs[i]}
		msgs[i].Addr = dst
	}
	var interval time.Duration
	if *pps > 0 {
		interval = time.Second / time.Duration(*pps)
	}
	start := time.Now()
	sent := 0
	next := start
	for time.Since(start) < *duration {
		now := time.Now()
		stamp := uint64(now.UnixNano())
		for i := range msgs {
			binary.BigEndian.PutUint64(bufs[i][:8], stamp)
		}
		off := 0
		for off < batch {
			k, err := spc.WriteBatch(msgs[off:], 0)
			if k > 0 {
				sent += k
				off += k
			}
			if err != nil && k == 0 {
				break
			}
			if k == 0 {
				break
			}
		}
		if interval > 0 {
			next = next.Add(interval * time.Duration(batch))
			if sleep := time.Until(next); sleep > 0 {
				time.Sleep(sleep)
			}
		}
	}
	elapsed := time.Since(start)
	time.Sleep(200 * time.Millisecond)
	_ = recvConn.SetReadDeadline(time.Now())
	<-recvDone
	close(latencies)

	in := counter(reg, "mcast_relay_packets_in_total")
	out := counter(reg, "mcast_relay_packets_out_total")
	drops := counter(reg, "mcast_relay_drops_total")
	rate := float64(out) / elapsed.Seconds()
	p99 := histogramQuantile(reg, "mcast_relay_forward_latency_seconds", 0.99)
	var e2e []time.Duration
	for s := range latencies {
		e2e = append(e2e, s.latency)
	}
	fmt.Printf("duration=%s sent=%d relay_in=%.0f relay_out=%.0f relay_drops=%.0f recv=%d pps=%.0f relay_p99=%s e2e_p99=%s payload=%d\n",
		elapsed.Round(time.Millisecond), sent, in, out, drops, recvCount, rate, p99.Round(time.Microsecond), quantile(e2e, 0.99).Round(time.Microsecond), *payload)

	if recvCount == 0 || out == 0 {
		fatalf("no traffic was forwarded")
	}
	if *strict {
		if rate < *minPPS {
			fatalf("pps %.0f is below %.0f", rate, *minPPS)
		}
		if p99 > *maxP99 {
			fatalf("relay p99 %s is above %s", p99, *maxP99)
		}
		if drops > 0 {
			fatalf("relay dropped %.0f datagrams", drops)
		}
	}
}

func freeUDP() int {
	c, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		fatalf("port: %v", err)
	}
	defer c.Close()
	return c.LocalAddr().(*net.UDPAddr).Port
}

func counter(reg *prometheus.Registry, name string) float64 {
	fams, _ := reg.Gather()
	var sum float64
	for _, fam := range fams {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			if c := m.GetCounter(); c != nil {
				sum += c.GetValue()
			}
		}
	}
	return sum
}

func histogramQuantile(reg *prometheus.Registry, name string, q float64) time.Duration {
	fams, _ := reg.Gather()
	for _, fam := range fams {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			h := m.GetHistogram()
			if h == nil || h.GetSampleCount() == 0 {
				continue
			}
			target := q * float64(h.GetSampleCount())
			for _, b := range h.GetBucket() {
				if float64(b.GetCumulativeCount()) >= target {
					return time.Duration(b.GetUpperBound() * float64(time.Second))
				}
			}
		}
	}
	return 0
}

func quantile(samples []time.Duration, q float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	// Partial selection would be nicer; N is capped by the channel.
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(q * float64(len(sorted)-1))
	return sorted[idx]
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "load: "+format+"\n", args...)
	os.Exit(1)
}
