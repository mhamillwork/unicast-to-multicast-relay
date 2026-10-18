package relay

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mhamillwork/unicast-to-multicast-relay/internal/acl"
	"github.com/mhamillwork/unicast-to-multicast-relay/internal/config"
	"github.com/mhamillwork/unicast-to-multicast-relay/internal/metrics"
	"golang.org/x/net/ipv4"
	"golang.org/x/sys/unix"
)

type forwarder struct {
	cfg        config.Stream
	egress     string
	batch      int
	readBuf    int
	writeBuf   int
	log        *slog.Logger
	met        *metrics.Handle
	allow      *acl.List
	dst        *net.UDPAddr
	ifi        *net.Interface
	ifIP       net.IP
	listenPort int

	mu          sync.Mutex
	recv        *ipv4.PacketConn
	send        *ipv4.PacketConn
	byPort      map[int]*ipv4.PacketConn
	groups      map[int][]ipv4.Message
	lastSendLog time.Time
	bound       atomic.Bool
}

func newForwarder(s config.Stream, cfg *config.Config, log *slog.Logger, met *metrics.Metrics) (*forwarder, error) {
	ifi, err := net.InterfaceByName(cfg.EgressInterface)
	if err != nil {
		return nil, fmt.Errorf("stream %s: interface %s: %w", s.Name, cfg.EgressInterface, err)
	}
	ip, err := ifaceIPv4(ifi)
	if err != nil {
		return nil, fmt.Errorf("stream %s: %w", s.Name, err)
	}
	allow, err := acl.New(s.AllowedSources)
	if err != nil {
		return nil, fmt.Errorf("stream %s: %w", s.Name, err)
	}
	_, portStr, err := net.SplitHostPort(s.Listen)
	if err != nil {
		return nil, fmt.Errorf("stream %s: listen: %w", s.Name, err)
	}
	listenPort, _ := strconv.Atoi(portStr)
	f := &forwarder{
		cfg:        s,
		egress:     cfg.EgressInterface,
		batch:      cfg.BatchSize,
		readBuf:    cfg.ReadBufferBytes,
		writeBuf:   cfg.WriteBufferBytes,
		log:        log.With("stream", s.Name, "direction", metrics.DirForward),
		met:        met.For(s.Name, metrics.DirForward),
		allow:      allow,
		dst:        &net.UDPAddr{IP: net.ParseIP(s.Group), Port: s.Port},
		ifi:        ifi,
		ifIP:       ip,
		listenPort: listenPort,
		groups:     map[int][]ipv4.Message{},
	}
	if config.LowMulticastGroup(s.Group) {
		f.log.Warn("group is inside 224.0.0.0/24; routers will not forward it")
	}
	return f, nil
}

func (f *forwarder) open() error {
	conn, err := listenUDP(f.cfg.Listen, false)
	if err != nil {
		return fmt.Errorf("listen %s: %w", f.cfg.Listen, err)
	}
	if err := setBuffers(conn, f.readBuf, f.writeBuf); err != nil {
		conn.Close()
		return err
	}
	recv := wrap(conn)
	if err := configureSend(recv, conn, f.ifi, f.ifIP, f.cfg.TTL, f.cfg.Loop); err != nil {
		recv.Close()
		return err
	}
	send := recv
	if f.cfg.SourcePort != 0 && f.cfg.SourcePort != f.listenPort {
		addr := net.JoinHostPort(f.ifIP.String(), strconv.Itoa(f.cfg.SourcePort))
		sc, err := listenUDP(addr, true)
		if err != nil {
			recv.Close()
			return fmt.Errorf("bind source port %s: %w", addr, err)
		}
		if err := setBuffers(sc, 0, f.writeBuf); err != nil {
			recv.Close()
			sc.Close()
			return err
		}
		send = wrap(sc)
		if err := configureSend(send, sc, f.ifi, f.ifIP, f.cfg.TTL, f.cfg.Loop); err != nil {
			recv.Close()
			send.Close()
			return err
		}
	}
	f.mu.Lock()
	f.recv = recv
	f.send = send
	f.byPort = map[int]*ipv4.PacketConn{}
	f.mu.Unlock()
	f.bound.Store(true)
	f.met.Up.Set(1)
	f.log.Info("listening",
		"listen", f.cfg.Listen,
		"group", f.dst.String(),
		"ttl", f.cfg.TTL,
		"loop", f.cfg.Loop,
		"iface", f.egress,
		"source_port", f.cfg.SourcePort,
		"preserve_source_port", f.cfg.PreserveSourcePort,
	)
	return nil
}

func (f *forwarder) close() {
	f.bound.Store(false)
	f.met.Up.Set(0)
	f.mu.Lock()
	defer f.mu.Unlock()
	closed := map[*ipv4.PacketConn]struct{}{}
	shut := func(p *ipv4.PacketConn) {
		if p == nil {
			return
		}
		if _, ok := closed[p]; ok {
			return
		}
		closed[p] = struct{}{}
		_ = p.Close()
	}
	shut(f.recv)
	shut(f.send)
	for _, p := range f.byPort {
		shut(p)
	}
	f.recv = nil
	f.send = nil
	f.byPort = nil
}

func (f *forwarder) ready() (bool, string) {
	if !f.bound.Load() {
		return false, fmt.Sprintf("stream %s is not bound", f.cfg.Name)
	}
	return true, ""
}

func (f *forwarder) loop(ctx context.Context) error {
	stop := wakeOnCancel(ctx, func() *ipv4.PacketConn {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.recv
	})
	defer stop()

	f.mu.Lock()
	recv := f.recv
	f.mu.Unlock()
	if recv == nil {
		return fmt.Errorf("stream %s is not open", f.cfg.Name)
	}

	msgs, bufs := newBatch(f.batch, f.cfg.MaxDatagramBytes)
	out := make([]ipv4.Message, f.batch)
	ports := make([]int, f.batch)

	for {
		n, err := recv.ReadBatch(msgs, recvBatchFlags)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if temporaryRead(err) {
				continue
			}
			return err
		}
		if n == 0 {
			continue
		}
		start := time.Now()
		inBytes := 0
		aclDrops := 0
		oversize := 0
		w := 0
		for i := 0; i < n; i++ {
			m := &msgs[i]
			inBytes += m.N
			if m.Flags&unix.MSG_TRUNC != 0 || m.N > f.cfg.MaxDatagramBytes {
				oversize++
				continue
			}
			src, _ := m.Addr.(*net.UDPAddr)
			if src == nil || !f.allow.Allow(src.IP) {
				aclDrops++
				continue
			}
			var payload []byte
			if m.N == 0 {
				payload = []byte{}
			} else {
				payload = bufs[i][:m.N]
			}
			out[w].Buffers = [][]byte{payload}
			out[w].Addr = f.dst
			out[w].OOB = nil
			out[w].N = 0
			out[w].NN = 0
			out[w].Flags = 0
			if src != nil {
				ports[w] = src.Port
			}
			w++
		}
		f.met.InPackets.Add(float64(n))
		f.met.InBytes.Add(float64(inBytes))
		if aclDrops > 0 {
			f.met.DropACL.Add(float64(aclDrops))
		}
		if oversize > 0 {
			f.met.DropOver.Add(float64(oversize))
		}
		sent := 0
		if w > 0 {
			var sendErr error
			if f.cfg.PreserveSourcePort {
				sent, sendErr = f.writePreserved(out[:w], ports[:w])
			} else {
				sendErr = writeAll(f.send, out[:w], func(pkts, bytes int) {
					f.met.OutPackets.Add(float64(pkts))
					f.met.OutBytes.Add(float64(bytes))
					sent += pkts
				}, f.countDrop)
			}
			if sent > 0 {
				f.met.ObserveLatency(time.Since(start), sent)
			}
			if sendErr != nil {
				f.logSend(sendErr)
				resetBatch(msgs, bufs, n)
				return sendErr
			}
		}
		resetBatch(msgs, bufs, n)
	}
}

func (f *forwarder) writePreserved(msgs []ipv4.Message, ports []int) (int, error) {
	if len(f.groups) > maxPreservePorts {
		f.groups = map[int][]ipv4.Message{}
	}
	for k, sl := range f.groups {
		f.groups[k] = sl[:0]
	}
	for i, m := range msgs {
		port := ports[i]
		f.groups[port] = append(f.groups[port], m)
	}
	sent := 0
	for port, batch := range f.groups {
		if len(batch) == 0 {
			continue
		}
		pc := f.senderFor(port)
		err := writeAll(pc, batch, func(pkts, bytes int) {
			f.met.OutPackets.Add(float64(pkts))
			f.met.OutBytes.Add(float64(bytes))
			sent += pkts
		}, f.countDrop)
		if err != nil {
			return sent, err
		}
	}
	return sent, nil
}

func (f *forwarder) senderFor(port int) *ipv4.PacketConn {
	if port == 0 || port == f.listenPort {
		return f.send
	}
	if pc, ok := f.byPort[port]; ok {
		return pc
	}
	if len(f.byPort) >= maxPreservePorts {
		f.met.Fallbacks.Inc()
		return f.send
	}
	addr := net.JoinHostPort(f.ifIP.String(), strconv.Itoa(port))
	conn, err := listenUDP(addr, true)
	if err != nil {
		f.met.Fallbacks.Inc()
		f.log.Debug("source port bind failed", "port", port, "err", err)
		return f.send
	}
	if err := setBuffers(conn, 0, f.writeBuf); err != nil {
		conn.Close()
		f.met.Fallbacks.Inc()
		f.log.Debug("source port buffers failed", "port", port, "err", err)
		return f.send
	}
	pc := wrap(conn)
	if err := configureSend(pc, conn, f.ifi, f.ifIP, f.cfg.TTL, f.cfg.Loop); err != nil {
		pc.Close()
		f.met.Fallbacks.Inc()
		f.log.Debug("source port setup failed", "port", port, "err", err)
		return f.send
	}
	f.byPort[port] = pc
	return pc
}

func (f *forwarder) countDrop(reason string, n int) {
	if n <= 0 {
		return
	}
	switch reason {
	case "oversize":
		f.met.DropOver.Add(float64(n))
	default:
		f.met.DropSend.Add(float64(n))
	}
}

func (f *forwarder) logSend(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if time.Since(f.lastSendLog) < time.Second {
		return
	}
	f.lastSendLog = time.Now()
	f.log.Warn("send failed", "err", err)
}
