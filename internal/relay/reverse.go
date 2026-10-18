package relay

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mhamillwork/unicast-to-multicast-relay/internal/config"
	"github.com/mhamillwork/unicast-to-multicast-relay/internal/metrics"
	"golang.org/x/net/ipv4"
	"golang.org/x/sys/unix"
)

type reverser struct {
	cfg      config.ReverseStream
	batch    int
	readBuf  int
	writeBuf int
	maxDG    int
	log      *slog.Logger
	met      *metrics.Handle
	ifi      *net.Interface
	group    net.IP
	targets  []net.Addr

	mu          sync.Mutex
	conn        *ipv4.PacketConn
	bound       atomic.Bool
	lastSendLog time.Time
}

func newReverser(s config.ReverseStream, cfg *config.Config, log *slog.Logger, met *metrics.Metrics) (*reverser, error) {
	ifi, err := net.InterfaceByName(s.Interface)
	if err != nil {
		return nil, fmt.Errorf("reverse stream %s: interface %s: %w", s.Name, s.Interface, err)
	}
	targets := make([]net.Addr, 0, len(s.Targets))
	for _, t := range s.Targets {
		addr, err := net.ResolveUDPAddr("udp4", t)
		if err != nil {
			return nil, fmt.Errorf("reverse stream %s: target %s: %w", s.Name, t, err)
		}
		targets = append(targets, addr)
	}
	r := &reverser{
		cfg:      s,
		batch:    cfg.BatchSize,
		readBuf:  cfg.ReadBufferBytes,
		writeBuf: cfg.WriteBufferBytes,
		maxDG:    cfg.MaxDatagramBytes,
		log:      log.With("stream", s.Name, "direction", metrics.DirReverse),
		met:      met.For(s.Name, metrics.DirReverse),
		ifi:      ifi,
		group:    net.ParseIP(s.JoinGroup).To4(),
		targets:  targets,
	}
	if config.LowMulticastGroup(s.JoinGroup) {
		r.log.Warn("group is inside 224.0.0.0/24; routers will not forward it")
	}
	return r, nil
}

func (r *reverser) open() error {
	addr := net.JoinHostPort(r.group.String(), itoa(r.cfg.Port))
	conn, err := listenUDP(addr, true)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	if err := setBuffers(conn, r.readBuf, r.writeBuf); err != nil {
		conn.Close()
		return err
	}
	pc := wrap(conn)
	if len(r.cfg.Sources) == 0 {
		if err := pc.JoinGroup(r.ifi, &net.UDPAddr{IP: r.group}); err != nil {
			pc.Close()
			return fmt.Errorf("join %s on %s: %w", r.group, r.ifi.Name, err)
		}
	} else {
		for _, src := range r.cfg.Sources {
			if err := pc.JoinSourceSpecificGroup(r.ifi, &net.UDPAddr{IP: r.group}, &net.UDPAddr{IP: net.ParseIP(src)}); err != nil {
				pc.Close()
				return fmt.Errorf("join %s source %s on %s: %w", r.group, src, r.ifi.Name, err)
			}
		}
	}
	r.mu.Lock()
	r.conn = pc
	r.mu.Unlock()
	r.bound.Store(true)
	r.met.Up.Set(1)
	r.log.Info("joined", "group", net.JoinHostPort(r.group.String(), itoa(r.cfg.Port)), "iface", r.ifi.Name, "sources", r.cfg.Sources, "targets", r.cfg.Targets)
	return nil
}

func (r *reverser) close() {
	r.bound.Store(false)
	r.met.Up.Set(0)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conn != nil {
		if len(r.cfg.Sources) == 0 {
			_ = r.conn.LeaveGroup(r.ifi, &net.UDPAddr{IP: r.group})
		} else {
			for _, src := range r.cfg.Sources {
				_ = r.conn.LeaveSourceSpecificGroup(r.ifi, &net.UDPAddr{IP: r.group}, &net.UDPAddr{IP: net.ParseIP(src)})
			}
		}
		_ = r.conn.Close()
		r.conn = nil
	}
}

func (r *reverser) ready() (bool, string) {
	if !r.bound.Load() {
		return false, fmt.Sprintf("reverse stream %s is not joined", r.cfg.Name)
	}
	return true, ""
}

func (r *reverser) loop(ctx context.Context) error {
	stop := wakeOnCancel(ctx, func() *ipv4.PacketConn {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.conn
	})
	defer stop()

	r.mu.Lock()
	conn := r.conn
	r.mu.Unlock()
	if conn == nil {
		return fmt.Errorf("reverse stream %s is not open", r.cfg.Name)
	}

	msgs, bufs := newBatch(r.batch, r.maxDG)
	nTargets := len(r.targets)
	out := make([]ipv4.Message, r.batch*nTargets)

	for {
		n, err := conn.ReadBatch(msgs, recvBatchFlags)
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
		oversize := 0
		w := 0
		for i := 0; i < n; i++ {
			m := &msgs[i]
			inBytes += m.N
			if m.Flags&unix.MSG_TRUNC != 0 || m.N > r.maxDG {
				oversize++
				continue
			}
			var payload []byte
			if m.N == 0 {
				payload = []byte{}
			} else {
				payload = bufs[i][:m.N]
			}
			for _, tgt := range r.targets {
				out[w].Buffers = [][]byte{payload}
				out[w].Addr = tgt
				out[w].OOB = nil
				out[w].N = 0
				out[w].NN = 0
				out[w].Flags = 0
				w++
			}
		}
		r.met.InPackets.Add(float64(n))
		r.met.InBytes.Add(float64(inBytes))
		if oversize > 0 {
			r.met.DropOver.Add(float64(oversize))
		}
		sent := 0
		if w > 0 {
			err = writeAll(conn, out[:w], func(pkts, bytes int) {
				r.met.OutPackets.Add(float64(pkts))
				r.met.OutBytes.Add(float64(bytes))
				sent += pkts
			}, func(reason string, dn int) {
				if reason == "oversize" {
					r.met.DropOver.Add(float64(dn))
				} else {
					r.met.DropSend.Add(float64(dn))
				}
			})
			if sent > 0 {
				r.met.ObserveLatency(time.Since(start), sent)
			}
			if err != nil {
				r.logSend(err)
				resetBatch(msgs, bufs, n)
				return err
			}
		}
		resetBatch(msgs, bufs, n)
	}
}

func (r *reverser) logSend(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if time.Since(r.lastSendLog) < time.Second {
		return
	}
	r.lastSendLog = time.Now()
	r.log.Warn("send failed", "err", err)
}

func itoa(n int) string {
	return fmt.Sprintf("%d", n)
}
