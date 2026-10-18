// Package relay forwards unicast UDP to multicast and, when configured,
// multicast back to unicast targets. Each stream runs on its own sockets.
package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mhamillwork/unicast-to-multicast-relay/internal/config"
	"github.com/mhamillwork/unicast-to-multicast-relay/internal/metrics"
)

// Manager starts, reloads, and stops streams.
type Manager struct {
	applyMu sync.Mutex
	mu      sync.Mutex
	log     *slog.Logger
	metrics *metrics.Metrics
	cfg     *config.Config
	running map[string]*running
	nextID  atomic.Uint64
}

type running struct {
	key    string
	sig    string
	id     uint64
	cancel context.CancelFunc
	done   chan struct{}
	ready  func() (bool, string)
}

type spec struct {
	key    string
	sig    string
	launch func(initial bool) (*running, error)
}

// NewManager builds a manager. cfg is applied separately so a bad config
// never leaves a half-started process without an error.
func NewManager(log *slog.Logger, met *metrics.Metrics) *Manager {
	if log == nil {
		log = slog.Default()
	}
	return &Manager{
		log:     log,
		metrics: met,
		running: map[string]*running{},
	}
}

// Apply starts the configuration. On the first call a bind failure is
// returned and any streams started by this call are stopped. Later calls
// keep streams whose signature did not change, restart the ones that did,
// and retry new sockets in the background when a bind fails.
func (m *Manager) Apply(ctx context.Context, cfg *config.Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	m.applyMu.Lock()
	defer m.applyMu.Unlock()

	initial := m.cfg == nil
	desired := map[string]spec{}
	for _, s := range cfg.Streams {
		s := s
		key := "f/" + s.Name
		sig := cfg.ForwardSignature(s)
		desired[key] = spec{key: key, sig: sig, launch: func(initial bool) (*running, error) {
			f, err := newForwarder(s, cfg, m.log, m.metrics)
			if err != nil {
				return nil, err
			}
			return m.launch(ctx, key, sig, s.Name, initial, f.open, f.loop, f.close, f.ready)
		}}
	}
	for _, s := range cfg.ReverseStreams {
		s := s
		key := "r/" + s.Name
		sig := cfg.ReverseSignature(s)
		desired[key] = spec{key: key, sig: sig, launch: func(initial bool) (*running, error) {
			rv, err := newReverser(s, cfg, m.log, m.metrics)
			if err != nil {
				return nil, err
			}
			return m.launch(ctx, key, sig, s.Name, initial, rv.open, rv.loop, rv.close, rv.ready)
		}}
	}

	m.mu.Lock()
	var stop []*running
	for key, rs := range m.running {
		d, ok := desired[key]
		if ok && d.sig == rs.sig {
			delete(desired, key)
			continue
		}
		stop = append(stop, rs)
		delete(m.running, key)
	}
	m.mu.Unlock()

	for _, rs := range stop {
		rs.cancel()
	}
	for _, rs := range stop {
		<-rs.done
	}

	var started []*running
	for _, d := range desired {
		rs, err := d.launch(initial)
		if err != nil {
			for _, rs := range started {
				rs.cancel()
			}
			for _, rs := range started {
				<-rs.done
			}
			return err
		}
		started = append(started, rs)
	}

	m.mu.Lock()
	for _, rs := range started {
		m.running[rs.key] = rs
	}
	m.cfg = cfg
	m.mu.Unlock()
	if cfg.MaxDatagramBytes < 9000 {
		m.log.Warn("max_datagram_bytes is below 9000; larger payloads are dropped", "max_datagram_bytes", cfg.MaxDatagramBytes)
	}
	m.log.Info("configuration applied", "streams", len(cfg.Streams), "reverse_streams", len(cfg.ReverseStreams), "initial", initial)
	return nil
}

func (m *Manager) launch(parent context.Context, key, sig, name string, initial bool, openFn func() error, loopFn func(context.Context) error, closeFn func(), readyFn func() (bool, string)) (*running, error) {
	already := false
	if initial {
		if err := openFn(); err != nil {
			return nil, fmt.Errorf("stream %s: %w", name, err)
		}
		already = true
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	var bound atomic.Bool
	rs := &running{
		key:    key,
		sig:    sig,
		id:     m.nextID.Add(1),
		cancel: cancel,
		done:   done,
		ready: func() (bool, string) {
			if !bound.Load() {
				ok, msg := readyFn()
				if ok {
					return true, ""
				}
				if msg != "" {
					return false, msg
				}
				return false, fmt.Sprintf("stream %s is not bound", name)
			}
			return readyFn()
		},
	}
	if already {
		bound.Store(true)
	}
	go supervise(ctx, done, &bound, openFn, loopFn, closeFn, m.log, name, already)
	return rs, nil
}

// Generation returns the start id of a stream. It changes when the stream
// is recreated and stays put when a reload keeps it.
func (m *Manager) Generation(name string) (uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if rs, ok := m.running["f/"+name]; ok {
		return rs.id, true
	}
	if rs, ok := m.running["r/"+name]; ok {
		return rs.id, true
	}
	return 0, false
}

// Ready reports whether every stream is bound and the egress interface is up.
func (m *Manager) Ready() (bool, string) {
	m.mu.Lock()
	cfg := m.cfg
	checks := make([]func() (bool, string), 0, len(m.running))
	for _, rs := range m.running {
		checks = append(checks, rs.ready)
	}
	m.mu.Unlock()
	if cfg == nil {
		return false, "not configured"
	}
	if err := config.InterfaceReady(cfg.EgressInterface); err != nil {
		return false, err.Error()
	}
	seen := map[string]struct{}{cfg.EgressInterface: {}}
	for _, s := range cfg.ReverseStreams {
		if _, ok := seen[s.Interface]; ok {
			continue
		}
		seen[s.Interface] = struct{}{}
		if err := config.InterfaceReady(s.Interface); err != nil {
			return false, err.Error()
		}
	}
	for _, check := range checks {
		if ok, msg := check(); !ok {
			return false, msg
		}
	}
	return true, ""
}

// Shutdown stops every stream.
func (m *Manager) Shutdown(ctx context.Context) {
	m.applyMu.Lock()
	defer m.applyMu.Unlock()
	m.mu.Lock()
	all := m.running
	m.running = map[string]*running{}
	m.mu.Unlock()
	for _, rs := range all {
		rs.cancel()
	}
	for _, rs := range all {
		select {
		case <-rs.done:
		case <-ctx.Done():
			m.log.Warn("shutdown timed out")
			return
		}
	}
}

func supervise(ctx context.Context, done chan struct{}, bound *atomic.Bool, openFn func() error, loopFn func(context.Context) error, closeFn func(), log *slog.Logger, name string, alreadyOpen bool) {
	defer close(done)
	backoff := 100 * time.Millisecond
	opened := alreadyOpen
	for {
		if ctx.Err() != nil {
			closeFn()
			bound.Store(false)
			return
		}
		if !opened {
			if err := openFn(); err != nil {
				bound.Store(false)
				log.Error("bind failed", "stream", name, "err", err, "backoff", backoff.String())
				if !wait(ctx, backoff) {
					closeFn()
					return
				}
				backoff = nextBackoff(backoff)
				continue
			}
			backoff = 100 * time.Millisecond
		}
		opened = false
		bound.Store(true)
		err := loopFn(ctx)
		closeFn()
		bound.Store(false)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			err = errors.New("stream stopped")
		}
		log.Error("stream failed", "stream", name, "err", err, "backoff", backoff.String())
		if !wait(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff)
	}
}

func wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > 5*time.Second {
		return 5 * time.Second
	}
	return d
}
