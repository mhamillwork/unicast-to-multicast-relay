// Package metrics exposes per-stream Prometheus counters and a forwarding
// latency histogram.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	DirForward = "forward"
	DirReverse = "reverse"

	ReasonACL       = "acl"
	ReasonOversize  = "oversize"
	ReasonSendError = "send_error"
)

// Metrics is the process-wide set of relay instruments.
type Metrics struct {
	reg        prometheus.Registerer
	packetsIn  *prometheus.CounterVec
	packetsOut *prometheus.CounterVec
	bytesIn    *prometheus.CounterVec
	bytesOut   *prometheus.CounterVec
	drops      *prometheus.CounterVec
	latency    *prometheus.HistogramVec
	up         *prometheus.GaugeVec
	fallbacks  *prometheus.CounterVec
	build      *prometheus.GaugeVec
}

// Handle is the cached set of instruments for one stream. The hot path
// uses these directly so it does not look labels up per packet.
type Handle struct {
	InPackets  prometheus.Counter
	InBytes    prometheus.Counter
	OutPackets prometheus.Counter
	OutBytes   prometheus.Counter
	DropACL    prometheus.Counter
	DropOver   prometheus.Counter
	DropSend   prometheus.Counter
	Fallbacks  prometheus.Counter
	Latency    prometheus.Observer
	Up         prometheus.Gauge
}

// New registers relay metrics on reg.
func New(reg prometheus.Registerer, version string) *Metrics {
	m := &Metrics{
		reg: reg,
		packetsIn: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mcast_relay_packets_in_total",
			Help: "UDP datagrams received, including those later dropped.",
		}, []string{"stream", "direction"}),
		packetsOut: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mcast_relay_packets_out_total",
			Help: "UDP datagrams successfully sent. Reverse fan-out counts one per target.",
		}, []string{"stream", "direction"}),
		bytesIn: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mcast_relay_bytes_in_total",
			Help: "UDP payload bytes received.",
		}, []string{"stream", "direction"}),
		bytesOut: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mcast_relay_bytes_out_total",
			Help: "UDP payload bytes successfully sent.",
		}, []string{"stream", "direction"}),
		drops: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mcast_relay_drops_total",
			Help: "Datagrams dropped before or during send.",
		}, []string{"stream", "direction", "reason"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "mcast_relay_forward_latency_seconds",
			Help: "Time from batch receive returning to the corresponding send completing.",
			Buckets: []float64{
				0.00005, 0.0001, 0.00025, 0.0005, 0.001,
				0.0025, 0.005, 0.01, 0.025, 0.05, 0.1,
			},
		}, []string{"stream"}),
		up: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "mcast_relay_stream_up",
			Help: "1 when the stream sockets are bound.",
		}, []string{"stream", "direction"}),
		fallbacks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mcast_relay_source_port_fallbacks_total",
			Help: "Packets sent from the default socket because the original source port could not be bound.",
		}, []string{"stream"}),
		build: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "mcast_relay_build_info",
			Help: "Build information.",
		}, []string{"version"}),
	}
	reg.MustRegister(m.packetsIn, m.packetsOut, m.bytesIn, m.bytesOut, m.drops, m.latency, m.up, m.fallbacks, m.build)
	m.build.WithLabelValues(version).Set(1)
	return m
}

// For returns the instruments for one stream and direction.
func (m *Metrics) For(stream, direction string) *Handle {
	return &Handle{
		InPackets:  m.packetsIn.WithLabelValues(stream, direction),
		InBytes:    m.bytesIn.WithLabelValues(stream, direction),
		OutPackets: m.packetsOut.WithLabelValues(stream, direction),
		OutBytes:   m.bytesOut.WithLabelValues(stream, direction),
		DropACL:    m.drops.WithLabelValues(stream, direction, ReasonACL),
		DropOver:   m.drops.WithLabelValues(stream, direction, ReasonOversize),
		DropSend:   m.drops.WithLabelValues(stream, direction, ReasonSendError),
		Fallbacks:  m.fallbacks.WithLabelValues(stream),
		Latency:    m.latency.WithLabelValues(stream),
		Up:         m.up.WithLabelValues(stream, direction),
	}
}

// ObserveLatency records d for n successfully forwarded datagrams.
func (h *Handle) ObserveLatency(d time.Duration, n int) {
	if n <= 0 {
		return
	}
	sec := d.Seconds()
	for i := 0; i < n; i++ {
		h.Latency.Observe(sec)
	}
}
