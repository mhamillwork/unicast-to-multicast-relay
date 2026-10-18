// Package config loads and validates the relay YAML configuration.
package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/mhamillwork/unicast-to-multicast-relay/internal/acl"
	"gopkg.in/yaml.v3"
)

const (
	DefaultMetricsListen = "0.0.0.0:9100"
	DefaultLogLevel      = "info"
	DefaultBatchSize     = 64
	DefaultBufferBytes   = 8 << 20
	DefaultMaxDatagram   = 65535
	DefaultTTL           = 1
	maxBatchSize         = 1024
)

// Config is the normalized runtime configuration.
type Config struct {
	EgressInterface  string          `json:"egress_interface" yaml:"egress_interface"`
	MetricsListen    string          `json:"metrics_listen" yaml:"metrics_listen"`
	LogLevel         string          `json:"log_level" yaml:"log_level"`
	BatchSize        int             `json:"batch_size" yaml:"batch_size"`
	ReadBufferBytes  int             `json:"read_buffer_bytes" yaml:"read_buffer_bytes"`
	WriteBufferBytes int             `json:"write_buffer_bytes" yaml:"write_buffer_bytes"`
	MaxDatagramBytes int             `json:"max_datagram_bytes" yaml:"max_datagram_bytes"`
	Streams          []Stream        `json:"streams" yaml:"streams"`
	ReverseStreams   []ReverseStream `json:"reverse_streams" yaml:"reverse_streams"`
}

// Stream is one unicast-to-multicast mapping.
type Stream struct {
	Name               string   `json:"name" yaml:"name"`
	Listen             string   `json:"listen" yaml:"listen"`
	Group              string   `json:"group" yaml:"group"`
	Port               int      `json:"port" yaml:"port"`
	TTL                int      `json:"ttl" yaml:"ttl"`
	Loop               bool     `json:"loop" yaml:"loop"`
	SourcePort         int      `json:"source_port" yaml:"source_port"`
	PreserveSourcePort bool     `json:"preserve_source_port" yaml:"preserve_source_port"`
	AllowedSources     []string `json:"allowed_sources" yaml:"allowed_sources"`
	MaxDatagramBytes   int      `json:"max_datagram_bytes" yaml:"max_datagram_bytes"`
}

// ReverseStream joins an external group and unicasts into the cluster.
type ReverseStream struct {
	Name      string   `json:"name" yaml:"name"`
	JoinGroup string   `json:"join_group" yaml:"join_group"`
	Port      int      `json:"port" yaml:"port"`
	Interface string   `json:"interface" yaml:"interface"`
	Sources   []string `json:"sources" yaml:"sources"`
	Targets   []string `json:"targets" yaml:"targets"`
}

type rawConfig struct {
	EgressInterface  string       `yaml:"egress_interface"`
	MetricsListen    string       `yaml:"metrics_listen"`
	LogLevel         string       `yaml:"log_level"`
	BatchSize        *int         `yaml:"batch_size"`
	ReadBufferBytes  *int         `yaml:"read_buffer_bytes"`
	WriteBufferBytes *int         `yaml:"write_buffer_bytes"`
	MaxDatagramBytes *int         `yaml:"max_datagram_bytes"`
	Streams          []rawStream  `yaml:"streams"`
	ReverseStreams   []rawReverse `yaml:"reverse_streams"`
}

type rawStream struct {
	Name               string   `yaml:"name"`
	Listen             string   `yaml:"listen"`
	Group              string   `yaml:"group"`
	Port               int      `yaml:"port"`
	TTL                *int     `yaml:"ttl"`
	Loop               *bool    `yaml:"loop"`
	SourcePort         int      `yaml:"source_port"`
	PreserveSourcePort bool     `yaml:"preserve_source_port"`
	AllowedSources     []string `yaml:"allowed_sources"`
	MaxDatagramBytes   *int     `yaml:"max_datagram_bytes"`
}

type rawReverse struct {
	Name      string   `yaml:"name"`
	JoinGroup string   `yaml:"join_group"`
	Port      int      `yaml:"port"`
	Interface string   `yaml:"interface"`
	Sources   []string `yaml:"sources"`
	Targets   []string `yaml:"targets"`
}

var envRe = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}`)
var nameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$`)

// Load reads path, expands ${VAR} and ${VAR:-default}, applies environment
// overrides, fills defaults, and validates the result.
func Load(path string) (*Config, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cfg, err := Parse(buf)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// Parse validates a YAML document that is already in memory.
func Parse(buf []byte) (*Config, error) {
	expanded, err := expandEnv(buf)
	if err != nil {
		return nil, err
	}
	dec := yaml.NewDecoder(bytes.NewReader(expanded))
	dec.KnownFields(true)
	var raw rawConfig
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); err == nil && extra.Kind != 0 {
		return nil, fmt.Errorf("parse yaml: multiple documents are not supported")
	}
	if err := applyEnvOverrides(&raw); err != nil {
		return nil, err
	}
	cfg := normalize(raw)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func expandEnv(in []byte) ([]byte, error) {
	var missing []string
	out := envRe.ReplaceAllFunc(in, func(match []byte) []byte {
		sub := envRe.FindSubmatch(match)
		key := string(sub[1])
		if v, ok := os.LookupEnv(key); ok {
			return []byte(v)
		}
		if bytes.Contains(match, []byte(":-")) {
			return sub[2]
		}
		missing = append(missing, key)
		return match
	})
	if len(missing) > 0 {
		return nil, fmt.Errorf("unresolved environment variables: %s", strings.Join(missing, ", "))
	}
	return out, nil
}

func applyEnvOverrides(raw *rawConfig) error {
	if v := os.Getenv("MCAST_EGRESS_INTERFACE"); v != "" {
		raw.EgressInterface = v
	}
	if v := os.Getenv("MCAST_METRICS_LISTEN"); v != "" {
		raw.MetricsListen = v
	}
	if v := os.Getenv("MCAST_LOG_LEVEL"); v != "" {
		raw.LogLevel = v
	}
	setInt := func(key string, dst **int) error {
		v := os.Getenv(key)
		if v == "" {
			return nil
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		*dst = &n
		return nil
	}
	if err := setInt("MCAST_BATCH_SIZE", &raw.BatchSize); err != nil {
		return err
	}
	if err := setInt("MCAST_READ_BUFFER_BYTES", &raw.ReadBufferBytes); err != nil {
		return err
	}
	if err := setInt("MCAST_WRITE_BUFFER_BYTES", &raw.WriteBufferBytes); err != nil {
		return err
	}
	if err := setInt("MCAST_MAX_DATAGRAM_BYTES", &raw.MaxDatagramBytes); err != nil {
		return err
	}
	return nil
}

func normalize(raw rawConfig) *Config {
	cfg := &Config{
		EgressInterface: raw.EgressInterface,
		MetricsListen:   raw.MetricsListen,
		LogLevel:        strings.ToLower(strings.TrimSpace(raw.LogLevel)),
	}
	if cfg.MetricsListen == "" {
		cfg.MetricsListen = DefaultMetricsListen
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = DefaultLogLevel
	}
	if cfg.LogLevel == "warning" {
		cfg.LogLevel = "warn"
	}
	cfg.BatchSize = DefaultBatchSize
	if raw.BatchSize != nil {
		cfg.BatchSize = *raw.BatchSize
	}
	cfg.ReadBufferBytes = DefaultBufferBytes
	if raw.ReadBufferBytes != nil {
		cfg.ReadBufferBytes = *raw.ReadBufferBytes
	}
	cfg.WriteBufferBytes = DefaultBufferBytes
	if raw.WriteBufferBytes != nil {
		cfg.WriteBufferBytes = *raw.WriteBufferBytes
	}
	cfg.MaxDatagramBytes = DefaultMaxDatagram
	if raw.MaxDatagramBytes != nil {
		cfg.MaxDatagramBytes = *raw.MaxDatagramBytes
	}
	for _, s := range raw.Streams {
		st := Stream{
			Name:               s.Name,
			Listen:             s.Listen,
			Group:              s.Group,
			Port:               s.Port,
			TTL:                DefaultTTL,
			SourcePort:         s.SourcePort,
			PreserveSourcePort: s.PreserveSourcePort,
			AllowedSources:     append([]string(nil), s.AllowedSources...),
			MaxDatagramBytes:   cfg.MaxDatagramBytes,
		}
		if s.TTL != nil {
			st.TTL = *s.TTL
		}
		if s.Loop != nil {
			st.Loop = *s.Loop
		}
		if s.MaxDatagramBytes != nil {
			st.MaxDatagramBytes = *s.MaxDatagramBytes
		}
		cfg.Streams = append(cfg.Streams, st)
	}
	for _, s := range raw.ReverseStreams {
		iface := s.Interface
		if iface == "" {
			iface = cfg.EgressInterface
		}
		cfg.ReverseStreams = append(cfg.ReverseStreams, ReverseStream{
			Name:      s.Name,
			JoinGroup: s.JoinGroup,
			Port:      s.Port,
			Interface: iface,
			Sources:   append([]string(nil), s.Sources...),
			Targets:   append([]string(nil), s.Targets...),
		})
	}
	return cfg
}

// Validate checks the configuration and returns every problem it finds.
func (c *Config) Validate() error {
	var errs []string
	if strings.TrimSpace(c.EgressInterface) == "" {
		errs = append(errs, "egress_interface is required")
	} else if msg := checkIface(c.EgressInterface); msg != "" {
		errs = append(errs, msg)
	}
	if _, _, err := splitHostPort(c.MetricsListen); err != nil {
		errs = append(errs, fmt.Sprintf("metrics_listen: %v", err))
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Sprintf("log_level %q is not one of debug, info, warn, error", c.LogLevel))
	}
	if c.BatchSize < 1 || c.BatchSize > maxBatchSize {
		errs = append(errs, fmt.Sprintf("batch_size %d is outside 1-%d", c.BatchSize, maxBatchSize))
	}
	if c.ReadBufferBytes < 2048 {
		errs = append(errs, "read_buffer_bytes must be at least 2048")
	}
	if c.WriteBufferBytes < 2048 {
		errs = append(errs, "write_buffer_bytes must be at least 2048")
	}
	if c.MaxDatagramBytes < 1 || c.MaxDatagramBytes > 65535 {
		errs = append(errs, fmt.Sprintf("max_datagram_bytes %d is outside 1-65535", c.MaxDatagramBytes))
	}

	names := map[string]string{}
	var binds []socketBind
	for i := range c.Streams {
		s := &c.Streams[i]
		prefix := fmt.Sprintf("streams[%d]", i)
		if s.Name != "" {
			prefix = fmt.Sprintf("stream %q", s.Name)
		}
		if !nameRe.MatchString(s.Name) {
			errs = append(errs, fmt.Sprintf("%s: name must match %s", prefix, nameRe.String()))
		} else if prev, ok := names[s.Name]; ok {
			errs = append(errs, fmt.Sprintf("%s: duplicate name also used by %s", prefix, prev))
		} else {
			names[s.Name] = prefix
		}
		ip, port, err := splitHostPort(s.Listen)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: listen: %v", prefix, err))
		} else {
			binds = append(binds, socketBind{name: prefix, ip: ip, port: port})
			if ip != nil {
				s.Listen = net.JoinHostPort(ip.String(), strconv.Itoa(port))
			} else {
				s.Listen = net.JoinHostPort("0.0.0.0", strconv.Itoa(port))
			}
		}
		g, gerr := multicastIP(s.Group)
		if gerr != nil {
			errs = append(errs, fmt.Sprintf("%s: group: %v", prefix, gerr))
		} else {
			s.Group = g.String()
		}
		if s.Port < 1 || s.Port > 65535 {
			errs = append(errs, fmt.Sprintf("%s: port %d is outside 1-65535", prefix, s.Port))
		}
		if s.TTL < 0 || s.TTL > 255 {
			errs = append(errs, fmt.Sprintf("%s: ttl %d is outside 0-255", prefix, s.TTL))
		}
		if s.SourcePort < 0 || s.SourcePort > 65535 {
			errs = append(errs, fmt.Sprintf("%s: source_port %d is outside 0-65535", prefix, s.SourcePort))
		}
		if s.PreserveSourcePort && s.SourcePort != 0 {
			errs = append(errs, fmt.Sprintf("%s: preserve_source_port and source_port are mutually exclusive", prefix))
		}
		if s.MaxDatagramBytes < 1 || s.MaxDatagramBytes > 65535 {
			errs = append(errs, fmt.Sprintf("%s: max_datagram_bytes %d is outside 1-65535", prefix, s.MaxDatagramBytes))
		}
		canon := make([]string, 0, len(s.AllowedSources))
		for _, src := range s.AllowedSources {
			c, err := acl.Canonical(src)
			if err != nil {
				errs = append(errs, fmt.Sprintf("%s: allowed_sources: %v", prefix, err))
				continue
			}
			canon = append(canon, c)
		}
		sort.Strings(canon)
		s.AllowedSources = canon
	}

	for i := range c.ReverseStreams {
		s := &c.ReverseStreams[i]
		prefix := fmt.Sprintf("reverse_streams[%d]", i)
		if s.Name != "" {
			prefix = fmt.Sprintf("reverse stream %q", s.Name)
		}
		if !nameRe.MatchString(s.Name) {
			errs = append(errs, fmt.Sprintf("%s: name must match %s", prefix, nameRe.String()))
		} else if prev, ok := names[s.Name]; ok {
			errs = append(errs, fmt.Sprintf("%s: duplicate name also used by %s", prefix, prev))
		} else {
			names[s.Name] = prefix
		}
		g, gerr := multicastIP(s.JoinGroup)
		if gerr != nil {
			errs = append(errs, fmt.Sprintf("%s: join_group: %v", prefix, gerr))
		} else {
			s.JoinGroup = g.String()
		}
		if s.Port < 1 || s.Port > 65535 {
			errs = append(errs, fmt.Sprintf("%s: port %d is outside 1-65535", prefix, s.Port))
		} else if gerr == nil {
			binds = append(binds, socketBind{name: prefix, ip: g, port: s.Port})
		}
		if strings.TrimSpace(s.Interface) == "" {
			errs = append(errs, fmt.Sprintf("%s: interface is required", prefix))
		} else if s.Interface != c.EgressInterface {
			if msg := checkIface(s.Interface); msg != "" {
				errs = append(errs, fmt.Sprintf("%s: %s", prefix, msg))
			}
		}
		if len(s.Targets) == 0 {
			errs = append(errs, fmt.Sprintf("%s: at least one target is required", prefix))
		}
		for j, tgt := range s.Targets {
			ip, port, err := splitHostPort(tgt)
			if err != nil || ip == nil {
				errs = append(errs, fmt.Sprintf("%s: targets[%d]: need an IPv4 host:port", prefix, j))
				continue
			}
			if ip.IsMulticast() {
				errs = append(errs, fmt.Sprintf("%s: targets[%d]: %s is multicast; reverse targets are unicast", prefix, j, ip))
				continue
			}
			s.Targets[j] = net.JoinHostPort(ip.String(), strconv.Itoa(port))
		}
		canon := make([]string, 0, len(s.Sources))
		for _, src := range s.Sources {
			ip := net.ParseIP(src)
			if ip == nil || ip.To4() == nil || ip.IsMulticast() {
				errs = append(errs, fmt.Sprintf("%s: sources: %q is not an IPv4 unicast address", prefix, src))
				continue
			}
			canon = append(canon, ip.To4().String())
		}
		sort.Strings(canon)
		s.Sources = canon
	}

	for i := 0; i < len(binds); i++ {
		for j := i + 1; j < len(binds); j++ {
			if binds[i].overlaps(binds[j]) {
				errs = append(errs, fmt.Sprintf("port conflict between %s and %s on UDP %d", binds[i].name, binds[j].name, binds[i].port))
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("invalid configuration:\n  %s", strings.Join(errs, "\n  "))
	}
	return nil
}

// ForwardSignature identifies a forward stream and the process settings
// that require its sockets to be reopened.
func (c *Config) ForwardSignature(s Stream) string {
	payload := struct {
		Stream           Stream `json:"stream"`
		Egress           string `json:"egress"`
		BatchSize        int    `json:"batch_size"`
		ReadBufferBytes  int    `json:"read_buffer_bytes"`
		WriteBufferBytes int    `json:"write_buffer_bytes"`
	}{s, c.EgressInterface, c.BatchSize, c.ReadBufferBytes, c.WriteBufferBytes}
	b, _ := json.Marshal(payload)
	return "f:" + string(b)
}

// ReverseSignature identifies a reverse stream and the settings that
// require its sockets to be reopened.
func (c *Config) ReverseSignature(s ReverseStream) string {
	payload := struct {
		Stream           ReverseStream `json:"stream"`
		BatchSize        int           `json:"batch_size"`
		ReadBufferBytes  int           `json:"read_buffer_bytes"`
		WriteBufferBytes int           `json:"write_buffer_bytes"`
		MaxDatagramBytes int           `json:"max_datagram_bytes"`
	}{s, c.BatchSize, c.ReadBufferBytes, c.WriteBufferBytes, c.MaxDatagramBytes}
	b, _ := json.Marshal(payload)
	return "r:" + string(b)
}

type socketBind struct {
	name string
	ip   net.IP // nil is a wildcard
	port int
}

func (a socketBind) overlaps(b socketBind) bool {
	if a.port != b.port {
		return false
	}
	if a.ip == nil || b.ip == nil {
		return true
	}
	return a.ip.Equal(b.ip)
}

func splitHostPort(addr string) (net.IP, int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, 0, fmt.Errorf("%q: %w", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return nil, 0, fmt.Errorf("%q: port must be 1-65535", addr)
	}
	if host == "" || host == "0.0.0.0" {
		return nil, port, nil
	}
	ip := net.ParseIP(host)
	if ip == nil || ip.To4() == nil {
		return nil, 0, fmt.Errorf("%q: host must be an IPv4 address", addr)
	}
	return ip.To4(), port, nil
}

func multicastIP(s string) (net.IP, error) {
	ip := net.ParseIP(s)
	if ip == nil || ip.To4() == nil || !ip.IsMulticast() {
		return nil, fmt.Errorf("%q is outside the IPv4 multicast range 224.0.0.0-239.255.255.255", s)
	}
	return ip.To4(), nil
}

// InterfaceReady reports whether name exists, is up, and supports multicast.
func InterfaceReady(name string) error {
	if msg := checkIface(name); msg != "" {
		return fmt.Errorf("%s", msg)
	}
	return nil
}

func checkIface(name string) string {
	ifi, err := net.InterfaceByName(name)
	if err != nil {
		return fmt.Sprintf("interface %q: %v", name, err)
	}
	if ifi.Flags&net.FlagUp == 0 {
		return fmt.Sprintf("interface %q is down", name)
	}
	if ifi.Flags&net.FlagMulticast == 0 {
		return fmt.Sprintf("interface %q does not support multicast", name)
	}
	return ""
}

// LowMulticastGroup reports whether ip is in 224.0.0.0/24. Routers do not
// forward that block, and stacks often force its TTL to 1.
func LowMulticastGroup(ip string) bool {
	parsed := net.ParseIP(ip).To4()
	if parsed == nil {
		return false
	}
	return parsed[0] == 224 && parsed[1] == 0 && parsed[2] == 0
}
