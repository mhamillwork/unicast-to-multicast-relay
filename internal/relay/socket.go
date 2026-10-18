package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"time"

	"golang.org/x/net/ipv4"
	"golang.org/x/sys/unix"
)

// recvBatchFlags is passed to recvmmsg. MSG_WAITFORONE blocks until the
// first datagram arrives, then returns whatever else is already queued
// instead of waiting for the batch to fill.
const recvBatchFlags = unix.MSG_WAITFORONE

const maxPreservePorts = 4096

func listenUDP(addr string, reuse bool) (*net.UDPConn, error) {
	var lc net.ListenConfig
	if reuse {
		lc.Control = func(_, _ string, c syscall.RawConn) error {
			var sockErr error
			if err := c.Control(func(fd uintptr) {
				sockErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
			}); err != nil {
				return err
			}
			return sockErr
		}
	}
	pc, err := lc.ListenPacket(context.Background(), "udp4", addr)
	if err != nil {
		return nil, err
	}
	udp, ok := pc.(*net.UDPConn)
	if !ok {
		pc.Close()
		return nil, fmt.Errorf("unexpected packet conn %T", pc)
	}
	return udp, nil
}

func wrap(c *net.UDPConn) *ipv4.PacketConn {
	return ipv4.NewPacketConn(c)
}

func configureSend(pc *ipv4.PacketConn, conn *net.UDPConn, ifi *net.Interface, ip net.IP, ttl int, loop bool) error {
	if err := pc.SetMulticastInterface(ifi); err != nil {
		return fmt.Errorf("set IP_MULTICAST_IF %s: %w", ifi.Name, err)
	}
	// Also set the classic in_addr form. Some stacks accept the ip_mreqn
	// ifindex form used by x/net and then ignore it. The address form is
	// honored everywhere and pins egress to the interface that owns ip.
	ip4 := ip.To4()
	if ip4 == nil {
		return fmt.Errorf("interface %s has no IPv4 address for IP_MULTICAST_IF", ifi.Name)
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		return fmt.Errorf("socket control: %w", err)
	}
	var opErr error
	if err := raw.Control(func(fd uintptr) {
		var addr [4]byte
		copy(addr[:], ip4)
		opErr = unix.SetsockoptInet4Addr(int(fd), unix.IPPROTO_IP, unix.IP_MULTICAST_IF, addr)
	}); err != nil {
		return fmt.Errorf("set IP_MULTICAST_IF %s: %w", ip4, err)
	}
	if opErr != nil {
		return fmt.Errorf("set IP_MULTICAST_IF %s: %w", ip4, opErr)
	}
	if err := pc.SetMulticastTTL(ttl); err != nil {
		return fmt.Errorf("set IP_MULTICAST_TTL %d: %w", ttl, err)
	}
	if err := pc.SetMulticastLoopback(loop); err != nil {
		return fmt.Errorf("set IP_MULTICAST_LOOP: %w", err)
	}
	return nil
}

func setBuffers(c *net.UDPConn, readBuf, writeBuf int) error {
	if readBuf > 0 {
		if err := c.SetReadBuffer(readBuf); err != nil {
			return fmt.Errorf("set SO_RCVBUF: %w", err)
		}
	}
	if writeBuf > 0 {
		if err := c.SetWriteBuffer(writeBuf); err != nil {
			return fmt.Errorf("set SO_SNDBUF: %w", err)
		}
	}
	return nil
}

func ifaceIPv4(ifi *net.Interface) (net.IP, error) {
	addrs, err := ifi.Addrs()
	if err != nil {
		return nil, err
	}
	var fallback net.IP
	for _, a := range addrs {
		ipn, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		ip := ipn.IP.To4()
		if ip == nil || ip.IsLinkLocalUnicast() {
			continue
		}
		if !ip.IsLoopback() {
			return ip, nil
		}
		if fallback == nil {
			fallback = ip
		}
	}
	if fallback != nil {
		return fallback, nil
	}
	return nil, fmt.Errorf("interface %s has no IPv4 address", ifi.Name)
}

func newBatch(n, size int) ([]ipv4.Message, [][]byte) {
	msgs := make([]ipv4.Message, n)
	bufs := make([][]byte, n)
	for i := range msgs {
		bufs[i] = make([]byte, size)
		msgs[i].Buffers = [][]byte{bufs[i]}
	}
	return msgs, bufs
}

func resetBatch(msgs []ipv4.Message, bufs [][]byte, n int) {
	for i := 0; i < n; i++ {
		msgs[i].Buffers[0] = bufs[i]
		msgs[i].N = 0
		msgs[i].NN = 0
		msgs[i].Flags = 0
		msgs[i].OOB = nil
		msgs[i].Addr = nil
	}
}

// writeAll sends msgs with sendmmsg. A single oversized datagram is dropped
// and the rest of the batch is retried. Benign resource errors drop the
// remainder without tearing the socket down.
func writeAll(pc *ipv4.PacketConn, msgs []ipv4.Message, onSent func(n, bytes int), onDrop func(reason string, n int)) error {
	for len(msgs) > 0 {
		k, err := pc.WriteBatch(msgs, 0)
		if k > 0 {
			bytes := 0
			for i := 0; i < k; i++ {
				if len(msgs[i].Buffers) > 0 {
					bytes += len(msgs[i].Buffers[0])
				}
			}
			onSent(k, bytes)
			msgs = msgs[k:]
		}
		if err == nil {
			if k == 0 {
				return io.ErrShortWrite
			}
			continue
		}
		if errors.Is(err, syscall.EMSGSIZE) {
			onDrop("oversize", 1)
			if k == 0 && len(msgs) > 0 {
				msgs = msgs[1:]
			}
			continue
		}
		if benignSend(err) {
			onDrop("send_error", len(msgs))
			return nil
		}
		onDrop("send_error", len(msgs))
		return err
	}
	return nil
}

func benignSend(err error) bool {
	return errors.Is(err, syscall.ENOBUFS) ||
		errors.Is(err, syscall.EAGAIN) ||
		errors.Is(err, syscall.EWOULDBLOCK) ||
		errors.Is(err, syscall.EINTR) ||
		errors.Is(err, syscall.ENETDOWN) ||
		errors.Is(err, syscall.ENETUNREACH) ||
		errors.Is(err, syscall.EHOSTUNREACH) ||
		errors.Is(err, syscall.ENODEV)
}

func temporaryRead(err error) bool {
	if errors.Is(err, syscall.EINTR) || errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func wakeOnCancel(ctx context.Context, get func() *ipv4.PacketConn) func() bool {
	return context.AfterFunc(ctx, func() {
		if p := get(); p != nil {
			_ = p.SetReadDeadline(time.Now())
		}
	})
}
