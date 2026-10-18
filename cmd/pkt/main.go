// Command pkt sends or receives UDP datagrams. It is the traffic generator
// used by the kind test and by manual checks.
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"golang.org/x/net/ipv4"
	"golang.org/x/sys/unix"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "send":
		err = send(os.Args[2:])
	case "recv":
		err = recv(os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "pkt: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage:
  pkt send --dst host:port [--count N] [--payload text] [--interval 10ms] [--src-port N]
  pkt recv --group 239.1.1.10 --port N --iface eth0 [--count N] [--timeout 10s] [--expect text]
`)
}

func send(args []string) error {
	fs := flag.NewFlagSet("send", flag.ContinueOnError)
	dst := fs.String("dst", "", "unicast or multicast host:port")
	count := fs.Int("count", 1, "number of datagrams")
	payload := fs.String("payload", "mcast-relay", "payload string")
	interval := fs.Duration("interval", 0, "delay between datagrams")
	srcPort := fs.Int("src-port", 0, "local UDP source port")
	ifaceName := fs.String("iface", "", "multicast egress interface")
	ttl := fs.Int("ttl", 1, "multicast TTL")
	loop := fs.Bool("loop", true, "IP_MULTICAST_LOOP")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dst == "" || *count < 1 {
		return fmt.Errorf("--dst and a positive --count are required")
	}
	remote, err := net.ResolveUDPAddr("udp4", *dst)
	if err != nil {
		return err
	}
	var local *net.UDPAddr
	if *srcPort != 0 {
		local = &net.UDPAddr{IP: net.IPv4zero, Port: *srcPort}
	}
	conn, err := net.ListenUDP("udp4", local)
	if err != nil {
		return err
	}
	defer conn.Close()
	if remote.IP.IsMulticast() {
		pc := ipv4.NewPacketConn(conn)
		if *ifaceName != "" {
			ifi, err := net.InterfaceByName(*ifaceName)
			if err != nil {
				return err
			}
			if err := pc.SetMulticastInterface(ifi); err != nil {
				return err
			}
			if ip4 := firstIPv4(ifi); ip4 != nil {
				raw, err := conn.SyscallConn()
				if err != nil {
					return err
				}
				var opErr error
				if err := raw.Control(func(fd uintptr) {
					var addr [4]byte
					copy(addr[:], ip4)
					opErr = unix.SetsockoptInet4Addr(int(fd), unix.IPPROTO_IP, unix.IP_MULTICAST_IF, addr)
				}); err != nil {
					return err
				}
				if opErr != nil {
					return opErr
				}
			}
		}
		if err := pc.SetMulticastTTL(*ttl); err != nil {
			return err
		}
		if err := pc.SetMulticastLoopback(*loop); err != nil {
			return err
		}
	}
	body := []byte(*payload)
	for i := 0; i < *count; i++ {
		if _, err := conn.WriteToUDP(body, remote); err != nil {
			return err
		}
		if *interval > 0 && i+1 < *count {
			time.Sleep(*interval)
		}
	}
	return nil
}

func firstIPv4(ifi *net.Interface) net.IP {
	addrs, err := ifi.Addrs()
	if err != nil {
		return nil
	}
	for _, a := range addrs {
		ipn, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		if ip := ipn.IP.To4(); ip != nil && !ip.IsLinkLocalUnicast() {
			return ip
		}
	}
	return nil
}

func recv(args []string) error {
	fs := flag.NewFlagSet("recv", flag.ContinueOnError)
	group := fs.String("group", "", "multicast group")
	port := fs.Int("port", 0, "UDP port")
	ifaceName := fs.String("iface", "", "interface to join on")
	count := fs.Int("count", 1, "datagrams to receive")
	timeout := fs.Duration("timeout", 10*time.Second, "overall timeout")
	expect := fs.String("expect", "", "required payload, empty skips the check")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ip := net.ParseIP(*group).To4()
	if ip == nil || !ip.IsMulticast() || *port < 1 || *ifaceName == "" || *count < 1 {
		return fmt.Errorf("--group, --port, and --iface are required")
	}
	ifi, err := net.InterfaceByName(*ifaceName)
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: *port})
	if err != nil {
		return err
	}
	defer conn.Close()
	pc := ipv4.NewPacketConn(conn)
	if err := pc.JoinGroup(ifi, &net.UDPAddr{IP: ip}); err != nil {
		return fmt.Errorf("join %s on %s: %w", ip, ifi.Name, err)
	}
	if err := conn.SetReadBuffer(4 << 20); err != nil {
		return err
	}
	deadline := time.Now().Add(*timeout)
	buf := make([]byte, 65535)
	for got := 0; got < *count; got++ {
		if err := conn.SetReadDeadline(deadline); err != nil {
			return err
		}
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			return fmt.Errorf("received %d/%d: %w", got, *count, err)
		}
		if *expect != "" && string(buf[:n]) != *expect {
			return fmt.Errorf("datagram %d payload %q", got, buf[:n])
		}
	}
	fmt.Printf("received %d datagrams on %s%%%s\n", *count, ip, ifi.Name)
	return nil
}
