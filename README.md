# Unicast-to-multicast edge relay

Cilium can copy multicast between pods, but only inside the cluster and only in VXLAN or Geneve tunnel mode. On the way out of a node that traffic is unicast, so a receiver on the physical network that joined `239.x.x.x` never sees it.

`mcast-relay` runs at the edge, on a gateway node or on a VM. Pods send ordinary unicast UDP to it. The relay emits the same payload, byte for byte, as multicast on an interface you choose. It does not change Cilium, and it does not speak PIM or IGMP routing. `pimd`, `igmpproxy`, or `smcroute` do that when receivers sit behind a router.

```
[Pod] --unicast UDP--> [Cilium / node] --> [relay listen ip:port]
                                              |
                                              v
                                    [egress NIC] --multicast--> 239.x.x.x:port
                                              |
                                   optional pimd / igmpproxy --> routed receivers
```

The reverse direction is optional and per stream: the relay joins a group on the outside and unicasts each datagram to one or more in-cluster targets.

## Assumptions

The relay is IPv4 UDP only. It is aimed at market data, discovery (SSDP and similar), and any other UDP multicast whose senders can be pointed at a unicast address. Same-LAN receivers use the default TTL of 1. Receivers behind a router need `ttl` of 2 or more plus `pimd` or `igmpproxy`. Reverse mode is implemented and stays off until a `reverse_streams` entry is configured. Both a gateway DaemonSet and a VM outside the cluster are supported.

## Build

Go 1.22 or newer. The binary is static (`CGO_ENABLED=0`) and has no runtime dependencies.

```bash
make build          # bin/mcast-relay and bin/pkt
make test           # unit tests, including a loopback forward
make lint
make integration    # root network-namespace test; needs iproute2
make loadtest       # print throughput and latency
make loadtest-nfr   # fail unless the relay holds 100k pkt/s and p99 < 1ms
make docker         # distroless image for the host architecture
make docker-multiarch
```

`make loadtest-nfr` is the check for the performance target: at least 100,000 packets per second and a relay-added p99 under 1ms. It uses `recvmmsg` / `sendmmsg` (`MSG_WAITFORONE`, so a quiet stream is not held until the batch fills). Raise `net.core.rmem_max` and `net.core.wmem_max` (see `deploy/vm/sysctl.d/99-mcast-relay.conf`) before expecting that rate. Loopback must have multicast enabled: `ip link set lo multicast on`.

## Configuration

The process reads a YAML file (`--config` or `MCAST_CONFIG`, default `/etc/mcast-relay/config.yaml`). `${VAR}` and `${VAR:-default}` expand before parsing. After that, these variables override the file:

| Variable | Field |
| --- | --- |
| `MCAST_EGRESS_INTERFACE` | `egress_interface` |
| `MCAST_METRICS_LISTEN` | `metrics_listen` |
| `MCAST_LOG_LEVEL` | `log_level` |
| `MCAST_BATCH_SIZE` | `batch_size` |
| `MCAST_READ_BUFFER_BYTES` | `read_buffer_bytes` |
| `MCAST_WRITE_BUFFER_BYTES` | `write_buffer_bytes` |
| `MCAST_MAX_DATAGRAM_BYTES` | `max_datagram_bytes` |

Startup validation exits with a non-zero status and a line for every problem: bad CIDR, port conflict, missing or down interface, interface without multicast, group outside `224.0.0.0/4`, TTL outside 0–255, duplicate stream name. `SIGHUP` reloads the file. Streams whose settings did not change keep their sockets. A rejected reload leaves the previous configuration running. One stream's socket error is retried with backoff and does not stop the others.

```yaml
egress_interface: eth1
metrics_listen: 0.0.0.0:9100
log_level: info          # debug, info, warn, error. Logs are JSON.
batch_size: 64           # recvmmsg batch, 1-1024
read_buffer_bytes: 8388608
write_buffer_bytes: 8388608
max_datagram_bytes: 65535
streams:
  - name: market-feed-a
    listen: 0.0.0.0:5000
    group: 239.1.1.10
    port: 5000           # multicast destination port; may differ from listen
    ttl: 4               # default 1. 0 stays on the host. Use >1 to cross a router.
    loop: false          # IP_MULTICAST_LOOP, default false
    source_port: 0       # 0 means "use the listen port"
    preserve_source_port: false
    allowed_sources: ["10.0.0.0/8"]   # empty allows every IPv4 source
    max_datagram_bytes: 9000
reverse_streams:
  - name: ext-status
    join_group: 239.2.2.2
    port: 6000
    interface: eth1      # defaults to egress_interface
    sources: []          # optional IPv4 sources; non-empty means IGMPv3 SSM
    targets: ["10.96.12.34:6000"]
```

`preserve_source_port` and `source_port` cannot both be set. Preserving the sender's port binds a UDP socket on the egress address for that port (ordinary UDP, no raw socket). Ports below 1024 need `CAP_NET_BIND_SERVICE`. If a bind fails, that datagram is sent from the stream's default socket and `mcast_relay_source_port_fallbacks_total` increments.

An empty `allowed_sources` list allows every source. Otherwise the source address must sit in one of the CIDRs (a bare address is a `/32`).

Payloads are not rewritten. The read buffer defaults to 65535 bytes, so jumbo frames up to the interface MTU are forwarded. A datagram larger than `max_datagram_bytes` is counted as an oversize drop.

### Metrics, logs, health

`metrics_listen` serves:

- `GET /healthz` — process is up
- `GET /readyz` — every stream is bound and the egress interface is up
- `GET /metrics` — Prometheus

Per stream (`direction` is `forward` or `reverse`):

- `mcast_relay_packets_in_total`, `mcast_relay_packets_out_total`
- `mcast_relay_bytes_in_total`, `mcast_relay_bytes_out_total`
- `mcast_relay_drops_total{reason="acl|oversize|send_error"}`
- `mcast_relay_forward_latency_seconds` — time from a batch read returning to the matching send finishing
- `mcast_relay_stream_up`

Reverse fan-out counts one output packet per target. Import `deploy/grafana/mcast-relay.json` for the dashboard.

Logs are JSON on stdout. Per-packet lines are not emitted; drops show up in metrics. Bind and send failures are logged, send errors at most once a second per stream.

## Kubernetes

Label the gateway nodes and, if they are tainted, tolerate `node-role/multicast-gateway`:

```bash
kubectl label node gateway-1 node-role/multicast-gateway=true
kubectl taint node gateway-1 node-role/multicast-gateway=true:NoSchedule
```

The DaemonSet uses `hostNetwork: true` so multicast is transmitted on the node's NIC (`eth1` in the sample), not on a veth inside the pod network. The container runs as uid 65532, with a read-only root filesystem and every capability dropped. UDP sockets do not need `CAP_NET_RAW`.

```bash
helm upgrade --install mcast-relay deploy/k8s/helm/mcast-relay \
  --set image.repository=ghcr.io/example/mcast-relay \
  --set image.tag=0.1.0 \
  --set serviceMonitor.enabled=true \
  --set ciliumNetworkPolicy.enabled=true
```

Or without Helm:

```bash
kubectl apply -k deploy/k8s/kustomize
kubectl apply -k deploy/k8s/kustomize/cilium       # needs the Cilium CRDs
kubectl apply -k deploy/k8s/kustomize/monitoring  # needs a ServiceMonitor CRD
```

Edit `config.yaml` in the chart values or the kustomize ConfigMap so `egress_interface` is the NIC that faces the receivers. Probes call port 9100.

### Where senders send

Pick one of these. Mixing a load-balanced Service with a failover VIP sends some datagrams to a node that does not hold the VIP.

1. **Service ClusterIP.** Works with `hostNetwork` because kube-proxy or Cilium DNATs to the node IP. Fine for a single gateway. With two gateways this is active/active (see below).
2. **Node IP.** Send straight to the gateway node's InternalIP and the listen port. No Service required.
3. **VIP.** MetalLB, kube-vip, or keepalived. Put the address in `service.externalIPs` only when that address really lives on the gateway node. For keepalived, skip the Service and send to the VIP; the relay listens on `0.0.0.0`.

Example policy, also rendered by the chart when `ciliumNetworkPolicy.enabled` is true. With `hostNetwork`, confirm Cilium is enforcing policy on that endpoint; host-network pods are not ordinary pod IPs.

```yaml
apiVersion: cilium.io/v2
kind: CiliumNetworkPolicy
metadata:
  name: mcast-relay
spec:
  endpointSelector:
    matchLabels:
      app.kubernetes.io/name: mcast-relay
  ingress:
    - fromEndpoints:
        - matchLabels:
            app: market-sender
      toPorts:
        - ports:
            - port: "5000"
              protocol: UDP
```

No Cilium settings have to change for the relay. The pod-to-relay path is unicast. Cilium multicast (`multicast-enabled=true`, VXLAN or Geneve) remains an in-cluster feature and is independent of this process. `test/kind/run.sh` installs Cilium 1.20 in tunnel mode with multicast enabled, pins the relay to a labeled worker, and checks that a container on the kind bridge (outside the cluster) receives the group.

## VM

```bash
make build
sudo ./deploy/vm/install.sh
sudoedit /etc/mcast-relay/config.yaml
sudo systemctl enable --now mcast-relay
sudo systemctl reload mcast-relay   # SIGHUP
```

The unit runs as user `mcast-relay`, with a read-only system tree and no extra capabilities. Sample configs are installed beside it:

- `/etc/igmpproxy/igmpproxy.conf.example`
- `/etc/pimd/pimd.conf.example`
- `/etc/keepalived/keepalived.conf.example`
- `/etc/sysctl.d/99-mcast-relay.conf`

Apply the sysctl file (`sysctl --system`) on the gateway. `rp_filter` must be 0 on the egress NIC or the kernel drops multicast and asymmetric traffic.

### igmpproxy

Use igmpproxy when this host is the router between the LAN where the relay transmits and a downstream LAN. Upstream is the source side (the relay's egress). Downstream faces the receivers. `ttl` on the stream must be greater than 1.

```
phyint eth1 upstream ratelimit 0 threshold 1
        altnet 10.0.0.0/8
phyint eth2 downstream ratelimit 0 threshold 1
```

### pimd

Use pimd when receivers are PIM-SM routers away from the gateway. Turn on multicast forwarding and run pimd on the same host as the relay so locally generated groups are registered:

```bash
sysctl net.ipv4.conf.all.mc_forwarding=1
# /etc/pimd.conf — see deploy/vm/pimd.conf.example
phyint eth1 enable
phyint eth2 enable
rp-address 192.0.2.50 239.0.0.0/8
```

`224.0.0.0/24` is link-local. The relay logs a warning for those groups; routers will not forward them. SSDP `239.255.255.250` is a normal group and is fine.

## High availability

### Active/standby (HA-1, HA-3)

Run the relay and keepalived on two hosts. Both use `state BACKUP`; the higher `priority` becomes master. Senders use the VIP. The relay listens on `0.0.0.0`, so whichever host owns the VIP receives the unicast.

`deploy/vm/keepalived.conf.example` tracks `/usr/local/libexec/mcast-relay-ready`, which GETs `/readyz` with a one-second timeout. The script has no weight, so two failures (`interval 1`, `fall 2`) move the instance to FAULT. keepalived then sends a priority-0 advert and the standby takes the VIP. With `advert_int 1` that completes in about two seconds, inside the three-second budget. Killing the process makes `/readyz` fail immediately (connection refused).

Do not give the track script a negative weight that leaves the failed node above the standby's priority. That never releases the VIP.

### Active/active (HA-2)

Two relays subscribed to the same unicast Service, or two hosts emitting the same group, both transmit. Receivers see duplicates. That is acceptable only when the application deduplicates (sequence numbers in the payload, for example). The relay does not deduplicate and does not elect a single sender. Prefer the VIP for feeds that must arrive once.

## Troubleshooting

**Receivers on the LAN see nothing.** `ttl` defaults to 1, which is correct for one L2 segment and fatal if a router is in the path. Confirm `egress_interface` is the NIC facing the receivers (`ip route get 239.1.1.10` is not enough; the socket option `IP_MULTICAST_IF` selects the NIC). `ip link show eth1` must include `UP` and `MULTICAST`. `loop: false` is right in production; set `loop: true` only when a receiver on the same host must see the group.

**A switch with IGMP snooping stays quiet.** The relay is a source, not a member, so it does not send a join for the groups it transmits. Receivers must join, and the switch must see those IGMP reports on the gateway's port. If the switch never forwards the group, mirror the port or temporarily disable snooping to confirm. `pimd` or `igmpproxy` is what generates the joins toward a router.

**Datagrams vanish on the gateway.** `rp_filter` drops them when the ingress and egress NICs differ. Set `net.ipv4.conf.all.rp_filter=0` and the same on the egress device. Check `mcast_relay_drops_total`: `acl` is the allow list, `oversize` is `max_datagram_bytes` or the path MTU (`EMSGSIZE`), `send_error` is a socket failure.

**Jumbo frames stop at 1500 bytes.** Set the MTU on the egress NIC and on every switch in the path. The default buffer accepts a 9000-byte payload; the NIC still has to.

**Checksum offload corrupts UDP multicast.** If a capture on the wire shows a bad UDP checksum and the relay metrics show the packet as sent, disable TX checksum offload on the egress NIC (`ethtool -K eth1 tx off`) and retest.

**`/readyz` is 503.** The body names the stream that is not bound, or says the egress interface is down. `journalctl -u mcast-relay` (or `kubectl logs`) has the bind error. A port conflict between a forward `listen` and a reverse `join_group` on the same port is rejected at startup.

**Cilium pod cannot reach the relay.** The relay is on the node IP, not a ClusterIP pod IP, when `hostNetwork` is set. Policy has to allow UDP to that node. In-cluster multicast between pods is unchanged and still requires Cilium's own multicast maps; this relay is only the path out.

## Layout

```
cmd/mcast-relay/     relay process
cmd/pkt/             small send/recv helper used by the kind test
internal/            config, ACL, forwarding, metrics, health
deploy/k8s/          Helm chart and Kustomize manifests
deploy/vm/           systemd unit, installer, igmpproxy, pimd, keepalived
deploy/grafana/      dashboard JSON
test/integration/    network-namespace forward test (build tag integration)
test/load/           throughput and latency
test/kind/           Cilium-on-kind end-to-end script
```
