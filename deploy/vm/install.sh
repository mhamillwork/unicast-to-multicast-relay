#!/bin/sh
# Install the relay binary, sample config, and systemd unit.
set -eu
ROOT="$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)"
PREFIX="${PREFIX:-/usr/local}"
CONFIG_DIR=/etc/mcast-relay

if [ "$(id -u)" -ne 0 ]; then
  echo "run as root" >&2
  exit 1
fi

if [ ! -x "$ROOT/bin/mcast-relay" ]; then
  echo "build first: make build" >&2
  exit 1
fi

if ! id mcast-relay >/dev/null 2>&1; then
  useradd --system --home-dir /nonexistent --shell /usr/sbin/nologin mcast-relay
fi

install -d -m 0755 "$PREFIX/bin" "$PREFIX/libexec" "$CONFIG_DIR" /etc/igmpproxy /etc/pimd /etc/keepalived
install -m 0755 "$ROOT/bin/mcast-relay" "$PREFIX/bin/mcast-relay"
install -m 0755 "$ROOT/deploy/vm/mcast-relay-ready" "$PREFIX/libexec/mcast-relay-ready"
install -m 0644 "$ROOT/deploy/vm/mcast-relay.service" /etc/systemd/system/mcast-relay.service
if [ ! -f "$CONFIG_DIR/config.yaml" ]; then
  install -m 0644 "$ROOT/configs/config.example.yaml" "$CONFIG_DIR/config.yaml"
fi
install -m 0644 "$ROOT/deploy/vm/igmpproxy.conf.example" /etc/igmpproxy/igmpproxy.conf.example
install -m 0644 "$ROOT/deploy/vm/pimd.conf.example" /etc/pimd/pimd.conf.example
install -m 0644 "$ROOT/deploy/vm/keepalived.conf.example" /etc/keepalived/keepalived.conf.example
if [ -d /etc/sysctl.d ]; then
  install -m 0644 "$ROOT/deploy/vm/sysctl.d/99-mcast-relay.conf" /etc/sysctl.d/99-mcast-relay.conf
fi

systemctl daemon-reload
echo "installed. Edit $CONFIG_DIR/config.yaml, then: systemctl enable --now mcast-relay"
