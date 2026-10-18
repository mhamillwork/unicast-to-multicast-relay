#!/usr/bin/env bash
# Install Cilium with multicast on a kind cluster, run the relay on a labeled
# gateway node, and check that a container outside the cluster receives the
# group the relay emits.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER="${KIND_CLUSTER_NAME:-mcast-relay}"
CILIUM_VERSION="${CILIUM_VERSION:-1.20.3}"
IMAGE="${IMAGE:-mcast-relay:kind}"
PORT="${PORT:-35000}"
GROUP="${GROUP:-239.1.1.10}"
PAYLOAD="cilium-edge-relay"

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing $1" >&2; exit 1; }
}
need kind
need kubectl
need helm
need docker
need go

cleanup() {
  docker rm -f mcast-recv >/dev/null 2>&1 || true
  kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
kind create cluster --name "$CLUSTER" --config - <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: ${CLUSTER}
nodes:
  - role: control-plane
  - role: worker
networking:
  disableDefaultCNI: true
  kubeProxyMode: none
EOF

kubectl label node "${CLUSTER}-worker" node-role/multicast-gateway=true --overwrite

API_IP="$(docker inspect "${CLUSTER}-control-plane" --format '{{.NetworkSettings.Networks.kind.IPAddress}}')"
helm upgrade --install cilium "oci://quay.io/cilium/charts/cilium" \
  --version "$CILIUM_VERSION" \
  --namespace kube-system \
  --values "$ROOT/test/kind/cilium-values.yaml" \
  --set "k8sServiceHost=${API_IP}" \
  --set k8sServicePort=6443

kubectl -n kube-system rollout status ds/cilium --timeout=300s
kubectl -n kube-system rollout status deploy/cilium-operator --timeout=300s
got="$(kubectl -n kube-system get cm cilium-config -o jsonpath='{.data.multicast-enabled}')"
if [[ "$got" != "true" ]]; then
  echo "cilium multicast-enabled=${got:-<unset>}" >&2
  exit 1
fi

docker build -t "$IMAGE" "$ROOT"
kind load docker-image "$IMAGE" --name "$CLUSTER"

IFACE="$(docker exec "${CLUSTER}-worker" ip -o -4 route show default | awk '{print $5; exit}')"
NODE_IP="$(kubectl get node "${CLUSTER}-worker" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')"
echo "gateway ${CLUSTER}-worker ${NODE_IP} iface ${IFACE}"

# Docker bridge IGMP snooping drops multicast that has no membership yet.
BRIDGE="$(docker network inspect kind -f '{{index .Options "com.docker.network.bridge.name"}}' 2>/dev/null || true)"
if [[ -z "$BRIDGE" ]]; then
  BRIDGE="$(docker network inspect kind -f '{{.Id}}' | cut -c1-12)"
  BRIDGE="br-${BRIDGE}"
fi
if [[ -w "/sys/class/net/${BRIDGE}/bridge/multicast_snooping" ]]; then
  echo 0 | sudo tee "/sys/class/net/${BRIDGE}/bridge/multicast_snooping" >/dev/null || true
fi

kubectl -n default apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: mcast-relay
data:
  config.yaml: |
    egress_interface: ${IFACE}
    metrics_listen: 0.0.0.0:9100
    log_level: info
    streams:
      - name: edge
        listen: 0.0.0.0:${PORT}
        group: ${GROUP}
        port: ${PORT}
        ttl: 1
        loop: false
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: mcast-relay
  labels:
    app.kubernetes.io/name: mcast-relay
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: mcast-relay
  template:
    metadata:
      labels:
        app.kubernetes.io/name: mcast-relay
    spec:
      hostNetwork: true
      dnsPolicy: ClusterFirstWithHostNet
      nodeSelector:
        node-role/multicast-gateway: "true"
      tolerations:
        - key: node-role/multicast-gateway
          operator: Exists
          effect: NoSchedule
      containers:
        - name: mcast-relay
          image: ${IMAGE}
          imagePullPolicy: IfNotPresent
          args: ["--config", "/etc/mcast-relay/config.yaml"]
          securityContext:
            runAsNonRoot: true
            runAsUser: 65532
            readOnlyRootFilesystem: true
            allowPrivilegeEscalation: false
            capabilities:
              drop: ["ALL"]
          ports:
            - name: feed
              containerPort: ${PORT}
              protocol: UDP
            - name: metrics
              containerPort: 9100
              protocol: TCP
          readinessProbe:
            httpGet:
              path: /readyz
              port: metrics
          volumeMounts:
            - name: config
              mountPath: /etc/mcast-relay
              readOnly: true
      volumes:
        - name: config
          configMap:
            name: mcast-relay
EOF

kubectl -n default rollout status ds/mcast-relay --timeout=180s

docker rm -f mcast-recv >/dev/null 2>&1 || true
docker run -d --name mcast-recv --network kind --entrypoint /pkt "$IMAGE" \
  recv --group "$GROUP" --port "$PORT" --iface eth0 --count 20 --timeout 30s --expect "$PAYLOAD"

kubectl -n default delete pod sender --ignore-not-found
kubectl -n default run sender --restart=Never --image="$IMAGE" --image-pull-policy=IfNotPresent \
  --command -- /pkt send --dst "${NODE_IP}:${PORT}" --count 20 --interval 50ms --payload "$PAYLOAD"
kubectl -n default wait --for=jsonpath='{.status.phase}'=Succeeded pod/sender --timeout=60s

docker wait mcast-recv >/tmp/mcast-recv.exit
code="$(tr -d '[:space:]' </tmp/mcast-recv.exit)"
docker logs mcast-recv
if [[ "$code" != "0" ]]; then
  kubectl -n default logs ds/mcast-relay || true
  exit 1
fi
echo "kind test passed"
