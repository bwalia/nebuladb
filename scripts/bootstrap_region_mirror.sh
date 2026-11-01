#!/usr/bin/env bash
# Bootstrap a NebulaDB peer (region-b or follower) from the primary's
# current .nsnap so it holds the full corpus. XDCR / TailWal cannot
# backfill compacted history — this is the supported catch-up path.
#
# Transfers in 128 MiB chunks via: kubectl exec dd → local → kubectl cp
# (direct exec|exec pipes drop binary stdin into alpine helpers).
#
# Critical: the live snapshot can be rewritten while we copy. We freeze a
# private copy on the source pod first, hash that, then transfer the freeze.
#
# Usage:
#   KUBECONFIG=~/.kube/k3s1.yaml \
#     NS=prod SOURCE=nebuladb TARGET=nebuladb-region-b \
#     ./scripts/bootstrap_region_mirror.sh
set -euo pipefail

NS="${NS:-prod}"
SOURCE="${SOURCE:-nebuladb}"
TARGET="${TARGET:-nebuladb-region-b}"
DATA_DIR="${DATA_DIR:-/var/lib/nebuladb}"
CHUNK_MB="${CHUNK_MB:-128}"
WORKDIR="${WORKDIR:-/tmp/nebula-mirror-$$}"

need() { command -v "$1" >/dev/null || { echo "need $1" >&2; exit 1; }; }
need kubectl
need python3

case "$SOURCE" in
  *-follower) SRC_COMP=server-follower ;;
  *-region-b) SRC_COMP=server-region-b ;;
  *) SRC_COMP=server ;;
esac
case "$TARGET" in
  *-follower) TGT_COMP=server-follower ;;
  *-region-b) TGT_COMP=server-region-b ;;
  *) TGT_COMP=server ;;
esac

echo "── ns=$NS source=$SOURCE target=$TARGET"

src_pod=$(kubectl -n "$NS" get pod \
  -l "app.kubernetes.io/instance=nebuladb,app.kubernetes.io/component=${SRC_COMP}" \
  -o jsonpath='{.items[0].metadata.name}')
tgt_pod=$(kubectl -n "$NS" get pod \
  -l "app.kubernetes.io/instance=nebuladb,app.kubernetes.io/component=${TGT_COMP}" \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)

echo "✓ source pod $src_pod"
[ -n "$tgt_pod" ] && echo "✓ target pod $tgt_pod (will scale to 0)"

LIVE=$(kubectl -n "$NS" exec "$src_pod" -c server -- \
  sh -c "ls -1 ${DATA_DIR}/snapshots/*.nsnap 2>/dev/null | grep -v mirror-freeze | sort | tail -1")
[ -n "$LIVE" ] || { echo "no .nsnap on source" >&2; exit 1; }
SNAP_BASE=$(basename "$LIVE")
TIP=$(kubectl -n "$NS" exec "$src_pod" -c server -- wget -qO- http://127.0.0.1:8080/healthz/wal-tip)
echo "✓ live snapshot $SNAP_BASE"
echo "✓ wal tip $TIP"

mkdir -p "$WORKDIR"
FREEZE="${DATA_DIR}/snapshots/.mirror-freeze-${TARGET}.nsnap"
cleanup() {
  rm -rf "$WORKDIR"
  kubectl -n "$NS" exec "$src_pod" -c server -- rm -f "$FREEZE" 2>/dev/null || true
}
trap cleanup EXIT

echo "── freeze live snapshot on source (stable bytes for transfer)"
kubectl -n "$NS" exec "$src_pod" -c server -- \
  sh -c "rm -f '$FREEZE' && cp -f -- '$LIVE' '$FREEZE' && ls -lh '$FREEZE'"
SNAP=$FREEZE
SIZE=$(kubectl -n "$NS" exec "$src_pod" -c server -- stat -c%s "$SNAP")
echo "✓ freeze size=$SIZE"

echo "── scale target to 0"
kubectl -n "$NS" scale "deploy/${TARGET}" --replicas=0
[ -n "$tgt_pod" ] && kubectl -n "$NS" wait --for=delete "pod/${tgt_pod}" --timeout=300s 2>/dev/null || true
# Also wait for any crashlooping pods with the component label
kubectl -n "$NS" wait --for=delete pod \
  -l "app.kubernetes.io/instance=nebuladb,app.kubernetes.io/component=${TGT_COMP}" \
  --timeout=300s 2>/dev/null || true

PVC="${TARGET}-data"
HELPER="nebula-mirror-helper-$$"
# Reuse existing helper if it already mounts the PVC
existing=$(kubectl -n "$NS" get pods -l app=nebula-mirror-helper \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [ -n "$existing" ]; then
  HELPER=$existing
  echo "✓ reusing helper $HELPER"
else
  cat <<EOF | kubectl -n "$NS" apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: ${HELPER}
  labels:
    app: nebula-mirror-helper
spec:
  restartPolicy: Never
  containers:
    - name: helper
      image: alpine:3.20
      command: ["sleep", "86400"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: ${PVC}
EOF
  kubectl -n "$NS" wait --for=condition=Ready "pod/${HELPER}" --timeout=300s
fi
# Wipe ALL local state, including the replica WAL (records mirrored from
# other regions) and saved cross-region cursors: a stale cursor would
# override the seed below and resume from the wrong place.
kubectl -n "$NS" exec "$HELPER" -- sh -c 'rm -rf /data/snapshots /data/wal /data/replica-wal /data/cross-region-*.cursor /data/follower.cursor; mkdir -p /data/snapshots /data/wal'

echo "── sha256 freeze (may take several minutes)"
SRC_HASH=$(kubectl -n "$NS" exec "$src_pod" -c server -- sha256sum "$SNAP" | awk '{print $1}')
SIZE2=$(kubectl -n "$NS" exec "$src_pod" -c server -- stat -c%s "$SNAP")
[ "$SIZE" = "$SIZE2" ] || { echo "✗ freeze mutated ($SIZE -> $SIZE2)" >&2; exit 1; }
echo "✓ sha256=$SRC_HASH"

# Read exact byte windows from the freeze file. Full MiB chunks use dd bs=1M;
# the final partial chunk is truncated locally with head -c so we never over-read
# if the live snapshot path were used by mistake.
TOTAL=$SIZE
OFFSET=0
i=0
CHUNK_BYTES=$((CHUNK_MB * 1024 * 1024))
while [ "$OFFSET" -lt "$TOTAL" ]; do
  LEFT=$((TOTAL - OFFSET))
  N=$CHUNK_BYTES
  [ "$LEFT" -lt "$N" ] && N=$LEFT
  SKIP_MB=$((OFFSET / 1024 / 1024))
  # For a partial last chunk, ask dd for one extra MiB then trim — freeze is immutable.
  DD_COUNT=$(( (N + 1024 * 1024 - 1) / (1024 * 1024) ))
  echo "── chunk $i offset=$OFFSET length=$N skip_mib=$SKIP_MB"
  tries=0
  while true; do
    tries=$((tries + 1))
    set +e
    kubectl -n "$NS" exec "$src_pod" -c server -- \
      dd "if=$SNAP" bs=1M skip=$SKIP_MB count=$DD_COUNT 2>/dev/null \
      | head -c "$N" >"${WORKDIR}/part.$i"
    set -e
    PART=$(wc -c <"${WORKDIR}/part.$i" | tr -d ' ')
    [ "$PART" -eq "$N" ] && break
    echo "  retry $tries got=$PART expect=$N"
    [ "$tries" -ge 8 ] && { echo "✗ give up on chunk $i" >&2; exit 1; }
    sleep 2
  done
  set +e
  kubectl -n "$NS" cp "${WORKDIR}/part.$i" "${HELPER}:/data/snapshots/part.$i"
  cp_rc=$?
  set -e
  [ "$cp_rc" -eq 0 ] || { echo "✗ kubectl cp failed rc=$cp_rc" >&2; exit 1; }
  REMOTE=$(kubectl -n "$NS" exec "$HELPER" -- wc -c "/data/snapshots/part.$i" | awk '{print $1}')
  [ "$PART" = "$REMOTE" ] || { echo "✗ cp mismatch $PART vs $REMOTE" >&2; exit 1; }
  # Append immediately (loop order); drop part to save PVC space
  kubectl -n "$NS" exec "$HELPER" -- sh -c \
    "cat /data/snapshots/part.$i >> /data/snapshots/${SNAP_BASE} && rm -f /data/snapshots/part.$i"
  rm -f "${WORKDIR}/part.$i"
  OFFSET=$((OFFSET + N))
  i=$((i + 1))
done

kubectl -n "$NS" exec "$HELPER" -- touch "/data/snapshots/${SNAP_BASE}.ok"
DST=$(kubectl -n "$NS" exec "$HELPER" -- stat -c%s "/data/snapshots/${SNAP_BASE}")
echo "src=$SIZE dst=$DST"
[ "$SIZE" = "$DST" ] || { echo "✗ size mismatch" >&2; exit 1; }

echo "── sha256 dest"
DST_HASH=$(kubectl -n "$NS" exec "$HELPER" -- sha256sum "/data/snapshots/${SNAP_BASE}" | awk '{print $1}')
echo "src=$SRC_HASH dst=$DST_HASH"
[ "$SRC_HASH" = "$DST_HASH" ] || { echo "✗ sha256 mismatch — refusing to start target" >&2; exit 1; }

# Seed the resume cursor from the SNAPSHOT's WAL seq (start of that
# segment), not the live tip: the tip is later than the snapshot, so
# seeding from it skipped every write in between. Replaying the head of
# segment SEQ is harmless (idempotent applies) and the source keeps it
# (it only compacts segments < its newest snapshot's seq).
SNAP_SEQ=$((10#$(echo "$SNAP_BASE" | sed -E 's/^snapshot-0*([0-9]+)\.nsnap$/\1/')))
printf '{"segment_seq":%d,"byte_offset":0}' "$SNAP_SEQ" >"${WORKDIR}/follower.cursor"
echo "✓ resume cursor seeded at segment $SNAP_SEQ (live tip was $TIP)"
kubectl -n "$NS" cp "${WORKDIR}/follower.cursor" "${HELPER}:/data/follower.cursor"

echo "── scale target to 1"
kubectl -n "$NS" delete pod "$HELPER" --wait=true --timeout=180s
kubectl -n "$NS" scale "deploy/${TARGET}" --replicas=1
kubectl -n "$NS" rollout status "deploy/${TARGET}" --timeout=1800s

echo "── health"
for _ in $(seq 1 60); do
  tp=$(kubectl -n "$NS" get pod \
    -l "app.kubernetes.io/instance=nebuladb,app.kubernetes.io/component=${TGT_COMP}" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  [ -z "$tp" ] && sleep 15 && continue
  logs=$(kubectl -n "$NS" logs "$tp" -c server --tail=8 2>/dev/null || true)
  echo "$logs" | grep -q 'Data corruption' && { echo "✗ corruption on load" >&2; exit 1; }
  body=$(kubectl -n "$NS" exec "$tp" -c server -- wget -qO- http://127.0.0.1:8080/healthz 2>/dev/null || true)
  echo "  $body"
  echo "$body" | grep -q '"status":"ok"' && break
  sleep 30
done

src_docs=$(kubectl -n "$NS" exec "$src_pod" -c server -- wget -qO- http://127.0.0.1:8080/healthz \
  | python3 -c 'import json,sys; print(json.load(sys.stdin).get("docs"))')
tp=$(kubectl -n "$NS" get pod \
  -l "app.kubernetes.io/instance=nebuladb,app.kubernetes.io/component=${TGT_COMP}" \
  -o jsonpath='{.items[0].metadata.name}')
tgt_docs=$(kubectl -n "$NS" exec "$tp" -c server -- wget -qO- http://127.0.0.1:8080/healthz \
  | python3 -c 'import json,sys; print(json.load(sys.stdin).get("docs"))')
echo "── docs source=$src_docs target=$tgt_docs"
echo "Done."
