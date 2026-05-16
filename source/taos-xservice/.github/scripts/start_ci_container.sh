#!/usr/bin/env bash
set -euo pipefail

IMAGE_TAG="${IMAGE_TAG:?IMAGE_TAG is required}"
COVERAGE_BASE_DIR="${COVERAGE_BASE_DIR:?COVERAGE_BASE_DIR is required}"
DIR_PATH="${DIR_PATH:?DIR_PATH is required}"
GITHUB_OUTPUT="${GITHUB_OUTPUT:?GITHUB_OUTPUT is required}"
GRANT_SSH="${GRANT_SSH:-tdengine-grant}"

resolve_mapped_port() {
  local container_id="${1:?container id is required}"
  local container_port="${2:?container port is required}"
  local mapped

  mapped="$(docker port "${container_id}" "${container_port}/tcp" | head -n 1)"
  if [ -z "${mapped}" ]; then
    echo "ERROR: Failed to resolve mapped host port for container port ${container_port}" >&2
    exit 1
  fi

  printf '%s\n' "${mapped##*:}"
}

CONTAINER_ID="$(docker run -d \
  --privileged=true \
  --add-host u1-40:192.168.1.40 \
  --add-host u1-45:192.168.1.45 \
  -v "${COVERAGE_BASE_DIR}/${DIR_PATH}:${COVERAGE_BASE_DIR}/${DIR_PATH}" \
  -v /corefile:/corefile \
  -p "0:6030" \
  -p "0:6041" \
  -p "0:6043" \
  -p "0:6050" \
  -p "0:6055" \
  -p "0:6060" \
  -e "LLVM_PROFILE_FILE=${COVERAGE_BASE_DIR}/${DIR_PATH}/coverage-%p-%m.profraw" \
  -e PLUGINS_HOME=/usr/local/taos/plugins/ \
  "$IMAGE_TAG")"

PORT_6030="$(resolve_mapped_port "${CONTAINER_ID}" 6030)"
PORT_6041="$(resolve_mapped_port "${CONTAINER_ID}" 6041)"
PORT_6043="$(resolve_mapped_port "${CONTAINER_ID}" 6043)"
PORT_6050="$(resolve_mapped_port "${CONTAINER_ID}" 6050)"
PORT_6055="$(resolve_mapped_port "${CONTAINER_ID}" 6055)"
PORT_6060="$(resolve_mapped_port "${CONTAINER_ID}" 6060)"

echo "Allocated ports: 6030→${PORT_6030}, 6041→${PORT_6041}, 6043→${PORT_6043}, 6050→${PORT_6050}, 6055→${PORT_6055}, 6060→${PORT_6060}"

echo "container_id=${CONTAINER_ID}" >> "$GITHUB_OUTPUT"
echo "port_6030=${PORT_6030}" >> "$GITHUB_OUTPUT"
echo "port_6041=${PORT_6041}" >> "$GITHUB_OUTPUT"
echo "port_6043=${PORT_6043}" >> "$GITHUB_OUTPUT"
echo "port_6050=${PORT_6050}" >> "$GITHUB_OUTPUT"
echo "port_6055=${PORT_6055}" >> "$GITHUB_OUTPUT"
echo "port_6060=${PORT_6060}" >> "$GITHUB_OUTPUT"

echo "Waiting for TDengine to open port ${PORT_6030} (up to 120s)..."
for i in $(seq 1 120); do
  if nc -z localhost "$PORT_6030" 2>/dev/null; then
    echo "TDengine is reachable on ${PORT_6030} after ${i}s"
    break
  fi
  sleep 1
done

if ! nc -z localhost "$PORT_6030" 2>/dev/null; then
  echo "ERROR: TDengine did not open port ${PORT_6030} within 120 seconds"
  docker logs "$CONTAINER_ID"
  exit 1
fi

echo "Activating TDengine license..."
CONTAINER_ID="$CONTAINER_ID" GRANT_SSH="$GRANT_SSH" ./.github/scripts/activate_tdengine_license.sh

echo "Waiting for remaining services to start (up to 180s)..."
for i in $(seq 1 180); do
  if nc -z localhost "$PORT_6041" 2>/dev/null && \
     nc -z localhost "$PORT_6060" 2>/dev/null; then
    echo "Mapped service ports are reachable (6041, 6060) after ${i}s"
    break
  fi
  sleep 1
done

if ! nc -z localhost "$PORT_6041" 2>/dev/null || \
   ! nc -z localhost "$PORT_6060" 2>/dev/null; then
  echo "ERROR: Services failed to start within 180 seconds"
  docker logs "$CONTAINER_ID"
  exit 1
fi
