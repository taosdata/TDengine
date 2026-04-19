#!/usr/bin/env bash
set -euo pipefail

IMAGE_TAG="${IMAGE_TAG:?IMAGE_TAG is required}"
COVERAGE_BASE_DIR="${COVERAGE_BASE_DIR:?COVERAGE_BASE_DIR is required}"
DIR_PATH="${DIR_PATH:?DIR_PATH is required}"
GITHUB_OUTPUT="${GITHUB_OUTPUT:?GITHUB_OUTPUT is required}"
GRANT_SSH="${GRANT_SSH:-tdengine-grant}"

find_available_ports() {
  local start_port=10000
  local end_port=65000
  local needed=6

  for port in $(seq "$start_port" "$end_port"); do
    local all_available=true
    for i in $(seq 0 $((needed - 1))); do
      local check_port=$((port + i))
      if (echo >/dev/tcp/localhost/"$check_port") &>/dev/null; then
        all_available=false
        break
      fi
    done

    if [ "$all_available" = true ]; then
      echo "$port"
      return 0
    fi
  done

  return 1
}

BASE_PORT="$(find_available_ports)"
if [ -z "$BASE_PORT" ]; then
  echo "ERROR: No available port range found in 10000-65000"
  exit 1
fi

PORT_6030="$BASE_PORT"
PORT_6041="$((BASE_PORT + 1))"
PORT_6043="$((BASE_PORT + 2))"
PORT_6050="$((BASE_PORT + 3))"
PORT_6055="$((BASE_PORT + 4))"
PORT_6060="$((BASE_PORT + 5))"

echo "Allocated ports: 6030→${PORT_6030}, 6041→${PORT_6041}, 6043→${PORT_6043}, 6050→${PORT_6050}, 6055→${PORT_6055}, 6060→${PORT_6060}"

CONTAINER_ID="$(docker run -d \
  --privileged=true \
  --add-host u1-40:192.168.1.40 \
  --add-host u1-45:192.168.1.45 \
  -v "${COVERAGE_BASE_DIR}/${DIR_PATH}:${COVERAGE_BASE_DIR}/${DIR_PATH}" \
  -v /corefile:/corefile \
  -p "${PORT_6030}:6030" \
  -p "${PORT_6041}:6041" \
  -p "${PORT_6043}:6043" \
  -p "${PORT_6050}:6050" \
  -p "${PORT_6055}:6055" \
  -p "${PORT_6060}:6060" \
  -e "LLVM_PROFILE_FILE=${COVERAGE_BASE_DIR}/${DIR_PATH}/coverage-%p-%m.profraw" \
  -e PLUGINS_HOME=/usr/local/taos/plugins/ \
  "$IMAGE_TAG")"

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
