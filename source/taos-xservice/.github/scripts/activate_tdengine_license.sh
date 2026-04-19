#!/usr/bin/env bash
set -euo pipefail

CONTAINER_ID="${CONTAINER_ID:?CONTAINER_ID is required}"
GRANT_SSH="${GRANT_SSH:-tdengine-grant}"

read_cluster_info() {
  # Use -s (single statement mode) so that \G format specifier is honoured.
  # taos -f (file mode) does not interpret \G.
  docker exec "$CONTAINER_ID" taos -s 'show cluster machines\G;' 2>/dev/null \
    | tr -d '\r' | grep -v "terminal properties" || true
}

extract_colon_field() {
  local field_name="${1:?field name is required}"

  awk -F': ' -v field_name="$field_name" '
    $1 ~ "^[[:space:]]*" field_name "$" {
      gsub(/^[[:space:]]+/, "", $2)
      print $2
      exit
    }
  '
}

extract_all_colon_fields() {
  local field_name="${1:?field name is required}"

  awk -F': ' -v field_name="$field_name" '
    $1 ~ "^[[:space:]]*" field_name "$" {
      gsub(/^[[:space:]]+/, "", $2)
      print $2
    }
  '
}

echo ">>> Fetching cluster machine info from container '${CONTAINER_ID}'..."
cluster_info=""
prev_machines=""
stable_count=0
for _ in $(seq 1 60); do
  raw="$(read_cluster_info || true)"
  # Only accept output that contains an "id:" field — the bare CLI banner does not.
  if echo "$raw" | extract_colon_field "id" >/dev/null; then
    cur_machines="$(echo "$raw" | extract_all_colon_fields "machine" | paste -sd, -)"
    if [[ "$cur_machines" == "$prev_machines" && -n "$cur_machines" ]]; then
      stable_count=$(( stable_count + 1 ))
    else
      stable_count=1
      prev_machines="$cur_machines"
    fi
    # Require the same machines value twice in a row before trusting it.
    if [[ $stable_count -ge 2 ]]; then
      cluster_info="$raw"
      break
    fi
  fi
  sleep 1
done

if [[ -z "$cluster_info" ]]; then
  echo "ERROR: Could not retrieve cluster machine info."
  echo "       Is the container '${CONTAINER_ID}' running and TDengine healthy?"
  exit 1
fi

cluster_id="$(echo "$cluster_info" | extract_colon_field "id")"
machines="$(
  echo "$cluster_info" \
    | extract_all_colon_fields "machine" \
    | paste -sd, -
)"

if [[ -z "$cluster_id" || -z "$machines" ]]; then
  echo "ERROR: Failed to parse cluster_id or machines from cluster info."
  echo "--- show cluster machines\\G raw output ---"
  echo "$cluster_info"
  echo "-----------------------------------------"
  exit 1
fi

echo "    cluster_id : $cluster_id"
echo "    machines   : $machines"
echo "--- cluster_id hex ---"
printf '%s' "$cluster_id" | xxd | head -3
echo "--- machines hex ---"
printf '%s' "$machines" | xxd | head -3
echo "----------------------"

echo ">>> Generating activation code from $GRANT_SSH..."
ssh_raw="$(
  ssh -o StrictHostKeyChecking=no "$GRANT_SSH" \
    "cd /data/common/activation-code && ./generateActivationCode.sh $cluster_id $machines"
)"

activation_sql="$(
  printf '%s\n' "$ssh_raw" \
    | sed -n 's/.*"Command":[[:space:]]*"\([^"]*\)".*/\1/p' \
    | head -n 1
)"

if [[ -z "$activation_sql" ]]; then
  echo "ERROR: No activation SQL returned from the activation server."
  exit 1
fi

echo "    activation SQL generated"
echo ">>> Applying activation..."

# Write activation SQL to a fixed temp file inside the container and run with -f
# to avoid shell quoting issues with the activation code payload.
docker exec -i "$CONTAINER_ID" bash -c 'cat > /tmp/taos_activate.sql' <<< "$activation_sql"
activation_apply_output="$(docker exec "$CONTAINER_ID" taos -f /tmp/taos_activate.sql 2>&1 | grep -v "terminal properties" || true)"
docker exec "$CONTAINER_ID" rm -f /tmp/taos_activate.sql
if [[ -n "$activation_apply_output" ]]; then
  echo "$activation_apply_output"
fi

if echo "$activation_apply_output" | grep -q "Cluster machines mismatch with active code"; then
  echo "ERROR: Activation code does not match the current cluster machine info."
  echo "--- show cluster machines\\G ---"
  echo "$cluster_info"
  echo "-------------------------------"
  exit 1
fi

echo ">>> Verifying activation (polling until granted)..."
max_retries=10
retry_interval=5
attempt=0
activated=0
grants_output=""

while [[ $attempt -lt $max_retries ]]; do
  attempt=$((attempt + 1))
  echo "    Attempt $attempt/$max_retries..."

  grants_output="$(docker exec "$CONTAINER_ID" taos -s 'show grants\G;' 2>&1 | tr -d '\r' | grep -v "terminal properties" || true)"

  expired="$(echo "$grants_output" | awk -F': ' '/^[[:space:]]*expired:/{gsub(/^[[:space:]]+/,"",$2); print $2}')"
  state="$(echo "$grants_output" | awk -F': ' '/^[[:space:]]*state:/{gsub(/^[[:space:]]+/,"",$2); print $2}')"

  echo "      state=$state  expired=$expired"

  if [[ "$state" == "granted" && "$expired" == "false" ]]; then
    activated=1
    break
  fi

  if [[ $attempt -lt $max_retries ]]; then
    echo "      Not yet granted, retrying in ${retry_interval}s..."
    sleep "$retry_interval"
  fi
done

expire_time="$(echo "$grants_output" | extract_colon_field "expire_time")"
service_time="$(echo "$grants_output" | extract_colon_field "service_time")"

echo "  expire_time  : ${expire_time:-(not found)}"
echo "  service_time : ${service_time:-(not found)}"
echo "  expired      : ${expired:-(not found)}"
echo "  state        : ${state:-(not found)}"
echo ""

if [[ "$activated" -eq 1 ]]; then
  echo "TDengine activated successfully"
else
  echo "ERROR: TDengine activation verification failed after $max_retries attempts."
  exit 1
fi
