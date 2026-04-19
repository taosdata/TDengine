#!/bin/bash
set -e

# Set core dump pattern
ulimit -c 1000000000 >/dev/null 2>&1 || true
sysctl -w kernel.core_pattern=/corefile/core-%e-%p >/dev/null 2>&1 || true

# Print configuration file names for debugging without leaking secrets
echo "=========================================="
echo "Configuration files in /etc/taos/:"
echo "=========================================="
shopt -s nullglob
for config_file in /etc/taos/*.cfg /etc/taos/*.toml; do
    if [ -f "$config_file" ]; then
        echo "Found config file: $(basename "$config_file")"
    fi
done
shopt -u nullglob
echo "=========================================="

# Replace all buildkitsandbox references with localhost in config files
echo "Updating configuration files to use localhost..."
sed -i 's/buildkitsandbox/localhost/g' /etc/taos/explorer.toml 2>/dev/null || true
sed -i 's/buildkitsandbox/localhost/g' /etc/taos/taosx.toml 2>/dev/null || true
sed -i 's/buildkitsandbox/localhost/g' /etc/taos/taosadapter.toml 2>/dev/null || true
sed -i 's/buildkitsandbox/localhost/g' /etc/taos/taoskeeper.toml 2>/dev/null || true
sed -i 's/buildkitsandbox/localhost/g' /etc/taos/taos.cfg 2>/dev/null || true

# Start taosd
echo "Starting taosd..."
taosd &
for _ in $(seq 1 20); do
  nc -z localhost 6030 && break
  sleep 0.5
done

# Start taos-explorer early so CI route validation can proceed while activation runs.
# In debug/test builds, explicitly serving /explorer-dist is more reliable than
# relying on rust-embed's runtime filesystem lookup.
if [ -d /explorer-dist ]; then
    export EXPLORER_ASSETS=/explorer-dist
    echo "Using explorer assets from $EXPLORER_ASSETS"
fi
echo "Starting taos-explorer..."
taos-explorer > /tmp/taos-explorer.log 2>&1 &
for _ in $(seq 1 20); do
  nc -z localhost 6060 && break
  sleep 0.5
done

# Wait for activation (performed externally after the container starts)
echo "Waiting for TDengine activation..."
for _ in $(seq 1 180); do
  grants_output="$(taos -s "show grants\\G;" 2>/dev/null | tr -d '\r' || true)"
  expired="$(echo "$grants_output" | awk -F': ' '/^[[:space:]]*expired:/{gsub(/^[[:space:]]+/,"",$2); print $2}')"
  state="$(echo "$grants_output" | awk -F': ' '/^[[:space:]]*state:/{gsub(/^[[:space:]]+/,"",$2); print $2}')"
  if [ "$state" = "granted" ] && [ "$expired" = "false" ]; then
    echo "TDengine activation confirmed"
    break
  fi
  sleep 1
done

if [ "${state:-}" != "granted" ] || [ "${expired:-}" != "false" ]; then
  echo "ERROR: TDengine activation did not complete within 180 seconds"
  echo "Last observed activation state: ${state:-unknown}, expired: ${expired:-unknown}"
  exit 1
fi

# Start taosadapter
echo "Starting taosadapter..."
taosadapter &
for _ in $(seq 1 20); do
  nc -z localhost 6041 && break
  sleep 0.5
done

# Create integration_test database
echo "Creating integration_test database..."
taos -s "create database if not exists integration_test;" || echo "Database may already exist"

# Start taoskeeper
echo "Starting taoskeeper..."
taoskeeper &
for _ in $(seq 1 20); do
  nc -z localhost 6043 && break
  sleep 0.5
done

# Start taosx
echo "Starting taosx..."
taosx serve > /tmp/taosx.log 2>&1 &
for _ in $(seq 1 20); do
  nc -z localhost 6050 && break
  sleep 0.5
done

# Create xnode
echo "Creating xnode 'localhost:6055'..."
taos -s "create xnode 'localhost:6055' user root pass 'taosdata';" || echo "xnode may already exist"

# Create xnode agent and get token
echo "Creating xnode agent 'integration_test_agent'..."
taos -s "create xnode agent 'integration_test_agent';" || echo "Agent may already exist"

# Extract token from database using \G format
TOKEN=$(
  taos -s "show xnode agent where name = 'integration_test_agent'\G;" |
    awk '
      /^[[:space:]]*token:/ {
        sub(/^[[:space:]]*token:[[:space:]]*/, "", $0)
        gsub(/["\r\n ]/, "", $0)
        print
        exit
      }
    '
)
echo "Agent token extracted successfully"

# Validate token extraction
if [ -z "$TOKEN" ] || [ "$TOKEN" = "NULL" ]; then
  echo "ERROR: Failed to extract agent token"
  exit 1
fi

# Configure agent.toml
cat > /etc/taos/agent.toml <<EOF
# taosX service endpoint
endpoint = "http://localhost:6055"

# Agent token
token = "$TOKEN"

# Keep the agent alive when the taosX service exits or disconnects
keep_online = true

[log]
path = "/var/log/taos"
level = "info"
EOF

# Start taosx-agent
echo "Starting taosx-agent..."
taosx-agent > /tmp/taosx-agent.log 2>&1 &

# Wait for agent to start (check process, not port - agent is a client)
for _ in $(seq 1 20); do
  pgrep -x taosx-agent >/dev/null && break
  sleep 0.5
done

# Verify agent is running
if ! pgrep -x taosx-agent >/dev/null; then
  echo "ERROR: taosx-agent failed to start"
  cat /tmp/taosx-agent.log
  exit 1
fi

echo "All services started successfully"
ps -ef | grep -E "taosd|taosadapter|taoskeeper|taosx|taos-explorer|taosx-agent"

# Keep container running
tail -f /dev/null
