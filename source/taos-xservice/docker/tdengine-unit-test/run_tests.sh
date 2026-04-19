#!/bin/bash
set -euo pipefail

wait_for_port() {
  local port="$1"
  local service_name="$2"

  for _ in $(seq 1 30); do
    if nc -z localhost "$port" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done

  echo "ERROR: ${service_name} did not become ready on port ${port}"
  return 1
}

wait_for_activation() {
  echo "Waiting for TDengine activation..."

  for _ in $(seq 1 120); do
    local grants_output
    grants_output="$(taos -s "show grants\\G;" 2>/dev/null | tr -d '\r' || true)"
    local expired
    expired="$(echo "$grants_output" | awk -F': ' '/^[[:space:]]*expired:/{gsub(/^[[:space:]]+/,"",$2); print $2}')"
    local state
    state="$(echo "$grants_output" | awk -F': ' '/^[[:space:]]*state:/{gsub(/^[[:space:]]+/,"",$2); print $2}')"

    if [ "$state" = "granted" ] && [ "$expired" = "false" ]; then
      echo "TDengine activation confirmed"
      return 0
    fi

    sleep 1
  done

  echo "ERROR: TDengine activation did not complete within 120 seconds"
  taos -s "show grants\\G;" || true
  return 1
}

print_failure_context() {
  local exit_code="$?"

  if [ "$exit_code" -ne 0 ]; then
    if [ -f /tmp/cargo-make-test.log ]; then
      echo "=== Last 200 lines of cargo make test output ==="
      tail -n 200 /tmp/cargo-make-test.log || true
    fi

    if [ -f /tmp/taosadapter.log ]; then
      echo "=== Last 200 lines of taosadapter log ==="
      tail -n 200 /tmp/taosadapter.log || true
    fi

    if [ -f /tmp/taoskeeper.log ]; then
      echo "=== Last 200 lines of taoskeeper log ==="
      tail -n 200 /tmp/taoskeeper.log || true
    fi
  fi

  exit "$exit_code"
}

trap print_failure_context EXIT

# Fix config files generated at Docker build time (fqdn may contain build hostname)
sed -i 's/buildkitsandbox/localhost/g' /etc/taos/taos.cfg 2>/dev/null || true
sed -i 's/buildkitsandbox/localhost/g' /etc/taos/taosadapter.toml 2>/dev/null || true
sed -i 's/buildkitsandbox/localhost/g' /etc/taos/taoskeeper.toml 2>/dev/null || true

# Start taosd
echo "Starting taosd..."
taosd >/tmp/taosd.log 2>&1 &
wait_for_port 6030 "taosd"
echo "TDengine ready for activation"
wait_for_activation

# Start taosadapter
echo "Starting taosadapter..."
taosadapter >/tmp/taosadapter.log 2>&1 &
wait_for_port 6041 "taosadapter"

# Start taoskeeper to avoid repeated monitor errors from taosd.
echo "Starting taoskeeper..."
if ! command -v taoskeeper >/dev/null 2>&1; then
  echo "ERROR: taoskeeper binary is not installed"
  exit 1
fi
taoskeeper >/tmp/taoskeeper.log 2>&1 &
wait_for_port 6043 "taoskeeper"

echo "All services started successfully"

# Run unit tests with llvm-cov coverage instrumentation.
# Source code is expected to be bind-mounted at /workspace/taosx.
# Coverage output goes to /workspace/taosx/target/ (visible on the host via bind mount).
# NO_BUILD_UI=true skips the Docker-in-Docker explorer UI build step.
echo "Running tests..."
cd /workspace/taosx
echo "Cleaning previous llvm-cov build artifacts..."
rm -rf target/llvm-cov-target
rm -f target/llvm-cov-taosx.lcov target/llvm-cov-explorer.lcov target/llvm-cov-merged.lcov
rm -rf /usr/local/cargo/git/db/taos-connector-rust-* /usr/local/cargo/git/checkouts/taos-connector-rust-* 2>/dev/null || true
CARGO_NET_GIT_FETCH_WITH_CLI=true NO_BUILD_UI=true cargo make test 2>&1 | stdbuf -oL tee /tmp/cargo-make-test.log
