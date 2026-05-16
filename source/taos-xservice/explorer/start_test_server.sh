#!/bin/bash
# Quick start script for Custom OAuth testing

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Starting taos-explorer with Custom OAuth test configuration..."
echo ""
echo "Configuration file: $SCRIPT_DIR/custom-oauth-test.toml"
echo "SSO Provider: http://www.dodocloud.cn:43391/"
echo ""
echo "Server will start on: http://localhost:6060"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

cd "$SCRIPT_DIR/server"

# Set debug logging for OAuth
export RUST_LOG=info,taos_explorer::oauth=debug,taos_explorer::oauth::custom_client=debug

# Run with test configuration
cargo run -- --config ../custom-oauth-test.toml
