#!/usr/bin/env bash
# =================================================================
# TDengine TSDB Enterprise Uninstaller
# =================================================================
# Usage: uninstall.sh [OPTIONS] HOST
#
# Positional:
#   HOST       Target server IP (required)
#
# Options:
#   -p, --pass PASSWORD  SSH password (uses sshpass)
#   --dry-run            Test SSH only, do not uninstall
#   -y, --yes            Skip confirmation
#   -h, --help           Show help
#
# Exit codes: 0=success, 1=SSH failed, 2=not installed, 3=uninstall failed
# =================================================================
set -uo pipefail

HOST=""
SSH_PASS=""
DRY_RUN=false
AUTO_YES=false

usage() {
  cat <<'USAGE'
TDengine TSDB Enterprise Uninstaller

Usage: uninstall.sh [OPTIONS] HOST

Positional:
  HOST       Target server IP (required)

Options:
  -p, --pass PASSWORD  SSH password (uses sshpass)
  --dry-run            Test SSH only, do not uninstall
  -y, --yes            Skip confirmation
  -h, --help           Show help

Exit codes: 0=success, 1=SSH failed, 2=not installed, 3=uninstall failed
USAGE
  exit 0
}

# ---- Argument parsing ----
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--pass)     SSH_PASS="$2"; shift 2 ;;
    --dry-run)     DRY_RUN=true; shift ;;
    -y|--yes)      AUTO_YES=true; shift ;;
    -h|--help)     usage ;;
    -*)            echo "ERROR: Unknown option: $1" >&2; exit 1 ;;
    *)
      if [[ -z "$HOST" ]]; then HOST="$1"
      else echo "ERROR: Unexpected argument: $1" >&2; exit 1; fi
      shift ;;
  esac
done

[[ -z "$HOST" ]] && { echo "ERROR: HOST required. Use --help for usage." >&2; exit 1; }

# ---- SSH helper ----
run_remote() {
  if [[ -n "$SSH_PASS" ]]; then
    command -v sshpass &>/dev/null || {
      cat >&2 <<'MSG'
ERROR: sshpass not installed.
  macOS:  brew install hudochenkov/sshpass/sshpass
  Ubuntu: apt-get install -y sshpass
  CentOS: yum install -y sshpass
MSG
      exit 1
    }
    sshpass -p "$SSH_PASS" ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
      "root@${HOST}" bash -s -- "$@"
  else
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
      "root@${HOST}" bash -s -- "$@"
  fi
}

# ================================================================
# Step 1: Test SSH
# ================================================================
echo ">>> Testing SSH to root@${HOST}..."
ssh_ok=$(run_remote <<'REMOTE' 2>&1
echo "SSH_OK"
REMOTE
) || true

if [[ "$ssh_ok" != *"SSH_OK"* ]]; then
  echo "ERROR: SSH connection failed" >&2
  [[ -z "$SSH_PASS" ]] && echo "Hint: use -p PASSWORD" >&2
  exit 1
fi
echo "  OK"

# ================================================================
# Step 2: Check if TSDB is installed
# ================================================================
echo ">>> Checking TSDB installation..."
check_result=$(run_remote <<'REMOTE' 2>&1
if command -v rmtaos &>/dev/null; then
  echo "INSTALLED"
  # Try to get version
  taosd -V 2>/dev/null | head -1 || true
else
  echo "NOT_INSTALLED"
fi
REMOTE
) || true

if [[ "$check_result" == *"NOT_INSTALLED"* ]]; then
  echo "  TSDB is not installed on this server."
  exit 2
fi
echo "  TSDB is installed."
# Extract version info if available
VERSION_INFO=$(echo "$check_result" | grep -v "INSTALLED" | head -1)
[[ -n "$VERSION_INFO" ]] && echo "  ${VERSION_INFO}"

# ================================================================
# Step 3: Summary
# ================================================================
cat <<SUMMARY

========================================
  TDengine TSDB Uninstall
========================================
  Target:     root@${HOST}
  Command:    rmtaos -e yes

  ⚠️  WARNING: This will DELETE all:
    - Data directory:   /var/lib/taos/
    - Log directory:    /var/log/taos/
    - Config files:     /etc/taos/
    This operation is IRREVERSIBLE!
========================================
SUMMARY

[[ "$DRY_RUN" == true ]] && { echo "Dry-run complete."; exit 0; }

if [[ "$AUTO_YES" != true ]]; then
  read -rp "Proceed? [y/N] " confirm
  [[ "$confirm" =~ ^[yY]([eE][sS])?$ ]] || { echo "Cancelled."; exit 0; }
fi

# ================================================================
# Step 4: Uninstall
# ================================================================
echo ""
echo ">>> Uninstalling TSDB..."

if run_remote <<'REMOTE'; then
set -e
rmtaos -e yes
REMOTE
  echo ""
  echo "✅ TDengine TSDB uninstalled from root@${HOST}"
else
  echo ""
  echo "❌ Uninstall failed. See output above." >&2
  exit 3
fi
