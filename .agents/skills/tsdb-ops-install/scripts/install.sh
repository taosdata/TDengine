#!/usr/bin/env bash
# =================================================================
# TDengine TSDB Enterprise Installer
# =================================================================
# Usage: install.sh [OPTIONS] HOST [VERSION]
#
# Positional:
#   HOST       Target server IP (required)
#   VERSION    Version (optional):
#                omitted    auto-discover latest
#                3.3        latest 3.3.x.x
#                3.3.8      latest 3.3.8.x
#                3.3.8.22   exact version
#
# Options:
#   -d, --dir DIR        Install directory (default: ~/skill_tsdb_install)
#   -p, --pass PASSWORD  SSH password (uses sshpass)
#   --dry-run            Resolve + validate only
#   -y, --yes            Skip confirmation
#   -h, --help           Show help
#
# Exit codes: 0=success, 1=SSH failed, 2=version/NAS error, 3=install failed
# =================================================================
set -uo pipefail

readonly NAS_ROOT="http://192.168.1.131/data/nas/TDengine"
HOST=""
VERSION_INPUT=""
VERSION=""
INSTALL_DIR="~/skill_tsdb_install"
SSH_PASS=""
DRY_RUN=false
AUTO_YES=false

usage() {
  cat <<'USAGE'
TDengine TSDB Enterprise Installer

Usage: install.sh [OPTIONS] HOST [VERSION]

Positional:
  HOST       Target server IP (required)
  VERSION    Version (optional):
               omitted    auto-discover latest
               3.3        latest 3.3.x.x
               3.3.8      latest 3.3.8.x
               3.3.8.22   exact version

Options:
  -d, --dir DIR        Install directory (default: ~/skill_tsdb_install)
  -p, --pass PASSWORD  SSH password (uses sshpass)
  --dry-run            Resolve + validate only
  -y, --yes            Skip confirmation
  -h, --help           Show help

Exit codes: 0=success, 1=SSH failed, 2=version/NAS error, 3=install failed
USAGE
  exit 0
}

# ---- Argument parsing ----
while [[ $# -gt 0 ]]; do
  case "$1" in
    -d|--dir)      INSTALL_DIR="$2"; shift 2 ;;
    -p|--pass)     SSH_PASS="$2"; shift 2 ;;
    --dry-run)     DRY_RUN=true; shift ;;
    -y|--yes)      AUTO_YES=true; shift ;;
    -h|--help)     usage ;;
    -*)            echo "ERROR: Unknown option: $1" >&2; exit 1 ;;
    *)
      if [[ -z "$HOST" ]]; then HOST="$1"
      elif [[ -z "$VERSION_INPUT" ]]; then VERSION_INPUT="$1"
      else echo "ERROR: Unexpected argument: $1" >&2; exit 1; fi
      shift ;;
  esac
done

[[ -z "$HOST" ]] && { echo "ERROR: HOST required. Use --help for usage." >&2; exit 1; }

# ---- SSH helper ----
# Executes 'bash -s' on the remote host.
# Pipe script via heredoc; positional args passed after '--' become $1, $2, ... remotely.
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
# Step 2: Resolve version
# ================================================================
if [[ -z "$VERSION_INPUT" ]]; then
  echo ">>> Auto-discovering latest version..."
  VERSION=$(run_remote <<'REMOTE'
MAJOR=$(curl -sf http://192.168.1.131/data/nas/TDengine/ \
  | grep -oP 'href="\K[0-9]+\.[0-9]+(?=/)' | sort -V | tail -1)
[ -z "$MAJOR" ] && exit 1
VER=$(curl -sf "http://192.168.1.131/data/nas/TDengine/${MAJOR}/" \
  | grep -oP 'href="\Kv[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+(?=/)' \
  | sed 's/^v//' | sort -V | tail -1)
[ -z "$VER" ] && exit 1
echo "$VER"
REMOTE
  ) || { echo "ERROR: Cannot discover version from NAS" >&2; exit 2; }
  echo "  Latest: ${VERSION}"
else
  SEGMENTS=$(echo "$VERSION_INPUT" | awk -F. '{print NF}')

  case "$SEGMENTS" in
    4)
      VERSION="$VERSION_INPUT"
      ;;
    3)
      echo ">>> Resolving ${VERSION_INPUT}.x ..."
      VERSION=$(run_remote "$VERSION_INPUT" <<'REMOTE'
PREFIX="$1"; MAJOR=$(echo "$PREFIX" | cut -d. -f1-2)
VER=$(curl -sf "http://192.168.1.131/data/nas/TDengine/${MAJOR}/" \
  | grep -oP 'href="\Kv[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+(?=/)' \
  | sed 's/^v//' | grep "^${PREFIX}\." | sort -V | tail -1)
[ -z "$VER" ] && exit 1
echo "$VER"
REMOTE
      ) || { echo "ERROR: No ${VERSION_INPUT}.x versions on NAS" >&2; exit 2; }
      echo "  Resolved: ${VERSION_INPUT} → ${VERSION}"
      ;;
    2)
      echo ">>> Resolving ${VERSION_INPUT}.x.x ..."
      VERSION=$(run_remote "$VERSION_INPUT" <<'REMOTE'
MAJOR="$1"
VER=$(curl -sf "http://192.168.1.131/data/nas/TDengine/${MAJOR}/" \
  | grep -oP 'href="\Kv[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+(?=/)' \
  | sed 's/^v//' | sort -V | tail -1)
[ -z "$VER" ] && exit 1
echo "$VER"
REMOTE
      ) || { echo "ERROR: No ${VERSION_INPUT} versions on NAS" >&2; exit 2; }
      echo "  Resolved: ${VERSION_INPUT} → ${VERSION}"
      ;;
    *)
      echo "ERROR: Invalid version '${VERSION_INPUT}' (use 2/3/4 segments)" >&2
      exit 2
      ;;
  esac
fi

VERSION=$(echo "$VERSION" | tr -d '[:space:]')

# ================================================================
# Step 3: Validate package on NAS
# ================================================================
echo ">>> Validating package..."
MAJOR=$(echo "$VERSION" | cut -d. -f1-2)
PKG_NAME="tdengine-tsdb-enterprise-${VERSION}-linux-x64"
PKG_URL="${NAS_ROOT}/${MAJOR}/v${VERSION}/enterprise/${PKG_NAME}.tar.gz"

HTTP_CODE=$(run_remote "$PKG_URL" <<'REMOTE'
curl -s -o /dev/null -w "%{http_code}" --max-time 10 -I "$1"
REMOTE
) || HTTP_CODE="000"

if [[ "$HTTP_CODE" != "200" ]]; then
  echo "ERROR: Package not found (HTTP ${HTTP_CODE})" >&2
  echo "  URL: ${PKG_URL}" >&2
  echo "  Browse: ${NAS_ROOT}/" >&2
  exit 2
fi
echo "  OK"

# ================================================================
# Step 4: Summary
# ================================================================
[[ -n "$VERSION_INPUT" && "$VERSION_INPUT" != "$VERSION" ]] \
  && V_NOTE=" (from ${VERSION_INPUT})" || V_NOTE=""

cat <<SUMMARY

========================================
  TDengine TSDB Enterprise Installation
========================================
  Target:     root@${HOST}
  Version:    v${VERSION}${V_NOTE}
  Directory:  ${INSTALL_DIR}
  Package:    ${PKG_URL}

  Steps:
    1. Uninstall existing TSDB (rmtaos -e yes)
    2. Download package from NAS
    3. Extract and install (install.sh -e no)
    4. Start all services (start-all.sh)
    5. Create explorer-register.cfg (bypass registration)
    6. Set supportVnodes to 1024 in taos.cfg
========================================
SUMMARY

[[ "$DRY_RUN" == true ]] && { echo "Dry-run complete."; exit 0; }

if [[ "$AUTO_YES" != true ]]; then
  read -rp "Proceed? [y/N] " confirm
  [[ "$confirm" =~ ^[yY]([eE][sS])?$ ]] || { echo "Cancelled."; exit 0; }
fi

# ================================================================
# Step 5: Install
# ================================================================
echo ""
echo ">>> Installing..."

if run_remote "$INSTALL_DIR" "$PKG_URL" "$PKG_NAME" <<'REMOTE'; then
set -e
DIR="$1"; URL="$2"; PKG="$3"

echo "[1/6] Uninstalling existing TSDB..."
rmtaos -e yes 2>/dev/null || true

echo "[2/6] Downloading package..."
mkdir -p "$DIR" && cd "$DIR"
wget -q "$URL" -O "${PKG}.tar.gz"

echo "[3/6] Extracting and installing..."
tar -zxf "${PKG}.tar.gz"
EXTRACTED_DIR=$(tar -tzf "${PKG}.tar.gz" | head -1 | cut -d/ -f1)
cd "$EXTRACTED_DIR"
./install.sh -e no

echo "[4/6] Starting all services..."
./start-all.sh

echo "[5/6] Creating explorer-register.cfg (bypass registration)..."
touch /etc/taos/explorer-register.cfg

echo "[6/6] Setting supportVnodes to 1024 in taos.cfg..."
sed -i 's/^[# ]*supportVnodes.*/supportVnodes             1024/' /etc/taos/taos.cfg
REMOTE
  echo ""
  echo "✅ TDengine TSDB Enterprise v${VERSION} installed on root@${HOST}"
else
  echo ""
  echo "❌ Installation failed. See output above." >&2
  exit 3
fi
