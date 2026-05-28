#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="/home/simon/dev/TDinternal"

if [[ $# -lt 1 ]]; then
  echo "usage: $0 '<repo command>'" >&2
  exit 1
fi

REMOTE_CMD="cd ${REPO_ROOT} && $1"
"${SCRIPT_DIR}/remote_exec.sh" "$REMOTE_CMD"
