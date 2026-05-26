#!/usr/bin/env bash
set -euo pipefail

SSH_USER="simon"
SSH_HOST="192.168.127.101"
: "${TDINTERNAL_LINUX_101_PASSWORD:?set TDINTERNAL_LINUX_101_PASSWORD before using tsdb-build-linux}"

DRY_RUN=0
if [[ ${1-} == "--dry-run" ]]; then
  DRY_RUN=1
  shift
fi

if [[ $# -lt 1 ]]; then
  echo "usage: $0 [--dry-run] '<remote command>'" >&2
  exit 1
fi

if ! command -v sshpass >/dev/null 2>&1; then
  echo "sshpass is required but not found in PATH" >&2
  exit 1
fi

REMOTE_CMD="$1"
REMOTE_TARGET="${SSH_USER}@${SSH_HOST}"

if [[ "$DRY_RUN" -eq 1 ]]; then
  printf 'dry-run: sshpass -p ****** ssh -o StrictHostKeyChecking=no %s bash -se <<"CMD"\n%s\nCMD\n' "$REMOTE_TARGET" "$REMOTE_CMD"
  exit 0
fi

printf '%s\n' "$REMOTE_CMD" | \
  sshpass -p "$TDINTERNAL_LINUX_101_PASSWORD" ssh -o StrictHostKeyChecking=no "$REMOTE_TARGET" 'bash -se'
