#!/bin/bash

# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

set -euo pipefail

REMOTE="${1:?Usage: $0 <user@host> [tdengine_dir]}"
TD_DIR="${2:-/usr/local/taos}"
REMOTE_DIR="/opt/boosttools"

echo "=========================================="
echo " Deploying boosttools to ${REMOTE}"
echo " TDengine dir: ${TD_DIR}"
echo " Remote path:  ${REMOTE_DIR}"
echo "=========================================="

# Step 1: Create remote directory
echo "[1/4] Creating remote directory..."
ssh "${REMOTE}" "mkdir -p ${REMOTE_DIR}/src"

# Step 2: Copy source files
echo "[2/4] Uploading source files..."
scp -q src/boost.h \
       src/main.c \
       src/conn_pool.c \
       src/schema_sync.c \
       src/data_sync.c \
       src/progress.c \
       "${REMOTE}:${REMOTE_DIR}/src/"

scp -q Makefile "${REMOTE}:${REMOTE_DIR}/"

# Step 3: Build on remote
echo "[3/4] Building on remote server..."
ssh "${REMOTE}" "cd ${REMOTE_DIR} && TDENGINE_DIR=${TD_DIR} make clean all"

# Step 4: Verify
echo "[4/4] Verifying build..."
ssh "${REMOTE}" "${REMOTE_DIR}/boosttools --help 2>&1 | head -5"

echo ""
echo "=========================================="
echo " Deployment complete!"
echo " Run on server:"
echo "   ssh ${REMOTE}"
echo "   cd ${REMOTE_DIR}"
echo "   ./boosttools --src-host <A_IP> --dst-host <B_IP> --database <db>"
echo "=========================================="
