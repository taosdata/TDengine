/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "mndBackup.h"
#include "mndShow.h"

#ifndef TD_ENTERPRISE

static int32_t mndRetrieveBackups(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  return TSDB_CODE_SUCCESS;
}

static int32_t mndProcessBackupReq(SRpcMsg *pReq) {
  mError("failed to process backup req since %s", tstrerror(TSDB_CODE_OPS_NOT_SUPPORT));
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

static int32_t mndProcessKillBackupReq(SRpcMsg *pReq) {
  mError("failed to process kill backup req since %s", tstrerror(TSDB_CODE_OPS_NOT_SUPPORT));
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

static int32_t mndProcessBackupTimer(SRpcMsg *pReq) {
  return 0;
}

int32_t mndInitBackup(SMnode *pMnode) {
  // Backup/restore is an enterprise-only feature. The community build keeps
  // only the external entry points: CREATE/KILL BACKUP answer
  // OPS_NOT_SUPPORT, SHOW BACKUPS returns no rows, and the periodic backup
  // pullup message is swallowed by a silent timer stub (avoiding repeated
  // "failed to get msg handle" error logs).
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_BACKUP, mndRetrieveBackups);
  mndSetMsgHandle(pMnode, TDMT_MND_BACKUP, mndProcessBackupReq);
  mndSetMsgHandle(pMnode, TDMT_MND_KILL_BACKUP, mndProcessKillBackupReq);
  mndSetMsgHandle(pMnode, TDMT_MND_BACKUP_TIMER, mndProcessBackupTimer);
  return TSDB_CODE_SUCCESS;
}

void mndCleanupBackup(SMnode *pMnode) {}

#endif // TD_ENTERPRISE
