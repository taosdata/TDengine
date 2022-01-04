/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#define _DEFAULT_SOURCE
#include "mndSync.h"

int32_t mndInitSync(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  tsem_init(&pMgmt->syncSem, 0, 0);

  pMgmt->state = TAOS_SYNC_STATE_LEADER;
  pMgmt->pSyncNode = NULL;
  return 0;
}

void mndCleanupSync(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  tsem_destroy(&pMgmt->syncSem);
}

static int32_t mndSyncApplyCb(struct SSyncFSM *fsm, SyncIndex index, const SSyncBuffer *buf, void *pData) {
  SMnode    *pMnode = pData;
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;

  pMgmt->errCode = 0;
  tsem_post(&pMgmt->syncSem);

  return 0;
}

int32_t mndSyncPropose(SMnode *pMnode, SSdbRaw *pRaw) {
#if 1
  return 0;
#else
  if (pMnode->replica == 1) return 0;

  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  pMgmt->errCode = 0;

  SSyncBuffer buf = {.data = pRaw, .len = sdbGetRawTotalSize(pRaw)};

  bool    isWeak = false;
  int32_t code = syncPropose(pMgmt->pSyncNode, &buf, pMnode, isWeak);

  if (code != 0) return code;

  tsem_wait(&pMgmt->syncSem);
  return pMgmt->errCode;
#endif
}

bool mndIsMaster(SMnode *pMnode) {
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  return pMgmt->state == TAOS_SYNC_STATE_LEADER;
}