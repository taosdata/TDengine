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
#include "os.h"
#include "mndInt.h"
#include "mndTrans.h"

int32_t mndInitSync(SMnode *pMnode) { return 0; }
void    mndCleanupSync(SMnode *pMnode) {}

int32_t mndSyncPropose(SMnode *pMnode, SSdbRaw *pRaw, STransMsg *pMsg) {
  int32_t code = 0;

  int32_t  len = sdbGetRawTotalSize(pRaw);
  SSdbRaw *pReceived = calloc(1, len);
  memcpy(pReceived, pRaw, len);
  mDebug("trans:%d, data:%p recv from sync, code:0x%x pMsg:%p", pMsg->id, pReceived, code & 0xFFFF, pMsg);

  mndTransApply(pMnode, pReceived, pMsg, code);
  return 0;
}

bool mndIsMaster(SMnode *pMnode) { return true; }