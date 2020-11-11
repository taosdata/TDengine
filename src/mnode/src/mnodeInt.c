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
#include "taosmsg.h"
#include "taoserror.h"
#include "trpc.h"
#include "tqueue.h"
#include "mnode.h"
#include "dnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeAcct.h"
#include "mnodeDb.h"
#include "mnodeDnode.h"
#include "mnodeMnode.h"
#include "mnodeProfile.h"
#include "mnodeSdb.h"
#include "mnodeShow.h"
#include "mnodeTable.h"
#include "mnodeUser.h"
#include "mnodeVgroup.h"

void *mnodeCreateMsg(SRpcMsg *pRpcMsg) {
  int32_t    size = sizeof(SMnodeMsg) + pRpcMsg->contLen;
  SMnodeMsg *pMsg = taosAllocateQitem(size);

  pMsg->rpcMsg = *pRpcMsg;
  pMsg->rpcMsg.pCont = pMsg->pCont;
  pMsg->incomingTs = taosGetTimestampSec();
  memcpy(pMsg->pCont, pRpcMsg->pCont, pRpcMsg->contLen);

  return pMsg;
}

int32_t mnodeInitMsg(SMnodeMsg *pMsg) {
  if (pMsg->pUser != NULL) {
    mDebug("app:%p:%p, user info already inited", pMsg->rpcMsg.ahandle, pMsg);
    return TSDB_CODE_SUCCESS;
  }

  pMsg->pUser = mnodeGetUserFromConn(pMsg->rpcMsg.handle);
  if (pMsg->pUser == NULL) {
    return TSDB_CODE_MND_INVALID_USER;
  }

  return TSDB_CODE_SUCCESS;
}

void mnodeCleanupMsg(SMnodeMsg *pMsg) {
  if (pMsg != NULL) {
    if (pMsg->rpcMsg.pCont != pMsg->pCont) {
      tfree(pMsg->rpcMsg.pCont);
    }
    if (pMsg->pUser) mnodeDecUserRef(pMsg->pUser);
    if (pMsg->pDb) mnodeDecDbRef(pMsg->pDb);
    if (pMsg->pVgroup) mnodeDecVgroupRef(pMsg->pVgroup);
    if (pMsg->pTable) mnodeDecTableRef(pMsg->pTable);
    if (pMsg->pSTable) mnodeDecTableRef(pMsg->pSTable);
    if (pMsg->pAcct) mnodeDecAcctRef(pMsg->pAcct);
    if (pMsg->pDnode) mnodeDecDnodeRef(pMsg->pDnode);
  }
}
