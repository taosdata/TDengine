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
#include "vnodeInt.h"
#include "tqueue.h"

int32_t vnodeInit(SVnodePara para) { return 0; }
void    vnodeCleanup() {}

SVnode *vnodeOpen(int32_t vgId, const char *path) { return NULL; }
void    vnodeClose(SVnode *pVnode) {}
int32_t vnodeAlter(SVnode *pVnode, const SVnodeCfg *pCfg) { return 0; }
SVnode *vnodeCreate(int32_t vgId, const char *path, const SVnodeCfg *pCfg) { return NULL; }
int32_t vnodeDrop(SVnode *pVnode) { return 0; }
int32_t vnodeCompact(SVnode *pVnode) { return 0; }
int32_t vnodeSync(SVnode *pVnode) { return 0; }

void vnodeGetLoad(SVnode *pVnode, SVnodeLoad *pLoad) {}

SVnodeMsg *vnodeInitMsg(int32_t msgNum) {
  SVnodeMsg *pMsg = taosAllocateQitem(msgNum * sizeof(SRpcMsg *) + sizeof(SVnodeMsg));
  if (pMsg == NULL) {
    terrno = TSDB_CODE_VND_OUT_OF_MEMORY;
    return NULL;
  } else {
    pMsg->allocNum = msgNum;
    return pMsg;
  }
}

int32_t vnodeAppendMsg(SVnodeMsg *pMsg, SRpcMsg *pRpcMsg) {
  if (pMsg->curNum >= pMsg->allocNum) {
    return TSDB_CODE_VND_OUT_OF_MEMORY;
  }

  pMsg->rpcMsg[pMsg->curNum++] = *pRpcMsg;
}

void vnodeCleanupMsg(SVnodeMsg *pMsg) {
  for (int32_t i = 0; i < pMsg->curNum; ++i) {
    rpcFreeCont(pMsg->rpcMsg[i].pCont);
  }
  taosFreeQitem(pMsg);
}

void vnodeProcessMsg(SVnode *pVnode, SVnodeMsg *pMsg, EVMType msgType) {
  switch (msgType) {
    case VN_MSG_TYPE_WRITE:
      break;
    case VN_MSG_TYPE_APPLY:
      break;
    case VN_MSG_TYPE_SYNC:
      break;
    case VN_MSG_TYPE_QUERY:
      break;
    case VN_MSG_TYPE_FETCH:
      break;
  }
}
