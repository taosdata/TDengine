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

#include "vnodeDef.h"

int vnodeProcessWMsgs(SVnode *pVnode, SArray *pMsgs) {
  SRpcMsg *  pMsg;
  SVnodeReq *pVnodeReq;

  for (int i = 0; i < taosArrayGetSize(pMsgs); i++) {
    pMsg = *(SRpcMsg **)taosArrayGet(pMsgs, i);

    // ser request version
    void *   pBuf = pMsg->pCont;
    uint64_t ver = pVnode->state.processed++;
    taosEncodeFixedU64(&pBuf, ver);

    if (walWrite(pVnode->pWal, ver, pMsg->pCont, pMsg->contLen) < 0) {
      // TODO: handle error
    }
  }

  walFsync(pVnode->pWal, false);

  // Apply each request now
  for (int i = 0; i < taosArrayGetSize(pMsgs); i++) {
    pMsg = *(SRpcMsg **)taosArrayGet(pMsgs, i);
    SVnodeReq vReq;

    // Apply the request
    {
      void *ptr = vnodeMalloc(pVnode, pMsg->contLen);
      if (ptr == NULL) {
        // TODO: handle error
      }

      // TODO: copy here need to be extended
      memcpy(ptr, pMsg->pCont, pMsg->contLen);

      // todo: change the interface here
      uint64_t ver;
      taosDecodeFixedU64(pMsg->pCont, &ver);
      if (tqPushMsg(pVnode->pTq, ptr, ver) < 0) {
        // TODO: handle error
      }

      vnodeParseReq(pMsg->pCont, &vReq, pMsg->msgType);

      switch (pMsg->msgType) {
        case TSDB_MSG_TYPE_CREATE_TABLE:
          if (metaCreateTable(pVnode->pMeta, &(vReq.ctReq)) < 0) {
            // TODO: handle error
          }

          // TODO: maybe need to clear the requst struct
          break;
        case TSDB_MSG_TYPE_DROP_TABLE:
          if (metaDropTable(pVnode->pMeta, vReq.dtReq.uid) < 0) {
            // TODO: handle error
          }
          break;
        case TSDB_MSG_TYPE_SUBMIT:
          /* code */
          break;
        default:
          break;
      }

      pVnode->state.applied = ver;
    }

    // Check if it needs to commit
    if (vnodeShouldCommit(pVnode)) {
      if (vnodeAsyncCommit(pVnode) < 0) {
        // TODO: handle error
      }
    }
  }

  return 0;
}

int vnodeApplyWMsg(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp) {
  // TODO
  return 0;
}

/* ------------------------ STATIC METHODS ------------------------ */