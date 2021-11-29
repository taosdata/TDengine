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
    pVnodeReq = (SVnodeReq *)(pMsg->pCont);
    pVnodeReq->ver = pVnode->state.processed++;

    if (walWrite(pVnode->pWal, pVnodeReq->ver, pVnodeReq->req, pMsg->contLen - sizeof(pVnodeReq->ver)) < 0) {
      // TODO: handle error
    }
  }

  walFsync(pVnode->pWal, false);

  // Apply each request now
  for (int i = 0; i < taosArrayGetSize(pMsgs); i++) {
    pMsg = *(SRpcMsg **)taosArrayGet(pMsgs, i);
    pVnodeReq = (SVnodeReq *)(pMsg->pCont);
    SVCreateTableReq ctReq;
    SVDropTableReq   dtReq;

    // Apply the request
    {
      void *ptr = vnodeMalloc(pVnode, pMsg->contLen);
      if (ptr == NULL) {
        // TODO: handle error
      }

      memcpy(ptr, pVnodeReq, pMsg->contLen);

      // todo: change the interface here
      if (tqPushMsg(pVnode->pTq, ptr, pVnodeReq->ver) < 0) {
        // TODO: handle error
      }

      switch (pMsg->msgType) {
        case TSDB_MSG_TYPE_CREATE_TABLE:
          if (vnodeParseCreateTableReq(pVnodeReq->req, pMsg->contLen - sizeof(pVnodeReq->ver), &(ctReq)) < 0) {
            // TODO: handle error
          }

          if (metaCreateTable(pVnode->pMeta, &ctReq) < 0) {
            // TODO: handle error
          }

          // TODO: maybe need to clear the requst struct
          break;
        case TSDB_MSG_TYPE_DROP_TABLE:
          if (vnodeParseDropTableReq(pVnodeReq->req, pMsg->contLen - sizeof(pVnodeReq->ver), &(dtReq)) < 0) {
            // TODO: handle error
          }

          if (metaDropTable(pVnode->pMeta, dtReq.uid) < 0) {
            // TODO: handle error
          }
          break;
        case TSDB_MSG_TYPE_SUBMIT:
          /* code */
          break;
        default:
          break;
      }
    }

    pVnode->state.applied = pVnodeReq->ver;

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