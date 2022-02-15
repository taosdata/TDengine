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

#include "tq.h"
#include "vnd.h"

int vnodeProcessWMsgs(SVnode *pVnode, SArray *pMsgs) {
  SRpcMsg *pMsg;

  for (int i = 0; i < taosArrayGetSize(pMsgs); i++) {
    pMsg = *(SRpcMsg **)taosArrayGet(pMsgs, i);

    // ser request version
    void   *pBuf = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
    int64_t ver = pVnode->state.processed++;
    taosEncodeFixedI64(&pBuf, ver);

    if (walWrite(pVnode->pWal, ver, pMsg->msgType, pMsg->pCont, pMsg->contLen) < 0) {
      /*ASSERT(false);*/
      // TODO: handle error
    }
  }

  walFsync(pVnode->pWal, false);

  // TODO: Integrate RAFT module here

  return 0;
}

int vnodeApplyWMsg(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp) {
  SVCreateTbReq      vCreateTbReq;
  SVCreateTbBatchReq vCreateTbBatchReq;
  void              *ptr = vnodeMalloc(pVnode, pMsg->contLen);
  if (ptr == NULL) {
    // TODO: handle error
  }

  // TODO: copy here need to be extended
  memcpy(ptr, pMsg->pCont, pMsg->contLen);

  // todo: change the interface here
  int64_t ver;
  taosDecodeFixedI64(POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead)), &ver);
  if (tqPushMsg(pVnode->pTq, ptr, ver) < 0) {
    // TODO: handle error
  }

  switch (pMsg->msgType) {
    case TDMT_VND_CREATE_STB:
      tDeserializeSVCreateTbReq(POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead)), &vCreateTbReq);
      if (metaCreateTable(pVnode->pMeta, &(vCreateTbReq)) < 0) {
        // TODO: handle error
      }

      // TODO: maybe need to clear the requst struct
      free(vCreateTbReq.stbCfg.pSchema);
      free(vCreateTbReq.stbCfg.pTagSchema);
      free(vCreateTbReq.name);
      break;
    case TDMT_VND_CREATE_TABLE:
      tDeserializeSVCreateTbBatchReq(POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead)), &vCreateTbBatchReq);
      for (int i = 0; i < taosArrayGetSize(vCreateTbBatchReq.pArray); i++) {
        SVCreateTbReq *pCreateTbReq = taosArrayGet(vCreateTbBatchReq.pArray, i);
        if (metaCreateTable(pVnode->pMeta, pCreateTbReq) < 0) {
          // TODO: handle error
          vError("vgId:%d, failed to create table: %s", pVnode->vgId, pCreateTbReq->name);
        }
        free(pCreateTbReq->name);
        if (pCreateTbReq->type == TD_SUPER_TABLE) {
          free(pCreateTbReq->stbCfg.pSchema);
          free(pCreateTbReq->stbCfg.pTagSchema);
        } else if (pCreateTbReq->type == TD_CHILD_TABLE) {
          free(pCreateTbReq->ctbCfg.pTag);
        } else {
          free(pCreateTbReq->ntbCfg.pSchema);
        }
      }

      vTrace("vgId:%d process create %" PRIzu " tables", pVnode->vgId, taosArrayGetSize(vCreateTbBatchReq.pArray));
      taosArrayDestroy(vCreateTbBatchReq.pArray);
      break;

    case TDMT_VND_ALTER_STB:
      vTrace("vgId:%d, process alter stb req", pVnode->vgId);
      tDeserializeSVCreateTbReq(POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead)), &vCreateTbReq);
      free(vCreateTbReq.stbCfg.pSchema);
      free(vCreateTbReq.stbCfg.pTagSchema);
      free(vCreateTbReq.name);
      break;
    case TDMT_VND_DROP_STB:
      vTrace("vgId:%d, process drop stb req", pVnode->vgId);
      break;
    case TDMT_VND_DROP_TABLE:
      // if (metaDropTable(pVnode->pMeta, vReq.dtReq.uid) < 0) {
      //   // TODO: handle error
      // }
      break;
    case TDMT_VND_SUBMIT:
      if (tsdbInsertData(pVnode->pTsdb, (SSubmitMsg *)ptr, NULL) < 0) {
        // TODO: handle error
      }
      break;
    case TDMT_VND_MQ_SET_CONN: {
      if (tqProcessSetConnReq(pVnode->pTq, POINTER_SHIFT(ptr, sizeof(SMsgHead))) < 0) {
        // TODO: handle error
      }
    } break;
    case TDMT_VND_MQ_REB: {
      if (tqProcessRebReq(pVnode->pTq, POINTER_SHIFT(ptr, sizeof(SMsgHead))) < 0) {
      }
    } break;
    default:
      ASSERT(0);
      break;
  }

  pVnode->state.applied = ver;

  // Check if it needs to commit
  if (vnodeShouldCommit(pVnode)) {
    // tsem_wait(&(pVnode->canCommit));
    if (vnodeAsyncCommit(pVnode) < 0) {
      // TODO: handle error
    }
  }

  return 0;
}

/* ------------------------ STATIC METHODS ------------------------ */
