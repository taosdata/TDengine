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

#include "vnd.h"
#include "tq.h"

int vnodeProcessNoWalWMsgs(SVnode *pVnode, SRpcMsg *pMsg) {
  switch (pMsg->msgType) {
    case TDMT_VND_MQ_SET_CUR:
      if (tqSetCursor(pVnode->pTq, pMsg->pCont) < 0) {
        // TODO: handle error
      }
      break;
  }
  return 0;
}

int vnodeProcessWMsgs(SVnode *pVnode, SArray *pMsgs) {
  SRpcMsg *pMsg;

  for (int i = 0; i < taosArrayGetSize(pMsgs); i++) {
    pMsg = *(SRpcMsg **)taosArrayGet(pMsgs, i);

    // ser request version
    void *  pBuf = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
    int64_t ver = pVnode->state.processed++;
    taosEncodeFixedU64(&pBuf, ver);

    if (walWrite(pVnode->pWal, ver, pMsg->msgType, pMsg->pCont, pMsg->contLen) < 0) {
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
  void *             ptr = vnodeMalloc(pVnode, pMsg->contLen);
  if (ptr == NULL) {
    // TODO: handle error
  }

  // TODO: copy here need to be extended
  memcpy(ptr, pMsg->pCont, pMsg->contLen);

  // todo: change the interface here
  uint64_t ver;
  taosDecodeFixedU64(POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead)), &ver);
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
      break;
    case TDMT_VND_CREATE_TABLE:
      tSVCreateTbBatchReqDeserialize(POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead)), &vCreateTbBatchReq);
      for (int i = 0; i < taosArrayGetSize(vCreateTbBatchReq.pArray); i++) {
        SVCreateTbReq *pCreateTbReq = taosArrayGet(vCreateTbBatchReq.pArray, i);
        if (metaCreateTable(pVnode->pMeta, pCreateTbReq) < 0) {
          // TODO: handle error
        }
        vTrace("vgId:%d process create table %s", pVnode->vgId, pCreateTbReq->name);
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
      taosArrayDestroy(vCreateTbBatchReq.pArray);
      break;

    case TDMT_VND_DROP_STB:
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
      //TODO: wrap in a function
      char* reqStr = ptr;
      SMqSetCVgReq req;
      tDecodeSMqSetCVgReq(reqStr, &req);
      STqConsumerHandle* pConsumer = calloc(sizeof(STqConsumerHandle), 1);
      
      STqTopicHandle* pTopic = calloc(sizeof(STqTopicHandle), 1);
      if (pTopic == NULL) {
        // TODO: handle error
      }
      strcpy(pTopic->topicName, req.topicName); 
      strcpy(pTopic->cgroup, req.cGroup); 
      strcpy(pTopic->sql, req.sql);
      strcpy(pTopic->logicalPlan, req.logicalPlan);
      strcpy(pTopic->physicalPlan, req.physicalPlan);
      SArray *pArray;
      //TODO: deserialize to SQueryDag
      SQueryDag *pDag;
      // convert to task
      if (schedulerConvertDagToTaskList(pDag, &pArray) < 0) {
        // TODO: handle error
      }
      ASSERT(taosArrayGetSize(pArray) == 0);
      STaskInfo *pInfo = taosArrayGet(pArray, 0);
      SArray* pTasks;
      schedulerCopyTask(pInfo, &pTasks, TQ_BUFFER_SIZE);
      pTopic->buffer.firstOffset = -1;
      pTopic->buffer.lastOffset = -1;
      for (int i = 0; i < TQ_BUFFER_SIZE; i++) {
        SSubQueryMsg* pMsg = taosArrayGet(pTasks, i);
        pTopic->buffer.output[i].pMsg = pMsg;
        pTopic->buffer.output[i].status = 0;
      }
      pTopic->pReadhandle = walOpenReadHandle(pVnode->pTq->pWal);
      // write mq meta
    }
      break;
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
