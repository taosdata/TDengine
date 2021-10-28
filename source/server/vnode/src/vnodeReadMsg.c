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
#include "tglobal.h"
// #include "query.h"
#include "vnodeStatus.h"
#include "vnodeRead.h"
#include "vnodeReadMsg.h"

#if 0
// notify connection(handle) that current qhandle is created, if current connection from
// client is broken, the query needs to be killed immediately.
static int32_t vnodeNotifyCurrentQhandle(void *handle, uint64_t qId, void *qhandle, int32_t vgId) {
  SRetrieveTableMsg *pMsg = rpcMallocCont(sizeof(SRetrieveTableMsg));
  pMsg->qId = htobe64(qId);
  pMsg->header.vgId = htonl(vgId);
  pMsg->header.contLen = htonl(sizeof(SRetrieveTableMsg));

  vTrace("QInfo:0x%" PRIx64 "-%p register qhandle to connect:%p", qId, qhandle, handle);
  return rpcReportProgress(handle, (char *)pMsg, sizeof(SRetrieveTableMsg));
}

/**
 * @param pRet         response message object
 * @param pVnode       the vnode object
 * @param handle       qhandle for executing query
 * @param freeHandle   free qhandle or not
 * @param ahandle      sqlObj address at client side
 * @return
 */
static int32_t vnodeDumpQueryResult(SVnRsp *pRet, void *pVnode, uint64_t qId, void **handle, bool *freeHandle,
                                    void *ahandle) {
  bool continueExec = false;

  int32_t code = TSDB_CODE_SUCCESS;
  if ((code = qDumpRetrieveResult(*handle, (SRetrieveTableRsp **)&pRet->rsp, &pRet->len, &continueExec)) ==
      TSDB_CODE_SUCCESS) {
    if (continueExec) {
      *freeHandle = false;
      code = vnodeReputPutToRQueue(pVnode, handle, ahandle);
      if (code != TSDB_CODE_SUCCESS) {
        *freeHandle = true;
        return code;
      } else {
        pRet->qhandle = *handle;
      }
    } else {
      *freeHandle = true;
      vTrace("QInfo:0x%" PRIx64 "-%p exec completed, free handle:%d", qId, *handle, *freeHandle);
    }
  } else {
    SRetrieveTableRsp *pRsp = (SRetrieveTableRsp *)rpcMallocCont(sizeof(SRetrieveTableRsp));
    memset(pRsp, 0, sizeof(SRetrieveTableRsp));
    pRsp->completed = true;

    pRet->rsp = pRsp;
    pRet->len = sizeof(SRetrieveTableRsp);
    *freeHandle = true;
  }

  return code;
}

static void vnodeBuildNoResultQueryRsp(SVnRsp *pRet) {
  pRet->rsp = (SRetrieveTableRsp *)rpcMallocCont(sizeof(SRetrieveTableRsp));
  pRet->len = sizeof(SRetrieveTableRsp);

  memset(pRet->rsp, 0, sizeof(SRetrieveTableRsp));
  SRetrieveTableRsp *pRsp = pRet->rsp;

  pRsp->completed = true;
}
#endif

int32_t vnodeProcessQueryMsg(SVnode *pVnode, SReadMsg *pRead) {
#if 0  
  void *   pCont = pRead->pCont;
  int32_t  contLen = pRead->contLen;
  SVnRsp *pRet = &pRead->rspRet;

  SQueryTableMsg *pQueryTableMsg = (SQueryTableMsg *)pCont;
  memset(pRet, 0, sizeof(SVnRsp));

  // qHandle needs to be freed correctly
  if (pRead->code == TSDB_CODE_RPC_NETWORK_UNAVAIL) {
    vError("error rpc msg in query, %s", tstrerror(pRead->code));
  }

  int32_t code = TSDB_CODE_SUCCESS;
  void ** handle = NULL;

  if (contLen != 0) {
    qinfo_t  pQInfo = NULL;
    uint64_t qId = genQueryId();
    code = qCreateQueryInfo(pVnode->tsdb, pVnode->vgId, pQueryTableMsg, &pQInfo, qId);

    SQueryTableRsp *pRsp = (SQueryTableRsp *)rpcMallocCont(sizeof(SQueryTableRsp));
    pRsp->code = code;
    pRsp->qId = 0;

    pRet->len = sizeof(SQueryTableRsp);
    pRet->rsp = pRsp;
    int32_t vgId = pVnode->vgId;

    // current connect is broken
    if (code == TSDB_CODE_SUCCESS) {
      handle = qRegisterQInfo(pVnode->qMgmt, qId, pQInfo);
      if (handle == NULL) {  // failed to register qhandle
        pRsp->code = terrno;
        terrno = 0;

        vError("vgId:%d, QInfo:0x%" PRIx64 "-%p register qhandle failed, return to app, code:%s,", pVnode->vgId, qId,
               (void *)pQInfo, tstrerror(pRsp->code));
        qDestroyQueryInfo(pQInfo);  // destroy it directly
        return pRsp->code;
      } else {
        assert(*handle == pQInfo);
        pRsp->qId = htobe64(qId);
      }

      if (handle != NULL &&
          vnodeNotifyCurrentQhandle(pRead->rpcHandle, qId, *handle, pVnode->vgId) != TSDB_CODE_SUCCESS) {
        vError("vgId:%d, QInfo:0x%" PRIx64 "-%p, query discarded since link is broken, %p", pVnode->vgId, qId, *handle,
               pRead->rpcHandle);

        pRsp->code = TSDB_CODE_RPC_NETWORK_UNAVAIL;
        qReleaseQInfo(pVnode->qMgmt, (void **)&handle, true);
        return pRsp->code;
      }

    } else {
      assert(pQInfo == NULL);
    }

    if (handle != NULL) {
      vTrace("vgId:%d, QInfo:0x%" PRIx64 "-%p, query msg disposed, create qhandle and returns to app", vgId, qId,
             *handle);
      code = vnodeReputPutToRQueue(pVnode, handle, pRead->rpcHandle);
      if (code != TSDB_CODE_SUCCESS) {
        pRsp->code = code;
        qReleaseQInfo(pVnode->qMgmt, (void **)&handle, true);
        return pRsp->code;
      }
    }

    int32_t remain = atomic_add_fetch_32(&pVnode->numOfQHandle, 1);
    vTrace("vgId:%d, new qhandle created, total qhandle:%d", pVnode->vgId, remain);
  } else {
    assert(pCont != NULL);
    void **  qhandle = (void **)pRead->qhandle;
    uint64_t qId = 0;

    vTrace("vgId:%d, QInfo:%p, continues to exec query", pVnode->vgId, *qhandle);

    // In the retrieve blocking model, only 50% CPU will be used in query processing
    if (tsRetrieveBlockingModel) {
      qTableQuery(*qhandle, &qId);  // do execute query
      qReleaseQInfo(pVnode->qMgmt, (void **)&qhandle, false);
    } else {
      bool freehandle = false;
      bool buildRes = qTableQuery(*qhandle, &qId);  // do execute query

      // build query rsp, the retrieve request has reached here already
      if (buildRes) {
        // update the connection info according to the retrieve connection
        pRead->rpcHandle = qGetResultRetrieveMsg(*qhandle);
        assert(pRead->rpcHandle != NULL);

        vTrace("vgId:%d, QInfo:%p, start to build retrieval rsp after query paused, %p", pVnode->vgId, *qhandle,
               pRead->rpcHandle);

        // set the real rsp error code
        pRead->code = vnodeDumpQueryResult(&pRead->rspRet, pVnode, qId, qhandle, &freehandle, pRead->rpcHandle);

        // NOTE: set return code to be TSDB_CODE_QRY_HAS_RSP to notify dnode to return msg to client
        code = TSDB_CODE_QRY_HAS_RSP;
      } else {
        // void *h1 = qGetResultRetrieveMsg(*qhandle);

        /* remove this assert, one possible case that will cause h1 not NULL: query thread unlock pQInfo->lock, and then
         * FETCH thread execute twice before query thread reach here */
        // assert(h1 == NULL);

        freehandle = qQueryCompleted(*qhandle);
      }

      // NOTE: if the qhandle is not put into vread queue or query is completed, free the qhandle.
      // If the building of result is not required, simply free it. Otherwise, mandatorily free the qhandle
      if (freehandle || (!buildRes)) {
        if (freehandle) {
          int32_t remain = atomic_sub_fetch_32(&pVnode->numOfQHandle, 1);
          vTrace("vgId:%d, QInfo:%p, start to free qhandle, remain qhandle:%d", pVnode->vgId, *qhandle, remain);
        }

        qReleaseQInfo(pVnode->qMgmt, (void **)&qhandle, freehandle);
      }
    }
  }

  return code;
#endif
  return 0;  
}

//mq related
int32_t vnodeProcessConsumeMsg(SVnode *pVnode, SReadMsg *pRead) {
  //parse message and optionally move offset
  void* pMsg = pRead->pCont;
  TmqConsumeReq *pConsumeMsg = (TmqConsumeReq*) pMsg;
  TmqMsgHead msgHead = pConsumeMsg->head;
  //extract head
  STQ *pTq = pVnode->pTQ;
  /*tqBufferHandle *pHandle = tqGetHandle(pTq, msgHead.clientId);*/
  //return msg if offset not moved
  /*if(pConsumeMsg->commitOffset == pHandle->consumeOffset) {*/
    //return msg
    /*return 0;*/
  /*}*/
  //or move offset
  /*tqMoveOffsetToNext(pHandle);*/
  //fetch or register context
  /*tqFetchMsg(pHandle, pRead);*/
  //judge mode, tail read or catch up read
  /*int64_t lastVer = walLastVer(pVnode->wal);*/
  //launch new query
  return 0;
}

int32_t vnodeProcessTqQueryMsg(SVnode *pVnode, SReadMsg *pRead) {
  //get operator tree from tq data structure
  //execute operator tree
  //put data into ringbuffer
  //unref memory
  return 0;
}
//mq related end

int32_t vnodeProcessFetchMsg(SVnode *pVnode, SReadMsg *pRead) {
#if 0  
  void *   pCont = pRead->pCont;
  SVnRsp *pRet = &pRead->rspRet;

  SRetrieveTableMsg *pRetrieve = pCont;
  pRetrieve->free = htons(pRetrieve->free);
  pRetrieve->qId = htobe64(pRetrieve->qId);

  vTrace("vgId:%d, qId:0x%" PRIx64 ", retrieve msg is disposed, free:%d, conn:%p", pVnode->vgId, pRetrieve->qId,
         pRetrieve->free, pRead->rpcHandle);

  memset(pRet, 0, sizeof(SVnRsp));

  terrno = TSDB_CODE_SUCCESS;
  int32_t code = TSDB_CODE_SUCCESS;
  void ** handle = qAcquireQInfo(pVnode->qMgmt, pRetrieve->qId);
  if (handle == NULL) {
    code = terrno;
    terrno = TSDB_CODE_SUCCESS;
  } else if (!checkQIdEqual(*handle, pRetrieve->qId)) {
    code = TSDB_CODE_QRY_INVALID_QHANDLE;
  }

  if (code != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, invalid qId in retrieving result, code:%s, QInfo:%" PRIu64, pVnode->vgId, tstrerror(code),
           pRetrieve->qId);
    vnodeBuildNoResultQueryRsp(pRet);
    return code;
  }

  // kill current query and free corresponding resources.
  if (pRetrieve->free == 1) {
    int32_t remain = atomic_sub_fetch_32(&pVnode->numOfQHandle, 1);
    vWarn("vgId:%d, QInfo:%" PRIx64 "-%p, retrieve msg received to kill query and free qhandle, remain qhandle:%d",
          pVnode->vgId, pRetrieve->qId, *handle, remain);

    qKillQuery(*handle);
    qReleaseQInfo(pVnode->qMgmt, (void **)&handle, true);

    vnodeBuildNoResultQueryRsp(pRet);
    code = TSDB_CODE_TSC_QUERY_CANCELLED;
    return code;
  }

  // register the qhandle to connect to quit query immediate if connection is broken
  if (vnodeNotifyCurrentQhandle(pRead->rpcHandle, pRetrieve->qId, *handle, pVnode->vgId) != TSDB_CODE_SUCCESS) {
    int32_t remain = atomic_sub_fetch_32(&pVnode->numOfQHandle, 1);
    vError("vgId:%d, QInfo:%" PRIu64 "-%p, retrieve discarded since link is broken, conn:%p, remain qhandle:%d",
           pVnode->vgId, pRetrieve->qhandle, *handle, pRead->rpcHandle, remain);

    code = TSDB_CODE_RPC_NETWORK_UNAVAIL;
    qKillQuery(*handle);
    qReleaseQInfo(pVnode->qMgmt, (void **)&handle, true);
    return code;
  }

  bool freeHandle = true;
  bool buildRes = false;

  code = qRetrieveQueryResultInfo(*handle, &buildRes, pRead->rpcHandle);
  if (code != TSDB_CODE_SUCCESS) {
    // TODO handle malloc failure
    pRet->rsp = (SRetrieveTableRsp *)rpcMallocCont(sizeof(SRetrieveTableRsp));
    pRet->len = sizeof(SRetrieveTableRsp);
    memset(pRet->rsp, 0, sizeof(SRetrieveTableRsp));
    freeHandle = true;
  } else {  // result is not ready, return immediately
    // Only affects the non-blocking model
    if (!tsRetrieveBlockingModel) {
      if (!buildRes) {
        assert(pRead->rpcHandle != NULL);
        qReleaseQInfo(pVnode->qMgmt, (void **)&handle, false);
        return TSDB_CODE_QRY_NOT_READY;
      }
    }

    // ahandle is the sqlObj pointer
    code = vnodeDumpQueryResult(pRet, pVnode, pRetrieve->qId, handle, &freeHandle, pRead->rpcHandle);
  }

  // If qhandle is not added into vread queue, the query should be completed already or paused with error.
  // Here free qhandle immediately
  if (freeHandle) {
    int32_t remain = atomic_sub_fetch_32(&pVnode->numOfQHandle, 1);
    vTrace("vgId:%d, QInfo:%p, start to free qhandle, remain qhandle:%d", pVnode->vgId, *handle, remain);
    qReleaseQInfo(pVnode->qMgmt, (void **)&handle, true);
  }

  return code;
#endif 
  return 0;
}

