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

int32_t tqInit() {
  int8_t old;
  while (1) {
    old = atomic_val_compare_exchange_8(&tqMgmt.inited, 0, 2);
    if (old != 2) break;
  }

  if (old == 0) {
    tqMgmt.timer = taosTmrInit(10000, 100, 10000, "TQ");
    if (tqMgmt.timer == NULL) {
      atomic_store_8(&tqMgmt.inited, 0);
      return -1;
    }
    if (streamInit() < 0) {
      return -1;
    }
    atomic_store_8(&tqMgmt.inited, 1);
  }

  return 0;
}

void tqCleanUp() {
  int8_t old;
  while (1) {
    old = atomic_val_compare_exchange_8(&tqMgmt.inited, 1, 2);
    if (old != 2) break;
  }

  if (old == 1) {
    taosTmrCleanUp(tqMgmt.timer);
    streamCleanUp();
    atomic_store_8(&tqMgmt.inited, 0);
  }
}

STQ* tqOpen(const char* path, SVnode* pVnode) {
  STQ* pTq = taosMemoryCalloc(1, sizeof(STQ));
  if (pTq == NULL) {
    terrno = TSDB_CODE_TQ_OUT_OF_MEMORY;
    return NULL;
  }
  pTq->path = strdup(path);
  pTq->pVnode = pVnode;

  pTq->handles = taosHashInit(64, MurmurHash3_32, true, HASH_ENTRY_LOCK);

  pTq->pushMgr = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);

  pTq->pAlterInfo = taosHashInit(64, MurmurHash3_32, true, HASH_ENTRY_LOCK);

  if (tqMetaOpen(pTq) < 0) {
    ASSERT(0);
  }

  if (tqOffsetOpen(pTq) < 0) {
    ASSERT(0);
  }

  pTq->pStreamMeta = streamMetaOpen(path, pTq, (FTaskExpand*)tqExpandTask);
  if (pTq->pStreamMeta == NULL) {
    ASSERT(0);
  }

  return pTq;
}

void tqClose(STQ* pTq) {
  if (pTq) {
    tqOffsetClose(pTq->pOffsetStore);
    taosHashCleanup(pTq->handles);
    taosHashCleanup(pTq->pushMgr);
    taosHashCleanup(pTq->pAlterInfo);
    taosMemoryFree(pTq->path);
    tqMetaClose(pTq);
    streamMetaClose(pTq->pStreamMeta);
    taosMemoryFree(pTq);
  }
}

int32_t tqSendMetaPollRsp(STQ* pTq, const SRpcMsg* pMsg, const SMqPollReq* pReq, const SMqMetaRsp* pRsp) {
  int32_t tlen = sizeof(SMqRspHead) + tEncodeSMqMetaRsp(NULL, pRsp);
  void*   buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    return -1;
  }

  ((SMqRspHead*)buf)->mqMsgType = TMQ_MSG_TYPE__POLL_META_RSP;
  ((SMqRspHead*)buf)->epoch = pReq->epoch;
  ((SMqRspHead*)buf)->consumerId = pReq->consumerId;

  void* abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));
  tEncodeSMqMetaRsp(&abuf, pRsp);

  SRpcMsg resp = {
      .info = pMsg->info,
      .pCont = buf,
      .contLen = tlen,
      .code = 0,
  };
  tmsgSendRsp(&resp);

  tqDebug("vgId:%d, from consumer:%" PRId64 ", (epoch %d) send rsp, res msg type %d, reqOffset:%" PRId64
          ", rspOffset:%" PRId64,
          TD_VID(pTq->pVnode), pReq->consumerId, pReq->epoch, pRsp->resMsgType, pRsp->reqOffset, pRsp->rspOffset);

  return 0;
}

int32_t tqSendDataRsp(STQ* pTq, const SRpcMsg* pMsg, const SMqPollReq* pReq, const SMqDataRsp* pRsp) {
  ASSERT(taosArrayGetSize(pRsp->blockData) == pRsp->blockNum);
  ASSERT(taosArrayGetSize(pRsp->blockDataLen) == pRsp->blockNum);

  if (pRsp->withSchema) {
    ASSERT(taosArrayGetSize(pRsp->blockSchema) == pRsp->blockNum);
  } else {
    ASSERT(taosArrayGetSize(pRsp->blockSchema) == 0);
  }

  if (pRsp->reqOffset.type == TMQ_OFFSET__LOG) {
    if (pRsp->blockNum > 0) {
      ASSERT(pRsp->rspOffset.version > pRsp->reqOffset.version);
    } else {
      ASSERT(pRsp->rspOffset.version >= pRsp->reqOffset.version);
    }
  }

  int32_t len = 0;
  int32_t code = 0;
  tEncodeSize(tEncodeSMqDataRsp, pRsp, len, code);
  if (code < 0) {
    return -1;
  }
  int32_t tlen = sizeof(SMqRspHead) + len;
  void*   buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    return -1;
  }

  ((SMqRspHead*)buf)->mqMsgType = TMQ_MSG_TYPE__POLL_RSP;
  ((SMqRspHead*)buf)->epoch = pReq->epoch;
  ((SMqRspHead*)buf)->consumerId = pReq->consumerId;

  void* abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));

  SEncoder encoder = {0};
  tEncoderInit(&encoder, abuf, len);
  tEncodeSMqDataRsp(&encoder, pRsp);
  tEncoderClear(&encoder);

  SRpcMsg rsp = {
      .info = pMsg->info,
      .pCont = buf,
      .contLen = tlen,
      .code = 0,
  };
  tmsgSendRsp(&rsp);

  char buf1[80] = {0};
  char buf2[80] = {0};
  tFormatOffset(buf1, 80, &pRsp->reqOffset);
  tFormatOffset(buf2, 80, &pRsp->rspOffset);
  tqDebug("vgId:%d, from consumer:%" PRId64 ", (epoch %d) send rsp, block num: %d, reqOffset:%s, rspOffset:%s",
          TD_VID(pTq->pVnode), pReq->consumerId, pReq->epoch, pRsp->blockNum, buf1, buf2);

  return 0;
}

int32_t tqProcessOffsetCommitReq(STQ* pTq, char* msg, int32_t msgLen) {
  STqOffset offset = {0};
  SDecoder  decoder;
  tDecoderInit(&decoder, msg, msgLen);
  if (tDecodeSTqOffset(&decoder, &offset) < 0) {
    ASSERT(0);
    return -1;
  }
  tDecoderClear(&decoder);

  if (offset.val.type == TMQ_OFFSET__SNAPSHOT_DATA) {
    tqDebug("receive offset commit msg to %s on vgId:%d, offset(type:snapshot) uid:%" PRId64 ", ts:%" PRId64,
            offset.subKey, TD_VID(pTq->pVnode), offset.val.uid, offset.val.ts);
  } else if (offset.val.type == TMQ_OFFSET__LOG) {
    tqDebug("receive offset commit msg to %s on vgId:%d, offset(type:log) version:%" PRId64, offset.subKey,
            TD_VID(pTq->pVnode), offset.val.version);
  } else {
    ASSERT(0);
  }
  /*STqOffset* pOffset = tqOffsetRead(pTq->pOffsetStore, offset.subKey);*/
  /*if (pOffset != NULL) {*/
  /*if (pOffset->val.type == TMQ_OFFSET__LOG && pOffset->val.version < offset.val.version) {*/
  if (tqOffsetWrite(pTq->pOffsetStore, &offset) < 0) {
    ASSERT(0);
    return -1;
  }

  if (offset.val.type == TMQ_OFFSET__LOG) {
    STqHandle* pHandle = taosHashGet(pTq->handles, offset.subKey, strlen(offset.subKey));
    if (pHandle) {
      if (walRefVer(pHandle->pRef, offset.val.version) < 0) {
        ASSERT(0);
        return -1;
      }
    }
  }

  /*}*/
  /*}*/

  return 0;
}

int32_t tqCheckColModifiable(STQ* pTq, int64_t tbUid, int32_t colId) {
  void* pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pTq->pAlterInfo, pIter);
    if (pIter == NULL) break;
    SCheckAlterInfo* pCheck = (SCheckAlterInfo*)pIter;
    if (pCheck->ntbUid == tbUid) {
      int32_t sz = taosArrayGetSize(pCheck->colIdList);
      for (int32_t i = 0; i < sz; i++) {
        int16_t forbidColId = *(int16_t*)taosArrayGet(pCheck->colIdList, i);
        if (forbidColId == colId) {
          taosHashCancelIterate(pTq->pAlterInfo, pIter);
          return -1;
        }
      }
    }
  }
  return 0;
}

static int32_t tqInitDataRsp(SMqDataRsp* pRsp, const SMqPollReq* pReq, int8_t subType) {
  pRsp->reqOffset = pReq->reqOffset;

  pRsp->blockData = taosArrayInit(0, sizeof(void*));
  pRsp->blockDataLen = taosArrayInit(0, sizeof(int32_t));

  if (pRsp->blockData == NULL || pRsp->blockDataLen == NULL) {
    return -1;
  }

  pRsp->withTbName = pReq->withTbName;
  if (pRsp->withTbName) {
    pRsp->blockTbName = taosArrayInit(0, sizeof(void*));
    if (pRsp->blockTbName == NULL) {
      // TODO free
      return -1;
    }
  }

  if (subType == TOPIC_SUB_TYPE__COLUMN) {
    pRsp->withSchema = false;
  } else {
    pRsp->withSchema = true;
    pRsp->blockSchema = taosArrayInit(0, sizeof(void*));
    if (pRsp->blockSchema == NULL) {
      // TODO free
      return -1;
    }
  }
  return 0;
}

int32_t tqProcessPollReq(STQ* pTq, SRpcMsg* pMsg) {
  SMqPollReq*  pReq = pMsg->pCont;
  int64_t      consumerId = pReq->consumerId;
  int64_t      timeout = pReq->timeout;
  int32_t      reqEpoch = pReq->epoch;
  int32_t      code = 0;
  STqOffsetVal reqOffset = pReq->reqOffset;
  STqOffsetVal fetchOffsetNew;
  SWalCkHead*  pCkHead = NULL;

  // 1.find handle
  STqHandle* pHandle = taosHashGet(pTq->handles, pReq->subKey, strlen(pReq->subKey));
  /*ASSERT(pHandle);*/
  if (pHandle == NULL) {
    tqError("tmq poll: no consumer handle for consumer:%" PRId64 ", in vgId:%d, subkey %s", consumerId,
            TD_VID(pTq->pVnode), pReq->subKey);
    return -1;
  }

  // check rebalance
  if (pHandle->consumerId != consumerId) {
    tqError("tmq poll: consumer handle mismatch for consumer:%" PRId64
            ", in vgId:%d, subkey %s, handle consumer id %" PRId64,
            consumerId, TD_VID(pTq->pVnode), pReq->subKey, pHandle->consumerId);
    return -1;
  }

  // update epoch if need
  int32_t consumerEpoch = atomic_load_32(&pHandle->epoch);
  while (consumerEpoch < reqEpoch) {
    consumerEpoch = atomic_val_compare_exchange_32(&pHandle->epoch, consumerEpoch, reqEpoch);
  }

  char buf[80];
  tFormatOffset(buf, 80, &reqOffset);
  tqDebug("tmq poll: consumer %" PRId64 " (epoch %d), subkey %s, recv poll req in vg %d, req offset %s", consumerId,
          pReq->epoch, pHandle->subKey, TD_VID(pTq->pVnode), buf);

  SMqDataRsp dataRsp = {0};
  tqInitDataRsp(&dataRsp, pReq, pHandle->execHandle.subType);

  // 2.reset offset if needed
  if (reqOffset.type > 0) {
    fetchOffsetNew = reqOffset;
  } else {
    STqOffset* pOffset = tqOffsetRead(pTq->pOffsetStore, pReq->subKey);
    if (pOffset != NULL) {
      fetchOffsetNew = pOffset->val;
      char formatBuf[80];
      tFormatOffset(formatBuf, 80, &fetchOffsetNew);
      tqDebug("tmq poll: consumer %" PRId64 ", subkey %s, vg %d, offset reset to %s", consumerId, pHandle->subKey,
              TD_VID(pTq->pVnode), formatBuf);
    } else {
      if (reqOffset.type == TMQ_OFFSET__RESET_EARLIEAST) {
        if (pReq->useSnapshot && pHandle->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
          if (!pHandle->fetchMeta) {
            tqOffsetResetToData(&fetchOffsetNew, 0, 0);
          } else {
            // reset to meta
            ASSERT(0);
          }
        } else {
          tqOffsetResetToLog(&fetchOffsetNew, walGetFirstVer(pTq->pVnode->pWal));
        }
      } else if (reqOffset.type == TMQ_OFFSET__RESET_LATEST) {
        tqOffsetResetToLog(&dataRsp.rspOffset, walGetLastVer(pTq->pVnode->pWal));
        tqDebug("tmq poll: consumer %" PRId64 ", subkey %s, vg %d, offset reset to %" PRId64, consumerId,
                pHandle->subKey, TD_VID(pTq->pVnode), dataRsp.rspOffset.version);
        if (tqSendDataRsp(pTq, pMsg, pReq, &dataRsp) < 0) {
          code = -1;
        }
        goto OVER;
      } else if (reqOffset.type == TMQ_OFFSET__RESET_NONE) {
        tqError("tmq poll: subkey %s, no offset committed for consumer %" PRId64
                " in vg %d, subkey %s, reset none failed",
                pHandle->subKey, consumerId, TD_VID(pTq->pVnode), pReq->subKey);
        terrno = TSDB_CODE_TQ_NO_COMMITTED_OFFSET;
        code = -1;
        goto OVER;
      }
    }
  }

  // 3.query
  if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
    /*if (fetchOffsetNew.type == TMQ_OFFSET__LOG) {*/
    /*fetchOffsetNew.version++;*/
    /*}*/
    if (tqScan(pTq, pHandle, &dataRsp, &fetchOffsetNew) < 0) {
      ASSERT(0);
      code = -1;
      goto OVER;
    }
    if (dataRsp.blockNum == 0) {
      // TODO add to async task pool
      /*dataRsp.rspOffset.version--;*/
    }
    if (tqSendDataRsp(pTq, pMsg, pReq, &dataRsp) < 0) {
      code = -1;
    }
    goto OVER;
  }

  if (pHandle->execHandle.subType != TOPIC_SUB_TYPE__COLUMN) {
    ASSERT(fetchOffsetNew.type == TMQ_OFFSET__LOG);
    int64_t fetchVer = fetchOffsetNew.version + 1;
    pCkHead = taosMemoryMalloc(sizeof(SWalCkHead) + 2048);
    if (pCkHead == NULL) {
      code = -1;
      goto OVER;
    }

    walSetReaderCapacity(pHandle->pWalReader, 2048);

    while (1) {
      consumerEpoch = atomic_load_32(&pHandle->epoch);
      if (consumerEpoch > reqEpoch) {
        tqWarn("tmq poll: consumer %" PRId64 " (epoch %d), subkey %s, vg %d offset %" PRId64
               ", found new consumer epoch %d, discard req epoch %d",
               consumerId, pReq->epoch, pHandle->subKey, TD_VID(pTq->pVnode), fetchVer, consumerEpoch, reqEpoch);
        break;
      }

      if (tqFetchLog(pTq, pHandle, &fetchVer, &pCkHead) < 0) {
        // TODO add push mgr

        tqOffsetResetToLog(&dataRsp.rspOffset, fetchVer);
        ASSERT(dataRsp.rspOffset.version >= dataRsp.reqOffset.version);
        if (tqSendDataRsp(pTq, pMsg, pReq, &dataRsp) < 0) {
          code = -1;
        }
        goto OVER;
      }

      SWalCont* pHead = &pCkHead->head;

      tqDebug("tmq poll: consumer:%" PRId64 ", (epoch %d) iter log, vgId:%d offset %" PRId64 " msgType %d", consumerId,
              pReq->epoch, TD_VID(pTq->pVnode), fetchVer, pHead->msgType);

      if (pHead->msgType == TDMT_VND_SUBMIT) {
        SSubmitReq* pCont = (SSubmitReq*)&pHead->body;

        if (tqLogScanExec(pTq, &pHandle->execHandle, pCont, &dataRsp) < 0) {
          /*ASSERT(0);*/
        }
        // TODO batch optimization:
        // TODO continue scan until meeting batch requirement
        if (dataRsp.blockNum > 0 /* threshold */) {
          tqOffsetResetToLog(&dataRsp.rspOffset, fetchVer);
          ASSERT(dataRsp.rspOffset.version >= dataRsp.reqOffset.version);

          if (tqSendDataRsp(pTq, pMsg, pReq, &dataRsp) < 0) {
            code = -1;
          }
          goto OVER;
        } else {
          fetchVer++;
        }

      } else {
        ASSERT(pHandle->fetchMeta);
        ASSERT(IS_META_MSG(pHead->msgType));
        tqDebug("fetch meta msg, ver:%" PRId64 ", type:%d", pHead->version, pHead->msgType);
        SMqMetaRsp metaRsp = {0};
        /*metaRsp.reqOffset = pReq->reqOffset.version;*/
        metaRsp.rspOffset = fetchVer;
        /*metaRsp.rspOffsetNew.version = fetchVer;*/
        tqOffsetResetToLog(&metaRsp.reqOffsetNew, pReq->reqOffset.version);
        tqOffsetResetToLog(&metaRsp.rspOffsetNew, fetchVer);
        metaRsp.resMsgType = pHead->msgType;
        metaRsp.metaRspLen = pHead->bodyLen;
        metaRsp.metaRsp = pHead->body;
        if (tqSendMetaPollRsp(pTq, pMsg, pReq, &metaRsp) < 0) {
          code = -1;
          goto OVER;
        }
        code = 0;
        goto OVER;
      }
    }
  }

OVER:
  if (pCkHead) taosMemoryFree(pCkHead);
  // TODO wrap in destroy func
  taosArrayDestroy(dataRsp.blockDataLen);
  taosArrayDestroyP(dataRsp.blockData, (FDelete)taosMemoryFree);

  if (dataRsp.withSchema) {
    taosArrayDestroyP(dataRsp.blockSchema, (FDelete)tDeleteSSchemaWrapper);
  }

  if (dataRsp.withTbName) {
    taosArrayDestroyP(dataRsp.blockTbName, (FDelete)taosMemoryFree);
  }

  return code;
}

int32_t tqProcessVgDeleteReq(STQ* pTq, char* msg, int32_t msgLen) {
  SMqVDeleteReq* pReq = (SMqVDeleteReq*)msg;

  int32_t code = taosHashRemove(pTq->handles, pReq->subKey, strlen(pReq->subKey));
  ASSERT(code == 0);

  tqOffsetDelete(pTq->pOffsetStore, pReq->subKey);

  if (tqMetaDeleteHandle(pTq, pReq->subKey) < 0) {
    ASSERT(0);
  }
  return 0;
}

int32_t tqProcessCheckAlterInfoReq(STQ* pTq, char* msg, int32_t msgLen) {
  SCheckAlterInfo info = {0};
  SDecoder        decoder;
  tDecoderInit(&decoder, msg, msgLen);
  if (tDecodeSCheckAlterInfo(&decoder, &info) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  tDecoderClear(&decoder);
  if (taosHashPut(pTq->pAlterInfo, info.topic, strlen(info.topic), &info, sizeof(SCheckAlterInfo)) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  return 0;
}

int32_t tqProcessVgChangeReq(STQ* pTq, char* msg, int32_t msgLen) {
  SMqRebVgReq req = {0};
  tDecodeSMqRebVgReq(msg, &req);
  // todo lock
  STqHandle* pHandle = taosHashGet(pTq->handles, req.subKey, strlen(req.subKey));
  if (pHandle == NULL) {
    if (req.oldConsumerId != -1) {
      tqError("vgId:%d, build new consumer handle %s for consumer %d, but old consumerId is %ld", req.vgId, req.subKey,
              req.newConsumerId, req.oldConsumerId);
    }
    ASSERT(req.newConsumerId != -1);
    STqHandle tqHandle = {0};
    pHandle = &tqHandle;
    /*taosInitRWLatch(&pExec->lock);*/

    memcpy(pHandle->subKey, req.subKey, TSDB_SUBSCRIBE_KEY_LEN);
    pHandle->consumerId = req.newConsumerId;
    pHandle->epoch = -1;

    pHandle->execHandle.subType = req.subType;
    pHandle->fetchMeta = req.withMeta;
    // TODO version should be assigned and refed during preprocess
    SWalRef* pRef = walRefCommittedVer(pTq->pVnode->pWal);
    if (pRef == NULL) {
      ASSERT(0);
      return -1;
    }
    int64_t ver = pRef->refVer;
    pHandle->pRef = pRef;

    if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
      pHandle->execHandle.execCol.qmsg = req.qmsg;
      pHandle->snapshotVer = ver;
      req.qmsg = NULL;
      SReadHandle handle = {
          .meta = pTq->pVnode->pMeta,
          .vnode = pTq->pVnode,
          .initTableReader = true,
          .initTqReader = true,
          .version = ver,
      };
      pHandle->execHandle.execCol.task =
          qCreateQueueExecTaskInfo(pHandle->execHandle.execCol.qmsg, &handle, &pHandle->execHandle.numOfCols,
                                   &pHandle->execHandle.pSchemaWrapper);
      ASSERT(pHandle->execHandle.execCol.task);
      void* scanner = NULL;
      qExtractStreamScanner(pHandle->execHandle.execCol.task, &scanner);
      ASSERT(scanner);
      pHandle->execHandle.pExecReader = qExtractReaderFromStreamScanner(scanner);
      ASSERT(pHandle->execHandle.pExecReader);
    } else if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__DB) {
      pHandle->pWalReader = walOpenReader(pTq->pVnode->pWal, NULL);

      pHandle->execHandle.pExecReader = tqOpenReader(pTq->pVnode);
      pHandle->execHandle.execDb.pFilterOutTbUid =
          taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    } else if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__TABLE) {
      pHandle->pWalReader = walOpenReader(pTq->pVnode->pWal, NULL);

      pHandle->execHandle.execTb.suid = req.suid;
      SArray* tbUidList = taosArrayInit(0, sizeof(int64_t));
      vnodeGetCtbIdList(pTq->pVnode, req.suid, tbUidList);
      tqDebug("vgId:%d, tq try to get all ctb, suid:%" PRId64, pTq->pVnode->config.vgId, req.suid);
      for (int32_t i = 0; i < taosArrayGetSize(tbUidList); i++) {
        int64_t tbUid = *(int64_t*)taosArrayGet(tbUidList, i);
        tqDebug("vgId:%d, idx %d, uid:%" PRId64, TD_VID(pTq->pVnode), i, tbUid);
      }
      pHandle->execHandle.pExecReader = tqOpenReader(pTq->pVnode);
      tqReaderSetTbUidList(pHandle->execHandle.pExecReader, tbUidList);
      taosArrayDestroy(tbUidList);
    }
    taosHashPut(pTq->handles, req.subKey, strlen(req.subKey), pHandle, sizeof(STqHandle));
    tqDebug("try to persist handle %s consumer %" PRId64, req.subKey, pHandle->consumerId);
    if (tqMetaSaveHandle(pTq, req.subKey, pHandle) < 0) {
      // TODO
      ASSERT(0);
    }
  } else {
    /*ASSERT(pExec->consumerId == req.oldConsumerId);*/
    // TODO handle qmsg and exec modification
    atomic_store_32(&pHandle->epoch, -1);
    atomic_store_64(&pHandle->consumerId, req.newConsumerId);
    atomic_add_fetch_32(&pHandle->epoch, 1);
    if (tqMetaSaveHandle(pTq, req.subKey, pHandle) < 0) {
      // TODO
    }
  }

  return 0;
}

int32_t tqExpandTask(STQ* pTq, SStreamTask* pTask) {
  int32_t code = 0;

  if (pTask->taskLevel == TASK_LEVEL__AGG) {
    ASSERT(taosArrayGetSize(pTask->childEpInfo) != 0);
  }

  pTask->schedStatus = TASK_SCHED_STATUS__INACTIVE;

  pTask->inputQueue = streamQueueOpen();
  pTask->outputQueue = streamQueueOpen();

  if (pTask->inputQueue == NULL || pTask->outputQueue == NULL) {
    code = -1;
    goto FAIL;
  }

  pTask->inputStatus = TASK_INPUT_STATUS__NORMAL;
  pTask->outputStatus = TASK_OUTPUT_STATUS__NORMAL;

  pTask->pMsgCb = &pTq->pVnode->msgCb;

  // expand executor
  if (pTask->taskLevel == TASK_LEVEL__SOURCE) {
    SReadHandle handle = {
        .meta = pTq->pVnode->pMeta,
        .vnode = pTq->pVnode,
        .initTqReader = 1,
    };
    pTask->exec.executor = qCreateStreamExecTaskInfo(pTask->exec.qmsg, &handle);
    ASSERT(pTask->exec.executor);
  } else if (pTask->taskLevel == TASK_LEVEL__AGG) {
    SReadHandle mgHandle = {
        .vnode = NULL,
        .numOfVgroups = (int32_t)taosArrayGetSize(pTask->childEpInfo),
    };
    pTask->exec.executor = qCreateStreamExecTaskInfo(pTask->exec.qmsg, &mgHandle);
    ASSERT(pTask->exec.executor);
  }

  // sink
  /*pTask->ahandle = pTq->pVnode;*/
  if (pTask->outputType == TASK_OUTPUT__SMA) {
    pTask->smaSink.vnode = pTq->pVnode;
    pTask->smaSink.smaSink = smaHandleRes;
  } else if (pTask->outputType == TASK_OUTPUT__TABLE) {
    pTask->tbSink.vnode = pTq->pVnode;
    pTask->tbSink.tbSinkFunc = tqTableSink;

    ASSERT(pTask->tbSink.pSchemaWrapper);
    ASSERT(pTask->tbSink.pSchemaWrapper->pSchema);

    pTask->tbSink.pTSchema =
        tdGetSTSChemaFromSSChema(pTask->tbSink.pSchemaWrapper->pSchema, pTask->tbSink.pSchemaWrapper->nCols, 1);
    ASSERT(pTask->tbSink.pTSchema);
  }

  streamSetupTrigger(pTask);

  tqInfo("deploy stream task on vg %d, task id %d, child id %d", TD_VID(pTq->pVnode), pTask->taskId,
         pTask->selfChildId);

FAIL:
  if (pTask->inputQueue) streamQueueClose(pTask->inputQueue);
  if (pTask->outputQueue) streamQueueClose(pTask->outputQueue);
  // TODO free executor
  return code;
}

int32_t tqProcessTaskDeployReq(STQ* pTq, char* msg, int32_t msgLen) {
  //
  return streamMetaAddSerializedTask(pTq->pStreamMeta, msg, msgLen);
#if 0
  SStreamTask* pTask = taosMemoryCalloc(1, sizeof(SStreamTask));
  if (pTask == NULL) {
    return -1;
  }
  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, msgLen);
  if (tDecodeSStreamTask(&decoder, pTask) < 0) {
    ASSERT(0);
    goto FAIL;
  }
  tDecoderClear(&decoder);

  if (tqExpandTask(pTq, pTask) < 0) {
    goto FAIL;
  }

  taosHashPut(pTq->pStreamTasks, &pTask->taskId, sizeof(int32_t), &pTask, sizeof(void*));

  return 0;

FAIL:
  if (pTask) taosMemoryFree(pTask);
  return -1;
#endif
}

int32_t tqProcessStreamTrigger(STQ* pTq, SSubmitReq* pReq, int64_t ver) {
  void*              pIter = NULL;
  bool               failed = false;
  SStreamDataSubmit* pSubmit = NULL;

  pSubmit = streamDataSubmitNew(pReq);
  if (pSubmit == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    qError("failed to create data submit for stream since out of memory");
    failed = true;
  }

  while (1) {
    pIter = taosHashIterate(pTq->pStreamMeta->pTasks, pIter);
    if (pIter == NULL) break;
    SStreamTask* pTask = *(SStreamTask**)pIter;
    if (pTask->taskLevel != TASK_LEVEL__SOURCE) continue;

    qDebug("data submit enqueue stream task: %d, ver: %" PRId64, pTask->taskId, ver);

    if (!failed) {
      if (streamTaskInput(pTask, (SStreamQueueItem*)pSubmit) < 0) {
        qError("stream task input failed, task id %d", pTask->taskId);
        continue;
      }

      if (streamSchedExec(pTask) < 0) {
        qError("stream task launch failed, task id %d", pTask->taskId);
        continue;
      }
    } else {
      streamTaskInputFail(pTask);
    }
  }

  if (pSubmit) {
    streamDataSubmitRefDec(pSubmit);
    taosFreeQitem(pSubmit);
  }

  return failed ? -1 : 0;
}

int32_t tqProcessTaskRunReq(STQ* pTq, SRpcMsg* pMsg) {
  //
  SStreamTaskRunReq* pReq = pMsg->pCont;
  int32_t            taskId = pReq->taskId;
  SStreamTask*       pTask = streamMetaGetTask(pTq->pStreamMeta, taskId);
  if (pTask) {
    streamProcessRunReq(pTask);
    return 0;
  } else {
    return -1;
  }
}

int32_t tqProcessTaskDispatchReq(STQ* pTq, SRpcMsg* pMsg, bool exec) {
  ASSERT(0);
  char*              msgStr = pMsg->pCont;
  char*              msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t            msgLen = pMsg->contLen - sizeof(SMsgHead);
  SStreamDispatchReq req;
  SDecoder           decoder;
  tDecoderInit(&decoder, (uint8_t*)msgBody, msgLen);
  tDecodeStreamDispatchReq(&decoder, &req);
  int32_t taskId = req.taskId;

  SStreamTask* pTask = streamMetaGetTask(pTq->pStreamMeta, taskId);
  if (pTask) {
    SRpcMsg rsp = {
        .info = pMsg->info,
        .code = 0,
    };
    streamProcessDispatchReq(pTask, &req, &rsp, exec);
    return 0;
  } else {
    return -1;
  }
}

int32_t tqProcessTaskRecoverReq(STQ* pTq, SRpcMsg* pMsg) {
  SStreamTaskRecoverReq* pReq = pMsg->pCont;
  int32_t                taskId = pReq->taskId;
  SStreamTask*           pTask = streamMetaGetTask(pTq->pStreamMeta, taskId);
  if (pTask) {
    streamProcessRecoverReq(pTask, pReq, pMsg);
    return 0;
  } else {
    return -1;
  }
}

int32_t tqProcessTaskDispatchRsp(STQ* pTq, SRpcMsg* pMsg) {
  SStreamDispatchRsp* pRsp = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t             taskId = pRsp->taskId;
  SStreamTask*        pTask = streamMetaGetTask(pTq->pStreamMeta, taskId);
  if (pTask) {
    streamProcessDispatchRsp(pTask, pRsp);
    return 0;
  } else {
    return -1;
  }
}

int32_t tqProcessTaskRecoverRsp(STQ* pTq, SRpcMsg* pMsg) {
  SStreamTaskRecoverRsp* pRsp = pMsg->pCont;
  int32_t                taskId = pRsp->rspTaskId;

  SStreamTask* pTask = streamMetaGetTask(pTq->pStreamMeta, taskId);
  if (pTask) {
    streamProcessRecoverRsp(pTask, pRsp);
    return 0;
  } else {
    return -1;
  }
}

int32_t tqProcessTaskDropReq(STQ* pTq, char* msg, int32_t msgLen) {
  SVDropStreamTaskReq* pReq = (SVDropStreamTaskReq*)msg;

  return streamMetaRemoveTask(pTq->pStreamMeta, pReq->taskId);
}

int32_t tqProcessTaskRetrieveReq(STQ* pTq, SRpcMsg* pMsg) {
  char*              msgStr = pMsg->pCont;
  char*              msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t            msgLen = pMsg->contLen - sizeof(SMsgHead);
  SStreamRetrieveReq req;
  SDecoder           decoder;
  tDecoderInit(&decoder, msgBody, msgLen);
  tDecodeStreamRetrieveReq(&decoder, &req);
  int32_t      taskId = req.dstTaskId;
  SStreamTask* pTask = streamMetaGetTask(pTq->pStreamMeta, taskId);
  if (pTask) {
    SRpcMsg rsp = {
        .info = pMsg->info,
        .code = 0,
    };
    streamProcessRetrieveReq(pTask, &req, &rsp);
    return 0;
  } else {
    return -1;
  }
}

int32_t tqProcessTaskRetrieveRsp(STQ* pTq, SRpcMsg* pMsg) {
  //
  return 0;
}

void vnodeEnqueueStreamMsg(SVnode* pVnode, SRpcMsg* pMsg) {
  STQ*    pTq = pVnode->pTq;
  char*   msgStr = pMsg->pCont;
  char*   msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);
  int32_t code = 0;

  SStreamDispatchReq req;
  SDecoder           decoder;
  tDecoderInit(&decoder, msgBody, msgLen);
  if (tDecodeStreamDispatchReq(&decoder, &req) < 0) {
    code = TSDB_CODE_MSG_DECODE_ERROR;
    tDecoderClear(&decoder);
    goto FAIL;
  }
  tDecoderClear(&decoder);

  int32_t taskId = req.taskId;

  SStreamTask* pTask = streamMetaGetTask(pTq->pStreamMeta, taskId);
  if (pTask) {
    SRpcMsg rsp = {
        .info = pMsg->info,
        .code = 0,
    };
    streamProcessDispatchReq(pTask, &req, &rsp, false);
    return;
  }

FAIL:
  if (pMsg->info.handle == NULL) return;
  SRpcMsg rsp = {
      .code = code,
      .info = pMsg->info,
  };
  tmsgSendRsp(&rsp);
}
