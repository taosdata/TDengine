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

#include "taoserror.h"
#include "tarray.h"
#include "tdef.h"
#include "thash.h"
#include "tmsg.h"
#include "tpriv.h"
#include "tq.h"

static int32_t tqRetrieveCols(STqReader *pReader, SSDataBlock *pRes, SHashObj* pCol2SlotId);
static int32_t tqAddTableListForStbSub(STqHandle* pTqHandle, STQ* pTq, const SArray* tbUidList, int64_t version);
static void    tqAlterTagForStbSub(SVnode *pVnode, const SArray* tbUidList, const SArray* tags, const SArray* tagsArray, STqHandle* pTqHandle, int64_t version);
static int32_t tqSendMetaPollRsp(STqHandle* pHandle, const SRpcMsg* pMsg, const SMqPollReq* pReq,
                                 const SMqMetaRsp* pRsp, int32_t vgId);
static int32_t tqSendBatchMetaPollRsp(STqHandle* pHandle, const SRpcMsg* pMsg, const SMqPollReq* pReq,
                                      const SMqBatchMetaRsp* pRsp, int32_t vgId);
                                      
static void tqProcessCreateTbMsg(SDecoder* dcoder, SWalCont* pHead, STQ* pTq, STqHandle* pHandle, int64_t* realTbSuid, int64_t tbSuid) {
  int32_t code = 0;
  int32_t lino = 0;
  SVCreateTbReq* pCreateReq = NULL;
  SVCreateTbBatchReq reqNew = {0};
  void* buf = NULL;
  SArray *tbUids = NULL;
  SVCreateTbBatchReq req = {0};
  code = tDecodeSVCreateTbBatchReq(dcoder, &req);
  TSDB_CHECK_CODE(code, lino, end);

  STqExecHandle* pExec = &pHandle->execHandle;
  STqReader* pReader = pExec->pTqReader;

  tbUids = taosArrayInit(req.nReqs, sizeof(int64_t));
  TSDB_CHECK_NULL(tbUids, code, lino, end, terrno);
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    pCreateReq = req.pReqs + iReq;
    if ((pCreateReq->type == TSDB_CHILD_TABLE || pCreateReq->type == TSDB_VIRTUAL_CHILD_TABLE) && pCreateReq->ctb.suid == tbSuid) {
      TSDB_CHECK_NULL(taosArrayPush(tbUids, &pCreateReq->uid), code, lino, end, terrno);
    }
  }
  TSDB_CHECK_CONDITION(taosArrayGetSize(tbUids) != 0, code, lino, end, TSDB_CODE_SUCCESS);

  taosWLockLatch(&pTq->lock);
  tqReaderRemoveTbUidList(pHandle->execHandle.pTqReader, tbUids);
  code = tqAddTableListForStbSub(pHandle, pTq, tbUids, pHead->version);
  taosWUnLockLatch(&pTq->lock);
  TSDB_CHECK_CODE(code, lino, end);

  reqNew.pArray = taosArrayInit(req.nReqs, sizeof(struct SVCreateTbReq));
  TSDB_CHECK_NULL(reqNew.pArray, code, lino, end, terrno);
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    pCreateReq = req.pReqs + iReq;
    if ((pCreateReq->type == TSDB_CHILD_TABLE || pCreateReq->type == TSDB_VIRTUAL_CHILD_TABLE) &&
        pCreateReq->ctb.suid == tbSuid &&
        taosHashGet(pReader->tbIdHash, &pCreateReq->uid, sizeof(int64_t)) != NULL) {
      TSDB_CHECK_NULL(taosArrayPush(reqNew.pArray, pCreateReq), code, lino, end, terrno);
      reqNew.nReqs++;
    }
  }

  TSDB_CHECK_CONDITION(reqNew.nReqs != 0, code, lino, end, TSDB_CODE_SUCCESS);
  *realTbSuid = tbSuid;

  int     tlen = 0;
  tEncodeSize(tEncodeSVCreateTbBatchReq, &reqNew, tlen, code);
  TSDB_CHECK_CODE(code, lino, end);

  buf = taosMemoryMalloc(tlen);
  TSDB_CHECK_NULL(buf, code, lino, end, terrno);

  SEncoder coderNew = {0};
  tEncoderInit(&coderNew, buf, tlen);
  code = tEncodeSVCreateTbBatchReq(&coderNew, &reqNew);
  tEncoderClear(&coderNew);
  TSDB_CHECK_CODE(code, lino, end);

  (void)memcpy(pHead->body + sizeof(SMsgHead), buf, tlen);
  pHead->bodyLen = tlen + sizeof(SMsgHead);

end:
  taosMemoryFree(buf);
  taosArrayDestroy(reqNew.pArray);
  tDeleteSVCreateTbBatchReq(&req);
  taosArrayDestroy(tbUids);
  if (code < 0) {
    tqError("tqProcessCreateTbMsg failed, code:%d, line:%d", code, lino);
  }
}

static int32_t tqGetUidSuid(SMeta* pMeta, const char* tbName, int64_t* uid, int64_t* suid){
  SMetaReader mr = {0};

  metaReaderDoInit(&mr, pMeta, META_READER_LOCK);
  int32_t code = metaGetTableEntryByName(&mr, tbName);
  if (code == 0) {
    *uid = mr.me.uid;
    *suid = mr.me.ctbEntry.suid;
  } else {
    tqError("tqGetUidSuid failed at %s:%d since table %s not found, code:%d", __FILE__, __LINE__, tbName, code);
  }

  metaReaderClear(&mr);
  return code;
}

static void tqAlterMultiTag(SVAlterTbReq* req, SWalCont* pHead, STQ* pTq, STqHandle* pHandle, int64_t* realTbSuid, int64_t tbSuid) {
  int32_t lino = 0;
  int32_t code = 0;
  SVAlterTbReq reqNew = {0};
  SArray* uidList = NULL;
  SArray* tagListArray = NULL;
  void* buf = NULL;

  STqExecHandle* pExec = &pHandle->execHandle;
  STqReader* pReader = pExec->pTqReader;
  
  int32_t nTables = taosArrayGetSize(req->tables);
  uidList = taosArrayInit(nTables, sizeof(tb_uid_t));
  TSDB_CHECK_NULL(uidList, code, lino, end, terrno);

  tagListArray = taosArrayInit(nTables, sizeof(void*));
  TSDB_CHECK_NULL(tagListArray, code, lino, end, terrno);

  for (int32_t i = 0; i < nTables; i++) {
    SUpdateTableTagVal *pTable = taosArrayGet(req->tables, i);
    // Collect UID for batch notification
    int64_t uid = 0;
    int64_t suid = 0;
    int32_t ret = tqGetUidSuid(pTq->pVnode->pMeta, pTable->tbName, &uid, &suid);
    if (ret != 0) {
      tqError("vgId:%d, %s failed at %s:%d since table %s not found", TD_VID(pTq->pVnode), __func__, __FILE__, __LINE__, pTable->tbName);
      continue;
    }
    if (suid != tbSuid) continue;
    if (taosArrayPush(uidList, &uid) == NULL) {
      tqError("vgId:%d, %s failed at %s:%d since %s", TD_VID(pTq->pVnode), __func__, __FILE__, __LINE__, tstrerror(terrno));
      continue;
    }
    if (taosArrayPush(tagListArray, &pTable->tags) == NULL){
      void* ret = taosArrayPop(uidList);  // make sure the size of uidList and tagListArray are same
      tqError("vgId:%d, %s failed at %s:%d since %s, ret:%p", TD_VID(pTq->pVnode), __func__, __FILE__, __LINE__, tstrerror(terrno), ret);
      continue;
    }
  }

  if (taosArrayGetSize(uidList) > 0) {
    tqAlterTagForStbSub(pTq->pVnode, uidList, NULL, tagListArray, pHandle, pHead->version);
  }

  // Build filtered message
  reqNew.action = req->action;
  reqNew.tbName = req->tbName;
  reqNew.tables = taosArrayInit(nTables, sizeof(SUpdateTableTagVal));
  TSDB_CHECK_NULL(reqNew.tables, code, lino, end, terrno);

  // Collect only subscribed tables
  for (int32_t i = 0; i < taosArrayGetSize(req->tables); i++) {
    SUpdateTableTagVal* pTable = taosArrayGet(req->tables, i);
    if (pTable == NULL || pTable->tbName == NULL) {
      continue;
    }
    int64_t uid = 0;
    int64_t suid = 0;
    int32_t ret = tqGetUidSuid(pTq->pVnode->pMeta, pTable->tbName, &uid, &suid);
    if (ret != 0) {
      tqError("vgId:%d, %s failed at %s:%d since table %s not found", TD_VID(pTq->pVnode), __func__, __FILE__, __LINE__, pTable->tbName);
      continue;
    }

    if (suid == tbSuid && taosHashGet(pReader->tbIdHash, &uid, sizeof(int64_t)) != NULL) {
      TSDB_CHECK_NULL(taosArrayPush(reqNew.tables, pTable), code, lino, end, terrno);
    }
  }

  TSDB_CHECK_CONDITION(taosArrayGetSize(reqNew.tables) != 0, code, lino, end, TSDB_CODE_SUCCESS);

  *realTbSuid = tbSuid;

  // Encode filtered message
  int tlen = 0;
  tEncodeSize(tEncodeSVAlterTbReq, &reqNew, tlen, code);
  TSDB_CHECK_CODE(code, lino, end);

  buf = taosMemoryMalloc(tlen);
  TSDB_CHECK_NULL(buf, code, lino, end, terrno);

  SEncoder coderNew = {0};
  tEncoderInit(&coderNew, buf, tlen);
  code = tEncodeSVAlterTbReq(&coderNew, &reqNew);
  tEncoderClear(&coderNew);
  TSDB_CHECK_CODE(code, lino, end);

  (void)memcpy(pHead->body + sizeof(SMsgHead), buf, tlen);
  pHead->bodyLen = tlen + sizeof(SMsgHead);

end:
  taosArrayDestroy(uidList);
  taosArrayDestroy(tagListArray);
  taosMemoryFree(buf);
  taosArrayDestroy(reqNew.tables);
  if (code != 0) {
    tqError("%s failed, code:%d, line:%d", __func__, code, lino);
  }
}

static void tqProcessAlterTbMsg(SDecoder* dcoder, SWalCont* pHead, STQ* pTq, STqHandle* pHandle, int64_t* realTbSuid, int64_t tbSuid) {
  SVAlterTbReq req = {0};
  SArray* uidList = NULL;
  int32_t lino = 0;
  int32_t code = tDecodeSVAlterTbReq(dcoder, &req);
  TSDB_CHECK_CODE(code, lino, end);

  if (req.action == TSDB_ALTER_TABLE_UPDATE_MULTI_TABLE_TAG_VAL) {
    tqAlterMultiTag(&req, pHead, pTq, pHandle, realTbSuid, tbSuid);
  } else if (req.action == TSDB_ALTER_TABLE_UPDATE_CHILD_TABLE_TAG_VAL) {
    ETableType tbType = 0;
    uint64_t suid = 0;
    STREAM_CHECK_RET_GOTO(metaGetTableTypeSuidByName(pTq->pVnode, req.tbName, &tbType, &suid));
    if (tbType != TSDB_SUPER_TABLE) {
      tqError("%s failed at line:%d since table %s is not super table, code:%d", __func__, lino, req.tbName, code);
      goto end;
    }
    SNode* pTagCond = getTagCondNodeForStableTmq(pHandle->execHandle.execTb.node);
    if (pTagCond != NULL) {
      uidList = taosArrayInit(8, sizeof(uint64_t));
      STREAM_CHECK_NULL_GOTO(uidList, terrno);
      STREAM_CHECK_RET_GOTO(vnodeGetCtbIdList(pTq->pVnode, suid, uidList));
      tqAlterTagForStbSub(pTq->pVnode, uidList, req.pMultiTag, NULL, pHandle, pHead->version);
    }
    *realTbSuid = suid;
  } else {
    int64_t uid = 0;
    int64_t suid = 0;
    code = tqGetUidSuid(pTq->pVnode->pMeta, req.tbName, &uid, &suid);
    TSDB_CHECK_CODE(code, lino, end);

    STqExecHandle* pExec = &pHandle->execHandle;
    STqReader* pReader = pExec->pTqReader;
    if (taosHashGet(pReader->tbIdHash, &uid, sizeof(int64_t)) != NULL) {
      *realTbSuid = suid;
    }
  }

end:
  destroyAlterTbReq(&req);
  taosArrayDestroy(uidList);
  if (code != 0) {
    tqError("%s failed at line:%d, code:%s, table:%s", __func__, lino, tstrerror(code), req.tbName);
  }
} 

static void tqProcessDropTbMsg(SDecoder* dcoder, SWalCont* pHead, STqHandle* pHandle, int64_t* realTbSuid, int64_t tbSuid) {
  SVDropTbBatchReq req = {0};
  SVDropTbBatchReq reqNew = {0};
  void* buf = NULL;
  int32_t lino = 0;
  int32_t code = tDecodeSVDropTbBatchReq(dcoder, &req);
  TSDB_CHECK_CODE(code, lino, end);

  STqExecHandle* pExec = &pHandle->execHandle;
  STqReader* pReader = pExec->pTqReader;

  reqNew.pArray = taosArrayInit(req.nReqs, sizeof(SVDropTbReq));
  TSDB_CHECK_NULL(reqNew.pArray, code, lino, end, terrno);
  SVDropTbReq* pDropReq = NULL;
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    pDropReq = req.pReqs + iReq;
    if (pDropReq->suid == tbSuid && taosHashGet(pReader->tbIdHash, &pDropReq->uid, sizeof(int64_t)) != NULL) {
      reqNew.nReqs++;
      TSDB_CHECK_NULL(taosArrayPush(reqNew.pArray, pDropReq), code, lino, end, terrno);
    }
  }

  TSDB_CHECK_CONDITION(taosArrayGetSize(reqNew.pArray) != 0, code, lino, end, TSDB_CODE_SUCCESS);

  *realTbSuid = tbSuid;
  int     tlen = 0;
  tEncodeSize(tEncodeSVDropTbBatchReq, &reqNew, tlen, code);
  TSDB_CHECK_CODE(code, lino, end);

  buf = taosMemoryMalloc(tlen);
  TSDB_CHECK_NULL(buf, code, lino, end, terrno);

  SEncoder coderNew = {0};
  tEncoderInit(&coderNew, buf, tlen);
  code = tEncodeSVDropTbBatchReq(&coderNew, &reqNew);
  tEncoderClear(&coderNew);
  TSDB_CHECK_CODE(code, lino, end);

  (void)memcpy(pHead->body + sizeof(SMsgHead), buf, tlen);
  pHead->bodyLen = tlen + sizeof(SMsgHead);

end:
  taosMemoryFree(buf);
  taosArrayDestroy(reqNew.pArray);
  if (code != 0) {
    tqError("%s failed, code:%d, line:%d", __func__, code, lino);
  }
}

static bool tqProcessMetaForStbSub(STQ* pTq, STqHandle* pHandle, SWalCont* pHead) {
  int32_t code = 0;
  int32_t lino = 0;
  if (pHandle == NULL || pHead == NULL) {
    return false;
  }
  if (pHandle->execHandle.subType != TOPIC_SUB_TYPE__TABLE) {
    return true;
  }

  int16_t msgType = pHead->msgType;
  char*   body = pHead->body;
  int32_t bodyLen = pHead->bodyLen;

  int64_t  tbSuid = pHandle->execHandle.execTb.suid;
  int64_t  realTbSuid = 0;
  SDecoder dcoder = {0};
  void*    data = POINTER_SHIFT(body, sizeof(SMsgHead));
  int32_t  len = bodyLen - sizeof(SMsgHead);
  tDecoderInit(&dcoder, data, len);

  if (msgType == TDMT_VND_CREATE_STB || msgType == TDMT_VND_ALTER_STB) {
    SVCreateStbReq req = {0};
    if (tDecodeSVCreateStbReq(&dcoder, &req) < 0) {
      goto end;
    }
    realTbSuid = req.suid;
  } else if (msgType == TDMT_VND_DROP_STB) {
    SVDropStbReq req = {0};
    if (tDecodeSVDropStbReq(&dcoder, &req) < 0) {
      goto end;
    }
    realTbSuid = req.suid;
  } else if (msgType == TDMT_VND_CREATE_TABLE) {
    tqProcessCreateTbMsg(&dcoder, pHead, pTq, pHandle, &realTbSuid, tbSuid);
  } else if (msgType == TDMT_VND_ALTER_TABLE) {
    tqProcessAlterTbMsg(&dcoder, pHead, pTq, pHandle, &realTbSuid, tbSuid);
  } else if (msgType == TDMT_VND_DROP_TABLE) {
    tqProcessDropTbMsg(&dcoder, pHead, pHandle, &realTbSuid, tbSuid);
  } else if (msgType == TDMT_VND_DELETE) {
    SDeleteRes req = {0};
    if (tDecodeDeleteRes(&dcoder, &req) < 0) {
      goto end;
    }
    realTbSuid = req.suid;
  }

end:
  tDecoderClear(&dcoder);
  bool tmp = tbSuid == realTbSuid;
  if (pHandle->fetchMeta == ONLY_DATA){
    tmp = false;
  }
  tqDebug("%s suid:%" PRId64 " realSuid:%" PRId64 " return:%d", __FUNCTION__, tbSuid, realTbSuid, tmp);
  return tmp;
}

static int32_t tqFetchLog(STQ* pTq, STqHandle* pHandle, int64_t* fetchOffset, uint64_t reqId) {
  if (pTq == NULL || pHandle == NULL || fetchOffset == NULL) {
    return -1;
  }
  int32_t code = -1;
  int32_t vgId = TD_VID(pTq->pVnode);
  int64_t id = pHandle->pWalReader->readerId;

  int64_t offset = *fetchOffset;
  int64_t lastVer = walGetLastVer(pHandle->pWalReader->pWal);
  int64_t committedVer = walGetCommittedVer(pHandle->pWalReader->pWal);
  int64_t appliedVer = walGetAppliedVer(pHandle->pWalReader->pWal);

  tqDebug("vgId:%d, start to fetch wal, index:%" PRId64 ", last:%" PRId64 " commit:%" PRId64 ", applied:%" PRId64
          ", 0x%" PRIx64,
          vgId, offset, lastVer, committedVer, appliedVer, id);

  while (offset <= appliedVer) {
    if (walFetchHead(pHandle->pWalReader, offset) < 0) {
      tqDebug("tmq poll: consumer:0x%" PRIx64 ", (epoch %d) vgId:%d offset %" PRId64
              ", no more log to return, QID:0x%" PRIx64 " 0x%" PRIx64,
              pHandle->consumerId, pHandle->epoch, vgId, offset, reqId, id);
      goto END;
    }

    tqDebug("vgId:%d, consumer:0x%" PRIx64 " taosx get msg ver %" PRId64 ", type:%s, QID:0x%" PRIx64 " 0x%" PRIx64,
            vgId, pHandle->consumerId, offset, TMSG_INFO(pHandle->pWalReader->pHead->head.msgType), reqId, id);

    if (pHandle->pWalReader->pHead->head.msgType == TDMT_VND_SUBMIT) {
      code = walFetchBody(pHandle->pWalReader);
      goto END;
    } else {
      if (pHandle->fetchMeta != ONLY_DATA || pHandle->execHandle.subType == TOPIC_SUB_TYPE__TABLE) {
        SWalCont* pHead = &(pHandle->pWalReader->pHead->head);
        if (IS_META_MSG(pHead->msgType) && !(pHead->msgType == TDMT_VND_DELETE && pHandle->fetchMeta == ONLY_META)) {
          code = walFetchBody(pHandle->pWalReader);
          if (code < 0) {
            goto END;
          }

          pHead = &(pHandle->pWalReader->pHead->head);
          if (tqProcessMetaForStbSub(pTq, pHandle, pHead)) {
            code = 0;
            goto END;
          } else {
            offset++;
            code = -1;
            continue;
          }
        }
      }
      code = walSkipFetchBody(pHandle->pWalReader);
      if (code < 0) {
        goto END;
      }
      offset++;
    }
    code = -1;
  }

END:
  *fetchOffset = offset;
  tqDebug("vgId:%d, end to fetch wal, code:%d , index:%" PRId64 ", last:%" PRId64 " commit:%" PRId64
          ", applied:%" PRId64 ", 0x%" PRIx64,
          vgId, code, offset, lastVer, committedVer, appliedVer, id);
  return code;
}

bool tqGetTablePrimaryKey(STqReader* pReader) {
  if (pReader == NULL) {
    return false;
  }
  return pReader->hasPrimaryKey;
}

void tqSetTablePrimaryKey(STqReader* pReader, int64_t uid) {
  tqDebug("%s:%p uid:%" PRId64, __FUNCTION__, pReader, uid);

  if (pReader == NULL) {
    return;
  }
  bool            ret = false;
  SSchemaWrapper* schema = metaGetTableSchema(pReader->pVnode->pMeta, uid, -1, 1, NULL, 0, false);
  if (schema && schema->nCols >= 2 && schema->pSchema[1].flags & COL_IS_KEY) {
    ret = true;
  }
  tDeleteSchemaWrapper(schema);
  pReader->hasPrimaryKey = ret;
}

static void tqFreeTagCache(void* pData){
  if (pData == NULL) return;
  SArray* tagCache = *(SArray**)pData;
  taosArrayDestroyP(tagCache, taosMemFree);
}

STqReader* tqReaderOpen(SVnode* pVnode) {
  tqDebug("%s:%p", __FUNCTION__, pVnode);
  if (pVnode == NULL) {
    return NULL;
  }
  STqReader* pReader = taosMemoryCalloc(1, sizeof(STqReader));
  if (pReader == NULL) {
    return NULL;
  }

  pReader->pWalReader = walOpenReader(pVnode->pWal, 0);
  if (pReader->pWalReader == NULL) {
    taosMemoryFree(pReader);
    return NULL;
  }

  pReader->pVnode = pVnode;
  pReader->pSchemaWrapper = NULL;
  pReader->tbIdHash = NULL;
  pReader->pTableTagCacheForTmq = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
  if (pReader->pTableTagCacheForTmq == NULL) {
    walCloseReader(pReader->pWalReader);
    taosMemoryFree(pReader);
    return NULL;
  }
  taosHashSetFreeFp(pReader->pTableTagCacheForTmq, tqFreeTagCache);
  taosInitRWLatch(&pReader->tagCachelock);

  return pReader;
}

void tqReaderClose(STqReader* pReader) {
  tqDebug("%s:%p", __FUNCTION__, pReader);
  if (pReader == NULL) return;

  // close wal reader
  walCloseReader(pReader->pWalReader);
  taosHashCleanup(pReader->pTableTagCacheForTmq);
  tDeleteSchemaWrapper(pReader->pSchemaWrapper);
  taosMemoryFree(pReader->pTSchema);
  taosMemoryFree(pReader->extSchema);

  // free hash
  taosHashCleanup(pReader->tbIdHash);
  tDestroySubmitReq(&pReader->submit, TSDB_MSG_FLG_DECODE);

  taosMemoryFree(pReader);
}

int32_t tqReaderSeek(STqReader* pReader, int64_t ver, const char* id) {
  if (pReader == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (walReaderSeekVer(pReader->pWalReader, ver) < 0) {
    return terrno;
  }
  tqDebug("wal reader seek to ver:%" PRId64 " %s", ver, id);
  return 0;
}

static int32_t tqGetTableTagCache(STqReader* pReader, SExprInfo* pExprInfo, int32_t numOfExpr, int64_t uid) {
  int32_t code = 0;
  int32_t lino = 0;

  void* data = taosHashGet(pReader->pTableTagCacheForTmq, &uid, LONG_BYTES);
  if (data == NULL) {
    SStorageAPI api = {0}; 
    initStorageAPI(&api);
    code = cacheTag(pReader->pVnode, pReader->pTableTagCacheForTmq, pExprInfo, numOfExpr, &api, uid, 0, &pReader->tagCachelock);
    TSDB_CHECK_CODE(code, lino, END);
  }

  END:
  if (code != TSDB_CODE_SUCCESS) {
    tqError("%s failed at %d, failed to add tbName to response:%s, uid:%"PRId64, __FUNCTION__, lino, tstrerror(code), uid);
  }
  
  return code;
}

void tqUpdateTableTagCache(STqReader* pReader, SExprInfo* pExprInfo, int32_t numOfExpr, int64_t uid, col_id_t colId) {
  int32_t code = 0;
  int32_t lino = 0;

  void* data = taosHashGet(pReader->pTableTagCacheForTmq, &uid, LONG_BYTES);
  if (data == NULL) {
    return;
  }

  SStorageAPI api = {0}; 
  initStorageAPI(&api);
  code = cacheTag(pReader->pVnode, pReader->pTableTagCacheForTmq, pExprInfo, numOfExpr, &api, uid, colId, &pReader->tagCachelock);
  TSDB_CHECK_CODE(code, lino, END);

  END:
  if (code != TSDB_CODE_SUCCESS) {
    tqError("%s failed at %d, failed to update tag cache code:%s, uid:%"PRId64, __FUNCTION__, lino, tstrerror(code), uid);
  }
}

static int32_t tqRetrievePseudoCols(STqReader* pReader, SSDataBlock* pBlock, int32_t numOfRows, int64_t uid, SExprInfo* pPseudoExpr, int32_t numOfPseudoExpr) {
  if (pReader == NULL || pBlock == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  
  code = tqGetTableTagCache(pReader, pPseudoExpr, numOfPseudoExpr, uid);
  TSDB_CHECK_CODE(code, lino, END);

  code = fillTag(pReader->pTableTagCacheForTmq, pPseudoExpr, numOfPseudoExpr, uid, pBlock, numOfRows, pBlock->info.rows - numOfRows, 1, &pReader->tagCachelock);
  TSDB_CHECK_CODE(code, lino, END);

END:
  if (code != 0) {
    tqError("tqRetrievePseudoCols failed, line:%d, msg:%s", lino, tstrerror(code));
  }
  return code;
}

int32_t tqNextBlockInWal(STqReader* pReader, SSDataBlock* pRes, SHashObj* pCol2SlotId, SExprInfo* pPseudoExpr, int32_t numOfPseudoExpr,
                         int sourceExcluded, int32_t minPollRows, int64_t timeout, int8_t enableReplay) {
  int32_t code = 0;
  if (pReader == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SWalReader* pWalReader = pReader->pWalReader;

  int64_t st = taosGetTimestampMs();
  while (1) {
    code = walNextValidMsg(pWalReader, false);
    if (code != 0) {
      break;
    }

    void*   pBody = POINTER_SHIFT(pWalReader->pHead->head.body, sizeof(SSubmitReq2Msg));
    int32_t bodyLen = pWalReader->pHead->head.bodyLen - sizeof(SSubmitReq2Msg);
    int64_t ver = pWalReader->pHead->head.version;
    SDecoder decoder = {0};
    code = tqReaderSetSubmitMsg(pReader, pBody, bodyLen, ver, NULL, &decoder);
    tDecoderClear(&decoder);
    if (code != 0) {
      return code;
    }
    pReader->nextBlk = 0;

    int32_t numOfBlocks = taosArrayGetSize(pReader->submit.aSubmitTbData);
    while (pReader->nextBlk < numOfBlocks) {
      tqDebug("tq reader next data block %d/%d, len:%d %" PRId64, pReader->nextBlk, numOfBlocks, pReader->msg.msgLen,
              pReader->msg.ver);

      SSubmitTbData* pSubmitTbData = taosArrayGet(pReader->submit.aSubmitTbData, pReader->nextBlk);
      if (pSubmitTbData == NULL) {
        tqError("tq reader next data block %d/%d, len:%d %" PRId64, pReader->nextBlk, numOfBlocks, pReader->msg.msgLen,
                pReader->msg.ver);
        return terrno;
      }
      if ((pSubmitTbData->flags & sourceExcluded) != 0) {
        pReader->nextBlk += 1;
        continue;
      }
      if (pReader->tbIdHash == NULL || taosHashGet(pReader->tbIdHash, &pSubmitTbData->uid, sizeof(int64_t)) != NULL) {
        tqDebug("tq reader return submit block, uid:%" PRId64, pSubmitTbData->uid);
        int32_t numOfRows = pRes->info.rows;
        code = tqRetrieveCols(pReader, pRes, pCol2SlotId);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
        code = tqRetrievePseudoCols(pReader, pRes, numOfRows, pSubmitTbData->uid, pPseudoExpr, numOfPseudoExpr);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }

      }
      pReader->nextBlk += 1;
    }

    tDestroySubmitReq(&pReader->submit, TSDB_MSG_FLG_DECODE);
    pReader->msg.msgStr = NULL;

    if (pRes->info.rows >= minPollRows || (enableReplay && pRes->info.rows > 0)){
      break;
    }
    int64_t elapsed = taosGetTimestampMs() - st;
    if (elapsed > timeout || elapsed < 0) {
      code = TSDB_CODE_TMQ_FETCH_TIMEOUT;
      terrno = code;
      break;
    }
  }
  return code;
}

int32_t tqReaderSetSubmitMsg(STqReader* pReader, void* msgStr, int32_t msgLen, int64_t ver, SArray* rawList, SDecoder* decoder) {
  if (pReader == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  pReader->msg.msgStr = msgStr;
  pReader->msg.msgLen = msgLen;
  pReader->msg.ver = ver;

  tqTrace("tq reader set msg pointer:%p, msg len:%d", msgStr, msgLen);

  tDecoderInit(decoder, pReader->msg.msgStr, pReader->msg.msgLen);
  int32_t code = tDecodeSubmitReq(decoder, &pReader->submit, rawList);

  if (code != 0) {
    tqError("DecodeSSubmitReq2 error, msgLen:%d, ver:%" PRId64, msgLen, ver);
  }

  return code;
}

void tqReaderClearSubmitMsg(STqReader* pReader) {
  tDestroySubmitReq(&pReader->submit, TSDB_MSG_FLG_DECODE);
  pReader->nextBlk = 0;
  pReader->msg.msgStr = NULL;
}

SWalReader* tqGetWalReader(STqReader* pReader) {
  if (pReader == NULL) {
    return NULL;
  }
  return pReader->pWalReader;
}

int64_t tqGetResultBlockTime(STqReader* pReader) {
  if (pReader == NULL) {
    return 0;
  }
  return pReader->lastTs;
}

int32_t tqMaskBlock(SSchemaWrapper* pDst, SSDataBlock* pBlock, const SSchemaWrapper* pSrc, char* mask,
                    SExtSchema* extSrc) {
  if (pDst == NULL || pBlock == NULL || pSrc == NULL || mask == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t code = 0;

  int32_t cnt = 0;
  for (int32_t i = 0; i < pSrc->nCols; i++) {
    cnt += mask[i];
  }

  pDst->nCols = cnt;
  pDst->pSchema = taosMemoryCalloc(cnt, sizeof(SSchema));
  if (pDst->pSchema == NULL) {
    return TAOS_GET_TERRNO(terrno);
  }

  int32_t j = 0;
  for (int32_t i = 0; i < pSrc->nCols; i++) {
    if (mask[i]) {
      pDst->pSchema[j++] = pSrc->pSchema[i];
      SColumnInfoData colInfo =
          createColumnInfoData(pSrc->pSchema[i].type, pSrc->pSchema[i].bytes, pSrc->pSchema[i].colId);
      if (extSrc != NULL) {
        decimalFromTypeMod(extSrc[i].typeMod, &colInfo.info.precision, &colInfo.info.scale);
      }
      code = blockDataAppendColInfo(pBlock, &colInfo);
      if (code != 0) {
        return code;
      }
    }
  }
  return 0;
}

static int32_t tqDoSetBlobVal(SColumnInfoData* pColumnInfoData, int32_t idx, SColVal* pColVal, SBlobSet* pBlobRow2) {
  int32_t code = 0;
  if (pColumnInfoData == NULL || pColVal == NULL || pBlobRow2 == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  // TODO(yhDeng)
  if (COL_VAL_IS_VALUE(pColVal)) {
    char* val = taosMemCalloc(1, pColVal->value.nData + sizeof(BlobDataLenT));
    if (val == NULL) {
      return terrno;
    }

    uint64_t seq = 0;
    int32_t  len = 0;
    if (pColVal->value.pData != NULL) {
      if (tGetU64(pColVal->value.pData, &seq) < 0){
        TAOS_CHECK_RETURN(TSDB_CODE_INVALID_PARA);
      }
      SBlobItem item = {0};
      code = tBlobSetGet(pBlobRow2, seq, &item);
      if (code != 0) {
        taosMemoryFree(val);
        terrno = code;
        uError("tq set blob val, idx:%d, get blob item failed, seq:%" PRIu64 ", code:%d", idx, seq, code);
        return code;
      }

      val = taosMemRealloc(val, item.len + sizeof(BlobDataLenT));
      (void)memcpy(blobDataVal(val), item.data, item.len);
      len = item.len;
    }

    blobDataSetLen(val, len);
    code = colDataSetVal(pColumnInfoData, idx, val, false);

    taosMemoryFree(val);
  } else {
    colDataSetNULL(pColumnInfoData, idx);
  }
  return code;
}
static int32_t tqDoSetVal(SColumnInfoData* pColumnInfoData, int32_t rowIndex, SColVal* pColVal) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (IS_VAR_DATA_TYPE(pColVal->value.type)) {
    if (COL_VAL_IS_VALUE(pColVal)) {
      char val[65535 + 2] = {0};
      if (pColVal->value.pData != NULL) {
        (void)memcpy(varDataVal(val), pColVal->value.pData, pColVal->value.nData);
      }
      varDataSetLen(val, pColVal->value.nData);
      code = colDataSetVal(pColumnInfoData, rowIndex, val, false);
    } else {
      colDataSetNULL(pColumnInfoData, rowIndex);
    }
  } else {
    code = colDataSetVal(pColumnInfoData, rowIndex, VALUE_GET_DATUM(&pColVal->value, pColVal->value.type),
                         !COL_VAL_IS_VALUE(pColVal));
  }

  return code;
}

static int32_t tqSetBlockData(SSDataBlock* pBlock, int32_t slotId, int32_t rowIndex, SColVal* colVal, SBlobSet* pBlobSet) {
  int32_t        code = 0;
  SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, slotId);
  if (pColData == NULL) {
    return terrno;
  }

  uint8_t isBlob = IS_STR_DATA_BLOB(pColData->info.type) ? 1 : 0;
  if (isBlob == 0) {
    code = tqDoSetVal(pColData, rowIndex, colVal);
  } else {
    code = tqDoSetBlobVal(pColData, rowIndex, colVal, pBlobSet);
  }
  return code;
}

static int32_t tqProcessSubmitRow(SArray*         pRows,
                                SSDataBlock*    pBlock,
                                SHashObj*       pCol2SlotId,
                                STqReader*      pReader,
                                SBlobSet*       pBlobSet) {
  int32_t        code = 0;
  int32_t        line = 0;

  SArray* pColArray = taosArrayInit(4, INT_BYTES * 2);
  TSDB_CHECK_NULL(pColArray, code, line, END, terrno);

  int32_t sourceIdx = -1;
  int32_t rowIndex = 0;
  SRow* pRow = taosArrayGetP(pRows, rowIndex);
  TSDB_CHECK_NULL(pRow, code, line, END, terrno);
  while (++sourceIdx < pReader->pTSchema->numOfCols) {
    SColVal colVal = {0};
    code = tRowGet(pRow, pReader->pTSchema, sourceIdx, &colVal);
    TSDB_CHECK_CODE(code, line, END);
    void* pSlotId = taosHashGet(pCol2SlotId, &colVal.cid, sizeof(colVal.cid));
    if (pSlotId == NULL) {
      continue;
    }
    int32_t pData[2] = {sourceIdx, *(int16_t*)pSlotId};
    TSDB_CHECK_NULL(taosArrayPush(pColArray, pData), code, line, END, terrno);
    code = tqSetBlockData(pBlock, pData[1], pBlock->info.rows + rowIndex, &colVal, pBlobSet);
    TSDB_CHECK_CODE(code, line, END);
  }
  
  for (rowIndex = 1; rowIndex < taosArrayGetSize(pRows); rowIndex++) {
    SRow* pRow = taosArrayGetP(pRows, rowIndex);
    TSDB_CHECK_NULL(pRow, code, line, END, terrno);
    for (int32_t j = 0; j < taosArrayGetSize(pColArray); j++) {
      int32_t* pData = taosArrayGet(pColArray, j);
      TSDB_CHECK_NULL(pData, code, line, END, terrno);

      SColVal colVal = {0};
      code = tRowGet(pRow, pReader->pTSchema, pData[0], &colVal);
      TSDB_CHECK_CODE(code, line, END);

      code = tqSetBlockData(pBlock, pData[1], pBlock->info.rows + rowIndex, &colVal, pBlobSet);
      TSDB_CHECK_CODE(code, line, END);
    }
  }

  END:
  taosArrayDestroy(pColArray);
  return code;
}

static int32_t tqProcessSubmitCol(SArray*         pCols,
                                SSDataBlock*    pBlock,
                                SHashObj*       pCol2SlotId,
                                SBlobSet*       pBlobSet) {
  int32_t        code = 0;
  int32_t        line = 0;

  for (int32_t i = 0; i < taosArrayGetSize(pCols); i++) {
    SColData* pCol = taosArrayGet(pCols, i);
    TSDB_CHECK_NULL(pCol, code, line, END, terrno);
    void* pSlotId = taosHashGet(pCol2SlotId, &pCol->cid, sizeof(pCol->cid));
    if (pSlotId == NULL) {
      continue;
    }
    SColVal colVal = {0};
    for (int32_t row = 0; row < pCol->nVal; row++) {
      code = tColDataGetValue(pCol, row, &colVal);
      TSDB_CHECK_CODE(code, line, END);

      code = tqSetBlockData(pBlock, *(int16_t*)pSlotId, pBlock->info.rows + row, &colVal, pBlobSet);
      TSDB_CHECK_CODE(code, line, END);
    }
  }
  
  END:
  return code;
}

static int32_t tqCheckSchema(STqReader* pReader, SSubmitTbData* pSubmitTbData) {
  int32_t vgId = pReader->pWalReader->pWal->cfg.vgId;
  int32_t sversion = pSubmitTbData->sver;
  int64_t suid = pSubmitTbData->suid;
  int64_t uid = pSubmitTbData->uid;
  if ((suid != 0 && pReader->cachedSchemaSuid != suid) || (suid == 0 && pReader->cachedSchemaUid != uid) ||
      (pReader->cachedSchemaVer != sversion)) {
    tDeleteSchemaWrapper(pReader->pSchemaWrapper);
    taosMemoryFreeClear(pReader->extSchema);
    taosMemoryFreeClear(pReader->pTSchema);
    pReader->pSchemaWrapper = metaGetTableSchema(pReader->pVnode->pMeta, uid, sversion, 1, &pReader->extSchema, 0, true);
    if (pReader->pSchemaWrapper == NULL) {
      tqWarn("vgId:%d, cannot found schema wrapper for table: suid:%" PRId64 ", uid:%" PRId64 ",version %d, possibly dropped table",
              vgId, suid, uid, pReader->cachedSchemaVer);
      return TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND;
    }
    pReader->pTSchema = tBuildTSchema(pReader->pSchemaWrapper->pSchema, pReader->pSchemaWrapper->nCols, pReader->pSchemaWrapper->version);
    if (pReader->pTSchema == NULL) {
      tqWarn("vgId:%d, cannot build schema for table: suid:%" PRId64 ", uid:%" PRId64 ",version %d",
              vgId, suid, uid, pReader->cachedSchemaVer);
      return terrno;
    }
    pReader->cachedSchemaUid = uid;
    pReader->cachedSchemaSuid = suid;
    pReader->cachedSchemaVer = sversion;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t tqRetrieveCols(STqReader* pReader, SSDataBlock* pBlock, SHashObj* pCol2SlotId) {
  if (pReader == NULL || pBlock == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  tqDebug("tq reader retrieve data block %p, index:%d", pReader->msg.msgStr, pReader->nextBlk);
  int32_t        code = 0;
  int32_t        line = 0;
  SSubmitTbData* pSubmitTbData = taosArrayGet(pReader->submit.aSubmitTbData, pReader->nextBlk);
  TSDB_CHECK_NULL(pSubmitTbData, code, line, END, terrno);
  pReader->lastTs = pSubmitTbData->ctimeMs;

  int32_t numOfRows = 0;
  if (pSubmitTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    SColData* pCol = taosArrayGet(pSubmitTbData->aCol, 0);
    TSDB_CHECK_NULL(pCol, code, line, END, terrno);
    numOfRows = pCol->nVal;
  } else {
    numOfRows = taosArrayGetSize(pSubmitTbData->aRowP);
  }

  code = blockDataEnsureCapacity(pBlock, pBlock->info.rows + numOfRows);
  TSDB_CHECK_CODE(code, line, END);

  code = tqCheckSchema(pReader, pSubmitTbData);
  TSDB_CHECK_CODE(code, line, END);

  // convert and scan one block
  if (pSubmitTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    SArray* pCols = pSubmitTbData->aCol;
    code = tqProcessSubmitCol(pCols, pBlock, pCol2SlotId, pSubmitTbData->pBlobSet);
    TSDB_CHECK_CODE(code, line, END);
  } else {
    SArray*         pRows = pSubmitTbData->aRowP;
    code = tqProcessSubmitRow(pRows, pBlock, pCol2SlotId, pReader, pSubmitTbData->pBlobSet);
    TSDB_CHECK_CODE(code, line, END);
  }
  pBlock->info.rows += numOfRows;
END:
  if (code != 0) {
    tqError("tqRetrieveCols failed, line:%d, msg:%s", line, tstrerror(code));
  }
  return code;
}

#define PROCESS_VAL                                      \
  if (curRow == 0) {                                     \
    assigned[j] = !COL_VAL_IS_NONE(&colVal);             \
    buildNew = true;                                     \
  } else {                                               \
    bool currentRowAssigned = !COL_VAL_IS_NONE(&colVal); \
    if (currentRowAssigned != assigned[j]) {             \
      assigned[j] = currentRowAssigned;                  \
      buildNew = true;                                   \
    }                                                    \
  }

#define SET_DATA                                                                                    \
  if (colVal.cid < pColData->info.colId) {                                                          \
    sourceIdx++;                                                                                    \
  } else if (colVal.cid == pColData->info.colId) {                                                  \
    if (IS_STR_DATA_BLOB(pColData->info.type)) {                                                    \
      TQ_ERR_GO_TO_END(tqDoSetBlobVal(pColData, curRow - lastRow, &colVal, pSubmitTbData->pBlobSet)); \
    } else {                                                                                        \
      TQ_ERR_GO_TO_END(tqDoSetVal(pColData, curRow - lastRow, &colVal));                              \
    }                                                                                               \
    sourceIdx++;                                                                                    \
    targetIdx++;                                                                                    \
  } else {                                                                                          \
    colDataSetNULL(pColData, curRow - lastRow);                                                     \
    targetIdx++;                                                                                    \
  }

static int32_t tqProcessBuildNew(STqReader* pReader, SSubmitTbData* pSubmitTbData, SArray* blocks, SArray* schemas,
                               char* assigned, int32_t numOfRows, int32_t curRow, int32_t* lastRow) {
  int32_t         code = 0;
  SSchemaWrapper* pSW = NULL;
  SSDataBlock*    block = NULL;
  if (taosArrayGetSize(blocks) > 0) {
    SSDataBlock* pLastBlock = taosArrayGetLast(blocks);
    TQ_NULL_GO_TO_END(pLastBlock);
    pLastBlock->info.rows = curRow - *lastRow;
    *lastRow = curRow;
  }

  block = taosMemoryCalloc(1, sizeof(SSDataBlock));
  TQ_NULL_GO_TO_END(block);

  pSW = taosMemoryCalloc(1, sizeof(SSchemaWrapper));
  TQ_NULL_GO_TO_END(pSW);

  TQ_ERR_GO_TO_END(tqMaskBlock(pSW, block, pReader->pSchemaWrapper, assigned, pReader->extSchema));
  tqTrace("vgId:%d, build new block, col %d", pReader->pWalReader->pWal->cfg.vgId,
          (int32_t)taosArrayGetSize(block->pDataBlock));

  block->info.id.uid = pSubmitTbData->uid;
  block->info.version = pReader->msg.ver;
  TQ_ERR_GO_TO_END(blockDataEnsureCapacity(block, numOfRows - curRow));
  TQ_NULL_GO_TO_END(taosArrayPush(blocks, block));
  TQ_NULL_GO_TO_END(taosArrayPush(schemas, &pSW));
  pSW = NULL;

  taosMemoryFreeClear(block);

END:
  if (code != 0) {
    tqError("tqProcessBuildNew failed, code:%d", code);
  }
  tDeleteSchemaWrapper(pSW);
  blockDataFreeRes(block);
  taosMemoryFree(block);
  return code;
}
static int32_t tqProcessColData(STqReader* pReader, SSubmitTbData* pSubmitTbData, SArray* blocks, SArray* schemas) {
  int32_t code = 0;
  int32_t curRow = 0;
  int32_t lastRow = 0;

  SSchemaWrapper* pSchemaWrapper = pReader->pSchemaWrapper;
  char*           assigned = taosMemoryCalloc(1, pSchemaWrapper->nCols);
  TQ_NULL_GO_TO_END(assigned);

  SArray*   pCols = pSubmitTbData->aCol;
  SColData* pCol = taosArrayGet(pCols, 0);
  TQ_NULL_GO_TO_END(pCol);
  int32_t numOfRows = pCol->nVal;
  int32_t numOfCols = taosArrayGetSize(pCols);
  tqTrace("vgId:%d, tqProcessColData start, col num: %d, rows:%d", pReader->pWalReader->pWal->cfg.vgId, numOfCols,
          numOfRows);
  for (int32_t i = 0; i < numOfRows; i++) {
    bool buildNew = false;

    for (int32_t j = 0; j < pSchemaWrapper->nCols; j++) {
      int32_t k = 0;
      for (; k < numOfCols; k++) {
        pCol = taosArrayGet(pCols, k);
        TQ_NULL_GO_TO_END(pCol);
        if (pSchemaWrapper->pSchema[j].colId == pCol->cid) {
          SColVal colVal = {0};
          TQ_ERR_GO_TO_END(tColDataGetValue(pCol, i, &colVal));
          PROCESS_VAL
          tqTrace("assign[%d] = %d, nCols:%d", j, assigned[j], numOfCols);
          break;
        }
      }
      if (k >= numOfCols) {
        // this column is not in the current row, so we set it to NULL
        assigned[j] = 0;
        buildNew = true;
      }
    }

    if (buildNew) {
      TQ_ERR_GO_TO_END(tqProcessBuildNew(pReader, pSubmitTbData, blocks, schemas, assigned, numOfRows, curRow, &lastRow));
    }

    SSDataBlock* pBlock = taosArrayGetLast(blocks);
    TQ_NULL_GO_TO_END(pBlock);

    tqTrace("vgId:%d, taosx scan, block num: %d", pReader->pWalReader->pWal->cfg.vgId,
            (int32_t)taosArrayGetSize(blocks));

    int32_t targetIdx = 0;
    int32_t sourceIdx = 0;
    int32_t colActual = blockDataGetNumOfCols(pBlock);
    while (targetIdx < colActual && sourceIdx < numOfCols) {
      pCol = taosArrayGet(pCols, sourceIdx);
      TQ_NULL_GO_TO_END(pCol);
      SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, targetIdx);
      TQ_NULL_GO_TO_END(pColData);
      SColVal colVal = {0};
      TQ_ERR_GO_TO_END(tColDataGetValue(pCol, i, &colVal));
      SET_DATA
      tqTrace("targetIdx:%d sourceIdx:%d colActual:%d", targetIdx, sourceIdx, colActual);
    }

    curRow++;
  }
  SSDataBlock* pLastBlock = taosArrayGetLast(blocks);
  pLastBlock->info.rows = curRow - lastRow;
  tqTrace("vgId:%d, tqProcessColData end, col num: %d, rows:%d, block num:%d", pReader->pWalReader->pWal->cfg.vgId,
          numOfCols, numOfRows, (int)taosArrayGetSize(blocks));
END:
  if (code != TSDB_CODE_SUCCESS) {
    tqError("vgId:%d, process col data failed, code:%d", pReader->pWalReader->pWal->cfg.vgId, code);
  }
  taosMemoryFree(assigned);
  return code;
}

int32_t tqProcessRowData(STqReader* pReader, SSubmitTbData* pSubmitTbData, SArray* blocks, SArray* schemas) {
  int32_t   code = 0;
  STSchema* pTSchema = NULL;

  SSchemaWrapper* pSchemaWrapper = pReader->pSchemaWrapper;
  char*           assigned = taosMemoryCalloc(1, pSchemaWrapper->nCols);
  TQ_NULL_GO_TO_END(assigned);

  int32_t curRow = 0;
  int32_t lastRow = 0;
  SArray* pRows = pSubmitTbData->aRowP;
  int32_t numOfRows = taosArrayGetSize(pRows);
  pTSchema = tBuildTSchema(pSchemaWrapper->pSchema, pSchemaWrapper->nCols, pSchemaWrapper->version);
  TQ_NULL_GO_TO_END(pTSchema);
  tqTrace("vgId:%d, tqProcessRowData start, rows:%d", pReader->pWalReader->pWal->cfg.vgId, numOfRows);

  for (int32_t i = 0; i < numOfRows; i++) {
    bool  buildNew = false;
    SRow* pRow = taosArrayGetP(pRows, i);
    TQ_NULL_GO_TO_END(pRow);

    for (int32_t j = 0; j < pTSchema->numOfCols; j++) {
      SColVal colVal = {0};
      TQ_ERR_GO_TO_END(tRowGet(pRow, pTSchema, j, &colVal));
      PROCESS_VAL
      tqTrace("assign[%d] = %d, nCols:%d", j, assigned[j], pTSchema->numOfCols);
    }

    if (buildNew) {
      TQ_ERR_GO_TO_END(tqProcessBuildNew(pReader, pSubmitTbData, blocks, schemas, assigned, numOfRows, curRow, &lastRow));
    }

    SSDataBlock* pBlock = taosArrayGetLast(blocks);
    TQ_NULL_GO_TO_END(pBlock);

    tqTrace("vgId:%d, taosx scan, block num: %d", pReader->pWalReader->pWal->cfg.vgId,
            (int32_t)taosArrayGetSize(blocks));

    int32_t targetIdx = 0;
    int32_t sourceIdx = 0;
    int32_t colActual = blockDataGetNumOfCols(pBlock);
    while (targetIdx < colActual && sourceIdx < pTSchema->numOfCols) {
      SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, targetIdx);
      TQ_NULL_GO_TO_END(pColData);
      SColVal          colVal = {0};
      TQ_ERR_GO_TO_END(tRowGet(pRow, pTSchema, sourceIdx, &colVal));
      SET_DATA
      tqTrace("targetIdx:%d sourceIdx:%d colActual:%d", targetIdx, sourceIdx, colActual);
    }

    curRow++;
  }
  SSDataBlock* pLastBlock = taosArrayGetLast(blocks);
  if (pLastBlock != NULL) {
    pLastBlock->info.rows = curRow - lastRow;
  }

  tqTrace("vgId:%d, tqProcessRowData end, rows:%d, block num:%d", pReader->pWalReader->pWal->cfg.vgId, numOfRows,
          (int)taosArrayGetSize(blocks));
END:
  if (code != TSDB_CODE_SUCCESS) {
    tqError("vgId:%d, process row data failed, code:%d", pReader->pWalReader->pWal->cfg.vgId, code);
  }
  taosMemoryFreeClear(pTSchema);
  taosMemoryFree(assigned);
  return code;
}

static int32_t tqBuildCreateTbInfo(SMqDataRsp* pRsp, SVCreateTbReq* pCreateTbReq) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   createReq = NULL;
  TSDB_CHECK_NULL(pRsp, code, lino, END, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pCreateTbReq, code, lino, END, TSDB_CODE_INVALID_PARA);

  if (pRsp->createTableNum == 0) {
    pRsp->createTableLen = taosArrayInit(0, sizeof(int32_t));
    TSDB_CHECK_NULL(pRsp->createTableLen, code, lino, END, terrno);
    pRsp->createTableReq = taosArrayInit(0, sizeof(void*));
    TSDB_CHECK_NULL(pRsp->createTableReq, code, lino, END, terrno);
  }

  uint32_t len = 0;
  tEncodeSize(tEncodeSVCreateTbReq, pCreateTbReq, len, code);
  TSDB_CHECK_CODE(code, lino, END);
  createReq = taosMemoryCalloc(1, len);
  TSDB_CHECK_NULL(createReq, code, lino, END, terrno);

  SEncoder encoder = {0};
  tEncoderInit(&encoder, createReq, len);
  code = tEncodeSVCreateTbReq(&encoder, pCreateTbReq);
  tEncoderClear(&encoder);
  TSDB_CHECK_CODE(code, lino, END);
  TSDB_CHECK_NULL(taosArrayPush(pRsp->createTableLen, &len), code, lino, END, terrno);
  TSDB_CHECK_NULL(taosArrayPush(pRsp->createTableReq, &createReq), code, lino, END, terrno);
  pRsp->createTableNum++;
  tqTrace("build create table info msg success");

END:
  if (code != 0) {
    tqError("%s failed at %d, failed to build create table info msg:%s", __FUNCTION__, lino, tstrerror(code));
    taosMemoryFree(createReq);
  }
  return code;
}



int32_t tqReaderSetTbUidList(STqReader* pReader, const SArray* tbUidList, const char* id) {
  if (pReader == NULL || tbUidList == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  if (pReader->tbIdHash) {
    taosHashClear(pReader->tbIdHash);
  } else {
    pReader->tbIdHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
    if (pReader->tbIdHash == NULL) {
      tqError("s-task:%s failed to init hash table", id);
      return terrno;
    }
  }

  for (int i = 0; i < taosArrayGetSize(tbUidList); i++) {
    int64_t* pKey = (int64_t*)taosArrayGet(tbUidList, i);
    if (pKey && taosHashPut(pReader->tbIdHash, pKey, sizeof(int64_t), NULL, 0) != 0) {
      tqError("s-task:%s failed to add table uid:%" PRId64 " to hash", id, *pKey);
      continue;
    }
  }

  tqDebug("s-task:%s %d tables are set to be queried target table", id, (int32_t)taosArrayGetSize(tbUidList));
  return TSDB_CODE_SUCCESS;
}

void tqReaderAddTbUidList(STqReader* pReader, const SArray* pTableUidList) {
  if (pReader == NULL || pTableUidList == NULL) {
    return;
  }
  if (pReader->tbIdHash == NULL) {
    pReader->tbIdHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
    if (pReader->tbIdHash == NULL) {
      tqError("failed to init hash table");
      return;
    }
  }

  int32_t numOfTables = taosArrayGetSize(pTableUidList);
  for (int i = 0; i < numOfTables; i++) {
    int64_t* pKey = (int64_t*)taosArrayGet(pTableUidList, i);
    if (taosHashPut(pReader->tbIdHash, pKey, sizeof(int64_t), NULL, 0) != 0) {
      tqError("failed to add table uid:%" PRId64 " to hash", *pKey);
      continue;
    }
    tqDebug("%s add table uid:%" PRId64 " to hash:%p %p", __func__, *pKey, pReader, pReader->tbIdHash);
  }
}

bool tqReaderIsQueriedTable(STqReader* pReader, uint64_t uid) {
  if (pReader == NULL) {
    return false;
  }
  return taosHashGet(pReader->tbIdHash, &uid, sizeof(uint64_t)) != NULL;
}

bool tqCurrentBlockConsumed(const STqReader* pReader) {
  if (pReader == NULL) {
    return false;
  }
  return pReader->msg.msgStr == NULL;
}

void tqReaderRemoveTbUidList(STqReader* pReader, const SArray* tbUidList) {
  if (pReader == NULL || tbUidList == NULL) {
    return;
  }
  for (int32_t i = 0; i < taosArrayGetSize(tbUidList); i++) {
    int64_t* pKey = (int64_t*)taosArrayGet(tbUidList, i);
    int32_t code = taosHashRemove(pReader->tbIdHash, pKey, sizeof(int64_t));
    if (code != 0) {
      tqWarn("%s failed to remove table uid:%" PRId64 " from hash:%p %p, msg:%s", __func__, pKey != NULL ? *pKey : 0, pReader, pReader->tbIdHash, tstrerror(code));
    }
  }
}

int32_t tqDeleteTbUidList(STQ* pTq, SArray* tbUidList) {
  if (pTq == NULL) {
    return 0;  // mounted vnode may have no tq
  }
  if (tbUidList == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  void*   pIter = NULL;
  int32_t vgId = TD_VID(pTq->pVnode);

  // update the table list for each consumer handle
  taosWLockLatch(&pTq->lock);
  while (1) {
    pIter = taosHashIterate(pTq->pHandle, pIter);
    if (pIter == NULL) {
      break;
    }

    STqHandle* pTqHandle = (STqHandle*)pIter;
    tqDebug("%s subKey:%s, consumer:0x%" PRIx64 " delete table list", __func__, pTqHandle->subKey, pTqHandle->consumerId);
    if (pTqHandle->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
      int32_t code = qDeleteTableListForQuerySub(pTqHandle->execHandle.task, tbUidList);
      if (code != 0) {
        tqError("update qualified table error for %s", pTqHandle->subKey);
        continue;
      }
    }
  }
  taosWUnLockLatch(&pTq->lock);
  return 0;
}

static SArray* tqCopyUidList(const SArray* tbUidList) {
  SArray* tbUidListCopy = taosArrayInit(4, sizeof(int64_t));
  if (tbUidListCopy == NULL) {
    return NULL;
  }

  if (taosArrayAddAll(tbUidListCopy, tbUidList) == NULL) {
    taosArrayDestroy(tbUidListCopy);
    tqError("copy table uid list failed");
    return NULL;
  }
  return tbUidListCopy;
}

static int32_t tqAddTableListForStbSub(STqHandle* pTqHandle, STQ* pTq, const SArray* tbUidList, int64_t version) {
  int32_t code = 0;
  SArray* tbUidListCopy = tqCopyUidList(tbUidList);
  if (tbUidListCopy == NULL) {
    code = terrno;
    goto END;
  }
  code = qFilterTableList(pTq->pVnode, tbUidListCopy, version, pTqHandle->execHandle.execTb.node,
                      pTqHandle->execHandle.task, pTqHandle->execHandle.execTb.suid);
  if (code != TDB_CODE_SUCCESS) {
    tqError("%s error:%d handle %s consumer:0x%" PRIx64, __func__, code, pTqHandle->subKey,
            pTqHandle->consumerId);
    goto END;
  }
  tqDebug("%s handle %s consumer:0x%" PRIx64 " add %d tables to tqReader", __func__, pTqHandle->subKey,
          pTqHandle->consumerId, (int32_t)taosArrayGetSize(tbUidListCopy));
  tqReaderAddTbUidList(pTqHandle->execHandle.pTqReader, tbUidListCopy);

END:
  taosArrayDestroy(tbUidListCopy);
  return code;
}

int32_t tqAddTbUidListForQuerySub(STQ* pTq, const SArray* tbUidList) {
  if (pTq == NULL) {
    return 0;  // mounted vnode may have no tq
  }
  if (tbUidList == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  void*   pIter = NULL;
  int32_t vgId = TD_VID(pTq->pVnode);
  int32_t code = 0;

  // update the table list for each consumer handle
  taosWLockLatch(&pTq->lock);
  while (1) {
    pIter = taosHashIterate(pTq->pHandle, pIter);
    if (pIter == NULL) {
      break;
    }

    STqHandle* pTqHandle = (STqHandle*)pIter;
    tqDebug("%s subKey:%s, consumer:0x%" PRIx64 " add table list", __func__, pTqHandle->subKey, pTqHandle->consumerId);
    if (pTqHandle->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
      code = qAddTableListForQuerySub(pTqHandle->execHandle.task, tbUidList);
      if (code != 0) {
        tqError("add table list for query tmq error for %s, msg:%s", pTqHandle->subKey, tstrerror(code));
        break;
      }
    }
  }
  taosHashCancelIterate(pTq->pHandle, pIter);
  taosWUnLockLatch(&pTq->lock);

  return code;
}

int32_t tqUpdateTbUidListForQuerySub(STQ* pTq, const SArray* tbUidList, SArray* cidList, SArray* cidListArray) {
  if (pTq == NULL) {
    return 0;  // mounted vnode may have no tq
  }
  if (tbUidList == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  void*   pIter = NULL;
  int32_t vgId = TD_VID(pTq->pVnode);
  int32_t code = 0;
  // update the table list for each consumer handle
  taosWLockLatch(&pTq->lock);
  while (1) {
    pIter = taosHashIterate(pTq->pHandle, pIter);
    if (pIter == NULL) {
      break;
    }

    STqHandle* pTqHandle = (STqHandle*)pIter;
    tqDebug("%s subKey:%s, consumer:0x%" PRIx64 " update table list", __func__, pTqHandle->subKey, pTqHandle->consumerId);
    if (pTqHandle->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
      SNode* pTagCond = getTagCondNodeForQueryTmq(pTqHandle->execHandle.task);
      bool ret = checkCidInTagCondition(pTagCond, cidList);
      if (ret){
        code = qUpdateTableListForQuerySub(pTqHandle->execHandle.task, tbUidList);
        if (code != 0) {
          tqError("update table list for query tmq error for %s, msg:%s", pTqHandle->subKey, tstrerror(code));
          break;
        }
      }
      qUpdateTableTagCacheForQuerySub(pTqHandle->execHandle.task, tbUidList, cidList, cidListArray);
    }
  }

  taosHashCancelIterate(pTq->pHandle, pIter);
  taosWUnLockLatch(&pTq->lock);

  return code;
}

static int32_t tqUpdateTbUidListForStbSub(STQ* pTq, const SArray* tbUidList, SArray* cidList, SArray* cidListArray, STqHandle* pTqHandle, int64_t version) {
  if (pTq == NULL) {
    return 0;  // mounted vnode may have no tq
  }
  if (tbUidList == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  void*   pIter = NULL;
  int32_t vgId = TD_VID(pTq->pVnode);
  int32_t code = 0;
  taosWLockLatch(&pTq->lock);
  tqDebug("%s subKey:%s, consumer:0x%" PRIx64 " update table list", __func__, pTqHandle->subKey, pTqHandle->consumerId);
  SNode* pTagCond = getTagCondNodeForStableTmq(pTqHandle->execHandle.execTb.node);
  bool ret = checkCidInTagCondition(pTagCond, cidList);
  if (ret){
    tqReaderRemoveTbUidList(pTqHandle->execHandle.pTqReader, tbUidList);
    code = tqAddTableListForStbSub(pTqHandle, pTq, tbUidList, version);
    if (code != 0) {
      tqError("update table list for stable tmq error for %s, msg:%s", pTqHandle->subKey, tstrerror(code));
    }
  }
  taosWUnLockLatch(&pTq->lock);

  return code;
}

static void tqAlterTagForStbSub(SVnode *pVnode, const SArray* tbUidList, const SArray* tags, const SArray* tagsArray, STqHandle* pTqHandle, int64_t version) {
  int32_t       code = 0;
  int32_t       lino = 0;
  SArray*       cidList = NULL;
  SArray*       cidListArray = NULL;

  code = getCidInfo(tags, tagsArray, &cidList, &cidListArray);
  QUERY_CHECK_CODE(code, lino, end);

  tqDebug("vgId:%d, try to add %d tables in query table list, cidList size:%"PRIzu,
         TD_VID(pVnode), (int32_t)taosArrayGetSize(tbUidList), taosArrayGetSize(cidList));
  code = tqUpdateTbUidListForStbSub(pVnode->pTq, tbUidList, cidList, cidListArray, pTqHandle, version);
  QUERY_CHECK_CODE(code, lino, end);

end:
  if (code != 0) {
    qError("vgId:%d, failed to alter tags for %d tables since %s",
           TD_VID(pVnode), (int32_t)taosArrayGetSize(tbUidList), tstrerror(code));
  }
  taosArrayDestroy(cidList);
  taosArrayDestroyP(cidListArray, (FDelete)taosArrayDestroy);
}

static void tqDestroySourceScanTables(void* ptr) {
  SArray** pTables = ptr;
  if (pTables && *pTables) {
    taosArrayDestroy(*pTables);
    *pTables = NULL;
  }
}

static int32_t tqCompareSVTColInfo(const void* p1, const void* p2) {
  SVTColInfo* pCol1 = (SVTColInfo*)p1;
  SVTColInfo* pCol2 = (SVTColInfo*)p2;
  if (pCol1->vColId == pCol2->vColId) {
    return 0;
  } else if (pCol1->vColId < pCol2->vColId) {
    return -1;
  } else {
    return 1;
  }
}

static void tqFreeTableSchemaCache(const void* key, size_t keyLen, void* value, void* ud) {
  if (value) {
    SSchemaWrapper* pSchemaWrapper = value;
    tDeleteSchemaWrapper(pSchemaWrapper);
  }
}

static int32_t tqAddRawDataToRsp(const void* rawData, SMqDataRsp* pRsp, int8_t precision) {
  int32_t    code = TDB_CODE_SUCCESS;
  int32_t    lino = 0;
  void*      buf = NULL;

  int32_t dataStrLen = sizeof(SRetrieveTableRspForTmq) + *(uint32_t *)rawData + INT_BYTES;
  buf = taosMemoryCalloc(1, dataStrLen);
  TSDB_CHECK_NULL(buf, code, lino, END, terrno);

  SRetrieveTableRspForTmq* pRetrieve = (SRetrieveTableRspForTmq*)buf;
  pRetrieve->version = RETRIEVE_TABLE_RSP_TMQ_RAW_VERSION;
  pRetrieve->precision = precision;
  pRetrieve->compressed = 0;

  memcpy(pRetrieve->data, rawData, *(uint32_t *)rawData + INT_BYTES);
  TSDB_CHECK_NULL(taosArrayPush(pRsp->blockDataLen, &dataStrLen), code, lino, END, terrno);
  TSDB_CHECK_NULL(taosArrayPush(pRsp->blockData, &buf), code, lino, END, terrno);
  pRsp->blockDataElementFree = true;

  tqTrace("tqAddRawDataToRsp add block data to block array, blockDataLen:%d, blockData:%p", dataStrLen, buf);
  END:
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(buf);
    tqError("%s failed at %d, failed to add block data to response:%s", __FUNCTION__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tqAddBlockDataToRsp(const SSDataBlock* pBlock, SMqDataRsp* pRsp, const SSchemaWrapper* pSW, int8_t precision) {
  int32_t code = 0;
  int32_t lino = 0;
  SSchemaWrapper* pSchema = NULL;
  
  size_t dataEncodeBufSize = blockGetEncodeSize(pBlock);
  int32_t dataStrLen = sizeof(SRetrieveTableRspForTmq) + dataEncodeBufSize;
  void*   buf = taosMemoryCalloc(1, dataStrLen);
  TSDB_CHECK_NULL(buf, code, lino, END, terrno);

  SRetrieveTableRspForTmq* pRetrieve = (SRetrieveTableRspForTmq*)buf;
  pRetrieve->version = RETRIEVE_TABLE_RSP_TMQ_VERSION;
  pRetrieve->precision = precision;
  pRetrieve->compressed = 0;
  pRetrieve->numOfRows = htobe64((int64_t)pBlock->info.rows);

  int32_t actualLen = blockEncode(pBlock, pRetrieve->data, dataEncodeBufSize, pSW->nCols);
  TSDB_CHECK_CONDITION(actualLen >= 0, code, lino, END, terrno);

  actualLen += sizeof(SRetrieveTableRspForTmq);
  TSDB_CHECK_NULL(taosArrayPush(pRsp->blockDataLen, &actualLen), code, lino, END, terrno);
  TSDB_CHECK_NULL(taosArrayPush(pRsp->blockData, &buf), code, lino, END, terrno);
  pSchema = tCloneSSchemaWrapper(pSW);
  TSDB_CHECK_NULL(pSchema, code, lino, END, terrno);
  TSDB_CHECK_NULL(taosArrayPush(pRsp->blockSchema, &pSchema), code, lino, END, terrno);
  pSchema = NULL;
  pRsp->blockDataElementFree = true;
  tqTrace("tqAddBlockDataToRsp add block data to block array, blockDataLen:%d, blockData:%p", dataStrLen, buf);

END:
  tDeleteSchemaWrapper(pSchema);
  if (code != TSDB_CODE_SUCCESS){
    taosMemoryFree(buf);
    tqError("%s failed at line %d with msg:%s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tqAddTbNameToRsp(const STQ* pTq, int64_t uid, SMqDataRsp* pRsp, int32_t n) {
  int32_t    code = TDB_CODE_SUCCESS;
  int32_t    lino = 0;
  SMetaReader mr = {0};

  TSDB_CHECK_NULL(pTq, code, lino, END, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pRsp, code, lino, END, TSDB_CODE_INVALID_PARA);

  metaReaderDoInit(&mr, pTq->pVnode->pMeta, META_READER_LOCK);

  code = metaReaderGetTableEntryByUidCache(&mr, uid);
  if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST){
    char tbname[TSDB_TABLE_NAME_LEN] = {0};
    code = metaGetTbnameByIdIfTableNotExist(pTq->pVnode->pMeta, uid, tbname);
    TSDB_CHECK_CODE(code, lino, END);

    for (int32_t i = 0; i < n; i++) {
      char* tbName = taosStrdup(tbname);
      TSDB_CHECK_NULL(tbName, code, lino, END, terrno);
      if(taosArrayPush(pRsp->blockTbName, &tbName) == NULL){
        tqError("failed to push tbName to blockTbName:%s, uid:%"PRId64, tbName, uid);
        continue;
      }
      tqTrace("add tbName to response success tbname:%s, uid:%"PRId64, tbName, uid);
    }
    goto END;
  }
  TSDB_CHECK_CODE(code, lino, END);

  for (int32_t i = 0; i < n; i++) {
    char* tbName = taosStrdup(mr.me.name);
    TSDB_CHECK_NULL(tbName, code, lino, END, terrno);
    if(taosArrayPush(pRsp->blockTbName, &tbName) == NULL){
      tqError("failed to push tbName to blockTbName:%s, uid:%"PRId64, tbName, uid);
      continue;
    }
    tqTrace("add tbName to response success tbname:%s, uid:%"PRId64, tbName, uid);
  }

END:
  if (code != TSDB_CODE_SUCCESS) {
    tqError("%s failed at %d, failed to add tbName to response:%s, uid:%"PRId64, __FUNCTION__, lino, tstrerror(code), uid);
  }
  metaReaderClear(&mr);
  return code;
}

int32_t tqGetDataBlock(qTaskInfo_t task, const STqHandle* pHandle, int32_t vgId, SSDataBlock** res) {
  if (task == NULL || pHandle == NULL || res == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  uint64_t ts = 0;
  qStreamSetOpen(task);

  tqDebug("consumer:0x%" PRIx64 " vgId:%d, tmq one task start execute", pHandle->consumerId, vgId);
  int32_t code = qExecTask(task, res, &ts);
  if (code != TSDB_CODE_SUCCESS) {
    tqError("consumer:0x%" PRIx64 " vgId:%d, task exec error since %s", pHandle->consumerId, vgId, tstrerror(code));
  }

  tqDebug("consumer:0x%" PRIx64 " vgId:%d tmq one task end executed, pDataBlock:%p", pHandle->consumerId, vgId, *res);
  return code;
}

static int32_t tqProcessReplayRsp(STQ* pTq, STqHandle* pHandle, SMqDataRsp* pRsp, const SMqPollReq* pRequest, SSDataBlock* pDataBlock, qTaskInfo_t task){
  int32_t code = 0;
  int32_t lino = 0;

  if (IS_OFFSET_RESET_TYPE(pRequest->reqOffset.type) && pHandle->block != NULL) {
    blockDataDestroy(pHandle->block);
    pHandle->block = NULL;
  }
  if (pHandle->block == NULL) {
    if (pDataBlock == NULL) {
      goto END;
    }

    STqOffsetVal offset = {0};
    code = qStreamExtractOffset(task, &offset);
    TSDB_CHECK_CODE(code, lino, END);

    pHandle->block = NULL;

    code = createOneDataBlock(pDataBlock, true, &pHandle->block);
    TSDB_CHECK_CODE(code, lino, END);

    pHandle->blockTime = offset.ts;
    tOffsetDestroy(&offset);
    int32_t vgId = TD_VID(pTq->pVnode);
    code = tqGetDataBlock(task, pHandle, vgId, &pDataBlock);
    TSDB_CHECK_CODE(code, lino, END);
  }

  const STqExecHandle* pExec = &pHandle->execHandle;
  code = tqAddBlockDataToRsp(pHandle->block, pRsp, &pExec->execCol.pSW, pTq->pVnode->config.tsdbCfg.precision);
  TSDB_CHECK_CODE(code, lino, END);

  pRsp->blockNum++;
  if (pDataBlock == NULL) {
    blockDataDestroy(pHandle->block);
    pHandle->block = NULL;
  } else {
    code = copyDataBlock(pHandle->block, pDataBlock);
    TSDB_CHECK_CODE(code, lino, END);

    STqOffsetVal offset = {0};
    code = qStreamExtractOffset(task, &offset);
    TSDB_CHECK_CODE(code, lino, END);

    pRsp->sleepTime = offset.ts - pHandle->blockTime;
    pHandle->blockTime = offset.ts;
    tOffsetDestroy(&offset);
  }

END:
  if (code != TSDB_CODE_SUCCESS) {
    tqError("%s failed at %d, failed to process replay response:%s", __FUNCTION__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tqScanData(STQ* pTq, STqHandle* pHandle, SMqDataRsp* pRsp, STqOffsetVal* pOffset, const SMqPollReq* pRequest) {
  int32_t code = 0;
  int32_t lino = 0;
  TSDB_CHECK_NULL(pRsp, code, lino, END, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pTq, code, lino, END, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pHandle, code, lino, END, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pOffset, code, lino, END, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pRequest, code, lino, END, TSDB_CODE_INVALID_PARA);

  int32_t vgId = TD_VID(pTq->pVnode);
  int32_t totalRows = 0;

  const STqExecHandle* pExec = &pHandle->execHandle;
  qTaskInfo_t          task = pExec->task;

  code = qStreamPrepareScan(task, pOffset, pHandle->execHandle.subType);
  TSDB_CHECK_CODE(code, lino, END);

  qStreamSetParams(task, pRequest->sourceExcluded, pRequest->minPollRows, pRequest->timeout, pRequest->enableReplay);
  do {
    SSDataBlock* pDataBlock = NULL;
    code = tqGetDataBlock(task, pHandle, vgId, &pDataBlock);
    TSDB_CHECK_CODE(code, lino, END);

    if (pRequest->enableReplay) {
      code = tqProcessReplayRsp(pTq, pHandle, pRsp, pRequest, pDataBlock, task);
      TSDB_CHECK_CODE(code, lino, END);
      break;
    }
    if (pDataBlock == NULL) {
      break;
    }
    code = tqAddBlockDataToRsp(pDataBlock, pRsp, &pExec->execCol.pSW, pTq->pVnode->config.tsdbCfg.precision);
    TSDB_CHECK_CODE(code, lino, END);

    pRsp->blockNum++;
    totalRows += pDataBlock->info.rows;
  } while(0);

  tqDebug("consumer:0x%" PRIx64 " vgId:%d tmq task executed finished, total blocks:%d, totalRows:%d", pHandle->consumerId, vgId, pRsp->blockNum, totalRows);
  code = qStreamExtractOffset(task, &pRsp->rspOffset);

END:
  if (code != 0) {
    tqError("%s failed at %d, tmq task executed error msg:%s", __FUNCTION__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tqScanTaosx(STQ* pTq, const STqHandle* pHandle, SMqDataRsp* pRsp, SMqBatchMetaRsp* pBatchMetaRsp, STqOffsetVal* pOffset, const SMqPollReq* pRequest) {
  int32_t code = 0;
  int32_t lino = 0;
  char* tbName = NULL;
  const STqExecHandle* pExec = &pHandle->execHandle;
  qTaskInfo_t          task = pExec->task;
  code = qStreamPrepareScan(task, pOffset, pHandle->execHandle.subType);
  TSDB_CHECK_CODE(code, lino, END);

  qStreamSetParams(task, pRequest->sourceExcluded, pRequest->minPollRows, pRequest->timeout, false);

  int32_t rowCnt = 0;
  int64_t st = taosGetTimestampMs();
  while (1) {
    SSDataBlock* pDataBlock = NULL;
    uint64_t     ts = 0;
    tqDebug("tmqsnap task start to execute");
    code = qExecTask(task, &pDataBlock, &ts);
    TSDB_CHECK_CODE(code, lino, END);
    tqDebug("tmqsnap task execute end, get %p", pDataBlock);

    if (pDataBlock != NULL && pDataBlock->info.rows > 0) {
      if (pRsp->withTbName) {
        tbName = taosStrdup(qExtractTbnameFromTask(task));
        TSDB_CHECK_NULL(tbName, code, lino, END, terrno);
        TSDB_CHECK_NULL(taosArrayPush(pRsp->blockTbName, &tbName), code, lino, END, terrno);
        tqDebug("vgId:%d, add tbname:%s to rsp msg", pTq->pVnode->config.vgId, tbName);
        tbName = NULL;
      }

      code = tqAddBlockDataToRsp(pDataBlock, pRsp, qExtractSchemaFromTask(task), pTq->pVnode->config.tsdbCfg.precision);
      TSDB_CHECK_CODE(code, lino, END);

      pRsp->blockNum++;
      rowCnt += pDataBlock->info.rows;
      if (rowCnt <= pRequest->minPollRows && (taosGetTimestampMs() - st <= pRequest->timeout)) {
        continue;
      }
    }

    // get meta
    SMqBatchMetaRsp* tmp = qStreamExtractMetaMsg(task);
    if (taosArrayGetSize(tmp->batchMetaReq) > 0) {
      code = qStreamExtractOffset(task, &tmp->rspOffset);
      TSDB_CHECK_CODE(code, lino, END);
      *pBatchMetaRsp = *tmp;
      tqDebug("tmqsnap task get meta");
      break;
    }

    if (pDataBlock == NULL) {
      code = qStreamExtractOffset(task, pOffset);
      TSDB_CHECK_CODE(code, lino, END);

      if (pOffset->type == TMQ_OFFSET__SNAPSHOT_DATA) {
        continue;
      }

      tqDebug("tmqsnap vgId: %d, tsdb consume over, switch to wal, ver %" PRId64, TD_VID(pTq->pVnode), pHandle->snapshotVer + 1);
      code = qStreamExtractOffset(task, &pRsp->rspOffset);
      break;
    }

    if (pRsp->blockNum > 0) {
      tqDebug("tmqsnap task exec exited, get data");
      code = qStreamExtractOffset(task, &pRsp->rspOffset);
      break;
    }
  }
  tqDebug("%s:%d success", __FUNCTION__, lino);
END:
  if (code != 0){
    tqError("%s failed at %d, vgId:%d, task exec error since %s", __FUNCTION__ , lino, pTq->pVnode->config.vgId, tstrerror(code));
  }
  taosMemoryFree(tbName);
  return code;
}

static int32_t tqRetrieveTaosxBlock(STqReader* pReader, SMqDataRsp* pRsp, SArray* blocks, SArray* schemas,
                             SSubmitTbData* pSubmitTbData, SArray* rawList, int8_t fetchMeta) {
  tqTrace("tq reader retrieve data block msg pointer:%p, index:%d", pReader->msg.msgStr, pReader->nextBlk);
  if (fetchMeta == ONLY_META) {
    if (pSubmitTbData->pCreateTbReq != NULL) {
      if (pRsp->createTableReq == NULL) {
        pRsp->createTableReq = taosArrayInit(0, POINTER_BYTES);
        if (pRsp->createTableReq == NULL) {
          return terrno;
        }
      }
      if (taosArrayPush(pRsp->createTableReq, &pSubmitTbData->pCreateTbReq) == NULL) {
        return terrno;
      }
      pSubmitTbData->pCreateTbReq = NULL;
    }
    return 0;
  }

  int32_t sversion = pSubmitTbData->sver;
  int64_t uid = pSubmitTbData->uid;
  pReader->lastBlkUid = uid;

  tDeleteSchemaWrapper(pReader->pSchemaWrapper);
  taosMemoryFreeClear(pReader->extSchema);
  pReader->pSchemaWrapper = metaGetTableSchema(pReader->pVnode->pMeta, uid, sversion, 1, &pReader->extSchema, 0, true);
  if (pReader->pSchemaWrapper == NULL) {
    tqWarn("vgId:%d, cannot found schema wrapper for table: suid:%" PRId64 ", version %d, possibly dropped table",
           pReader->pWalReader->pWal->cfg.vgId, uid, sversion);
    pReader->cachedSchemaSuid = 0;
    return TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND;
  }

  if (pSubmitTbData->pCreateTbReq != NULL && fetchMeta != ONLY_DATA) {
    int32_t code = tqBuildCreateTbInfo(pRsp, pSubmitTbData->pCreateTbReq);
    if (code != 0) {
      return code;
    }
  } else if (rawList != NULL) {
    if (taosArrayPush(schemas, &pReader->pSchemaWrapper) == NULL) {
      return terrno;
    }
    pReader->pSchemaWrapper = NULL;
    return 0;
  }

  if (pSubmitTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    return tqProcessColData(pReader, pSubmitTbData, blocks, schemas);
  } else {
    return tqProcessRowData(pReader, pSubmitTbData, blocks, schemas);
  }
}

static bool tqFilterForStbSub(STQ* pTq, STqHandle* pHandle, SSubmitTbData* pSubmitTbData, int64_t version) {
  bool ret = false;
  SArray* tbUids = NULL;
  if (pHandle->execHandle.subType != TOPIC_SUB_TYPE__TABLE) {
    goto end;
  }
  if (pSubmitTbData->pCreateTbReq != NULL && (pSubmitTbData->pCreateTbReq->type == TSDB_CHILD_TABLE || pSubmitTbData->pCreateTbReq->type == TSDB_VIRTUAL_CHILD_TABLE)
        && pSubmitTbData->pCreateTbReq->ctb.suid == pHandle->execHandle.execTb.suid) {
    tbUids = taosArrayInit(1, sizeof(int64_t));
    if (tbUids == NULL) {
      goto end;
    }
    if (taosArrayPush(tbUids, &pSubmitTbData->uid) == NULL) {
      goto end;
    }
    
    taosWLockLatch(&pTq->lock);
    tqReaderRemoveTbUidList(pHandle->execHandle.pTqReader, tbUids);
    int32_t code = tqAddTableListForStbSub(pHandle, pTq, tbUids, version);
    taosWUnLockLatch(&pTq->lock);
    if (code != 0) {
      goto end;
    }
  }
  
  STqExecHandle* pExec = &pHandle->execHandle;
  STqReader* pReader = pExec->pTqReader;
  if (taosHashGet(pReader->tbIdHash, &pSubmitTbData->uid, sizeof(int64_t)) == NULL) {
    tqInfo("iterator submit block in hash continue for stb sub, progress:%d/%d, total queried tables:%d, uid:%" PRId64,
            pReader->nextBlk, (int32_t)taosArrayGetSize(pReader->submit.aSubmitTbData), (int32_t)taosHashGetSize(pReader->tbIdHash), pSubmitTbData->uid);
    ret = true;
  }

end:
  taosArrayDestroy(tbUids);
  return ret;
}

static void tqProcessSubData(STQ* pTq, STqHandle* pHandle, SMqDataRsp* pRsp, int32_t* totalRows,
                             const SMqPollReq* pRequest, SArray* rawList, int64_t version){
  int32_t code = 0;
  int32_t lino = 0;
  SArray* pBlocks = NULL;
  SArray* pSchemas = NULL;

  STqExecHandle* pExec = &pHandle->execHandle;
  STqReader* pReader = pExec->pTqReader;

  SSubmitTbData* pSubmitTbData = taosArrayGet(pReader->submit.aSubmitTbData, pReader->nextBlk);
  TSDB_CHECK_NULL(pSubmitTbData, code, lino, END, terrno);

  if (tqFilterForStbSub(pTq, pHandle, pSubmitTbData, version)) {
    goto END;
  }

  pBlocks = taosArrayInit(0, sizeof(SSDataBlock));
  TSDB_CHECK_NULL(pBlocks, code, lino, END, terrno);
  pSchemas = taosArrayInit(0, sizeof(void*));
  TSDB_CHECK_NULL(pSchemas, code, lino, END, terrno);
  
  code = tqRetrieveTaosxBlock(pReader, pRsp, pBlocks, pSchemas, pSubmitTbData, rawList, pHandle->fetchMeta);
  TSDB_CHECK_CODE(code, lino, END);
  bool tmp = (pSubmitTbData->flags & pRequest->sourceExcluded) != 0;
  TSDB_CHECK_CONDITION(!tmp, code, lino, END, TSDB_CODE_SUCCESS);

  if (pHandle->fetchMeta == ONLY_META){
    goto END;
  }

  int32_t blockNum = taosArrayGetSize(pBlocks) == 0 ? 1 : taosArrayGetSize(pBlocks);
  if (pRsp->withTbName) {
    int64_t uid = pExec->pTqReader->lastBlkUid;
    code = tqAddTbNameToRsp(pTq, uid, pRsp, blockNum);
    TSDB_CHECK_CODE(code, lino, END);
  }

  TSDB_CHECK_CONDITION(!tmp, code, lino, END, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < blockNum; i++) {
    if (taosArrayGetSize(pBlocks) == 0){
      void* rawData = taosArrayGetP(rawList, pReader->nextBlk);
      if (rawData == NULL) {
        continue;
      }
      if (tqAddRawDataToRsp(rawData, pRsp, pTq->pVnode->config.tsdbCfg.precision) != 0){
        tqError("vgId:%d, failed to add block to rsp msg", pTq->pVnode->config.vgId);
        continue;
      }
      *totalRows += *(uint32_t *)rawData + INT_BYTES; // bytes actually
    } else {
      SSDataBlock* pBlock = taosArrayGet(pBlocks, i);
      if (pBlock == NULL) {
        continue;
      }

      SSchemaWrapper* pSW = (SSchemaWrapper*)taosArrayGetP(pSchemas, i);
      if (tqAddBlockDataToRsp(pBlock, pRsp, pSW, pTq->pVnode->config.tsdbCfg.precision) != 0){
        tqError("vgId:%d, failed to add block to rsp msg", pTq->pVnode->config.vgId);
        continue;
      }
      *totalRows += pBlock->info.rows;
    }

    pRsp->blockNum++;
  }
  tqTrace("vgId:%d, process sub data success, response blocknum:%d, rows:%d", pTq->pVnode->config.vgId, pRsp->blockNum, *totalRows);
END:
  if (code != 0) {
    tqError("%s failed at %d, failed to process sub data:%s", __FUNCTION__, lino, tstrerror(code));
  }
  taosArrayDestroyEx(pBlocks, (FDelete)blockDataFreeRes);
  taosArrayDestroyP(pSchemas, (FDelete)tDeleteSchemaWrapper);
}

static void tqPreProcessSubmitMsg(STqHandle* pHandle, const SMqPollReq* pRequest, SArray** rawList){
  STqExecHandle* pExec = &pHandle->execHandle;
  STqReader* pReader = pExec->pTqReader;
  int32_t blockSz = taosArrayGetSize(pReader->submit.aSubmitTbData);
  for (int32_t i = 0; i < blockSz; i++){
    SSubmitTbData* pSubmitTbData = taosArrayGet(pReader->submit.aSubmitTbData, i);
    if (pSubmitTbData== NULL){
      taosArrayDestroy(*rawList);
      *rawList = NULL;
      return;
    }

    int64_t uid = pSubmitTbData->uid;
    if (pRequest->rawData) {
      if (taosHashGet(pRequest->uidHash, &uid, LONG_BYTES) != NULL) {
        tqDebug("poll rawdata split,uid:%" PRId64 " is already exists", uid);
        terrno = TSDB_CODE_TMQ_RAW_DATA_SPLIT;
        return;
      } else {
        int32_t code = taosHashPut(pRequest->uidHash, &uid, LONG_BYTES, &uid, LONG_BYTES);
        if (code != 0) {
          tqError("failed to add table uid to hash, code:%d, uid:%" PRId64, code, uid);
        }
      }
    }

    if (pSubmitTbData->pCreateTbReq == NULL){
      continue;
    }

    int64_t createTime = INT64_MAX;
    int64_t *cTime = (int64_t*)taosHashGet(pHandle->tableCreateTimeHash, &uid, LONG_BYTES);
    if (cTime != NULL){
      createTime = *cTime;
    } else{
      createTime = metaGetTableCreateTime(pReader->pVnode->pMeta, uid, 1);
      if (createTime != INT64_MAX){
        int32_t code = taosHashPut(pHandle->tableCreateTimeHash, &uid, LONG_BYTES, &createTime, LONG_BYTES);
        if (code != 0){
          tqError("failed to add table create time to hash,code:%d, uid:%"PRId64, code, uid);
        }
      }
    }
    if (pSubmitTbData->ctimeMs > createTime){
      tDestroySVSubmitCreateTbReq(pSubmitTbData->pCreateTbReq, TSDB_MSG_FLG_DECODE);
      taosMemoryFreeClear(pSubmitTbData->pCreateTbReq);
    } else if (pHandle->fetchMeta != ONLY_DATA){
      taosArrayDestroy(*rawList);
      *rawList = NULL;
    }
  }
}

static int32_t tqTaosxScanLog(STQ* pTq, STqHandle* pHandle, SPackedData submit, SMqDataRsp* pRsp, int32_t* totalRows, const SMqPollReq* pRequest) {
  int32_t code = 0;
  int32_t lino = 0;
  SDecoder decoder = {0};
  STqExecHandle* pExec = &pHandle->execHandle;
  STqReader* pReader = pExec->pTqReader;
  SArray *rawList = NULL;
  if (pRequest->rawData){
    rawList = taosArrayInit(0, POINTER_BYTES);
    TSDB_CHECK_NULL(rawList, code, lino, END, terrno);
  }
  code = tqReaderSetSubmitMsg(pReader, submit.msgStr, submit.msgLen, submit.ver, rawList, &decoder);
  TSDB_CHECK_CODE(code, lino, END);
  tqPreProcessSubmitMsg(pHandle, pRequest, &rawList);
  // data could not contains same uid data in rawdata mode
  if (pRequest->rawData != 0 && terrno == TSDB_CODE_TMQ_RAW_DATA_SPLIT){
    goto END;
  }

  // this submit data is metadata and previous data is rawdata
  if (pRequest->rawData != 0 && *totalRows > 0 && pRsp->createTableNum == 0 && rawList == NULL){
    tqDebug("poll rawdata split,vgId:%d, this wal submit data contains metadata and previous data is data", pTq->pVnode->config.vgId);
    terrno = TSDB_CODE_TMQ_RAW_DATA_SPLIT;
    goto END;
  }

  // this submit data is rawdata and previous data is metadata
  if (pRequest->rawData != 0 && pRsp->createTableNum > 0 && rawList != NULL){
    tqDebug("poll rawdata split,vgId:%d, this wal submit data is data and previous data is metadata", pTq->pVnode->config.vgId);
    terrno = TSDB_CODE_TMQ_RAW_DATA_SPLIT;
    goto END;
  }

  int32_t blockSz = taosArrayGetSize(pReader->submit.aSubmitTbData);
  while (pReader->nextBlk < blockSz) {
    tqProcessSubData(pTq, pHandle, pRsp, totalRows, pRequest, rawList, submit.ver);
    pReader->nextBlk++;
  }

END:
  tDecoderClear(&decoder);
  tqReaderClearSubmitMsg(pReader);
  taosArrayDestroy(rawList);
  if (code != 0){
    tqError("%s failed at %d, failed to scan log:%s", __FUNCTION__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tqInitTaosxRsp(SMqDataRsp* pRsp, STqOffsetVal pOffset) {
  int32_t code = TDB_CODE_SUCCESS;
  int32_t lino = 0;
  tqDebug("%s called", __FUNCTION__);
  TSDB_CHECK_NULL(pRsp, code, lino, END, TSDB_CODE_INVALID_PARA);
  tOffsetCopy(&pRsp->reqOffset, &pOffset);
  tOffsetCopy(&pRsp->rspOffset, &pOffset);

  pRsp->withTbName = 1;
  pRsp->withSchema = 1;
  pRsp->blockData = taosArrayInit(0, sizeof(void*));
  TSDB_CHECK_NULL(pRsp->blockData, code, lino, END, terrno);

  pRsp->blockDataLen = taosArrayInit(0, sizeof(int32_t));
  TSDB_CHECK_NULL(pRsp->blockDataLen, code, lino, END, terrno);

  pRsp->blockTbName = taosArrayInit(0, sizeof(void*));
  TSDB_CHECK_NULL(pRsp->blockTbName, code, lino, END, terrno);

  pRsp->blockSchema = taosArrayInit(0, sizeof(void*));
  TSDB_CHECK_NULL(pRsp->blockSchema, code, lino, END, terrno);

END:
  if (code != 0) {
    tqError("%s failed at:%d, code:%s", __FUNCTION__, lino, tstrerror(code));
    taosArrayDestroy(pRsp->blockData);
    taosArrayDestroy(pRsp->blockDataLen);
    taosArrayDestroy(pRsp->blockTbName);
    taosArrayDestroy(pRsp->blockSchema);
  }
  return code;
}

static int32_t tqExtractResetOffsetVal(STqOffsetVal* pOffsetVal, STQ* pTq, STqHandle* pHandle, const SMqPollReq* pRequest,
                                     SRpcMsg* pMsg, bool* pBlockReturned) {
  if (pOffsetVal == NULL || pTq == NULL || pHandle == NULL || pRequest == NULL || pMsg == NULL || pBlockReturned == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  uint64_t   consumerId = pRequest->consumerId;
  STqOffset* pOffset = NULL;
  int32_t    code = tqMetaGetOffset(pTq, pRequest->subKey, &pOffset);
  int32_t    vgId = TD_VID(pTq->pVnode);

  *pBlockReturned = false;
  // In this vnode, data has been polled by consumer for this topic, so let's continue from the last offset value.
  if (code == 0) {
    tOffsetCopy(pOffsetVal, &pOffset->val);

    char formatBuf[TSDB_OFFSET_LEN] = {0};
    tFormatOffset(formatBuf, TSDB_OFFSET_LEN, pOffsetVal);
    tqDebug("tmq poll: consumer:0x%" PRIx64
                ", subkey %s, vgId:%d, existed offset found, offset reset to %s and continue.QID:0x%" PRIx64,
            consumerId, pHandle->subKey, vgId, formatBuf, pRequest->reqId);
    return 0;
  } else {
    // no poll occurs in this vnode for this topic, let's seek to the right offset value.
    if (pRequest->reqOffset.type == TMQ_OFFSET__RESET_EARLIEST) {
      if (pRequest->useSnapshot) {
        tqDebug("tmq poll: consumer:0x%" PRIx64 ", subkey:%s, vgId:%d, (earliest) set offset to be snapshot",
                consumerId, pHandle->subKey, vgId);
        if (pHandle->fetchMeta) {
          tqOffsetResetToMeta(pOffsetVal, 0);
        } else {
          SValue val = {0};
          tqOffsetResetToData(pOffsetVal, 0, 0, val);
        }
      } else {
        walRefFirstVer(pTq->pVnode->pWal, pHandle->pRef);
        tqOffsetResetToLog(pOffsetVal, pHandle->pRef->refVer);
      }
    } else if (pRequest->reqOffset.type == TMQ_OFFSET__RESET_LATEST) {
      walRefLastVer(pTq->pVnode->pWal, pHandle->pRef);
      SMqDataRsp dataRsp = {0};
      tqOffsetResetToLog(pOffsetVal, pHandle->pRef->refVer + 1);

      code = tqInitDataRsp(&dataRsp, *pOffsetVal);
      if (code != 0) {
        return code;
      }
      tqDebug("tmq poll: consumer:0x%" PRIx64 ", subkey %s, vgId:%d, (latest) offset reset to %" PRId64, consumerId,
              pHandle->subKey, vgId, dataRsp.rspOffset.version);
      code = tqSendDataRsp(pHandle, pMsg, pRequest, &dataRsp, (pRequest->rawData == 1) ? TMQ_MSG_TYPE__POLL_RAW_DATA_RSP : TMQ_MSG_TYPE__POLL_DATA_RSP, vgId);
      tDeleteMqDataRsp(&dataRsp);

      *pBlockReturned = true;
      return code;
    } else if (pRequest->reqOffset.type == TMQ_OFFSET__RESET_NONE) {
      tqError("tmq poll: subkey:%s, no offset committed for consumer:0x%" PRIx64
                  " in vg %d, subkey %s, reset none failed",
              pHandle->subKey, consumerId, vgId, pRequest->subKey);
      return TSDB_CODE_TQ_NO_COMMITTED_OFFSET;
    }
  }

  return 0;
}

static int32_t tqExtractDataAndRspForNormalSubscribe(STQ* pTq, STqHandle* pHandle, const SMqPollReq* pRequest,
                                                   SRpcMsg* pMsg, STqOffsetVal* pOffset) {
  int32_t    code = TDB_CODE_SUCCESS;
  int32_t    lino = 0;
  tqDebug("%s called", __FUNCTION__ );
  uint64_t consumerId = pRequest->consumerId;
  int32_t  vgId = TD_VID(pTq->pVnode);
  terrno = 0;

  SMqDataRsp dataRsp = {0};
  code = tqInitDataRsp(&dataRsp, *pOffset);
  TSDB_CHECK_CODE(code, lino, end);

  code = qSetTaskId(pHandle->execHandle.task, consumerId, pRequest->reqId);
  TSDB_CHECK_CODE(code, lino, end);

  code = tqScanData(pTq, pHandle, &dataRsp, pOffset, pRequest);
  if (code != 0 && terrno != TSDB_CODE_WAL_LOG_NOT_EXIST) {
    goto end;
  }

  if (terrno == TSDB_CODE_TMQ_FETCH_TIMEOUT && dataRsp.blockNum == 0) {
    dataRsp.timeout = true;
  }
  
  // reqOffset represents the current date offset, may be changed if wal not exists
  tOffsetCopy(&dataRsp.reqOffset, pOffset);
  code = tqSendDataRsp(pHandle, pMsg, pRequest, &dataRsp, TMQ_MSG_TYPE__POLL_DATA_RSP, vgId);

end:
  {
    char buf[TSDB_OFFSET_LEN] = {0};
    tFormatOffset(buf, TSDB_OFFSET_LEN, &dataRsp.rspOffset);
    if (code != 0){
      tqError("tmq poll: consumer:0x%" PRIx64 ", subkey %s, vgId:%d, rsp block:%d, rsp offset type:%s, QID:0x%" PRIx64 " error msg:%s, line:%d",
              consumerId, pHandle->subKey, vgId, dataRsp.blockNum, buf, pRequest->reqId, tstrerror(code), lino);
    } else {
      tqDebug("tmq poll: consumer:0x%" PRIx64 ", subkey %s, vgId:%d, rsp block:%d, rsp offset type:%s, QID:0x%" PRIx64 " success",
              consumerId, pHandle->subKey, vgId, dataRsp.blockNum, buf, pRequest->reqId);
    }

    tDeleteMqDataRsp(&dataRsp);
    return code;
  }
}

#define PROCESS_EXCLUDED_MSG(TYPE, DECODE_FUNC, DELETE_FUNC)                                               \
  SDecoder decoder = {0};                                                                                  \
  TYPE     req = {0};                                                                                      \
  void*    data = POINTER_SHIFT(pHead->body, sizeof(SMsgHead));                                            \
  int32_t  len = pHead->bodyLen - sizeof(SMsgHead);                                                        \
  tDecoderInit(&decoder, data, len);                                                                       \
  if (DECODE_FUNC(&decoder, &req) == 0 && (req.source & TD_REQ_FROM_TAOX) != 0) {                          \
    tqDebug("tmq poll: consumer:0x%" PRIx64 " (epoch %d) iter log, jump meta for, vgId:%d offset %" PRId64 \
            " msgType %d",                                                                                 \
            pRequest->consumerId, pRequest->epoch, vgId, fetchVer, pHead->msgType);                        \
    fetchVer++;                                                                                            \
    DELETE_FUNC(&req);                                                                                     \
    tDecoderClear(&decoder);                                                                               \
    continue;                                                                                              \
  }                                                                                                        \
  DELETE_FUNC(&req);                                                                                       \
  tDecoderClear(&decoder);

static void tqDeleteCommon(void* parm) {}

#define POLL_RSP_TYPE(pRequest,taosxRsp) \
taosxRsp.createTableNum > 0 ? TMQ_MSG_TYPE__POLL_DATA_META_RSP : \
(pRequest->rawData == 1 ? TMQ_MSG_TYPE__POLL_RAW_DATA_RSP : TMQ_MSG_TYPE__POLL_DATA_RSP)

static int32_t tqBuildBatchMeta(SMqBatchMetaRsp *btMetaRsp, int16_t type, int32_t bodyLen, void* body){
  int32_t         code = 0;

  if (!btMetaRsp->batchMetaReq) {
    btMetaRsp->batchMetaReq = taosArrayInit(4, POINTER_BYTES);
    TQ_NULL_GO_TO_END(btMetaRsp->batchMetaReq);
    btMetaRsp->batchMetaLen = taosArrayInit(4, sizeof(int32_t));
    TQ_NULL_GO_TO_END(btMetaRsp->batchMetaLen);
  }

  SMqMetaRsp tmpMetaRsp = {0};
  tmpMetaRsp.resMsgType = type;
  tmpMetaRsp.metaRspLen = bodyLen;
  tmpMetaRsp.metaRsp = body;
  uint32_t len = 0;
  tEncodeSize(tEncodeMqMetaRsp, &tmpMetaRsp, len, code);
  if (TSDB_CODE_SUCCESS != code) {
    tqError("tmq extract meta from log, tEncodeMqMetaRsp error");
    goto END;
  }
  int32_t tLen = sizeof(SMqRspHead) + len;
  void*   tBuf = taosMemoryCalloc(1, tLen);
  TQ_NULL_GO_TO_END(tBuf);
  void*    metaBuff = POINTER_SHIFT(tBuf, sizeof(SMqRspHead));
  SEncoder encoder = {0};
  tEncoderInit(&encoder, metaBuff, len);
  code = tEncodeMqMetaRsp(&encoder, &tmpMetaRsp);
  tEncoderClear(&encoder);

  if (code < 0) {
    tqError("tmq extract meta from log, tEncodeMqMetaRsp error");
    goto END;
  }
  TQ_NULL_GO_TO_END (taosArrayPush(btMetaRsp->batchMetaReq, &tBuf));
  TQ_NULL_GO_TO_END (taosArrayPush(btMetaRsp->batchMetaLen, &tLen));

END:
  return code;
}

static int32_t tqBuildCreateTbBatchReqBinary(SMqDataRsp *taosxRsp, void** pBuf, int32_t *len){
  int32_t code = 0;
  SVCreateTbBatchReq pReq = {0};
  pReq.nReqs = taosArrayGetSize(taosxRsp->createTableReq);
  pReq.pArray = taosArrayInit(1, sizeof(struct SVCreateTbReq));
  TQ_NULL_GO_TO_END(pReq.pArray);
  for (int i = 0; i < taosArrayGetSize(taosxRsp->createTableReq); i++){
    void   *createTableReq = taosArrayGetP(taosxRsp->createTableReq, i);
    TQ_NULL_GO_TO_END(taosArrayPush(pReq.pArray, createTableReq));
  }
  tEncodeSize(tEncodeSVCreateTbBatchReq, &pReq, *len, code);
  if (code < 0) {
    goto END;
  }
  *len += sizeof(SMsgHead);
  *pBuf = taosMemoryMalloc(*len);
  TQ_NULL_GO_TO_END(pBuf);
  SEncoder coder = {0};
  tEncoderInit(&coder, POINTER_SHIFT(*pBuf, sizeof(SMsgHead)), *len);
  code = tEncodeSVCreateTbBatchReq(&coder, &pReq);
  tEncoderClear(&coder);

END:
  taosArrayDestroy(pReq.pArray);
  return code;
}

#define SEND_BATCH_META_RSP \
tqOffsetResetToLog(&btMetaRsp.rspOffset, fetchVer);\
code = tqSendBatchMetaPollRsp(pHandle, pMsg, pRequest, &btMetaRsp, vgId);\
goto END;

#define SEND_DATA_RSP \
tqOffsetResetToLog(&taosxRsp.rspOffset, fetchVer);\
code = tqSendDataRsp(pHandle, pMsg, pRequest, &taosxRsp, POLL_RSP_TYPE(pRequest, taosxRsp), vgId);\
goto END;

static int32_t tqExtractDataAndRspForDbStbSubscribe(STQ* pTq, STqHandle* pHandle, const SMqPollReq* pRequest,
                                                  SRpcMsg* pMsg, STqOffsetVal* offset) {
  int32_t         vgId = TD_VID(pTq->pVnode);
  SMqDataRsp      taosxRsp = {0};
  SMqBatchMetaRsp btMetaRsp = {0};
  int32_t         code = 0;

  TQ_ERR_GO_TO_END(tqInitTaosxRsp(&taosxRsp, *offset));
  if (offset->type != TMQ_OFFSET__LOG) {
    TQ_ERR_GO_TO_END(tqScanTaosx(pTq, pHandle, &taosxRsp, &btMetaRsp, offset, pRequest));

    if (taosArrayGetSize(btMetaRsp.batchMetaReq) > 0) {
      code = tqSendBatchMetaPollRsp(pHandle, pMsg, pRequest, &btMetaRsp, vgId);
      tqDebug("tmq poll: consumer:0x%" PRIx64 " subkey:%s vgId:%d, send meta offset type:%d,uid:%" PRId64 ",ts:%" PRId64,
              pRequest->consumerId, pHandle->subKey, vgId, btMetaRsp.rspOffset.type, btMetaRsp.rspOffset.uid,btMetaRsp.rspOffset.ts);
      goto END;
    }

    tqDebug("taosx poll: consumer:0x%" PRIx64 " subkey:%s vgId:%d, send data blockNum:%d, offset type:%d,uid:%" PRId64",ts:%" PRId64,
            pRequest->consumerId, pHandle->subKey, vgId, taosxRsp.blockNum, taosxRsp.rspOffset.type, taosxRsp.rspOffset.uid, taosxRsp.rspOffset.ts);
    if (taosxRsp.blockNum > 0) {
      code = tqSendDataRsp(pHandle, pMsg, pRequest, &taosxRsp, TMQ_MSG_TYPE__POLL_DATA_RSP, vgId);
      goto END;
    } else {
      tOffsetCopy(offset, &taosxRsp.rspOffset);
    }
  }

  if (offset->type == TMQ_OFFSET__LOG) {
    walReaderVerifyOffset(pHandle->pWalReader, offset);
    int64_t fetchVer = offset->version;

    uint64_t st = taosGetTimestampMs();
    int      totalRows = 0;
    int32_t  totalMetaRows = 0;
    while (1) {
      int32_t savedEpoch = atomic_load_32(&pHandle->epoch);
      if (savedEpoch > pRequest->epoch) {
        tqError("tmq poll: consumer:0x%" PRIx64 " (epoch %d) iter log, savedEpoch error, vgId:%d offset %" PRId64,
                pRequest->consumerId, pRequest->epoch, vgId, fetchVer);
        code = TSDB_CODE_TQ_INTERNAL_ERROR;
        goto END;
      }

      if (tqFetchLog(pTq, pHandle, &fetchVer, pRequest->reqId) < 0) {
        if (totalMetaRows > 0) {
          SEND_BATCH_META_RSP
        }
        SEND_DATA_RSP
      }

      SWalCont* pHead = &pHandle->pWalReader->pHead->head;
      tqDebug("tmq poll: consumer:0x%" PRIx64 " (epoch %d) iter log, vgId:%d offset %" PRId64 " msgType %s",
              pRequest->consumerId, pRequest->epoch, vgId, fetchVer, TMSG_INFO(pHead->msgType));

      // process meta
      if (pHead->msgType != TDMT_VND_SUBMIT) {
        if (totalRows > 0) {
          SEND_DATA_RSP
        }

        if ((pRequest->sourceExcluded & TD_REQ_FROM_TAOX) != 0) {
          if (pHead->msgType == TDMT_VND_CREATE_TABLE) {
            PROCESS_EXCLUDED_MSG(SVCreateTbBatchReq, tDecodeSVCreateTbBatchReq, tDeleteSVCreateTbBatchReq)
          } else if (pHead->msgType == TDMT_VND_ALTER_TABLE) {
            PROCESS_EXCLUDED_MSG(SVAlterTbReq, tDecodeSVAlterTbReq, destroyAlterTbReq)
          } else if (pHead->msgType == TDMT_VND_CREATE_STB || pHead->msgType == TDMT_VND_ALTER_STB) {
            PROCESS_EXCLUDED_MSG(SVCreateStbReq, tDecodeSVCreateStbReq, tqDeleteCommon)
          } else if (pHead->msgType == TDMT_VND_DELETE) {
            PROCESS_EXCLUDED_MSG(SDeleteRes, tDecodeDeleteRes, tqDeleteCommon)
          }
        }

        tqDebug("fetch meta msg, ver:%" PRId64 ", vgId:%d, type:%s, enable batch meta:%d", pHead->version, vgId,
                TMSG_INFO(pHead->msgType), pRequest->enableBatchMeta);
        if (!pRequest->enableBatchMeta && !pRequest->useSnapshot) {
          SMqMetaRsp metaRsp = {0};
          tqOffsetResetToLog(&metaRsp.rspOffset, fetchVer + 1);
          metaRsp.resMsgType = pHead->msgType;
          metaRsp.metaRspLen = pHead->bodyLen;
          metaRsp.metaRsp = pHead->body;
          code = tqSendMetaPollRsp(pHandle, pMsg, pRequest, &metaRsp, vgId);
          goto END;
        }
        code = tqBuildBatchMeta(&btMetaRsp, pHead->msgType, pHead->bodyLen, pHead->body);
        fetchVer++;
        if (code != 0){
          goto END;
        }
        totalMetaRows++;
        if ((taosArrayGetSize(btMetaRsp.batchMetaReq) >= pRequest->minPollRows) || (taosGetTimestampMs() - st > pRequest->timeout)) {
          SEND_BATCH_META_RSP
        }
        continue;
      }

      if (totalMetaRows > 0 && pHandle->fetchMeta != ONLY_META) {
        SEND_BATCH_META_RSP
      }

      // process data
      SPackedData submit = {
          .msgStr = POINTER_SHIFT(pHead->body, sizeof(SSubmitReq2Msg)),
          .msgLen = pHead->bodyLen - sizeof(SSubmitReq2Msg),
          .ver = pHead->version,
      };

      TQ_ERR_GO_TO_END(tqTaosxScanLog(pTq, pHandle, submit, &taosxRsp, &totalRows, pRequest));

      if (pHandle->fetchMeta == ONLY_META && taosArrayGetSize(taosxRsp.createTableReq) > 0){
        int32_t len = 0;
        void *pBuf = NULL;
        code = tqBuildCreateTbBatchReqBinary(&taosxRsp, &pBuf, &len);
        if (code == 0){
          code = tqBuildBatchMeta(&btMetaRsp, TDMT_VND_CREATE_TABLE, len, pBuf);
        }
        taosMemoryFree(pBuf);
        for (int i = 0; i < taosArrayGetSize(taosxRsp.createTableReq); i++) {
          void* pCreateTbReq = taosArrayGetP(taosxRsp.createTableReq, i);
          if (pCreateTbReq != NULL) {
            tDestroySVSubmitCreateTbReq(pCreateTbReq, TSDB_MSG_FLG_DECODE);
          }
          taosMemoryFree(pCreateTbReq);
        }
        taosArrayDestroy(taosxRsp.createTableReq);
        taosxRsp.createTableReq = NULL;
        fetchVer++;
        if (code != 0){
          goto END;
        }
        totalMetaRows++;
        if ((taosArrayGetSize(btMetaRsp.batchMetaReq) >= pRequest->minPollRows) ||
            (taosGetTimestampMs() - st > pRequest->timeout) ||
            (!pRequest->enableBatchMeta && !pRequest->useSnapshot)) {
          SEND_BATCH_META_RSP
        }
        continue;
      }

      if ((pRequest->rawData == 0 && totalRows >= pRequest->minPollRows) ||
          (taosGetTimestampMs() - st > pRequest->timeout) ||
          (pRequest->rawData != 0 && (taosArrayGetSize(taosxRsp.blockData) > pRequest->minPollRows ||
                                      terrno == TSDB_CODE_TMQ_RAW_DATA_SPLIT))) {
        if (terrno == TSDB_CODE_TMQ_RAW_DATA_SPLIT){
          terrno = 0;
        } else{
          fetchVer++;
        }
        SEND_DATA_RSP
      } else {
        fetchVer++;
      }
    }
  }

END:
  if (code != 0){
    tqError("tmq poll: tqTaosxScanLog error. consumerId:0x%" PRIx64 ", in vgId:%d, subkey %s", pRequest->consumerId, vgId,
            pRequest->subKey);
  }
  tDeleteMqBatchMetaRsp(&btMetaRsp);
  tDeleteSTaosxRsp(&taosxRsp);
  return code;
}

int32_t tqExtractDataForMq(STQ* pTq, STqHandle* pHandle, const SMqPollReq* pRequest, SRpcMsg* pMsg) {
  if (pTq == NULL || pHandle == NULL || pRequest == NULL || pMsg == NULL) {
    return TSDB_CODE_TMQ_INVALID_MSG;
  }
  int32_t      code = 0;
  STqOffsetVal reqOffset = {0};
  tOffsetCopy(&reqOffset, &pRequest->reqOffset);

  // reset the offset if needed
  if (IS_OFFSET_RESET_TYPE(pRequest->reqOffset.type)) {
    bool blockReturned = false;
    code = tqExtractResetOffsetVal(&reqOffset, pTq, pHandle, pRequest, pMsg, &blockReturned);
    if (code != 0) {
      goto END;
    }

    // empty block returned, quit
    if (blockReturned) {
      goto END;
    }
  } else if (reqOffset.type == 0) {  // use the consumer specified offset
    uError("req offset type is 0");
    code = TSDB_CODE_TMQ_INVALID_MSG;
    goto END;
  }

  if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
    code = tqExtractDataAndRspForNormalSubscribe(pTq, pHandle, pRequest, pMsg, &reqOffset);
  } else {
    code = tqExtractDataAndRspForDbStbSubscribe(pTq, pHandle, pRequest, pMsg, &reqOffset);
  }

END:
  if (code != 0){
    uError("failed to extract data for mq, msg:%s", tstrerror(code));
  }
  tOffsetDestroy(&reqOffset);
  return code;
}

static int32_t tqSendBatchMetaPollRsp(STqHandle* pHandle, const SRpcMsg* pMsg, const SMqPollReq* pReq,
                               const SMqBatchMetaRsp* pRsp, int32_t vgId) {
  if (pHandle == NULL || pMsg == NULL || pReq == NULL || pRsp == NULL) {
    return TSDB_CODE_TMQ_INVALID_MSG;
  }
  int32_t len = 0;
  int32_t code = 0;
  tEncodeSize(tEncodeMqBatchMetaRsp, pRsp, len, code);
  if (code < 0) {
    return TAOS_GET_TERRNO(code);
  }
  int32_t tlen = sizeof(SMqRspHead) + len;
  void*   buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    return TAOS_GET_TERRNO(terrno);
  }

  int64_t sver = 0, ever = 0;
  walReaderValidVersionRange(pHandle->execHandle.pTqReader->pWalReader, &sver, &ever);
  tqInitMqRspHead(buf, TMQ_MSG_TYPE__POLL_BATCH_META_RSP, pReq->epoch, pReq->consumerId, sver, ever);

  void* abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));

  SEncoder encoder = {0};
  tEncoderInit(&encoder, abuf, len);
  code = tEncodeMqBatchMetaRsp(&encoder, pRsp);
  tEncoderClear(&encoder);
  if (code < 0) {
    rpcFreeCont(buf);
    return TAOS_GET_TERRNO(code);
  }
  SRpcMsg resp = {.info = pMsg->info, .pCont = buf, .contLen = tlen, .code = 0};

  tmsgSendRsp(&resp);
  tqDebug("vgId:%d, from consumer:0x%" PRIx64 " (epoch %d) send rsp, res msg type: batch meta, size:%ld offset type:%d",
          vgId, pReq->consumerId, pReq->epoch, taosArrayGetSize(pRsp->batchMetaReq), pRsp->rspOffset.type);

  return 0;
}

static int32_t tqSendMetaPollRsp(STqHandle* pHandle, const SRpcMsg* pMsg, const SMqPollReq* pReq, const SMqMetaRsp* pRsp,
                          int32_t vgId) {
  if (pHandle == NULL || pMsg == NULL || pReq == NULL || pRsp == NULL) {
    return TSDB_CODE_TMQ_INVALID_MSG;
  }
  int32_t len = 0;
  int32_t code = 0;
  tEncodeSize(tEncodeMqMetaRsp, pRsp, len, code);
  if (code < 0) {
    return TAOS_GET_TERRNO(code);
  }
  int32_t tlen = sizeof(SMqRspHead) + len;
  void*   buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    return TAOS_GET_TERRNO(TSDB_CODE_OUT_OF_MEMORY);
  }

  int64_t sver = 0, ever = 0;
  walReaderValidVersionRange(pHandle->execHandle.pTqReader->pWalReader, &sver, &ever);
  tqInitMqRspHead(buf, TMQ_MSG_TYPE__POLL_META_RSP, pReq->epoch, pReq->consumerId, sver, ever);

  void* abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));

  SEncoder encoder = {0};
  tEncoderInit(&encoder, abuf, len);
  code = tEncodeMqMetaRsp(&encoder, pRsp);
  tEncoderClear(&encoder);
  if (code < 0) {
    rpcFreeCont(buf);
    return TAOS_GET_TERRNO(code);
  }

  SRpcMsg resp = {.info = pMsg->info, .pCont = buf, .contLen = tlen, .code = 0};

  tmsgSendRsp(&resp);
  tqDebug("vgId:%d, from consumer:0x%" PRIx64 " (epoch %d) send rsp, res msg type %d, offset type:%d", vgId,
          pReq->consumerId, pReq->epoch, pRsp->resMsgType, pRsp->rspOffset.type);

  return 0;
}


