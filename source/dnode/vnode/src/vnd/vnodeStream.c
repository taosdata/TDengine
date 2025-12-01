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

#include <stdbool.h>
#include <stdint.h>
#include <taos.h>
#include <tdef.h>
#include "executor.h"
#include "nodes.h"
#include "osMemPool.h"
#include "osMemory.h"
#include "scalar.h"
#include "streamReader.h"
#include "taosdef.h"
#include "taoserror.h"
#include "tarray.h"
#include "tcommon.h"
#include "tdatablock.h"
#include "tdb.h"
#include "tdef.h"
#include "tencode.h"
#include "tglobal.h"
#include "thash.h"
#include "tlist.h"
#include "tmsg.h"
#include "tsimplehash.h"
#include "vnd.h"
#include "vnode.h"
#include "vnodeInt.h"

static int32_t cacheTag(SVnode* pVnode, SHashObj* metaCache, SExprInfo* pExprInfo, int32_t numOfExpr, SStorageAPI* api, uint64_t uid);

#define BUILD_OPTION(options, _suid, _ver, _order, startTime, endTime, _schemas, _isSchema, _pSlotList)      \
  SStreamOptions                       options = {.suid = _suid,                                                   \
                                                  .ver = _ver,                                                     \
                                                  .order = _order,                                                 \
                                                  .twindows = {.skey = startTime, .ekey = endTime},                \
                                                  .schemas = _schemas,                                             \
                                                  .isSchema = _isSchema,                                           \
                                                  .pSlotList = _pSlotList};

typedef struct WalMetaResult {
  uint64_t    id;
  int64_t     skey;
  int64_t     ekey;
} WalMetaResult;

static int64_t getSuid(SStreamTriggerReaderInfo* sStreamReaderInfo, STableKeyInfo* pList) {
  int64_t suid = 0;
  if (!sStreamReaderInfo->isVtableStream) {
    suid = sStreamReaderInfo->suid;
    goto end;
  }

  if (pList == NULL) {
    goto end;
  }

  taosRLockLatch(&sStreamReaderInfo->lock);
  SStreamTableMapElement* element = taosHashGet(sStreamReaderInfo->vSetTableList.uIdMap, &pList->uid, LONG_BYTES);  
  if (element != 0) {
    suid = element->table->groupId;
    taosRUnLockLatch(&sStreamReaderInfo->lock);
    goto end;
  }
  taosRUnLockLatch(&sStreamReaderInfo->lock);

end:
  return suid;
}

static int64_t getSessionKey(int64_t session, int64_t type) { return (session | (type << 32)); }

int32_t sortCid(const void *lp, const void *rp) {
  int16_t* c1 = (int16_t*)lp;
  int16_t* c2 = (int16_t*)rp;

  if (*c1 < *c2) {
    return -1;
  } else if (*c1 > *c2) {
    return 1;
  }

  return 0;
}

int32_t sortSSchema(const void *lp, const void *rp) {
  SSchema* c1 = (SSchema*)lp;
  SSchema* c2 = (SSchema*)rp;

  if (c1->colId < c2->colId) {
    return -1;
  } else if (c1->colId > c2->colId) {
    return 1;
  }

  return 0;
}

static int32_t addColData(SSDataBlock* pResBlock, int32_t index, void* data) {
  SColumnInfoData* pSrc = taosArrayGet(pResBlock->pDataBlock, index);
  if (pSrc == NULL) {
    return terrno;
  }

  memcpy(pSrc->pData + pResBlock->info.rows * pSrc->info.bytes, data, pSrc->info.bytes);
  return 0;
}

static int32_t getTableDataInfo(SStreamReaderTaskInner* pTask, bool* hasNext) {
  int32_t code = pTask->storageApi->tsdReader.tsdNextDataBlock(pTask->pReader, hasNext);
  if (code != TSDB_CODE_SUCCESS) {
    pTask->storageApi->tsdReader.tsdReaderReleaseDataBlock(pTask->pReader);
  }

  return code;
}

static int32_t getTableData(SStreamReaderTaskInner* pTask, SSDataBlock** ppRes) {
  return pTask->storageApi->tsdReader.tsdReaderRetrieveDataBlock(pTask->pReader, ppRes);
}

static int32_t buildOTableInfoRsp(const SSTriggerOrigTableInfoRsp* rsp, void** data, size_t* size) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  int32_t len = tSerializeSTriggerOrigTableInfoRsp(NULL, 0, rsp);
  STREAM_CHECK_CONDITION_GOTO(len <= 0, TSDB_CODE_INVALID_PARA);
  buf = rpcMallocCont(len);
  STREAM_CHECK_NULL_GOTO(buf, terrno);
  int32_t actLen = tSerializeSTriggerOrigTableInfoRsp(buf, len, rsp);
  STREAM_CHECK_CONDITION_GOTO(actLen != len, TSDB_CODE_INVALID_PARA);
  *data = buf;
  *size = len;
  buf = NULL;
end:
  rpcFreeCont(buf);
  return code;
}

static bool needReLoadTableList(SStreamTriggerReaderInfo* sStreamReaderInfo, int8_t tableType, int64_t suid, int64_t uid, bool isCalc){
  if ((tableType == TD_CHILD_TABLE || tableType == TD_VIRTUAL_CHILD_TABLE) &&
      sStreamReaderInfo->tableType == TD_SUPER_TABLE && 
      suid == sStreamReaderInfo->suid) {
    taosRLockLatch(&sStreamReaderInfo->lock);
    uint64_t gid = qStreamGetGroupIdFromOrigin(sStreamReaderInfo, uid);
    taosRUnLockLatch(&sStreamReaderInfo->lock);
    if (gid == (uint64_t)-1) return true;
  }
  return false;
}

static bool uidInTableList(SStreamTriggerReaderInfo* sStreamReaderInfo, int64_t suid, int64_t uid, uint64_t* id){
  int32_t  ret = false;
  if (sStreamReaderInfo->tableType == TD_SUPER_TABLE) {
    if (suid != sStreamReaderInfo->suid) goto end;
    if (qStreamGetTableListNum(sStreamReaderInfo) == 0) goto end;
    if (sStreamReaderInfo->pTagCond == NULL) {
      if (sStreamReaderInfo->partitionCols == NULL){
        *id = 0;
      } else if (sStreamReaderInfo->groupByTbname){
        *id= uid;
      } else {
        *id = qStreamGetGroupIdFromOrigin(sStreamReaderInfo, uid);
        if (*id == -1) goto end;
      }
    } else {
      //*id= uid;
      *id = qStreamGetGroupIdFromOrigin(sStreamReaderInfo, uid);
      if (*id == -1) goto end;
    }
  } else {
    *id = qStreamGetGroupIdFromOrigin(sStreamReaderInfo, uid);
    if (*id == -1) goto end;
  }
  ret = true;

end:
  stTrace("%s ret:%d check suid:%" PRId64 " uid:%" PRId64 " gid:%"PRIu64, __func__, ret, suid, uid, *id);
  return ret;
}

static bool uidInTableListOrigin(SStreamTriggerReaderInfo* sStreamReaderInfo, int64_t suid, int64_t uid, uint64_t* id) {
  return uidInTableList(sStreamReaderInfo, suid, uid, id);
}

static bool uidInTableListSet(SStreamTriggerReaderInfo* sStreamReaderInfo, int64_t suid, int64_t uid, uint64_t* id, bool isCalc) {
  bool ret = false;
  taosRLockLatch(&sStreamReaderInfo->lock);
  if (sStreamReaderInfo->isVtableStream) {
    int64_t tmp[2] = {suid, uid};
    if(tSimpleHashGet(isCalc ? sStreamReaderInfo->uidHashCalc : sStreamReaderInfo->uidHashTrigger, tmp, sizeof(tmp)) != NULL) {
      *id = uid;
      ret = true;
    }
  } else {
    ret = uidInTableList(sStreamReaderInfo, suid, uid, id);
  }

end:
  taosRUnLockLatch(&sStreamReaderInfo->lock);
  return ret;
}

static int32_t  qTransformStreamTableList(SStreamTriggerReaderInfo* sStreamReaderInfo, void* pTableListInfo, StreamTableListInfo* tableInfo){
  SArray* pList = qStreamGetTableListArray(pTableListInfo);
  int32_t totalSize = taosArrayGetSize(pList);
  int32_t code = 0;
  void* pTask = sStreamReaderInfo->pTask;
  for (int32_t i = 0; i < totalSize; ++i) {
    STableKeyInfo* info = taosArrayGet(pList, i);
    if (info == NULL) {
      continue;
    }
    code = cacheTag(sStreamReaderInfo->pVnode, sStreamReaderInfo->pTableMetaCacheTrigger, sStreamReaderInfo->pExprInfoTriggerTag, sStreamReaderInfo->numOfExprTriggerTag, &sStreamReaderInfo->storageApi, info->uid);
    if (code != 0){
      ST_TASK_WLOG("%s cacheTag trigger failed for uid:%" PRId64",code:%d", __func__, info->uid, code);
      continue;
    }
    code = cacheTag(sStreamReaderInfo->pVnode, sStreamReaderInfo->pTableMetaCacheCalc, sStreamReaderInfo->pExprInfoCalcTag, sStreamReaderInfo->numOfExprCalcTag, &sStreamReaderInfo->storageApi, info->uid);
    if (code != 0){
      ST_TASK_WLOG("%s cacheTag calc failed for uid:%" PRId64",code:%d", __func__, info->uid, code);
      continue;
    }
    code = qStreamSetTableList(tableInfo, info->uid, info->groupId);
    if (code != 0){
      return code;
    }
  }
  return 0;
}

static int32_t generateTablistForStreamReader(SVnode* pVnode, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t                   code = 0;
  int32_t                   lino = 0;
  SNodeList* groupNew = NULL;   
  void* pTableListInfo = NULL;

  
  STREAM_CHECK_RET_GOTO(nodesCloneList(sStreamReaderInfo->partitionCols, &groupNew));

  STREAM_CHECK_RET_GOTO(qStreamCreateTableListForReader(pVnode, sStreamReaderInfo->suid, sStreamReaderInfo->uid, sStreamReaderInfo->tableType, groupNew,
                                         true, sStreamReaderInfo->pTagCond, sStreamReaderInfo->pTagIndexCond, &sStreamReaderInfo->storageApi, 
                                         &pTableListInfo, sStreamReaderInfo->groupIdMap));
  
  STREAM_CHECK_RET_GOTO(qTransformStreamTableList(sStreamReaderInfo, pTableListInfo, &sStreamReaderInfo->tableList));
  
  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s tablelist size:%" PRIzu, TD_VID(pVnode), __func__, taosArrayGetSize(sStreamReaderInfo->tableList.pTableList));
end:
  nodesDestroyList(groupNew);
  qStreamDestroyTableList(pTableListInfo);
  STREAM_PRINT_LOG_END(code, lino);
  return code;
}

static int32_t buildVTableInfoRsp(const SStreamMsgVTableInfo* rsp, void** data, size_t* size) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  int32_t len = tSerializeSStreamMsgVTableInfo(NULL, 0, rsp);
  STREAM_CHECK_CONDITION_GOTO(len <= 0, TSDB_CODE_INVALID_PARA);
  buf = rpcMallocCont(len);
  STREAM_CHECK_NULL_GOTO(buf, terrno);
  int32_t actLen = tSerializeSStreamMsgVTableInfo(buf, len, rsp);
  STREAM_CHECK_CONDITION_GOTO(actLen != len, TSDB_CODE_INVALID_PARA);
  *data = buf;
  *size = len;
  buf = NULL;
end:
  rpcFreeCont(buf);
  return code;
}

static int32_t buildTsRsp(const SStreamTsResponse* tsRsp, void** data, size_t* size) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  int32_t len = tSerializeSStreamTsResponse(NULL, 0, tsRsp);
  STREAM_CHECK_CONDITION_GOTO(len <= 0, TSDB_CODE_INVALID_PARA);
  buf = rpcMallocCont(len);
  STREAM_CHECK_NULL_GOTO(buf, terrno);
  int32_t actLen = tSerializeSStreamTsResponse(buf, len, tsRsp);
  STREAM_CHECK_CONDITION_GOTO(actLen != len, TSDB_CODE_INVALID_PARA);
  *data = buf;
  *size = len;
  buf = NULL;
end:
  rpcFreeCont(buf);
  return code;
}


static int32_t buildRsp(SSDataBlock* pBlock, void** data, size_t* size) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  STREAM_CHECK_CONDITION_GOTO(pBlock == NULL || pBlock->info.rows == 0, TSDB_CODE_SUCCESS);
  size_t dataEncodeSize = blockGetEncodeSize(pBlock);
  buf = rpcMallocCont(dataEncodeSize);
  STREAM_CHECK_NULL_GOTO(buf, terrno);
  int32_t actualLen = blockEncode(pBlock, buf, dataEncodeSize, taosArrayGetSize(pBlock->pDataBlock));
  STREAM_CHECK_CONDITION_GOTO(actualLen < 0, terrno);
  *data = buf;
  *size = dataEncodeSize;
  buf = NULL;
end:
  rpcFreeCont(buf);
  return code;
}

static int32_t buildArrayRsp(SArray* pBlockList, void** data, size_t* size) {
  int32_t code = 0;
  int32_t lino = 0;

  void*   buf = NULL;

  int32_t blockNum = 0;
  size_t  dataEncodeBufSize = 0;
  for(size_t i = 0; i < taosArrayGetSize(pBlockList); i++){
    SSDataBlock* pBlock = taosArrayGetP(pBlockList, i);
    if (pBlock == NULL || pBlock->info.rows == 0) continue;
    int32_t blockSize = blockGetEncodeSize(pBlock);
    dataEncodeBufSize += blockSize;
    blockNum++;
  }
  buf = rpcMallocCont(INT_BYTES + dataEncodeBufSize);
  STREAM_CHECK_NULL_GOTO(buf, terrno);

  char* dataBuf = (char*)buf;
  *((int32_t*)(dataBuf)) = blockNum;
  dataBuf += INT_BYTES;
  for(size_t i = 0; i < taosArrayGetSize(pBlockList); i++){
    SSDataBlock* pBlock = taosArrayGetP(pBlockList, i);
    if (pBlock == NULL || pBlock->info.rows == 0) continue;
    int32_t actualLen = blockEncode(pBlock, dataBuf, dataEncodeBufSize, taosArrayGetSize(pBlock->pDataBlock));
    STREAM_CHECK_CONDITION_GOTO(actualLen < 0, terrno);
    dataBuf += actualLen;
  }
  *data = buf;
  *size = INT_BYTES + dataEncodeBufSize;
  buf = NULL;
end:
  rpcFreeCont(buf);
  return code;
}

static int32_t buildWalMetaBlock(SSDataBlock* pBlock, int8_t type, int64_t id, bool isVTable, int64_t uid,
                                 int64_t skey, int64_t ekey, int64_t ver, int64_t rows) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t index = 0;
  STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &type));
  if (!isVTable) {
    STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &id));
  }
  STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &uid));
  STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &skey));
  STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &ekey));
  STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &ver));
  STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &rows));

end:
  // STREAM_PRINT_LOG_END(code, lino)
  return code;
}

static int32_t buildWalMetaBlockNew(SSDataBlock* pBlock, int64_t id, int64_t skey, int64_t ekey, int64_t ver) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t index = 0;
  STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &id));
  STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &skey));
  STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &ekey));
  STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &ver));

end:
  return code;
}

static int32_t buildDropTableBlock(SSDataBlock* pBlock, int64_t id, int64_t ver) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t index = 0;
  STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &id));
  STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &ver));

end:
  return code;
}

static void buildTSchema(STSchema* pTSchema, int32_t ver, col_id_t colId, int8_t type, int32_t bytes) {
  pTSchema->numOfCols = 1;
  pTSchema->version = ver;
  pTSchema->columns[0].colId = colId;
  pTSchema->columns[0].type = type;
  pTSchema->columns[0].bytes = bytes;
}

static int32_t scanDeleteDataNew(SStreamTriggerReaderInfo* sStreamReaderInfo, SSTriggerWalNewRsp* rsp, void* data, int32_t len,
                              int64_t ver) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SDecoder   decoder = {0};
  SDeleteRes req = {0};
  void* pTask = sStreamReaderInfo->pTask;

  req.uidList = taosArrayInit(0, sizeof(tb_uid_t));
  tDecoderInit(&decoder, data, len);
  STREAM_CHECK_RET_GOTO(tDecodeDeleteRes(&decoder, &req));
  STREAM_CHECK_CONDITION_GOTO((sStreamReaderInfo->tableType == TSDB_SUPER_TABLE && !sStreamReaderInfo->isVtableStream && req.suid != sStreamReaderInfo->suid), TDB_CODE_SUCCESS);
  
  for (int32_t i = 0; i < taosArrayGetSize(req.uidList); i++) {
    uint64_t* uid = taosArrayGet(req.uidList, i);
    STREAM_CHECK_NULL_GOTO(uid, terrno);
    uint64_t   id = 0;
    ST_TASK_DLOG("stream reader scan delete start data:uid %" PRIu64 ", skey %" PRIu64 ", ekey %" PRIu64, *uid, req.skey, req.ekey);
    STREAM_CHECK_CONDITION_GOTO(!uidInTableListSet(sStreamReaderInfo, req.suid, *uid, &id, false), TDB_CODE_SUCCESS);
    STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(rsp->deleteBlock, ((SSDataBlock*)rsp->deleteBlock)->info.rows + 1));
    STREAM_CHECK_RET_GOTO(buildWalMetaBlockNew(rsp->deleteBlock, id, req.skey, req.ekey, ver));
    ((SSDataBlock*)rsp->deleteBlock)->info.rows++;
    rsp->totalRows++;
  }

end:
  taosArrayDestroy(req.uidList);
  tDecoderClear(&decoder);
  return code;
}

static int32_t scanDropTableNew(SStreamTriggerReaderInfo* sStreamReaderInfo, SSTriggerWalNewRsp* rsp, void* data, int32_t len,
                             int64_t ver) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SDecoder decoder = {0};
  void* pTask = sStreamReaderInfo->pTask;

  SVDropTbBatchReq req = {0};
  tDecoderInit(&decoder, data, len);
  STREAM_CHECK_RET_GOTO(tDecodeSVDropTbBatchReq(&decoder, &req));

  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    SVDropTbReq* pDropTbReq = req.pReqs + iReq;
    STREAM_CHECK_NULL_GOTO(pDropTbReq, TSDB_CODE_INVALID_PARA);
    uint64_t id = 0;
    if(!uidInTableListOrigin(sStreamReaderInfo, pDropTbReq->suid, pDropTbReq->uid, &id)) {
      continue;
    }

    STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(rsp->dropBlock, ((SSDataBlock*)rsp->dropBlock)->info.rows + 1));
    STREAM_CHECK_RET_GOTO(buildDropTableBlock(rsp->dropBlock, id, ver));
    ((SSDataBlock*)rsp->dropBlock)->info.rows++;
    rsp->totalRows++;
    ST_TASK_DLOG("stream reader scan drop uid %" PRId64 ", id %" PRIu64, pDropTbReq->uid, id);
  }

end:
  tDecoderClear(&decoder);
  return code;
}

static int32_t qStreamModifyTableList(SStreamTriggerReaderInfo* sStreamReaderInfo, SArray* tableListAdd, SArray* tableListDel) {
  int32_t      code = 0;
  int32_t      lino = 0;
  void* pTask = sStreamReaderInfo->pTask;
  
  taosWLockLatch(&sStreamReaderInfo->lock);
  int32_t totalSize = taosArrayGetSize(tableListDel);
  for (int32_t i = 0; i < totalSize; ++i) {
    int64_t* uid = taosArrayGet(tableListDel, i);
    if (uid == NULL) {
      continue;
    }
    STREAM_CHECK_RET_GOTO(qStreamRemoveTableList(&sStreamReaderInfo->tableList, *uid));
  }

  totalSize = taosArrayGetSize(tableListAdd);
  for (int32_t i = 0; i < totalSize; ++i) {
    STableKeyInfo* info = taosArrayGet(tableListAdd, i);
    if (info == NULL) {
      continue;
    }
    int ret = cacheTag(sStreamReaderInfo->pVnode, sStreamReaderInfo->pTableMetaCacheTrigger, sStreamReaderInfo->pExprInfoTriggerTag, sStreamReaderInfo->numOfExprTriggerTag, &sStreamReaderInfo->storageApi, info->uid);
    if (ret != 0){
      ST_TASK_WLOG("%s cacheTag trigger failed for uid:%" PRId64",code:%d", __func__, info->uid, ret);
      continue;
    }
    ret = cacheTag(sStreamReaderInfo->pVnode, sStreamReaderInfo->pTableMetaCacheCalc, sStreamReaderInfo->pExprInfoCalcTag, sStreamReaderInfo->numOfExprCalcTag, &sStreamReaderInfo->storageApi, info->uid);
    if (ret != 0){
      ST_TASK_WLOG("%s cacheTag calc failed for uid:%" PRId64",code:%d", __func__, info->uid, ret);
      continue;
    }
    STREAM_CHECK_RET_GOTO(qStreamRemoveTableList(&sStreamReaderInfo->tableList, info->uid));
    STREAM_CHECK_RET_GOTO(qStreamSetTableList(&sStreamReaderInfo->tableList, info->uid, info->groupId));
  }

end:
  taosWUnLockLatch(&sStreamReaderInfo->lock);
  return code;
}

static int32_t reloadTableList(SStreamTriggerReaderInfo* sStreamReaderInfo, SArray* uidList) {
  int32_t code = 0;
  int32_t lino = 0;
  SNodeList* groupNew = NULL;   
  SArray* tableList = NULL;  
  void* pTask = sStreamReaderInfo->pTask;

  if (taosArrayGetSize(uidList) == 0) {
    return 0;
  }
  STREAM_CHECK_RET_GOTO(nodesCloneList(sStreamReaderInfo->partitionCols, &groupNew));  
  STREAM_CHECK_RET_GOTO(qStreamFilterTableListForReader(sStreamReaderInfo->pVnode, uidList, groupNew, sStreamReaderInfo->pTagCond,
                                                    sStreamReaderInfo->pTagIndexCond, &sStreamReaderInfo->storageApi,
                                                    sStreamReaderInfo->groupIdMap, sStreamReaderInfo->suid, &tableList));
  STREAM_CHECK_RET_GOTO(qStreamModifyTableList(sStreamReaderInfo, tableList, uidList));
end:
  taosArrayDestroy(tableList);
  nodesDestroyList(groupNew);
  return code;
}

static int32_t scanCreateTableNew(SStreamTriggerReaderInfo* sStreamReaderInfo, void* data, int32_t len) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SDecoder decoder = {0};
  SArray*  uidList = NULL;
  void* pTask = sStreamReaderInfo->pTask;

  SVCreateTbBatchReq req = {0};
  tDecoderInit(&decoder, data, len);
  
  STREAM_CHECK_RET_GOTO(tDecodeSVCreateTbBatchReq(&decoder, &req));

  uidList = taosArrayInit(8, sizeof(tb_uid_t));
  STREAM_CHECK_NULL_GOTO(uidList, terrno);

  SVCreateTbReq* pCreateReq = NULL;
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    pCreateReq = req.pReqs + iReq;
    if (!needReLoadTableList(sStreamReaderInfo, pCreateReq->type, pCreateReq->ctb.suid, pCreateReq->uid, false)) {
      ST_TASK_DLOG("stream reader scan create table jump, %s", pCreateReq->name);
      continue;
    }
  
    ST_TASK_DLOG("stream reader scan create table %s", pCreateReq->name);  
    STREAM_CHECK_NULL_GOTO(taosArrayPush(uidList, &pCreateReq->uid), terrno);
  }
  
  STREAM_CHECK_RET_GOTO(reloadTableList(sStreamReaderInfo, uidList));
end:
  taosArrayDestroy(uidList);
  tDeleteSVCreateTbBatchReq(&req);
  tDecoderClear(&decoder);
  return code;
}

static int32_t processAutoCreateTableNew(SStreamTriggerReaderInfo* sStreamReaderInfo, SVCreateTbReq* pCreateReq) {
  int32_t  code = 0;
  int32_t  lino = 0;
  void*    pTask = sStreamReaderInfo->pTask;
  SArray*  uidList = NULL;

  if (!needReLoadTableList(sStreamReaderInfo, pCreateReq->type, pCreateReq->ctb.suid, pCreateReq->uid, false)) {
    ST_TASK_DLOG("stream reader scan auto create table jump, %s", pCreateReq->name);
    goto end;
  }
  uidList = taosArrayInit(8, sizeof(tb_uid_t));
  STREAM_CHECK_NULL_GOTO(uidList, terrno);
  STREAM_CHECK_NULL_GOTO(taosArrayPush(uidList, &pCreateReq->uid), terrno);
  ST_TASK_DLOG("stream reader scan auto create table %s", pCreateReq->name);

  STREAM_CHECK_RET_GOTO(reloadTableList(sStreamReaderInfo, uidList));
end:
  taosArrayDestroy(uidList);
  return code;
}

static int32_t scanAlterTableNew(SStreamTriggerReaderInfo* sStreamReaderInfo, void* data, int32_t len) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SDecoder decoder = {0};
  void* pTask = sStreamReaderInfo->pTask;
  SArray*  uidList = NULL;

  SVAlterTbReq req = {0};
  tDecoderInit(&decoder, data, len);
  
  STREAM_CHECK_RET_GOTO(tDecodeSVAlterTbReq(&decoder, &req));
  STREAM_CHECK_CONDITION_GOTO(req.action != TSDB_ALTER_TABLE_UPDATE_TAG_VAL && req.action != TSDB_ALTER_TABLE_UPDATE_MULTI_TAG_VAL, TDB_CODE_SUCCESS);

  ETableType tbType = 0;
  uint64_t suid = 0;
  STREAM_CHECK_RET_GOTO(metaGetTableTypeSuidByName(sStreamReaderInfo->pVnode, req.tbName, &tbType, &suid));
  STREAM_CHECK_CONDITION_GOTO(tbType != TSDB_CHILD_TABLE, TDB_CODE_SUCCESS);
  STREAM_CHECK_CONDITION_GOTO(suid != sStreamReaderInfo->suid, TDB_CODE_SUCCESS);

  uint64_t uid = 0;
  STREAM_CHECK_RET_GOTO(metaGetTableUidByName(sStreamReaderInfo->pVnode, req.tbName, &uid));

  uidList = taosArrayInit(8, sizeof(tb_uid_t));
  STREAM_CHECK_NULL_GOTO(uidList, terrno);
  STREAM_CHECK_NULL_GOTO(taosArrayPush(uidList, &uid), terrno);
  STREAM_CHECK_RET_GOTO(reloadTableList(sStreamReaderInfo, uidList));
  ST_TASK_DLOG("stream reader scan alter table %s", req.tbName);

end:
  taosArrayDestroy(uidList);
  taosArrayDestroy(req.pMultiTag);
  tDecoderClear(&decoder);
  return code;
}

// static int32_t scanAlterSTableNew(SStreamTriggerReaderInfo* sStreamReaderInfo, void* data, int32_t len) {
//   int32_t  code = 0;
//   int32_t  lino = 0;
//   SDecoder decoder = {0};
//   SMAlterStbReq reqAlter = {0};
//   SVCreateStbReq req = {0};
//   tDecoderInit(&decoder, data, len);
//   void* pTask = sStreamReaderInfo->pTask;
  
//   STREAM_CHECK_RET_GOTO(tDecodeSVCreateStbReq(&decoder, &req));
//   STREAM_CHECK_CONDITION_GOTO(req.suid != sStreamReaderInfo->suid, TDB_CODE_SUCCESS);
//   if (req.alterOriData != 0) {
//     STREAM_CHECK_RET_GOTO(tDeserializeSMAlterStbReq(req.alterOriData, req.alterOriDataLen, &reqAlter));
//     STREAM_CHECK_CONDITION_GOTO(reqAlter.alterType != TSDB_ALTER_TABLE_DROP_TAG && reqAlter.alterType != TSDB_ALTER_TABLE_UPDATE_TAG_NAME, TDB_CODE_SUCCESS);
//   }
  
//   STREAM_CHECK_RET_GOTO(reloadTableList(sStreamReaderInfo));

//   ST_TASK_DLOG("stream reader scan alter suid %" PRId64, req.suid);
// end:
//   tFreeSMAltertbReq(&reqAlter);
//   tDecoderClear(&decoder);
//   return code;
// }

// static int32_t scanDropSTableNew(SStreamTriggerReaderInfo* sStreamReaderInfo, void* data, int32_t len) {
//   int32_t  code = 0;
//   int32_t  lino = 0;
//   SDecoder decoder = {0};
//   void* pTask = sStreamReaderInfo->pTask;

//   SVDropStbReq req = {0};
//   tDecoderInit(&decoder, data, len);
//   STREAM_CHECK_RET_GOTO(tDecodeSVDropStbReq(&decoder, &req));
//   STREAM_CHECK_CONDITION_GOTO(req.suid != sStreamReaderInfo->suid, TDB_CODE_SUCCESS);

//   ST_TASK_DLOG("stream reader scan drop suid %" PRId64, req.suid);
// end:
//   tDecoderClear(&decoder);
//   return code;
// }

static int32_t scanSubmitTbDataForMeta(SDecoder *pCoder, SStreamTriggerReaderInfo* sStreamReaderInfo, SSHashObj* gidHash) {
  int32_t code = 0;
  int32_t lino = 0;
  WalMetaResult walMeta = {0};
  SSubmitTbData submitTbData = {0};
  
  if (tStartDecode(pCoder) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }

  uint8_t       version = 0;
  if (tDecodeI32v(pCoder, &submitTbData.flags) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }
  version = (submitTbData.flags >> 8) & 0xff;
  submitTbData.flags = submitTbData.flags & 0xff;

  // STREAM_CHECK_CONDITION_GOTO(version < 2, TDB_CODE_SUCCESS);
  if (submitTbData.flags & SUBMIT_REQ_AUTO_CREATE_TABLE) {
    submitTbData.pCreateTbReq = taosMemoryCalloc(1, sizeof(SVCreateTbReq));
    STREAM_CHECK_NULL_GOTO(submitTbData.pCreateTbReq, terrno);
    STREAM_CHECK_RET_GOTO(tDecodeSVCreateTbReq(pCoder, submitTbData.pCreateTbReq));
    STREAM_CHECK_RET_GOTO(processAutoCreateTableNew(sStreamReaderInfo, submitTbData.pCreateTbReq));
  }

  // submit data
  if (tDecodeI64(pCoder, &submitTbData.suid) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }
  if (tDecodeI64(pCoder, &submitTbData.uid) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }

  if (!uidInTableListSet(sStreamReaderInfo, submitTbData.suid, submitTbData.uid, &walMeta.id, false)){
    goto end;
  }
  if (tDecodeI32v(pCoder, &submitTbData.sver) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }

  if (submitTbData.flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    uint64_t nColData = 0;
    if (tDecodeU64v(pCoder, &nColData) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, end);
    }

    SColData colData = {0};
    code = tDecodeColData(version, pCoder, &colData, false);
    if (code) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, end);
    }

    if (colData.flag != HAS_VALUE) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, end);
    }
    walMeta.skey = ((TSKEY *)colData.pData)[0];
    walMeta.ekey = ((TSKEY *)colData.pData)[colData.nVal - 1];

    for (uint64_t i = 1; i < nColData; i++) {
      code = tDecodeColData(version, pCoder, &colData, true);
      if (code) {
        code = TSDB_CODE_INVALID_MSG;
        TSDB_CHECK_CODE(code, lino, end);
      }
    }
  } else {
    uint64_t nRow = 0;
    if (tDecodeU64v(pCoder, &nRow) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, end);
    }

    for (int32_t iRow = 0; iRow < nRow; ++iRow) {
      SRow *pRow = (SRow *)(pCoder->data + pCoder->pos);
      pCoder->pos += pRow->len;
      if (iRow == 0){
#ifndef NO_UNALIGNED_ACCESS
        walMeta.skey = pRow->ts;
#else
        walMeta.skey = taosGetInt64Aligned(&pRow->ts);
#endif
      }
      if (iRow == nRow - 1) {
#ifndef NO_UNALIGNED_ACCESS
        walMeta.ekey = pRow->ts;
#else
        walMeta.ekey = taosGetInt64Aligned(&pRow->ts);
#endif
      }
    }
  }

  WalMetaResult* data = (WalMetaResult*)tSimpleHashGet(gidHash, &walMeta.id, LONG_BYTES);
  if (data != NULL) {
    if (walMeta.skey < data->skey) data->skey = walMeta.skey;
    if (walMeta.ekey > data->ekey) data->ekey = walMeta.ekey;
  } else {
    STREAM_CHECK_RET_GOTO(tSimpleHashPut(gidHash, &walMeta.id, LONG_BYTES, &walMeta, sizeof(WalMetaResult)));
  }

end:
  tDestroySVSubmitCreateTbReq(submitTbData.pCreateTbReq, TSDB_MSG_FLG_DECODE);
  taosMemoryFreeClear(submitTbData.pCreateTbReq);
  tEndDecode(pCoder);
  return code;
}

static int32_t scanSubmitDataForMeta(SStreamTriggerReaderInfo* sStreamReaderInfo, SSTriggerWalNewRsp* rsp, void* data, int32_t len, int64_t ver) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SDecoder decoder = {0};
  SSHashObj* gidHash = NULL;
  void* pTask = sStreamReaderInfo->pTask;

  tDecoderInit(&decoder, data, len);
  if (tStartDecode(&decoder) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }

  uint64_t nSubmitTbData = 0;
  if (tDecodeU64v(&decoder, &nSubmitTbData) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }

  gidHash = tSimpleHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  STREAM_CHECK_NULL_GOTO(gidHash, terrno);

  for (int32_t i = 0; i < nSubmitTbData; i++) {
    STREAM_CHECK_RET_GOTO(scanSubmitTbDataForMeta(&decoder, sStreamReaderInfo, gidHash));
  }
  tEndDecode(&decoder);

  STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(rsp->metaBlock, ((SSDataBlock*)rsp->metaBlock)->info.rows + tSimpleHashGetSize(gidHash)));
  int32_t iter = 0;
  void*   px = tSimpleHashIterate(gidHash, NULL, &iter);
  while (px != NULL) {
    WalMetaResult* pMeta = (WalMetaResult*)px;
    STREAM_CHECK_RET_GOTO(buildWalMetaBlockNew(rsp->metaBlock, pMeta->id, pMeta->skey, pMeta->ekey, ver));
    ((SSDataBlock*)rsp->metaBlock)->info.rows++;
    rsp->totalRows++;
    ST_TASK_DLOG("stream reader scan submit data:skey %" PRId64 ", ekey %" PRId64 ", id %" PRIu64
          ", ver:%"PRId64, pMeta->skey, pMeta->ekey, pMeta->id, ver);
    px = tSimpleHashIterate(gidHash, px, &iter);
  }
end:
  tDecoderClear(&decoder);
  tSimpleHashCleanup( gidHash);
  return code;
}

static int32_t createBlockForTsdbMeta(SSDataBlock** pBlock, bool isVTable) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = taosArrayInit(8, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno);

  int32_t index = 1;
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, index++))  // skey
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, index++))  // ekey
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, index++))  // uid
  if (!isVTable) {
    STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_UBIGINT, LONG_BYTES, index++))  // gid
  }
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, index++))     // nrows

  STREAM_CHECK_RET_GOTO(createDataBlockForStream(schemas, pBlock));

end:
  taosArrayDestroy(schemas);
  return code;
}

static int32_t createBlockForWalMetaNew(SSDataBlock** pBlock) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;

  schemas = taosArrayInit(8, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno);

  int32_t index = 0;
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, index++))  // gid non vtable/uid vtable
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, index++))  // skey
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, index++))  // ekey
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, index++))  // ver

  STREAM_CHECK_RET_GOTO(createDataBlockForStream(schemas, pBlock));

end:
  taosArrayDestroy(schemas);
  return code;
}

static int32_t createBlockForDropTable(SSDataBlock** pBlock) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;

  schemas = taosArrayInit(8, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno);

  int32_t index = 0;
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, index++))  // gid non vtable/uid vtable
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, index++))  // ver

  STREAM_CHECK_RET_GOTO(createDataBlockForStream(schemas, pBlock));

end:
  taosArrayDestroy(schemas);
  return code;
}

static int32_t processMeta(int16_t msgType, SStreamTriggerReaderInfo* sStreamReaderInfo, void *data, int32_t len, SSTriggerWalNewRsp* rsp, int32_t ver) {
  int32_t code = 0;
  int32_t lino = 0;
  SDecoder dcoder = {0};
  tDecoderInit(&dcoder, data, len);
  if (msgType == TDMT_VND_DELETE && sStreamReaderInfo->deleteReCalc != 0) {
    if (rsp->deleteBlock == NULL) {
      STREAM_CHECK_RET_GOTO(createBlockForWalMetaNew((SSDataBlock**)&rsp->deleteBlock));
    }
      
    STREAM_CHECK_RET_GOTO(scanDeleteDataNew(sStreamReaderInfo, rsp, data, len, ver));
  } else if (msgType == TDMT_VND_DROP_TABLE && sStreamReaderInfo->deleteOutTbl != 0) {
    if (rsp->dropBlock == NULL) {
      STREAM_CHECK_RET_GOTO(createBlockForDropTable((SSDataBlock**)&rsp->dropBlock));
    }
    STREAM_CHECK_RET_GOTO(scanDropTableNew(sStreamReaderInfo, rsp, data, len, ver));
  // } else if (msgType == TDMT_VND_DROP_STB) {
  //   STREAM_CHECK_RET_GOTO(scanDropSTableNew(sStreamReaderInfo, data, len));
  } else if (msgType == TDMT_VND_CREATE_TABLE) {
    STREAM_CHECK_RET_GOTO(scanCreateTableNew(sStreamReaderInfo, data, len));
  } else if (msgType == TDMT_VND_ALTER_STB) {
    // STREAM_CHECK_RET_GOTO(scanAlterSTableNew(sStreamReaderInfo, data, len));
  } else if (msgType == TDMT_VND_ALTER_TABLE) {
    STREAM_CHECK_RET_GOTO(scanAlterTableNew(sStreamReaderInfo, data, len));
  }

  end:
  tDecoderClear(&dcoder);
  return code;
}
static int32_t processWalVerMetaNew(SVnode* pVnode, SSTriggerWalNewRsp* rsp, SStreamTriggerReaderInfo* sStreamReaderInfo,
                       int64_t ctime) {
  int32_t code = 0;
  int32_t lino = 0;
  void* pTask = sStreamReaderInfo->pTask;

  SWalReader* pWalReader = walOpenReader(pVnode->pWal, 0);
  STREAM_CHECK_NULL_GOTO(pWalReader, terrno);
  code = walReaderSeekVer(pWalReader, rsp->ver);
  if (code == TSDB_CODE_WAL_LOG_NOT_EXIST){
    if (rsp->ver < walGetFirstVer(pWalReader->pWal)) {
      rsp->ver = walGetFirstVer(pWalReader->pWal);
    }
    ST_TASK_DLOG("vgId:%d %s scan wal end:%s", TD_VID(pVnode), __func__, tstrerror(code));
    code = TSDB_CODE_SUCCESS;
    goto end;
  }
  STREAM_CHECK_RET_GOTO(code);

  STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(rsp->metaBlock, STREAM_RETURN_ROWS_NUM));
  while (1) {
    code = walNextValidMsg(pWalReader, true);
    if (code == TSDB_CODE_WAL_LOG_NOT_EXIST){\
      ST_TASK_DLOG("vgId:%d %s scan wal end:%s", TD_VID(pVnode), __func__, tstrerror(code));
      code = TSDB_CODE_SUCCESS;
      goto end;
    }
    STREAM_CHECK_RET_GOTO(code);
    rsp->ver = pWalReader->curVersion;
    SWalCont* wCont = &pWalReader->pHead->head;
    rsp->verTime = wCont->ingestTs;
    if (wCont->ingestTs / 1000 > ctime) break;
    void*   data = POINTER_SHIFT(wCont->body, sizeof(SMsgHead));
    int32_t len = wCont->bodyLen - sizeof(SMsgHead);
    int64_t ver = wCont->version;

    ST_TASK_DLOG("vgId:%d stream reader scan wal ver:%" PRId64 "/%" PRId64 ", type:%d, deleteData:%d, deleteTb:%d",
      TD_VID(pVnode), ver, walGetAppliedVer(pWalReader->pWal), wCont->msgType, sStreamReaderInfo->deleteReCalc, sStreamReaderInfo->deleteOutTbl);
    if (wCont->msgType == TDMT_VND_SUBMIT) {
      data = POINTER_SHIFT(wCont->body, sizeof(SSubmitReq2Msg));
      len = wCont->bodyLen - sizeof(SSubmitReq2Msg);
      STREAM_CHECK_RET_GOTO(scanSubmitDataForMeta(sStreamReaderInfo, rsp, data, len, ver));
    } else {
      STREAM_CHECK_RET_GOTO(processMeta(wCont->msgType, sStreamReaderInfo, data, len, rsp, ver));
    }

    if (rsp->totalRows >= STREAM_RETURN_ROWS_NUM) {
      break;
    }
  }

end:
  walCloseReader(pWalReader);
  return code;
}

static int32_t cacheTag(SVnode* pVnode, SHashObj* metaCache, SExprInfo* pExprInfo, int32_t numOfExpr, SStorageAPI* api, uint64_t uid) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SMetaReader mr = {0};
  SArray* tagCache = NULL;
  char* data = NULL;

  STREAM_CHECK_CONDITION_GOTO(numOfExpr == 0, code);
  stDebug("%s start,uid:%"PRIu64, __func__, uid);
  void* uidData = taosHashGet(metaCache, &uid, LONG_BYTES);
  if (uidData == NULL) {
    tagCache = taosArrayInit(numOfExpr, POINTER_BYTES);
    STREAM_CHECK_NULL_GOTO(tagCache, terrno);
    if(taosHashPut(metaCache, &uid, LONG_BYTES, &tagCache, POINTER_BYTES) != 0) {
      taosArrayDestroyP(tagCache, taosMemFree);
      code = terrno;
      goto end;
    }
  } else {
    tagCache = *(SArray**)uidData;
    STREAM_CHECK_CONDITION_GOTO(taosArrayGetSize(tagCache) != numOfExpr, TSDB_CODE_INVALID_PARA);
  }
  
  api->metaReaderFn.initReader(&mr, pVnode, META_READER_LOCK, &api->metaFn);
  code = api->metaReaderFn.getEntryGetUidCache(&mr, uid);
  api->metaReaderFn.readerReleaseLock(&mr);
  STREAM_CHECK_RET_GOTO(code);
  
  for (int32_t j = 0; j < numOfExpr; ++j) {
    const SExprInfo* pExpr1 = &pExprInfo[j];
    int32_t functionId = pExpr1->pExpr->_function.functionId;
    // this is to handle the tbname
    if (fmIsScanPseudoColumnFunc(functionId)) {
      int32_t fType = pExpr1->pExpr->_function.functionType;
      if (fType == FUNCTION_TYPE_TBNAME) {
        data = taosMemoryCalloc(1, strlen(mr.me.name) + VARSTR_HEADER_SIZE);
        STREAM_CHECK_NULL_GOTO(data, terrno);
        STR_TO_VARSTR(data, mr.me.name)
      }
    } else {  // these are tags
      const char* p = NULL;
      char* pData = NULL;
      int8_t type = pExpr1->base.resSchema.type;
      int32_t len = pExpr1->base.resSchema.bytes;
      STagVal tagVal = {0};
      tagVal.cid = pExpr1->base.pParam[0].pCol->colId;
      p = api->metaFn.extractTagVal(mr.me.ctbEntry.pTags, type, &tagVal);

      if (type != TSDB_DATA_TYPE_JSON && p != NULL) {
        pData = tTagValToData((const STagVal*)p, false);
      } else {
        pData = (char*)p;
      }

      if (pData != NULL && (type == TSDB_DATA_TYPE_JSON || !IS_VAR_DATA_TYPE(type))) {
        if (type == TSDB_DATA_TYPE_JSON) {
          len = getJsonValueLen(pData);
        }
        data = taosMemoryCalloc(1, len);
        STREAM_CHECK_NULL_GOTO(data, terrno);
        (void)memcpy(data, pData, len);
      } else {
        data = pData;
      }
    }
    if (uidData == NULL){
      STREAM_CHECK_NULL_GOTO(taosArrayPush(tagCache, &data), terrno);
    } else {
      void* pre = taosArrayGetP(tagCache, j);
      taosMemoryFree(pre);
      taosArraySet(tagCache, j, &data);
    }
    data = NULL;
  }

end:
  taosMemoryFree(data);
  api->metaReaderFn.clearReader(&mr);
  return code;
}

static int32_t processTag(SVnode* pVnode, SStreamTriggerReaderInfo* info, bool isCalc, 
  uint64_t uid, SSDataBlock* pBlock, uint32_t currentRow, uint32_t numOfRows, uint32_t numOfBlocks) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SArray* tagCache = NULL;

  void* pTask = info->pTask;
  ST_TASK_DLOG("%s start. rows:%" PRIu32 ",uid:%"PRIu64, __func__,  numOfRows, uid);
  
  SHashObj* metaCache = isCalc ? info->pTableMetaCacheCalc : info->pTableMetaCacheTrigger;
  SExprInfo*   pExprInfo = isCalc ? info->pExprInfoCalcTag : info->pExprInfoTriggerTag; 
  int32_t      numOfExpr = isCalc ? info->numOfExprCalcTag : info->numOfExprTriggerTag;
  if (numOfExpr == 0) {
    return TSDB_CODE_SUCCESS;
  }

  void* uidData = taosHashGet(metaCache, &uid, LONG_BYTES);
  if (uidData == NULL) {
    ST_TASK_ELOG("%s error uidData is null,uid:%"PRIu64, __func__, uid);
    code = TSDB_CODE_STREAM_INTERNAL_ERROR;
    goto end;
  } else {
    tagCache = *(SArray**)uidData;
    STREAM_CHECK_CONDITION_GOTO(taosArrayGetSize(tagCache) != numOfExpr, TSDB_CODE_INVALID_PARA);
  }
  
  for (int32_t j = 0; j < numOfExpr; ++j) {
    const SExprInfo* pExpr1 = &pExprInfo[j];
    int32_t          dstSlotId = pExpr1->base.resSchema.slotId;

    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, dstSlotId);
    STREAM_CHECK_NULL_GOTO(pColInfoData, terrno);
    int32_t functionId = pExpr1->pExpr->_function.functionId;

    // this is to handle the tbname
    if (fmIsScanPseudoColumnFunc(functionId)) {
      int32_t fType = pExpr1->pExpr->_function.functionType;
      if (fType == FUNCTION_TYPE_TBNAME) {
        pColInfoData->info.colId = -1;
      }
    } 
    char* data = taosArrayGetP(tagCache, j);

    bool isNullVal = (data == NULL) || (pColInfoData->info.type == TSDB_DATA_TYPE_JSON && tTagIsJsonNull(data));
    if (isNullVal) {
      colDataSetNNULL(pColInfoData, currentRow, numOfRows);
    } else {
      if (!IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
        for (uint32_t i = 0; i < numOfRows; i++){
          colDataClearNull_f(pColInfoData->nullbitmap, currentRow + i);
        }
      }
      code = colDataSetNItems(pColInfoData, currentRow, data, numOfRows, numOfBlocks, false);
      STREAM_CHECK_RET_GOTO(code);
    }
  }

end:
  return code;
}

int32_t getRowRange(SColData* pCol, STimeWindow* window, int32_t* rowStart, int32_t* rowEnd, int32_t* nRows) {
  int32_t code = 0;
  int32_t lino = 0;
  *nRows = 0;
  *rowStart = 0;
  *rowEnd = pCol->nVal;
  if (window != NULL) {
    SColVal colVal = {0};
    *rowStart = -1;
    *rowEnd = -1;
    for (int32_t k = 0; k < pCol->nVal; k++) {
      STREAM_CHECK_RET_GOTO(tColDataGetValue(pCol, k, &colVal));
      int64_t ts = VALUE_GET_TRIVIAL_DATUM(&colVal.value);
      if (ts >= window->skey && *rowStart == -1) {
        *rowStart = k;
      }
      if (ts > window->ekey && *rowEnd == -1) {
        *rowEnd = k;
      }
    }
    STREAM_CHECK_CONDITION_GOTO(*rowStart == -1 || *rowStart == *rowEnd, TDB_CODE_SUCCESS);

    if (*rowStart != -1 && *rowEnd == -1) {
      *rowEnd = pCol->nVal;
    }
  }
  *nRows = *rowEnd - *rowStart;

end:
  return code;
}

static int32_t setColData(int64_t rows, int32_t rowStart, int32_t rowEnd, SColData* colData, SColumnInfoData* pColData) {
  int32_t code = 0;
  int32_t lino = 0;
  for (int32_t k = rowStart; k < rowEnd; k++) {
    SColVal colVal = {0};
    STREAM_CHECK_RET_GOTO(tColDataGetValue(colData, k, &colVal));
    STREAM_CHECK_RET_GOTO(colDataSetVal(pColData, rows + k - rowStart, VALUE_GET_DATUM(&colVal.value, colVal.value.type),
                                        !COL_VAL_IS_VALUE(&colVal)));
  }
  end:
  return code;
}

static int32_t getColId(int64_t suid, int64_t uid, int16_t i, SStreamTriggerReaderInfo* sStreamReaderInfo, SSTriggerWalNewRsp* rsp, int16_t* colId) {
  int32_t code = 0;
  int32_t lino = 0;
  int64_t id[2] = {suid, uid};
  taosRLockLatch(&sStreamReaderInfo->lock);
  void *px = tSimpleHashGet(rsp->isCalc ? sStreamReaderInfo->uidHashCalc : sStreamReaderInfo->uidHashTrigger, id, sizeof(id));
  STREAM_CHECK_NULL_GOTO(px, TSDB_CODE_INVALID_PARA);
  SSHashObj* uInfo = *(SSHashObj **)px;
  STREAM_CHECK_NULL_GOTO(uInfo, TSDB_CODE_INVALID_PARA);
  int16_t*  tmp = tSimpleHashGet(uInfo, &i, sizeof(i));
  if (tmp != NULL) {
    *colId = *tmp;
  } else {
    *colId = -1;
  }

end:
  taosRUnLockLatch(&sStreamReaderInfo->lock);
  return code;
}

static int32_t getSchemas(SVnode* pVnode, int64_t suid, int64_t uid, int32_t sver, SStreamTriggerReaderInfo* sStreamReaderInfo, STSchema** schema) {
  int32_t code = 0;
  int32_t lino = 0;
  int64_t id = suid != 0 ? suid : uid;
  if (sStreamReaderInfo->isVtableStream) {
    STSchema** schemaTmp = taosHashGet(sStreamReaderInfo->triggerTableSchemaMapVTable, &id, LONG_BYTES);
    if (schemaTmp == NULL || *schemaTmp == NULL || (*schemaTmp)->version != sver) {
      *schema = metaGetTbTSchema(pVnode->pMeta, id, sver, 1);
      STREAM_CHECK_NULL_GOTO(*schema, terrno);
      code = taosHashPut(sStreamReaderInfo->triggerTableSchemaMapVTable, &id, LONG_BYTES, schema, POINTER_BYTES);
      if (code != 0) {
        taosMemoryFree(*schema);
        goto end;
      }
    } else {
      *schema = *schemaTmp;
    }
  } else {
    if (sStreamReaderInfo->triggerTableSchema == NULL || sStreamReaderInfo->triggerTableSchema->version != sver) {
      taosMemoryFree(sStreamReaderInfo->triggerTableSchema);
      sStreamReaderInfo->triggerTableSchema = metaGetTbTSchema(pVnode->pMeta, id, sver, 1);
      STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->triggerTableSchema, terrno);
    }
    *schema = sStreamReaderInfo->triggerTableSchema;
  }
  
end:
  return code;
}

static int32_t scanSubmitTbData(SVnode* pVnode, SDecoder *pCoder, SStreamTriggerReaderInfo* sStreamReaderInfo, 
  SSHashObj* ranges, SSHashObj* gidHash, SSTriggerWalNewRsp* rsp, int64_t ver) {
  int32_t code = 0;
  int32_t lino = 0;
  uint64_t id = 0;
  WalMetaResult walMeta = {0};
  void* pTask = sStreamReaderInfo->pTask;
  SSDataBlock * pBlock = (SSDataBlock*)rsp->dataBlock;

  if (tStartDecode(pCoder) < 0) {
    ST_TASK_ELOG("vgId:%d %s invalid submit data", TD_VID(pVnode), __func__);
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }

  SSubmitTbData submitTbData = {0};
  uint8_t       version = 0;
  if (tDecodeI32v(pCoder, &submitTbData.flags) < 0) {
    ST_TASK_ELOG("vgId:%d %s invalid submit data flags", TD_VID(pVnode), __func__);
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }
  version = (submitTbData.flags >> 8) & 0xff;
  submitTbData.flags = submitTbData.flags & 0xff;
  // STREAM_CHECK_CONDITION_GOTO(version < 2, TDB_CODE_SUCCESS);
  if (submitTbData.flags & SUBMIT_REQ_AUTO_CREATE_TABLE) {
    if (tStartDecode(pCoder) < 0) {
      ST_TASK_ELOG("vgId:%d %s invalid auto create table data", TD_VID(pVnode), __func__);
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, end);
    }
    tEndDecode(pCoder);
  }

  // submit data
  if (tDecodeI64(pCoder, &submitTbData.suid) < 0) {
    ST_TASK_ELOG("vgId:%d %s invalid submit data suid", TD_VID(pVnode), __func__);
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }
  if (tDecodeI64(pCoder, &submitTbData.uid) < 0) {
    ST_TASK_ELOG("vgId:%d %s invalid submit data uid", TD_VID(pVnode), __func__);
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }

  ST_TASK_TLOG("%s uid:%" PRId64 ", suid:%" PRId64 ", ver:%" PRId64, __func__, submitTbData.uid, submitTbData.suid, ver);

  if (rsp->uidHash != NULL) {
    uint64_t* gid = tSimpleHashGet(rsp->uidHash, &submitTbData.uid, LONG_BYTES);
    STREAM_CHECK_CONDITION_GOTO(gid == NULL, TDB_CODE_SUCCESS);
    ST_TASK_TLOG("%s get uid gid from uidHash, uid:%" PRId64 ", suid:%" PRId64 " gid:%"PRIu64, __func__, submitTbData.uid, submitTbData.suid, *gid);
    id = *gid;
  } else {
    STREAM_CHECK_CONDITION_GOTO(!uidInTableListSet(sStreamReaderInfo, submitTbData.suid, submitTbData.uid, &id, rsp->isCalc), TDB_CODE_SUCCESS);
  }

  walMeta.id = id;
  STimeWindow window = {.skey = INT64_MIN, .ekey = INT64_MAX};

  if (ranges != NULL){
    void* timerange = tSimpleHashGet(ranges, &id, sizeof(id));
    if (timerange == NULL) goto end;;
    int64_t* pRange = (int64_t*)timerange;
    window.skey = pRange[0];
    window.ekey = pRange[1];
  }
  
  if (tDecodeI32v(pCoder, &submitTbData.sver) < 0) {
    ST_TASK_ELOG("vgId:%d %s invalid submit data sver", TD_VID(pVnode), __func__);
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }

  STSchema*    schema = NULL;
  STREAM_CHECK_RET_GOTO(getSchemas(pVnode, submitTbData.suid, submitTbData.uid, submitTbData.sver, sStreamReaderInfo, &schema));

  SStreamWalDataSlice* pSlice = (SStreamWalDataSlice*)tSimpleHashGet(rsp->indexHash, &submitTbData.uid, LONG_BYTES);
  int32_t blockStart = 0;
  int32_t numOfRows = 0;
  if (submitTbData.flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    uint64_t nColData = 0;
    if (tDecodeU64v(pCoder, &nColData) < 0) {
      ST_TASK_ELOG("vgId:%d %s invalid submit data nColData", TD_VID(pVnode), __func__);
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, end);
    }

    SColData colData = {0};
    code = tDecodeColData(version, pCoder, &colData, false);
    if (code) {
      ST_TASK_ELOG("vgId:%d %s invalid submit data colData", TD_VID(pVnode), __func__);
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, end);
    }

    if (colData.flag != HAS_VALUE) {
      ST_TASK_ELOG("vgId:%d %s invalid submit data colData flag", TD_VID(pVnode), __func__);
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, end);
    }
    
    walMeta.skey = ((TSKEY *)colData.pData)[0];
    walMeta.ekey = ((TSKEY *)colData.pData)[colData.nVal - 1];

    int32_t rowStart = 0;
    int32_t rowEnd = 0;
    STREAM_CHECK_RET_GOTO(getRowRange(&colData, &window, &rowStart, &rowEnd, &numOfRows));
    STREAM_CHECK_CONDITION_GOTO(numOfRows <= 0, TDB_CODE_SUCCESS);

    STREAM_CHECK_NULL_GOTO(pSlice, TSDB_CODE_INVALID_PARA);
    blockStart = pSlice->currentRowIdx;
    int32_t pos = pCoder->pos;
    for (int16_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); i++) {
      SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, i);
      STREAM_CHECK_NULL_GOTO(pColData, terrno);
      if (pColData->info.colId <= -1) {
        pColData->hasNull = true;
        continue;
      }
      if (pColData->info.colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        STREAM_CHECK_RET_GOTO(setColData(blockStart, rowStart, rowEnd, &colData, pColData));
        continue;
      }

      pCoder->pos = pos;

      int16_t colId = 0;
      if (sStreamReaderInfo->isVtableStream){
        STREAM_CHECK_RET_GOTO(getColId(submitTbData.suid, submitTbData.uid, i, sStreamReaderInfo, rsp, &colId));
        ST_TASK_TLOG("%s vtable colId:%d, i:%d, uid:%" PRId64, __func__, colId, i, submitTbData.uid);
      } else {
        colId = pColData->info.colId;
      }
      
      uint64_t j = 1;
      for (; j < nColData; j++) {
        int16_t cid = 0;
        int32_t posTmp = pCoder->pos;
        pCoder->pos += INT_BYTES;
        if ((code = tDecodeI16v(pCoder, &cid))) return code;
        pCoder->pos = posTmp;
        if (cid == colId) {
          SColData colDataTmp = {0};
          code = tDecodeColData(version, pCoder, &colDataTmp, false);
          if (code) {
            code = TSDB_CODE_INVALID_MSG;
            TSDB_CHECK_CODE(code, lino, end);
          }
          STREAM_CHECK_RET_GOTO(setColData(blockStart, rowStart, rowEnd, &colDataTmp, pColData));
          break;
        }
        code = tDecodeColData(version, pCoder, &colData, true);
        if (code) {
          code = TSDB_CODE_INVALID_MSG;
          TSDB_CHECK_CODE(code, lino, end);
        }
      }
      if (j == nColData) {
        colDataSetNNULL(pColData, blockStart, numOfRows);
      }
    }
  } else {
    uint64_t nRow = 0;
    if (tDecodeU64v(pCoder, &nRow) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, end);
    }
    for (int32_t iRow = 0; iRow < nRow; ++iRow) {
      SRow *pRow = (SRow *)(pCoder->data + pCoder->pos);
      pCoder->pos += pRow->len;

      if (iRow == 0){
#ifndef NO_UNALIGNED_ACCESS
        walMeta.skey = pRow->ts;
#else
        walMeta.skey = taosGetInt64Aligned(&pRow->ts);
#endif
      }
      if (iRow == nRow - 1) {
#ifndef NO_UNALIGNED_ACCESS
        walMeta.ekey = pRow->ts;
#else
        walMeta.ekey = taosGetInt64Aligned(&pRow->ts);
#endif
      }

      if (pRow->ts < window.skey || pRow->ts > window.ekey) {
        continue;
      }
      STREAM_CHECK_NULL_GOTO(pSlice, TSDB_CODE_INVALID_PARA);
      blockStart = pSlice->currentRowIdx;
     
      for (int16_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); i++) {  // reader todo test null
        SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, i);
        STREAM_CHECK_NULL_GOTO(pColData, terrno);
        if (pColData->info.colId <= -1) {
          pColData->hasNull = true;
          continue;
        }
        int16_t colId = 0;
        if (sStreamReaderInfo->isVtableStream){
          STREAM_CHECK_RET_GOTO(getColId(submitTbData.suid, submitTbData.uid, i, sStreamReaderInfo, rsp, &colId));
          ST_TASK_TLOG("%s vtable colId:%d, i:%d, uid:%" PRId64, __func__, colId, i, submitTbData.uid);
        } else {
          colId = pColData->info.colId;
        }
        
        SColVal colVal = {0};
        int32_t sourceIdx = 0;
        while (1) {
          if (sourceIdx >= schema->numOfCols) {
            break;
          }
          STREAM_CHECK_RET_GOTO(tRowGet(pRow, schema, sourceIdx, &colVal));
          if (colVal.cid == colId) {
            break;
          }
          sourceIdx++;
        }
        if (colVal.cid == colId && COL_VAL_IS_VALUE(&colVal)) {
          if (IS_VAR_DATA_TYPE(colVal.value.type) || colVal.value.type == TSDB_DATA_TYPE_DECIMAL){
            STREAM_CHECK_RET_GOTO(varColSetVarData(pColData, blockStart+ numOfRows, (const char*)colVal.value.pData, colVal.value.nData, !COL_VAL_IS_VALUE(&colVal)));
            ST_TASK_TLOG("%s vtable colId:%d, i:%d, colData:%p, data:%s, len:%d, rowIndex:%d, offset:%d, uid:%" PRId64, __func__, colId, i, pColData, 
              (const char*)colVal.value.pData, colVal.value.nData, blockStart+ numOfRows, pColData->varmeta.offset[blockStart+ numOfRows], submitTbData.uid);
          } else {
            STREAM_CHECK_RET_GOTO(colDataSetVal(pColData, blockStart + numOfRows, (const char*)(&(colVal.value.val)), !COL_VAL_IS_VALUE(&colVal)));
          }
        } else {
          colDataSetNULL(pColData, blockStart + numOfRows);
        }
      }
      
      numOfRows++;
    }
  }

  if (numOfRows > 0) {
    if (!sStreamReaderInfo->isVtableStream) {
      STREAM_CHECK_RET_GOTO(processTag(pVnode, sStreamReaderInfo, rsp->isCalc, submitTbData.uid, pBlock, blockStart, numOfRows, 1));
    }
    
    SColumnInfoData* pColData = taosArrayGetLast(pBlock->pDataBlock);
    STREAM_CHECK_NULL_GOTO(pColData, terrno);
    STREAM_CHECK_RET_GOTO(colDataSetNItems(pColData, blockStart, (const char*)&ver, numOfRows, 1, false));
  }
  STREAM_CHECK_NULL_GOTO(pSlice, TSDB_CODE_INVALID_PARA);
  ST_TASK_DLOG("%s process submit data:skey %" PRId64 ", ekey %" PRId64 ", id %" PRIu64
    ", uid:%" PRId64 ", ver:%"PRId64 ", row index:%d, rows:%d", __func__, window.skey, window.ekey, 
    id, submitTbData.uid, ver, pSlice->currentRowIdx, numOfRows);
  pSlice->currentRowIdx += numOfRows;
  pBlock->info.rows += numOfRows;
  
  if (gidHash == NULL) goto end;

  WalMetaResult* data = (WalMetaResult*)tSimpleHashGet(gidHash, &walMeta.id, LONG_BYTES);
  if (data != NULL) {
    if (walMeta.skey < data->skey) data->skey = walMeta.skey;
    if (walMeta.ekey > data->ekey) data->ekey = walMeta.ekey;
  } else {
    STREAM_CHECK_RET_GOTO(tSimpleHashPut(gidHash, &walMeta.id, LONG_BYTES, &walMeta, sizeof(WalMetaResult)));
  }

end:
  if (code != 0) {                                                             \
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code)); \
  }
  tEndDecode(pCoder);
  return code;
}
static int32_t scanSubmitData(SVnode* pVnode, SStreamTriggerReaderInfo* sStreamReaderInfo,
  void* data, int32_t len, SSHashObj* ranges, SSTriggerWalNewRsp* rsp, int64_t ver) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SDecoder decoder = {0};
  SSHashObj* gidHash = NULL;
  void* pTask = sStreamReaderInfo->pTask;

  tDecoderInit(&decoder, data, len);
  if (tStartDecode(&decoder) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }

  uint64_t nSubmitTbData = 0;
  if (tDecodeU64v(&decoder, &nSubmitTbData) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }

  if (rsp->metaBlock != NULL){
    gidHash = tSimpleHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    STREAM_CHECK_NULL_GOTO(gidHash, terrno);
  }

  for (int32_t i = 0; i < nSubmitTbData; i++) {
    STREAM_CHECK_RET_GOTO(scanSubmitTbData(pVnode, &decoder, sStreamReaderInfo, ranges, gidHash, rsp, ver));
  }

  tEndDecode(&decoder);

  if (rsp->metaBlock != NULL){
    STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(rsp->metaBlock, ((SSDataBlock*)rsp->metaBlock)->info.rows + tSimpleHashGetSize(gidHash)));
    int32_t iter = 0;
    void*   px = tSimpleHashIterate(gidHash, NULL, &iter);
    while (px != NULL) {
      WalMetaResult* pMeta = (WalMetaResult*)px;
      STREAM_CHECK_RET_GOTO(buildWalMetaBlockNew(rsp->metaBlock, pMeta->id, pMeta->skey, pMeta->ekey, ver));
      ((SSDataBlock*)rsp->metaBlock)->info.rows++;
      ST_TASK_DLOG("%s process meta data:skey %" PRId64 ", ekey %" PRId64 ", id %" PRIu64
            ", ver:%"PRId64, __func__, pMeta->skey, pMeta->ekey, pMeta->id, ver);
      px = tSimpleHashIterate(gidHash, px, &iter);
    }
  }
  

end:
  tSimpleHashCleanup(gidHash);
  tDecoderClear(&decoder);
  return code;
}

static int32_t scanSubmitTbDataPre(SDecoder *pCoder, SStreamTriggerReaderInfo* sStreamReaderInfo, SSHashObj* ranges, 
  uint64_t* gid, int64_t* uid, int32_t* numOfRows, SSTriggerWalNewRsp* rsp) {
  int32_t code = 0;
  int32_t lino = 0;
  void* pTask = sStreamReaderInfo->pTask;

  if (tStartDecode(pCoder) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }

  SSubmitTbData submitTbData = {0};
  uint8_t       version = 0;
  if (tDecodeI32v(pCoder, &submitTbData.flags) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }
  version = (submitTbData.flags >> 8) & 0xff;
  submitTbData.flags = submitTbData.flags & 0xff;

  // STREAM_CHECK_CONDITION_GOTO(version < 2, TDB_CODE_SUCCESS);
  if (submitTbData.flags & SUBMIT_REQ_AUTO_CREATE_TABLE) {
    submitTbData.pCreateTbReq = taosMemoryCalloc(1, sizeof(SVCreateTbReq));
    STREAM_CHECK_NULL_GOTO(submitTbData.pCreateTbReq, terrno);
    STREAM_CHECK_RET_GOTO(tDecodeSVCreateTbReq(pCoder, submitTbData.pCreateTbReq));
    STREAM_CHECK_RET_GOTO(processAutoCreateTableNew(sStreamReaderInfo, submitTbData.pCreateTbReq));
  }

  // submit data
  if (tDecodeI64(pCoder, &submitTbData.suid) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }
  if (tDecodeI64(pCoder, uid) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }
  ST_TASK_TLOG("%s uid:%" PRId64 ", suid:%" PRId64, __func__, *uid, submitTbData.suid);
  STREAM_CHECK_CONDITION_GOTO(!uidInTableListSet(sStreamReaderInfo, submitTbData.suid, *uid, gid, rsp->isCalc), TDB_CODE_SUCCESS);
  if (rsp->uidHash != NULL) {
    STREAM_CHECK_RET_GOTO(tSimpleHashPut(rsp->uidHash, uid, LONG_BYTES, gid, LONG_BYTES));
    ST_TASK_TLOG("%s put uid into uidHash, uid:%" PRId64 ", suid:%" PRId64 " gid:%"PRIu64, __func__, *uid, submitTbData.suid, *gid);
  }
  STimeWindow window = {.skey = INT64_MIN, .ekey = INT64_MAX};

  if (ranges != NULL){
    void* timerange = tSimpleHashGet(ranges, gid, sizeof(*gid));
    if (timerange == NULL) goto end;;
    int64_t* pRange = (int64_t*)timerange;
    window.skey = pRange[0];
    window.ekey = pRange[1];
  }
  
  if (tDecodeI32v(pCoder, &submitTbData.sver) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }

  if (submitTbData.flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    uint64_t nColData = 0;
    if (tDecodeU64v(pCoder, &nColData) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, end);
    }

    SColData colData = {0};
    code = tDecodeColData(version, pCoder, &colData, false);
    if (code) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, end);
    }

    if (colData.flag != HAS_VALUE) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, end);
    }
    int32_t rowStart = 0;
    int32_t rowEnd = 0;
    if (window.skey != INT64_MIN || window.ekey != INT64_MAX) {
      STREAM_CHECK_RET_GOTO(getRowRange(&colData, &window, &rowStart, &rowEnd, numOfRows));
    } else {
      (*numOfRows) = colData.nVal;
    } 
  } else {
    uint64_t nRow = 0;
    if (tDecodeU64v(pCoder, &nRow) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, end);
    }

    if (window.skey != INT64_MIN || window.ekey != INT64_MAX) { 
      for (int32_t iRow = 0; iRow < nRow; ++iRow) {
        SRow *pRow = (SRow *)(pCoder->data + pCoder->pos);
        pCoder->pos += pRow->len;
        if (pRow->ts < window.skey || pRow->ts > window.ekey) {
          continue;
        }
        (*numOfRows)++;
      }
    } else {
      (*numOfRows) = nRow;
    }
  }
  
end:
  tDestroySVSubmitCreateTbReq(submitTbData.pCreateTbReq, TSDB_MSG_FLG_DECODE);
  taosMemoryFreeClear(submitTbData.pCreateTbReq);
  tEndDecode(pCoder);
  return code;
}

static int32_t scanSubmitDataPre(SStreamTriggerReaderInfo* sStreamReaderInfo, void* data, int32_t len, SSHashObj* ranges, SSTriggerWalNewRsp* rsp, int64_t ver) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SDecoder decoder = {0};
  void* pTask = sStreamReaderInfo->pTask;

  tDecoderInit(&decoder, data, len);
  if (tStartDecode(&decoder) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }

  uint64_t nSubmitTbData = 0;
  if (tDecodeU64v(&decoder, &nSubmitTbData) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, end);
  }
  ST_TASK_TLOG("%s nSubmitTbData:%" PRIu64 ", ver:%"PRId64, __func__, nSubmitTbData, ver);

  for (int32_t i = 0; i < nSubmitTbData; i++) {
    uint64_t gid = -1;
    int64_t  uid = 0;
    int32_t numOfRows = 0;
    STREAM_CHECK_RET_GOTO(scanSubmitTbDataPre(&decoder, sStreamReaderInfo, ranges, &gid, &uid, &numOfRows, rsp));
    if (numOfRows <= 0) {
      continue;
    }
    rsp->totalRows += numOfRows;

    SStreamWalDataSlice* pSlice = (SStreamWalDataSlice*)tSimpleHashGet(rsp->indexHash, &uid, LONG_BYTES);
    if (pSlice != NULL) {
      pSlice->numRows += numOfRows;
      ST_TASK_DLOG("%s again uid:%" PRId64 ", gid:%" PRIu64 ", total numOfRows:%d, hash:%p %d, ver:%"PRId64, __func__, uid, gid, pSlice->numRows, rsp->indexHash, tSimpleHashGetSize(rsp->indexHash), ver);
      pSlice->gId = gid;
    } else {
      SStreamWalDataSlice tmp = {.gId=gid,.numRows=numOfRows,.currentRowIdx=0,.startRowIdx=0};
      ST_TASK_DLOG("%s first uid:%" PRId64 ", gid:%" PRIu64 ", numOfRows:%d, hash:%p %d, ver:%"PRId64, __func__, uid, gid, tmp.numRows, rsp->indexHash, tSimpleHashGetSize(rsp->indexHash), ver);
      STREAM_CHECK_RET_GOTO(tSimpleHashPut(rsp->indexHash, &uid, LONG_BYTES, &tmp, sizeof(tmp)));
    } 
  }

  tEndDecode(&decoder);

end:
  tDecoderClear(&decoder);
  return code;
}

static void buildIndexHash(SSHashObj* indexHash, void* pTask){
  void*   pe = NULL;
  int32_t iter = 0;
  int32_t index = 0;
  while ((pe = tSimpleHashIterate(indexHash, pe, &iter)) != NULL) {
    SStreamWalDataSlice* pInfo = (SStreamWalDataSlice*)pe;
    pInfo->startRowIdx = index;
    pInfo->currentRowIdx = index;
    index += pInfo->numRows;
    ST_TASK_DLOG("%s uid:%" PRId64 ", gid:%" PRIu64 ", startRowIdx:%d, numRows:%d", __func__, *(int64_t*)(tSimpleHashGetKey(pe, NULL)),
    pInfo->gId, pInfo->startRowIdx, pInfo->numRows);
  }
}

static void printIndexHash(SSHashObj* indexHash, void* pTask){
  if (qDebugFlag & DEBUG_TRACE) {
    void*   pe = NULL;
    int32_t iter = 0;
    while ((pe = tSimpleHashIterate(indexHash, pe, &iter)) != NULL) {
      SStreamWalDataSlice* pInfo = (SStreamWalDataSlice*)pe;
      ST_TASK_TLOG("%s uid:%" PRId64 ", gid:%" PRIu64 ", startRowIdx:%d, numRows:%d", __func__, *(int64_t*)(tSimpleHashGetKey(pe, NULL)),
      pInfo->gId, pInfo->startRowIdx, pInfo->numRows);
    }
  }
}

static void filterIndexHash(SSHashObj* indexHash, SColumnInfoData* pRet){
  void*   pe = NULL;
  int32_t iter = 0;
  int32_t index = 0;
  int32_t pIndex = 0;
  int8_t* pIndicator = (int8_t*)pRet->pData;
  while ((pe = tSimpleHashIterate(indexHash, pe, &iter)) != NULL) {
    SStreamWalDataSlice* pInfo = (SStreamWalDataSlice*)pe;
    pInfo->startRowIdx = index;
    int32_t size = pInfo->numRows;
    for (int32_t i = 0; i < pInfo->numRows; i++) {
      if (pIndicator && !pIndicator[pIndex++]) {
        size--;
      }
    }
    pInfo->numRows = size;
    index += pInfo->numRows;
    stTrace("stream reader re build index hash uid:%" PRId64 ", gid:%" PRIu64 ", startRowIdx:%d, numRows:%d", *(int64_t*)(tSimpleHashGetKey(pe, NULL)),
    pInfo->gId, pInfo->startRowIdx, pInfo->numRows);
  }
}

static int32_t prepareIndexMetaData(SWalReader* pWalReader, SStreamTriggerReaderInfo* sStreamReaderInfo, SSTriggerWalNewRsp* resultRsp){
  int32_t      code = 0;
  int32_t      lino = 0;
  void* pTask = sStreamReaderInfo->pTask;

  code = walReaderSeekVer(pWalReader, resultRsp->ver);
  if (code == TSDB_CODE_WAL_LOG_NOT_EXIST){
    if (resultRsp->ver < walGetFirstVer(pWalReader->pWal)) {
      resultRsp->ver = walGetFirstVer(pWalReader->pWal);
    }
    ST_TASK_DLOG("%s scan wal end:%s",  __func__, tstrerror(code));
    code = TSDB_CODE_SUCCESS;
    goto end;
  }
  STREAM_CHECK_RET_GOTO(code);

  while (1) {
    code = walNextValidMsg(pWalReader, true);
    if (code == TSDB_CODE_WAL_LOG_NOT_EXIST){
      ST_TASK_DLOG("%s scan wal end:%s", __func__, tstrerror(code));
      code = TSDB_CODE_SUCCESS;
      goto end;
    }
    STREAM_CHECK_RET_GOTO(code);
    resultRsp->ver = pWalReader->curVersion;
    SWalCont* wCont = &pWalReader->pHead->head;
    resultRsp->verTime = wCont->ingestTs;
    void*   data = POINTER_SHIFT(wCont->body, sizeof(SMsgHead));
    int32_t len = wCont->bodyLen - sizeof(SMsgHead);
    int64_t ver = wCont->version;
    ST_TASK_DLOG("%s scan wal ver:%" PRId64 ", type:%d, deleteData:%d, deleteTb:%d", __func__,
      ver, wCont->msgType, sStreamReaderInfo->deleteReCalc, sStreamReaderInfo->deleteOutTbl);
    if (wCont->msgType == TDMT_VND_SUBMIT) {
      data = POINTER_SHIFT(wCont->body, sizeof(SSubmitReq2Msg));
      len = wCont->bodyLen - sizeof(SSubmitReq2Msg);
      STREAM_CHECK_RET_GOTO(scanSubmitDataPre(sStreamReaderInfo, data, len, NULL, resultRsp, ver));
    } else if (wCont->msgType == TDMT_VND_ALTER_TABLE && resultRsp->totalRows > 0) {
      resultRsp->ver--;
      break;
    } else {
      STREAM_CHECK_RET_GOTO(processMeta(wCont->msgType, sStreamReaderInfo, data, len, resultRsp, ver));
    }

    ST_TASK_DLOG("%s scan wal next ver:%" PRId64 ", totalRows:%d", __func__, resultRsp->ver, resultRsp->totalRows);
    if (resultRsp->totalRows >= STREAM_RETURN_ROWS_NUM) {
      break;
    }
  }
  
end:
  STREAM_PRINT_LOG_END(code, lino);
  return code;
}

static int32_t prepareIndexData(SWalReader* pWalReader, SStreamTriggerReaderInfo* sStreamReaderInfo, 
  SArray* versions, SSHashObj* ranges, SSTriggerWalNewRsp* rsp){
  int32_t      code = 0;
  int32_t      lino = 0;
  void* pTask = sStreamReaderInfo->pTask;

  for(int32_t i = 0; i < taosArrayGetSize(versions); i++) {
    int64_t *ver = taosArrayGet(versions, i);
    if (ver == NULL) continue;

    STREAM_CHECK_RET_GOTO(walFetchHead(pWalReader, *ver));
    if(pWalReader->pHead->head.msgType != TDMT_VND_SUBMIT) {
      TAOS_CHECK_RETURN(walSkipFetchBody(pWalReader));
      ST_TASK_TLOG("%s not data, skip, ver:%"PRId64, __func__, *ver);
      continue;
    }
    STREAM_CHECK_RET_GOTO(walFetchBody(pWalReader));

    SWalCont* wCont = &pWalReader->pHead->head;
    void*   pBody = POINTER_SHIFT(wCont->body, sizeof(SSubmitReq2Msg));
    int32_t bodyLen = wCont->bodyLen - sizeof(SSubmitReq2Msg);

    STREAM_CHECK_RET_GOTO(scanSubmitDataPre(sStreamReaderInfo, pBody, bodyLen, ranges, rsp, *ver));
  }
  
end:
  return code;
}

static int32_t filterData(SSTriggerWalNewRsp* resultRsp, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t      code = 0;
  int32_t       lino = 0;
  SColumnInfoData* pRet = NULL;

  int64_t totalRows = ((SSDataBlock*)resultRsp->dataBlock)->info.rows;
  STREAM_CHECK_RET_GOTO(qStreamFilter(((SSDataBlock*)resultRsp->dataBlock), sStreamReaderInfo->pFilterInfo, &pRet));

  if (((SSDataBlock*)resultRsp->dataBlock)->info.rows < totalRows) {
    filterIndexHash(resultRsp->indexHash, pRet);
  }

end:
  colDataDestroy(pRet);
  taosMemoryFree(pRet);
  return code;
}

static int32_t processWalVerMetaDataNew(SVnode* pVnode, SStreamTriggerReaderInfo* sStreamReaderInfo, 
                                    SSTriggerWalNewRsp* resultRsp) {
  int32_t      code = 0;
  int32_t      lino = 0;
  void* pTask = sStreamReaderInfo->pTask;
                                        
  SWalReader* pWalReader = walOpenReader(pVnode->pWal, 0);
  STREAM_CHECK_NULL_GOTO(pWalReader, terrno);
  blockDataEmpty(resultRsp->dataBlock);
  blockDataEmpty(resultRsp->metaBlock);
  int64_t lastVer = resultRsp->ver;                                      
  STREAM_CHECK_RET_GOTO(prepareIndexMetaData(pWalReader, sStreamReaderInfo, resultRsp));
  STREAM_CHECK_CONDITION_GOTO(resultRsp->totalRows == 0, TDB_CODE_SUCCESS);

  buildIndexHash(resultRsp->indexHash, pTask);
  STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(((SSDataBlock*)resultRsp->dataBlock), resultRsp->totalRows));
  while(lastVer < resultRsp->ver) {
    STREAM_CHECK_RET_GOTO(walFetchHead(pWalReader, lastVer++));
    if(pWalReader->pHead->head.msgType != TDMT_VND_SUBMIT) {
      TAOS_CHECK_RETURN(walSkipFetchBody(pWalReader));
      continue;
    }
    STREAM_CHECK_RET_GOTO(walFetchBody(pWalReader));
    SWalCont* wCont = &pWalReader->pHead->head;
    void*   pBody = POINTER_SHIFT(wCont->body, sizeof(SSubmitReq2Msg));
    int32_t bodyLen = wCont->bodyLen - sizeof(SSubmitReq2Msg);

    STREAM_CHECK_RET_GOTO(scanSubmitData(pVnode, sStreamReaderInfo, pBody, bodyLen, NULL, resultRsp, wCont->version));
  }

  int32_t metaRows = resultRsp->totalRows - ((SSDataBlock*)resultRsp->dataBlock)->info.rows;
  STREAM_CHECK_RET_GOTO(filterData(resultRsp, sStreamReaderInfo));
  resultRsp->totalRows = ((SSDataBlock*)resultRsp->dataBlock)->info.rows + metaRows;

end:
  ST_TASK_DLOG("vgId:%d %s end, get result totalRows:%d, process:%"PRId64"/%"PRId64, TD_VID(pVnode), __func__, 
          resultRsp->totalRows, resultRsp->ver, walGetAppliedVer(pWalReader->pWal));
  walCloseReader(pWalReader);
  return code;
}

static int32_t processWalVerDataNew(SVnode* pVnode, SStreamTriggerReaderInfo* sStreamReaderInfo, 
                                    SArray* versions, SSHashObj* ranges, SSTriggerWalNewRsp* rsp) {
  int32_t      code = 0;
  int32_t      lino = 0;

  void* pTask = sStreamReaderInfo->pTask;
  SWalReader* pWalReader = walOpenReader(pVnode->pWal, 0);
  STREAM_CHECK_NULL_GOTO(pWalReader, terrno);
  
  if (taosArrayGetSize(versions) > 0) {
    rsp->ver = *(int64_t*)taosArrayGetLast(versions);
  }
  
  STREAM_CHECK_RET_GOTO(prepareIndexData(pWalReader, sStreamReaderInfo, versions, ranges, rsp));
  STREAM_CHECK_CONDITION_GOTO(rsp->totalRows == 0, TDB_CODE_SUCCESS);

  ST_TASK_TLOG("%s index hash:%p %d", __func__, rsp->indexHash, tSimpleHashGetSize(rsp->indexHash));
  buildIndexHash(rsp->indexHash, pTask);

  blockDataEmpty(rsp->dataBlock);
  STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(rsp->dataBlock, rsp->totalRows));

  for(int32_t i = 0; i < taosArrayGetSize(versions); i++) {
    int64_t *ver = taosArrayGet(versions, i);
    if (ver == NULL) continue;
    ST_TASK_TLOG("vgId:%d %s scan wal process:%"PRId64"/%"PRId64, TD_VID(pVnode), __func__, *ver, walGetAppliedVer(pWalReader->pWal));

    STREAM_CHECK_RET_GOTO(walFetchHead(pWalReader, *ver));
    if(pWalReader->pHead->head.msgType != TDMT_VND_SUBMIT) {
      TAOS_CHECK_RETURN(walSkipFetchBody(pWalReader));
      continue;
    }
    STREAM_CHECK_RET_GOTO(walFetchBody(pWalReader));
    SWalCont* wCont = &pWalReader->pHead->head;
    void*   pBody = POINTER_SHIFT(wCont->body, sizeof(SSubmitReq2Msg));
    int32_t bodyLen = wCont->bodyLen - sizeof(SSubmitReq2Msg);

    STREAM_CHECK_RET_GOTO(scanSubmitData(pVnode, sStreamReaderInfo, pBody, bodyLen, ranges, rsp, wCont->version));
  }
  // printDataBlock(rsp->dataBlock, __func__, "processWalVerDataNew");
  STREAM_CHECK_RET_GOTO(filterData(rsp, sStreamReaderInfo));
  rsp->totalRows = ((SSDataBlock*)rsp->dataBlock)->info.rows;

end:
  ST_TASK_DLOG("vgId:%d %s end, get result totalRows:%d, process:%"PRId64"/%"PRId64, TD_VID(pVnode), __func__, 
            rsp->totalRows, rsp->ver, walGetAppliedVer(pWalReader->pWal));
  walCloseReader(pWalReader);
  return code;
}

static int32_t buildScheamFromMeta(SVnode* pVnode, int64_t uid, SArray** schemas, SStorageAPI* api) {
  int32_t code = 0;
  int32_t lino = 0;
  SMetaReader metaReader = {0};
  *schemas = taosArrayInit(8, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(*schemas, terrno);
  
  api->metaReaderFn.initReader(&metaReader, pVnode, META_READER_LOCK, &api->metaFn);
  STREAM_CHECK_RET_GOTO(api->metaReaderFn.getTableEntryByUid(&metaReader, uid));

  SSchemaWrapper* sSchemaWrapper = NULL;
  if (metaReader.me.type == TD_CHILD_TABLE) {
    int64_t suid = metaReader.me.ctbEntry.suid;
    tDecoderClear(&metaReader.coder);
    STREAM_CHECK_RET_GOTO(api->metaReaderFn.getTableEntryByUid(&metaReader, suid));
    sSchemaWrapper = &metaReader.me.stbEntry.schemaRow;
  } else if (metaReader.me.type == TD_NORMAL_TABLE) {
    sSchemaWrapper = &metaReader.me.ntbEntry.schemaRow;
  } else {
    qError("invalid table type:%d", metaReader.me.type);
  }

  for (size_t j = 0; j < sSchemaWrapper->nCols; j++) {
    SSchema* s = sSchemaWrapper->pSchema + j;
    STREAM_CHECK_NULL_GOTO(taosArrayPush(*schemas, s), terrno);
  }

end:
  api->metaReaderFn.clearReader(&metaReader);
  STREAM_PRINT_LOG_END(code, lino);
  if (code != 0)  {
    taosArrayDestroy(*schemas);
    *schemas = NULL;
  }
  return code;
}

static int32_t shrinkScheams(SArray* cols, SArray* schemas) {
  int32_t code = 0;
  int32_t lino = 0;
  size_t  schemaLen = taosArrayGetSize(schemas);
  STREAM_CHECK_RET_GOTO(taosArrayEnsureCap(schemas, schemaLen + taosArrayGetSize(cols)));
  for (size_t i = 0; i < taosArrayGetSize(cols); i++) {
    col_id_t* id = taosArrayGet(cols, i);
    STREAM_CHECK_NULL_GOTO(id, terrno);
    for (size_t i = 0; i < schemaLen; i++) {
      SSchema* s = taosArrayGet(schemas, i);
      STREAM_CHECK_NULL_GOTO(s, terrno);
      if (*id == s->colId) {
        STREAM_CHECK_NULL_GOTO(taosArrayPush(schemas, s), terrno);
        break;
      }
    }
  }
  taosArrayPopFrontBatch(schemas, schemaLen);

end:
  return code;
}

static int32_t createTSAndCondition(int64_t start, int64_t end, SLogicConditionNode** pCond,
                                    STargetNode* pTargetNodeTs) {
  int32_t code = 0;
  int32_t lino = 0;

  SColumnNode*         pCol = NULL;
  SColumnNode*         pCol1 = NULL;
  SValueNode*          pVal = NULL;
  SValueNode*          pVal1 = NULL;
  SOperatorNode*       op = NULL;
  SOperatorNode*       op1 = NULL;
  SLogicConditionNode* cond = NULL;

  STREAM_CHECK_RET_GOTO(nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol));
  pCol->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  pCol->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
  pCol->node.resType.bytes = LONG_BYTES;
  pCol->slotId = pTargetNodeTs->slotId;
  pCol->dataBlockId = pTargetNodeTs->dataBlockId;

  STREAM_CHECK_RET_GOTO(nodesCloneNode((SNode*)pCol, (SNode**)&pCol1));

  STREAM_CHECK_RET_GOTO(nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pVal));
  pVal->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  pVal->node.resType.bytes = LONG_BYTES;
  pVal->datum.i = start;
  pVal->typeData = start;

  STREAM_CHECK_RET_GOTO(nodesCloneNode((SNode*)pVal, (SNode**)&pVal1));
  pVal1->datum.i = end;
  pVal1->typeData = end;

  STREAM_CHECK_RET_GOTO(nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&op));
  op->opType = OP_TYPE_GREATER_EQUAL;
  op->node.resType.type = TSDB_DATA_TYPE_BOOL;
  op->node.resType.bytes = CHAR_BYTES;
  op->pLeft = (SNode*)pCol;
  op->pRight = (SNode*)pVal;
  pCol = NULL;
  pVal = NULL;

  STREAM_CHECK_RET_GOTO(nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&op1));
  op1->opType = OP_TYPE_LOWER_EQUAL;
  op1->node.resType.type = TSDB_DATA_TYPE_BOOL;
  op1->node.resType.bytes = CHAR_BYTES;
  op1->pLeft = (SNode*)pCol1;
  op1->pRight = (SNode*)pVal1;
  pCol1 = NULL;
  pVal1 = NULL;

  STREAM_CHECK_RET_GOTO(nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, (SNode**)&cond));
  cond->condType = LOGIC_COND_TYPE_AND;
  cond->node.resType.type = TSDB_DATA_TYPE_BOOL;
  cond->node.resType.bytes = CHAR_BYTES;
  STREAM_CHECK_RET_GOTO(nodesMakeList(&cond->pParameterList));
  STREAM_CHECK_RET_GOTO(nodesListAppend(cond->pParameterList, (SNode*)op));
  op = NULL;
  STREAM_CHECK_RET_GOTO(nodesListAppend(cond->pParameterList, (SNode*)op1));
  op1 = NULL;

  *pCond = cond;

end:
  if (code != 0) {
    nodesDestroyNode((SNode*)pCol);
    nodesDestroyNode((SNode*)pCol1);
    nodesDestroyNode((SNode*)pVal);
    nodesDestroyNode((SNode*)pVal1);
    nodesDestroyNode((SNode*)op);
    nodesDestroyNode((SNode*)op1);
    nodesDestroyNode((SNode*)cond);
  }
  STREAM_PRINT_LOG_END(code, lino);

  return code;
}

/*
static int32_t createExternalConditions(SStreamRuntimeFuncInfo* data, SLogicConditionNode** pCond, STargetNode* pTargetNodeTs, STimeRangeNode* node) {
  int32_t              code = 0;
  int32_t              lino = 0;
  SLogicConditionNode* pAndCondition = NULL;
  SLogicConditionNode* cond = NULL;

  if (pTargetNodeTs == NULL) {
    vError("stream reader %s no ts column", __func__);
    return TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN;
  }
  STREAM_CHECK_RET_GOTO(nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, (SNode**)&cond));
  cond->condType = LOGIC_COND_TYPE_OR;
  cond->node.resType.type = TSDB_DATA_TYPE_BOOL;
  cond->node.resType.bytes = CHAR_BYTES;
  STREAM_CHECK_RET_GOTO(nodesMakeList(&cond->pParameterList));

  for (int i = 0; i < taosArrayGetSize(data->pStreamPesudoFuncVals); ++i) {
    data->curIdx = i;

    SReadHandle handle = {0};
    calcTimeRange(node, data, &handle.winRange, &handle.winRangeValid);
    if (!handle.winRangeValid) {
      stError("stream reader %s invalid time range, skey:%" PRId64 ", ekey:%" PRId64, __func__, handle.winRange.skey,
              handle.winRange.ekey);
      continue;
    }
    STREAM_CHECK_RET_GOTO(createTSAndCondition(handle.winRange.skey, handle.winRange.ekey, &pAndCondition, pTargetNodeTs));
    stDebug("%s create condition skey:%" PRId64 ", eksy:%" PRId64, __func__, handle.winRange.skey, handle.winRange.ekey);
    STREAM_CHECK_RET_GOTO(nodesListAppend(cond->pParameterList, (SNode*)pAndCondition));
    pAndCondition = NULL;
  }

  *pCond = cond;

end:
  if (code != 0) {
    nodesDestroyNode((SNode*)pAndCondition);
    nodesDestroyNode((SNode*)cond);
  }
  STREAM_PRINT_LOG_END(code, lino);

  return code;
}
*/

static int32_t processCalaTimeRange(SStreamTriggerReaderCalcInfo* sStreamReaderCalcInfo, SResFetchReq* req,
                                    STimeRangeNode* node, SReadHandle* handle) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* funcVals = NULL;
  if (req->pStRtFuncInfo->withExternalWindow) {
/*
    nodesDestroyNode(sStreamReaderCalcInfo->tsConditions);
    filterFreeInfo(sStreamReaderCalcInfo->pFilterInfo);
    sStreamReaderCalcInfo->pFilterInfo = NULL;

    STREAM_CHECK_RET_GOTO(createExternalConditions(req->pStRtFuncInfo,
                                                   (SLogicConditionNode**)&sStreamReaderCalcInfo->tsConditions,
                                                   sStreamReaderCalcInfo->pTargetNodeTs, node));

    STREAM_CHECK_RET_GOTO(filterInitFromNode((SNode*)sStreamReaderCalcInfo->tsConditions,
                                             (SFilterInfo**)&sStreamReaderCalcInfo->pFilterInfo,
                                             FLT_OPTION_NO_REWRITE | FLT_OPTION_SCALAR_MODE, NULL));
*/                                             
    sStreamReaderCalcInfo->tmpRtFuncInfo.curIdx = 0;
    sStreamReaderCalcInfo->tmpRtFuncInfo.triggerType = req->pStRtFuncInfo->triggerType;
    sStreamReaderCalcInfo->tmpRtFuncInfo.isWindowTrigger = req->pStRtFuncInfo->isWindowTrigger;
    sStreamReaderCalcInfo->tmpRtFuncInfo.precision = req->pStRtFuncInfo->precision;

    SSTriggerCalcParam* pFirst = taosArrayGet(req->pStRtFuncInfo->pStreamPesudoFuncVals, 0);
    SSTriggerCalcParam* pLast = taosArrayGetLast(req->pStRtFuncInfo->pStreamPesudoFuncVals);
    STREAM_CHECK_NULL_GOTO(pFirst, terrno);
    STREAM_CHECK_NULL_GOTO(pLast, terrno);

    if (!node->needCalc) {
      handle->winRange.skey = pFirst->wstart;
      handle->winRange.ekey = pLast->wend;
      handle->winRangeValid = true;
      if (req->pStRtFuncInfo->triggerType == STREAM_TRIGGER_SLIDING) {
        handle->winRange.ekey--;
      }
    } else {
      SSTriggerCalcParam* pTmp = taosArrayGet(sStreamReaderCalcInfo->tmpRtFuncInfo.pStreamPesudoFuncVals, 0);
      memcpy(pTmp, pFirst, sizeof(*pTmp));

      STREAM_CHECK_RET_GOTO(streamCalcCurrWinTimeRange(node, &sStreamReaderCalcInfo->tmpRtFuncInfo, &handle->winRange, &handle->winRangeValid, 1));
      if (handle->winRangeValid) {
        int64_t skey = handle->winRange.skey;

        memcpy(pTmp, pLast, sizeof(*pTmp));
        STREAM_CHECK_RET_GOTO(streamCalcCurrWinTimeRange(node, &sStreamReaderCalcInfo->tmpRtFuncInfo, &handle->winRange, &handle->winRangeValid, 2));

        if (handle->winRangeValid) {
          handle->winRange.skey = skey;
        }
      }
      handle->winRange.ekey--;
    }
  } else {
    if (!node->needCalc) {
      SSTriggerCalcParam* pCurr = taosArrayGet(req->pStRtFuncInfo->pStreamPesudoFuncVals, req->pStRtFuncInfo->curIdx);
      handle->winRange.skey = pCurr->wstart;
      handle->winRange.ekey = pCurr->wend;
      handle->winRangeValid = true;
      if (req->pStRtFuncInfo->triggerType == STREAM_TRIGGER_SLIDING) {
        handle->winRange.ekey--;
      }
    } else {
      STREAM_CHECK_RET_GOTO(streamCalcCurrWinTimeRange(node, req->pStRtFuncInfo, &handle->winRange, &handle->winRangeValid, 3));
      handle->winRange.ekey--;
    }
  }

  stDebug("%s withExternalWindow is %d, skey:%" PRId64 ", ekey:%" PRId64 ", validRange:%d", 
      __func__, req->pStRtFuncInfo->withExternalWindow, handle->winRange.skey, handle->winRange.ekey, handle->winRangeValid);

end:
  taosArrayDestroy(funcVals);
  return code;
}

static int32_t createDataBlockTsUid(SSDataBlock** pBlockRet, uint32_t numOfRows) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SSDataBlock* pBlock = NULL;
  STREAM_CHECK_RET_GOTO(createDataBlock(&pBlock));
  SColumnInfoData idata = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, LONG_BYTES, PRIMARYKEY_TIMESTAMP_COL_ID);
  STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(pBlock, &idata));
  idata = createColumnInfoData(TSDB_DATA_TYPE_BIGINT, LONG_BYTES, PRIMARYKEY_TIMESTAMP_COL_ID + 1);
  STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(pBlock, &idata));
  STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(pBlock, numOfRows));

end:
  STREAM_PRINT_LOG_END(code, lino)
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    pBlock = NULL;
  }
  *pBlockRet = pBlock;
  return code;
}

static int32_t processTsOutPutAllTables(SStreamTriggerReaderInfo* sStreamReaderInfo, SStreamTsResponse* tsRsp, SSDataBlock* pResBlock, int32_t order) {
  int32_t code = 0;
  int32_t lino = 0;
  void* pTask = sStreamReaderInfo->pTask;

  tsRsp->tsInfo = taosArrayInit(pResBlock->info.rows, sizeof(STsInfo));
  STREAM_CHECK_NULL_GOTO(tsRsp->tsInfo, terrno);
  SColumnInfoData* pColInfoDataTs = taosArrayGet(pResBlock->pDataBlock, 0);
  SColumnInfoData* pColInfoDataUid = taosArrayGet(pResBlock->pDataBlock, 1);
  for (int32_t j = 0; j < pResBlock->info.rows; j++) {
    if (colDataIsNull_s(pColInfoDataTs, j) || pColInfoDataTs->pData == NULL) {
      continue;
    }
    STsInfo* tsInfo = taosArrayReserve(tsRsp->tsInfo, 1);
    STREAM_CHECK_NULL_GOTO(tsInfo, terrno)
    if (order == TSDB_ORDER_ASC) {
      tsInfo->ts = INT64_MAX;
    } else {
      tsInfo->ts = INT64_MIN;
    }
    int64_t ts = *(int64_t*)colDataGetNumData(pColInfoDataTs, j);
    if (order == TSDB_ORDER_ASC && ts < tsInfo->ts) {
      tsInfo->ts = ts;
    } else if (order == TSDB_ORDER_DESC && ts > tsInfo->ts) {
      tsInfo->ts = ts;
    }
    tsInfo->gId = *(int64_t*)colDataGetNumData(pColInfoDataUid, j);
    ST_TASK_DLOG("%s get ts:%" PRId64 ", gId:%" PRIu64 ", ver:%" PRId64, __func__, tsInfo->ts, tsInfo->gId, tsRsp->ver);
  }

end:
  return code;
}

static int32_t processTsOutPutOneGroup(SStreamTriggerReaderInfo* sStreamReaderInfo, SStreamTsResponse* tsRsp, SSDataBlock* pResBlock, int32_t order) {
  int32_t code = 0;
  int32_t lino = 0;
  void* pTask = sStreamReaderInfo->pTask;

  tsRsp->tsInfo = taosArrayInit(1, sizeof(STsInfo));
  STREAM_CHECK_NULL_GOTO(tsRsp->tsInfo, terrno);
  STsInfo* tsInfo = taosArrayReserve(tsRsp->tsInfo, 1);
  STREAM_CHECK_NULL_GOTO(tsInfo, terrno)
  if (order == TSDB_ORDER_ASC) {
    tsInfo->ts = INT64_MAX;
  } else {
    tsInfo->ts = INT64_MIN;
  }

  SColumnInfoData* pColInfoDataTs = taosArrayGet(pResBlock->pDataBlock, 0);
  SColumnInfoData* pColInfoDataUid = taosArrayGet(pResBlock->pDataBlock, 1);
  for (int32_t j = 0; j < pResBlock->info.rows; j++) {
    if (colDataIsNull_s(pColInfoDataTs, j) || pColInfoDataTs->pData == NULL) {
      continue;
    }
    int64_t ts = *(int64_t*)colDataGetNumData(pColInfoDataTs, j);
    if (order == TSDB_ORDER_ASC && ts < tsInfo->ts) {
      tsInfo->ts = ts;
    } else if (order == TSDB_ORDER_DESC && ts > tsInfo->ts) {
      tsInfo->ts = ts;
    }
  }
  int64_t uid = *(int64_t*)colDataGetNumData(pColInfoDataUid, 0);
  tsInfo->gId = qStreamGetGroupIdFromSet(sStreamReaderInfo, uid);
  ST_TASK_DLOG("%s get ts:%" PRId64 ", gId:%" PRIu64 ", ver:%" PRId64, __func__, tsInfo->ts, tsInfo->gId, tsRsp->ver);

end:
  return code;
}

static int32_t processTsOutPutAllGroups(SStreamTriggerReaderInfo* sStreamReaderInfo, SStreamTsResponse* tsRsp, SSDataBlock* pResBlock, int32_t order) {
  int32_t code = 0;
  int32_t lino = 0;
  STableKeyInfo* pList = NULL;
  StreamTableListInfo     tableInfo = {0};

  void* pTask = sStreamReaderInfo->pTask;
  STREAM_CHECK_RET_GOTO(qStreamCopyTableInfo(sStreamReaderInfo, &tableInfo));

  SSHashObj*   uidTsHash = tSimpleHashInit(pResBlock->info.rows, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  STREAM_CHECK_NULL_GOTO(uidTsHash, terrno);
  SColumnInfoData* pColInfoDataTs = taosArrayGet(pResBlock->pDataBlock, 0);
  SColumnInfoData* pColInfoDataUid = taosArrayGet(pResBlock->pDataBlock, 1);
  for (int32_t j = 0; j < pResBlock->info.rows; j++) {
    if (colDataIsNull_s(pColInfoDataTs, j) || pColInfoDataTs->pData == NULL) {
      continue;
    }
    int64_t ts = *(int64_t*)colDataGetNumData(pColInfoDataTs, j);
    int64_t uid = *(int64_t*)colDataGetNumData(pColInfoDataUid, j);
    STREAM_CHECK_RET_GOTO(tSimpleHashPut(uidTsHash, &uid, LONG_BYTES, &ts, LONG_BYTES));
  }
  tsRsp->tsInfo = taosArrayInit(qStreamGetTableListGroupNum(sStreamReaderInfo), sizeof(STsInfo));
  STREAM_CHECK_NULL_GOTO(tsRsp->tsInfo, terrno);
  while (true) {
    int32_t        pNum = 0;
    int64_t        suid = 0;
    STREAM_CHECK_RET_GOTO(qStreamIterTableList(&tableInfo, &pList, &pNum, &suid));
    if(pNum == 0) break;
    STsInfo* tsInfo = taosArrayReserve(tsRsp->tsInfo, 1);
    STREAM_CHECK_NULL_GOTO(tsInfo, terrno)
    if (order == TSDB_ORDER_ASC) {
      tsInfo->ts = INT64_MAX;
    } else {
      tsInfo->ts = INT64_MIN;
    }
    for (int32_t i = 0; i < pNum; i++) {
      int64_t uid = pList[i].uid;
      int64_t *ts = tSimpleHashGet(uidTsHash, &uid, LONG_BYTES);
      STREAM_CHECK_NULL_GOTO(ts, terrno);
      if (order == TSDB_ORDER_ASC && *ts < tsInfo->ts) {
        tsInfo->ts = *ts;
      } else if (order == TSDB_ORDER_DESC && *ts > tsInfo->ts) {
        tsInfo->ts = *ts;
      }
    }
    int64_t uid = pList[0].uid;
    tsInfo->gId = qStreamGetGroupIdFromSet(sStreamReaderInfo, uid);
    ST_TASK_DLOG("%s get ts:%" PRId64 ", gId:%" PRIu64 ", ver:%" PRId64, __func__, tsInfo->ts, tsInfo->gId, tsRsp->ver);
    taosMemoryFreeClear(pList);
  }

end:
  qStreamDestroyTableInfo(&tableInfo);
  taosMemoryFreeClear(pList);
  tSimpleHashCleanup(uidTsHash);
  return code;
}

// static bool stReaderTaskWaitQuit(SStreamTask* pTask) { return taosHasRWWFlag(&pTask->entryLock); }

static int32_t getAllTs(SVnode* pVnode, SSDataBlock*  pResBlock, SStreamReaderTaskInner* pTaskInner, STableKeyInfo* pList, int32_t pNum) {
  int32_t code = 0;
  int32_t lino = 0;

  STREAM_CHECK_RET_GOTO(pTaskInner->storageApi->tsdReader.tsdCreateFirstLastTsIter(pVnode, &pTaskInner->options->twindows, &(SVersionRange){.minVer = -1, .maxVer = pTaskInner->options->ver},
                                                pTaskInner->options->suid, pList, pNum, pTaskInner->options->order, &pTaskInner->pReader, pTaskInner->idStr));
  bool hasNext = true;
  while(1){
    STREAM_CHECK_RET_GOTO(pTaskInner->storageApi->tsdReader.tsdNextFirstLastTsBlock(pTaskInner->pReader, pResBlock, &hasNext));
    STREAM_CHECK_CONDITION_GOTO(!hasNext, TDB_CODE_SUCCESS);
  }

end:
  pTaskInner->storageApi->tsdReader.tsdDestroyFirstLastTsIter(pTaskInner->pReader);
  pTaskInner->pReader = NULL;
  return code;
}

static int32_t processTsVTable(SVnode* pVnode, SStreamTsResponse* tsRsp, SStreamTriggerReaderInfo* sStreamReaderInfo,
                                  SStreamReaderTaskInner* pTaskInner) {
  int32_t code = 0;
  int32_t lino = 0;
  STableKeyInfo* pList = NULL;
  StreamTableListInfo     tableInfo = {0};

  void* pTask = sStreamReaderInfo->pTask;
  STREAM_CHECK_RET_GOTO(qStreamCopyTableInfo(sStreamReaderInfo, &tableInfo));

  SSDataBlock*  pResBlock = NULL;
  STREAM_CHECK_RET_GOTO(createDataBlockTsUid(&pResBlock, qStreamGetTableListNum(sStreamReaderInfo)));

  while (true) {
    int32_t        pNum = 0;
    int64_t        suid = 0;
    STREAM_CHECK_RET_GOTO(qStreamIterTableList(&tableInfo, &pList, &pNum, &suid));
    if(pNum == 0) break;
    pTaskInner->options->suid = suid;
    STREAM_CHECK_RET_GOTO(getAllTs(pVnode, pResBlock, pTaskInner, pList, pNum));
    taosMemoryFreeClear(pList);
  }

  STREAM_CHECK_RET_GOTO(processTsOutPutAllTables(sStreamReaderInfo, tsRsp, pResBlock, pTaskInner->options->order));

end:
  qStreamDestroyTableInfo(&tableInfo);
  taosMemoryFreeClear(pList);
  blockDataDestroy(pResBlock);
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  return code;
}

static int32_t processTsNonVTable(SVnode* pVnode, SStreamTsResponse* tsRsp, SStreamTriggerReaderInfo* sStreamReaderInfo,
                                  SStreamReaderTaskInner* pTaskInner) {
  int32_t code = 0;
  int32_t lino = 0;
  STableKeyInfo* pList = NULL;
  void* pTask = sStreamReaderInfo->pTask;
  
  SSDataBlock*  pResBlock = NULL;

  int32_t        pNum = 0;
  int64_t        suid = 0;
  STREAM_CHECK_RET_GOTO(qStreamGetTableList(sStreamReaderInfo, 0, &pList, &pNum));
  STREAM_CHECK_CONDITION_GOTO(pNum == 0, TSDB_CODE_SUCCESS);
  STREAM_CHECK_RET_GOTO(createDataBlockTsUid(&pResBlock, pNum));

  pTaskInner->options->suid = sStreamReaderInfo->suid;
  STREAM_CHECK_RET_GOTO(getAllTs(pVnode, pResBlock, pTaskInner, pList, pNum));
  STREAM_CHECK_CONDITION_GOTO(pResBlock->info.rows == 0, TDB_CODE_SUCCESS);
  int32_t order = pTaskInner->options->order;

  if (sStreamReaderInfo->groupByTbname) {
    STREAM_CHECK_RET_GOTO(processTsOutPutAllTables(sStreamReaderInfo, tsRsp, pResBlock, order));
  } else if (sStreamReaderInfo->partitionCols == NULL) {
    STREAM_CHECK_RET_GOTO(processTsOutPutOneGroup(sStreamReaderInfo, tsRsp, pResBlock, order));
  } else {
    STREAM_CHECK_RET_GOTO(processTsOutPutAllGroups(sStreamReaderInfo, tsRsp, pResBlock, order));
  }                             
end:
  blockDataDestroy(pResBlock);
  taosMemoryFreeClear(pList);
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  return code;
}

static int32_t processTsOnce(SVnode* pVnode, SStreamTsResponse* tsRsp, SStreamTriggerReaderInfo* sStreamReaderInfo,
                                  SStreamReaderTaskInner* pTaskInner, uint64_t gid) {
  int32_t code = 0;
  int32_t lino = 0;
  STableKeyInfo* pList = NULL;
  void* pTask = sStreamReaderInfo->pTask;
  
  SSDataBlock*  pResBlock = NULL;

  int32_t        pNum = 0;
  STREAM_CHECK_RET_GOTO(qStreamGetTableList(sStreamReaderInfo, gid, &pList, &pNum));
  STREAM_CHECK_CONDITION_GOTO(pNum == 0, TSDB_CODE_SUCCESS);
  STREAM_CHECK_RET_GOTO(createDataBlockTsUid(&pResBlock, pNum));

  pTaskInner->options->suid = sStreamReaderInfo->suid;
  STREAM_CHECK_RET_GOTO(getAllTs(pVnode, pResBlock, pTaskInner, pList, pNum));
  STREAM_CHECK_CONDITION_GOTO(pResBlock->info.rows == 0, TDB_CODE_SUCCESS);
  int32_t order = pTaskInner->options->order;

  STREAM_CHECK_RET_GOTO(processTsOutPutOneGroup(sStreamReaderInfo, tsRsp, pResBlock, order));
end:
  blockDataDestroy(pResBlock);
  taosMemoryFreeClear(pList);
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  return code;
}

static int32_t processTs(SVnode* pVnode, SStreamTsResponse* tsRsp, SStreamTriggerReaderInfo* sStreamReaderInfo,
                                  SStreamReaderTaskInner* pTaskInner) {
  if (sStreamReaderInfo->isVtableStream) {
    return processTsVTable(pVnode, tsRsp, sStreamReaderInfo, pTaskInner);
  }

  return processTsNonVTable(pVnode, tsRsp, sStreamReaderInfo, pTaskInner);
}

static int32_t vnodeProcessStreamSetTableReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  size_t  size = 0;
  void* pTask = sStreamReaderInfo->pTask;

  ST_TASK_DLOG("vgId:%d %s start, trigger hash size:%d, calc hash size:%d", TD_VID(pVnode), __func__,
                tSimpleHashGetSize(req->setTableReq.uidInfoTrigger), tSimpleHashGetSize(req->setTableReq.uidInfoCalc));

  taosWLockLatch(&sStreamReaderInfo->lock);
  TSWAP(sStreamReaderInfo->uidHashTrigger, req->setTableReq.uidInfoTrigger);
  TSWAP(sStreamReaderInfo->uidHashCalc, req->setTableReq.uidInfoCalc);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->uidHashTrigger, TSDB_CODE_INVALID_PARA);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->uidHashCalc, TSDB_CODE_INVALID_PARA);

  STREAM_CHECK_RET_GOTO(initStreamTableListInfo(&sStreamReaderInfo->vSetTableList));
  STREAM_CHECK_RET_GOTO(qBuildVTableList(sStreamReaderInfo));
end:
  taosWUnLockLatch(&sStreamReaderInfo->lock);
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  return code;
}

static int32_t vnodeProcessStreamLastTsReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SStreamReaderTaskInner* pTaskInner = NULL;
  SStreamTsResponse       tsRsp = {0};
  void*                   buf = NULL;
  size_t                  size = 0;

  void* pTask = sStreamReaderInfo->pTask;

  ST_TASK_DLOG("vgId:%d %s start", TD_VID(pVnode), __func__);

  BUILD_OPTION(options, 0, -1, TSDB_ORDER_DESC, INT64_MIN, INT64_MAX, NULL, false, NULL);
  STREAM_CHECK_RET_GOTO(createStreamTaskForTs(&options, &pTaskInner, &sStreamReaderInfo->storageApi));

  tsRsp.ver = pVnode->state.applied + 1;

  STREAM_CHECK_RET_GOTO(processTs(pVnode, &tsRsp, sStreamReaderInfo, pTaskInner));
  
end:
  ST_TASK_DLOG("vgId:%d %s get result size:%"PRIzu", ver:%"PRId64, TD_VID(pVnode), __func__, taosArrayGetSize(tsRsp.tsInfo), tsRsp.ver);
  code = buildTsRsp(&tsRsp, &buf, &size);
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  taosArrayDestroy(tsRsp.tsInfo);
  taosMemoryFree(pTaskInner);
  return code;
}

static int32_t vnodeProcessStreamFirstTsReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SStreamReaderTaskInner* pTaskInner = NULL;
  SStreamTsResponse       tsRsp = {0};
  void*                   buf = NULL;
  size_t                  size = 0;

  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start, startTime:%"PRId64" ver:%"PRId64" gid:%"PRId64, TD_VID(pVnode), __func__, req->firstTsReq.startTime, req->firstTsReq.ver, req->firstTsReq.gid);
  int32_t        pNum = 0;

  tsRsp.ver = pVnode->state.applied;

  BUILD_OPTION(options, 0, req->firstTsReq.ver, TSDB_ORDER_ASC, req->firstTsReq.startTime, INT64_MAX, NULL, false, NULL);
  STREAM_CHECK_RET_GOTO(createStreamTaskForTs(&options, &pTaskInner, &sStreamReaderInfo->storageApi));

  if (req->firstTsReq.gid != 0) {
    STREAM_CHECK_RET_GOTO(processTsOnce(pVnode, &tsRsp, sStreamReaderInfo, pTaskInner, req->firstTsReq.gid));
  } else {
    STREAM_CHECK_RET_GOTO(processTs(pVnode, &tsRsp, sStreamReaderInfo, pTaskInner));
  }

end:
  ST_TASK_DLOG("vgId:%d %s get result size:%"PRIzu", ver:%"PRId64, TD_VID(pVnode), __func__, taosArrayGetSize(tsRsp.tsInfo), tsRsp.ver);
  code = buildTsRsp(&tsRsp, &buf, &size);
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  taosArrayDestroy(tsRsp.tsInfo);
  taosMemoryFree(pTaskInner);
  return code;
}

static int32_t vnodeProcessStreamTsdbMetaReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  size_t  size = 0;
  STableKeyInfo* pList = NULL;

  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start", TD_VID(pVnode), __func__);

  SStreamReaderTaskInner* pTaskInner = NULL;
  int64_t                 key = getSessionKey(req->base.sessionId, STRIGGER_PULL_TSDB_META);

  if (req->base.type == STRIGGER_PULL_TSDB_META) {
    int32_t        pNum = 0;
    STREAM_CHECK_RET_GOTO(qStreamGetTableList(sStreamReaderInfo, req->tsdbMetaReq.gid, &pList, &pNum));
    BUILD_OPTION(options, getSuid(sStreamReaderInfo, pList), req->tsdbMetaReq.ver, req->tsdbMetaReq.order, req->tsdbMetaReq.startTime, req->tsdbMetaReq.endTime, 
                          sStreamReaderInfo->tsSchemas, true, NULL);
    STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTaskInner, NULL, pList, pNum, &sStreamReaderInfo->storageApi));
    STREAM_CHECK_RET_GOTO(taosHashPut(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES, &pTaskInner, sizeof(pTaskInner)));
    
    STREAM_CHECK_RET_GOTO(createBlockForTsdbMeta(&pTaskInner->pResBlockDst, sStreamReaderInfo->isVtableStream));
  } else {
    void** tmp = taosHashGet(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES);
    STREAM_CHECK_NULL_GOTO(tmp, TSDB_CODE_STREAM_NO_CONTEXT);
    pTaskInner = *(SStreamReaderTaskInner**)tmp;
    STREAM_CHECK_NULL_GOTO(pTaskInner, TSDB_CODE_INTERNAL_ERROR);
  }

  blockDataCleanup(pTaskInner->pResBlockDst);
  STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(pTaskInner->pResBlockDst, STREAM_RETURN_ROWS_NUM));
  bool hasNext = true;
  while (true) {
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTaskInner, &hasNext));
    if (!hasNext) {
      break;
    }
    pTaskInner->storageApi->tsdReader.tsdReaderReleaseDataBlock(pTaskInner->pReader);
    pTaskInner->pResBlock->info.id.groupId = qStreamGetGroupIdFromSet(sStreamReaderInfo, pTaskInner->pResBlock->info.id.uid);

    int32_t index = 0;
    STREAM_CHECK_RET_GOTO(addColData(pTaskInner->pResBlockDst, index++, &pTaskInner->pResBlock->info.window.skey));
    STREAM_CHECK_RET_GOTO(addColData(pTaskInner->pResBlockDst, index++, &pTaskInner->pResBlock->info.window.ekey));
    STREAM_CHECK_RET_GOTO(addColData(pTaskInner->pResBlockDst, index++, &pTaskInner->pResBlock->info.id.uid));
    if (!sStreamReaderInfo->isVtableStream) {
      STREAM_CHECK_RET_GOTO(addColData(pTaskInner->pResBlockDst, index++, &pTaskInner->pResBlock->info.id.groupId));
    }
    STREAM_CHECK_RET_GOTO(addColData(pTaskInner->pResBlockDst, index++, &pTaskInner->pResBlock->info.rows));

    stDebug("vgId:%d %s get  skey:%" PRId64 ", eksy:%" PRId64 ", uid:%" PRId64 ", gId:%" PRIu64 ", rows:%" PRId64,
            TD_VID(pVnode), __func__, pTaskInner->pResBlock->info.window.skey, pTaskInner->pResBlock->info.window.ekey,
            pTaskInner->pResBlock->info.id.uid, pTaskInner->pResBlock->info.id.groupId, pTaskInner->pResBlock->info.rows);
            pTaskInner->pResBlockDst->info.rows++;
    if (pTaskInner->pResBlockDst->info.rows >= STREAM_RETURN_ROWS_NUM) {
      break;
    }
  }

  ST_TASK_DLOG("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pTaskInner->pResBlockDst->info.rows);
  STREAM_CHECK_RET_GOTO(buildRsp(pTaskInner->pResBlockDst, &buf, &size));
  printDataBlock(pTaskInner->pResBlockDst, __func__, "meta", ((SStreamTask *)sStreamReaderInfo->pTask)->streamId);
  if (!hasNext) {
    STREAM_CHECK_RET_GOTO(taosHashRemove(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES));
  }

end:
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  taosMemoryFree(pList);
  return code;
}

static int32_t vnodeProcessStreamTsdbTsDataReqNonVTable(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SStreamReaderTaskInner* pTaskInner = NULL;
  void*                   buf = NULL;
  size_t                  size = 0;
  SSDataBlock*            pBlockRes = NULL;

  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start, ver:%"PRId64",skey:%"PRId64",ekey:%"PRId64",uid:%"PRId64",suid:%"PRId64, TD_VID(pVnode), __func__, req->tsdbTsDataReq.ver, 
                req->tsdbTsDataReq.skey, req->tsdbTsDataReq.ekey, 
                req->tsdbTsDataReq.uid, req->tsdbTsDataReq.suid);

  int32_t        pNum = 1;
  STableKeyInfo  pList = {.groupId = qStreamGetGroupIdFromSet(sStreamReaderInfo, req->tsdbTsDataReq.uid), .uid = req->tsdbTsDataReq.uid};
  STREAM_CHECK_CONDITION_GOTO(pList.groupId == -1, TSDB_CODE_INVALID_PARA);
  BUILD_OPTION(options, getSuid(sStreamReaderInfo, &pList), req->tsdbTsDataReq.ver, TSDB_ORDER_ASC, req->tsdbTsDataReq.skey, req->tsdbTsDataReq.ekey,
               sStreamReaderInfo->triggerCols, false, NULL);
  STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTaskInner, sStreamReaderInfo->triggerResBlock, &pList, pNum, &sStreamReaderInfo->storageApi));
  STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamReaderInfo->triggerResBlock, false, &pTaskInner->pResBlockDst));
  STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamReaderInfo->tsBlock, false, &pBlockRes));

  while (1) {
    bool hasNext = false;
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTaskInner, &hasNext));
    if (!hasNext) {
      break;
    }
    // if (!sStreamReaderInfo->isVtableStream){
    pTaskInner->pResBlock->info.id.groupId = qStreamGetGroupIdFromSet(sStreamReaderInfo, pTaskInner->pResBlock->info.id.uid);
    // }

    SSDataBlock* pBlock = NULL;
    STREAM_CHECK_RET_GOTO(getTableData(pTaskInner, &pBlock));
    if (pBlock != NULL && pBlock->info.rows > 0) {
      STREAM_CHECK_RET_GOTO(processTag(pVnode, sStreamReaderInfo, false, pBlock->info.id.uid, pBlock,
          0, pBlock->info.rows, 1));
    }
    
    STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock, sStreamReaderInfo->pFilterInfo, NULL));
    STREAM_CHECK_RET_GOTO(blockDataMerge(pTaskInner->pResBlockDst, pBlock));
    ST_TASK_DLOG("vgId:%d %s get  skey:%" PRId64 ", eksy:%" PRId64 ", uid:%" PRId64 ", gId:%" PRIu64 ", rows:%" PRId64,
            TD_VID(pVnode), __func__, pTaskInner->pResBlock->info.window.skey, pTaskInner->pResBlock->info.window.ekey,
            pTaskInner->pResBlock->info.id.uid, pTaskInner->pResBlock->info.id.groupId, pTaskInner->pResBlock->info.rows);
  }

  blockDataTransform(pBlockRes, pTaskInner->pResBlockDst);

  ST_TASK_DLOG("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pTaskInner->pResBlockDst->info.rows);
  STREAM_CHECK_RET_GOTO(buildRsp(pBlockRes, &buf, &size));

end:
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  blockDataDestroy(pBlockRes);

  releaseStreamTask(&pTaskInner);
  return code;
}

static int32_t vnodeProcessStreamTsdbTsDataReqVTable(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SStreamReaderTaskInner* pTaskInner = NULL;
  void*                   buf = NULL;
  size_t                  size = 0;
  SSDataBlock*            pBlockRes = NULL;

  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_ELOG("vgId:%d %s start, ver:%"PRId64",skey:%"PRId64",ekey:%"PRId64",uid:%"PRId64",suid:%"PRId64, TD_VID(pVnode), __func__, req->tsdbTsDataReq.ver, 
                req->tsdbTsDataReq.skey, req->tsdbTsDataReq.ekey, 
                req->tsdbTsDataReq.uid, req->tsdbTsDataReq.suid);

  int32_t        pNum = 1;
  STableKeyInfo  pList = {.groupId = qStreamGetGroupIdFromSet(sStreamReaderInfo, req->tsdbTsDataReq.uid), .uid = req->tsdbTsDataReq.uid};
  STREAM_CHECK_CONDITION_GOTO(pList.groupId == -1, TSDB_CODE_INVALID_PARA);
  BUILD_OPTION(options, getSuid(sStreamReaderInfo, &pList), req->tsdbTsDataReq.ver, TSDB_ORDER_ASC, req->tsdbTsDataReq.skey, req->tsdbTsDataReq.ekey,
               sStreamReaderInfo->tsSchemas, true, NULL);
  STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTaskInner, sStreamReaderInfo->tsBlock, &pList, pNum, &sStreamReaderInfo->storageApi));
  STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamReaderInfo->tsBlock, false, &pBlockRes));

  while (1) {
    bool hasNext = false;
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTaskInner, &hasNext));
    if (!hasNext) {
      break;
    }

    SSDataBlock* pBlock = NULL;
    STREAM_CHECK_RET_GOTO(getTableData(pTaskInner, &pBlock));
    STREAM_CHECK_RET_GOTO(blockDataMerge(pBlockRes, pBlock));
    ST_TASK_DLOG("vgId:%d %s get  skey:%" PRId64 ", eksy:%" PRId64 ", uid:%" PRId64 ", gId:%" PRIu64 ", rows:%" PRId64,
            TD_VID(pVnode), __func__, pBlockRes->info.window.skey, pBlockRes->info.window.ekey,
            pBlockRes->info.id.uid, pBlockRes->info.id.groupId, pBlockRes->info.rows);
  }

  ST_TASK_DLOG("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlockRes->info.rows);
  STREAM_CHECK_RET_GOTO(buildRsp(pBlockRes, &buf, &size));

end:
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  blockDataDestroy(pBlockRes);

  releaseStreamTask(&pTaskInner);
  return code;
}

static int32_t vnodeProcessStreamTsdbTriggerDataReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  size_t  size = 0;
  STableKeyInfo* pList = NULL;
  SArray*        pResList = NULL;
  SSDataBlock*   pBlockTmp = NULL;

  SStreamReaderTaskInner* pTaskInner = NULL;
  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start. ver:%"PRId64",order:%d,startTs:%"PRId64",gid:%"PRId64, TD_VID(pVnode), __func__, req->tsdbTriggerDataReq.ver, req->tsdbTriggerDataReq.order, req->tsdbTriggerDataReq.startTime, req->tsdbTriggerDataReq.gid);
  
  int64_t                 key = getSessionKey(req->base.sessionId, STRIGGER_PULL_TSDB_TRIGGER_DATA);

  if (req->base.type == STRIGGER_PULL_TSDB_TRIGGER_DATA) {
    int32_t        pNum = 0;
    STREAM_CHECK_RET_GOTO(qStreamGetTableList(sStreamReaderInfo, req->tsdbTriggerDataReq.gid, &pList, &pNum));
    BUILD_OPTION(options, getSuid(sStreamReaderInfo, pList), req->tsdbTriggerDataReq.ver, req->tsdbTriggerDataReq.order, req->tsdbTriggerDataReq.startTime, INT64_MAX,
                 sStreamReaderInfo->triggerCols, false, NULL);
    STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTaskInner, sStreamReaderInfo->triggerResBlock, pList, pNum, &sStreamReaderInfo->storageApi));
    STREAM_CHECK_RET_GOTO(taosHashPut(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES, &pTaskInner, sizeof(pTaskInner)));
  } else {
    void** tmp = taosHashGet(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES);
    STREAM_CHECK_NULL_GOTO(tmp, TSDB_CODE_STREAM_NO_CONTEXT);
    pTaskInner = *(SStreamReaderTaskInner**)tmp;
    STREAM_CHECK_NULL_GOTO(pTaskInner, TSDB_CODE_INTERNAL_ERROR);
  }

  blockDataCleanup(pTaskInner->pResBlockDst);
  bool hasNext = true;
  int32_t totalRows = 0;
    
  pResList = taosArrayInit(4, POINTER_BYTES);
  STREAM_CHECK_NULL_GOTO(pResList, terrno);
  while (1) {
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTaskInner, &hasNext));
    if (!hasNext) {
      break;
    }
    pTaskInner->pResBlock->info.id.groupId = qStreamGetGroupIdFromSet(sStreamReaderInfo, pTaskInner->pResBlock->info.id.uid);
    // pTaskInner->pResBlockDst->info.id.groupId = pTaskInner->pResBlock->info.id.groupId;

    SSDataBlock* pBlock = NULL;
    STREAM_CHECK_RET_GOTO(getTableData(pTaskInner, &pBlock));
    if (pBlock != NULL && pBlock->info.rows > 0) {
      STREAM_CHECK_RET_GOTO(
        processTag(pVnode, sStreamReaderInfo, false, pBlock->info.id.uid, pBlock, 0, pBlock->info.rows, 1));
    }
    STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock, sStreamReaderInfo->pFilterInfo, NULL));
    // STREAM_CHECK_RET_GOTO(blockDataMerge(pTaskInner->pResBlockDst, pBlock));
    ST_TASK_DLOG("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock->info.rows);
    STREAM_CHECK_RET_GOTO(createOneDataBlock(pBlock, true, &pBlockTmp));
    STREAM_CHECK_NULL_GOTO(taosArrayPush(pResList, &pBlockTmp), terrno);
    totalRows += blockDataGetNumOfRows(pBlockTmp);
    pBlockTmp = NULL;

    ST_TASK_DLOG("vgId:%d %s get skey:%" PRId64 ", eksy:%" PRId64 ", uid:%" PRId64 ", gId:%" PRIu64 ", rows:%" PRId64,
            TD_VID(pVnode), __func__, pTaskInner->pResBlock->info.window.skey, pTaskInner->pResBlock->info.window.ekey,
            pTaskInner->pResBlock->info.id.uid, pTaskInner->pResBlock->info.id.groupId, pTaskInner->pResBlock->info.rows);
    if (totalRows >= STREAM_RETURN_ROWS_NUM) {  //todo optimize send multi blocks in one group
      break;
    }
  }

  STREAM_CHECK_RET_GOTO(buildArrayRsp(pResList, &buf, &size));
  if (!hasNext) {
    STREAM_CHECK_RET_GOTO(taosHashRemove(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES));
  }

end:
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  taosMemoryFree(pList);
  blockDataDestroy(pBlockTmp);
  taosArrayDestroyP(pResList, (FDelete)blockDataDestroy);
  return code;
}

static int32_t vnodeProcessStreamTsdbCalcDataReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  size_t  size = 0;
  SSDataBlock*   pBlockRes = NULL;
  STableKeyInfo* pList = NULL;


  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start, skey:%"PRId64",ekey:%"PRId64",gid:%"PRId64, TD_VID(pVnode), __func__, 
    req->tsdbCalcDataReq.skey, req->tsdbCalcDataReq.ekey, req->tsdbCalcDataReq.gid);

  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->triggerCols, TSDB_CODE_STREAM_NOT_TABLE_SCAN_PLAN);

  SStreamReaderTaskInner* pTaskInner = NULL;
  int64_t                 key = getSessionKey(req->base.sessionId, STRIGGER_PULL_TSDB_CALC_DATA);

  if (req->base.type == STRIGGER_PULL_TSDB_CALC_DATA) {
    int32_t        pNum = 0;
    STREAM_CHECK_RET_GOTO(qStreamGetTableList(sStreamReaderInfo, req->tsdbCalcDataReq.gid, &pList, &pNum));
    BUILD_OPTION(options, getSuid(sStreamReaderInfo, pList), req->tsdbCalcDataReq.ver, TSDB_ORDER_ASC, req->tsdbCalcDataReq.skey, req->tsdbCalcDataReq.ekey,
                 sStreamReaderInfo->triggerCols, false, NULL);
    STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTaskInner, sStreamReaderInfo->triggerResBlock, pList, pNum, &sStreamReaderInfo->storageApi));

    STREAM_CHECK_RET_GOTO(taosHashPut(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES, &pTaskInner, sizeof(pTaskInner)));
    STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamReaderInfo->triggerResBlock, false, &pTaskInner->pResBlockDst));
  } else {
    void** tmp = taosHashGet(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES);
    STREAM_CHECK_NULL_GOTO(tmp, TSDB_CODE_STREAM_NO_CONTEXT);
    pTaskInner = *(SStreamReaderTaskInner**)tmp;
    STREAM_CHECK_NULL_GOTO(pTaskInner, TSDB_CODE_INTERNAL_ERROR);
  }

  blockDataCleanup(pTaskInner->pResBlockDst);
  bool hasNext = true;
  while (1) {
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTaskInner, &hasNext));
    if (!hasNext) {
      break;
    }
    pTaskInner->pResBlock->info.id.groupId = qStreamGetGroupIdFromSet(sStreamReaderInfo, pTaskInner->pResBlock->info.id.uid);

    SSDataBlock* pBlock = NULL;
    STREAM_CHECK_RET_GOTO(getTableData(pTaskInner, &pBlock));
    STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock, sStreamReaderInfo->pFilterInfo, NULL));
    STREAM_CHECK_RET_GOTO(blockDataMerge(pTaskInner->pResBlockDst, pBlock));
    if (pTaskInner->pResBlockDst->info.rows >= STREAM_RETURN_ROWS_NUM) {
      break;
    }
  }

  STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamReaderInfo->calcResBlock, false, &pBlockRes));
  STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(pBlockRes, pTaskInner->pResBlockDst->info.capacity));
  blockDataTransform(pBlockRes, pTaskInner->pResBlockDst);
  STREAM_CHECK_RET_GOTO(buildRsp(pBlockRes, &buf, &size));
  ST_TASK_DLOG("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlockRes->info.rows);
  printDataBlock(pBlockRes, __func__, "tsdb_data", ((SStreamTask*)pTask)->streamId);

  if (!hasNext) {
    STREAM_CHECK_RET_GOTO(taosHashRemove(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES));
  }

end:
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  blockDataDestroy(pBlockRes);
  taosMemoryFree(pList);
  return code;
}

static int32_t vnodeProcessStreamTsdbVirtalDataReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  size_t  size = 0;
  int32_t* slotIdList = NULL;
  SArray* sortedCid = NULL;
  SArray* schemas = NULL;
  SSDataBlock*   pBlockRes = NULL;
  
  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start", TD_VID(pVnode), __func__);

  SStreamReaderTaskInner* pTaskInner = NULL;
  int64_t key = req->tsdbDataReq.uid;

  if (req->base.type == STRIGGER_PULL_TSDB_DATA) {
    // sort cid and build slotIdList
    slotIdList = taosMemoryMalloc(taosArrayGetSize(req->tsdbDataReq.cids) * sizeof(int32_t));
    STREAM_CHECK_NULL_GOTO(slotIdList, terrno);
    sortedCid = taosArrayDup(req->tsdbDataReq.cids, NULL);
    STREAM_CHECK_NULL_GOTO(sortedCid, terrno);
    taosArraySort(sortedCid, sortCid);
    for (int32_t i = 0; i < taosArrayGetSize(req->tsdbDataReq.cids); i++) {
      int16_t* cid = taosArrayGet(req->tsdbDataReq.cids, i);
      STREAM_CHECK_NULL_GOTO(cid, terrno);
      for (int32_t j = 0; j < taosArrayGetSize(sortedCid); j++) {
        int16_t* cidSorted = taosArrayGet(sortedCid, j);
        STREAM_CHECK_NULL_GOTO(cidSorted, terrno);
        if (*cid == *cidSorted) {
          slotIdList[j] = i;
          break;
        }
      }
    }

    STREAM_CHECK_RET_GOTO(buildScheamFromMeta(pVnode, req->tsdbDataReq.uid, &schemas, &sStreamReaderInfo->storageApi));
    STREAM_CHECK_RET_GOTO(shrinkScheams(req->tsdbDataReq.cids, schemas));
    STREAM_CHECK_RET_GOTO(createDataBlockForStream(schemas, &pBlockRes));

    taosArraySort(schemas, sortSSchema);
    BUILD_OPTION(options, req->tsdbDataReq.suid, req->tsdbDataReq.ver, req->tsdbDataReq.order, req->tsdbDataReq.skey,
                    req->tsdbDataReq.ekey, schemas, true, &slotIdList);
    STableKeyInfo       keyInfo = {.uid = req->tsdbDataReq.uid, .groupId = 0};
    STREAM_CHECK_RET_GOTO(createStreamTask(pVnode, &options, &pTaskInner, pBlockRes, &keyInfo, 1, &sStreamReaderInfo->storageApi));
    STREAM_CHECK_RET_GOTO(taosHashPut(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES, &pTaskInner, sizeof(pTaskInner)));
    pTaskInner->pResBlockDst = pBlockRes;
    pBlockRes = NULL;
  } else {
    void** tmp = taosHashGet(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES);
    STREAM_CHECK_NULL_GOTO(tmp, TSDB_CODE_STREAM_NO_CONTEXT);
    pTaskInner = *(SStreamReaderTaskInner**)tmp;
    STREAM_CHECK_NULL_GOTO(pTaskInner, TSDB_CODE_INTERNAL_ERROR);
  }

  blockDataCleanup(pTaskInner->pResBlockDst);
  bool hasNext = true;
  while (1) {
    STREAM_CHECK_RET_GOTO(getTableDataInfo(pTaskInner, &hasNext));
    if (!hasNext) {
      break;
    }

    SSDataBlock* pBlock = NULL;
    STREAM_CHECK_RET_GOTO(getTableData(pTaskInner, &pBlock));
    STREAM_CHECK_RET_GOTO(blockDataMerge(pTaskInner->pResBlockDst, pBlock));
    if (pTaskInner->pResBlockDst->info.rows >= STREAM_RETURN_ROWS_NUM) {
      break;
    }
  }
  STREAM_CHECK_RET_GOTO(buildRsp(pTaskInner->pResBlockDst, &buf, &size));
  ST_TASK_DLOG("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pTaskInner->pResBlockDst->info.rows);
  printDataBlock(pTaskInner->pResBlockDst, __func__, "tsdb_data", ((SStreamTask*)pTask)->streamId);
  if (!hasNext) {
    STREAM_CHECK_RET_GOTO(taosHashRemove(sStreamReaderInfo->streamTaskMap, &key, LONG_BYTES));
  }

end:
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  taosMemFree(slotIdList);
  taosArrayDestroy(sortedCid);
  taosArrayDestroy(schemas);
  blockDataDestroy(pBlockRes);
  return code;
}

static int32_t vnodeProcessStreamWalMetaNewReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t      code = 0;
  int32_t      lino = 0;
  void*        buf = NULL;
  size_t       size = 0;
  int64_t      lastVer = 0;
  SSTriggerWalNewRsp resultRsp = {0};

  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start, request paras lastVer:%" PRId64, TD_VID(pVnode), __func__, req->walMetaNewReq.lastVer);

  if (sStreamReaderInfo->metaBlock == NULL) {
    STREAM_CHECK_RET_GOTO(createBlockForWalMetaNew((SSDataBlock**)&sStreamReaderInfo->metaBlock));
    STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(sStreamReaderInfo->metaBlock, STREAM_RETURN_ROWS_NUM));
  }
  blockDataEmpty(sStreamReaderInfo->metaBlock);
  resultRsp.metaBlock = sStreamReaderInfo->metaBlock;
  resultRsp.ver = req->walMetaNewReq.lastVer;
  STREAM_CHECK_RET_GOTO(processWalVerMetaNew(pVnode, &resultRsp, sStreamReaderInfo, req->walMetaNewReq.ctime));

  ST_TASK_DLOG("vgId:%d %s get result last ver:%"PRId64" rows:%d", TD_VID(pVnode), __func__, resultRsp.ver, resultRsp.totalRows);
  STREAM_CHECK_CONDITION_GOTO(resultRsp.totalRows == 0, TDB_CODE_SUCCESS);
  size = tSerializeSStreamWalDataResponse(NULL, 0, &resultRsp);
  buf = rpcMallocCont(size);
  size = tSerializeSStreamWalDataResponse(buf, size, &resultRsp);
  printDataBlock(sStreamReaderInfo->metaBlock, __func__, "meta", ((SStreamTask*)pTask)->streamId);

end:
  if (resultRsp.totalRows == 0) {
    code = TSDB_CODE_STREAM_NO_DATA;
    buf = rpcMallocCont(sizeof(int64_t));
    *(int64_t *)buf = resultRsp.ver;
    size = sizeof(int64_t);
  }
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  if (code == TSDB_CODE_STREAM_NO_DATA){
    code = 0;
  }
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  blockDataDestroy(resultRsp.deleteBlock);
  blockDataDestroy(resultRsp.dropBlock);

  return code;
}
static int32_t vnodeProcessStreamWalMetaDataNewReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t      code = 0;
  int32_t      lino = 0;
  void*        buf = NULL;
  size_t       size = 0;
  SSTriggerWalNewRsp resultRsp = {0};
  
  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start, request paras lastVer:%" PRId64, TD_VID(pVnode), __func__, req->walMetaDataNewReq.lastVer);

  if (sStreamReaderInfo->metaBlock == NULL) {
    STREAM_CHECK_RET_GOTO(createBlockForWalMetaNew((SSDataBlock**)&sStreamReaderInfo->metaBlock));
    STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(sStreamReaderInfo->metaBlock, STREAM_RETURN_ROWS_NUM));
  }

  resultRsp.metaBlock = sStreamReaderInfo->metaBlock;
  STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamReaderInfo->triggerBlock, false, (SSDataBlock**)&resultRsp.dataBlock));
  resultRsp.ver = req->walMetaDataNewReq.lastVer;
  resultRsp.indexHash = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  STREAM_CHECK_NULL_GOTO(resultRsp.indexHash, terrno);

  STREAM_CHECK_RET_GOTO(processWalVerMetaDataNew(pVnode, sStreamReaderInfo, &resultRsp));

  STREAM_CHECK_CONDITION_GOTO(resultRsp.totalRows == 0, TDB_CODE_SUCCESS);
  size = tSerializeSStreamWalDataResponse(NULL, 0, &resultRsp);
  buf = rpcMallocCont(size);
  size = tSerializeSStreamWalDataResponse(buf, size, &resultRsp);
  printDataBlock(sStreamReaderInfo->metaBlock, __func__, "meta", ((SStreamTask*)pTask)->streamId);
  printDataBlock(resultRsp.dataBlock, __func__, "data", ((SStreamTask*)pTask)->streamId);
  printDataBlock(resultRsp.dropBlock, __func__, "drop", ((SStreamTask*)pTask)->streamId);
  printDataBlock(resultRsp.deleteBlock, __func__, "delete", ((SStreamTask*)pTask)->streamId);
  printIndexHash(resultRsp.indexHash, pTask);

end:
  if (resultRsp.totalRows == 0) {
    buf = rpcMallocCont(sizeof(int64_t));
    *(int64_t *)buf = resultRsp.ver;
    size = sizeof(int64_t);
    code = TSDB_CODE_STREAM_NO_DATA;
  }
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  if (code == TSDB_CODE_STREAM_NO_DATA){
    code = 0;
  }
  blockDataDestroy(resultRsp.dataBlock);
  blockDataDestroy(resultRsp.deleteBlock);
  blockDataDestroy(resultRsp.dropBlock);
  tSimpleHashCleanup(resultRsp.indexHash);

  STREAM_PRINT_LOG_END_WITHID(code, lino);

  return code;
}

static int32_t vnodeProcessStreamWalDataNewReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t      code = 0;
  int32_t      lino = 0;
  void*        buf = NULL;
  size_t       size = 0;
  SSTriggerWalNewRsp resultRsp = {0};

  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start, request paras size:%zu", TD_VID(pVnode), __func__, taosArrayGetSize(req->walDataNewReq.versions));

  STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamReaderInfo->triggerBlock, false, (SSDataBlock**)&resultRsp.dataBlock));
  resultRsp.indexHash = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  STREAM_CHECK_NULL_GOTO(resultRsp.indexHash, terrno);
  resultRsp.uidHash = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  STREAM_CHECK_NULL_GOTO(resultRsp.uidHash, terrno);

  STREAM_CHECK_RET_GOTO(processWalVerDataNew(pVnode, sStreamReaderInfo, req->walDataNewReq.versions, req->walDataNewReq.ranges, &resultRsp));
  ST_TASK_DLOG("vgId:%d %s get result last ver:%"PRId64" rows:%d", TD_VID(pVnode), __func__, resultRsp.ver, resultRsp.totalRows);

  STREAM_CHECK_CONDITION_GOTO(resultRsp.totalRows == 0, TDB_CODE_SUCCESS);

  size = tSerializeSStreamWalDataResponse(NULL, 0, &resultRsp);
  buf = rpcMallocCont(size);
  size = tSerializeSStreamWalDataResponse(buf, size, &resultRsp);
  printDataBlock(resultRsp.dataBlock, __func__, "data", ((SStreamTask*)pTask)->streamId);
  printIndexHash(resultRsp.indexHash, pTask);

end:
  if (resultRsp.totalRows == 0) {
    buf = rpcMallocCont(sizeof(int64_t));
    *(int64_t *)buf = resultRsp.ver;
    size = sizeof(int64_t);
    code = TSDB_CODE_STREAM_NO_DATA;
  }
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  if (code == TSDB_CODE_STREAM_NO_DATA){
    code = 0;
  }

  blockDataDestroy(resultRsp.dataBlock);
  blockDataDestroy(resultRsp.deleteBlock);
  blockDataDestroy(resultRsp.dropBlock);
  tSimpleHashCleanup(resultRsp.indexHash);
  tSimpleHashCleanup(resultRsp.uidHash);
  STREAM_PRINT_LOG_END_WITHID(code, lino);

  return code;
}

static int32_t vnodeProcessStreamWalCalcDataNewReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t      code = 0;
  int32_t      lino = 0;
  void*        buf = NULL;
  size_t       size = 0;
  SSTriggerWalNewRsp resultRsp = {0};
  SSDataBlock* pBlock1 = NULL;
  SSDataBlock* pBlock2 = NULL;
  
  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start, request paras size:%zu", TD_VID(pVnode), __func__, taosArrayGetSize(req->walDataNewReq.versions));

  SSDataBlock* dataBlock = sStreamReaderInfo->isVtableStream ? sStreamReaderInfo->calcBlock : sStreamReaderInfo->triggerBlock;
  STREAM_CHECK_RET_GOTO(createOneDataBlock(dataBlock, false, (SSDataBlock**)&resultRsp.dataBlock));
  resultRsp.isCalc = sStreamReaderInfo->isVtableStream ? true : false;
  resultRsp.indexHash = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  STREAM_CHECK_NULL_GOTO(resultRsp.indexHash, terrno);
  resultRsp.uidHash = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  STREAM_CHECK_NULL_GOTO(resultRsp.uidHash, terrno);

  STREAM_CHECK_RET_GOTO(processWalVerDataNew(pVnode, sStreamReaderInfo, req->walDataNewReq.versions, req->walDataNewReq.ranges, &resultRsp));
  STREAM_CHECK_CONDITION_GOTO(resultRsp.totalRows == 0, TDB_CODE_SUCCESS);

  if (!sStreamReaderInfo->isVtableStream){
    STREAM_CHECK_RET_GOTO(createOneDataBlock(sStreamReaderInfo->calcBlock, false, &pBlock2));
  
    blockDataTransform(pBlock2, resultRsp.dataBlock);
    blockDataDestroy(resultRsp.dataBlock);
    resultRsp.dataBlock = pBlock2;
    pBlock2 = NULL;
  }

  size = tSerializeSStreamWalDataResponse(NULL, 0, &resultRsp);
  buf = rpcMallocCont(size);
  size = tSerializeSStreamWalDataResponse(buf, size, &resultRsp);
  printDataBlock(resultRsp.dataBlock, __func__, "data", ((SStreamTask*)pTask)->streamId);
  printIndexHash(resultRsp.indexHash, pTask);

end:
  if (resultRsp.totalRows == 0) {
    buf = rpcMallocCont(sizeof(int64_t));
    *(int64_t *)buf = resultRsp.ver;
    size = sizeof(int64_t);
    code = TSDB_CODE_STREAM_NO_DATA;
  }
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  if (code == TSDB_CODE_STREAM_NO_DATA){
    code = 0;
  }

  blockDataDestroy(pBlock1);
  blockDataDestroy(pBlock2);
  blockDataDestroy(resultRsp.dataBlock);
  blockDataDestroy(resultRsp.deleteBlock);
  blockDataDestroy(resultRsp.dropBlock);
  tSimpleHashCleanup(resultRsp.indexHash);
  tSimpleHashCleanup(resultRsp.uidHash);
  STREAM_PRINT_LOG_END_WITHID(code, lino);

  return code;
}

static int32_t vnodeProcessStreamGroupColValueReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   buf = NULL;
  size_t  size = 0;
  SArray** gInfo = NULL;
  
  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start, request gid:%" PRId64, TD_VID(pVnode), __func__, req->groupColValueReq.gid);

  gInfo = taosHashAcquire(sStreamReaderInfo->groupIdMap, &req->groupColValueReq.gid, POINTER_BYTES);
  STREAM_CHECK_NULL_GOTO(gInfo, TSDB_CODE_STREAM_NO_CONTEXT);
  SStreamGroupInfo pGroupInfo = {0};
  pGroupInfo.gInfo = *gInfo;

  size = tSerializeSStreamGroupInfo(NULL, 0, &pGroupInfo, TD_VID(pVnode));
  STREAM_CHECK_CONDITION_GOTO(size < 0, size);
  buf = rpcMallocCont(size);
  STREAM_CHECK_NULL_GOTO(buf, terrno);
  size = tSerializeSStreamGroupInfo(buf, size, &pGroupInfo, TD_VID(pVnode));
  STREAM_CHECK_CONDITION_GOTO(size < 0, size);
end:
  taosHashRelease(sStreamReaderInfo->groupIdMap, gInfo);
  if (code != 0) {
    rpcFreeCont(buf);
    buf = NULL;
    size = 0;
  }
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);

  return code;
}

static int32_t vnodeProcessStreamVTableInfoReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t              code = 0;
  int32_t              lino = 0;
  void*                buf = NULL;
  size_t               size = 0;
  SStreamMsgVTableInfo vTableInfo = {0};
  SMetaReader          metaReader = {0};
  SArray*              pTableListArray = NULL;

  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start", TD_VID(pVnode), __func__);

  SArray* cids = req->virTableInfoReq.cids;
  STREAM_CHECK_NULL_GOTO(cids, terrno);

  pTableListArray = qStreamGetTableArrayList(sStreamReaderInfo);
  STREAM_CHECK_NULL_GOTO(pTableListArray, terrno);

  vTableInfo.infos = taosArrayInit(taosArrayGetSize(pTableListArray), sizeof(VTableInfo));
  STREAM_CHECK_NULL_GOTO(vTableInfo.infos, terrno);
  sStreamReaderInfo->storageApi.metaReaderFn.initReader(&metaReader, pVnode, META_READER_LOCK, &sStreamReaderInfo->storageApi.metaFn);

  for (size_t i = 0; i < taosArrayGetSize(pTableListArray); i++) {
    SStreamTableKeyInfo* pKeyInfo = taosArrayGetP(pTableListArray, i);
    if (pKeyInfo == NULL || pKeyInfo->markedDeleted) {
      continue;
    }
    VTableInfo* vTable = taosArrayReserve(vTableInfo.infos, 1);
    STREAM_CHECK_NULL_GOTO(vTable, terrno);
    vTable->uid = pKeyInfo->uid;
    vTable->gId = pKeyInfo->groupId;

    ST_TASK_DLOG("vgId:%d %s put vtable uid:%"PRId64, TD_VID(pVnode), __func__, pKeyInfo->uid);

    code = sStreamReaderInfo->storageApi.metaReaderFn.getTableEntryByUid(&metaReader, pKeyInfo->uid);
    if (code != 0) {
      ST_TASK_WLOG("vgId:%d %s get table entry by uid:%"PRId64" failed, msg:%s", TD_VID(pVnode), __func__, pKeyInfo->uid, tstrerror(code));
      continue;
    }
    if (taosArrayGetSize(cids) == 1 && *(col_id_t*)taosArrayGet(cids, 0) == PRIMARYKEY_TIMESTAMP_COL_ID){
      vTable->cols.nCols = metaReader.me.colRef.nCols;
      vTable->cols.version = metaReader.me.colRef.version;
      vTable->cols.pColRef = taosMemoryCalloc(metaReader.me.colRef.nCols, sizeof(SColRef));
      for (size_t j = 0; j < metaReader.me.colRef.nCols; j++) {
        memcpy(vTable->cols.pColRef + j, &metaReader.me.colRef.pColRef[j], sizeof(SColRef));
      }
    } else {
      vTable->cols.nCols = taosArrayGetSize(cids);
      vTable->cols.version = metaReader.me.colRef.version;
      vTable->cols.pColRef = taosMemoryCalloc(taosArrayGetSize(cids), sizeof(SColRef));
      for (size_t i = 0; i < taosArrayGetSize(cids); i++) {
        for (size_t j = 0; j < metaReader.me.colRef.nCols; j++) {
          if (metaReader.me.colRef.pColRef[j].hasRef &&
              metaReader.me.colRef.pColRef[j].id == *(col_id_t*)taosArrayGet(cids, i)) {
            memcpy(vTable->cols.pColRef + i, &metaReader.me.colRef.pColRef[j], sizeof(SColRef));
            break;
          }
        }
      }
    }
    tDecoderClear(&metaReader.coder);
  }
  ST_TASK_DLOG("vgId:%d %s end, size:%"PRIzu, TD_VID(pVnode), __func__, taosArrayGetSize(vTableInfo.infos));
  STREAM_CHECK_RET_GOTO(buildVTableInfoRsp(&vTableInfo, &buf, &size));

end:
  taosArrayDestroyP(pTableListArray, taosMemFree);
  tDestroySStreamMsgVTableInfo(&vTableInfo);
  sStreamReaderInfo->storageApi.metaReaderFn.clearReader(&metaReader);
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  return code;
}

static int32_t vnodeProcessStreamOTableInfoReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t                   code = 0;
  int32_t                   lino = 0;
  void*                     buf = NULL;
  size_t                    size = 0;
  SSTriggerOrigTableInfoRsp oTableInfo = {0};
  SMetaReader               metaReader = {0};
  void*                     pTask = sStreamReaderInfo->pTask;

  ST_TASK_DLOG("vgId:%d %s start", TD_VID(pVnode), __func__);

  SArray* cols = req->origTableInfoReq.cols;
  STREAM_CHECK_NULL_GOTO(cols, terrno);

  oTableInfo.cols = taosArrayInit(taosArrayGetSize(cols), sizeof(OTableInfoRsp));

  STREAM_CHECK_NULL_GOTO(oTableInfo.cols, terrno);

  sStreamReaderInfo->storageApi.metaReaderFn.initReader(&metaReader, pVnode, META_READER_LOCK, &sStreamReaderInfo->storageApi.metaFn);
  for (size_t i = 0; i < taosArrayGetSize(cols); i++) {
    OTableInfo*    oInfo = taosArrayGet(cols, i);
    OTableInfoRsp* vTableInfo = taosArrayReserve(oTableInfo.cols, 1);
    STREAM_CHECK_NULL_GOTO(oInfo, terrno);
    STREAM_CHECK_NULL_GOTO(vTableInfo, terrno);
    STREAM_CHECK_RET_GOTO(sStreamReaderInfo->storageApi.metaReaderFn.getTableEntryByName(&metaReader, oInfo->refTableName));
    vTableInfo->uid = metaReader.me.uid;
    ST_TASK_DLOG("vgId:%d %s get original uid:%"PRId64, TD_VID(pVnode), __func__, vTableInfo->uid);

    SSchemaWrapper* sSchemaWrapper = NULL;
    if (metaReader.me.type == TD_CHILD_TABLE) {
      int64_t suid = metaReader.me.ctbEntry.suid;
      vTableInfo->suid = suid;
      tDecoderClear(&metaReader.coder);
      STREAM_CHECK_RET_GOTO(sStreamReaderInfo->storageApi.metaReaderFn.getTableEntryByUid(&metaReader, suid));
      sSchemaWrapper = &metaReader.me.stbEntry.schemaRow;
    } else if (metaReader.me.type == TD_NORMAL_TABLE) {
      vTableInfo->suid = 0;
      sSchemaWrapper = &metaReader.me.ntbEntry.schemaRow;
    } else {
      ST_TASK_ELOG("invalid table type:%d", metaReader.me.type);
    }

    for (size_t j = 0; j < sSchemaWrapper->nCols; j++) {
      SSchema* s = sSchemaWrapper->pSchema + j;
      if (strcmp(s->name, oInfo->refColName) == 0) {
        vTableInfo->cid = s->colId;
        break;
      }
    }
    if (vTableInfo->cid == 0) {
      stError("vgId:%d %s, not found col %s in table %s", TD_VID(pVnode), __func__, oInfo->refColName,
              oInfo->refTableName);
    }
    tDecoderClear(&metaReader.coder);
  }

  STREAM_CHECK_RET_GOTO(buildOTableInfoRsp(&oTableInfo, &buf, &size));

end:
  tDestroySTriggerOrigTableInfoRsp(&oTableInfo);
  sStreamReaderInfo->storageApi.metaReaderFn.clearReader(&metaReader);
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  return code;
}

static int32_t vnodeProcessStreamVTableTagInfoReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t                   code = 0;
  int32_t                   lino = 0;
  void*                     buf = NULL;
  size_t                    size = 0;
  SSDataBlock* pBlock = NULL;

  SMetaReader               metaReader = {0};
  SMetaReader               metaReaderStable = {0};
  int64_t streamId = req->base.streamId;
  stsDebug("vgId:%d %s start", TD_VID(pVnode), __func__);

  SArray* cols = req->virTablePseudoColReq.cids;
  STREAM_CHECK_NULL_GOTO(cols, terrno);

  sStreamReaderInfo->storageApi.metaReaderFn.initReader(&metaReader, pVnode, META_READER_LOCK, &sStreamReaderInfo->storageApi.metaFn);
  STREAM_CHECK_RET_GOTO(sStreamReaderInfo->storageApi.metaReaderFn.getTableEntryByUid(&metaReader, req->virTablePseudoColReq.uid));

  STREAM_CHECK_CONDITION_GOTO(metaReader.me.type != TD_VIRTUAL_CHILD_TABLE && metaReader.me.type != TD_VIRTUAL_NORMAL_TABLE, TSDB_CODE_INVALID_PARA);

  STREAM_CHECK_RET_GOTO(createDataBlock(&pBlock));
  if (metaReader.me.type == TD_VIRTUAL_NORMAL_TABLE) {
    STREAM_CHECK_CONDITION_GOTO (taosArrayGetSize(cols) < 1 || *(col_id_t*)taosArrayGet(cols, 0) != -1, TSDB_CODE_INVALID_PARA);
    SColumnInfoData idata = createColumnInfoData(TSDB_DATA_TYPE_BINARY, TSDB_TABLE_NAME_LEN, -1);
    STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(pBlock, &idata));
    STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(pBlock, 1));
    pBlock->info.rows = 1;
    SColumnInfoData* pDst = taosArrayGet(pBlock->pDataBlock, 0);
    STREAM_CHECK_NULL_GOTO(pDst, terrno);
    STREAM_CHECK_RET_GOTO(varColSetVarData(pDst, 0, metaReader.me.name, strlen(metaReader.me.name), false));
  } else if (metaReader.me.type == TD_VIRTUAL_CHILD_TABLE){
    int64_t suid = metaReader.me.ctbEntry.suid;
    sStreamReaderInfo->storageApi.metaReaderFn.readerReleaseLock(&metaReader);
    sStreamReaderInfo->storageApi.metaReaderFn.initReader(&metaReaderStable, pVnode, META_READER_LOCK, &sStreamReaderInfo->storageApi.metaFn);

    STREAM_CHECK_RET_GOTO(sStreamReaderInfo->storageApi.metaReaderFn.getTableEntryByUid(&metaReaderStable, suid));
    SSchemaWrapper*  sSchemaWrapper = &metaReaderStable.me.stbEntry.schemaTag;
    for (size_t i = 0; i < taosArrayGetSize(cols); i++){
      col_id_t* id = taosArrayGet(cols, i);
      STREAM_CHECK_NULL_GOTO(id, terrno);
      if (*id == -1) {
        SColumnInfoData idata = createColumnInfoData(TSDB_DATA_TYPE_BINARY, TSDB_TABLE_NAME_LEN, -1);
        STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(pBlock, &idata));
        continue;
      }
      size_t j = 0;
      for (; j < sSchemaWrapper->nCols; j++) {
        SSchema* s = sSchemaWrapper->pSchema + j;
        if (s->colId == *id) {
          SColumnInfoData idata = createColumnInfoData(s->type, s->bytes, s->colId);
          STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(pBlock, &idata));
          break;
        }
      }
      if (j == sSchemaWrapper->nCols) {
        SColumnInfoData idata = createColumnInfoData(TSDB_DATA_TYPE_NULL, CHAR_BYTES, *id);
        STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(pBlock, &idata));
      }
    }
    STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(pBlock, 1));
    pBlock->info.rows = 1;
    
    for (size_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); i++){
      SColumnInfoData* pDst = taosArrayGet(pBlock->pDataBlock, i);
      STREAM_CHECK_NULL_GOTO(pDst, terrno);

      if (pDst->info.colId == -1) {
        STREAM_CHECK_RET_GOTO(varColSetVarData(pDst, 0, metaReader.me.name, strlen(metaReader.me.name), false));
        continue;
      }
      if (pDst->info.type == TSDB_DATA_TYPE_NULL) {
        STREAM_CHECK_RET_GOTO(colDataSetVal(pDst, 0, NULL, true));
        continue;
      }

      STagVal val = {0};
      val.cid = pDst->info.colId;
      const char* p = sStreamReaderInfo->storageApi.metaFn.extractTagVal(metaReader.me.ctbEntry.pTags, pDst->info.type, &val);

      char* data = NULL;
      if (pDst->info.type != TSDB_DATA_TYPE_JSON && p != NULL) {
        data = tTagValToData((const STagVal*)p, false);
      } else {
        data = (char*)p;
      }

      STREAM_CHECK_RET_GOTO(colDataSetVal(pDst, 0, data,
                            (data == NULL) || (pDst->info.type == TSDB_DATA_TYPE_JSON && tTagIsJsonNull(data))));

      if ((pDst->info.type != TSDB_DATA_TYPE_JSON) && (p != NULL) && IS_VAR_DATA_TYPE(((const STagVal*)p)->type) &&
          (data != NULL)) {
        taosMemoryFree(data);
      }
    }
  } else {
    stError("vgId:%d %s, invalid table type:%d", TD_VID(pVnode), __func__, metaReader.me.type);
    code = TSDB_CODE_INVALID_PARA;
    goto end;
  }
  
  stsDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock->info.rows);
  printDataBlock(pBlock, __func__, "", streamId);
  STREAM_CHECK_RET_GOTO(buildRsp(pBlock, &buf, &size));

end:
  if(size == 0){
    code = TSDB_CODE_STREAM_NO_DATA;
  }
  sStreamReaderInfo->storageApi.metaReaderFn.clearReader(&metaReaderStable);
  sStreamReaderInfo->storageApi.metaReaderFn.clearReader(&metaReader);
  STREAM_PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  blockDataDestroy(pBlock);
  return code;
}

static int32_t vnodeProcessStreamFetchMsg(SVnode* pVnode, SRpcMsg* pMsg) {
  int32_t            code = 0;
  int32_t            lino = 0;
  void*              buf = NULL;
  size_t             size = 0;
  void*              taskAddr = NULL;
  SArray*            pResList = NULL;
  bool               hasNext = false;

  SResFetchReq req = {0};
  STREAM_CHECK_CONDITION_GOTO(tDeserializeSResFetchReq(pMsg->pCont, pMsg->contLen, &req) < 0,
                              TSDB_CODE_QRY_INVALID_INPUT);
  SArray* calcInfoList = (SArray*)qStreamGetReaderInfo(req.queryId, req.taskId, &taskAddr);
  STREAM_CHECK_NULL_GOTO(calcInfoList, terrno);

  STREAM_CHECK_CONDITION_GOTO(req.execId < 0, TSDB_CODE_INVALID_PARA);
  SStreamTriggerReaderCalcInfo* sStreamReaderCalcInfo = taosArrayGetP(calcInfoList, req.execId);
  STREAM_CHECK_NULL_GOTO(sStreamReaderCalcInfo, terrno);
  void* pTask = sStreamReaderCalcInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start, execId:%d, reset:%d, pTaskInfo:%p, scan type:%d", TD_VID(pVnode), __func__, req.execId, req.reset,
               sStreamReaderCalcInfo->pTaskInfo, nodeType(sStreamReaderCalcInfo->calcAst->pNode));

  if (req.reset) {
    int64_t uid = 0;
    if (req.dynTbname) {
      SArray* vals = req.pStRtFuncInfo->pStreamPartColVals;
      for (int32_t i = 0; i < taosArrayGetSize(vals); ++i) {
        SStreamGroupValue* pValue = taosArrayGet(vals, i);
        if (pValue != NULL && pValue->isTbname) {
          uid = pValue->uid;
          break;
        }
      }
    }
    
    SReadHandle handle = {0};
    handle.vnode = pVnode;
    handle.uid = uid;
    handle.cacheSttStatis = true;

    initStorageAPI(&handle.api);
    if (QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN == nodeType(sStreamReaderCalcInfo->calcAst->pNode) ||
      QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN == nodeType(sStreamReaderCalcInfo->calcAst->pNode)){
      STimeRangeNode* node = (STimeRangeNode*)((STableScanPhysiNode*)(sStreamReaderCalcInfo->calcAst->pNode))->pTimeRange;
      if (node != NULL) {
        STREAM_CHECK_RET_GOTO(processCalaTimeRange(sStreamReaderCalcInfo, &req, node, &handle));
      } else {
        ST_TASK_DLOG("vgId:%d %s no time range node", TD_VID(pVnode), __func__);
      }
    }

    TSWAP(sStreamReaderCalcInfo->rtInfo.funcInfo, *req.pStRtFuncInfo);
    sStreamReaderCalcInfo->rtInfo.funcInfo.hasPlaceHolder = sStreamReaderCalcInfo->hasPlaceHolder;
    handle.streamRtInfo = &sStreamReaderCalcInfo->rtInfo;

    if (sStreamReaderCalcInfo->pTaskInfo == NULL || !qNeedReset(sStreamReaderCalcInfo->pTaskInfo)) {
      qDestroyTask(sStreamReaderCalcInfo->pTaskInfo);
      STREAM_CHECK_RET_GOTO(qCreateStreamExecTaskInfo(&sStreamReaderCalcInfo->pTaskInfo,
                                                    sStreamReaderCalcInfo->calcScanPlan, &handle, NULL, TD_VID(pVnode),
                                                    req.taskId));
    } else {
      STREAM_CHECK_RET_GOTO(qResetTableScan(sStreamReaderCalcInfo->pTaskInfo, &handle));
    }

    STREAM_CHECK_RET_GOTO(qSetTaskId(sStreamReaderCalcInfo->pTaskInfo, req.taskId, req.queryId));
  }

  if (req.pOpParam != NULL) {
    qUpdateOperatorParam(sStreamReaderCalcInfo->pTaskInfo, req.pOpParam);
  }
  
  pResList = taosArrayInit(4, POINTER_BYTES);
  STREAM_CHECK_NULL_GOTO(pResList, terrno);
  uint64_t ts = 0;
  STREAM_CHECK_RET_GOTO(qExecTaskOpt(sStreamReaderCalcInfo->pTaskInfo, pResList, &ts, &hasNext, NULL, req.pOpParam != NULL));

  for(size_t i = 0; i < taosArrayGetSize(pResList); i++){
    SSDataBlock* pBlock = taosArrayGetP(pResList, i);
    if (pBlock == NULL) continue;
    printDataBlock(pBlock, __func__, "fetch", ((SStreamTask*)pTask)->streamId);
/*    
    if (sStreamReaderCalcInfo->rtInfo.funcInfo.withExternalWindow) {
      STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock, sStreamReaderCalcInfo->pFilterInfo, NULL));
      printDataBlock(pBlock, __func__, "fetch filter");
    }
*/    
  }

end:
  STREAM_CHECK_RET_GOTO(streamBuildFetchRsp(pResList, hasNext, &buf, &size, pVnode->config.tsdbCfg.precision));
  taosArrayDestroy(pResList);
  streamReleaseTask(taskAddr);

  if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST || code == TSDB_CODE_TDB_TABLE_NOT_EXIST){
    code = TDB_CODE_SUCCESS;
  }
  STREAM_PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {.msgType = TDMT_STREAM_FETCH_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  tDestroySResFetchReq(&req);
  return code;
}

int32_t vnodeProcessStreamReaderMsg(SVnode* pVnode, SRpcMsg* pMsg) {
  int32_t                   code = 0;
  int32_t                   lino = 0;
  SSTriggerPullRequestUnion req = {0};
  void*                     taskAddr = NULL;

  vDebug("vgId:%d, msg:%p in stream reader queue is processing", pVnode->config.vgId, pMsg);
  if (!syncIsReadyForRead(pVnode->sync)) {
    vnodeRedirectRpcMsg(pVnode, pMsg, terrno);
    return 0;
  }

  if (pMsg->msgType == TDMT_STREAM_FETCH) {
    return vnodeProcessStreamFetchMsg(pVnode, pMsg);
  } else if (pMsg->msgType == TDMT_STREAM_TRIGGER_PULL) {
    void*   pReq = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
    int32_t len = pMsg->contLen - sizeof(SMsgHead);
    STREAM_CHECK_RET_GOTO(tDeserializeSTriggerPullRequest(pReq, len, &req));
    stDebug("vgId:%d %s start, type:%d, streamId:%" PRIx64 ", readerTaskId:%" PRIx64 ", sessionId:%" PRIx64,
            TD_VID(pVnode), __func__, req.base.type, req.base.streamId, req.base.readerTaskId, req.base.sessionId);
    SStreamTriggerReaderInfo* sStreamReaderInfo = qStreamGetReaderInfo(req.base.streamId, req.base.readerTaskId, &taskAddr);
    STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);
    if (sStreamReaderInfo->tableList.pTableList == NULL) {  
      taosWLockLatch(&sStreamReaderInfo->lock);
      sStreamReaderInfo->pVnode = pVnode;
      initStorageAPI(&sStreamReaderInfo->storageApi);
      if (sStreamReaderInfo->tableList.pTableList == NULL) {
        STREAM_CHECK_RET_GOTO(initStreamTableListInfo(&sStreamReaderInfo->tableList));
        code = generateTablistForStreamReader(pVnode, sStreamReaderInfo);
      }
      taosWUnLockLatch(&sStreamReaderInfo->lock);
      STREAM_CHECK_RET_GOTO(code);
    }
    switch (req.base.type) {
      case STRIGGER_PULL_SET_TABLE:
        STREAM_CHECK_RET_GOTO(vnodeProcessStreamSetTableReq(pVnode, pMsg, &req, sStreamReaderInfo));
        break;
      case STRIGGER_PULL_LAST_TS:
        STREAM_CHECK_RET_GOTO(vnodeProcessStreamLastTsReq(pVnode, pMsg, &req, sStreamReaderInfo));
        break;
      case STRIGGER_PULL_FIRST_TS:
        STREAM_CHECK_RET_GOTO(vnodeProcessStreamFirstTsReq(pVnode, pMsg, &req, sStreamReaderInfo));
        break;
      case STRIGGER_PULL_TSDB_META:
      case STRIGGER_PULL_TSDB_META_NEXT:
        STREAM_CHECK_RET_GOTO(vnodeProcessStreamTsdbMetaReq(pVnode, pMsg, &req, sStreamReaderInfo));
        break;
      case STRIGGER_PULL_TSDB_TS_DATA:
        if (sStreamReaderInfo->isVtableStream) {
          STREAM_CHECK_RET_GOTO(vnodeProcessStreamTsdbTsDataReqVTable(pVnode, pMsg, &req, sStreamReaderInfo));
        } else {
          STREAM_CHECK_RET_GOTO(vnodeProcessStreamTsdbTsDataReqNonVTable(pVnode, pMsg, &req, sStreamReaderInfo));
        }
        break;
      case STRIGGER_PULL_TSDB_TRIGGER_DATA:
      case STRIGGER_PULL_TSDB_TRIGGER_DATA_NEXT:
        STREAM_CHECK_RET_GOTO(vnodeProcessStreamTsdbTriggerDataReq(pVnode, pMsg, &req, sStreamReaderInfo));
        break;
      case STRIGGER_PULL_TSDB_CALC_DATA:
      case STRIGGER_PULL_TSDB_CALC_DATA_NEXT:
        STREAM_CHECK_RET_GOTO(vnodeProcessStreamTsdbCalcDataReq(pVnode, pMsg, &req, sStreamReaderInfo));
        break;
      case STRIGGER_PULL_TSDB_DATA:
      case STRIGGER_PULL_TSDB_DATA_NEXT:
        STREAM_CHECK_RET_GOTO(vnodeProcessStreamTsdbVirtalDataReq(pVnode, pMsg, &req, sStreamReaderInfo));
        break;
      case STRIGGER_PULL_GROUP_COL_VALUE:
        STREAM_CHECK_RET_GOTO(vnodeProcessStreamGroupColValueReq(pVnode, pMsg, &req, sStreamReaderInfo));
        break;
      case STRIGGER_PULL_VTABLE_INFO:
        STREAM_CHECK_RET_GOTO(vnodeProcessStreamVTableInfoReq(pVnode, pMsg, &req, sStreamReaderInfo));
        break;
      case STRIGGER_PULL_VTABLE_PSEUDO_COL:
        STREAM_CHECK_RET_GOTO(vnodeProcessStreamVTableTagInfoReq(pVnode, pMsg, &req, sStreamReaderInfo));
        break;
      case STRIGGER_PULL_OTABLE_INFO:
        STREAM_CHECK_RET_GOTO(vnodeProcessStreamOTableInfoReq(pVnode, pMsg, &req, sStreamReaderInfo));
        break;
      case STRIGGER_PULL_WAL_META_NEW:
        STREAM_CHECK_RET_GOTO(vnodeProcessStreamWalMetaNewReq(pVnode, pMsg, &req, sStreamReaderInfo));
        break;
      case STRIGGER_PULL_WAL_DATA_NEW:
        STREAM_CHECK_RET_GOTO(vnodeProcessStreamWalDataNewReq(pVnode, pMsg, &req, sStreamReaderInfo));
        break;
      case STRIGGER_PULL_WAL_META_DATA_NEW:
        STREAM_CHECK_RET_GOTO(vnodeProcessStreamWalMetaDataNewReq(pVnode, pMsg, &req, sStreamReaderInfo));
        break;
      case STRIGGER_PULL_WAL_CALC_DATA_NEW:
        STREAM_CHECK_RET_GOTO(vnodeProcessStreamWalCalcDataNewReq(pVnode, pMsg, &req, sStreamReaderInfo));
        break;
      default:
        vError("unknown inner msg type:%d in stream reader queue", req.base.type);
        STREAM_CHECK_RET_GOTO(TSDB_CODE_APP_ERROR);
        break;
    }
  } else {
    vError("unknown msg type:%d in stream reader queue", pMsg->msgType);
    STREAM_CHECK_RET_GOTO(TSDB_CODE_APP_ERROR);
  }
end:

  streamReleaseTask(taskAddr);

  tDestroySTriggerPullRequest(&req);
  STREAM_PRINT_LOG_END(code, lino);
  return code;
}
