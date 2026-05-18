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
#include "osSemaphore.h"
#include "query.h"
#include "scalar.h"
#include "stream.h"
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
#include "tlockfree.h"
#include "tmsg.h"
#include "tsimplehash.h"
#include "ttypes.h"
#include "vnd.h"
#include "vnode.h"
#include "vnodeInt.h"
#include "executor.h"

int32_t cacheTag(SVnode* pVnode, SHashObj* metaCache, SExprInfo* pExprInfo, int32_t numOfExpr, SStorageAPI* api, uint64_t uid, col_id_t colId, SRWLatch* lock);

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

static bool ignoreMetaChange(int64_t tableListVer, int64_t ver) {
  stDebug("%s tableListVer:%" PRId64 " ver:%" PRId64, __func__, tableListVer, ver);
  return tableListVer >= ver;
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
  } 
  *id = qStreamGetGroupIdFromOrigin(sStreamReaderInfo, uid);
  if (*id == -1) goto end;
  ret = true;

end:
  stTrace("%s ret:%d %p %p check suid:%" PRId64 " uid:%" PRId64 " gid:%"PRIu64, __func__, ret, sStreamReaderInfo, sStreamReaderInfo->tableList.gIdMap, suid, uid, *id);
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
    code = cacheTag(sStreamReaderInfo->pVnode, sStreamReaderInfo->pTableMetaCacheTrigger, sStreamReaderInfo->pExprInfoTriggerTag, sStreamReaderInfo->numOfExprTriggerTag, &sStreamReaderInfo->storageApi, info->uid, 0, NULL);
    if (code != 0){
      ST_TASK_WLOG("%s cacheTag trigger failed for uid:%" PRId64",code:%d", __func__, info->uid, code);
      continue;
    }
    code = cacheTag(sStreamReaderInfo->pVnode, sStreamReaderInfo->pTableMetaCacheCalc, sStreamReaderInfo->pExprInfoCalcTag, sStreamReaderInfo->numOfExprCalcTag, &sStreamReaderInfo->storageApi, info->uid, 0, NULL);
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

// Forward declaration: throttled vtable cache recheck hook used by WAL meta entry points.
static int32_t streamMaybeRecheckVTableCache(SVnode *pVnode, SStreamTriggerReaderInfo *pInfo,
                                             int64_t walVer, SSTriggerWalNewRsp *pRsp);

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

static int32_t buildTableBlock(SSDataBlock* pBlock, int64_t id, int64_t ver, ETableBlockType type) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t index = 0;
  STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &id));
  STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &ver));
  STREAM_CHECK_RET_GOTO(addColData(pBlock, index++, &type));

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

static int32_t createBlockForProcessMeta(SSDataBlock** pBlock) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray* schemas = NULL;

  schemas = taosArrayInit(8, sizeof(SSchema));
  STREAM_CHECK_NULL_GOTO(schemas, terrno);

  int32_t index = 0;
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, index++))  // gid non vtable/uid vtable
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_BIGINT, LONG_BYTES, index++))  // ver
  STREAM_CHECK_RET_GOTO(qStreamBuildSchema(schemas, TSDB_DATA_TYPE_TINYINT, CHAR_BYTES, index++))  // type

  STREAM_CHECK_RET_GOTO(createDataBlockForStream(schemas, pBlock));

end:
  taosArrayDestroy(schemas);
  return code;
}

static int32_t addOneRow(void** tmp, int64_t id, int64_t ver, ETableBlockType type) {
  int32_t  code = 0;
  int32_t  lino = 0;
  if (*tmp == NULL) {
    STREAM_CHECK_RET_GOTO(createBlockForProcessMeta((SSDataBlock**)tmp));
  }
  STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(*tmp, ((SSDataBlock*)(*tmp))->info.rows + 1));
  STREAM_CHECK_RET_GOTO(buildTableBlock(*tmp, id, ver, type));
  ((SSDataBlock*)(*tmp))->info.rows++;
  
end:
  return code;
}

static int32_t addUidListToBlock(SArray* uidListAdd, void** block, int64_t ver, int32_t* totalRows, ETableBlockType type) {
  for (int32_t i = 0; i < taosArrayGetSize(uidListAdd); ++i) {
    uint64_t* uid = taosArrayGet(uidListAdd, i);
    if (uid == NULL) {
      continue;
    }
    int32_t code = addOneRow(block, *uid, ver, type);
    if (code != 0) {
      return code;
    }
    (*totalRows)++;
  }
  return 0;
}

static int32_t qStreamGetAddTable(SStreamTriggerReaderInfo* sStreamReaderInfo, SArray* tableListAdd, SArray* uidListAdd) {
  int32_t      code = 0;
  int32_t      lino = 0;
  if (uidListAdd == NULL) {
    return 0;
  }
  void* pTask = sStreamReaderInfo->pTask;
  
  taosRLockLatch(&sStreamReaderInfo->lock);
  int32_t totalSize = taosArrayGetSize(tableListAdd);
  for (int32_t i = 0; i < totalSize; ++i) {
    STableKeyInfo* info = taosArrayGet(tableListAdd, i);
    if (info == NULL) {
      continue;
    }
    if (taosHashGet(sStreamReaderInfo->tableList.uIdMap, &info->uid, LONG_BYTES) != NULL) {
      continue;
    }
    STREAM_CHECK_NULL_GOTO(taosArrayPush(uidListAdd, &info->uid), terrno);
    ST_TASK_WLOG("%s real add table to list for uid:%" PRId64, __func__, info->uid);
  }

end:
  taosRUnLockLatch(&sStreamReaderInfo->lock);
  return code;
}

static int32_t qStreamGetDelTable(SStreamTriggerReaderInfo* sStreamReaderInfo, SArray* tableListDel, SArray* uidListDel) {
  int32_t      code = 0;
  int32_t      lino = 0;
  if (uidListDel == NULL) {
    return 0;
  }
  void* pTask = sStreamReaderInfo->pTask;
  
  taosRLockLatch(&sStreamReaderInfo->lock);
  int32_t totalSize = taosArrayGetSize(tableListDel);
  for (int32_t i = 0; i < totalSize; ++i) {
    int64_t* uid = taosArrayGet(tableListDel, i);
    if (uid == NULL) {
      continue;
    }
    if (taosHashGet(sStreamReaderInfo->tableList.uIdMap, uid, LONG_BYTES) == NULL) {
      continue;
    }
    STREAM_CHECK_NULL_GOTO(taosArrayPush(uidListDel, uid), terrno);
    ST_TASK_WLOG("%s real del table from list for uid:%" PRId64, __func__, *uid);
  }

end:
  taosRUnLockLatch(&sStreamReaderInfo->lock);
  return code;
}

static int32_t scanDropTableNew(SStreamTriggerReaderInfo* sStreamReaderInfo, SSTriggerWalNewRsp* rsp, void* data, int32_t len,
                             int64_t ver) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SDecoder decoder = {0};
  void* pTask = sStreamReaderInfo->pTask;
  SArray* uidList = NULL;
  SArray* uidListDel = NULL;
  SArray* uidListDelOutTbl = NULL;
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

    if (sStreamReaderInfo->deleteOutTbl != 0) {
      if (uidListDelOutTbl == NULL) {
        uidListDelOutTbl = taosArrayInit(8, sizeof(tb_uid_t));
        STREAM_CHECK_NULL_GOTO(uidListDelOutTbl, terrno);
      }
      STREAM_CHECK_NULL_GOTO(taosArrayPush(uidListDelOutTbl, &pDropTbReq->uid), terrno);
    }
    if (sStreamReaderInfo->isVtableStream) {
      if (uidList == NULL) {
        uidList = taosArrayInit(8, sizeof(tb_uid_t));
        STREAM_CHECK_NULL_GOTO(uidList, terrno);
      }
      STREAM_CHECK_NULL_GOTO(taosArrayPush(uidList, &pDropTbReq->uid), terrno);
    }
    
    ST_TASK_DLOG("stream reader scan drop uid %" PRId64 ", id %" PRIu64, pDropTbReq->uid, id);
  }
  STREAM_CHECK_RET_GOTO(addUidListToBlock(uidListDelOutTbl, &rsp->tableBlock, ver, &rsp->totalRows, TABLE_BLOCK_DROP));

  if (sStreamReaderInfo->isVtableStream) {
    uidListDel = taosArrayInit(8, sizeof(tb_uid_t));
    STREAM_CHECK_NULL_GOTO(uidListDel, terrno);
    STREAM_CHECK_RET_GOTO(qStreamGetDelTable(sStreamReaderInfo, uidList, uidListDel));
    STREAM_CHECK_RET_GOTO(addUidListToBlock(uidListDel, &rsp->tableBlock, ver, &rsp->totalRows, TABLE_BLOCK_RETIRE));
  }
  
end:
  taosArrayDestroy(uidList);
  taosArrayDestroy(uidListDel);
  taosArrayDestroy(uidListDelOutTbl);
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
    int ret = cacheTag(sStreamReaderInfo->pVnode, sStreamReaderInfo->pTableMetaCacheTrigger, sStreamReaderInfo->pExprInfoTriggerTag, sStreamReaderInfo->numOfExprTriggerTag, &sStreamReaderInfo->storageApi, info->uid, 0, NULL);
    if (ret != 0){
      ST_TASK_WLOG("%s cacheTag trigger failed for uid:%" PRId64",code:%d", __func__, info->uid, ret);
      continue;
    }
    ret = cacheTag(sStreamReaderInfo->pVnode, sStreamReaderInfo->pTableMetaCacheCalc, sStreamReaderInfo->pExprInfoCalcTag, sStreamReaderInfo->numOfExprCalcTag, &sStreamReaderInfo->storageApi, info->uid, 0, NULL);
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

static int32_t processTableList(SStreamTriggerReaderInfo* sStreamReaderInfo, SArray* uidList, SArray** tableList) {
  int32_t code = 0;
  int32_t lino = 0;
  SNodeList* groupNew = NULL;   

  if (taosArrayGetSize(uidList) == 0) {
    return 0;
  }
  STREAM_CHECK_RET_GOTO(nodesCloneList(sStreamReaderInfo->partitionCols, &groupNew));  
  STREAM_CHECK_RET_GOTO(qStreamFilterTableListForReader(sStreamReaderInfo->pVnode, uidList, groupNew, sStreamReaderInfo->pTagCond,
                                                    sStreamReaderInfo->pTagIndexCond, &sStreamReaderInfo->storageApi,
                                                    sStreamReaderInfo->groupIdMap, sStreamReaderInfo->suid, tableList));

end:
  nodesDestroyList(groupNew);
  return code;
}

static int32_t scanCreateTableNew(SStreamTriggerReaderInfo* sStreamReaderInfo, SSTriggerWalNewRsp* rsp, void* data, int32_t len,
                             int64_t ver) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SDecoder decoder = {0};
  SArray*  uidList = NULL;
  SArray*  tableList = NULL;
  SArray*  uidListAdd = NULL;
  void* pTask = sStreamReaderInfo->pTask;

  SVCreateTbBatchReq req = {0};
  tDecoderInit(&decoder, data, len);
  
  STREAM_CHECK_RET_GOTO(tDecodeSVCreateTbBatchReq(&decoder, &req));

  uidList = taosArrayInit(8, sizeof(tb_uid_t));
  STREAM_CHECK_NULL_GOTO(uidList, terrno);

  if (sStreamReaderInfo->isVtableStream) {
    uidListAdd = taosArrayInit(8, sizeof(tb_uid_t));
    STREAM_CHECK_NULL_GOTO(uidListAdd, terrno);
  }
  
  SVCreateTbReq* pCreateReq = NULL;
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    pCreateReq = req.pReqs + iReq;
    if (!needReLoadTableList(sStreamReaderInfo, pCreateReq->type, pCreateReq->ctb.suid, pCreateReq->uid, false)) {
      ST_TASK_DLOG("stream reader scan create table jump, %s", pCreateReq->name);
      continue;
    }
    ST_TASK_ILOG("stream reader scan create table %s", pCreateReq->name);
    STREAM_CHECK_NULL_GOTO(taosArrayPush(uidList, &pCreateReq->uid), terrno);
  }
  
  STREAM_CHECK_RET_GOTO(processTableList(sStreamReaderInfo, uidList, &tableList));
  STREAM_CHECK_RET_GOTO(qStreamGetAddTable(sStreamReaderInfo, tableList, uidListAdd));
  if (sStreamReaderInfo->isVtableStream) {
    STREAM_CHECK_RET_GOTO(addUidListToBlock(uidListAdd, &rsp->tableBlock, ver, &rsp->totalRows, TABLE_BLOCK_ADD));
  }

  STREAM_CHECK_RET_GOTO(qStreamModifyTableList(sStreamReaderInfo, tableList, uidList));
end:
  taosArrayDestroy(uidList);
  taosArrayDestroy(uidListAdd);
  taosArrayDestroy(tableList);
  tDeleteSVCreateTbBatchReq(&req);
  tDecoderClear(&decoder);
  return code;
}

static int32_t processAutoCreateTableNew(SStreamTriggerReaderInfo* sStreamReaderInfo, SVCreateTbReq* pCreateReq, int64_t ver) {
  int32_t  code = 0;
  int32_t  lino = 0;
  void*    pTask = sStreamReaderInfo->pTask;
  SArray*  uidList = NULL;
  SArray*  tableList = NULL;

  ST_TASK_DLOG("%s start, name:%s uid:%"PRId64, __func__, pCreateReq->name, pCreateReq->uid);
  if (!needReLoadTableList(sStreamReaderInfo, pCreateReq->type, pCreateReq->ctb.suid, pCreateReq->uid, false) ||
      ignoreMetaChange(sStreamReaderInfo->tableList.version, ver)) {
    ST_TASK_DLOG("stream reader scan auto create table jump, %s", pCreateReq->name);
    goto end;
  }
  uidList = taosArrayInit(8, sizeof(tb_uid_t));
  STREAM_CHECK_NULL_GOTO(uidList, terrno);
  STREAM_CHECK_NULL_GOTO(taosArrayPush(uidList, &pCreateReq->uid), terrno);
  ST_TASK_DLOG("stream reader scan auto create table %s", pCreateReq->name);

  STREAM_CHECK_RET_GOTO(processTableList(sStreamReaderInfo, uidList, &tableList));
  STREAM_CHECK_RET_GOTO(qStreamModifyTableList(sStreamReaderInfo, tableList, uidList));
end:
  taosArrayDestroy(uidList);
  taosArrayDestroy(tableList);
  return code;
}

static bool isColIdInList(SNodeList* colList, col_id_t cid){
  int32_t  code = 0;
  int32_t  lino = 0;
  SNode*  nodeItem = NULL;
  FOREACH(nodeItem, colList) {
    SNode*           pNode = ((STargetNode*)nodeItem)->pExpr;
    if (nodeType(pNode) == QUERY_NODE_COLUMN) {
      SColumnNode*     valueNode = (SColumnNode*)(pNode);
      if (cid == valueNode->colId) {
        return true;
      }
    }
  }
end:
  return false;
}

static bool isAlteredTable(int8_t action, ETableType tbType) {
  if (action == TSDB_ALTER_TABLE_UPDATE_MULTI_TABLE_TAG_VAL && tbType == TSDB_CHILD_TABLE) {
    return true;
  } else if (action == TSDB_ALTER_TABLE_UPDATE_CHILD_TABLE_TAG_VAL && tbType == TSDB_SUPER_TABLE) {
    return true;
  } else if ((action == TSDB_ALTER_TABLE_ALTER_COLUMN_REF || action == TSDB_ALTER_TABLE_REMOVE_COLUMN_REF) && 
     (tbType == TSDB_VIRTUAL_CHILD_TABLE || tbType == TSDB_VIRTUAL_NORMAL_TABLE)) {
    return true;
  }
  return false;
}

void getAlterColId(void* pVnode, int64_t uid, const char* colName, col_id_t* colId) {
  SSchemaWrapper *pSchema = metaGetTableSchema(((SVnode *)pVnode)->pMeta, uid, -1, 1, NULL, 0, false);
  if (pSchema == NULL) {
    return;
  }
  for (int32_t i = 0; i < pSchema->nCols; i++) {
    if (strncmp(pSchema->pSchema[i].name, colName, TSDB_COL_NAME_LEN) == 0) {
      *colId = pSchema->pSchema[i].colId;
      break;
    }
  }
  tDeleteSchemaWrapper(pSchema);
  return;
}

// Handle TSDB_ALTER_TABLE_ALTER_COLUMN_REF and TSDB_ALTER_TABLE_REMOVE_COLUMN_REF
static int32_t scanAlterTableColumnRef(SStreamTriggerReaderInfo* sStreamReaderInfo, SSTriggerWalNewRsp* rsp, 
                                       SVAlterTbReq* pReq, uint64_t uid, int64_t ver) {
  int32_t code = 0;
  int32_t lino = 0;
  void* pTask = sStreamReaderInfo->pTask;
  SArray* uidListAdd = NULL;

  uidListAdd = taosArrayInit(8, sizeof(tb_uid_t));
  STREAM_CHECK_NULL_GOTO(uidListAdd, terrno);

  uint64_t id = 0;
  STREAM_CHECK_CONDITION_GOTO(!uidInTableListOrigin(sStreamReaderInfo, sStreamReaderInfo->suid, uid, &id), TDB_CODE_SUCCESS);

  col_id_t colId = 0;
  getAlterColId(sStreamReaderInfo->pVnode, uid, pReq->colName, &colId);
  if (atomic_load_8(&sStreamReaderInfo->isVtableOnlyTs) == 0 && !isColIdInList(sStreamReaderInfo->triggerCols, colId)) {
    ST_TASK_ILOG("stream reader scan alter table %s, colId %d not in trigger cols", pReq->tbName, colId);
    goto end;
  }

  STREAM_CHECK_NULL_GOTO(taosArrayPush(uidListAdd, &uid), terrno);
  STREAM_CHECK_RET_GOTO(addUidListToBlock(uidListAdd, &rsp->tableBlock, ver, &rsp->totalRows, TABLE_BLOCK_ADD));

  ST_TASK_DLOG("stream reader scan alter table column ref %s", pReq->tbName);

end:
  taosArrayDestroy(uidListAdd);
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  return code;
}

static int32_t checkAlter(SStreamTriggerReaderInfo* sStreamReaderInfo, char* tbName, int8_t action, uint64_t *uid) {
  int32_t  code = 0;
  int32_t  lino = 0;
  ETableType tbType = 0;
  uint64_t suid = 0;

  STREAM_CHECK_RET_GOTO(metaGetTableTypeSuidByName(sStreamReaderInfo->pVnode, tbName, &tbType, &suid));
  STREAM_CHECK_CONDITION_GOTO(!isAlteredTable(action, tbType), TDB_CODE_SUCCESS);
  STREAM_CHECK_CONDITION_GOTO(suid != sStreamReaderInfo->suid, TDB_CODE_SUCCESS);
  if (action == TSDB_ALTER_TABLE_UPDATE_CHILD_TABLE_TAG_VAL) {
    *uid = suid;
    goto end;
  }
  STREAM_CHECK_RET_GOTO(metaGetTableUidByName(sStreamReaderInfo->pVnode, tbName, uid));

end:
  return code;
}

static SArray* getTableListForAlterSuperTable(SStreamTriggerReaderInfo* sStreamReaderInfo, SVAlterTbReq* pReq){
  int32_t code = 0;
  int32_t lino = 0;
  void* pTask = sStreamReaderInfo->pTask;
  SArray* uidList = taosArrayInit(8, sizeof(tb_uid_t));
  STREAM_CHECK_NULL_GOTO(uidList, terrno);
  for (int32_t i = 0; i < taosArrayGetSize(pReq->tables); i++) {
    SUpdateTableTagVal *pTable = taosArrayGet(pReq->tables, i);
    uint64_t uid = 0;
    code = checkAlter(sStreamReaderInfo, pTable->tbName, pReq->action, &uid);
    if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST || uid == 0) {
      code = 0;
      ST_TASK_WLOG("stream reader scan alter ctable table %s not exist, %s %"PRIu64, pTable->tbName, __func__, uid);
      continue;
    }
    STREAM_CHECK_RET_GOTO(code);
    STREAM_CHECK_NULL_GOTO(taosArrayPush(uidList, (const void *)&uid), terrno);
  }

end:
  if (code != 0) {
    ST_TASK_ELOG("%s failed,code:%d", __func__, code);
    taosArrayDestroy(uidList);
    uidList = NULL;
  }
  return uidList;
}

// Handle TSDB_ALTER_TABLE_UPDATE_CHILD_TABLE_TAG_VAL and TSDB_ALTER_TABLE_UPDATE_MULTI_TABLE_TAG_VAL
static int32_t scanAlterTableTagVal(SStreamTriggerReaderInfo* sStreamReaderInfo, SSTriggerWalNewRsp* rsp, 
                                    SArray* uidList, int64_t ver) {
  int32_t code = 0;
  int32_t lino = 0;
  void* pTask = sStreamReaderInfo->pTask;
  SArray* uidListAdd = NULL;
  SArray* uidListDel = NULL;
  SArray* tableList = NULL;

  if (sStreamReaderInfo->isVtableStream) {
    uidListAdd = taosArrayInit(8, sizeof(tb_uid_t));
    STREAM_CHECK_NULL_GOTO(uidListAdd, terrno);
  }

  uidListDel = taosArrayInit(8, sizeof(tb_uid_t));
  STREAM_CHECK_NULL_GOTO(uidListDel, terrno);

  STREAM_CHECK_RET_GOTO(processTableList(sStreamReaderInfo, uidList, &tableList));
  STREAM_CHECK_RET_GOTO(qStreamGetDelTable(sStreamReaderInfo, uidList, uidListDel));

  if (rsp->checkAlter && taosArrayGetSize(uidListDel) > 0 && rsp->totalDataRows > 0) {
    rsp->needReturn = true;
    rsp->ver--;
    ST_TASK_DLOG("%s stream reader scan alter table need return data", __func__);
    goto end;
  }

  STREAM_CHECK_RET_GOTO(qStreamGetAddTable(sStreamReaderInfo, tableList, uidListAdd));
  if (sStreamReaderInfo->isVtableStream) {
    STREAM_CHECK_RET_GOTO(addUidListToBlock(uidListAdd, &rsp->tableBlock, ver, &rsp->totalRows, TABLE_BLOCK_ADD));
    STREAM_CHECK_RET_GOTO(addUidListToBlock(uidListDel, &rsp->tableBlock, ver, &rsp->totalRows, TABLE_BLOCK_RETIRE));
  }
  STREAM_CHECK_RET_GOTO(qStreamModifyTableList(sStreamReaderInfo, tableList, uidList));

  ST_TASK_DLOG("%s stream reader scan alter table tag val", __func__);

end:
  taosArrayDestroy(uidListAdd);
  taosArrayDestroy(uidListDel);
  taosArrayDestroy(tableList);
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  return code;
}

static int32_t scanAlterTableNew(SStreamTriggerReaderInfo* sStreamReaderInfo, SSTriggerWalNewRsp* rsp, void* data, int32_t len, int64_t ver) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SDecoder decoder = {0};
  void* pTask = sStreamReaderInfo->pTask;
  SArray* uidList = NULL;

  ST_TASK_DLOG("%s start", __func__);

  SVAlterTbReq req = {0};
  tDecoderInit(&decoder, data, len);
  
  STREAM_CHECK_RET_GOTO(tDecodeSVAlterTbReq(&decoder, &req));

  STREAM_CHECK_CONDITION_GOTO(req.action != TSDB_ALTER_TABLE_UPDATE_MULTI_TABLE_TAG_VAL && req.action != TSDB_ALTER_TABLE_UPDATE_CHILD_TABLE_TAG_VAL && 
    req.action != TSDB_ALTER_TABLE_ALTER_COLUMN_REF && req.action != TSDB_ALTER_TABLE_REMOVE_COLUMN_REF, TDB_CODE_SUCCESS);

  uint64_t uid = 0;
  if (req.action == TSDB_ALTER_TABLE_ALTER_COLUMN_REF || req.action == TSDB_ALTER_TABLE_REMOVE_COLUMN_REF) {
    STREAM_CHECK_CONDITION_GOTO(!sStreamReaderInfo->isVtableStream, TDB_CODE_SUCCESS);
    code = checkAlter(sStreamReaderInfo, req.tbName, req.action, &uid);
    if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST || uid == 0) {
      ST_TASK_WLOG("stream reader scan alter ref table %s not exist, %s uid:%" PRIu64, req.tbName, __func__, uid);
      code = 0;
      goto end;
    }
    STREAM_CHECK_RET_GOTO(scanAlterTableColumnRef(sStreamReaderInfo, rsp, &req, uid, ver));
  } else if (req.action == TSDB_ALTER_TABLE_UPDATE_MULTI_TABLE_TAG_VAL) {
    uidList = getTableListForAlterSuperTable(sStreamReaderInfo, &req);
    STREAM_CHECK_NULL_GOTO(uidList, terrno);
    STREAM_CHECK_RET_GOTO(scanAlterTableTagVal(sStreamReaderInfo, rsp, uidList, ver));
  } else if (req.action == TSDB_ALTER_TABLE_UPDATE_CHILD_TABLE_TAG_VAL) {
    code = checkAlter(sStreamReaderInfo, req.tbName, req.action, &uid);
    if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST || uid == 0) {
      ST_TASK_WLOG("stream reader scan alter suid table %s not exist, %s uid:%" PRIu64, req.tbName, __func__, uid);
      code = 0;
      goto end;
    }
    uidList = taosArrayInit(8, sizeof(uint64_t));
    STREAM_CHECK_NULL_GOTO(uidList, terrno);
    STREAM_CHECK_RET_GOTO(vnodeGetCtbIdList(sStreamReaderInfo->pVnode, uid, uidList));
    STREAM_CHECK_RET_GOTO(scanAlterTableTagVal(sStreamReaderInfo, rsp, uidList, ver));
  }

  ST_TASK_DLOG("%s stream reader scan alter table", __func__);

end:
  destroyAlterTbReq(&req);

  taosArrayDestroy(uidList);
  tDecoderClear(&decoder);
  STREAM_PRINT_LOG_END_WITHID(code, lino);
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
  
//   STREAM_CHECK_RET_GOTO(processTableList(sStreamReaderInfo));

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

static int32_t scanSubmitTbDataForMeta(SDecoder *pCoder, SStreamTriggerReaderInfo* sStreamReaderInfo, SSHashObj* gidHash, int64_t ver) {
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
    STREAM_CHECK_RET_GOTO(processAutoCreateTableNew(sStreamReaderInfo, submitTbData.pCreateTbReq, ver));
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

  for (uint64_t i = 0; i < nSubmitTbData; i++) {
    STREAM_CHECK_RET_GOTO(scanSubmitTbDataForMeta(&decoder, sStreamReaderInfo, gidHash, ver));
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

static int32_t processMeta(int16_t msgType, SStreamTriggerReaderInfo* sStreamReaderInfo, void *data, int32_t len, SSTriggerWalNewRsp* rsp, int64_t ver) {
  int32_t code = 0;
  int32_t lino = 0;
  void* pTask = sStreamReaderInfo->pTask;

  ST_TASK_DLOG("%s check meta msg, stream ver:%" PRId64 ", wal ver:%" PRId64, __func__, sStreamReaderInfo->tableList.version, ver);

  SDecoder dcoder = {0};
  tDecoderInit(&dcoder, data, len);
  if (msgType == TDMT_VND_DELETE && sStreamReaderInfo->deleteReCalc != 0) {
    if (rsp->deleteBlock == NULL) {
      STREAM_CHECK_RET_GOTO(createBlockForWalMetaNew((SSDataBlock**)&rsp->deleteBlock));
    }
      
    STREAM_CHECK_RET_GOTO(scanDeleteDataNew(sStreamReaderInfo, rsp, data, len, ver));
  } else if (msgType == TDMT_VND_DROP_TABLE && 
    (sStreamReaderInfo->deleteOutTbl != 0 || sStreamReaderInfo->isVtableStream)) {
    STREAM_CHECK_RET_GOTO(scanDropTableNew(sStreamReaderInfo, rsp, data, len, ver));
  // } else if (msgType == TDMT_VND_DROP_STB) {
  //   STREAM_CHECK_RET_GOTO(scanDropSTableNew(sStreamReaderInfo, data, len));
  } else if (msgType == TDMT_VND_CREATE_TABLE && !ignoreMetaChange(sStreamReaderInfo->tableList.version, ver)) {
    STREAM_CHECK_RET_GOTO(scanCreateTableNew(sStreamReaderInfo, rsp, data, len, ver));
  } else if (msgType == TDMT_VND_ALTER_STB && !ignoreMetaChange(sStreamReaderInfo->tableList.version, ver)) {
    // STREAM_CHECK_RET_GOTO(scanAlterSTableNew(sStreamReaderInfo, data, len));
  } else if (msgType == TDMT_VND_ALTER_TABLE && !ignoreMetaChange(sStreamReaderInfo->tableList.version, ver)) {
    STREAM_CHECK_RET_GOTO(scanAlterTableNew(sStreamReaderInfo, rsp, data, len, ver));
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
      rsp->verTime = 0;
    } else {
      rsp->verTime = taosGetTimestampUs();
    }
    ST_TASK_DLOG("vgId:%d %s scan wal end:%s", TD_VID(pVnode), __func__, tstrerror(code));
    code = TSDB_CODE_SUCCESS;
    goto end;
  }
  STREAM_CHECK_RET_GOTO(code);

  STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(rsp->metaBlock, STREAM_RETURN_ROWS_NUM));
  while (1) {
    code = walNextValidMsg(pWalReader, true);
    if (code == TSDB_CODE_WAL_LOG_NOT_EXIST){
      rsp->verTime = taosGetTimestampUs();
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

    ST_TASK_DLOG("vgId:%d stream reader scan wal ver:%" PRId64 "/%" PRId64 ", type:%s, deleteData:%d, deleteTb:%d",
      TD_VID(pVnode), ver, walGetAppliedVer(pWalReader->pWal), TMSG_INFO(wCont->msgType), sStreamReaderInfo->deleteReCalc, sStreamReaderInfo->deleteOutTbl);
    if (wCont->msgType == TDMT_VND_SUBMIT) {
      // return when getting data if there are meta data in vtable scan
      if (sStreamReaderInfo->isVtableStream && rsp->tableBlock != NULL && ((SSDataBlock*)rsp->tableBlock)->info.rows > 0) {
        rsp->ver--;
        break;
      }
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

int32_t cacheTag(SVnode* pVnode, SHashObj* metaCache, SExprInfo* pExprInfo, int32_t numOfExpr, SStorageAPI* api, uint64_t uid, col_id_t colId, SRWLatch* lock) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SMetaReader mr = {0};
  SArray* tagCache = NULL;
  char* data = NULL;

  if (lock != NULL) taosWLockLatch(lock);
  STREAM_CHECK_CONDITION_GOTO(numOfExpr == 0, code);
  stDebug("%s start,uid:%"PRIu64, __func__, uid);
  void* uidData = taosHashGet(metaCache, &uid, LONG_BYTES);
  if (uidData == NULL) {
    tagCache = taosArrayInit(numOfExpr, POINTER_BYTES);
    STREAM_CHECK_NULL_GOTO(tagCache, terrno);
    if(taosHashPut(metaCache, &uid, LONG_BYTES, &tagCache, POINTER_BYTES) != 0) {
      taosArrayDestroy(tagCache);
      code = terrno;
      goto end;
    }
  } else {
    tagCache = *(SArray**)uidData;
    stDebug("%s found tagCache, size:%zu %d, uid:%"PRIu64, __func__, taosArrayGetSize(tagCache), numOfExpr, uid);
    STREAM_CHECK_CONDITION_GOTO(taosArrayGetSize(tagCache) != numOfExpr, TSDB_CODE_INVALID_PARA);
  }
  
  api->metaReaderFn.initReader(&mr, pVnode, META_READER_LOCK, &api->metaFn);
  code = api->metaReaderFn.getEntryGetUidCache(&mr, uid);
  api->metaReaderFn.readerReleaseLock(&mr);
  STREAM_CHECK_RET_GOTO(code);
  
  for (int32_t j = 0; j < numOfExpr; ++j) {
    const SExprInfo* pExpr1 = &pExprInfo[j];
    int32_t functionId = pExpr1->pExpr->_function.functionId;
    col_id_t cid = 0;
    // this is to handle the tbname
    if (fmIsScanPseudoColumnFunc(functionId)) {
      int32_t fType = pExpr1->pExpr->_function.functionType;
      if (fType == FUNCTION_TYPE_TBNAME) {
        data = taosMemoryCalloc(1, strlen(mr.me.name) + VARSTR_HEADER_SIZE);
        STREAM_CHECK_NULL_GOTO(data, terrno);
        STR_TO_VARSTR(data, mr.me.name)
      }
      cid = -1;
    } else {  // these are tags
      const char* p = NULL;
      char* pData = NULL;
      int8_t type = pExpr1->base.resSchema.type;
      int32_t len = pExpr1->base.resSchema.bytes;
      STagVal tagVal = {0};
      tagVal.cid = pExpr1->base.pParam[0].pCol->colId;
      cid = tagVal.cid;
      if (colId != 0 && cid != colId) {
        continue;
      }
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
  if (lock != NULL) taosWUnLockLatch(lock);
  return code;
}

int32_t fillTag(SHashObj* metaCache, SExprInfo* pExprInfo, int32_t numOfExpr,
                uint64_t uid, SSDataBlock* pBlock, uint32_t currentRow, uint32_t numOfRows, uint32_t numOfBlocks, SRWLatch* lock) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SArray* tagCache = NULL;
  if (numOfExpr == 0) {
    return TSDB_CODE_SUCCESS;
  }

  taosRLockLatch(lock);
  void* uidData = taosHashGet(metaCache, &uid, LONG_BYTES);
  if (uidData == NULL) {
    stError("%s error uidData is null,uid:%"PRIu64, __func__, uid);
  } else {
    tagCache = *(SArray**)uidData;
    if(taosArrayGetSize(tagCache) != numOfExpr) {
      stError("%s numOfExpr:%d,tagCache size:%zu", __func__, numOfExpr, taosArrayGetSize(tagCache));
      tagCache = NULL;
    }
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
    char* data = tagCache == NULL ? NULL : taosArrayGetP(tagCache, j);

    bool isNullVal = (data == NULL) || (pColInfoData->info.type == TSDB_DATA_TYPE_JSON && tTagIsJsonNull(data));
    if (isNullVal) {
      colDataSetNNULL(pColInfoData, currentRow, numOfRows);
    } else {
      if (!IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
        for (uint32_t i = 0; i < numOfRows; i++){
          colDataClearNull_f(pColInfoData->nullbitmap, currentRow + i);
        }
      }
      code = colDataSetNItems(pColInfoData, currentRow, (const char*)data, numOfRows, numOfBlocks, false);
      STREAM_CHECK_RET_GOTO(code);
    }
  }
end:
  taosRUnLockLatch(lock);
  return code;
}

static int32_t processTag(SStreamTriggerReaderInfo* info, bool isCalc, 
  uint64_t uid, SSDataBlock* pBlock, uint32_t currentRow, uint32_t numOfRows, uint32_t numOfBlocks) {
  int32_t     code = 0;
  int32_t     lino = 0;

  void* pTask = info->pTask;
  ST_TASK_DLOG("%s start. rows:%" PRIu32 ",uid:%"PRIu64, __func__,  numOfRows, uid);
  
  SHashObj* metaCache = isCalc ? info->pTableMetaCacheCalc : info->pTableMetaCacheTrigger;
  SExprInfo*   pExprInfo = isCalc ? info->pExprInfoCalcTag : info->pExprInfoTriggerTag; 
  int32_t      numOfExpr = isCalc ? info->numOfExprCalcTag : info->numOfExprTriggerTag;
  
  code = fillTag(metaCache, pExprInfo, numOfExpr, uid, pBlock, currentRow, numOfRows, numOfBlocks, &info->lock);
  STREAM_CHECK_RET_GOTO(code);

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

  ST_TASK_DLOG("%s uid:%" PRId64 ", suid:%" PRId64 ", ver:%" PRId64, __func__, submitTbData.uid, submitTbData.suid, ver);

  if (rsp->uidHash != NULL) {
    uint64_t* gid = tSimpleHashGet(rsp->uidHash, &submitTbData.uid, LONG_BYTES);
    STREAM_CHECK_CONDITION_GOTO(gid == NULL, TDB_CODE_SUCCESS);
    ST_TASK_DLOG("%s get uid gid from uidHash, uid:%" PRId64 ", suid:%" PRId64 " gid:%"PRIu64, __func__, submitTbData.uid, submitTbData.suid, *gid);
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
    ST_TASK_DLOG("%s get time range from ranges, uid:%" PRId64 ", suid:%" PRId64 ", gid:%" PRIu64 ", skey:%" PRId64 ", ekey:%" PRId64,
      __func__, submitTbData.uid, submitTbData.suid, id, window.skey, window.ekey);
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
    for (uint64_t iRow = 0; iRow < nRow; ++iRow) {
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
      STREAM_CHECK_RET_GOTO(processTag(sStreamReaderInfo, rsp->isCalc, submitTbData.uid, pBlock, blockStart, numOfRows, 1));
    }
    
    SColumnInfoData* pColData = taosArrayGetLast(pBlock->pDataBlock);
    STREAM_CHECK_NULL_GOTO(pColData, terrno);
    STREAM_CHECK_RET_GOTO(colDataSetNItems(pColData, blockStart, (const char*)&ver, numOfRows, 1, false));

    STREAM_CHECK_NULL_GOTO(pSlice, TSDB_CODE_INVALID_PARA);
    ST_TASK_DLOG("%s process submit data:skey %" PRId64 ", ekey %" PRId64 ", id %" PRIu64
      ", uid:%" PRId64 ", ver:%"PRId64 ", row index:%d, rows:%d", __func__, window.skey, window.ekey, 
      id, submitTbData.uid, ver, pSlice->currentRowIdx, numOfRows);
    pSlice->currentRowIdx += numOfRows;
    pBlock->info.rows += numOfRows;
  } else {
    ST_TASK_DLOG("%s no valid data in time range:skey %" PRId64 ", ekey %" PRId64 ", uid:%" PRId64 ", suid:%" PRId64,
      __func__, window.skey, window.ekey, submitTbData.uid, submitTbData.suid);
  }
  
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

  for (uint64_t i = 0; i < nSubmitTbData; i++) {
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
      rsp->totalRows++;
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
  uint64_t* gid, int64_t* uid, int32_t* numOfRows, SSTriggerWalNewRsp* rsp, int64_t ver) {
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
    STREAM_CHECK_RET_GOTO(processAutoCreateTableNew(sStreamReaderInfo, submitTbData.pCreateTbReq, ver));
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
  ST_TASK_DLOG("%s uid:%" PRId64 ", suid:%" PRId64, __func__, *uid, submitTbData.suid);
  STREAM_CHECK_CONDITION_GOTO(!uidInTableListSet(sStreamReaderInfo, submitTbData.suid, *uid, gid, rsp->isCalc), TDB_CODE_SUCCESS);
  if (rsp->uidHash != NULL) {
    STREAM_CHECK_RET_GOTO(tSimpleHashPut(rsp->uidHash, uid, LONG_BYTES, gid, LONG_BYTES));
    ST_TASK_DLOG("%s put uid into uidHash, uid:%" PRId64 ", suid:%" PRId64 " gid:%"PRIu64, __func__, *uid, submitTbData.suid, *gid);
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
      for (uint64_t iRow = 0; iRow < nRow; ++iRow) {
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
  ST_TASK_DLOG("%s nSubmitTbData:%" PRIu64 ", ver:%"PRId64 " bodyLen:%d", __func__, nSubmitTbData, ver, len);

  for (int32_t i = 0; i < nSubmitTbData; i++) {
    uint64_t gid = -1;
    int64_t  uid = 0;
    int32_t numOfRows = 0;
    STREAM_CHECK_RET_GOTO(scanSubmitTbDataPre(&decoder, sStreamReaderInfo, ranges, &gid, &uid, &numOfRows, rsp, ver));
    if (numOfRows <= 0) {
      ST_TASK_DLOG("%s no valid data uid:%" PRId64 ", gid:%" PRIu64 ", numOfRows:%d, ver:%"PRId64, __func__, uid, gid, numOfRows, ver);
      continue;
    }
    rsp->totalRows += numOfRows;
    rsp->totalDataRows += numOfRows;

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
      resultRsp->verTime = 0;
    } else {
      resultRsp->verTime = taosGetTimestampUs();
    }
    ST_TASK_DLOG("%s scan wal end:%s",  __func__, tstrerror(code));
    code = TSDB_CODE_SUCCESS;
    goto end;
  }
  STREAM_CHECK_RET_GOTO(code);

  while (1) {
    code = walNextValidMsg(pWalReader, true);
    if (code == TSDB_CODE_WAL_LOG_NOT_EXIST){
      resultRsp->verTime = taosGetTimestampUs();
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
    ST_TASK_DLOG("%s scan wal ver:%" PRId64 ", type:%s, deleteData:%d, deleteTb:%d, msg len:%d", __func__,
      ver, TMSG_INFO(wCont->msgType), sStreamReaderInfo->deleteReCalc, sStreamReaderInfo->deleteOutTbl, len);
    if (wCont->msgType == TDMT_VND_SUBMIT) {
      // return when getting data if there are meta data in vtable scan
      if (sStreamReaderInfo->isVtableStream && resultRsp->tableBlock != NULL && ((SSDataBlock*)resultRsp->tableBlock)->info.rows > 0) {
        resultRsp->ver--;
        break;
      }
      data = POINTER_SHIFT(wCont->body, sizeof(SSubmitReq2Msg));
      len = wCont->bodyLen - sizeof(SSubmitReq2Msg);
      STREAM_CHECK_RET_GOTO(scanSubmitDataPre(sStreamReaderInfo, data, len, NULL, resultRsp, ver));
    } else {
      STREAM_CHECK_RET_GOTO(processMeta(wCont->msgType, sStreamReaderInfo, data, len, resultRsp, ver));
    }

    ST_TASK_DLOG("%s scan wal next ver:%" PRId64 ", totalRows:%d", __func__, resultRsp->ver, resultRsp->totalRows);
    if (resultRsp->totalRows >= STREAM_RETURN_ROWS_NUM || resultRsp->needReturn) {
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
    ST_TASK_DLOG("process wal ver:%" PRId64 ", type:%d, bodyLen:%d", wCont->version, wCont->msgType, bodyLen);
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
                                    STimeRangeNode* node, SReadHandle* handle, bool isExtWin) {
  int32_t code = 0;
  int32_t lino = 0;
  void* pTask = sStreamReaderCalcInfo->pTask;
  STimeWindow* pWin = isExtWin ? &handle->extWinRange : &handle->winRange;
  bool* pValid = isExtWin ? &handle->extWinRangeValid : &handle->winRangeValid;
  
  if (req->pStRtFuncInfo->withExternalWindow) {
    sStreamReaderCalcInfo->tmpRtFuncInfo.curIdx = 0;
    sStreamReaderCalcInfo->tmpRtFuncInfo.triggerType = req->pStRtFuncInfo->triggerType;
    sStreamReaderCalcInfo->tmpRtFuncInfo.isWindowTrigger = req->pStRtFuncInfo->isWindowTrigger;
    sStreamReaderCalcInfo->tmpRtFuncInfo.precision = req->pStRtFuncInfo->precision;

    SSTriggerCalcParam* pFirst = NULL;
    SSTriggerCalcParam* pLast = NULL;
    if (req->pStRtFuncInfo->isMultiGroupCalc) {
      SSTriggerGroupReadInfo* pGrp = taosArrayGet(req->pStRtFuncInfo->curGrpRead, 0);
      pFirst = &pGrp->firstParam;
      pLast = &pGrp->lastParam;
    } else {
      pFirst = taosArrayGet(req->pStRtFuncInfo->pStreamPesudoFuncVals, 0);
      pLast = taosArrayGetLast(req->pStRtFuncInfo->pStreamPesudoFuncVals);
      STREAM_CHECK_NULL_GOTO(pFirst, terrno);
      STREAM_CHECK_NULL_GOTO(pLast, terrno);
    }

    if (!node->needCalc) {
      pWin->skey = pFirst->wstart;
      pWin->ekey = pLast->wend;
      *pValid = true;
      if (req->pStRtFuncInfo->triggerType == STREAM_TRIGGER_SLIDING) {
        pWin->ekey--;
      }
    } else {
      SSTriggerCalcParam* pTmp = taosArrayGet(sStreamReaderCalcInfo->tmpRtFuncInfo.pStreamPesudoFuncVals, 0);
      memcpy(pTmp, pFirst, sizeof(*pTmp));

      STREAM_CHECK_RET_GOTO(streamCalcCurrWinTimeRange(node, &sStreamReaderCalcInfo->tmpRtFuncInfo, pWin, pValid, 1));
      if (*pValid) {
        int64_t skey = pWin->skey;

        memcpy(pTmp, pLast, sizeof(*pTmp));
        STREAM_CHECK_RET_GOTO(streamCalcCurrWinTimeRange(node, &sStreamReaderCalcInfo->tmpRtFuncInfo, pWin, pValid, 2));

        if (*pValid) {
          pWin->skey = skey;
        }
      }
      pWin->ekey--;
    }
  } else {
    if (!node->needCalc) {
      SSTriggerCalcParam* pCurr = taosArrayGet(req->pStRtFuncInfo->pStreamPesudoFuncVals, req->pStRtFuncInfo->curIdx);
      pWin->skey = pCurr->wstart;
      pWin->ekey = pCurr->wend;
      *pValid = true;
      if (req->pStRtFuncInfo->triggerType == STREAM_TRIGGER_SLIDING) {
        pWin->ekey--;
      }
    } else {
      STREAM_CHECK_RET_GOTO(streamCalcCurrWinTimeRange(node, req->pStRtFuncInfo, pWin, pValid, 3));
      pWin->ekey--;
    }
  }

  ST_TASK_DLOG("%s type:%s, withExternalWindow:%d, skey:%" PRId64 ", ekey:%" PRId64 ", validRange:%d", 
      __func__, isExtWin ? "interp range" : "scan time range", req->pStRtFuncInfo->withExternalWindow, pWin->skey, pWin->ekey, *pValid);

end:

  if (code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  
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

  stDebug("%s getAllTs enter: pNum:%d suid:%"PRId64" order:%d skey:%"PRId64" ekey:%"PRId64" verRange:[%"PRId64",%"PRId64"]",
          pTaskInner->idStr, pNum, pTaskInner->options->suid, pTaskInner->options->order,
          pTaskInner->options->twindows.skey, pTaskInner->options->twindows.ekey,
          (int64_t)-1, pTaskInner->options->ver);
  for (int32_t i = 0; i < pNum; i++) {
    stDebug("%s getAllTs table[%d]: uid:%"PRId64, pTaskInner->idStr, i, pList[i].uid);
  }

  STREAM_CHECK_RET_GOTO(pTaskInner->storageApi->tsdReader.tsdCreateFirstLastTsIter(pVnode, &pTaskInner->options->twindows, &(SVersionRange){.minVer = -1, .maxVer = pTaskInner->options->ver},
                                                pTaskInner->options->suid, pList, pNum, pTaskInner->options->order, &pTaskInner->pReader, pTaskInner->idStr));
  bool hasNext = true;
  int32_t iterCount = 0;
  while(1){
    STREAM_CHECK_RET_GOTO(pTaskInner->storageApi->tsdReader.tsdNextFirstLastTsBlock(pTaskInner->pReader, pResBlock, &hasNext));
    stDebug("%s getAllTs iter[%d]: hasNext:%d pResBlock->info.rows:%"PRId64, pTaskInner->idStr, iterCount++, hasNext, pResBlock->info.rows);
    STREAM_CHECK_CONDITION_GOTO(!hasNext, TDB_CODE_SUCCESS);
  }

end:
  stDebug("%s getAllTs done: code:%d pResBlock->info.rows:%"PRId64, pTaskInner->idStr, code, pResBlock ? pResBlock->info.rows : -1);
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
  ST_TASK_DLOG("vgId:%d %s qStreamGetTableList returned pNum:%d", TD_VID(pVnode), __func__, pNum);
  STREAM_CHECK_CONDITION_GOTO(pNum == 0, TSDB_CODE_SUCCESS);
  STREAM_CHECK_RET_GOTO(createDataBlockTsUid(&pResBlock, pNum));

  pTaskInner->options->suid = sStreamReaderInfo->suid;
  ST_TASK_DLOG("vgId:%d %s calling getAllTs: suid:%"PRId64" order:%d skey:%"PRId64" ekey:%"PRId64" ver:%"PRId64,
               TD_VID(pVnode), __func__, pTaskInner->options->suid, pTaskInner->options->order,
               pTaskInner->options->twindows.skey, pTaskInner->options->twindows.ekey, pTaskInner->options->ver);
  STREAM_CHECK_RET_GOTO(getAllTs(pVnode, pResBlock, pTaskInner, pList, pNum));
  ST_TASK_DLOG("vgId:%d %s getAllTs done: pResBlock rows:%"PRId64, TD_VID(pVnode), __func__, pResBlock->info.rows);
  int32_t order = pTaskInner->options->order;
  if (pResBlock->info.rows == 0 && sStreamReaderInfo->groupByTbname) {
    tsRsp->tsInfo = taosArrayInit(pNum, sizeof(STsInfo));
    STREAM_CHECK_NULL_GOTO(tsRsp->tsInfo, terrno);
    for (int32_t i = 0; i < pNum; i++) {
      STsInfo* tsInfo = taosArrayReserve(tsRsp->tsInfo, 1);
      STREAM_CHECK_NULL_GOTO(tsInfo, terrno);
      tsInfo->gId = pList[i].uid;
      tsInfo->ts = 0;
      ST_TASK_DLOG("%s no data but return gId (uid):%" PRIu64 " for tbname partition", __func__, tsInfo->gId);
    }
    goto end;
  }

  STREAM_CHECK_CONDITION_GOTO(pResBlock->info.rows == 0, TDB_CODE_SUCCESS);

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

  ST_TASK_DLOG("vgId:%d %s start, trigger hash size:%d, calc hash size:%d, appver:%"PRId64, TD_VID(pVnode), __func__,
                tSimpleHashGetSize(req->setTableReq.uidInfoTrigger), tSimpleHashGetSize(req->setTableReq.uidInfoCalc), pVnode->state.applied);

  taosWLockLatch(&sStreamReaderInfo->lock);
  TSWAP(sStreamReaderInfo->uidHashTrigger, req->setTableReq.uidInfoTrigger);
  TSWAP(sStreamReaderInfo->uidHashCalc, req->setTableReq.uidInfoCalc);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->uidHashTrigger, TSDB_CODE_INVALID_PARA);
  STREAM_CHECK_NULL_GOTO(sStreamReaderInfo->uidHashCalc, TSDB_CODE_INVALID_PARA);

  qStreamClearTableInfo(&sStreamReaderInfo->vSetTableList);
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

  BUILD_OPTION(options, 0, sStreamReaderInfo->tableList.version, TSDB_ORDER_DESC, INT64_MIN, INT64_MAX, NULL, false, NULL);
  STREAM_CHECK_RET_GOTO(createStreamTaskForTs(&options, &pTaskInner, &sStreamReaderInfo->storageApi));

  tsRsp.ver = sStreamReaderInfo->tableList.version + 1;

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
  ST_TASK_DLOG("vgId:%d %s start, startTime:%"PRId64" ver:%"PRId64" gid:%"PRId64
               " applied:%"PRId64" tableListNum:%d isVtable:%d groupByTbname:%d partitionCols:%p",
               TD_VID(pVnode), __func__, req->firstTsReq.startTime, req->firstTsReq.ver, req->firstTsReq.gid,
               pVnode->state.applied, qStreamGetTableListNum(sStreamReaderInfo),
               sStreamReaderInfo->isVtableStream, sStreamReaderInfo->groupByTbname, sStreamReaderInfo->partitionCols);
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
  ST_TASK_DLOG("vgId:%d %s start, ver:%" PRId64 ",skey:%" PRId64 ",ekey:%" PRId64 ",gid:%" PRId64, TD_VID(pVnode),
               __func__, req->tsdbMetaReq.ver, req->tsdbMetaReq.startTime, req->tsdbMetaReq.endTime,
               req->tsdbMetaReq.gid);

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
      STREAM_CHECK_RET_GOTO(processTag(sStreamReaderInfo, false, pBlock->info.id.uid, pBlock,
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
  ST_TASK_DLOG("vgId:%d %s start, ver:%"PRId64",skey:%"PRId64",ekey:%"PRId64",uid:%"PRId64",suid:%"PRId64, TD_VID(pVnode), __func__, req->tsdbTsDataReq.ver, 
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
        processTag(sStreamReaderInfo, false, pBlock->info.id.uid, pBlock, 0, pBlock->info.rows, 1));
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
  ST_TASK_DLOG("vgId:%d %s start, skey:%"PRId64",ekey:%"PRId64",gid:%"PRId64",ver:%"PRId64, TD_VID(pVnode), __func__, 
    req->tsdbCalcDataReq.skey, req->tsdbCalcDataReq.ekey, req->tsdbCalcDataReq.gid, req->tsdbCalcDataReq.ver);

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
  printDataBlock(pBlockRes, __func__, "tsdb_calc_data", ((SStreamTask*)pTask)->streamId);
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
  ST_TASK_DLOG("vgId:%d %s start, skey:%"PRId64",ekey:%"PRId64",uid:%"PRId64",ver:%"PRId64, TD_VID(pVnode), __func__, 
    req->tsdbDataReq.skey, req->tsdbDataReq.ekey, req->tsdbDataReq.uid, req->tsdbDataReq.ver);
    
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
  {
    int32_t hookRc = streamMaybeRecheckVTableCache(pVnode, sStreamReaderInfo, resultRsp.ver, &resultRsp);
    ST_TASK_DLOG("vgId:%d %s hook rc=0x%x ver=%" PRId64, TD_VID(pVnode), __func__, hookRc, resultRsp.ver);
    // H2 v0.5: any non-zero hook rc (TAG_CHANGED, REF_TABLE_NOT_EXIST,
    // REF_COL_NOT_EXIST, REF_TOO_DEEP, RPC failure, etc.) is propagated to
    // the trigger via rsp.code so it can fail-fast and request a redeploy.
    if (hookRc != 0) { code = hookRc; goto end; }
  }
  STREAM_CHECK_RET_GOTO(processWalVerMetaNew(pVnode, &resultRsp, sStreamReaderInfo, req->walMetaNewReq.ctime));

  ST_TASK_DLOG("vgId:%d %s get result last ver:%"PRId64" rows:%d", TD_VID(pVnode), __func__, resultRsp.ver, resultRsp.totalRows);
  STREAM_CHECK_CONDITION_GOTO(resultRsp.totalRows == 0, TDB_CODE_SUCCESS);
  size = tSerializeSStreamWalDataResponse(NULL, 0, &resultRsp);
  buf = rpcMallocCont(size);
  size = tSerializeSStreamWalDataResponse(buf, size, &resultRsp);
  printDataBlock(sStreamReaderInfo->metaBlock, __func__, "meta", ((SStreamTask*)pTask)->streamId);
  printDataBlock(resultRsp.deleteBlock, __func__, "delete", ((SStreamTask*)pTask)->streamId);
  printDataBlock(resultRsp.tableBlock, __func__, "table", ((SStreamTask*)pTask)->streamId);

end:
  if (code == 0 && resultRsp.totalRows == 0) {
    code = TSDB_CODE_STREAM_NO_DATA;
    size = sizeof(int64_t) * 2;
    buf = rpcMallocCont(size);
    *(int64_t*)buf = resultRsp.ver;
    *(((int64_t*)buf) + 1) = resultRsp.verTime;
  }
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  if (code == TSDB_CODE_STREAM_NO_DATA){
    code = 0;
  }
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  blockDataDestroy(resultRsp.deleteBlock);
  blockDataDestroy(resultRsp.tableBlock);

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
  resultRsp.checkAlter = true;
  resultRsp.indexHash = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  STREAM_CHECK_NULL_GOTO(resultRsp.indexHash, terrno);
  {
    int32_t hookRc = streamMaybeRecheckVTableCache(pVnode, sStreamReaderInfo, resultRsp.ver, &resultRsp);
    ST_TASK_DLOG("vgId:%d %s hook rc=0x%x ver=%" PRId64, TD_VID(pVnode), __func__, hookRc, resultRsp.ver);
    // H2 v0.5: propagate any hook error so trigger fail-fasts.
    if (hookRc != 0) { code = hookRc; goto end; }
  }

  STREAM_CHECK_RET_GOTO(processWalVerMetaDataNew(pVnode, sStreamReaderInfo, &resultRsp));

  STREAM_CHECK_CONDITION_GOTO(resultRsp.totalRows == 0, TDB_CODE_SUCCESS);
  size = tSerializeSStreamWalDataResponse(NULL, 0, &resultRsp);
  buf = rpcMallocCont(size);
  size = tSerializeSStreamWalDataResponse(buf, size, &resultRsp);
  printDataBlock(sStreamReaderInfo->metaBlock, __func__, "meta", ((SStreamTask*)pTask)->streamId);
  printDataBlock(resultRsp.dataBlock, __func__, "data", ((SStreamTask*)pTask)->streamId);
  printDataBlock(resultRsp.deleteBlock, __func__, "delete", ((SStreamTask*)pTask)->streamId);
  printDataBlock(resultRsp.tableBlock, __func__, "table", ((SStreamTask*)pTask)->streamId);
  printIndexHash(resultRsp.indexHash, pTask);

end:
  if (resultRsp.totalRows == 0) {
    code = TSDB_CODE_STREAM_NO_DATA;
    size = sizeof(int64_t) * 2;
    buf = rpcMallocCont(size);
    *(int64_t*)buf = resultRsp.ver;
    *(((int64_t*)buf) + 1) = resultRsp.verTime;
  }
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  if (code == TSDB_CODE_STREAM_NO_DATA){
    code = 0;
  }
  blockDataDestroy(resultRsp.dataBlock);
  blockDataDestroy(resultRsp.deleteBlock);
  blockDataDestroy(resultRsp.tableBlock);
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
  blockDataDestroy(resultRsp.tableBlock);
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
  blockDataDestroy(resultRsp.tableBlock);
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

// Extract tag-typed column ids from the reader's partition-cols node list.
// On success returns a fresh SArray<col_id_t> (may be empty); caller frees it.
static int32_t streamCollectTagCidsFromPartitionCols(SNodeList *partitionCols, SArray **ppTagCids) {
  *ppTagCids = NULL;
  SArray *tagCids = taosArrayInit(0, sizeof(col_id_t));
  if (tagCids == NULL) return terrno;
  SNode *pNode = NULL;
  FOREACH(pNode, partitionCols) {
    if (pNode == NULL || nodeType(pNode) != QUERY_NODE_COLUMN) continue;
    SColumnNode *c = (SColumnNode *)pNode;
    if (c->colType != COLUMN_TYPE_TAG) continue;
    col_id_t cid = c->colId;
    if (taosArrayPush(tagCids, &cid) == NULL) { taosArrayDestroy(tagCids); return terrno; }
  }
  *ppTagCids = tagCids;
  return 0;
}

// For a single resolved uid, fill one VTableInfo entry: copy each requested cid's
// resolved terminal SColResolveItem into pColRef[i] (hasRef + ref{Db,Table,Col}Name);
// id is the virtual cid itself. version is taken from metaReader if available.
static int32_t streamFillVTableInfoFromResolved(SVnode *pVnode, SStreamTriggerReaderInfo *sStreamReaderInfo,
                                                int64_t uid, uint64_t gid, int64_t ver, SArray *cids,
                                                SVTableResolveResult *pRes, SMetaReader *metaReader,
                                                SArray *infos) {
  int32_t code = 0;
  int32_t lino = 0;
  void   *pTask = sStreamReaderInfo->pTask;

  VTableInfo *vTable = taosArrayReserve(infos, 1);
  STREAM_CHECK_NULL_GOTO(vTable, terrno);
  vTable->uid = uid;
  vTable->gId = gid;

  // Pull schema version + colRef from meta. cids==NULL means "all columns of
  // this vtable", in which case we also need me.colRef.pColRef as the iteration
  // source. Soft-fail (leave version=0 / nCols=0) if the entry is gone.
  int32_t version  = 0;
  bool    haveMeta = false;
  code = sStreamReaderInfo->storageApi.metaReaderFn.getTableEntryByVersionUid(metaReader, ver, uid);
  if (code == 0) {
    version  = metaReader->me.colRef.version;
    haveMeta = true;
  } else {
    code = 0;
  }

  if (cids == NULL) {
    // "All columns" mode: enumerate the vtable's own pColRef.
    int32_t nAll = haveMeta ? metaReader->me.colRef.nCols : 0;
    vTable->cols.nCols   = nAll;
    vTable->cols.version = version;
    if (nAll > 0) {
      vTable->cols.pColRef = taosMemoryCalloc(nAll, sizeof(SColRef));
      STREAM_CHECK_NULL_GOTO(vTable->cols.pColRef, terrno);
      for (int32_t j = 0; j < nAll; ++j) {
        col_id_t cid = metaReader->me.colRef.pColRef[j].id;
        vTable->cols.pColRef[j].id = cid;
        if (pRes == NULL || pRes->colMap == NULL) continue;
        SColResolveItem **pp = (SColResolveItem **)tSimpleHashGet(pRes->colMap, &cid, sizeof(cid));
        if (pp == NULL || *pp == NULL) continue;
        SColResolveItem *item = *pp;
        vTable->cols.pColRef[j].hasRef = item->hasRef;
        if (item->hasRef) {
          tstrncpy(vTable->cols.pColRef[j].refDbName,    item->refDbName,    TSDB_DB_NAME_LEN);
          tstrncpy(vTable->cols.pColRef[j].refTableName, item->refTableName, TSDB_TABLE_NAME_LEN);
          tstrncpy(vTable->cols.pColRef[j].refColName,   item->refColName,   TSDB_COL_NAME_LEN);
        }
      }
    }
  } else {
    int32_t nCids = (int32_t)taosArrayGetSize(cids);
    vTable->cols.nCols   = nCids;
    vTable->cols.version = version;
    vTable->cols.pColRef = taosMemoryCalloc(nCids, sizeof(SColRef));
    STREAM_CHECK_NULL_GOTO(vTable->cols.pColRef, terrno);

    for (int32_t i = 0; i < nCids; ++i) {
      col_id_t cid = *(col_id_t *)taosArrayGet(cids, i);
      vTable->cols.pColRef[i].id = cid;
      if (pRes == NULL || pRes->colMap == NULL) continue;
      SColResolveItem **pp = (SColResolveItem **)tSimpleHashGet(pRes->colMap, &cid, sizeof(cid));
      if (pp == NULL || *pp == NULL) continue;
      SColResolveItem *item = *pp;
      vTable->cols.pColRef[i].hasRef = item->hasRef;
      if (item->hasRef) {
        tstrncpy(vTable->cols.pColRef[i].refDbName,    item->refDbName,    TSDB_DB_NAME_LEN);
        tstrncpy(vTable->cols.pColRef[i].refTableName, item->refTableName, TSDB_TABLE_NAME_LEN);
        tstrncpy(vTable->cols.pColRef[i].refColName,   item->refColName,   TSDB_COL_NAME_LEN);
      }
    }
  }

  if (haveMeta) {
    tDecoderClear(&metaReader->coder);
  }

end:
  return code;
}

// Commit chain-resolver output into the per-reader cache.
// fullScan==true:   atomically swap in the new map (old map is destroyed).
// fullScan==false:  per-uid overwrite (old entries destroyed and replaced).
// On return, *ppUid2Result is set to NULL — ownership has moved into the cache.
static int32_t streamCacheCommitResolved(SStreamVTableInfoCache *pCache, bool fullScan,
                                         SArray *cids, SArray *tagCids, SSHashObj **ppUid2Result) {
  int32_t code = 0;
  if (pCache == NULL || ppUid2Result == NULL || *ppUid2Result == NULL) return TSDB_CODE_INVALID_PARA;
  SSHashObj *uid2Result = *ppUid2Result;

  taosWLockLatch(&pCache->lock);

  if (fullScan) {
    // C2a: atomic full-map replacement. Once swapped in, pCache owns the map;
    // detach from the local handle so a later failure (cids sync) cannot lead
    // to a double-destroy via the caller's cleanup path.
    SSHashObj *oldMap = pCache->uid2Result;
    pCache->uid2Result = uid2Result;
    uid2Result    = NULL;
    *ppUid2Result = NULL;
    if (oldMap) {
      void *iter = NULL; int32_t it = 0;
      while ((iter = tSimpleHashIterate(oldMap, iter, &it)) != NULL) {
        SVTableResolveResult **pp = (SVTableResolveResult **)iter;
        if (pp && *pp) streamVTableResolveResultDestroy(*pp);
      }
      tSimpleHashCleanup(oldMap);
    }
  } else {
    // M1: per-uid overwrite.
    if (pCache->uid2Result == NULL) {
      pCache->uid2Result = tSimpleHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
      if (pCache->uid2Result == NULL) { code = terrno; goto _exit; }
    }
    // Transfer per-uid entries from uid2Result into pCache->uid2Result. As each
    // entry is moved we NULL the slot in uid2Result so a later failure path can
    // safely clean only the entries that have NOT been transferred yet.
    void *iter = NULL; int32_t it = 0;
    while ((iter = tSimpleHashIterate(uid2Result, iter, &it)) != NULL) {
      int64_t                uid = *(int64_t *)tSimpleHashGetKey(iter, NULL);
      SVTableResolveResult **pSlot = (SVTableResolveResult **)iter;
      SVTableResolveResult  *r     = *pSlot;
      if (r == NULL) continue;
      SVTableResolveResult **pOld = (SVTableResolveResult **)tSimpleHashGet(pCache->uid2Result, &uid, sizeof(uid));
      if (pOld && *pOld) streamVTableResolveResultDestroy(*pOld);
      int32_t rc = tSimpleHashPut(pCache->uid2Result, &uid, sizeof(uid), &r, POINTER_BYTES);
      if (rc != 0) {
        // Put failed: r is still referenced by uid2Result[uid]; leave the slot
        // non-NULL so the _exit cleanup destroys it together with the rest of
        // the un-transferred entries.
        code = rc;
        goto _exit;
      }
      *pSlot = NULL;  // ownership moved to pCache
    }
  }

  // Sync request col/tag cid arrays so refresh hook knows what to re-resolve.
  // IMPORTANT: must mirror the caller's NULL/non-NULL exactly. The downstream
  // streamPushInitialWorkItemsForUid treats NULL as "all columns" and a
  // (possibly empty) array as "specified cid list". If we left a stale empty
  // array here when the caller passed NULL, the next recheck would resolve
  // zero columns and falsely diff against the fully-resolved cache.
  if (pCache->reqColCids != NULL) {
    taosArrayDestroy(pCache->reqColCids);
    pCache->reqColCids = NULL;
  }
  if (cids != NULL) {
    pCache->reqColCids = taosArrayDup(cids, NULL);
    if (pCache->reqColCids == NULL) { code = terrno; goto _exit; }
  }
  if (pCache->reqTagCids != NULL) {
    taosArrayDestroy(pCache->reqTagCids);
    pCache->reqTagCids = NULL;
  }
  if (tagCids != NULL) {
    pCache->reqTagCids = taosArrayDup(tagCids, NULL);
    if (pCache->reqTagCids == NULL) { code = terrno; goto _exit; }
  }
  pCache->lastCheckMs = taosGetTimestampMs();
  pCache->valid       = true;

_exit:
  taosWUnLockLatch(&pCache->lock);
  if (uid2Result != NULL) {
    // Either success on the M1 path (all slots NULLed, just free the shell) or
    // failure that leaves un-transferred entries; let the caller's cleanup
    // path destroy any remaining values and the shell. Always null the local
    // ownership to keep semantics symmetric with the fullScan branch above.
    if (code == 0) {
      tSimpleHashCleanup(uid2Result);
      *ppUid2Result = NULL;
    }
  }
  return code;
}

static int32_t vnodeProcessStreamVTableInfoReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t              code = 0;
  int32_t              lino = 0;
  void*                buf = NULL;
  size_t               size = 0;
  SStreamMsgVTableInfo vTableInfo = {0};
  SMetaReader          metaReader = {0};
  SArray              *tagCids   = NULL;
  SSHashObj           *uid2Result = NULL;

  void* pTask = sStreamReaderInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start, version:%"PRId64, TD_VID(pVnode), __func__, req->virTableInfoReq.ver);

  SArray* cids = req->virTableInfoReq.cids;
  STREAM_CHECK_NULL_GOTO(cids, terrno);

  if (taosArrayGetSize(cids) == 1 && *(col_id_t*)taosArrayGet(cids, 0) == PRIMARYKEY_TIMESTAMP_COL_ID){
    (void)atomic_val_compare_exchange_8(&sStreamReaderInfo->isVtableOnlyTs, 0, 1);
  }
  sStreamReaderInfo->storageApi.metaReaderFn.initReader(&metaReader, pVnode, META_READER_LOCK, &sStreamReaderInfo->storageApi.metaFn);

  bool fullScan = req->virTableInfoReq.fetchAllTable
                  || req->virTableInfoReq.uids == NULL
                  || taosArrayGetSize(req->virTableInfoReq.uids) == 0;

  // When the trigger only references the TS column, req->cids carries just the
  // primary-key timestamp, but the response must still describe every column
  // ref of each vtable. Pass cids=NULL into the chain-resolver / formatter to
  // request "all columns of the vtable, per-uid".
  if (atomic_load_8(&sStreamReaderInfo->isVtableOnlyTs) == 1) {
    cids = NULL;
  }

  // Chain-resolver path.
  STREAM_CHECK_RET_GOTO(streamCollectTagCidsFromPartitionCols(sStreamReaderInfo->partitionCols, &tagCids));

  SArray *uidList = fullScan ? NULL : req->virTableInfoReq.uids;
  STREAM_CHECK_RET_GOTO(streamResolveVTableRefChain(pVnode, sStreamReaderInfo->vtbCache, sStreamReaderInfo,
                                                    req->virTableInfoReq.ver, uidList, cids, tagCids, &uid2Result));

  // Encode response: iterate the resolved set.
  int32_t expected = (int32_t)tSimpleHashGetSize(uid2Result);
  vTableInfo.infos = taosArrayInit(expected, sizeof(VTableInfo));
  STREAM_CHECK_NULL_GOTO(vTableInfo.infos, terrno);

  if (fullScan) {
    void *iter = NULL; int32_t it = 0;
    while ((iter = tSimpleHashIterate(uid2Result, iter, &it)) != NULL) {
      int64_t uid = *(int64_t *)tSimpleHashGetKey(iter, NULL);
      SVTableResolveResult *r = *(SVTableResolveResult **)iter;
      taosRLockLatch(&sStreamReaderInfo->lock);
      uint64_t groupId = qStreamGetGroupIdFromOrigin(sStreamReaderInfo, uid);
      taosRUnLockLatch(&sStreamReaderInfo->lock);
      if (groupId == (uint64_t)-1) continue;
      int32_t rc = streamFillVTableInfoFromResolved(pVnode, sStreamReaderInfo, uid, groupId,
                                                    req->virTableInfoReq.ver, cids, r, &metaReader, vTableInfo.infos);
      if (rc != 0) { code = rc; goto end; }
    }
  } else {
    int32_t nReq = (int32_t)taosArrayGetSize(req->virTableInfoReq.uids);
    for (int32_t i = 0; i < nReq; ++i) {
      int64_t uid = *(int64_t *)taosArrayGet(req->virTableInfoReq.uids, i);
      SVTableResolveResult **pp = (SVTableResolveResult **)tSimpleHashGet(uid2Result, &uid, sizeof(uid));
      if (pp == NULL || *pp == NULL) continue;
      taosRLockLatch(&sStreamReaderInfo->lock);
      uint64_t groupId = qStreamGetGroupIdFromOrigin(sStreamReaderInfo, uid);
      taosRUnLockLatch(&sStreamReaderInfo->lock);
      if (groupId == (uint64_t)-1) continue;
      int32_t rc = streamFillVTableInfoFromResolved(pVnode, sStreamReaderInfo, uid, groupId,
                                                    req->virTableInfoReq.ver, cids, *pp, &metaReader, vTableInfo.infos);
      if (rc != 0) { code = rc; goto end; }
    }
  }

  ST_TASK_DLOG("vgId:%d %s end, size:%"PRIzu, TD_VID(pVnode), __func__, taosArrayGetSize(vTableInfo.infos));
  STREAM_CHECK_RET_GOTO(buildVTableInfoRsp(&vTableInfo, &buf, &size));

  // Move ownership into cache (uid2Result becomes NULL on success).
  if (sStreamReaderInfo->vtbCache != NULL) {
    int32_t rc = streamCacheCommitResolved(sStreamReaderInfo->vtbCache, fullScan, cids, tagCids, &uid2Result);
    if (rc != 0) { code = rc; goto end; }
  }

end:
  if (tagCids != NULL) taosArrayDestroy(tagCids);
  streamDestroyUid2ResultMap(&uid2Result);
  tDestroySStreamMsgVTableInfo(&vTableInfo);
  sStreamReaderInfo->storageApi.metaReaderFn.clearReader(&metaReader);
  STREAM_PRINT_LOG_END_WITHID(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  return code;
}

// Compare two SColResolveItem; returns true if they refer to the same terminal column.
static bool colResolveItemEqual(const SColResolveItem *a, const SColResolveItem *b) {
  if (a == NULL && b == NULL) return true;
  if (a == NULL || b == NULL) return false;
  if (a->hasRef != b->hasRef) return false;
  if (!a->hasRef) return true;
  return strcmp(a->refDbName, b->refDbName) == 0 &&
         strcmp(a->refTableName, b->refTableName) == 0 &&
         strcmp(a->refColName, b->refColName) == 0;
}

static bool tagValueEqual(const STagValue *a, const STagValue *b) {
  if (a == NULL && b == NULL) return true;
  if (a == NULL || b == NULL) return false;
  if (a->type != b->type) return false;
  if (a->nLen != b->nLen) return false;
  if (a->nLen == 0) return true;
  if (a->pData == NULL || b->pData == NULL) return a->pData == b->pData;
  return memcmp(a->pData, b->pData, a->nLen) == 0;
}

// Sliced re-check tuning: every STREAM_VTB_RECHECK_INTERVAL_MS scans at most
// STREAM_VTB_RECHECK_SLICE_SIZE uids. A full sweep of N uids therefore takes
// roughly ceil(N / SLICE_SIZE) * INTERVAL_MS. With INTERVAL=1000 ms and
// SLICE=1000, up to 1000 uids/sec are verified per vnode.
#define STREAM_VTB_RECHECK_INTERVAL_MS 1000
#define STREAM_VTB_RECHECK_SLICE_SIZE  1000

// Throttled hook called at the entry of every WAL meta request.
// On tag change: returns TSDB_CODE_STREAM_VTB_TAG_CHANGED so caller bails out fast.
// On col-only change: appends affected uids into rsp->tableBlock as TABLE_BLOCK_ADD.
// All other cases: returns 0 and lets caller continue normal processing.
//
// Locking discipline: the resolver round-trip (RPC + tsem_wait) is expensive
// and MUST run outside the cache W-latch, otherwise WAL meta processing and
// the foreground vtable-info request path stall on every recheck tick. The
// hook therefore splits work into three phases:
//   1) under lock: throttle check + snapshot of reqColCids/reqTagCids and the
//      slice uid list, advance the slice cursor, and claim lastCheckMs = now
//      so concurrent callers see the throttle and skip;
//   2) without lock: streamResolveVTableRefChain over the snapshot;
//   3) under lock: diff the resolved result against the live cache and apply
//      M1-style per-uid updates / fail-fast on tag changes.
static int32_t streamMaybeRecheckVTableCache(SVnode *pVnode, SStreamTriggerReaderInfo *pInfo,
                                             int64_t walVer, SSTriggerWalNewRsp *pRsp) {
  if (pInfo == NULL || pInfo->vtbCache == NULL || !pInfo->vtbCache->valid) {
    return 0;
  }
  SStreamVTableInfoCache *pCache = pInfo->vtbCache;
  int64_t now = taosGetTimestampMs();
  if (now - pCache->lastCheckMs < STREAM_VTB_RECHECK_INTERVAL_MS) {
    return 0;
  }

  int32_t    code         = 0;
  SArray    *sliceUids    = NULL;
  SArray    *reqColCids   = NULL;
  SArray    *reqTagCids   = NULL;
  SArray    *changedUids  = NULL;
  SSHashObj *uid2Result   = NULL;

  // ---- Phase 1: snapshot under lock ----
  taosWLockLatch(&pCache->lock);
  if (now - pCache->lastCheckMs < STREAM_VTB_RECHECK_INTERVAL_MS) {
    taosWUnLockLatch(&pCache->lock);
    return 0;
  }

  // Refill uidSlice whenever the cursor wraps to 0 so newly registered vtables
  // (not yet in uid2Result) are picked up by the next sweep.
  if (pCache->sliceCursor == 0) {
    taosArrayClear(pCache->uidSlice);
    SArray *pTableListArray = qStreamGetTableArrayList(pInfo);
    if (pTableListArray == NULL) {
      taosWUnLockLatch(&pCache->lock);
      return terrno;
    }
    int32_t nAll = (int32_t)taosArrayGetSize(pTableListArray);
    for (int32_t i = 0; i < nAll; ++i) {
      SStreamTableKeyInfo *pKey = taosArrayGetP(pTableListArray, i);
      if (pKey == NULL || pKey->markedDeleted) continue;
      if (taosArrayPush(pCache->uidSlice, &pKey->uid) == NULL) {
        code = terrno;
        taosArrayDestroyP(pTableListArray, taosMemFree);
        goto _unlock_phase1;
      }
    }
    taosArrayDestroyP(pTableListArray, taosMemFree);
  }

  int32_t total = (int32_t)taosArrayGetSize(pCache->uidSlice);
  if (total == 0) {
    pCache->lastCheckMs = taosGetTimestampMs();
    taosWUnLockLatch(&pCache->lock);
    stDebug("vgId:%d %s skip: cache empty", TD_VID(pVnode), __func__);
    return 0;
  }

  int32_t begin = pCache->sliceCursor;
  int32_t end   = TMIN(begin + STREAM_VTB_RECHECK_SLICE_SIZE, total);
  sliceUids = taosArrayInit(end - begin, sizeof(int64_t));
  if (sliceUids == NULL) { code = terrno; goto _unlock_phase1; }
  for (int32_t i = begin; i < end; ++i) {
    if (taosArrayPush(sliceUids, taosArrayGet(pCache->uidSlice, i)) == NULL) {
      code = terrno;
      goto _unlock_phase1;
    }
  }

  if (pCache->reqColCids != NULL) {
    reqColCids = taosArrayDup(pCache->reqColCids, NULL);
    if (reqColCids == NULL) { code = terrno; goto _unlock_phase1; }
  }
  if (pCache->reqTagCids != NULL) {
    reqTagCids = taosArrayDup(pCache->reqTagCids, NULL);
    if (reqTagCids == NULL) { code = terrno; goto _unlock_phase1; }
  }

  // Advance cursor and claim the throttle slot so concurrent callers skip.
  pCache->sliceCursor = (end >= total) ? 0 : end;
  pCache->lastCheckMs = taosGetTimestampMs();

_unlock_phase1:
  taosWUnLockLatch(&pCache->lock);
  if (code != 0) goto _cleanup;

  stDebug("vgId:%d %s walVer=%" PRId64 " total=%d slice=[%d,%d)",
          TD_VID(pVnode), __func__, walVer, total, begin, end);

  // ---- Phase 2: resolver round-trip, no lock held ----
  code = streamResolveVTableRefChain(pVnode, NULL, pInfo, walVer, sliceUids,
                                     reqColCids, reqTagCids, &uid2Result);
  if (code != 0) goto _cleanup;

  changedUids = taosArrayInit(0, sizeof(int64_t));
  if (changedUids == NULL) { code = terrno; goto _cleanup; }

  // ---- Phase 3: diff + apply under lock ----
  taosWLockLatch(&pCache->lock);
  for (int32_t i = 0; i < (int32_t)taosArrayGetSize(sliceUids); ++i) {
    int64_t uid = *(int64_t *)taosArrayGet(sliceUids, i);
    SVTableResolveResult **ppNew = (SVTableResolveResult **)tSimpleHashGet(uid2Result, &uid, sizeof(uid));
    SVTableResolveResult **ppOld = (SVTableResolveResult **)tSimpleHashGet(pCache->uid2Result, &uid, sizeof(uid));
    SVTableResolveResult *newRes = (ppNew == NULL) ? NULL : *ppNew;
    SVTableResolveResult *oldRes = (ppOld == NULL) ? NULL : *ppOld;

    // uid skipped by resolver (top-level vtable dropped, H2 fallback) -> drop from cache.
    if (newRes == NULL) {
      if (oldRes != NULL) {
        stDebug("vgId:%d %s uid dropped: uid=%" PRId64, TD_VID(pVnode), __func__, uid);
        streamVTableResolveResultDestroy(oldRes);
        tSimpleHashRemove(pCache->uid2Result, &uid, sizeof(uid));
      }
      continue;
    }

    // Tag diff -- any tag change is fatal.
    bool tagChanged = false;
    if (oldRes != NULL && oldRes->tagMap != NULL) {
      void *it2 = NULL; int32_t i2 = 0;
      while ((it2 = tSimpleHashIterate(oldRes->tagMap, it2, &i2)) != NULL) {
        col_id_t cid = *(col_id_t *)tSimpleHashGetKey(it2, NULL);
        STagValue *oldV = *(STagValue **)it2;
        STagValue **ppNewV = (newRes->tagMap == NULL) ? NULL :
                             (STagValue **)tSimpleHashGet(newRes->tagMap, &cid, sizeof(cid));
        STagValue *newV = (ppNewV == NULL) ? NULL : *ppNewV;
        if (!tagValueEqual(oldV, newV)) {
          stDebug("vgId:%d %s tag changed: uid=%" PRId64 " cid=%d", TD_VID(pVnode), __func__,
                  uid, (int32_t)cid);
          tagChanged = true;
          break;
        }
      }
    }
    if (tagChanged) {
      code = TSDB_CODE_STREAM_VTB_TAG_CHANGED;
      break;
    }

    // Col diff -- collect uids that need re-publication.
    bool colChanged = false;
    if (oldRes != NULL && oldRes->colMap != NULL) {
      void *it2 = NULL; int32_t i2 = 0;
      while ((it2 = tSimpleHashIterate(oldRes->colMap, it2, &i2)) != NULL) {
        col_id_t cid = *(col_id_t *)tSimpleHashGetKey(it2, NULL);
        SColResolveItem *oldI = *(SColResolveItem **)it2;
        SColResolveItem **ppNewI = (newRes->colMap == NULL) ? NULL :
                                   (SColResolveItem **)tSimpleHashGet(newRes->colMap, &cid, sizeof(cid));
        SColResolveItem *newI = (ppNewI == NULL) ? NULL : *ppNewI;
        if (!colResolveItemEqual(oldI, newI)) { colChanged = true; break; }
      }
    }
    if (colChanged) {
      if (taosArrayPush(changedUids, &uid) == NULL) { code = terrno; break; }
    }

    // Replace cache entry with the freshly resolved result; transfer ownership.
    if (tSimpleHashPut(pCache->uid2Result, &uid, sizeof(uid), &newRes, POINTER_BYTES) != 0) {
      code = terrno;
      break;
    }
    *ppNew = NULL;  // newly-owned by cache; null the slot to prevent double-free.
    if (oldRes != NULL) streamVTableResolveResultDestroy(oldRes);
  }
  pCache->lastCheckMs = taosGetTimestampMs();
  taosWUnLockLatch(&pCache->lock);

_cleanup:
  streamDestroyUid2ResultMap(&uid2Result);
  taosArrayDestroy(sliceUids);
  taosArrayDestroy(reqColCids);
  taosArrayDestroy(reqTagCids);

  if (code == TSDB_CODE_STREAM_VTB_TAG_CHANGED) {
    stWarn("vgId:%d %s tag changed, abort fast walVer=%" PRId64, TD_VID(pVnode), __func__, walVer);
    taosArrayDestroy(changedUids);
    return code;
  }
  if (code != 0) {
    stError("vgId:%d %s recheck failed since %s", TD_VID(pVnode), __func__, tstrerror(code));
    taosArrayDestroy(changedUids);
    return code;
  }
  if (pRsp != NULL && changedUids != NULL && taosArrayGetSize(changedUids) > 0) {
    int32_t rc = addUidListToBlock(changedUids, &pRsp->tableBlock, walVer, &pRsp->totalRows, TABLE_BLOCK_ADD);
    stDebug("vgId:%d %s appended %d changed uids walVer=%" PRId64, TD_VID(pVnode), __func__,
            (int32_t)taosArrayGetSize(changedUids), walVer);
    if (rc != 0) { taosArrayDestroy(changedUids); return rc; }
  }
  taosArrayDestroy(changedUids);
  return 0;
}

static int32_t vnodeProcessStreamOTableInfoReq(SVnode* pVnode, SRpcMsg* pMsg, SSTriggerPullRequestUnion* req, SStreamTriggerReaderInfo* sStreamReaderInfo) {
  int32_t                   code = 0;
  int32_t                   lino = 0;
  void*                     buf = NULL;
  size_t                    size = 0;
  SSTriggerOrigTableInfoRsp oTableInfo = {0};
  SMetaReader               metaReader = {0};
  void*                     pTask = sStreamReaderInfo->pTask;

  ST_TASK_DLOG("vgId:%d %s start, ver:%" PRId64, TD_VID(pVnode), __func__, req->origTableInfoReq.ver);

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
    code = sStreamReaderInfo->storageApi.metaReaderFn.getTableEntryByVersionName(&metaReader, req->origTableInfoReq.ver, oInfo->refTableName);
    if (code != 0) {
      code = 0;
      ST_TASK_ELOG("vgId:%d %s get table entry by name:%s failed, msg:%s", TD_VID(pVnode), __func__, oInfo->refTableName, tstrerror(code));
      continue;
    }
    vTableInfo->uid = metaReader.me.uid;
    ST_TASK_DLOG("vgId:%d %s get original uid:%"PRId64, TD_VID(pVnode), __func__, vTableInfo->uid);

    SSchemaWrapper* sSchemaWrapper = NULL;
    if (metaReader.me.type == TD_CHILD_TABLE) {
      int64_t suid = metaReader.me.ctbEntry.suid;
      vTableInfo->suid = suid;
      tDecoderClear(&metaReader.coder);
      STREAM_CHECK_RET_GOTO(sStreamReaderInfo->storageApi.metaReaderFn.getTableEntryByVersionUid(&metaReader, req->origTableInfoReq.ver, suid));
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
  SSDataBlock              *pBlock      = NULL;
  SArray                   *singleUid   = NULL;
  SArray                   *emptyCols   = NULL;
  SArray                   *tagOnly     = NULL;
  SSHashObj                *uid2Result  = NULL;
  SVTableResolveResult     *pRes        = NULL;
  SMetaReader               metaReader  = {0};
  int64_t streamId = req->base.streamId;
  stsDebug("vgId:%d %s start, ver:%"PRId64" uid:%"PRId64" cols_size:%d", TD_VID(pVnode), __func__,
           req->virTablePseudoColReq.ver, req->virTablePseudoColReq.uid,
           req->virTablePseudoColReq.cids ? (int)taosArrayGetSize(req->virTablePseudoColReq.cids) : -1);

  SArray* cols = req->virTablePseudoColReq.cids;
  STREAM_CHECK_NULL_GOTO(cols, terrno);

  // We still need metaReader for vtable name (cid == -1) and to assert table type.
  sStreamReaderInfo->storageApi.metaReaderFn.initReader(&metaReader, pVnode, META_READER_LOCK, &sStreamReaderInfo->storageApi.metaFn);
  STREAM_CHECK_RET_GOTO(sStreamReaderInfo->storageApi.metaReaderFn.getTableEntryByVersionUid(&metaReader, req->virTablePseudoColReq.ver, req->virTablePseudoColReq.uid));
  STREAM_CHECK_CONDITION_GOTO(metaReader.me.type != TD_VIRTUAL_CHILD_TABLE && metaReader.me.type != TD_VIRTUAL_NORMAL_TABLE, TSDB_CODE_INVALID_PARA);

  STREAM_CHECK_RET_GOTO(createDataBlock(&pBlock));

  if (metaReader.me.type == TD_VIRTUAL_NORMAL_TABLE) {
    // Normal vtable: caller only requests the table-name pseudo column.
    STREAM_CHECK_CONDITION_GOTO (taosArrayGetSize(cols) < 1 || *(col_id_t*)taosArrayGet(cols, 0) != -1, TSDB_CODE_INVALID_PARA);
    SColumnInfoData idata = createColumnInfoData(TSDB_DATA_TYPE_BINARY, TSDB_TABLE_NAME_LEN, -1);
    STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(pBlock, &idata));
    STREAM_CHECK_RET_GOTO(blockDataEnsureCapacity(pBlock, 1));
    pBlock->info.rows = 1;
    SColumnInfoData* pDst = taosArrayGet(pBlock->pDataBlock, 0);
    STREAM_CHECK_NULL_GOTO(pDst, terrno);
    STREAM_CHECK_RET_GOTO(varColSetVarData(pDst, 0, metaReader.me.name, strlen(metaReader.me.name), false));
  } else {
    // Virtual child table: resolve tags via chain resolver (single uid, cache-bypass).
    singleUid = taosArrayInit(1, sizeof(int64_t));
    STREAM_CHECK_NULL_GOTO(singleUid, terrno);
    int64_t uidVal = req->virTablePseudoColReq.uid;
    STREAM_CHECK_NULL_GOTO(taosArrayPush(singleUid, &uidVal), terrno);

    emptyCols = taosArrayInit(0, sizeof(col_id_t));
    STREAM_CHECK_NULL_GOTO(emptyCols, terrno);

    // The pseudo-column request can carry cid==-1 for the vtable name; the
    // chain resolver only handles real tag cids, so pass a filtered copy.
    tagOnly = taosArrayInit(taosArrayGetSize(cols), sizeof(col_id_t));
    STREAM_CHECK_NULL_GOTO(tagOnly, terrno);
    for (size_t i = 0; i < taosArrayGetSize(cols); i++) {
      col_id_t cid = *(col_id_t *)taosArrayGet(cols, i);
      if (cid == -1) continue;
      STREAM_CHECK_NULL_GOTO(taosArrayPush(tagOnly, &cid), terrno);
    }

    // PSEUDO_COL bypasses cache — pass NULL so resolver does not read/write it.
    code = streamResolveVTableRefChain(pVnode, NULL, sStreamReaderInfo,
                                       req->virTablePseudoColReq.ver,
                                       singleUid, emptyCols, tagOnly, &uid2Result);
    taosArrayDestroy(tagOnly); tagOnly = NULL;
    STREAM_CHECK_RET_GOTO(code);

    SVTableResolveResult **pp = (SVTableResolveResult **)tSimpleHashGet(uid2Result, &uidVal, sizeof(uidVal));
    if (pp == NULL || *pp == NULL) {
      // H2 v0.5: A returned 0 but the uid has no entry in uid2Result. This
      // happens only when the top-level uid was missing from local meta
      // (vtable dropped concurrently). PSEUDO_COL is single-uid and has no
      // partial-success semantic, so propagate as REF_TABLE_NOT_EXIST.
      code = TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST;
      goto end;
    }
    pRes = *pp;

    // Append column metadata according to caller-requested cids.
    for (size_t i = 0; i < taosArrayGetSize(cols); i++){
      col_id_t cid = *(col_id_t*)taosArrayGet(cols, i);
      if (cid == -1) {
        SColumnInfoData idata = createColumnInfoData(TSDB_DATA_TYPE_BINARY, TSDB_TABLE_NAME_LEN, -1);
        STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(pBlock, &idata));
        continue;
      }
      STagValue **ppv = (pRes->tagMap == NULL) ? NULL : (STagValue **)tSimpleHashGet(pRes->tagMap, &cid, sizeof(cid));
      if (ppv == NULL || *ppv == NULL) {
        SColumnInfoData idata = createColumnInfoData(TSDB_DATA_TYPE_NULL, CHAR_BYTES, cid);
        STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(pBlock, &idata));
        continue;
      }
      STagValue       *v     = *ppv;
      int32_t          bytes = (v->nLen > 0 ? v->nLen : 1);
      SColumnInfoData  idata = createColumnInfoData(v->type, bytes, cid);
      STREAM_CHECK_RET_GOTO(blockDataAppendColInfo(pBlock, &idata));
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

      STagValue **ppv = (STagValue **)tSimpleHashGet(pRes->tagMap, &pDst->info.colId, sizeof(pDst->info.colId));
      if (ppv == NULL || *ppv == NULL || (*ppv)->pData == NULL) {
        STREAM_CHECK_RET_GOTO(colDataSetVal(pDst, 0, NULL, true));
        continue;
      }
      STREAM_CHECK_RET_GOTO(colDataSetVal(pDst, 0, (*ppv)->pData, false));
    }
  }

  stsDebug("vgId:%d %s get result rows:%" PRId64, TD_VID(pVnode), __func__, pBlock->info.rows);
  printDataBlock(pBlock, __func__, "", streamId);
  STREAM_CHECK_RET_GOTO(buildRsp(pBlock, &buf, &size));

end:
  if(size == 0){
    code = TSDB_CODE_STREAM_NO_DATA;
  }
  if (singleUid != NULL) taosArrayDestroy(singleUid);
  if (emptyCols != NULL) taosArrayDestroy(emptyCols);
  if (tagOnly   != NULL) taosArrayDestroy(tagOnly);
  streamDestroyUid2ResultMap(&uid2Result);
  sStreamReaderInfo->storageApi.metaReaderFn.clearReader(&metaReader);
  STREAM_PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {
      .msgType = TDMT_STREAM_TRIGGER_PULL_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  blockDataDestroy(pBlock);
  return code;
}

static int32_t vnodeProcessStreamFetchMsg(SVnode* pVnode, SRpcMsg* pMsg, SQueueInfo *pInfo) {
  int32_t            code = 0;
  int32_t            lino = 0;
  void*              buf = NULL;
  size_t             size = 0;
  void*              taskAddr = NULL;
  SArray*            pResList = NULL;
  bool               hasNext = false;
  SStreamTriggerReaderCalcInfo* sStreamReaderCalcInfo = NULL;

  SResFetchReq req = {0};
  STREAM_CHECK_CONDITION_GOTO(tDeserializeSResFetchReq(pMsg->pCont, pMsg->contLen, &req) < 0,
                              TSDB_CODE_QRY_INVALID_INPUT);
  SArray* calcInfoList = (SArray*)qStreamGetReaderInfo(req.queryId, req.taskId, &taskAddr);
  STREAM_CHECK_NULL_GOTO(calcInfoList, terrno);

  STREAM_CHECK_CONDITION_GOTO(req.execId < 0, TSDB_CODE_INVALID_PARA);
  sStreamReaderCalcInfo = taosArrayGetP(calcInfoList, req.execId);
  STREAM_CHECK_NULL_GOTO(sStreamReaderCalcInfo, terrno);
  sStreamReaderCalcInfo->rtInfo.execId = req.execId;

  void* pTask = sStreamReaderCalcInfo->pTask;
  ST_TASK_DLOG("vgId:%d %s start, execId:%d, reset:%d, pTaskInfo:%p, scan type:%d", TD_VID(pVnode), __func__, req.execId, req.reset,
               sStreamReaderCalcInfo->pTaskInfo, nodeType(sStreamReaderCalcInfo->calcAst->pNode));

  if (req.reset) {
    int64_t uid = 0;
    if (req.dynTbname && !req.pStRtFuncInfo->isMultiGroupCalc) {
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
    handle.pMsgCb = &pVnode->msgCb;
    handle.pWorkerCb = pInfo->workerCb;
    handle.uid = uid;
    handle.cacheSttStatis = true;

    initStorageAPI(&handle.api);
    if (QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN == nodeType(sStreamReaderCalcInfo->calcAst->pNode) ||
      QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN == nodeType(sStreamReaderCalcInfo->calcAst->pNode)){
      STimeRangeNode* node = (STimeRangeNode*)((STableScanPhysiNode*)(sStreamReaderCalcInfo->calcAst->pNode))->pTimeRange;
      if (node != NULL) {
        STREAM_CHECK_RET_GOTO(processCalaTimeRange(sStreamReaderCalcInfo, &req, node, &handle, false));
      } else {
        ST_TASK_DLOG("vgId:%d %s no scan time range node", TD_VID(pVnode), __func__);
      }

      node = (STimeRangeNode*)((STableScanPhysiNode*)(sStreamReaderCalcInfo->calcAst->pNode))->pExtTimeRange;
      if (node != NULL) {
        STREAM_CHECK_RET_GOTO(processCalaTimeRange(sStreamReaderCalcInfo, &req, node, &handle, true));
      } else {
        ST_TASK_DLOG("vgId:%d %s no interp time range node", TD_VID(pVnode), __func__);
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
      STREAM_CHECK_RET_GOTO(qSetTaskId(sStreamReaderCalcInfo->pTaskInfo, req.taskId, req.queryId));
    } else {
      STREAM_CHECK_RET_GOTO(qResetTableScan(sStreamReaderCalcInfo->pTaskInfo, &handle));
    }

    STREAM_CHECK_RET_GOTO(qSetTaskId(sStreamReaderCalcInfo->pTaskInfo, req.taskId, req.queryId));
  }

  if (req.pOpParam != NULL) {
    qUpdateOperatorParam(sStreamReaderCalcInfo->pTaskInfo, (void*)req.pOpParam);
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
  code = streamBuildFetchRsp(pResList, hasNext, &buf, &size, pVnode->config.tsdbCfg.precision);

  if (sStreamReaderCalcInfo && sStreamReaderCalcInfo->rtInfo.funcInfo.isMultiGroupCalc) {
    sStreamReaderCalcInfo->rtInfo.funcInfo.pStreamPesudoFuncVals = NULL;
    sStreamReaderCalcInfo->rtInfo.funcInfo.pStreamPartColVals = NULL;
  }
  
  taosArrayDestroy(pResList);
  streamReleaseTask(taskAddr);
  
  if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST || code == TSDB_CODE_TDB_TABLE_NOT_EXIST){
    code = TDB_CODE_SUCCESS;
  }
  STREAM_PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {.msgType = TDMT_STREAM_FETCH_RSP, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  tDestroySResFetchReq(&req);
  if (TDB_CODE_SUCCESS != code) {
    ST_TASK_ELOG("vgId:%d %s failed, code:%d - %s", TD_VID(pVnode), __func__,
                 code, tstrerror(code));
  }
  return code;
}

static int32_t initTableList(SStreamTriggerReaderInfo* sStreamReaderInfo, SVnode* pVnode) {
  int32_t code = 0;
  if (sStreamReaderInfo->tableList.pTableList != NULL) {  
    return code;
  }
  taosWLockLatch(&sStreamReaderInfo->lock);
  sStreamReaderInfo->pVnode = pVnode;
  initStorageAPI(&sStreamReaderInfo->storageApi);
  if (sStreamReaderInfo->tableList.pTableList == NULL) {
    code = initStreamTableListInfo(&sStreamReaderInfo->tableList);
    if (code == 0) {
      code = generateTablistForStreamReader(pVnode, sStreamReaderInfo);
      if (code != 0) {
        qStreamDestroyTableInfo(&sStreamReaderInfo->tableList);
      } else {
        sStreamReaderInfo->tableList.version = pVnode->state.applied;
        stDebug("vgId:%d %s init table list for stream reader, table num:%zu, version:%" PRId64,
                TD_VID(pVnode), __func__, taosArrayGetSize(sStreamReaderInfo->tableList.pTableList), sStreamReaderInfo->tableList.version);
      }
    }
  }
  taosWUnLockLatch(&sStreamReaderInfo->lock);
  return code;
}

int32_t vnodeProcessStreamReaderMsg(SVnode* pVnode, SRpcMsg* pMsg, SQueueInfo *pInfo) {
  int32_t                   code = 0;
  int32_t                   lino = 0;
  SSTriggerPullRequestUnion req = {0};
  void*                     taskAddr = NULL;
  bool                      sendRsp = false;

  vDebug("vgId:%d, msg:%p in stream reader queue is processing", pVnode->config.vgId, pMsg);
  if (!syncIsReadyForRead(pVnode->sync)) {
    vnodeRedirectRpcMsg(pVnode, pMsg, terrno);
    return 0;
  }

  if (pMsg->msgType == TDMT_STREAM_FETCH) {
    return vnodeProcessStreamFetchMsg(pVnode, pMsg, pInfo);
  } else if (pMsg->msgType == TDMT_VND_VTABLE_REF_RESOLVE) {
    return vnodeProcessVTableRefResolveReq(pVnode, pMsg);
  } else if (pMsg->msgType == TDMT_STREAM_TRIGGER_PULL) {
    void*   pReq = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
    int32_t len = pMsg->contLen - sizeof(SMsgHead);
    STREAM_CHECK_RET_GOTO(tDeserializeSTriggerPullRequest(pReq, len, &req));
    stDebug("vgId:%d %s start, type:%d, streamId:%" PRIx64 ", readerTaskId:%" PRIx64 ", sessionId:%" PRIx64 ", applied:%" PRIx64,
            TD_VID(pVnode), __func__, req.base.type, req.base.streamId, req.base.readerTaskId, req.base.sessionId, pVnode->state.applied);
    SStreamTriggerReaderInfo* sStreamReaderInfo = qStreamGetReaderInfo(req.base.streamId, req.base.readerTaskId, &taskAddr);
    STREAM_CHECK_NULL_GOTO(sStreamReaderInfo, terrno);
    STREAM_CHECK_RET_GOTO(initTableList(sStreamReaderInfo, pVnode));
    sendRsp = true;
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
        sendRsp = false;
        STREAM_CHECK_RET_GOTO(TSDB_CODE_APP_ERROR);
    }
  } else {
    vError("unknown msg type:%d in stream reader queue", pMsg->msgType);
    STREAM_CHECK_RET_GOTO(TSDB_CODE_APP_ERROR);
  }
end:

  streamReleaseTask(taskAddr);

  tDestroySTriggerPullRequest(&req);
  STREAM_PRINT_LOG_END(code, lino);
  if (!sendRsp) {
    SRpcMsg rsp = {
      .code = code,
      .pCont = pMsg->info.rsp,
      .contLen = pMsg->info.rspLen,
      .info = pMsg->info,
    };
    tmsgSendRsp(&rsp);
  }
  return code;
}

// ============================================================================
// TDMT_VND_VTABLE_REF_RESOLVE — single-hop chain resolver for vtable references.
//
// Caller (driver A in stream-trigger reader info path) groups per-batch refs by
// vgId and sends one request per vgId. Each item carries (kind, refDb, refTbl,
// refCol). For every item we do exactly ONE hop:
//   - table not on this vnode               -> r.code = STREAM_VTB_REF_TABLE_NOT_EXIST
//   - column/tag name not found             -> r.code = STREAM_VTB_REF_COL_NOT_EXIST
//   - vtable + COL + hasRef                 -> r.terminated = false, r.nextRef = stored ref
//   - vtable + COL + !hasRef                -> r.terminated = true,  r.nextRef.hasRef = false
//                                              (terminal triple is meaningless; signals NULL value)
//   - vtable + TAG + hasRef                 -> r.terminated = false, r.nextRef = stored ref
//   - vchild + TAG + !hasRef                -> r.terminated = true,  r.nextRef.hasRef = false,
//                                              r.tagType/tagLen/tagData filled from local STag
//   - vnormal+ TAG + !hasRef                -> r.code = STREAM_VTB_REF_COL_NOT_EXIST
//                                              (normal vtable has no tag concept)
//   - physical table  + COL kind            -> r.terminated = true,  r.nextRef = current triple
//   - child table     + TAG kind            -> r.terminated = true,  r.tagType/tagLen/tagData filled
//   - normal table    + TAG kind            -> r.code = STREAM_VTB_REF_COL_NOT_EXIST
// Per-item errors never abort the batch — they are reported in r.code.
// ============================================================================

// Reads a tag's constant value from a (virtual or physical) child table entry.
// The stable schema (for type/colId lookup) is fetched here under META_READER_LOCK;
// the child entry is provided by the caller.
//   pVnode      : owning vnode
//   pChildEntry : decoded entry whose type is *_CHILD_TABLE (carries ctbEntry.suid/pTags)
//   tagColName  : tag name on the stable (vchild's SColRef.colName matches stable tag name
//                 by build-time convention)
// Outputs:
//   *outType    : tag SDataType
//   *outLen     : payload length; 0 when tag absent on this child
//   *outData    : newly allocated buffer (caller frees); NULL when *outLen==0
// Returns:
//   0                                          success (incl. "tag absent")
//   TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST   suid not present on this vnode
//   TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST     tag name not in stable schema
//   terrno                                     OOM
// Internal helper: read a constant tag value from a virtual child table.
// Tag is located in the parent stable's schemaTag by either colId (preferred when > 0)
// or by colName (fallback). vtable on-disk SColRef does not persist colName, so callers
// holding only a SColRef entry must pass the cid.
static int32_t streamReadChildTagConstValueImpl(SVnode *pVnode, const SMetaEntry *pChildEntry,
                                                col_id_t tagColId, const char *tagColName,
                                                int8_t *outType, int32_t *outLen, char **outData) {
  SMetaReader stb  = {0};
  int32_t     code = 0;
  *outType = 0;
  *outLen  = 0;
  *outData = NULL;

  metaReaderDoInit(&stb, pVnode->pMeta, META_READER_LOCK);
  if (metaReaderGetTableEntryByUid(&stb, pChildEntry->ctbEntry.suid) != 0) {
    code = TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST;
    goto _end;
  }

  SSchemaWrapper *pSW = &stb.me.stbEntry.schemaTag;
  SSchema        *pTagSchema = NULL;
  for (int32_t i = 0; i < pSW->nCols; ++i) {
    if (tagColId > 0) {
      if (pSW->pSchema[i].colId == tagColId) {
        pTagSchema = &pSW->pSchema[i];
        break;
      }
    } else if (tagColName != NULL &&
               strncmp(pSW->pSchema[i].name, tagColName, TSDB_COL_NAME_LEN) == 0) {
      pTagSchema = &pSW->pSchema[i];
      break;
    }
  }
  if (pTagSchema == NULL) {
    code = TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST;
    goto _end;
  }

  *outType = pTagSchema->type;

  STag   *pTag  = (STag *)pChildEntry->ctbEntry.pTags;
  STagVal tv    = {.cid = pTagSchema->colId, .type = pTagSchema->type};
  bool    found = (pTag != NULL) && tTagGet(pTag, &tv);
  if (!found) {
    // tag has no value on this child: outLen=0 / outData=NULL
    goto _end;
  }

  if (IS_VAR_DATA_TYPE(pTagSchema->type)) {
    *outLen = (int32_t)tv.nData;
    if (*outLen > 0) {
      *outData = taosMemoryMalloc(*outLen);
      if (*outData == NULL) { code = terrno; goto _end; }
      memcpy(*outData, tv.pData, *outLen);
    }
  } else {
    *outLen  = (int32_t)tDataTypes[pTagSchema->type].bytes;
    *outData = taosMemoryMalloc(*outLen);
    if (*outData == NULL) { code = terrno; goto _end; }
    memcpy(*outData, &tv.i64, *outLen);
  }

_end:
  metaReaderClear(&stb);
  return code;
}

// Look up by name (used by request-driven path where wire holds refColName).
static int32_t streamReadChildTagConstValue(SVnode *pVnode, const SMetaEntry *pChildEntry,
                                            const char *tagColName, int8_t *outType,
                                            int32_t *outLen, char **outData) {
  return streamReadChildTagConstValueImpl(pVnode, pChildEntry, 0, tagColName,
                                          outType, outLen, outData);
}

// Look up by cid (used by local seed where SColRef.colName is not persisted).
static int32_t streamReadChildTagConstValueByCid(SVnode *pVnode, const SMetaEntry *pChildEntry,
                                                 col_id_t tagColId, int8_t *outType,
                                                 int32_t *outLen, char **outData) {
  return streamReadChildTagConstValueImpl(pVnode, pChildEntry, tagColId, NULL,
                                          outType, outLen, outData);
}

static int32_t vnodeFillTagValueFromChild(SVnode *pVnode, const SMetaEntry *pChildEntry,
                                          const char *tagColName, SVTableRefResolveRspItem *r) {
  r->terminated = true;
  int32_t code = streamReadChildTagConstValue(pVnode, pChildEntry, tagColName,
                                              &r->tagType, &r->tagLen, &r->tagData);
  vDebug("vgId:%d %s tag=%s code=0x%x type=%d len=%d", TD_VID(pVnode), __func__, tagColName, code,
         r->tagType, r->tagLen);
  if (code == TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST ||
      code == TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST) {
    // per-item soft error: surface via r->code, do not abort the batch.
    r->code = code;
    return 0;
  }
  return code;
}

// Batch-resolve multiple columns within the same table. Opens meta once for the
// table, then resolves each (colName, kind) pair against the same metadata.
// Results are appended to pRspItems in the same order as pCols.
static int32_t vnodeResolveTableGroup(SVnode *pVnode, const char *dbName, const char *tableName,
                                      SArray *pCols, SArray *pRspItems) {
  SMetaReader mr   = {0};
  int32_t     code = 0;
  int32_t     nCols = (pCols != NULL) ? (int32_t)taosArrayGetSize(pCols) : 0;

  vDebug("vgId:%d %s enter: db=%s table=%s nCols=%d", TD_VID(pVnode), __func__, dbName, tableName, nCols);

  metaReaderDoInit(&mr, pVnode->pMeta, META_READER_LOCK);
  if (metaGetTableEntryByName(&mr, tableName) != 0) {
    vDebug("vgId:%d %s ref table not exist: %s", TD_VID(pVnode), __func__, tableName);
    // Fill all columns with table-not-exist error
    for (int32_t i = 0; i < nCols; ++i) {
      SVTableRefResolveRspItem r = {0};
      r.code = TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST;
      if (taosArrayPush(pRspItems, &r) == NULL) { code = terrno; break; }
    }
    metaReaderClear(&mr);
    return code;
  }

  bool isVtable = (mr.me.type == TSDB_VIRTUAL_NORMAL_TABLE || mr.me.type == TSDB_VIRTUAL_CHILD_TABLE);

  // Pre-read parent stable info for virtual child table (shared across all columns)
  SMetaReader stbReader      = {0};
  bool        stbReaderInited = false;
  if (isVtable && mr.me.type == TSDB_VIRTUAL_CHILD_TABLE) {
    metaReaderDoInit(&stbReader, pVnode->pMeta, META_READER_LOCK);
    if (metaReaderGetTableEntryByUid(&stbReader, mr.me.ctbEntry.suid) != 0) {
      // Parent stable not found: all columns fail
      for (int32_t i = 0; i < nCols; ++i) {
        SVTableRefResolveRspItem r = {0};
        r.code = TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST;
        if (taosArrayPush(pRspItems, &r) == NULL) { code = terrno; break; }
      }
      metaReaderClear(&stbReader);
      metaReaderClear(&mr);
      return code;
    }
    stbReaderInited = true;
  }

  for (int32_t ci = 0; ci < nCols; ++ci) {
    SVTableRefResolveColSpec *c = taosArrayGet(pCols, ci);
    SVTableRefResolveRspItem  r = {0};

    if (isVtable) {
      // Resolve column on virtual table
      SColRefWrapper *pWrap = &mr.me.colRef;
      SColRef        *pArr  = (c->kind == STREAM_VREF_KIND_TAG) ? pWrap->pTagRef : pWrap->pColRef;
      int32_t         nArr  = (c->kind == STREAM_VREF_KIND_TAG) ? pWrap->nTagRefs : pWrap->nCols;

      SSchemaWrapper *pSW = NULL;
      if (mr.me.type == TSDB_VIRTUAL_NORMAL_TABLE) {
        pSW = &mr.me.ntbEntry.schemaRow;
      } else {
        pSW = (c->kind == STREAM_VREF_KIND_TAG) ? &stbReader.me.stbEntry.schemaTag
                                                 : &stbReader.me.stbEntry.schemaRow;
      }

      // Find cid by column name in schema
      col_id_t targetCid = 0;
      bool     cidFound  = false;
      for (int32_t k = 0; pSW != NULL && k < pSW->nCols; ++k) {
        if (strncmp(pSW->pSchema[k].name, c->colName, TSDB_COL_NAME_LEN) == 0) {
          targetCid = pSW->pSchema[k].colId;
          cidFound  = true;
          break;
        }
      }

      SColRef *pFound = NULL;
      if (cidFound) {
        for (int32_t j = 0; j < nArr && pArr != NULL; ++j) {
          if (pArr[j].id == targetCid) {
            pFound = &pArr[j];
            break;
          }
        }
      }

      if (pFound == NULL) {
        r.code = TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST;
      } else if (pFound->hasRef) {
        r.terminated     = false;
        r.nextRef.kind   = c->kind;
        r.nextRef.hasRef = true;
        tstrncpy(r.nextRef.refDbName,    pFound->refDbName,    TSDB_DB_NAME_LEN);
        tstrncpy(r.nextRef.refTableName, pFound->refTableName, TSDB_TABLE_NAME_LEN);
        tstrncpy(r.nextRef.refColName,   pFound->refColName,   TSDB_COL_NAME_LEN);
      } else {
        // !hasRef
        if (c->kind == STREAM_VREF_KIND_TAG) {
          if (mr.me.type != TSDB_VIRTUAL_CHILD_TABLE) {
            r.code = TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST;
          } else {
            r.nextRef.kind            = STREAM_VREF_KIND_TAG;
            r.nextRef.hasRef          = false;
            r.nextRef.refDbName[0]    = '\0';
            r.nextRef.refTableName[0] = '\0';
            r.nextRef.refColName[0]   = '\0';
            int32_t rc = vnodeFillTagValueFromChild(pVnode, &mr.me, c->colName, &r);
            if (rc != 0 && rc != TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST &&
                rc != TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST) {
              r.code = rc;
            } else if (rc != 0) {
              r.code = rc;
            }
          }
        } else {
          // STREAM_VREF_KIND_COL on vtable with NULL ref: terminal empty
          r.terminated              = true;
          r.nextRef.kind            = STREAM_VREF_KIND_COL;
          r.nextRef.hasRef          = false;
          r.nextRef.refDbName[0]    = '\0';
          r.nextRef.refTableName[0] = '\0';
          r.nextRef.refColName[0]   = '\0';
        }
      }
    } else {
      // Physical table
      if (c->kind == STREAM_VREF_KIND_COL) {
        r.terminated     = true;
        r.nextRef.kind   = STREAM_VREF_KIND_COL;
        r.nextRef.hasRef = true;
        tstrncpy(r.nextRef.refDbName,    dbName,    TSDB_DB_NAME_LEN);
        tstrncpy(r.nextRef.refTableName, tableName, TSDB_TABLE_NAME_LEN);
        tstrncpy(r.nextRef.refColName,   c->colName, TSDB_COL_NAME_LEN);
      } else {
        // TAG on physical table: only child table
        if (mr.me.type != TSDB_CHILD_TABLE) {
          r.code = TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST;
        } else {
          r.nextRef.kind   = STREAM_VREF_KIND_TAG;
          r.nextRef.hasRef = false;
          int32_t rc = vnodeFillTagValueFromChild(pVnode, &mr.me, c->colName, &r);
          if (rc != 0) r.code = rc;
        }
      }
    }

    if (taosArrayPush(pRspItems, &r) == NULL) {
      taosMemoryFreeClear(r.tagData);
      code = terrno;
      break;
    }
  }

  if (stbReaderInited) metaReaderClear(&stbReader);
  metaReaderClear(&mr);
  return code;
}

static int32_t vnodeResolveOneHop(SVnode *pVnode, const SVTableRefResolveItem *q,
                                  SVTableRefResolveRspItem *r) {
  SMetaReader mr   = {0};
  int32_t     code = 0;

  vDebug("vgId:%d %s enter: kind=%d ref=%s.%s.%s", TD_VID(pVnode), __func__, q->kind, q->refDbName,
         q->refTableName, q->refColName);

  metaReaderDoInit(&mr, pVnode->pMeta, META_READER_LOCK);
  if (metaGetTableEntryByName(&mr, q->refTableName) != 0) {
    vDebug("vgId:%d %s ref table not exist: %s", TD_VID(pVnode), __func__, q->refTableName);
    r->code = TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST;
    metaReaderClear(&mr);
    return 0;
  }

  bool isVtable = (mr.me.type == TSDB_VIRTUAL_NORMAL_TABLE || mr.me.type == TSDB_VIRTUAL_CHILD_TABLE);
  vDebug("vgId:%d %s table found: name=%s type=%d isVtable=%d", TD_VID(pVnode), __func__,
         q->refTableName, mr.me.type, isVtable);

  if (isVtable) {
    SColRefWrapper *pWrap = &mr.me.colRef;
    SColRef        *pArr  = (q->kind == STREAM_VREF_KIND_TAG) ? pWrap->pTagRef : pWrap->pColRef;
    int32_t         nArr  = (q->kind == STREAM_VREF_KIND_TAG) ? pWrap->nTagRefs : pWrap->nCols;

    // SColRef.colName is not populated for vtable storage, so resolve the schema
    // cid for refColName first and then look up pArr[] by id.
    col_id_t        targetCid = 0;
    bool            cidFound  = false;
    SSchemaWrapper *pSW       = NULL;
    SMetaReader     stbReader = {0};

    if (mr.me.type == TSDB_VIRTUAL_NORMAL_TABLE) {
      pSW = &mr.me.ntbEntry.schemaRow;
    } else {
      // VIRTUAL_CHILD_TABLE: col schema on parent stable's schemaRow,
      // tag schema on parent stable's schemaTag.
      metaReaderDoInit(&stbReader, pVnode->pMeta, META_READER_LOCK);
      if (metaReaderGetTableEntryByUid(&stbReader, mr.me.ctbEntry.suid) != 0) {
        vDebug("vgId:%d %s parent stable not found: suid=%" PRId64, TD_VID(pVnode), __func__,
               mr.me.ctbEntry.suid);
        metaReaderClear(&stbReader);
        r->code = TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST;
        metaReaderClear(&mr);
        return 0;
      }
      pSW = (q->kind == STREAM_VREF_KIND_TAG) ? &stbReader.me.stbEntry.schemaTag
                                              : &stbReader.me.stbEntry.schemaRow;
    }

    for (int32_t k = 0; pSW != NULL && k < pSW->nCols; ++k) {
      if (strncmp(pSW->pSchema[k].name, q->refColName, TSDB_COL_NAME_LEN) == 0) {
        targetCid = pSW->pSchema[k].colId;
        cidFound  = true;
        break;
      }
    }
    if (stbReader.pMeta != NULL) metaReaderClear(&stbReader);

    SColRef *pFound = NULL;
    if (cidFound) {
      for (int32_t j = 0; j < nArr && pArr != NULL; ++j) {
        if (pArr[j].id == targetCid) {
          pFound = &pArr[j];
          break;
        }
      }
    }
    if (pFound == NULL) {
      vDebug("vgId:%d %s ref col not found in vtable: col=%s cidFound=%d cid=%d kind=%d nArr=%d",
             TD_VID(pVnode), __func__, q->refColName, cidFound, targetCid, q->kind, nArr);
      r->code = TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST;
      metaReaderClear(&mr);
      return 0;
    }

    if (pFound->hasRef) {
      vDebug("vgId:%d %s vtable next-hop: -> %s.%s.%s", TD_VID(pVnode), __func__,
             pFound->refDbName, pFound->refTableName, pFound->refColName);
      r->terminated     = false;
      r->nextRef.kind   = q->kind;
      r->nextRef.hasRef = true;
      tstrncpy(r->nextRef.refDbName,    pFound->refDbName,    TSDB_DB_NAME_LEN);
      tstrncpy(r->nextRef.refTableName, pFound->refTableName, TSDB_TABLE_NAME_LEN);
      tstrncpy(r->nextRef.refColName,   pFound->refColName,   TSDB_COL_NAME_LEN);
      metaReaderClear(&mr);
      return 0;
    }

    // !hasRef branch:
    //   * COL on a vtable: NULL ref means terminal-empty (hasRef=false signals to caller).
    //   * TAG on a virtual child table: tag value may be stored as a constant on the
    //     vchild's own STag — read it locally and terminate with the value.
    //   * TAG on a virtual normal table: tag concept does not apply, treat as missing.
    if (q->kind == STREAM_VREF_KIND_TAG) {
      if (mr.me.type != TSDB_VIRTUAL_CHILD_TABLE) {
        r->code = TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST;
        metaReaderClear(&mr);
        return 0;
      }
      r->nextRef.kind            = STREAM_VREF_KIND_TAG;
      r->nextRef.hasRef          = false;
      r->nextRef.refDbName[0]    = '\0';
      r->nextRef.refTableName[0] = '\0';
      r->nextRef.refColName[0]   = '\0';
      code = vnodeFillTagValueFromChild(pVnode, &mr.me, q->refColName, r);
      metaReaderClear(&mr);
      return code;
    }

    // STREAM_VREF_KIND_COL on a vtable with NULL ref: terminate as empty.
    r->terminated              = true;
    r->nextRef.kind            = STREAM_VREF_KIND_COL;
    r->nextRef.hasRef          = false;
    r->nextRef.refDbName[0]    = '\0';
    r->nextRef.refTableName[0] = '\0';
    r->nextRef.refColName[0]   = '\0';
    metaReaderClear(&mr);
    return 0;
  }

  // Physical table reached: COL terminates with the current triple; TAG needs value.
  if (q->kind == STREAM_VREF_KIND_COL) {
    r->terminated     = true;
    r->nextRef.kind   = STREAM_VREF_KIND_COL;
    r->nextRef.hasRef = true;
    tstrncpy(r->nextRef.refDbName,    q->refDbName,    TSDB_DB_NAME_LEN);
    tstrncpy(r->nextRef.refTableName, q->refTableName, TSDB_TABLE_NAME_LEN);
    tstrncpy(r->nextRef.refColName,   q->refColName,   TSDB_COL_NAME_LEN);
    metaReaderClear(&mr);
    return 0;
  }

  // STREAM_VREF_KIND_TAG on physical table: only child table carries tag values.
  if (mr.me.type != TSDB_CHILD_TABLE) {
    r->code = TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST;
    metaReaderClear(&mr);
    return 0;
  }

  // Mark the terminal hop kind so the wire codec serializes the tag bytes.
  r->nextRef.kind   = STREAM_VREF_KIND_TAG;
  r->nextRef.hasRef = false;
  code = vnodeFillTagValueFromChild(pVnode, &mr.me, q->refColName, r);
  metaReaderClear(&mr);
  return code;
}

int32_t vnodeProcessVTableRefResolveReq(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t              code   = 0;
  int32_t              rspLen = 0;
  void                *pBuf   = NULL;
  SVTableRefResolveReq req    = {0};
  SVTableRefResolveRsp rsp    = {0};
  SRpcMsg              rspMsg = {0};

  vTrace("vgId:%d %s enter: contLen=%d msgType=%d", TD_VID(pVnode), __func__, pMsg->contLen,
         pMsg->msgType);

  if (tDeserializeSVTableRefResolveReq((char *)pMsg->pCont + sizeof(SMsgHead),
                                       pMsg->contLen - (int32_t)sizeof(SMsgHead), &req) < 0) {
    vError("vgId:%d %s deserialize failed", TD_VID(pVnode), __func__);
    code = TSDB_CODE_INVALID_MSG;
    goto _end;
  }

  {
    // Table-grouped format: resolve per-table batch (meta opened once per table)
    int32_t nGroups = (req.groups != NULL) ? (int32_t)taosArrayGetSize(req.groups) : 0;
    // Count total columns across all groups for pre-allocation
    int32_t totalCols = 0;
    for (int32_t i = 0; i < nGroups; ++i) {
      SVTableRefResolveGroupItem *g = taosArrayGet(req.groups, i);
      totalCols += (g->cols != NULL) ? (int32_t)taosArrayGetSize(g->cols) : 0;
    }
    vTrace("vgId:%d %s req: ver=%" PRId64 " groups=%d totalCols=%d",
           TD_VID(pVnode), __func__, req.ver, nGroups, totalCols);

    rsp.items = taosArrayInit(totalCols, sizeof(SVTableRefResolveRspItem));
    if (rsp.items == NULL) { code = terrno; goto _end; }

    for (int32_t i = 0; i < nGroups; ++i) {
      SVTableRefResolveGroupItem *g = taosArrayGet(req.groups, i);
      int32_t rc = vnodeResolveTableGroup(pVnode, g->dbName, g->tableName, g->cols, rsp.items);
      if (rc != 0) {
        code = rc;
        goto _end;
      }
    }
  }

  rspLen = tSerializeSVTableRefResolveRsp(NULL, 0, &rsp);
  if (rspLen < 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _end;
  }
  pBuf = rpcMallocCont(rspLen);
  if (pBuf == NULL) {
    code = terrno;
    goto _end;
  }
  if (tSerializeSVTableRefResolveRsp(pBuf, rspLen, &rsp) < 0) {
    rpcFreeCont(pBuf);
    pBuf = NULL;
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _end;
  }

_end:
  tFreeSVTableRefResolveReq(&req);
  tFreeSVTableRefResolveRsp(&rsp);

  rspMsg.info    = pMsg->info;
  rspMsg.pCont   = (code == 0) ? pBuf : NULL;
  rspMsg.contLen = (code == 0) ? rspLen : 0;
  rspMsg.code    = code;
  rspMsg.msgType = pMsg->msgType;

  if (code != 0) {
    vError("vgId:%d, vtable ref resolve failed since %s", TD_VID(pVnode), tstrerror(code));
    if (pBuf != NULL) rpcFreeCont(pBuf);
  }

  vDebug("vgId:%d %s send rsp: code=0x%x rspLen=%d", TD_VID(pVnode), __func__, code, rspMsg.contLen);
  tmsgSendRsp(&rspMsg);
  return 0;
}

// ============================================================================
// Task 6: chain resolution loop (single-vgId / local-vnode simplified version)
// ============================================================================

#define STREAM_VTB_MAX_HOPS 32

typedef struct SResolveWorkItem {
  int64_t  originVtbUid;                          // origin vtable uid this chain belongs to
  col_id_t originCid;                             // origin virtual cid (col or tag)
  int8_t   kind;                                  // EStreamVRefKind: COL or TAG
  char     refDbName   [TSDB_DB_NAME_LEN];
  char     refTableName[TSDB_TABLE_NAME_LEN];
  char     refColName  [TSDB_COL_NAME_LEN];
} SResolveWorkItem;

static SVTableResolveResult *streamGetOrCreateUidResult(SSHashObj *uid2Result, int64_t uid) {
  SVTableResolveResult **ppRes = (SVTableResolveResult **)tSimpleHashGet(uid2Result, &uid, sizeof(uid));
  if (ppRes != NULL && *ppRes != NULL) {
    return *ppRes;
  }

  SVTableResolveResult *pRes = taosMemoryCalloc(1, sizeof(*pRes));
  if (pRes == NULL) return NULL;
  pRes->colMap = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT));
  pRes->tagMap = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT));
  if (pRes->colMap == NULL || pRes->tagMap == NULL) {
    streamVTableResolveResultDestroy(pRes);
    return NULL;
  }
  if (tSimpleHashPut(uid2Result, &uid, sizeof(uid), &pRes, sizeof(pRes)) != 0) {
    streamVTableResolveResultDestroy(pRes);
    return NULL;
  }
  return pRes;
}

// Push initial work-items for a single vtable uid. Each requested cid (col or tag)
// is resolved against the local vtable entry's pColRef / pTagRef:
//   - COL hasRef=true   -> push next-hop work-item
//   - COL hasRef=false  -> directly write terminal SColResolveItem{hasRef=false} into colMap
//   - TAG hasRef=true   -> push next-hop work-item
//   - TAG hasRef=false  -> on a virtual child table the tag may be stored as a
//                          constant value on the vchild's own STag; read it locally
//                          and write a terminal STagValue into tagMap (no work-item).
//                          Virtual normal tables have no tag concept and fail.
//
// colCids == NULL means "all columns of this vtable" (used by the only-ts trigger
// path where the request carries just the primary-key TS but the response must
// describe every column ref). tagCids == NULL is treated as "no tag".
// Returns 0 on success; non-zero means whole-uid skip (table missing / cid missing / OOM).
static int32_t streamPushInitialWorkItemsForUid(SVnode *pVnode, int64_t uid, SArray *colCids, SArray *tagCids,
                                                SArray *workList, SSHashObj *uid2Result) {
  int32_t     code = 0;
  SMetaReader mr   = {0};
  metaReaderDoInit(&mr, pVnode->pMeta, META_READER_LOCK);

  // H2 v0.5: top-level vtable uid not present in local meta (concurrently
  // dropped) or entry type is not a vtable. Treat as a soft skip: log a
  // warning and return 0 without producing any uid2Result entry. The caller
  // (streamResolveVTableRefChain seed loop) sees rc==0 and simply continues;
  // downstream consumers that strictly require this uid (e.g. PSEUDO_COL
  // single-uid path) detect the missing entry and raise the error.
  if (metaReaderGetTableEntryByUid(&mr, uid) != 0) {
    stWarn("vgId:%d %s uid=%" PRId64 " META_NOT_FOUND -> H2 skip", TD_VID(pVnode), __func__, uid);
    goto _end;
  }
  if (mr.me.type != TSDB_VIRTUAL_NORMAL_TABLE && mr.me.type != TSDB_VIRTUAL_CHILD_TABLE) {
    stWarn("vgId:%d %s uid=%" PRId64 " type=%d not vtable -> H2 skip",
           TD_VID(pVnode), __func__, uid, mr.me.type);
    goto _end;
  }

  SVTableResolveResult *pRes = streamGetOrCreateUidResult(uid2Result, uid);
  if (pRes == NULL) {
    code = terrno;
    goto _end;
  }

  // Resolve column cids against pColRef. When colCids==NULL, iterate every
  // entry of this vtable's pColRef directly (no per-cid lookup needed).
  if (colCids == NULL) {
    for (int32_t j = 0; j < mr.me.colRef.nCols; ++j) {
      SColRef *pRef = &mr.me.colRef.pColRef[j];
      col_id_t cid  = pRef->id;
      if (!pRef->hasRef) {
        SColResolveItem *item = taosMemoryCalloc(1, sizeof(*item));
        if (item == NULL) { code = terrno; goto _end; }
        item->hasRef = false;
        if (tSimpleHashPut(pRes->colMap, &cid, sizeof(cid), &item, sizeof(item)) != 0) {
          taosMemoryFree(item);
          code = terrno;
          goto _end;
        }
        continue;
      }
      SResolveWorkItem w = {0};
      w.originVtbUid = uid;
      w.originCid    = cid;
      w.kind         = STREAM_VREF_KIND_COL;
      tstrncpy(w.refDbName,    pRef->refDbName,    TSDB_DB_NAME_LEN);
      tstrncpy(w.refTableName, pRef->refTableName, TSDB_TABLE_NAME_LEN);
      tstrncpy(w.refColName,   pRef->refColName,   TSDB_COL_NAME_LEN);
      if (taosArrayPush(workList, &w) == NULL) { code = terrno; goto _end; }
    }
  } else {
    int32_t nCol = (int32_t)taosArrayGetSize(colCids);
    for (int32_t i = 0; i < nCol; ++i) {
      col_id_t cid    = *(col_id_t *)taosArrayGet(colCids, i);
      SColRef *pFound = NULL;
      for (int32_t j = 0; j < mr.me.colRef.nCols; ++j) {
        if (mr.me.colRef.pColRef[j].id == cid) {
          pFound = &mr.me.colRef.pColRef[j];
          break;
        }
      }
      if (pFound == NULL) {
        stWarn("vgId:%d %s uid=%" PRId64 " COL cid=%d NOT_IN_COLREF -> uid skip",
               TD_VID(pVnode), __func__, uid, cid);
        code = TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST;
        goto _end;
      }
      if (!pFound->hasRef) {
        SColResolveItem *item = taosMemoryCalloc(1, sizeof(*item));
        if (item == NULL) { code = terrno; goto _end; }
        item->hasRef = false;
        if (tSimpleHashPut(pRes->colMap, &cid, sizeof(cid), &item, sizeof(item)) != 0) {
          taosMemoryFree(item);
          code = terrno;
          goto _end;
        }
        continue;
      }
      SResolveWorkItem w = {0};
      w.originVtbUid = uid;
      w.originCid    = cid;
      w.kind         = STREAM_VREF_KIND_COL;
      tstrncpy(w.refDbName,    pFound->refDbName,    TSDB_DB_NAME_LEN);
      tstrncpy(w.refTableName, pFound->refTableName, TSDB_TABLE_NAME_LEN);
      tstrncpy(w.refColName,   pFound->refColName,   TSDB_COL_NAME_LEN);
      if (taosArrayPush(workList, &w) == NULL) { code = terrno; goto _end; }
    }
  }

  // resolve tag cids against pTagRef
  int32_t nTag = (tagCids != NULL) ? (int32_t)taosArrayGetSize(tagCids) : 0;
  for (int32_t i = 0; i < nTag; ++i) {
    col_id_t cid    = *(col_id_t *)taosArrayGet(tagCids, i);
    SColRef *pFound = NULL;
    for (int32_t j = 0; j < mr.me.colRef.nTagRefs; ++j) {
      if (mr.me.colRef.pTagRef[j].id == cid) {
        pFound = &mr.me.colRef.pTagRef[j];
        break;
      }
    }
    // For a VCT, a tag cid that is absent from pTagRef[] (or present with
    // hasRef=0) means the value is stored locally on the child entry as a
    // constant inherited from the parent vstable schemaTag. Both cases must
    // go through the local constant-read path. Only non-VCT (i.e. VNT) tags
    // truly do not exist and should skip the uid.
    if (pFound == NULL && mr.me.type != TSDB_VIRTUAL_CHILD_TABLE) {
      stWarn("vgId:%d %s uid=%" PRId64 " TAG cid=%d NOT_IN_TAGREF type=%d -> uid skip",
             TD_VID(pVnode), __func__, uid, cid, mr.me.type);
      code = TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST;
      goto _end;
    }

    if (pFound == NULL || !pFound->hasRef) {
      // Constant tag on a virtual child table: read locally, write terminal STagValue.
      STagValue *tv = taosMemoryCalloc(1, sizeof(*tv));
      if (tv == NULL) { code = terrno; goto _end; }
      // Use cid: SColRef.colName is not persisted for vtable on disk
      // (the field is "for tmq get json" only). Resolve tag by colId in stable schemaTag.
      int32_t rc = streamReadChildTagConstValueByCid(pVnode, &mr.me, cid,
                                                    &tv->type, &tv->nLen, &tv->pData);
      if (rc != 0) {
        stWarn("vgId:%d %s uid=%" PRId64 " TAG cid=%d const-read err=0x%x -> uid skip",
               TD_VID(pVnode), __func__, uid, cid, rc);
        taosMemoryFreeClear(tv->pData);
        taosMemoryFree(tv);
        code = rc;
        goto _end;
      }
      if (tSimpleHashPut(pRes->tagMap, &cid, sizeof(cid), &tv, sizeof(tv)) != 0) {
        taosMemoryFreeClear(tv->pData);
        taosMemoryFree(tv);
        code = terrno;
        goto _end;
      }
      continue;
    }

    SResolveWorkItem w = {0};
    w.originVtbUid = uid;
    w.originCid    = cid;
    w.kind         = STREAM_VREF_KIND_TAG;
    tstrncpy(w.refDbName,    pFound->refDbName,    TSDB_DB_NAME_LEN);
    tstrncpy(w.refTableName, pFound->refTableName, TSDB_TABLE_NAME_LEN);
    tstrncpy(w.refColName,   pFound->refColName,   TSDB_COL_NAME_LEN);
    if (taosArrayPush(workList, &w) == NULL) { code = terrno; goto _end; }
  }

_end:
  metaReaderClear(&mr);
  return code;
}

// Local hash comparator: search a hash value in a sorted SArray<SVgroupInfo>.
// Mirrors catalog/ctgUtil.c:ctgHashValueComp; keep them in sync.
static int32_t streamVgHashValueComp(void const *lp, void const *rp) {
  uint32_t    *key = (uint32_t *)lp;
  SVgroupInfo *pVg = (SVgroupInfo *)rp;
  if (*key < pVg->hashBegin) return -1;
  if (*key > pVg->hashEnd)   return 1;
  return 0;
}

static int32_t streamVgInfoBeginComp(void const *lp, void const *rp) {
  SVgroupInfo *pLeft  = (SVgroupInfo *)lp;
  SVgroupInfo *pRight = (SVgroupInfo *)rp;
  if (pLeft->hashBegin < pRight->hashBegin) return -1;
  if (pLeft->hashBegin > pRight->hashBegin) return 1;
  return 0;
}

// Async-callback context used by streamFetchDbVgInfo to receive SUseDbRsp.
typedef struct SStreamFetchDbVgCtx {
  tsem_t     ready;
  SUseDbRsp *pRsp;
  int32_t    code;
} SStreamFetchDbVgCtx;

static int32_t streamProcessFetchDbVgRsp(void *param, SDataBuf *pMsg, int32_t code) {
  SStreamFetchDbVgCtx *pCtx = (SStreamFetchDbVgCtx *)param;
  if (code == TSDB_CODE_SUCCESS && pMsg != NULL && pMsg->pData != NULL && pMsg->len > 0) {
    pCtx->pRsp = taosMemoryCalloc(1, sizeof(SUseDbRsp));
    if (pCtx->pRsp == NULL) {
      code = terrno;
    } else if (tDeserializeSUseDbRsp(pMsg->pData, (int32_t)pMsg->len, pCtx->pRsp) != 0) {
      code = TSDB_CODE_INVALID_MSG;
    }
  } else if (code == TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_INVALID_MSG;
  }
  pCtx->code = code;

  if (pMsg != NULL) {
    taosMemoryFreeClear(pMsg->pData);
    taosMemoryFreeClear(pMsg->pEpSet);
  }
  TAOS_UNUSED(tsem_post(&pCtx->ready));
  return code;
}

// Fetch SUseDbRsp asynchronously from mnode for dbFName ("acctId.dbName").
// Caller owns *ppOut on success and must call tFreeSUsedbRsp + free the pointer.
static int32_t streamFetchDbVgInfo(SVnode *pVnode, const char *dbFName, SUseDbRsp **ppOut) {
  int32_t              code     = 0;
  SUseDbReq            req      = {0};
  void                *pReqBuf  = NULL;
  SMsgSendInfo        *pSendInfo = NULL;
  SStreamFetchDbVgCtx  ctx       = {0};
  bool                 semInited = false;
  SEpSet               epSet     = {0};

  *ppOut = NULL;
  tstrncpy(req.db, dbFName, sizeof(req.db));
  req.vgVersion  = -1;
  req.dbId       = 0;
  req.numOfTable = 0;
  req.stateTs    = 0;

  void *clientRpc = pVnode->msgCb.clientRpc;
  if (clientRpc == NULL) { code = TSDB_CODE_INVALID_PARA; goto _end; }

  if (tsem_init(&ctx.ready, 0, 0) != 0) { code = terrno; goto _end; }
  semInited = true;

  int32_t reqLen = tSerializeSUseDbReq(NULL, 0, &req);
  if (reqLen < 0) { code = terrno; goto _end; }
  pReqBuf = taosMemoryCalloc(1, reqLen);
  if (pReqBuf == NULL) { code = terrno; goto _end; }
  if (tSerializeSUseDbReq(pReqBuf, reqLen, &req) < 0) { code = terrno; goto _end; }

  pSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (pSendInfo == NULL) { code = terrno; goto _end; }

  pSendInfo->param          = &ctx;
  pSendInfo->msgInfo.pData  = pReqBuf;
  pSendInfo->msgInfo.len    = reqLen;
  pSendInfo->msgType        = TDMT_MND_GET_DB_INFO;
  pSendInfo->fp             = streamProcessFetchDbVgRsp;
  pReqBuf = NULL;  // ownership transferred to pSendInfo

  streamGetMnodeEpset(&epSet);

  code = asyncSendMsgToServer(clientRpc, &epSet, NULL, pSendInfo);
  pSendInfo = NULL;  // ownership transferred on success path
  if (code != 0) goto _end;

  if (tsem_timewait(&ctx.ready, 30000) != 0) { code = TSDB_CODE_TIMEOUT_ERROR; goto _end; }

  if (ctx.code != 0) { code = ctx.code; goto _end; }
  if (ctx.pRsp == NULL) { code = TSDB_CODE_INVALID_MSG; goto _end; }

  // Sort vgroup array by hashBegin so we can binary-search for routing.
  if (ctx.pRsp->pVgroupInfos != NULL) {
    taosArraySort(ctx.pRsp->pVgroupInfos, streamVgInfoBeginComp);
  }

  *ppOut  = ctx.pRsp;
  ctx.pRsp = NULL;

_end:
  if (pReqBuf != NULL) taosMemoryFree(pReqBuf);
  if (pSendInfo != NULL) taosMemoryFree(pSendInfo);
  if (ctx.pRsp != NULL) {
    tFreeSUsedbRsp(ctx.pRsp);
    taosMemoryFree(ctx.pRsp);
  }
  if (semInited) TAOS_UNUSED(tsem_destroy(&ctx.ready));
  return code;
}

// Get SUseDbRsp for dbFName, using cache if available; otherwise fetch and insert.
// Returned *ppOut is owned by the cache (when pCache != NULL) or by the caller
// (when pCache == NULL); caller never frees the cached entry.
static int32_t streamGetOrFetchDbVgInfo(SVnode *pVnode, SStreamVTableInfoCache *pCache,
                                        const char *dbFName, SUseDbRsp **ppOut, bool *pCached) {
  *ppOut = NULL;
  if (pCached) *pCached = false;

  if (pCache != NULL && pCache->dbVgInfo != NULL) {
    SUseDbRsp *pHit = (SUseDbRsp *)taosHashGet(pCache->dbVgInfo, dbFName, strlen(dbFName));
    if (pHit != NULL) {
      *ppOut = pHit;
      if (pCached) *pCached = true;
      return 0;
    }
  }

  SUseDbRsp *pNew = NULL;
  int32_t    code = streamFetchDbVgInfo(pVnode, dbFName, &pNew);
  if (code != 0) return code;
  // return 0;

  if (pCache != NULL && pCache->dbVgInfo != NULL) {
    // taosHashPut copies the value bytes; we must still keep the inner array
    // alive (pVgroupInfos is a heap pointer the cached entry now owns).
    if (taosHashPut(pCache->dbVgInfo, dbFName, strlen(dbFName), pNew, sizeof(*pNew)) != 0) {
      tFreeSUsedbRsp(pNew);
      taosMemoryFree(pNew);
      return terrno;
    }
    // Hash now owns pVgroupInfos via the copied struct; drop our outer wrapper
    // without freeing the array (cleanup uses tFreeSUsedbRsp on hash entries).
    taosMemoryFree(pNew);
    *ppOut = (SUseDbRsp *)taosHashGet(pCache->dbVgInfo, dbFName, strlen(dbFName));
    return 0;
  }

  *ppOut = pNew;
  return 0;
}

// Resolve target vgId/epSet for a (db, table) using cached SUseDbRsp routing info.
// dbFName is "acctId.dbName" (matches SUseDbRsp->db); tableName is the child name.
static int32_t streamRouteTableToVg(SUseDbRsp *pRsp, const char *dbFName, const char *tableName,
                                    int32_t *pVgId, SEpSet *pEpSet) {
  if (pRsp == NULL || pRsp->pVgroupInfos == NULL) return TSDB_CODE_INVALID_PARA;
  int32_t vgNum = (int32_t)taosArrayGetSize(pRsp->pVgroupInfos);
  if (vgNum <= 0) return TSDB_CODE_MND_DB_NOT_EXIST;

  char fullName[TSDB_TABLE_FNAME_LEN] = {0};
  int32_t n = tsnprintf(fullName, sizeof(fullName), "%s.%s", dbFName, tableName);
  if (n <= 0) return TSDB_CODE_INVALID_PARA;

  uint32_t hashValue = (uint32_t)taosGetTbHashVal(fullName, n, pRsp->hashMethod,
                                                  pRsp->hashPrefix, pRsp->hashSuffix);
  SVgroupInfo *pVg = (SVgroupInfo *)taosArraySearch(pRsp->pVgroupInfos, &hashValue,
                                                    streamVgHashValueComp, TD_EQ);
  if (pVg == NULL) return TSDB_CODE_MND_DB_NOT_EXIST;
  *pVgId  = pVg->vgId;
  *pEpSet = pVg->epSet;
  vDebug("stream route table:%s to vgId:%d, epSet inUse:%d numOfEps:%d",
        fullName, pVg->vgId, pVg->epSet.inUse, pVg->epSet.numOfEps);
  for (int32_t i = 0; i < pVg->epSet.numOfEps; ++i) {
    vDebug("stream route table:%s vgId:%d ep[%d]: %s:%u",
          fullName, pVg->vgId, i, pVg->epSet.eps[i].fqdn, pVg->epSet.eps[i].port);
  }
  return 0;
}

// Async-callback context used by streamSendOneVgResolveRpc to receive
// SVTableRefResolveRsp from a vnode.
typedef struct SStreamVgResolveCtx {
  tsem_t               ready;
  SVTableRefResolveRsp rsp;
  int32_t              code;
} SStreamVgResolveCtx;

static int32_t streamProcessVgResolveRsp(void *param, SDataBuf *pMsg, int32_t code) {
  SStreamVgResolveCtx *pCtx = (SStreamVgResolveCtx *)param;
  stTrace("stream vtable resolve rsp arrived: code=0x%x len=%d pData=%p", code,
          pMsg ? (int32_t)pMsg->len : -1, pMsg ? pMsg->pData : NULL);
  if (code == TSDB_CODE_SUCCESS) {
    if (pMsg != NULL && pMsg->pData != NULL && pMsg->len > 0) {
      if (tDeserializeSVTableRefResolveRsp(pMsg->pData, (int32_t)pMsg->len, &pCtx->rsp) < 0) {
        code = TSDB_CODE_OUT_OF_MEMORY;
      }
    } else {
      code = TSDB_CODE_INVALID_MSG;
    }
  }
  pCtx->code = code;
  stTrace("stream vtable resolve rsp processed: code=0x%x rspItems=%d", code,
          pCtx->rsp.items ? (int32_t)taosArrayGetSize(pCtx->rsp.items) : 0);

  if (pMsg != NULL) {
    taosMemoryFreeClear(pMsg->pData);
    taosMemoryFreeClear(pMsg->pEpSet);
  }
  TAOS_UNUSED(tsem_post(&pCtx->ready));
  return code;
}

// Send one TDMT_VND_VTABLE_REF_RESOLVE RPC for a subset of work-items targeting
// the same vgId. indexList[] holds positions inside the parent batch/outRspItems
// arrays so we can reorder responses back to the caller's original layout.
//
// Return value: 0 -> all items in this group filled successfully;
//              != 0 -> RPC failed; H2 v0.5 strict: caller propagates the error
//              (no per-uid skipping on transport failure).
static int32_t streamSendOneVgResolveRpc(SVnode *pVnode, const SEpSet *pEpSet, int32_t vgId,
                                         int64_t ver, SArray *batch, SArray *indexList,
                                         SArray *outRspItems) {
  int32_t              code      = 0;
  SVTableRefResolveReq req       = {0};
  void                *pReqBuf   = NULL;
  SMsgSendInfo        *pSendInfo = NULL;
  SStreamVgResolveCtx  ctx       = {0};
  bool                 semInited = false;

  int32_t cnt = (int32_t)taosArrayGetSize(indexList);
  stTrace("vgId:%d %s enter: targetVgId=%d ver=%" PRId64 " items=%d", TD_VID(pVnode), __func__,
          vgId, ver, cnt);

  // Build table-grouped request: group work items by (dbName, tableName)
  req.ver    = ver;
  req.groups = taosArrayInit(4, sizeof(SVTableRefResolveGroupItem));
  if (req.groups == NULL) { code = terrno; goto _end; }

  // Use a temp hash to map "dbName\0tableName" -> index in req.groups
  SHashObj *tblGroupMap = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (tblGroupMap == NULL) { code = terrno; goto _end; }

  int32_t totalCols = 0;
  for (int32_t i = 0; i < cnt; ++i) {
    int32_t           pos = *(int32_t *)taosArrayGet(indexList, i);
    SResolveWorkItem *w   = taosArrayGet(batch, pos);

    // Build table key
    char    tblKey[TSDB_DB_NAME_LEN + 1 + TSDB_TABLE_NAME_LEN];
    int32_t dLen = (int32_t)strlen(w->refDbName);
    int32_t tLen = (int32_t)strlen(w->refTableName);
    memcpy(tblKey, w->refDbName, dLen);
    tblKey[dLen] = '\0';
    memcpy(tblKey + dLen + 1, w->refTableName, tLen);
    int32_t keyLen = dLen + 1 + tLen;

    int32_t *pGroupIdx = taosHashGet(tblGroupMap, tblKey, keyLen);
    int32_t  groupIdx;
    if (pGroupIdx == NULL) {
      // New table group
      SVTableRefResolveGroupItem g = {0};
      tstrncpy(g.dbName, w->refDbName, TSDB_DB_NAME_LEN);
      tstrncpy(g.tableName, w->refTableName, TSDB_TABLE_NAME_LEN);
      g.cols = taosArrayInit(4, sizeof(SVTableRefResolveColSpec));
      if (g.cols == NULL) { code = terrno; taosHashCleanup(tblGroupMap); goto _end; }
      if (taosArrayPush(req.groups, &g) == NULL) {
        taosArrayDestroy(g.cols);
        code = terrno;
        taosHashCleanup(tblGroupMap);
        goto _end;
      }
      groupIdx = (int32_t)taosArrayGetSize(req.groups) - 1;
      taosHashPut(tblGroupMap, tblKey, keyLen, &groupIdx, sizeof(groupIdx));
    } else {
      groupIdx = *pGroupIdx;
    }

    // Add column to the group
    SVTableRefResolveGroupItem *gp = taosArrayGet(req.groups, groupIdx);
    SVTableRefResolveColSpec    colSpec = {0};
    tstrncpy(colSpec.colName, w->refColName, TSDB_COL_NAME_LEN);
    colSpec.kind = w->kind;
    if (taosArrayPush(gp->cols, &colSpec) == NULL) {
      code = terrno;
      taosHashCleanup(tblGroupMap);
      goto _end;
    }

    totalCols++;
  }

  taosHashCleanup(tblGroupMap);

  void *clientRpc = pVnode->msgCb.clientRpc;
  if (clientRpc == NULL) { code = TSDB_CODE_INVALID_PARA; goto _end; }

  if (tsem_init(&ctx.ready, 0, 0) != 0) { code = terrno; goto _end; }
  semInited = true;

  int32_t reqLen = tSerializeSVTableRefResolveReq(NULL, 0, &req);
  if (reqLen < 0) { code = terrno; goto _end; }
  // Prepend SMsgHead so dnode dispatcher (vmPutMsgToQueue) can route by vgId.
  int32_t totalLen = reqLen + (int32_t)sizeof(SMsgHead);
  pReqBuf = taosMemoryCalloc(1, totalLen);
  if (pReqBuf == NULL) { code = terrno; goto _end; }
  if (tSerializeSVTableRefResolveReq((char *)pReqBuf + sizeof(SMsgHead), reqLen, &req) < 0) {
    code = terrno;
    goto _end;
  }
  ((SMsgHead *)pReqBuf)->vgId    = htonl(vgId);
  ((SMsgHead *)pReqBuf)->contLen = htonl(totalLen);

  pSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (pSendInfo == NULL) { code = terrno; goto _end; }

  pSendInfo->param         = &ctx;
  pSendInfo->msgInfo.pData = pReqBuf;
  pSendInfo->msgInfo.len   = totalLen;
  pSendInfo->msgType       = TDMT_VND_VTABLE_REF_RESOLVE;
  pSendInfo->fp            = streamProcessVgResolveRsp;
  pReqBuf = NULL;  // ownership transferred to pSendInfo

  code = asyncSendMsgToServer(clientRpc, (SEpSet *)pEpSet, NULL, pSendInfo);
  pSendInfo = NULL;  // ownership transferred (or freed by asyncSendMsgToServer on error)
  stTrace("vgId:%d %s asyncSend done: targetVgId=%d code=0x%x reqLen=%d", TD_VID(pVnode), __func__,
          vgId, code, totalLen);
  if (code != 0) goto _end;

  if (tsem_timewait(&ctx.ready, 30000) != 0) { code = TSDB_CODE_TIMEOUT_ERROR; goto _end; }
  stTrace("vgId:%d %s wait done: targetVgId=%d ctxCode=0x%x rspItems=%d", TD_VID(pVnode), __func__,
          vgId, ctx.code, ctx.rsp.items ? (int32_t)taosArrayGetSize(ctx.rsp.items) : 0);

  if (ctx.code != 0) { code = ctx.code; goto _end; }

  int32_t m = (ctx.rsp.items != NULL) ? (int32_t)taosArrayGetSize(ctx.rsp.items) : 0;
  if (m != totalCols) { code = TSDB_CODE_INVALID_MSG; goto _end; }

  // Move each rsp item to its original position in outRspItems (pre-sized).
  // Response order matches the flattened column order (same as indexList iteration).
  for (int32_t i = 0; i < cnt; ++i) {
    int32_t pos = *(int32_t *)taosArrayGet(indexList, i);
    SVTableRefResolveRspItem *src = taosArrayGet(ctx.rsp.items, i);
    SVTableRefResolveRspItem *dst = taosArrayGet(outRspItems, pos);
    *dst = *src;
    src->tagData = NULL;
    src->tagLen  = 0;
  }

_end:
  stTrace("vgId:%d %s exit: targetVgId=%d code=0x%x", TD_VID(pVnode), __func__, vgId, code);
  if (pReqBuf != NULL) taosMemoryFree(pReqBuf);
  if (pSendInfo != NULL) taosMemoryFree(pSendInfo);
  tFreeSVTableRefResolveReq(&req);
  tFreeSVTableRefResolveRsp(&ctx.rsp);
  if (semInited) TAOS_UNUSED(tsem_destroy(&ctx.ready));
  return code;
}

// Drive one resolution round for a heterogeneous batch: group work-items by the
// target vgId of (refDbName, refTableName), issue one RPC per vg, and write
// responses back to outRspItems in batch order.
//
// pCache (optional): caches db routing info across hops/uids to avoid hammering
// mnode. NULL means no cache (every miss goes to mnode).
//
// outRspItems must be pre-sized with batch.size() default-zero entries; this
// function fills them in place.
//
// Helper: build a composite key "dbName\0tableName" for tblRefCache lookup.
static void streamBuildTblCacheKey(const char *dbName, const char *tableName, char *out, int32_t *outLen) {
  int32_t dLen = (int32_t)strlen(dbName);
  int32_t tLen = (int32_t)strlen(tableName);
  memcpy(out, dbName, dLen);
  out[dLen] = '\0';  // separator
  memcpy(out + dLen + 1, tableName, tLen);
  *outLen = dLen + 1 + tLen;
}

// Helper: look up tblRefCache for a resolved column. Returns pointer to cached
// SVTableRefResolveRspItem or NULL if not cached.
static SVTableRefResolveRspItem *streamTblRefCacheLookup(SStreamVTableInfoCache *pCache,
                                                          const char *dbName, const char *tableName,
                                                          const char *colName, int8_t kind) {
  (void)kind;  // tag and col share namespace within a table
  if (pCache == NULL || pCache->tblRefCache == NULL) return NULL;
  char    key[TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN + 2];
  int32_t keyLen = 0;
  streamBuildTblCacheKey(dbName, tableName, key, &keyLen);
  STableRefCacheEntry *pEntry = (STableRefCacheEntry *)taosHashGet(pCache->tblRefCache, key, keyLen);
  if (pEntry == NULL || pEntry->colResults == NULL) return NULL;
  // Lookup by colName only (tag/col namespace is unified within a table)
  return (SVTableRefResolveRspItem *)taosHashGet(pEntry->colResults, colName, (int32_t)strlen(colName));
}

// Helper: insert a resolved column result into tblRefCache.
static void streamTblRefCacheInsert(SStreamVTableInfoCache *pCache,
                                     const char *dbName, const char *tableName,
                                     const char *colName, int8_t kind,
                                     const SVTableRefResolveRspItem *pItem) {
  (void)kind;  // tag and col share namespace within a table
  if (pCache == NULL || pCache->tblRefCache == NULL) return;
  char    key[TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN + 2];
  int32_t keyLen = 0;
  streamBuildTblCacheKey(dbName, tableName, key, &keyLen);
  STableRefCacheEntry *pEntry = (STableRefCacheEntry *)taosHashGet(pCache->tblRefCache, key, keyLen);
  if (pEntry == NULL) {
    STableRefCacheEntry newEntry = {0};
    newEntry.colResults = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    if (newEntry.colResults == NULL) return;
    if (taosHashPut(pCache->tblRefCache, key, keyLen, &newEntry, sizeof(newEntry)) != 0) {
      taosHashCleanup(newEntry.colResults);
      return;
    }
    pEntry = (STableRefCacheEntry *)taosHashGet(pCache->tblRefCache, key, keyLen);
    if (pEntry == NULL) return;
  }
  int32_t colKeyLen = (int32_t)strlen(colName);
  // Store a copy (tagData is not deep-copied: tag results are consumed immediately by caller)
  SVTableRefResolveRspItem copy = *pItem;
  copy.tagData = NULL;  // tag ownership stays with caller
  copy.tagLen  = 0;
  taosHashPut(pEntry->colResults, colName, colKeyLen, &copy, sizeof(copy));
}

//
// streamCallResolveBatched: drive one hop of resolution with table-level dedup.
//
// Optimization (Issue 4): instead of sending per-(table,column) items blindly,
// we (a) check the local tblRefCache first, (b) deduplicate by (db,table,col,kind)
// so the same physical column is only resolved once per RPC round, and (c) cache
// the results for use in subsequent hops.
//
// H2 v0.5 strict: any per-vg routing/RPC failure is propagated upward as the
// return value. Per-item business errors are still reported through
// outRspItems[i].code so the caller can include the originating uid/cid in
// its log; the caller (streamResolveVTableRefChain) decides how to react.
//
// Returns 0 on success; non-zero on OOM, routing, or RPC failure.
static int32_t streamCallResolveBatched(SVnode *pVnode, SStreamVTableInfoCache *pCache,
                                        int64_t ver, SArray *batch, SArray *outRspItems) {
  int32_t   code     = 0;
  SHashObj *vg2Idx   = NULL;  // key: int32_t vgId, value: SArray<int32_t>* (positions in dedupItems)
  SHashObj *vg2Ep    = NULL;  // key: int32_t vgId, value: SEpSet
  SHashObj *dedupMap = NULL;  // key: "kind:db\0table\0col", value: int32_t (position in dedupItems)
  SArray   *dedupItems    = NULL;  // SArray<SResolveWorkItem> unique items to send
  SArray   *dedupRspItems = NULL;  // SArray<SVTableRefResolveRspItem> responses for dedup items

  int32_t n = (int32_t)taosArrayGetSize(batch);
  stDebug("vgId:%d %s enter: ver=%" PRId64 " batch=%d", TD_VID(pVnode), __func__, ver, n);
  // Pre-size outRspItems with n zero entries so positional writes are safe.
  for (int32_t i = (int32_t)taosArrayGetSize(outRspItems); i < n; ++i) {
    SVTableRefResolveRspItem zero = {0};
    if (taosArrayPush(outRspItems, &zero) == NULL) { code = terrno; goto _end; }
  }

  // Phase 1: Check tblRefCache and deduplicate work items.
  // For items hitting the cache, write the result directly into outRspItems.
  // For items needing remote resolution, deduplicate by (kind, db, table, col).
  dedupMap   = taosHashInit(n, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  dedupItems = taosArrayInit(n, sizeof(SResolveWorkItem));
  if (dedupMap == NULL || dedupItems == NULL) { code = terrno; goto _end; }

  // origToDedupIdx[i] = position in dedupItems for batch[i], or -1 if served from cache.
  int32_t *origToDedupIdx = taosMemoryCalloc(n, sizeof(int32_t));
  if (origToDedupIdx == NULL) { code = terrno; goto _end; }

  int32_t cacheHits = 0;
  for (int32_t i = 0; i < n; ++i) {
    SResolveWorkItem *w = taosArrayGet(batch, i);

    // Check tblRefCache
    SVTableRefResolveRspItem *cached = streamTblRefCacheLookup(pCache, w->refDbName, w->refTableName,
                                                               w->refColName, w->kind);
    if (cached != NULL) {
      // Cache hit: copy result directly to outRspItems[i]
      SVTableRefResolveRspItem *dst = taosArrayGet(outRspItems, i);
      *dst = *cached;
      dst->tagData = NULL;  // tag data is not stored in cache
      dst->tagLen  = 0;
      origToDedupIdx[i] = -1;
      cacheHits++;
      continue;
    }

    // Build dedup key: "dbName\0tableName\0colName"
    // (tag and col names share the same namespace within a table, no need for kind)
    char dedupKey[TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN + 4];
    int32_t dkLen = 0;
    int32_t dbLen = (int32_t)strlen(w->refDbName);
    memcpy(dedupKey, w->refDbName, dbLen);
    dkLen = dbLen;
    dedupKey[dkLen] = '\0'; dkLen++;
    int32_t tbLen = (int32_t)strlen(w->refTableName);
    memcpy(dedupKey + dkLen, w->refTableName, tbLen);
    dkLen += tbLen;
    dedupKey[dkLen] = '\0'; dkLen++;
    int32_t clLen = (int32_t)strlen(w->refColName);
    memcpy(dedupKey + dkLen, w->refColName, clLen);
    dkLen += clLen;

    int32_t *pExistIdx = (int32_t *)taosHashGet(dedupMap, dedupKey, dkLen);
    if (pExistIdx != NULL) {
      // Already have this (kind,db,table,col) in dedupItems
      origToDedupIdx[i] = *pExistIdx;
    } else {
      int32_t newIdx = (int32_t)taosArrayGetSize(dedupItems);
      if (taosArrayPush(dedupItems, w) == NULL) { code = terrno; taosMemoryFree(origToDedupIdx); goto _end; }
      if (taosHashPut(dedupMap, dedupKey, dkLen, &newIdx, sizeof(newIdx)) != 0) {
        code = terrno; taosMemoryFree(origToDedupIdx); goto _end;
      }
      origToDedupIdx[i] = newIdx;
    }
  }
  stDebug("vgId:%d %s dedup: batch=%d cacheHits=%d dedupItems=%d",
          TD_VID(pVnode), __func__, n, cacheHits, (int32_t)taosArrayGetSize(dedupItems));

  int32_t dedupN = (int32_t)taosArrayGetSize(dedupItems);
  if (dedupN == 0) {
    // All items served from cache
    taosMemoryFree(origToDedupIdx);
    goto _end;
  }

  // Phase 2: Route deduplicated items to vgId groups
  vg2Idx = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  vg2Ep  = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (vg2Idx == NULL || vg2Ep == NULL) { code = terrno; taosMemoryFree(origToDedupIdx); goto _end; }

  int32_t acctId = 0;
  if (sscanf(pVnode->config.dbname, "%d.", &acctId) != 1) {
    code = TSDB_CODE_INVALID_PARA;
    taosMemoryFree(origToDedupIdx); goto _end;
  }

  for (int32_t i = 0; i < dedupN; ++i) {
    SResolveWorkItem *w = taosArrayGet(dedupItems, i);

    char dbFName[TSDB_DB_FNAME_LEN] = {0};
    (void)tsnprintf(dbFName, sizeof(dbFName), "%d.%s", acctId, w->refDbName);

    SUseDbRsp *pRsp = NULL;
    bool       fromCache = false;
    int32_t    rc = streamGetOrFetchDbVgInfo(pVnode, pCache, dbFName, &pRsp, &fromCache);
    if (rc != 0) {
      stError("vgId:%d %s uid=%" PRId64 " getDbVgInfo db=%s rc=0x%x -> propagate",
              TD_VID(pVnode), __func__, w->originVtbUid, dbFName, rc);
      code = rc;
      taosMemoryFree(origToDedupIdx); goto _end;
    }

    int32_t vgId  = 0;
    SEpSet  epSet = {0};
    rc = streamRouteTableToVg(pRsp, dbFName, w->refTableName, &vgId, &epSet);
    if (!fromCache && pCache == NULL) {
      tFreeSUsedbRsp(pRsp);
      taosMemoryFree(pRsp);
    }
    if (rc != 0) {
      stError("vgId:%d %s uid=%" PRId64 " routeTableToVg db=%s tb=%s rc=0x%x -> propagate",
              TD_VID(pVnode), __func__, w->originVtbUid, dbFName, w->refTableName, rc);
      code = rc;
      taosMemoryFree(origToDedupIdx); goto _end;
    }

    SArray **ppList = (SArray **)taosHashGet(vg2Idx, &vgId, sizeof(vgId));
    SArray  *pList  = NULL;
    if (ppList == NULL) {
      pList = taosArrayInit(4, sizeof(int32_t));
      if (pList == NULL) { code = terrno; taosMemoryFree(origToDedupIdx); goto _end; }
      if (taosHashPut(vg2Idx, &vgId, sizeof(vgId), &pList, sizeof(pList)) != 0) {
        taosArrayDestroy(pList);
        code = terrno; taosMemoryFree(origToDedupIdx); goto _end;
      }
      if (taosHashPut(vg2Ep, &vgId, sizeof(vgId), &epSet, sizeof(epSet)) != 0) {
        code = terrno; taosMemoryFree(origToDedupIdx); goto _end;
      }
    } else {
      pList = *ppList;
    }
    if (taosArrayPush(pList, &i) == NULL) { code = terrno; taosMemoryFree(origToDedupIdx); goto _end; }
  }

  // Phase 3: Issue one RPC per vg group using deduplicated items
  dedupRspItems = taosArrayInit(dedupN, sizeof(SVTableRefResolveRspItem));
  if (dedupRspItems == NULL) { code = terrno; taosMemoryFree(origToDedupIdx); goto _end; }
  for (int32_t i = 0; i < dedupN; ++i) {
    SVTableRefResolveRspItem zero = {0};
    if (taosArrayPush(dedupRspItems, &zero) == NULL) { code = terrno; taosMemoryFree(origToDedupIdx); goto _end; }
  }

  void *pIter = taosHashIterate(vg2Idx, NULL);
  while (pIter != NULL) {
    SArray  *pList   = *(SArray **)pIter;
    size_t   keyLen  = 0;
    int32_t *pVgKey  = (int32_t *)taosHashGetKey(pIter, &keyLen);
    int32_t  vgId    = *pVgKey;
    SEpSet  *pEpSet  = (SEpSet *)taosHashGet(vg2Ep, &vgId, sizeof(vgId));

    int32_t rc = 0;
    if (vgId == TD_VID(pVnode)) {
      // Local fast path: skip RPC and call the resolver in-process.
      int32_t cnt = (int32_t)taosArrayGetSize(pList);
      for (int32_t j = 0; j < cnt; ++j) {
        int32_t           pos = *(int32_t *)taosArrayGet(pList, j);
        SResolveWorkItem *w   = taosArrayGet(dedupItems, pos);
        SVTableRefResolveItem q = {0};
        q.kind   = w->kind;
        q.hasRef = true;
        tstrncpy(q.refDbName,    w->refDbName,    TSDB_DB_NAME_LEN);
        tstrncpy(q.refTableName, w->refTableName, TSDB_TABLE_NAME_LEN);
        tstrncpy(q.refColName,   w->refColName,   TSDB_COL_NAME_LEN);
        SVTableRefResolveRspItem *dst = taosArrayGet(dedupRspItems, pos);
        int32_t one = vnodeResolveOneHop(pVnode, &q, dst);
        if (one != 0) {
          dst->code = one;
          if (one == TSDB_CODE_OUT_OF_MEMORY) { rc = one; break; }
        }
      }
    } else {
      rc = streamSendOneVgResolveRpc(pVnode, pEpSet, vgId, ver, dedupItems, pList, dedupRspItems);
    }
    stTrace("vgId:%d %s per-vg done: targetVgId=%d items=%d rc=0x%x local=%d", TD_VID(pVnode),
            __func__, vgId, (int32_t)taosArrayGetSize(pList), rc, vgId == TD_VID(pVnode));
    if (rc != 0) {
      stError("vgId:%d %s per-vg RPC failed: targetVgId=%d items=%d rc=0x%x -> propagate",
              TD_VID(pVnode), __func__, vgId, (int32_t)taosArrayGetSize(pList), rc);
      taosHashCancelIterate(vg2Idx, pIter);
      code = rc;
      taosMemoryFree(origToDedupIdx); goto _end;
    }
    pIter = taosHashIterate(vg2Idx, pIter);
  }

  // Phase 4: Scatter dedup results back to original positions and update cache
  for (int32_t i = 0; i < dedupN; ++i) {
    SResolveWorkItem         *w   = taosArrayGet(dedupItems, i);
    SVTableRefResolveRspItem *rsp = taosArrayGet(dedupRspItems, i);
    // Insert into tblRefCache for future hops
    streamTblRefCacheInsert(pCache, w->refDbName, w->refTableName, w->refColName, w->kind, rsp);
  }

  for (int32_t i = 0; i < n; ++i) {
    int32_t dedupIdx = origToDedupIdx[i];
    if (dedupIdx < 0) continue;  // was served from cache
    SVTableRefResolveRspItem *src = taosArrayGet(dedupRspItems, dedupIdx);
    SVTableRefResolveRspItem *dst = taosArrayGet(outRspItems, i);
    *dst = *src;
    // tagData ownership: only the first consumer takes it; others get NULL
    // (TAG results produce only one terminal entry per originCid anyway, and
    // dedup groups only identical (db,table,col,kind) tuples).
  }
  // Transfer tagData ownership: for TAG kind, only dedupRspItems still owns
  // the tagData pointer. We need to give it to the *first* outRspItems entry
  // that maps to each dedupIdx.
  {
    bool *tagTaken = taosMemoryCalloc(dedupN, sizeof(bool));
    if (tagTaken != NULL) {
      for (int32_t i = 0; i < n; ++i) {
        int32_t dedupIdx = origToDedupIdx[i];
        if (dedupIdx < 0) continue;
        SVTableRefResolveRspItem *src = taosArrayGet(dedupRspItems, dedupIdx);
        SVTableRefResolveRspItem *dst = taosArrayGet(outRspItems, i);
        if (src->tagData != NULL && !tagTaken[dedupIdx]) {
          dst->tagData = src->tagData;
          dst->tagLen  = src->tagLen;
          src->tagData = NULL;
          src->tagLen  = 0;
          tagTaken[dedupIdx] = true;
        } else {
          dst->tagData = NULL;
          dst->tagLen  = 0;
        }
      }
      taosMemoryFree(tagTaken);
    }
  }

  taosMemoryFree(origToDedupIdx);
  origToDedupIdx = NULL;

_end:
  stDebug("vgId:%d %s exit: code=0x%x", TD_VID(pVnode), __func__, code);
  if (vg2Idx != NULL) {
    void *p = taosHashIterate(vg2Idx, NULL);
    while (p != NULL) {
      taosArrayDestroy(*(SArray **)p);
      p = taosHashIterate(vg2Idx, p);
    }
    taosHashCleanup(vg2Idx);
  }
  if (vg2Ep != NULL) taosHashCleanup(vg2Ep);
  if (dedupMap != NULL) taosHashCleanup(dedupMap);
  taosArrayDestroy(dedupItems);
  if (dedupRspItems != NULL) {
    // Free any remaining tagData in dedup responses that were not transferred
    int32_t sz = (int32_t)taosArrayGetSize(dedupRspItems);
    for (int32_t i = 0; i < sz; ++i) {
      SVTableRefResolveRspItem *r = taosArrayGet(dedupRspItems, i);
      taosMemoryFreeClear(r->tagData);
    }
    taosArrayDestroy(dedupRspItems);
  }
  return code;
}

// Function A: drive multi-hop chain resolution for a batch of vtable uids on the
// triggering vnode. Cross-vgId version: groups each batch by target vgId, then
// dispatches one TDMT_VND_VTABLE_REF_RESOLVE RPC per group via streamCallResolveBatched.
//
// H2 v0.5 strict error policy:
//   - top-level uid not in local meta (or not a vtable type) -> warn + skip
//     that uid; function returns 0 and uid simply has no entry in *ppUid2Result.
//   - any other error (mid-chain table/col/tag missing, RPC failure, OOM,
//     hop > MAX_HOPS, ref-triple inconsistency) -> A returns the underlying
//     errCode; caller (reader -> trigger -> mnode) propagates and fail-fasts.
// pCache (optional): caches db routing info (SUseDbRsp) across calls.
// pReaderInfo (optional): when vtbUids is NULL/empty, all live uids are pulled from
//                          qStreamGetTableArrayList(pReaderInfo). If both are NULL/empty
//                          this function returns INVALID_PARA.
// Output: *ppUid2Result is a fresh SSHashObj<uid -> SVTableResolveResult*>;
// caller owns it and must use streamVTableResolveResultDestroy + tSimpleHashCleanup.
int32_t streamResolveVTableRefChain(SVnode *pVnode, SStreamVTableInfoCache *pCache,
                                    SStreamTriggerReaderInfo *pReaderInfo, int64_t ver,
                                    SArray *vtbUids, SArray *virtColCids, SArray *virtTagCids,
                                    SSHashObj **ppUid2Result) {
  int32_t    code         = 0;
  SArray    *workList     = NULL;
  SArray    *nextWorkList = NULL;
  SArray    *rspItems     = NULL;
  SSHashObj *uid2Result   = NULL;

  SArray    *fullUids     = NULL;
  SArray    *pTableListArray = NULL;

  if (pVnode == NULL || ppUid2Result == NULL) return TSDB_CODE_INVALID_PARA;
  *ppUid2Result = NULL;

  // Invalidate per-table ref cache at the start of each full resolve cycle.
  // The cache is only useful within a single multi-hop resolve call to avoid
  // redundant RPC for the same (db,table,col) across hops; stale results from
  // a previous cycle could mask schema changes.
  if (pCache != NULL) {
    streamTblRefCacheInvalidate(pCache);
  }

  stDebug("vgId:%d %s enter: ver=%" PRId64 " vtbUids=%d virtCols=%d virtTags=%d", TD_VID(pVnode),
          __func__, ver, (int32_t)taosArrayGetSize(vtbUids),
          (int32_t)taosArrayGetSize(virtColCids), (int32_t)taosArrayGetSize(virtTagCids));

  // Full-uid branch: pull live uids from the reader's table list.
  if (vtbUids == NULL || taosArrayGetSize(vtbUids) == 0) {
    if (pReaderInfo == NULL) return TSDB_CODE_INVALID_PARA;
    pTableListArray = qStreamGetTableArrayList(pReaderInfo);
    if (pTableListArray == NULL) { code = terrno; goto _end; }

    int32_t nAll = (int32_t)taosArrayGetSize(pTableListArray);
    fullUids = taosArrayInit(nAll, sizeof(int64_t));
    if (fullUids == NULL) { code = terrno; goto _end; }
    for (int32_t i = 0; i < nAll; ++i) {
      SStreamTableKeyInfo *pKey = taosArrayGetP(pTableListArray, i);
      if (pKey == NULL || pKey->markedDeleted) continue;
      if (taosArrayPush(fullUids, &pKey->uid) == NULL) { code = terrno; goto _end; }
    }
    vtbUids = fullUids;
    stDebug("vgId:%d %s full-uid branch: tableList=%d activeUids=%d", TD_VID(pVnode), __func__,
            nAll, (int32_t)taosArrayGetSize(fullUids));
  }

  // Dump the uid list and the requested cid/tag set so we can compare against
  // what gets committed to cache and what later recheck sees.
  {
    int32_t nu = (int32_t)taosArrayGetSize(vtbUids);
    for (int32_t i = 0; i < nu && i < 32; ++i) {
      stDebug("vgId:%d %s seed uid[%d]=%" PRId64, TD_VID(pVnode), __func__, i,
              *(int64_t *)taosArrayGet(vtbUids, i));
    }
    int32_t nc = virtColCids ? (int32_t)taosArrayGetSize(virtColCids) : -1;
    for (int32_t i = 0; i < nc && i < 32; ++i) {
      stDebug("vgId:%d %s req colCid[%d]=%d", TD_VID(pVnode), __func__, i,
              *(col_id_t *)taosArrayGet(virtColCids, i));
    }
    int32_t nt = virtTagCids ? (int32_t)taosArrayGetSize(virtTagCids) : 0;
    for (int32_t i = 0; i < nt && i < 32; ++i) {
      stDebug("vgId:%d %s req tagCid[%d]=%d", TD_VID(pVnode), __func__, i,
              *(col_id_t *)taosArrayGet(virtTagCids, i));
    }
  }

  uid2Result = tSimpleHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  if (uid2Result == NULL) { code = terrno; goto _end; }

  workList = taosArrayInit(64, sizeof(SResolveWorkItem));
  if (workList == NULL) { code = terrno; goto _end; }

  // 1. seed work-list. H2 v0.5: streamPushInitialWorkItemsForUid swallows
  //    top-level uid-not-exist (warn + return 0 without entry); any other
  //    error (col/tag not in ref triple, OOM) is propagated upward so the
  //    caller (reader -> trigger -> mnode) can fail-fast and trigger a
  //    redeploy.
  int32_t nUid = (int32_t)taosArrayGetSize(vtbUids);
  for (int32_t i = 0; i < nUid; ++i) {
    int64_t uid = *(int64_t *)taosArrayGet(vtbUids, i);
    int32_t rc  = streamPushInitialWorkItemsForUid(pVnode, uid, virtColCids, virtTagCids, workList, uid2Result);
    if (rc == 0) continue;
    stError("vgId:%d %s seed uid=%" PRId64 " push rc=0x%x -> propagate (strict)",
            TD_VID(pVnode), __func__, uid, rc);
    code = rc;
    goto _end;
  }
  stDebug("vgId:%d %s after seed: workListSz=%d uid2ResultSz=%d",
          TD_VID(pVnode), __func__,
          (int32_t)taosArrayGetSize(workList),
          tSimpleHashGetSize(uid2Result));

  // 2. main hop loop
  for (int32_t hop = 0; hop < STREAM_VTB_MAX_HOPS; ++hop) {
    int32_t cur = (int32_t)taosArrayGetSize(workList);
    stDebug("vgId:%d %s hop=%d workListSz=%d", TD_VID(pVnode), __func__, hop, cur);
    if (cur == 0) break;

    rspItems = taosArrayInit(cur, sizeof(SVTableRefResolveRspItem));
    if (rspItems == NULL) { code = terrno; goto _end; }

    int32_t rc = streamCallResolveBatched(pVnode, pCache, ver, workList, rspItems);
    if (rc != 0) {
      // H2 v0.5: any error (OOM, routing, RPC) propagates immediately.
      code = rc;
      goto _end;
    }

    nextWorkList = taosArrayInit(cur, sizeof(SResolveWorkItem));
    if (nextWorkList == NULL) { code = terrno; goto _end; }

    int32_t bn = (int32_t)taosArrayGetSize(workList);
    for (int32_t i = 0; i < bn; ++i) {
      SResolveWorkItem         *w = taosArrayGet(workList, i);
      SVTableRefResolveRspItem *r = taosArrayGet(rspItems, i);

      if (r->code != 0) {
        // H2 v0.5: any per-item business error (mid-chain ref-table missing,
        // ref-col missing, tag changed, etc.) is propagated upward.
        stError("vgId:%d %s hop=%d uid=%" PRId64 " kind=%d cid=%d rspCode=0x%x -> propagate",
                TD_VID(pVnode), __func__, hop, w->originVtbUid, w->kind, w->originCid, r->code);
        int32_t rspCode = r->code;
        taosMemoryFreeClear(r->tagData);
        code = rspCode;
        goto _end;
      }

      if (r->terminated) {
        SVTableResolveResult *pRes = streamGetOrCreateUidResult(uid2Result, w->originVtbUid);
        if (pRes == NULL) { taosMemoryFreeClear(r->tagData); code = terrno; goto _end; }

        if (w->kind == STREAM_VREF_KIND_COL) {
          SColResolveItem *item = taosMemoryCalloc(1, sizeof(*item));
          if (item == NULL) { taosMemoryFreeClear(r->tagData); code = terrno; goto _end; }
          item->hasRef = r->nextRef.hasRef;
          if (item->hasRef) {
            tstrncpy(item->refDbName,    r->nextRef.refDbName,    TSDB_DB_NAME_LEN);
            tstrncpy(item->refTableName, r->nextRef.refTableName, TSDB_TABLE_NAME_LEN);
            tstrncpy(item->refColName,   r->nextRef.refColName,   TSDB_COL_NAME_LEN);
          }
          if (tSimpleHashPut(pRes->colMap, &w->originCid, sizeof(w->originCid), &item, sizeof(item)) != 0) {
            taosMemoryFree(item);
            taosMemoryFreeClear(r->tagData);
            code = terrno; goto _end;
          }
          stDebug("vgId:%d %s hop=%d uid=%" PRId64 " COL cid=%d TERMINATED hasRef=%d ref=%s.%s.%s -> colMap",
                  TD_VID(pVnode), __func__, hop, w->originVtbUid, w->originCid,
                  item->hasRef, item->refDbName, item->refTableName, item->refColName);
          taosMemoryFreeClear(r->tagData);
        } else {
          STagValue *tv = taosMemoryCalloc(1, sizeof(*tv));
          if (tv == NULL) { taosMemoryFreeClear(r->tagData); code = terrno; goto _end; }
          tv->type  = r->tagType;
          tv->nLen  = r->tagLen;
          tv->pData = r->tagData;
          r->tagData = NULL;
          if (tSimpleHashPut(pRes->tagMap, &w->originCid, sizeof(w->originCid), &tv, sizeof(tv)) != 0) {
            taosMemoryFreeClear(tv->pData);
            taosMemoryFree(tv);
            code = terrno; goto _end;
          }
        }
      } else {
        SResolveWorkItem next = {0};
        next.originVtbUid = w->originVtbUid;
        next.originCid    = w->originCid;
        next.kind         = r->nextRef.kind;
        tstrncpy(next.refDbName,    r->nextRef.refDbName,    TSDB_DB_NAME_LEN);
        tstrncpy(next.refTableName, r->nextRef.refTableName, TSDB_TABLE_NAME_LEN);
        tstrncpy(next.refColName,   r->nextRef.refColName,   TSDB_COL_NAME_LEN);
        if (taosArrayPush(nextWorkList, &next) == NULL) {
          taosMemoryFreeClear(r->tagData);
          code = terrno; goto _end;
        }
        stDebug("vgId:%d %s hop=%d uid=%" PRId64 " kind=%d cid=%d NEXT-HOP -> %s.%s.%s",
                TD_VID(pVnode), __func__, hop, w->originVtbUid, next.kind, w->originCid,
                next.refDbName, next.refTableName, next.refColName);
        taosMemoryFreeClear(r->tagData);
      }
    }

    taosArrayDestroy(rspItems); rspItems = NULL;
    taosArrayDestroy(workList);
    workList     = nextWorkList;
    nextWorkList = NULL;
  }

  // 3. hop overflow: any leftover work-items mean the chain exceeded MAX_HOPS.
  //    H2 v0.5: report TSDB_CODE_STREAM_VTB_REF_TOO_DEEP rather than silently
  //    skipping the offending uids.
  if (workList != NULL) {
    int32_t leftover = (int32_t)taosArrayGetSize(workList);
    if (leftover > 0) {
      for (int32_t i = 0; i < leftover; ++i) {
        SResolveWorkItem *w = taosArrayGet(workList, i);
        stError("vgId:%d %s OVERFLOW uid=%" PRId64 " kind=%d cid=%d ref=%s.%s.%s",
                TD_VID(pVnode), __func__, w->originVtbUid, w->kind, w->originCid,
                w->refDbName, w->refTableName, w->refColName);
      }
      stError("vgId:%d %s HOP_OVERFLOW leftover=%d -> propagate TOO_DEEP",
              TD_VID(pVnode), __func__, leftover);
      code = TSDB_CODE_STREAM_VTB_REF_TOO_DEEP;
      goto _end;
    }
  }

  // Final dump: per-uid colMap/tagMap contents.
  {
    void *p = NULL; int32_t it = 0;
    while ((p = tSimpleHashIterate(uid2Result, p, &it)) != NULL) {
      int64_t                uid = *(int64_t *)tSimpleHashGetKey(p, NULL);
      SVTableResolveResult  *res = *(SVTableResolveResult **)p;
      stDebug("vgId:%d %s FINAL uid=%" PRId64 " colMapSz=%d tagMapSz=%d",
              TD_VID(pVnode), __func__, uid,
              tSimpleHashGetSize(res->colMap),
              tSimpleHashGetSize(res->tagMap));
      void *cp = NULL; int32_t ci = 0;
      while ((cp = tSimpleHashIterate(res->colMap, cp, &ci)) != NULL) {
        col_id_t         cid  = *(col_id_t *)tSimpleHashGetKey(cp, NULL);
        SColResolveItem *item = *(SColResolveItem **)cp;
        stDebug("vgId:%d %s   FINAL uid=%" PRId64 " COL cid=%d hasRef=%d ref=%s.%s.%s",
                TD_VID(pVnode), __func__, uid, cid, item ? item->hasRef : -1,
                item ? item->refDbName : "", item ? item->refTableName : "",
                item ? item->refColName : "");
      }
      void *tp = NULL; int32_t ti = 0;
      while ((tp = tSimpleHashIterate(res->tagMap, tp, &ti)) != NULL) {
        col_id_t   cid = *(col_id_t *)tSimpleHashGetKey(tp, NULL);
        STagValue *tv  = *(STagValue **)tp;
        stDebug("vgId:%d %s   FINAL uid=%" PRId64 " TAG cid=%d type=%d nLen=%d",
                TD_VID(pVnode), __func__, uid, cid, tv ? tv->type : -1, tv ? tv->nLen : -1);
      }
    }
  }

  *ppUid2Result = uid2Result;
  uid2Result    = NULL;

_end:
  stDebug("vgId:%d %s exit: code=0x%x outUidCnt=%d", TD_VID(pVnode), __func__, code,
          uid2Result ? tSimpleHashGetSize(uid2Result) :
          (*ppUid2Result ? tSimpleHashGetSize(*ppUid2Result) : 0));
  if (fullUids        != NULL) taosArrayDestroy(fullUids);
  if (pTableListArray != NULL) taosArrayDestroyP(pTableListArray, taosMemFree);
  if (workList     != NULL) taosArrayDestroy(workList);
  if (nextWorkList != NULL) taosArrayDestroy(nextWorkList);
  if (rspItems     != NULL) {
    int32_t m = (int32_t)taosArrayGetSize(rspItems);
    for (int32_t i = 0; i < m; ++i) {
      SVTableRefResolveRspItem *r = taosArrayGet(rspItems, i);
      taosMemoryFreeClear(r->tagData);
    }
    taosArrayDestroy(rspItems);
  }
  streamDestroyUid2ResultMap(&uid2Result);
  return code;
}

// ============================================================================
// vnodeResolveVTableTagChain
//
// For trigger streams whose source is a virtual super table, executor's
// `getColInfoResultForGroupbyForStream` needs literal tag values per vchild to
// compute the partition groupId. The default `metaGetTableTagsByUidsVersion`
// only reads ctbEntry.pTags directly, so col-ref tags resolve to NULL and all
// vchildren collapse into the same group.
//
// This helper post-processes the STUidTagInfo list: when suid is a virtual
// stable, each vchild uid is fed into `streamResolveVTableRefChain` (which
// already handles multi-hop and cross-vnode resolution), and the returned tag
// values are repacked into a fresh STag in stable-schemaTag order. Failures
// per uid are best-effort and leave the original pTagVal untouched.
// ============================================================================
int32_t vnodeResolveVTableTagChain(void *pVnode, int64_t suid, SArray *pUidTagList) {
  if (pVnode == NULL || pUidTagList == NULL) return 0;

  int32_t      code         = 0;
  SVnode      *pVn          = (SVnode *)pVnode;
  stTrace("vgId:%d %s ENTER suid=%" PRId64 " nUids=%d", TD_VID(pVn), __func__, suid,
          (int32_t)taosArrayGetSize(pUidTagList));
  SMetaReader  mr           = {0};
  bool         readerInited = false;
  SArray      *uids         = NULL;
  SArray      *tagCids      = NULL;
  SArray      *tagVals      = NULL;
  SSHashObj   *uid2Result   = NULL;
  SSchema     *pTagSchema   = NULL;
  int32_t      nTagCols     = 0;

  int32_t nUids = (int32_t)taosArrayGetSize(pUidTagList);
  if (nUids == 0) return 0;

  // 1) confirm suid refers to a virtual super table; otherwise no-op.
  metaReaderDoInit(&mr, pVn->pMeta, META_READER_LOCK);
  readerInited = true;
  if (metaReaderGetTableEntryByUid(&mr, suid) != 0) {
    stDebug("vgId:%d %s metaReader miss suid=%" PRId64, TD_VID(pVn), __func__, suid);
    goto _end;
  }
  if (mr.me.type != TSDB_SUPER_TABLE || !TABLE_IS_VIRTUAL(mr.me.flags)) {
    stDebug("vgId:%d %s skip suid=%" PRId64 " type=%d flags=0x%x", TD_VID(pVn), __func__,
            suid, (int32_t)mr.me.type, (uint32_t)mr.me.flags);
    goto _end;
  }
  pTagSchema = mr.me.stbEntry.schemaTag.pSchema;
  nTagCols   = mr.me.stbEntry.schemaTag.nCols;
  if (pTagSchema == NULL || nTagCols <= 0) {
    goto _end;
  }

  stTrace("vgId:%d %s suid=%" PRId64 " nUids=%d nTagCols=%d", TD_VID(pVn), __func__,
          suid, nUids, nTagCols);

  // 2) build uid + tagCid arrays for the chain resolver.
  uids = taosArrayInit(nUids, sizeof(int64_t));
  if (uids == NULL) { code = terrno; goto _end; }
  for (int32_t i = 0; i < nUids; ++i) {
    STUidTagInfo *p = taosArrayGet(pUidTagList, i);
    if (p == NULL) continue;
    int64_t uid = (int64_t)p->uid;
    if (taosArrayPush(uids, &uid) == NULL) { code = terrno; goto _end; }
  }

  tagCids = taosArrayInit(nTagCols, sizeof(col_id_t));
  if (tagCids == NULL) { code = terrno; goto _end; }
  for (int32_t i = 0; i < nTagCols; ++i) {
    col_id_t cid = pTagSchema[i].colId;
    if (taosArrayPush(tagCids, &cid) == NULL) { code = terrno; goto _end; }
  }

  // 3) chain-resolve.
  code = streamResolveVTableRefChain(pVn, NULL, NULL, -1, uids, NULL, tagCids, &uid2Result);
  if (code != 0 || uid2Result == NULL) {
    stTrace("vgId:%d %s chain resolve rc=0x%x uid2Result=%p", TD_VID(pVn), __func__,
            code, (void *)uid2Result);
    code = 0;  // best-effort; do not propagate to caller
    goto _end;
  }

  // 4) rebuild STag per uid by merging:
  //      - literal STagVals already present in p->pTagVal (vchild may declare
  //        some tags as plain literals and only some as colRefs)
  //      - chain-resolved STagValues from streamResolveVTableRefChain
  //    The chain resolver only fills cids that appeared as colRefs, so without
  //    this merge the literal tags would be lost when we tTagNew a fresh STag.
  for (int32_t i = 0; i < nUids; ++i) {
    STUidTagInfo *p = taosArrayGet(pUidTagList, i);
    if (p == NULL) continue;
    int64_t uid = (int64_t)p->uid;

    SVTableResolveResult **ppRes =
        (SVTableResolveResult **)tSimpleHashGet(uid2Result, &uid, sizeof(uid));
    SVTableResolveResult  *pRes  = (ppRes != NULL) ? *ppRes : NULL;
    bool hasResolvedTags = (pRes != NULL && pRes->tagMap != NULL && tSimpleHashGetSize(pRes->tagMap) > 0);
    stTrace("vgId:%d %s merge uid=%" PRId64 " ppRes=%p pRes=%p tagMap=%p tagMapSz=%d",
            TD_VID(pVn), __func__, uid, (void *)ppRes, (void *)pRes,
            (void *)(pRes ? pRes->tagMap : NULL),
            (pRes && pRes->tagMap) ? tSimpleHashGetSize(pRes->tagMap) : -1);

    if (tagVals != NULL) {
      taosArrayClear(tagVals);
    } else {
      tagVals = taosArrayInit(nTagCols, sizeof(STagVal));
      if (tagVals == NULL) { code = terrno; goto _end; }
    }

    bool anyChange = false;
    for (int32_t j = 0; j < nTagCols; ++j) {
      col_id_t cid = pTagSchema[j].colId;

      // Prefer chain-resolved value when present (overrides any stale literal).
      if (hasResolvedTags) {
        STagValue **ppTV = (STagValue **)tSimpleHashGet(pRes->tagMap, &cid, sizeof(cid));
        stTrace("vgId:%d %s merge uid=%" PRId64 " probe cid=%d ppTV=%p",
                TD_VID(pVn), __func__, uid, (int32_t)cid, (void *)ppTV);
        if (ppTV != NULL && *ppTV != NULL) {
          STagValue *tv = *ppTV;
          stTrace("vgId:%d %s merge uid=%" PRId64 " cid=%d tv=%p type=%d nLen=%d pData=%p",
                  TD_VID(pVn), __func__, uid, (int32_t)cid, (void *)tv,
                  (int32_t)tv->type, (int32_t)tv->nLen, (void *)tv->pData);
          if (tv->pData != NULL && tv->nLen > 0) {
            STagVal v = {0};
            v.cid  = cid;
            v.type = tv->type;
            if (IS_VAR_DATA_TYPE(tv->type)) {
              v.nData = (uint32_t)tv->nLen;
              v.pData = (uint8_t *)tv->pData;
            } else {
              int32_t copyLen = tv->nLen < (int32_t)sizeof(int64_t) ? tv->nLen : (int32_t)sizeof(int64_t);
              memcpy(&v.i64, tv->pData, copyLen);
            }
            if (taosArrayPush(tagVals, &v) == NULL) { code = terrno; goto _end; }
            anyChange = true;
            continue;
          }
        }
      }

      // Fall back to the literal tag in the original STag, if any.
      if (p->pTagVal != NULL) {
        STagVal probe = {.cid = cid};
        if (tTagGet((const STag *)p->pTagVal, &probe)) {
          if (taosArrayPush(tagVals, &probe) == NULL) { code = terrno; goto _end; }
        }
      }
    }

    if (!anyChange) continue;  // nothing was resolved -> keep the original STag

    STag   *pNewTag = NULL;
    int32_t rc      = tTagNew(tagVals, 1, false, &pNewTag);
    if (rc != 0 || pNewTag == NULL) {
      stDebug("vgId:%d %s uid=%" PRId64 " tTagNew rc=0x%x -> keep original", TD_VID(pVn),
              __func__, uid, rc);
      continue;
    }
    if (p->pTagVal != NULL) taosMemoryFree(p->pTagVal);
    p->pTagVal = pNewTag;
    stDebug("vgId:%d %s uid=%" PRId64 " rebuilt STag with %d tag(s) (literals merged)",
            TD_VID(pVn), __func__, uid, (int32_t)taosArrayGetSize(tagVals));
  }

_end:
  if (readerInited) metaReaderClear(&mr);
  if (uids    != NULL) taosArrayDestroy(uids);
  if (tagCids != NULL) taosArrayDestroy(tagCids);
  if (tagVals != NULL) taosArrayDestroy(tagVals);
  streamDestroyUid2ResultMap(&uid2Result);
  return code;
}
