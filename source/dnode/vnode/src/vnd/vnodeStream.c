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
    if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
      code = 0;
      ST_TASK_WLOG("stream reader scan alter table %s not exist, metaGetTableUidByName", pTable->tbName);
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
    if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
      ST_TASK_WLOG("stream reader scan alter table %s not exist, metaGetTableUidByName", req.tbName);
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
    if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
      ST_TASK_WLOG("stream reader scan alter table %s not exist, metaGetTableUidByName", req.tbName);
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
    if (hookRc == TSDB_CODE_STREAM_VTB_TAG_CHANGED) { code = hookRc; goto end; }
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
    if (hookRc == TSDB_CODE_STREAM_VTB_TAG_CHANGED) { code = hookRc; goto end; }
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
    // C2a: atomic full-map replacement.
    SSHashObj *oldMap = pCache->uid2Result;
    pCache->uid2Result = uid2Result;
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
    void *iter = NULL; int32_t it = 0;
    while ((iter = tSimpleHashIterate(uid2Result, iter, &it)) != NULL) {
      int64_t uid = *(int64_t *)tSimpleHashGetKey(iter, NULL);
      SVTableResolveResult *r = *(SVTableResolveResult **)iter;
      SVTableResolveResult **pOld = (SVTableResolveResult **)tSimpleHashGet(pCache->uid2Result, &uid, sizeof(uid));
      if (pOld && *pOld) streamVTableResolveResultDestroy(*pOld);
      int32_t rc = tSimpleHashPut(pCache->uid2Result, &uid, sizeof(uid), &r, POINTER_BYTES);
      if (rc != 0) { code = rc; goto _exit; }
    }
    // Detach values from uid2Result (they are now owned by the cache) and free the shell.
    tSimpleHashCleanup(uid2Result);
  }

  // Sync request col/tag cid arrays so refresh hook knows what to re-resolve.
  if (cids != NULL) {
    if (pCache->reqColCids != NULL) taosArrayDestroy(pCache->reqColCids);
    pCache->reqColCids = taosArrayDup(cids, NULL);
  }
  if (tagCids != NULL) {
    if (pCache->reqTagCids != NULL) taosArrayDestroy(pCache->reqTagCids);
    pCache->reqTagCids = taosArrayDup(tagCids, NULL);
  }
  pCache->lastCheckMs = taosGetTimestampMs();
  pCache->valid       = true;

_exit:
  taosWUnLockLatch(&pCache->lock);
  *ppUid2Result = NULL;
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
  if (uid2Result != NULL) {
    void *iter = NULL; int32_t it = 0;
    while ((iter = tSimpleHashIterate(uid2Result, iter, &it)) != NULL) {
      SVTableResolveResult **pp = (SVTableResolveResult **)iter;
      if (pp && *pp) streamVTableResolveResultDestroy(*pp);
    }
    tSimpleHashCleanup(uid2Result);
  }
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

// Re-resolve full uid set and diff against existing cache.
// Returns 0 (col-only diffs collected into changedUids), TSDB_CODE_STREAM_VTB_TAG_CHANGED,
// or other non-zero for technical errors. Cache lock must be held by caller (write).
static int32_t streamRecheckVTableCache(SVnode *pVnode, SStreamTriggerReaderInfo *pInfo,
                                        int64_t walVer, SArray *changedUids) {
  int32_t                code = 0;
  SSHashObj             *uid2Result = NULL;
  SStreamVTableInfoCache *pCache    = pInfo->vtbCache;
  if (pCache == NULL) return 0;

  code = streamResolveVTableRefChain(pVnode, NULL, pInfo, walVer, NULL,
                                     pCache->reqColCids, pCache->reqTagCids, &uid2Result);
  if (code != 0) return code;

  // Diff against existing cache.
  if (pCache->uid2Result != NULL) {
    void *iter = NULL; int32_t it = 0;
    while ((iter = tSimpleHashIterate(pCache->uid2Result, iter, &it)) != NULL) {
      int64_t uid = *(int64_t *)tSimpleHashGetKey(iter, NULL);
      SVTableResolveResult *oldRes = *(SVTableResolveResult **)iter;
      SVTableResolveResult **ppNew = (SVTableResolveResult **)tSimpleHashGet(uid2Result, &uid, sizeof(uid));
      if (ppNew == NULL || *ppNew == NULL) continue;
      SVTableResolveResult *newRes = *ppNew;

      // Tag diff first — any tag change is fatal.
      if (oldRes->tagMap != NULL) {
        void *it2 = NULL; int32_t i2 = 0;
        while ((it2 = tSimpleHashIterate(oldRes->tagMap, it2, &i2)) != NULL) {
          col_id_t cid = *(col_id_t *)tSimpleHashGetKey(it2, NULL);
          STagValue *oldV = *(STagValue **)it2;
          STagValue **ppNewV = (newRes->tagMap == NULL) ? NULL :
                               (STagValue **)tSimpleHashGet(newRes->tagMap, &cid, sizeof(cid));
          STagValue *newV = (ppNewV == NULL) ? NULL : *ppNewV;
          if (!tagValueEqual(oldV, newV)) {
            code = TSDB_CODE_STREAM_VTB_TAG_CHANGED;
            goto _exit;
          }
        }
      }

      // Col diff — only changed uids are appended to changedUids.
      bool colChanged = false;
      if (oldRes->colMap != NULL) {
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
      if (colChanged && changedUids != NULL) {
        if (taosArrayPush(changedUids, &uid) == NULL) { code = terrno; goto _exit; }
      }
    }
  }

  // Atomic full replacement (C2a).
  {
    SSHashObj *oldMap = pCache->uid2Result;
    pCache->uid2Result = uid2Result;
    uid2Result = NULL;
    if (oldMap != NULL) {
      void *iter = NULL; int32_t it = 0;
      while ((iter = tSimpleHashIterate(oldMap, iter, &it)) != NULL) {
        SVTableResolveResult **pp = (SVTableResolveResult **)iter;
        if (pp && *pp) streamVTableResolveResultDestroy(*pp);
      }
      tSimpleHashCleanup(oldMap);
    }
    pCache->valid = true;
  }

_exit:
  if (uid2Result != NULL) {
    void *iter = NULL; int32_t it = 0;
    while ((iter = tSimpleHashIterate(uid2Result, iter, &it)) != NULL) {
      SVTableResolveResult **pp = (SVTableResolveResult **)iter;
      if (pp && *pp) streamVTableResolveResultDestroy(*pp);
    }
    tSimpleHashCleanup(uid2Result);
  }
  return code;
}

// Throttled hook called at the entry of every WAL meta request.
// On tag change: returns TSDB_CODE_STREAM_VTB_TAG_CHANGED so caller bails out fast.
// On col-only change: appends affected uids into rsp->tableBlock as TABLE_BLOCK_ADD.
// All other cases: returns 0 and lets caller continue normal processing.
static int32_t streamMaybeRecheckVTableCache(SVnode *pVnode, SStreamTriggerReaderInfo *pInfo,
                                             int64_t walVer, SSTriggerWalNewRsp *pRsp) {
  if (pInfo == NULL || pInfo->vtbCache == NULL || !pInfo->vtbCache->valid) return 0;
  SStreamVTableInfoCache *pCache = pInfo->vtbCache;
  int64_t now = taosGetTimestampMs();
  if (now - pCache->lastCheckMs < 10 * 1000) return 0;

  int32_t code = 0;
  SArray *changedUids = NULL;
  taosWLockLatch(&pCache->lock);
  if (now - pCache->lastCheckMs < 10 * 1000) {
    taosWUnLockLatch(&pCache->lock);
    return 0;
  }
  changedUids = taosArrayInit(0, sizeof(int64_t));
  if (changedUids == NULL) { taosWUnLockLatch(&pCache->lock); return terrno; }
  code = streamRecheckVTableCache(pVnode, pInfo, walVer, changedUids);
  pCache->lastCheckMs = taosGetTimestampMs();
  taosWUnLockLatch(&pCache->lock);

  if (code == TSDB_CODE_STREAM_VTB_TAG_CHANGED) {
    taosArrayDestroy(changedUids);
    return code;
  }
  if (code != 0) {
    taosArrayDestroy(changedUids);
    return code;
  }
  if (pRsp != NULL && taosArrayGetSize(changedUids) > 0) {
    int32_t rc = addUidListToBlock(changedUids, &pRsp->tableBlock, walVer, &pRsp->totalRows, TABLE_BLOCK_ADD);
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
    ST_TASK_DLOG("vgId:%d %s i=%zu refTableName='%s' refColName='%s'",
                 TD_VID(pVnode), __func__, i, oInfo->refTableName, oInfo->refColName);
    code = sStreamReaderInfo->storageApi.metaReaderFn.getTableEntryByVersionName(&metaReader, req->origTableInfoReq.ver, oInfo->refTableName);
    if (code != 0) {
      int32_t origCode = code;
      code = 0;
      ST_TASK_ELOG("vgId:%d %s get table entry by name:%s failed, msg:%s", TD_VID(pVnode), __func__, oInfo->refTableName, tstrerror(origCode));
      continue;
    }
    vTableInfo->uid = metaReader.me.uid;
    vTableInfo->resolved = 1;  // default: resolved
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

    if (sSchemaWrapper != NULL) {
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
  SSHashObj                *uid2Result  = NULL;
  SVTableResolveResult     *pRes        = NULL;
  SMetaReader               metaReader  = {0};
  int64_t streamId = req->base.streamId;
  stsDebug("vgId:%d %s start, ver:%"PRId64, TD_VID(pVnode), __func__, req->virTablePseudoColReq.ver);

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

    // PSEUDO_COL bypasses cache — pass NULL so resolver does not read/write it.
    STREAM_CHECK_RET_GOTO(streamResolveVTableRefChain(pVnode, NULL, sStreamReaderInfo,
                                                     req->virTablePseudoColReq.ver,
                                                     singleUid, emptyCols, cols, &uid2Result));

    SVTableResolveResult **pp = (SVTableResolveResult **)tSimpleHashGet(uid2Result, &uidVal, sizeof(uidVal));
    if (pp == NULL || *pp == NULL) {
      code = TSDB_CODE_INVALID_PARA;
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
  if (uid2Result != NULL) {
    void *iter = NULL; int32_t it = 0;
    while ((iter = tSimpleHashIterate(uid2Result, iter, &it)) != NULL) {
      SVTableResolveResult **pp2 = (SVTableResolveResult **)iter;
      if (pp2 && *pp2) streamVTableResolveResultDestroy(*pp2);
    }
    tSimpleHashCleanup(uid2Result);
  }
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
static int32_t streamReadChildTagConstValue(SVnode *pVnode, const SMetaEntry *pChildEntry,
                                            const char *tagColName, int8_t *outType,
                                            int32_t *outLen, char **outData) {
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
    if (strncmp(pSW->pSchema[i].name, tagColName, TSDB_COL_NAME_LEN) == 0) {
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

static int32_t vnodeFillTagValueFromChild(SVnode *pVnode, const SMetaEntry *pChildEntry,
                                          const char *tagColName, SVTableRefResolveRspItem *r) {
  r->terminated = true;
  int32_t code = streamReadChildTagConstValue(pVnode, pChildEntry, tagColName,
                                              &r->tagType, &r->tagLen, &r->tagData);
  if (code == TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST ||
      code == TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST) {
    // per-item soft error: surface via r->code, do not abort the batch.
    r->code = code;
    return 0;
  }
  return code;
}

static int32_t vnodeResolveOneHop(SVnode *pVnode, const SVTableRefResolveItem *q,
                                  SVTableRefResolveRspItem *r) {
  SMetaReader mr   = {0};
  int32_t     code = 0;

  metaReaderDoInit(&mr, pVnode->pMeta, META_READER_LOCK);
  if (metaGetTableEntryByName(&mr, q->refTableName) != 0) {
    r->code = TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST;
    metaReaderClear(&mr);
    return 0;
  }

  bool isVtable = (mr.me.type == TSDB_VIRTUAL_NORMAL_TABLE || mr.me.type == TSDB_VIRTUAL_CHILD_TABLE);

  if (isVtable) {
    SColRefWrapper *pWrap = &mr.me.colRef;
    SColRef        *pArr  = (q->kind == STREAM_VREF_KIND_TAG) ? pWrap->pTagRef : pWrap->pColRef;
    int32_t         nArr  = (q->kind == STREAM_VREF_KIND_TAG) ? pWrap->nTagRefs : pWrap->nCols;

    SColRef *pFound = NULL;
    for (int32_t j = 0; j < nArr && pArr != NULL; ++j) {
      if (strncmp(pArr[j].colName, q->refColName, TSDB_COL_NAME_LEN) == 0) {
        pFound = &pArr[j];
        break;
      }
    }
    if (pFound == NULL) {
      r->code = TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST;
      metaReaderClear(&mr);
      return 0;
    }

    if (pFound->hasRef) {
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

  if (tDeserializeSVTableRefResolveReq((char *)pMsg->pCont + sizeof(SMsgHead),
                                       pMsg->contLen - (int32_t)sizeof(SMsgHead), &req) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _end;
  }

  int32_t n = (req.items != NULL) ? (int32_t)taosArrayGetSize(req.items) : 0;
  rsp.items = taosArrayInit(n, sizeof(SVTableRefResolveRspItem));
  if (rsp.items == NULL) {
    code = terrno;
    goto _end;
  }

  for (int32_t i = 0; i < n; ++i) {
    SVTableRefResolveItem    *q = (SVTableRefResolveItem *)taosArrayGet(req.items, i);
    SVTableRefResolveRspItem  r = {0};
    int32_t                   rc = vnodeResolveOneHop(pVnode, q, &r);
    if (rc != 0) {
      // Hard failure (e.g. OOM): record and continue, do not abort batch.
      r.code = rc;
    }
    if (taosArrayPush(rsp.items, &r) == NULL) {
      taosMemoryFreeClear(r.tagData);
      code = terrno;
      goto _end;
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
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _end;
  }
  pMsg->info.rsp    = pBuf;
  pMsg->info.rspLen = rspLen;

_end:
  tFreeSVTableRefResolveReq(&req);
  tFreeSVTableRefResolveRsp(&rsp);
  return code;
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
  if (ppRes != NULL && *ppRes != NULL) return *ppRes;

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

  if (metaReaderGetTableEntryByUid(&mr, uid) != 0) {
    code = TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST;
    goto _end;
  }
  if (mr.me.type != TSDB_VIRTUAL_NORMAL_TABLE && mr.me.type != TSDB_VIRTUAL_CHILD_TABLE) {
    code = TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST;
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
    if (pFound == NULL) {
      code = TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST;
      goto _end;
    }

    if (!pFound->hasRef) {
      // Constant tag on a virtual child table: read locally, write terminal STagValue.
      // Virtual normal tables have no tag concept, so reject.
      if (mr.me.type != TSDB_VIRTUAL_CHILD_TABLE) {
        code = TSDB_CODE_STREAM_VTB_REF_COL_NOT_EXIST;
        goto _end;
      }
      STagValue *tv = taosMemoryCalloc(1, sizeof(*tv));
      if (tv == NULL) { code = terrno; goto _end; }
      int32_t rc = streamReadChildTagConstValue(pVnode, &mr.me, pFound->colName,
                                                &tv->type, &tv->nLen, &tv->pData);
      if (rc != 0) {
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

  if (tsem_wait(&ctx.ready) != 0) { code = terrno; goto _end; }

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

// Send one TDMT_VND_VTABLE_REF_RESOLVE RPC for a subset of work-items targeting
// the same vgId. indexList[] holds positions inside the parent batch/outRspItems
// arrays so we can reorder responses back to the caller's original layout.
//
// Return value: 0 -> all items in this group filled successfully;
//              != 0 -> RPC failed; whole-group failure handled by the caller
//              (mark every uid in indexList as skipped).
static int32_t streamSendOneVgResolveRpc(SVnode *pVnode, const SEpSet *pEpSet, int32_t vgId,
                                         int64_t ver, SArray *batch, SArray *indexList,
                                         SArray *outRspItems) {
  int32_t              code     = 0;
  SVTableRefResolveReq req      = {0};
  SVTableRefResolveRsp rsp      = {0};
  SRpcMsg              rpcMsg   = {0};
  SRpcMsg              rpcRsp   = {0};
  void                *pReqBuf  = NULL;

  int32_t cnt = (int32_t)taosArrayGetSize(indexList);
  req.ver   = ver;
  req.items = taosArrayInit(cnt, sizeof(SVTableRefResolveItem));
  if (req.items == NULL) { code = terrno; goto _end; }

  for (int32_t i = 0; i < cnt; ++i) {
    int32_t           pos = *(int32_t *)taosArrayGet(indexList, i);
    SResolveWorkItem *w   = taosArrayGet(batch, pos);
    SVTableRefResolveItem it = {0};
    it.kind   = w->kind;
    it.hasRef = true;
    tstrncpy(it.refDbName,    w->refDbName,    TSDB_DB_NAME_LEN);
    tstrncpy(it.refTableName, w->refTableName, TSDB_TABLE_NAME_LEN);
    tstrncpy(it.refColName,   w->refColName,   TSDB_COL_NAME_LEN);
    if (taosArrayPush(req.items, &it) == NULL) { code = terrno; goto _end; }
  }

  int32_t reqLen = tSerializeSVTableRefResolveReq(NULL, 0, &req);
  if (reqLen < 0) { code = terrno; goto _end; }
  // Prepend SMsgHead so dnode dispatcher (vmPutMsgToQueue) can route by vgId.
  int32_t totalLen = reqLen + (int32_t)sizeof(SMsgHead);
  pReqBuf = rpcMallocCont(totalLen);
  if (pReqBuf == NULL) { code = terrno; goto _end; }
  if (tSerializeSVTableRefResolveReq((char *)pReqBuf + sizeof(SMsgHead), reqLen, &req) < 0) {
    code = terrno;
    goto _end;
  }
  ((SMsgHead *)pReqBuf)->vgId    = htonl(vgId);
  ((SMsgHead *)pReqBuf)->contLen = htonl(totalLen);

  rpcMsg.msgType            = TDMT_VND_VTABLE_REF_RESOLVE;
  rpcMsg.pCont              = pReqBuf;
  rpcMsg.contLen            = totalLen;
  rpcMsg.info.ahandle       = (void *)0x9527;
  rpcMsg.info.notFreeAhandle = 1;

  void *clientRpc = pVnode->msgCb.clientRpc;
  if (clientRpc == NULL) { code = TSDB_CODE_INVALID_PARA; goto _end; }

  code = rpcSendRecv(clientRpc, (SEpSet *)pEpSet, &rpcMsg, &rpcRsp);
  pReqBuf = NULL;
  if (code != 0) goto _end;
  if (rpcRsp.code != 0) { code = rpcRsp.code; goto _end; }
  if (rpcRsp.pCont == NULL || rpcRsp.contLen <= 0) { code = TSDB_CODE_INVALID_MSG; goto _end; }

  if (tDeserializeSVTableRefResolveRsp(rpcRsp.pCont, rpcRsp.contLen, &rsp) < 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _end;
  }
  int32_t m = (rsp.items != NULL) ? (int32_t)taosArrayGetSize(rsp.items) : 0;
  if (m != cnt) { code = TSDB_CODE_INVALID_MSG; goto _end; }

  // Move each rsp item to its original position in outRspItems (pre-sized).
  for (int32_t i = 0; i < m; ++i) {
    int32_t pos = *(int32_t *)taosArrayGet(indexList, i);
    SVTableRefResolveRspItem *src = taosArrayGet(rsp.items, i);
    SVTableRefResolveRspItem *dst = taosArrayGet(outRspItems, pos);
    *dst = *src;
    src->tagData = NULL;
    src->tagLen  = 0;
  }

_end:
  if (pReqBuf != NULL) rpcFreeCont(pReqBuf);
  if (rpcRsp.pCont != NULL) rpcFreeCont(rpcRsp.pCont);
  tFreeSVTableRefResolveReq(&req);
  tFreeSVTableRefResolveRsp(&rsp);
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
// function fills them in place. Per-group RPC failure (non-OOM) marks every uid
// in that group as skipped via *pPerVgFail; callers translate that into the
// existing skipped-uid bookkeeping.
//
// Returns 0 on success (per-group failures are reported via pPerVgFail/skipped),
// non-zero only on OOM/structural errors that abort the whole resolution.
static int32_t streamCallResolveBatched(SVnode *pVnode, SStreamVTableInfoCache *pCache,
                                        int64_t ver, SArray *batch, SArray *outRspItems,
                                        SHashObj *skipped) {
  int32_t   code     = 0;
  SHashObj *vg2Idx   = NULL;  // key: int32_t vgId, value: SArray<int32_t>* (positions)
  SHashObj *vg2Ep    = NULL;  // key: int32_t vgId, value: SEpSet

  int32_t n = (int32_t)taosArrayGetSize(batch);
  // Pre-size outRspItems with n zero entries so positional writes are safe.
  for (int32_t i = (int32_t)taosArrayGetSize(outRspItems); i < n; ++i) {
    SVTableRefResolveRspItem zero = {0};
    if (taosArrayPush(outRspItems, &zero) == NULL) { code = terrno; goto _end; }
  }

  vg2Idx = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  vg2Ep  = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (vg2Idx == NULL || vg2Ep == NULL) { code = terrno; goto _end; }

  // pVnode->config.dbname is always "acctId.dbName"; parse acctId once.
  int32_t acctId = 0;
  if (sscanf(pVnode->config.dbname, "%d.", &acctId) != 1) {
    code = TSDB_CODE_INVALID_PARA;
    goto _end;
  }

  // 1. Route each work-item to its target vgId (skip on routing failure).
  for (int32_t i = 0; i < n; ++i) {
    SResolveWorkItem *w = taosArrayGet(batch, i);

    char dbFName[TSDB_DB_FNAME_LEN] = {0};
    (void)tsnprintf(dbFName, sizeof(dbFName), "%d.%s", acctId, w->refDbName);

    SUseDbRsp *pRsp = NULL;
    bool       fromCache = false;
    int32_t    rc = streamGetOrFetchDbVgInfo(pVnode, pCache, dbFName, &pRsp, &fromCache);
    if (rc != 0) {
      if (rc == TSDB_CODE_OUT_OF_MEMORY) { code = rc; goto _end; }
      (void)taosHashPut(skipped, &w->originVtbUid, sizeof(w->originVtbUid),
                        &w->originVtbUid, sizeof(w->originVtbUid));
      continue;
    }

    int32_t vgId  = 0;
    SEpSet  epSet = {0};
    rc = streamRouteTableToVg(pRsp, dbFName, w->refTableName, &vgId, &epSet);
    if (!fromCache && pCache == NULL) {
      tFreeSUsedbRsp(pRsp);
      taosMemoryFree(pRsp);
    }
    if (rc != 0) {
      (void)taosHashPut(skipped, &w->originVtbUid, sizeof(w->originVtbUid),
                        &w->originVtbUid, sizeof(w->originVtbUid));
      continue;
    }

    SArray **ppList = (SArray **)taosHashGet(vg2Idx, &vgId, sizeof(vgId));
    SArray  *pList  = NULL;
    if (ppList == NULL) {
      pList = taosArrayInit(4, sizeof(int32_t));
      if (pList == NULL) { code = terrno; goto _end; }
      if (taosHashPut(vg2Idx, &vgId, sizeof(vgId), &pList, sizeof(pList)) != 0) {
        taosArrayDestroy(pList);
        code = terrno; goto _end;
      }
      if (taosHashPut(vg2Ep, &vgId, sizeof(vgId), &epSet, sizeof(epSet)) != 0) {
        code = terrno; goto _end;
      }
    } else {
      pList = *ppList;
    }
    if (taosArrayPush(pList, &i) == NULL) { code = terrno; goto _end; }
  }

  // 2. Issue one RPC per vg group.
  void *pIter = taosHashIterate(vg2Idx, NULL);
  while (pIter != NULL) {
    SArray  *pList   = *(SArray **)pIter;
    size_t   keyLen  = 0;
    int32_t *pVgKey  = (int32_t *)taosHashGetKey(pIter, &keyLen);
    int32_t  vgId    = *pVgKey;
    SEpSet  *pEpSet  = (SEpSet *)taosHashGet(vg2Ep, &vgId, sizeof(vgId));

    int32_t rc = streamSendOneVgResolveRpc(pVnode, pEpSet, vgId, ver, batch, pList, outRspItems);
    if (rc != 0) {
      if (rc == TSDB_CODE_OUT_OF_MEMORY) {
        taosHashCancelIterate(vg2Idx, pIter);
        code = rc;
        goto _end;
      }
      // Whole-group RPC failure: mark every uid in this group as skipped.
      int32_t cnt = (int32_t)taosArrayGetSize(pList);
      for (int32_t j = 0; j < cnt; ++j) {
        int32_t           pos = *(int32_t *)taosArrayGet(pList, j);
        SResolveWorkItem *w   = taosArrayGet(batch, pos);
        (void)taosHashPut(skipped, &w->originVtbUid, sizeof(w->originVtbUid),
                          &w->originVtbUid, sizeof(w->originVtbUid));
      }
    }
    pIter = taosHashIterate(vg2Idx, pIter);
  }

_end:
  if (vg2Idx != NULL) {
    void *p = taosHashIterate(vg2Idx, NULL);
    while (p != NULL) {
      taosArrayDestroy(*(SArray **)p);
      p = taosHashIterate(vg2Idx, p);
    }
    taosHashCleanup(vg2Idx);
  }
  if (vg2Ep != NULL) taosHashCleanup(vg2Ep);
  return code;
}

// Function A: drive multi-hop chain resolution for a batch of vtable uids on the
// triggering vnode. Cross-vgId version: groups each batch by target vgId, then
// dispatches one TDMT_VND_VTABLE_REF_RESOLVE RPC per group via streamCallResolveBatched.
//   - per-uid local failure (table missing / cid missing / chain too deep) -> uid skipped
//   - per-item failure inside response (r->code != 0) -> whole-uid skipped
//   - per-vg RPC failure -> all uids in that vg group skipped (only OOM bubbles up)
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
  SArray    *batch        = NULL;
  SArray    *rspItems     = NULL;
  SSHashObj *uid2Result   = NULL;
  SHashObj  *skipped      = NULL;
  SArray    *fullUids     = NULL;
  SArray    *pTableListArray = NULL;

  if (pVnode == NULL || ppUid2Result == NULL) return TSDB_CODE_INVALID_PARA;
  *ppUid2Result = NULL;

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
  }

  uid2Result = tSimpleHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  if (uid2Result == NULL) { code = terrno; goto _end; }

  skipped = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (skipped == NULL) { code = terrno; goto _end; }

  workList = taosArrayInit(64, sizeof(SResolveWorkItem));
  if (workList == NULL) { code = terrno; goto _end; }

  // 1. seed work-list
  int32_t nUid = (int32_t)taosArrayGetSize(vtbUids);
  for (int32_t i = 0; i < nUid; ++i) {
    int64_t uid = *(int64_t *)taosArrayGet(vtbUids, i);
    int32_t rc  = streamPushInitialWorkItemsForUid(pVnode, uid, virtColCids, virtTagCids, workList, uid2Result);
    if (rc != 0) {
      if (rc == TSDB_CODE_OUT_OF_MEMORY) { code = rc; goto _end; }
      (void)taosHashPut(skipped, &uid, sizeof(uid), &uid, sizeof(uid));
    }
  }

  // 2. main hop loop
  for (int32_t hop = 0; hop < STREAM_VTB_MAX_HOPS; ++hop) {
    int32_t cur = (int32_t)taosArrayGetSize(workList);
    if (cur == 0) break;

    batch = taosArrayInit(cur, sizeof(SResolveWorkItem));
    if (batch == NULL) { code = terrno; goto _end; }
    for (int32_t i = 0; i < cur; ++i) {
      SResolveWorkItem *w = taosArrayGet(workList, i);
      if (taosHashGet(skipped, &w->originVtbUid, sizeof(w->originVtbUid)) != NULL) continue;
      if (taosArrayPush(batch, w) == NULL) { code = terrno; goto _end; }
    }
    if (taosArrayGetSize(batch) == 0) {
      taosArrayDestroy(batch); batch = NULL;
      taosArrayClear(workList);
      break;
    }

    rspItems = taosArrayInit(taosArrayGetSize(batch), sizeof(SVTableRefResolveRspItem));
    if (rspItems == NULL) { code = terrno; goto _end; }

    int32_t rc = streamCallResolveBatched(pVnode, pCache, ver, batch, rspItems, skipped);
    if (rc != 0) {
      // Only OOM/structural errors bubble up; per-vg RPC failures already mark
      // their uids in `skipped` inside streamCallResolveBatched.
      code = rc;
      goto _end;
    }

    nextWorkList = taosArrayInit(taosArrayGetSize(batch), sizeof(SResolveWorkItem));
    if (nextWorkList == NULL) { code = terrno; goto _end; }

    int32_t bn = (int32_t)taosArrayGetSize(batch);
    for (int32_t i = 0; i < bn; ++i) {
      SResolveWorkItem         *w = taosArrayGet(batch, i);
      SVTableRefResolveRspItem *r = taosArrayGet(rspItems, i);

      if (r->code != 0) {
        (void)taosHashPut(skipped, &w->originVtbUid, sizeof(w->originVtbUid), &w->originVtbUid, sizeof(w->originVtbUid));
        taosMemoryFreeClear(r->tagData);
        continue;
      }
      // already in skip list (race from earlier failure on same uid)
      if (taosHashGet(skipped, &w->originVtbUid, sizeof(w->originVtbUid)) != NULL) {
        taosMemoryFreeClear(r->tagData);
        continue;
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
        taosMemoryFreeClear(r->tagData);
      }
    }

    taosArrayDestroy(rspItems); rspItems = NULL;
    taosArrayDestroy(batch);    batch    = NULL;
    taosArrayDestroy(workList);
    workList     = nextWorkList;
    nextWorkList = NULL;
  }

  // 3. hop overflow: any leftover work-items belong to chains that exceeded MAX_HOPS
  if (workList != NULL) {
    int32_t leftover = (int32_t)taosArrayGetSize(workList);
    for (int32_t i = 0; i < leftover; ++i) {
      SResolveWorkItem *w = taosArrayGet(workList, i);
      (void)taosHashPut(skipped, &w->originVtbUid, sizeof(w->originVtbUid), &w->originVtbUid, sizeof(w->originVtbUid));
    }
  }

  // 4. drop partial entries for any skipped uid
  void *pIter = taosHashIterate(skipped, NULL);
  while (pIter != NULL) {
    int64_t                uid    = *(int64_t *)pIter;
    SVTableResolveResult **ppRes  = (SVTableResolveResult **)tSimpleHashGet(uid2Result, &uid, sizeof(uid));
    if (ppRes != NULL && *ppRes != NULL) {
      streamVTableResolveResultDestroy(*ppRes);
      (void)tSimpleHashRemove(uid2Result, &uid, sizeof(uid));
    }
    pIter = taosHashIterate(skipped, pIter);
  }

  *ppUid2Result = uid2Result;
  uid2Result    = NULL;

_end:
  if (fullUids        != NULL) taosArrayDestroy(fullUids);
  if (pTableListArray != NULL) taosArrayDestroyP(pTableListArray, taosMemFree);
  if (workList     != NULL) taosArrayDestroy(workList);
  if (nextWorkList != NULL) taosArrayDestroy(nextWorkList);
  if (batch        != NULL) taosArrayDestroy(batch);
  if (rspItems     != NULL) {
    int32_t m = (int32_t)taosArrayGetSize(rspItems);
    for (int32_t i = 0; i < m; ++i) {
      SVTableRefResolveRspItem *r = taosArrayGet(rspItems, i);
      taosMemoryFreeClear(r->tagData);
    }
    taosArrayDestroy(rspItems);
  }
  if (skipped != NULL) taosHashCleanup(skipped);
  if (uid2Result != NULL) {
    void *p = NULL; int32_t it = 0;
    while ((p = tSimpleHashIterate(uid2Result, p, &it)) != NULL) {
      SVTableResolveResult **pp = (SVTableResolveResult **)p;
      if (pp != NULL && *pp != NULL) streamVTableResolveResultDestroy(*pp);
    }
    tSimpleHashCleanup(uid2Result);
  }
  return code;
}
