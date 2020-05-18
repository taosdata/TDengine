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

#include "tscUtil.h"
#include "hash.h"
#include "os.h"
#include "qast.h"
#include "taosmsg.h"
#include "tcache.h"
#include "tkey.h"
#include "tmd5.h"
#include "tscProfile.h"
#include "tscSecondaryMerge.h"
#include "tscSubquery.h"
#include "tschemautil.h"
#include "tsclient.h"
#include "ttimer.h"
#include "ttokendef.h"
#include "tscLog.h"

static void freeQueryInfoImpl(SQueryInfo* pQueryInfo);
static void clearAllTableMetaInfo(SQueryInfo* pQueryInfo, const char* address, bool removeFromCache);
  
  SCond* tsGetSTableQueryCond(STagCond* pTagCond, uint64_t uid) {
  if (pTagCond->pCond == NULL) {
    return NULL;
  }
  
  size_t size = taosArrayGetSize(pTagCond->pCond);
  for (int32_t i = 0; i < size; ++i) {
    SCond* pCond = taosArrayGet(pTagCond->pCond, i);
    
    if (uid == pCond->uid) {
      return pCond;
    }
  }

  return NULL;
}

void tsSetSTableQueryCond(STagCond* pTagCond, uint64_t uid, SBufferWriter* bw) {
  if (tbufTell(bw) == 0) {
    return;
  }
  
  SCond cond = {
    .uid = uid,
    .len = tbufTell(bw),
    .cond = NULL,
  };
  
  cond.cond = tbufGetData(bw, true);
  
  if (pTagCond->pCond == NULL) {
    pTagCond->pCond = taosArrayInit(3, sizeof(SCond));
  }
  
  taosArrayPush(pTagCond->pCond, &cond);
}

bool tscQueryOnSTable(SSqlCmd* pCmd) {
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);

  return ((pQueryInfo->type & TSDB_QUERY_TYPE_STABLE_QUERY) == TSDB_QUERY_TYPE_STABLE_QUERY) &&
         (pCmd->msgType == TSDB_MSG_TYPE_QUERY);
}

bool tscQueryTags(SQueryInfo* pQueryInfo) {
  for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
    int32_t functId = tscSqlExprGet(pQueryInfo, i)->functionId;
    
    if (functId != TSDB_FUNC_TAGPRJ && functId != TSDB_FUNC_TID_TAG) {
      return false;
    }
  }

  return true;
}

bool tscIsSelectivityWithTagQuery(SSqlCmd* pCmd) {
  bool    hasTags = false;
  int32_t numOfSelectivity = 0;

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);

  for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
    int32_t functId = tscSqlExprGet(pQueryInfo, i)->functionId;
    if (functId == TSDB_FUNC_TAG_DUMMY) {
      hasTags = true;
      continue;
    }

    if ((aAggs[functId].nStatus & TSDB_FUNCSTATE_SELECTIVITY) != 0) {
      numOfSelectivity++;
    }
  }

  if (numOfSelectivity > 0 && hasTags) {
    return true;
  }

  return false;
}

void tscGetDBInfoFromMeterId(char* tableId, char* db) {
  char* st = strstr(tableId, TS_PATH_DELIMITER);
  if (st != NULL) {
    char* end = strstr(st + 1, TS_PATH_DELIMITER);
    if (end != NULL) {
      memcpy(db, tableId, (end - tableId));
      db[end - tableId] = 0;
      return;
    }
  }

  db[0] = 0;
}

//STableIdInfo* tscGetMeterSidInfo(SVnodeSidList* pSidList, int32_t idx) {
//  if (pSidList == NULL) {
//    tscError("illegal sidlist");
//    return 0;
//  }
//
//  if (idx < 0 || idx >= pSidList->numOfSids) {
//    int32_t sidRange = (pSidList->numOfSids > 0) ? (pSidList->numOfSids - 1) : 0;
//
//    tscError("illegal sidIdx:%d, reset to 0, sidIdx range:%d-%d", idx, 0, sidRange);
//    idx = 0;
//  }
//
//  assert(pSidList->pSidExtInfoList[idx] >= 0);
//
//  return (STableIdInfo*)(pSidList->pSidExtInfoList[idx] + (char*)pSidList);
//}

bool tscIsTwoStageSTableQuery(SQueryInfo* pQueryInfo, int32_t tableIndex) {
  if (pQueryInfo == NULL) {
    return false;
  }

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tableIndex);
  if (pTableMetaInfo == NULL) {
    return false;
  }
  
  // for select query super table, the super table vgroup list can not be null in any cases.
  if (pQueryInfo->command == TSDB_SQL_SELECT && UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    assert(pTableMetaInfo->vgroupList != NULL);
  }
  
  if ((pQueryInfo->type & TSDB_QUERY_TYPE_FREE_RESOURCE) == TSDB_QUERY_TYPE_FREE_RESOURCE) {
    return false;
  }

  // for ordered projection query, iterate all qualified vnodes sequentially
  if (tscNonOrderedProjectionQueryOnSTable(pQueryInfo, tableIndex)) {
    return false;
  }

  if (((pQueryInfo->type & TSDB_QUERY_TYPE_STABLE_SUBQUERY) != TSDB_QUERY_TYPE_STABLE_SUBQUERY) &&
      pQueryInfo->command == TSDB_SQL_SELECT) {
    return UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo);
  }

  return false;
}

bool tscIsProjectionQueryOnSTable(SQueryInfo* pQueryInfo, int32_t tableIndex) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tableIndex);
  
  /*
   * In following cases, return false for non ordered project query on super table
   * 1. failed to get metermeta from server; 2. not a super table; 3. limitation is 0;
   * 4. show queries, instead of a select query
   */
  size_t numOfExprs = tscSqlExprNumOfExprs(pQueryInfo);
  if (pTableMetaInfo == NULL || !UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo) ||
      pQueryInfo->command == TSDB_SQL_RETRIEVE_EMPTY_RESULT || numOfExprs == 0) {
    return false;
  }
  
  // only query on tag, a project query
  if (tscQueryTags(pQueryInfo)) {
    return true;
  }
  
  for (int32_t i = 0; i < numOfExprs; ++i) {
    int32_t functionId = tscSqlExprGet(pQueryInfo, i)->functionId;
    if (functionId != TSDB_FUNC_PRJ && functionId != TSDB_FUNC_TAGPRJ && functionId != TSDB_FUNC_TAG &&
        functionId != TSDB_FUNC_TS && functionId != TSDB_FUNC_ARITHM) {
      return false;
    }
  }
  
  return true;
}

bool tscNonOrderedProjectionQueryOnSTable(SQueryInfo* pQueryInfo, int32_t tableIndex) {
  if (!tscIsProjectionQueryOnSTable(pQueryInfo, tableIndex)) {
    return false;
  }
  
  // order by columnIndex exists, not a non-ordered projection query
  return pQueryInfo->order.orderColId < 0;
}

bool tscOrderedProjectionQueryOnSTable(SQueryInfo* pQueryInfo, int32_t tableIndex) {
  if (!tscIsProjectionQueryOnSTable(pQueryInfo, tableIndex)) {
    return false;
  }
  
  // order by columnIndex exists, a non-ordered projection query
  return pQueryInfo->order.orderColId >= 0;
}

bool tscProjectionQueryOnTable(SQueryInfo* pQueryInfo) {
  for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
    int32_t functionId = tscSqlExprGet(pQueryInfo, i)->functionId;
    if (functionId != TSDB_FUNC_PRJ && functionId != TSDB_FUNC_TS) {
      return false;
    }
  }

  return true;
}

bool tscIsPointInterpQuery(SQueryInfo* pQueryInfo) {
  size_t size = tscSqlExprNumOfExprs(pQueryInfo);
  
  for (int32_t i = 0; i < size; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);
    if (pExpr == NULL) {
      return false;
    }

    int32_t functionId = pExpr->functionId;
    if (functionId == TSDB_FUNC_TAG) {
      continue;
    }

    if (functionId != TSDB_FUNC_INTERP) {
      return false;
    }
  }
  return true;
}

bool tscIsTWAQuery(SQueryInfo* pQueryInfo) {
  size_t numOfExprs = tscSqlExprNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < numOfExprs; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);
    if (pExpr == NULL) {
      continue;
    }

    int32_t functionId = pExpr->functionId;
    if (functionId == TSDB_FUNC_TWA) {
      return true;
    }
  }

  return false;
}

void tscClearInterpInfo(SQueryInfo* pQueryInfo) {
  if (!tscIsPointInterpQuery(pQueryInfo)) {
    return;
  }

  pQueryInfo->interpoType = TSDB_INTERPO_NONE;
  tfree(pQueryInfo->defaultVal);
}

int32_t tscCreateResPointerInfo(SSqlRes* pRes, SQueryInfo* pQueryInfo) {
  if (pRes->tsrow == NULL) {
    int32_t numOfOutput = pQueryInfo->fieldsInfo.numOfOutput;
    pRes->numOfCols = numOfOutput;
  
    pRes->tsrow  = calloc(numOfOutput, POINTER_BYTES);
    pRes->length = calloc(numOfOutput, sizeof(int32_t));  // todo refactor
    pRes->buffer = calloc(numOfOutput, POINTER_BYTES);
  
    // not enough memory
    if (pRes->tsrow == NULL || (pRes->buffer == NULL && pRes->numOfCols > 0)) {
      tfree(pRes->tsrow);
      tfree(pRes->buffer);
      tfree(pRes->length);
    
      pRes->code = TSDB_CODE_CLI_OUT_OF_MEMORY;
      return pRes->code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

void tscDestroyResPointerInfo(SSqlRes* pRes) {
  if (pRes->buffer != NULL) { // free all buffers containing the multibyte string
    for (int i = 0; i < pRes->numOfCols; i++) {
      tfree(pRes->buffer[i]);
    }
    
    pRes->numOfCols = 0;
  }
  
  tfree(pRes->pRsp);
  tfree(pRes->tsrow);
  tfree(pRes->length);
  
  tfree(pRes->pGroupRec);
  tfree(pRes->pColumnIndex);
  tfree(pRes->buffer);
  
  if (pRes->pArithSup != NULL) {
    tfree(pRes->pArithSup->data);
    tfree(pRes->pArithSup);
  }
  
  pRes->data = NULL;  // pRes->data points to the buffer of pRsp, no need to free
}

static void tscFreeQueryInfo(SSqlCmd* pCmd) {
  if (pCmd == NULL || pCmd->numOfClause == 0) {
    return;
  }
  
  for (int32_t i = 0; i < pCmd->numOfClause; ++i) {
    char* addr = (char*)pCmd - offsetof(SSqlObj, cmd);
    SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, i);
    
    freeQueryInfoImpl(pQueryInfo);
    clearAllTableMetaInfo(pQueryInfo, (const char*)addr, false);
    tfree(pQueryInfo);
  }
  
  pCmd->numOfClause = 0;
  tfree(pCmd->pQueryInfo);
}

void tscResetSqlCmdObj(SSqlCmd* pCmd) {
  pCmd->command   = 0;
  pCmd->numOfCols = 0;
  pCmd->count     = 0;
  pCmd->curSql    = NULL;
  pCmd->msgType   = 0;
  pCmd->parseFinished = 0;
  pCmd->autoCreated = 0;
  
  taosHashCleanup(pCmd->pTableList);
  pCmd->pTableList = NULL;
  
  pCmd->pDataBlocks = tscDestroyBlockArrayList(pCmd->pDataBlocks);
  
  tscFreeQueryInfo(pCmd);
}

void tscFreeSqlResult(SSqlObj* pSql) {
  tscDestroyLocalReducer(pSql);
  
  SSqlRes* pRes = &pSql->res;
  tscDestroyResPointerInfo(pRes);
  
  memset(&pSql->res, 0, sizeof(SSqlRes));
}

void tscPartiallyFreeSqlObj(SSqlObj* pSql) {
  if (pSql == NULL || pSql->signature != pSql) {
    return;
  }

  SSqlCmd* pCmd = &pSql->cmd;
  STscObj* pObj = pSql->pTscObj;

  int32_t cmd = pCmd->command;
  if (cmd < TSDB_SQL_INSERT || cmd == TSDB_SQL_RETRIEVE_LOCALMERGE || cmd == TSDB_SQL_RETRIEVE_EMPTY_RESULT ||
      cmd == TSDB_SQL_TABLE_JOIN_RETRIEVE) {
    tscRemoveFromSqlList(pSql);
  }
  
  // pSql->sqlstr will be used by tscBuildQueryStreamDesc
  if (pObj->signature == pObj) {
    pthread_mutex_lock(&pObj->mutex);
    tfree(pSql->sqlstr);
    pthread_mutex_unlock(&pObj->mutex);
  }
  
  tscFreeSqlResult(pSql);
  
  tfree(pSql->pSubs);
  pSql->freed = 0;
  pSql->numOfSubs = 0;
  
  tscResetSqlCmdObj(pCmd);
  
  tscTrace("%p partially free sqlObj completed", pSql);
}

void tscFreeSqlObj(SSqlObj* pSql) {
  if (pSql == NULL || pSql->signature != pSql) return;

  tscTrace("%p start to free sql object", pSql);
  tscPartiallyFreeSqlObj(pSql);

  pSql->signature = NULL;
  pSql->fp = NULL;
  
  SSqlCmd* pCmd = &pSql->cmd;

  memset(pCmd->payload, 0, (size_t)pCmd->allocSize);
  tfree(pCmd->payload);

  pCmd->allocSize = 0;
  free(pSql);
}

void tscDestroyDataBlock(STableDataBlocks* pDataBlock) {
  if (pDataBlock == NULL) {
    return;
  }

  tfree(pDataBlock->pData);
  tfree(pDataBlock->params);

  // free the refcount for metermeta
  taosCacheRelease(tscCacheHandle, (void**)&(pDataBlock->pTableMeta), false);
  tfree(pDataBlock);
}

SParamInfo* tscAddParamToDataBlock(STableDataBlocks* pDataBlock, char type, uint8_t timePrec, short bytes,
                                   uint32_t offset) {
  uint32_t needed = pDataBlock->numOfParams + 1;
  if (needed > pDataBlock->numOfAllocedParams) {
    needed *= 2;
    void* tmp = realloc(pDataBlock->params, needed * sizeof(SParamInfo));
    if (tmp == NULL) {
      return NULL;
    }
    pDataBlock->params = (SParamInfo*)tmp;
    pDataBlock->numOfAllocedParams = needed;
  }

  SParamInfo* param = pDataBlock->params + pDataBlock->numOfParams;
  param->idx = -1;
  param->type = type;
  param->timePrec = timePrec;
  param->bytes = bytes;
  param->offset = offset;

  ++pDataBlock->numOfParams;
  return param;
}

SDataBlockList* tscCreateBlockArrayList() {
  const int32_t DEFAULT_INITIAL_NUM_OF_BLOCK = 16;

  SDataBlockList* pDataBlockArrayList = calloc(1, sizeof(SDataBlockList));
  if (pDataBlockArrayList == NULL) {
    return NULL;
  }
  
  pDataBlockArrayList->nAlloc = DEFAULT_INITIAL_NUM_OF_BLOCK;
  pDataBlockArrayList->pData = calloc(1, POINTER_BYTES * pDataBlockArrayList->nAlloc);
  if (pDataBlockArrayList->pData == NULL) {
    free(pDataBlockArrayList);
    return NULL;
  }

  return pDataBlockArrayList;
}

void tscAppendDataBlock(SDataBlockList* pList, STableDataBlocks* pBlocks) {
  if (pList->nSize >= pList->nAlloc) {
    pList->nAlloc = (pList->nAlloc) << 1U;
    pList->pData = realloc(pList->pData, POINTER_BYTES * (size_t)pList->nAlloc);

    // reset allocated memory
    memset(pList->pData + pList->nSize, 0, POINTER_BYTES * (pList->nAlloc - pList->nSize));
  }

  pList->pData[pList->nSize++] = pBlocks;
}

void* tscDestroyBlockArrayList(SDataBlockList* pList) {
  if (pList == NULL) {
    return NULL;
  }

  for (int32_t i = 0; i < pList->nSize; i++) {
    tscDestroyDataBlock(pList->pData[i]);
  }

  tfree(pList->pData);
  tfree(pList);

  return NULL;
}

int32_t tscCopyDataBlockToPayload(SSqlObj* pSql, STableDataBlocks* pDataBlock) {
  SSqlCmd* pCmd = &pSql->cmd;
  assert(pDataBlock->pTableMeta != NULL);

  pCmd->numOfTablesInSubmit = pDataBlock->numOfTables;

  assert(pCmd->numOfClause == 1);
  STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);

  // set the correct table meta object, the table meta has been locked in pDataBlocks, so it must be in the cache
  if (pTableMetaInfo->pTableMeta != pDataBlock->pTableMeta) {
    strcpy(pTableMetaInfo->name, pDataBlock->tableId);
    taosCacheRelease(tscCacheHandle, (void**)&(pTableMetaInfo->pTableMeta), false);

    pTableMetaInfo->pTableMeta = taosCacheTransfer(tscCacheHandle, (void**)&pDataBlock->pTableMeta);
  } else {
    assert(strncmp(pTableMetaInfo->name, pDataBlock->tableId, tListLen(pDataBlock->tableId)) == 0);
  }

  /*
   * the submit message consists of : [RPC header|message body|digest]
   * the dataBlock only includes the RPC Header buffer and actual submit message body, space for digest needs
   * additional space.
   */
  int ret = tscAllocPayload(pCmd, pDataBlock->nAllocSize + 100);
  if (TSDB_CODE_SUCCESS != ret) {
    return ret;
  }

  memcpy(pCmd->payload, pDataBlock->pData, pDataBlock->nAllocSize);

  /*
   * the payloadLen should be actual message body size
   * the old value of payloadLen is the allocated payload size
   */
  pCmd->payloadLen = pDataBlock->nAllocSize - tsRpcHeadSize;

  assert(pCmd->allocSize >= pCmd->payloadLen + tsRpcHeadSize + 100 && pCmd->payloadLen > 0);
  return TSDB_CODE_SUCCESS;
}

void tscFreeUnusedDataBlocks(SDataBlockList* pList) {
  /* release additional memory consumption */
  for (int32_t i = 0; i < pList->nSize; ++i) {
    STableDataBlocks* pDataBlock = pList->pData[i];
    pDataBlock->pData = realloc(pDataBlock->pData, pDataBlock->size);
    pDataBlock->nAllocSize = (uint32_t)pDataBlock->size;
  }
}

/**
 * create the in-memory buffer for each table to keep the submitted data block
 * @param initialSize
 * @param rowSize
 * @param startOffset
 * @param name
 * @param dataBlocks
 * @return
 */
int32_t tscCreateDataBlock(size_t initialSize, int32_t rowSize, int32_t startOffset, const char* name,
                           STableMeta* pTableMeta, STableDataBlocks** dataBlocks) {
  STableDataBlocks* dataBuf = (STableDataBlocks*)calloc(1, sizeof(STableDataBlocks));
  if (dataBuf == NULL) {
    tscError("failed to allocated memory, reason:%s", strerror(errno));
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  dataBuf->nAllocSize = (uint32_t)initialSize;
  dataBuf->headerSize = startOffset; // the header size will always be the startOffset value, reserved for the subumit block header
  if (dataBuf->nAllocSize <= dataBuf->headerSize) {
    dataBuf->nAllocSize = dataBuf->headerSize*2;
  }
  
  dataBuf->pData = calloc(1, dataBuf->nAllocSize);
  dataBuf->ordered = true;
  dataBuf->prevTS = INT64_MIN;

  dataBuf->rowSize = rowSize;
  dataBuf->size = startOffset;
  dataBuf->tsSource = -1;

  strncpy(dataBuf->tableId, name, TSDB_TABLE_ID_LEN);

  /*
   * The table meta may be released since the table meta cache are completed clean by other thread
   * due to operation such as drop database. So here we add the reference count directly instead of invoke
   * taosGetDataFromCache, which may return NULL value.
   */
  dataBuf->pTableMeta = taosCacheAcquireByData(tscCacheHandle, pTableMeta);
  assert(initialSize > 0 && pTableMeta != NULL && dataBuf->pTableMeta != NULL);

  *dataBlocks = dataBuf;
  return TSDB_CODE_SUCCESS;
}

int32_t tscGetDataBlockFromList(void* pHashList, SDataBlockList* pDataBlockList, int64_t id, int32_t size,
                                int32_t startOffset, int32_t rowSize, const char* tableId, STableMeta* pTableMeta,
                                STableDataBlocks** dataBlocks) {
  *dataBlocks = NULL;

  STableDataBlocks** t1 = (STableDataBlocks**)taosHashGet(pHashList, (const char*)&id, sizeof(id));
  if (t1 != NULL) {
    *dataBlocks = *t1;
  }

  if (*dataBlocks == NULL) {
    int32_t ret = tscCreateDataBlock((size_t)size, rowSize, startOffset, tableId, pTableMeta, dataBlocks);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    taosHashPut(pHashList, (const char*)&id, sizeof(int64_t), (char*)dataBlocks, POINTER_BYTES);
    tscAppendDataBlock(pDataBlockList, *dataBlocks);
  }

  return TSDB_CODE_SUCCESS;
}

static int trimDataBlock(void* pDataBlock, STableDataBlocks* pTableDataBlock) {
  // TODO: optimize this function, handle the case while binary is not presented
  int len = 0;

  STableMeta*   pTableMeta = pTableDataBlock->pTableMeta;
  STableComInfo tinfo = tscGetTableInfo(pTableMeta);
  SSchema*      pSchema = tscGetTableSchema(pTableMeta);

  SSubmitBlk* pBlock = pDataBlock;
  memcpy(pDataBlock, pTableDataBlock->pData, sizeof(SSubmitBlk));
  pDataBlock += sizeof(SSubmitBlk);

  int32_t flen = 0;  // original total length of row
  for (int32_t i = 0; i < tinfo.numOfColumns; ++i) {
    flen += TYPE_BYTES[pSchema[i].type];
  }

  char* p = pTableDataBlock->pData + sizeof(SSubmitBlk);
  pBlock->len = 0;
  int32_t numOfRows = htons(pBlock->numOfRows);
  
  for (int32_t i = 0; i < numOfRows; ++i) {
    SDataRow trow = (SDataRow)pDataBlock;
    dataRowSetLen(trow, TD_DATA_ROW_HEAD_SIZE + flen);

    int toffset = 0;
    for (int32_t j = 0; j < tinfo.numOfColumns; j++) {
      tdAppendColVal(trow, p, pSchema[j].type, pSchema[j].bytes, toffset);
      toffset += TYPE_BYTES[pSchema[j].type];
      p += pSchema[j].bytes;
    }

    // p += pTableDataBlock->rowSize;
    pDataBlock += dataRowLen(trow);
    pBlock->len += dataRowLen(trow);
  }

  len = pBlock->len;
  pBlock->len = htonl(pBlock->len);
  return len;
}

int32_t tscMergeTableDataBlocks(SSqlObj* pSql, SDataBlockList* pTableDataBlockList) {
  SSqlCmd* pCmd = &pSql->cmd;

  void* pVnodeDataBlockHashList = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false);
  SDataBlockList* pVnodeDataBlockList = tscCreateBlockArrayList();

  for (int32_t i = 0; i < pTableDataBlockList->nSize; ++i) {
    STableDataBlocks* pOneTableBlock = pTableDataBlockList->pData[i];

    STableDataBlocks* dataBuf = NULL;
    
    int32_t ret =
        tscGetDataBlockFromList(pVnodeDataBlockHashList, pVnodeDataBlockList, pOneTableBlock->vgId, TSDB_PAYLOAD_SIZE,
                                tsInsertHeadSize, 0, pOneTableBlock->tableId, pOneTableBlock->pTableMeta, &dataBuf);
    if (ret != TSDB_CODE_SUCCESS) {
      tscError("%p failed to prepare the data block buffer for merging table data, code:%d", pSql, ret);
      taosHashCleanup(pVnodeDataBlockHashList);
      tscDestroyBlockArrayList(pVnodeDataBlockList);
      return ret;
    }

    int64_t destSize = dataBuf->size + pOneTableBlock->size + pOneTableBlock->size*sizeof(int32_t)*2;
    if (dataBuf->nAllocSize < destSize) {
      while (dataBuf->nAllocSize < destSize) {
        dataBuf->nAllocSize = dataBuf->nAllocSize * 1.5;
      }

      char* tmp = realloc(dataBuf->pData, dataBuf->nAllocSize);
      if (tmp != NULL) {
        dataBuf->pData = tmp;
        memset(dataBuf->pData + dataBuf->size, 0, dataBuf->nAllocSize - dataBuf->size);
      } else {  // failed to allocate memory, free already allocated memory and return error code
        tscError("%p failed to allocate memory for merging submit block, size:%d", pSql, dataBuf->nAllocSize);

        taosHashCleanup(pVnodeDataBlockHashList);
        tscDestroyBlockArrayList(pVnodeDataBlockList);
        tfree(dataBuf->pData);

        return TSDB_CODE_CLI_OUT_OF_MEMORY;
      }
    }

    SSubmitBlk* pBlocks = (SSubmitBlk*) pOneTableBlock->pData;
    tscSortRemoveDataBlockDupRows(pOneTableBlock);

    char* e = (char*)pBlocks->data + pOneTableBlock->rowSize*(pBlocks->numOfRows-1);
    
    tscTrace("%p tableId:%s, sid:%d rows:%d sversion:%d skey:%" PRId64 ", ekey:%" PRId64, pSql, pOneTableBlock->tableId,
        pBlocks->tid, pBlocks->numOfRows, pBlocks->sversion, GET_INT64_VAL(pBlocks->data), GET_INT64_VAL(e));

    int32_t len = pBlocks->numOfRows * (pOneTableBlock->rowSize + sizeof(int32_t) * 2);
    
    pBlocks->tid = htonl(pBlocks->tid);
    pBlocks->uid = htobe64(pBlocks->uid);
    pBlocks->sversion = htonl(pBlocks->sversion);
    pBlocks->numOfRows = htons(pBlocks->numOfRows);
    
    pBlocks->len = htonl(len);
    
    // erase the empty space reserved for binary data
    len = trimDataBlock(dataBuf->pData + dataBuf->size, pOneTableBlock);
    dataBuf->size += (len + sizeof(SSubmitBlk));
    dataBuf->numOfTables += 1;
  }

  tscDestroyBlockArrayList(pTableDataBlockList);

  // free the table data blocks;
  pCmd->pDataBlocks = pVnodeDataBlockList;

  tscFreeUnusedDataBlocks(pCmd->pDataBlocks);
  taosHashCleanup(pVnodeDataBlockHashList);

  return TSDB_CODE_SUCCESS;
}

void tscCloseTscObj(STscObj* pObj) {
  assert(pObj != NULL);
  
  pObj->signature = NULL;
  SSqlObj* pSql = pObj->pSql;
  
  if (pSql) {
    terrno = pSql->res.code;
    sem_destroy(&pSql->rspSem);
  }
  
  taosTmrStopA(&(pObj->pTimer));
  tscFreeSqlObj(pSql);

  if (pSql) {
    sem_destroy(&pSql->rspSem);
  }
  
  pthread_mutex_destroy(&pObj->mutex);
  
  if (pObj->pDnodeConn != NULL) {
    rpcClose(pObj->pDnodeConn);
  }
  
  tscTrace("%p DB connection is closed, dnodeConn:%p", pObj, pObj->pDnodeConn);
  tfree(pObj);
}

bool tscIsInsertData(char* sqlstr) {
  int32_t index = 0;

  do {
    SSQLToken t0 = tStrGetToken(sqlstr, &index, false, 0, NULL);
    if (t0.type != TK_LP) {
      return t0.type == TK_INSERT || t0.type == TK_IMPORT;
    }
  } while (1);
}

int tscAllocPayload(SSqlCmd* pCmd, int size) {
  assert(size > 0);

  if (pCmd->payload == NULL) {
    assert(pCmd->allocSize == 0);

    pCmd->payload = (char*)calloc(1, size);
    if (pCmd->payload == NULL) return TSDB_CODE_CLI_OUT_OF_MEMORY;
    pCmd->allocSize = size;
  } else {
    if (pCmd->allocSize < size) {
      char* b = realloc(pCmd->payload, size);
      if (b == NULL) return TSDB_CODE_CLI_OUT_OF_MEMORY;
      pCmd->payload = b;
      pCmd->allocSize = size;
    }
    
    memset(pCmd->payload, 0, pCmd->allocSize);
  }

  assert(pCmd->allocSize >= size);
  return TSDB_CODE_SUCCESS;
}

TAOS_FIELD tscCreateField(int8_t type, const char* name, int16_t bytes) {
  TAOS_FIELD f = { .type = type, .bytes = bytes, };
  strncpy(f.name, name, TSDB_COL_NAME_LEN);
  return f;
}

SFieldSupInfo* tscFieldInfoAppend(SFieldInfo* pFieldInfo, TAOS_FIELD* pField) {
  assert(pFieldInfo != NULL);
  taosArrayPush(pFieldInfo->pFields, pField);
  pFieldInfo->numOfOutput++;
  
  struct SFieldSupInfo info = {
    .pSqlExpr = NULL,
    .pArithExprInfo = NULL,
    .visible = true,
  };
  
  return taosArrayPush(pFieldInfo->pSupportInfo, &info);
}

SFieldSupInfo* tscFieldInfoGetSupp(SFieldInfo* pFieldInfo, int32_t index) {
  return taosArrayGet(pFieldInfo->pSupportInfo, index);
}

SFieldSupInfo* tscFieldInfoInsert(SFieldInfo* pFieldInfo, int32_t index, TAOS_FIELD* field) {
  taosArrayInsert(pFieldInfo->pFields, index, field);
  pFieldInfo->numOfOutput++;
  
  struct SFieldSupInfo info = {
      .pSqlExpr = NULL,
      .pArithExprInfo = NULL,
      .visible = true,
  };
  
  return taosArrayInsert(pFieldInfo->pSupportInfo, index, &info);
}

void tscFieldInfoUpdateOffset(SQueryInfo* pQueryInfo) {
  size_t numOfExprs = tscSqlExprNumOfExprs(pQueryInfo);
  
  SSqlExpr* pExpr = taosArrayGetP(pQueryInfo->exprList, 0);
  pExpr->offset = 0;
  
  for (int32_t i = 1; i < numOfExprs; ++i) {
    SSqlExpr* prev = taosArrayGetP(pQueryInfo->exprList, i - 1);
    SSqlExpr* p = taosArrayGetP(pQueryInfo->exprList, i);
  
    p->offset = prev->offset + prev->resBytes;
  }
}

void tscFieldInfoUpdateOffsetForInterResult(SQueryInfo* pQueryInfo) {
  if (tscSqlExprNumOfExprs(pQueryInfo) == 0) {
    return;
  }
  
  SSqlExpr* pExpr = taosArrayGetP(pQueryInfo->exprList, 0);
  pExpr->offset = 0;
  
  size_t numOfExprs = tscSqlExprNumOfExprs(pQueryInfo);
  for (int32_t i = 1; i < numOfExprs; ++i) {
    SSqlExpr* prev = taosArrayGetP(pQueryInfo->exprList, i - 1);
    SSqlExpr* p = taosArrayGetP(pQueryInfo->exprList, i);
    
    p->offset = prev->offset + prev->resBytes;
  }
}

void tscFieldInfoCopy(SFieldInfo* dst, const SFieldInfo* src) {
  dst->numOfOutput = src->numOfOutput;

  if (dst->pFields == NULL) {
    dst->pFields = taosArrayClone(src->pFields);
  } else {
    taosArrayCopy(dst->pFields, src->pFields);
  }
  
  if (dst->pSupportInfo == NULL) {
    dst->pSupportInfo = taosArrayClone(src->pSupportInfo);
  } else {
    taosArrayCopy(dst->pSupportInfo, src->pSupportInfo);
  }
}

TAOS_FIELD* tscFieldInfoGetField(SFieldInfo* pFieldInfo, int32_t index) {
  return taosArrayGet(pFieldInfo->pFields, index);
}

int32_t tscNumOfFields(SQueryInfo* pQueryInfo) { return pQueryInfo->fieldsInfo.numOfOutput; }

int16_t tscFieldInfoGetOffset(SQueryInfo* pQueryInfo, int32_t index) {
  SFieldSupInfo* pInfo = tscFieldInfoGetSupp(&pQueryInfo->fieldsInfo, index);
  assert(pInfo != NULL);

  return pInfo->pSqlExpr->offset;
}

int32_t tscFieldInfoCompare(const SFieldInfo* pFieldInfo1, const SFieldInfo* pFieldInfo2) {
  assert(pFieldInfo1 != NULL && pFieldInfo2 != NULL);

  if (pFieldInfo1->numOfOutput != pFieldInfo2->numOfOutput) {
    return pFieldInfo1->numOfOutput - pFieldInfo2->numOfOutput;
  }

  for (int32_t i = 0; i < pFieldInfo1->numOfOutput; ++i) {
    TAOS_FIELD* pField1 = tscFieldInfoGetField((SFieldInfo*) pFieldInfo1, i);
    TAOS_FIELD* pField2 = tscFieldInfoGetField((SFieldInfo*) pFieldInfo2, i);

    if (pField1->type != pField2->type ||
        pField1->bytes != pField2->bytes ||
        strcasecmp(pField1->name, pField2->name) != 0) {
      return 1;
    }
  }

  return 0;
}

int32_t tscGetResRowLength(SArray* pExprList) {
  size_t num = taosArrayGetSize(pExprList);
  if (num == 0) {
    return 0;
  }
  
  int32_t size = 0;
  for(int32_t i = 0; i < num; ++i) {
    SSqlExpr* pExpr = taosArrayGetP(pExprList, i);
    size += pExpr->resBytes;
  }
  
  return size;
}

void tscFieldInfoClear(SFieldInfo* pFieldInfo) {
  if (pFieldInfo == NULL) {
    return;
  }

  taosArrayDestroy(pFieldInfo->pFields);
  
  for(int32_t i = 0; i < pFieldInfo->numOfOutput; ++i) {
    SFieldSupInfo* pInfo = taosArrayGet(pFieldInfo->pSupportInfo, i);
    
    if (pInfo->pArithExprInfo != NULL) {
      tExprTreeDestroy(&pInfo->pArithExprInfo->pExpr, NULL);
      tfree(pInfo->pArithExprInfo);
    }
  }
  
  taosArrayDestroy(pFieldInfo->pSupportInfo);
  memset(pFieldInfo, 0, sizeof(SFieldInfo));
}

static SSqlExpr* doBuildSqlExpr(SQueryInfo* pQueryInfo, int16_t functionId, SColumnIndex* pColIndex, int16_t type,
    int16_t size, int16_t interSize, bool isTagCol) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, pColIndex->tableIndex);
  
  SSqlExpr* pExpr = calloc(1, sizeof(SSqlExpr));
  pExpr->functionId = functionId;
  
  // set the correct columnIndex index
  if (pColIndex->columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
    pExpr->colInfo.colId = TSDB_TBNAME_COLUMN_INDEX;
  } else {
    if (isTagCol) {
      SSchema* pSchema = tscGetTableTagSchema(pTableMetaInfo->pTableMeta);
      pExpr->colInfo.colId = pSchema[pColIndex->columnIndex].colId;
      strncpy(pExpr->colInfo.name, pSchema[pColIndex->columnIndex].name, TSDB_COL_NAME_LEN);
    } else {
      SSchema* pSchema = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, pColIndex->columnIndex);
      pExpr->colInfo.colId = pSchema->colId;
      strncpy(pExpr->colInfo.name, pSchema->name, TSDB_COL_NAME_LEN);
    }
  }
  
  pExpr->colInfo.flag = isTagCol? TSDB_COL_TAG:TSDB_COL_NORMAL;
  
  pExpr->colInfo.colIndex = pColIndex->columnIndex;
  pExpr->resType       = type;
  pExpr->resBytes      = size;
  pExpr->interBytes = interSize;
  pExpr->uid           = pTableMetaInfo->pTableMeta->uid;
  
  return pExpr;
}

SSqlExpr* tscSqlExprInsert(SQueryInfo* pQueryInfo, int32_t index, int16_t functionId, SColumnIndex* pColIndex, int16_t type,
                           int16_t size, int16_t interSize, bool isTagCol) {
  int32_t num = taosArrayGetSize(pQueryInfo->exprList);
  if (index == num) {
    return tscSqlExprAppend(pQueryInfo, functionId, pColIndex, type, size, interSize, isTagCol);
  }
  
  SSqlExpr* pExpr = doBuildSqlExpr(pQueryInfo, functionId, pColIndex, type, size, interSize, isTagCol);
  taosArrayInsert(pQueryInfo->exprList, index, &pExpr);
  return pExpr;
}

SSqlExpr* tscSqlExprAppend(SQueryInfo* pQueryInfo, int16_t functionId, SColumnIndex* pColIndex, int16_t type,
    int16_t size, int16_t interSize, bool isTagCol) {
  SSqlExpr* pExpr = doBuildSqlExpr(pQueryInfo, functionId, pColIndex, type, size, interSize, isTagCol);
  taosArrayPush(pQueryInfo->exprList, &pExpr);
  return pExpr;
}

SSqlExpr* tscSqlExprUpdate(SQueryInfo* pQueryInfo, int32_t index, int16_t functionId, int16_t srcColumnIndex,
                           int16_t type, int16_t size) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, index);
  if (pExpr == NULL) {
    return NULL;
  }

  pExpr->functionId = functionId;

  pExpr->colInfo.colIndex = srcColumnIndex;
  pExpr->colInfo.colId = tscGetTableColumnSchema(pTableMetaInfo->pTableMeta, srcColumnIndex)->colId;

  pExpr->resType = type;
  pExpr->resBytes = size;

  return pExpr;
}

int32_t  tscSqlExprNumOfExprs(SQueryInfo* pQueryInfo) {
  return taosArrayGetSize(pQueryInfo->exprList);
}

void addExprParams(SSqlExpr* pExpr, char* argument, int32_t type, int32_t bytes, int16_t tableIndex) {
  if (pExpr == NULL || argument == NULL || bytes == 0) {
    return;
  }

  // set parameter value
  // transfer to tVariant from byte data/no ascii data
  tVariantCreateFromBinary(&pExpr->param[pExpr->numOfParams], argument, bytes, type);

  pExpr->numOfParams += 1;
  assert(pExpr->numOfParams <= 3);
}

SSqlExpr* tscSqlExprGet(SQueryInfo* pQueryInfo, int32_t index) {
  return taosArrayGetP(pQueryInfo->exprList, index);
}

void* sqlExprDestroy(SSqlExpr* pExpr) {
  if (pExpr == NULL) {
    return NULL;
  }
  
  for(int32_t i = 0; i < tListLen(pExpr->param); ++i) {
    tVariantDestroy(&pExpr->param[i]);
  }
  
  tfree(pExpr);
  
  return NULL;
}

/*
 * NOTE: Does not release SSqlExprInfo here.
 */
void tscSqlExprInfoDestroy(SArray* pExprInfo) {
  size_t size = taosArrayGetSize(pExprInfo);
  
  for(int32_t i = 0; i < size; ++i) {
    SSqlExpr* pExpr = taosArrayGetP(pExprInfo, i);
    sqlExprDestroy(pExpr);
  }
  
  taosArrayDestroy(pExprInfo);
}

void tscSqlExprCopy(SArray* dst, const SArray* src, uint64_t uid, bool deepcopy) {
  assert(src != NULL && dst != NULL);
  
  size_t size = taosArrayGetSize(src);
  for (int32_t i = 0; i < size; ++i) {
    SSqlExpr* pExpr = taosArrayGetP(src, i);
    
    if (pExpr->uid == uid) {
      
      if (deepcopy) {
        SSqlExpr* p1 = calloc(1, sizeof(SSqlExpr));
        *p1 = *pExpr;
  
        for (int32_t j = 0; j < pExpr->numOfParams; ++j) {
          tVariantAssign(&p1->param[j], &pExpr->param[j]);
        }
        
        taosArrayPush(dst, &p1);
      } else {
        taosArrayPush(dst, &pExpr);
      }
    }
  }
}

SColumn* tscColumnListInsert(SArray* pColumnList, SColumnIndex* pColIndex) {
  // ignore the tbname columnIndex to be inserted into source list
  if (pColIndex->columnIndex < 0) {
    return NULL;
  }
  
  size_t numOfCols = taosArrayGetSize(pColumnList);
  int16_t col = pColIndex->columnIndex;

  int32_t i = 0;
  while (i < numOfCols) {
    SColumn* pCol = taosArrayGetP(pColumnList, i);
    if (pCol->colIndex.columnIndex < col) {
      i++;
    } else if (pCol->colIndex.tableIndex < pColIndex->tableIndex) {
      i++;
    } else {
      break;
    }
  }

  if (i >= numOfCols || numOfCols == 0) {
    SColumn* b = calloc(1, sizeof(SColumn));
    b->colIndex = *pColIndex;
    
    taosArrayInsert(pColumnList, i, &b);
  } else {
    SColumn* pCol = taosArrayGetP(pColumnList, i);
  
    if (i < numOfCols && (pCol->colIndex.columnIndex > col || pCol->colIndex.tableIndex != pColIndex->tableIndex)) {
      SColumn* b = calloc(1, sizeof(SColumn));
      b->colIndex = *pColIndex;
      
      taosArrayInsert(pColumnList, i, &b);
    }
  }

  return taosArrayGetP(pColumnList, i);
}

SColumnFilterInfo* tscFilterInfoClone(const SColumnFilterInfo* src, int32_t numOfFilters) {
  if (numOfFilters == 0) {
    assert(src == NULL);
    return NULL;
  }
  
  SColumnFilterInfo* pFilter = calloc(1, numOfFilters * sizeof(SColumnFilterInfo));
  
  memcpy(pFilter, src, sizeof(SColumnFilterInfo) * numOfFilters);
  for (int32_t j = 0; j < numOfFilters; ++j) {
    
    if (pFilter[j].filterstr) {
      size_t len = (size_t) pFilter[j].len + 1;
      pFilter[j].pz = (int64_t) calloc(1, len);
      
      memcpy((char*)pFilter[j].pz, (char*)src[j].pz, (size_t)len);
    }
  }
  
  assert(src->filterstr == 0 || src->filterstr == 1);
  assert(!(src->lowerRelOptr == TSDB_RELATION_INVALID && src->upperRelOptr == TSDB_RELATION_INVALID));
  
  return pFilter;
}

static void destroyFilterInfo(SColumnFilterInfo* pFilterInfo, int32_t numOfFilters) {
  for(int32_t i = 0; i < numOfFilters; ++i) {
    if (pFilterInfo[i].filterstr) {
      tfree(pFilterInfo[i].pz);
    }
  }
  
  tfree(pFilterInfo);
}

SColumn* tscColumnClone(const SColumn* src) {
  assert(src != NULL);
  
  SColumn* dst = calloc(1, sizeof(SColumn));
  
  dst->colIndex     = src->colIndex;
  dst->numOfFilters = src->numOfFilters;
  dst->filterInfo   = tscFilterInfoClone(src->filterInfo, src->numOfFilters);
  
  return dst;
}

static void tscColumnDestroy(SColumn* pCol) {
  destroyFilterInfo(pCol->filterInfo, pCol->numOfFilters);
  free(pCol);
}

void tscColumnListCopy(SArray* dst, const SArray* src, int16_t tableIndex) {
  assert(src != NULL && dst != NULL);
  
  size_t num = taosArrayGetSize(src);
  for (int32_t i = 0; i < num; ++i) {
    SColumn* pCol = taosArrayGetP(src, i);

    if (pCol->colIndex.tableIndex == tableIndex || tableIndex < 0) {
      SColumn* p = tscColumnClone(pCol);
      taosArrayPush(dst, &p);
    }
  }
}

void tscColumnListDestroy(SArray* pColumnList) {
  if (pColumnList == NULL) {
    return;
  }

  size_t num = taosArrayGetSize(pColumnList);
  for (int32_t i = 0; i < num; ++i) {
    SColumn* pCol = taosArrayGetP(pColumnList, i);
    tscColumnDestroy(pCol);
  }

  taosArrayDestroy(pColumnList);
}

/*
 * 1. normal name, not a keyword or number
 * 2. name with quote
 * 3. string with only one delimiter '.'.
 *
 * only_one_part
 * 'only_one_part'
 * first_part.second_part
 * first_part.'second_part'
 * 'first_part'.second_part
 * 'first_part'.'second_part'
 * 'first_part.second_part'
 *
 */
static int32_t validateQuoteToken(SSQLToken* pToken) {
  pToken->n = strdequote(pToken->z);
  strtrim(pToken->z);
  pToken->n = (uint32_t)strlen(pToken->z);

  int32_t k = tSQLGetToken(pToken->z, &pToken->type);

  if (pToken->type == TK_STRING) {
    return tscValidateName(pToken);
  }

  if (k != pToken->n || pToken->type != TK_ID) {
    return TSDB_CODE_INVALID_SQL;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tscValidateName(SSQLToken* pToken) {
  if (pToken->type != TK_STRING && pToken->type != TK_ID) {
    return TSDB_CODE_INVALID_SQL;
  }

  char* sep = strnchr(pToken->z, TS_PATH_DELIMITER[0], pToken->n, true);
  if (sep == NULL) {  // single part
    if (pToken->type == TK_STRING) {
      pToken->n = strdequote(pToken->z);
      strtrim(pToken->z);
      pToken->n = (uint32_t)strlen(pToken->z);

      int len = tSQLGetToken(pToken->z, &pToken->type);

      // single token, validate it
      if (len == pToken->n) {
        return validateQuoteToken(pToken);
      } else {
        sep = strnchr(pToken->z, TS_PATH_DELIMITER[0], pToken->n, true);
        if (sep == NULL) {
          return TSDB_CODE_INVALID_SQL;
        }

        return tscValidateName(pToken);
      }
    } else {
      if (isNumber(pToken)) {
        return TSDB_CODE_INVALID_SQL;
      }
    }
  } else {  // two part
    int32_t oldLen = pToken->n;
    char*   pStr = pToken->z;

    if (pToken->type == TK_SPACE) {
      strtrim(pToken->z);
      pToken->n = (uint32_t)strlen(pToken->z);
    }

    pToken->n = tSQLGetToken(pToken->z, &pToken->type);
    if (pToken->z[pToken->n] != TS_PATH_DELIMITER[0]) {
      return TSDB_CODE_INVALID_SQL;
    }

    if (pToken->type != TK_STRING && pToken->type != TK_ID) {
      return TSDB_CODE_INVALID_SQL;
    }

    if (pToken->type == TK_STRING && validateQuoteToken(pToken) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_INVALID_SQL;
    }

    int32_t firstPartLen = pToken->n;

    pToken->z = sep + 1;
    pToken->n = oldLen - (sep - pStr) - 1;
    int32_t len = tSQLGetToken(pToken->z, &pToken->type);
    if (len != pToken->n || (pToken->type != TK_STRING && pToken->type != TK_ID)) {
      return TSDB_CODE_INVALID_SQL;
    }

    if (pToken->type == TK_STRING && validateQuoteToken(pToken) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_INVALID_SQL;
    }

    // re-build the whole name string
    if (pStr[firstPartLen] == TS_PATH_DELIMITER[0]) {
      // first part do not have quote do nothing
    } else {
      pStr[firstPartLen] = TS_PATH_DELIMITER[0];
      memmove(&pStr[firstPartLen + 1], pToken->z, pToken->n);
      pStr[firstPartLen + sizeof(TS_PATH_DELIMITER[0]) + pToken->n] = 0;
    }
    pToken->n += (firstPartLen + sizeof(TS_PATH_DELIMITER[0]));
    pToken->z = pStr;
  }

  return TSDB_CODE_SUCCESS;
}

void tscIncStreamExecutionCount(void* pStream) {
  if (pStream == NULL) {
    return;
  }

  SSqlStream* ps = (SSqlStream*)pStream;
  ps->num += 1;
}

bool tscValidateColumnId(STableMetaInfo* pTableMetaInfo, int32_t colId) {
  if (pTableMetaInfo->pTableMeta == NULL) {
    return false;
  }

  if (colId == -1 && UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    return true;
  }

  SSchema* pSchema = tscGetTableSchema(pTableMetaInfo->pTableMeta);
  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
  
  int32_t  numOfTotal = tinfo.numOfTags + tinfo.numOfColumns;

  for (int32_t i = 0; i < numOfTotal; ++i) {
    if (pSchema[i].colId == colId) {
      return true;
    }
  }

  return false;
}

void tscTagCondCopy(STagCond* dest, const STagCond* src) {
  memset(dest, 0, sizeof(STagCond));

  if (src->tbnameCond.cond != NULL) {
    dest->tbnameCond.cond = strdup(src->tbnameCond.cond);
  }

  dest->tbnameCond.uid = src->tbnameCond.uid;

  memcpy(&dest->joinInfo, &src->joinInfo, sizeof(SJoinInfo));
  dest->relType = src->relType;
  
  if (src->pCond == NULL) {
    return;
  }
  
  size_t s = taosArrayGetSize(src->pCond);
  dest->pCond = taosArrayInit(s, sizeof(SCond));
  
  for (int32_t i = 0; i < s; ++i) {
    SCond* pCond = taosArrayGet(src->pCond, i);
    
    SCond c = {0};
    c.len = pCond->len;
    c.uid = pCond->uid;
    
    if (pCond->len > 0) {
      assert(pCond->cond != NULL);
      c.cond = malloc(c.len);
      memcpy(c.cond, pCond->cond, c.len);
    }
    
    taosArrayPush(dest->pCond, &c);
  }
}

void tscTagCondRelease(STagCond* pTagCond) {
  free(pTagCond->tbnameCond.cond);
  
  if (pTagCond->pCond != NULL) {
    size_t s = taosArrayGetSize(pTagCond->pCond);
    for (int32_t i = 0; i < s; ++i) {
      SCond* p = taosArrayGet(pTagCond->pCond, i);
      tfree(p->cond);
    }
  
    taosArrayDestroy(pTagCond->pCond);
  }

  memset(pTagCond, 0, sizeof(STagCond));
}

void tscGetSrcColumnInfo(SSrcColumnInfo* pColInfo, SQueryInfo* pQueryInfo) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  SSchema*        pSchema = tscGetTableSchema(pTableMetaInfo->pTableMeta);
  
  size_t numOfExprs = tscSqlExprNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < numOfExprs; ++i) {
    SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);
    pColInfo[i].functionId = pExpr->functionId;

    if (TSDB_COL_IS_TAG(pExpr->colInfo.flag)) {
      SSchema* pTagSchema = tscGetTableTagSchema(pTableMetaInfo->pTableMeta);
      
      int16_t index = pExpr->colInfo.colIndex;
      pColInfo[i].type = (index != -1) ? pTagSchema[index].type : TSDB_DATA_TYPE_BINARY;
    } else {
      pColInfo[i].type = pSchema[pExpr->colInfo.colIndex].type;
    }
  }
}

void tscSetFreeHeatBeat(STscObj* pObj) {
  if (pObj == NULL || pObj->signature != pObj || pObj->pHb == NULL) {
    return;
  }

  SSqlObj* pHeatBeat = pObj->pHb;
  assert(pHeatBeat == pHeatBeat->signature);

  // to denote the heart-beat timer close connection and free all allocated resources
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pHeatBeat->cmd, 0);
  pQueryInfo->type = TSDB_QUERY_TYPE_FREE_RESOURCE;
}

bool tscShouldFreeHeatBeat(SSqlObj* pHb) {
  assert(pHb == pHb->signature);

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(&pHb->cmd, 0);
  return pQueryInfo->type == TSDB_QUERY_TYPE_FREE_RESOURCE;
}

/*
 * the following four kinds of SqlObj should not be freed
 * 1. SqlObj for stream computing
 * 2. main SqlObj
 * 3. heartbeat SqlObj
 * 4. SqlObj for subscription
 *
 * If res code is error and SqlObj does not belong to above types, it should be
 * automatically freed for async query, ignoring that connection should be kept.
 *
 * If connection need to be recycled, the SqlObj also should be freed.
 */
bool tscShouldBeFreed(SSqlObj* pSql) {
  if (pSql == NULL || pSql->signature != pSql || pSql->fp == NULL) {
    return false;
  }

  STscObj* pTscObj = pSql->pTscObj;
  if (pSql->pStream != NULL || pTscObj->pHb == pSql || pTscObj->pSql == pSql || pSql->pSubscription != NULL) {
    return false;
  }

  int32_t command = pSql->cmd.command;
  if (command == TSDB_SQL_CONNECT || command == TSDB_SQL_INSERT) {
    return true;
  } else {
    return tscKeepConn[command] == 0 ||
           (pSql->res.code != TSDB_CODE_ACTION_IN_PROGRESS && pSql->res.code != TSDB_CODE_SUCCESS);
  }
}

/**
 *
 * @param pCmd
 * @param clauseIndex denote the index of the union sub clause, usually are 0, if no union query exists.
 * @param tableIndex  denote the table index for join query, where more than one table exists
 * @return
 */
STableMetaInfo* tscGetTableMetaInfoFromCmd(SSqlCmd* pCmd, int32_t clauseIndex, int32_t tableIndex) {
  if (pCmd == NULL || pCmd->numOfClause == 0) {
    return NULL;
  }

  assert(clauseIndex >= 0 && clauseIndex < pCmd->numOfClause);

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, clauseIndex);
  return tscGetMetaInfo(pQueryInfo, tableIndex);
}

STableMetaInfo* tscGetMetaInfo(SQueryInfo* pQueryInfo, int32_t tableIndex) {
  assert(pQueryInfo != NULL);

  if (pQueryInfo->pTableMetaInfo == NULL) {
    assert(pQueryInfo->numOfTables == 0);
    return NULL;
  }

  assert(tableIndex >= 0 && tableIndex <= pQueryInfo->numOfTables && pQueryInfo->pTableMetaInfo != NULL);

  return pQueryInfo->pTableMetaInfo[tableIndex];
}

SQueryInfo* tscGetQueryInfoDetail(SSqlCmd* pCmd, int32_t subClauseIndex) {
  assert(pCmd != NULL && subClauseIndex >= 0 && subClauseIndex < TSDB_MAX_UNION_CLAUSE);

  if (pCmd->pQueryInfo == NULL || subClauseIndex >= pCmd->numOfClause) {
    return NULL;
  }

  return pCmd->pQueryInfo[subClauseIndex];
}

int32_t tscGetQueryInfoDetailSafely(SSqlCmd* pCmd, int32_t subClauseIndex, SQueryInfo** pQueryInfo) {
  int32_t ret = TSDB_CODE_SUCCESS;

  *pQueryInfo = tscGetQueryInfoDetail(pCmd, subClauseIndex);

  while ((*pQueryInfo) == NULL) {
    if ((ret = tscAddSubqueryInfo(pCmd)) != TSDB_CODE_SUCCESS) {
      return ret;
    }

    (*pQueryInfo) = tscGetQueryInfoDetail(pCmd, subClauseIndex);
  }

  return TSDB_CODE_SUCCESS;
}

STableMetaInfo* tscGetTableMetaInfoByUid(SQueryInfo* pQueryInfo, uint64_t uid, int32_t* index) {
  int32_t k = -1;

  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    if (pQueryInfo->pTableMetaInfo[i]->pTableMeta->uid == uid) {
      k = i;
      break;
    }
  }

  if (index != NULL) {
    *index = k;
  }

  assert(k != -1);
  return tscGetMetaInfo(pQueryInfo, k);
}

void tscInitQueryInfo(SQueryInfo* pQueryInfo) {
  assert(pQueryInfo->fieldsInfo.pFields == NULL);
  pQueryInfo->fieldsInfo.pFields = taosArrayInit(4, sizeof(TAOS_FIELD));
  
  assert(pQueryInfo->fieldsInfo.pSupportInfo == NULL);
  pQueryInfo->fieldsInfo.pSupportInfo = taosArrayInit(4, sizeof(SFieldSupInfo));
  
  assert(pQueryInfo->exprList == NULL);
  pQueryInfo->exprList = taosArrayInit(4, POINTER_BYTES);
  pQueryInfo->colList  = taosArrayInit(4, POINTER_BYTES);
}

int32_t tscAddSubqueryInfo(SSqlCmd* pCmd) {
  assert(pCmd != NULL);

  // todo refactor: remove this structure
  size_t s = pCmd->numOfClause + 1;
  char*  tmp = realloc(pCmd->pQueryInfo, s * POINTER_BYTES);
  if (tmp == NULL) {
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  pCmd->pQueryInfo = (SQueryInfo**)tmp;

  SQueryInfo* pQueryInfo = calloc(1, sizeof(SQueryInfo));
  tscInitQueryInfo(pQueryInfo);
  
  pQueryInfo->msg = pCmd->payload;  // pointer to the parent error message buffer

  pCmd->pQueryInfo[pCmd->numOfClause++] = pQueryInfo;
  return TSDB_CODE_SUCCESS;
}

static void freeQueryInfoImpl(SQueryInfo* pQueryInfo) {
  tscTagCondRelease(&pQueryInfo->tagCond);
  tscFieldInfoClear(&pQueryInfo->fieldsInfo);

  tscSqlExprInfoDestroy(pQueryInfo->exprList);
  pQueryInfo->exprList = NULL;

  tscColumnListDestroy(pQueryInfo->colList);
  pQueryInfo->colList = NULL;

  if (pQueryInfo->groupbyExpr.columnInfo != NULL) {
    taosArrayDestroy(pQueryInfo->groupbyExpr.columnInfo);
    pQueryInfo->groupbyExpr.columnInfo = NULL;
  }
  
  pQueryInfo->tsBuf = tsBufDestory(pQueryInfo->tsBuf);

  tfree(pQueryInfo->defaultVal);
}

void tscClearSubqueryInfo(SSqlCmd* pCmd) {
  for (int32_t i = 0; i < pCmd->numOfClause; ++i) {
    SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, i);
    freeQueryInfoImpl(pQueryInfo);
  }
}

void doRemoveTableMetaInfo(SQueryInfo* pQueryInfo, int32_t index, bool removeFromCache) {
  if (index < 0 || index >= pQueryInfo->numOfTables) {
    return;
  }
  
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, index);
  
  tscClearTableMetaInfo(pTableMetaInfo, removeFromCache);
  free(pTableMetaInfo);
  
  int32_t after = pQueryInfo->numOfTables - index - 1;
  if (after > 0) {
    memmove(&pQueryInfo->pTableMetaInfo[index], &pQueryInfo->pTableMetaInfo[index + 1], after * POINTER_BYTES);
  }
  
  pQueryInfo->numOfTables -= 1;
}

void clearAllTableMetaInfo(SQueryInfo* pQueryInfo, const char* address, bool removeFromCache) {
  tscTrace("%p deref the table meta in cache, numOfTables:%d", address, pQueryInfo->numOfTables);
  
  for(int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, i);
  
    tscClearTableMetaInfo(pTableMetaInfo, removeFromCache);
    free(pTableMetaInfo);
  }
  
  tfree(pQueryInfo->pTableMetaInfo);
}

STableMetaInfo* tscAddTableMetaInfo(SQueryInfo* pQueryInfo, const char* name, STableMeta* pTableMeta,
                                    SVgroupsInfo* vgroupList, SArray* pTagCols) {
  void* pAlloc = realloc(pQueryInfo->pTableMetaInfo, (pQueryInfo->numOfTables + 1) * POINTER_BYTES);
  if (pAlloc == NULL) {
    return NULL;
  }

  pQueryInfo->pTableMetaInfo = pAlloc;
  pQueryInfo->pTableMetaInfo[pQueryInfo->numOfTables] = calloc(1, sizeof(STableMetaInfo));

  STableMetaInfo* pTableMetaInfo = pQueryInfo->pTableMetaInfo[pQueryInfo->numOfTables];
  assert(pTableMetaInfo != NULL);

  if (name != NULL) {
    strncpy(pTableMetaInfo->name, name, TSDB_TABLE_ID_LEN);
  }

  pTableMetaInfo->pTableMeta = pTableMeta;
  
  if (vgroupList != NULL) {
    size_t size = sizeof(SVgroupsInfo) + sizeof(SCMVgroupInfo) * vgroupList->numOfVgroups;
    pTableMetaInfo->vgroupList = malloc(size);
    memcpy(pTableMetaInfo->vgroupList, vgroupList, size);
  }

  pTableMetaInfo->tagColList = taosArrayInit(4, POINTER_BYTES);
  if (pTagCols != NULL) {
    tscColumnListCopy(pTableMetaInfo->tagColList, pTagCols, -1);
  }
  
  pQueryInfo->numOfTables += 1;
  return pTableMetaInfo;
}

STableMetaInfo* tscAddEmptyMetaInfo(SQueryInfo* pQueryInfo) {
  return tscAddTableMetaInfo(pQueryInfo, NULL, NULL, NULL, NULL);
}

void tscClearTableMetaInfo(STableMetaInfo* pTableMetaInfo, bool removeFromCache) {
  if (pTableMetaInfo == NULL) {
    return;
  }

  taosCacheRelease(tscCacheHandle, (void**)&(pTableMetaInfo->pTableMeta), removeFromCache);
  tfree(pTableMetaInfo->vgroupList);
  
  tscColumnListDestroy(pTableMetaInfo->tagColList);
  pTableMetaInfo->tagColList = NULL;
}

void tscResetForNextRetrieve(SSqlRes* pRes) {
  if (pRes == NULL) {
    return;
  }

  pRes->row = 0;
  pRes->numOfRows = 0;
}

SSqlObj* createSubqueryObj(SSqlObj* pSql, int16_t tableIndex, void (*fp)(), void* param, int32_t cmd, SSqlObj* pPrevSql) {
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlObj* pNew = (SSqlObj*)calloc(1, sizeof(SSqlObj));
  if (pNew == NULL) {
    tscError("%p new subquery failed, tableIndex:%d", pSql, tableIndex);
    return NULL;
  }
  
  STableMetaInfo* pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, tableIndex);

  pNew->pTscObj = pSql->pTscObj;
  pNew->signature = pNew;

  pNew->sqlstr = strdup(pSql->sqlstr);
  if (pNew->sqlstr == NULL) {
    tscError("%p new subquery failed, tableIndex:%d, vgroupIndex:%d", pSql, tableIndex, pTableMetaInfo->vgroupIndex);

    free(pNew);
    return NULL;
  }

  SSqlCmd* pnCmd = &pNew->cmd;
  memcpy(pnCmd, pCmd, sizeof(SSqlCmd));
  
  pnCmd->command = cmd;
  pnCmd->payload = NULL;
  pnCmd->allocSize = 0;

  pnCmd->pQueryInfo = NULL;
  pnCmd->numOfClause = 0;
  pnCmd->clauseIndex = 0;
  pnCmd->pDataBlocks = NULL;

  if (tscAddSubqueryInfo(pnCmd) != TSDB_CODE_SUCCESS) {
    tscFreeSqlObj(pNew);
    return NULL;
  }

  SQueryInfo* pNewQueryInfo = tscGetQueryInfoDetail(pnCmd, 0);
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  pNewQueryInfo->command = pQueryInfo->command;
  pNewQueryInfo->slidingTimeUnit = pQueryInfo->slidingTimeUnit;
  pNewQueryInfo->intervalTime = pQueryInfo->intervalTime;
  pNewQueryInfo->slidingTime  = pQueryInfo->slidingTime;
  pNewQueryInfo->type   = pQueryInfo->type;
  pNewQueryInfo->window = pQueryInfo->window;
  pNewQueryInfo->limit  = pQueryInfo->limit;
  pNewQueryInfo->slimit = pQueryInfo->slimit;
  pNewQueryInfo->order  = pQueryInfo->order;
  pNewQueryInfo->clauseLimit = pQueryInfo->clauseLimit;
  pNewQueryInfo->pTableMetaInfo = NULL;
  pNewQueryInfo->defaultVal  = NULL;
  pNewQueryInfo->numOfTables = 0;
  pNewQueryInfo->tsBuf = NULL;
  
  pNewQueryInfo->groupbyExpr = pQueryInfo->groupbyExpr;
  if (pQueryInfo->groupbyExpr.columnInfo != NULL) {
    pNewQueryInfo->groupbyExpr.columnInfo = taosArrayClone(pQueryInfo->groupbyExpr.columnInfo);
  }
  
  tscTagCondCopy(&pNewQueryInfo->tagCond, &pQueryInfo->tagCond);

  if (pQueryInfo->interpoType != TSDB_INTERPO_NONE) {
    pNewQueryInfo->defaultVal = malloc(pQueryInfo->fieldsInfo.numOfOutput * sizeof(int64_t));
    memcpy(pNewQueryInfo->defaultVal, pQueryInfo->defaultVal, pQueryInfo->fieldsInfo.numOfOutput * sizeof(int64_t));
  }

  if (tscAllocPayload(pnCmd, TSDB_DEFAULT_PAYLOAD_SIZE) != TSDB_CODE_SUCCESS) {
    tscError("%p new subquery failed, tableIndex:%d, vgroupIndex:%d", pSql, tableIndex, pTableMetaInfo->vgroupIndex);
    tscFreeSqlObj(pNew);
    return NULL;
  }
  
  tscColumnListCopy(pNewQueryInfo->colList, pQueryInfo->colList, (int16_t)tableIndex);

  // set the correct query type
  if (pPrevSql != NULL) {
    SQueryInfo* pPrevQueryInfo = tscGetQueryInfoDetail(&pPrevSql->cmd, pPrevSql->cmd.clauseIndex);
    pNewQueryInfo->type = pPrevQueryInfo->type;
  } else {
    pNewQueryInfo->type |= TSDB_QUERY_TYPE_SUBQUERY;  // it must be the subquery
  }

  uint64_t uid = pTableMetaInfo->pTableMeta->uid;
  tscSqlExprCopy(pNewQueryInfo->exprList, pQueryInfo->exprList, uid, true);

  int32_t numOfOutput = tscSqlExprNumOfExprs(pNewQueryInfo);

  if (numOfOutput > 0) {  // todo refactor to extract method
    size_t numOfExprs = tscSqlExprNumOfExprs(pQueryInfo);
    SFieldInfo* pFieldInfo = &pQueryInfo->fieldsInfo;
    
    for (int32_t i = 0; i < numOfExprs; ++i) {
      SSqlExpr* pExpr = tscSqlExprGet(pQueryInfo, i);
      
      if (pExpr->uid == uid) {
        TAOS_FIELD* p = tscFieldInfoGetField(pFieldInfo, i);
        SFieldSupInfo* pInfo = tscFieldInfoGetSupp(pFieldInfo, i);
  
        SFieldSupInfo* pInfo1 = tscFieldInfoAppend(&pNewQueryInfo->fieldsInfo, p);
        *pInfo1 = *pInfo;
      }
    }

    // make sure the the sqlExpr for each fields is correct
// todo handle the agg arithmetic expression
    for(int32_t f = 0; f < pNewQueryInfo->fieldsInfo.numOfOutput; ++f) {
      TAOS_FIELD* field = tscFieldInfoGetField(&pNewQueryInfo->fieldsInfo, f);
      numOfExprs = tscSqlExprNumOfExprs(pNewQueryInfo);
      
      for(int32_t k1 = 0; k1 < numOfExprs; ++k1) {
        SSqlExpr* pExpr1 = tscSqlExprGet(pNewQueryInfo, k1);
        
        if (strcmp(field->name, pExpr1->aliasName) == 0) {  // eatablish link according to the result field name
          SFieldSupInfo* pInfo = tscFieldInfoGetSupp(&pNewQueryInfo->fieldsInfo, f);
          pInfo->pSqlExpr = pExpr1;
        }
      }
    }
  
    tscFieldInfoUpdateOffset(pNewQueryInfo);
  }

  pNew->fp = fp;
  pNew->param = param;
  pNew->maxRetry = TSDB_MAX_REPLICA_NUM;

  char* name = pTableMetaInfo->name;
  STableMetaInfo* pFinalInfo = NULL;

  if (pPrevSql == NULL) {
    STableMeta* pTableMeta = taosCacheAcquireByName(tscCacheHandle, name);
    // todo handle error
    assert(pTableMeta != NULL);
    pFinalInfo = tscAddTableMetaInfo(pNewQueryInfo, name, pTableMeta, pTableMetaInfo->vgroupList, pTableMetaInfo->tagColList);
  } else {  // transfer the ownership of pTableMeta to the newly create sql object.
    STableMetaInfo* pPrevInfo = tscGetTableMetaInfoFromCmd(&pPrevSql->cmd, pPrevSql->cmd.clauseIndex, 0);

    STableMeta*  pPrevTableMeta = taosCacheTransfer(tscCacheHandle, (void**)&pPrevInfo->pTableMeta);
    
    SVgroupsInfo* pVgroupsInfo = pPrevInfo->vgroupList;
    pPrevInfo->vgroupList = NULL;
    pFinalInfo = tscAddTableMetaInfo(pNewQueryInfo, name, pPrevTableMeta, pVgroupsInfo, pTableMetaInfo->tagColList);
  }

  if (pFinalInfo->pTableMeta == NULL) {
    tscError("%p new subquery failed for get pMeterMeta is NULL from cache", pSql);
    tscFreeSqlObj(pNew);
    return NULL;
  }
  
  assert(pNewQueryInfo->numOfTables == 1);
  
  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    assert(pFinalInfo->vgroupList != NULL);
  }
  
  if (cmd == TSDB_SQL_SELECT) {
    size_t size = taosArrayGetSize(pNewQueryInfo->colList);
    
    tscTrace(
        "%p new subquery:%p, tableIndex:%d, vgroupIndex:%d, type:%d, exprInfo:%d, colList:%d,"
        "fieldInfo:%d, name:%s, qrang:%" PRId64 " - %" PRId64 " order:%d, limit:%" PRId64,
        pSql, pNew, tableIndex, pTableMetaInfo->vgroupIndex, pNewQueryInfo->type, tscSqlExprNumOfExprs(pNewQueryInfo),
        size, pNewQueryInfo->fieldsInfo.numOfOutput, pFinalInfo->name, pNewQueryInfo->window.skey,
        pNewQueryInfo->window.ekey, pNewQueryInfo->order.order, pNewQueryInfo->limit.limit);
    
    tscPrintSelectClause(pNew, 0);
  } else {
    tscTrace("%p new sub insertion: %p, vnodeIdx:%d", pSql, pNew, pTableMetaInfo->vgroupIndex);
  }

  return pNew;
}

/**
 * To decide if current is a two-stage super table query, join query, or insert. And invoke different
 * procedure accordingly
 * @param pSql
 */
void tscDoQuery(SSqlObj* pSql) {
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;
  
  pRes->code = TSDB_CODE_SUCCESS;
  
  if (pCmd->command > TSDB_SQL_LOCAL) {
    tscProcessLocalCmd(pSql);
    return;
  }
  
  if (pCmd->command == TSDB_SQL_SELECT) {
    tscAddIntoSqlList(pSql);
  }

  if (pCmd->dataSourceType == DATA_FROM_DATA_FILE) {
    tscProcessMultiVnodesInsertFromFile(pSql);
  } else {
    SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
    uint16_t type = pQueryInfo->type;
  
    if (pSql->fp == (void(*)())tscHandleMultivnodeInsert) {  // multi-vnodes insertion
      tscHandleMultivnodeInsert(pSql);
      return;
    }
  
    if (QUERY_IS_JOIN_QUERY(type)) {
      if ((pQueryInfo->type & TSDB_QUERY_TYPE_SUBQUERY) == 0) {
        tscHandleMasterJoinQuery(pSql);
        return;
      } else {
        // for first stage sub query, iterate all vnodes to get all timestamp
        if ((pQueryInfo->type & TSDB_QUERY_TYPE_JOIN_SEC_STAGE) != TSDB_QUERY_TYPE_JOIN_SEC_STAGE) {
//          doProcessSql(pSql);
          assert(0);
        }
      }
    } else if (tscIsTwoStageSTableQuery(pQueryInfo, 0)) {  // super table query
      tscHandleMasterSTableQuery(pSql);
      return;
    }
    
    tscProcessSql(pSql);
  }
}

int16_t tscGetJoinTagColIndexByUid(STagCond* pTagCond, uint64_t uid) {
  if (pTagCond->joinInfo.left.uid == uid) {
    return pTagCond->joinInfo.left.tagCol;
  } else {
    return pTagCond->joinInfo.right.tagCol;
  }
}

bool tscIsUpdateQuery(STscObj* pObj) {
  if (pObj == NULL || pObj->signature != pObj) {
    terrno = TSDB_CODE_DISCONNECTED;
    return TSDB_CODE_DISCONNECTED;
  }

  SSqlCmd* pCmd = &pObj->pSql->cmd;
  return ((pCmd->command >= TSDB_SQL_INSERT && pCmd->command <= TSDB_SQL_DROP_DNODE) ||
          TSDB_SQL_USE_DB == pCmd->command);
}

int32_t tscInvalidSQLErrMsg(char* msg, const char* additionalInfo, const char* sql) {
  const char* msgFormat1 = "invalid SQL: %s";
  const char* msgFormat2 = "invalid SQL: syntax error near \"%s\" (%s)";
  const char* msgFormat3 = "invalid SQL: syntax error near \"%s\"";

  const int32_t BACKWARD_CHAR_STEP = 0;

  if (sql == NULL) {
    assert(additionalInfo != NULL);
    sprintf(msg, msgFormat1, additionalInfo);
    return TSDB_CODE_INVALID_SQL;
  }

  char buf[64] = {0};  // only extract part of sql string
  strncpy(buf, (sql - BACKWARD_CHAR_STEP), tListLen(buf) - 1);

  if (additionalInfo != NULL) {
    sprintf(msg, msgFormat2, buf, additionalInfo);
  } else {
    sprintf(msg, msgFormat3, buf);  // no additional information for invalid sql error
  }

  return TSDB_CODE_INVALID_SQL;
}

bool tscHasReachLimitation(SQueryInfo* pQueryInfo, SSqlRes* pRes) {
  assert(pQueryInfo != NULL && pQueryInfo->clauseLimit != 0);
  return (pQueryInfo->clauseLimit > 0 && pRes->numOfTotalInCurrentClause >= pQueryInfo->clauseLimit);
}

char* tscGetErrorMsgPayload(SSqlCmd* pCmd) { return pCmd->payload; }

/**
 *  If current vnode query does not return results anymore (pRes->numOfRows == 0), try the next vnode if exists,
 *  while multi-vnode super table projection query and the result does not reach the limitation.
 */
bool hasMoreVnodesToTry(SSqlObj* pSql) {
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;
  if (pCmd->command != TSDB_SQL_FETCH) {
    return false;
  }

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  assert(pRes->completed);
  
  // for normal table, do not try any more if result are exhausted
  if (!UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo) || (pTableMetaInfo->vgroupList == NULL)) {
    return false;
  }
  
  int32_t numOfVgroups = pTableMetaInfo->vgroupList->numOfVgroups;
  return tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0) &&
         (!tscHasReachLimitation(pQueryInfo, pRes)) && (pTableMetaInfo->vgroupIndex < numOfVgroups - 1);
}

void tscTryQueryNextVnode(SSqlObj* pSql, __async_cb_func_t fp) {
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  /*
   * no result returned from the current virtual node anymore, try the next vnode if exists
   * if case of: multi-vnode super table projection query
   */
  assert(pRes->numOfRows == 0 && tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0) && !tscHasReachLimitation(pQueryInfo, pRes));

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  
  int32_t totalVgroups = pTableMetaInfo->vgroupList->numOfVgroups;
  while (++pTableMetaInfo->vgroupIndex < totalVgroups) {
    tscTrace("%p current vnode:%d exhausted, try next:%d. total vnode:%d. current numOfRes:%d", pSql,
             pTableMetaInfo->vgroupIndex - 1, pTableMetaInfo->vgroupIndex, totalVgroups, pRes->numOfTotalInCurrentClause);

    /*
     * update the limit and offset value for the query on the next vnode,
     * according to current retrieval results
     *
     * NOTE:
     * if the pRes->offset is larger than 0, the start returned position has not reached yet.
     * Therefore, the pRes->numOfRows, as well as pRes->numOfTotalInCurrentClause, must be 0.
     * The pRes->offset value will be updated by virtual node, during query execution.
     */
    if (pQueryInfo->clauseLimit >= 0) {
      pQueryInfo->limit.limit = pQueryInfo->clauseLimit - pRes->numOfTotalInCurrentClause;
    }

    pQueryInfo->limit.offset = pRes->offset;
    assert((pRes->offset >= 0 && pRes->numOfRows == 0) || (pRes->offset == 0 && pRes->numOfRows >= 0));
    
    tscTrace("%p new query to next vgroup, index:%d, limit:%" PRId64 ", offset:%" PRId64 ", glimit:%" PRId64,
        pSql, pTableMetaInfo->vgroupIndex, pQueryInfo->limit.limit, pQueryInfo->limit.offset, pQueryInfo->clauseLimit);

    /*
     * For project query with super table join, the numOfSub is equalled to the number of all subqueries.
     * Therefore, we need to reset the value of numOfSubs to be 0.
     *
     * For super table join with projection query, if anyone of the subquery is exhausted, the query completed.
     */
    pSql->numOfSubs = 0;
    pCmd->command = TSDB_SQL_SELECT;

    tscResetForNextRetrieve(pRes);

    // set the callback function
    pSql->fp = fp;
    int32_t ret = tscProcessSql(pSql);
    if (ret == TSDB_CODE_SUCCESS) {
      return;
    } else {// todo check for failure
    }
  }
}

void tscTryQueryNextClause(SSqlObj* pSql, void (*queryFp)()) {
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;

  // current subclause is completed, try the next subclause
  assert(pCmd->clauseIndex < pCmd->numOfClause - 1);

  pCmd->clauseIndex++;
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  pSql->cmd.command = pQueryInfo->command;

  //backup the total number of result first
  int64_t num = pRes->numOfTotal + pRes->numOfTotalInCurrentClause;
  tscFreeSqlResult(pSql);
  
  pRes->numOfTotal = num;
  
  tfree(pSql->pSubs);
  pSql->numOfSubs = 0;
  
  if (pSql->fp != NULL) {
    pSql->fp = queryFp;
    assert(queryFp != NULL);
  }

  tscTrace("%p try data in the next subclause:%d, total subclause:%d", pSql, pCmd->clauseIndex, pCmd->numOfClause);
  if (pCmd->command > TSDB_SQL_LOCAL) {
    tscProcessLocalCmd(pSql);
  } else {
    tscProcessSql(pSql);
  }
}

void tscGetResultColumnChr(SSqlRes* pRes, SFieldInfo* pFieldInfo, int32_t columnIndex) {
  SFieldSupInfo* pInfo = tscFieldInfoGetSupp(pFieldInfo, columnIndex);
  assert(pInfo->pSqlExpr != NULL);

  int32_t type = pInfo->pSqlExpr->resType;
  int32_t bytes = pInfo->pSqlExpr->resBytes;
  
  char* pData = ((char*) pRes->data) + pInfo->pSqlExpr->offset * pRes->numOfRows + bytes * pRes->row;
  
  if (type == TSDB_DATA_TYPE_NCHAR || type == TSDB_DATA_TYPE_BINARY) {
    int32_t realLen = varDataLen(pData);
    assert(realLen <= bytes - VARSTR_HEADER_SIZE);
    
    if (realLen < pInfo->pSqlExpr->resBytes - VARSTR_HEADER_SIZE) { // todo refactor
      *(char*) (pData + realLen + VARSTR_HEADER_SIZE) = 0;
    }
    
    pRes->tsrow[columnIndex] = pData + VARSTR_HEADER_SIZE;
    pRes->length[columnIndex] = realLen;
  } else {
    assert(bytes == tDataTypeDesc[type].nSize);
    
    pRes->tsrow[columnIndex] = pData;
    pRes->length[columnIndex] = bytes;
  }
}

void* malloc_throw(size_t size) {
  void* p = malloc(size);
  if (p == NULL) {
    THROW(TSDB_CODE_CLI_OUT_OF_MEMORY);
  }
  return p;
}

void* calloc_throw(size_t nmemb, size_t size) {
  void* p = calloc(nmemb, size);
  if (p == NULL) {
    THROW(TSDB_CODE_CLI_OUT_OF_MEMORY);
  }
  return p;
}

char* strdup_throw(const char* str) {
  char* p = strdup(str);
  if (p == NULL) {
    THROW(TSDB_CODE_CLI_OUT_OF_MEMORY);
  }
  return p;
}

int tscSetMgmtIpListFromCfg(const char *first, const char *second) {
  tscMgmtIpSet.numOfIps = 0;
  tscMgmtIpSet.inUse = 0;

  if (first && first[0] != 0) {
    if (strlen(first) >= TSDB_FQDN_LEN) {
      terrno = TSDB_CODE_INVALID_FQDN;
      return -1;
    }
    taosGetFqdnPortFromEp(first, tscMgmtIpSet.fqdn[tscMgmtIpSet.numOfIps], &tscMgmtIpSet.port[tscMgmtIpSet.numOfIps]);
    tscMgmtIpSet.numOfIps++;
  }

  if (second && second[0] != 0) {
    if (strlen(second) >= TSDB_FQDN_LEN) {
      terrno = TSDB_CODE_INVALID_FQDN;
      return -1;
    }
    taosGetFqdnPortFromEp(second, tscMgmtIpSet.fqdn[tscMgmtIpSet.numOfIps], &tscMgmtIpSet.port[tscMgmtIpSet.numOfIps]);
    tscMgmtIpSet.numOfIps++;
  }

  if ( tscMgmtIpSet.numOfIps == 0) {
    terrno = TSDB_CODE_INVALID_FQDN;
    return -1;
  }

  return 0;
}
