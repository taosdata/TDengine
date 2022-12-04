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

#include "parInsertUtil.h"
#include "parInt.h"
#include "parToken.h"
#include "ttime.h"

int32_t qCreateSName(SName* pName, const char* pTableName, int32_t acctId, char* dbName, char* msgBuf,
                     int32_t msgBufLen) {
  SMsgBuf msg = {.buf = msgBuf, .len = msgBufLen};
  SToken  sToken;
  int32_t code = 0;
  char*   tbName = NULL;

  NEXT_TOKEN(pTableName, sToken);

  if (sToken.n == 0) {
    return buildInvalidOperationMsg(&msg, "empty table name");
  }

  code = insCreateSName(pName, &sToken, acctId, dbName, &msg);
  if (code) {
    return code;
  }

  NEXT_TOKEN(pTableName, sToken);

  if (sToken.n > 0) {
    return buildInvalidOperationMsg(&msg, "table name format is wrong");
  }

  return TSDB_CODE_SUCCESS;
}

typedef struct SmlExecTableHandle {
  SParsedDataColInfo tags;          // each table
  SVCreateTbReq      createTblReq;  // each table
} SmlExecTableHandle;

typedef struct SmlExecHandle {
  SHashObj*          pBlockHash;
  SmlExecTableHandle tableExecHandle;
  SQuery*            pQuery;
} SSmlExecHandle;

static void smlDestroyTableHandle(void* pHandle) {
  SmlExecTableHandle* handle = (SmlExecTableHandle*)pHandle;
  destroyBoundColumnInfo(&handle->tags);
  tdDestroySVCreateTbReq(&handle->createTblReq);
}

static int32_t smlBoundColumnData(SArray* cols, SParsedDataColInfo* pColList, SSchema* pSchema, bool isTag) {
  col_id_t nCols = pColList->numOfCols;

  pColList->numOfBound = 0;
  pColList->boundNullLen = 0;
  memset(pColList->boundColumns, 0, sizeof(col_id_t) * nCols);
  for (col_id_t i = 0; i < nCols; ++i) {
    pColList->cols[i].valStat = VAL_STAT_NONE;
  }

  bool     isOrdered = true;
  col_id_t lastColIdx = -1;  // last column found
  for (int i = 0; i < taosArrayGetSize(cols); ++i) {
    SSmlKv*  kv = taosArrayGetP(cols, i);
    SToken   sToken = {.n = kv->keyLen, .z = (char*)kv->key};
    col_id_t t = lastColIdx + 1;
    col_id_t index = ((t == 0 && !isTag) ? 0 : insFindCol(&sToken, t, nCols, pSchema));
    uDebug("SML, index:%d, t:%d, ncols:%d", index, t, nCols);
    if (index < 0 && t > 0) {
      index = insFindCol(&sToken, 0, t, pSchema);
      isOrdered = false;
    }
    if (index < 0) {
      uError("smlBoundColumnData. index:%d", index);
      return TSDB_CODE_SML_INVALID_DATA;
    }
    if (pColList->cols[index].valStat == VAL_STAT_HAS) {
      uError("smlBoundColumnData. already set. index:%d", index);
      return TSDB_CODE_SML_INVALID_DATA;
    }
    lastColIdx = index;
    pColList->cols[index].valStat = VAL_STAT_HAS;
    pColList->boundColumns[pColList->numOfBound] = index;
    ++pColList->numOfBound;
    switch (pSchema[t].type) {
      case TSDB_DATA_TYPE_BINARY:
        pColList->boundNullLen += (sizeof(VarDataOffsetT) + VARSTR_HEADER_SIZE + CHAR_BYTES);
        break;
      case TSDB_DATA_TYPE_NCHAR:
        pColList->boundNullLen += (sizeof(VarDataOffsetT) + VARSTR_HEADER_SIZE + TSDB_NCHAR_SIZE);
        break;
      default:
        pColList->boundNullLen += TYPE_BYTES[pSchema[t].type];
        break;
    }
  }

  pColList->orderStatus = isOrdered ? ORDER_STATUS_ORDERED : ORDER_STATUS_DISORDERED;

  if (!isOrdered) {
    pColList->colIdxInfo = taosMemoryCalloc(pColList->numOfBound, sizeof(SBoundIdxInfo));
    if (NULL == pColList->colIdxInfo) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    SBoundIdxInfo* pColIdx = pColList->colIdxInfo;
    for (col_id_t i = 0; i < pColList->numOfBound; ++i) {
      pColIdx[i].schemaColIdx = pColList->boundColumns[i];
      pColIdx[i].boundIdx = i;
    }
    taosSort(pColIdx, pColList->numOfBound, sizeof(SBoundIdxInfo), insSchemaIdxCompar);
    for (col_id_t i = 0; i < pColList->numOfBound; ++i) {
      pColIdx[i].finalIdx = i;
    }
    taosSort(pColIdx, pColList->numOfBound, sizeof(SBoundIdxInfo), insBoundIdxCompar);
  }

  if (pColList->numOfCols > pColList->numOfBound) {
    memset(&pColList->boundColumns[pColList->numOfBound], 0,
           sizeof(col_id_t) * (pColList->numOfCols - pColList->numOfBound));
  }

  return TSDB_CODE_SUCCESS;
}

/**
 * @brief No json tag for schemaless
 *
 * @param cols
 * @param tags
 * @param pSchema
 * @param ppTag
 * @param msg
 * @return int32_t
 */
static int32_t smlBuildTagRow(SArray* cols, SParsedDataColInfo* tags, SSchema* pSchema, STag** ppTag, SArray** tagName,
                              SMsgBuf* msg) {
  SArray* pTagArray = taosArrayInit(tags->numOfBound, sizeof(STagVal));
  if (!pTagArray) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  *tagName = taosArrayInit(8, TSDB_COL_NAME_LEN);
  if (!*tagName) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  for (int i = 0; i < tags->numOfBound; ++i) {
    SSchema* pTagSchema = &pSchema[tags->boundColumns[i]];
    SSmlKv*  kv = taosArrayGetP(cols, i);

    taosArrayPush(*tagName, pTagSchema->name);
    STagVal val = {.cid = pTagSchema->colId, .type = pTagSchema->type};
    //    strcpy(val.colName, pTagSchema->name);
    if (pTagSchema->type == TSDB_DATA_TYPE_BINARY) {
      val.pData = (uint8_t*)kv->value;
      val.nData = kv->length;
    } else if (pTagSchema->type == TSDB_DATA_TYPE_NCHAR) {
      int32_t output = 0;
      void*   p = taosMemoryCalloc(1, kv->length * TSDB_NCHAR_SIZE);
      if (p == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto end;
      }
      if (!taosMbsToUcs4(kv->value, kv->length, (TdUcs4*)(p), kv->length * TSDB_NCHAR_SIZE, &output)) {
        if (errno == E2BIG) {
          taosMemoryFree(p);
          code = generateSyntaxErrMsg(msg, TSDB_CODE_PAR_VALUE_TOO_LONG, pTagSchema->name);
          goto end;
        }
        char buf[512] = {0};
        snprintf(buf, tListLen(buf), " taosMbsToUcs4 error:%s", strerror(errno));
        taosMemoryFree(p);
        code = buildSyntaxErrMsg(msg, buf, kv->value);
        goto end;
      }
      val.pData = p;
      val.nData = output;
    } else {
      memcpy(&val.i64, &(kv->value), kv->length);
    }
    taosArrayPush(pTagArray, &val);
  }

  code = tTagNew(pTagArray, 1, false, ppTag);
end:
  for (int i = 0; i < taosArrayGetSize(pTagArray); ++i) {
    STagVal* p = (STagVal*)taosArrayGet(pTagArray, i);
    if (p->type == TSDB_DATA_TYPE_NCHAR) {
      taosMemoryFree(p->pData);
    }
  }
  taosArrayDestroy(pTagArray);
  return code;
}

int32_t smlBindData(void* handle, SArray* tags, SArray* colsSchema, SArray* cols, bool format, STableMeta* pTableMeta,
                    char* tableName, const char* sTableName, int32_t sTableNameLen, int32_t ttl, char* msgBuf, int16_t msgBufLen) {
  SMsgBuf pBuf = {.buf = msgBuf, .len = msgBufLen};

  SSmlExecHandle* smlHandle = (SSmlExecHandle*)handle;
  smlDestroyTableHandle(&smlHandle->tableExecHandle);  // free for each table
  SSchema* pTagsSchema = getTableTagSchema(pTableMeta);
  insSetBoundColumnInfo(&smlHandle->tableExecHandle.tags, pTagsSchema, getNumOfTags(pTableMeta));
  int ret = smlBoundColumnData(tags, &smlHandle->tableExecHandle.tags, pTagsSchema, true);
  if (ret != TSDB_CODE_SUCCESS) {
    buildInvalidOperationMsg(&pBuf, "bound tags error");
    return ret;
  }
  STag*   pTag = NULL;
  SArray* tagName = NULL;
  ret = smlBuildTagRow(tags, &smlHandle->tableExecHandle.tags, pTagsSchema, &pTag, &tagName, &pBuf);
  if (ret != TSDB_CODE_SUCCESS) {
    taosArrayDestroy(tagName);
    return ret;
  }

  insBuildCreateTbReq(&smlHandle->tableExecHandle.createTblReq, tableName, pTag, pTableMeta->suid, NULL, tagName,
                      pTableMeta->tableInfo.numOfTags, ttl);
  taosArrayDestroy(tagName);

  smlHandle->tableExecHandle.createTblReq.ctb.stbName = taosMemoryMalloc(sTableNameLen + 1);
  memcpy(smlHandle->tableExecHandle.createTblReq.ctb.stbName, sTableName, sTableNameLen);
  smlHandle->tableExecHandle.createTblReq.ctb.stbName[sTableNameLen] = 0;

  STableDataBlocks* pDataBlock = NULL;
  ret = insGetDataBlockFromList(smlHandle->pBlockHash, &pTableMeta->uid, sizeof(pTableMeta->uid),
                                TSDB_DEFAULT_PAYLOAD_SIZE, sizeof(SSubmitBlk), getTableInfo(pTableMeta).rowSize,
                                pTableMeta, &pDataBlock, NULL, &smlHandle->tableExecHandle.createTblReq);
  if (ret != TSDB_CODE_SUCCESS) {
    buildInvalidOperationMsg(&pBuf, "create data block error");
    return ret;
  }

  SSchema* pSchema = getTableColumnSchema(pTableMeta);

  ret = smlBoundColumnData(colsSchema, &pDataBlock->boundColumnInfo, pSchema, false);
  if (ret != TSDB_CODE_SUCCESS) {
    buildInvalidOperationMsg(&pBuf, "bound cols error");
    return ret;
  }
  int32_t             extendedRowSize = insGetExtendedRowSize(pDataBlock);
  SParsedDataColInfo* spd = &pDataBlock->boundColumnInfo;
  SRowBuilder*        pBuilder = &pDataBlock->rowBuilder;
  SMemParam           param = {.rb = pBuilder};

  insInitRowBuilder(&pDataBlock->rowBuilder, pDataBlock->pTableMeta->sversion, &pDataBlock->boundColumnInfo);

  int32_t rowNum = taosArrayGetSize(cols);
  if (rowNum <= 0) {
    return buildInvalidOperationMsg(&pBuf, "cols size <= 0");
  }
  ret = insAllocateMemForSize(pDataBlock, extendedRowSize * rowNum);
  if (ret != TSDB_CODE_SUCCESS) {
    buildInvalidOperationMsg(&pBuf, "allocate memory error");
    return ret;
  }
  for (int32_t r = 0; r < rowNum; ++r) {
    STSRow* row = (STSRow*)(pDataBlock->pData + pDataBlock->size);  // skip the SSubmitBlk header
    tdSRowResetBuf(pBuilder, row);
    void*  rowData = taosArrayGetP(cols, r);
    size_t rowDataSize = 0;
    if (format) {
      rowDataSize = taosArrayGetSize(rowData);
    }

    // 1. set the parsed value from sql string
    for (int c = 0, j = 0; c < spd->numOfBound; ++c) {
      SSchema* pColSchema = &pSchema[spd->boundColumns[c]];

      param.schema = pColSchema;
      insGetSTSRowAppendInfo(pBuilder->rowType, spd, c, &param.toffset, &param.colIdx);

      SSmlKv* kv = NULL;
      if (format) {
        if (j < rowDataSize) {
          kv = taosArrayGetP(rowData, j);
          if (rowDataSize != spd->numOfBound && j != 0 &&
              (kv->keyLen != strlen(pColSchema->name) || strncmp(kv->key, pColSchema->name, kv->keyLen) != 0)) {
            kv = NULL;
          } else {
            j++;
          }
        }
      } else {
        void** p = taosHashGet(rowData, pColSchema->name, strlen(pColSchema->name));
        if (p) kv = *p;
      }

      if (kv) {
        int32_t colLen = kv->length;
        if (pColSchema->type == TSDB_DATA_TYPE_TIMESTAMP) {
          uDebug("SML:data before:%" PRId64 ", precision:%d", kv->i, pTableMeta->tableInfo.precision);
          kv->i = convertTimePrecision(kv->i, TSDB_TIME_PRECISION_NANO, pTableMeta->tableInfo.precision);
          uDebug("SML:data after:%" PRId64 ", precision:%d", kv->i, pTableMeta->tableInfo.precision);
        }

        if (IS_VAR_DATA_TYPE(kv->type)) {
          insMemRowAppend(&pBuf, kv->value, colLen, &param);
        } else {
          insMemRowAppend(&pBuf, &(kv->value), colLen, &param);
        }
      } else {
        pBuilder->hasNone = true;
      }

      if (PRIMARYKEY_TIMESTAMP_COL_ID == pColSchema->colId) {
        TSKEY tsKey = TD_ROW_KEY(row);
        insCheckTimestamp(pDataBlock, (const char*)&tsKey);
      }
    }

    // set the null value for the columns that do not assign values
    if ((spd->numOfBound < spd->numOfCols) && TD_IS_TP_ROW(row)) {
      pBuilder->hasNone = true;
    }

    tdSRowEnd(pBuilder);
    pDataBlock->size += extendedRowSize;
  }

  SSubmitBlk* pBlocks = (SSubmitBlk*)(pDataBlock->pData);
  return insSetBlockInfo(pBlocks, pDataBlock, rowNum, &pBuf);
}

void* smlInitHandle(SQuery* pQuery) {
  SSmlExecHandle* handle = taosMemoryCalloc(1, sizeof(SSmlExecHandle));
  if (!handle) return NULL;
  handle->pBlockHash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, false);
  handle->pQuery = pQuery;

  return handle;
}

void smlDestroyHandle(void* pHandle) {
  if (!pHandle) return;
  SSmlExecHandle* handle = (SSmlExecHandle*)pHandle;
  insDestroyBlockHashmap(handle->pBlockHash);
  smlDestroyTableHandle(&handle->tableExecHandle);
  taosMemoryFree(handle);
}

int32_t smlBuildOutput(void* handle, SHashObj* pVgHash) {
  SSmlExecHandle* smlHandle = (SSmlExecHandle*)handle;
  return qBuildStmtOutput(smlHandle->pQuery, pVgHash, smlHandle->pBlockHash);
}
