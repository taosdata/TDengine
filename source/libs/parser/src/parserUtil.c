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

#include "parserUtil.h"
#include <tglobal.h>
#include <ttime.h>
#include "function.h"
#include "parser.h"
#include "parserInt.h"
#include "queryInfoUtil.h"
#include "taoserror.h"
#include "tbuffer.h"
#include "thash.h"
#include "tmsg.h"
#include "tmsgtype.h"
#include "ttypes.h"
#include "tutil.h"

typedef struct STableFilterCond {
  uint64_t uid;
  int16_t  idx;  //table index
  int32_t  len;  // length of tag query condition data
  char *   cond;
} STableFilterCond;

static STableMetaInfo* addTableMetaInfo(SQueryStmtInfo* pQueryInfo, SName* name, STableMeta* pTableMeta,
                                        SVgroupsInfo* vgroupList, SArray* pTagCols, SArray* pVgroupTables);

int32_t parserValidateIdToken(SToken* pToken) {
  if (pToken == NULL || pToken->z == NULL || pToken->type != TK_ID) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  // it is a token quoted with escape char '`'
  if (pToken->z[0] == TS_ESCAPE_CHAR && pToken->z[pToken->n - 1] == TS_ESCAPE_CHAR) {
    return TSDB_CODE_SUCCESS;
  }

  char* sep = strnchr(pToken->z, TS_PATH_DELIMITER[0], pToken->n, true);
  if (sep == NULL) {  // It is a single part token, not a complex type
    if (isNumber(pToken)) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    strntolower(pToken->z, pToken->z, pToken->n);
  } else {  // two part
    int32_t oldLen = pToken->n;
    char*   pStr = pToken->z;

    if (pToken->type == TK_SPACE) {
      pToken->n = (uint32_t)strtrim(pToken->z);
    }

    pToken->n = tGetToken(pToken->z, &pToken->type);
    if (pToken->z[pToken->n] != TS_PATH_DELIMITER[0]) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if (pToken->type != TK_ID) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    int32_t firstPartLen = pToken->n;

    pToken->z = sep + 1;
    pToken->n = (uint32_t)(oldLen - (sep - pStr) - 1);
    int32_t len = tGetToken(pToken->z, &pToken->type);
    if (len != pToken->n || pToken->type != TK_ID) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    // re-build the whole name string
    if (pStr[firstPartLen] == TS_PATH_DELIMITER[0]) {
      // first part do not have quote do nothing
    } else {
      pStr[firstPartLen] = TS_PATH_DELIMITER[0];
      memmove(&pStr[firstPartLen + 1], pToken->z, pToken->n);
      uint32_t offset = (uint32_t)(pToken->z - (pStr + firstPartLen + 1));
      memset(pToken->z + pToken->n - offset, ' ', offset);
    }

    pToken->n += (firstPartLen + sizeof(TS_PATH_DELIMITER[0]));
    pToken->z = pStr;

    strntolower(pToken->z, pToken->z, pToken->n);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t parserValidatePassword(SToken* pToken, SMsgBuf* pMsgBuf) {
  const char* msg1 = "password can not be empty";
  const char* msg2 = "name or password too long";
  const char* msg3 = "password needs single quote marks enclosed";

  if (pToken->type != TK_STRING) {
    return buildInvalidOperationMsg(pMsgBuf, msg3);
  }

  strdequote(pToken->z);

  pToken->n = (uint32_t)strtrim(pToken->z);  // trim space before and after passwords
  if (pToken->n <= 0) {
    return buildInvalidOperationMsg(pMsgBuf, msg1);
  }

  if (pToken->n >= TSDB_USET_PASSWORD_LEN) {
    return buildInvalidOperationMsg(pMsgBuf, msg2);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t parserValidateNameToken(SToken* pToken) {
  if (pToken == NULL || pToken->z == NULL || pToken->type != TK_ID || pToken->n == 0) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  // it is a token quoted with escape char '`'
  if (pToken->z[0] == TS_ESCAPE_CHAR && pToken->z[pToken->n - 1] == TS_ESCAPE_CHAR) {
    pToken->n = strdequote(pToken->z);
    return TSDB_CODE_SUCCESS;
  }

  char* sep = strnchr(pToken->z, TS_PATH_DELIMITER[0], pToken->n, true);
  if (sep != NULL) {  // It is a complex type, not allow
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  strntolower(pToken->z, pToken->z, pToken->n);
  return TSDB_CODE_SUCCESS;
}

int32_t buildInvalidOperationMsg(SMsgBuf* pBuf, const char* msg) {
  strncpy(pBuf->buf, msg, pBuf->len);
  return TSDB_CODE_TSC_INVALID_OPERATION;
}

int32_t buildSyntaxErrMsg(SMsgBuf* pBuf, const char* additionalInfo, const char* sourceStr) {
  const char* msgFormat1 = "syntax error near \'%s\'";
  const char* msgFormat2 = "syntax error near \'%s\' (%s)";
  const char* msgFormat3 = "%s";

  const char* prefix = "syntax error";
  if (sourceStr == NULL) {
    assert(additionalInfo != NULL);
    snprintf(pBuf->buf, pBuf->len, msgFormat1, additionalInfo);
    return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
  }

  char buf[64] = {0};  // only extract part of sql string
  strncpy(buf, sourceStr, tListLen(buf) - 1);

  if (additionalInfo != NULL) {
    snprintf(pBuf->buf, pBuf->len, msgFormat2, buf, additionalInfo);
  } else {
    const char* msgFormat = (0 == strncmp(sourceStr, prefix, strlen(prefix))) ? msgFormat3 : msgFormat1;
    snprintf(pBuf->buf, pBuf->len, msgFormat, buf);
  }

  return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
}

SCond* getSTableQueryCond(STagCond* pTagCond, uint64_t uid) {
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

STableFilterCond* tsGetTableFilter(SArray* filters, uint64_t uid, int16_t idx) {
  if (filters == NULL) {
    return NULL;
  }

  size_t size = taosArrayGetSize(filters);
  for (int32_t i = 0; i < size; ++i) {
    STableFilterCond* cond = taosArrayGet(filters, i);

    if (uid == cond->uid && (idx >= 0 && cond->idx == idx)) {
      return cond;
    }
  }

  return NULL;
}

void setSTableQueryCond(STagCond* pTagCond, uint64_t uid, SBufferWriter* bw) {
  if (tbufTell(bw) == 0) {
    return;
  }

  SCond cond = {
      .uid = uid,
      .len = (int32_t)(tbufTell(bw)),
      .cond = NULL,
  };

  cond.cond = tbufGetData(bw, true);

  if (pTagCond->pCond == NULL) {
    pTagCond->pCond = taosArrayInit(3, sizeof(SCond));
  }

  taosArrayPush(pTagCond->pCond, &cond);
}

//typedef struct SJoinStatus {
//  SSDataBlock* pBlock;   // point to the upstream block
//  int32_t      index;
//  bool         completed;// current upstream is completed or not
//} SJoinStatus;

/*
static void createInputDataFilterInfo(SQueryStmtInfo* px, int32_t numOfCol1, int32_t* numOfFilterCols, SSingleColumnFilterInfo** pFilterInfo) {
  SColumnInfo* tableCols = calloc(numOfCol1, sizeof(SColumnInfo));
  for(int32_t i = 0; i < numOfCol1; ++i) {
    SColumn* pCol = taosArrayGetP(px->colList, i);
    if (pCol->info.flist.numOfFilters > 0) {
      (*numOfFilterCols) += 1;
    }

    tableCols[i] = pCol->info;
  }

  if ((*numOfFilterCols) > 0) {
    doCreateFilterInfo(tableCols, numOfCol1, (*numOfFilterCols), pFilterInfo, 0);
  }

  tfree(tableCols);
}
*/

//void destroyTableNameList(SInsertStatementParam* pInsertParam) {
//  if (pInsertParam->numOfTables == 0) {
//    assert(pInsertParam->pTableNameList == NULL);
//    return;
//  }
//
//  for(int32_t i = 0; i < pInsertParam->numOfTables; ++i) {
//    tfree(pInsertParam->pTableNameList[i]);
//  }
//
//  pInsertParam->numOfTables = 0;
//  tfree(pInsertParam->pTableNameList);
//}

//void tscDestroyBoundColumnInfo(SParsedDataColInfo* pColInfo) {
//  tfree(pColInfo->boundedColumns);
//  tfree(pColInfo->cols);
//  tfree(pColInfo->colIdxInfo);
//}
//
//void tscDestroyDataBlock(STableDataBlocks* pDataBlock, bool removeMeta) {
//  if (pDataBlock == NULL) {
//    return;
//  }
//
//  tfree(pDataBlock->pData);
//
//  if (removeMeta) {
//    char name[TSDB_TABLE_FNAME_LEN] = {0};
//    tNameExtractFullName(&pDataBlock->tableName, name);
//
//    taosHashRemove(tscTableMetaMap, name, strnlen(name, TSDB_TABLE_FNAME_LEN));
//  }
//
//  if (!pDataBlock->cloned) {
//    tfree(pDataBlock->params);
//
//    // free the refcount for metermeta
//    if (pDataBlock->pTableMeta != NULL) {
//      tfree(pDataBlock->pTableMeta);
//    }
//
//    tscDestroyBoundColumnInfo(&pDataBlock->boundColumnInfo);
//  }
//
//  tfree(pDataBlock);
//}

//SParamInfo* tscAddParamToDataBlock(STableDataBlocks* pDataBlock, char type, uint8_t timePrec, int16_t bytes,
//                                   uint32_t offset) {
//  uint32_t needed = pDataBlock->numOfParams + 1;
//  if (needed > pDataBlock->numOfAllocedParams) {
//    needed *= 2;
//    void* tmp = realloc(pDataBlock->params, needed * sizeof(SParamInfo));
//    if (tmp == NULL) {
//      return NULL;
//    }
//    pDataBlock->params = (SParamInfo*)tmp;
//    pDataBlock->numOfAllocedParams = needed;
//  }
//
//  SParamInfo* param = pDataBlock->params + pDataBlock->numOfParams;
//  param->idx = -1;
//  param->type = type;
//  param->timePrec = timePrec;
//  param->bytes = bytes;
//  param->offset = offset;
//
//  ++pDataBlock->numOfParams;
//  return param;
//}

//void*  tscDestroyBlockArrayList(SArray* pDataBlockList) {
//  if (pDataBlockList == NULL) {
//    return NULL;
//  }
//
//  size_t size = taosArrayGetSize(pDataBlockList);
//  for (int32_t i = 0; i < size; i++) {
//    void* d = taosArrayGetP(pDataBlockList, i);
//    tscDestroyDataBlock(d, false);
//  }
//
//  taosArrayDestroy(pDataBlockList);
//  return NULL;
//}


//void freeUdfInfo(SUdfInfo* pUdfInfo) {
//  if (pUdfInfo == NULL) {
//    return;
//  }
//
//  if (pUdfInfo->funcs[TSDB_UDF_FUNC_DESTROY]) {
//    (*(udfDestroyFunc)pUdfInfo->funcs[TSDB_UDF_FUNC_DESTROY])(&pUdfInfo->init);
//  }
//
//  tfree(pUdfInfo->name);
//
//  if (pUdfInfo->path) {
//    unlink(pUdfInfo->path);
//  }
//
//  tfree(pUdfInfo->path);
//
//  tfree(pUdfInfo->content);
//
//  taosCloseDll(pUdfInfo->handle);
//}

//void*  tscDestroyUdfArrayList(SArray* pUdfList) {
//  if (pUdfList == NULL) {
//    return NULL;
//  }
//
//  size_t size = taosArrayGetSize(pUdfList);
//  for (int32_t i = 0; i < size; i++) {
//    SUdfInfo* udf = taosArrayGet(pUdfList, i);
//    freeUdfInfo(udf);
//  }
//
//  taosArrayDestroy(pUdfList);
//  return NULL;
//}

//void* tscDestroyBlockHashTable(SHashObj* pBlockHashTable, bool removeMeta) {
//  if (pBlockHashTable == NULL) {
//    return NULL;
//  }
//
//  STableDataBlocks** p = taosHashIterate(pBlockHashTable, NULL);
//  while(p) {
//    tscDestroyDataBlock(*p, removeMeta);
//    p = taosHashIterate(pBlockHashTable, p);
//  }
//
//  taosHashCleanup(pBlockHashTable);
//  return NULL;
//}

/**
 * create the in-memory buffer for each table to keep the submitted data block
 * @param initialSize
 * @param rowSize
 * @param startOffset
 * @param name
 * @param dataBlocks
 * @return
 */
//  int32_t tscCreateDataBlock(size_t defaultSize, int32_t rowSize, int32_t startOffset, SName* name,
//                           STableMeta* pTableMeta, STableDataBlocks** dataBlocks) {
//  STableDataBlocks* dataBuf = (STableDataBlocks*)calloc(1, sizeof(STableDataBlocks));
//  if (dataBuf == NULL) {
//    tscError("failed to allocated memory, reason:%s", strerror(errno));
//    return TSDB_CODE_TSC_OUT_OF_MEMORY;
//  }
//
//  dataBuf->nAllocSize = (uint32_t)defaultSize;
//  dataBuf->headerSize = startOffset;
//
//  // the header size will always be the startOffset value, reserved for the subumit block header
//  if (dataBuf->nAllocSize <= dataBuf->headerSize) {
//    dataBuf->nAllocSize = dataBuf->headerSize * 2;
//  }
//
//  //dataBuf->pData = calloc(1, dataBuf->nAllocSize);
//  dataBuf->pData = malloc(dataBuf->nAllocSize);
//  if (dataBuf->pData == NULL) {
//    tscError("failed to allocated memory, reason:%s", strerror(errno));
//    tfree(dataBuf);
//    return TSDB_CODE_TSC_OUT_OF_MEMORY;
//  }
//  memset(dataBuf->pData, 0, sizeof(SSubmitBlk));
//
//  //Here we keep the tableMeta to avoid it to be remove by other threads.
//  dataBuf->pTableMeta = tscTableMetaDup(pTableMeta);
//
//  SParsedDataColInfo* pColInfo = &dataBuf->boundColumnInfo;
//  SSchema* pSchema = getTableColumnSchema(dataBuf->pTableMeta);
//  tscSetBoundColumnInfo(pColInfo, pSchema, dataBuf->pTableMeta->tableInfo.numOfColumns);
//
//  dataBuf->ordered  = true;
//  dataBuf->prevTS   = INT64_MIN;
//  dataBuf->rowSize  = rowSize;
//  dataBuf->size     = startOffset;
//  dataBuf->tsSource = -1;
//  dataBuf->vgId     = dataBuf->pTableMeta->vgId;
//
//  tNameAssign(&dataBuf->tableName, name);
//
//  assert(defaultSize > 0 && pTableMeta != NULL && dataBuf->pTableMeta != NULL);
//
//  *dataBlocks = dataBuf;
//  return TSDB_CODE_SUCCESS;
//}
//
//  int32_t tscGetDataBlockFromList(SHashObj* pHashList, int64_t id, int32_t size, int32_t startOffset, int32_t rowSize,
//                                SName* name, STableMeta* pTableMeta, STableDataBlocks** dataBlocks,
//                                SArray* pBlockList) {
//  *dataBlocks = NULL;
//  STableDataBlocks** t1 = (STableDataBlocks**)taosHashGet(pHashList, (const char*)&id, sizeof(id));
//  if (t1 != NULL) {
//    *dataBlocks = *t1;
//  }
//
//  if (*dataBlocks == NULL) {
//    int32_t ret = tscCreateDataBlock((size_t)size, rowSize, startOffset, name, pTableMeta, dataBlocks);
//    if (ret != TSDB_CODE_SUCCESS) {
//      return ret;
//    }
//
//    taosHashPut(pHashList, (const char*)&id, sizeof(int64_t), (char*)dataBlocks, POINTER_BYTES);
//    if (pBlockList) {
//      taosArrayPush(pBlockList, dataBlocks);
//    }
//  }
//
//  return TSDB_CODE_SUCCESS;
//}
//
//// Erase the empty space reserved for binary data
//  static int trimDataBlock(void* pDataBlock, STableDataBlocks* pTableDataBlock, SInsertStatementParam* insertParam,
//                         SBlockKeyTuple* blkKeyTuple) {
//  // TODO: optimize this function, handle the case while binary is not presented
//  STableMeta*     pTableMeta = pTableDataBlock->pTableMeta;
//  STableComInfo   tinfo = tscGetTableInfo(pTableMeta);
//  SSchema*        pSchema = getTableColumnSchema(pTableMeta);
//
//  SSubmitBlk* pBlock = pDataBlock;
//  memcpy(pDataBlock, pTableDataBlock->pData, sizeof(SSubmitBlk));
//  pDataBlock = (char*)pDataBlock + sizeof(SSubmitBlk);
//
//  int32_t flen = 0;  // original total length of row
//
//  // schema needs to be included into the submit data block
//  if (insertParam->schemaAttached) {
//    int32_t numOfCols = tscGetNumOfColumns(pTableDataBlock->pTableMeta);
//    for(int32_t j = 0; j < numOfCols; ++j) {
//      STColumn* pCol = (STColumn*) pDataBlock;
//      pCol->colId = htons(pSchema[j].colId);
//      pCol->type  = pSchema[j].type;
//      pCol->bytes = htons(pSchema[j].bytes);
//      pCol->offset = 0;
//
//      pDataBlock = (char*)pDataBlock + sizeof(STColumn);
//      flen += TYPE_BYTES[pSchema[j].type];
//    }
//
//    int32_t schemaSize = sizeof(STColumn) * numOfCols;
//    pBlock->schemaLen = schemaSize;
//  } else {
//    if (IS_RAW_PAYLOAD(insertParam->payloadType)) {
//      for (int32_t j = 0; j < tinfo.numOfColumns; ++j) {
//        flen += TYPE_BYTES[pSchema[j].type];
//      }
//    }
//    pBlock->schemaLen = 0;
//  }
//
//  char* p = pTableDataBlock->pData + sizeof(SSubmitBlk);
//  pBlock->dataLen = 0;
//  int32_t numOfRows = htons(pBlock->numOfRows);
//
//  if (IS_RAW_PAYLOAD(insertParam->payloadType)) {
//    for (int32_t i = 0; i < numOfRows; ++i) {
//      STSRow* memRow = (STSRow*)pDataBlock;
//      memRowSetType(memRow, SMEM_ROW_DATA);
//      SDataRow trow = memRowDataBody(memRow);
//      dataRowSetLen(trow, (uint16_t)(TD_DATA_ROW_HEAD_SIZE + flen));
//      dataRowSetVersion(trow, pTableMeta->sversion);
//
//      int toffset = 0;
//      for (int32_t j = 0; j < tinfo.numOfColumns; j++) {
//        tdAppendColVal(trow, p, pSchema[j].type, toffset);
//        toffset += TYPE_BYTES[pSchema[j].type];
//        p += pSchema[j].bytes;
//      }
//
//      pDataBlock = (char*)pDataBlock + TD_ROW_LEN(memRow);
//      pBlock->dataLen += TD_ROW_LEN(memRow);
//    }
//  } else {
//    for (int32_t i = 0; i < numOfRows; ++i) {
//       char*      payload = (blkKeyTuple + i)->payloadAddr;
//       TDRowLenT rowTLen = TD_ROW_LEN(payload);
//       memcpy(pDataBlock, payload, rowTLen);
//       pDataBlock = POINTER_SHIFT(pDataBlock, rowTLen);
//       pBlock->dataLen += rowTLen;
//    }
//  }

//  int32_t len = pBlock->dataLen + pBlock->schemaLen;
//  pBlock->dataLen = htonl(pBlock->dataLen);
//  pBlock->schemaLen = htonl(pBlock->schemaLen);

//  return len;
// }

TAOS_FIELD createField(const SSchema* pSchema) {
  TAOS_FIELD f = { .type = pSchema->type, .bytes = pSchema->bytes, };
  tstrncpy(f.name, pSchema->name, sizeof(f.name));
  return f;
}

void setColumn(SColumn* pColumn, uint64_t uid, const char* tableName, int8_t flag, const SSchema* pSchema) {
  pColumn->uid  = uid;
  pColumn->flag = flag;
  pColumn->info.colId = pSchema->colId;
  pColumn->info.bytes = pSchema->bytes;
  pColumn->info.type  = pSchema->type;

  if (tableName != NULL) {
    char n[TSDB_COL_NAME_LEN + 1 + TSDB_TABLE_NAME_LEN] = {0};
    snprintf(n, tListLen(n), "%s.%s", tableName, pSchema->name);
    tstrncpy(pColumn->name, n, tListLen(pColumn->name));
  } else {
    tstrncpy(pColumn->name, pSchema->name, tListLen(pColumn->name));
  }
}

SColumn createColumn(uint64_t uid, const char* tableName, int8_t flag, const SSchema* pSchema) {
  SColumn c;
  c.uid = uid;
  c.flag = flag;
  c.info.colId = pSchema->colId;
  c.info.bytes = pSchema->bytes;
  c.info.type = pSchema->type;

  if (tableName != NULL) {
    char n[TSDB_COL_NAME_LEN + 1 + TSDB_TABLE_NAME_LEN] = {0};
    snprintf(n, tListLen(n), "%s.%s", tableName, pSchema->name);

    tstrncpy(c.name, n, tListLen(c.name));
  } else {
    tstrncpy(c.name, pSchema->name, tListLen(c.name));
  }

  return c;
}

void addIntoSourceParam(SSourceParam* pSourceParam, tExprNode* pNode, SColumn* pColumn) {
  assert(pSourceParam != NULL);
  pSourceParam->num += 1;

  if (pSourceParam->pExprNodeList != NULL) {
    assert(pNode != NULL && pColumn == NULL);
    if (pSourceParam->pExprNodeList == NULL) {
      pSourceParam->pExprNodeList = taosArrayInit(4, POINTER_BYTES);
    }

    taosArrayPush(pSourceParam->pExprNodeList, &pNode);
  } else {
    assert(pColumn != NULL);
    if (pSourceParam->pColumnList == NULL) {
      pSourceParam->pColumnList = taosArrayInit(4, POINTER_BYTES);
    }

    taosArrayPush(pSourceParam->pColumnList, &pColumn);
  }
}

int32_t getNumOfFields(SFieldInfo* pFieldInfo) {
  return pFieldInfo->numOfOutput;
}

SInternalField* appendFieldInfo(SFieldInfo* pFieldInfo, TAOS_FIELD* pField) {
  assert(pFieldInfo != NULL);
  pFieldInfo->numOfOutput++;

  struct SInternalField info = { .pExpr = NULL, .visible = true };

  info.field = *pField;
  return taosArrayPush(pFieldInfo->internalField, &info);
}

SInternalField* insertFieldInfo(SFieldInfo* pFieldInfo, int32_t index, SSchema* pSchema) {
  pFieldInfo->numOfOutput++;
  struct SInternalField info = { .pExpr = NULL, .visible = true };

  info.field.type = pSchema->type;
  info.field.bytes = pSchema->bytes;
  tstrncpy(info.field.name, pSchema->name, tListLen(pSchema->name));

  return taosArrayInsert(pFieldInfo->internalField, index, &info);
}

void fieldInfoUpdateOffset(SQueryStmtInfo* pQueryInfo) {
  int32_t offset = 0;
  size_t numOfExprs = getNumOfExprs(pQueryInfo);

  SArray* pList = getCurrentExprList(pQueryInfo);
  for (int32_t i = 0; i < numOfExprs; ++i) {
    SExprInfo* p = taosArrayGetP(pList, i);

//    p->base.offset = offset;
    offset += p->base.resSchema.bytes;
  }
}

SInternalField* getInternalField(SFieldInfo* pFieldInfo, int32_t index) {
  assert(index < pFieldInfo->numOfOutput);
  return TARRAY_GET_ELEM(pFieldInfo->internalField, index);
}

TAOS_FIELD* getFieldInfo(SFieldInfo* pFieldInfo, int32_t index) {
  assert(index < pFieldInfo->numOfOutput);
  return &((SInternalField*)TARRAY_GET_ELEM(pFieldInfo->internalField, index))->field;
}

int32_t fieldInfoCompare(const SFieldInfo* pFieldInfo1, const SFieldInfo* pFieldInfo2, int32_t *diffSize) {
  assert(pFieldInfo1 != NULL && pFieldInfo2 != NULL);

  if (pFieldInfo1->numOfOutput != pFieldInfo2->numOfOutput) {
    return pFieldInfo1->numOfOutput - pFieldInfo2->numOfOutput;
  }

  for (int32_t i = 0; i < pFieldInfo1->numOfOutput; ++i) {
    TAOS_FIELD* pField1 = getFieldInfo((SFieldInfo*) pFieldInfo1, i);
    TAOS_FIELD* pField2 = getFieldInfo((SFieldInfo*) pFieldInfo2, i);

    if (pField1->type != pField2->type ||
        strcasecmp(pField1->name, pField2->name) != 0) {
      return 1;
    }

    if (pField1->bytes != pField2->bytes) {
      *diffSize = 1;

      if (pField2->bytes > pField1->bytes) {
        assert(IS_VAR_DATA_TYPE(pField1->type));
        pField1->bytes = pField2->bytes;
      }
    }
  }

  return 0;
}

int32_t getFieldInfoSize(const SFieldInfo* pFieldInfo1, const SFieldInfo* pFieldInfo2) {
  assert(pFieldInfo1 != NULL && pFieldInfo2 != NULL);

  for (int32_t i = 0; i < pFieldInfo1->numOfOutput; ++i) {
    TAOS_FIELD* pField1 = getFieldInfo((SFieldInfo*) pFieldInfo1, i);
    TAOS_FIELD* pField2 = getFieldInfo((SFieldInfo*) pFieldInfo2, i);

    pField2->bytes = pField1->bytes;
  }

  return 0;
}

static void destroyFilterInfo(SColumnFilterList* pFilterList) {
  if (pFilterList->filterInfo == NULL) {
    pFilterList->numOfFilters = 0;
    return;
  }

  for(int32_t i = 0; i < pFilterList->numOfFilters; ++i) {
    if (pFilterList->filterInfo[i].filterstr) {
      tfree(pFilterList->filterInfo[i].pz);
    }
  }

  tfree(pFilterList->filterInfo);
  pFilterList->numOfFilters = 0;
}

void cleanupFieldInfo(SFieldInfo* pFieldInfo) {
  if (pFieldInfo == NULL) {
    return;
  }

  taosArrayDestroy(pFieldInfo->internalField);
  tfree(pFieldInfo->final);

  memset(pFieldInfo, 0, sizeof(SFieldInfo));
}

void copyFieldInfo(SFieldInfo* pFieldInfo, const SFieldInfo* pSrc, const SArray* pExprList) {
  assert(pFieldInfo != NULL && pSrc != NULL && pExprList != NULL);
  pFieldInfo->numOfOutput = pSrc->numOfOutput;

  if (pSrc->final != NULL) {
    pFieldInfo->final = calloc(pSrc->numOfOutput, sizeof(TAOS_FIELD));
    memcpy(pFieldInfo->final, pSrc->final, sizeof(TAOS_FIELD) * pSrc->numOfOutput);
  }

  if (pSrc->internalField != NULL) {
    size_t num = taosArrayGetSize(pSrc->internalField);
    size_t numOfExpr = taosArrayGetSize(pExprList);

    for (int32_t i = 0; i < num; ++i) {
      SInternalField* pfield = taosArrayGet(pSrc->internalField, i);

      SInternalField p = {.visible = pfield->visible, .field = pfield->field};

      bool found = false;
      int32_t resColId = pfield->pExpr->base.resSchema.colId;
      for(int32_t j = 0; j < numOfExpr; ++j) {
        SExprInfo* pExpr = taosArrayGetP(pExprList, j);
        if (pExpr->base.resSchema.colId == resColId) {
          p.pExpr = pExpr;
          found = true;
          break;
        }
      }

      if (!found) {
        assert(pfield->pExpr->pExpr != NULL);
        p.pExpr = calloc(1, sizeof(SExprInfo));
        assignExprInfo(p.pExpr, pfield->pExpr);
      }

      taosArrayPush(pFieldInfo->internalField, &p);
    }
  }
}

// ignore the tbname columnIndex to be inserted into source list
int32_t columnExists(SArray* pColumnList, int32_t columnId, uint64_t uid) {
  size_t numOfCols = taosArrayGetSize(pColumnList);

  int32_t i = 0;
  while (i < numOfCols) {
    SColumn* pCol = taosArrayGetP(pColumnList, i);
    if ((pCol->info.colId != columnId) || (pCol->uid != uid)) {
      ++i;
      continue;
    } else {
      break;
    }
  }

  if (i >= numOfCols || numOfCols == 0) {
    return -1;
  }

  return i;
}

static int32_t doFindPosition(const SArray* pColumnList, uint64_t uid, const SSchema* pSchema) {
  int32_t i = 0;

  size_t numOfCols = taosArrayGetSize(pColumnList);
  while (i < numOfCols) {
    SColumn* pCol = taosArrayGetP(pColumnList, i);
    if (pCol->uid < uid) {
      i++;
      continue;
    }

    if (pCol->info.colId < pSchema->colId) {
      i++;
      continue;
    }

    break;
  }

  return i;
}

SColumn* columnListInsert(SArray* pColumnList, uint64_t uid, SSchema* pSchema, int32_t flag) {
  // ignore the tbname columnIndex to be inserted into source list
  assert(pSchema != NULL && pColumnList != NULL);

  int32_t  i = doFindPosition(pColumnList, uid, pSchema);
  size_t size = taosArrayGetSize(pColumnList);
  if (size > 0 && i < size) {
    SColumn* pCol = taosArrayGetP(pColumnList, i);
    if (pCol->uid == uid && pCol->info.colId == pSchema->colId) {
      return pCol;
    }
  }

  SColumn* b = calloc(1, sizeof(SColumn));
  if (b == NULL) {
    return NULL;
  }

  b->uid        = uid;
  b->flag       = flag;
  b->info.colId = pSchema->colId;
  b->info.bytes = pSchema->bytes;
  b->info.type  = pSchema->type;
  tstrncpy(b->name, pSchema->name, tListLen(b->name));
  taosArrayInsert(pColumnList, i, &b);

  return b;
}

SColumn* insertPrimaryTsColumn(SArray* pColumnList, const char* colName, uint64_t tableUid) {
  SSchema s = {.type = TSDB_DATA_TYPE_TIMESTAMP, .bytes = TSDB_KEYSIZE, .colId = PRIMARYKEY_TIMESTAMP_COL_ID};
  strncpy(s.name, colName, tListLen(s.name));

  return columnListInsert(pColumnList, tableUid, &s, TSDB_COL_NORMAL);
}

void columnCopy(SColumn* pDest, const SColumn* pSrc);

SColumn* columnClone(const SColumn* src) {
  assert(src != NULL);

  SColumn* dst = calloc(1, sizeof(SColumn));
  if (dst == NULL) {
    return NULL;
  }

  columnCopy(dst, src);
  return dst;
}

SColumnFilterInfo* tFilterInfoDup(const SColumnFilterInfo* src, int32_t numOfFilters) {
  if (numOfFilters == 0 || src == NULL) {
    assert(src == NULL);
    return NULL;
  }

  SColumnFilterInfo* pFilter = calloc(1, numOfFilters * sizeof(SColumnFilterInfo));

  memcpy(pFilter, src, sizeof(SColumnFilterInfo) * numOfFilters);
  for (int32_t j = 0; j < numOfFilters; ++j) {
    if (pFilter[j].filterstr) {
      size_t len = (size_t) pFilter[j].len + 1 * TSDB_NCHAR_SIZE;
      pFilter[j].pz = (int64_t) calloc(1, len);

      memcpy((char*)pFilter[j].pz, (char*)src[j].pz, (size_t) pFilter[j].len);
    }
  }

  assert(src->filterstr == 0 || src->filterstr == 1);
  assert(!(src->lowerRelOptr == 0 && src->upperRelOptr == 0));

  return pFilter;
}

void columnCopy(SColumn* pDest, const SColumn* pSrc) {
  destroyFilterInfo(&pDest->info.flist);

  pDest->uid = pSrc->uid;
  pDest->info.flist.numOfFilters = pSrc->info.flist.numOfFilters;
  pDest->info.flist.filterInfo   = tFilterInfoDup(pSrc->info.flist.filterInfo, pSrc->info.flist.numOfFilters);
  pDest->info.type        = pSrc->info.type;
  pDest->info.colId        = pSrc->info.colId;
  pDest->info.bytes      = pSrc->info.bytes;
}

void columnListCopyAll(SArray* dst, const SArray* src) {
  assert(src != NULL && dst != NULL);

  size_t num = taosArrayGetSize(src);
  for (int32_t i = 0; i < num; ++i) {
    SColumn* pCol = taosArrayGetP(src, i);
    SColumn* p = columnClone(pCol);
    taosArrayPush(dst, &p);
  }
}

void columnListCopy(SArray* dst, const SArray* src, uint64_t uid) {
  assert(src != NULL && dst != NULL);

  size_t num = taosArrayGetSize(src);
  for (int32_t i = 0; i < num; ++i) {
    SColumn* pCol = taosArrayGetP(src, i);

    if (pCol->uid == uid) {
      SColumn* p = columnClone(pCol);
      taosArrayPush(dst, &p);
    }
  }
}

static void columnDestroy(SColumn* pCol) {
  destroyFilterInfo(&pCol->info.flist);
  free(pCol);
}

void columnListDestroy(SArray* pColumnList) {
  if (pColumnList == NULL) {
    return;
  }

  size_t num = taosArrayGetSize(pColumnList);
  for (int32_t i = 0; i < num; ++i) {
    SColumn* pCol = taosArrayGetP(pColumnList, i);
    columnDestroy(pCol);
  }

  taosArrayDestroy(pColumnList);
}

bool validateColumnId(STableMetaInfo* pTableMetaInfo, int32_t colId, int32_t numOfParams) {
  if (pTableMetaInfo->pTableMeta == NULL) {
    return false;
  }

  if (colId == TSDB_TBNAME_COLUMN_INDEX || (colId <= TSDB_UD_COLUMN_INDEX && numOfParams == 2)) {
    return true;
  }

  SSchema* pSchema = getTableColumnSchema(pTableMetaInfo->pTableMeta);
  STableComInfo tinfo = getTableInfo(pTableMetaInfo->pTableMeta);

  int32_t  numOfTotal = tinfo.numOfTags + tinfo.numOfColumns;

  for (int32_t i = 0; i < numOfTotal; ++i) {
    if (pSchema[i].colId == colId) {
      return true;
    }
  }

  return false;
}

int32_t tscTagCondCopy(STagCond* dest, const STagCond* src) {
  memset(dest, 0, sizeof(STagCond));

  if (src->tbnameCond.cond != NULL) {
    dest->tbnameCond.cond = strdup(src->tbnameCond.cond);
    if (dest->tbnameCond.cond == NULL) {
      return -1;
    }
  }

  dest->tbnameCond.uid = src->tbnameCond.uid;
  dest->tbnameCond.len = src->tbnameCond.len;

  dest->joinInfo.hasJoin = src->joinInfo.hasJoin;

  for (int32_t i = 0; i < TSDB_MAX_JOIN_TABLE_NUM; ++i) {
    if (src->joinInfo.joinTables[i]) {
      dest->joinInfo.joinTables[i] = calloc(1, sizeof(SJoinNode));

      memcpy(dest->joinInfo.joinTables[i], src->joinInfo.joinTables[i], sizeof(SJoinNode));

      if (src->joinInfo.joinTables[i]->tsJoin) {
        dest->joinInfo.joinTables[i]->tsJoin = taosArrayDup(src->joinInfo.joinTables[i]->tsJoin);
      }

      if (src->joinInfo.joinTables[i]->tagJoin) {
        dest->joinInfo.joinTables[i]->tagJoin = taosArrayDup(src->joinInfo.joinTables[i]->tagJoin);
      }
    }
  }


  dest->relType = src->relType;

  if (src->pCond == NULL) {
    return 0;
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
      if (c.cond == NULL) {
        return -1;
      }

      memcpy(c.cond, pCond->cond, c.len);
    }

    taosArrayPush(dest->pCond, &c);
  }

  return 0;
}

int32_t tscColCondCopy(SArray** dest, const SArray* src, uint64_t uid, int16_t tidx) {
  if (src == NULL) {
    return 0;
  }

  size_t s = taosArrayGetSize(src);
  *dest = taosArrayInit(s, sizeof(SCond));

  for (int32_t i = 0; i < s; ++i) {
    STableFilterCond* pCond = taosArrayGet(src, i);
    STableFilterCond c = {0};

    if (tidx > 0) {
      if (!(pCond->uid == uid && pCond->idx == tidx)) {
        continue;
      }

      c.idx = 0;
    } else {
      c.idx = pCond->idx;
    }

    c.len = pCond->len;
    c.uid = pCond->uid;

    if (pCond->len > 0) {
      assert(pCond->cond != NULL);
      c.cond = malloc(c.len);
      if (c.cond == NULL) {
        return -1;
      }

      memcpy(c.cond, pCond->cond, c.len);
    }

    taosArrayPush(*dest, &c);
  }

  return 0;
}

void cleanupColumnCond(SArray** pCond) {
  if (*pCond == NULL) {
    return;
  }

  size_t s = taosArrayGetSize(*pCond);
  for (int32_t i = 0; i < s; ++i) {
    STableFilterCond* p = taosArrayGet(*pCond, i);
    tfree(p->cond);
  }

  taosArrayDestroy(*pCond);

  *pCond = NULL;
}

void cleanupTagCond(STagCond* pTagCond) {
  free(pTagCond->tbnameCond.cond);

  if (pTagCond->pCond != NULL) {
    size_t s = taosArrayGetSize(pTagCond->pCond);
    for (int32_t i = 0; i < s; ++i) {
      SCond* p = taosArrayGet(pTagCond->pCond, i);
      tfree(p->cond);
    }

    taosArrayDestroy(pTagCond->pCond);
  }

  for (int32_t i = 0; i < TSDB_MAX_JOIN_TABLE_NUM; ++i) {
    SJoinNode *node = pTagCond->joinInfo.joinTables[i];
    if (node == NULL) {
      continue;
    }

    if (node->tsJoin != NULL) {
      taosArrayDestroy(node->tsJoin);
    }

    if (node->tagJoin != NULL) {
      taosArrayDestroy(node->tagJoin);
    }

    tfree(node);
  }

  memset(pTagCond, 0, sizeof(STagCond));
}

//void tscGetSrcColumnInfo(SSrcColumnInfo* pColInfo, SQueryStmtInfo* pQueryInfo) {
//  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
//  SSchema*        pSchema = getTableColumnSchema(pTableMetaInfo->pTableMeta);
//
//  size_t numOfExprs = getNumOfExprs(pQueryInfo);
//  for (int32_t i = 0; i < numOfExprs; ++i) {
//    SExprInfo* pExpr = getExprInfo(pQueryInfo, i);
//    pColInfo[i].functionId = pExpr->base.functionId;
//
//    if (TSDB_COL_IS_TAG(pExpr->base.colInfo.flag)) {
//      SSchema* pTagSchema = tscGetTableTagSchema(pTableMetaInfo->pTableMeta);
//
//      int16_t index = pExpr->base.colInfo.colIndex;
//      pColInfo[i].type = (index != -1) ? pTagSchema[index].type : TSDB_DATA_TYPE_BINARY;
//    } else {
//      pColInfo[i].type = pSchema[pExpr->base.colInfo.colIndex].type;
//    }
//  }
//}

/**
 *
 * @param clauseIndex denote the index of the union sub clause, usually are 0, if no union query exists.
 * @param tableIndex  denote the table index for join query, where more than one table exists
 * @return
 */
STableMetaInfo* getMetaInfo(const SQueryStmtInfo* pQueryInfo, int32_t tableIndex) {
  assert(pQueryInfo != NULL);
  if (pQueryInfo->pTableMetaInfo == NULL) {
    assert(pQueryInfo->numOfTables == 0);
    return NULL;
  }

  assert(tableIndex >= 0 && tableIndex <= pQueryInfo->numOfTables && pQueryInfo->pTableMetaInfo != NULL);
  return pQueryInfo->pTableMetaInfo[tableIndex];
}

STableMetaInfo* getTableMetaInfoByUid(SQueryStmtInfo* pQueryInfo, uint64_t uid, int32_t* index) {
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
  return getMetaInfo(pQueryInfo, k);
}

int32_t queryInfoCopy(SQueryStmtInfo* pQueryInfo, const SQueryStmtInfo* pSrc) {
  assert(pQueryInfo != NULL && pSrc != NULL);
  int32_t code = TSDB_CODE_SUCCESS;

  memcpy(&pQueryInfo->interval, &pSrc->interval, sizeof(pQueryInfo->interval));

  pQueryInfo->command        = pSrc->command;
  pQueryInfo->type           = pSrc->type;
  pQueryInfo->window         = pSrc->window;
  pQueryInfo->limit          = pSrc->limit;
  pQueryInfo->slimit         = pSrc->slimit;
  pQueryInfo->order          = pSrc->order;
  pQueryInfo->vgroupLimit    = pSrc->vgroupLimit;
  pQueryInfo->tsBuf          = NULL;
  pQueryInfo->fillType       = pSrc->fillType;
  pQueryInfo->fillVal        = NULL;
  pQueryInfo->numOfFillVal   = 0;;
  pQueryInfo->clauseLimit    = pSrc->clauseLimit;
  pQueryInfo->prjOffset      = pSrc->prjOffset;
  pQueryInfo->numOfTables    = 0;
  pQueryInfo->window         = pSrc->window;
  pQueryInfo->sessionWindow  = pSrc->sessionWindow;
  pQueryInfo->pTableMetaInfo = NULL;

  pQueryInfo->bufLen         = pSrc->bufLen;
//  pQueryInfo->orderProjectQuery = pSrc->orderProjectQuery;
//  pQueryInfo->arithmeticOnAgg   = pSrc->arithmeticOnAgg;
  pQueryInfo->buf            = malloc(pSrc->bufLen);
  if (pQueryInfo->buf == NULL) {
    code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto _error;
  }

  if (pSrc->bufLen > 0) {
    memcpy(pQueryInfo->buf, pSrc->buf, pSrc->bufLen);
  }

  pQueryInfo->groupbyExpr = pSrc->groupbyExpr;
  if (pSrc->groupbyExpr.columnInfo != NULL) {
    pQueryInfo->groupbyExpr.columnInfo = taosArrayDup(pSrc->groupbyExpr.columnInfo);
    if (pQueryInfo->groupbyExpr.columnInfo == NULL) {
      code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      goto _error;
    }
  }

  if (tscTagCondCopy(&pQueryInfo->tagCond, &pSrc->tagCond) != 0) {
    code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto _error;
  }

  if (tscColCondCopy(&pQueryInfo->colCond, pSrc->colCond, 0, -1) != 0) {
    code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto _error;
  }

  if (pSrc->fillType != TSDB_FILL_NONE) {
    pQueryInfo->fillVal = calloc(1, pSrc->fieldsInfo.numOfOutput * sizeof(int64_t));
    if (pQueryInfo->fillVal == NULL) {
      code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      goto _error;
    }
    pQueryInfo->numOfFillVal = pSrc->fieldsInfo.numOfOutput;

    memcpy(pQueryInfo->fillVal, pSrc->fillVal, pSrc->fieldsInfo.numOfOutput * sizeof(int64_t));
  }

  if (copyAllExprInfo(pQueryInfo->exprList[0], pSrc->exprList[0], true) != 0) {
    code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto _error;
  }

  columnListCopyAll(pQueryInfo->colList, pSrc->colList);
  copyFieldInfo(&pQueryInfo->fieldsInfo, &pSrc->fieldsInfo, pQueryInfo->exprList[0]);

  for(int32_t i = 0; i < pSrc->numOfTables; ++i) {
    STableMetaInfo* p1 = getMetaInfo((SQueryStmtInfo*) pSrc, i);

    STableMeta* pMeta = tableMetaDup(p1->pTableMeta);
    if (pMeta == NULL) {
      // todo handle the error
    }

    addTableMetaInfo(pQueryInfo, &p1->name, pMeta, p1->vgroupList, p1->tagColList, NULL);
  }

  SArray *pUdfInfo = NULL;
  if (pSrc->pUdfInfo) {
    pUdfInfo = taosArrayDup(pSrc->pUdfInfo);
  }

  pQueryInfo->pUdfInfo = pUdfInfo;

  _error:
  return code;
}

void clearAllTableMetaInfo(SQueryStmtInfo* pQueryInfo, bool removeMeta, uint64_t id) {
  for(int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    STableMetaInfo* pTableMetaInfo = getMetaInfo(pQueryInfo, i);
    clearTableMetaInfo(pTableMetaInfo);
  }

  tfree(pQueryInfo->pTableMetaInfo);
}

STableMetaInfo* addTableMetaInfo(SQueryStmtInfo* pQueryInfo, SName* name, STableMeta* pTableMeta,
                                 SVgroupsInfo* vgroupList, SArray* pTagCols, SArray* pVgroupTables) {
  void* tmp = realloc(pQueryInfo->pTableMetaInfo, (pQueryInfo->numOfTables + 1) * POINTER_BYTES);
  if (tmp == NULL) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  pQueryInfo->pTableMetaInfo = tmp;
  STableMetaInfo* pTableMetaInfo = calloc(1, sizeof(STableMetaInfo));

  if (pTableMetaInfo == NULL) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  pQueryInfo->pTableMetaInfo[pQueryInfo->numOfTables] = pTableMetaInfo;

  if (name != NULL) {
    tNameAssign(&pTableMetaInfo->name, name);
  }

  pTableMetaInfo->pTableMeta = pTableMeta;

  if (vgroupList != NULL) {
//    pTableMetaInfo->vgroupList = vgroupInfoClone(vgroupList);
  }

  // TODO handle malloc failure
  pTableMetaInfo->tagColList = taosArrayInit(4, POINTER_BYTES);
  if (pTableMetaInfo->tagColList == NULL) {
    return NULL;
  }

  if (pTagCols != NULL && pTableMetaInfo->pTableMeta != NULL) {
    columnListCopy(pTableMetaInfo->tagColList, pTagCols, pTableMetaInfo->pTableMeta->uid);
  }

  pQueryInfo->numOfTables += 1;
  return pTableMetaInfo;
}

STableMetaInfo* addEmptyMetaInfo(SQueryStmtInfo* pQueryInfo) {
  return addTableMetaInfo(pQueryInfo, NULL, NULL, NULL, NULL, NULL);
}

SInternalField* getInternalFieldInfo(SFieldInfo* pFieldInfo, int32_t index) {
  assert(index < pFieldInfo->numOfOutput);
  return TARRAY_GET_ELEM(pFieldInfo->internalField, index);
}

int32_t getNumOfInternalField(SFieldInfo* pFieldInfo) {
  return (int32_t) taosArrayGetSize(pFieldInfo->internalField);
}

static void doSetSqlExprAndResultFieldInfo(SQueryStmtInfo* pNewQueryInfo, int64_t uid) {
  int32_t numOfOutput = (int32_t)getNumOfExprs(pNewQueryInfo);
  if (numOfOutput == 0) {
    return;
  }

  // set the field info in pNewQueryInfo object according to sqlExpr information
  for (int32_t i = 0; i < numOfOutput; ++i) {
    SExprInfo* pExpr = getExprInfo(pNewQueryInfo, i);

    TAOS_FIELD f = createField(&pExpr->base.resSchema);
    SInternalField* pInfo1 = appendFieldInfo(&pNewQueryInfo->fieldsInfo, &f);
    pInfo1->pExpr = pExpr;
  }

  // update the pSqlExpr pointer in SInternalField according the field name
  // make sure the pSqlExpr point to the correct SqlExpr in pNewQueryInfo, not SqlExpr in pQueryInfo
  for (int32_t f = 0; f < pNewQueryInfo->fieldsInfo.numOfOutput; ++f) {
    TAOS_FIELD* field = getFieldInfo(&pNewQueryInfo->fieldsInfo, f);

    bool matched = false;
    for (int32_t k1 = 0; k1 < numOfOutput; ++k1) {
      SExprInfo* pExpr1 = getExprInfo(pNewQueryInfo, k1);

      if (strcmp(field->name, pExpr1->base.resSchema.name) == 0) {  // establish link according to the result field name
        SInternalField* pInfo = getInternalFieldInfo(&pNewQueryInfo->fieldsInfo, f);
        pInfo->pExpr = pExpr1;

        matched = true;
        break;
      }
    }

    assert(matched);
    (void)matched;
  }

//  updateFieldInfoOffset(pNewQueryInfo);
}

int16_t getJoinTagColIdByUid(STagCond* pTagCond, uint64_t uid) {
  int32_t i = 0;
  while (i < TSDB_MAX_JOIN_TABLE_NUM) {
    SJoinNode* node = pTagCond->joinInfo.joinTables[i];
    if (node && node->uid == uid) {
      return node->tagColId;
    }

    i++;
  }

  assert(0);
  return -1;
}

int16_t getTagColIndexById(STableMeta* pTableMeta, int16_t colId) {
  int32_t numOfTags = getNumOfTags(pTableMeta);

  SSchema* pSchema = getTableTagSchema(pTableMeta);
  for(int32_t i = 0; i < numOfTags; ++i) {
    if (pSchema[i].colId == colId) {
      return i;
    }
  }

  // can not reach here
  assert(0);
  return INT16_MIN;
}

bool isQueryWithLimit(SQueryStmtInfo* pQueryInfo) {
  while(pQueryInfo != NULL) {
    if (pQueryInfo->limit.limit > 0) {
      return true;
    }

    pQueryInfo = pQueryInfo->sibling;
  }

  return false;
}

void* vgroupInfoClear(SVgroupsInfo *vgroupList) {
  if (vgroupList == NULL) {
    return NULL;
  }

  tfree(vgroupList);
  return NULL;
}

int32_t copyTagData(STagData* dst, const STagData* src) {
  dst->dataLen = src->dataLen;
  tstrncpy(dst->name, src->name, tListLen(dst->name));

  if (dst->dataLen > 0) {
    dst->data = malloc(dst->dataLen);
    if (dst->data == NULL) {
      return -1;
    }

    memcpy(dst->data, src->data, dst->dataLen);
  }

  return 0;
}


uint32_t getTableMetaSize(const STableMeta* pTableMeta) {
  assert(pTableMeta != NULL);

  int32_t totalCols = 0;
  if (pTableMeta->tableInfo.numOfColumns >= 0) {
    totalCols = pTableMeta->tableInfo.numOfColumns + pTableMeta->tableInfo.numOfTags;
  }

  return sizeof(STableMeta) + totalCols * sizeof(SSchema);
}

uint32_t getTableMetaMaxSize() {
  return sizeof(STableMeta) + TSDB_MAX_COLUMNS * sizeof(SSchema);
}

STableMeta* tableMetaDup(const STableMeta* pTableMeta) {
  assert(pTableMeta != NULL);
  size_t size = getTableMetaSize(pTableMeta);

  STableMeta* p = malloc(size);
  memcpy(p, pTableMeta, size);
  return p;
}

int32_t getNumOfOutput(SFieldInfo* pFieldInfo) {
  return pFieldInfo->numOfOutput;
}

int32_t getColFilterSerializeLen(SQueryStmtInfo* pQueryInfo) {
  int16_t numOfCols = (int16_t)taosArrayGetSize(pQueryInfo->colList);
  int32_t len = 0;

  for(int32_t i = 0; i < numOfCols; ++i) {
    SColumn* pCol = taosArrayGetP(pQueryInfo->colList, i);
    for (int32_t j = 0; j < pCol->info.flist.numOfFilters; ++j) {
      len += sizeof(SColumnFilterInfo);
      if (pCol->info.flist.filterInfo[j].filterstr) {
        len += (int32_t)pCol->info.flist.filterInfo[j].len + 1 * TSDB_NCHAR_SIZE;
      }
    }
  }
  return len;
}

int32_t getTagFilterSerializeLen(SQueryStmtInfo* pQueryInfo) {
  // serialize tag column query condition
  if (pQueryInfo->tagCond.pCond != NULL && taosArrayGetSize(pQueryInfo->tagCond.pCond) > 0) {
    STagCond* pTagCond = &pQueryInfo->tagCond;

    STableMetaInfo *pTableMetaInfo = getMetaInfo(pQueryInfo, 0);
    STableMeta * pTableMeta = pTableMetaInfo->pTableMeta;
    SCond *pCond = getSTableQueryCond(pTagCond, pTableMeta->uid);
    if (pCond != NULL && pCond->cond != NULL) {
      return pCond->len;
    }
  }
  return 0;
}

uint32_t convertRelationalOperator(SToken *pToken) {
  switch (pToken->type) {
    case TK_LT:
      return OP_TYPE_LOWER_THAN;
    case TK_LE:
      return OP_TYPE_LOWER_EQUAL;
    case TK_GT:
      return OP_TYPE_GREATER_THAN;
    case TK_GE:
      return OP_TYPE_GREATER_EQUAL;
    case TK_NE:
      return OP_TYPE_NOT_EQUAL;
    case TK_AND:
      return LOGIC_COND_TYPE_AND;
    case TK_OR:
      return LOGIC_COND_TYPE_OR;
    case TK_EQ:
      return OP_TYPE_EQUAL;

    case TK_PLUS:
      return OP_TYPE_ADD;
    case TK_MINUS:
      return OP_TYPE_SUB;
    case TK_STAR:
      return OP_TYPE_MULTI;
    case TK_SLASH:
    case TK_DIVIDE:
      return OP_TYPE_DIV;
    case TK_REM:
      return OP_TYPE_MOD;
    case TK_LIKE:
      return OP_TYPE_LIKE;
    case TK_MATCH:
      return OP_TYPE_MATCH;
    case TK_NMATCH:
      return OP_TYPE_NMATCH;
    case TK_ISNULL:
      return OP_TYPE_IS_NULL;
    case TK_NOTNULL:
      return OP_TYPE_IS_NOT_NULL;
    case TK_IN:
      return OP_TYPE_IN;
    default: { return 0; }
  }
}

bool isDclSqlStatement(SSqlInfo* pSqlInfo) {
  int32_t type = pSqlInfo->type;
  return (type == TSDB_SQL_CREATE_USER || type == TSDB_SQL_CREATE_ACCT || type == TSDB_SQL_DROP_USER ||
          type == TSDB_SQL_DROP_ACCT || type == TSDB_SQL_SHOW);
}

bool isDdlSqlStatement(SSqlInfo* pSqlInfo) {
  int32_t type = pSqlInfo->type;
  return (type == TSDB_SQL_CREATE_TABLE || type == TSDB_SQL_CREATE_DB || type == TSDB_SQL_DROP_DB);
}

bool isDqlSqlStatement(SSqlInfo* pSqlInfo) {
  return pSqlInfo->type == TSDB_SQL_SELECT;
}

static uint8_t TRUE_VALUE = (uint8_t)TSDB_TRUE;
static uint8_t FALSE_VALUE = (uint8_t)TSDB_FALSE;

static FORCE_INLINE int32_t toDouble(SToken *pToken, double *value, char **endPtr) {
  errno = 0;
  *value = strtold(pToken->z, endPtr);

  // not a valid integer number, return error
  if ((*endPtr - pToken->z) != pToken->n) {
    return TK_ILLEGAL;
  }

  return pToken->type;
}

static bool isNullStr(SToken *pToken) {
  return (pToken->type == TK_NULL) || ((pToken->type == TK_STRING) && (pToken->n != 0) &&
                                       (strncasecmp(TSDB_DATA_NULL_STR_L, pToken->z, pToken->n) == 0));
}

static FORCE_INLINE int32_t checkAndTrimValue(SToken* pToken, uint32_t type, char* tmpTokenBuf, SMsgBuf* pMsgBuf) {
  if ((pToken->type != TK_NOW && pToken->type != TK_INTEGER && pToken->type != TK_STRING && pToken->type != TK_FLOAT && pToken->type != TK_BOOL &&
       pToken->type != TK_NULL && pToken->type != TK_HEX && pToken->type != TK_OCT && pToken->type != TK_BIN) ||
      (pToken->n == 0) || (pToken->type == TK_RP)) {
    return buildSyntaxErrMsg(pMsgBuf, "invalid data or symbol", pToken->z);
  }

  if (IS_NUMERIC_TYPE(type) && pToken->n == 0) {
    return buildSyntaxErrMsg(pMsgBuf, "invalid numeric data", pToken->z);
  }

  // Remove quotation marks
  if (TSDB_DATA_TYPE_BINARY == type) {
    if (pToken->n >= TSDB_MAX_BYTES_PER_ROW) {
      return buildSyntaxErrMsg(pMsgBuf, "too long string", pToken->z);
    }

    // delete escape character: \\, \', \"
    char delim = pToken->z[0];
    int32_t cnt = 0;
    int32_t j = 0;
    for (uint32_t k = 1; k < pToken->n - 1; ++k) {
      if (pToken->z[k] == '\\' || (pToken->z[k] == delim && pToken->z[k + 1] == delim)) {
        tmpTokenBuf[j] = pToken->z[k + 1];
        cnt++;
        j++;
        k++;
        continue;
      }
      tmpTokenBuf[j] = pToken->z[k];
      j++;
    }

    tmpTokenBuf[j] = 0;
    pToken->z = tmpTokenBuf;
    pToken->n -= 2 + cnt;
  }

  return TSDB_CODE_SUCCESS;
}

static int parseTime(char **end, SToken *pToken, int16_t timePrec, int64_t *time, SMsgBuf* pMsgBuf) {
  int32_t   index = 0;
  SToken    sToken;
  int64_t   interval;
  int64_t   ts = 0;
  char* pTokenEnd = *end;

  if (pToken->type == TK_NOW) {
    ts = taosGetTimestamp(timePrec);
  } else if (pToken->type == TK_INTEGER) {
    bool isSigned = false;
    toInteger(pToken->z, pToken->n, 10, &ts, &isSigned);
  } else { // parse the RFC-3339/ISO-8601 timestamp format string
    if (taosParseTime(pToken->z, time, pToken->n, timePrec, tsDaylight) != TSDB_CODE_SUCCESS) {
      return buildSyntaxErrMsg(pMsgBuf, "invalid timestamp format", pToken->z);
    }

    return TSDB_CODE_SUCCESS;
  }

  for (int k = pToken->n; pToken->z[k] != '\0'; k++) {
    if (pToken->z[k] == ' ' || pToken->z[k] == '\t') continue;
    if (pToken->z[k] == ',') {
      *end = pTokenEnd;
      *time = ts;
      return 0;
    }

    break;
  }

  /*
   * time expression:
   * e.g., now+12a, now-5h
   */
  SToken valueToken;
  index = 0;
  sToken = tStrGetToken(pTokenEnd, &index, false);
  pTokenEnd += index;

  if (sToken.type == TK_MINUS || sToken.type == TK_PLUS) {
    index = 0;
    valueToken = tStrGetToken(pTokenEnd, &index, false);
    pTokenEnd += index;

    if (valueToken.n < 2) {
      return buildSyntaxErrMsg(pMsgBuf, "value expected in timestamp", sToken.z);
    }

    char unit = 0;
    if (parseAbsoluteDuration(valueToken.z, valueToken.n, &interval, &unit, timePrec) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if (sToken.type == TK_PLUS) {
      ts += interval;
    } else {
      ts = ts - interval;
    }

    *end = pTokenEnd;
  }

  *time = ts;
  return TSDB_CODE_SUCCESS;
}

int32_t parseValueToken(char** end, SToken* pToken, SSchema* pSchema, int16_t timePrec, char* tmpTokenBuf, _row_append_fn_t func, void* param, SMsgBuf* pMsgBuf) {
  int64_t iv;
  char   *endptr = NULL;
  bool    isSigned = false;

  int32_t code = checkAndTrimValue(pToken, pSchema->type, tmpTokenBuf, pMsgBuf);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (isNullStr(pToken)) {
    if (TSDB_DATA_TYPE_TIMESTAMP == pSchema->type && PRIMARYKEY_TIMESTAMP_COL_ID == pSchema->colId) {
      int64_t tmpVal = 0;
      return func(&tmpVal, pSchema->bytes, param);
    }

    return func(getNullValue(pSchema->type), 0, param);
  }

  switch (pSchema->type) {
    case TSDB_DATA_TYPE_BOOL: {
      if ((pToken->type == TK_BOOL || pToken->type == TK_STRING) && (pToken->n != 0)) {
        if (strncmp(pToken->z, "true", pToken->n) == 0) {
          return func(&TRUE_VALUE, pSchema->bytes, param);
        } else if (strncmp(pToken->z, "false", pToken->n) == 0) {
          return func(&FALSE_VALUE, pSchema->bytes, param);
        } else {
          return buildSyntaxErrMsg(pMsgBuf, "invalid bool data", pToken->z);
        }
      } else if (pToken->type == TK_INTEGER) {
        return func(((strtoll(pToken->z, NULL, 10) == 0) ? &FALSE_VALUE : &TRUE_VALUE), pSchema->bytes, param);
      } else if (pToken->type == TK_FLOAT) {
        return func(((strtod(pToken->z, NULL) == 0) ? &FALSE_VALUE : &TRUE_VALUE), pSchema->bytes, param);
      } else {
        return buildSyntaxErrMsg(pMsgBuf, "invalid bool data", pToken->z);
      }
    }

    case TSDB_DATA_TYPE_TINYINT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &iv, &isSigned)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid tinyint data", pToken->z);
      } else if (!IS_VALID_TINYINT(iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "tinyint data overflow", pToken->z);
      }

      uint8_t tmpVal = (uint8_t)iv;
      return func(&tmpVal, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_UTINYINT:{
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &iv, &isSigned)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid unsigned tinyint data", pToken->z);
      } else if (!IS_VALID_UTINYINT(iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "unsigned tinyint data overflow", pToken->z);
      }
      uint8_t tmpVal = (uint8_t)iv;
      return func(&tmpVal, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_SMALLINT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &iv, &isSigned)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid smallint data", pToken->z);
      } else if (!IS_VALID_SMALLINT(iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "smallint data overflow", pToken->z);
      }
      int16_t tmpVal = (int16_t)iv;
      return func(&tmpVal, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_USMALLINT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &iv, &isSigned)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid unsigned smallint data", pToken->z);
      } else if (!IS_VALID_USMALLINT(iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "unsigned smallint data overflow", pToken->z);
      }
      uint16_t tmpVal = (uint16_t)iv;
      return func(&tmpVal, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_INT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &iv, &isSigned)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid int data", pToken->z);
      } else if (!IS_VALID_INT(iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "int data overflow", pToken->z);
      }
      int32_t tmpVal = (int32_t)iv;
      return func(&tmpVal, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_UINT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &iv, &isSigned)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid unsigned int data", pToken->z);
      } else if (!IS_VALID_UINT(iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "unsigned int data overflow", pToken->z);
      }
      uint32_t tmpVal = (uint32_t)iv;
      return func(&tmpVal, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_BIGINT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &iv, &isSigned)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid bigint data", pToken->z);
      } else if (!IS_VALID_BIGINT(iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "bigint data overflow", pToken->z);
      }
      return func(&iv, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_UBIGINT: {
      if (TSDB_CODE_SUCCESS != toInteger(pToken->z, pToken->n, 10, &iv, &isSigned)) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid unsigned bigint data", pToken->z);
      } else if (!IS_VALID_UBIGINT((uint64_t)iv)) {
        return buildSyntaxErrMsg(pMsgBuf, "unsigned bigint data overflow", pToken->z);
      }
      uint64_t tmpVal = (uint64_t)iv;
      return func(&tmpVal, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_FLOAT: {
      double dv;
      if (TK_ILLEGAL == toDouble(pToken, &dv, &endptr)) {
        return buildSyntaxErrMsg(pMsgBuf, "illegal float data", pToken->z);
      }
      if (((dv == HUGE_VAL || dv == -HUGE_VAL) && errno == ERANGE) || dv > FLT_MAX || dv < -FLT_MAX || isinf(dv) || isnan(dv)) {
        return buildSyntaxErrMsg(pMsgBuf, "illegal float data", pToken->z);
      }
      float tmpVal = (float)dv;
      return func(&tmpVal, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_DOUBLE: {
      double dv;
      if (TK_ILLEGAL == toDouble(pToken, &dv, &endptr)) {
        return buildSyntaxErrMsg(pMsgBuf, "illegal double data", pToken->z);
      }
      if (((dv == HUGE_VAL || dv == -HUGE_VAL) && errno == ERANGE) || isinf(dv) || isnan(dv)) {
        return buildSyntaxErrMsg(pMsgBuf, "illegal double data", pToken->z);
      }
      return func(&dv, pSchema->bytes, param);
    }

    case TSDB_DATA_TYPE_BINARY: {
      // Too long values will raise the invalid sql error message
      if (pToken->n + VARSTR_HEADER_SIZE > pSchema->bytes) {
        return buildSyntaxErrMsg(pMsgBuf, "string data overflow", pToken->z);
      }

      return func(pToken->z, pToken->n, param);
    }

    case TSDB_DATA_TYPE_NCHAR: {
      return func(pToken->z, pToken->n, param);
    }

    case TSDB_DATA_TYPE_TIMESTAMP: {
      int64_t tmpVal;
      if (parseTime(end, pToken, timePrec, &tmpVal, pMsgBuf) != TSDB_CODE_SUCCESS) {
        return buildSyntaxErrMsg(pMsgBuf, "invalid timestamp", pToken->z);
      }

      return func(&tmpVal, pSchema->bytes, param);
    }
  }

  return TSDB_CODE_FAILED;
}

int32_t KvRowAppend(const void *value, int32_t len, void *param) {
  SKvParam* pa = (SKvParam*) param;

  int32_t type  = pa->schema->type;
  int32_t colId = pa->schema->colId;

  if (TSDB_DATA_TYPE_BINARY == type) {
    STR_WITH_SIZE_TO_VARSTR(pa->buf, value, len);
    tdAddColToKVRow(pa->builder, colId, type, pa->buf);
  } else if (TSDB_DATA_TYPE_NCHAR == type) {
    // if the converted output len is over than pColumnModel->bytes, return error: 'Argument list too long'
    int32_t output = 0;
    if (!taosMbsToUcs4(value, len, varDataVal(pa->buf), pa->schema->bytes - VARSTR_HEADER_SIZE, &output)) {
      return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
    }

    varDataSetLen(pa->buf, output);
    tdAddColToKVRow(pa->builder, colId, type, pa->buf);
  } else {
    tdAddColToKVRow(pa->builder, colId, type, value);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t createSName(SName* pName, SToken* pTableName, SParseContext* pParseCtx, SMsgBuf* pMsgBuf) {
  const char* msg1 = "name too long";
  const char* msg2 = "invalid database name";
  const char* msg3 = "db is not specified";

  int32_t  code = TSDB_CODE_SUCCESS;
  char* p  = strnchr(pTableName->z, TS_PATH_DELIMITER[0], pTableName->n, true);

  if (p != NULL) { // db has been specified in sql string so we ignore current db path
    assert(*p == TS_PATH_DELIMITER[0]);

    int32_t dbLen = p - pTableName->z;
    char name[TSDB_DB_FNAME_LEN] = {0};
    strncpy(name, pTableName->z, dbLen);
    dbLen = strdequote(name);

    code = tNameSetDbName(pName, pParseCtx->acctId, name, dbLen);
    if (code != TSDB_CODE_SUCCESS) {
      return buildInvalidOperationMsg(pMsgBuf, msg1);
    }

    int32_t tbLen = pTableName->n - dbLen - 1;
    char tbname[TSDB_TABLE_FNAME_LEN] = {0};
    strncpy(tbname, p + 1, tbLen);
    /*tbLen = */strdequote(tbname);

    code = tNameFromString(pName, tbname, T_NAME_TABLE);
    if (code != 0) {
      return buildInvalidOperationMsg(pMsgBuf, msg1);
    }
  } else {  // get current DB name first, and then set it into path
    if (pTableName->n >= TSDB_TABLE_NAME_LEN) {
      return buildInvalidOperationMsg(pMsgBuf, msg1);
    }

    assert(pTableName->n < TSDB_TABLE_FNAME_LEN);

    char name[TSDB_TABLE_FNAME_LEN] = {0};
    strncpy(name, pTableName->z, pTableName->n);
    strdequote(name);

    if (pParseCtx->db == NULL) {
      return buildInvalidOperationMsg(pMsgBuf, msg3);
    }

    code = tNameSetDbName(pName, pParseCtx->acctId, pParseCtx->db, strlen(pParseCtx->db));
    if (code != TSDB_CODE_SUCCESS) {
      code = buildInvalidOperationMsg(pMsgBuf, msg2);
      return code;
    }

    code = tNameFromString(pName, name, T_NAME_TABLE);
    if (code != 0) {
      code = buildInvalidOperationMsg(pMsgBuf, msg1);
    }
  }

  return code;
}
