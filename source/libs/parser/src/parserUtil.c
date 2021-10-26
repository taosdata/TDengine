#include "taosmsg.h"
#include "parser.h"
#include "parserUtil.h"
#include "taoserror.h"
#include "tutil.h"
#include "ttypes.h"
#include "thash.h"
#include "tbuffer.h"
#include "parserInt.h"
#include "queryInfoUtil.h"
#include "function.h"

typedef struct STableFilterCond {
  uint64_t uid;
  int16_t  idx;  //table index
  int32_t  len;  // length of tag query condition data
  char *   cond;
} STableFilterCond;

static STableMetaInfo* addTableMetaInfo(SQueryStmtInfo* pQueryInfo, SName* name, STableMeta* pTableMeta,
                                        SVgroupsInfo* vgroupList, SArray* pTagCols, SArray* pVgroupTables);
STableMeta* tableMetaDup(STableMeta* pTableMeta);

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

int32_t buildInvalidOperationMsg(SMsgBuf* pBuf, const char* msg) {
  strncpy(pBuf->buf, msg, pBuf->len);
  return TSDB_CODE_TSC_INVALID_OPERATION;
}

int32_t buildSyntaxErrMsg(char* dst, int32_t dstBufLen, const char* additionalInfo,  const char* sourceStr) {
  const char* msgFormat1 = "syntax error near \'%s\'";
  const char* msgFormat2 = "syntax error near \'%s\' (%s)";
  const char* msgFormat3 = "%s";

  const char* prefix = "syntax error";
  if (sourceStr == NULL) {
    assert(additionalInfo != NULL);
    snprintf(dst, dstBufLen, msgFormat1, additionalInfo);
    return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
  }

  char buf[64] = {0};  // only extract part of sql string
  strncpy(buf, sourceStr, tListLen(buf) - 1);

  if (additionalInfo != NULL) {
    snprintf(dst, dstBufLen, msgFormat2, buf, additionalInfo);
  } else {
    const char* msgFormat = (0 == strncmp(sourceStr, prefix, strlen(prefix))) ? msgFormat3 : msgFormat1;
    snprintf(dst, dstBufLen, msgFormat, buf);
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
//int32_t tscCreateDataBlock(size_t defaultSize, int32_t rowSize, int32_t startOffset, SName* name,
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
//int32_t tscGetDataBlockFromList(SHashObj* pHashList, int64_t id, int32_t size, int32_t startOffset, int32_t rowSize,
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
//static int trimDataBlock(void* pDataBlock, STableDataBlocks* pTableDataBlock, SInsertStatementParam* insertParam,
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
//      SMemRow memRow = (SMemRow)pDataBlock;
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
//      pDataBlock = (char*)pDataBlock + memRowTLen(memRow);
//      pBlock->dataLen += memRowTLen(memRow);
//    }
//  } else {
//    for (int32_t i = 0; i < numOfRows; ++i) {
//      char* payload = (blkKeyTuple + i)->payloadAddr;
//      if (isNeedConvertRow(payload)) {
//        convertSMemRow(pDataBlock, payload, pTableDataBlock);
//        TDRowTLenT rowTLen = memRowTLen(pDataBlock);
//        pDataBlock = POINTER_SHIFT(pDataBlock, rowTLen);
//        pBlock->dataLen += rowTLen;
//      } else {
//        TDRowTLenT rowTLen = memRowTLen(payload);
//        memcpy(pDataBlock, payload, rowTLen);
//        pDataBlock = POINTER_SHIFT(pDataBlock, rowTLen);
//        pBlock->dataLen += rowTLen;
//      }
//    }
//  }
//
//  int32_t len = pBlock->dataLen + pBlock->schemaLen;
//  pBlock->dataLen = htonl(pBlock->dataLen);
//  pBlock->schemaLen = htonl(pBlock->schemaLen);
//
//  return len;
//}

TAOS_FIELD createField(const SSchema* pSchema) {
  TAOS_FIELD f = { .type = pSchema->type, .bytes = pSchema->bytes, };
  tstrncpy(f.name, pSchema->name, sizeof(f.name));
  return f;
}

SSchema createSchema(uint8_t type, int16_t bytes, int16_t colId, const char* name) {
  SSchema s = {0};
  s.type = type;
  s.bytes = bytes;
  s.colId = colId;

  tstrncpy(s.name, name, tListLen(s.name));
  return s;
}

int32_t getNumOfFields(SFieldInfo* pFieldInfo) {
  return pFieldInfo->numOfOutput;
}

int32_t getFirstInvisibleFieldPos(SQueryStmtInfo* pQueryInfo) {
  if (pQueryInfo->fieldsInfo.numOfOutput <= 0 || pQueryInfo->fieldsInfo.internalField == NULL) {
    return 0;
  }

  for (int32_t i = 0; i < pQueryInfo->fieldsInfo.numOfOutput; ++i) {
    SInternalField* pField = taosArrayGet(pQueryInfo->fieldsInfo.internalField, i);
    if (!pField->visible) {
      return i;
    }
  }

  return pQueryInfo->fieldsInfo.numOfOutput;
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

  for (int32_t i = 0; i < numOfExprs; ++i) {
    SExprInfo* p = taosArrayGetP(pQueryInfo->exprList, i);

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

int16_t getFieldInfoOffset(SQueryStmtInfo* pQueryInfo, int32_t index) {
  SInternalField* pInfo = getInternalField(&pQueryInfo->fieldsInfo, index);
  assert(pInfo != NULL && pInfo->pExpr->pExpr == NULL);
  return 0;
//  return pInfo->pExpr->base.offset;
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

  if (pFieldInfo->internalField != NULL) {
    size_t num = taosArrayGetSize(pFieldInfo->internalField);
    for (int32_t i = 0; i < num; ++i) {
//      SInternalField* pfield = taosArrayGet(pFieldInfo->internalField, i);
//      if (pfield->pExpr != NULL && pfield->pExpr->pExpr != NULL) {
//        sqlExprDestroy(pfield->pExpr);
//      }
    }
  }

  taosArrayDestroy(pFieldInfo->internalField);
//  tfree(pFieldInfo->final);

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
    if ((pCol->info.colId != columnId) || (pCol->tableUid != uid)) {
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

SColumn* columnListInsert(SArray* pColumnList, int32_t columnIndex, uint64_t uid, SSchema* pSchema) {
  // ignore the tbname columnIndex to be inserted into source list
  if (columnIndex < 0) {
    return NULL;
  }

  size_t numOfCols = taosArrayGetSize(pColumnList);

  int32_t i = 0;
  while (i < numOfCols) {
    SColumn* pCol = taosArrayGetP(pColumnList, i);
    if (pCol->columnIndex < columnIndex) {
      i++;
    } else if (pCol->tableUid < uid) {
      i++;
    } else {
      break;
    }
  }

  if (i >= numOfCols || numOfCols == 0) {
    SColumn* b = calloc(1, sizeof(SColumn));
    if (b == NULL) {
      return NULL;
    }

    b->columnIndex = columnIndex;
    b->tableUid    = uid;
    b->info.colId  = pSchema->colId;
    b->info.bytes  = pSchema->bytes;
    b->info.type   = pSchema->type;

    taosArrayInsert(pColumnList, i, &b);
  } else {
    SColumn* pCol = taosArrayGetP(pColumnList, i);

    if (i < numOfCols && (pCol->columnIndex > columnIndex || pCol->tableUid != uid)) {
      SColumn* b = calloc(1, sizeof(SColumn));
      if (b == NULL) {
        return NULL;
      }

      b->columnIndex = columnIndex;
      b->tableUid    = uid;
      b->info.colId = pSchema->colId;
      b->info.bytes = pSchema->bytes;
      b->info.type  = pSchema->type;

      taosArrayInsert(pColumnList, i, &b);
    }
  }

  return taosArrayGetP(pColumnList, i);
}

SColumn* insertPrimaryTsColumn(SArray* pColumnList, uint64_t tableUid) {
  SSchema s = {.type = TSDB_DATA_TYPE_TIMESTAMP, .bytes = TSDB_KEYSIZE, .colId = PRIMARYKEY_TIMESTAMP_COL_INDEX};
  return columnListInsert(pColumnList, PRIMARYKEY_TIMESTAMP_COL_INDEX, tableUid, &s);
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
  assert(!(src->lowerRelOptr == TSDB_RELATION_INVALID && src->upperRelOptr == TSDB_RELATION_INVALID));

  return pFilter;
}

void columnCopy(SColumn* pDest, const SColumn* pSrc) {
  destroyFilterInfo(&pDest->info.flist);

  pDest->columnIndex       = pSrc->columnIndex;
  pDest->tableUid          = pSrc->tableUid;
  pDest->info.flist.numOfFilters = pSrc->info.flist.numOfFilters;
  pDest->info.flist.filterInfo   = tFilterInfoDup(pSrc->info.flist.filterInfo, pSrc->info.flist.numOfFilters);
  pDest->info.type        = pSrc->info.type;
  pDest->info.colId        = pSrc->info.colId;
  pDest->info.bytes      = pSrc->info.bytes;
}

void columnListCopy(SArray* dst, const SArray* src, uint64_t tableUid) {
  assert(src != NULL && dst != NULL);

  size_t num = taosArrayGetSize(src);
  for (int32_t i = 0; i < num; ++i) {
    SColumn* pCol = taosArrayGetP(src, i);

    if (pCol->tableUid == tableUid) {
      SColumn* p = columnClone(pCol);
      taosArrayPush(dst, &p);
    }
  }
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
STableMetaInfo* getMetaInfo(SQueryStmtInfo* pQueryInfo, int32_t tableIndex) {
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

  if (copyAllExprInfo(pQueryInfo->exprList, pSrc->exprList, true) != 0) {
    code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto _error;
  }

//  if (pQueryInfo->arithmeticOnAgg) {
//    pQueryInfo->exprList1 = taosArrayInit(4, POINTER_BYTES);
//    if (copyAllExprInfo(pQueryInfo->exprList1, pSrc->exprList1, true) != 0) {
//      code = TSDB_CODE_TSC_OUT_OF_MEMORY;
//      goto _error;
//    }
//  }

  columnListCopyAll(pQueryInfo->colList, pSrc->colList);
  copyFieldInfo(&pQueryInfo->fieldsInfo, &pSrc->fieldsInfo, pQueryInfo->exprList);

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

SVgroupsInfo* vgroupInfoClone(SVgroupsInfo *vgroupList) {
  if (vgroupList == NULL) {
    return NULL;
  }

  size_t size = sizeof(SVgroupsInfo) + sizeof(SVgroupMsg) * vgroupList->numOfVgroups;
  SVgroupsInfo* pNew = malloc(size);
  if (pNew == NULL) {
    return NULL;
  }

  pNew->numOfVgroups = vgroupList->numOfVgroups;

  for(int32_t i = 0; i < vgroupList->numOfVgroups; ++i) {
    SVgroupMsg* pNewVInfo = &pNew->vgroups[i];

    SVgroupMsg* pvInfo = &vgroupList->vgroups[i];
    pNewVInfo->vgId = pvInfo->vgId;
    pNewVInfo->numOfEps = pvInfo->numOfEps;

    for(int32_t j = 0; j < pvInfo->numOfEps; ++j) {
      pNewVInfo->epAddr[j].port = pvInfo->epAddr[j].port;
      tstrncpy(pNewVInfo->epAddr[j].fqdn, pvInfo->epAddr[j].fqdn, TSDB_FQDN_LEN);
    }
  }

  return pNew;
}

void* vgroupInfoClear(SVgroupsInfo *vgroupList) {
  if (vgroupList == NULL) {
    return NULL;
  }

  tfree(vgroupList);
  return NULL;
}

char* serializeTagData(STagData* pTagData, char* pMsg) {
  int32_t n = (int32_t) strlen(pTagData->name);
  *(int32_t*) pMsg = htonl(n);
  pMsg += sizeof(n);

  memcpy(pMsg, pTagData->name, n);
  pMsg += n;

  *(int32_t*)pMsg = htonl(pTagData->dataLen);
  pMsg += sizeof(int32_t);

  memcpy(pMsg, pTagData->data, pTagData->dataLen);
  pMsg += pTagData->dataLen;

  return pMsg;
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

STableMeta* createSuperTableMeta(STableMetaMsg* pChild) {
  assert(pChild != NULL);
  int32_t total = pChild->numOfColumns + pChild->numOfTags;

  STableMeta* pTableMeta = calloc(1, sizeof(STableMeta) + sizeof(SSchema) * total);
  pTableMeta->tableType = TSDB_SUPER_TABLE;
  pTableMeta->tableInfo.numOfTags = pChild->numOfTags;
  pTableMeta->tableInfo.numOfColumns = pChild->numOfColumns;
  pTableMeta->tableInfo.precision = pChild->precision;

  pTableMeta->uid = pChild->suid;
  pTableMeta->tversion = pChild->tversion;
  pTableMeta->sversion = pChild->sversion;

  memcpy(pTableMeta->schema, pChild->schema, sizeof(SSchema) * total);

  int32_t num = pTableMeta->tableInfo.numOfColumns;
  for(int32_t i = 0; i < num; ++i) {
    pTableMeta->tableInfo.rowSize += pTableMeta->schema[i].bytes;
  }

  return pTableMeta;
}

uint32_t getTableMetaSize(STableMeta* pTableMeta) {
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

STableMeta* tableMetaDup(STableMeta* pTableMeta) {
  assert(pTableMeta != NULL);
  size_t size = getTableMetaSize(pTableMeta);

  STableMeta* p = malloc(size);
  memcpy(p, pTableMeta, size);
  return p;
}

SVgroupsInfo* vgroupsInfoDup(SVgroupsInfo* pVgroupsInfo) {
  assert(pVgroupsInfo != NULL);

  size_t size = sizeof(SVgroupMsg) * pVgroupsInfo->numOfVgroups + sizeof(SVgroupsInfo);
  SVgroupsInfo* pInfo = calloc(1, size);
  pInfo->numOfVgroups = pVgroupsInfo->numOfVgroups;
  for (int32_t m = 0; m < pVgroupsInfo->numOfVgroups; ++m) {
    memcpy(&pInfo->vgroups[m], &pVgroupsInfo->vgroups[m], sizeof(SVgroupMsg));
  }

  return pInfo;
}

int32_t getNumOfOutput(SFieldInfo* pFieldInfo) {
  return pFieldInfo->numOfOutput;
}

// todo move to planner module
int32_t createProjectionExpr(SQueryStmtInfo* pQueryInfo, STableMetaInfo* pTableMetaInfo, SExprInfo*** pExpr, int32_t* num) {
//  if (!pQueryInfo->arithmeticOnAgg) {
//    return TSDB_CODE_SUCCESS;
//  }
#if 0
  *num = getNumOfOutput(pQueryInfo);
  *pExpr = calloc(*(num), POINTER_BYTES);
  if ((*pExpr) == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < (*num); ++i) {
    SInternalField* pField = getInternalFieldInfo(&pQueryInfo->fieldsInfo, i);
    SExprInfo* pSource = pField->pExpr;

    SExprInfo* px = calloc(1, sizeof(SExprInfo));
    (*pExpr)[i] = px;

    SSqlExpr *pse = &px->base;
    pse->uid      = pTableMetaInfo->pTableMeta->uid;
    memcpy(&pse->resSchema, &pSource->base.resSchema, sizeof(SSchema));

    if (pSource->base.functionId != FUNCTION_ARITHM) {  // this should be switched to projection query
      pse->numOfParams = 0;      // no params for projection query
      pse->functionId  = FUNCTION_PRJ;
      pse->colInfo.colId = pSource->base.resSchema.colId;

      int32_t numOfOutput = (int32_t) taosArrayGetSize(pQueryInfo->exprList);
      for (int32_t j = 0; j < numOfOutput; ++j) {
        SExprInfo* p = taosArrayGetP(pQueryInfo->exprList, j);
        if (p->base.resSchema.colId == pse->colInfo.colId) {
          pse->colInfo.colIndex = j;
          break;
        }
      }

      pse->colInfo.flag = TSDB_COL_NORMAL;
      strncpy(pse->colInfo.name, pSource->base.resSchema.name, tListLen(pse->colInfo.name));

      // TODO restore refactor
      int32_t functionId = pSource->base.functionId;
      if (pSource->base.functionId == FUNCTION_FIRST_DST) {
        functionId = FUNCTION_FIRST;
      } else if (pSource->base.functionId == FUNCTION_LAST_DST) {
        functionId = FUNCTION_LAST;
      } else if (pSource->base.functionId == FUNCTION_STDDEV_DST) {
        functionId = FUNCTION_STDDEV;
      }

      int32_t inter = 0;
      getResultDataInfo(pSource->base.colType, pSource->base.colBytes, functionId, 0, &pse->resSchema.type,
                        &pse->resSchema.bytes, &inter, 0, false/*, NULL*/);
      pse->colType  = pse->resSchema.type;
      pse->colBytes = pse->resSchema.bytes;

    } else {  // arithmetic expression
      pse->colInfo.colId = pSource->base.colInfo.colId;
      pse->colType  = pSource->base.colType;
      pse->colBytes = pSource->base.colBytes;
      pse->resSchema.bytes = sizeof(double);
      pse->resSchema.type  = TSDB_DATA_TYPE_DOUBLE;

      pse->functionId = pSource->base.functionId;
      pse->numOfParams = pSource->base.numOfParams;

      for (int32_t j = 0; j < pSource->base.numOfParams; ++j) {
        taosVariantAssign(&pse->param[j], &pSource->base.param[j]);
//        buildArithmeticExprFromMsg(px, NULL);
      }
    }
  }
#endif
  return TSDB_CODE_SUCCESS;
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
      return TSDB_RELATION_LESS;
    case TK_LE:
      return TSDB_RELATION_LESS_EQUAL;
    case TK_GT:
      return TSDB_RELATION_GREATER;
    case TK_GE:
      return TSDB_RELATION_GREATER_EQUAL;
    case TK_NE:
      return TSDB_RELATION_NOT_EQUAL;
    case TK_AND:
      return TSDB_RELATION_AND;
    case TK_OR:
      return TSDB_RELATION_OR;
    case TK_EQ:
      return TSDB_RELATION_EQUAL;
    case TK_PLUS:
      return TSDB_BINARY_OP_ADD;

    case TK_MINUS:
      return TSDB_BINARY_OP_SUBTRACT;
    case TK_STAR:
      return TSDB_BINARY_OP_MULTIPLY;
    case TK_SLASH:
    case TK_DIVIDE:
      return TSDB_BINARY_OP_DIVIDE;
    case TK_REM:
      return TSDB_BINARY_OP_REMAINDER;
    case TK_LIKE:
      return TSDB_RELATION_LIKE;
    case TK_MATCH:
      return TSDB_RELATION_MATCH;
    case TK_NMATCH:
      return TSDB_RELATION_NMATCH;
    case TK_ISNULL:
      return TSDB_RELATION_ISNULL;
    case TK_NOTNULL:
      return TSDB_RELATION_NOTNULL;
    case TK_IN:
      return TSDB_RELATION_IN;
    default: { return 0; }
  }
}

#if 0
int32_t tscCreateQueryFromQueryInfo(SQueryStmtInfo* pQueryInfo, SQueryAttr* pQueryAttr, void* addr) {
  memset(pQueryAttr, 0, sizeof(SQueryAttr));

  int16_t numOfCols        = (int16_t) taosArrayGetSize(pQueryInfo->colList);
  int16_t numOfOutput      = (int16_t) getNumOfExprs(pQueryInfo);

  pQueryAttr->topBotQuery       = tscIsTopBotQuery(pQueryInfo);
  pQueryAttr->hasTagResults     = hasTagValOutput(pQueryInfo);
  pQueryAttr->stabledev         = isStabledev(pQueryInfo);
  pQueryAttr->tsCompQuery       = isTsCompQuery(pQueryInfo);
  pQueryAttr->diffQuery         = tscIsDiffDerivQuery(pQueryInfo);
  pQueryAttr->simpleAgg         = isSimpleAggregateRv(pQueryInfo);
  pQueryAttr->needReverseScan   = tscNeedReverseScan(pQueryInfo);
  pQueryAttr->stableQuery       = QUERY_IS_STABLE_QUERY(pQueryInfo->type);
  pQueryAttr->groupbyColumn     = (!pQueryInfo->stateWindow) && tscGroupbyColumn(pQueryInfo);
  pQueryAttr->queryBlockDist    = isBlockDistQuery(pQueryInfo);
  pQueryAttr->pointInterpQuery  = tscIsPointInterpQuery(pQueryInfo);
  pQueryAttr->timeWindowInterpo = timeWindowInterpoRequired(pQueryInfo);
  pQueryAttr->distinct          = pQueryInfo->distinct;
  pQueryAttr->sw                = pQueryInfo->sessionWindow;
  pQueryAttr->stateWindow       = pQueryInfo->stateWindow;
  pQueryAttr->multigroupResult  = pQueryInfo->multigroupResult;

  pQueryAttr->numOfCols         = numOfCols;
  pQueryAttr->numOfOutput       = numOfOutput;
  pQueryAttr->limit             = pQueryInfo->limit;
  pQueryAttr->slimit            = pQueryInfo->slimit;
  pQueryAttr->order             = pQueryInfo->order;
  pQueryAttr->fillType          = pQueryInfo->fillType;
  pQueryAttr->havingNum         = pQueryInfo->havingFieldNum;
  pQueryAttr->pUdfInfo          = pQueryInfo->pUdfInfo;

  if (pQueryInfo->order.order == TSDB_ORDER_ASC) {   // TODO refactor
    pQueryAttr->window = pQueryInfo->window;
  } else {
    pQueryAttr->window.skey = pQueryInfo->window.ekey;
    pQueryAttr->window.ekey = pQueryInfo->window.skey;
  }

  memcpy(&pQueryAttr->interval, &pQueryInfo->interval, sizeof(pQueryAttr->interval));

  STableMetaInfo* pTableMetaInfo = pQueryInfo->pTableMetaInfo[0];

  if (pQueryInfo->groupbyExpr.numOfGroupCols > 0) {
    pQueryAttr->pGroupbyExpr    = calloc(1, sizeof(SGroupbyExpr));
    *(pQueryAttr->pGroupbyExpr) = pQueryInfo->groupbyExpr;
    pQueryAttr->pGroupbyExpr->columnInfo = taosArrayDup(pQueryInfo->groupbyExpr.columnInfo);
  } else {
    assert(pQueryInfo->groupbyExpr.columnInfo == NULL);
  }

  pQueryAttr->pExpr1 = calloc(pQueryAttr->numOfOutput, sizeof(SExprInfo));
  for(int32_t i = 0; i < pQueryAttr->numOfOutput; ++i) {
    SExprInfo* pExpr = getExprInfo(pQueryInfo, i);
    tscExprAssign(&pQueryAttr->pExpr1[i], pExpr);

    if (pQueryAttr->pExpr1[i].base.functionId == FUNCTION_ARITHM) {
      for (int32_t j = 0; j < pQueryAttr->pExpr1[i].base.numOfParams; ++j) {
        buildArithmeticExprFromMsg(&pQueryAttr->pExpr1[i], NULL);
      }
    }
  }

  pQueryAttr->tableCols = calloc(numOfCols, sizeof(SColumnInfo));
  for(int32_t i = 0; i < numOfCols; ++i) {
    SColumn* pCol = taosArrayGetP(pQueryInfo->colList, i);
    if (!isValidDataType(pCol->info.type) || pCol->info.type == TSDB_DATA_TYPE_NULL) {
      assert(0);
    }

    pQueryAttr->tableCols[i] = pCol->info;
    pQueryAttr->tableCols[i].flist.filterInfo = tFilterInfoDup(pCol->info.flist.filterInfo, pQueryAttr->tableCols[i].flist.numOfFilters);
  }

  // global aggregate query
  if (pQueryAttr->stableQuery && (pQueryAttr->simpleAgg || pQueryAttr->interval.interval > 0) && tscIsTwoStageSTableQuery(pQueryInfo, 0)) {
    createGlobalAggregateExpr(pQueryAttr, pQueryInfo);
  }

  // for simple table, not for super table
  if (pQueryInfo->arithmeticOnAgg) {
    pQueryAttr->numOfExpr2 = (int32_t) taosArrayGetSize(pQueryInfo->exprList1);
    pQueryAttr->pExpr2 = calloc(pQueryAttr->numOfExpr2, sizeof(SExprInfo));
    for(int32_t i = 0; i < pQueryAttr->numOfExpr2; ++i) {
      SExprInfo* p = taosArrayGetP(pQueryInfo->exprList1, i);
      tscExprAssign(&pQueryAttr->pExpr2[i], p);
    }
  }

  // tag column info
  int32_t code = createTagColumnInfo(pQueryAttr, pQueryInfo, pTableMetaInfo);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (pQueryAttr->fillType != TSDB_FILL_NONE) {
    pQueryAttr->fillVal = calloc(pQueryAttr->numOfOutput, sizeof(int64_t));
    memcpy(pQueryAttr->fillVal, pQueryInfo->fillVal, pQueryInfo->numOfFillVal * sizeof(int64_t));
  }

  pQueryAttr->srcRowSize = 0;
  pQueryAttr->maxTableColumnWidth = 0;
  for (int16_t i = 0; i < numOfCols; ++i) {
    pQueryAttr->srcRowSize += pQueryAttr->tableCols[i].bytes;
    if (pQueryAttr->maxTableColumnWidth < pQueryAttr->tableCols[i].bytes) {
      pQueryAttr->maxTableColumnWidth = pQueryAttr->tableCols[i].bytes;
    }
  }

  pQueryAttr->interBufSize = getOutputInterResultBufSize(pQueryAttr);

  if (pQueryAttr->numOfCols <= 0 && !tscQueryTags(pQueryInfo) && !pQueryAttr->queryBlockDist) {
    tscError("%p illegal value of numOfCols in query msg: %" PRIu64 ", table cols:%d", addr,
             (uint64_t)pQueryAttr->numOfCols, numOfCols);

    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  if (pQueryAttr->interval.interval < 0) {
    tscError("%p illegal value of aggregation time interval in query msg: %" PRId64, addr,
             (int64_t)pQueryInfo->interval.interval);
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  if (pQueryAttr->pGroupbyExpr != NULL && pQueryAttr->pGroupbyExpr->numOfGroupCols < 0) {
    tscError("%p illegal value of numOfGroupCols in query msg: %d", addr, pQueryInfo->groupbyExpr.numOfGroupCols);
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t doAddTableName(char* nextStr, char** str, SArray* pNameArray, SSqlObj* pSql) {
  int32_t code = TSDB_CODE_SUCCESS;
  SSqlCmd* pCmd = &pSql->cmd;

  char  tablename[TSDB_TABLE_FNAME_LEN] = {0};
  int32_t len = 0;

  if (nextStr == NULL) {
    tstrncpy(tablename, *str, TSDB_TABLE_FNAME_LEN);
    len = (int32_t) strlen(tablename);
  } else {
    len = (int32_t)(nextStr - (*str));
    if (len >= TSDB_TABLE_NAME_LEN) {
      sprintf(pCmd->payload, "table name too long");
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    memcpy(tablename, *str, nextStr - (*str));
    tablename[len] = '\0';
  }

  (*str) = nextStr + 1;
  len = (int32_t)strtrim(tablename);

  SToken sToken = {.n = len, .type = TK_ID, .z = tablename};
  tGetToken(tablename, &sToken.type);

  // Check if the table name available or not
  if (tscValidateName(&sToken) != TSDB_CODE_SUCCESS) {
    sprintf(pCmd->payload, "table name is invalid");
    return TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
  }

  SName name = {0};
  if ((code = tscSetTableFullName(&name, &sToken, pSql)) != TSDB_CODE_SUCCESS) {
    return code;
  }

  memset(tablename, 0, tListLen(tablename));
  tNameExtractFullName(&name, tablename);

  char* p = strdup(tablename);
  taosArrayPush(pNameArray, &p);
  return TSDB_CODE_SUCCESS;
}

int32_t nameComparFn(const void* n1, const void* n2) {
  int32_t ret = strcmp(*(char**)n1, *(char**)n2);
  if (ret == 0) {
    return 0;
  } else {
    return ret > 0? 1:-1;
  }
}

static void freeContent(void* p) {
  char* ptr = *(char**)p;
  tfree(ptr);
}


int tscTransferTableNameList(SSqlObj *pSql, const char *pNameList, int32_t length, SArray* pNameArray) {
  SSqlCmd *pCmd = &pSql->cmd;

  pCmd->command = TSDB_SQL_MULTI_META;
  pCmd->msgType = TSDB_MSG_TYPE_CM_TABLES_META;

  int   code = TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
  char *str = (char *)pNameList;

  SQueryStmtInfo *pQueryInfo = tscGetQueryInfoS(pCmd);
  if (pQueryInfo == NULL) {
    pSql->res.code = terrno;
    return terrno;
  }

  char *nextStr;
  while (1) {
    nextStr = strchr(str, ',');
    if (nextStr == NULL) {
      code = doAddTableName(nextStr, &str, pNameArray, pSql);
      break;
    }

    code = doAddTableName(nextStr, &str, pNameArray, pSql);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (taosArrayGetSize(pNameArray) > TSDB_MULTI_TABLEMETA_MAX_NUM) {
      code = TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
      sprintf(pCmd->payload, "tables over the max number");
      return code;
    }
  }

  size_t len = taosArrayGetSize(pNameArray);
  if (len == 1) {
    return TSDB_CODE_SUCCESS;
  }

  if (len > TSDB_MULTI_TABLEMETA_MAX_NUM) {
    code = TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH;
    sprintf(pCmd->payload, "tables over the max number");
    return code;
  }

  taosArraySort(pNameArray, nameComparFn);
  taosArrayRemoveDuplicate(pNameArray, nameComparFn, freeContent);
  return TSDB_CODE_SUCCESS;
}

bool vgroupInfoIdentical(SNewVgroupInfo *pExisted, SVgroupMsg* src) {
  assert(pExisted != NULL && src != NULL);
  if (pExisted->numOfEps != src->numOfEps) {
    return false;
  }

  for(int32_t i = 0; i < pExisted->numOfEps; ++i) {
    if (pExisted->ep[i].port != src->epAddr[i].port) {
      return false;
    }

    if (strncmp(pExisted->ep[i].fqdn, src->epAddr[i].fqdn, tListLen(pExisted->ep[i].fqdn)) != 0) {
      return false;
    }
  }

  return true;
}

SNewVgroupInfo createNewVgroupInfo(SVgroupMsg *pVgroupMsg) {
  assert(pVgroupMsg != NULL);

  SNewVgroupInfo info = {0};
  info.numOfEps = pVgroupMsg->numOfEps;
  info.vgId     = pVgroupMsg->vgId;
  info.inUse    = 0;   // 0 is the default value of inUse in case of multiple replica

  assert(info.numOfEps >= 1 && info.vgId >= 1);
  for(int32_t i = 0; i < pVgroupMsg->numOfEps; ++i) {
    tstrncpy(info.ep[i].fqdn, pVgroupMsg->epAddr[i].fqdn, TSDB_FQDN_LEN);
    info.ep[i].port = pVgroupMsg->epAddr[i].port;
  }

  return info;
}

char* cloneCurrentDBName(SSqlObj* pSql) {
  char        *p = NULL;
  HttpContext *pCtx = NULL;

  pthread_mutex_lock(&pSql->pTscObj->mutex);
  STscObj *pTscObj = pSql->pTscObj;
  switch (pTscObj->from) {
    case TAOS_REQ_FROM_HTTP:
      pCtx = pSql->param;
      if (pCtx && pCtx->db[0] != '\0') {
        char db[TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN] = {0};
        int32_t len = sprintf(db, "%s%s%s", pTscObj->acctId, TS_PATH_DELIMITER, pCtx->db);
        assert(len <= sizeof(db));

        p = strdup(db);
      }
      break;
    default:
      break;
  }
  if (p == NULL) {
    p = strdup(pSql->pTscObj->db);
  }
  pthread_mutex_unlock(&pSql->pTscObj->mutex);

  return p;
}

#endif