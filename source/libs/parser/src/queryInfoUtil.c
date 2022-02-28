#include "os.h"
#include "queryInfoUtil.h"
#include "function.h"
#include "parser.h"
#include "parserUtil.h"

static struct SSchema _s = {
    .colId = TSDB_TBNAME_COLUMN_INDEX,
    .type  = TSDB_DATA_TYPE_BINARY,
    .bytes = TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE,
    .name = "tbname",
};

SSchema* getTbnameColumnSchema() {
  return &_s;
}

SArray* getCurrentExprList(SQueryStmtInfo* pQueryInfo) {
  assert(pQueryInfo != NULL && pQueryInfo->exprListLevelIndex >= 0 && pQueryInfo->exprListLevelIndex < 10);
  return pQueryInfo->exprList[pQueryInfo->exprListLevelIndex];
}

size_t getNumOfExprs(SQueryStmtInfo* pQueryInfo) {
  SArray* pExprList = getCurrentExprList(pQueryInfo);
  return taosArrayGetSize(pExprList);
}

SSchema* getOneColumnSchema(const STableMeta* pTableMeta, int32_t colIndex) {
  assert(pTableMeta != NULL && pTableMeta->schema != NULL && colIndex >= 0 && colIndex < (getNumOfColumns(pTableMeta) + getNumOfTags(pTableMeta)));

  SSchema* pSchema = (SSchema*) pTableMeta->schema;
  return &pSchema[colIndex];
}

STableComInfo getTableInfo(const STableMeta* pTableMeta) {
  assert(pTableMeta != NULL);
  return pTableMeta->tableInfo;
}

int32_t getNumOfColumns(const STableMeta* pTableMeta) {
  assert(pTableMeta != NULL);
  // table created according to super table, use data from super table
  return getTableInfo(pTableMeta).numOfColumns;
}

int32_t getNumOfTags(const STableMeta* pTableMeta) {
  assert(pTableMeta != NULL);
  return getTableInfo(pTableMeta).numOfTags;
}

SSchema *getTableColumnSchema(const STableMeta *pTableMeta) {
  assert(pTableMeta != NULL);
  return (SSchema*) pTableMeta->schema;
}

SSchema* getTableTagSchema(const STableMeta* pTableMeta) {
  assert(pTableMeta != NULL && (pTableMeta->tableType == TSDB_SUPER_TABLE || pTableMeta->tableType == TSDB_CHILD_TABLE));
  return getOneColumnSchema(pTableMeta, getTableInfo(pTableMeta).numOfColumns);
}

static tExprNode* createFunctionExprNode(const char* funcName, struct SSourceParam *pParam) {
  tExprNode** p = malloc(pParam->num * POINTER_BYTES);

  if (pParam->pColumnList != NULL) {
    for(int32_t i = 0; i < pParam->num; ++i) {
      p[i] = calloc(1, sizeof(tExprNode));
      p[i]->nodeType = TEXPR_COL_NODE;

      SColumn* pSrc = taosArrayGetP(pParam->pColumnList, i);
      SSchema* pSchema = calloc(1, sizeof(SSchema));

      tstrncpy(pSchema->name, pSrc->name, tListLen(pSchema->name));
      pSchema->type  = pSrc->info.type;
      pSchema->bytes = pSrc->info.bytes;
      pSchema->colId = pSrc->info.colId;
      p[i]->pSchema = pSchema;
    }
  } else {
    assert(pParam->pColumnList == NULL);
    for(int32_t i = 0; i < pParam->num; ++i) {
      p[i] = taosArrayGetP(pParam->pExprNodeList, i);
    }
  }

  tExprNode* pNode = calloc(1, sizeof(tExprNode));

  pNode->nodeType = TEXPR_FUNCTION_NODE;
  tstrncpy(pNode->_function.functionName, funcName, tListLen(pNode->_function.functionName));
  pNode->_function.pChild = p;
  pNode->_function.num = pParam->num;

  return pNode;
}

SExprInfo* createBinaryExprInfo(tExprNode* pNode, SSchema* pResSchema) {
  assert(pNode != NULL && pResSchema != NULL);

  SExprInfo* pExpr = calloc(1, sizeof(SExprInfo));
  if (pExpr == NULL) {
    return NULL;
  }

  pExpr->pExpr = pNode;
  memcpy(&pExpr->base.resSchema, pResSchema, sizeof(SSchema));
  return pExpr;
}

SExprInfo* createExprInfo(STableMetaInfo* pTableMetaInfo, const char* funcName, SSourceParam* pSourceParam, SSchema* pResSchema, int16_t interSize) {
  SExprInfo* pExpr = calloc(1, sizeof(SExprInfo));
  if (pExpr == NULL) {
    return NULL;
  }

  uint64_t uid = 0;
  if (pTableMetaInfo->pTableMeta) {
    uid = pTableMetaInfo->pTableMeta->uid;
  }

  SSqlExpr* p = &pExpr->base;

  p->pColumns   = calloc(pSourceParam->num, sizeof(SColumn));
  p->numOfCols  = pSourceParam->num;
  p->interBytes = interSize;
  memcpy(&p->resSchema, pResSchema, sizeof(SSchema));

  if (pSourceParam->pExprNodeList != NULL) {
    pExpr->pExpr = createFunctionExprNode(funcName, pSourceParam);
    return pExpr;
  }

  SColumn* pCol = taosArrayGetP(pSourceParam->pColumnList, 0);
  if (pCol->info.colId == TSDB_TBNAME_COLUMN_INDEX) {
    assert(pSourceParam->num == 1);

    SSchema* s = getTbnameColumnSchema();
    setColumn(p->pColumns, uid, pTableMetaInfo->aliasName, TSDB_COL_TAG, s);

    pExpr->pExpr = createFunctionExprNode(funcName, pSourceParam);
  } else if (TSDB_COL_IS_UD_COL(pCol->flag) || strcmp(funcName, "block_dist") == 0) {
    setColumn(p->pColumns, uid, pTableMetaInfo->aliasName, TSDB_COL_UDC, pResSchema);
    pExpr->pExpr = createFunctionExprNode(funcName, pSourceParam);
  } else {
    for(int32_t i = 0; i < pSourceParam->num; ++i) {
      SColumn* c = taosArrayGetP(pSourceParam->pColumnList, i);
      p->pColumns[i] = *c;
    }
    pExpr->pExpr = createFunctionExprNode(funcName, pSourceParam);
  }

  return pExpr;
}

void addExprInfo(SArray* pExprList, int32_t index, SExprInfo* pExprInfo, int32_t level) {
  assert(pExprList != NULL );

  int32_t num = (int32_t) taosArrayGetSize(pExprList);
  if (index == num) {
    taosArrayPush(pExprList, &pExprInfo);
  } else {
    taosArrayInsert(pExprList, index, &pExprInfo);
  }

#if 0
  if (pExprInfo->pExpr->nodeType == TEXPR_FUNCTION_NODE) {
    printf("add function: %s, level:%d, total:%ld\n", pExprInfo->pExpr->_function.functionName, level, taosArrayGetSize(pExprList));
  } else {
    printf("add operator: %s, level:%d, total:%ld\n", pExprInfo->base.resSchema.name, level, taosArrayGetSize(pExprList));
  }
#endif

}

void updateExprInfo(SExprInfo* pExprInfo, int16_t functionId, int32_t colId, int16_t srcColumnIndex, int16_t resType, int16_t resSize) {
  assert(pExprInfo != NULL);

  SSqlExpr* pse = &pExprInfo->base;
  assert(0);

  pse->resSchema.type  = resType;
  pse->resSchema.bytes = resSize;
}

SExprInfo* getExprInfo(SQueryStmtInfo* pQueryInfo, int32_t index) {
  assert(pQueryInfo != NULL && pQueryInfo->exprList && index >= 0);
  return taosArrayGetP(getCurrentExprList(pQueryInfo), index);
}

void destroyExprInfo(SExprInfo* pExprInfo) {
  tExprTreeDestroy(pExprInfo->pExpr, NULL);

  for(int32_t i = 0; i < pExprInfo->base.numOfParams; ++i) {
    taosVariantDestroy(&pExprInfo->base.param[i]);
  }

  tfree(pExprInfo->base.pColumns);
  tfree(pExprInfo);
}

void dropOneLevelExprInfo(SArray* pExprInfo) {
  size_t size = taosArrayGetSize(pExprInfo);

  for (int32_t i = 0; i < size; ++i) {
    SExprInfo* pExpr = taosArrayGetP(pExprInfo, i);
    destroyExprInfo(pExpr);
  }

  taosArrayDestroy(pExprInfo);
}

void dropAllExprInfo(SArray** pExprInfo, int32_t numOfLevel) {
  for(int32_t i = 0; i < numOfLevel; ++i) {
    dropOneLevelExprInfo(pExprInfo[i]);
  }
}

void addExprInfoParam(SSqlExpr* pExpr, char* argument, int32_t type, int32_t bytes) {
  assert (pExpr != NULL || argument != NULL || bytes != 0);

  // set parameter value
  // transfer to tVariant from byte data/no ascii data
  taosVariantCreateFromBinary(&pExpr->param[pExpr->numOfParams], argument, bytes, type);
  pExpr->numOfParams += 1;

  assert(pExpr->numOfParams <= 3);
}

int32_t getExprFunctionId(SExprInfo *pExprInfo) {
  assert(pExprInfo != NULL && pExprInfo->pExpr != NULL && pExprInfo->pExpr->nodeType == TEXPR_FUNCTION_NODE);
  return 0;
}

void assignExprInfo(SExprInfo* dst, const SExprInfo* src) {
  assert(dst != NULL && src != NULL/* && src->base.numOfCols > 0*/);

  *dst = *src;
#if 0
  if (src->base.flist.numOfFilters > 0) {
    dst->base.flist.filterInfo = calloc(src->base.flist.numOfFilters, sizeof(SColumnFilterInfo));
    memcpy(dst->base.flist.filterInfo, src->base.flist.filterInfo, sizeof(SColumnFilterInfo) * src->base.flist.numOfFilters);
  }
#endif

  dst->pExpr = exprdup(src->pExpr);
  if (src->base.numOfCols > 0) {
    dst->base.pColumns = calloc(src->base.numOfCols, sizeof(SColumn));
    memcpy(dst->base.pColumns, src->base.pColumns, sizeof(SColumn) * src->base.numOfCols);
  } else {
    dst->base.pColumns = NULL;
  }

  memset(dst->base.param, 0, sizeof(SVariant) * tListLen(dst->base.param));
  for (int32_t j = 0; j < src->base.numOfParams; ++j) {
    taosVariantAssign(&dst->base.param[j], &src->base.param[j]);
  }
}

int32_t copyExprInfoList(SArray* dst, const SArray* src, uint64_t uid, bool deepcopy) {
  assert(src != NULL && dst != NULL);

  size_t size = taosArrayGetSize(src);
  for (int32_t i = 0; i < size; ++i) {
    SExprInfo* pExpr = taosArrayGetP(src, i);
    uint64_t exprUid = pExpr->base.pColumns->uid;

    if (exprUid == uid) {
      if (deepcopy) {
        SExprInfo* p1 = calloc(1, sizeof(SExprInfo));
        assignExprInfo(p1, pExpr);

        taosArrayPush(dst, &p1);
      } else {
        taosArrayPush(dst, &pExpr);
      }
    }
  }

  return 0;
}

int32_t copyAllExprInfo(SArray* dst, const SArray* src, bool deepcopy) {
  assert(src != NULL && dst != NULL);

  size_t size = taosArrayGetSize(src);
  for (int32_t i = 0; i < size; ++i) {
    SExprInfo* pExpr = taosArrayGetP(src, i);

    SExprInfo* p1 = calloc(1, sizeof(SExprInfo));
    assignExprInfo(p1, pExpr);
    taosArrayPush(dst, &p1);
  }

  return 0;
}

//void* tSqlExprDestroy(SExprInfo* pExpr) {
//  if (pExpr == NULL) {
//    return NULL;
//  }
//
//  SSqlExpr* p = &pExpr->base;
//  for(int32_t i = 0; i < tListLen(p->param); ++i) {
//    taosVariantDestroy(&p->param[i]);
//  }
//
//  if (p->flist.numOfFilters > 0) {
//    tfree(p->flist.filterInfo);
//  }
//
//  if (pExpr->pExpr != NULL) {
//    tExprTreeDestroy(pExpr->pExpr, NULL);
//  }
//
//  tfree(pExpr);
//  return NULL;
//}

int32_t getResRowLength(SArray* pExprList) {
  size_t num = taosArrayGetSize(pExprList);
  if (num == 0) {
    return 0;
  }

  int32_t size = 0;
  for(int32_t i = 0; i < num; ++i) {
    SExprInfo* pExpr = taosArrayGetP(pExprList, i);
    size += pExpr->base.resSchema.bytes;
  }

  return size;
}

SArray* extractFunctionList(SArray* pExprInfoList) {
  assert(pExprInfoList != NULL);

  size_t len = taosArrayGetSize(pExprInfoList);
  SArray* p = taosArrayInit(len, POINTER_BYTES);

  for(int32_t i = 0; i < len; ++i) {
    SExprInfo* pExprInfo = taosArrayGetP(pExprInfoList, i);
    if (pExprInfo->pExpr->nodeType == TEXPR_FUNCTION_NODE) {
      char* name = strdup(pExprInfo->pExpr->_function.functionName);
      taosArrayPush(p, &name);
    } else {
      char* name = strdup("project");
      taosArrayPush(p, &name);
    }
  }

  return p;
}

bool tscHasColumnFilter(SQueryStmtInfo* pQueryInfo) {
  // filter on primary timestamp column
  if (pQueryInfo->window.skey != INT64_MIN || pQueryInfo->window.ekey != INT64_MAX) {
    return true;
  }

  size_t size = taosArrayGetSize(pQueryInfo->colList);
  for (int32_t i = 0; i < size; ++i) {
    SColumn* pCol = taosArrayGetP(pQueryInfo->colList, i);
    if (pCol->info.flist.numOfFilters > 0) {
      return true;
    }
  }

  return false;
}

int32_t getExprFunctionLevel(const SQueryStmtInfo* pQueryInfo) {
  int32_t n = 10;

  int32_t level = 0;
  for(int32_t i = 0; i < n; ++i) {
    SArray* pList = pQueryInfo->exprList[i];
    if (taosArrayGetSize(pList) > 0) {
      level += 1;
    }
  }

  return level;
}