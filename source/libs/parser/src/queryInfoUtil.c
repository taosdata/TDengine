#include "queryInfoUtil.h"
#include <function.h>
#include "astGenerator.h"
#include "function.h"
#include "os.h"
#include "parser.h"
#include "parserInt.h"
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

size_t getNumOfExprs(SQueryStmtInfo* pQueryInfo) {
  return taosArrayGetSize(pQueryInfo->exprList);
}

SSchema* getOneColumnSchema(const STableMeta* pTableMeta, int32_t colIndex) {
  assert(pTableMeta != NULL && pTableMeta->schema != NULL && colIndex >= 0 && colIndex < getNumOfColumns(pTableMeta));

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

static tExprNode* createUnaryFunctionExprNode(int32_t functionId, SSchema* pSchema, tExprNode* pColumnNode) {
  if (pColumnNode == NULL) {
    pColumnNode = calloc(1, sizeof(tExprNode));
    pColumnNode->nodeType = TEXPR_COL_NODE;
    pColumnNode->pSchema = calloc(1, sizeof(SSchema));
    memcpy(pColumnNode->pSchema, pSchema, sizeof(SSchema));
  } else {
    assert(pSchema == NULL);
  }

  tExprNode* pNode = calloc(1, sizeof(tExprNode));
  pNode->nodeType = TEXPR_UNARYEXPR_NODE;
  pNode->_node.functionId = functionId;
  pNode->_node.pLeft = pColumnNode;

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

SExprInfo* createExprInfo(STableMetaInfo* pTableMetaInfo, int16_t functionId, SColumnIndex* pColIndex, tExprNode* pParamExpr, SSchema* pResSchema, int16_t interSize) {
  SExprInfo* pExpr = calloc(1, sizeof(SExprInfo));
  if (pExpr == NULL) {
    return NULL;
  }

  SSqlExpr* p = &pExpr->base;

  if (pParamExpr != NULL) {
    pExpr->pExpr = createUnaryFunctionExprNode(functionId, NULL, pParamExpr);
  } else if (pColIndex->columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
    assert(pParamExpr == NULL);

    SSchema* s = getTbnameColumnSchema();
    p->colInfo.colId = TSDB_TBNAME_COLUMN_INDEX;
    pExpr->pExpr = createUnaryFunctionExprNode(functionId, s, pParamExpr);
  } else if (pColIndex->columnIndex <= TSDB_UD_COLUMN_INDEX || functionId == FUNCTION_BLKINFO) {
    assert(pParamExpr == NULL);

    p->colInfo.colId = pColIndex->columnIndex;
    SSchema s = createSchema(pResSchema->type, pResSchema->bytes, pColIndex->columnIndex, pResSchema->name);
    pExpr->pExpr = createUnaryFunctionExprNode(functionId, &s, pParamExpr);
  } else {
    int32_t len = tListLen(p->colInfo.name);
    if (TSDB_COL_IS_TAG(pColIndex->type)) {
      SSchema* pSchema = getTableTagSchema(pTableMetaInfo->pTableMeta);
      p->colInfo.colId = pSchema[pColIndex->columnIndex].colId;
      pExpr->pExpr = createUnaryFunctionExprNode(functionId, &pSchema[pColIndex->columnIndex], pParamExpr);
      snprintf(p->colInfo.name, len, "%s.%s", pTableMetaInfo->aliasName, pSchema[pColIndex->columnIndex].name);
    } else if (pTableMetaInfo->pTableMeta != NULL) {
      // in handling select database/version/server_status(), the pTableMeta is NULL
      SSchema* pSchema = getOneColumnSchema(pTableMetaInfo->pTableMeta, pColIndex->columnIndex);
      p->colInfo.colId = pSchema->colId;
      snprintf(p->colInfo.name, len, "%s.%s", pTableMetaInfo->aliasName, pSchema->name);

      pExpr->pExpr = createUnaryFunctionExprNode(functionId, pSchema, pParamExpr);
    }
  }

  p->colInfo.flag     = pColIndex->type;
  p->colInfo.colIndex = pColIndex->columnIndex;
  p->interBytes       = interSize;
  memcpy(&p->resSchema, pResSchema, sizeof(SSchema));

  if (pTableMetaInfo->pTableMeta) {
    p->uid = pTableMetaInfo->pTableMeta->uid;
  }

  return pExpr;
}

void addExprInfo(SQueryStmtInfo* pQueryInfo, int32_t index, SExprInfo* pExprInfo) {
  assert(pQueryInfo != NULL && pQueryInfo->exprList != NULL);

  int32_t num = (int32_t) taosArrayGetSize(pQueryInfo->exprList);
  if (index == num) {
    taosArrayPush(pQueryInfo->exprList, &pExprInfo);
  } else {
    taosArrayInsert(pQueryInfo->exprList, index, &pExprInfo);
  }
}

void updateExprInfo(SExprInfo* pExprInfo, int16_t functionId, int32_t colId, int16_t srcColumnIndex, int16_t resType, int16_t resSize) {
  assert(pExprInfo != NULL);

  SSqlExpr* pse = &pExprInfo->base;
  pExprInfo->pExpr->_node.functionId = functionId;

  pse->colInfo.colIndex = srcColumnIndex;
  pse->colInfo.colId  = colId;
  pse->resSchema.type  = resType;
  pse->resSchema.bytes = resSize;
}

SExprInfo* getExprInfo(SQueryStmtInfo* pQueryInfo, int32_t index) {
  assert(pQueryInfo != NULL && pQueryInfo->exprList && index >= 0);
  return taosArrayGetP(pQueryInfo->exprList, index);
}

void destroyExprInfo(SExprInfo* pExprInfo) {
  tExprTreeDestroy(pExprInfo->pExpr, NULL);

  for(int32_t i = 0; i < pExprInfo->base.numOfParams; ++i) {
    taosVariantDestroy(&pExprInfo->base.param[i]);
  }
  tfree(pExprInfo);
}

void dropAllExprInfo(SArray* pExprInfo) {
  size_t size = taosArrayGetSize(pExprInfo);

  for(int32_t i = 0; i < size; ++i) {
    SExprInfo* pExpr = taosArrayGetP(pExprInfo, i);
    destroyExprInfo(pExpr);
  }

  taosArrayDestroy(pExprInfo);
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
  assert(pExprInfo != NULL && pExprInfo->pExpr != NULL && pExprInfo->pExpr->nodeType == TEXPR_UNARYEXPR_NODE);
  return pExprInfo->pExpr->_node.functionId;
}

void assignExprInfo(SExprInfo* dst, const SExprInfo* src) {
  assert(dst != NULL && src != NULL);

  *dst = *src;
#if 0
  if (src->base.flist.numOfFilters > 0) {
    dst->base.flist.filterInfo = calloc(src->base.flist.numOfFilters, sizeof(SColumnFilterInfo));
    memcpy(dst->base.flist.filterInfo, src->base.flist.filterInfo, sizeof(SColumnFilterInfo) * src->base.flist.numOfFilters);
  }
#endif

//  dst->pExpr = exprdup(src->pExpr);
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

    if (pExpr->base.uid == uid) {
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

SArray* extractFunctionIdList(SArray* pExprInfoList) {
  assert(pExprInfoList != NULL);

  size_t len = taosArrayGetSize(pExprInfoList);
  SArray* p = taosArrayInit(len, sizeof(int32_t));
  for(int32_t i = 0; i < len; ++i) {
    SExprInfo* pExprInfo = taosArrayGetP(pExprInfoList, i);
    taosArrayPush(p, &pExprInfo->pExpr->_node.functionId);
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

//void tscClearInterpInfo(SQueryStmtInfo* pQueryInfo) {
//  if (!tscIsPointInterpQuery(pQueryInfo)) {
//    return;
//  }
//
//  pQueryInfo->fillType = TSDB_FILL_NONE;
//  tfree(pQueryInfo->fillVal);
//}