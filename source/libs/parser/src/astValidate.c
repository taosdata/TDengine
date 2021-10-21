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

#include "ttime.h"
#include "parserInt.h"
#include "parserUtil.h"
#include "queryInfoUtil.h"

#define TSQL_TBNAME_L "tbname"
#define DEFAULT_PRIMARY_TIMESTAMP_COL_NAME "_c0"
#define VALID_COLUMN_INDEX(index) (((index).tableIndex >= 0) && ((index).columnIndex >= TSDB_TBNAME_COLUMN_INDEX))

// -1 is tbname column index, so here use the -2 as the initial value
#define COLUMN_INDEX_INITIAL_VAL (-2)
#define COLUMN_INDEX_INITIALIZER { COLUMN_INDEX_INITIAL_VAL, COLUMN_INDEX_INITIAL_VAL }

static int32_t validateSelectNodeList(SQueryStmtInfo* pQueryInfo, SArray* pSelNodeList, bool joinQuery, bool timeWindowQuery, bool outerQuery, char* msg, int32_t msgBufLen);

size_t tscNumOfExprs(SQueryStmtInfo* pQueryInfo) {
  return taosArrayGetSize(pQueryInfo->exprList);
}

  static int32_t evaluateImpl(tSqlExpr* pExpr, int32_t tsPrecision) {
  int32_t code = 0;
  if (pExpr->type == SQL_NODE_EXPR) {
    code = evaluateImpl(pExpr->pLeft, tsPrecision);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    code = evaluateImpl(pExpr->pRight, tsPrecision);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (pExpr->pLeft->type == SQL_NODE_VALUE && pExpr->pRight->type == SQL_NODE_VALUE) {
      tSqlExpr* pLeft  = pExpr->pLeft;
      tSqlExpr* pRight = pExpr->pRight;
      if ((pLeft->tokenId == TK_TIMESTAMP && (pRight->tokenId == TK_INTEGER || pRight->tokenId == TK_FLOAT)) ||
          ((pRight->tokenId == TK_TIMESTAMP && (pLeft->tokenId == TK_INTEGER || pLeft->tokenId == TK_FLOAT)))) {
        return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
      } else if (pLeft->tokenId == TK_TIMESTAMP && pRight->tokenId == TK_TIMESTAMP) {
        tSqlExprEvaluate(pExpr);
      } else {
        tSqlExprEvaluate(pExpr);
      }
    } else {
      // Other types of expressions are not evaluated, they will be handled during the validation of the abstract syntax tree.
    }
  } else if (pExpr->type == SQL_NODE_VALUE) {
    if (pExpr->tokenId == TK_NOW) {
      pExpr->value.i64   = taosGetTimestamp(tsPrecision);
      pExpr->value.nType = TSDB_DATA_TYPE_BIGINT;
      pExpr->tokenId     = TK_TIMESTAMP;
    } else if (pExpr->tokenId == TK_VARIABLE) {
      char    unit = 0;
      SToken* pToken = &pExpr->exprToken;
      int32_t ret = parseAbsoluteDuration(pToken->z, pToken->n, &pExpr->value.i64, &unit, tsPrecision);
      if (ret != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
      }

      pExpr->value.nType = TSDB_DATA_TYPE_BIGINT;
      pExpr->tokenId = TK_TIMESTAMP;
    }  else if (pExpr->tokenId == TK_NULL) {
      pExpr->value.nType = TSDB_DATA_TYPE_NULL;
    } else if (pExpr->tokenId == TK_INTEGER || pExpr->tokenId == TK_STRING || pExpr->tokenId == TK_FLOAT || pExpr->tokenId == TK_BOOL) {
      SToken* pToken = &pExpr->exprToken;

      int32_t tokenType = pToken->type;
      toTSDBType(tokenType);
      taosVariantCreate(&pExpr->value, pToken->z, pToken->n, tokenType);
    }

    return  TSDB_CODE_SUCCESS;
    // other types of data are handled in the parent level.
  }

  return  TSDB_CODE_SUCCESS;
}

static STableMetaInfo* getMetaInfo(SQueryStmtInfo* pQueryInfo, int32_t tableIndex) {
  assert(pQueryInfo != NULL);

  if (pQueryInfo->pTableMetaInfo == NULL) {
    assert(pQueryInfo->numOfTables == 0);
    return NULL;
  }

  assert(tableIndex >= 0 && tableIndex <= pQueryInfo->numOfTables && pQueryInfo->pTableMetaInfo != NULL);
  return pQueryInfo->pTableMetaInfo[tableIndex];
}

typedef struct SVgroupTableInfo {
  SVgroupMsg  vgInfo;
  SArray     *itemList;   // SArray<STableIdInfo>
} SVgroupTableInfo;

void freeVgroupTableInfo(SArray* pVgroupTables) {
  if (pVgroupTables == NULL) {
    return;
  }

  size_t num = taosArrayGetSize(pVgroupTables);
  for (size_t i = 0; i < num; i++) {
    SVgroupTableInfo* pInfo = taosArrayGet(pVgroupTables, i);
    taosArrayDestroy(pInfo->itemList);
  }

  taosArrayDestroy(pVgroupTables);
}

void destroyFilterInfo(SColumnFilterList* pFilterList) {
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

void columnDestroy(SColumn* pCol) {
  destroyFilterInfo(&pCol->info.flist);
  free(pCol);
}

void destroyColumnList(SArray* pColumnList) {
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

void clearTableMetaInfo(STableMetaInfo* pTableMetaInfo) {
  if (pTableMetaInfo == NULL) {
    return;
  }

  tfree(pTableMetaInfo->pTableMeta);
  tfree(pTableMetaInfo->vgroupList);

  destroyColumnList(pTableMetaInfo->tagColList);
  pTableMetaInfo->tagColList = NULL;

  free(pTableMetaInfo);
}

void clearAllTableMetaInfo(SQueryStmtInfo* pQueryInfo, bool removeMeta, uint64_t id) {
  for(int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    STableMetaInfo* pTableMetaInfo = getMetaInfo(pQueryInfo, i);
    if (removeMeta) {
      // todo
//      removeCachedTableMeta(pTableMetaInfo, id);
    }

    clearTableMetaInfo(pTableMetaInfo);
  }

  tfree(pQueryInfo->pTableMetaInfo);
}

static STableMeta* extractTempTableMetaFromSubquery(SQueryStmtInfo* pUpstream) {
  STableMetaInfo* pUpstreamTableMetaInfo /*= tscGetMetaInfo(pUpstream, 0)*/;

  int32_t     numOfColumns = pUpstream->fieldsInfo.numOfOutput;
  STableMeta *meta = calloc(1, sizeof(STableMeta) + sizeof(SSchema) * numOfColumns);
  meta->tableType = TSDB_TEMP_TABLE;

  STableComInfo *info = &meta->tableInfo;
  info->numOfColumns = numOfColumns;
  info->precision    = pUpstreamTableMetaInfo->pTableMeta->tableInfo.precision;
  info->numOfTags    = 0;

  int32_t n = 0;
  for(int32_t i = 0; i < numOfColumns; ++i) {
//    SInternalField* pField = tscFieldInfoGetInternalField(&pUpstream->fieldsInfo, i);
//    if (!pField->visible) {
//      continue;
//    }
//
//    meta->schema[n].bytes = pField->field.bytes;
//    meta->schema[n].type  = pField->field.type;
//
//    SExprInfo* pExpr = pField->pExpr;
//    meta->schema[n].colId = pExpr->base.resColId;
//    tstrncpy(meta->schema[n].name, pField->pExpr->base.aliasName, TSDB_COL_NAME_LEN);
//    info->rowSize += meta->schema[n].bytes;
//
//    n += 1;
  }

  info->numOfColumns = n;
  return meta;
}

void initQueryInfo(SQueryStmtInfo* pQueryInfo) {
  assert(pQueryInfo->fieldsInfo.internalField == NULL);
  //assert(0);
//  pQueryInfo->fieldsInfo.internalField = taosArrayInit(4, sizeof(SInternalField));

  assert(pQueryInfo->exprList == NULL);

  pQueryInfo->exprList       = taosArrayInit(4, POINTER_BYTES);
  pQueryInfo->colList        = taosArrayInit(4, POINTER_BYTES);
  pQueryInfo->udColumnId     = TSDB_UD_COLUMN_INDEX;
  pQueryInfo->limit.limit    = -1;
  pQueryInfo->limit.offset   = 0;

  pQueryInfo->slimit.limit   = -1;
  pQueryInfo->slimit.offset  = 0;
  pQueryInfo->pUpstream      = taosArrayInit(4, POINTER_BYTES);
  pQueryInfo->window         = TSWINDOW_INITIALIZER;
//  pQueryInfo->multigroupResult = true;
}

int32_t validateSqlNode(SSqlNode* pSqlNode, SQueryStmtInfo* pQueryInfo, char* msg, int32_t msgBufLen);

SColumn* tscColumnListInsert(SArray* pColumnList, int32_t columnIndex, uint64_t uid, SSchema* pSchema) {
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

static int32_t doValidateSubquery(SSqlNode* pSqlNode, int32_t index, SQueryStmtInfo* pQueryInfo, char* msgBuf, int32_t msgBufLen) {
  SRelElementPair* subInfo = taosArrayGet(pSqlNode->from->list, index);

  // union all is not support currently
  SSqlNode* p = taosArrayGetP(subInfo->pSubquery, 0);
  if (taosArrayGetSize(subInfo->pSubquery) >= 2) {
    return buildInvalidOperationMsg(msgBuf, msgBufLen, "not support union in subquery");
  }

  SQueryStmtInfo* pSub = calloc(1, sizeof(SQueryStmtInfo));
  initQueryInfo(pSub);

  SArray *pUdfInfo = NULL;
  if (pQueryInfo->pUdfInfo) {
    pUdfInfo = taosArrayDup(pQueryInfo->pUdfInfo);
  }

  pSub->pUdfInfo = pUdfInfo;
  pSub->pDownstream = pQueryInfo;
  int32_t code = validateSqlNode(p, pSub, msgBuf, msgBufLen);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // create dummy table meta info
  STableMetaInfo* pTableMetaInfo1 = calloc(1, sizeof(STableMetaInfo));
  if (pTableMetaInfo1 == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  pTableMetaInfo1->pTableMeta = extractTempTableMetaFromSubquery(pSub);

  if (subInfo->aliasName.n > 0) {
    if (subInfo->aliasName.n >= TSDB_TABLE_FNAME_LEN) {
      tfree(pTableMetaInfo1);
      return buildInvalidOperationMsg(msgBuf, msgBufLen, "subquery alias name too long");
    }

    tstrncpy(pTableMetaInfo1->aliasName, subInfo->aliasName.z, subInfo->aliasName.n + 1);
  }

  taosArrayPush(pQueryInfo->pUpstream, &pSub);

  // NOTE: order mix up in subquery not support yet.
  pQueryInfo->order = pSub->order;

  STableMetaInfo** tmp = realloc(pQueryInfo->pTableMetaInfo, (pQueryInfo->numOfTables + 1) * POINTER_BYTES);
  if (tmp == NULL) {
    tfree(pTableMetaInfo1);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  pQueryInfo->pTableMetaInfo = tmp;

  pQueryInfo->pTableMetaInfo[pQueryInfo->numOfTables] = pTableMetaInfo1;
  pQueryInfo->numOfTables += 1;

  // all columns are added into the table column list
  STableMeta* pMeta = pTableMetaInfo1->pTableMeta;
  int32_t startOffset = (int32_t) taosArrayGetSize(pQueryInfo->colList);

  for(int32_t i = 0; i < pMeta->tableInfo.numOfColumns; ++i) {
    tscColumnListInsert(pQueryInfo->colList, i + startOffset, pMeta->uid, &pMeta->schema[i]);
  }

  return TSDB_CODE_SUCCESS;
}

SVgroupsInfo* tscVgroupInfoClone(SVgroupsInfo *vgroupList) {
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

void tscVgroupTableCopy(SVgroupTableInfo* info, SVgroupTableInfo* pInfo) {
  memset(info, 0, sizeof(SVgroupTableInfo));
  info->vgInfo = pInfo->vgInfo;

  if (pInfo->itemList) {
    info->itemList = taosArrayDup(pInfo->itemList);
  }
}

SArray* tscVgroupTableInfoDup(SArray* pVgroupTables) {
  if (pVgroupTables == NULL) {
    return NULL;
  }

  size_t num = taosArrayGetSize(pVgroupTables);
  SArray* pa = taosArrayInit(num, sizeof(SVgroupTableInfo));

  SVgroupTableInfo info;
  for (size_t i = 0; i < num; i++) {
    SVgroupTableInfo* pInfo = taosArrayGet(pVgroupTables, i);
    tscVgroupTableCopy(&info, pInfo);

    taosArrayPush(pa, &info);
  }

  return pa;
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

void tscColumnCopy(SColumn* pDest, const SColumn* pSrc) {
  destroyFilterInfo(&pDest->info.flist);

  pDest->columnIndex       = pSrc->columnIndex;
  pDest->tableUid          = pSrc->tableUid;
  pDest->info.flist.numOfFilters = pSrc->info.flist.numOfFilters;
  pDest->info.flist.filterInfo   = tFilterInfoDup(pSrc->info.flist.filterInfo, pSrc->info.flist.numOfFilters);
  pDest->info.type        = pSrc->info.type;
  pDest->info.colId        = pSrc->info.colId;
  pDest->info.bytes      = pSrc->info.bytes;
}

SColumn* tscColumnClone(const SColumn* src) {
  assert(src != NULL);

  SColumn* dst = calloc(1, sizeof(SColumn));
  if (dst == NULL) {
    return NULL;
  }

  tscColumnCopy(dst, src);
  return dst;
}

void tscColumnListCopy(SArray* dst, const SArray* src, uint64_t tableUid) {
  assert(src != NULL && dst != NULL);

  size_t num = taosArrayGetSize(src);
  for (int32_t i = 0; i < num; ++i) {
    SColumn* pCol = taosArrayGetP(src, i);

    if (pCol->tableUid == tableUid) {
      SColumn* p = tscColumnClone(pCol);
      taosArrayPush(dst, &p);
    }
  }
}

STableMetaInfo* tscAddTableMetaInfo(SQueryStmtInfo* pQueryInfo, char* name, STableMeta* pTableMeta,
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
//  pTableMetaInfo->tableMetaSize = (pTableMetaInfo->pTableMeta == NULL)? 0:tscGetTableMetaSize(pTableMeta);
//  pTableMetaInfo->tableMetaCapacity = (size_t)(pTableMetaInfo->tableMetaSize);

  if (vgroupList != NULL) {
    pTableMetaInfo->vgroupList = tscVgroupInfoClone(vgroupList);
  }

  // TODO handle malloc failure
  pTableMetaInfo->tagColList = taosArrayInit(4, POINTER_BYTES);
  if (pTableMetaInfo->tagColList == NULL) {
    return NULL;
  }

  if (pTagCols != NULL && pTableMetaInfo->pTableMeta != NULL) {
    tscColumnListCopy(pTableMetaInfo->tagColList, pTagCols, pTableMetaInfo->pTableMeta->uid);
  }

  pQueryInfo->numOfTables += 1;
  return pTableMetaInfo;
}

STableMetaInfo* tscAddEmptyMetaInfo(SQueryStmtInfo* pQueryInfo) {
  return tscAddTableMetaInfo(pQueryInfo, NULL, NULL, NULL, NULL, NULL);
}

int32_t getTableIndexImpl(SToken* pTableToken, SQueryStmtInfo* pQueryInfo, SColumnIndex* pIndex) {
  if (pTableToken->n == 0) {  // only one table and no table name prefix in column name
    if (pQueryInfo->numOfTables == 1) {
      pIndex->tableIndex = 0;
    } else {
      pIndex->tableIndex = COLUMN_INDEX_INITIAL_VAL;
    }

    return TSDB_CODE_SUCCESS;
  }

  pIndex->tableIndex = COLUMN_INDEX_INITIAL_VAL;
  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    STableMetaInfo* pTableMetaInfo /*= tscGetMetaInfo(pQueryInfo, i)*/;
    char* name = pTableMetaInfo->aliasName;
    if (strncasecmp(name, pTableToken->z, pTableToken->n) == 0 && strlen(name) == pTableToken->n) {
      pIndex->tableIndex = i;
      break;
    }
  }

  if (pIndex->tableIndex < 0) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  return TSDB_CODE_SUCCESS;
}

/*
 * tablePrefix.columnName
 * extract table name and save it in pTable, with only column name in pToken
 */
void extractTableNameFromToken(SToken* pToken, SToken* pTable) {
  const char sep = TS_PATH_DELIMITER[0];

  if (pToken == pTable || pToken == NULL || pTable == NULL) {
    return;
  }

  char* r = strnchr(pToken->z, sep, pToken->n, false);

  if (r != NULL) {  // record the table name token
    pTable->n = (uint32_t)(r - pToken->z);
    pTable->z = pToken->z;

    r += 1;
    pToken->n -= (uint32_t)(r - pToken->z);
    pToken->z = r;
  }
}

int32_t getTableIndexByName(SToken* pToken, SQueryStmtInfo* pQueryInfo, SColumnIndex* pIndex) {
  SToken tableToken = {0};
  extractTableNameFromToken(pToken, &tableToken);

  if (getTableIndexImpl(&tableToken, pQueryInfo, pIndex) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  return TSDB_CODE_SUCCESS;
}

static int16_t doGetColumnIndex(SQueryStmtInfo* pQueryInfo, int32_t index, SToken* pToken) {
  STableMeta* pTableMeta = getMetaInfo(pQueryInfo, index)->pTableMeta;

  int32_t  numOfCols = getNumOfColumns(pTableMeta) + getNumOfTags(pTableMeta);
  SSchema* pSchema = getTableColumnSchema(pTableMeta);

  int16_t columnIndex = COLUMN_INDEX_INITIAL_VAL;

  for (int32_t i = 0; i < numOfCols; ++i) {
    if (pToken->n != strlen(pSchema[i].name)) {
      continue;
    }

    if (strncasecmp(pSchema[i].name, pToken->z, pToken->n) == 0) {
      columnIndex = i;
      break;
    }
  }

  return columnIndex;
}

static bool isTablenameToken(SToken* token) {
  SToken tmpToken = *token;
  SToken tableToken = {0};

  extractTableNameFromToken(&tmpToken, &tableToken);
  return (tmpToken.n == strlen(TSQL_TBNAME_L) && strncasecmp(TSQL_TBNAME_L, tmpToken.z, tmpToken.n) == 0);
}

int32_t doGetColumnIndexByName(SToken* pToken, SQueryStmtInfo* pQueryInfo, SColumnIndex* pIndex, char* msg, int32_t msgBufLen) {
  const char* msg0 = "ambiguous column name";
  const char* msg1 = "invalid column name";

  if (isTablenameToken(pToken)) {
    pIndex->columnIndex = TSDB_TBNAME_COLUMN_INDEX;
  } else if (strlen(DEFAULT_PRIMARY_TIMESTAMP_COL_NAME) == pToken->n &&
             strncasecmp(pToken->z, DEFAULT_PRIMARY_TIMESTAMP_COL_NAME, pToken->n) == 0) {
    pIndex->columnIndex = PRIMARYKEY_TIMESTAMP_COL_INDEX; // just make runtime happy, need fix java test case InsertSpecialCharacterJniTest
  } else if (pToken->n == 0) {
    pIndex->columnIndex = PRIMARYKEY_TIMESTAMP_COL_INDEX; // just make runtime happy, need fix java test case InsertSpecialCharacterJniTest
  } else {
    // not specify the table name, try to locate the table index by column name
    if (pIndex->tableIndex == COLUMN_INDEX_INITIAL_VAL) {
      for (int16_t i = 0; i < pQueryInfo->numOfTables; ++i) {
        int16_t colIndex = doGetColumnIndex(pQueryInfo, i, pToken);

        if (colIndex != COLUMN_INDEX_INITIAL_VAL) {
          if (pIndex->columnIndex != COLUMN_INDEX_INITIAL_VAL) {
            return buildInvalidOperationMsg(msg, msgBufLen, msg0);
          } else {
            pIndex->tableIndex = i;
            pIndex->columnIndex = colIndex;
          }
        }
      }
    } else {  // table index is valid, get the column index
      int16_t colIndex = doGetColumnIndex(pQueryInfo, pIndex->tableIndex, pToken);
      if (colIndex != COLUMN_INDEX_INITIAL_VAL) {
        pIndex->columnIndex = colIndex;
      }
    }

    if (pIndex->columnIndex == COLUMN_INDEX_INITIAL_VAL) {
      return buildInvalidOperationMsg(msg, msgBufLen, msg1);
    }
  }

  if (VALID_COLUMN_INDEX(*pIndex)) {
    return TSDB_CODE_SUCCESS;
  } else {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }
}

int32_t getColumnIndexByName(const SToken* pToken, SQueryStmtInfo* pQueryInfo, SColumnIndex* pIndex, char* msg, int32_t msgBufLen) {
  if (pQueryInfo->pTableMetaInfo == NULL || pQueryInfo->numOfTables == 0) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  SToken tmpToken = *pToken;

  if (getTableIndexByName(&tmpToken, pQueryInfo, pIndex) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  return doGetColumnIndexByName(&tmpToken, pQueryInfo, pIndex, msg, msgBufLen);
}

int32_t validateGroupbyNode(SQueryStmtInfo* pQueryInfo, SArray* pList, char* msg, int32_t msgBufLen) {
  const char* msg1 = "too many columns in group by clause";
  const char* msg2 = "invalid column name in group by clause";
  const char* msg3 = "columns from one table allowed as group by columns";
  const char* msg4 = "join query does not support group by";
  const char* msg5 = "not allowed column type for group by";
  const char* msg6 = "tags not allowed for table query";
  const char* msg7 = "not support group by expression";
  const char* msg8 = "normal column can only locate at the end of group by clause";

  // todo : handle two tables situation
  STableMetaInfo* pTableMetaInfo = NULL;
  if (pList == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  if (pQueryInfo->numOfTables > 1) {
    return buildInvalidOperationMsg(msg, msgBufLen, msg4);
  }

  SGroupbyExpr* pGroupExpr = &pQueryInfo->groupbyExpr;
  if (pGroupExpr->columnInfo == NULL) {
    pGroupExpr->columnInfo = taosArrayInit(4, sizeof(SColIndex));
  }

  if (pQueryInfo->colList == NULL) {
    pQueryInfo->colList = taosArrayInit(4, POINTER_BYTES);
  }

  if (pGroupExpr->columnInfo == NULL || pQueryInfo->colList == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  int32_t numOfGroupCols = (int16_t) taosArrayGetSize(pList);
  if (numOfGroupCols > TSDB_MAX_TAGS) {
    return buildInvalidOperationMsg(msg, msgBufLen, msg1);
  }

  SSchema *pSchema       = NULL;
  int32_t tableIndex     = COLUMN_INDEX_INITIAL_VAL;

  size_t num = taosArrayGetSize(pList);
  for (int32_t i = 0; i < num; ++i) {
    SListItem * pItem = taosArrayGet(pList, i);
    SVariant* pVar = &pItem->pVar;

    SToken token = {pVar->nLen, pVar->nType, pVar->pz};

    SColumnIndex index = COLUMN_INDEX_INITIALIZER;
    if (getColumnIndexByName(&token, pQueryInfo, &index, msg, msgBufLen) != TSDB_CODE_SUCCESS) {
      return buildInvalidOperationMsg(msg, msgBufLen, msg2);
    }

    if (tableIndex == COLUMN_INDEX_INITIAL_VAL) {
      tableIndex = index.tableIndex;
    } else if (tableIndex != index.tableIndex) {
      return buildInvalidOperationMsg(msg, msgBufLen, msg3);
    }

    pTableMetaInfo = getMetaInfo(pQueryInfo, index.tableIndex);
    STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;

    if (index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
      pSchema = getTbnameColumnSchema();
    } else {
      pSchema = getOneColumnSchema(pTableMeta, index.columnIndex);
    }

    int32_t numOfCols = getNumOfColumns(pTableMeta);
    bool groupTag = (index.columnIndex == TSDB_TBNAME_COLUMN_INDEX || index.columnIndex >= numOfCols);

    if (groupTag) {
      if (!UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
        return buildInvalidOperationMsg(msg, msgBufLen, msg6);
      }

      int32_t relIndex = index.columnIndex;
      if (index.columnIndex != TSDB_TBNAME_COLUMN_INDEX) {
        relIndex -= numOfCols;
      }

      SColIndex colIndex = { .colIndex = relIndex, .flag = TSDB_COL_TAG, .colId = pSchema->colId, };
      strncpy(colIndex.name, pSchema->name, tListLen(colIndex.name));
      taosArrayPush(pGroupExpr->columnInfo, &colIndex);

      index.columnIndex = relIndex;
      tscColumnListInsert(pTableMetaInfo->tagColList, index.columnIndex, pTableMeta->uid, pSchema);
    } else {
      // check if the column type is valid, here only support the bool/tinyint/smallint/bigint group by
      if (pSchema->type == TSDB_DATA_TYPE_TIMESTAMP || pSchema->type == TSDB_DATA_TYPE_FLOAT || pSchema->type == TSDB_DATA_TYPE_DOUBLE) {
        return buildInvalidOperationMsg(msg, msgBufLen, msg5);
      }

      tscColumnListInsert(pQueryInfo->colList, index.columnIndex, pTableMeta->uid, pSchema);

      SColIndex colIndex = { .colIndex = index.columnIndex, .flag = TSDB_COL_NORMAL, .colId = pSchema->colId };
      strncpy(colIndex.name, pSchema->name, tListLen(colIndex.name));

      taosArrayPush(pGroupExpr->columnInfo, &colIndex);
      pQueryInfo->groupbyExpr.orderType = TSDB_ORDER_ASC;
      numOfGroupCols++;
    }
  }

  // 1. only one normal column allowed in the group by clause
  // 2. the normal column in the group by clause can only located in the end position
  if (numOfGroupCols > 1) {
    return buildInvalidOperationMsg(msg, msgBufLen, msg7);
  }

  for(int32_t i = 0; i < num; ++i) {
    SColIndex* pIndex = taosArrayGet(pGroupExpr->columnInfo, i);
    if (TSDB_COL_IS_NORMAL_COL(pIndex->flag) && i != num - 1) {
      return buildInvalidOperationMsg(msg, msgBufLen, msg8);
    }
  }

  pQueryInfo->groupbyExpr.tableIndex = tableIndex;
  return TSDB_CODE_SUCCESS;
}

int32_t validateSqlNode(SSqlNode* pSqlNode, SQueryStmtInfo* pQueryInfo, char* msg, int32_t msgBufLen) {
  assert(pSqlNode != NULL && (pSqlNode->from == NULL || taosArrayGetSize(pSqlNode->from->list) > 0));

  const char* msg1 = "point interpolation query needs timestamp";
  const char* msg2 = "too many tables in from clause";
  const char* msg3 = "start(end) time of query range required or time range too large";
  const char* msg4 = "interval query not supported, since the result of sub query not include valid timestamp column";
  const char* msg5 = "only tag query not compatible with normal column filter";
  const char* msg6 = "not support stddev/percentile/interp in the outer query yet";
  const char* msg7 = "derivative/twa/irate requires timestamp column exists in subquery";
  const char* msg8 = "condition missing for join query";
  const char* msg9 = "not support 3 level select";

  int32_t  code = TSDB_CODE_SUCCESS;
  STableMetaInfo  *pTableMetaInfo = tscAddEmptyMetaInfo(pQueryInfo);


  /*
   * handle the sql expression without from subclause
   * select server_status();
   * select server_version();
   * select client_version();
   * select database();
   */
  if (pSqlNode->from == NULL) {
    assert(pSqlNode->fillType == NULL && pSqlNode->pGroupby == NULL && pSqlNode->pWhere == NULL &&
           pSqlNode->pSortOrder == NULL);
    assert(0);
//    return doLocalQueryProcess(pCmd, pQueryInfo, pSqlNode);
  }

  if (pSqlNode->from->type == SQL_NODE_FROM_SUBQUERY) {
    pQueryInfo->numOfTables = 0;

    // parse the subquery in the first place
    int32_t numOfSub = (int32_t)taosArrayGetSize(pSqlNode->from->list);
    for (int32_t i = 0; i < numOfSub; ++i) {
      // check if there is 3 level select
      SRelElementPair* subInfo = taosArrayGet(pSqlNode->from->list, i);
      SSqlNode* p = taosArrayGetP(subInfo->pSubquery, 0);
      if (p->from->type == SQL_NODE_FROM_SUBQUERY) {
        return buildInvalidOperationMsg(msg, msgBufLen, msg9);
      }

      code = doValidateSubquery(pSqlNode, i, pQueryInfo, msg, msgBufLen);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }

    int32_t timeWindowQuery =
        (TPARSER_HAS_TOKEN(pSqlNode->interval.interval) || TPARSER_HAS_TOKEN(pSqlNode->sessionVal.gap));
//    TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_TABLE_QUERY);

    // parse the group by clause in the first place
    if (validateGroupbyNode(pQueryInfo, pSqlNode->pGroupby, msg, msgBufLen) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }


    if (validateSelectNodeList(pQueryInfo, pSqlNode->pSelNodeList, false, timeWindowQuery, true, msg, msgBufLen) !=
        TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    // todo NOT support yet
    for (int32_t i = 0; i < tscNumOfExprs(pQueryInfo); ++i) {
      SExprInfo* pExpr = getExprInfo(pQueryInfo, i);
      int32_t    f = pExpr->base.functionId;
      if (f == TSDB_FUNC_STDDEV || f == TSDB_FUNC_PERCT || f == TSDB_FUNC_INTERP) {
        return buildInvalidOperationMsg(msg, msgBufLen, msg6);
      }

      if ((timeWindowQuery || pQueryInfo->stateWindow) && f == TSDB_FUNC_LAST) {
        pExpr->base.numOfParams = 1;
        pExpr->base.param[0].i64 = TSDB_ORDER_ASC;
        pExpr->base.param[0].nType = TSDB_DATA_TYPE_INT;
      }
    }

    STableMeta* pTableMeta = getMetaInfo(pQueryInfo, 0)->pTableMeta;
    SSchema*    pSchema = getOneColumnSchema(pTableMeta, 0);

    if (pSchema->type != TSDB_DATA_TYPE_TIMESTAMP) {
      int32_t numOfExprs = (int32_t)tscNumOfExprs(pQueryInfo);

      for (int32_t i = 0; i < numOfExprs; ++i) {
        SExprInfo* pExpr = getExprInfo(pQueryInfo, i);

        int32_t f = pExpr->base.functionId;
        if (f == TSDB_FUNC_DERIVATIVE || f == TSDB_FUNC_TWA || f == TSDB_FUNC_IRATE) {
          return buildInvalidOperationMsg(msg, msgBufLen, msg7);
        }
      }
    }

    // validate the query filter condition info
    if (pSqlNode->pWhere != NULL) {
      if (validateWhereNode(pQueryInfo, &pSqlNode->pWhere, pSql) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
    } else {
      if (pQueryInfo->numOfTables > 1) {
        return buildInvalidOperationMsg(msg, msgBufLen, msg8);
      }
    }

    // validate the interval info
    if (validateIntervalNode(pSql, pQueryInfo, pSqlNode) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    } else {
      if (validateSessionNode(pCmd, pQueryInfo, pSqlNode) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      // parse the window_state
      if (validateStateWindowNode(pCmd, pQueryInfo, pSqlNode, false) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      if (isTimeWindowQuery(pQueryInfo)) {
        // check if the first column of the nest query result is timestamp column
        SColumn* pCol = taosArrayGetP(pQueryInfo->colList, 0);
        if (pCol->info.type != TSDB_DATA_TYPE_TIMESTAMP) {
          return buildInvalidOperationMsg(msg, msgBufLen, msg4);
        }

        if (validateFunctionsInIntervalOrGroupbyQuery(pCmd, pQueryInfo) != TSDB_CODE_SUCCESS) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
        }
      }
    }

    // disable group result mixed up if interval/session window query exists.
    if (isTimeWindowQuery(pQueryInfo)) {
      size_t num = taosArrayGetSize(pQueryInfo->pUpstream);
      for(int32_t i = 0; i < num; ++i) {
        SQueryStmtInfo* pUp = taosArrayGetP(pQueryInfo->pUpstream, i);
        pUp->multigroupResult = false;
      }
    }

    // parse the having clause in the first place
    int32_t joinQuery = (pSqlNode->from != NULL && taosArrayGetSize(pSqlNode->from->list) > 1);
    if (validateHavingClause(pQueryInfo, pSqlNode->pHaving, pCmd, pSqlNode->pSelNodeList, joinQuery, timeWindowQuery) !=
        TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if ((code = validateLimitNode(pCmd, pQueryInfo, pSqlNode, pSql)) != TSDB_CODE_SUCCESS) {
      return code;
    }

    // set order by info
    if (validateOrderbyNode(pCmd, pQueryInfo, pSqlNode, tscGetTableSchema(pTableMeta)) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if ((code = doFunctionsCompatibleCheck(pCmd, pQueryInfo, tscGetErrorMsgPayload(pCmd))) != TSDB_CODE_SUCCESS) {
      return code;
    }

//    updateFunctionInterBuf(pQueryInfo, false);
    updateLastScanOrderIfNeeded(pQueryInfo);

    if ((code = validateFillNode(pCmd, pQueryInfo, pSqlNode)) != TSDB_CODE_SUCCESS) {
      return code;
    }
  } else {
    pQueryInfo->command = TSDB_SQL_SELECT;

    size_t numOfTables = taosArrayGetSize(pSqlNode->from->list);
    if (numOfTables > TSDB_MAX_JOIN_TABLE_NUM) {
      return buildInvalidOperationMsg(msg, msgBufLen, msg2);
    }

    // set all query tables, which are maybe more than one.
    code = doLoadAllTableMeta(pSql, pQueryInfo, pSqlNode, (int32_t) numOfTables);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    bool isSTable = UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo);

    int32_t type = isSTable? TSDB_QUERY_TYPE_STABLE_QUERY:TSDB_QUERY_TYPE_TABLE_QUERY;
    TSDB_QUERY_SET_TYPE(pQueryInfo->type, type);

    // parse the group by clause in the first place
    if (validateGroupbyNode(pQueryInfo, pSqlNode->pGroupby, pCmd) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }
    pQueryInfo->onlyHasTagCond = true;
    // set where info
    if (pSqlNode->pWhere != NULL) {
      if (validateWhereNode(pQueryInfo, &pSqlNode->pWhere, pSql) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      pSqlNode->pWhere = NULL;
    } else {
      if (taosArrayGetSize(pSqlNode->from->list) > 1) { // Cross join not allowed yet
        return buildInvalidOperationMsg(msg, msgBufLen, "cross join not supported yet");
      }
    }

    int32_t joinQuery = (pSqlNode->from != NULL && taosArrayGetSize(pSqlNode->from->list) > 1);
    int32_t timeWindowQuery =
        (TPARSER_HAS_TOKEN(pSqlNode->interval.interval) || TPARSER_HAS_TOKEN(pSqlNode->sessionVal.gap));

    if (validateSelectNodeList(pQueryInfo, pSqlNode->pSelNodeList, joinQuery, timeWindowQuery, false, msg, msgBufLen) !=
        TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if (isSTable && tscQueryTags(pQueryInfo) && pQueryInfo->distinct && !pQueryInfo->onlyHasTagCond) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    // parse the window_state
    if (validateStateWindowNode(pCmd, pQueryInfo, pSqlNode, isSTable) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    // set order by info
    if (validateOrderbyNode(pCmd, pQueryInfo, pSqlNode, tscGetTableSchema(pTableMetaInfo->pTableMeta)) !=
        TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    // set interval value
    if (validateIntervalNode(pSql, pQueryInfo, pSqlNode) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if (tscQueryTags(pQueryInfo)) {
      SExprInfo* pExpr1 = getExprInfo(pQueryInfo, 0);

      if (pExpr1->base.functionId != TSDB_FUNC_TID_TAG) {
        if ((pQueryInfo->colCond && taosArrayGetSize(pQueryInfo->colCond) > 0) || IS_TSWINDOW_SPECIFIED(pQueryInfo->window))  {
          return buildInvalidOperationMsg(msg, msgBufLen, msg5);
        }
      }
    }

    // parse the having clause in the first place
    if (validateHavingClause(pQueryInfo, pSqlNode->pHaving, pCmd, pSqlNode->pSelNodeList, joinQuery, timeWindowQuery) !=
        TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    /*
     * transfer sql functions that need secondary merge into another format
     * in dealing with super table queries such as: count/first/last
     */
    if (validateSessionNode(pCmd, pQueryInfo, pSqlNode) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if (isTimeWindowQuery(pQueryInfo) && (validateFunctionsInIntervalOrGroupbyQuery(pCmd, pQueryInfo) != TSDB_CODE_SUCCESS)) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if (isSTable) {
      tscTansformFuncForSTableQuery(pQueryInfo);
      if (hasUnsupportFunctionsForSTableQuery(pCmd, pQueryInfo)) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
    }

    // no result due to invalid query time range
    if (pQueryInfo->window.skey > pQueryInfo->window.ekey) {
      pQueryInfo->command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
      return TSDB_CODE_SUCCESS;
    }

    if (!hasTimestampForPointInterpQuery(pQueryInfo)) {
      return buildInvalidOperationMsg(msg, msgBufLen, msg1);
    }

    // in case of join query, time range is required.
    if (QUERY_IS_JOIN_QUERY(pQueryInfo->type)) {
      uint64_t timeRange = (uint64_t)pQueryInfo->window.ekey - pQueryInfo->window.skey;
      if (timeRange == 0 && pQueryInfo->window.skey == 0) {
        return buildInvalidOperationMsg(msg, msgBufLen, msg3);
      }
    }

    if ((code = validateLimitNode(pCmd, pQueryInfo, pSqlNode, pSql)) != TSDB_CODE_SUCCESS) {
      return code;
    }

    if ((code = doFunctionsCompatibleCheck(pCmd, pQueryInfo,tscGetErrorMsgPayload(pCmd))) != TSDB_CODE_SUCCESS) {
      return code;
    }

    updateLastScanOrderIfNeeded(pQueryInfo);
    tscFieldInfoUpdateOffset(pQueryInfo);
//    updateFunctionInterBuf(pQueryInfo, isSTable);

    if ((code = validateFillNode(pCmd, pQueryInfo, pSqlNode)) != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  { // set the query info
    pQueryInfo->projectionQuery = tscIsProjectionQuery(pQueryInfo);
    pQueryInfo->hasFilter = tscHasColumnFilter(pQueryInfo);
    pQueryInfo->simpleAgg = isSimpleAggregateRv(pQueryInfo);
    pQueryInfo->onlyTagQuery = onlyTagPrjFunction(pQueryInfo);
    pQueryInfo->groupbyColumn = tscGroupbyColumn(pQueryInfo);
    pQueryInfo->arithmeticOnAgg   = tsIsArithmeticQueryOnAggResult(pQueryInfo);
    pQueryInfo->orderProjectQuery = tscOrderedProjectionQueryOnSTable(pQueryInfo, 0);

    SExprInfo** p = NULL;
    int32_t numOfExpr = 0;
    pTableMetaInfo = getMetaInfo(pQueryInfo, 0);
    code = createProjectionExpr(pQueryInfo, pTableMetaInfo, &p, &numOfExpr);
    if (pQueryInfo->exprList1 == NULL) {
      pQueryInfo->exprList1 = taosArrayInit(4, POINTER_BYTES);
    }

    taosArrayAddBatch(pQueryInfo->exprList1, (void*) p, numOfExpr);
    tfree(p);
  }

  return TSDB_CODE_SUCCESS;  // Does not build query message here
}

int32_t validateSelectNodeList(SQueryStmtInfo* pQueryInfo, SArray* pSelNodeList, bool joinQuery, bool timeWindowQuery, bool outerQuery, char* msg, int32_t msgBufLen) {
  assert(pSelNodeList != NULL);

  const char* msg1 = "too many items in selection clause";
  const char* msg2 = "functions or others can not be mixed up";
  const char* msg3 = "not support query expression";
  const char* msg4 = "not support distinct mixed with proj/agg func";
  const char* msg5 = "invalid function name";
  const char* msg6 = "not support distinct mixed with join";
  const char* msg7 = "not support distinct mixed with groupby";
  const char* msg8 = "not support distinct in nest query";
  const char* msg9 = "_block_dist not support subquery, only support stable/table";

  // too many result columns not support order by in query
  if (taosArrayGetSize(pSelNodeList) > TSDB_MAX_COLUMNS) {
    return buildInvalidOperationMsg(msg, msgBufLen, msg1);
  }

  if (pQueryInfo->colList == NULL) {
    pQueryInfo->colList = taosArrayInit(4, POINTER_BYTES);
  }


  bool hasDistinct = false;
  bool hasAgg      = false;
  size_t numOfExpr = taosArrayGetSize(pSelNodeList);
  int32_t distIdx = -1;
  for (int32_t i = 0; i < numOfExpr; ++i) {
    int32_t outputIndex = (int32_t)tscNumOfExprs(pQueryInfo);
    tSqlExprItem* pItem = taosArrayGet(pSelNodeList, i);
    if (hasDistinct == false) {
      hasDistinct = (pItem->distinct == true);
      distIdx     =  hasDistinct ? i : -1;
    }

    int32_t type = pItem->pNode->type;
    if (type == SQL_NODE_SQLFUNCTION) {
      hasAgg = true;
      if (hasDistinct)  break;

      pItem->functionId = qIsBuiltinFunction(pItem->pNode->Expr.operand.z, pItem->pNode->Expr.operand.n);

      if (pItem->pNode->functionId == TSDB_FUNC_BLKINFO && taosArrayGetSize(pQueryInfo->pUpstream) > 0) {
        return buildInvalidOperationMsg(msg, msgBufLen, msg9);
      }

      SUdfInfo* pUdfInfo = NULL;
      if (pItem->pNode->functionId < 0) {
        pUdfInfo = isValidUdf(pQueryInfo->pUdfInfo, pItem->pNode->Expr.operand.z, pItem->pNode->Expr.operand.n);
        if (pUdfInfo == NULL) {
          return buildInvalidOperationMsg(msg, msgBufLen, msg5);
        }

        pItem->pNode->functionId = pUdfInfo->functionId;
      }

      // sql function in selection clause, append sql function info in pSqlCmd structure sequentially
      if (addExprAndResultField(pCmd, pQueryInfo, outputIndex, pItem, true, pUdfInfo) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
    } else if (type == SQL_NODE_TABLE_COLUMN || type == SQL_NODE_VALUE) {
      // use the dynamic array list to decide if the function is valid or not
      // select table_name1.field_name1, table_name2.field_name2  from table_name1, table_name2
      if (addProjectionExprAndResultField(pCmd, pQueryInfo, pItem, outerQuery) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
    } else if (type == SQL_NODE_EXPR) {
      int32_t code = handleArithmeticExpr(pCmd, pQueryInfo, i, pItem);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    } else {
      return buildInvalidOperationMsg(msg, msgBufLen, msg3);
    }

    if (pQueryInfo->fieldsInfo.numOfOutput > TSDB_MAX_COLUMNS) {
      return buildInvalidOperationMsg(msg, msgBufLen, msg1);
    }
  }

  //TODO(dengyihao), refactor as function
  //handle distinct func mixed with other func
  if (hasDistinct == true) {
    if (distIdx != 0 || hasAgg) {
      return buildInvalidOperationMsg(msg, msgBufLen, msg4);
    }
    if (joinQuery) {
      return buildInvalidOperationMsg(msg, msgBufLen, msg6);
    }
    if (pQueryInfo->groupbyExpr.numOfGroupCols  != 0) {
      return buildInvalidOperationMsg(msg, msgBufLen, msg7);
    }
    if (pQueryInfo->pDownstream != NULL) {
      return buildInvalidOperationMsg(msg, msgBufLen, msg8);
    }

    pQueryInfo->distinct = true;
  }


  // there is only one user-defined column in the final result field, add the timestamp column.
  size_t numOfSrcCols = taosArrayGetSize(pQueryInfo->colList);
  if ((numOfSrcCols <= 0 || !hasNoneUserDefineExpr(pQueryInfo)) && !tscQueryTags(pQueryInfo) && !tscQueryBlockInfo(pQueryInfo)) {
    addPrimaryTsColIntoResult(pQueryInfo, pCmd);
  }

  if (!functionCompatibleCheck(pQueryInfo, joinQuery, timeWindowQuery)) {
    return buildInvalidOperationMsg(msg, msgBufLen, msg2);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t evaluateSqlNode(SSqlNode* pNode, int32_t tsPrecision, char* msg, int32_t msgBufLen) {
  assert(pNode != NULL && msg != NULL && msgBufLen > 0);
  if (pNode->pWhere == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = evaluateImpl(pNode->pWhere, tsPrecision);
  if (code != TSDB_CODE_SUCCESS) {
    strncpy(msg, "invalid time expression in sql", msgBufLen);
    return code;
  }

  return code;
}

int32_t qParserValidateSqlNode(struct SCatalog* pCatalog, SSqlInfo* pInfo, SQueryStmtInfo* pQueryInfo, int64_t id, char* msgBuf, int32_t msgBufLen) {
  //1. if it is a query, get the meta info and continue.
  assert(pCatalog != NULL && pInfo != NULL);
  int32_t code = 0;
#if 0
  switch (pInfo->type) {
    case TSDB_SQL_DROP_TABLE:
    case TSDB_SQL_DROP_USER:
    case TSDB_SQL_DROP_ACCT:
    case TSDB_SQL_DROP_DNODE:
    case TSDB_SQL_DROP_DB: {
      const char* msg1 = "param name too long";
      const char* msg2 = "invalid name";

      SToken* pzName = taosArrayGet(pInfo->pMiscInfo->a, 0);
      if ((pInfo->type != TSDB_SQL_DROP_DNODE) && (parserValidateIdToken(pzName) != TSDB_CODE_SUCCESS)) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg2);
      }

      if (pInfo->type == TSDB_SQL_DROP_DB) {
        assert(taosArrayGetSize(pInfo->pMiscInfo->a) == 1);
        code = tNameSetDbName(&pTableMetaInfo->name, getAccountId(pSql), pzName);
        if (code != TSDB_CODE_SUCCESS) {
          return setInvalidOperatorMsg(msgBuf, msgBufLen, msg2);
        }

      } else if (pInfo->type == TSDB_SQL_DROP_TABLE) {
        assert(taosArrayGetSize(pInfo->pMiscInfo->a) == 1);

        code = tscSetTableFullName(&pTableMetaInfo->name, pzName, pSql);
        if(code != TSDB_CODE_SUCCESS) {
          return code;
        }
      } else if (pInfo->type == TSDB_SQL_DROP_DNODE) {
        if (pzName->type == TK_STRING) {
          pzName->n = strdequote(pzName->z);
        }
        strncpy(pCmd->payload, pzName->z, pzName->n);
      } else {  // drop user/account
        if (pzName->n >= TSDB_USER_LEN) {
          return setInvalidOperatorMsg(msgBuf, msgBufLen, msg3);
        }

        strncpy(pCmd->payload, pzName->z, pzName->n);
      }

      break;
    }

    case TSDB_SQL_USE_DB: {
      const char* msg = "invalid db name";
      SToken* pToken = taosArrayGet(pInfo->pMiscInfo->a, 0);

      if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg);
      }

      int32_t ret = tNameSetDbName(&pTableMetaInfo->name, getAccountId(pSql), pToken);
      if (ret != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg);
      }

      break;
    }

    case TSDB_SQL_RESET_CACHE: {
      return TSDB_CODE_SUCCESS;
    }

    case TSDB_SQL_SHOW: {
      if (setShowInfo(pSql, pInfo) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      break;
    }

    case TSDB_SQL_CREATE_FUNCTION:
    case TSDB_SQL_DROP_FUNCTION:  {
      code = handleUserDefinedFunc(pSql, pInfo);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      break;
    }

    case TSDB_SQL_ALTER_DB:
    case TSDB_SQL_CREATE_DB: {
      const char* msg1 = "invalid db name";
      const char* msg2 = "name too long";

      SCreateDbInfo* pCreateDB = &(pInfo->pMiscInfo->dbOpt);
      if (pCreateDB->dbname.n >= TSDB_DB_NAME_LEN) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg2);
      }

      char buf[TSDB_DB_NAME_LEN] = {0};
      SToken token = taosTokenDup(&pCreateDB->dbname, buf, tListLen(buf));

      if (tscValidateName(&token) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg1);
      }

      int32_t ret = tNameSetDbName(&pTableMetaInfo->name, getAccountId(pSql), &token);
      if (ret != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg2);
      }

      if (parseCreateDBOptions(pCmd, pCreateDB) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      break;
    }

    case TSDB_SQL_CREATE_DNODE: {
      const char* msg = "invalid host name (ip address)";

      if (taosArrayGetSize(pInfo->pMiscInfo->a) > 1) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg);
      }

      SToken* id = taosArrayGet(pInfo->pMiscInfo->a, 0);
      if (id->type == TK_STRING) {
        id->n = strdequote(id->z);
      }
      break;
    }

    case TSDB_SQL_CREATE_ACCT:
    case TSDB_SQL_ALTER_ACCT: {
      const char* msg1 = "invalid state option, available options[no, r, w, all]";
      const char* msg2 = "invalid user/account name";
      const char* msg3 = "name too long";

      SToken* pName = &pInfo->pMiscInfo->user.user;
      SToken* pPwd = &pInfo->pMiscInfo->user.passwd;

      if (handlePassword(pCmd, pPwd) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      if (pName->n >= TSDB_USER_LEN) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg3);
      }

      if (tscValidateName(pName) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg2);
      }

      SCreateAcctInfo* pAcctOpt = &pInfo->pMiscInfo->acctOpt;
      if (pAcctOpt->stat.n > 0) {
        if (pAcctOpt->stat.z[0] == 'r' && pAcctOpt->stat.n == 1) {
        } else if (pAcctOpt->stat.z[0] == 'w' && pAcctOpt->stat.n == 1) {
        } else if (strncmp(pAcctOpt->stat.z, "all", 3) == 0 && pAcctOpt->stat.n == 3) {
        } else if (strncmp(pAcctOpt->stat.z, "no", 2) == 0 && pAcctOpt->stat.n == 2) {
        } else {
          return setInvalidOperatorMsg(msgBuf, msgBufLen, msg1);
        }
      }

      break;
    }

    case TSDB_SQL_DESCRIBE_TABLE: {
      const char* msg1 = "invalid table name";

      SToken* pToken = taosArrayGet(pInfo->pMiscInfo->a, 0);
      if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg1);
      }
      // additional msg has been attached already
      code = tscSetTableFullName(&pTableMetaInfo->name, pToken, pSql);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      return tscGetTableMeta(pSql, pTableMetaInfo);
    }
    case TSDB_SQL_SHOW_CREATE_STABLE:
    case TSDB_SQL_SHOW_CREATE_TABLE: {
      const char* msg1 = "invalid table name";

      SToken* pToken = taosArrayGet(pInfo->pMiscInfo->a, 0);
      if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg1);
      }

      code = tscSetTableFullName(&pTableMetaInfo->name, pToken, pSql);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      return tscGetTableMeta(pSql, pTableMetaInfo);
    }
    case TSDB_SQL_SHOW_CREATE_DATABASE: {
      const char* msg1 = "invalid database name";

      SToken* pToken = taosArrayGet(pInfo->pMiscInfo->a, 0);
      if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg1);
      }

      if (pToken->n > TSDB_DB_NAME_LEN) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg1);
      }
      return tNameSetDbName(&pTableMetaInfo->name, getAccountId(pSql), pToken);
    }
    case TSDB_SQL_CFG_DNODE: {
      const char* msg2 = "invalid configure options or values, such as resetlog / debugFlag 135 / balance 'vnode:2-dnode:2' / monitor 1 ";
      const char* msg3 = "invalid dnode ep";

      /* validate the ip address */
      SMiscInfo* pMiscInfo = pInfo->pMiscInfo;

      /* validate the parameter names and options */
      if (validateDNodeConfig(pMiscInfo) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg2);
      }

      char* pMsg = pCmd->payload;

      SCfgDnodeMsg* pCfg = (SCfgDnodeMsg*)pMsg;

      SToken* t0 = taosArrayGet(pMiscInfo->a, 0);
      SToken* t1 = taosArrayGet(pMiscInfo->a, 1);

      t0->n = strdequote(t0->z);
      strncpy(pCfg->ep, t0->z, t0->n);

      if (validateEp(pCfg->ep) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg3);
      }

      strncpy(pCfg->config, t1->z, t1->n);

      if (taosArrayGetSize(pMiscInfo->a) == 3) {
        SToken* t2 = taosArrayGet(pMiscInfo->a, 2);

        pCfg->config[t1->n] = ' ';  // add sep
        strncpy(&pCfg->config[t1->n + 1], t2->z, t2->n);
      }

      break;
    }

    case TSDB_SQL_CREATE_USER:
    case TSDB_SQL_ALTER_USER: {
      const char* msg2 = "invalid user/account name";
      const char* msg3 = "name too long";
      const char* msg5 = "invalid user rights";
      const char* msg7 = "not support options";

      pCmd->command = pInfo->type;

      SUserInfo* pUser = &pInfo->pMiscInfo->user;
      SToken* pName = &pUser->user;
      SToken* pPwd = &pUser->passwd;

      if (pName->n >= TSDB_USER_LEN) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg3);
      }

      if (tscValidateName(pName) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg2);
      }

      if (pCmd->command == TSDB_SQL_CREATE_USER) {
        if (handlePassword(pCmd, pPwd) != TSDB_CODE_SUCCESS) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
        }
      } else {
        if (pUser->type == TSDB_ALTER_USER_PASSWD) {
          if (handlePassword(pCmd, pPwd) != TSDB_CODE_SUCCESS) {
            return TSDB_CODE_TSC_INVALID_OPERATION;
          }
        } else if (pUser->type == TSDB_ALTER_USER_PRIVILEGES) {
          assert(pPwd->type == TSDB_DATA_TYPE_NULL);

          SToken* pPrivilege = &pUser->privilege;

          if (strncasecmp(pPrivilege->z, "super", 5) == 0 && pPrivilege->n == 5) {
            pCmd->count = 1;
          } else if (strncasecmp(pPrivilege->z, "read", 4) == 0 && pPrivilege->n == 4) {
            pCmd->count = 2;
          } else if (strncasecmp(pPrivilege->z, "write", 5) == 0 && pPrivilege->n == 5) {
            pCmd->count = 3;
          } else {
            return setInvalidOperatorMsg(msgBuf, msgBufLen, msg5);
          }
        } else {
          return setInvalidOperatorMsg(msgBuf, msgBufLen, msg7);
        }
      }

      break;
    }

    case TSDB_SQL_CFG_LOCAL: {
      SMiscInfo  *pMiscInfo = pInfo->pMiscInfo;
      const char *msg = "invalid configure options or values";

      // validate the parameter names and options
      if (validateLocalConfig(pMiscInfo) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg);
      }

      int32_t numOfToken = (int32_t) taosArrayGetSize(pMiscInfo->a);
      assert(numOfToken >= 1 && numOfToken <= 2);

      SToken* t = taosArrayGet(pMiscInfo->a, 0);
      strncpy(pCmd->payload, t->z, t->n);
      if (numOfToken == 2) {
        SToken* t1 = taosArrayGet(pMiscInfo->a, 1);
        pCmd->payload[t->n] = ' ';  // add sep
        strncpy(&pCmd->payload[t->n + 1], t1->z, t1->n);
      }
      return TSDB_CODE_SUCCESS;
    }

    case TSDB_SQL_CREATE_TABLE: {
      SCreateTableSql* pCreateTable = pInfo->pCreateTableInfo;

      if (pCreateTable->type == TSQL_CREATE_TABLE || pCreateTable->type == TSQL_CREATE_STABLE) {
        if ((code = doCheckForCreateTable(pSql, 0, pInfo)) != TSDB_CODE_SUCCESS) {
          return code;
        }

      } else if (pCreateTable->type == TSQL_CREATE_TABLE_FROM_STABLE) {
        assert(pCmd->numOfCols == 0);
        if ((code = doCheckForCreateFromStable(pSql, pInfo)) != TSDB_CODE_SUCCESS) {
          return code;
        }

      } else if (pCreateTable->type == TSQL_CREATE_STREAM) {
        if ((code = doCheckForStream(pSql, pInfo)) != TSDB_CODE_SUCCESS) {
          return code;
        }
      }

      break;
    }

    case TSDB_SQL_SELECT: {
      const char * msg1 = "no nested query supported in union clause";
      code = loadAllTableMeta(pSql, pInfo);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      pQueryInfo = tscGetQueryInfo(pCmd);

      size_t size = taosArrayGetSize(pInfo->list);
      for (int32_t i = 0; i < size; ++i) {
        SSqlNode* pSqlNode = taosArrayGetP(pInfo->list, i);

        tscTrace("0x%"PRIx64" start to parse the %dth subclause, total:%"PRIzu, pSql->self, i, size);

        if (size > 1 && pSqlNode->from && pSqlNode->from->type == SQL_NODE_FROM_SUBQUERY) {
          return setInvalidOperatorMsg(msgBuf, msgBufLen, msg1);
        }

//        normalizeSqlNode(pSqlNode); // normalize the column name in each function
        if ((code = validateSqlNode(pSql, pSqlNode, pQueryInfo)) != TSDB_CODE_SUCCESS) {
          return code;
        }

        tscPrintSelNodeList(pSql, i);

        if ((i + 1) < size && pQueryInfo->sibling == NULL) {
          if ((code = tscAddQueryInfo(pCmd)) != TSDB_CODE_SUCCESS) {
            return code;
          }

          SArray *pUdfInfo = NULL;
          if (pQueryInfo->pUdfInfo) {
            pUdfInfo = taosArrayDup(pQueryInfo->pUdfInfo);
          }

          pQueryInfo = pCmd->active;
          pQueryInfo->pUdfInfo = pUdfInfo;
          pQueryInfo->udfCopy = true;
        }
      }

      if ((code = normalizeVarDataTypeLength(pCmd)) != TSDB_CODE_SUCCESS) {
        return code;
      }

      // set the command/global limit parameters from the first subclause to the sqlcmd object
      pCmd->active = pCmd->pQueryInfo;
      pCmd->command = pCmd->pQueryInfo->command;

      STableMetaInfo* pTableMetaInfo1 = getMetaInfo(pCmd->active, 0);
      if (pTableMetaInfo1->pTableMeta != NULL) {
        pSql->res.precision = tscGetTableInfo(pTableMetaInfo1->pTableMeta).precision;
      }

      return TSDB_CODE_SUCCESS;  // do not build query message here
    }

    case TSDB_SQL_ALTER_TABLE: {
      if ((code = setAlterTableInfo(pSql, pInfo)) != TSDB_CODE_SUCCESS) {
        return code;
      }

      break;
    }

    case TSDB_SQL_KILL_QUERY:
    case TSDB_SQL_KILL_STREAM:
    case TSDB_SQL_KILL_CONNECTION: {
      if ((code = setKillInfo(pSql, pInfo, pInfo->type)) != TSDB_CODE_SUCCESS) {
        return code;
      }
      break;
    }

    case TSDB_SQL_SYNC_DB_REPLICA: {
      const char* msg1 = "invalid db name";
      SToken* pzName = taosArrayGet(pInfo->pMiscInfo->a, 0);

      assert(taosArrayGetSize(pInfo->pMiscInfo->a) == 1);
      code = tNameSetDbName(&pTableMetaInfo->name, getAccountId(pSql), pzName);
      if (code != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg1);
      }
      break;
    }
    case TSDB_SQL_COMPACT_VNODE:{
      const char* msg = "invalid compact";
      if (setCompactVnodeInfo(pSql, pInfo) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(msgBuf, msgBufLen, msg);
      }
      break;
    }
    default:
      return setInvalidOperatorMsg(msgBuf, msgBufLen, "not support sql expression");
  }
#endif

  SMetaReq req = {0};
  SMetaData data = {0};

  // TODO: check if the qnode info has been cached already
  req.qNodeEpset = true;
  code = qParserExtractRequestedMetaInfo(pInfo, &req, msgBuf, msgBufLen);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // load the meta data from catalog
  code = catalogGetMetaData(pCatalog, &req, &data);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // evaluate the sqlnode
  STableMeta* pTableMeta = (STableMeta*) taosArrayGetP(data.pTableMeta, 0);
  assert(pTableMeta != NULL);

  size_t len = taosArrayGetSize(pInfo->list);
  for(int32_t i = 0; i < len; ++i) {
    SSqlNode* p = taosArrayGetP(pInfo->list, i);
    code = evaluateSqlNode(p, pTableMeta->tableInfo.precision, msgBuf, msgBufLen);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  // convert the sqlnode into queryinfo
  return code;
}
