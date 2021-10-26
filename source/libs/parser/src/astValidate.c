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

#include "function.h"
#include "astGenerator.h"
#include "parserInt.h"
#include "parserUtil.h"
#include "queryInfoUtil.h"
#include "tbuffer.h"
#include "tglobal.h"
#include "tmsgtype.h"
#include "ttime.h"

#define TSQL_TBNAME_L "tbname"
#define DEFAULT_PRIMARY_TIMESTAMP_COL_NAME "_c0"
#define VALID_COLUMN_INDEX(index) (((index).tableIndex >= 0) && ((index).columnIndex >= TSDB_TBNAME_COLUMN_INDEX))

// -1 is tbname column index, so here use the -2 as the initial value
#define COLUMN_INDEX_INITIAL_VAL (-2)
#define COLUMN_INDEX_INITIALIZER { COLUMN_INDEX_INITIAL_VAL, COLUMN_INDEX_INITIAL_VAL }

static int32_t validateSelectNodeList(SQueryStmtInfo* pQueryInfo, SArray* pSelNodeList, bool outerQuery, SMsgBuf* pMsgBuf);

void setTokenAndResColumnName(tSqlExprItem* pItem, char* resColumnName, char* rawName, int32_t nameLength) {
  memset(resColumnName, 0, nameLength);

  int32_t len = ((int32_t)pItem->pNode->exprToken.n < nameLength) ? (int32_t)pItem->pNode->exprToken.n : nameLength;
  strncpy(rawName, pItem->pNode->exprToken.z, len);

  if (pItem->aliasName != NULL) {
    assert(strlen(pItem->aliasName) < nameLength);
    tstrncpy(resColumnName, pItem->aliasName, len);
  } else {
    strncpy(resColumnName, rawName, len);
  }
}

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

static STableMeta* extractTempTableMetaFromSubquery(SQueryStmtInfo* pUpstream) {
  STableMetaInfo* pUpstreamTableMetaInfo /*= getMetaInfo(pUpstream, 0)*/;

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
  pQueryInfo->fieldsInfo.internalField = taosArrayInit(4, sizeof(SInternalField));
  pQueryInfo->exprList       = taosArrayInit(4, POINTER_BYTES);
  pQueryInfo->colList        = taosArrayInit(4, POINTER_BYTES);
  pQueryInfo->udColumnId     = TSDB_UD_COLUMN_INDEX;
  pQueryInfo->limit.limit    = -1;
  pQueryInfo->limit.offset   = 0;

  pQueryInfo->slimit.limit   = -1;
  pQueryInfo->slimit.offset  = 0;
  pQueryInfo->pUpstream      = taosArrayInit(4, POINTER_BYTES);
  pQueryInfo->window         = TSWINDOW_INITIALIZER;
}

static int32_t doValidateSubquery(SSqlNode* pSqlNode, int32_t index, SQueryStmtInfo* pQueryInfo, SMsgBuf* pMsgBuf) {
  SRelElementPair* subInfo = taosArrayGet(pSqlNode->from->list, index);

  // union all is not support currently
  SSqlNode* p = taosArrayGetP(subInfo->pSubquery, 0);
  if (taosArrayGetSize(subInfo->pSubquery) >= 2) {
    return buildInvalidOperationMsg(pMsgBuf, "not support union in subquery");
  }

  SQueryStmtInfo* pSub = calloc(1, sizeof(SQueryStmtInfo));
  initQueryInfo(pSub);

  SArray *pUdfInfo = NULL;
  if (pQueryInfo->pUdfInfo) {
    pUdfInfo = taosArrayDup(pQueryInfo->pUdfInfo);
  }

  pSub->pUdfInfo = pUdfInfo;
  pSub->pDownstream = pQueryInfo;
  int32_t code = validateSqlNode(p, pSub, pMsgBuf);
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
      return buildInvalidOperationMsg(pMsgBuf, "subquery alias name too long");
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
    columnListInsert(pQueryInfo->colList, i + startOffset, pMeta->uid, &pMeta->schema[i]);
  }

  return TSDB_CODE_SUCCESS;
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
    STableMetaInfo* pTableMetaInfo = getMetaInfo(pQueryInfo, i);
    char* name = pTableMetaInfo->aliasName;
    if (strncasecmp(name, pTableToken->z, pTableToken->n) == 0 && strlen(name) == pTableToken->n) {
      pIndex->tableIndex = i;
      return TSDB_CODE_SUCCESS;
    }
  }

  return TSDB_CODE_TSC_INVALID_OPERATION;
}

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

static int16_t doGetColumnIndex(SQueryStmtInfo* pQueryInfo, int32_t index, const SToken* pToken, int16_t* type) {
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

  *type = (columnIndex >= getNumOfColumns(pTableMeta))? TSDB_COL_TAG:TSDB_COL_NORMAL;
  return columnIndex;
}

static bool isTablenameToken(SToken* token) {
  SToken tmpToken = *token;
  SToken tableToken = {0};

  extractTableNameFromToken(&tmpToken, &tableToken);
  return (tmpToken.n == strlen(TSQL_TBNAME_L) && strncasecmp(TSQL_TBNAME_L, tmpToken.z, tmpToken.n) == 0);
}

int32_t doGetColumnIndexByName(SToken* pToken, SQueryStmtInfo* pQueryInfo, SColumnIndex* pIndex, SMsgBuf* pMsgBuf) {
  const char* msg0 = "ambiguous column name";
  const char* msg1 = "invalid column name";

  pIndex->type = TSDB_COL_NORMAL;

  if (isTablenameToken(pToken)) {
    pIndex->columnIndex = TSDB_TBNAME_COLUMN_INDEX;
    pIndex->type = TSDB_COL_TAG;
  } else if (strlen(DEFAULT_PRIMARY_TIMESTAMP_COL_NAME) == pToken->n &&
             strncasecmp(pToken->z, DEFAULT_PRIMARY_TIMESTAMP_COL_NAME, pToken->n) == 0) {
    pIndex->columnIndex = PRIMARYKEY_TIMESTAMP_COL_INDEX; // just make runtime happy, need fix java test case InsertSpecialCharacterJniTest
  } else if (pToken->n == 0) {
    pIndex->columnIndex = PRIMARYKEY_TIMESTAMP_COL_INDEX; // just make runtime happy, need fix java test case InsertSpecialCharacterJniTest
  } else {
    // not specify the table name, try to locate the table index by column name
    if (pIndex->tableIndex == COLUMN_INDEX_INITIAL_VAL) {
      for (int16_t i = 0; i < pQueryInfo->numOfTables; ++i) {
        int16_t colIndex = doGetColumnIndex(pQueryInfo, i, pToken, &pIndex->type);

        if (colIndex != COLUMN_INDEX_INITIAL_VAL) {
          if (pIndex->columnIndex != COLUMN_INDEX_INITIAL_VAL) {
            return buildInvalidOperationMsg(pMsgBuf, msg0);
          } else {
            pIndex->tableIndex = i;
            pIndex->columnIndex = colIndex;
          }
        }
      }
    } else {  // table index is valid, get the column index
      pIndex->columnIndex = doGetColumnIndex(pQueryInfo, pIndex->tableIndex, pToken, &pIndex->type);
    }

    if (pIndex->columnIndex == COLUMN_INDEX_INITIAL_VAL) {
      return buildInvalidOperationMsg(pMsgBuf, msg1);
    }
  }

  if (VALID_COLUMN_INDEX(*pIndex)) {
    return TSDB_CODE_SUCCESS;
  } else {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }
}

int32_t getColumnIndexByName(const SToken* pToken, SQueryStmtInfo* pQueryInfo, SColumnIndex* pIndex, SMsgBuf* pMsgBuf) {
  if (pQueryInfo->pTableMetaInfo == NULL || pQueryInfo->numOfTables == 0) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  SToken tmpToken = *pToken;
  if (getTableIndexByName(&tmpToken, pQueryInfo, pIndex) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  return doGetColumnIndexByName(&tmpToken, pQueryInfo, pIndex, pMsgBuf);
}

int32_t validateGroupbyNode(SQueryStmtInfo* pQueryInfo, SArray* pList, SMsgBuf* pMsgBuf) {
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
    return buildInvalidOperationMsg(pMsgBuf, msg4);
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
    return buildInvalidOperationMsg(pMsgBuf, msg1);
  }

  SSchema *pSchema       = NULL;
  int32_t tableIndex     = COLUMN_INDEX_INITIAL_VAL;

  size_t num = taosArrayGetSize(pList);
  for (int32_t i = 0; i < num; ++i) {
    SListItem * pItem = taosArrayGet(pList, i);
    SVariant* pVar = &pItem->pVar;

    SToken token = {pVar->nLen, pVar->nType, pVar->pz};

    SColumnIndex index = COLUMN_INDEX_INITIALIZER;
    if (getColumnIndexByName(&token, pQueryInfo, &index, pMsgBuf) != TSDB_CODE_SUCCESS) {
      return buildInvalidOperationMsg(pMsgBuf, msg2);
    }

    if (tableIndex == COLUMN_INDEX_INITIAL_VAL) {
      tableIndex = index.tableIndex;
    } else if (tableIndex != index.tableIndex) {
      return buildInvalidOperationMsg(pMsgBuf, msg3);
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
        return buildInvalidOperationMsg(pMsgBuf, msg6);
      }

      int32_t relIndex = index.columnIndex;
      if (index.columnIndex != TSDB_TBNAME_COLUMN_INDEX) {
        relIndex -= numOfCols;
      }

      SColIndex colIndex = { .colIndex = relIndex, .flag = TSDB_COL_TAG, .colId = pSchema->colId, };
      strncpy(colIndex.name, pSchema->name, tListLen(colIndex.name));
      taosArrayPush(pGroupExpr->columnInfo, &colIndex);

      index.columnIndex = relIndex;
      columnListInsert(pTableMetaInfo->tagColList, index.columnIndex, pTableMeta->uid, pSchema);
    } else {
      // check if the column type is valid, here only support the bool/tinyint/smallint/bigint group by
      if (pSchema->type == TSDB_DATA_TYPE_TIMESTAMP || pSchema->type == TSDB_DATA_TYPE_FLOAT || pSchema->type == TSDB_DATA_TYPE_DOUBLE) {
        return buildInvalidOperationMsg(pMsgBuf, msg5);
      }

      columnListInsert(pQueryInfo->colList, index.columnIndex, pTableMeta->uid, pSchema);

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
    return buildInvalidOperationMsg(pMsgBuf, msg7);
  }

  for(int32_t i = 0; i < num; ++i) {
    SColIndex* pIndex = taosArrayGet(pGroupExpr->columnInfo, i);
    if (TSDB_COL_IS_NORMAL_COL(pIndex->flag) && i != num - 1) {
      return buildInvalidOperationMsg(pMsgBuf, msg8);
    }
  }

  pQueryInfo->groupbyExpr.tableIndex = tableIndex;
  return TSDB_CODE_SUCCESS;
}

int32_t filterUnsupportedQueryFunction(SQueryStmtInfo* pQueryInfo, SMsgBuf* pMsgBuf) {
  // todo NOT support yet
  const char* msg6 = "not support stddev/percentile/interp in the outer query yet";
  const char* msg9 = "not support 3 level select";

  for (int32_t i = 0; i < tscNumOfExprs(pQueryInfo); ++i) {
    SExprInfo* pExpr = getExprInfo(pQueryInfo, i);
    assert(pExpr->pExpr->nodeType == TEXPR_UNARYEXPR_NODE);

    int32_t f = pExpr->pExpr->_node.functionId;
    if (f == FUNCTION_STDDEV || f == FUNCTION_PERCT || f == FUNCTION_INTERP) {
      return buildInvalidOperationMsg(pMsgBuf, msg6);
    }

    if (f == FUNCTION_BLKINFO && taosArrayGetSize(pQueryInfo->pUpstream) > 0) {
      return buildInvalidOperationMsg(pMsgBuf, msg9);
    }

    if (/*(timeWindowQuery || pQueryInfo->stateWindow) &&*/ f == FUNCTION_LAST) {
      pExpr->base.numOfParams = 1;
      pExpr->base.param[0].i64 = TSDB_ORDER_ASC;
      pExpr->base.param[0].nType = TSDB_DATA_TYPE_INT;
    }
  }
}

int32_t validateWhereNode(SQueryStmtInfo *pQueryInfo, tSqlExpr* pWhereExpr, SMsgBuf* pMsgBuf) {
  return 0;
}

// validate the interval info
int32_t validateIntervalNode(SQueryStmtInfo *pQueryInfo, SSqlNode* pSqlNode, SMsgBuf* pMsgBuf) {
  return 0;
}

int32_t validateSessionNode(SQueryStmtInfo *pQueryInfo, SSqlNode* pSqlNode, SMsgBuf* pMsgBuf) {
  return 0;
}

// parse the window_state
int32_t validateStateWindowNode(SQueryStmtInfo *pQueryInfo, SSqlNode* pSqlNode, SMsgBuf* pMsgBuf) {
return 0;
}

// parse the having clause in the first place
int32_t validateHavingNode(SQueryStmtInfo *pQueryInfo, SSqlNode* pSqlNode, SMsgBuf* pMsgBuf) {
  return 0;
}

int32_t validateLimitNode(SQueryStmtInfo *pQueryInfo, SSqlNode* pSqlNode, SMsgBuf* pMsgBuf) {
  return 0;
}

// set order by info
int32_t validateOrderbyNode(SQueryStmtInfo *pQueryInfo, SSqlNode* pSqlNode, SMsgBuf* pMsgBuf) {
  return 0;
}

int32_t validateFillNode(SQueryStmtInfo *pQueryInfo, SSqlNode* pSqlNode, SMsgBuf* pMsgBuf) {
  return 0;
}

int32_t validateSqlNode(SSqlNode* pSqlNode, SQueryStmtInfo* pQueryInfo, SMsgBuf* pMsgBuf) {
  assert(pSqlNode != NULL && (pSqlNode->from == NULL || taosArrayGetSize(pSqlNode->from->list) > 0));

  const char* msg1 = "point interpolation query needs timestamp";
  const char* msg2 = "too many tables in from clause";
  const char* msg3 = "start(end) time of query range required or time range too large";
  const char* msg4 = "interval query not supported, since the result of sub query not include valid timestamp column";
  const char* msg5 = "only tag query not compatible with normal column filter";
  const char* msg7 = "derivative/twa/irate requires timestamp column exists in subquery";
  const char* msg8 = "condition missing for join query";

  int32_t  code = TSDB_CODE_SUCCESS;

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
      SRelElementPair* subInfo = taosArrayGet(pSqlNode->from->list, i);
      code = doValidateSubquery(pSqlNode, i, pQueryInfo, pMsgBuf);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }

    int32_t timeWindowQuery =
        (TPARSER_HAS_TOKEN(pSqlNode->interval.interval) || TPARSER_HAS_TOKEN(pSqlNode->sessionVal.gap));
//    TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_TABLE_QUERY);

    // parse the group by clause in the first place
    if (validateGroupbyNode(pQueryInfo, pSqlNode->pGroupby, pMsgBuf) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if (validateSelectNodeList(pQueryInfo, pSqlNode->pSelNodeList, true, pMsgBuf) !=
        TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    code = filterUnsupportedQueryFunction(pQueryInfo, pMsgBuf);

    STableMeta* pTableMeta = getMetaInfo(pQueryInfo, 0)->pTableMeta;
    SSchema*    pSchema = getOneColumnSchema(pTableMeta, 0);

    if (pSchema->type != TSDB_DATA_TYPE_TIMESTAMP) {
      int32_t numOfExprs = (int32_t)tscNumOfExprs(pQueryInfo);

      for (int32_t i = 0; i < numOfExprs; ++i) {
        SExprInfo* pExpr = getExprInfo(pQueryInfo, i);

        int32_t f = pExpr->pExpr->_node.functionId;
        if (f == FUNCTION_DERIVATIVE || f == FUNCTION_TWA || f == FUNCTION_IRATE) {
          return buildInvalidOperationMsg(pMsgBuf, msg7);
        }
      }
    }

    // validate the query filter condition info
    if (pSqlNode->pWhere != NULL) {
      if (validateWhereNode(pQueryInfo, pSqlNode->pWhere, pMsgBuf) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
    } else {
      if (pQueryInfo->numOfTables > 1) {
        return buildInvalidOperationMsg(pMsgBuf, msg8);
      }
    }

    // validate the interval info
    if (validateIntervalNode(pQueryInfo, pSqlNode, pMsgBuf) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    } else {
      if (validateSessionNode(pQueryInfo, pSqlNode, pMsgBuf) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      // parse the window_state
      if (validateStateWindowNode(pQueryInfo, pSqlNode, pMsgBuf) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

//      if (isTimeWindowQuery(pQueryInfo)) {
//        // check if the first column of the nest query result is timestamp column
//        SColumn* pCol = taosArrayGetP(pQueryInfo->colList, 0);
//        if (pCol->info.type != TSDB_DATA_TYPE_TIMESTAMP) {
//          return buildInvalidOperationMsg(pMsgBuf, msg4);
//        }
//
//        if (validateFunctionsInIntervalOrGroupbyQuery(pCmd, pQueryInfo) != TSDB_CODE_SUCCESS) {
//          return TSDB_CODE_TSC_INVALID_OPERATION;
//        }
//      }
    }

    // parse the having clause in the first place
    int32_t joinQuery = (pSqlNode->from != NULL && taosArrayGetSize(pSqlNode->from->list) > 1);
    if (validateHavingNode(pQueryInfo, pSqlNode, pMsgBuf) !=
        TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if ((code = validateLimitNode(pQueryInfo, pSqlNode, pMsgBuf)) != TSDB_CODE_SUCCESS) {
      return code;
    }

    // set order by info
    if (validateOrderbyNode(pQueryInfo, pSqlNode, pMsgBuf) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if ((code = validateFillNode(pQueryInfo, pSqlNode, pMsgBuf)) != TSDB_CODE_SUCCESS) {
      return code;
    }
  } else {
    pQueryInfo->command = TSDB_SQL_SELECT;

    size_t numOfTables = taosArrayGetSize(pSqlNode->from->list);
    if (numOfTables > TSDB_MAX_JOIN_TABLE_NUM) {
      return buildInvalidOperationMsg(pMsgBuf, msg2);
    }

    STableMetaInfo* pTableMetaInfo = getMetaInfo(pQueryInfo, 0);
    bool isSTable = UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo);

    int32_t type = isSTable? TSDB_QUERY_TYPE_STABLE_QUERY:TSDB_QUERY_TYPE_TABLE_QUERY;
    TSDB_QUERY_SET_TYPE(pQueryInfo->type, type);

    // parse the group by clause in the first place
    if (validateGroupbyNode(pQueryInfo, pSqlNode->pGroupby, pMsgBuf) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }
    pQueryInfo->onlyHasTagCond = true;

    // set where info
    if (pSqlNode->pWhere != NULL) {
      if (validateWhereNode(pQueryInfo, pSqlNode->pWhere, pMsgBuf) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
      pSqlNode->pWhere = NULL;
    } else {
      if (taosArrayGetSize(pSqlNode->from->list) > 1) { // Cross join not allowed yet
        return buildInvalidOperationMsg(pMsgBuf, "cross join not supported yet");
      }
    }

    int32_t joinQuery = (pSqlNode->from != NULL && taosArrayGetSize(pSqlNode->from->list) > 1);
    int32_t timeWindowQuery =
        (TPARSER_HAS_TOKEN(pSqlNode->interval.interval) || TPARSER_HAS_TOKEN(pSqlNode->sessionVal.gap));

    if (validateSelectNodeList(pQueryInfo, pSqlNode->pSelNodeList, false, pMsgBuf) !=
        TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    if (isSTable && (pQueryInfo) && pQueryInfo->distinct && !pQueryInfo->onlyHasTagCond) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    // parse the window_state
    if (validateStateWindowNode(pQueryInfo, pSqlNode, pMsgBuf) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    // set order by info
    if (validateOrderbyNode(pQueryInfo, pSqlNode, pMsgBuf) !=
        TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    // set interval value
    if (validateIntervalNode(pQueryInfo, pSqlNode, pMsgBuf) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    // parse the having clause in the first place
    if (validateHavingNode(pQueryInfo, pSqlNode, pMsgBuf) !=
        TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    /*
     * transfer sql functions that need secondary merge into another format
     * in dealing with super table queries such as: count/first/last
     */
    if (validateSessionNode(pQueryInfo, pSqlNode, pMsgBuf) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

//    if (isTimeWindowQuery(pQueryInfo) && (validateFunctionsInIntervalOrGroupbyQuery(pCmd, pQueryInfo) != TSDB_CODE_SUCCESS)) {
//      return TSDB_CODE_TSC_INVALID_OPERATION;
//    }

    // no result due to invalid query time range
    if (pQueryInfo->window.skey > pQueryInfo->window.ekey) {
      pQueryInfo->command = TSDB_SQL_RETRIEVE_EMPTY_RESULT;
      return TSDB_CODE_SUCCESS;
    }

    if ((code = validateLimitNode(pQueryInfo, pSqlNode, pMsgBuf)) != TSDB_CODE_SUCCESS) {
      return code;
    }

    if ((code = validateFillNode(pQueryInfo, pSqlNode, pMsgBuf)) != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  // set the query info
  SExprInfo** p = NULL;
  int32_t numOfExpr = 0;
  STableMetaInfo* pTableMetaInfo = getMetaInfo(pQueryInfo, 0);
  code = createProjectionExpr(pQueryInfo, pTableMetaInfo, &p, &numOfExpr);
  if (pQueryInfo->exprList1 == NULL) {
    pQueryInfo->exprList1 = taosArrayInit(4, POINTER_BYTES);
  }

  taosArrayAddBatch(pQueryInfo->exprList1, (void*) p, numOfExpr);
  tfree(p);

  return TSDB_CODE_SUCCESS;  // Does not build query message here
}

static int32_t distinctCompatibleCheck(SQueryStmtInfo* pQueryInfo, bool joinQuery, SMsgBuf* pMsgBuf) {
  const char* msg6 = "not support distinct mixed with join";
  const char* msg7 = "not support distinct mixed with groupby";
  const char* msg8 = "not support distinct in nest query";

  if (pQueryInfo->distinct) {
    if (joinQuery) {
      return buildInvalidOperationMsg(pMsgBuf, msg6);
    }

    if (taosArrayGetSize(pQueryInfo->groupbyExpr.columnInfo) != 0) {
      return buildInvalidOperationMsg(pMsgBuf, msg7);
    }

    if (pQueryInfo->pDownstream != NULL) {
      return buildInvalidOperationMsg(pMsgBuf, msg8);
    }
  }
}

static int32_t resColId = 5000;
static int32_t getNewResColId() {
  return resColId++;
}

int32_t addResColumnInfo(SQueryStmtInfo* pQueryInfo, int32_t outputIndex, SSchema* pSchema, SExprInfo* pSqlExpr) {
  SInternalField* pInfo = insertFieldInfo(&pQueryInfo->fieldsInfo, outputIndex, pSchema);
  pInfo->pExpr = pSqlExpr;
  return TSDB_CODE_SUCCESS;
}

void setResultColName(char* name, tSqlExprItem* pItem, SToken* pToken, SToken* functionToken, bool multiCols) {
  if (pItem->aliasName != NULL) {
    tstrncpy(name, pItem->aliasName, TSDB_COL_NAME_LEN);
  } else if (multiCols) {
    char uname[TSDB_COL_NAME_LEN] = {0};
    int32_t len = MIN(pToken->n + 1, TSDB_COL_NAME_LEN);
    tstrncpy(uname, pToken->z, len);

    if (tsKeepOriginalColumnName) { // keep the original column name
      tstrncpy(name, uname, TSDB_COL_NAME_LEN);
    } else {
      const int32_t size = TSDB_COL_NAME_LEN + FUNCTIONS_NAME_MAX_LENGTH + 2 + 1;
      char tmp[TSDB_COL_NAME_LEN + FUNCTIONS_NAME_MAX_LENGTH + 2 + 1] = {0};

      char f[FUNCTIONS_NAME_MAX_LENGTH] = {0};
      strncpy(f, functionToken->z, functionToken->n);

      snprintf(tmp, size, "%s(%s)", f, uname);
      tstrncpy(name, tmp, TSDB_COL_NAME_LEN);
    }
  } else  { // use the user-input result column name
    int32_t len = MIN(pItem->pNode->exprToken.n + 1, TSDB_COL_NAME_LEN);
    tstrncpy(name, pItem->pNode->exprToken.z, len);
  }
}

SExprInfo* doAddOneExprInfo(SQueryStmtInfo* pQueryInfo, int32_t outputColIndex, int16_t functionId, SColumnIndex* pIndex,
                           SSchema* pColSchema, const char* token) {
  STableMetaInfo* pTableMetaInfo = getMetaInfo(pQueryInfo, pIndex->tableIndex);

  SSchema resultSchema = createSchema(pColSchema->type, pColSchema->bytes, getNewResColId(), pColSchema->name);

  SExprInfo* pExpr = createExprInfo(pTableMetaInfo, functionId, pIndex, NULL, &resultSchema, 0);
  addExprInfo(pQueryInfo, outputColIndex, pExpr);

  tstrncpy(pExpr->base.token, token, sizeof(pExpr->base.token));

  pExpr->base.colInfo.flag = pIndex->type;
  columnListInsert(pTableMetaInfo->tagColList, pIndex->columnIndex, pTableMetaInfo->pTableMeta->uid, pColSchema);
  addResColumnInfo(pQueryInfo, outputColIndex, pColSchema, pExpr);

  return pExpr;
}

void doAddResColumnOrSourceColumn(SQueryStmtInfo* pQueryInfo, SColumnIndex* index, int32_t outputIndex, SExprInfo* pExpr, SSchema* pColSchema, bool finalResult) {
  columnListInsert(pQueryInfo->colList, index->columnIndex, pExpr->base.uid, pColSchema);

  if (finalResult) {
    addResColumnInfo(pQueryInfo, outputIndex, &pExpr->base.resSchema, pExpr);
  }

  if (TSDB_COL_IS_NORMAL_COL(index->type)) {
    insertPrimaryTsColumn(pQueryInfo->colList, pExpr->base.uid);
  }
}

static int32_t setMultipleExprsInSelect(SQueryStmtInfo* pQueryInfo, SSchema* pSchema, int32_t functionId,
                                       const char* name, int32_t resColIdx, SColumnIndex* pColIndex, bool finalResult,
                                       SMsgBuf* pMsgBuf) {
  const char* msg1 = "not support column types";
  if (functionId == FUNCTION_SPREAD) {
    if (IS_VAR_DATA_TYPE(pSchema->type) || pSchema->type == TSDB_DATA_TYPE_BOOL) {
      return buildInvalidOperationMsg(pMsgBuf, msg1);
    }
  }

  int16_t resType = 0;
  int16_t resBytes = 0;
  int32_t interBufSize = 0;
  getResultDataInfo(pSchema->type, pSchema->bytes, functionId, 0, &resType, &resBytes, &interBufSize, 0, false);

  STableMetaInfo* pTableMetaInfo = getMetaInfo(pQueryInfo, pColIndex->tableIndex);

  SSchema resultSchema = createSchema(resType, resBytes, getNewResColId(), name);

  SExprInfo* pExpr = createExprInfo(pTableMetaInfo, functionId, pColIndex, NULL, &resultSchema, interBufSize);
  addExprInfo(pQueryInfo, resColIdx, pExpr);
  tstrncpy(pExpr->base.token, resultSchema.name, tListLen(pExpr->base.token));
  
  doAddResColumnOrSourceColumn(pQueryInfo, pColIndex, resColIdx, pExpr, pSchema, finalResult);
  return TSDB_CODE_SUCCESS;
}

static int32_t checkForAliasName(SMsgBuf* pMsgBuf, char* aliasName) {
  const char* msg1 = "column alias name too long";
  if (aliasName != NULL && strlen(aliasName) >= TSDB_COL_NAME_LEN) {
    return buildInvalidOperationMsg(pMsgBuf, msg1);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t validateComplexExpr(tSqlExpr* pExpr, SQueryStmtInfo* pQueryInfo, SArray* pColList, int32_t* type, SMsgBuf* pMsgBuf);
static int32_t sqlExprToExprNode(tExprNode **pExpr, const tSqlExpr* pSqlExpr, SQueryStmtInfo* pQueryInfo, SArray* pCols, uint64_t *uid, SMsgBuf* pMsgBuf);

static int64_t getTickPerSecond(SVariant* pVariant, int32_t precision, int64_t* tickPerSec, SMsgBuf *pMsgBuf) {
  const char* msg10 = "derivative duration should be greater than 1 Second";

  if (taosVariantDump(pVariant, (char*) tickPerSec, TSDB_DATA_TYPE_BIGINT, true) < 0) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  if (precision == TSDB_TIME_PRECISION_MILLI) {
    *tickPerSec /= TSDB_TICK_PER_SECOND(TSDB_TIME_PRECISION_MICRO);
  } else if (precision == TSDB_TIME_PRECISION_MICRO) {
    *tickPerSec /= TSDB_TICK_PER_SECOND(TSDB_TIME_PRECISION_MILLI);
  }

  if (*tickPerSec <= 0 || *tickPerSec < TSDB_TICK_PER_SECOND(precision)) {
    return buildInvalidOperationMsg(pMsgBuf, msg10);
  }

  return TSDB_CODE_SUCCESS;
}

// set the first column ts for top/bottom query
static void setTsOutputExprInfo(SQueryStmtInfo* pQueryInfo, STableMetaInfo* pTableMetaInfo, int32_t outputIndex, int32_t tableIndex) {
  SColumnIndex indexTS = {.tableIndex = tableIndex, .columnIndex = PRIMARYKEY_TIMESTAMP_COL_INDEX, .type = TSDB_COL_NORMAL};
  SSchema s = createSchema(TSDB_DATA_TYPE_TIMESTAMP, TSDB_KEYSIZE, getNewResColId(), "ts");

  SExprInfo* pExpr = createExprInfo(pTableMetaInfo, FUNCTION_TS_DUMMY, &indexTS, NULL, &s, TSDB_KEYSIZE);
  addExprInfo(pQueryInfo, outputIndex, pExpr);

  SSchema* pSourceSchema = getOneColumnSchema(pTableMetaInfo->pTableMeta, indexTS.columnIndex);
  columnListInsert(pQueryInfo->colList, indexTS.columnIndex, pTableMetaInfo->pTableMeta->uid, pSourceSchema);
  addResColumnInfo(pQueryInfo, outputIndex, &pExpr->base.resSchema, pExpr);
}

static int32_t multiColumnListInsert(SQueryStmtInfo* pQueryInfo, SArray* pColumnList, SMsgBuf* pMsgBuf);

int32_t addExprAndResColumn(SQueryStmtInfo* pQueryInfo, int32_t colIndex, tSqlExprItem* pItem, bool finalResult, SMsgBuf* pMsgBuf) {
  STableMetaInfo* pTableMetaInfo = NULL;
  int32_t functionId = pItem->functionId;

  const char* msg1 = "not support column types";
  const char* msg2 = "invalid parameters";
  const char* msg3 = "illegal column name";
  const char* msg4 = "invalid table name";
  const char* msg5 = "parameter is out of range [0, 100]";
  const char* msg6 = "functions applied to tags are not allowed";
  const char* msg7 = "normal table can not apply this function";
  const char* msg8 = "multi-columns selection does not support alias column name";
  const char* msg9 = "diff/derivative can no be applied to unsigned numeric type";
  const char* msg10 = "derivative duration should be greater than 1 Second";
  const char* msg11 = "third parameter in derivative should be 0 or 1";
  const char* msg12 = "parameter is out of range [1, 100]";
  const char* msg13 = "nested function is not supported";

  if (checkForAliasName(pMsgBuf, pItem->aliasName) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  switch (functionId) {
    case FUNCTION_COUNT: {
      // more than one parameter for count() function
      SArray* pParamList = pItem->pNode->Expr.paramList;
      if (pParamList != NULL && taosArrayGetSize(pParamList) != 1) {
        return buildInvalidOperationMsg(pMsgBuf, msg2);
      }

      SExprInfo* pExpr = NULL;
      SColumnIndex index = COLUMN_INDEX_INITIALIZER;

      if (pParamList != NULL) {
        tSqlExprItem* pParamElem = taosArrayGet(pParamList, 0);

        SToken* pToken = &pParamElem->pNode->columnName;
        int16_t tokenId = pParamElem->pNode->tokenId;
        if ((pToken->z == NULL || pToken->n == 0) && (TK_INTEGER != tokenId)) {
          return buildInvalidOperationMsg(pMsgBuf, msg3);
        }

        // select count(table.*), select count(1), count(2)
        if (tokenId == TK_ALL || tokenId == TK_INTEGER) {
          // check if the table name is valid or not
          SToken tmpToken = pParamElem->pNode->columnName;
          if (getTableIndexByName(&tmpToken, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
            return buildInvalidOperationMsg(pMsgBuf, msg4);
          }

          index = (SColumnIndex) {0, PRIMARYKEY_TIMESTAMP_COL_INDEX};
        } else {
          // count the number of table created according to the super table
          if (getColumnIndexByName(pToken, pQueryInfo, &index, pMsgBuf) != TSDB_CODE_SUCCESS) {
            return buildInvalidOperationMsg(pMsgBuf, msg3);
          }
        }
      } else {  // count(*) is equalled to count(primary_timestamp_key)
        index = (SColumnIndex){0, PRIMARYKEY_TIMESTAMP_COL_INDEX, false};
      }

      int32_t size = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
      SSchema s = createSchema(TSDB_DATA_TYPE_BIGINT, size, getNewResColId(), "");

      pTableMetaInfo = getMetaInfo(pQueryInfo, index.tableIndex);
      pExpr = createExprInfo(pTableMetaInfo, functionId, &index, NULL, &s, size);
      addExprInfo(pQueryInfo, colIndex, pExpr);

      setTokenAndResColumnName(pItem, pExpr->base.resSchema.name, pExpr->base.token,sizeof(pExpr->base.resSchema.name) - 1);

      int32_t outputColumnIndex = getNumOfFields(&pQueryInfo->fieldsInfo);
      SSchema* ps = getOneColumnSchema(pTableMetaInfo->pTableMeta, index.columnIndex);
      doAddResColumnOrSourceColumn(pQueryInfo, &index, outputColumnIndex, pExpr, ps, finalResult);
      return TSDB_CODE_SUCCESS;
    }

    case FUNCTION_SUM:
    case FUNCTION_AVG:
    case FUNCTION_RATE:
    case FUNCTION_IRATE:
    case FUNCTION_TWA:
    case FUNCTION_MIN:
    case FUNCTION_MAX:
    case FUNCTION_DIFF:
    case FUNCTION_DERIVATIVE:
    case FUNCTION_STDDEV:
    case FUNCTION_LEASTSQR: {
      // 1. valid the number of parameters
      int32_t numOfParams = (pItem->pNode->Expr.paramList == NULL)? 0: (int32_t) taosArrayGetSize(pItem->pNode->Expr.paramList);

      // no parameters or more than one parameter for function
      if (pItem->pNode->Expr.paramList == NULL ||
          (functionId != FUNCTION_LEASTSQR && functionId != FUNCTION_DERIVATIVE && numOfParams != 1) ||
          ((functionId == FUNCTION_LEASTSQR || functionId == FUNCTION_DERIVATIVE) && numOfParams != 3)) {
        return buildInvalidOperationMsg(pMsgBuf, msg2);
      }

      tSqlExprItem* pParamElem = taosArrayGet(pItem->pNode->Expr.paramList, 0);

      tExprNode* pNode = NULL;
      int32_t tokenId = pParamElem->pNode->tokenId;
      SColumnIndex index = COLUMN_INDEX_INITIALIZER;
      SSchema columnSchema = {0};

      if (tokenId == TK_ALL || tokenId == TK_ID) {  // simple parameter
        if ((getColumnIndexByName(&pParamElem->pNode->columnName, pQueryInfo, &index, pMsgBuf) != TSDB_CODE_SUCCESS)) {
          return buildInvalidOperationMsg(pMsgBuf, msg3);
        }

        // functions can not be applied to tags
        if (TSDB_COL_IS_TAG(index.type)) {
          return buildInvalidOperationMsg(pMsgBuf, msg6);
        }

        pTableMetaInfo = getMetaInfo(pQueryInfo, index.tableIndex);

        // 2. check if sql function can be applied on this column data type
        columnSchema = *(SSchema*) getOneColumnSchema(pTableMetaInfo->pTableMeta, index.columnIndex);

        if (!IS_NUMERIC_TYPE(columnSchema.type)) {
          return buildInvalidOperationMsg(pMsgBuf, msg1);
        } else if (IS_UNSIGNED_NUMERIC_TYPE(columnSchema.type) && (functionId == FUNCTION_DIFF || functionId == FUNCTION_DERIVATIVE)) {
          return buildInvalidOperationMsg(pMsgBuf, msg9);
        }
      } else if (tokenId == TK_PLUS || tokenId == TK_MINUS || tokenId == TK_TIMES || tokenId == TK_REM || tokenId == TK_CONCAT) {
        int32_t arithmeticType = NON_ARITHMEIC_EXPR;
        SArray* pColumnList = taosArrayInit(4, sizeof(SColumnIndex));
        if (validateComplexExpr(pParamElem->pNode, pQueryInfo, pColumnList, &arithmeticType, pMsgBuf) != TSDB_CODE_SUCCESS) {
          return buildInvalidOperationMsg(pMsgBuf, msg1);
        }

        if (arithmeticType != NORMAL_ARITHMETIC) {
          return buildInvalidOperationMsg(pMsgBuf, msg13);
        }

        pTableMetaInfo = getMetaInfo(pQueryInfo, 0);  // get the first table meta.
        columnSchema = createSchema(TSDB_DATA_TYPE_DOUBLE, sizeof(double), getNewResColId(), "");

        SArray* colList = taosArrayInit(10, sizeof(SColIndex));
        int32_t ret = sqlExprToExprNode(&pNode, pItem->pNode, pQueryInfo, colList, NULL, pMsgBuf);
        if (ret != TSDB_CODE_SUCCESS) {
          taosArrayDestroy(colList);
          tExprTreeDestroy(pNode, NULL);
          return buildInvalidOperationMsg(pMsgBuf, msg2);
        }

        multiColumnListInsert(pQueryInfo, pColumnList, pMsgBuf);
      } else {
        assert(0);
      }

      int32_t precision = pTableMetaInfo->pTableMeta->tableInfo.precision;

      int16_t resultType = 0;
      int16_t resultSize = 0;
      int32_t intermediateResSize = 0;

      if (getResultDataInfo(columnSchema.type, columnSchema.bytes, functionId, 0, &resultType, &resultSize,
                            &intermediateResSize, 0, false) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      // set the first column ts for diff query
      int32_t numOfOutput = getNumOfFields(&pQueryInfo->fieldsInfo);
      if (functionId == FUNCTION_DIFF || functionId == FUNCTION_DERIVATIVE) {
        setTsOutputExprInfo(pQueryInfo, pTableMetaInfo, numOfOutput, index.tableIndex);
        numOfOutput += 1;
      }

      SSchema s = createSchema(resultType, resultSize, getNewResColId(), "ts");

      SExprInfo* pExpr = createExprInfo(pTableMetaInfo, functionId, &index, pNode, &s, intermediateResSize);
      addExprInfo(pQueryInfo, numOfOutput, pExpr);

      if (functionId == FUNCTION_LEASTSQR) { // set the leastsquares parameters
        char val[8] = {0};
        if (taosVariantDump(&pParamElem[1].pNode->value, val, TSDB_DATA_TYPE_DOUBLE, true) < 0) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
        }

        addExprInfoParam(&pExpr->base, val, TSDB_DATA_TYPE_DOUBLE, DOUBLE_BYTES);

        memset(val, 0, tListLen(val));
        if (taosVariantDump(&pParamElem[2].pNode->value, val, TSDB_DATA_TYPE_DOUBLE, true) < 0) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
        }

        addExprInfoParam(&pExpr->base, val, TSDB_DATA_TYPE_DOUBLE, DOUBLE_BYTES);
      } else if (functionId == FUNCTION_IRATE) {
        addExprInfoParam(&pExpr->base, (char*) &precision, TSDB_DATA_TYPE_BIGINT, LONG_BYTES);
      } else if (functionId == FUNCTION_DERIVATIVE) {
        char val[8] = {0};

        int64_t tickPerSec = 0;
        int32_t code = getTickPerSecond(&pParamElem[1].pNode->value, precision, (char*) &tickPerSec, pMsgBuf);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }

        addExprInfoParam(&pExpr->base, (char*) &tickPerSec, TSDB_DATA_TYPE_BIGINT, LONG_BYTES);
        memset(val, 0, tListLen(val));

        if (taosVariantDump(&pParamElem[2].pNode->value, val, TSDB_DATA_TYPE_BIGINT, true) < 0) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
        }

        if (GET_INT64_VAL(val) != 0 && GET_INT64_VAL(val) != 1) {
          return buildInvalidOperationMsg(pMsgBuf, msg11);
        }

        addExprInfoParam(&pExpr->base, val, TSDB_DATA_TYPE_BIGINT, LONG_BYTES);
      }

      setTokenAndResColumnName(pItem, pExpr->base.resSchema.name, pExpr->base.token, sizeof(pExpr->base.resSchema.name) - 1);

      numOfOutput = getNumOfFields(&pQueryInfo->fieldsInfo);
      doAddResColumnOrSourceColumn(pQueryInfo, &index, numOfOutput, pExpr, &columnSchema, finalResult);
      return TSDB_CODE_SUCCESS;
    }

    case FUNCTION_FIRST:
    case FUNCTION_LAST:
    case FUNCTION_SPREAD:
    case FUNCTION_LAST_ROW:
    case FUNCTION_INTERP: {
      bool requireAllFields = (pItem->pNode->Expr.paramList == NULL);

      if (!requireAllFields) {
        SArray* pParamList = pItem->pNode->Expr.paramList;
        if (taosArrayGetSize(pParamList) < 1) {
          return buildInvalidOperationMsg(pMsgBuf, msg3);
        }

        if (taosArrayGetSize(pParamList) > 1 && (pItem->aliasName != NULL)) {
          return buildInvalidOperationMsg(pMsgBuf, msg8);
        }

        /* in first/last function, multiple columns can be add to resultset */
        for (int32_t i = 0; i < taosArrayGetSize(pParamList); ++i) {
          tSqlExprItem* pParamElem = taosArrayGet(pParamList, i);
          if (pParamElem->pNode->tokenId != TK_ALL && pParamElem->pNode->tokenId != TK_ID) {
            return buildInvalidOperationMsg(pMsgBuf, msg3);
          }

          SColumnIndex index = COLUMN_INDEX_INITIALIZER;

          if (pParamElem->pNode->tokenId == TK_ALL) { // select table.*
            SToken tmpToken = pParamElem->pNode->columnName;

            if (getTableIndexByName(&tmpToken, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
              return buildInvalidOperationMsg(pMsgBuf, msg4);
            }

            pTableMetaInfo = getMetaInfo(pQueryInfo, index.tableIndex);
            SSchema* pSchema = getTableColumnSchema(pTableMetaInfo->pTableMeta);

            char name[TSDB_COL_NAME_LEN] = {0};
            for (int32_t j = 0; j < getNumOfColumns(pTableMetaInfo->pTableMeta); ++j) {
              index.columnIndex = j;
              SToken t = {.z = pSchema[j].name, .n = (uint32_t)strnlen(pSchema[j].name, TSDB_COL_NAME_LEN)};
              setResultColName(name, pItem, &t, &pItem->pNode->exprToken, true);

              if (setMultipleExprsInSelect(pQueryInfo, &pSchema[j], functionId, name, colIndex++, &index, finalResult, pMsgBuf) != 0) {
                return TSDB_CODE_TSC_INVALID_OPERATION;
              }
            }

          } else {
            if (getColumnIndexByName(&pParamElem->pNode->columnName, pQueryInfo, &index, pMsgBuf) != TSDB_CODE_SUCCESS) {
              return buildInvalidOperationMsg(pMsgBuf, msg3);
            }

            pTableMetaInfo = getMetaInfo(pQueryInfo, index.tableIndex);

            // functions can not be applied to tags
            if ((index.columnIndex >= getNumOfColumns(pTableMetaInfo->pTableMeta)) || (index.columnIndex < 0)) {
              return buildInvalidOperationMsg(pMsgBuf, msg6);
            }

            char name[TSDB_COL_NAME_LEN] = {0};
            SSchema* pSchema = getOneColumnSchema(pTableMetaInfo->pTableMeta, index.columnIndex);

            bool multiColOutput = taosArrayGetSize(pItem->pNode->Expr.paramList) > 1;
            setResultColName(name, pItem, &pParamElem->pNode->columnName, &pItem->pNode->exprToken, multiColOutput);

            if (setMultipleExprsInSelect(pQueryInfo, pSchema, functionId, name, colIndex++, &index, finalResult, pMsgBuf) != 0) {
              return TSDB_CODE_TSC_INVALID_OPERATION;
            }
          }
        }

      } else {  // select function(*) from xxx
        int32_t numOfFields = 0;

        // multicolumn selection does not support alias name
        if (pItem->aliasName != NULL && strlen(pItem->aliasName) > 0) {
          return buildInvalidOperationMsg(pMsgBuf, msg8);
        }

        for (int32_t j = 0; j < pQueryInfo->numOfTables; ++j) {
          pTableMetaInfo = getMetaInfo(pQueryInfo, j);
          SSchema* pSchema = getTableColumnSchema(pTableMetaInfo->pTableMeta);

          for (int32_t i = 0; i < getNumOfColumns(pTableMetaInfo->pTableMeta); ++i) {
            SColumnIndex index = {.tableIndex = j, .columnIndex = i, .type = TSDB_COL_NORMAL};

            char name[TSDB_COL_NAME_LEN] = {0};
            SToken t = {.z = pSchema[i].name, .n = (uint32_t)strnlen(pSchema[i].name, TSDB_COL_NAME_LEN)};
            setResultColName(name, pItem, &t, &pItem->pNode->Expr.operand, true);

            if (setMultipleExprsInSelect(pQueryInfo, &pSchema[index.columnIndex], functionId, name, colIndex, &index, finalResult, pMsgBuf) != 0) {
              return TSDB_CODE_TSC_INVALID_OPERATION;
            }
            colIndex++;
          }

          numOfFields += getNumOfColumns(pTableMetaInfo->pTableMeta);
        }
      }
      return TSDB_CODE_SUCCESS;
    }

    case FUNCTION_TOP:
    case FUNCTION_BOTTOM:
    case FUNCTION_PERCT:
    case FUNCTION_APERCT: {
      // 1. valid the number of parameters
      // no parameters or more than one parameter for function
      if (pItem->pNode->Expr.paramList == NULL || taosArrayGetSize(pItem->pNode->Expr.paramList) != 2) {
        return buildInvalidOperationMsg(pMsgBuf, msg2);
      }

      tSqlExprItem* pParamElem = taosArrayGet(pItem->pNode->Expr.paramList, 0);
      if (pParamElem->pNode->tokenId != TK_ID) {
        return buildInvalidOperationMsg(pMsgBuf, msg2);
      }

      SColumnIndex index = COLUMN_INDEX_INITIALIZER;
      if (getColumnIndexByName(&pParamElem->pNode->columnName, pQueryInfo, &index, pMsgBuf) != TSDB_CODE_SUCCESS) {
        return buildInvalidOperationMsg(pMsgBuf, msg3);
      }

      // functions can not be applied to tags
      if (TSDB_COL_IS_TAG(index.type)) {
        return buildInvalidOperationMsg(pMsgBuf, msg6);
      }

      pTableMetaInfo = getMetaInfo(pQueryInfo, index.tableIndex);
      SSchema* pSchema = getOneColumnSchema(pTableMetaInfo->pTableMeta, index.columnIndex);

      // 2. valid the column type
      if (!IS_NUMERIC_TYPE(pSchema->type)) {
        return buildInvalidOperationMsg(pMsgBuf, msg1);
      }

      // 3. valid the parameters
      if (pParamElem[1].pNode->tokenId == TK_ID) {
        return buildInvalidOperationMsg(pMsgBuf, msg2);
      }

      SVariant* pVariant = &pParamElem[1].pNode->value;

      int16_t  resultType = pSchema->type;
      int16_t  resultSize = pSchema->bytes;
      int32_t  interResult = 0;

      char val[8] = {0};

      SExprInfo* pExpr = NULL;
      if (functionId == FUNCTION_PERCT || functionId == FUNCTION_APERCT) {
        taosVariantDump(pVariant, val, TSDB_DATA_TYPE_DOUBLE, true);

        double dp = GET_DOUBLE_VAL(val);
        if (dp < 0 || dp > TOP_BOTTOM_QUERY_LIMIT) {
          return buildInvalidOperationMsg(pMsgBuf, msg5);
        }

        getResultDataInfo(pSchema->type, pSchema->bytes, functionId, 0, &resultType, &resultSize, &interResult, 0, false);

        /*
         * sql function transformation
         * for dp = 0, it is actually min,
         * for dp = 100, it is max,
         */
        colIndex += 1;  // the first column is ts

        SSchema s = createSchema(resultType, resultSize, getNewResColId(), "");

        pExpr = createExprInfo(pTableMetaInfo, functionId, &index, NULL, &s, interResult);
        addExprInfo(pQueryInfo, colIndex, pExpr);
        addExprInfoParam(&pExpr->base, val, TSDB_DATA_TYPE_DOUBLE, sizeof(double));
      } else {
        taosVariantDump(pVariant, val, TSDB_DATA_TYPE_BIGINT, true);

        int64_t nTop = GET_INT32_VAL(val);
        if (nTop <= 0 || nTop > 100) {  // todo use macro
          return buildInvalidOperationMsg(pMsgBuf, msg12);
        }

        // set the first column ts for top/bottom query
        setTsOutputExprInfo(pQueryInfo, pTableMetaInfo, colIndex, index.tableIndex);
        colIndex += 1;  // the first column is ts

        SSchema s = createSchema(resultType, resultSize, getNewResColId(), "");
        pExpr = createExprInfo(pTableMetaInfo, functionId, &index, NULL, &s, resultSize);
        addExprInfo(pQueryInfo, colIndex, pExpr);

        addExprInfoParam(&pExpr->base, val, TSDB_DATA_TYPE_BIGINT, sizeof(int64_t));
      }

      setTokenAndResColumnName(pItem, pExpr->base.resSchema.name, pExpr->base.token, sizeof(pExpr->base.resSchema.name) - 1);
      doAddResColumnOrSourceColumn(pQueryInfo, &index, colIndex, pExpr, pSchema, finalResult);
      return TSDB_CODE_SUCCESS;
    }

    case FUNCTION_TID_TAG: {
      pTableMetaInfo = getMetaInfo(pQueryInfo, 0);
      if (UTIL_TABLE_IS_NORMAL_TABLE(pTableMetaInfo)) {
        return buildInvalidOperationMsg(pMsgBuf, msg7);
      }

      // no parameters or more than one parameter for function
      if (pItem->pNode->Expr.paramList == NULL || taosArrayGetSize(pItem->pNode->Expr.paramList) != 1) {
        return buildInvalidOperationMsg(pMsgBuf, msg2);
      }

      tSqlExprItem* pParamItem = taosArrayGet(pItem->pNode->Expr.paramList, 0);
      tSqlExpr* pParam = pParamItem->pNode;

      SColumnIndex index = COLUMN_INDEX_INITIALIZER;
      if (getColumnIndexByName(&pParam->columnName, pQueryInfo, &index, pMsgBuf) != TSDB_CODE_SUCCESS) {
        return buildInvalidOperationMsg(pMsgBuf, msg3);
      }

      pTableMetaInfo = getMetaInfo(pQueryInfo, index.tableIndex);
      SSchema* pSchema = getTableTagSchema(pTableMetaInfo->pTableMeta);

      // functions can not be applied to normal columns
      int32_t numOfCols = getNumOfColumns(pTableMetaInfo->pTableMeta);
      if (index.columnIndex < numOfCols && index.columnIndex != TSDB_TBNAME_COLUMN_INDEX) {
        return buildInvalidOperationMsg(pMsgBuf, msg6);
      }

      if (index.columnIndex > 0) {
        index.columnIndex -= numOfCols;
      }

      // 2. valid the column type
      int16_t colType = 0;
      if (index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
        colType = TSDB_DATA_TYPE_BINARY;
      } else {
        colType = pSchema[index.columnIndex].type;
      }

      if (colType == TSDB_DATA_TYPE_BOOL) {
        return buildInvalidOperationMsg(pMsgBuf, msg1);
      }

      columnListInsert(pTableMetaInfo->tagColList, index.columnIndex, pTableMetaInfo->pTableMeta->uid, &pSchema[index.columnIndex]);
      SSchema* pTagSchema = getTableTagSchema(pTableMetaInfo->pTableMeta);

      SSchema s = {0};
      if (index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
        s = *getTbnameColumnSchema();
      } else {
        s = pTagSchema[index.columnIndex];
      }

      int16_t bytes = 0;
      int16_t type  = 0;
      int32_t inter = 0;

      int32_t ret = getResultDataInfo(s.type, s.bytes, FUNCTION_TID_TAG, 0, &type, &bytes, &inter, 0, 0);
      assert(ret == TSDB_CODE_SUCCESS);

      s.type = (uint8_t)type;
      s.bytes = bytes;
      s.colId = getNewResColId();
      TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_TAG_FILTER_QUERY);

      doAddOneExprInfo(pQueryInfo, 0, FUNCTION_TID_TAG, &index, &s, s.name);
      return TSDB_CODE_SUCCESS;
    }

    case FUNCTION_BLKINFO: {
      // no parameters or more than one parameter for function
      if (pItem->pNode->Expr.paramList != NULL && taosArrayGetSize(pItem->pNode->Expr.paramList) != 0) {
        return buildInvalidOperationMsg(pMsgBuf, msg2);
      }

      SColumnIndex index = {.tableIndex = 0, .columnIndex = 0, .type = TSDB_COL_NORMAL};
      pTableMetaInfo = getMetaInfo(pQueryInfo, index.tableIndex);

      int32_t inter   = 0;
      int16_t resType = 0;
      int16_t bytes   = 0;

      getResultDataInfo(TSDB_DATA_TYPE_INT, 4, functionId, 0, &resType, &bytes, &inter, 0, 0);

      SSchema s = createSchema(TSDB_DATA_TYPE_BINARY, bytes, getNewResColId(), "block_dist");
      SExprInfo* pExpr = createExprInfo(pTableMetaInfo, functionId, &index, NULL, &s, 0);
      addExprInfo(pQueryInfo, 0, pExpr);

      setTokenAndResColumnName(pItem, pExpr->base.resSchema.name, pExpr->base.token, TSDB_COL_NAME_LEN);

      int64_t rowSize = pTableMetaInfo->pTableMeta->tableInfo.rowSize;
      addExprInfoParam(&pExpr->base, (char*) &rowSize, TSDB_DATA_TYPE_BIGINT, 8);
      doAddResColumnOrSourceColumn(pQueryInfo, &index, 0, pExpr, &s, true);

      return TSDB_CODE_SUCCESS;
    }

    default: {
//      pUdfInfo = isValidUdf(pQueryInfo->pUdfInfo, pItem->pNode->Expr.operand.z, pItem->pNode->Expr.operand.n);
//      if (pUdfInfo == NULL) {
//        return buildInvalidOperationMsg(pMsgBuf, msg9);
//      }

      tSqlExprItem* pParamElem = taosArrayGet(pItem->pNode->Expr.paramList, 0);;
      if (pParamElem->pNode->tokenId != TK_ID) {
        return buildInvalidOperationMsg(pMsgBuf, msg2);
      }

      SColumnIndex index = COLUMN_INDEX_INITIALIZER;
      if (getColumnIndexByName(&pParamElem->pNode->columnName, pQueryInfo, &index, pMsgBuf) != TSDB_CODE_SUCCESS) {
        return buildInvalidOperationMsg(pMsgBuf, msg3);
      }

      if (index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
        return buildInvalidOperationMsg(pMsgBuf, msg6);
      }

      pTableMetaInfo = getMetaInfo(pQueryInfo, index.tableIndex);

      // functions can not be applied to tags
      if (index.columnIndex >= getNumOfColumns(pTableMetaInfo->pTableMeta)) {
        return buildInvalidOperationMsg(pMsgBuf, msg6);
      }

      int32_t inter   = 0;
      int16_t resType = 0;
      int16_t bytes   = 0;
      getResultDataInfo(TSDB_DATA_TYPE_INT, 4, functionId, 0, &resType, &bytes, &inter, 0, false/*, pUdfInfo*/);

      SSchema s = createSchema(resType, bytes, getNewResColId(), "");
      SExprInfo *pExpr = createExprInfo(pTableMetaInfo, functionId, &index, NULL, &s, inter);
      addExprInfo(pQueryInfo, colIndex, pExpr);

      setTokenAndResColumnName(pItem, s.name, pExpr->base.token, sizeof(pExpr->base.resSchema.name) - 1);

      s = createSchema(resType, bytes, pExpr->base.colInfo.colId, "");
      doAddResColumnOrSourceColumn(pQueryInfo, &index, colIndex, pExpr, &s, finalResult);
      return TSDB_CODE_SUCCESS;
    }
  }

  return TSDB_CODE_TSC_INVALID_OPERATION;
}

SExprInfo* doAddProjectCol(SQueryStmtInfo* pQueryInfo, int32_t outputColIndex, SColumnIndex* pColIndex, const char* aliasName, int32_t colId) {
  STableMeta*     pTableMeta = getMetaInfo(pQueryInfo, pColIndex->tableIndex)->pTableMeta;

  SSchema* pSchema = getOneColumnSchema(pTableMeta, pColIndex->columnIndex);
  SColumnIndex index = *pColIndex;

  int16_t functionId = 0;
  if (TSDB_COL_IS_TAG(index.type)) {
    int32_t numOfCols = getNumOfColumns(pTableMeta);
    index.columnIndex = pColIndex->columnIndex - numOfCols;
    functionId = FUNCTION_TAGPRJ;
  } else {
    index.columnIndex = pColIndex->columnIndex;
    functionId = FUNCTION_PRJ;
  }

  const char* name = (aliasName == NULL)? pSchema->name:aliasName;
  SSchema s = createSchema(pSchema->type, pSchema->bytes, colId, name);

  return doAddOneExprInfo(pQueryInfo, outputColIndex, functionId, &index, &s, pSchema->name);
}

static int32_t doAddProjectionExprAndResColumn(SQueryStmtInfo* pQueryInfo, SColumnIndex* pIndex, int32_t startPos) {
  STableMetaInfo* pTableMetaInfo = getMetaInfo(pQueryInfo, pIndex->tableIndex);

  STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;
  STableComInfo tinfo = getTableInfo(pTableMeta);

  int32_t numOfTotalColumns = tinfo.numOfColumns;
  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    numOfTotalColumns += tinfo.numOfTags;
  }

  for (int32_t j = 0; j < numOfTotalColumns; ++j) {
    pIndex->columnIndex = j;
    doAddProjectCol(pQueryInfo, startPos + j, pIndex, NULL, getNewResColId());
  }

  return numOfTotalColumns;
}

// User input constant value as a new result column
static SColumnIndex createConstantColumnIndex(int32_t* colId) {
  SColumnIndex index = COLUMN_INDEX_INITIALIZER;
  index.columnIndex = ((*colId)--);
  index.tableIndex = 0;
  index.type = TSDB_COL_UDC;
  return index;
}

static SSchema createConstantColumnSchema(SVariant* pVal, const SToken* exprStr, const char* name) {
  SSchema s = {0};

  s.type  = pVal->nType;
  if (IS_VAR_DATA_TYPE(s.type)) {
    s.bytes = (int16_t)(pVal->nLen + VARSTR_HEADER_SIZE);
  } else {
    s.bytes = tDataTypes[pVal->nType].bytes;
  }

  s.colId = TSDB_UD_COLUMN_INDEX;

  if (name != NULL) {
    tstrncpy(s.name, name, sizeof(s.name));
  } else {
    size_t tlen = MIN(sizeof(s.name), exprStr->n + 1);
    tstrncpy(s.name, exprStr->z, tlen);
    strdequote(s.name);
  }

  return s;
}

int32_t addProjectionExprAndResColumn(SQueryStmtInfo* pQueryInfo, tSqlExprItem* pItem, bool outerQuery, SMsgBuf* pMsgBuf) {
  const char* msg1 = "tag for normal table query is not allowed";
  const char* msg2 = "invalid column name";
  const char* msg3 = "tbname not allowed in outer query";

  if (checkForAliasName(pMsgBuf, pItem->aliasName) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  int32_t startPos = (int32_t)tscNumOfExprs(pQueryInfo);
  int32_t tokenId = pItem->pNode->tokenId;
  if (tokenId == TK_ALL) {  // project on all fields
    TSDB_QUERY_SET_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_PROJECTION_QUERY);

    SColumnIndex index = COLUMN_INDEX_INITIALIZER;
    if (getTableIndexByName(&pItem->pNode->columnName, pQueryInfo, &index) != TSDB_CODE_SUCCESS) {
      return buildInvalidOperationMsg(pMsgBuf, msg2);
    }

    // all columns are required
    if (index.tableIndex == COLUMN_INDEX_INITIAL_VAL) {  // all table columns are required.
      for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
        index.tableIndex = i;
        int32_t inc = doAddProjectionExprAndResColumn(pQueryInfo, &index, startPos);
        startPos += inc;
      }
    } else {
      doAddProjectionExprAndResColumn(pQueryInfo, &index, startPos);
    }

    // add the primary timestamp column even though it is not required by user
    STableMeta* pTableMeta = pQueryInfo->pTableMetaInfo[index.tableIndex]->pTableMeta;
    if (pTableMeta->tableType != TSDB_TEMP_TABLE) {
      insertPrimaryTsColumn(pQueryInfo->colList, pTableMeta->uid);
    }
  } else if (tokenId == TK_STRING || tokenId == TK_INTEGER || tokenId == TK_FLOAT) {  // simple column projection query
    SColumnIndex index = createConstantColumnIndex(&pQueryInfo->udColumnId);
    SSchema colSchema = createConstantColumnSchema(&pItem->pNode->value, &pItem->pNode->exprToken, pItem->aliasName);

    char rawName[TSDB_COL_NAME_LEN] = {0};
    tstrncpy(rawName, pItem->pNode->exprToken.z, MIN(TSDB_COL_NAME_LEN, TSDB_COL_NAME_LEN));
    SExprInfo* pExpr = doAddOneExprInfo(pQueryInfo, startPos, FUNCTION_PRJ, &index, &colSchema, rawName);

    // NOTE: the first parameter is reserved for the tag column id during join query process.
    pExpr->base.numOfParams = 2;
    taosVariantAssign(&pExpr->base.param[1], &pItem->pNode->value);
  } else if (tokenId == TK_ID) {
    SColumnIndex index = COLUMN_INDEX_INITIALIZER;
    if (getColumnIndexByName(&pItem->pNode->columnName, pQueryInfo, &index, pMsgBuf) != TSDB_CODE_SUCCESS) {
      return buildInvalidOperationMsg(pMsgBuf, msg2);
    }

    if (index.columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
      SSchema colSchema = {0};
      int32_t functionId = 0;

      if (outerQuery) {  // todo??
        STableMetaInfo* pTableMetaInfo = getMetaInfo(pQueryInfo, index.tableIndex);

        bool     existed = false;
        SSchema* pSchema = pTableMetaInfo->pTableMeta->schema;

        int32_t numOfCols = getNumOfColumns(pTableMetaInfo->pTableMeta);
        for (int32_t i = 0; i < numOfCols; ++i) {
          if (strncasecmp(pSchema[i].name, TSQL_TBNAME_L, tListLen(pSchema[i].name)) == 0) {
            existed = true;
            index.columnIndex = i;
            break;
          }
        }

        if (!existed) {
          return buildInvalidOperationMsg(pMsgBuf, msg3);
        }

        colSchema = pSchema[index.columnIndex];
        functionId = FUNCTION_PRJ;
      } else {
        colSchema = *getTbnameColumnSchema();
        functionId = FUNCTION_TAGPRJ;
      }

      colSchema.colId = getNewResColId();
      char rawName[TSDB_COL_NAME_LEN] = {0};
      setTokenAndResColumnName(pItem, colSchema.name, rawName, sizeof(colSchema.name) - 1);
      doAddOneExprInfo(pQueryInfo, startPos, functionId, &index, &colSchema, rawName);
    } else {
      STableMetaInfo* pTableMetaInfo = getMetaInfo(pQueryInfo, index.tableIndex);
      if (TSDB_COL_IS_TAG(index.type) && UTIL_TABLE_IS_NORMAL_TABLE(pTableMetaInfo)) {
        return buildInvalidOperationMsg(pMsgBuf, msg1);
      }

      doAddProjectCol(pQueryInfo, startPos, &index, pItem->aliasName, getNewResColId());
    }

    // add the primary timestamp column even though it is not required by user
    STableMetaInfo* pTableMetaInfo = getMetaInfo(pQueryInfo, index.tableIndex);
    if (!UTIL_TABLE_IS_TMP_TABLE(pTableMetaInfo)) {
      insertPrimaryTsColumn(pQueryInfo->colList, pTableMetaInfo->pTableMeta->uid);
    }
  } else {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t validateExprLeafNode(tSqlExpr* pExpr, SQueryStmtInfo* pQueryInfo, SArray* pList, int32_t* type, uint64_t* uid,
    SMsgBuf* pMsgBuf) {
  if (pExpr->type == SQL_NODE_TABLE_COLUMN) {
    if (*type == NON_ARITHMEIC_EXPR) {
      *type = NORMAL_ARITHMETIC;
    } else if (*type == AGG_ARIGHTMEIC) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    SColumnIndex index = COLUMN_INDEX_INITIALIZER;
    if (getColumnIndexByName(&pExpr->columnName, pQueryInfo, &index, pMsgBuf) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    // if column is timestamp, bool, binary, nchar, not support arithmetic, so return invalid sql
    STableMeta* pTableMeta = getMetaInfo(pQueryInfo, index.tableIndex)->pTableMeta;

    SSchema* pSchema = getOneColumnSchema(pTableMeta, index.columnIndex);
    if ((pSchema->type == TSDB_DATA_TYPE_TIMESTAMP) || (pSchema->type == TSDB_DATA_TYPE_BOOL) ||
        (pSchema->type == TSDB_DATA_TYPE_BINARY) || (pSchema->type == TSDB_DATA_TYPE_NCHAR)) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    taosArrayPush(pList, &index);
  } else if ((pExpr->tokenId == TK_FLOAT && (isnan(pExpr->value.d) || isinf(pExpr->value.d))) ||
             pExpr->tokenId == TK_NULL) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  } else if (pExpr->type == SQL_NODE_SQLFUNCTION) {
    if (*type == NON_ARITHMEIC_EXPR) {
      *type = AGG_ARIGHTMEIC;
    } else if (*type == NORMAL_ARITHMETIC) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    tSqlExprItem item = {.pNode = pExpr, .aliasName = NULL};

    // sql function list in selection clause.
    // Append the sqlExpr into exprList of pQueryInfo structure sequentially
    item.functionId = qIsBuiltinFunction(pExpr->Expr.operand.z, pExpr->Expr.operand.n);
    if (item.functionId < 0) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    int32_t outputIndex = (int32_t)tscNumOfExprs(pQueryInfo);
    if (addExprAndResColumn(pQueryInfo, outputIndex, &item, false, pMsgBuf) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    // It is invalid in case of more than one sqlExpr, such as first(ts, k) - last(ts, k)
    int32_t inc = (int32_t) tscNumOfExprs(pQueryInfo) - outputIndex;
    if (inc > 1) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    // Not supported data type in arithmetic expression
    uint64_t id = -1;
    for(int32_t i = 0; i < inc; ++i) {
      SExprInfo* p1 = getExprInfo(pQueryInfo, i + outputIndex);

      int16_t t = p1->base.resSchema.type;
      if (IS_VAR_DATA_TYPE(t) || t == TSDB_DATA_TYPE_BOOL || t == TSDB_DATA_TYPE_TIMESTAMP) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      if (i == 0) {
        id = p1->base.uid;
        continue;
      }

      if (id != p1->base.uid) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
    }

    *uid = id;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t validateComplexExpr(tSqlExpr* pExpr, SQueryStmtInfo* pQueryInfo, SArray* pColList, int32_t* type, SMsgBuf* pMsgBuf) {
  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  tSqlExpr* pLeft = pExpr->pLeft;
  uint64_t uidLeft = 0;
  uint64_t uidRight = 0;

  if (pLeft->type == SQL_NODE_EXPR) {
    int32_t ret = validateComplexExpr(pLeft, pQueryInfo, pColList, type, pMsgBuf);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  } else {
    int32_t ret = validateExprLeafNode(pLeft, pQueryInfo, pColList, type, &uidLeft, pMsgBuf);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  }

  tSqlExpr* pRight = pExpr->pRight;
  if (pRight->type == SQL_NODE_EXPR) {
    int32_t ret = validateComplexExpr(pRight, pQueryInfo, pColList, type, pMsgBuf);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  } else {
    int32_t ret = validateExprLeafNode(pRight, pQueryInfo, pColList, type, &uidRight, pMsgBuf);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t  sqlExprToExprNode(tExprNode **pExpr, const tSqlExpr* pSqlExpr, SQueryStmtInfo* pQueryInfo, SArray* pCols, uint64_t *uid, SMsgBuf* pMsgBuf) {
  tExprNode* pLeft = NULL;
  tExprNode* pRight= NULL;

  SColumnIndex index = COLUMN_INDEX_INITIALIZER;
  if (pSqlExpr->pLeft != NULL) {
    int32_t ret = sqlExprToExprNode(&pLeft, pSqlExpr->pLeft, pQueryInfo, pCols, uid, pMsgBuf);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
  }

  if (pSqlExpr->pRight != NULL) {
    int32_t ret = sqlExprToExprNode(&pRight, pSqlExpr->pRight, pQueryInfo, pCols, uid, pMsgBuf);
    if (ret != TSDB_CODE_SUCCESS) {
      tExprTreeDestroy(pLeft, NULL);
      return ret;
    }
  }

  if (pSqlExpr->pLeft == NULL && pSqlExpr->pRight == NULL && pSqlExpr->tokenId == 0) {
    *pExpr = calloc(1, sizeof(tExprNode));
    return TSDB_CODE_SUCCESS;
  }

  if (pSqlExpr->pLeft == NULL) {  // it is the leaf node
    assert(pSqlExpr->pRight == NULL);

    if (pSqlExpr->type == SQL_NODE_VALUE) {
      int32_t ret = TSDB_CODE_SUCCESS;
      *pExpr = calloc(1, sizeof(tExprNode));
      (*pExpr)->nodeType = TEXPR_VALUE_NODE;
      (*pExpr)->pVal = calloc(1, sizeof(SVariant));
      taosVariantAssign((*pExpr)->pVal, &pSqlExpr->value);

      STableMeta* pTableMeta = getMetaInfo(pQueryInfo, 0)->pTableMeta;
      if (pCols != NULL && taosArrayGetSize(pCols) > 0) {
        SColIndex* idx = taosArrayGet(pCols, 0);
        SSchema* pSchema = getOneColumnSchema(pTableMeta, idx->colIndex);

        // convert time by precision
        if (pSchema != NULL && TSDB_DATA_TYPE_TIMESTAMP == pSchema->type && TSDB_DATA_TYPE_BINARY == (*pExpr)->pVal->nType) {
#if 0
          ret = setColumnFilterInfoForTimestamp(pCmd, pQueryInfo, (*pExpr)->pVal);
#endif
        }
      }
      return ret;
    } else if (pSqlExpr->type == SQL_NODE_SQLFUNCTION) {
      // arithmetic expression on the results of aggregation functions
      *pExpr = calloc(1, sizeof(tExprNode));
      (*pExpr)->nodeType = TEXPR_COL_NODE;
      (*pExpr)->pSchema = calloc(1, sizeof(SSchema));
      strncpy((*pExpr)->pSchema->name, pSqlExpr->exprToken.z, pSqlExpr->exprToken.n);

      // set the input column data byte and type.
      size_t size = taosArrayGetSize(pQueryInfo->exprList);

      for (int32_t i = 0; i < size; ++i) {
        SExprInfo* p1 = taosArrayGetP(pQueryInfo->exprList, i);

        if (strcmp((*pExpr)->pSchema->name, p1->base.resSchema.name) == 0) {
          memcpy((*pExpr)->pSchema, &p1->base.resSchema, sizeof(SSchema));
          if (uid != NULL) {
            *uid = p1->base.uid;
          }

          break;
        }
      }
    } else if (pSqlExpr->type == SQL_NODE_TABLE_COLUMN) { // column name, normal column arithmetic expression
      int32_t ret = getColumnIndexByName(&pSqlExpr->columnName, pQueryInfo, &index, pMsgBuf);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }

      pQueryInfo->curTableIdx = index.tableIndex;
      STableMeta* pTableMeta = getMetaInfo(pQueryInfo, index.tableIndex)->pTableMeta;

      *pExpr = calloc(1, sizeof(tExprNode));
      (*pExpr)->nodeType = TEXPR_COL_NODE;
      (*pExpr)->pSchema = calloc(1, sizeof(SSchema));

      SSchema* pSchema = getOneColumnSchema(pTableMeta, index.columnIndex);
      *(*pExpr)->pSchema = *pSchema;

      if (pCols != NULL) {  // record the involved columns
        SColIndex colIndex = {0};
        tstrncpy(colIndex.name, pSchema->name, sizeof(colIndex.name));
        colIndex.colId = pSchema->colId;
        colIndex.colIndex = index.columnIndex;
        colIndex.flag = index.type;

        taosArrayPush(pCols, &colIndex);
      }

      return TSDB_CODE_SUCCESS;
    } else if (pSqlExpr->tokenId == TK_SET) {
      int32_t colType = -1;
      STableMeta* pTableMeta = getMetaInfo(pQueryInfo, pQueryInfo->curTableIdx)->pTableMeta;
      if (pCols != NULL) {
        size_t colSize = taosArrayGetSize(pCols);

        if (colSize > 0) {
          SColIndex* idx = taosArrayGet(pCols, colSize - 1);
          SSchema* pSchema = getOneColumnSchema(pTableMeta, idx->colIndex);
          if (pSchema != NULL) {
            colType = pSchema->type;
          }
        }
      }

      SVariant *pVal;
      if (colType >= TSDB_DATA_TYPE_TINYINT && colType <= TSDB_DATA_TYPE_BIGINT) {
        colType = TSDB_DATA_TYPE_BIGINT;
      } else if (colType == TSDB_DATA_TYPE_FLOAT || colType == TSDB_DATA_TYPE_DOUBLE) {
        colType = TSDB_DATA_TYPE_DOUBLE;
      }
      STableMetaInfo* pTableMetaInfo = getMetaInfo(pQueryInfo, pQueryInfo->curTableIdx);
      STableComInfo tinfo = getTableInfo(pTableMetaInfo->pTableMeta);
#if 0
      if (serializeExprListToVariant(pSqlExpr->Expr.paramList, &pVal, colType, tinfo.precision) == false) {
        return buildInvalidOperationMsg(pMsgBuf, "not support filter expression");
      }
#endif
      *pExpr = calloc(1, sizeof(tExprNode));
      (*pExpr)->nodeType = TEXPR_VALUE_NODE;
      (*pExpr)->pVal = pVal;
    } else {
      return buildInvalidOperationMsg(pMsgBuf, "not support filter expression");
    }

  } else {
    *pExpr = (tExprNode *)calloc(1, sizeof(tExprNode));
    (*pExpr)->nodeType = TEXPR_BINARYEXPR_NODE;

    (*pExpr)->_node.pLeft = pLeft;
    (*pExpr)->_node.pRight = pRight;

    SToken t = {.type = pSqlExpr->tokenId};
    (*pExpr)->_node.optr = convertRelationalOperator(&t);

    assert((*pExpr)->_node.optr != 0);

    // check for dividing by 0
    if ((*pExpr)->_node.optr == TSDB_BINARY_OP_DIVIDE) {
      if (pRight->nodeType == TEXPR_VALUE_NODE) {
        if (pRight->pVal->nType == TSDB_DATA_TYPE_INT && pRight->pVal->i64 == 0) {
          return buildInvalidOperationMsg(pMsgBuf, "invalid expr (divide by 0)");
        } else if (pRight->pVal->nType == TSDB_DATA_TYPE_FLOAT && pRight->pVal->d == 0) {
          return buildInvalidOperationMsg(pMsgBuf, "invalid expr (divide by 0)");
        }
      }
    }

    // NOTE: binary|nchar data allows the >|< type filter
    if ((*pExpr)->_node.optr != TSDB_RELATION_EQUAL && (*pExpr)->_node.optr != TSDB_RELATION_NOT_EQUAL) {
      if (pRight != NULL && pRight->nodeType == TEXPR_VALUE_NODE) {
        if (pRight->pVal->nType == TSDB_DATA_TYPE_BOOL && pLeft->pSchema->type == TSDB_DATA_TYPE_BOOL) {
          return buildInvalidOperationMsg(pMsgBuf, "invalid operator for bool");
        }
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t multiColumnListInsert(SQueryStmtInfo* pQueryInfo, SArray* pColumnList, SMsgBuf* pMsgBuf) {
  const char* msg3 = "tag columns can not be used in arithmetic expression";

  SColumnIndex* p1 = taosArrayGet(pColumnList, 0);
  STableMeta* pTableMeta = getMetaInfo(pQueryInfo, p1->tableIndex)->pTableMeta;

  size_t numOfNode = taosArrayGetSize(pColumnList);
  for(int32_t k = 0; k < numOfNode; ++k) {
    SColumnIndex* pIndex = taosArrayGet(pColumnList, k);
    if (TSDB_COL_IS_TAG(pIndex->type)) {
      return buildInvalidOperationMsg(pMsgBuf, msg3);
    }

    SSchema* ps = getOneColumnSchema(pTableMeta, pIndex->columnIndex);
    columnListInsert(pQueryInfo->colList, pIndex->columnIndex, pTableMeta->uid, ps);
  }

  insertPrimaryTsColumn(pQueryInfo->colList, pTableMeta->uid);
  return TSDB_CODE_SUCCESS;
}

static int32_t createComplexExpr(SQueryStmtInfo* pQueryInfo, int32_t exprIndex, tSqlExprItem* pItem, SMsgBuf* pMsgBuf) {
  const char* msg1 = "invalid column name, illegal column type, or columns in arithmetic expression from two tables";
  const char* msg2 = "invalid arithmetic expression in select clause";
  const char* msg3 = "tag columns can not be used in arithmetic expression";

  int32_t arithmeticType = NON_ARITHMEIC_EXPR;
  SArray* pColumnList = taosArrayInit(4, sizeof(SColumnIndex));
  if (validateComplexExpr(pItem->pNode, pQueryInfo, pColumnList, &arithmeticType, pMsgBuf) != TSDB_CODE_SUCCESS) {
    return buildInvalidOperationMsg(pMsgBuf, msg1);
  }

  if (arithmeticType == NORMAL_ARITHMETIC) {
    // expr string is set as the parameter of function
    SSchema s = createSchema(TSDB_DATA_TYPE_DOUBLE, sizeof(double), getNewResColId(), "");

    tExprNode* pNode = NULL;
    SArray* colList = taosArrayInit(10, sizeof(SColIndex));
    int32_t ret = sqlExprToExprNode(&pNode, pItem->pNode, pQueryInfo, colList, NULL, pMsgBuf);
    if (ret != TSDB_CODE_SUCCESS) {
      taosArrayDestroy(colList);
      tExprTreeDestroy(pNode, NULL);
      return buildInvalidOperationMsg(pMsgBuf, msg2);
    }

    SExprInfo* pExpr = createBinaryExprInfo(pNode, &s);
    addExprInfo(pQueryInfo, exprIndex, pExpr);
    setTokenAndResColumnName(pItem, pExpr->base.resSchema.name, pExpr->base.token, TSDB_COL_NAME_LEN);

    // check for if there is a tag in the arithmetic express
    int32_t code = multiColumnListInsert(pQueryInfo, pColumnList, pMsgBuf);
    if (code != TSDB_CODE_SUCCESS) {
      taosArrayDestroy(colList);
      tExprTreeDestroy(pNode, NULL);
      return code;
    }

    SBufferWriter bw = tbufInitWriter(NULL, false);

//    TRY(0) {
      exprTreeToBinary(&bw, pNode);
//    } CATCH(code) {
//      tbufCloseWriter(&bw);
//      UNUSED(code);
//       TODO: other error handling
//    } END_TRY

    int32_t len = tbufTell(&bw);
    char* c = tbufGetData(&bw, false);

    // set the serialized binary string as the parameter of arithmetic expression
    SColumnIndex* index1 = taosArrayGet(pColumnList, 0);
    addExprInfoParam(&pExpr->base, c, TSDB_DATA_TYPE_BINARY, (int32_t)len);
    addResColumnInfo(pQueryInfo, exprIndex, &pExpr->base.resSchema, pExpr);

    tbufCloseWriter(&bw);
    taosArrayDestroy(colList);
    tExprTreeDestroy(pNode, NULL);
  } else {
    SColumnIndex  columnIndex = {0};

    SSchema s = createSchema(TSDB_DATA_TYPE_DOUBLE, sizeof(double), getNewResColId(), "");
    addResColumnInfo(pQueryInfo, exprIndex, &s, NULL);

    tExprNode* pNode = NULL;
    int32_t ret = sqlExprToExprNode(&pNode, pItem->pNode, pQueryInfo, NULL, NULL, pMsgBuf);
    if (ret != TSDB_CODE_SUCCESS) {
      tExprTreeDestroy(pNode, NULL);
      return buildInvalidOperationMsg(pMsgBuf, "invalid expression in select clause");
    }

    SExprInfo* pExpr = createBinaryExprInfo(pNode, &s);
    addExprInfo(pQueryInfo, exprIndex, pExpr);
    setTokenAndResColumnName(pItem, pExpr->base.resSchema.name, pExpr->base.token, TSDB_COL_NAME_LEN);

    pExpr->base.numOfParams = 1;

    SBufferWriter bw = tbufInitWriter(NULL, false);
//    TRY(0) {
      exprTreeToBinary(&bw, pExpr->pExpr);
//    } CATCH(code) {
//      tbufCloseWriter(&bw);
//      UNUSED(code);
//       TODO: other error handling
//    } END_TRY

    SSqlExpr* pSqlExpr = &pExpr->base;
    pSqlExpr->param[0].nLen = (int16_t) tbufTell(&bw);
    pSqlExpr->param[0].pz   = tbufGetData(&bw, true);
    pSqlExpr->param[0].nType = TSDB_DATA_TYPE_BINARY;

    tbufCloseWriter(&bw);

//    tbufCloseWriter(&bw); // TODO there is a memory leak
  }

  return TSDB_CODE_SUCCESS;
}

int32_t validateSelectNodeList(SQueryStmtInfo* pQueryInfo, SArray* pSelNodeList, bool outerQuery, SMsgBuf* pMsgBuf) {
  assert(pSelNodeList != NULL);

  const char* msg1 = "too many items in selection clause";
  const char* msg2 = "functions or others can not be mixed up";
  const char* msg3 = "not support query expression";
  const char* msg4 = "not support distinct mixed with proj/agg func";
  const char* msg5 = "invalid function name";
  const char* msg9 = "_block_dist not support subquery, only support stable/table";

  // too many result columns not support order by in query
  if (taosArrayGetSize(pSelNodeList) > TSDB_MAX_COLUMNS) {
    return buildInvalidOperationMsg(pMsgBuf, msg1);
  }

  if (pQueryInfo->colList == NULL) {
    pQueryInfo->colList = taosArrayInit(4, POINTER_BYTES);
  }

  size_t numOfExpr = taosArrayGetSize(pSelNodeList);

  for (int32_t i = 0; i < numOfExpr; ++i) {
    int32_t outputIndex = (int32_t)tscNumOfExprs(pQueryInfo);
    tSqlExprItem* pItem = taosArrayGet(pSelNodeList, i);
    int32_t type = pItem->pNode->type;

    if (pItem->distinct) {
      if (i != 0 || type == SQL_NODE_SQLFUNCTION || type == SQL_NODE_EXPR) {
        return buildInvalidOperationMsg(pMsgBuf, msg4);
      }

      pQueryInfo->distinct = true;
    }

    if (type == SQL_NODE_SQLFUNCTION) {
      pItem->functionId = qIsBuiltinFunction(pItem->pNode->Expr.operand.z, pItem->pNode->Expr.operand.n);
      if (pItem->functionId == FUNCTION_INVALID_ID) {
        int32_t functionId = FUNCTION_INVALID_ID;
        bool valid = qIsValidUdf(pQueryInfo->pUdfInfo, pItem->pNode->Expr.operand.z, pItem->pNode->Expr.operand.n, &functionId);
        if (!valid) {
          return buildInvalidOperationMsg(pMsgBuf, msg5);
        }

        pItem->functionId = functionId;
      }

      // sql function in selection clause, append sql function info in pSqlCmd structure sequentially
      if (addExprAndResColumn(pQueryInfo, outputIndex, pItem, true, pMsgBuf) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
    } else if (type == SQL_NODE_TABLE_COLUMN || type == SQL_NODE_VALUE) {
      // use the dynamic array list to decide if the function is valid or not
      // select table_name1.field_name1, table_name2.field_name2  from table_name1, table_name2
      if (addProjectionExprAndResColumn(pQueryInfo, pItem, outerQuery, pMsgBuf) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
    } else if (type == SQL_NODE_EXPR) {
      int32_t code = createComplexExpr(pQueryInfo, i, pItem, pMsgBuf);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    } else {
      return buildInvalidOperationMsg(pMsgBuf, msg3);
    }

    if (pQueryInfo->fieldsInfo.numOfOutput > TSDB_MAX_COLUMNS) {
      return buildInvalidOperationMsg(pMsgBuf, msg1);
    }
  }

  // there is only one user-defined column in the final result field, add the timestamp column.
//  size_t numOfSrcCols = taosArrayGetSize(pQueryInfo->colList);
//  if ((numOfSrcCols <= 0 || !hasNoneUserDefineExpr(pQueryInfo)) && !tscQueryTags(pQueryInfo) && !tscQueryBlockInfo(pQueryInfo)) {
//    addPrimaryTsColIntoResult(pQueryInfo, pCmd);
//  }

  return TSDB_CODE_SUCCESS;
}

int32_t evaluateSqlNode(SSqlNode* pNode, int32_t tsPrecision, SMsgBuf* pMsgBuf) {
  assert(pNode != NULL && pMsgBuf != NULL && pMsgBuf->len > 0);
  if (pNode->pWhere == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = evaluateImpl(pNode->pWhere, tsPrecision);
  if (code != TSDB_CODE_SUCCESS) {
    strncpy(pMsgBuf->buf, "invalid time expression in sql", pMsgBuf->len);
    return code;
  }

  size_t size = taosArrayGetSize(pNode->pSelNodeList);
  for(int32_t i = 0; i < size; ++i) {
    tSqlExprItem* pItem = taosArrayGet(pNode->pSelNodeList, i);
    code = evaluateImpl(pItem->pNode, tsPrecision);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  code = evaluateImpl(pNode->pSelNodeList, tsPrecision);

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
        return setInvalidOperatorMsg(pMsgBuf, msg2);
      }

      if (pInfo->type == TSDB_SQL_DROP_DB) {
        assert(taosArrayGetSize(pInfo->pMiscInfo->a) == 1);
        code = tNameSetDbName(&pTableMetaInfo->name, getAccountId(pSql), pzName);
        if (code != TSDB_CODE_SUCCESS) {
          return setInvalidOperatorMsg(pMsgBuf, msg2);
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
          return setInvalidOperatorMsg(pMsgBuf, msg3);
        }

        strncpy(pCmd->payload, pzName->z, pzName->n);
      }

      break;
    }

    case TSDB_SQL_USE_DB: {
      const char* msg = "invalid db name";
      SToken* pToken = taosArrayGet(pInfo->pMiscInfo->a, 0);

      if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(pMsgBuf, msg);
      }

      int32_t ret = tNameSetDbName(&pTableMetaInfo->name, getAccountId(pSql), pToken);
      if (ret != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(pMsgBuf, msg);
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
        return setInvalidOperatorMsg(pMsgBuf, msg2);
      }

      char buf[TSDB_DB_NAME_LEN] = {0};
      SToken token = taosTokenDup(&pCreateDB->dbname, buf, tListLen(buf));

      if (tscValidateName(&token) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(pMsgBuf, msg1);
      }

      int32_t ret = tNameSetDbName(&pTableMetaInfo->name, getAccountId(pSql), &token);
      if (ret != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(pMsgBuf, msg2);
      }

      if (parseCreateDBOptions(pCmd, pCreateDB) != TSDB_CODE_SUCCESS) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      break;
    }

    case TSDB_SQL_CREATE_DNODE: {
      const char* msg = "invalid host name (ip address)";

      if (taosArrayGetSize(pInfo->pMiscInfo->a) > 1) {
        return setInvalidOperatorMsg(pMsgBuf, msg);
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
        return setInvalidOperatorMsg(pMsgBuf, msg3);
      }

      if (tscValidateName(pName) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(pMsgBuf, msg2);
      }

      SCreateAcctInfo* pAcctOpt = &pInfo->pMiscInfo->acctOpt;
      if (pAcctOpt->stat.n > 0) {
        if (pAcctOpt->stat.z[0] == 'r' && pAcctOpt->stat.n == 1) {
        } else if (pAcctOpt->stat.z[0] == 'w' && pAcctOpt->stat.n == 1) {
        } else if (strncmp(pAcctOpt->stat.z, "all", 3) == 0 && pAcctOpt->stat.n == 3) {
        } else if (strncmp(pAcctOpt->stat.z, "no", 2) == 0 && pAcctOpt->stat.n == 2) {
        } else {
          return setInvalidOperatorMsg(pMsgBuf, msg1);
        }
      }

      break;
    }

    case TSDB_SQL_DESCRIBE_TABLE: {
      const char* msg1 = "invalid table name";

      SToken* pToken = taosArrayGet(pInfo->pMiscInfo->a, 0);
      if (tscValidateName(pToken) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(pMsgBuf, msg1);
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
        return setInvalidOperatorMsg(pMsgBuf, msg1);
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
        return setInvalidOperatorMsg(pMsgBuf, msg1);
      }

      if (pToken->n > TSDB_DB_NAME_LEN) {
        return setInvalidOperatorMsg(pMsgBuf, msg1);
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
        return setInvalidOperatorMsg(pMsgBuf, msg2);
      }

      char* pMsg = pCmd->payload;

      SCfgDnodeMsg* pCfg = (SCfgDnodeMsg*)pMsg;

      SToken* t0 = taosArrayGet(pMiscInfo->a, 0);
      SToken* t1 = taosArrayGet(pMiscInfo->a, 1);

      t0->n = strdequote(t0->z);
      strncpy(pCfg->ep, t0->z, t0->n);

      if (validateEp(pCfg->ep) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(pMsgBuf, msg3);
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
        return setInvalidOperatorMsg(pMsgBuf, msg3);
      }

      if (tscValidateName(pName) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(pMsgBuf, msg2);
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
            return setInvalidOperatorMsg(pMsgBuf, msg5);
          }
        } else {
          return setInvalidOperatorMsg(pMsgBuf, msg7);
        }
      }

      break;
    }

    case TSDB_SQL_CFG_LOCAL: {
      SMiscInfo  *pMiscInfo = pInfo->pMiscInfo;
      const char *msg = "invalid configure options or values";

      // validate the parameter names and options
      if (validateLocalConfig(pMiscInfo) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(pMsgBuf, msg);
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
          return setInvalidOperatorMsg(pMsgBuf, msg1);
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
        return setInvalidOperatorMsg(pMsgBuf, msg1);
      }
      break;
    }
    case TSDB_SQL_COMPACT_VNODE:{
      const char* msg = "invalid compact";
      if (setCompactVnodeInfo(pSql, pInfo) != TSDB_CODE_SUCCESS) {
        return setInvalidOperatorMsg(pMsgBuf, msg);
      }
      break;
    }
    default:
      return setInvalidOperatorMsg(pMsgBuf, "not support sql expression");
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

  SMsgBuf buf = {.buf = msgBuf, .len = msgBufLen};

  size_t len = taosArrayGetSize(pInfo->list);
  for(int32_t i = 0; i < len; ++i) {
    SSqlNode* p = taosArrayGetP(pInfo->list, i);
    code = evaluateSqlNode(p, pTableMeta->tableInfo.precision, &buf);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  for(int32_t i = 0; i < len; ++i) {
    SSqlNode* p = taosArrayGetP(pInfo->list, i);
    validateSqlNode(p, pQueryInfo, &buf);
  }

  // convert the sqlnode into queryinfo
  return code;
}
