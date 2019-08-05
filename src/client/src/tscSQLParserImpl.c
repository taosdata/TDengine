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

#include <stdio.h>
#include <stdlib.h>

#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include <assert.h>
#include <errno.h>
#include <stdarg.h>

#include "os.h"
#include "tglobalcfg.h"
#include "tsql.h"
#include "tstoken.h"
#include "ttime.h"
#include "tutil.h"

int32_t tSQLParse(SSqlInfo *pSQLInfo, const char *pStr) {
  void *pParser = ParseAlloc(malloc);
  pSQLInfo->validSql = true;

  int32_t i = 0;
  while (1) {
    SSQLToken t0 = {0};

    if (pStr[i] == 0) {
      Parse(pParser, 0, t0, pSQLInfo);
      goto abort_parse;
    }

    t0.n = tSQLGetToken((char *)&pStr[i], &t0.type);
    t0.z = (char *)(pStr + i);
    i += t0.n;

    switch (t0.type) {
      case TK_SPACE:
      case TK_COMMENT: {
        break;
      }
      case TK_SEMI: {
        Parse(pParser, 0, t0, pSQLInfo);
        goto abort_parse;
      }
      case TK_ILLEGAL: {
        sprintf(pSQLInfo->pzErrMsg, "unrecognized token: \"%s\"", t0.z);
        pSQLInfo->validSql = false;
        goto abort_parse;
      }
      default:
        Parse(pParser, t0.type, t0, pSQLInfo);
        if (pSQLInfo->validSql == false) {
          goto abort_parse;
        }
    }
  }

abort_parse:
  ParseFree(pParser, free);
  return 0;
}

tSQLExprList *tSQLExprListAppend(tSQLExprList *pList, tSQLExpr *pNode, SSQLToken *pToken) {
  if (pList == NULL) {
    pList = calloc(1, sizeof(tSQLExprList));
  }

  if (pList->nAlloc <= pList->nExpr) {  //
    pList->nAlloc = (pList->nAlloc << 1) + 4;
    pList->a = realloc(pList->a, pList->nAlloc * sizeof(pList->a[0]));
    if (pList->a == 0) {
      pList->nExpr = pList->nAlloc = 0;
      return pList;
    }
  }
  assert(pList->a != 0);

  if (pNode || pToken) {
    struct tSQLExprItem *pItem = &pList->a[pList->nExpr++];
    memset(pItem, 0, sizeof(*pItem));
    pItem->pNode = pNode;
    if (pToken) {  // set the as clause
      pItem->aliasName = malloc(pToken->n + 1);
      strncpy(pItem->aliasName, pToken->z, pToken->n);
      pItem->aliasName[pToken->n] = 0;

      strdequote(pItem->aliasName);
    }
  }
  return pList;
}

void tSQLExprListDestroy(tSQLExprList *pList) {
  if (pList == NULL) return;

  for (int32_t i = 0; i < pList->nExpr; ++i) {
    if (pList->a[i].aliasName != NULL) {
      free(pList->a[i].aliasName);
    }
    tSQLExprDestroy(pList->a[i].pNode);
  }

  free(pList->a);
  free(pList);
}

tSQLExpr *tSQLExprIdValueCreate(SSQLToken *pToken, int32_t optrType) {
  tSQLExpr *nodePtr = calloc(1, sizeof(tSQLExpr));

  if (optrType == TK_INTEGER || optrType == TK_STRING || optrType == TK_FLOAT || optrType == TK_BOOL) {
    toTSDBType(pToken->type);

    tVariantCreate(&nodePtr->val, pToken);
    nodePtr->nSQLOptr = optrType;
  } else if (optrType == TK_NOW) {
    // default use microsecond
    nodePtr->val.i64Key = taosGetTimestamp(TSDB_TIME_PRECISION_MICRO);
    nodePtr->val.nType = TSDB_DATA_TYPE_BIGINT;
    nodePtr->nSQLOptr = TK_TIMESTAMP;
    // TK_TIMESTAMP used to denote the time value is in microsecond
  } else if (optrType == TK_VARIABLE) {
    int32_t ret = getTimestampInUsFromStr(pToken->z, pToken->n, &nodePtr->val.i64Key);
    UNUSED(ret);

    nodePtr->val.nType = TSDB_DATA_TYPE_BIGINT;
    nodePtr->nSQLOptr = TK_TIMESTAMP;
  } else {  // must be field id if not numbers
    assert(optrType == TK_ALL || optrType == TK_ID);

    if (pToken != NULL) { // it must be the column name (tk_id)
      nodePtr->colInfo = *pToken;
    }

    nodePtr->nSQLOptr = optrType;
  }
  return nodePtr;
}

/*
 * pList is the parameters for function with id(optType)
 * function name is denoted by pFunctionToken
 */
tSQLExpr *tSQLExprCreateFunction(tSQLExprList *pList, SSQLToken *pFuncToken, SSQLToken *endToken, int32_t optType) {
  if (pFuncToken == NULL) return NULL;

  tSQLExpr *pExpr = calloc(1, sizeof(tSQLExpr));
  pExpr->nSQLOptr = optType;
  pExpr->pParam = pList;

  int32_t len = (endToken->z + endToken->n) - pFuncToken->z;
  pExpr->operand.z = pFuncToken->z;

  pExpr->operand.n = len;  // raw field name
  pExpr->operand.type = pFuncToken->type;
  return pExpr;
}

/*
 * create binary expression in this procedure
 * if the expr is arithmetic, calculate the result and set it to tSQLExpr Object
 */
tSQLExpr *tSQLExprCreate(tSQLExpr *pLeft, tSQLExpr *pRight, int32_t optrType) {
  tSQLExpr *pExpr = calloc(1, sizeof(tSQLExpr));

  if (optrType == TK_PLUS || optrType == TK_MINUS || optrType == TK_STAR || optrType == TK_DIVIDE ||
      optrType == TK_REM) {
    /*
     * if a token is noted as the TK_TIMESTAMP, the time precision is microsecond
     * Otherwise, the time precision is adaptive, determined by the time precision from databases.
     */
    if ((pLeft->nSQLOptr == TK_INTEGER && pRight->nSQLOptr == TK_INTEGER) ||
        (pLeft->nSQLOptr == TK_TIMESTAMP && pRight->nSQLOptr == TK_TIMESTAMP)) {
      pExpr->val.nType = TSDB_DATA_TYPE_BIGINT;
      pExpr->nSQLOptr = pLeft->nSQLOptr;

      switch (optrType) {
        case TK_PLUS: {
          pExpr->val.i64Key = pLeft->val.i64Key + pRight->val.i64Key;
          break;
        }
        case TK_MINUS: {
          pExpr->val.i64Key = pLeft->val.i64Key - pRight->val.i64Key;
          break;
        }
        case TK_STAR: {
          pExpr->val.i64Key = pLeft->val.i64Key * pRight->val.i64Key;
          break;
        }
        case TK_DIVIDE: {
          pExpr->nSQLOptr = TK_FLOAT;
          pExpr->val.nType = TSDB_DATA_TYPE_DOUBLE;
          pExpr->val.dKey = (double)pLeft->val.i64Key / pRight->val.i64Key;
          break;
        }
        case TK_REM: {
          pExpr->val.i64Key = pLeft->val.i64Key % pRight->val.i64Key;
          break;
        }
      }

      tSQLExprDestroy(pLeft);
      tSQLExprDestroy(pRight);

    } else if ((pLeft->val.nType == TSDB_DATA_TYPE_DOUBLE && pRight->val.nType == TSDB_DATA_TYPE_BIGINT) ||
               (pRight->val.nType == TSDB_DATA_TYPE_DOUBLE && pLeft->val.nType == TSDB_DATA_TYPE_BIGINT)) {
      pExpr->val.nType = TSDB_DATA_TYPE_DOUBLE;
      pExpr->nSQLOptr = TK_FLOAT;

      double left = pLeft->val.nType == TSDB_DATA_TYPE_DOUBLE ? pLeft->val.dKey : pLeft->val.i64Key;
      double right = pRight->val.nType == TSDB_DATA_TYPE_DOUBLE ? pRight->val.dKey : pRight->val.i64Key;

      switch (optrType) {
        case TK_PLUS: {
          pExpr->val.dKey = left + right;
          break;
        }
        case TK_MINUS: {
          pExpr->val.dKey = left - right;
          break;
        }
        case TK_STAR: {
          pExpr->val.dKey = left * right;
          break;
        }
        case TK_DIVIDE: {
          pExpr->val.dKey = left / right;
          break;
        }
        case TK_REM: {
          pExpr->val.dKey = left - ((int64_t)(left / right)) * right;
          break;
        }
      }

      tSQLExprDestroy(pLeft);
      tSQLExprDestroy(pRight);

    } else {
      pExpr->nSQLOptr = optrType;
      pExpr->pLeft = pLeft;
      pExpr->pRight = pRight;
    }
  } else if (optrType == TK_IN) {
    pExpr->nSQLOptr = optrType;
    pExpr->pLeft = pLeft;

    tSQLExpr *pRSub = calloc(1, sizeof(tSQLExpr));
    pRSub->nSQLOptr = TK_SET;  // TODO refactor .....
    pRSub->pParam = (tSQLExprList *)pRight;

    pExpr->pRight = pRSub;
  } else {
    pExpr->nSQLOptr = optrType;
    pExpr->pLeft = pLeft;
    pExpr->pRight = pRight;
  }

  return pExpr;
}

void tSQLExprDestroy(tSQLExpr *pExpr) {
  if (pExpr == NULL) return;

  tSQLExprDestroy(pExpr->pLeft);
  tSQLExprDestroy(pExpr->pRight);

  if (pExpr->nSQLOptr == TK_STRING) {
    tVariantDestroy(&pExpr->val);
  }

  tSQLExprListDestroy(pExpr->pParam);

  free(pExpr);
}

static void *tVariantListExpand(tVariantList *pList) {
  if (pList->nAlloc <= pList->nExpr) {  //
    int32_t newSize = (pList->nAlloc << 1) + 4;

    void *ptr = realloc(pList->a, newSize * sizeof(pList->a[0]));
    if (ptr == 0) {
      return NULL;
    }

    pList->nAlloc = newSize;
    pList->a = ptr;
  }

  assert(pList->a != 0);
  return pList;
}

tVariantList *tVariantListAppend(tVariantList *pList, tVariant *pVar, uint8_t sortOrder) {
  if (pList == NULL) {
    pList = calloc(1, sizeof(tVariantList));
  }

  if (tVariantListExpand(pList) == NULL) {
    return pList;
  }

  if (pVar) {
    tVariantListItem *pItem = &pList->a[pList->nExpr++];
    /*
     * Here we do not employ the assign function, since we need the pz attribute of structure
     * , which is the point to char string, to free it!
     *
     * Otherwise, the original pointer may be lost, which causes memory leak.
     */
    memcpy(pItem, pVar, sizeof(tVariant));
    pItem->sortOrder = sortOrder;
  }
  return pList;
}

tVariantList *tVariantListInsert(tVariantList *pList, tVariant *pVar, uint8_t sortOrder, int32_t index) {
  if (pList == NULL || index >= pList->nExpr) {
    return tVariantListAppend(NULL, pVar, sortOrder);
  }

  if (tVariantListExpand(pList) == NULL) {
    return pList;
  }

  if (pVar) {
    memmove(&pList->a[index + 1], &pList->a[index], sizeof(tVariantListItem) * (pList->nExpr - index));

    tVariantListItem *pItem = &pList->a[index];
    /*
     * Here we do not employ the assign function, since we need the pz attribute of structure
     * , which is the point to char string, to free it!
     *
     * Otherwise, the original pointer may be lost, which causes memory leak.
     */
    memcpy(pItem, pVar, sizeof(tVariant));
    pItem->sortOrder = sortOrder;

    pList->nExpr++;
  }

  return pList;
}

void tVariantListDestroy(tVariantList *pList) {
  if (pList == NULL) return;

  for (int32_t i = 0; i < pList->nExpr; ++i) {
    tVariantDestroy(&pList->a[i].pVar);
  }

  free(pList->a);
  free(pList);
}

tVariantList *tVariantListAppendToken(tVariantList *pList, SSQLToken *pAliasToken, uint8_t sortOrder) {
  if (pList == NULL) {
    pList = calloc(1, sizeof(tVariantList));
  }

  if (tVariantListExpand(pList) == NULL) {
    return pList;
  }

  if (pAliasToken) {
    tVariant t = {0};
    tVariantCreate(&t, pAliasToken);

    tVariantListItem *pItem = &pList->a[pList->nExpr++];
    memcpy(pItem, &t, sizeof(tVariant));
    pItem->sortOrder = sortOrder;
  }
  return pList;
}

tFieldList *tFieldListAppend(tFieldList *pList, TAOS_FIELD *pField) {
  if (pList == NULL) pList = calloc(1, sizeof(tFieldList));

  if (pList->nAlloc <= pList->nField) {  //
    pList->nAlloc = (pList->nAlloc << 1) + 4;
    pList->p = realloc(pList->p, pList->nAlloc * sizeof(pList->p[0]));
    if (pList->p == 0) {
      pList->nField = pList->nAlloc = 0;
      return pList;
    }
  }
  assert(pList->p != 0);

  if (pField) {
    struct TAOS_FIELD *pItem = (struct TAOS_FIELD *)&pList->p[pList->nField++];
    memcpy(pItem, pField, sizeof(TAOS_FIELD));
  }
  return pList;
}

void tFieldListDestroy(tFieldList *pList) {
  if (pList == NULL) return;

  free(pList->p);
  free(pList);
}

void setDBName(SSQLToken *pCpxName, SSQLToken *pDB) {
  pCpxName->type = pDB->type;
  pCpxName->z = pDB->z;
  pCpxName->n = pDB->n;
}

void tSQLSetColumnInfo(TAOS_FIELD *pField, SSQLToken *pName, TAOS_FIELD *pType) {
  int32_t maxLen = sizeof(pField->name) / sizeof(pField->name[0]);
  /* truncate the column name */
  if (pName->n >= maxLen) {
    pName->n = maxLen - 1;
  }

  strncpy(pField->name, pName->z, pName->n);
  pField->name[pName->n] = 0;

  pField->type = pType->type;
  pField->bytes = pType->bytes;
}

void tSQLSetColumnType(TAOS_FIELD *pField, SSQLToken *type) {
  pField->type = -1;

  for (int8_t i = 0; i < sizeof(tDataTypeDesc) / sizeof(tDataTypeDesc[0]); ++i) {
    if ((strncasecmp(type->z, tDataTypeDesc[i].aName, tDataTypeDesc[i].nameLen) == 0) &&
        (type->n == tDataTypeDesc[i].nameLen)) {
      pField->type = i;
      pField->bytes = tDataTypeDesc[i].nSize;

      if (i == TSDB_DATA_TYPE_NCHAR) {
        /*
         * for nchar, the TOKENTYPE is the number of character, so the length is the
         * number of bytes in UCS-4 format, which is 4 times larger than the
         * number of characters
         */
        pField->bytes = -(int32_t)type->type * TSDB_NCHAR_SIZE;
      } else if (i == TSDB_DATA_TYPE_BINARY) {
        /* for binary, the TOKENTYPE is the length of binary */
        pField->bytes = -(int32_t)type->type;
      }
      break;
    }
  }
}

/*
 * extract the select info out of sql string
 */
SQuerySQL *tSetQuerySQLElems(SSQLToken *pSelectToken, tSQLExprList *pSelection, SSQLToken *pFrom, tSQLExpr *pWhere,
                             tVariantList *pGroupby, tVariantList *pSortOrder, SSQLToken *pInterval,
                             SSQLToken *pSliding, tVariantList *pFill, SLimitVal *pLimit, SLimitVal *pGLimit) {
  assert(pSelection != NULL && pFrom != NULL && pInterval != NULL && pLimit != NULL && pGLimit != NULL);

  SQuerySQL *pQuery = calloc(1, sizeof(SQuerySQL));
  pQuery->selectToken = *pSelectToken;
  pQuery->selectToken.n = strlen(pQuery->selectToken.z);  // all later sql string are belonged to the stream sql

  pQuery->pSelection = pSelection;
  pQuery->from = *pFrom;
  pQuery->pGroupby = pGroupby;
  pQuery->pSortOrder = pSortOrder;
  pQuery->pWhere = pWhere;

  pQuery->limit = *pLimit;
  pQuery->glimit = *pGLimit;

  pQuery->interval = *pInterval;
  pQuery->sliding = *pSliding;
  pQuery->fillType = pFill;

  return pQuery;
}

tSQLExprListList *tSQLListListAppend(tSQLExprListList *pList, tSQLExprList *pExprList) {
  if (pList == NULL) pList = calloc(1, sizeof(tSQLExprListList));

  if (pList->nAlloc <= pList->nList) {  //
    pList->nAlloc = (pList->nAlloc << 1) + 4;
    pList->a = realloc(pList->a, pList->nAlloc * sizeof(pList->a[0]));
    if (pList->a == 0) {
      pList->nList = pList->nAlloc = 0;
      return pList;
    }
  }
  assert(pList->a != 0);

  if (pExprList) {
    pList->a[pList->nList++] = pExprList;
  }

  return pList;
}

void tSetInsertSQLElems(SSqlInfo *pInfo, SSQLToken *pName, tSQLExprListList *pList) {
  SInsertSQL *pInsert = calloc(1, sizeof(SInsertSQL));

  pInsert->name = *pName;
  pInsert->pValue = pList;

  pInfo->pInsertInfo = pInsert;
  pInfo->sqlType = TSQL_INSERT;
}

void destroyQuerySql(SQuerySQL *pSql) {
  if (pSql == NULL) return;

  tSQLExprListDestroy(pSql->pSelection);
  pSql->pSelection = NULL;

  tSQLExprDestroy(pSql->pWhere);
  pSql->pWhere = NULL;

  tVariantListDestroy(pSql->pSortOrder);
  pSql->pSortOrder = NULL;

  tVariantListDestroy(pSql->pGroupby);
  pSql->pGroupby = NULL;

  tVariantListDestroy(pSql->fillType);

  free(pSql);
}

SCreateTableSQL *tSetCreateSQLElems(tFieldList *pCols, tFieldList *pTags, SSQLToken *pMetricName,
                                    tVariantList *pTagVals, SQuerySQL *pSelect, int32_t type) {
  SCreateTableSQL *pCreate = calloc(1, sizeof(SCreateTableSQL));

  switch (type) {
    case TSQL_CREATE_NORMAL_METER: {
      pCreate->colInfo.pColumns = pCols;
      assert(pTagVals == NULL && pTags == NULL);
      break;
    }
    case TSQL_CREATE_NORMAL_METRIC: {
      pCreate->colInfo.pColumns = pCols;
      pCreate->colInfo.pTagColumns = pTags;
      assert(pTagVals == NULL && pTags != NULL && pCols != NULL);
      break;
    }
    case TSQL_CREATE_METER_FROM_METRIC: {
      pCreate->usingInfo.pTagVals = pTagVals;
      pCreate->usingInfo.metricName = *pMetricName;
      break;
    }
    case TSQL_CREATE_STREAM: {
      pCreate->pSelect = pSelect;
      break;
    }
    default:
      assert(false);
  }

  return pCreate;
}

SAlterTableSQL *tAlterTableSQLElems(SSQLToken *pMeterName, tFieldList *pCols, tVariantList *pVals, int32_t type) {
  SAlterTableSQL *pAlterTable = calloc(1, sizeof(SAlterTableSQL));
  pAlterTable->name = *pMeterName;

  if (type == ALTER_TABLE_ADD_COLUMN || type == ALTER_TABLE_TAGS_ADD) {
    pAlterTable->pAddColumns = pCols;
    assert(pVals == NULL);
  } else {
    /* ALTER_TABLE_TAGS_CHG, ALTER_TABLE_TAGS_SET, ALTER_TABLE_TAGS_DROP,
     * ALTER_TABLE_DROP_COLUMN */
    pAlterTable->varList = pVals;
    assert(pCols == NULL);
  }

  return pAlterTable;
}

void SQLInfoDestroy(SSqlInfo *pInfo) {
  if (pInfo == NULL) return;

  if (pInfo->sqlType == TSQL_QUERY_METER) {
    destroyQuerySql(pInfo->pQueryInfo);
  } else if (pInfo->sqlType >= TSQL_CREATE_NORMAL_METER && pInfo->sqlType <= TSQL_CREATE_STREAM) {
    SCreateTableSQL *pCreateTableInfo = pInfo->pCreateTableInfo;
    destroyQuerySql(pCreateTableInfo->pSelect);

    tFieldListDestroy(pCreateTableInfo->colInfo.pColumns);
    tFieldListDestroy(pCreateTableInfo->colInfo.pTagColumns);

    tVariantListDestroy(pCreateTableInfo->usingInfo.pTagVals);
    tfree(pInfo->pCreateTableInfo);
  } else if (pInfo->sqlType >= ALTER_TABLE_TAGS_ADD && pInfo->sqlType <= ALTER_TABLE_DROP_COLUMN) {
    tVariantListDestroy(pInfo->pAlterInfo->varList);
    tFieldListDestroy(pInfo->pAlterInfo->pAddColumns);
    tfree(pInfo->pAlterInfo);
  } else {
    if (pInfo->pDCLInfo != NULL && pInfo->pDCLInfo->nAlloc > 0) {
      free(pInfo->pDCLInfo->a);
    }

    if (pInfo->sqlType == CREATE_DATABASE) {
      tVariantListDestroy(pInfo->pDCLInfo->dbOpt.keep);
    }

    tfree(pInfo->pDCLInfo);
  }
}

void setSQLInfo(SSqlInfo *pInfo, void *pSqlExprInfo, SSQLToken *pMeterName, int32_t type) {
  pInfo->sqlType = type;
  pInfo->pCreateTableInfo = pSqlExprInfo;

  if (pMeterName != NULL) {
    pInfo->pCreateTableInfo->name = *pMeterName;
  }
}

void setCreatedMeterName(SSqlInfo *pInfo, SSQLToken *pMeterName, SSQLToken *pIfNotExists) {
  pInfo->pCreateTableInfo->name = *pMeterName;
  pInfo->pCreateTableInfo->existCheck = (pIfNotExists->n != 0);
}

void tTokenListBuyMoreSpace(tDCLSQL *pTokenList) {
  if (pTokenList->nAlloc <= pTokenList->nTokens) {  //
    pTokenList->nAlloc = (pTokenList->nAlloc << 1) + 4;
    pTokenList->a = realloc(pTokenList->a, pTokenList->nAlloc * sizeof(pTokenList->a[0]));
    if (pTokenList->a == 0) {
      pTokenList->nTokens = pTokenList->nAlloc = 0;
    }
  }
}

tDCLSQL *tTokenListAppend(tDCLSQL *pTokenList, SSQLToken *pToken) {
  if (pToken == NULL) return NULL;

  if (pTokenList == NULL) pTokenList = calloc(1, sizeof(tDCLSQL));

  tTokenListBuyMoreSpace(pTokenList);
  pTokenList->a[pTokenList->nTokens++] = *pToken;

  return pTokenList;
}

void setDCLSQLElems(SSqlInfo *pInfo, int32_t type, int32_t nParam, ...) {
  pInfo->sqlType = type;

  if (nParam == 0) return;
  if (pInfo->pDCLInfo == NULL) pInfo->pDCLInfo = calloc(1, sizeof(tDCLSQL));

  va_list va;
  va_start(va, nParam);

  while (nParam-- > 0) {
    SSQLToken *pToken = va_arg(va, SSQLToken *);
    tTokenListAppend(pInfo->pDCLInfo, pToken);
  }
  va_end(va);
}

void setCreateDBSQL(SSqlInfo *pInfo, int32_t type, SSQLToken *pToken, SCreateDBInfo *pDB, SSQLToken *pIgExists) {
  pInfo->sqlType = type;
  if (pInfo->pDCLInfo == NULL) {
    pInfo->pDCLInfo = calloc(1, sizeof(tDCLSQL));
  }

  pInfo->pDCLInfo->dbOpt = *pDB;
  pInfo->pDCLInfo->dbOpt.dbname = *pToken;

  tTokenListAppend(pInfo->pDCLInfo, pIgExists);
}

void setCreateAcctSQL(SSqlInfo *pInfo, int32_t type, SSQLToken *pName, SSQLToken *pPwd, SCreateAcctSQL *pAcctInfo) {
  pInfo->sqlType = type;
  if (pInfo->pDCLInfo == NULL) {
    pInfo->pDCLInfo = calloc(1, sizeof(tDCLSQL));
  }

  pInfo->pDCLInfo->acctOpt = *pAcctInfo;

  tTokenListAppend(pInfo->pDCLInfo, pName);

  if (pPwd->n > 0) {
    tTokenListAppend(pInfo->pDCLInfo, pPwd);
  }
}
