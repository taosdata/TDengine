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

#include "os.h"
#include "qsqlparser.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "tglobal.h"
#include "tstoken.h"
#include "ttime.h"
#include "ttokendef.h"
#include "tutil.h"
#include "qsqltype.h"
#include "tstrbuild.h"
#include "queryLog.h"

int32_t tSQLParse(SSqlInfo *pSQLInfo, const char *pStr) {
  void *pParser = ParseAlloc(malloc);
  pSQLInfo->valid = true;

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
        snprintf(pSQLInfo->pzErrMsg, tListLen(pSQLInfo->pzErrMsg), "unrecognized token: \"%s\"", t0.z);
        pSQLInfo->valid = false;
        goto abort_parse;
      }
      default:
        Parse(pParser, t0.type, t0, pSQLInfo);
        if (pSQLInfo->valid == false) {
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

tSQLExpr *tSQLExprIdValueCreate(SSQLToken *pAliasToken, int32_t optrType) {
  tSQLExpr *nodePtr = calloc(1, sizeof(tSQLExpr));

  if (optrType == TK_INTEGER || optrType == TK_STRING || optrType == TK_FLOAT || optrType == TK_BOOL) {
    toTSDBType(pAliasToken->type);

    tVariantCreate(&nodePtr->val, pAliasToken);
    nodePtr->nSQLOptr = optrType;
  } else if (optrType == TK_NOW) {
    // default use microsecond
    nodePtr->val.i64Key = taosGetTimestamp(TSDB_TIME_PRECISION_MICRO);
    nodePtr->val.nType = TSDB_DATA_TYPE_BIGINT;
    nodePtr->nSQLOptr = TK_TIMESTAMP;  // TK_TIMESTAMP used to denote the time value is in microsecond
  } else if (optrType == TK_VARIABLE) {
    int32_t ret = getTimestampInUsFromStr(pAliasToken->z, pAliasToken->n, &nodePtr->val.i64Key);
    UNUSED(ret);

    nodePtr->val.nType = TSDB_DATA_TYPE_BIGINT;
    nodePtr->nSQLOptr = TK_TIMESTAMP;
  } else {  // it must be the column name (tk_id) if it is not the number
    assert(optrType == TK_ID || optrType == TK_ALL);
    if (pAliasToken != NULL) {
      nodePtr->colInfo = *pAliasToken;
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

void tSQLExprNodeDestroy(tSQLExpr *pExpr) {
  if (pExpr == NULL) {
    return;
  }

  if (pExpr->nSQLOptr == TK_STRING) {
    tVariantDestroy(&pExpr->val);
  }

  tSQLExprListDestroy(pExpr->pParam);

  free(pExpr);
}

void tSQLExprDestroy(tSQLExpr *pExpr) {
  if (pExpr == NULL) {
    return;
  }

  tSQLExprDestroy(pExpr->pLeft);
  tSQLExprDestroy(pExpr->pRight);

  tSQLExprNodeDestroy(pExpr);
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

int32_t getTimestampInUsFromStrImpl(int64_t val, char unit, int64_t *result) {
  *result = val;

  switch (unit) {
    case 's':
      (*result) *= MILLISECOND_PER_SECOND;
      break;
    case 'm':
      (*result) *= MILLISECOND_PER_MINUTE;
      break;
    case 'h':
      (*result) *= MILLISECOND_PER_HOUR;
      break;
    case 'd':
      (*result) *= MILLISECOND_PER_DAY;
      break;
    case 'w':
      (*result) *= MILLISECOND_PER_WEEK;
      break;
    case 'n':
      (*result) *= MILLISECOND_PER_MONTH;
      break;
    case 'y':
      (*result) *= MILLISECOND_PER_YEAR;
      break;
    case 'a':
      break;
    default: {
      ;
      return -1;
    }
  }

  /* get the value in microsecond */
  (*result) *= 1000L;
  return 0;
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
SQuerySQL *tSetQuerySQLElems(SSQLToken *pSelectToken, tSQLExprList *pSelection, tVariantList *pFrom, tSQLExpr *pWhere,
                             tVariantList *pGroupby, tVariantList *pSortOrder, SSQLToken *pInterval,
                             SSQLToken *pSliding, tVariantList *pFill, SLimitVal *pLimit, SLimitVal *pGLimit) {
  assert(pSelection != NULL);

  SQuerySQL *pQuery = calloc(1, sizeof(SQuerySQL));
  pQuery->selectToken = *pSelectToken;
  pQuery->selectToken.n = strlen(pQuery->selectToken.z);  // all later sql string are belonged to the stream sql

  pQuery->pSelection = pSelection;
  pQuery->from = pFrom;
  pQuery->pGroupby = pGroupby;
  pQuery->pSortOrder = pSortOrder;
  pQuery->pWhere = pWhere;

  if (pLimit != NULL) {
    pQuery->limit = *pLimit;
  }

  if (pGLimit != NULL) {
    pQuery->slimit = *pGLimit;
  }

  if (pInterval != NULL) {
    pQuery->interval = *pInterval;
  }

  if (pSliding != NULL) {
    pQuery->sliding = *pSliding;
  }

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

void doDestroyQuerySql(SQuerySQL *pQuerySql) {
  if (pQuerySql == NULL) {
    return;
  }
  
  tSQLExprListDestroy(pQuerySql->pSelection);
  
  pQuerySql->pSelection = NULL;
  
  tSQLExprDestroy(pQuerySql->pWhere);
  pQuerySql->pWhere = NULL;
  
  tVariantListDestroy(pQuerySql->pSortOrder);
  pQuerySql->pSortOrder = NULL;
  
  tVariantListDestroy(pQuerySql->pGroupby);
  pQuerySql->pGroupby = NULL;
  
  tVariantListDestroy(pQuerySql->from);
  pQuerySql->from = NULL;
  
  tVariantListDestroy(pQuerySql->fillType);
  
  free(pQuerySql);
}

void destroyAllSelectClause(SSubclauseInfo *pClause) {
  if (pClause == NULL || pClause->numOfClause == 0) {
    return;
  }

  for(int32_t i = 0; i < pClause->numOfClause; ++i) {
    SQuerySQL *pQuerySql = pClause->pClause[i];
    doDestroyQuerySql(pQuerySql);
  }
  
  tfree(pClause->pClause);
}

SCreateTableSQL *tSetCreateSQLElems(tFieldList *pCols, tFieldList *pTags, SSQLToken *pStableName,
                                    tVariantList *pTagVals, SQuerySQL *pSelect, int32_t type) {
  SCreateTableSQL *pCreate = calloc(1, sizeof(SCreateTableSQL));

  switch (type) {
    case TSQL_CREATE_TABLE: {
      pCreate->colInfo.pColumns = pCols;
      assert(pTagVals == NULL && pTags == NULL);
      break;
    }
    case TSQL_CREATE_STABLE: {
      pCreate->colInfo.pColumns = pCols;
      pCreate->colInfo.pTagColumns = pTags;
      assert(pTagVals == NULL && pTags != NULL && pCols != NULL);
      break;
    }
    case TSQL_CREATE_TABLE_FROM_STABLE: {
      pCreate->usingInfo.pTagVals = pTagVals;
      pCreate->usingInfo.stableName = *pStableName;
      break;
    }
    case TSQL_CREATE_STREAM: {
      pCreate->pSelect = pSelect;
      break;
    }
    default:
      assert(false);
  }

  pCreate->type = type;
  return pCreate;
}

SAlterTableSQL *tAlterTableSQLElems(SSQLToken *pMeterName, tFieldList *pCols, tVariantList *pVals, int32_t type) {
  SAlterTableSQL *pAlterTable = calloc(1, sizeof(SAlterTableSQL));
  
  pAlterTable->name = *pMeterName;
  pAlterTable->type = type;

  if (type == TSDB_ALTER_TABLE_ADD_COLUMN || type == TSDB_ALTER_TABLE_ADD_TAG_COLUMN) {
    pAlterTable->pAddColumns = pCols;
    assert(pVals == NULL);
  } else {
    /*
     * ALTER_TABLE_TAGS_CHG, ALTER_TABLE_TAGS_SET, ALTER_TABLE_TAGS_DROP,
     * ALTER_TABLE_DROP_COLUMN
     */
    pAlterTable->varList = pVals;
    assert(pCols == NULL);
  }

  return pAlterTable;
}

void SQLInfoDestroy(SSqlInfo *pInfo) {
  if (pInfo == NULL) return;

  if (pInfo->type == TSDB_SQL_SELECT) {
    destroyAllSelectClause(&pInfo->subclauseInfo);
  } else if (pInfo->type == TSDB_SQL_CREATE_TABLE) {
    SCreateTableSQL *pCreateTableInfo = pInfo->pCreateTableInfo;
    doDestroyQuerySql(pCreateTableInfo->pSelect);

    tFieldListDestroy(pCreateTableInfo->colInfo.pColumns);
    tFieldListDestroy(pCreateTableInfo->colInfo.pTagColumns);

    tVariantListDestroy(pCreateTableInfo->usingInfo.pTagVals);
    tfree(pInfo->pCreateTableInfo);
  } else if (pInfo->type == TSDB_SQL_ALTER_TABLE) {
    tVariantListDestroy(pInfo->pAlterInfo->varList);
    tFieldListDestroy(pInfo->pAlterInfo->pAddColumns);
    
    tfree(pInfo->pAlterInfo);
  } else {
    if (pInfo->pDCLInfo != NULL && pInfo->pDCLInfo->nAlloc > 0) {
      free(pInfo->pDCLInfo->a);
    }

    if (pInfo->type == TSDB_SQL_CREATE_DB) {
      tVariantListDestroy(pInfo->pDCLInfo->dbOpt.keep);
    }

    tfree(pInfo->pDCLInfo);
  }
}

SSubclauseInfo* setSubclause(SSubclauseInfo* pSubclause, void *pSqlExprInfo) {
  if (pSubclause == NULL) {
    pSubclause = calloc(1, sizeof(SSubclauseInfo));
  }
  
  int32_t newSize = pSubclause->numOfClause + 1;
  char* tmp = realloc(pSubclause->pClause, newSize * POINTER_BYTES);
  if (tmp == NULL) {
    return pSubclause;
  }
  
  pSubclause->pClause = (SQuerySQL**) tmp;
  
  pSubclause->pClause[newSize - 1] = pSqlExprInfo;
  pSubclause->numOfClause++;
  
  return pSubclause;
}

SSqlInfo* setSQLInfo(SSqlInfo *pInfo, void *pSqlExprInfo, SSQLToken *pMeterName, int32_t type) {
  pInfo->type = type;
  
  if (type == TSDB_SQL_SELECT) {
    pInfo->subclauseInfo = *(SSubclauseInfo*) pSqlExprInfo;
    free(pSqlExprInfo);
  } else {
    pInfo->pCreateTableInfo = pSqlExprInfo;
  }
  
  if (pMeterName != NULL) {
    pInfo->pCreateTableInfo->name = *pMeterName;
  }
  
  return pInfo;
}

SSubclauseInfo* appendSelectClause(SSubclauseInfo *pQueryInfo, void *pSubclause) {
  char* tmp = realloc(pQueryInfo->pClause, (pQueryInfo->numOfClause + 1) * POINTER_BYTES);
  if (tmp == NULL) {  // out of memory
    return pQueryInfo;
  }
  
  pQueryInfo->pClause = (SQuerySQL**) tmp;
  pQueryInfo->pClause[pQueryInfo->numOfClause++] = pSubclause;
  
  return pQueryInfo;
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
  pInfo->type = type;

  if (nParam == 0) return;
  if (pInfo->pDCLInfo == NULL) pInfo->pDCLInfo = (tDCLSQL *)calloc(1, sizeof(tDCLSQL));

  va_list va;
  va_start(va, nParam);

  while (nParam-- > 0) {
    SSQLToken *pToken = va_arg(va, SSQLToken *);
    (void)tTokenListAppend(pInfo->pDCLInfo, pToken);
  }
  va_end(va);
}

void setDropDBTableInfo(SSqlInfo *pInfo, int32_t type, SSQLToken* pToken, SSQLToken* existsCheck) {
  pInfo->type = type;
  
  if (pInfo->pDCLInfo == NULL) {
    pInfo->pDCLInfo = calloc(1, sizeof(tDCLSQL));
  }
  
  tTokenListAppend(pInfo->pDCLInfo, pToken);
  pInfo->pDCLInfo->existsCheck = (existsCheck->n == 1);
}

void setShowOptions(SSqlInfo *pInfo, int32_t type, SSQLToken* prefix, SSQLToken* pPatterns) {
  if (pInfo->pDCLInfo == NULL) {
    pInfo->pDCLInfo = calloc(1, sizeof(tDCLSQL));
  }
  
  pInfo->type = TSDB_SQL_SHOW;
  
  SShowInfo* pShowInfo = &pInfo->pDCLInfo->showOpt;
  pShowInfo->showType = type;
  
  if (prefix != NULL && prefix->type != 0) {
    pShowInfo->prefix = *prefix;
  } else {
    pShowInfo->prefix.type = 0;
  }
  
  if (pPatterns != NULL && pPatterns->type != 0) {
    pShowInfo->pattern = *pPatterns;
  } else {
    pShowInfo->pattern.type = 0;
  }
}

void setCreateDBSQL(SSqlInfo *pInfo, int32_t type, SSQLToken *pToken, SCreateDBInfo *pDB, SSQLToken *pIgExists) {
  pInfo->type = type;
  if (pInfo->pDCLInfo == NULL) {
    pInfo->pDCLInfo = calloc(1, sizeof(tDCLSQL));
  }

  pInfo->pDCLInfo->dbOpt = *pDB;
  pInfo->pDCLInfo->dbOpt.dbname = *pToken;

  tTokenListAppend(pInfo->pDCLInfo, pIgExists);
}

void setCreateAcctSQL(SSqlInfo *pInfo, int32_t type, SSQLToken *pName, SSQLToken *pPwd, SCreateAcctSQL *pAcctInfo) {
  pInfo->type = type;
  if (pInfo->pDCLInfo == NULL) {
    pInfo->pDCLInfo = calloc(1, sizeof(tDCLSQL));
  }

  pInfo->pDCLInfo->acctOpt = *pAcctInfo;
  
  assert(pName != NULL);
  pInfo->pDCLInfo->user.user = *pName;
  
  if (pPwd != NULL) {
    pInfo->pDCLInfo->user.passwd = *pPwd;
  }
}

void setCreateUserSQL(SSqlInfo *pInfo, SSQLToken *pName, SSQLToken *pPasswd) {
  pInfo->type = TSDB_SQL_CREATE_USER;
  if (pInfo->pDCLInfo == NULL) {
    pInfo->pDCLInfo = calloc(1, sizeof(tDCLSQL));
  }
  
  assert(pName != NULL && pPasswd != NULL);
  
  pInfo->pDCLInfo->user.user = *pName;
  pInfo->pDCLInfo->user.passwd = *pPasswd;
}

void setAlterUserSQL(SSqlInfo *pInfo, int16_t type, SSQLToken *pName, SSQLToken* pPwd, SSQLToken *pPrivilege) {
  pInfo->type = TSDB_SQL_ALTER_USER;
  if (pInfo->pDCLInfo == NULL) {
    pInfo->pDCLInfo = calloc(1, sizeof(tDCLSQL));
  }
  
  assert(pName != NULL);
  
  SUserInfo* pUser = &pInfo->pDCLInfo->user;
  pUser->type = type;
  pUser->user = *pName;
  
  if (pPwd != NULL) {
    pUser->passwd = *pPwd;
  } else {
    pUser->passwd.type = TSDB_DATA_TYPE_NULL;
  }
  
  if (pPrivilege != NULL) {
    pUser->privilege = *pPrivilege;
  } else {
    pUser->privilege.type = TSDB_DATA_TYPE_NULL;
  }
}

void setKillSQL(SSqlInfo *pInfo, int32_t type, SSQLToken *ip) {
  pInfo->type = type;
  if (pInfo->pDCLInfo == NULL) {
    pInfo->pDCLInfo = calloc(1, sizeof(tDCLSQL));
  }
  
  assert(ip != NULL);
  
  pInfo->pDCLInfo->ip = *ip;
}

void setDefaultCreateDbOption(SCreateDBInfo *pDBInfo) {
  pDBInfo->numOfBlocksPerTable = 50;
  pDBInfo->compressionLevel = -1;

  pDBInfo->commitLog = -1;
  pDBInfo->commitTime = -1;
  pDBInfo->tablesPerVnode = -1;
  pDBInfo->numOfAvgCacheBlocks = -1;

  pDBInfo->cacheBlockSize = -1;
  pDBInfo->rowPerFileBlock = -1;
  pDBInfo->daysPerFile = -1;

  pDBInfo->replica = -1;
  pDBInfo->keep = NULL;

  memset(&pDBInfo->precision, 0, sizeof(SSQLToken));
}