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
#include "qSqlparser.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "tcmdtype.h"
#include "tstoken.h"
#include "tstrbuild.h"
#include "ttokendef.h"
#include "tutil.h"

SSqlInfo qSQLParse(const char *pStr) {
  void *pParser = ParseAlloc(malloc);

  SSqlInfo sqlInfo = {0};
  sqlInfo.valid = true;

  int32_t i = 0;
  while (1) {
    SStrToken t0 = {0};

    if (pStr[i] == 0) {
      Parse(pParser, 0, t0, &sqlInfo);
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
        Parse(pParser, 0, t0, &sqlInfo);
        goto abort_parse;
      }
      
      case TK_QUESTION:
      case TK_ILLEGAL: {
        snprintf(sqlInfo.pzErrMsg, tListLen(sqlInfo.pzErrMsg), "unrecognized token: \"%s\"", t0.z);
        sqlInfo.valid = false;
        goto abort_parse;
      }
      default:
        Parse(pParser, t0.type, t0, &sqlInfo);
        if (sqlInfo.valid == false) {
          goto abort_parse;
        }
    }
  }

abort_parse:
  ParseFree(pParser, free);
  return sqlInfo;
}

tSQLExprList *tSQLExprListAppend(tSQLExprList *pList, tSQLExpr *pNode, SStrToken *pToken) {
  if (pList == NULL) {
    pList = calloc(1, sizeof(tSQLExprList));
  }

  if (pList->nAlloc <= pList->nExpr) {
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

tSQLExpr *tSQLExprIdValueCreate(SStrToken *pToken, int32_t optrType) {
  tSQLExpr *pSQLExpr = calloc(1, sizeof(tSQLExpr));

  if (pToken != NULL) {
    pSQLExpr->token = *pToken;
  }

  if (optrType == TK_INTEGER || optrType == TK_STRING || optrType == TK_FLOAT || optrType == TK_BOOL) {
    toTSDBType(pToken->type);

    tVariantCreate(&pSQLExpr->val, pToken);
    pSQLExpr->nSQLOptr = optrType;
  } else if (optrType == TK_NOW) {
    // use microsecond by default
    pSQLExpr->val.i64Key = taosGetTimestamp(TSDB_TIME_PRECISION_MICRO);
    pSQLExpr->val.nType = TSDB_DATA_TYPE_BIGINT;
    pSQLExpr->nSQLOptr = TK_TIMESTAMP;  // TK_TIMESTAMP used to denote the time value is in microsecond
  } else if (optrType == TK_VARIABLE) {
    int32_t ret = parseAbsoluteDuration(pToken->z, pToken->n, &pSQLExpr->val.i64Key);
    if (ret != TSDB_CODE_SUCCESS) {
      terrno = TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
    }

    pSQLExpr->val.nType = TSDB_DATA_TYPE_BIGINT;
    pSQLExpr->nSQLOptr = TK_TIMESTAMP;
  } else {  // it must be the column name (tk_id) if it is not the number
    assert(optrType == TK_ID || optrType == TK_ALL);
    if (pToken != NULL) {
      pSQLExpr->colInfo = *pToken;
    }

    pSQLExpr->nSQLOptr = optrType;
  }

  return pSQLExpr;
}

/*
 * pList is the parameters for function with id(optType)
 * function name is denoted by pFunctionToken
 */
tSQLExpr *tSQLExprCreateFunction(tSQLExprList *pList, SStrToken *pFuncToken, SStrToken *endToken, int32_t optType) {
  if (pFuncToken == NULL) return NULL;

  tSQLExpr *pExpr = calloc(1, sizeof(tSQLExpr));
  pExpr->nSQLOptr = optType;
  pExpr->pParam = pList;

  int32_t len = (int32_t)((endToken->z + endToken->n) - pFuncToken->z);
  pExpr->operand.z = pFuncToken->z;

  pExpr->operand.n = len;  // raw field name
  pExpr->operand.type = pFuncToken->type;

  pExpr->token = pExpr->operand;
  return pExpr;
}

/*
 * create binary expression in this procedure
 * if the expr is arithmetic, calculate the result and set it to tSQLExpr Object
 */
tSQLExpr *tSQLExprCreate(tSQLExpr *pLeft, tSQLExpr *pRight, int32_t optrType) {
  tSQLExpr *pExpr = calloc(1, sizeof(tSQLExpr));

  if (pLeft != NULL && pRight != NULL && (optrType != TK_IN)) {
    char* endPos = pRight->token.z + pRight->token.n;
    pExpr->token.z = pLeft->token.z;
    pExpr->token.n = (uint32_t)(endPos - pExpr->token.z);
    pExpr->token.type = pLeft->token.type;
  }

  if ((pLeft != NULL && pRight != NULL) &&
      (optrType == TK_PLUS || optrType == TK_MINUS || optrType == TK_STAR || optrType == TK_DIVIDE || optrType == TK_REM)) {
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

    } else if ((pLeft->nSQLOptr == TK_FLOAT && pRight->nSQLOptr == TK_INTEGER) || (pLeft->nSQLOptr == TK_INTEGER && pRight->nSQLOptr == TK_FLOAT) ||
        (pLeft->nSQLOptr == TK_FLOAT && pRight->nSQLOptr == TK_FLOAT)) {
      pExpr->val.nType = TSDB_DATA_TYPE_DOUBLE;
      pExpr->nSQLOptr  = TK_FLOAT;

      double left  = (pLeft->val.nType == TSDB_DATA_TYPE_DOUBLE) ? pLeft->val.dKey : pLeft->val.i64Key;
      double right = (pRight->val.nType == TSDB_DATA_TYPE_DOUBLE) ? pRight->val.dKey : pRight->val.i64Key;

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

    if (pRight == NULL) {
      pRight = calloc(1, sizeof(tSQLExpr));
    }

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

SArray *tVariantListAppendToken(SArray *pList, SStrToken *pToken, uint8_t order) {
  if (pList == NULL) {
    pList = taosArrayInit(4, sizeof(tVariantListItem));
  }

  if (pToken) {
    tVariantListItem item;
    tVariantCreate(&item.pVar, pToken);
    item.sortOrder = order;

    taosArrayPush(pList, &item);
  }

  return pList;
}

SArray *tVariantListAppend(SArray *pList, tVariant *pVar, uint8_t sortOrder) {
  if (pList == NULL) {
    pList = taosArrayInit(4, sizeof(tVariantListItem));
  }

  if (pVar == NULL) {
    return pList;
  }

  /*
   * Here we do not employ the assign function, since we need the pz attribute of structure
   * , which is the point to char string, to free it!
   *
   * Otherwise, the original pointer may be lost, which causes memory leak.
   */
  tVariantListItem item;
  item.pVar = *pVar;
  item.sortOrder = sortOrder;

  taosArrayPush(pList, &item);
  return pList;
}

SArray *tVariantListInsert(SArray *pList, tVariant *pVar, uint8_t sortOrder, int32_t index) {
  if (pList == NULL || pVar == NULL || index >= taosArrayGetSize(pList)) {
    return tVariantListAppend(NULL, pVar, sortOrder);
  }

  tVariantListItem item;

  item.pVar = *pVar;
  item.sortOrder = sortOrder;

  taosArrayInsert(pList, index, &item);
  return pList;
}

void setDBName(SStrToken *pCpxName, SStrToken *pDB) {
  pCpxName->type = pDB->type;
  pCpxName->z = pDB->z;
  pCpxName->n = pDB->n;
}

void tSQLSetColumnInfo(TAOS_FIELD *pField, SStrToken *pName, TAOS_FIELD *pType) {
  int32_t maxLen = sizeof(pField->name) / sizeof(pField->name[0]);
  
  // truncate the column name
  if ((int32_t)pName->n >= maxLen) {
    pName->n = maxLen - 1;
  }

  strncpy(pField->name, pName->z, pName->n);
  pField->name[pName->n] = 0;

  pField->type = pType->type;
  pField->bytes = pType->bytes;
}

void tSQLSetColumnType(TAOS_FIELD *pField, SStrToken *type) {
  pField->type = -1;

  for (int32_t i = 0; i < tListLen(tDataTypeDesc); ++i) {
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
        if (type->type == 0) {
          pField->bytes = 0;
        } else {
          int32_t bytes = -(int32_t)(type->type);
          if (bytes > (TSDB_MAX_NCHAR_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE) {
            // we have to postpone reporting the error because it cannot be done here
            // as pField->bytes is int16_t, use 'TSDB_MAX_NCHAR_LEN + 1' to avoid overflow
            bytes = TSDB_MAX_NCHAR_LEN + 1;
          } else {
            bytes = bytes * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE;
          }
          pField->bytes = (int16_t)bytes;
        }
      } else if (i == TSDB_DATA_TYPE_BINARY) {
        /* for binary, the TOKENTYPE is the length of binary */
        if (type->type == 0) {
          pField->bytes = 0;
        } else {
          int32_t bytes = -(int32_t)(type->type);
          if (bytes > TSDB_MAX_BINARY_LEN - VARSTR_HEADER_SIZE) {
            // refer comment for NCHAR above
            bytes = TSDB_MAX_BINARY_LEN + 1;
          } else {
            bytes += VARSTR_HEADER_SIZE;
          }
          pField->bytes = (int16_t)bytes;
        } 
      }
      break;
    }
  }
}

/*
 * extract the select info out of sql string
 */
SQuerySQL *tSetQuerySQLElems(SStrToken *pSelectToken, tSQLExprList *pSelection, SArray *pFrom, tSQLExpr *pWhere,
                             SArray *pGroupby, SArray *pSortOrder, SIntervalVal *pInterval,
                             SStrToken *pSliding, SArray *pFill, SLimitVal *pLimit, SLimitVal *pGLimit) {
  assert(pSelection != NULL);

  SQuerySQL *pQuery = calloc(1, sizeof(SQuerySQL));
  pQuery->selectToken = *pSelectToken;
  pQuery->selectToken.n = (uint32_t)strlen(pQuery->selectToken.z);  // all later sql string are belonged to the stream sql

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
    pQuery->interval = pInterval->interval;
    pQuery->offset = pInterval->offset;
  }

  if (pSliding != NULL) {
    pQuery->sliding = *pSliding;
  }

  pQuery->fillType = pFill;
  return pQuery;
}

static void freeVariant(void *pItem) {
  tVariantListItem* p = (tVariantListItem*) pItem;
  tVariantDestroy(&p->pVar);
}

void freeCreateTableInfo(void* p) {
  SCreatedTableInfo* pInfo = (SCreatedTableInfo*) p;
  taosArrayDestroyEx(pInfo->pTagVals, freeVariant);
  tfree(pInfo->fullname);
  tfree(pInfo->tagdata.data);
}

void doDestroyQuerySql(SQuerySQL *pQuerySql) {
  if (pQuerySql == NULL) {
    return;
  }
  
  tSQLExprListDestroy(pQuerySql->pSelection);
  
  pQuerySql->pSelection = NULL;
  
  tSQLExprDestroy(pQuerySql->pWhere);
  pQuerySql->pWhere = NULL;
  
  taosArrayDestroyEx(pQuerySql->pSortOrder, freeVariant);
  pQuerySql->pSortOrder = NULL;

  taosArrayDestroyEx(pQuerySql->pGroupby, freeVariant);
  pQuerySql->pGroupby = NULL;

  taosArrayDestroyEx(pQuerySql->from, freeVariant);
  pQuerySql->from = NULL;

  taosArrayDestroyEx(pQuerySql->fillType, freeVariant);
  pQuerySql->fillType = NULL;

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

SCreateTableSQL *tSetCreateSQLElems(SArray *pCols, SArray *pTags, SQuerySQL *pSelect, int32_t type) {
  SCreateTableSQL *pCreate = calloc(1, sizeof(SCreateTableSQL));

  switch (type) {
    case TSQL_CREATE_TABLE: {
      pCreate->colInfo.pColumns = pCols;
      assert(pTags == NULL);
      break;
    }
    case TSQL_CREATE_STABLE: {
      pCreate->colInfo.pColumns = pCols;
      pCreate->colInfo.pTagColumns = pTags;
      assert(pTags != NULL && pCols != NULL);
      break;
    }
    case TSQL_CREATE_STREAM: {
      pCreate->pSelect = pSelect;
      break;
    }

    case TSQL_CREATE_TABLE_FROM_STABLE: {
      assert(0);
    }

    default:
      assert(false);
  }

  pCreate->type = type;
  return pCreate;
}

SCreatedTableInfo createNewChildTableInfo(SStrToken *pTableName, SArray *pTagVals, SStrToken *pToken, SStrToken* igExists) {
  SCreatedTableInfo info;
  memset(&info, 0, sizeof(SCreatedTableInfo));

  info.name       = *pToken;
  info.pTagVals   = pTagVals;
  info.stableName = *pTableName;
  info.igExist    = (igExists->n > 0)? 1:0;

  return info;
}

SAlterTableSQL *tAlterTableSQLElems(SStrToken *pTableName, SArray *pCols, SArray *pVals, int32_t type) {
  SAlterTableSQL *pAlterTable = calloc(1, sizeof(SAlterTableSQL));
  
  pAlterTable->name = *pTableName;
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

void* destroyCreateTableSQL(SCreateTableSQL* pCreate) {
  doDestroyQuerySql(pCreate->pSelect);

  taosArrayDestroy(pCreate->colInfo.pColumns);
  taosArrayDestroy(pCreate->colInfo.pTagColumns);

  taosArrayDestroyEx(pCreate->childTableInfo, freeCreateTableInfo);
  tfree(pCreate);

  return NULL;
}

void SQLInfoDestroy(SSqlInfo *pInfo) {
  if (pInfo == NULL) return;

  if (pInfo->type == TSDB_SQL_SELECT) {
    destroyAllSelectClause(&pInfo->subclauseInfo);
  } else if (pInfo->type == TSDB_SQL_CREATE_TABLE) {
    pInfo->pCreateTableInfo = destroyCreateTableSQL(pInfo->pCreateTableInfo);
  } else if (pInfo->type == TSDB_SQL_ALTER_TABLE) {
    taosArrayDestroyEx(pInfo->pAlterInfo->varList, freeVariant);
    taosArrayDestroy(pInfo->pAlterInfo->pAddColumns);
    tfree(pInfo->pAlterInfo->tagData.data);
    tfree(pInfo->pAlterInfo);
  } else {
    if (pInfo->pDCLInfo != NULL && pInfo->pDCLInfo->nAlloc > 0) {
      free(pInfo->pDCLInfo->a);
    }

    if (pInfo->pDCLInfo != NULL && pInfo->type == TSDB_SQL_CREATE_DB) {
      taosArrayDestroyEx(pInfo->pDCLInfo->dbOpt.keep, freeVariant);
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

SSqlInfo* setSQLInfo(SSqlInfo *pInfo, void *pSqlExprInfo, SStrToken *pTableName, int32_t type) {
  pInfo->type = type;
  
  if (type == TSDB_SQL_SELECT) {
    pInfo->subclauseInfo = *(SSubclauseInfo*) pSqlExprInfo;
    free(pSqlExprInfo);
  } else {
    pInfo->pCreateTableInfo = pSqlExprInfo;
  }
  
  if (pTableName != NULL) {
    pInfo->pCreateTableInfo->name = *pTableName;
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

void setCreatedTableName(SSqlInfo *pInfo, SStrToken *pTableNameToken, SStrToken *pIfNotExists) {
  pInfo->pCreateTableInfo->name = *pTableNameToken;
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

tDCLSQL *tTokenListAppend(tDCLSQL *pTokenList, SStrToken *pToken) {
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
    SStrToken *pToken = va_arg(va, SStrToken *);
    pInfo->pDCLInfo = tTokenListAppend(pInfo->pDCLInfo, pToken);
  }
  va_end(va);
}

void setDropDBTableInfo(SSqlInfo *pInfo, int32_t type, SStrToken* pToken, SStrToken* existsCheck) {
  pInfo->type = type;
  pInfo->pDCLInfo = tTokenListAppend(pInfo->pDCLInfo, pToken);
  pInfo->pDCLInfo->existsCheck = (existsCheck->n == 1);
}

void setShowOptions(SSqlInfo *pInfo, int32_t type, SStrToken* prefix, SStrToken* pPatterns) {
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

void setCreateDBSQL(SSqlInfo *pInfo, int32_t type, SStrToken *pToken, SCreateDBInfo *pDB, SStrToken *pIgExists) {
  pInfo->type = type;
  if (pInfo->pDCLInfo == NULL) {
    pInfo->pDCLInfo = calloc(1, sizeof(tDCLSQL));
  }

  pInfo->pDCLInfo->dbOpt = *pDB;
  pInfo->pDCLInfo->dbOpt.dbname = *pToken;
  pInfo->pDCLInfo->dbOpt.ignoreExists = pIgExists->n; // sql.y has: ifnotexists(X) ::= IF NOT EXISTS.   {X.n = 1;}
}

void setCreateAcctSQL(SSqlInfo *pInfo, int32_t type, SStrToken *pName, SStrToken *pPwd, SCreateAcctSQL *pAcctInfo) {
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

void setCreateUserSQL(SSqlInfo *pInfo, SStrToken *pName, SStrToken *pPasswd) {
  pInfo->type = TSDB_SQL_CREATE_USER;
  if (pInfo->pDCLInfo == NULL) {
    pInfo->pDCLInfo = calloc(1, sizeof(tDCLSQL));
  }
  
  assert(pName != NULL && pPasswd != NULL);
  
  pInfo->pDCLInfo->user.user = *pName;
  pInfo->pDCLInfo->user.passwd = *pPasswd;
}

void setAlterUserSQL(SSqlInfo *pInfo, int16_t type, SStrToken *pName, SStrToken* pPwd, SStrToken *pPrivilege) {
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

void setKillSQL(SSqlInfo *pInfo, int32_t type, SStrToken *ip) {
  pInfo->type = type;
  if (pInfo->pDCLInfo == NULL) {
    pInfo->pDCLInfo = calloc(1, sizeof(tDCLSQL));
  }
  
  assert(ip != NULL);
  
  pInfo->pDCLInfo->ip = *ip;
}

void setDefaultCreateDbOption(SCreateDBInfo *pDBInfo) {
  pDBInfo->compressionLevel = -1;

  pDBInfo->walLevel = -1;
  pDBInfo->fsyncPeriod = -1;
  pDBInfo->commitTime = -1;
  pDBInfo->maxTablesPerVnode = -1;

  pDBInfo->cacheBlockSize = -1;
  pDBInfo->numOfBlocks = -1;
  pDBInfo->maxRowsPerBlock = -1;
  pDBInfo->minRowsPerBlock = -1;
  pDBInfo->daysPerFile = -1;

  pDBInfo->replica = -1;
  pDBInfo->quorum = -1;
  pDBInfo->keep = NULL;

  pDBInfo->update = -1;
  memset(&pDBInfo->precision, 0, sizeof(SStrToken));
}
