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

#include "astGenerator.h"
#include "../../../../include/client/taos.h"
#include "os.h"

int32_t tStrToInteger(const char* z, int16_t type, int32_t n, int64_t* value, bool issigned) {
  errno = 0;
  int32_t ret = 0;

  char* endPtr = NULL;
  if (type == TK_FLOAT) {
    double v = strtod(z, &endPtr);
    if ((errno == ERANGE && v == HUGE_VALF) || isinf(v) || isnan(v)) {
      ret = -1;
    } else if ((issigned && (v < INT64_MIN || v > INT64_MAX)) || ((!issigned) && (v < 0 || v > UINT64_MAX))) {
      ret = -1;
    } else {
      *value = (int64_t) round(v);
    }

    errno = 0;
    return ret;
  }

  int32_t radix = 10;
  if (type == TK_HEX) {
    radix = 16;
  } else if (type == TK_BIN) {
    radix = 2;
  }

  // the string may be overflow according to errno
  if (!issigned) {
    const char *p = z;
    while(*p != 0 && *p == ' ') p++;
    if (*p != 0 && *p == '-') { return -1;}

    *value = strtoull(z, &endPtr, radix);
  } else {
    *value = strtoll(z, &endPtr, radix);
  }

  // not a valid integer number, return error
  if (endPtr - z != n || errno == ERANGE) {
    ret = -1;
  }

  errno = 0;
  return ret;
}

SArray *tListItemAppend(SArray *pList, SVariant *pVar, uint8_t sortOrder) {
  if (pList == NULL) {
    pList = taosArrayInit(4, sizeof(SListItem));
  }

  if (pVar == NULL) {
    return pList;
  }

  /*
   * Here we do not employ the assign function, since we need the pz attribute of structure , which is the point to char string.
   * Otherwise, the original pointer may be lost, which causes memory leak.
   */
  SListItem item;
  item.pVar      = *pVar;
  item.sortOrder = sortOrder;

  taosArrayPush(pList, &item);
  return pList;
}

SArray *tListItemInsert(SArray *pList, SVariant *pVar, uint8_t sortOrder, int32_t index) {
  if (pList == NULL || pVar == NULL || index >= taosArrayGetSize(pList)) {
    return tListItemAppend(pList, pVar, sortOrder);
  }

  SListItem item;
  item.pVar      = *pVar;
  item.sortOrder = sortOrder;

  taosArrayInsert(pList, index, &item);
  return pList;
}

SArray *tListItemAppendToken(SArray *pList, SToken *pAliasToken, uint8_t sortOrder) {
  if (pList == NULL) {
    pList = taosArrayInit(4, sizeof(SListItem));
  }

  if (pAliasToken) {
    SListItem item;
    taosVariantCreate(&item.pVar, pAliasToken);
    item.sortOrder = sortOrder;

    taosArrayPush(pList, &item);
  }

  return pList;
}

SRelationInfo *setTableNameList(SRelationInfo *pRelationInfo, SToken *pName, SToken *pAlias) {
  if (pRelationInfo == NULL) {
    pRelationInfo = calloc(1, sizeof(SRelationInfo));
    pRelationInfo->list = taosArrayInit(4, sizeof(SRelElementPair));
  }

  pRelationInfo->type = SQL_NODE_FROM_TABLELIST;
  SRelElementPair p = {.tableName = *pName};
  if (pAlias != NULL) {
    p.aliasName = *pAlias;
  } else {
    TPARSER_SET_NONE_TOKEN(p.aliasName);
  }

  taosArrayPush(pRelationInfo->list, &p);
  return pRelationInfo;
}

void *destroyRelationInfo(SRelationInfo *pRelationInfo) {
  if (pRelationInfo == NULL) {
    return NULL;
  }

  if (pRelationInfo->type == SQL_NODE_FROM_TABLELIST) {
    taosArrayDestroy(pRelationInfo->list);
  } else {
    size_t size = taosArrayGetSize(pRelationInfo->list);
    for(int32_t i = 0; i < size; ++i) {
      SArray* pa = taosArrayGetP(pRelationInfo->list, i);
      destroyAllSqlNode(pa);
    }
    taosArrayDestroy(pRelationInfo->list);
  }

  tfree(pRelationInfo);
  return NULL;
}

SRelationInfo *addSubquery(SRelationInfo *pRelationInfo, SArray *pSub, SToken *pAlias) {
  if (pRelationInfo == NULL) {
    pRelationInfo = calloc(1, sizeof(SRelationInfo));
    pRelationInfo->list = taosArrayInit(4, sizeof(SRelElementPair));
  }

  pRelationInfo->type = SQL_NODE_FROM_SUBQUERY;

  SRelElementPair p = {.pSubquery = pSub};
  if (pAlias != NULL) {
    p.aliasName = *pAlias;
  } else {
    TPARSER_SET_NONE_TOKEN(p.aliasName);
  }

  taosArrayPush(pRelationInfo->list, &p);
  return pRelationInfo;
}

// sql expr leaf node
// todo Evalute the value during the validation process of AST.
tSqlExpr *tSqlExprCreateIdValue(SToken *pToken, int32_t optrType) {
  tSqlExpr *pSqlExpr = calloc(1, sizeof(tSqlExpr));

  if (pToken != NULL) {
    pSqlExpr->exprToken = *pToken;
  }

  if (optrType == TK_NULL) {
//    if (pToken) {
//      pToken->type = TSDB_DATA_TYPE_NULL;
//      tVariantCreate(&pSqlExpr->value, pToken);
//    }
    pSqlExpr->tokenId = optrType;
    pSqlExpr->type = SQL_NODE_VALUE;
  } else if (optrType == TK_INTEGER || optrType == TK_STRING || optrType == TK_FLOAT || optrType == TK_BOOL) {
//    if (pToken) {
//      toTSDBType(pToken->type);
//      tVariantCreate(&pSqlExpr->value, pToken);
//    }
    pSqlExpr->tokenId = optrType;
    pSqlExpr->type = SQL_NODE_VALUE;
  } else if (optrType == TK_NOW) {
    // use nanosecond by default TODO set value after getting database precision
//    pSqlExpr->value.i64 = taosGetTimestamp(TSDB_TIME_PRECISION_NANO);
//    pSqlExpr->value.nType = TSDB_DATA_TYPE_BIGINT;
    pSqlExpr->tokenId = TK_TIMESTAMP;  // TK_TIMESTAMP used to denote the time value is in microsecond
    pSqlExpr->type = SQL_NODE_VALUE;
//    pSqlExpr->flags |= 1 << EXPR_FLAG_NS_TIMESTAMP;
  } else if (optrType == TK_VARIABLE) {
    // use nanosecond by default
    // TODO set value after getting database precision
//    if (pToken) {
//      char    unit = 0;
//      int32_t ret = parseAbsoluteDuration(pToken->z, pToken->n, &pSqlExpr->value.i64, &unit, TSDB_TIME_PRECISION_NANO);
//      if (ret != TSDB_CODE_SUCCESS) {
//        terrno = TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
//      }
//    }

//    pSqlExpr->flags |= 1 << EXPR_FLAG_NS_TIMESTAMP;
//    pSqlExpr->flags |= 1 << EXPR_FLAG_TIMESTAMP_VAR;
//    pSqlExpr->value.nType = TSDB_DATA_TYPE_BIGINT;
    pSqlExpr->tokenId = TK_TIMESTAMP;
    pSqlExpr->type = SQL_NODE_VALUE;
  } else {
    // Here it must be the column name (tk_id) if it is not a number or string.
    assert(optrType == TK_ID || optrType == TK_ALL);
    if (pToken != NULL) {
      pSqlExpr->columnName = *pToken;
    }

    pSqlExpr->tokenId = optrType;
    pSqlExpr->type = SQL_NODE_TABLE_COLUMN;
  }

  return pSqlExpr;
}

tSqlExpr *tSqlExprCreateFunction(SArray *pParam, SToken *pFuncToken, SToken *endToken, int32_t optType) {
  if (pFuncToken == NULL) {
    return NULL;
  }

  tSqlExpr *pExpr     = calloc(1, sizeof(tSqlExpr));
  pExpr->tokenId      = optType;
  pExpr->type         = SQL_NODE_SQLFUNCTION;
  pExpr->Expr.paramList = pParam;

  int32_t len = (int32_t)((endToken->z + endToken->n) - pFuncToken->z);
  pExpr->Expr.operand = (*pFuncToken);

  pExpr->exprToken.n = len;
  pExpr->exprToken.z = pFuncToken->z;
  pExpr->exprToken.type = pFuncToken->type;

  return pExpr;
}

SArray *tAppendFuncName(SArray *pList, SToken *pToken) {
  assert(pList != NULL && pToken != NULL);
  taosArrayPush(pList, pToken);
  return pList;
}

tSqlExpr *tSqlExprCreate(tSqlExpr *pLeft, tSqlExpr *pRight, int32_t optrType) {
  tSqlExpr *pExpr = calloc(1, sizeof(tSqlExpr));
  pExpr->type = SQL_NODE_EXPR;

  if (pLeft != NULL && pRight != NULL && (optrType != TK_IN)) {
    char* endPos   = pRight->exprToken.z + pRight->exprToken.n;
    pExpr->exprToken.z = pLeft->exprToken.z;
    pExpr->exprToken.n = (uint32_t)(endPos - pExpr->exprToken.z);
    pExpr->exprToken.type = pLeft->exprToken.type;
  }

  if ((pLeft != NULL && pRight != NULL) &&
      (optrType == TK_PLUS || optrType == TK_MINUS || optrType == TK_STAR || optrType == TK_DIVIDE || optrType == TK_REM)) {
    /*
     * if a exprToken is noted as the TK_TIMESTAMP, the time precision is microsecond
     * Otherwise, the time precision is adaptive, determined by the time precision from databases.
     */
    if ((pLeft->tokenId == TK_INTEGER && pRight->tokenId == TK_INTEGER) ||
        (pLeft->tokenId == TK_TIMESTAMP && pRight->tokenId == TK_TIMESTAMP)) {
      pExpr->value.nType = TSDB_DATA_TYPE_BIGINT;
      pExpr->tokenId = pLeft->tokenId;
      pExpr->type    = SQL_NODE_VALUE;

      switch (optrType) {
        case TK_PLUS: {
          pExpr->value.i64 = pLeft->value.i64 + pRight->value.i64;
          break;
        }

        case TK_MINUS: {
          pExpr->value.i64 = pLeft->value.i64 - pRight->value.i64;
          break;
        }
        case TK_STAR: {
          pExpr->value.i64 = pLeft->value.i64 * pRight->value.i64;
          break;
        }
        case TK_DIVIDE: {
          pExpr->tokenId = TK_FLOAT;
          pExpr->value.nType = TSDB_DATA_TYPE_DOUBLE;
          pExpr->value.d = (double)pLeft->value.i64 / pRight->value.i64;
          break;
        }
        case TK_REM: {
          pExpr->value.i64 = pLeft->value.i64 % pRight->value.i64;
          break;
        }
      }

      tSqlExprDestroy(pLeft);
      tSqlExprDestroy(pRight);
    } else if ((pLeft->tokenId == TK_FLOAT && pRight->tokenId == TK_INTEGER) ||
               (pLeft->tokenId == TK_INTEGER && pRight->tokenId == TK_FLOAT) ||
               (pLeft->tokenId == TK_FLOAT && pRight->tokenId == TK_FLOAT)) {
      pExpr->value.nType = TSDB_DATA_TYPE_DOUBLE;
      pExpr->tokenId  = TK_FLOAT;
      pExpr->type     = SQL_NODE_VALUE;

      double left  = (pLeft->value.nType == TSDB_DATA_TYPE_DOUBLE) ? pLeft->value.d : pLeft->value.i64;
      double right = (pRight->value.nType == TSDB_DATA_TYPE_DOUBLE) ? pRight->value.d : pRight->value.i64;

      switch (optrType) {
        case TK_PLUS: {
          pExpr->value.d = left + right;
          break;
        }
        case TK_MINUS: {
          pExpr->value.d = left - right;
          break;
        }
        case TK_STAR: {
          pExpr->value.d = left * right;
          break;
        }
        case TK_DIVIDE: {
          pExpr->value.d = left / right;
          break;
        }
        case TK_REM: {
          pExpr->value.d = left - ((int64_t)(left / right)) * right;
          break;
        }
      }

      tSqlExprDestroy(pLeft);
      tSqlExprDestroy(pRight);

    } else {
      pExpr->tokenId = optrType;
      pExpr->pLeft = pLeft;
      pExpr->pRight = pRight;
    }
  } else if (optrType == TK_IN) {
    pExpr->tokenId = optrType;
    pExpr->pLeft = pLeft;

    tSqlExpr *pRSub = calloc(1, sizeof(tSqlExpr));
    pRSub->tokenId = TK_SET;  // TODO refactor .....
    pRSub->Expr.paramList = (SArray *)pRight;

    pExpr->pRight = pRSub;
  } else {
    pExpr->tokenId = optrType;
    pExpr->pLeft = pLeft;

    if (pLeft != NULL && pRight == NULL) {
      pRight = calloc(1, sizeof(tSqlExpr));
    }

    pExpr->pRight = pRight;
  }

  return pExpr;
}

tSqlExpr *tSqlExprClone(tSqlExpr *pSrc);
void      tSqlExprCompact(tSqlExpr **pExpr);
bool      tSqlExprIsLeaf(tSqlExpr *pExpr);
bool      tSqlExprIsParentOfLeaf(tSqlExpr *pExpr);
void      tSqlExprDestroy(tSqlExpr *pExpr);
SArray *  tSqlExprListAppend(SArray *pList, tSqlExpr *pNode, SToken *pDistinct, SToken *pToken);
void      tSqlExprListDestroy(SArray *pList);

SSqlNode *tSetQuerySqlNode(SToken *pSelectToken, SArray *pSelNodeList, SRelationInfo *pFrom, tSqlExpr *pWhere,
                           SArray *pGroupby, SArray *pSortOrder, SIntervalVal *pInterval, SSessionWindowVal *ps,
                           SWindowStateVal *pw, SToken *pSliding, SArray *pFill, SLimit *pLimit, SLimit *pgLimit, tSqlExpr *pHaving);
int32_t   tSqlExprCompare(tSqlExpr *left, tSqlExpr *right);

SCreateTableSql *tSetCreateTableInfo(SArray *pCols, SArray *pTags, SSqlNode *pSelect, int32_t type);

SAlterTableInfo * tSetAlterTableInfo(SToken *pTableName, SArray *pCols, SArray *pVals, int32_t type,
                                     int16_t tableTable);
SCreatedTableInfo createNewChildTableInfo(SToken *pTableName, SArray *pTagNames, SArray *pTagVals, SToken *pToken,
                                          SToken *igExists);

void destroyAllSqlNode(SArray *pSqlNode);
void destroySqlNode(SSqlNode *pSql);
void freeCreateTableInfo(void* p);

SSqlInfo *setSqlInfo(SSqlInfo *pInfo, void *pSqlExprInfo, SToken *pTableName, int32_t type);
SArray   *setSubclause(SArray *pList, void *pSqlNode);
SArray   *appendSelectClause(SArray *pList, void *pSubclause);

void setCreatedTableName(SSqlInfo *pInfo, SToken *pTableNameToken, SToken *pIfNotExists);

void SqlInfoDestroy(SSqlInfo *pInfo);

void setDCLSqlElems(SSqlInfo *pInfo, int32_t type, int32_t nParams, ...);
void setDropDbTableInfo(SSqlInfo *pInfo, int32_t type, SToken* pToken, SToken* existsCheck,int16_t dbType,int16_t tableType);
void setShowOptions(SSqlInfo *pInfo, int32_t type, SToken* prefix, SToken* pPatterns);

void setCreateDbInfo(SSqlInfo *pInfo, int32_t type, SToken *pToken, SCreateDbInfo *pDB, SToken *pIgExists);

void setCreateAcctSql(SSqlInfo *pInfo, int32_t type, SToken *pName, SToken *pPwd, SCreateAcctInfo *pAcctInfo);
void setCreateUserSql(SSqlInfo *pInfo, SToken *pName, SToken *pPasswd);
void setKillSql(SSqlInfo *pInfo, int32_t type, SToken *ip);
void setAlterUserSql(SSqlInfo *pInfo, int16_t type, SToken *pName, SToken* pPwd, SToken *pPrivilege);

void setCompactVnodeSql(SSqlInfo *pInfo, int32_t type, SArray *pParam);

void setDefaultCreateDbOption(SCreateDbInfo *pDBInfo);
void setDefaultCreateTopicOption(SCreateDbInfo *pDBInfo);

// prefix show db.tables;
void tSetDbName(SToken *pCpxName, SToken *pDb);

void tSetColumnInfo(struct SField *pField, SToken *pName, struct SField *pType);
void tSetColumnType(struct SField *pField, SToken *type);

/**
 *
 * @param yyp      The parser
 * @param yymajor  The major token code number
 * @param yyminor  The value for the token
 */
void Parse(void *yyp, int yymajor, ParseTOKENTYPE yyminor, SSqlInfo *);

/**
 *
 * @param p         The parser to be deleted
 * @param freeProc  Function used to reclaim memory
 */
void ParseFree(void *p, void (*freeProc)(void *));

/**
 *
 * @param mallocProc  The parser allocator
 * @return
 */
void *ParseAlloc(void *(*mallocProc)(size_t));

SSqlInfo genAST(const char *pStr) {
  void *pParser = ParseAlloc(malloc);

  SSqlInfo sqlInfo = {0};

  sqlInfo.valid = true;
  sqlInfo.funcs = taosArrayInit(4, sizeof(SToken));

  int32_t i = 0;
  while (1) {
    SToken t0 = {0};

    if (pStr[i] == 0) {
      Parse(pParser, 0, t0, &sqlInfo);
      goto abort_parse;
    }

    t0.n = tGetToken((char *)&pStr[i], &t0.type);
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
        snprintf(sqlInfo.msg, tListLen(sqlInfo.msg), "unrecognized token: \"%s\"", t0.z);
        sqlInfo.valid = false;
        goto abort_parse;
      }

      case TK_HEX:
      case TK_OCT:
      case TK_BIN: {
        snprintf(sqlInfo.msg, tListLen(sqlInfo.msg), "unsupported token: \"%s\"", t0.z);
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
