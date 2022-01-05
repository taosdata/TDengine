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

#include "qSqlparser.h"
#include "os.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "tcmdtype.h"
#include "tstrbuild.h"
#include "ttoken.h"
#include "ttokendef.h"
#include "tutil.h"

SSqlInfo qSqlParse(const char *pStr) {
  void *pParser = ParseAlloc(malloc);

  SSqlInfo sqlInfo = {0};
  sqlInfo.valid = true;
  sqlInfo.funcs = taosArrayInit(4, sizeof(SStrToken));

  int32_t i = 0;
  while (1) {
    SStrToken t0 = {0};

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
      case TK_BIN:{
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

SArray *tSqlExprListAppend(SArray *pList, tSqlExpr *pNode, SStrToken *pDistinct, SStrToken *pToken) {
  if (pList == NULL) {
    pList = taosArrayInit(4, sizeof(tSqlExprItem));
  }

  if (pNode || pToken) {
    struct tSqlExprItem item = {0};

    item.pNode = pNode;
    item.distinct = (pDistinct != NULL);

    if (pToken) {  // set the as clause
      item.aliasName = malloc(pToken->n + 1);
      strncpy(item.aliasName, pToken->z, pToken->n);
      item.aliasName[pToken->n] = 0;

      strdequote(item.aliasName);
    }

    taosArrayPush(pList, &item);
  }

  return pList;
}

static void freeExprElem(void* item) {
  tSqlExprItem* exprItem = item;

  tfree(exprItem->aliasName);
  tSqlExprDestroy(exprItem->pNode);
}

void tSqlExprListDestroy(SArray *pList) {
  if (pList == NULL) {
    return;
  }

  taosArrayDestroyEx(pList, freeExprElem);
}


SArray *tStrTokenAppend(SArray *pList, SStrToken *pToken) {
  if (pList == NULL) {
    pList = taosArrayInit(4, sizeof(tVariantListItem));
  }

  if (pToken) {
    taosArrayPush(pList, pToken);
  }

  return pList;
}

tSqlExpr *tSqlExprCreateIdValue(SSqlInfo* pInfo, SStrToken *pToken, int32_t optrType) {
  tSqlExpr *pSqlExpr = calloc(1, sizeof(tSqlExpr));

  if (pToken != NULL) {
    pSqlExpr->exprToken = *pToken;
  }

  if (optrType == TK_NULL) {
    if (pToken){
      pToken->type = TSDB_DATA_TYPE_NULL;
      tVariantCreate(&pSqlExpr->value, pToken);
    }
    pSqlExpr->tokenId = optrType;
    pSqlExpr->type    = SQL_NODE_VALUE;
  } else if (optrType == TK_INTEGER || optrType == TK_STRING || optrType == TK_FLOAT || optrType == TK_BOOL) {
    if (pToken) {
      toTSDBType(pToken->type);
      tVariantCreate(&pSqlExpr->value, pToken);
    }
    pSqlExpr->tokenId = optrType;
    pSqlExpr->type    = SQL_NODE_VALUE;
  } else if (optrType == TK_NOW) {
    // use nanosecond by default TODO set value after getting database precision
    pSqlExpr->value.i64 = taosGetTimestamp(TSDB_TIME_PRECISION_NANO);
    pSqlExpr->value.nType = TSDB_DATA_TYPE_BIGINT;
    pSqlExpr->tokenId = TK_TIMESTAMP;  // TK_TIMESTAMP used to denote the time value is in microsecond
    pSqlExpr->type    = SQL_NODE_VALUE;
    pSqlExpr->flags  |= 1 << EXPR_FLAG_NS_TIMESTAMP;
  } else if (optrType == TK_VARIABLE) {
    // use nanosecond by default
    // TODO set value after getting database precision
    if (pToken) {
      char unit = 0;
      int32_t ret = parseAbsoluteDuration(pToken->z, pToken->n, &pSqlExpr->value.i64, &unit, TSDB_TIME_PRECISION_NANO);
      if (ret != TSDB_CODE_SUCCESS) {
        snprintf(pInfo->msg, tListLen(pInfo->msg), "%s", pToken->z);
        terrno = TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
      }
    }

    pSqlExpr->flags  |= 1 << EXPR_FLAG_NS_TIMESTAMP;
    pSqlExpr->flags  |= 1 << EXPR_FLAG_TIMESTAMP_VAR;
    pSqlExpr->value.nType = TSDB_DATA_TYPE_BIGINT;
    pSqlExpr->tokenId = TK_TIMESTAMP;
    pSqlExpr->type    = SQL_NODE_VALUE;
  } else {
    // Here it must be the column name (tk_id) if it is not a number or string.
    assert(optrType == TK_ID || optrType == TK_ALL);
    if (pToken != NULL) {
      pSqlExpr->columnName = *pToken;
    }

    pSqlExpr->tokenId = optrType;
    pSqlExpr->type    = SQL_NODE_TABLE_COLUMN;
  }

  return pSqlExpr;
}

/*
 * pList is the parameters for function with id(optType)
 * function name is denoted by pFunctionToken
 */
tSqlExpr *tSqlExprCreateFunction(SArray *pParam, SStrToken *pFuncToken, SStrToken *endToken, int32_t optType) {
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

/*
 * create binary expression in this procedure
 * if the expr is arithmetic, calculate the result and set it to tSqlExpr Object
 */
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
      pExpr->flags   = pLeft->flags | pRight->flags;

      if ((pLeft->flags & (1 << EXPR_FLAG_TIMESTAMP_VAR)) && (pRight->flags & (1 << EXPR_FLAG_TIMESTAMP_VAR))) {
        pExpr->flags |= 1 << EXPR_FLAG_TS_ERROR;
      } else {
        pExpr->flags &= ~(1 << EXPR_FLAG_TIMESTAMP_VAR);
        pExpr->flags &= ~(1 << EXPR_FLAG_TS_ERROR);
      }

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
          pExpr->value.dKey = (double)pLeft->value.i64 / pRight->value.i64;
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

      double left  = (pLeft->value.nType == TSDB_DATA_TYPE_DOUBLE) ? pLeft->value.dKey : pLeft->value.i64;
      double right = (pRight->value.nType == TSDB_DATA_TYPE_DOUBLE) ? pRight->value.dKey : pRight->value.i64;

      switch (optrType) {
        case TK_PLUS: {
          pExpr->value.dKey = left + right;
          break;
        }
        case TK_MINUS: {
          pExpr->value.dKey = left - right;
          break;
        }
        case TK_STAR: {
          pExpr->value.dKey = left * right;
          break;
        }
        case TK_DIVIDE: {
          pExpr->value.dKey = left / right;
          break;
        }
        case TK_REM: {
          pExpr->value.dKey = left - ((int64_t)(left / right)) * right;
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

static FORCE_INLINE int32_t tStrTokenCompare(SStrToken* left, SStrToken* right) {
  return (left->type == right->type && left->n == right->n && strncasecmp(left->z, right->z, left->n) == 0) ? 0 : 1;
}

// this function is not used for temporary
int32_t tSqlExprCompare(tSqlExpr *left, tSqlExpr *right) {
  if ((left == NULL && right) || (left && right == NULL) || (left == NULL && right == NULL)) {
    return 1;
  }

  if (left->type != right->type) {
    return 1;
  }

  if (left->tokenId != right->tokenId) {
    return 1;
  }

  if (left->functionId != right->functionId) {
    return 1;
  }

  if ((left->pLeft && right->pLeft == NULL)
    || (left->pLeft == NULL && right->pLeft)
    || (left->pRight && right->pRight == NULL)
    || (left->pRight == NULL && right->pRight)
    || (left->Expr.paramList && right->Expr.paramList == NULL)
    || (left->Expr.paramList == NULL && right->Expr.paramList)) {
    return 1;
  }

  if (tVariantCompare(&left->value, &right->value)) {
    return 1;
  }

  if (tStrTokenCompare(&left->columnName, &right->columnName)) {
    return 1;
  }

  if (right->Expr.paramList && left->Expr.paramList) {
    size_t size = taosArrayGetSize(right->Expr.paramList);
    if (left->Expr.paramList && taosArrayGetSize(left->Expr.paramList) != size) {
      return 1;
    }

    for (int32_t i = 0; i < size; i++) {
      tSqlExprItem* pLeftElem = taosArrayGet(left->Expr.paramList, i);
      tSqlExpr* pSubLeft = pLeftElem->pNode;
      tSqlExprItem* pRightElem = taosArrayGet(right->Expr.paramList, i);
      tSqlExpr* pSubRight = pRightElem->pNode;

      if (tSqlExprCompare(pSubLeft, pSubRight)) {
        return 1;
      }
    }
  }

  if (left->pLeft && tSqlExprCompare(left->pLeft, right->pLeft)) {
    return 1;
  }

  if (left->pRight && tSqlExprCompare(left->pRight, right->pRight)) {
    return 1;
  }

  return 0;
}

tSqlExpr *tSqlExprClone(tSqlExpr *pSrc) {
  tSqlExpr *pExpr = calloc(1, sizeof(tSqlExpr));

  memcpy(pExpr, pSrc, sizeof(*pSrc));
  
  if (pSrc->pLeft) {
    pExpr->pLeft = tSqlExprClone(pSrc->pLeft);
  }

  if (pSrc->pRight) {
    pExpr->pRight = tSqlExprClone(pSrc->pRight);
  }

  //we don't clone paramList now because clone is only used for between/and
  assert(pSrc->Expr.paramList == NULL);
  return pExpr;
}

void tSqlExprCompact(tSqlExpr** pExpr) {
  if (*pExpr == NULL || tSqlExprIsParentOfLeaf(*pExpr)) {
    return;
  }

  if ((*pExpr)->pLeft) {
    tSqlExprCompact(&(*pExpr)->pLeft);
  }

  if ((*pExpr)->pRight) {
    tSqlExprCompact(&(*pExpr)->pRight);
  }

  if ((*pExpr)->pLeft == NULL && (*pExpr)->pRight == NULL && ((*pExpr)->tokenId == TK_OR || (*pExpr)->tokenId == TK_AND)) {
    tSqlExprDestroy(*pExpr);
    *pExpr = NULL;
  } else if ((*pExpr)->pLeft == NULL && (*pExpr)->pRight != NULL) {
    tSqlExpr* tmpPtr = (*pExpr)->pRight;
    (*pExpr)->pRight = NULL;

    tSqlExprDestroy(*pExpr);
    (*pExpr) = tmpPtr;
  } else if ((*pExpr)->pRight == NULL && (*pExpr)->pLeft != NULL) {
    tSqlExpr* tmpPtr = (*pExpr)->pLeft;
    (*pExpr)->pLeft = NULL;

    tSqlExprDestroy(*pExpr);
    (*pExpr) = tmpPtr;
  }
}

bool tSqlExprIsLeaf(tSqlExpr* pExpr) {
  return (pExpr->pRight == NULL && pExpr->pLeft == NULL) &&
         (pExpr->tokenId == 0 ||
         (pExpr->tokenId == TK_ID) ||
         (pExpr->tokenId >= TK_BOOL && pExpr->tokenId <= TK_NCHAR) ||
         (pExpr->tokenId == TK_NULL) ||
         (pExpr->tokenId == TK_SET));
}

bool tSqlExprIsParentOfLeaf(tSqlExpr* pExpr) {
  return (pExpr->pLeft != NULL && pExpr->pRight != NULL) &&
         (tSqlExprIsLeaf(pExpr->pLeft) && tSqlExprIsLeaf(pExpr->pRight));
}

static void doDestroySqlExprNode(tSqlExpr *pExpr) {
  if (pExpr == NULL) {
    return;
  }

  if (pExpr->tokenId == TK_STRING) {
    tVariantDestroy(&pExpr->value);
  }

  tSqlExprListDestroy(pExpr->Expr.paramList);
  free(pExpr);
}

void tSqlExprDestroy(tSqlExpr *pExpr) {
  if (pExpr == NULL) {
    return;
  }

  tSqlExprDestroy(pExpr->pLeft);
  pExpr->pLeft = NULL;
  tSqlExprDestroy(pExpr->pRight);
  pExpr->pRight = NULL;

  doDestroySqlExprNode(pExpr);
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

SArray *tVariantListInsert(SArray *pList, tVariant *pVar, uint8_t sortOrder, int32_t qry_index) {
  if (pList == NULL || pVar == NULL || qry_index >= taosArrayGetSize(pList)) {
    return tVariantListAppend(NULL, pVar, sortOrder);
  }

  tVariantListItem item;

  item.pVar = *pVar;
  item.sortOrder = sortOrder;

  taosArrayInsert(pList, qry_index, &item);
  return pList;
}

SRelationInfo *setTableNameList(SRelationInfo* pRelationInfo, SStrToken *pName, SStrToken* pAlias) {
  if (pRelationInfo == NULL) {
    pRelationInfo = calloc(1, sizeof(SRelationInfo));
    pRelationInfo->list = taosArrayInit(4, sizeof(SRelElementPair));
  }

  pRelationInfo->type = SQL_NODE_FROM_TABLELIST;
  SRelElementPair p;
  p.tableName = *pName;
  if (pAlias != NULL) {
    p.aliasName = *pAlias;
  } else {
    TPARSER_SET_NONE_TOKEN(p.aliasName);
  }

  taosArrayPush(pRelationInfo->list, &p);
  return pRelationInfo;
}

void* destroyRelationInfo(SRelationInfo* pRelationInfo) {
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

SRelationInfo* addSubqueryElem(SRelationInfo* pRelationInfo, SArray* pSub, SStrToken* pAlias) {
  if (pRelationInfo == NULL) {
    pRelationInfo = calloc(1, sizeof(SRelationInfo));
    pRelationInfo->list = taosArrayInit(4, sizeof(SRelElementPair));
  }

  pRelationInfo->type = SQL_NODE_FROM_SUBQUERY;

  SRelElementPair p;
  p.pSubquery = pSub;
  if (pAlias != NULL) {
    p.aliasName = *pAlias;
  } else {
    TPARSER_SET_NONE_TOKEN(p.aliasName);
  }

  taosArrayPush(pRelationInfo->list, &p);
  return pRelationInfo;
}

void tSetDbName(SStrToken *pCpxName, SStrToken *pDb) {
  pCpxName->type = pDb->type;
  pCpxName->z = pDb->z;
  pCpxName->n = pDb->n;
}

void tSetColumnInfo(TAOS_FIELD *pField, SStrToken *pName, TAOS_FIELD *pType) {
  int32_t maxLen = sizeof(pField->name) / sizeof(pField->name[0]);

  // column name is too long, set the it to be invalid.
  if ((int32_t) pName->n >= maxLen) {
    pName->n = -1;
  } else {
    strncpy(pField->name, pName->z, pName->n);
    pField->name[pName->n] = 0;
  }

  pField->type = pType->type;
  if(!isValidDataType(pField->type)){
    pField->bytes = 0;
  } else {
    pField->bytes = pType->bytes;
  }
}

static int32_t tryParseNameTwoParts(SStrToken *type) {
  int32_t t = -1;

  char* str = strndup(type->z, type->n);
  if (str == NULL) {
    return t;
  }

  char* p = strtok(str, " ");
  if (p == NULL) {
    tfree(str);
    return t;
  } else {
    char* unsign = strtok(NULL, " ");
    if (unsign == NULL) {
      tfree(str);
      return t;
    }

    if (strncasecmp(unsign, "UNSIGNED", 8) == 0) {
      for(int32_t j = TSDB_DATA_TYPE_TINYINT; j <= TSDB_DATA_TYPE_BIGINT; ++j) {
        if (strcasecmp(p, tDataTypes[j].name) == 0) {
          t = j;
          break;
        }
      }

      tfree(str);

      if (t == -1) {
        return -1;
      }

      switch(t) {
        case TSDB_DATA_TYPE_TINYINT:  return TSDB_DATA_TYPE_UTINYINT;
        case TSDB_DATA_TYPE_SMALLINT: return TSDB_DATA_TYPE_USMALLINT;
        case TSDB_DATA_TYPE_INT:      return TSDB_DATA_TYPE_UINT;
        case TSDB_DATA_TYPE_BIGINT:   return TSDB_DATA_TYPE_UBIGINT;
        default:
          return -1;
      }

    } else {
      tfree(str);
      return -1;
    }
  }
}

void tSetColumnType(TAOS_FIELD *pField, SStrToken *type) {
  // set the field type invalid
  pField->type = -1;
  pField->name[0] = 0;

  int32_t i = 0;
  while (i < tListLen(tDataTypes)) {
    if ((type->n == tDataTypes[i].nameLen) &&
        (strncasecmp(type->z, tDataTypes[i].name, tDataTypes[i].nameLen) == 0)) {
      break;
    }

    i += 1;
  }

  // no qualified data type found, try unsigned data type
  if (i == tListLen(tDataTypes)) {
    i = tryParseNameTwoParts(type);
    if (i == -1) {
      return;
    }
  }

  pField->type = i;
  pField->bytes = tDataTypes[i].bytes;

  if (i == TSDB_DATA_TYPE_NCHAR) {
    /*
     * for nchar, the TOKENTYPE is the number of character, so the length is the
     * number of bytes in UCS-4 format, which is 4 times larger than the number of characters
     */
    if (type->type == 0) {
      pField->bytes = 0;
    } else {
      int32_t bytes = -(int32_t)(type->type);
      if (bytes > (TSDB_MAX_NCHAR_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE) {
        // overflowed. set bytes to -1 so that error can be reported
        bytes = -1;
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
        // overflowed. set bytes to -1 so that error can be reported
        bytes = -1;
      } else {
        bytes += VARSTR_HEADER_SIZE;
      }

      pField->bytes = (int16_t)bytes;
    }
  } else {
    if (type->type > 0) {
      pField->type = -1;
    }
  }
}

/*
 * extract the select info out of sql string
 */
SSqlNode *tSetQuerySqlNode(SStrToken *pSelectToken, SArray *pSelNodeList, SRelationInfo *pFrom, tSqlExpr *pWhere,
                                SArray *pGroupby, SArray *pSortOrder, SIntervalVal *pInterval,
                                SSessionWindowVal *pSession, SWindowStateVal *pWindowStateVal, SStrToken *pSliding, SArray *pFill, SLimitVal *pLimit,
                                SLimitVal *psLimit, tSqlExpr *pHaving) {
  assert(pSelNodeList != NULL);

  SSqlNode *pSqlNode = calloc(1, sizeof(SSqlNode));

  // all later sql string are belonged to the stream sql
  pSqlNode->sqlstr   = *pSelectToken;
  pSqlNode->sqlstr.n = (uint32_t)strlen(pSqlNode->sqlstr.z);

  pSqlNode->pSelNodeList = pSelNodeList;
  pSqlNode->from        = pFrom;
  pSqlNode->pGroupby    = pGroupby;
  pSqlNode->pSortOrder  = pSortOrder;
  pSqlNode->pWhere      = pWhere;
  pSqlNode->fillType    = pFill;
  pSqlNode->pHaving = pHaving;

  if (pLimit != NULL) {
    pSqlNode->limit = *pLimit;
  } else {
    pSqlNode->limit.limit = -1;
    pSqlNode->limit.offset = 0;
  }

  if (psLimit != NULL) {
    pSqlNode->slimit = *psLimit;
  } else {
    pSqlNode->slimit.limit = -1;
    pSqlNode->slimit.offset = 0;
  }

  if (pInterval != NULL) {
    pSqlNode->interval = *pInterval;
  } else {
    TPARSER_SET_NONE_TOKEN(pSqlNode->interval.interval);
    TPARSER_SET_NONE_TOKEN(pSqlNode->interval.offset);
  }

  if (pSliding != NULL) {
    pSqlNode->sliding = *pSliding;
  } else {
    TPARSER_SET_NONE_TOKEN(pSqlNode->sliding);
  }

  if (pSession != NULL) {
    pSqlNode->sessionVal = *pSession;
  } else {
    TPARSER_SET_NONE_TOKEN(pSqlNode->sessionVal.gap);
    TPARSER_SET_NONE_TOKEN(pSqlNode->sessionVal.col);
  }

  if (pWindowStateVal != NULL) {
    pSqlNode->windowstateVal = *pWindowStateVal;
  } else {
    TPARSER_SET_NONE_TOKEN(pSqlNode->windowstateVal.col);
  }

  return pSqlNode;
}

static void freeVariant(void *pItem) {
  tVariantListItem* p = (tVariantListItem*) pItem;
  tVariantDestroy(&p->pVar);
}

void freeCreateTableInfo(void* p) {
  SCreatedTableInfo* pInfo = (SCreatedTableInfo*) p;  
  taosArrayDestroy(pInfo->pTagNames);
  taosArrayDestroyEx(pInfo->pTagVals, freeVariant);
  tfree(pInfo->fullname);
  tfree(pInfo->tagdata.data);
}

void destroySqlNode(SSqlNode *pSqlNode) {
  if (pSqlNode == NULL) {
    return;
  }

  tSqlExprListDestroy(pSqlNode->pSelNodeList);
  pSqlNode->pSelNodeList = NULL;

  tSqlExprDestroy(pSqlNode->pWhere);
  pSqlNode->pWhere = NULL;
  
  taosArrayDestroyEx(pSqlNode->pSortOrder, freeVariant);
  pSqlNode->pSortOrder = NULL;

  taosArrayDestroyEx(pSqlNode->pGroupby, freeVariant);
  pSqlNode->pGroupby = NULL;

  pSqlNode->from = destroyRelationInfo(pSqlNode->from);

  taosArrayDestroyEx(pSqlNode->fillType, freeVariant);
  pSqlNode->fillType = NULL;

  tSqlExprDestroy(pSqlNode->pHaving);
  free(pSqlNode);
}

void destroyAllSqlNode(SArray *pList) {
  if (pList == NULL) {
    return;
  }

  size_t size = taosArrayGetSize(pList);
  for(int32_t i = 0; i < size; ++i) {
    SSqlNode *pNode = taosArrayGetP(pList, i);
    destroySqlNode(pNode);
  }

  taosArrayDestroy(pList);
}

SCreateTableSql *tSetCreateTableInfo(SArray *pCols, SArray *pTags, SSqlNode *pSelect, int32_t type) {
  SCreateTableSql *pCreate = calloc(1, sizeof(SCreateTableSql));

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

SCreatedTableInfo createNewChildTableInfo(SStrToken *pTableName, SArray *pTagNames, SArray *pTagVals, SStrToken *pToken, SStrToken* igExists) {
  SCreatedTableInfo info;
  memset(&info, 0, sizeof(SCreatedTableInfo));

  info.name       = *pToken;
  info.pTagNames  = pTagNames;
  info.pTagVals   = pTagVals;
  info.stableName = *pTableName;
  info.igExist    = (igExists->n > 0)? 1:0;

  return info;
}

SAlterTableInfo *tSetAlterTableInfo(SStrToken *pTableName, SArray *pCols, SArray *pVals, int32_t type, int16_t tableType) {
  SAlterTableInfo *pAlterTable = calloc(1, sizeof(SAlterTableInfo));
  
  pAlterTable->name = *pTableName;
  pAlterTable->type = type;
  pAlterTable->tableType = tableType;

  if (type == TSDB_ALTER_TABLE_ADD_COLUMN || type == TSDB_ALTER_TABLE_ADD_TAG_COLUMN || type == TSDB_ALTER_TABLE_CHANGE_COLUMN || type == TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN) {
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

void* destroyCreateTableSql(SCreateTableSql* pCreate) {
  destroySqlNode(pCreate->pSelect);

  taosArrayDestroy(pCreate->colInfo.pColumns);
  taosArrayDestroy(pCreate->colInfo.pTagColumns);

  taosArrayDestroyEx(pCreate->childTableInfo, freeCreateTableInfo);
  tfree(pCreate);

  return NULL;
}

void SqlInfoDestroy(SSqlInfo *pInfo) {
  if (pInfo == NULL) return;;
  taosArrayDestroy(pInfo->funcs); 
  if (pInfo->type == TSDB_SQL_SELECT) {
    destroyAllSqlNode(pInfo->list);
  } else if (pInfo->type == TSDB_SQL_CREATE_TABLE) {
    pInfo->pCreateTableInfo = destroyCreateTableSql(pInfo->pCreateTableInfo);
  } else if (pInfo->type == TSDB_SQL_ALTER_TABLE) {
    taosArrayDestroyEx(pInfo->pAlterInfo->varList, freeVariant);
    taosArrayDestroy(pInfo->pAlterInfo->pAddColumns);
    tfree(pInfo->pAlterInfo->tagData.data);
    tfree(pInfo->pAlterInfo);
  } else {
    if (pInfo->pMiscInfo != NULL) {
      taosArrayDestroy(pInfo->pMiscInfo->a);
    }

    if (pInfo->pMiscInfo != NULL && (pInfo->type == TSDB_SQL_CREATE_DB || pInfo->type == TSDB_SQL_ALTER_DB)) {
      taosArrayDestroyEx(pInfo->pMiscInfo->dbOpt.keep, freeVariant);
    }

    tfree(pInfo->pMiscInfo);
  }
}

SArray* setSubclause(SArray* pList, void *pSqlNode) {
  if (pList == NULL) {
    pList = taosArrayInit(1, POINTER_BYTES);
  }
  
  taosArrayPush(pList, &pSqlNode);
  return pList;
}

SSqlInfo* setSqlInfo(SSqlInfo *pInfo, void *pSqlExprInfo, SStrToken *pTableName, int32_t type) {
  pInfo->type = type;
  
  if (type == TSDB_SQL_SELECT) {
    pInfo->list = (SArray*) pSqlExprInfo;
  } else {
    pInfo->pCreateTableInfo = pSqlExprInfo;
  }
  
  if (pTableName != NULL) {
    pInfo->pCreateTableInfo->name = *pTableName;
  }
  
  return pInfo;
}

SArray* appendSelectClause(SArray *pList, void *pSubclause) {
  taosArrayPush(pList, &pSubclause);
  return pList;
}

void setCreatedTableName(SSqlInfo *pInfo, SStrToken *pTableNameToken, SStrToken *pIfNotExists) {
  pInfo->pCreateTableInfo->name = *pTableNameToken;
  pInfo->pCreateTableInfo->existCheck = (pIfNotExists->n != 0);
}

void setCreatedStreamOpt(SSqlInfo *pInfo, SStrToken *pTo, SStrToken *pSplit) {
  pInfo->pCreateTableInfo->to = *pTo;
  pInfo->pCreateTableInfo->split = *pSplit;
}

void setDCLSqlElems(SSqlInfo *pInfo, int32_t type, int32_t nParam, ...) {
  pInfo->type = type;
  if (nParam == 0) {
    return;
  }

  if (pInfo->pMiscInfo == NULL) {
    pInfo->pMiscInfo = (SMiscInfo *)calloc(1, sizeof(SMiscInfo));
    pInfo->pMiscInfo->a = taosArrayInit(4, sizeof(SStrToken));
  }

  va_list va;
  va_start(va, nParam);

  while ((nParam--) > 0) {
    SStrToken *pToken = va_arg(va, SStrToken *);
    taosArrayPush(pInfo->pMiscInfo->a, pToken);
  }

  va_end(va);
}

void setDropDbTableInfo(SSqlInfo *pInfo, int32_t type, SStrToken* pToken, SStrToken* existsCheck, int16_t dbType, int16_t tableType) {
  pInfo->type = type;

  if (pInfo->pMiscInfo == NULL) {
    pInfo->pMiscInfo = (SMiscInfo *)calloc(1, sizeof(SMiscInfo));
    pInfo->pMiscInfo->a = taosArrayInit(4, sizeof(SStrToken));
  }

  taosArrayPush(pInfo->pMiscInfo->a, pToken);

  pInfo->pMiscInfo->existsCheck = (existsCheck->n == 1);
  pInfo->pMiscInfo->dbType = dbType;
  pInfo->pMiscInfo->tableType = tableType;
}

void setDropFuncInfo(SSqlInfo *pInfo, int32_t type, SStrToken* pToken) {
  pInfo->type = type;

  if (pInfo->pMiscInfo == NULL) {
    pInfo->pMiscInfo = (SMiscInfo *)calloc(1, sizeof(SMiscInfo));
    pInfo->pMiscInfo->a = taosArrayInit(4, sizeof(SStrToken));
  }

  taosArrayPush(pInfo->pMiscInfo->a, pToken);
}


void setShowOptions(SSqlInfo *pInfo, int32_t type, SStrToken* prefix, SStrToken* pPatterns) {
  if (pInfo->pMiscInfo == NULL) {
    pInfo->pMiscInfo = calloc(1, sizeof(SMiscInfo));
  }
  
  pInfo->type = TSDB_SQL_SHOW;
  
  SShowInfo* pShowInfo = &pInfo->pMiscInfo->showOpt;
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

void setCreateDbInfo(SSqlInfo *pInfo, int32_t type, SStrToken *pToken, SCreateDbInfo *pDB, SStrToken *pIgExists) {
  pInfo->type = type;
  if (pInfo->pMiscInfo == NULL) {
    pInfo->pMiscInfo = calloc(1, sizeof(SMiscInfo));
  }

  pInfo->pMiscInfo->dbOpt = *pDB;
  pInfo->pMiscInfo->dbOpt.dbname = *pToken;
  pInfo->pMiscInfo->dbOpt.ignoreExists = pIgExists->n; // sql.y has: ifnotexists(X) ::= IF NOT EXISTS.   {X.n = 1;}
}

void setCreateFuncInfo(SSqlInfo *pInfo, int32_t type, SStrToken *pName, SStrToken *pPath, TAOS_FIELD *output, SStrToken* bufSize, int32_t funcType) {
  pInfo->type = type;
  if (pInfo->pMiscInfo == NULL) {
    pInfo->pMiscInfo = calloc(1, sizeof(SMiscInfo));
  }

  pInfo->pMiscInfo->funcOpt.name = *pName;
  pInfo->pMiscInfo->funcOpt.path = *pPath;
  pInfo->pMiscInfo->funcOpt.output = *output;
  pInfo->pMiscInfo->funcOpt.type = funcType;
  if (bufSize->n > 0) {
    pInfo->pMiscInfo->funcOpt.bufSize = strtol(bufSize->z, NULL, 10);
  } else {
    pInfo->pMiscInfo->funcOpt.bufSize = 0;  
  }
}


void setCreateAcctSql(SSqlInfo *pInfo, int32_t type, SStrToken *pName, SStrToken *pPwd, SCreateAcctInfo *pAcctInfo) {
  pInfo->type = type;
  if (pInfo->pMiscInfo == NULL) {
    pInfo->pMiscInfo = calloc(1, sizeof(SMiscInfo));
  }

  pInfo->pMiscInfo->acctOpt = *pAcctInfo;
  
  assert(pName != NULL);
  pInfo->pMiscInfo->user.user = *pName;
  
  if (pPwd != NULL) {
    pInfo->pMiscInfo->user.passwd = *pPwd;
  }
}
void setCompactVnodeSql(SSqlInfo *pInfo, int32_t type, SArray *pParam) {
 pInfo->type = type;  
 pInfo->list = pParam; 
}

void setCreateUserSql(SSqlInfo *pInfo, SStrToken *pName, SStrToken *pPasswd) {
  pInfo->type = TSDB_SQL_CREATE_USER;
  if (pInfo->pMiscInfo == NULL) {
    pInfo->pMiscInfo = calloc(1, sizeof(SMiscInfo));
  }
  
  assert(pName != NULL && pPasswd != NULL);
  
  pInfo->pMiscInfo->user.user = *pName;
  pInfo->pMiscInfo->user.passwd = *pPasswd;
}

void setAlterUserSql(SSqlInfo *pInfo, int16_t type, SStrToken *pName, SStrToken* pPwd, SStrToken *pPrivilege) {
  pInfo->type = TSDB_SQL_ALTER_USER;
  if (pInfo->pMiscInfo == NULL) {
    pInfo->pMiscInfo = calloc(1, sizeof(SMiscInfo));
  }
  
  assert(pName != NULL);
  
  SUserInfo* pUser = &pInfo->pMiscInfo->user;
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

void setKillSql(SSqlInfo *pInfo, int32_t type, SStrToken *id) {
  pInfo->type = type;
  if (pInfo->pMiscInfo == NULL) {
    pInfo->pMiscInfo = calloc(1, sizeof(SMiscInfo));
  }
  
  assert(id != NULL);
  pInfo->pMiscInfo->id = *id;
}

void setDefaultCreateDbOption(SCreateDbInfo *pDBInfo) {
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
  pDBInfo->cachelast = -1;

  pDBInfo->dbType = -1;
  pDBInfo->partitions = -1;
  
  memset(&pDBInfo->precision, 0, sizeof(SStrToken));
}

void setDefaultCreateTopicOption(SCreateDbInfo *pDBInfo) {
  setDefaultCreateDbOption(pDBInfo);

  pDBInfo->dbType = TSDB_DB_TYPE_TOPIC;
  pDBInfo->partitions = TSDB_DEFAULT_DB_PARTITON_OPTION;
}
