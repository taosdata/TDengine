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
#include "taosmsg.h"
#include "tast.h"
#include "tlog.h"
#include "tscSQLParser.h"
#include "tscSyntaxtreefunction.h"
#include "tschemautil.h"
#include "tsdb.h"
#include "tskiplist.h"
#include "tsqldef.h"
#include "tsqlfunction.h"
#include "tstoken.h"
#include "ttypes.h"
#include "tutil.h"

/*
 *
 * @date 2018-2-15
 * @version 0.2  operation for column filter
 * @author liaohj
 *
 * @Description parse tag query expression to build ast
 * ver 0.2, filter the result on first column with high priority to limit the candidate set
 * ver 0.3, pipeline filter in the form of: (a+2)/9 > 14
 *
 */

static tSQLSyntaxNode *tSQLSyntaxNodeCreate(SSchema *pSchema, int32_t numOfCols, SSQLToken *pToken);
static void            tSQLSyntaxNodeDestroy(tSQLSyntaxNode *pNode, void (*fp)(void *));

static tSQLSyntaxNode *createSyntaxTree(SSchema *pSchema, int32_t numOfCols, char *str, int32_t *i);
static void            destroySyntaxTree(tSQLSyntaxNode *);

static uint8_t isQueryOnPrimaryKey(const char *primaryColumnName, const tSQLSyntaxNode *pLeft,
                                   const tSQLSyntaxNode *pRight);

/*
 * Check the filter value type on the right hand side based on the column id on the left hand side,
 * the filter value type must be identical to field type for relational operation
 * As for binary arithmetic operation, it is not necessary to do so.
 */
static void reviseBinaryExprIfNecessary(tSQLSyntaxNode **pLeft, tSQLSyntaxNode **pRight, uint8_t *optr) {
  if (*optr >= TSDB_RELATION_LESS && *optr <= TSDB_RELATION_LIKE) {
    // make sure that the type of data on both sides of relational comparision are identical
    if ((*pLeft)->nodeType == TSQL_NODE_VALUE) {
      tVariantTypeSetType((*pLeft)->pVal, (*pRight)->pSchema->type);
    } else if ((*pRight)->nodeType == TSQL_NODE_VALUE) {
      tVariantTypeSetType((*pRight)->pVal, (*pLeft)->pSchema->type);
    }

  } else if (*optr >= TSDB_BINARY_OP_ADD && *optr <= TSDB_BINARY_OP_REMAINDER) {
    if ((*pLeft)->nodeType == TSQL_NODE_VALUE) {
      /* convert to int/bigint may cause the precision loss */
      tVariantTypeSetType((*pLeft)->pVal, TSDB_DATA_TYPE_DOUBLE);
    } else if ((*pRight)->nodeType == TSQL_NODE_VALUE) {
      /* convert to int/bigint may cause the precision loss */
      tVariantTypeSetType((*pRight)->pVal, TSDB_DATA_TYPE_DOUBLE);
    }
  }

  /*
   * for expressions that are suitable for switch principle,
   * switch left and left and right hand side in expr if possible
   */
  if ((*pLeft)->nodeType == TSQL_NODE_VALUE && (*pRight)->nodeType == TSQL_NODE_COL) {
    if (*optr >= TSDB_RELATION_LARGE && *optr <= TSDB_RELATION_LARGE_EQUAL && *optr != TSDB_RELATION_EQUAL) {
      SWAP(*pLeft, *pRight, tSQLSyntaxNode *);
    }

    switch (*optr) {
      case TSDB_RELATION_LARGE:
        (*optr) = TSDB_RELATION_LESS;
        break;
      case TSDB_RELATION_LESS:
        (*optr) = TSDB_RELATION_LARGE;
        break;
      case TSDB_RELATION_LARGE_EQUAL:
        (*optr) = TSDB_RELATION_LESS_EQUAL;
        break;
      case TSDB_RELATION_LESS_EQUAL:
        (*optr) = TSDB_RELATION_LARGE_EQUAL;
        break;
      default:;
        // for other type of operations, do nothing
    }
  }
}

static tSQLSyntaxNode *tSQLSyntaxNodeCreate(SSchema *pSchema, int32_t numOfCols, SSQLToken *pToken) {
  /* if the token is not a value, return false */
  if (pToken->type == TK_RP || (pToken->type != TK_INTEGER && pToken->type != TK_FLOAT && pToken->type != TK_ID &&
                                pToken->type != TK_TBNAME && pToken->type != TK_STRING && pToken->type != TK_BOOL)) {
    return NULL;
  }

  size_t          nodeSize = sizeof(tSQLSyntaxNode);
  tSQLSyntaxNode *pNode = NULL;

  if (pToken->type == TK_ID || pToken->type == TK_TBNAME) {
    int32_t i = 0;
    if (pToken->type == TK_ID) {
      do {
        SSQLToken tableToken = {0};
        extractTableNameFromToken(pToken, &tableToken);

        size_t len = strlen(pSchema[i].name);
        if (strncmp(pToken->z, pSchema[i].name, pToken->n) == 0 && pToken->n == len) break;
      } while (++i < numOfCols);

      if (i == numOfCols) {  // column name is not valid, parse the expression failed
        return NULL;
      }
    }

    nodeSize += sizeof(SSchema);

    pNode = calloc(1, nodeSize);
    pNode->pSchema = (struct SSchema *)((char *)pNode + sizeof(tSQLSyntaxNode));
    pNode->nodeType = TSQL_NODE_COL;

    if (pToken->type == TK_ID) {
      pNode->colId = (int16_t)pSchema[i].colId;
      memcpy(pNode->pSchema, &pSchema[i], sizeof(SSchema));
    } else {
      pNode->colId = -1;
      pNode->pSchema->type = TSDB_DATA_TYPE_BINARY;
      pNode->pSchema->bytes = TSDB_METER_NAME_LEN;
      strcpy(pNode->pSchema->name, TSQL_TBNAME_L);
      pNode->pSchema->colId = -1;
    }

  } else {
    nodeSize += sizeof(tVariant);
    pNode = calloc(1, nodeSize);
    pNode->pVal = (tVariant *)((char *)pNode + sizeof(tSQLSyntaxNode));

    toTSDBType(pToken->type);
    tVariantCreate(pNode->pVal, pToken);
    pNode->nodeType = TSQL_NODE_VALUE;
    pNode->colId = -1;
  }

  return pNode;
}

uint8_t getBinaryExprOptr(SSQLToken *pToken) {
  switch (pToken->type) {
    case TK_LT:
      return TSDB_RELATION_LESS;
    case TK_LE:
      return TSDB_RELATION_LESS_EQUAL;
    case TK_GT:
      return TSDB_RELATION_LARGE;
    case TK_GE:
      return TSDB_RELATION_LARGE_EQUAL;
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
    default: { return 0; }
  }
}

// previous generated expr is reduced as the left child
static tSQLSyntaxNode *parseRemainStr(char *pstr, tSQLBinaryExpr *pExpr, SSchema *pSchema, int32_t optr,
                                      int32_t numOfCols, int32_t *i) {
  // set the previous generated node as the left child of new root
  tSQLSyntaxNode *pLeft = malloc(sizeof(tSQLSyntaxNode));
  pLeft->nodeType = TSQL_NODE_EXPR;
  pLeft->pExpr = pExpr;

  // remain is the right child
  tSQLSyntaxNode *pRight = createSyntaxTree(pSchema, numOfCols, pstr, i);
  if (pRight == NULL || (pRight->nodeType == TSQL_NODE_COL && pLeft->nodeType != TSQL_NODE_VALUE) ||
      (pLeft->nodeType == TSQL_NODE_VALUE && pRight->nodeType != TSQL_NODE_COL)) {
    tSQLSyntaxNodeDestroy(pLeft, NULL);
    tSQLSyntaxNodeDestroy(pRight, NULL);
    return NULL;
  }

  tSQLBinaryExpr *pNewExpr = (tSQLBinaryExpr *)calloc(1, sizeof(tSQLBinaryExpr));
  uint8_t         k = optr;
  reviseBinaryExprIfNecessary(&pLeft, &pRight, &k);
  pNewExpr->pLeft = pLeft;
  pNewExpr->pRight = pRight;
  pNewExpr->nSQLBinaryOptr = k;

  pNewExpr->filterOnPrimaryKey = isQueryOnPrimaryKey(pSchema[0].name, pLeft, pRight);

  tSQLSyntaxNode *pn = malloc(sizeof(tSQLSyntaxNode));
  pn->nodeType = TSQL_NODE_EXPR;
  pn->pExpr = pNewExpr;

  return pn;
}

uint8_t isQueryOnPrimaryKey(const char *primaryColumnName, const tSQLSyntaxNode *pLeft, const tSQLSyntaxNode *pRight) {
  if (pLeft->nodeType == TSQL_NODE_COL) {
    // if left node is the primary column,return true
    return (strcmp(primaryColumnName, pLeft->pSchema->name) == 0) ? 1 : 0;
  } else {
    // if any children have query on primary key, their parents are also keep this value
    return ((pLeft->nodeType == TSQL_NODE_EXPR && pLeft->pExpr->filterOnPrimaryKey == 1) ||
            (pRight->nodeType == TSQL_NODE_EXPR && pRight->pExpr->filterOnPrimaryKey == 1)) == true
               ? 1
               : 0;
  }
}

static tSQLSyntaxNode *createSyntaxTree(SSchema *pSchema, int32_t numOfCols, char *str, int32_t *i) {
  SSQLToken t0;

  t0 = tStrGetToken(str, i, false, 0, NULL);
  if (t0.n == 0) {
    return NULL;
  }

  tSQLSyntaxNode *pLeft = NULL;
  if (t0.type == TK_LP) {  // start new left child branch
    pLeft = createSyntaxTree(pSchema, numOfCols, str, i);
  } else {
    if (t0.type == TK_RP) {
      return NULL;
    }
    pLeft = tSQLSyntaxNodeCreate(pSchema, numOfCols, &t0);
  }

  if (pLeft == NULL) {
    return NULL;
  }

  t0 = tStrGetToken(str, i, false, 0, NULL);
  if (t0.n == 0 || t0.type == TK_RP) {
    if (pLeft->nodeType != TSQL_NODE_EXPR) {  // if left is not the expr, it is not a legal expr
      tSQLSyntaxNodeDestroy(pLeft, NULL);
      return NULL;
    }

    return pLeft;
  }

  // get the operator of expr
  uint8_t optr = getBinaryExprOptr(&t0);
  if (optr == 0) {
    pError("not support binary operator:%d", t0.type);
    tSQLSyntaxNodeDestroy(pLeft, NULL);
    return NULL;
  }

  assert(pLeft != NULL);
  tSQLSyntaxNode *pRight = NULL;

  if (t0.type == TK_AND || t0.type == TK_OR || t0.type == TK_LP) {
    pRight = createSyntaxTree(pSchema, numOfCols, str, i);
  } else {
    /*
     * In case that pLeft is a field identification,
     * we parse the value in expression according to queried field type,
     * if we do not get the information, in case of value of field presented first,
     * we revised the value after the binary expression is completed.
     */
    t0 = tStrGetToken(str, i, true, 0, NULL);
    if (t0.n == 0) {
      tSQLSyntaxNodeDestroy(pLeft, NULL);  // illegal expression
      return NULL;
    }

    if (t0.type == TK_LP) {
      pRight = createSyntaxTree(pSchema, numOfCols, str, i);
    } else {
      pRight = tSQLSyntaxNodeCreate(pSchema, numOfCols, &t0);
    }
  }

  if (pRight == NULL) {
    tSQLSyntaxNodeDestroy(pLeft, NULL);
    return NULL;
  }

  /* create binary expr as the child of new parent node */
  tSQLBinaryExpr *pBinExpr = (tSQLBinaryExpr *)calloc(1, sizeof(tSQLBinaryExpr));
  reviseBinaryExprIfNecessary(&pLeft, &pRight, &optr);

  pBinExpr->filterOnPrimaryKey = isQueryOnPrimaryKey(pSchema[0].name, pLeft, pRight);
  pBinExpr->pLeft = pLeft;
  pBinExpr->pRight = pRight;
  pBinExpr->nSQLBinaryOptr = optr;

  t0 = tStrGetToken(str, i, true, 0, NULL);

  if (t0.n == 0 || t0.type == TK_RP) {
    tSQLSyntaxNode *pn = malloc(sizeof(tSQLSyntaxNode));
    pn->nodeType = TSQL_NODE_EXPR;
    pn->pExpr = pBinExpr;
    pn->colId = -1;
    return pn;
  } else {
    uint8_t localOptr = getBinaryExprOptr(&t0);
    if (localOptr == 0) {
      pError("not support binary operator:%d", t0.type);
      free(pBinExpr);
      return NULL;
    }

    return parseRemainStr(str, pBinExpr, pSchema, localOptr, numOfCols, i);
  }
}

void tSQLBinaryExprFromString(tSQLBinaryExpr **pExpr, SSchema *pSchema, int32_t numOfCols, char *src, int32_t len) {
  *pExpr = NULL;
  if (len <= 0 || src == NULL || pSchema == NULL || numOfCols <= 0) {
    return;
  }

  int32_t         pos = 0;
  tSQLSyntaxNode *pStxNode = createSyntaxTree(pSchema, numOfCols, src, &pos);
  if (pStxNode != NULL) {
    assert(pStxNode->nodeType == TSQL_NODE_EXPR);
    *pExpr = pStxNode->pExpr;
    free(pStxNode);
  }
}

int32_t tSQLBinaryExprToStringImpl(tSQLSyntaxNode *pNode, char *dst, uint8_t type) {
  int32_t len = 0;
  if (type == TSQL_NODE_EXPR) {
    *dst = '(';
    tSQLBinaryExprToString(pNode->pExpr, dst + 1, &len);
    len += 2;
    *(dst + len - 1) = ')';
  } else if (type == TSQL_NODE_COL) {
    len = sprintf(dst, "%s", pNode->pSchema->name);
  } else {
    len = tVariantToString(pNode->pVal, dst);
  }
  return len;
}

// TODO REFACTOR WITH SQL PARSER
static char *tSQLOptrToString(uint8_t optr, char *dst) {
  switch (optr) {
    case TSDB_RELATION_LESS: {
      *dst = '<';
      dst += 1;
      break;
    }
    case TSDB_RELATION_LESS_EQUAL: {
      *dst = '<';
      *(dst + 1) = '=';
      dst += 2;
      break;
    }
    case TSDB_RELATION_EQUAL: {
      *dst = '=';
      dst += 1;
      break;
    }
    case TSDB_RELATION_LARGE: {
      *dst = '>';
      dst += 1;
      break;
    }
    case TSDB_RELATION_LARGE_EQUAL: {
      *dst = '>';
      *(dst + 1) = '=';
      dst += 2;
      break;
    }
    case TSDB_RELATION_NOT_EQUAL: {
      *dst = '<';
      *(dst + 1) = '>';
      dst += 2;
      break;
    }
    case TSDB_RELATION_OR: {
      memcpy(dst, "or", 2);
      dst += 2;
      break;
    }
    case TSDB_RELATION_AND: {
      memcpy(dst, "and", 3);
      dst += 3;
      break;
    }
    default:;
  }
  return dst;
}

void tSQLBinaryExprToString(tSQLBinaryExpr *pExpr, char *dst, int32_t *len) {
  if (pExpr == NULL) {
    *dst = 0;
    *len = 0;
    return;
  }

  int32_t lhs = tSQLBinaryExprToStringImpl(pExpr->pLeft, dst, pExpr->pLeft->nodeType);
  dst += lhs;
  *len = lhs;

  char *start = tSQLOptrToString(pExpr->nSQLBinaryOptr, dst);
  *len += (start - dst);

  *len += tSQLBinaryExprToStringImpl(pExpr->pRight, start, pExpr->pRight->nodeType);
}

static void UNUSED_FUNC destroySyntaxTree(tSQLSyntaxNode *pNode) { tSQLSyntaxNodeDestroy(pNode, NULL); }

static void tSQLSyntaxNodeDestroy(tSQLSyntaxNode *pNode, void (*fp)(void *)) {
  if (pNode == NULL) {
    return;
  }

  if (pNode->nodeType == TSQL_NODE_EXPR) {
    tSQLBinaryExprDestroy(&pNode->pExpr, fp);
  } else if (pNode->nodeType == TSQL_NODE_VALUE) {
    tVariantDestroy(pNode->pVal);
  }

  free(pNode);
}

void tSQLBinaryExprDestroy(tSQLBinaryExpr **pExpr, void (*fp)(void *)) {
  if (*pExpr == NULL) {
    return;
  }

  tSQLSyntaxNodeDestroy((*pExpr)->pLeft, fp);
  tSQLSyntaxNodeDestroy((*pExpr)->pRight, fp);

  if (fp != NULL) {
    fp((*pExpr)->info);
  }

  free(*pExpr);
  *pExpr = NULL;
}

static void setInitialValueForRangeQueryCondition(tSKipListQueryCond *q, int8_t type) {
  q->lowerBndRelOptr = TSDB_RELATION_LARGE;
  q->upperBndRelOptr = TSDB_RELATION_LESS;

  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT: {
      q->upperBnd.nType = TSDB_DATA_TYPE_BIGINT;
      q->lowerBnd.nType = TSDB_DATA_TYPE_BIGINT;

      q->upperBnd.i64Key = INT64_MAX;
      q->lowerBnd.i64Key = INT64_MIN;
      break;
    };
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE: {
      q->upperBnd.nType = TSDB_DATA_TYPE_DOUBLE;
      q->lowerBnd.nType = TSDB_DATA_TYPE_DOUBLE;
      q->upperBnd.dKey = DBL_MAX;
      q->lowerBnd.dKey = -DBL_MIN;
      break;
    };
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_BINARY: {
      q->upperBnd.nType = type;
      q->upperBnd.pz = NULL;
      q->upperBnd.nLen = -1;

      q->lowerBnd.nType = type;
      q->lowerBnd.pz = NULL;
      q->lowerBnd.nLen = -1;
    }
  }
}

static void tSQLDoFilterInitialResult(tSkipList *pSkipList, bool (*fp)(), tQueryInfo *queryColInfo,
                                      tQueryResultset *result) {
  // primary key filter, search according to skiplist
  if (queryColInfo->colIdx == 0 && queryColInfo->optr != TSDB_RELATION_LIKE) {
    tSKipListQueryCond q;
    setInitialValueForRangeQueryCondition(&q, queryColInfo->q.nType);

    switch (queryColInfo->optr) {
      case TSDB_RELATION_EQUAL: {
        result->num =
            tSkipListPointQuery(pSkipList, &queryColInfo->q, 1, INCLUDE_POINT_QUERY, (tSkipListNode ***)&result->pRes);
        break;
      }
      case TSDB_RELATION_NOT_EQUAL: {
        result->num =
            tSkipListPointQuery(pSkipList, &queryColInfo->q, 1, EXCLUDE_POINT_QUERY, (tSkipListNode ***)&result->pRes);
        break;
      }
      case TSDB_RELATION_LESS_EQUAL: {
        tVariantAssign(&q.upperBnd, &queryColInfo->q);
        q.upperBndRelOptr = queryColInfo->optr;
        result->num = tSkipListQuery(pSkipList, &q, (tSkipListNode ***)&result->pRes);
        break;
      }
      case TSDB_RELATION_LESS: {
        tVariantAssign(&q.upperBnd, &queryColInfo->q);
        result->num = tSkipListQuery(pSkipList, &q, (tSkipListNode ***)&result->pRes);
        break;
      }
      case TSDB_RELATION_LARGE: {
        tVariantAssign(&q.lowerBnd, &queryColInfo->q);
        result->num = tSkipListQuery(pSkipList, &q, (tSkipListNode ***)&result->pRes);
        break;
      }
      case TSDB_RELATION_LARGE_EQUAL: {
        tVariantAssign(&q.lowerBnd, &queryColInfo->q);
        q.lowerBndRelOptr = queryColInfo->optr;
        result->num = tSkipListQuery(pSkipList, &q, (tSkipListNode ***)&result->pRes);
        break;
      }
      default:
        pTrace("skiplist:%p, unsupport query operator:%d", pSkipList, queryColInfo->optr);
    }

    tSkipListDestroyKey(&q.upperBnd);
    tSkipListDestroyKey(&q.lowerBnd);
  } else {
    /*
     * Brutal force scan the whole skiplit to find the appropriate result,
     * since the filter is not applied to indexed column.
     */
    result->num = tSkipListIterateList(pSkipList, (tSkipListNode ***)&result->pRes, fp, queryColInfo);
  }
}

/*
 * qsort comparator
 * sort the result to ensure meters with the same gid is grouped together
 */
static int32_t compareByAddr(const void *pLeft, const void *pRight) {
  int64_t p1 = (int64_t) * ((tSkipListNode **)pLeft);
  int64_t p2 = (int64_t) * ((tSkipListNode **)pRight);

  DEFAULT_COMP(p1, p2);
}

int32_t merge(tQueryResultset *pLeft, tQueryResultset *pRight, tQueryResultset *pFinalRes) {
  assert(pFinalRes->pRes == 0);

  pFinalRes->pRes = calloc((size_t)(pLeft->num + pRight->num), POINTER_BYTES);
  pFinalRes->num = 0;

  // sort according to address
  tSkipListNode **pLeftNodes = (tSkipListNode **)pLeft->pRes;
  qsort(pLeftNodes, pLeft->num, sizeof(pLeft->pRes[0]), compareByAddr);

  tSkipListNode **pRightNodes = (tSkipListNode **)pRight->pRes;
  qsort(pRightNodes, pRight->num, sizeof(pRight->pRes[0]), compareByAddr);

  int32_t i = 0, j = 0;

  // merge two sorted arrays in O(n) time
  while (i < pLeft->num && j < pRight->num) {
    int64_t ret = (int64_t)pLeftNodes[i] - (int64_t)pRightNodes[j];

    if (ret < 0) {
      pFinalRes->pRes[pFinalRes->num++] = pLeftNodes[i++];
    } else if (ret > 0) {
      pFinalRes->pRes[pFinalRes->num++] = pRightNodes[j++];
    } else {  // pNode->key > pkey[i]
      pFinalRes->pRes[pFinalRes->num++] = pRightNodes[j++];
      i++;
    }
  }

  while (i < pLeft->num) {
    pFinalRes->pRes[pFinalRes->num++] = pLeftNodes[i++];
  }

  while (j < pRight->num) {
    pFinalRes->pRes[pFinalRes->num++] = pRightNodes[j++];
  }

  return pFinalRes->num;
}

int32_t intersect(tQueryResultset *pLeft, tQueryResultset *pRight, tQueryResultset *pFinalRes) {
  int64_t num = MIN(pLeft->num, pRight->num);

  assert(pFinalRes->pRes == 0);

  pFinalRes->pRes = calloc(num, POINTER_BYTES);
  pFinalRes->num = 0;

  // sort according to address
  tSkipListNode **pLeftNodes = (tSkipListNode **)pLeft->pRes;
  qsort(pLeftNodes, pLeft->num, sizeof(pLeft->pRes[0]), compareByAddr);

  tSkipListNode **pRightNodes = (tSkipListNode **)pRight->pRes;
  qsort(pRightNodes, pRight->num, sizeof(pRight->pRes[0]), compareByAddr);

  int32_t i = 0, j = 0;
  // merge two sorted arrays in O(n) time
  while (i < pLeft->num && j < pRight->num) {
    int64_t ret = (int64_t)pLeftNodes[i] - (int64_t)pRightNodes[j];

    if (ret < 0) {
      i++;
    } else if (ret > 0) {
      j++;
    } else {  // pNode->key > pkey[i]
      pFinalRes->pRes[pFinalRes->num++] = pRightNodes[j];
      i++;
      j++;
    }
  }

  return pFinalRes->num;
}

/*
 * traverse the result and apply the function to each item to check if the item is qualified or not
 */
static void tSQLListTraverseOnResult(struct tSQLBinaryExpr *pExpr, __result_filter_fn_t fp, tQueryResultset *pResult) {
  assert(pExpr->pLeft->nodeType == TSQL_NODE_COL && pExpr->pRight->nodeType == TSQL_NODE_VALUE);

  // brutal force scan the result list and check for each item in the list
  int64_t num = pResult->num;
  for (int32_t i = 0, j = 0; i < pResult->num; ++i) {
    if (fp == NULL || (fp(pResult->pRes[i], pExpr->info) == true)) {
      pResult->pRes[j++] = pResult->pRes[i];
    } else {
      num--;
    }
  }

  pResult->num = num;
}

static bool filterItem(tSQLBinaryExpr *pExpr, const void *pItem, SBinaryFilterSupp *param) {
  tSQLSyntaxNode *pLeft = pExpr->pLeft;
  tSQLSyntaxNode *pRight = pExpr->pRight;

  /*
   * non-leaf nodes, recursively traverse the syntax tree in the post-root order
   */
  if (pLeft->nodeType == TSQL_NODE_EXPR && pRight->nodeType == TSQL_NODE_EXPR) {
    if (pExpr->nSQLBinaryOptr == TSDB_RELATION_OR) {  // or
      if (filterItem(pLeft->pExpr, pItem, param)) {
        return true;
      }

      // left child does not satisfy the query condition, try right child
      return filterItem(pRight->pExpr, pItem, param);
    } else {  // and
      if (!filterItem(pLeft->pExpr, pItem, param)) {
        return false;
      }

      return filterItem(pRight->pExpr, pItem, param);
    }
  }

  // handle the leaf node
  assert(pLeft->nodeType == TSQL_NODE_COL && pRight->nodeType == TSQL_NODE_VALUE);
  param->setupInfoFn(pExpr, param->pExtInfo);

  return param->fp(pItem, pExpr->info);
}

/**
 * Apply the filter expression on non-indexed tag columns to each element in the result list, which is generated
 * by filtering on indexed tag column. So the whole result set only needs to be iterated once to generate
 * result that is satisfied to the filter expression, no matter how the filter expression consisting of.
 *
 * @param pExpr     filter expression on non-indexed tag columns.
 * @param pResult   results from filter on the indexed tag column, which is usually the first tag column
 * @param pSchema   tag schemas
 * @param fp        filter callback function
 */
static void tSQLBinaryTraverseOnResult(tSQLBinaryExpr *pExpr, tQueryResultset *pResult, SBinaryFilterSupp *param) {
  int32_t n = 0;
  for (int32_t i = 0; i < pResult->num; ++i) {
    void *pItem = pResult->pRes[i];

    if (filterItem(pExpr, pItem, param)) {
      pResult->pRes[n++] = pResult->pRes[i];
    }
  }

  pResult->num = n;
}

static void tSQLBinaryTraverseOnSkipList(tSQLBinaryExpr *pExpr, tQueryResultset *pResult, tSkipList *pSkipList,
                                         SBinaryFilterSupp *param) {
  int32_t           n = 0;
  SSkipListIterator iter = {0};

  int32_t ret = tSkipListIteratorReset(pSkipList, &iter);
  assert(ret == 0);

  pResult->pRes = calloc(pSkipList->nSize, POINTER_BYTES);

  while (tSkipListIteratorNext(&iter)) {
    tSkipListNode *pNode = tSkipListIteratorGet(&iter);
    if (filterItem(pExpr, pNode, param)) {
      pResult->pRes[n++] = pNode;
    }
  }

  pResult->num = n;
}

// post-root order traverse syntax tree
void tSQLBinaryExprTraverse(tSQLBinaryExpr *pExpr, tSkipList *pSkipList, tQueryResultset *result,
                            SBinaryFilterSupp *param) {
  if (pExpr == NULL) {
    return;
  }

  tSQLSyntaxNode *pLeft = pExpr->pLeft;
  tSQLSyntaxNode *pRight = pExpr->pRight;

  // recursive traverse left child branch
  if (pLeft->nodeType == TSQL_NODE_EXPR || pRight->nodeType == TSQL_NODE_EXPR) {
    uint8_t weight = pLeft->pExpr->filterOnPrimaryKey + pRight->pExpr->filterOnPrimaryKey;

    if (weight == 0 && result->num > 0 && pSkipList == NULL) {
      /**
       * Perform the filter operation based on the initial filter result, which is obtained from filtering from index.
       * Since no index presented, the filter operation is done by scan all elements in the result set.
       *
       * if the query is a high selectivity filter, only small portion of meters are retrieved.
       */
      tSQLBinaryTraverseOnResult(pExpr, result, param);
    } else if (weight == 0) {
      /**
       * apply the hierarchical expression to every node in skiplist for find the qualified nodes
       */
      assert(result->num == 0);
      tSQLBinaryTraverseOnSkipList(pExpr, result, pSkipList, param);
    } else if (weight == 2 || (weight == 1 && pExpr->nSQLBinaryOptr == TSDB_RELATION_OR)) {
      tQueryResultset rLeft = {0};
      tQueryResultset rRight = {0};

      tSQLBinaryExprTraverse(pLeft->pExpr, pSkipList, &rLeft, param);
      tSQLBinaryExprTraverse(pRight->pExpr, pSkipList, &rRight, param);

      if (pExpr->nSQLBinaryOptr == TSDB_RELATION_AND) {  // CROSS
        intersect(&rLeft, &rRight, result);
      } else if (pExpr->nSQLBinaryOptr == TSDB_RELATION_OR) {  // or
        merge(&rLeft, &rRight, result);
      } else {
        assert(false);
      }

      free(rLeft.pRes);
      free(rRight.pRes);
    } else {
      /*
       * (weight == 1 && pExpr->nSQLBinaryOptr == TSDB_RELATION_AND) is handled here
       *
       * first, we filter results based on the skiplist index, which is the initial filter stage,
       * then, we conduct the secondary filter operation based on the result from the initial filter stage.
       */
      assert(pExpr->nSQLBinaryOptr == TSDB_RELATION_AND);

      tSQLBinaryExpr *pFirst = NULL;
      tSQLBinaryExpr *pSecond = NULL;
      if (pLeft->pExpr->filterOnPrimaryKey == 1) {
        pFirst = pLeft->pExpr;
        pSecond = pRight->pExpr;
      } else {
        pFirst = pRight->pExpr;
        pSecond = pLeft->pExpr;
      }

      assert(pFirst != pSecond && pFirst != NULL && pSecond != NULL);

      // we filter the result based on the skiplist index in the first place
      tSQLBinaryExprTraverse(pFirst, pSkipList, result, param);

      /*
       * recursively perform the filter operation based on the initial results,
       * So, we do not set the skiplist index as a parameter
       */
      tSQLBinaryExprTraverse(pSecond, NULL, result, param);
    }
  } else {  // column project
    assert(pLeft->nodeType == TSQL_NODE_COL && pRight->nodeType == TSQL_NODE_VALUE);

    param->setupInfoFn(pExpr, param->pExtInfo);
    if (pSkipList == NULL) {
      tSQLListTraverseOnResult(pExpr, param->fp, result);
    } else {
      assert(result->num == 0);
      tSQLDoFilterInitialResult(pSkipList, param->fp, pExpr->info, result);
    }
  }
}

void tSQLBinaryExprCalcTraverse(tSQLBinaryExpr *pExprs, int32_t numOfRows, char *pOutput, void *param, int32_t order,
                                char *(*getSourceDataBlock)(void *, char *, int32_t)) {
  if (pExprs == NULL) {
    return;
  }

  tSQLSyntaxNode *pLeft = pExprs->pLeft;
  tSQLSyntaxNode *pRight = pExprs->pRight;

  /* the left output has result from the left child syntax tree */
  char *pLeftOutput = (char*)malloc(sizeof(int64_t) * numOfRows);
  if (pLeft->nodeType == TSQL_NODE_EXPR) {
    tSQLBinaryExprCalcTraverse(pLeft->pExpr, numOfRows, pLeftOutput, param, order, getSourceDataBlock);
  }

  /* the right output has result from the right child syntax tree */
  char *pRightOutput = malloc(sizeof(int64_t) * numOfRows);
  if (pRight->nodeType == TSQL_NODE_EXPR) {
    tSQLBinaryExprCalcTraverse(pRight->pExpr, numOfRows, pRightOutput, param, order, getSourceDataBlock);
  }

  if (pLeft->nodeType == TSQL_NODE_EXPR) {
    if (pRight->nodeType == TSQL_NODE_EXPR) {
      /*
       * exprLeft + exprRight
       * the type of returned value of one expression is always double float precious
       */
      _bi_consumer_fn_t fp = tGetBiConsumerFn(TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_DOUBLE, pExprs->nSQLBinaryOptr);
      fp(pLeftOutput, pRightOutput, numOfRows, numOfRows, pOutput, order);

    } else if (pRight->nodeType == TSQL_NODE_COL) {  // exprLeft + columnRight
      _bi_consumer_fn_t fp = tGetBiConsumerFn(TSDB_DATA_TYPE_DOUBLE, pRight->pSchema->type, pExprs->nSQLBinaryOptr);
      // set input buffer
      char *pInputData = getSourceDataBlock(param, pRight->pSchema->name, pRight->colId);
      fp(pLeftOutput, pInputData, numOfRows, numOfRows, pOutput, order);

    } else if (pRight->nodeType == TSQL_NODE_VALUE) {  // exprLeft + 12
      _bi_consumer_fn_t fp = tGetBiConsumerFn(TSDB_DATA_TYPE_DOUBLE, pRight->pVal->nType, pExprs->nSQLBinaryOptr);
      fp(pLeftOutput, &pRight->pVal->i64Key, numOfRows, 1, pOutput, order);
    }
  } else if (pLeft->nodeType == TSQL_NODE_COL) {
    // column data specified on left-hand-side
    char *pLeftInputData = getSourceDataBlock(param, pLeft->pSchema->name, pLeft->colId);
    if (pRight->nodeType == TSQL_NODE_EXPR) {  // columnLeft + expr2
      _bi_consumer_fn_t fp = tGetBiConsumerFn(pLeft->pSchema->type, TSDB_DATA_TYPE_DOUBLE, pExprs->nSQLBinaryOptr);
      fp(pLeftInputData, pRightOutput, numOfRows, numOfRows, pOutput, order);

    } else if (pRight->nodeType == TSQL_NODE_COL) {  // columnLeft + columnRight
      // column data specified on right-hand-side
      char *pRightInputData = getSourceDataBlock(param, pRight->pSchema->name, pRight->colId);

      _bi_consumer_fn_t fp = tGetBiConsumerFn(pLeft->pSchema->type, pRight->pSchema->type, pExprs->nSQLBinaryOptr);
      fp(pLeftInputData, pRightInputData, numOfRows, numOfRows, pOutput, order);

    } else if (pRight->nodeType == TSQL_NODE_VALUE) {  // columnLeft + 12
      _bi_consumer_fn_t fp = tGetBiConsumerFn(pLeft->pSchema->type, pRight->pVal->nType, pExprs->nSQLBinaryOptr);
      fp(pLeftInputData, &pRight->pVal->i64Key, numOfRows, 1, pOutput, order);
    }
  } else {
    // column data specified on left-hand-side
    if (pRight->nodeType == TSQL_NODE_EXPR) {  // 12 + expr2
      _bi_consumer_fn_t fp = tGetBiConsumerFn(pLeft->pVal->nType, TSDB_DATA_TYPE_DOUBLE, pExprs->nSQLBinaryOptr);
      fp(&pLeft->pVal->i64Key, pRightOutput, 1, numOfRows, pOutput, order);

    } else if (pRight->nodeType == TSQL_NODE_COL) {  // 12 + columnRight
      // column data specified on right-hand-side
      char *            pRightInputData = getSourceDataBlock(param, pRight->pSchema->name, pRight->colId);
      _bi_consumer_fn_t fp = tGetBiConsumerFn(pLeft->pVal->nType, pRight->pSchema->type, pExprs->nSQLBinaryOptr);
      fp(&pLeft->pVal->i64Key, pRightInputData, 1, numOfRows, pOutput, order);

    } else if (pRight->nodeType == TSQL_NODE_VALUE) {  // 12 + 12
      _bi_consumer_fn_t fp = tGetBiConsumerFn(pLeft->pVal->nType, pRight->pVal->nType, pExprs->nSQLBinaryOptr);
      fp(&pLeft->pVal->i64Key, &pRight->pVal->i64Key, 1, 1, pOutput, order);
    }
  }

  free(pLeftOutput);
  free(pRightOutput);
}

void tSQLBinaryExprTrv(tSQLBinaryExpr *pExprs, int32_t *val, int16_t *ids) {
  if (pExprs == NULL) {
    return;
  }

  tSQLSyntaxNode *pLeft = pExprs->pLeft;
  tSQLSyntaxNode *pRight = pExprs->pRight;

  // recursive traverse left child branch
  if (pLeft->nodeType == TSQL_NODE_EXPR) {
    tSQLBinaryExprTrv(pLeft->pExpr, val, ids);
  } else if (pLeft->nodeType == TSQL_NODE_COL) {
    ids[*val] = pLeft->pSchema->colId;
    (*val) += 1;
  }

  if (pRight->nodeType == TSQL_NODE_EXPR) {
    tSQLBinaryExprTrv(pRight->pExpr, val, ids);
  } else if (pRight->nodeType == TSQL_NODE_COL) {
    ids[*val] = pRight->pSchema->colId;
    (*val) += 1;
  }
}

void tQueryResultClean(tQueryResultset *pRes) {
  if (pRes == NULL) {
    return;
  }

  tfree(pRes->pRes);
  pRes->num = 0;
}
