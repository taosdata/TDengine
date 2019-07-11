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

#include <assert.h>
#include <float.h>
#include <math.h>
#include <stdbool.h>
#include <stdlib.h>

#include "taosmsg.h"
#include "tast.h"
#include "tlog.h"
#include "tscSyntaxtreefunction.h"
#include "tschemautil.h"
#include "tsdb.h"
#include "tskiplist.h"
#include "tsqlfunction.h"
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
static void tSQLSyntaxNodeDestroy(tSQLSyntaxNode *pNode);

static tSQLSyntaxNode *createSyntaxTree(SSchema *pSchema, int32_t numOfCols, char *str, int32_t *i);
static void destroySyntaxTree(tSQLSyntaxNode *);

static void tSQLListTraversePrepare(tQueryInfo *colInfo, SSchema *pSchema, int32_t numOfCols, SSchema *pOneColSchema,
                                    uint8_t optr, tVariant *val);

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

  // switch left and left and right hand side in expr
  if ((*pLeft)->nodeType == TSQL_NODE_VALUE && (*pRight)->nodeType == TSQL_NODE_COL) {
    SWAP(*pLeft, *pRight);

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

  int32_t         i = 0;
  size_t          nodeSize = sizeof(tSQLSyntaxNode);
  tSQLSyntaxNode *pNode = NULL;

  if (pToken->type == TK_ID || pToken->type == TK_TBNAME) {
    if (pToken->type == TK_ID) {
      do {
        size_t len = strlen(pSchema[i].name);
        if (strncmp(pToken->z, pSchema[i].name, pToken->n) == 0 && pToken->n == len) break;
      } while (++i < numOfCols);

      if (i == numOfCols) {
        // column name is not valid, parse the expression failed
        return NULL;
      }
    }

    nodeSize += sizeof(SSchema);

    pNode = malloc(nodeSize);
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
    pNode = malloc(nodeSize);
    pNode->pVal = (tVariant *)((char *)pNode + sizeof(tSQLSyntaxNode));

    toTSDBType(pToken->type);
    tVariantCreate(pNode->pVal, pToken);
    pNode->nodeType = TSQL_NODE_VALUE;
    pNode->colId = -1;
  }

  return pNode;
}

static uint8_t getBinaryExprOptr(SSQLToken *pToken) {
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
    tSQLSyntaxNodeDestroy(pLeft);
    tSQLSyntaxNodeDestroy(pRight);
    return NULL;
  }

  tSQLBinaryExpr *pNewExpr = (tSQLBinaryExpr *)malloc(sizeof(tSQLBinaryExpr));
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
    // if any children have query on primary key, their parents are also keep
    // this value
    return ((pLeft->nodeType == TSQL_NODE_EXPR && pLeft->pExpr->filterOnPrimaryKey == 1) ||
            (pRight->nodeType == TSQL_NODE_EXPR && pRight->pExpr->filterOnPrimaryKey == 1)) == true
               ? 1
               : 0;
  }
}

static tSQLSyntaxNode *createSyntaxTree(SSchema *pSchema, int32_t numOfCols, char *str, int32_t *i) {
  SSQLToken t0 = {0};

  tStrGetToken(str, i, &t0, false);
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

  tStrGetToken(str, i, &t0, false);
  if (t0.n == 0 || t0.type == TK_RP) {
    if (pLeft->nodeType != TSQL_NODE_EXPR) {
      // if left is not the expr, it is not a legal expr
      tSQLSyntaxNodeDestroy(pLeft);
      return NULL;
    }

    return pLeft;
  }

  // get the operator of expr
  uint8_t optr = getBinaryExprOptr(&t0);
  if (optr <= 0) {
    pError("not support binary operator:%d", t0.type);
    tSQLSyntaxNodeDestroy(pLeft);
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
    tStrGetToken(str, i, &t0, true);
    if (t0.n == 0) {
      tSQLSyntaxNodeDestroy(pLeft);  // illegal expression
      return NULL;
    }

    if (t0.type == TK_LP) {
      pRight = createSyntaxTree(pSchema, numOfCols, str, i);
    } else {
      pRight = tSQLSyntaxNodeCreate(pSchema, numOfCols, &t0);
    }
  }

  if (pRight == NULL) {
    tSQLSyntaxNodeDestroy(pLeft);
    return NULL;
  }

  /* create binary expr as the child of new parent node */
  tSQLBinaryExpr *pBinExpr = (tSQLBinaryExpr *)malloc(sizeof(tSQLBinaryExpr));
  reviseBinaryExprIfNecessary(&pLeft, &pRight, &optr);

  pBinExpr->filterOnPrimaryKey = isQueryOnPrimaryKey(pSchema[0].name, pLeft, pRight);
  pBinExpr->pLeft = pLeft;
  pBinExpr->pRight = pRight;
  pBinExpr->nSQLBinaryOptr = optr;

  tStrGetToken(str, i, &t0, true);

  if (t0.n == 0 || t0.type == TK_RP) {
    tSQLSyntaxNode *pn = malloc(sizeof(tSQLSyntaxNode));
    pn->nodeType = TSQL_NODE_EXPR;
    pn->pExpr = pBinExpr;
    pn->colId = -1;
    return pn;
  } else {
    int32_t optr = getBinaryExprOptr(&t0);
    if (optr <= 0) {
      pError("not support binary operator:%d", t0.type);
      return NULL;
    }

    return parseRemainStr(str, pBinExpr, pSchema, optr, numOfCols, i);
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
  }

  int32_t lhs = tSQLBinaryExprToStringImpl(pExpr->pLeft, dst, pExpr->pLeft->nodeType);
  dst += lhs;
  *len = lhs;

  char *start = tSQLOptrToString(pExpr->nSQLBinaryOptr, dst);
  *len += (start - dst);

  *len += tSQLBinaryExprToStringImpl(pExpr->pRight, start, pExpr->pRight->nodeType);
}

static void UNUSED_FUNC destroySyntaxTree(tSQLSyntaxNode *pNode) { tSQLSyntaxNodeDestroy(pNode); }

static void tSQLSyntaxNodeDestroy(tSQLSyntaxNode *pNode) {
  if (pNode == NULL) return;

  if (pNode->nodeType == TSQL_NODE_EXPR) {
    tSQLBinaryExprDestroy(&pNode->pExpr);
  } else if (pNode->nodeType == TSQL_NODE_VALUE) {
    tVariantDestroy(pNode->pVal);
  }

  free(pNode);
}

void tSQLBinaryExprDestroy(tSQLBinaryExpr **pExprs) {
  if (*pExprs == NULL) return;

  tSQLSyntaxNodeDestroy((*pExprs)->pLeft);
  tSQLSyntaxNodeDestroy((*pExprs)->pRight);

  free(*pExprs);
  *pExprs = NULL;
}

static int32_t compareIntVal(const void *pLeft, const void *pRight) {
  DEFAULT_COMP(GET_INT64_VAL(pLeft), GET_INT64_VAL(pRight));
}

static int32_t compareIntDoubleVal(const void *pLeft, const void *pRight) {
  DEFAULT_COMP(GET_INT64_VAL(pLeft), GET_DOUBLE_VAL(pRight));
}

static int32_t compareDoubleVal(const void *pLeft, const void *pRight) {
  DEFAULT_COMP(GET_DOUBLE_VAL(pLeft), GET_DOUBLE_VAL(pRight));
}

static int32_t compareDoubleIntVal(const void *pLeft, const void *pRight) {
  double ret = (*(double *)pLeft) - (*(int64_t *)pRight);
  if (fabs(ret) < DBL_EPSILON) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

static int32_t compareStrVal(const void *pLeft, const void *pRight) {
  int32_t ret = strcmp(pLeft, pRight);
  if (ret == 0) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

static int32_t compareWStrVal(const void *pLeft, const void *pRight) {
  int32_t ret = wcscmp(pLeft, pRight);
  if (ret == 0) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

static int32_t compareStrPatternComp(const void *pLeft, const void *pRight) {
  SPatternCompareInfo pInfo = {'%', '_'};

  const char *pattern = pRight;
  const char *str = pLeft;

  if (patternMatch(pattern, str, strlen(str), &pInfo) == TSDB_PATTERN_MATCH) {
    return 0;
  } else {
    return 1;
  }
}

static int32_t compareWStrPatternComp(const void *pLeft, const void *pRight) {
  SPatternCompareInfo pInfo = {'%', '_'};

  const wchar_t *pattern = pRight;
  const wchar_t *str = pLeft;

  if (WCSPatternMatch(pattern, str, wcslen(str), &pInfo) == TSDB_PATTERN_MATCH) {
    return 0;
  } else {
    return 1;
  }
}

static __compar_fn_t getFilterComparator(int32_t type, int32_t filterType, int32_t optr) {
  __compar_fn_t comparator = NULL;

  switch (type) {
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_BOOL: {
      if (filterType >= TSDB_DATA_TYPE_BOOL && filterType <= TSDB_DATA_TYPE_BIGINT) {
        comparator = compareIntVal;
      } else if (filterType >= TSDB_DATA_TYPE_FLOAT && filterType <= TSDB_DATA_TYPE_DOUBLE) {
        comparator = compareIntDoubleVal;
      }
      break;
    }

    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE: {
      if (filterType >= TSDB_DATA_TYPE_BOOL && filterType <= TSDB_DATA_TYPE_BIGINT) {
        comparator = compareDoubleIntVal;
      } else if (filterType >= TSDB_DATA_TYPE_FLOAT && filterType <= TSDB_DATA_TYPE_DOUBLE) {
        comparator = compareDoubleVal;
      }
      break;
    }

    case TSDB_DATA_TYPE_BINARY: {
      assert(filterType == TSDB_DATA_TYPE_BINARY);

      if (optr == TSDB_RELATION_LIKE) {
        /* wildcard query using like operator */
        comparator = compareStrPatternComp;
      } else {
        /* normal relational comparator */
        comparator = compareStrVal;
      }

      break;
    }

    case TSDB_DATA_TYPE_NCHAR: {
      assert(filterType == TSDB_DATA_TYPE_NCHAR);

      if (optr == TSDB_RELATION_LIKE) {
        comparator = compareWStrPatternComp;
      } else {
        comparator = compareWStrVal;
      }

      break;
    }
    default:
      comparator = compareIntVal;
      break;
  }

  return comparator;
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
      q->upperBnd.pz = "\0";
      q->upperBnd.nLen = -1;

      q->lowerBnd.nType = type;
      q->lowerBnd.pz = "\0";
      q->lowerBnd.nLen = 0;
    }
  }
}

static void tSQLDoFilterInitialResult(tSkipList *pSkipList, bool (*fp)(), tQueryInfo *colInfo,
                                      tQueryResultset *result) {
  // primary key, search according to skiplist
  if (colInfo->colIdx == 0 && colInfo->optr != TSDB_RELATION_LIKE) {
    tSKipListQueryCond q;
    setInitialValueForRangeQueryCondition(&q, colInfo->q.nType);

    switch (colInfo->optr) {
      case TSDB_RELATION_EQUAL: {
        result->num =
            tSkipListPointQuery(pSkipList, &colInfo->q, 1, INCLUDE_POINT_QUERY, (tSkipListNode ***)&result->pRes);
        break;
      }
      case TSDB_RELATION_NOT_EQUAL: {
        result->num =
            tSkipListPointQuery(pSkipList, &colInfo->q, 1, EXCLUDE_POINT_QUERY, (tSkipListNode ***)&result->pRes);
        break;
      }
      case TSDB_RELATION_LESS_EQUAL: {
        tVariantAssign(&q.upperBnd, &colInfo->q);
        q.upperBndRelOptr = colInfo->optr;
        result->num = tSkipListQuery(pSkipList, &q, (tSkipListNode ***)&result->pRes);
        break;
      }
      case TSDB_RELATION_LESS: {
        tVariantAssign(&q.upperBnd, &colInfo->q);
        result->num = tSkipListQuery(pSkipList, &q, (tSkipListNode ***)&result->pRes);
        break;
      }
      case TSDB_RELATION_LARGE: {
        tVariantAssign(&q.lowerBnd, &colInfo->q);
        result->num = tSkipListQuery(pSkipList, &q, (tSkipListNode ***)&result->pRes);
        break;
      }
      case TSDB_RELATION_LARGE_EQUAL: {
        tVariantAssign(&q.lowerBnd, &colInfo->q);
        q.lowerBndRelOptr = colInfo->optr;
        result->num = tSkipListQuery(pSkipList, &q, (tSkipListNode ***)&result->pRes);
        break;
      }
      default:
        pTrace("skiplist:%p, unsupport query operator:%d", pSkipList, colInfo->optr);
    }

    tSkipListDestroyKey(&q.upperBnd);
    tSkipListDestroyKey(&q.lowerBnd);
  } else {
    // brutal force search
    result->num = tSkipListIterateList(pSkipList, (tSkipListNode ***)&result->pRes, fp, colInfo);
  }
}

void tSQLListTraversePrepare(tQueryInfo *colInfo, SSchema *pSchema, int32_t numOfCols, SSchema *pOneColSchema,
                             uint8_t optr, tVariant *val) {
  int32_t i = 0, offset = 0;
  if (strcasecmp(pOneColSchema->name, TSQL_TBNAME_L) == 0) {
    i = -1;
    offset = -1;
  } else {
    while (i < numOfCols) {
      if (pSchema[i].bytes == pOneColSchema->bytes && pSchema[i].type == pOneColSchema->type &&
          strcmp(pSchema[i].name, pOneColSchema->name) == 0) {
        break;
      } else {
        offset += pSchema[i++].bytes;
      }
    }
  }

  colInfo->pSchema = pSchema;
  colInfo->colIdx = i;
  colInfo->optr = optr;
  colInfo->offset = offset;
  colInfo->comparator = getFilterComparator(pOneColSchema->type, val->nType, optr);

  if (colInfo->pSchema[i].type != val->nType) {
    /* convert the query string to be inline with the data type of the queried tags */
    if (colInfo->pSchema[i].type == TSDB_DATA_TYPE_NCHAR && val->nType == TSDB_DATA_TYPE_BINARY) {
      colInfo->q.nLen = TSDB_MAX_TAGS_LEN / TSDB_NCHAR_SIZE;

      colInfo->q.wpz = calloc(1, TSDB_MAX_TAGS_LEN);
      colInfo->q.nType = TSDB_DATA_TYPE_NCHAR;

      taosMbsToUcs4(val->pz, val->nLen, (char *)colInfo->q.wpz, TSDB_MAX_TAGS_LEN);
      colInfo->q.nLen = wcslen(colInfo->q.wpz) + 1;
      return;
    } else if (colInfo->pSchema[i].type == TSDB_DATA_TYPE_BINARY && val->nType == TSDB_DATA_TYPE_NCHAR) {
      colInfo->q.nLen = TSDB_MAX_TAGS_LEN;
      colInfo->q.pz = calloc(1, TSDB_MAX_TAGS_LEN);
      colInfo->q.nType = TSDB_DATA_TYPE_BINARY;

      taosUcs4ToMbs(val->wpz, val->nLen, colInfo->q.pz);
      colInfo->q.nLen = strlen(colInfo->q.pz) + 1;
      return;
    }
  }

  tVariantAssign(&colInfo->q, val);
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
  pFinalRes->pRes = malloc(POINTER_BYTES * (pLeft->num + pRight->num));
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

  pFinalRes->pRes = malloc(POINTER_BYTES * num);
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
 *
 */
void tSQLListTraverseOnResult(struct tSQLBinaryExpr *pExpr, bool (*fp)(tSkipListNode *, void *), tQueryInfo *colInfo,
                              tQueryResultset *pResult) {
  assert(pExpr->pLeft->nodeType == TSQL_NODE_COL && pExpr->pRight->nodeType == TSQL_NODE_VALUE);

  // brutal force search
  int32_t num = pResult->num;
  for (int32_t i = 0, j = 0; i < pResult->num; ++i) {
    if (fp == NULL || (fp != NULL && fp(pResult->pRes[i], colInfo) == true)) {
      pResult->pRes[j++] = pResult->pRes[i];
    } else {
      num--;
    }
  }

  pResult->num = num;
}

// post-root order traverse syntax tree
void tSQLBinaryExprTraverse(tSQLBinaryExpr *pExprs, tSkipList *pSkipList, SSchema *pSchema, int32_t numOfCols,
                            bool (*fp)(tSkipListNode *, void *), tQueryResultset *result) {
  if (pExprs == NULL) return;

  tSQLSyntaxNode *pLeft = pExprs->pLeft;
  tSQLSyntaxNode *pRight = pExprs->pRight;

  // recursive traverse left child branch
  if (pLeft->nodeType == TSQL_NODE_EXPR || pRight->nodeType == TSQL_NODE_EXPR) {
    uint8_t weight = pLeft->pExpr->filterOnPrimaryKey + pRight->pExpr->filterOnPrimaryKey;

    if (weight == 0 && result->num > 0 && pSkipList == NULL) {
      /* base on the initial filter result to perform the secondary filter */
      tSQLBinaryExprTraverse(pLeft->pExpr, pSkipList, pSchema, numOfCols, fp, result);
      tSQLBinaryExprTraverse(pRight->pExpr, pSkipList, pSchema, numOfCols, fp, result);
    } else if (weight == 0 || weight == 2 || (weight == 1 && pExprs->nSQLBinaryOptr == TSDB_RELATION_OR)) {
      tQueryResultset rLeft = {0};
      tQueryResultset rRight = {0};

      tSQLBinaryExprTraverse(pLeft->pExpr, pSkipList, pSchema, numOfCols, fp, &rLeft);
      tSQLBinaryExprTraverse(pRight->pExpr, pSkipList, pSchema, numOfCols, fp, &rRight);

      if (pExprs->nSQLBinaryOptr == TSDB_RELATION_AND) {  // CROSS
        intersect(&rLeft, &rRight, result);
      } else if (pExprs->nSQLBinaryOptr == TSDB_RELATION_OR) {  // or
        merge(&rLeft, &rRight, result);
      } else {
        assert(false);
      }

      free(rLeft.pRes);
      free(rRight.pRes);
    } else {
      /*
       * first, we filter results based on the skiplist index, which is initial filter stage,
       * then, we conduct the secondary filter operation based on the result from the initial filter stage.
       */
      if (pExprs->nSQLBinaryOptr == TSDB_RELATION_AND) {
        tSQLBinaryExpr *pFirst = (pLeft->pExpr->filterOnPrimaryKey == 1) ? pLeft->pExpr : pRight->pExpr;
        tSQLBinaryExpr *pSec = (pLeft->pExpr->filterOnPrimaryKey == 1) ? pRight->pExpr : pLeft->pExpr;
        assert(pFirst != pSec && pFirst != NULL && pSec != NULL);

        // we filter the result based on the skiplist index
        tSQLBinaryExprTraverse(pFirst, pSkipList, pSchema, numOfCols, fp, result);

        /*
         * recursively perform the filter operation based on the initial results,
         * So, we do not set the skiplist index as a parameter
         */
        tSQLBinaryExprTraverse(pSec, NULL, pSchema, numOfCols, fp, result);
      } else {
        assert(false);
      }
    }

  } else {  // column project
    assert(pLeft->nodeType == TSQL_NODE_COL && pRight->nodeType == TSQL_NODE_VALUE);
    tVariant *pCond = pRight->pVal;
    SSchema * pTagSchema = pLeft->pSchema;

    tQueryInfo queryColInfo = {0};
    tSQLListTraversePrepare(&queryColInfo, pSchema, numOfCols, pTagSchema, pExprs->nSQLBinaryOptr, pCond);

    if (pSkipList == NULL) {
      tSQLListTraverseOnResult(pExprs, fp, &queryColInfo, result);
    } else {
      assert(result->num == 0);
      tSQLDoFilterInitialResult(pSkipList, fp, &queryColInfo, result);
    }

    tVariantDestroy(&queryColInfo.q);
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
  char *pLeftOutput = malloc(sizeof(int64_t) * numOfRows);
  if (pLeft->nodeType == TSQL_NODE_EXPR) {
    tSQLBinaryExprCalcTraverse(pLeft->pExpr, numOfRows, pLeftOutput, param, order, getSourceDataBlock);
  }

  /* the right output has result from the right child syntax tree */
  char *pRightOutput = malloc(sizeof(int64_t) * numOfRows);
  if (pRight->nodeType == TSQL_NODE_EXPR) {
    tSQLBinaryExprCalcTraverse(pRight->pExpr, numOfRows, pRightOutput, param, order, getSourceDataBlock);
  }

  if (pLeft->nodeType == TSQL_NODE_EXPR) {
    if (pRight->nodeType == TSQL_NODE_EXPR) {  // exprLeft + exprRight
                                               /* the type of returned value of one expression is always double float
                                                * precious */
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
      char *pRightInputData = getSourceDataBlock(param, pRight->pSchema->name, pRight->colId);
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