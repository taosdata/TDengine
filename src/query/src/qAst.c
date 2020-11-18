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

#include "exception.h"
#include "qAst.h"
#include "qSyntaxtreefunction.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "tarray.h"
#include "tbuffer.h"
#include "tcompare.h"
#include "tname.h"
#include "tsdb.h"
#include "tskiplist.h"
#include "tsqlfunction.h"
#include "tstoken.h"
#include "tschemautil.h"

typedef struct {
  char*    v;
  int32_t  optr;
} SEndPoint;

typedef struct {
  SEndPoint* start;
  SEndPoint* end;
} SQueryCond;

static uint8_t UNUSED_FUNC isQueryOnPrimaryKey(const char *primaryColumnName, const tExprNode *pLeft, const tExprNode *pRight) {
  if (pLeft->nodeType == TSQL_NODE_COL) {
    // if left node is the primary column,return true
    return (strcmp(primaryColumnName, pLeft->pSchema->name) == 0) ? 1 : 0;
  } else {
    // if any children have query on primary key, their parents are also keep this value
    return ((pLeft->nodeType == TSQL_NODE_EXPR && pLeft->_node.hasPK == 1) ||
            (pRight->nodeType == TSQL_NODE_EXPR && pRight->_node.hasPK == 1)) == true
               ? 1
               : 0;
  }
}

void tExprNodeDestroy(tExprNode *pNode, void (*fp)(void *)) {
  if (pNode == NULL) {
    return;
  }

  if (pNode->nodeType == TSQL_NODE_EXPR) {
    tExprTreeDestroy(&pNode, fp);
  } else if (pNode->nodeType == TSQL_NODE_VALUE) {
    tVariantDestroy(pNode->pVal);
  } else if (pNode->nodeType == TSQL_NODE_COL) {
    free(pNode->pSchema);
  }

  free(pNode);
}

void tExprTreeDestroy(tExprNode **pExpr, void (*fp)(void *)) {
  if (*pExpr == NULL) {
    return;
  }
  
  if ((*pExpr)->nodeType == TSQL_NODE_EXPR) {
    tExprTreeDestroy(&(*pExpr)->_node.pLeft, fp);
    tExprTreeDestroy(&(*pExpr)->_node.pRight, fp);
  
    if (fp != NULL) {
      fp((*pExpr)->_node.info);
    }
  } else if ((*pExpr)->nodeType == TSQL_NODE_VALUE) {
    tVariantDestroy((*pExpr)->pVal);
    free((*pExpr)->pVal);
  } else if ((*pExpr)->nodeType == TSQL_NODE_COL) {
    free((*pExpr)->pSchema);
  }

  free(*pExpr);
  *pExpr = NULL;
}

// todo check for malloc failure
static int32_t setQueryCond(tQueryInfo *queryColInfo, SQueryCond* pCond) {
  int32_t optr = queryColInfo->optr;
  
  if (optr == TSDB_RELATION_GREATER || optr == TSDB_RELATION_GREATER_EQUAL ||
      optr == TSDB_RELATION_EQUAL || optr == TSDB_RELATION_NOT_EQUAL) {
    pCond->start = calloc(1, sizeof(SEndPoint));
    pCond->start->optr = queryColInfo->optr;
    pCond->start->v = queryColInfo->q;
  } else if (optr == TSDB_RELATION_LESS || optr == TSDB_RELATION_LESS_EQUAL) {
    pCond->end = calloc(1, sizeof(SEndPoint));
    pCond->end->optr = queryColInfo->optr;
    pCond->end->v = queryColInfo->q;
  } else if (optr == TSDB_RELATION_IN || optr == TSDB_RELATION_LIKE) {
    assert(0);
  }

  return TSDB_CODE_SUCCESS;
}

static void tQueryIndexColumn(SSkipList* pSkipList, tQueryInfo* pQueryInfo, SArray* result) {
  SSkipListIterator* iter = NULL;
  
  SQueryCond cond = {0};
  if (setQueryCond(pQueryInfo, &cond) != TSDB_CODE_SUCCESS) {
    //todo handle error
  }

  if (cond.start != NULL) {
    iter = tSkipListCreateIterFromVal(pSkipList, (char*) cond.start->v, pSkipList->type, TSDB_ORDER_ASC);
  } else {
    iter = tSkipListCreateIterFromVal(pSkipList, (char*)(cond.end ? cond.end->v: NULL), pSkipList->type, TSDB_ORDER_DESC);
  }
  
  if (cond.start != NULL) {
    int32_t optr = cond.start->optr;
    
    if (optr == TSDB_RELATION_EQUAL) {   // equals
      while(tSkipListIterNext(iter)) {
        SSkipListNode* pNode = tSkipListIterGet(iter);

        int32_t ret = pQueryInfo->compare(SL_GET_NODE_KEY(pSkipList, pNode), cond.start->v);
        if (ret != 0) {
          break;
        }

        STableKeyInfo info = {.pTable = (void*)SL_GET_NODE_DATA(pNode), .lastKey = TSKEY_INITIAL_VAL};
        taosArrayPush(result, &info);
      }
    } else if (optr == TSDB_RELATION_GREATER || optr == TSDB_RELATION_GREATER_EQUAL) { // greater equal
      bool comp = true;
      int32_t ret = 0;
      
      while(tSkipListIterNext(iter)) {
        SSkipListNode* pNode = tSkipListIterGet(iter);
    
        if (comp) {
          ret = pQueryInfo->compare(SL_GET_NODE_KEY(pSkipList, pNode), cond.start->v);
          assert(ret >= 0);
        }
        
        if (ret == 0 && optr == TSDB_RELATION_GREATER) {
          continue;
        } else {
          STableKeyInfo info = {.pTable = (void*)SL_GET_NODE_DATA(pNode), .lastKey = TSKEY_INITIAL_VAL};
          taosArrayPush(result, &info);
          comp = false;
        }
      }
    } else if (optr == TSDB_RELATION_NOT_EQUAL) {   // not equal
      bool comp = true;
      
      while(tSkipListIterNext(iter)) {
        SSkipListNode* pNode = tSkipListIterGet(iter);
        comp = comp && (pQueryInfo->compare(SL_GET_NODE_KEY(pSkipList, pNode), cond.start->v) == 0);
        if (comp) {
          continue;
        }

        STableKeyInfo info = {.pTable = (void*)SL_GET_NODE_DATA(pNode), .lastKey = TSKEY_INITIAL_VAL};
        taosArrayPush(result, &info);
      }
      
      tSkipListDestroyIter(iter);
  
      comp = true;
      iter = tSkipListCreateIterFromVal(pSkipList, (char*) cond.start->v, pSkipList->type, TSDB_ORDER_DESC);
      while(tSkipListIterNext(iter)) {
        SSkipListNode* pNode = tSkipListIterGet(iter);
        comp = comp && (pQueryInfo->compare(SL_GET_NODE_KEY(pSkipList, pNode), cond.start->v) == 0);
      if (comp) {
          continue;
        }

        STableKeyInfo info = {.pTable = (void*)SL_GET_NODE_DATA(pNode), .lastKey = TSKEY_INITIAL_VAL};
        taosArrayPush(result, &info);
      }
  
    } else {
      assert(0);
    }
  } else {
    int32_t optr = cond.end ? cond.end->optr : TSDB_RELATION_INVALID;
    if (optr == TSDB_RELATION_LESS || optr == TSDB_RELATION_LESS_EQUAL) {
      bool    comp = true;
      int32_t ret = 0;

      while (tSkipListIterNext(iter)) {
        SSkipListNode *pNode = tSkipListIterGet(iter);

        if (comp) {
          ret = pQueryInfo->compare(SL_GET_NODE_KEY(pSkipList, pNode), cond.end->v);
          assert(ret <= 0);
        }

        if (ret == 0 && optr == TSDB_RELATION_LESS) {
          continue;
        } else {
          STableKeyInfo info = {.pTable = (void *)SL_GET_NODE_DATA(pNode), .lastKey = TSKEY_INITIAL_VAL};
          taosArrayPush(result, &info);
          comp = false;  // no need to compare anymore
        }
      }
    } else {
      assert(pQueryInfo->optr == TSDB_RELATION_ISNULL || pQueryInfo->optr == TSDB_RELATION_NOTNULL);

      while (tSkipListIterNext(iter)) {
        SSkipListNode *pNode = tSkipListIterGet(iter);

        bool isnull = isNull(SL_GET_NODE_KEY(pSkipList, pNode), pQueryInfo->sch.type);
        if ((pQueryInfo->optr == TSDB_RELATION_ISNULL && isnull) ||
            (pQueryInfo->optr == TSDB_RELATION_NOTNULL && (!isnull))) {
          STableKeyInfo info = {.pTable = (void *)SL_GET_NODE_DATA(pNode), .lastKey = TSKEY_INITIAL_VAL};
          taosArrayPush(result, &info);
        }
      }
    }
  }

  free(cond.start);
  free(cond.end);
  tSkipListDestroyIter(iter);
}

static bool filterItem(tExprNode *pExpr, const void *pItem, SExprTraverseSupp *param) {
  tExprNode *pLeft = pExpr->_node.pLeft;
  tExprNode *pRight = pExpr->_node.pRight;

  //non-leaf nodes, recursively traverse the expression tree in the post-root order
  if (pLeft->nodeType == TSQL_NODE_EXPR && pRight->nodeType == TSQL_NODE_EXPR) {
    if (pExpr->_node.optr == TSDB_RELATION_OR) {  // or
      if (filterItem(pLeft, pItem, param)) {
        return true;
      }

      // left child does not satisfy the query condition, try right child
      return filterItem(pRight, pItem, param);
    } else {  // and
      if (!filterItem(pLeft, pItem, param)) {
        return false;
      }

      return filterItem(pRight, pItem, param);
    }
  }

  // handle the leaf node
  param->setupInfoFn(pExpr, param->pExtInfo);
  return param->nodeFilterFn(pItem, pExpr->_node.info);
}

static void tSQLBinaryTraverseOnSkipList(tExprNode *pExpr, SArray *pResult, SSkipList *pSkipList, SExprTraverseSupp *param ) {
  SSkipListIterator* iter = tSkipListCreateIter(pSkipList);

  while (tSkipListIterNext(iter)) {
    SSkipListNode *pNode = tSkipListIterGet(iter);
    if (filterItem(pExpr, pNode, param)) {
      taosArrayPush(pResult, &(SL_GET_NODE_DATA(pNode)));
    }
  }
  tSkipListDestroyIter(iter);
}

static void tQueryIndexlessColumn(SSkipList* pSkipList, tQueryInfo* pQueryInfo, SArray* res, __result_filter_fn_t filterFp) {
  SSkipListIterator* iter = tSkipListCreateIter(pSkipList);

  while (tSkipListIterNext(iter)) {
    bool addToResult = false;

    SSkipListNode *pNode = tSkipListIterGet(iter);
    char *         pData = SL_GET_NODE_DATA(pNode);

    tstr *name = (tstr*) tsdbGetTableName((void*) pData);

    // todo speed up by using hash
    if (pQueryInfo->sch.colId == TSDB_TBNAME_COLUMN_INDEX) {
      if (pQueryInfo->optr == TSDB_RELATION_IN) {
        addToResult = pQueryInfo->compare(name, pQueryInfo->q);
      } else if (pQueryInfo->optr == TSDB_RELATION_LIKE) {
        addToResult = !pQueryInfo->compare(name, pQueryInfo->q);
      }
    } else {
      addToResult = filterFp(pNode, pQueryInfo);
    }

    if (addToResult) {
      STableKeyInfo info = {.pTable = (void*)pData, .lastKey = TSKEY_INITIAL_VAL};
      taosArrayPush(res, &info);
    }
  }

  tSkipListDestroyIter(iter);
}

// post-root order traverse syntax tree
void tExprTreeTraverse(tExprNode *pExpr, SSkipList *pSkipList, SArray *result, SExprTraverseSupp *param) {
  if (pExpr == NULL) {
    return;
  }

  tExprNode *pLeft  = pExpr->_node.pLeft;
  tExprNode *pRight = pExpr->_node.pRight;

  // column project
  if (pLeft->nodeType != TSQL_NODE_EXPR && pRight->nodeType != TSQL_NODE_EXPR) {
    assert(pLeft->nodeType == TSQL_NODE_COL && (pRight->nodeType == TSQL_NODE_VALUE || pRight->nodeType == TSQL_NODE_DUMMY));

    param->setupInfoFn(pExpr, param->pExtInfo);

    tQueryInfo *pQueryInfo = pExpr->_node.info;
    if (pQueryInfo->indexed && pQueryInfo->optr != TSDB_RELATION_LIKE) {
      tQueryIndexColumn(pSkipList, pQueryInfo, result);
    } else {
      tQueryIndexlessColumn(pSkipList, pQueryInfo, result, param->nodeFilterFn);
    }

    return;
  }

  // The value of hasPK is always 0.
  uint8_t weight = pLeft->_node.hasPK + pRight->_node.hasPK;
  assert(weight == 0 && pSkipList != NULL && taosArrayGetSize(result) == 0);

  //apply the hierarchical expression to every node in skiplist for find the qualified nodes
  tSQLBinaryTraverseOnSkipList(pExpr, result, pSkipList, param);

#if 0
  /*
    * (weight == 1 && pExpr->nSQLBinaryOptr == TSDB_RELATION_AND) is handled here
    *
    * first, we filter results based on the skiplist index, which is the initial filter stage,
    * then, we conduct the secondary filter operation based on the result from the initial filter stage.
    */
  assert(pExpr->_node.optr == TSDB_RELATION_AND);

  tExprNode *pFirst = NULL;
  tExprNode *pSecond = NULL;
  if (pLeft->_node.hasPK == 1) {
    pFirst = pLeft;
    pSecond = pRight;
  } else {
    pFirst = pRight;
    pSecond = pLeft;
  }

  assert(pFirst != pSecond && pFirst != NULL && pSecond != NULL);

  // we filter the result based on the skiplist index in the first place
  tExprTreeTraverse(pFirst, pSkipList, result, param);

  /*
    * recursively perform the filter operation based on the initial results,
    * So, we do not set the skip list index as a parameter
    */
  tExprTreeTraverse(pSecond, NULL, result, param);
#endif
}

void tExprTreeCalcTraverse(tExprNode *pExprs, int32_t numOfRows, char *pOutput, void *param, int32_t order,
                                char *(*getSourceDataBlock)(void *, const char*, int32_t)) {
  if (pExprs == NULL) {
    return;
  }

  tExprNode *pLeft = pExprs->_node.pLeft;
  tExprNode *pRight = pExprs->_node.pRight;

  /* the left output has result from the left child syntax tree */
  char *pLeftOutput = (char*)malloc(sizeof(int64_t) * numOfRows);
  if (pLeft->nodeType == TSQL_NODE_EXPR) {
    tExprTreeCalcTraverse(pLeft, numOfRows, pLeftOutput, param, order, getSourceDataBlock);
  }

  /* the right output has result from the right child syntax tree */
  char *pRightOutput = malloc(sizeof(int64_t) * numOfRows);
  if (pRight->nodeType == TSQL_NODE_EXPR) {
    tExprTreeCalcTraverse(pRight, numOfRows, pRightOutput, param, order, getSourceDataBlock);
  }

  if (pLeft->nodeType == TSQL_NODE_EXPR) {
    if (pRight->nodeType == TSQL_NODE_EXPR) {
      /*
       * exprLeft + exprRight
       * the type of returned value of one expression is always double float precious
       */
      _bi_consumer_fn_t fp = tGetBiConsumerFn(TSDB_DATA_TYPE_DOUBLE, TSDB_DATA_TYPE_DOUBLE, pExprs->_node.optr);
      fp(pLeftOutput, pRightOutput, numOfRows, numOfRows, pOutput, order);

    } else if (pRight->nodeType == TSQL_NODE_COL) {  // exprLeft + columnRight
      _bi_consumer_fn_t fp = tGetBiConsumerFn(TSDB_DATA_TYPE_DOUBLE, pRight->pSchema->type, pExprs->_node.optr);
      // set input buffer
      char *pInputData = getSourceDataBlock(param, pRight->pSchema->name, pRight->pSchema->colId);
      fp(pLeftOutput, pInputData, numOfRows, numOfRows, pOutput, order);

    } else if (pRight->nodeType == TSQL_NODE_VALUE) {  // exprLeft + 12
      _bi_consumer_fn_t fp = tGetBiConsumerFn(TSDB_DATA_TYPE_DOUBLE, pRight->pVal->nType, pExprs->_node.optr);
      fp(pLeftOutput, &pRight->pVal->i64Key, numOfRows, 1, pOutput, order);
    }
  } else if (pLeft->nodeType == TSQL_NODE_COL) {
    // column data specified on left-hand-side
    char *pLeftInputData = getSourceDataBlock(param, pLeft->pSchema->name, pLeft->pSchema->colId);
    if (pRight->nodeType == TSQL_NODE_EXPR) {  // columnLeft + expr2
      _bi_consumer_fn_t fp = tGetBiConsumerFn(pLeft->pSchema->type, TSDB_DATA_TYPE_DOUBLE, pExprs->_node.optr);
      fp(pLeftInputData, pRightOutput, numOfRows, numOfRows, pOutput, order);

    } else if (pRight->nodeType == TSQL_NODE_COL) {  // columnLeft + columnRight
      // column data specified on right-hand-side
      char *pRightInputData = getSourceDataBlock(param, pRight->pSchema->name, pRight->pSchema->colId);

      _bi_consumer_fn_t fp = tGetBiConsumerFn(pLeft->pSchema->type, pRight->pSchema->type, pExprs->_node.optr);
      fp(pLeftInputData, pRightInputData, numOfRows, numOfRows, pOutput, order);

    } else if (pRight->nodeType == TSQL_NODE_VALUE) {  // columnLeft + 12
      _bi_consumer_fn_t fp = tGetBiConsumerFn(pLeft->pSchema->type, pRight->pVal->nType, pExprs->_node.optr);
      fp(pLeftInputData, &pRight->pVal->i64Key, numOfRows, 1, pOutput, order);
    }
  } else {
    // column data specified on left-hand-side
    if (pRight->nodeType == TSQL_NODE_EXPR) {  // 12 + expr2
      _bi_consumer_fn_t fp = tGetBiConsumerFn(pLeft->pVal->nType, TSDB_DATA_TYPE_DOUBLE, pExprs->_node.optr);
      fp(&pLeft->pVal->i64Key, pRightOutput, 1, numOfRows, pOutput, order);

    } else if (pRight->nodeType == TSQL_NODE_COL) {  // 12 + columnRight
      // column data specified on right-hand-side
      char *pRightInputData = getSourceDataBlock(param, pRight->pSchema->name, pRight->pSchema->colId);
      
      _bi_consumer_fn_t fp = tGetBiConsumerFn(pLeft->pVal->nType, pRight->pSchema->type, pExprs->_node.optr);
      fp(&pLeft->pVal->i64Key, pRightInputData, 1, numOfRows, pOutput, order);

    } else if (pRight->nodeType == TSQL_NODE_VALUE) {  // 12 + 12
      _bi_consumer_fn_t fp = tGetBiConsumerFn(pLeft->pVal->nType, pRight->pVal->nType, pExprs->_node.optr);
      fp(&pLeft->pVal->i64Key, &pRight->pVal->i64Key, 1, 1, pOutput, order);
    }
  }

  free(pLeftOutput);
  free(pRightOutput);
}

static void exprTreeToBinaryImpl(SBufferWriter* bw, tExprNode* expr) {
  tbufWriteUint8(bw, expr->nodeType);
  
  if (expr->nodeType == TSQL_NODE_VALUE) {
    tVariant* pVal = expr->pVal;
    
    tbufWriteUint32(bw, pVal->nType);
    if (pVal->nType == TSDB_DATA_TYPE_BINARY) {
      tbufWriteInt32(bw, pVal->nLen);
      tbufWrite(bw, pVal->pz, pVal->nLen);
    } else {
      tbufWriteInt64(bw, pVal->i64Key);
    }
    
  } else if (expr->nodeType == TSQL_NODE_COL) {
    SSchema* pSchema = expr->pSchema;
    tbufWriteInt16(bw, pSchema->colId);
    tbufWriteInt16(bw, pSchema->bytes);
    tbufWriteUint8(bw, pSchema->type);
    tbufWriteString(bw, pSchema->name);
    
  } else if (expr->nodeType == TSQL_NODE_EXPR) {
    tbufWriteUint8(bw, expr->_node.optr);
    tbufWriteUint8(bw, expr->_node.hasPK);
    exprTreeToBinaryImpl(bw, expr->_node.pLeft);
    exprTreeToBinaryImpl(bw, expr->_node.pRight);
  }
}

void exprTreeToBinary(SBufferWriter* bw, tExprNode* expr) {
  if (expr != NULL) {
    exprTreeToBinaryImpl(bw, expr);
  }
}

// TODO: these three functions should be made global
static void* exception_calloc(size_t nmemb, size_t size) {
  void* p = calloc(nmemb, size);
  if (p == NULL) {
    THROW(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }
  return p;
}

static void* exception_malloc(size_t size) {
  void* p = malloc(size);
  if (p == NULL) {
    THROW(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }
  return p;
}

static UNUSED_FUNC char* exception_strdup(const char* str) {
  char* p = strdup(str);
  if (p == NULL) {
    THROW(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }
  return p;
}

static tExprNode* exprTreeFromBinaryImpl(SBufferReader* br) {
  int32_t anchor = CLEANUP_GET_ANCHOR();
  if (CLEANUP_EXCEED_LIMIT()) {
    THROW(TSDB_CODE_QRY_EXCEED_TAGS_LIMIT);
    return NULL;
  }

  tExprNode* pExpr = exception_calloc(1, sizeof(tExprNode));
  CLEANUP_PUSH_VOID_PTR_PTR(true, tExprNodeDestroy, pExpr, NULL);
  pExpr->nodeType = tbufReadUint8(br);
  
  if (pExpr->nodeType == TSQL_NODE_VALUE) {
    tVariant* pVal = exception_calloc(1, sizeof(tVariant));
    pExpr->pVal = pVal;
  
    pVal->nType = tbufReadUint32(br);
    if (pVal->nType == TSDB_DATA_TYPE_BINARY) {
      tbufReadToBuffer(br, &pVal->nLen, sizeof(pVal->nLen));
      pVal->pz = calloc(1, pVal->nLen + 1);
      tbufReadToBuffer(br, pVal->pz, pVal->nLen);
    } else {
      pVal->i64Key = tbufReadInt64(br);
    }
    
  } else if (pExpr->nodeType == TSQL_NODE_COL) {
    SSchema* pSchema = exception_calloc(1, sizeof(SSchema));
    pExpr->pSchema = pSchema;

    pSchema->colId = tbufReadInt16(br);
    pSchema->bytes = tbufReadInt16(br);
    pSchema->type = tbufReadUint8(br);
    tbufReadToString(br, pSchema->name, TSDB_COL_NAME_LEN);
    
  } else if (pExpr->nodeType == TSQL_NODE_EXPR) {
    pExpr->_node.optr = tbufReadUint8(br);
    pExpr->_node.hasPK = tbufReadUint8(br);
    pExpr->_node.pLeft = exprTreeFromBinaryImpl(br);
    pExpr->_node.pRight = exprTreeFromBinaryImpl(br);
    assert(pExpr->_node.pLeft != NULL && pExpr->_node.pRight != NULL);
  }
  
  CLEANUP_EXECUTE_TO(anchor, false);
  return pExpr;
}

tExprNode* exprTreeFromBinary(const void* data, size_t size) {
  if (size == 0) {
    return NULL;
  }

  SBufferReader br = tbufInitReader(data, size, false);
  return exprTreeFromBinaryImpl(&br);
}

tExprNode* exprTreeFromTableName(const char* tbnameCond) {
  if (!tbnameCond) {
    return NULL;
  }

  int32_t anchor = CLEANUP_GET_ANCHOR();

  tExprNode* expr = exception_calloc(1, sizeof(tExprNode));
  CLEANUP_PUSH_VOID_PTR_PTR(true, tExprNodeDestroy, expr, NULL);

  expr->nodeType = TSQL_NODE_EXPR;

  tExprNode* left = exception_calloc(1, sizeof(tExprNode));
  expr->_node.pLeft = left;

  left->nodeType = TSQL_NODE_COL;
  SSchema* pSchema = exception_calloc(1, sizeof(SSchema));
  left->pSchema = pSchema;

  *pSchema = tscGetTbnameColumnSchema();

  tExprNode* right = exception_calloc(1, sizeof(tExprNode));
  expr->_node.pRight = right;

  if (strncmp(tbnameCond, QUERY_COND_REL_PREFIX_LIKE, QUERY_COND_REL_PREFIX_LIKE_LEN) == 0) {
    right->nodeType = TSQL_NODE_VALUE;
    expr->_node.optr = TSDB_RELATION_LIKE;
    tVariant* pVal = exception_calloc(1, sizeof(tVariant));
    right->pVal = pVal;
    size_t len = strlen(tbnameCond + QUERY_COND_REL_PREFIX_LIKE_LEN) + 1;
    pVal->pz = exception_malloc(len);
    memcpy(pVal->pz, tbnameCond + QUERY_COND_REL_PREFIX_LIKE_LEN, len);
    pVal->nType = TSDB_DATA_TYPE_BINARY;
    pVal->nLen = (int32_t)len;

  } else if (strncmp(tbnameCond, QUERY_COND_REL_PREFIX_IN, QUERY_COND_REL_PREFIX_IN_LEN) == 0) {
    right->nodeType = TSQL_NODE_VALUE;
    expr->_node.optr = TSDB_RELATION_IN;
    tVariant* pVal = exception_calloc(1, sizeof(tVariant));
    right->pVal = pVal;
    pVal->nType = TSDB_DATA_TYPE_ARRAY;
    pVal->arr = taosArrayInit(2, POINTER_BYTES);

    const char* cond = tbnameCond + QUERY_COND_REL_PREFIX_IN_LEN;
    for (const char *e = cond; *e != 0; e++) {
      if (*e == TS_PATH_DELIMITER[0]) {
        cond = e + 1;
      } else if (*e == ',') {
        size_t len = e - cond;
        char* p = exception_malloc(len + VARSTR_HEADER_SIZE);
        STR_WITH_SIZE_TO_VARSTR(p, cond, (VarDataLenT)len);
        cond += len;
        taosArrayPush(pVal->arr, &p);
      }
    }

    if (*cond != 0) {
      size_t len = strlen(cond) + VARSTR_HEADER_SIZE;
      
      char* p = exception_malloc(len);
      STR_WITH_SIZE_TO_VARSTR(p, cond, (VarDataLenT)(len - VARSTR_HEADER_SIZE));
      taosArrayPush(pVal->arr, &p);
    }

    taosArraySortString(pVal->arr, taosArrayCompareString);
  }

  CLEANUP_EXECUTE_TO(anchor, false);
  return expr;
}
