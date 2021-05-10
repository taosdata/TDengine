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

#include "texpr.h"
#include "exception.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "tarray.h"
#include "tbuffer.h"
#include "tcompare.h"
#include "tsdb.h"
#include "tskiplist.h"
#include "texpr.h"
#include "tarithoperator.h"

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

static void reverseCopy(char* dest, const char* src, int16_t type, int32_t numOfRows) {
  switch(type) {
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_UTINYINT:{
      int8_t* p = (int8_t*) dest;
      int8_t* pSrc = (int8_t*) src;

      for(int32_t i = 0; i < numOfRows; ++i) {
        p[i] = pSrc[numOfRows - i - 1];
      }
      return;
    }

    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_USMALLINT:{
      int16_t* p = (int16_t*) dest;
      int16_t* pSrc = (int16_t*) src;

      for(int32_t i = 0; i < numOfRows; ++i) {
        p[i] = pSrc[numOfRows - i - 1];
      }
      return;
    }
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_UINT: {
      int32_t* p = (int32_t*) dest;
      int32_t* pSrc = (int32_t*) src;

      for(int32_t i = 0; i < numOfRows; ++i) {
        p[i] = pSrc[numOfRows - i - 1];
      }
      return;
    }
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_UBIGINT: {
      int64_t* p = (int64_t*) dest;
      int64_t* pSrc = (int64_t*) src;

      for(int32_t i = 0; i < numOfRows; ++i) {
        p[i] = pSrc[numOfRows - i - 1];
      }
      return;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      float* p = (float*) dest;
      float* pSrc = (float*) src;

      for(int32_t i = 0; i < numOfRows; ++i) {
        p[i] = pSrc[numOfRows - i - 1];
      }
      return;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double* p = (double*) dest;
      double* pSrc = (double*) src;

      for(int32_t i = 0; i < numOfRows; ++i) {
        p[i] = pSrc[numOfRows - i - 1];
      }
      return;
    }
    default: assert(0);
  }
}

static void doExprTreeDestroy(tExprNode **pExpr, void (*fp)(void *));

void tExprTreeDestroy(tExprNode *pNode, void (*fp)(void *)) {
  if (pNode == NULL) {
    return;
  }

  if (pNode->nodeType == TSQL_NODE_EXPR) {
    doExprTreeDestroy(&pNode, fp);
  } else if (pNode->nodeType == TSQL_NODE_VALUE) {
    tVariantDestroy(pNode->pVal);
  } else if (pNode->nodeType == TSQL_NODE_COL) {
    free(pNode->pSchema);
  }

  free(pNode);
}

static void doExprTreeDestroy(tExprNode **pExpr, void (*fp)(void *)) {
  if (*pExpr == NULL) {
    return;
  }
  
  if ((*pExpr)->nodeType == TSQL_NODE_EXPR) {
    doExprTreeDestroy(&(*pExpr)->_node.pLeft, fp);
    doExprTreeDestroy(&(*pExpr)->_node.pRight, fp);
  
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

bool exprTreeApplyFilter(tExprNode *pExpr, const void *pItem, SExprTraverseSupp *param) {
  tExprNode *pLeft  = pExpr->_node.pLeft;
  tExprNode *pRight = pExpr->_node.pRight;

  //non-leaf nodes, recursively traverse the expression tree in the post-root order
  if (pLeft->nodeType == TSQL_NODE_EXPR && pRight->nodeType == TSQL_NODE_EXPR) {
    if (pExpr->_node.optr == TSDB_RELATION_OR) {  // or
      if (exprTreeApplyFilter(pLeft, pItem, param)) {
        return true;
      }

      // left child does not satisfy the query condition, try right child
      return exprTreeApplyFilter(pRight, pItem, param);
    } else {  // and
      if (!exprTreeApplyFilter(pLeft, pItem, param)) {
        return false;
      }

      return exprTreeApplyFilter(pRight, pItem, param);
    }
  }

  // handle the leaf node
  param->setupInfoFn(pExpr, param->pExtInfo);
  return param->nodeFilterFn(pItem, pExpr->_node.info);
}

void arithmeticTreeTraverse(tExprNode *pExprs, int32_t numOfRows, char *pOutput, void *param, int32_t order,
                                char *(*getSourceDataBlock)(void *, const char*, int32_t)) {
  if (pExprs == NULL) {
    return;
  }

  tExprNode *pLeft = pExprs->_node.pLeft;
  tExprNode *pRight = pExprs->_node.pRight;

  /* the left output has result from the left child syntax tree */
  char *pLeftOutput = (char*)malloc(sizeof(int64_t) * numOfRows);
  if (pLeft->nodeType == TSQL_NODE_EXPR) {
    arithmeticTreeTraverse(pLeft, numOfRows, pLeftOutput, param, order, getSourceDataBlock);
  }

  /* the right output has result from the right child syntax tree */
  char *pRightOutput = malloc(sizeof(int64_t) * numOfRows);
  char *pdata = malloc(sizeof(int64_t) * numOfRows);

  if (pRight->nodeType == TSQL_NODE_EXPR) {
    arithmeticTreeTraverse(pRight, numOfRows, pRightOutput, param, order, getSourceDataBlock);
  }

  if (pLeft->nodeType == TSQL_NODE_EXPR) {
    if (pRight->nodeType == TSQL_NODE_EXPR) {
      /*
       * exprLeft + exprRight
       * the type of returned value of one expression is always double float precious
       */
      _arithmetic_operator_fn_t OperatorFn = getArithmeticOperatorFn(pExprs->_node.optr);
      OperatorFn(pLeftOutput, numOfRows, TSDB_DATA_TYPE_DOUBLE, pRightOutput, numOfRows, TSDB_DATA_TYPE_DOUBLE, pOutput, TSDB_ORDER_ASC);

    } else if (pRight->nodeType == TSQL_NODE_COL) {  // exprLeft + columnRight
      _arithmetic_operator_fn_t OperatorFn = getArithmeticOperatorFn(pExprs->_node.optr);

      // set input buffer
      char *pInputData = getSourceDataBlock(param, pRight->pSchema->name, pRight->pSchema->colId);
      if (order == TSDB_ORDER_DESC) {
        reverseCopy(pdata, pInputData, pRight->pSchema->type, numOfRows);
        OperatorFn(pLeftOutput, numOfRows, TSDB_DATA_TYPE_DOUBLE, pdata, numOfRows, pRight->pSchema->type, pOutput, TSDB_ORDER_ASC);
      } else {
        OperatorFn(pLeftOutput, numOfRows, TSDB_DATA_TYPE_DOUBLE, pInputData, numOfRows, pRight->pSchema->type, pOutput, TSDB_ORDER_ASC);
      }

    } else if (pRight->nodeType == TSQL_NODE_VALUE) {  // exprLeft + 12
      _arithmetic_operator_fn_t OperatorFn = getArithmeticOperatorFn(pExprs->_node.optr);
      OperatorFn(pLeftOutput, numOfRows, TSDB_DATA_TYPE_DOUBLE, &pRight->pVal->i64, 1, pRight->pVal->nType, pOutput, TSDB_ORDER_ASC);
    }
  } else if (pLeft->nodeType == TSQL_NODE_COL) {
    // column data specified on left-hand-side
    char *pLeftInputData = getSourceDataBlock(param, pLeft->pSchema->name, pLeft->pSchema->colId);
    if (pRight->nodeType == TSQL_NODE_EXPR) {  // columnLeft + expr2
      _arithmetic_operator_fn_t OperatorFn = getArithmeticOperatorFn(pExprs->_node.optr);

      if (order == TSDB_ORDER_DESC) {
        reverseCopy(pdata, pLeftInputData, pLeft->pSchema->type, numOfRows);
        OperatorFn(pdata, numOfRows, pLeft->pSchema->type, pRightOutput, numOfRows, TSDB_DATA_TYPE_DOUBLE, pOutput, TSDB_ORDER_ASC);
      } else {
        OperatorFn(pLeftInputData, numOfRows, pLeft->pSchema->type, pRightOutput, numOfRows, TSDB_DATA_TYPE_DOUBLE, pOutput, TSDB_ORDER_ASC);
      }

    } else if (pRight->nodeType == TSQL_NODE_COL) {  // columnLeft + columnRight
      // column data specified on right-hand-side
      char *pRightInputData = getSourceDataBlock(param, pRight->pSchema->name, pRight->pSchema->colId);
      _arithmetic_operator_fn_t OperatorFn = getArithmeticOperatorFn(pExprs->_node.optr);

      // both columns are descending order, do not reverse the source data
      OperatorFn(pLeftInputData, numOfRows, pLeft->pSchema->type, pRightInputData, numOfRows, pRight->pSchema->type, pOutput, order);
    } else if (pRight->nodeType == TSQL_NODE_VALUE) {  // columnLeft + 12
      _arithmetic_operator_fn_t OperatorFn = getArithmeticOperatorFn(pExprs->_node.optr);

      if (order == TSDB_ORDER_DESC) {
        reverseCopy(pdata, pLeftInputData, pLeft->pSchema->type, numOfRows);
        OperatorFn(pdata, numOfRows, pLeft->pSchema->type, &pRight->pVal->i64, 1, pRight->pVal->nType, pOutput, TSDB_ORDER_ASC);
      } else {
        OperatorFn(pLeftInputData, numOfRows, pLeft->pSchema->type, &pRight->pVal->i64, 1, pRight->pVal->nType, pOutput, TSDB_ORDER_ASC);
      }
    }
  } else {
    // column data specified on left-hand-side
    if (pRight->nodeType == TSQL_NODE_EXPR) {  // 12 + expr2
      _arithmetic_operator_fn_t OperatorFn = getArithmeticOperatorFn(pExprs->_node.optr);
      OperatorFn(&pLeft->pVal->i64, 1, pLeft->pVal->nType, pRightOutput, numOfRows, TSDB_DATA_TYPE_DOUBLE, pOutput, TSDB_ORDER_ASC);

    } else if (pRight->nodeType == TSQL_NODE_COL) {  // 12 + columnRight
      // column data specified on right-hand-side
      char *pRightInputData = getSourceDataBlock(param, pRight->pSchema->name, pRight->pSchema->colId);
      _arithmetic_operator_fn_t OperatorFn = getArithmeticOperatorFn(pExprs->_node.optr);

      if (order == TSDB_ORDER_DESC) {
        reverseCopy(pdata, pRightInputData, pRight->pSchema->type, numOfRows);
        OperatorFn(&pLeft->pVal->i64, 1, pLeft->pVal->nType, pdata, numOfRows, pRight->pSchema->type, pOutput, TSDB_ORDER_ASC);
      } else {
        OperatorFn(&pLeft->pVal->i64, 1, pLeft->pVal->nType, pRightInputData, numOfRows, pRight->pSchema->type, pOutput, TSDB_ORDER_ASC);
      }

    } else if (pRight->nodeType == TSQL_NODE_VALUE) {  // 12 + 12
      _arithmetic_operator_fn_t OperatorFn = getArithmeticOperatorFn(pExprs->_node.optr);
      OperatorFn(&pLeft->pVal->i64, 1, pLeft->pVal->nType, &pRight->pVal->i64, 1, pRight->pVal->nType, pOutput, TSDB_ORDER_ASC);
    }
  }

  tfree(pdata);
  tfree(pLeftOutput);
  tfree(pRightOutput);
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
      tbufWriteInt64(bw, pVal->i64);
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
  CLEANUP_PUSH_VOID_PTR_PTR(true, tExprTreeDestroy, pExpr, NULL);
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
      pVal->i64 = tbufReadInt64(br);
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
  CLEANUP_PUSH_VOID_PTR_PTR(true, tExprTreeDestroy, expr, NULL);

  expr->nodeType = TSQL_NODE_EXPR;

  tExprNode* left = exception_calloc(1, sizeof(tExprNode));
  expr->_node.pLeft = left;

  left->nodeType = TSQL_NODE_COL;
  SSchema* pSchema = exception_calloc(1, sizeof(SSchema));
  left->pSchema = pSchema;

  *pSchema = *tGetTbnameColumnSchema();

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

tExprNode* exprdup(tExprNode* pTree) {
  if (pTree == NULL) {
    return NULL;
  }

  tExprNode* pNode = calloc(1, sizeof(tExprNode));
  if (pTree->nodeType == TSQL_NODE_EXPR) {
    tExprNode* pLeft  = exprdup(pTree->_node.pLeft);
    tExprNode* pRight = exprdup(pTree->_node.pRight);

    pNode->nodeType     = TSQL_NODE_EXPR;
    pNode->_node.pLeft  = pLeft;
    pNode->_node.pRight = pRight;
  } else if (pTree->nodeType == TSQL_NODE_VALUE) {
    pNode->pVal = calloc(1, sizeof(tVariant));
    tVariantAssign(pNode->pVal, pTree->pVal);
  } else if (pTree->nodeType == TSQL_NODE_COL) {
    pNode->pSchema = calloc(1, sizeof(SSchema));
    *pNode->pSchema = *pTree->pSchema;
  }

  return pNode;
}

