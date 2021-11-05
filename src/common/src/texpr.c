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

#include <texpr.h>
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


int32_t exprTreeValidateFunctionNode(tExprNode *pExpr) {
  int32_t code = TSDB_CODE_SUCCESS;
  //TODO: check childs for every function
  switch (pExpr->_func.functionId) {
    case TSDB_FUNC_SCALAR_POW:
    case TSDB_FUNC_SCALAR_LOG: {
      if (pExpr->_func.numChildren != 2) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
      tExprNode *child1 = pExpr->_func.pChildren[0];
      tExprNode *child2 = pExpr->_func.pChildren[1];
      if (child2->nodeType == TSQL_NODE_VALUE) {
        if (!IS_NUMERIC_TYPE(child2->pVal->nType)) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
        }
        child2->resultType = (int16_t)child2->pVal->nType;
        child2->resultBytes = (int16_t)tDataTypes[child2->resultType].bytes;
      }
      else if (!IS_NUMERIC_TYPE(child2->resultType)) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      if (!IS_NUMERIC_TYPE(child1->resultType)) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      pExpr->resultType = TSDB_DATA_TYPE_DOUBLE;
      pExpr->resultBytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;
      break;
    }
    case TSDB_FUNC_SCALAR_CONCAT: {
      if (pExpr->_func.numChildren != 2) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
      tExprNode *child1 = pExpr->_func.pChildren[0];
      tExprNode *child2 = pExpr->_func.pChildren[1];
      if (child1->nodeType == TSQL_NODE_VALUE) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      } else if (!IS_VAR_DATA_TYPE(child1->resultType)) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      if (child2->nodeType == TSQL_NODE_VALUE) {
        if (!IS_VAR_DATA_TYPE(child2->pVal->nType)) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
        }
        tVariantTypeSetType(child2->pVal, child1->resultType);
        child2->resultType = (int16_t)child2->pVal->nType;
        child2->resultBytes = (int16_t)(child2->pVal->nLen + VARSTR_HEADER_SIZE);
      }

      if (child1->resultType != child2->resultType) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      pExpr->resultType = child1->resultType;
      pExpr->resultBytes = child1->resultBytes + child2->resultBytes - VARSTR_HEADER_SIZE;

    }

    default:
      break;
  }
  return code;
}

int32_t exprTreeValidateExprNode(tExprNode *pExpr) {
  //TODO: modify. keep existing behavior
  if (pExpr->_node.optr == TSDB_BINARY_OP_ADD || pExpr->_node.optr == TSDB_BINARY_OP_SUBTRACT ||
      pExpr->_node.optr == TSDB_BINARY_OP_MULTIPLY || pExpr->_node.optr == TSDB_BINARY_OP_DIVIDE ||
      pExpr->_node.optr == TSDB_BINARY_OP_REMAINDER) {
    pExpr->resultType = TSDB_DATA_TYPE_DOUBLE;
    pExpr->resultBytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;
    return TSDB_CODE_SUCCESS;
  } else {
    return TSDB_CODE_SUCCESS;
  }
}

int32_t exprTreeValidateTree(tExprNode *pExpr) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  if (pExpr->nodeType == TSQL_NODE_VALUE) {
    pExpr->resultType = -1;
    pExpr->resultBytes = -1;
  } else if (pExpr->nodeType == TSQL_NODE_COL) {
    pExpr->resultType = pExpr->pSchema->type;
    if (pExpr->pSchema->colId != TSDB_TBNAME_COLUMN_INDEX) {
      pExpr->resultBytes = pExpr->pSchema->bytes;
    } else {
      pExpr->resultBytes = tGetTbnameColumnSchema()->bytes;
    }
  } else if (pExpr->nodeType == TSQL_NODE_EXPR) {
    code = exprTreeValidateTree(pExpr->_node.pLeft);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    code = exprTreeValidateTree(pExpr->_node.pRight);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    code = exprTreeValidateExprNode(pExpr);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  } else if (pExpr->nodeType == TSQL_NODE_FUNC) {
    for (int32_t i = 0; i < pExpr->_func.numChildren; ++i) {
      code = exprTreeValidateTree(pExpr->_func.pChildren[i]);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
    code = exprTreeValidateFunctionNode(pExpr);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

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
    tfree(pNode->pSchema);
  } else if (pNode->nodeType == TSQL_NODE_FUNC) {
    doExprTreeDestroy(&pNode, fp);
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
  } else if ((*pExpr)->nodeType == TSQL_NODE_FUNC) {
    for (int i = 0; i < (*pExpr)->_func.numChildren; ++i) {
      doExprTreeDestroy((*pExpr)->_func.pChildren + i, fp);
    }
    free((*pExpr)->_func.pChildren);
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

void exprTreeExprNodeTraverse(tExprNode *pExpr, int32_t numOfRows, tExprOperandInfo *output, void *param, int32_t order,
                              char *(*getSourceDataBlock)(void *, const char*, int32_t));

void exprTreeFunctionNodeTraverse(tExprNode *pExprs, int32_t numOfRows, tExprOperandInfo *output, void *param, int32_t order,
                              char *(*getSourceDataBlock)(void *, const char*, int32_t));
void exprTreeInternalNodeTraverse(tExprNode *pExpr, int32_t numOfRows, tExprOperandInfo *output, void *param, int32_t order,
                                  char *(*getSourceDataBlock)(void *, const char*, int32_t));

void exprTreeNodeTraverse(tExprNode *pExpr, int32_t numOfRows, tExprOperandInfo *output, void *param, int32_t order,
                          char *(*getSourceDataBlock)(void*, const char*, int32_t)) {
  char* pOutput = output->data;
  if (pExpr->nodeType == TSQL_NODE_FUNC || pExpr->nodeType == TSQL_NODE_EXPR) {
    exprTreeInternalNodeTraverse(pExpr, numOfRows, output, param, order, getSourceDataBlock);
  } else if (pExpr->nodeType == TSQL_NODE_COL) {
    char *pInputData = getSourceDataBlock(param, pExpr->pSchema->name, pExpr->pSchema->colId);
    if (order == TSDB_ORDER_DESC) {
      reverseCopy(pOutput, pInputData, pExpr->pSchema->type, numOfRows);
    } else {
      memcpy(pOutput, pInputData, pExpr->pSchema->bytes*numOfRows);
    }
    assert(pExpr->resultType == pExpr->pSchema->type && pExpr->pSchema->bytes == pExpr->resultBytes);
    output->numOfRows = numOfRows;
  } else if (pExpr->nodeType == TSQL_NODE_VALUE) {
    tVariantDump(pExpr->pVal, pOutput, pExpr->resultType, true);
    output->numOfRows = 1;
  }
}

void exprTreeInternalNodeTraverse(tExprNode *pExpr, int32_t numOfRows, tExprOperandInfo *output, void *param, int32_t order,
                                  char *(*getSourceDataBlock)(void *, const char*, int32_t)) {
  if (pExpr->nodeType == TSQL_NODE_FUNC) {
    exprTreeFunctionNodeTraverse(pExpr, numOfRows, output, param, order, getSourceDataBlock);
  } else if (pExpr->nodeType == TSQL_NODE_EXPR){
    exprTreeExprNodeTraverse(pExpr, numOfRows, output, param, order, getSourceDataBlock);
  }
}

void exprTreeFunctionNodeTraverse(tExprNode *pExpr, int32_t numOfRows, tExprOperandInfo *output, void *param, int32_t order,
                                  char *(*getSourceDataBlock)(void *, const char*, int32_t)) {
  uint8_t numChildren = pExpr->_func.numChildren;
  if (numChildren == 0) {
    _expr_scalar_function_t scalarFn = getExprScalarFunction(pExpr->_func.functionId);
    scalarFn(pExpr->_func.functionId, NULL, 0, output, order);
    output->numOfRows = numOfRows;
    return;
  }

  char** pChildrenOutput = calloc(numChildren, sizeof(char*));
  tExprOperandInfo* pChildrenResults = calloc(numChildren, sizeof(tExprOperandInfo));

  tExprOperandInfo* pInputs = calloc(numChildren, sizeof(tExprOperandInfo));
  for (int i = 0; i < numChildren; ++i) {
    tExprNode *pChild = pExpr->_func.pChildren[i];
    pInputs[i].type = pChild->resultType;
    pInputs[i].bytes = pChild->resultBytes;
  }

  for (int i = 0; i < numChildren; ++i) {
    tExprNode *pChild = pExpr->_func.pChildren[i];
    if (pChild->nodeType == TSQL_NODE_EXPR || pChild->nodeType == TSQL_NODE_FUNC) {
      pChildrenOutput[i] = malloc(pChild->resultBytes * numOfRows);
      pChildrenResults[i].data = pChildrenOutput[i];
      exprTreeInternalNodeTraverse(pChild, numOfRows, pChildrenResults+i, param, order, getSourceDataBlock);
      pInputs[i].data = pChildrenOutput[i];
      pInputs[i].numOfRows = pChildrenResults[i].numOfRows;
    } else if (pChild->nodeType == TSQL_NODE_COL) {
      assert(pChild->resultType == pChild->pSchema->type && pChild->resultBytes == pChild->pSchema->bytes);
      char *pInputData = getSourceDataBlock(param, pChild->pSchema->name, pChild->pSchema->colId);
      if (order == TSDB_ORDER_DESC) {
        pChildrenOutput[i] = malloc(pChild->pSchema->bytes * numOfRows);
        reverseCopy(pChildrenOutput[i], pInputData, pChild->pSchema->type, numOfRows);
        pInputs[i].data = pChildrenOutput[i];
      } else {
        pInputs[i].data = pInputData;
      }
      pInputs[i].numOfRows = numOfRows;
    } else if (pChild->nodeType == TSQL_NODE_VALUE) {
      pChildrenOutput[i] = malloc(pChild->resultBytes);
      tVariantDump(pChild->pVal, pChildrenOutput[i], pChild->resultType, true);
      pInputs[i].data = pChildrenOutput[i];
      pInputs[i].numOfRows = 1;
    }
  }

  _expr_scalar_function_t scalarFn = getExprScalarFunction(pExpr->_func.functionId);
  output->type = pExpr->resultType;
  output->bytes = pExpr->resultBytes;
  scalarFn(pExpr->_func.functionId, pInputs, numChildren, output, order);
  output->numOfRows = numOfRows;

  tfree(pChildrenResults);
  for (int i = 0; i < numChildren; ++i) {
    tfree(pChildrenOutput[i]);
  }
  tfree(pInputs);
  tfree(pChildrenOutput);
}

void exprTreeExprNodeTraverse(tExprNode *pExpr, int32_t numOfRows, tExprOperandInfo *output, void *param, int32_t order,
                            char *(*getSourceDataBlock)(void *, const char*, int32_t)) {

  tExprNode *pLeft = pExpr->_node.pLeft;
  tExprNode *pRight = pExpr->_node.pRight;

  /* the left output has result from the left child syntax tree */
  char *pLeftOutput = (char*)malloc(sizeof(int64_t) * numOfRows);
  tExprOperandInfo left;
  left.type = TSDB_DATA_TYPE_DOUBLE;
  left.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;
  left.data = pLeftOutput;
  if (pLeft->nodeType == TSQL_NODE_EXPR || pLeft->nodeType == TSQL_NODE_FUNC) {
    exprTreeInternalNodeTraverse(pLeft, numOfRows, &left, param, order, getSourceDataBlock);
  }

  /* the right output has result from the right child syntax tree */
  char *pRightOutput = malloc(sizeof(int64_t) * numOfRows);
  tExprOperandInfo right;
  right.type = TSDB_DATA_TYPE_DOUBLE;
  right.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;
  right.data = pRightOutput;

  char *pData = malloc(sizeof(int64_t) * numOfRows);

  if (pRight->nodeType == TSQL_NODE_EXPR || pRight->nodeType == TSQL_NODE_FUNC) {
    exprTreeInternalNodeTraverse(pRight, numOfRows, &right, param, order, getSourceDataBlock);
  }

  if (pLeft->nodeType == TSQL_NODE_EXPR || pLeft->nodeType == TSQL_NODE_FUNC) {
    if (pRight->nodeType == TSQL_NODE_EXPR || pRight->nodeType == TSQL_NODE_FUNC) {
      /*
       * exprLeft + exprRight
       * the type of returned value of one expression is always double float precious
       */
      _arithmetic_operator_fn_t OperatorFn = getArithmeticOperatorFn(pExpr->_node.optr);
      OperatorFn(pLeftOutput, numOfRows, TSDB_DATA_TYPE_DOUBLE, pRightOutput, numOfRows, TSDB_DATA_TYPE_DOUBLE, output->data, TSDB_ORDER_ASC);
      output->numOfRows = MAX(left.numOfRows, right.numOfRows);
    } else if (pRight->nodeType == TSQL_NODE_COL) {  // exprLeft + columnRight
      _arithmetic_operator_fn_t OperatorFn = getArithmeticOperatorFn(pExpr->_node.optr);

      // set input buffer
      char *pInputData = getSourceDataBlock(param, pRight->pSchema->name, pRight->pSchema->colId);
      if (order == TSDB_ORDER_DESC) {
        reverseCopy(pData, pInputData, pRight->pSchema->type, numOfRows);
        OperatorFn(pLeftOutput, numOfRows, TSDB_DATA_TYPE_DOUBLE, pData, numOfRows, pRight->pSchema->type, output->data, TSDB_ORDER_ASC);
      } else {
        OperatorFn(pLeftOutput, numOfRows, TSDB_DATA_TYPE_DOUBLE, pInputData, numOfRows, pRight->pSchema->type, output->data, TSDB_ORDER_ASC);
      }
      output->numOfRows = numOfRows;
    } else if (pRight->nodeType == TSQL_NODE_VALUE) {  // exprLeft + 12
      _arithmetic_operator_fn_t OperatorFn = getArithmeticOperatorFn(pExpr->_node.optr);
      OperatorFn(pLeftOutput, numOfRows, TSDB_DATA_TYPE_DOUBLE, &pRight->pVal->i64, 1, pRight->pVal->nType, output->data, TSDB_ORDER_ASC);
      output->numOfRows = numOfRows;
    }
  } else if (pLeft->nodeType == TSQL_NODE_COL) {
    // column data specified on left-hand-side
    char *pLeftInputData = getSourceDataBlock(param, pLeft->pSchema->name, pLeft->pSchema->colId);
    if (pRight->nodeType == TSQL_NODE_EXPR || pRight->nodeType == TSQL_NODE_FUNC) {  // columnLeft + expr2
      _arithmetic_operator_fn_t OperatorFn = getArithmeticOperatorFn(pExpr->_node.optr);

      if (order == TSDB_ORDER_DESC) {
        reverseCopy(pData, pLeftInputData, pLeft->pSchema->type, numOfRows);
        OperatorFn(pData, numOfRows, pLeft->pSchema->type, pRightOutput, numOfRows, TSDB_DATA_TYPE_DOUBLE, output->data, TSDB_ORDER_ASC);
      } else {
        OperatorFn(pLeftInputData, numOfRows, pLeft->pSchema->type, pRightOutput, numOfRows, TSDB_DATA_TYPE_DOUBLE, output->data, TSDB_ORDER_ASC);
      }

    } else if (pRight->nodeType == TSQL_NODE_COL) {  // columnLeft + columnRight
      // column data specified on right-hand-side
      char *pRightInputData = getSourceDataBlock(param, pRight->pSchema->name, pRight->pSchema->colId);
      _arithmetic_operator_fn_t OperatorFn = getArithmeticOperatorFn(pExpr->_node.optr);

      // both columns are descending order, do not reverse the source data
      OperatorFn(pLeftInputData, numOfRows, pLeft->pSchema->type, pRightInputData, numOfRows, pRight->pSchema->type, output->data, order);
    } else if (pRight->nodeType == TSQL_NODE_VALUE) {  // columnLeft + 12
      _arithmetic_operator_fn_t OperatorFn = getArithmeticOperatorFn(pExpr->_node.optr);

      if (order == TSDB_ORDER_DESC) {
        reverseCopy(pData, pLeftInputData, pLeft->pSchema->type, numOfRows);
        OperatorFn(pData, numOfRows, pLeft->pSchema->type, &pRight->pVal->i64, 1, pRight->pVal->nType, output->data, TSDB_ORDER_ASC);
      } else {
        OperatorFn(pLeftInputData, numOfRows, pLeft->pSchema->type, &pRight->pVal->i64, 1, pRight->pVal->nType, output->data, TSDB_ORDER_ASC);
      }
    }
    output->numOfRows = numOfRows;
  } else {
    // column data specified on left-hand-side
    if (pRight->nodeType == TSQL_NODE_EXPR || pRight->nodeType == TSQL_NODE_FUNC) {  // 12 + expr2
      _arithmetic_operator_fn_t OperatorFn = getArithmeticOperatorFn(pExpr->_node.optr);
      OperatorFn(&pLeft->pVal->i64, 1, pLeft->pVal->nType, pRightOutput, numOfRows, TSDB_DATA_TYPE_DOUBLE, output->data, TSDB_ORDER_ASC);
      output->numOfRows = right.numOfRows;
    } else if (pRight->nodeType == TSQL_NODE_COL) {  // 12 + columnRight
      // column data specified on right-hand-side
      char *pRightInputData = getSourceDataBlock(param, pRight->pSchema->name, pRight->pSchema->colId);
      _arithmetic_operator_fn_t OperatorFn = getArithmeticOperatorFn(pExpr->_node.optr);

      if (order == TSDB_ORDER_DESC) {
        reverseCopy(pData, pRightInputData, pRight->pSchema->type, numOfRows);
        OperatorFn(&pLeft->pVal->i64, 1, pLeft->pVal->nType, pData, numOfRows, pRight->pSchema->type, output->data, TSDB_ORDER_ASC);
      } else {
        OperatorFn(&pLeft->pVal->i64, 1, pLeft->pVal->nType, pRightInputData, numOfRows, pRight->pSchema->type, output->data, TSDB_ORDER_ASC);
      }
      output->numOfRows = numOfRows;
    } else if (pRight->nodeType == TSQL_NODE_VALUE) {  // 12 + 12
      _arithmetic_operator_fn_t OperatorFn = getArithmeticOperatorFn(pExpr->_node.optr);
      OperatorFn(&pLeft->pVal->i64, 1, pLeft->pVal->nType, &pRight->pVal->i64, 1, pRight->pVal->nType, output->data, TSDB_ORDER_ASC);
      output->numOfRows = 1;
    }
  }

  tfree(pData);
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
  } else if (expr->nodeType == TSQL_NODE_FUNC) {
    tbufWriteInt16(bw, expr->_func.functionId);
    tbufWriteUint8(bw, expr->_func.numChildren);
    for (int i = 0; i < expr->_func.numChildren; ++i) {
      exprTreeToBinaryImpl(bw, expr->_func.pChildren[i]);
    }
  }
  tbufWriteInt16(bw, expr->resultType);
  tbufWriteInt16(bw, expr->resultBytes);
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
  } else if (pExpr->nodeType == TSQL_NODE_FUNC) {
    pExpr->_func.functionId = tbufReadInt16(br);
    pExpr->_func.numChildren = tbufReadUint8(br);
    pExpr->_func.pChildren = (tExprNode**)calloc(pExpr->_func.numChildren, sizeof(tExprNode*));
    for (int i = 0; i < pExpr->_func.numChildren; ++i) {
      pExpr->_func.pChildren[i] = exprTreeFromBinaryImpl(br);
    }
  }
  pExpr->resultType = tbufReadInt16(br);
  pExpr->resultBytes = tbufReadInt16(br);
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

void buildFilterSetFromBinary(void **q, const char *buf, int32_t len) {
  SBufferReader br = tbufInitReader(buf, len, false); 
  uint32_t type  = tbufReadUint32(&br);     
  SHashObj *pObj = taosHashInit(256, taosGetDefaultHashFunction(type), true, false);
  
  taosHashSetEqualFp(pObj, taosGetDefaultEqualFunction(type)); 
  
  int dummy = -1;
  int32_t sz = tbufReadInt32(&br);
  for (int32_t i = 0; i < sz; i++) {
    if (type == TSDB_DATA_TYPE_BOOL || IS_SIGNED_NUMERIC_TYPE(type)) {
      int64_t val = tbufReadInt64(&br); 
      taosHashPut(pObj, (char *)&val, sizeof(val),  &dummy, sizeof(dummy));
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      uint64_t val = tbufReadUint64(&br); 
      taosHashPut(pObj, (char *)&val, sizeof(val),  &dummy, sizeof(dummy));
    }
    else if (type == TSDB_DATA_TYPE_TIMESTAMP) {
      int64_t val = tbufReadInt64(&br); 
      taosHashPut(pObj, (char *)&val, sizeof(val),  &dummy, sizeof(dummy));
    } else if (type == TSDB_DATA_TYPE_DOUBLE || type == TSDB_DATA_TYPE_FLOAT) {
      double  val = tbufReadDouble(&br);
      taosHashPut(pObj, (char *)&val, sizeof(val), &dummy, sizeof(dummy));
    } else if (type == TSDB_DATA_TYPE_BINARY) {
      size_t  t = 0;
      const char *val = tbufReadBinary(&br, &t);
      taosHashPut(pObj, (char *)val, t, &dummy, sizeof(dummy));
    } else if (type == TSDB_DATA_TYPE_NCHAR) {
      size_t  t = 0;
      const char *val = tbufReadBinary(&br, &t);      
      taosHashPut(pObj, (char *)val, t, &dummy, sizeof(dummy));
    }
  } 
  *q = (void *)pObj;
}

void convertFilterSetFromBinary(void **q, const char *buf, int32_t len, uint32_t tType) {
  SBufferReader br = tbufInitReader(buf, len, false); 
  uint32_t sType  = tbufReadUint32(&br);     
  SHashObj *pObj = taosHashInit(256, taosGetDefaultHashFunction(tType), true, false);
  
  taosHashSetEqualFp(pObj, taosGetDefaultEqualFunction(tType)); 
  
  int dummy = -1;
  tVariant tmpVar = {0};  
  size_t  t = 0;
  int32_t sz = tbufReadInt32(&br);
  void *pvar = NULL;  
  int64_t val = 0;
  int32_t bufLen = 0;
  if (IS_NUMERIC_TYPE(sType)) {
    bufLen = 60;  // The maximum length of string that a number is converted to.
  } else {
    bufLen = 128;
  }

  char *tmp = calloc(1, bufLen * TSDB_NCHAR_SIZE);
    
  for (int32_t i = 0; i < sz; i++) {
    switch (sType) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_TINYINT: {
      *(uint8_t *)&val = (uint8_t)tbufReadInt64(&br); 
      t = sizeof(val);
      pvar = &val;
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_SMALLINT: {
      *(uint16_t *)&val = (uint16_t)tbufReadInt64(&br); 
      t = sizeof(val);
      pvar = &val;
      break;
    }
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_INT: {
      *(uint32_t *)&val = (uint32_t)tbufReadInt64(&br); 
      t = sizeof(val);
      pvar = &val;
      break;
    }
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_UBIGINT:
    case TSDB_DATA_TYPE_BIGINT: {
      *(uint64_t *)&val = (uint64_t)tbufReadInt64(&br); 
      t = sizeof(val);
      pvar = &val;
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      *(double *)&val = tbufReadDouble(&br);
      t = sizeof(val);
      pvar = &val;
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      *(float *)&val = (float)tbufReadDouble(&br);
      t = sizeof(val);
      pvar = &val;
      break;
    }
    case TSDB_DATA_TYPE_BINARY: {
      pvar = (char *)tbufReadBinary(&br, &t);
      break;
    }
    case TSDB_DATA_TYPE_NCHAR: {
      pvar = (char *)tbufReadBinary(&br, &t);      
      break;
    }
    default:
      taosHashCleanup(pObj);
      *q = NULL;
      return;
    }
    
    tVariantCreateFromBinary(&tmpVar, (char *)pvar, t, sType);

    if (bufLen < t) {
      tmp = realloc(tmp, t * TSDB_NCHAR_SIZE);
      bufLen = (int32_t)t;
    }

    switch (tType) {
      case TSDB_DATA_TYPE_BOOL:
      case TSDB_DATA_TYPE_UTINYINT:
      case TSDB_DATA_TYPE_TINYINT: {
        if (tVariantDump(&tmpVar, (char *)&val, tType, false)) {
          goto err_ret;
        }
        pvar = &val;
        t = sizeof(val);
        break;
      }
      case TSDB_DATA_TYPE_USMALLINT:
      case TSDB_DATA_TYPE_SMALLINT: {
        if (tVariantDump(&tmpVar, (char *)&val, tType, false)) {
          goto err_ret;
        }
        pvar = &val;
        t = sizeof(val);
        break;
      }
      case TSDB_DATA_TYPE_UINT:
      case TSDB_DATA_TYPE_INT: {
        if (tVariantDump(&tmpVar, (char *)&val, tType, false)) {
          goto err_ret;
        }
        pvar = &val;
        t = sizeof(val);
        break;
      }
      case TSDB_DATA_TYPE_TIMESTAMP:
      case TSDB_DATA_TYPE_UBIGINT:
      case TSDB_DATA_TYPE_BIGINT: {
        if (tVariantDump(&tmpVar, (char *)&val, tType, false)) {
          goto err_ret;
        }
        pvar = &val;
        t = sizeof(val);
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        if (tVariantDump(&tmpVar, (char *)&val, tType, false)) {
          goto err_ret;
        }
        pvar = &val;
        t = sizeof(val);
        break;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        if (tVariantDump(&tmpVar, (char *)&val, tType, false)) {
          goto err_ret;
        }
        pvar = &val;
        t = sizeof(val);
        break;
      }
      case TSDB_DATA_TYPE_BINARY: {
        if (tVariantDump(&tmpVar, tmp, tType, true)) {
          goto err_ret;
        }
        t = varDataLen(tmp);
        pvar = varDataVal(tmp);
        break;
      }
      case TSDB_DATA_TYPE_NCHAR: {
        if (tVariantDump(&tmpVar, tmp, tType, true)) {
          goto err_ret;
        }
        t = varDataLen(tmp);
        pvar = varDataVal(tmp);        
        break;
      }
      default:
        goto err_ret;
    }
    
    taosHashPut(pObj, (char *)pvar, t,  &dummy, sizeof(dummy));
    tVariantDestroy(&tmpVar);
    memset(&tmpVar, 0, sizeof(tmpVar));
  } 

  *q = (void *)pObj;
  pObj = NULL;
  
err_ret:  
  tVariantDestroy(&tmpVar);
  taosHashCleanup(pObj);
  tfree(tmp);
}


tExprNode* exprdup(tExprNode* pNode) {
  if (pNode == NULL) {
    return NULL;
  }

  tExprNode* pCloned = calloc(1, sizeof(tExprNode));
  if (pNode->nodeType == TSQL_NODE_EXPR) {
    tExprNode* pLeft  = exprdup(pNode->_node.pLeft);
    tExprNode* pRight = exprdup(pNode->_node.pRight);

    pCloned->_node.pLeft  = pLeft;
    pCloned->_node.pRight = pRight;
    pCloned->_node.optr  = pNode->_node.optr;
    pCloned->_node.hasPK = pNode->_node.hasPK;
  } else if (pNode->nodeType == TSQL_NODE_VALUE) {
    pCloned->pVal = calloc(1, sizeof(tVariant));
    tVariantAssign(pCloned->pVal, pNode->pVal);
  } else if (pNode->nodeType == TSQL_NODE_COL) {
    pCloned->pSchema = calloc(1, sizeof(SSchema));
    *pCloned->pSchema = *pNode->pSchema;
  } else if (pNode->nodeType == TSQL_NODE_FUNC) {
    pCloned->_func.functionId = pNode->_func.functionId;
    pCloned->_func.numChildren = pNode->_func.numChildren;
    pCloned->_func.pChildren = calloc(pNode->_func.numChildren, sizeof(tExprNode*));
    for (int i = 0; i < pNode->_func.numChildren; ++i) {
      pCloned->_func.pChildren[i] = exprdup(pNode->_func.pChildren[i]);
    }
  }

  pCloned->nodeType = pNode->nodeType;
  return pCloned;
}


void vectorPow(int16_t functionId, tExprOperandInfo* pInputs, uint8_t numInputs, tExprOperandInfo* pOutput, int32_t order) {
  assert(numInputs == 2);
  assert(pInputs[1].numOfRows == 1  && pInputs[0].numOfRows >= 1);
  int numOfRows = pInputs[0].numOfRows;

  double base = 0;
  GET_TYPED_DATA(base, double, pInputs[1].type, pInputs[1].data);

  for (int i = 0; i < numOfRows; ++i) {
    char* pInputData = pInputs[0].data + i * pInputs[0].bytes;
    char* pOutputData = pOutput->data + i * pOutput->bytes;
    if (isNull(pInputData, pInputs[0].type)) {
      setNull(pOutputData, pOutput->type, pOutput->bytes);
    } else {
      double v1 = 0;
      GET_TYPED_DATA(v1, double, pInputs[0].type, pInputData);
      double result = pow(v1, base);
      SET_TYPED_DATA(pOutputData, pOutput->type, result);
    }
  }
}

void vectorLog(int16_t functionId, tExprOperandInfo* pInputs, uint8_t numInputs, tExprOperandInfo* pOutput, int32_t order) {
  assert(numInputs == 2);
  assert(pInputs[1].numOfRows == 1  && pInputs[0].numOfRows >= 1);
  int numOfRows = pInputs[0].numOfRows;

  double base = 0;
  GET_TYPED_DATA(base, double, pInputs[1].type, pInputs[1].data);

  for (int i = 0; i < numOfRows; ++i) {
    char* pInputData = pInputs[0].data + i * pInputs[0].bytes;
    char* pOutputData = pOutput->data + i * pOutput->bytes;
    if (isNull(pInputData, pInputs[0].type)) {
      setNull(pOutputData, pOutput->type, pOutput->bytes);
    } else {
      double v1 = 0;
      GET_TYPED_DATA(v1, double, pInputs[0].type, pInputData);
      double result = log(v1) / log(base);
      SET_TYPED_DATA(pOutputData, pOutput->type, result);
    }
  }
}

void vectorConcat(int16_t functionId, tExprOperandInfo* pInputs, uint8_t numInputs, tExprOperandInfo* pOutput, int32_t order) {

}

_expr_scalar_function_t getExprScalarFunction(uint16_t funcId) {
  assert(TSDB_FUNC_IS_SCALAR(funcId));
  int16_t scalaIdx = TSDB_FUNC_SCALAR_INDEX(funcId);
  assert(scalaIdx>=0 && scalaIdx <= TSDB_FUNC_SCALAR_MAX_NUM);
  return aScalarFunctions[scalaIdx].scalarFunc;
}

tScalarFunctionInfo aScalarFunctions[] = {
    {
        TSDB_FUNC_SCALAR_POW,
        "pow",
        vectorPow
    },
    {
        TSDB_FUNC_SCALAR_LOG,
        "log",
        vectorLog
    },
    {
        TSDB_FUNC_SCALAR_CONCAT,
        "concat",
        vectorConcat
    },
};