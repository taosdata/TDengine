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

static int32_t exprValidateMathNode(tExprNode *pExpr);
static int32_t exprValidateStringConcatNode(tExprNode *pExpr);
static int32_t exprValidateStringConcatWsNode(tExprNode *pExpr);
static int32_t exprValidateStringLengthNode(tExprNode *pExpr);
static int32_t exprValidateCastNode(char* msgbuf, tExprNode *pExpr);

static int32_t exprInvalidOperationMsg(char *msgbuf, const char *msg) {
  const char* msgFormat = "invalid operation: %s";

  sprintf(msgbuf, msgFormat, msg);

  return TSDB_CODE_TSC_INVALID_OPERATION;
}



int32_t exprTreeValidateFunctionNode(char* msgbuf, tExprNode *pExpr) {
  int32_t code = TSDB_CODE_SUCCESS;
  //TODO: check childs for every function
  switch (pExpr->_func.functionId) {
    case TSDB_FUNC_SCALAR_POW:
    case TSDB_FUNC_SCALAR_LOG:
    case TSDB_FUNC_SCALAR_ABS:
    case TSDB_FUNC_SCALAR_ACOS:
    case TSDB_FUNC_SCALAR_ASIN:
    case TSDB_FUNC_SCALAR_ATAN:
    case TSDB_FUNC_SCALAR_COS:
    case TSDB_FUNC_SCALAR_SIN:
    case TSDB_FUNC_SCALAR_TAN:
    case TSDB_FUNC_SCALAR_SQRT:
    case TSDB_FUNC_SCALAR_CEIL:
    case TSDB_FUNC_SCALAR_FLOOR:
    case TSDB_FUNC_SCALAR_ROUND: {
      return exprValidateMathNode(pExpr);
    }
    case TSDB_FUNC_SCALAR_CONCAT: {
      return exprValidateStringConcatNode(pExpr);
    }
    case TSDB_FUNC_SCALAR_LENGTH:
    case TSDB_FUNC_SCALAR_CHAR_LENGTH: {
      return exprValidateStringLengthNode(pExpr);
    }
    case TSDB_FUNC_SCALAR_CAST: {
      return exprValidateCastNode(msgbuf, pExpr);
    }
    case TSDB_FUNC_SCALAR_CONCAT_WS: {
      return exprValidateStringConcatWsNode(pExpr);
    }

    default:
      break;
  }
  return code;
}

int32_t exprTreeValidateExprNode(tExprNode *pExpr) {
  if (pExpr->_node.optr == TSDB_BINARY_OP_ADD || pExpr->_node.optr == TSDB_BINARY_OP_SUBTRACT ||
      pExpr->_node.optr == TSDB_BINARY_OP_MULTIPLY || pExpr->_node.optr == TSDB_BINARY_OP_DIVIDE ||
      pExpr->_node.optr == TSDB_BINARY_OP_REMAINDER) {
    int16_t leftType = pExpr->_node.pLeft->resultType;
    int16_t rightType = pExpr->_node.pRight->resultType;
    if (!IS_NUMERIC_TYPE(leftType) || !IS_NUMERIC_TYPE(rightType)) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }
    pExpr->resultType = TSDB_DATA_TYPE_DOUBLE;
    pExpr->resultBytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;
    return TSDB_CODE_SUCCESS;
  } else {
    return TSDB_CODE_SUCCESS;
  }
}

int32_t exprTreeValidateTree(char* msgbuf, tExprNode *pExpr) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (pExpr == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  if (pExpr->nodeType == TSQL_NODE_VALUE) {
    pExpr->resultType = pExpr->pVal->nType;
    if (!IS_VAR_DATA_TYPE(pExpr->pVal->nType)) {
      pExpr->resultBytes = tDataTypes[pExpr->pVal->nType].bytes;
    } else {
      pExpr->resultBytes = (int16_t)(pExpr->pVal->nLen + VARSTR_HEADER_SIZE);
    }
  } else if (pExpr->nodeType == TSQL_NODE_COL) {
    pExpr->resultType = pExpr->pSchema->type;
    if (pExpr->pSchema->colId != TSDB_TBNAME_COLUMN_INDEX) {
      pExpr->resultBytes = pExpr->pSchema->bytes;
    } else {
      pExpr->resultBytes = tGetTbnameColumnSchema()->bytes;
    }
  } else if (pExpr->nodeType == TSQL_NODE_EXPR) {
    code = exprTreeValidateTree(msgbuf, pExpr->_node.pLeft);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    code = exprTreeValidateTree(msgbuf, pExpr->_node.pRight);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    code = exprTreeValidateExprNode(pExpr);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  } else if (pExpr->nodeType == TSQL_NODE_FUNC) {
    for (int32_t i = 0; i < pExpr->_func.numChildren; ++i) {
      code = exprTreeValidateTree(msgbuf, pExpr->_func.pChildren[i]);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
    code = exprTreeValidateFunctionNode(msgbuf, pExpr);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  } else if (pExpr->nodeType == TSQL_NODE_TYPE) {
    pExpr->resultType = pExpr->pType->type;
    pExpr->resultBytes = pExpr->pType->bytes;
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

static void reverseCopy(char* dest, const char* src, int16_t type, int32_t numOfRows, int16_t colSize) {
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
    case TSDB_DATA_TYPE_BINARY: 
    case TSDB_DATA_TYPE_NCHAR:{
      for(int32_t i = 0; i < numOfRows; ++i) {
        memcpy(dest + i * colSize, src + (numOfRows - i - 1) * colSize, colSize);
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
    tfree(pNode->pVal);
  } else if (pNode->nodeType == TSQL_NODE_COL) {
    tfree(pNode->pSchema);
  } else if (pNode->nodeType == TSQL_NODE_FUNC) {
    doExprTreeDestroy(&pNode, fp);
  } else if (pNode->nodeType == TSQL_NODE_TYPE) {
    tfree(pNode->pType);
  }

  tfree(pNode);
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
    tfree((*pExpr)->pVal);
  } else if ((*pExpr)->nodeType == TSQL_NODE_COL) {
    free((*pExpr)->pSchema);
  } else if ((*pExpr)->nodeType == TSQL_NODE_FUNC) {
    for (int i = 0; i < (*pExpr)->_func.numChildren; ++i) {
      doExprTreeDestroy((*pExpr)->_func.pChildren + i, fp);
    }
    free((*pExpr)->_func.pChildren);
  } else if ((*pExpr)->nodeType == TSQL_NODE_TYPE) {
    tfree((*pExpr)->pType);
  }

  tfree(*pExpr);
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
      reverseCopy(pOutput, pInputData, pExpr->pSchema->type, numOfRows, pExpr->pSchema->bytes);
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
  int32_t numChildren = pExpr->_func.numChildren;
  if (numChildren == 0) {
    _expr_scalar_function_t scalarFn = getExprScalarFunction(pExpr->_func.functionId);
    output->type = pExpr->resultType;
    output->bytes = pExpr->resultBytes;
    output->numOfRows = numOfRows;
    scalarFn(pExpr->_func.functionId, NULL, 0, output, order);
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
        reverseCopy(pChildrenOutput[i], pInputData, pChild->pSchema->type, numOfRows, pChild->pSchema->bytes);
        pInputs[i].data = pChildrenOutput[i];
      } else {
        pInputs[i].data = pInputData;
      }
      pInputs[i].numOfRows = (int16_t)numOfRows;
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
  output->numOfRows = (int16_t)numOfRows;
  scalarFn(pExpr->_func.functionId, pInputs, numChildren, output, TSDB_ORDER_ASC);

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
  char *ltmp = NULL, *rtmp = NULL;
  char *leftIn = NULL, *rightIn = NULL;
  int32_t leftNum = 0, rightNum = 0;
  int32_t leftType = 0, rightType = 0;
  int32_t fnOrder = TSDB_ORDER_ASC;
  
  if (pLeft->nodeType == TSQL_NODE_EXPR || pLeft->nodeType == TSQL_NODE_FUNC) {
    ltmp = (char*)malloc(sizeof(int64_t) * numOfRows);
    tExprOperandInfo left;
    left.data = ltmp;
    exprTreeInternalNodeTraverse(pLeft, numOfRows, &left, param, order, getSourceDataBlock);
    
    leftIn = ltmp;
    leftType = left.type;
    leftNum = left.numOfRows;
  } else if (pLeft->nodeType == TSQL_NODE_COL) {
    char *pInputData = getSourceDataBlock(param, pLeft->pSchema->name, pLeft->pSchema->colId);
    if (order == TSDB_ORDER_DESC && (pRight->nodeType != TSQL_NODE_COL)) {      
      ltmp = malloc(sizeof(int64_t) * numOfRows);
      reverseCopy(ltmp, pInputData, pLeft->pSchema->type, numOfRows, pLeft->pSchema->bytes);
      leftIn = ltmp;
    } else {
      leftIn = pInputData;
      fnOrder = order;
    }

    leftType = pLeft->pSchema->type;
    leftNum = numOfRows;
  } else {
    assert(pLeft->nodeType == TSQL_NODE_VALUE);
    leftIn = (char *)&pLeft->pVal->i64;
    leftType = pLeft->pVal->nType;
    leftNum = 1;
  }

  if (pRight->nodeType == TSQL_NODE_EXPR || pRight->nodeType == TSQL_NODE_FUNC) {
    rtmp = (char*)malloc(sizeof(int64_t) * numOfRows);
    tExprOperandInfo right;
    right.data = rtmp;
    exprTreeInternalNodeTraverse(pRight, numOfRows, &right, param, order, getSourceDataBlock);
    
    rightIn = rtmp;
    rightType = right.type;
    rightNum = right.numOfRows;
  } else if (pRight->nodeType == TSQL_NODE_COL) {
    char *pInputData = getSourceDataBlock(param, pRight->pSchema->name, pRight->pSchema->colId);
    if (order == TSDB_ORDER_DESC && (pLeft->nodeType != TSQL_NODE_COL)) {      
      rtmp = malloc(sizeof(int64_t) * numOfRows);
      reverseCopy(rtmp, pInputData, pRight->pSchema->type, numOfRows, pRight->pSchema->bytes);
      rightIn = rtmp;
    } else {
      rightIn = pInputData;
      fnOrder = order;
    }

    rightType = pRight->pSchema->type;
    rightNum = numOfRows;    
  } else {
    assert(pRight->nodeType == TSQL_NODE_VALUE);
    rightIn = (char *)&pRight->pVal->i64;
    rightType = pRight->pVal->nType;
    rightNum = 1;
  }

  _arithmetic_operator_fn_t OperatorFn = getArithmeticOperatorFn(pExpr->_node.optr);
  OperatorFn(leftIn, leftNum, leftType, rightIn, rightNum, rightType, output->data, fnOrder);
  
  output->numOfRows = MAX(leftNum, rightNum);
  output->type = TSDB_DATA_TYPE_DOUBLE;
  output->bytes = tDataTypes[output->type].bytes;

  tfree(ltmp);
  tfree(rtmp);
}

static void exprTreeToBinaryImpl(SBufferWriter* bw, tExprNode* expr) {
  tbufWriteUint8(bw, expr->nodeType);
  
  if (expr->nodeType == TSQL_NODE_VALUE) {
    tVariant* pVal = expr->pVal;
    
    tbufWriteUint32(bw, pVal->nType);
    if (pVal->nType == TSDB_DATA_TYPE_BINARY || pVal->nType == TSDB_DATA_TYPE_NCHAR) {
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
    tbufWriteInt32(bw, expr->_func.numChildren);
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
    if (pVal->nType == TSDB_DATA_TYPE_BINARY || pVal->nType == TSDB_DATA_TYPE_NCHAR) {
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
    pExpr->_func.numChildren = tbufReadInt32(br);
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
  } else if (pNode->nodeType == TSQL_NODE_TYPE) {
    pCloned->pType = calloc(1, sizeof(TAOS_FIELD));
    *pCloned->pType = *pNode->pType;
  } 
  
  pCloned->nodeType = pNode->nodeType;
  pCloned->resultType = pNode->resultType;
  pCloned->resultBytes = pNode->resultBytes;
  return pCloned;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// scalar functions
int32_t exprValidateStringConcatNode(tExprNode *pExpr) {
  if (pExpr->_func.numChildren < 2 || pExpr->_func.numChildren > 8) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  int16_t prevResultType = TSDB_DATA_TYPE_NULL;
  int16_t resultType = TSDB_DATA_TYPE_NULL;
  bool resultTypeDeduced = false;
  for (int32_t i = 0; i < pExpr->_func.numChildren; ++i) {
    tExprNode *child = pExpr->_func.pChildren[i];
    if (child->nodeType != TSQL_NODE_VALUE) {
      resultType = child->resultType;
      if (!IS_VAR_DATA_TYPE(resultType)) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
      if (!resultTypeDeduced) {
        resultTypeDeduced = true;
      } else {
        if (resultType != prevResultType) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
        }
      }
      prevResultType = child->resultType;
    } else {
      if (!IS_VAR_DATA_TYPE(child->resultType)) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
    }
  }

  if (resultTypeDeduced) {
    for (int32_t i = 0; i < pExpr->_func.numChildren; ++i) {
      tExprNode *child = pExpr->_func.pChildren[i];
      if (child->nodeType == TSQL_NODE_VALUE) {
        if (!IS_VAR_DATA_TYPE(child->pVal->nType)) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
        }
        char* payload = malloc(child->pVal->nLen * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);
        tVariantDump(child->pVal, payload, resultType, true);
        int16_t resultBytes = varDataTLen(payload);
        free(payload);
        child->resultType = resultType;
        child->resultBytes = (int16_t)(resultBytes);
      }
    }
  } else {
    for (int32_t i = 0; i < pExpr->_func.numChildren; ++i) {
      tExprNode *child = pExpr->_func.pChildren[i];
      assert(child->nodeType == TSQL_NODE_VALUE) ;
      resultType = child->resultType;
      for (int j = i+1; j < pExpr->_func.numChildren; ++j) {
        if (pExpr->_func.pChildren[j]->resultType != resultType) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
        }
      }
    }
  }

  pExpr->resultType = resultType;
  int16_t resultBytes = 0;
  for (int32_t i = 0; i < pExpr->_func.numChildren; ++i) {
    tExprNode *child = pExpr->_func.pChildren[i];
    if (resultBytes <= resultBytes + child->resultBytes - VARSTR_HEADER_SIZE) {
      resultBytes += child->resultBytes - VARSTR_HEADER_SIZE;
    } else {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }
  }
  pExpr->resultBytes = resultBytes + VARSTR_HEADER_SIZE;
  return TSDB_CODE_SUCCESS;
}

int32_t exprValidateStringConcatWsNode(tExprNode *pExpr) {
  if (pExpr->_func.numChildren < 3 || pExpr->_func.numChildren > 9) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  int16_t prevResultType = TSDB_DATA_TYPE_NULL;
  int16_t resultType = TSDB_DATA_TYPE_NULL;
  bool resultTypeDeduced = false;
  for (int32_t i = 0; i < pExpr->_func.numChildren; ++i) {
    tExprNode *child = pExpr->_func.pChildren[i];
    if (child->nodeType != TSQL_NODE_VALUE) {
      resultType = child->resultType;
      if (!IS_VAR_DATA_TYPE(resultType)) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
      if (!resultTypeDeduced) {
        resultTypeDeduced = true;
      } else {
        if (resultType != prevResultType) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
        }
      }
      prevResultType = child->resultType;
    } else {
      if (!IS_VAR_DATA_TYPE(child->resultType)) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
    }
  }

  if (resultTypeDeduced) {
    for (int32_t i = 0; i < pExpr->_func.numChildren; ++i) {
      tExprNode *child = pExpr->_func.pChildren[i];
      if (child->nodeType == TSQL_NODE_VALUE) {
        if (!IS_VAR_DATA_TYPE(child->pVal->nType)) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
        }
        char* payload = malloc(child->pVal->nLen * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);
        tVariantDump(child->pVal, payload, resultType, true);
        int16_t resultBytes = varDataTLen(payload);
        free(payload);
        child->resultType = resultType;
        child->resultBytes = (int16_t)(resultBytes);
      }
    }
  } else {
    for (int32_t i = 0; i < pExpr->_func.numChildren; ++i) {
      tExprNode *child = pExpr->_func.pChildren[i];
      assert(child->nodeType == TSQL_NODE_VALUE) ;
      resultType = child->resultType;
      for (int j = i+1; j < pExpr->_func.numChildren; ++j) {
        if (pExpr->_func.pChildren[j]->resultType != resultType) {
          return TSDB_CODE_TSC_INVALID_OPERATION;
        }
      }
    }
  }

  pExpr->resultType = resultType;
  int16_t resultBytes = 0;
  for (int32_t i = 1; i < pExpr->_func.numChildren; ++i) {
    tExprNode *child = pExpr->_func.pChildren[i];
    if (resultBytes <= resultBytes + child->resultBytes - VARSTR_HEADER_SIZE) {
      resultBytes += child->resultBytes - VARSTR_HEADER_SIZE;
    } else {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }
  }
  tExprNode* wsNode = pExpr->_func.pChildren[0];
  int16_t wsResultBytes = wsNode->resultBytes - VARSTR_HEADER_SIZE;
  resultBytes += wsResultBytes * (pExpr->_func.numChildren - 2);
  pExpr->resultBytes = resultBytes + VARSTR_HEADER_SIZE;
  return TSDB_CODE_SUCCESS;
}


int32_t exprValidateStringLengthNode(tExprNode *pExpr) {
  if (pExpr->_func.numChildren != 1) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  tExprNode* child1 = pExpr->_func.pChildren[0];

  if (child1->nodeType == TSQL_NODE_VALUE) {
    child1->resultType = (int16_t)child1->pVal->nType;
    child1->resultBytes = (int16_t)(child1->pVal->nLen + VARSTR_HEADER_SIZE);
  }

  if (!IS_VAR_DATA_TYPE(child1->resultType)) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  pExpr->resultType = TSDB_DATA_TYPE_INT;
  pExpr->resultBytes = tDataTypes[TSDB_DATA_TYPE_INT].bytes;

  return TSDB_CODE_SUCCESS;
}

int32_t exprValidateCastNode(char* msgbuf, tExprNode *pExpr) {
  const char* msg1 = "invalid param num for cast function";
  const char* msg2 = "the second param should be a valid type name for cast function";
  const char* msg3 = "target type is not supported for cast function";
  const char* msg4 = "not supported type convertion for cast function";
  
  if (pExpr->_func.numChildren != 2) {
    return exprInvalidOperationMsg(msgbuf, msg1);
  }

  tExprNode* child0 = pExpr->_func.pChildren[0];
  tExprNode* child1 = pExpr->_func.pChildren[1];

  if (child1->nodeType != TSQL_NODE_TYPE) {
    return exprInvalidOperationMsg(msgbuf, msg2);
  }
  
  if (child1->resultType != TSDB_DATA_TYPE_BIGINT && child1->resultType != TSDB_DATA_TYPE_UBIGINT 
   && child1->resultType != TSDB_DATA_TYPE_TIMESTAMP && child1->resultType != TSDB_DATA_TYPE_BINARY
   && child1->resultType != TSDB_DATA_TYPE_NCHAR) {
    return exprInvalidOperationMsg(msgbuf, msg3);
  }

  if ((child0->resultType == TSDB_DATA_TYPE_BINARY && child1->resultType == TSDB_DATA_TYPE_TIMESTAMP)
    || (child0->resultType == TSDB_DATA_TYPE_TIMESTAMP && (child1->resultType == TSDB_DATA_TYPE_BINARY || child1->resultType == TSDB_DATA_TYPE_NCHAR))
    || (child0->resultType == TSDB_DATA_TYPE_NCHAR && (child1->resultType == TSDB_DATA_TYPE_BINARY || child1->resultType == TSDB_DATA_TYPE_TIMESTAMP))) {
    return exprInvalidOperationMsg(msgbuf, msg4);
  }
  
  pExpr->resultType = child1->resultType;
  pExpr->resultBytes = child1->resultBytes;

  doExprTreeDestroy(&pExpr->_func.pChildren[1], NULL);
  pExpr->_func.numChildren = 1;

  return TSDB_CODE_SUCCESS;
}


int32_t exprValidateMathNode(tExprNode *pExpr) {
  switch (pExpr->_func.functionId) {
    case TSDB_FUNC_SCALAR_POW:
    case TSDB_FUNC_SCALAR_LOG: {
      if (pExpr->_func.numChildren != 2) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
      tExprNode *child1 = pExpr->_func.pChildren[0];
      tExprNode *child2 = pExpr->_func.pChildren[1];
      if (!IS_NUMERIC_TYPE(child1->resultType) || !IS_NUMERIC_TYPE(child2->resultType)) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      pExpr->resultType = TSDB_DATA_TYPE_DOUBLE;
      pExpr->resultBytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;

      return TSDB_CODE_SUCCESS;
    }
    case TSDB_FUNC_SCALAR_ABS: {
      if (pExpr->_func.numChildren != 1) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
      tExprNode *child1 = pExpr->_func.pChildren[0];
      if (!IS_NUMERIC_TYPE(child1->resultType)) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
      if (IS_SIGNED_NUMERIC_TYPE(child1->resultType) || IS_UNSIGNED_NUMERIC_TYPE(child1->resultType)) {
        pExpr->resultType = TSDB_DATA_TYPE_UBIGINT;
        pExpr->resultBytes = tDataTypes[TSDB_DATA_TYPE_UBIGINT].bytes;
      } else if (IS_FLOAT_TYPE(child1->resultType)) {
        pExpr->resultType = TSDB_DATA_TYPE_DOUBLE;
        pExpr->resultBytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;
      }
      break;
    }
    case TSDB_FUNC_SCALAR_SQRT:
    case TSDB_FUNC_SCALAR_ASIN:
    case TSDB_FUNC_SCALAR_ACOS:
    case TSDB_FUNC_SCALAR_ATAN:
    case TSDB_FUNC_SCALAR_SIN:
    case TSDB_FUNC_SCALAR_COS:
    case TSDB_FUNC_SCALAR_TAN: {
      if (pExpr->_func.numChildren != 1) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
      tExprNode *child1 = pExpr->_func.pChildren[0];
      if (!IS_NUMERIC_TYPE(child1->resultType)) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
      pExpr->resultType = TSDB_DATA_TYPE_DOUBLE;
      pExpr->resultBytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;
      break;
    }

    case TSDB_FUNC_SCALAR_CEIL:
    case TSDB_FUNC_SCALAR_FLOOR:
    case TSDB_FUNC_SCALAR_ROUND: {
      if (pExpr->_func.numChildren != 1) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
      tExprNode* child = pExpr->_func.pChildren[0];
      if (!IS_NUMERIC_TYPE(child->resultType)) {
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }
      pExpr->resultType = child->resultType;
      pExpr->resultBytes = child->resultBytes;
      break;
    }
    default: {
      assert(false);
      break;
    }
  }
  return TSDB_CODE_SUCCESS;
}

void vectorConcat(int16_t functionId, tExprOperandInfo* pInputs, int32_t numInputs, tExprOperandInfo* pOutput, int32_t order) {
  assert(functionId == TSDB_FUNC_SCALAR_CONCAT && numInputs >=2 && order == TSDB_ORDER_ASC);
  for (int i = 0; i < numInputs; ++i) {
    assert(pInputs[i].numOfRows == 1 || pInputs[i].numOfRows == pOutput->numOfRows);
  }

  char* outputData = NULL;
  char** inputData = calloc(numInputs, sizeof(char*));
  for (int i = 0; i < pOutput->numOfRows; ++i) {
    for (int j = 0; j < numInputs; ++j) {
      if (pInputs[j].numOfRows == 1) {
        inputData[j] = pInputs[j].data;
      } else {
        inputData[j] = pInputs[j].data + i * pInputs[j].bytes;
      }
    }

    outputData = pOutput->data + i * pOutput->bytes;

    bool hasNullInputs = false;
    for (int j = 0; j < numInputs; ++j) {
      if (isNull(inputData[j], pInputs[j].type)) {
        hasNullInputs = true;
        setNull(outputData, pOutput->type, pOutput->bytes);
      }
    }

    if (!hasNullInputs) {
      int16_t dataLen = 0;
      for (int j = 0; j < numInputs; ++j) {
        memcpy(((char*)varDataVal(outputData))+dataLen, varDataVal(inputData[j]), varDataLen(inputData[j]));
        dataLen += varDataLen(inputData[j]);
      }
      varDataSetLen(outputData, dataLen);
    }
  }

  free(inputData);
}

void vectorConcatWs(int16_t functionId, tExprOperandInfo* pInputs, int32_t numInputs, tExprOperandInfo* pOutput, int32_t order) {
  assert(functionId == TSDB_FUNC_SCALAR_CONCAT_WS && numInputs >=3 && order == TSDB_ORDER_ASC);
  for (int i = 0; i < numInputs; ++i) {
    assert(pInputs[i].numOfRows == 1 || pInputs[i].numOfRows == pOutput->numOfRows);
  }

  char* outputData = NULL;
  char** inputData = calloc(numInputs, sizeof(char*));
  for (int i = 0; i < pOutput->numOfRows; ++i) {
    for (int j = 0; j < numInputs; ++j) {
      if (pInputs[j].numOfRows == 1) {
        inputData[j] = pInputs[j].data;
      } else {
        inputData[j] = pInputs[j].data + i * pInputs[j].bytes;
      }
    }

    outputData = pOutput->data + i * pOutput->bytes;

    if (isNull(inputData[0], pInputs[0].type)) {
      setNull(outputData, pOutput->type, pOutput->bytes);
      continue;
    }

    int16_t dataLen = 0;
    for (int j = 1; j < numInputs; ++j) {
      if (isNull(inputData[j], pInputs[j].type)) {
        continue;
      }
      memcpy(((char*)varDataVal(outputData))+dataLen, varDataVal(inputData[j]), varDataLen(inputData[j]));
      dataLen += varDataLen(inputData[j]);
      if (j < numInputs - 1) {
        memcpy(((char*)varDataVal(outputData))+dataLen, varDataVal(inputData[0]), varDataLen(inputData[0]));
        dataLen += varDataLen(inputData[0]);
      }
    }
    varDataSetLen(outputData, dataLen);
  }


  free(inputData);
}

void vectorLength(int16_t functionId, tExprOperandInfo *pInputs, int32_t numInputs, tExprOperandInfo* pOutput, int32_t order) {
  assert(functionId == TSDB_FUNC_SCALAR_LENGTH && numInputs == 1 && order == TSDB_ORDER_ASC);
  assert(IS_VAR_DATA_TYPE(pInputs[0].type));

  char* data0 = NULL;
  char* outputData = NULL;
  for (int32_t i = 0; i < pOutput->numOfRows; ++i) {
    if (pInputs[0].numOfRows == 1) {
      data0 = pInputs[0].data;
    } else {
      data0 = pInputs[0].data + i * pInputs[0].bytes;
    }

    outputData = pOutput->data + i * pOutput->bytes;
    if (isNull(data0, pInputs[0].type)) {
      setNull(outputData, pOutput->type, pOutput->bytes);
    } else {
      int16_t result = varDataLen(data0);
      SET_TYPED_DATA(outputData, pOutput->type, result);
    }
  }
}

void castConvert(int16_t inputType, int16_t inputBytes, char *input, int16_t OutputType, int16_t outputBytes, char *output) {
  switch (OutputType) {
    case TSDB_DATA_TYPE_BIGINT:
      if (inputType == TSDB_DATA_TYPE_BINARY) {
        char *tmp = malloc(varDataLen(input) + 1);
        memcpy(tmp, varDataVal(input), varDataLen(input));
        tmp[varDataLen(input)] = 0;
        *(int64_t *)output = strtoll(tmp, NULL, 10);
        free(tmp);
      } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
        char *newColData = calloc(1, outputBytes * TSDB_NCHAR_SIZE + 1);
        int len = taosUcs4ToMbs(varDataVal(input), varDataLen(input), newColData);
        newColData[len] = 0;
        *(int64_t *)output = strtoll(newColData, NULL, 10);
        tfree(newColData);
      } else {
        GET_TYPED_DATA(*(int64_t *)output, int64_t, inputType, input);
      }
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      if (inputType == TSDB_DATA_TYPE_BINARY) {
        char *tmp = malloc(varDataLen(input) + 1);
        memcpy(tmp, varDataVal(input), varDataLen(input));
        tmp[varDataLen(input)] = 0;
        *(uint64_t *)output = strtoull(tmp, NULL, 10);
        free(tmp);
      } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
        char *newColData = calloc(1, outputBytes * TSDB_NCHAR_SIZE + 1);
        int len = taosUcs4ToMbs(varDataVal(input), varDataLen(input), newColData);
        newColData[len] = 0;
        *(int64_t *)output = strtoull(newColData, NULL, 10);
        tfree(newColData);
      } else {
        GET_TYPED_DATA(*(uint64_t *)output, uint64_t, inputType, input);
      }
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      if (inputType == TSDB_DATA_TYPE_BINARY || inputType == TSDB_DATA_TYPE_NCHAR) {
        assert(0);
      } else {
        GET_TYPED_DATA(*(int64_t *)output, int64_t, inputType, input);
      }
      break;
    case TSDB_DATA_TYPE_BINARY:
      if (inputType == TSDB_DATA_TYPE_BOOL) {
        int32_t len = sprintf(varDataVal(output), "%.*s", (int32_t)(outputBytes - VARSTR_HEADER_SIZE), *(int8_t*)input ? "true" : "false");
        varDataSetLen(output, len);
      } else if (inputType == TSDB_DATA_TYPE_BINARY) {
        char *tmp = malloc(varDataLen(input) + 1);
        memcpy(tmp, varDataVal(input), varDataLen(input));
        tmp[varDataLen(input)] = 0;      
        int32_t len = sprintf(varDataVal(output), "%.*s", (int32_t)(outputBytes - VARSTR_HEADER_SIZE), tmp);
        varDataSetLen(output, len);
        free(tmp);
      } else if (inputType == TSDB_DATA_TYPE_TIMESTAMP || inputType == TSDB_DATA_TYPE_NCHAR) {
        assert(0);
      } else {
        char tmp[400] = {0};
        NUM_TO_STRING(inputType, input, sizeof(tmp), tmp);
        int32_t len = (int32_t)strlen(tmp);
        len = (outputBytes - VARSTR_HEADER_SIZE) > len ? len : (outputBytes - VARSTR_HEADER_SIZE);
        memcpy(varDataVal(output), tmp, len);
        varDataSetLen(output, len);
      }
      break;
    case TSDB_DATA_TYPE_NCHAR: {
        int32_t ncharSize = (outputBytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
        if (inputType == TSDB_DATA_TYPE_BOOL) {
          char tmp[8] = {0};
          int32_t len = sprintf(tmp, "%.*s", ncharSize, *(int8_t*)input ? "true" : "false");
          taosMbsToUcs4(tmp, len, varDataVal(output), outputBytes - VARSTR_HEADER_SIZE, &len);
          varDataSetLen(output, len);
        } else if (inputType == TSDB_DATA_TYPE_BINARY) {
          int32_t len = ncharSize > varDataLen(input) ? varDataLen(input) : ncharSize;
          taosMbsToUcs4(input + VARSTR_HEADER_SIZE, len, varDataVal(output), outputBytes - VARSTR_HEADER_SIZE, &len);
          varDataSetLen(output, len);
        } else if (inputType == TSDB_DATA_TYPE_TIMESTAMP) {
          assert(0);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = (inputBytes > outputBytes) ? outputBytes : inputBytes;
          memcpy(output, input, len);
          varDataSetLen(output, len - VARSTR_HEADER_SIZE);
        } else {
          char tmp[400] = {0};
          NUM_TO_STRING(inputType, input, sizeof(tmp), tmp);
          int32_t len = (int32_t)(ncharSize > strlen(tmp) ? strlen(tmp) : ncharSize);
          taosMbsToUcs4(tmp, len, varDataVal(output), outputBytes - VARSTR_HEADER_SIZE, &len);
          varDataSetLen(output, len);
        }
        break;
      }
    default:
      assert(0);
      break;
  }
}

void vectorCharLength(int16_t functionId, tExprOperandInfo *pInputs, int32_t numInputs, tExprOperandInfo* pOutput, int32_t order) {
  assert(functionId == TSDB_FUNC_SCALAR_CHAR_LENGTH && numInputs == 1 && order == TSDB_ORDER_ASC);
  assert(IS_VAR_DATA_TYPE(pInputs[0].type));

  char* data0 = NULL;
  char* outputData = NULL;
  for (int32_t i = 0; i < pOutput->numOfRows; ++i) {
    if (pInputs[0].numOfRows == 1) {
      data0 = pInputs[0].data;
    } else {
      data0 = pInputs[0].data + i * pInputs[0].bytes;
    }

    outputData = pOutput->data + i * pOutput->bytes;
    if (isNull(data0, pInputs[0].type)) {
      setNull(outputData, pOutput->type, pOutput->bytes);
    } else {
      int16_t result = varDataLen(data0);
      if (pInputs[0].type == TSDB_DATA_TYPE_BINARY) {
        SET_TYPED_DATA(outputData, pOutput->type, result);
      } else if (pInputs[0].type == TSDB_DATA_TYPE_NCHAR) {
        SET_TYPED_DATA(outputData, pOutput->type, result/TSDB_NCHAR_SIZE);
      }
    }
  }
}

void vectorMathFunc(int16_t functionId, tExprOperandInfo *pInputs, int32_t numInputs, tExprOperandInfo* pOutput, int32_t order)  {
  for (int i = 0; i < numInputs; ++i) {
    assert(pInputs[i].numOfRows == 1 || pInputs[i].numOfRows == pOutput->numOfRows);
  }

  char* outputData = NULL;
  char** inputData = calloc(numInputs, sizeof(char*));
  for (int i = 0; i < pOutput->numOfRows; ++i) {
    for (int j = 0; j < numInputs; ++j) {
      if (pInputs[j].numOfRows == 1) {
        inputData[j] = pInputs[j].data;
      } else {
        inputData[j] = pInputs[j].data + i * pInputs[j].bytes;
      }
    }

    outputData = pOutput->data + i * pOutput->bytes;

    bool hasNullInputs = false;
    for (int j = 0; j < numInputs; ++j) {
      if (isNull(inputData[j], pInputs[j].type)) {
        hasNullInputs = true;
        setNull(outputData, pOutput->type, pOutput->bytes);
      }
    }

    if (!hasNullInputs) {
      switch (functionId) {
        case TSDB_FUNC_SCALAR_LOG: {
          assert(numInputs == 2);
          double base = 0;
          GET_TYPED_DATA(base, double, pInputs[1].type, inputData[1]);
          double v1 = 0;
          GET_TYPED_DATA(v1, double, pInputs[0].type, inputData[0]);
          double result = log(v1) / log(base);
          SET_TYPED_DATA(outputData, pOutput->type, result);
          break;
        }

        case TSDB_FUNC_SCALAR_POW:{
          assert(numInputs == 2);
          double base = 0;
          GET_TYPED_DATA(base, double, pInputs[1].type, inputData[1]);
          double v1 = 0;
          GET_TYPED_DATA(v1, double, pInputs[0].type, inputData[0]);
          double result = pow(v1, base);
          SET_TYPED_DATA(outputData, pOutput->type, result);
          break;
        }

        case TSDB_FUNC_SCALAR_ABS: {
          assert(numInputs == 1);
          assert(IS_NUMERIC_TYPE(pInputs[0].type));
          if (IS_SIGNED_NUMERIC_TYPE(pInputs[0].type)) {
            int64_t v1 = 0;
            GET_TYPED_DATA(v1, int64_t, pInputs[0].type, inputData[0]);
            uint64_t result = (uint64_t)(llabs(v1));
            SET_TYPED_DATA(outputData, pOutput->type, result);
          } else if (IS_UNSIGNED_NUMERIC_TYPE(pInputs[0].type)) {
            uint64_t v1 = 0;
            GET_TYPED_DATA(v1, uint64_t, pInputs[0].type, inputData[0]);
            SET_TYPED_DATA(outputData, pOutput->type, v1);
          } else if (IS_FLOAT_TYPE(pInputs[0].type)) {
            double v1 = 0;
            GET_TYPED_DATA(v1, double, pInputs[0].type, inputData[0]);
            double result = fabs(v1);
            SET_TYPED_DATA(outputData, pOutput->type, result);
          }
          break;
        }
        case TSDB_FUNC_SCALAR_SQRT: {
          assert(numInputs == 1);
          assert(IS_NUMERIC_TYPE(pInputs[0].type));

          double v1 = 0;
          GET_TYPED_DATA(v1, double, pInputs[0].type, inputData[0]);
          double result = sqrt(v1);
          SET_TYPED_DATA(outputData, pOutput->type, result);

          break;
        }
        case TSDB_FUNC_SCALAR_ASIN: {
          assert(numInputs == 1);
          assert(IS_NUMERIC_TYPE(pInputs[0].type));

          double v1 = 0;
          GET_TYPED_DATA(v1, double, pInputs[0].type, inputData[0]);
          double result = asin(v1);
          SET_TYPED_DATA(outputData, pOutput->type, result);
          break;
        }
        case TSDB_FUNC_SCALAR_ACOS: {
          assert(numInputs == 1);
          assert(IS_NUMERIC_TYPE(pInputs[0].type));

          double v1 = 0;
          GET_TYPED_DATA(v1, double, pInputs[0].type, inputData[0]);
          double result = acos(v1);
          SET_TYPED_DATA(outputData, pOutput->type, result);
          break;
        }
        case TSDB_FUNC_SCALAR_ATAN: {
          assert(numInputs == 1);
          assert(IS_NUMERIC_TYPE(pInputs[0].type));

          double v1 = 0;
          GET_TYPED_DATA(v1, double, pInputs[0].type, inputData[0]);
          double result = atan(v1);
          SET_TYPED_DATA(outputData, pOutput->type, result);
          break;
        }
        case TSDB_FUNC_SCALAR_SIN: {
          assert(numInputs == 1);
          assert(IS_NUMERIC_TYPE(pInputs[0].type));

          double v1 = 0;
          GET_TYPED_DATA(v1, double, pInputs[0].type, inputData[0]);
          double result = sin(v1);
          SET_TYPED_DATA(outputData, pOutput->type, result);
          break;
        }
        case TSDB_FUNC_SCALAR_COS: {
          assert(numInputs == 1);
          assert(IS_NUMERIC_TYPE(pInputs[0].type));

          double v1 = 0;
          GET_TYPED_DATA(v1, double, pInputs[0].type, inputData[0]);
          double result = cos(v1);
          SET_TYPED_DATA(outputData, pOutput->type, result);
          break;
        }
        case TSDB_FUNC_SCALAR_TAN:{
          assert(numInputs == 1);
          assert(IS_NUMERIC_TYPE(pInputs[0].type));

          double v1 = 0;
          GET_TYPED_DATA(v1, double, pInputs[0].type, inputData[0]);
          double result = tan(v1);
          SET_TYPED_DATA(outputData, pOutput->type, result);
          break;
        }

        case TSDB_FUNC_SCALAR_CEIL: {
          assert(numInputs == 1);
          assert(IS_NUMERIC_TYPE(pInputs[0].type));
          if (IS_UNSIGNED_NUMERIC_TYPE(pInputs[0].type) || IS_SIGNED_NUMERIC_TYPE(pInputs[0].type)) {
            memcpy(outputData, inputData[0], pInputs[0].bytes);
          } else {
            if (pInputs[0].type == TSDB_DATA_TYPE_FLOAT) {
              float v = 0;
              GET_TYPED_DATA(v, float, pInputs[0].type, inputData[0]);
              float result = ceilf(v);
              SET_TYPED_DATA(outputData, pOutput->type, result);
            } else if (pInputs[0].type == TSDB_DATA_TYPE_DOUBLE) {
              double v = 0;
              GET_TYPED_DATA(v, double, pInputs[0].type, inputData[0]);
              double result = ceil(v);
              SET_TYPED_DATA(outputData, pOutput->type, result);
            }
          }
          break;
        }
        case TSDB_FUNC_SCALAR_FLOOR: {
          assert(numInputs == 1);
          assert(IS_NUMERIC_TYPE(pInputs[0].type));
          if (IS_UNSIGNED_NUMERIC_TYPE(pInputs[0].type) || IS_SIGNED_NUMERIC_TYPE(pInputs[0].type)) {
            memcpy(outputData, inputData[0], pInputs[0].bytes);
          } else {
            if (pInputs[0].type == TSDB_DATA_TYPE_FLOAT) {
              float v = 0;
              GET_TYPED_DATA(v, float, pInputs[0].type, inputData[0]);
              float result = floorf(v);
              SET_TYPED_DATA(outputData, pOutput->type, result);
            } else if (pInputs[0].type == TSDB_DATA_TYPE_DOUBLE) {
              double v = 0;
              GET_TYPED_DATA(v, double, pInputs[0].type, inputData[0]);
              double result = floor(v);
              SET_TYPED_DATA(outputData, pOutput->type, result);
            }
          }
          break;
        }

        case TSDB_FUNC_SCALAR_ROUND: {
          assert(numInputs == 1);
          assert(IS_NUMERIC_TYPE(pInputs[0].type));
          if (IS_UNSIGNED_NUMERIC_TYPE(pInputs[0].type) || IS_SIGNED_NUMERIC_TYPE(pInputs[0].type)) {
            memcpy(outputData, inputData[0], pInputs[0].bytes);
          } else {
            if (pInputs[0].type == TSDB_DATA_TYPE_FLOAT) {
              float v = 0;
              GET_TYPED_DATA(v, float, pInputs[0].type, inputData[0]);
              float result = roundf(v);
              SET_TYPED_DATA(outputData, pOutput->type, result);
            } else if (pInputs[0].type == TSDB_DATA_TYPE_DOUBLE) {
              double v = 0;
              GET_TYPED_DATA(v, double, pInputs[0].type, inputData[0]);
              double result = round(v);
              SET_TYPED_DATA(outputData, pOutput->type, result);
            }
          }
          break;
        }
        case TSDB_FUNC_SCALAR_CAST: {
          castConvert(pInputs[0].type, pInputs[0].bytes, inputData[0], pOutput->type, pOutput->bytes, outputData);
          break;
        }
        default: {
          assert(false);
          break;
        }
      } // end switch function(id)
    } // end can produce value, all child has value
  } // end for each row
  free(inputData);
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
        vectorMathFunc
    },
    {
        TSDB_FUNC_SCALAR_LOG,
        "log",
        vectorMathFunc
    },
    {
        TSDB_FUNC_SCALAR_ABS,
        "abs",
        vectorMathFunc,
    },
    {
        TSDB_FUNC_SCALAR_ACOS,
        "acos",
        vectorMathFunc,
    },
    {
        TSDB_FUNC_SCALAR_ASIN,
        "asin",
        vectorMathFunc,
    },
    {
        TSDB_FUNC_SCALAR_ATAN,
        "atan",
        vectorMathFunc,
    },
    {
        TSDB_FUNC_SCALAR_COS,
        "cos",
        vectorMathFunc,
    },
    {
        TSDB_FUNC_SCALAR_SIN,
        "sin",
        vectorMathFunc,
    },
    {
        TSDB_FUNC_SCALAR_TAN,
        "tan",
        vectorMathFunc,
    },
    {
        TSDB_FUNC_SCALAR_SQRT,
        "sqrt",
        vectorMathFunc,
    },
    {
        TSDB_FUNC_SCALAR_CEIL,
        "ceil",
        vectorMathFunc,
    },
    {
        TSDB_FUNC_SCALAR_FLOOR,
        "floor",
        vectorMathFunc,
    },
    {
        TSDB_FUNC_SCALAR_ROUND,
        "round",
        vectorMathFunc
    },
    {
        TSDB_FUNC_SCALAR_CONCAT,
        "concat",
        vectorConcat
    },
    {
        TSDB_FUNC_SCALAR_LENGTH,
        "length",
        vectorLength
    },
    {
        TSDB_FUNC_SCALAR_CONCAT_WS,
        "concat_ws",
        vectorConcatWs
    },
    {
        TSDB_FUNC_SCALAR_CHAR_LENGTH,
        "char_length",
        vectorCharLength
    },
    {
        TSDB_FUNC_SCALAR_CAST,
        "cast",
        vectorMathFunc
    },
};
