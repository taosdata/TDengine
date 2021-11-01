#include "tscalarfunction.h"
#include "tbinoperator.h"
#include "tunaryoperator.h"

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

static void setScalarFuncParam(SScalarFuncParam* param, int32_t type, int32_t bytes, void* pInput, int32_t numOfRows) {
  param->bytes = bytes;
  param->type = type;
  param->num  = numOfRows;
  param->data = pInput;
}

int32_t evaluateExprNodeTree(tExprNode* pExprs, int32_t numOfRows, SScalarFuncParam* pOutput, void* param,
                          char* (*getSourceDataBlock)(void*, const char*, int32_t)) {
  if (pExprs == NULL) {
    return 0;
  }

  tExprNode* pLeft = pExprs->_node.pLeft;
  tExprNode* pRight = pExprs->_node.pRight;

  /* the left output has result from the left child syntax tree */
  SScalarFuncParam leftOutput = {0};
  SScalarFuncParam rightOutput = {0};

  leftOutput.data = malloc(sizeof(int64_t) * numOfRows);

  if (pLeft->nodeType == TEXPR_BINARYEXPR_NODE || pLeft->nodeType == TEXPR_UNARYEXPR_NODE) {
    evaluateExprNodeTree(pLeft, numOfRows, &leftOutput, param, getSourceDataBlock);
  }

  // the right output has result from the right child syntax tree
  rightOutput.data = malloc(sizeof(int64_t) * numOfRows);

  if (pRight->nodeType == TEXPR_BINARYEXPR_NODE) {
    evaluateExprNodeTree(pRight, numOfRows, &rightOutput, param, getSourceDataBlock);
  }

  if (pExprs->nodeType == TEXPR_BINARYEXPR_NODE) {
    _bin_scalar_fn_t OperatorFn = getBinScalarOperatorFn(pExprs->_node.optr);

    SScalarFuncParam left = {0}, right = {0};

    if (pLeft->nodeType == TEXPR_BINARYEXPR_NODE || pLeft->nodeType == TEXPR_UNARYEXPR_NODE) {
      setScalarFuncParam(&left, leftOutput.type, leftOutput.bytes, leftOutput.data, leftOutput.num);
    } else if (pLeft->nodeType == TEXPR_COL_NODE) {
      SSchema* pschema = pLeft->pSchema;
      char*    pLeftInputData = getSourceDataBlock(param, pschema->name, pschema->colId);
      setScalarFuncParam(&right, pschema->type, pschema->bytes, pLeftInputData, numOfRows);
    } else if (pLeft->nodeType == TEXPR_VALUE_NODE) {
      SVariant* pVar = pRight->pVal;
      setScalarFuncParam(&left, pVar->nType, pVar->nLen, &pVar->i, 1);
    } else {
      assert(0);
    }

    if (pRight->nodeType == TEXPR_BINARYEXPR_NODE || pRight->nodeType == TEXPR_UNARYEXPR_NODE) {
      setScalarFuncParam(&right, rightOutput.type, rightOutput.bytes, rightOutput.data, rightOutput.num);
    } else if (pRight->nodeType == TEXPR_COL_NODE) {  // exprLeft + columnRight
      SSchema* pschema = pRight->pSchema;
      char*    pInputData = getSourceDataBlock(param, pschema->name, pschema->colId);
      setScalarFuncParam(&right, pschema->type, pschema->bytes, pInputData, numOfRows);
    } else if (pRight->nodeType == TEXPR_VALUE_NODE) {  // exprLeft + 12
      SVariant* pVar = pRight->pVal;
      setScalarFuncParam(&right, pVar->nType, pVar->nLen, &pVar->i, 1);
    } else {
      assert(0);
    }

    OperatorFn(&left, &right, pOutput->data, TSDB_ORDER_ASC);

    // Set the result info
    setScalarFuncParam(pOutput, TSDB_DATA_TYPE_DOUBLE, sizeof(double), pOutput->data, numOfRows);
  } else if (pExprs->nodeType == TEXPR_UNARYEXPR_NODE) {
    _unary_scalar_fn_t OperatorFn = getUnaryScalarOperatorFn(pExprs->_node.optr);
    SScalarFuncParam   left = {0};

    if (pLeft->nodeType == TEXPR_BINARYEXPR_NODE || pLeft->nodeType == TEXPR_UNARYEXPR_NODE) {
      setScalarFuncParam(&left, leftOutput.type, leftOutput.bytes, leftOutput.data, leftOutput.num);
    } else if (pLeft->nodeType == TEXPR_COL_NODE) {
      SSchema* pschema = pLeft->pSchema;
      char*    pLeftInputData = getSourceDataBlock(param, pschema->name, pschema->colId);
      setScalarFuncParam(&left, pschema->type, pschema->bytes, pLeftInputData, numOfRows);
    } else if (pLeft->nodeType == TEXPR_VALUE_NODE) {
      SVariant* pVar = pLeft->pVal;
      setScalarFuncParam(&left, pVar->nType, pVar->nLen, &pVar->i, 1);
    } else {
      assert(0);
    }

    OperatorFn(&left, pOutput);
  }

  tfree(leftOutput.data);
  tfree(rightOutput.data);

  return 0;
}

SScalarFunctionInfo scalarFunc[1] = {
    {

    },

};
