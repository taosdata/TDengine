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

#include "function.h"
#include "os.h"

#include "texception.h"
#include "taosdef.h"
#include "tmsg.h"
#include "tarray.h"
#include "tbuffer.h"
#include "tcompare.h"
#include "thash.h"
#include "texpr.h"
#include "tvariant.h"
#include "tdef.h"

//static uint8_t UNUSED_FUNC isQueryOnPrimaryKey(const char *primaryColumnName, const tExprNode *pLeft, const tExprNode *pRight) {
//  if (pLeft->nodeType == TEXPR_COL_NODE) {
//    // if left node is the primary column,return true
//    return (strcmp(primaryColumnName, pLeft->pSchema->name) == 0) ? 1 : 0;
//  } else {
//    // if any children have query on primary key, their parents are also keep this value
//    return ((pLeft->nodeType == TEXPR_BINARYEXPR_NODE && pLeft->_node.hasPK == 1) ||
//            (pRight->nodeType == TEXPR_BINARYEXPR_NODE && pRight->_node.hasPK == 1)) == true
//               ? 1
//               : 0;
//  }
//}

static void doExprTreeDestroy(tExprNode **pExpr, void (*fp)(void *));

void tExprTreeDestroy(tExprNode *pNode, void (*fp)(void *)) {
  if (pNode == NULL) {
    return;
  }

  if (pNode->nodeType == TEXPR_BINARYEXPR_NODE || pNode->nodeType == TEXPR_UNARYEXPR_NODE) {
    doExprTreeDestroy(&pNode, fp);
  } else if (pNode->nodeType == TEXPR_VALUE_NODE) {
    taosVariantDestroy(pNode->pVal);
  } else if (pNode->nodeType == TEXPR_COL_NODE) {
    taosMemoryFreeClear(pNode->pSchema);
  }

  taosMemoryFree(pNode);
}

static void doExprTreeDestroy(tExprNode **pExpr, void (*fp)(void *)) {
  if (*pExpr == NULL) {
    return;
  }

  int32_t type = (*pExpr)->nodeType;
  if (type == TEXPR_BINARYEXPR_NODE) {
    doExprTreeDestroy(&(*pExpr)->_node.pLeft, fp);
    doExprTreeDestroy(&(*pExpr)->_node.pRight, fp);
  
    if (fp != NULL) {
      fp((*pExpr)->_node.info);
    }
  } else if (type == TEXPR_UNARYEXPR_NODE) {
    doExprTreeDestroy(&(*pExpr)->_node.pLeft, fp);
    if (fp != NULL) {
      fp((*pExpr)->_node.info);
    }

    assert((*pExpr)->_node.pRight == NULL);
  } else if (type == TEXPR_VALUE_NODE) {
    taosVariantDestroy((*pExpr)->pVal);
    taosMemoryFree((*pExpr)->pVal);
  } else if (type == TEXPR_COL_NODE) {
    taosMemoryFree((*pExpr)->pSchema);
  }

  taosMemoryFree(*pExpr);
  *pExpr = NULL;
}

bool exprTreeApplyFilter(tExprNode *pExpr, const void *pItem, SExprTraverseSupp *param) {
  tExprNode *pLeft  = pExpr->_node.pLeft;
  tExprNode *pRight = pExpr->_node.pRight;

  //non-leaf nodes, recursively traverse the expression tree in the post-root order
  if (pLeft->nodeType == TEXPR_BINARYEXPR_NODE && pRight->nodeType == TEXPR_BINARYEXPR_NODE) {
    if (pExpr->_node.optr == LOGIC_COND_TYPE_OR) {  // or
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



static void exprTreeToBinaryImpl(SBufferWriter* bw, tExprNode* expr) {
  tbufWriteUint8(bw, expr->nodeType);
  
  if (expr->nodeType == TEXPR_VALUE_NODE) {
    SVariant* pVal = expr->pVal;
    
    tbufWriteUint32(bw, pVal->nType);
    if (pVal->nType == TSDB_DATA_TYPE_BINARY) {
      tbufWriteInt32(bw, pVal->nLen);
      tbufWrite(bw, pVal->pz, pVal->nLen);
    } else {
      tbufWriteInt64(bw, pVal->i);
    }
    
  } else if (expr->nodeType == TEXPR_COL_NODE) {
    SSchema* pSchema = expr->pSchema;
    tbufWriteInt16(bw, pSchema->colId);
    tbufWriteInt16(bw, pSchema->bytes);
    tbufWriteUint8(bw, pSchema->type);
    tbufWriteString(bw, pSchema->name);
    
  } else if (expr->nodeType == TEXPR_BINARYEXPR_NODE) {
    tbufWriteUint8(bw, expr->_node.optr);
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
  void* p = taosMemoryCalloc(nmemb, size);
  if (p == NULL) {
    THROW(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }
  return p;
}

static void* exception_malloc(size_t size) {
  void* p = taosMemoryMalloc(size);
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
  
  if (pExpr->nodeType == TEXPR_VALUE_NODE) {
    SVariant* pVal = exception_calloc(1, sizeof(SVariant));
    pExpr->pVal = pVal;
  
    pVal->nType = tbufReadUint32(br);
    if (pVal->nType == TSDB_DATA_TYPE_BINARY) {
      tbufReadToBuffer(br, &pVal->nLen, sizeof(pVal->nLen));
      pVal->pz = taosMemoryCalloc(1, pVal->nLen + 1);
      tbufReadToBuffer(br, pVal->pz, pVal->nLen);
    } else {
      pVal->i = tbufReadInt64(br);
    }
    
  } else if (pExpr->nodeType == TEXPR_COL_NODE) {
    SSchema* pSchema = exception_calloc(1, sizeof(SSchema));
    pExpr->pSchema = pSchema;

    pSchema->colId = tbufReadInt16(br);
    pSchema->bytes = tbufReadInt16(br);
    pSchema->type = tbufReadUint8(br);
    tbufReadToString(br, pSchema->name, TSDB_COL_NAME_LEN);
    
  } else if (pExpr->nodeType == TEXPR_BINARYEXPR_NODE) {
    pExpr->_node.optr = tbufReadUint8(br);
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

  expr->nodeType = TEXPR_BINARYEXPR_NODE;

  tExprNode* left = exception_calloc(1, sizeof(tExprNode));
  expr->_node.pLeft = left;

  left->nodeType = TEXPR_COL_NODE;
  SSchema* pSchema = exception_calloc(1, sizeof(SSchema));
  left->pSchema = pSchema;

//  *pSchema = NULL;//*tGetTbnameColumnSchema();

  tExprNode* right = exception_calloc(1, sizeof(tExprNode));
  expr->_node.pRight = right;

  if (strncmp(tbnameCond, QUERY_COND_REL_PREFIX_LIKE, QUERY_COND_REL_PREFIX_LIKE_LEN) == 0) {
    right->nodeType = TEXPR_VALUE_NODE;
    expr->_node.optr = OP_TYPE_LIKE;
    SVariant* pVal = exception_calloc(1, sizeof(SVariant));
    right->pVal = pVal;
    size_t len = strlen(tbnameCond + QUERY_COND_REL_PREFIX_LIKE_LEN) + 1;
    pVal->pz = exception_malloc(len);
    memcpy(pVal->pz, tbnameCond + QUERY_COND_REL_PREFIX_LIKE_LEN, len);
    pVal->nType = TSDB_DATA_TYPE_BINARY;
    pVal->nLen = (int32_t)len;

  } else if (strncmp(tbnameCond, QUERY_COND_REL_PREFIX_MATCH, QUERY_COND_REL_PREFIX_MATCH_LEN) == 0) {
    right->nodeType = TEXPR_VALUE_NODE;
    expr->_node.optr = OP_TYPE_MATCH;
    SVariant* pVal = exception_calloc(1, sizeof(SVariant));
    right->pVal = pVal;
    size_t len = strlen(tbnameCond + QUERY_COND_REL_PREFIX_MATCH_LEN) + 1;
    pVal->pz = exception_malloc(len);
    memcpy(pVal->pz, tbnameCond + QUERY_COND_REL_PREFIX_MATCH_LEN, len);
    pVal->nType = TSDB_DATA_TYPE_BINARY;
    pVal->nLen = (int32_t)len;
  } else if (strncmp(tbnameCond, QUERY_COND_REL_PREFIX_NMATCH, QUERY_COND_REL_PREFIX_NMATCH_LEN) == 0) {
    right->nodeType = TEXPR_VALUE_NODE;
    expr->_node.optr = OP_TYPE_NMATCH;
    SVariant* pVal = exception_calloc(1, sizeof(SVariant));
    right->pVal = pVal;
    size_t len = strlen(tbnameCond + QUERY_COND_REL_PREFIX_NMATCH_LEN) + 1;
    pVal->pz = exception_malloc(len);
    memcpy(pVal->pz, tbnameCond + QUERY_COND_REL_PREFIX_NMATCH_LEN, len);
    pVal->nType = TSDB_DATA_TYPE_BINARY;
    pVal->nLen = (int32_t)len;
  } else if (strncmp(tbnameCond, QUERY_COND_REL_PREFIX_IN, QUERY_COND_REL_PREFIX_IN_LEN) == 0) {
    right->nodeType = TEXPR_VALUE_NODE;
    expr->_node.optr = OP_TYPE_IN;
    SVariant* pVal = exception_calloc(1, sizeof(SVariant));
    right->pVal = pVal;
    pVal->nType = TSDB_DATA_TYPE_POINTER_ARRAY;
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

void buildFilterSetFromBinary(void **q, const char *buf, int32_t len) {
  SBufferReader br = tbufInitReader(buf, len, false); 
  uint32_t type  = tbufReadUint32(&br);     
  SHashObj *pObj = taosHashInit(256, taosGetDefaultHashFunction(type), true, false);
  
//  taosHashSetEqualFp(pObj, taosGetDefaultEqualFunction(type));

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
  
//  taosHashSetEqualFp(pObj, taosGetDefaultEqualFunction(tType));
  
  int dummy = -1;
  SVariant tmpVar = {0};  
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

  char *tmp = taosMemoryCalloc(1, bufLen * TSDB_NCHAR_SIZE);
    
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
    
    taosVariantCreateFromBinary(&tmpVar, (char *)pvar, t, sType);

    if (bufLen < t) {
      tmp = taosMemoryRealloc(tmp, t * TSDB_NCHAR_SIZE);
      bufLen = (int32_t)t;
    }

    switch (tType) {
      case TSDB_DATA_TYPE_BOOL:
      case TSDB_DATA_TYPE_UTINYINT:
      case TSDB_DATA_TYPE_TINYINT: {
        if (taosVariantDump(&tmpVar, (char *)&val, tType, false)) {
          goto err_ret;
        }
        pvar = &val;
        t = sizeof(val);
        break;
      }
      case TSDB_DATA_TYPE_USMALLINT:
      case TSDB_DATA_TYPE_SMALLINT: {
        if (taosVariantDump(&tmpVar, (char *)&val, tType, false)) {
          goto err_ret;
        }
        pvar = &val;
        t = sizeof(val);
        break;
      }
      case TSDB_DATA_TYPE_UINT:
      case TSDB_DATA_TYPE_INT: {
        if (taosVariantDump(&tmpVar, (char *)&val, tType, false)) {
          goto err_ret;
        }
        pvar = &val;
        t = sizeof(val);
        break;
      }
      case TSDB_DATA_TYPE_TIMESTAMP:
      case TSDB_DATA_TYPE_UBIGINT:
      case TSDB_DATA_TYPE_BIGINT: {
        if (taosVariantDump(&tmpVar, (char *)&val, tType, false)) {
          goto err_ret;
        }
        pvar = &val;
        t = sizeof(val);
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        if (taosVariantDump(&tmpVar, (char *)&val, tType, false)) {
          goto err_ret;
        }
        pvar = &val;
        t = sizeof(val);
        break;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        if (taosVariantDump(&tmpVar, (char *)&val, tType, false)) {
          goto err_ret;
        }
        pvar = &val;
        t = sizeof(val);
        break;
      }
      case TSDB_DATA_TYPE_BINARY: {
        if (taosVariantDump(&tmpVar, tmp, tType, true)) {
          goto err_ret;
        }
        t = varDataLen(tmp);
        pvar = varDataVal(tmp);
        break;
      }
      case TSDB_DATA_TYPE_NCHAR: {
        if (taosVariantDump(&tmpVar, tmp, tType, true)) {
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
    taosVariantDestroy(&tmpVar);
    memset(&tmpVar, 0, sizeof(tmpVar));
  } 

  *q = (void *)pObj;
  pObj = NULL;
  
err_ret:  
  taosVariantDestroy(&tmpVar);
  taosHashCleanup(pObj);
  taosMemoryFreeClear(tmp);
}

tExprNode* exprdup(tExprNode* pNode) {
  if (pNode == NULL) {
    return NULL;
  }

  tExprNode* pCloned = taosMemoryCalloc(1, sizeof(tExprNode));
  if (pNode->nodeType == TEXPR_BINARYEXPR_NODE) {
    tExprNode* pLeft  = exprdup(pNode->_node.pLeft);
    tExprNode* pRight = exprdup(pNode->_node.pRight);

    pCloned->_node.pLeft  = pLeft;
    pCloned->_node.pRight = pRight;
    pCloned->_node.optr  = pNode->_node.optr;
  } else if (pNode->nodeType == TEXPR_VALUE_NODE) {
    pCloned->pVal = taosMemoryCalloc(1, sizeof(SVariant));
    taosVariantAssign(pCloned->pVal, pNode->pVal);
  } else if (pNode->nodeType == TEXPR_COL_NODE) {
    pCloned->pSchema = taosMemoryCalloc(1, sizeof(SSchema));
    *pCloned->pSchema = *pNode->pSchema;
  } else if (pNode->nodeType == TEXPR_FUNCTION_NODE) {
    strcpy(pCloned->_function.functionName, pNode->_function.functionName);

    int32_t num = pNode->_function.num;
    pCloned->_function.num = num;
    pCloned->_function.pChild = taosMemoryCalloc(num, POINTER_BYTES);
    for(int32_t i = 0; i < num; ++i) {
      pCloned->_function.pChild[i] = exprdup(pNode->_function.pChild[i]);
    }
  }

  pCloned->nodeType = pNode->nodeType;
  return pCloned;
}

