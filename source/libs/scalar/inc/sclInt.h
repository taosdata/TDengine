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
#ifndef TDENGINE_SCALARINT_H
#define TDENGINE_SCALARINT_H

#ifdef __cplusplus
extern "C" {
#endif
#include "query.h"
#include "tcommon.h"
#include "thash.h"

typedef struct SOperatorValueType {
  int32_t opResType;
  int32_t selfType;
  int32_t peerType;
} SOperatorValueType;

typedef struct SScalarCtx {
  int32_t            code;
  bool               dual;
  SArray*            pBlockList; /* element is SSDataBlock* */
  SHashObj*          pRes;       /* element is SScalarParam */
  void*              param;      // additional parameter (meta actually) for acquire value such as tbname/tags values
  SOperatorValueType type;
} SScalarCtx;

#define SCL_DATA_TYPE_DUMMY_HASH 9000
#define SCL_DEFAULT_OP_NUM       10

#define SCL_IS_NOTNULL_CONST_NODE(_node) ((QUERY_NODE_VALUE == (_node)->type) || (QUERY_NODE_NODE_LIST == (_node)->type))
#define SCL_IS_CONST_NODE(_node) \
  ((NULL == (_node)) || SCL_IS_NOTNULL_CONST_NODE(_node))
#define SCL_IS_VAR_VALUE_NODE(_node) ((QUERY_NODE_VALUE == (_node)->type) && IS_STR_DATA_TYPE(((SValueNode*)(_node))->node.resType.type))

#define SCL_IS_CONST_CALC(_ctx) (NULL == (_ctx)->pBlockList)
//#define SCL_IS_NULL_VALUE_NODE(_node) ((QUERY_NODE_VALUE == nodeType(_node)) && (TSDB_DATA_TYPE_NULL == ((SValueNode
//*)_node)->node.resType.type) && (((SValueNode *)_node)->placeholderNo <= 0))
#define SCL_IS_NULL_VALUE_NODE(_node) \
  ((QUERY_NODE_VALUE == nodeType(_node)) && (TSDB_DATA_TYPE_NULL == ((SValueNode*)_node)->node.resType.type))
#define SCL_IS_COMPARISON_OPERATOR(_opType) ((_opType) >= OP_TYPE_GREATER_THAN && (_opType) < OP_TYPE_IS_NOT_UNKNOWN)
#define SCL_DOWNGRADE_DATETYPE(_type) \
  ((_type) == TSDB_DATA_TYPE_BIGINT || TSDB_DATA_TYPE_DOUBLE == (_type) || (_type) == TSDB_DATA_TYPE_UBIGINT)

#define sclFatal(...) qFatal(__VA_ARGS__)
#define sclError(...) qError(__VA_ARGS__)
#define sclWarn(...)  qWarn(__VA_ARGS__)
#define sclInfo(...)  qInfo(__VA_ARGS__)
#define sclDebug(...) qDebug(__VA_ARGS__)
#define sclTrace(...) qTrace(__VA_ARGS__)

#define SCL_ERR_RET(c)                \
  do {                                \
    int32_t _code = c;                \
    if (_code != TSDB_CODE_SUCCESS) { \
      terrno = _code;                 \
      return _code;                   \
    }                                 \
  } while (0)
#define SCL_RET(c)                    \
  do {                                \
    int32_t _code = c;                \
    if (_code != TSDB_CODE_SUCCESS) { \
      terrno = _code;                 \
    }                                 \
    return _code;                     \
  } while (0)
#define SCL_ERR_JRET(c)              \
  do {                               \
    code = c;                        \
    if (code != TSDB_CODE_SUCCESS) { \
      terrno = code;                 \
      goto _return;                  \
    }                                \
  } while (0)

int32_t sclConvertValueToSclParam(SValueNode* pValueNode, SScalarParam* out, int32_t* overflow);
int32_t sclCreateColumnInfoData(SDataType* pType, int32_t numOfRows, SScalarParam* pParam);
int32_t sclConvertToTsValueNode(int8_t precision, SValueNode* valueNode);

#define GET_PARAM_TYPE(_c)     ((_c)->columnData ? (_c)->columnData->info.type : (_c)->hashValueType)
#define GET_PARAM_BYTES(_c)    ((_c)->columnData->info.bytes)
#define GET_PARAM_PRECISON(_c) ((_c)->columnData->info.precision)

void sclFreeParam(SScalarParam* param);
void doVectorCompare(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t startIndex, int32_t numOfRows, 
                     int32_t _ord, int32_t optr);
void vectorCompareImpl(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t startIndex, int32_t numOfRows, 
                             int32_t _ord, int32_t optr);                     
void vectorCompare(SScalarParam* pLeft, SScalarParam* pRight, SScalarParam *pOut, int32_t _ord, int32_t optr);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_SCALARINT_H
