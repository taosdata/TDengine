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

#include "nodesUtil.h"
#include "plannodes.h"
#include "tdatablock.h"

#define NODES_MSG_DEFAULT_LEN 1024

#define tlvForEach(pDecoder, pTlv, code) \
  while (TSDB_CODE_SUCCESS == code && TSDB_CODE_SUCCESS == (code = tlvGetNextTlv(pDecoder, &pTlv)) && NULL != pTlv)

typedef struct STlv {
  int16_t type;
  int16_t len;
  char    value[0];
} STlv;

typedef struct STlvEncoder {
  int32_t allocSize;
  int32_t offset;
  char*   pBuf;
  int32_t tlvCount;
} STlvEncoder;

typedef struct STlvDecoder {
  int32_t     bufSize;
  int32_t     offset;
  const char* pBuf;
} STlvDecoder;

typedef int32_t (*FToMsg)(const void* pObj, STlvEncoder* pEncoder);
typedef int32_t (*FToObject)(STlvDecoder* pDecoder, void* pObj);
typedef void* (*FMakeObject)(int16_t type);
typedef int32_t (*FSetObject)(STlv* pTlv, void* pObj);

static int32_t nodeToMsg(const void* pObj, STlvEncoder* pEncoder);
static int32_t nodeListToMsg(const void* pObj, STlvEncoder* pEncoder);
static int32_t msgToNode(STlvDecoder* pDecoder, void** pObj);
static int32_t msgToNodeFromTlv(STlv* pTlv, void** pObj);
static int32_t msgToNodeList(STlvDecoder* pDecoder, void** pObj);
static int32_t msgToNodeListFromTlv(STlv* pTlv, void** pObj);

static int32_t initTlvEncoder(STlvEncoder* pEncoder) {
  pEncoder->allocSize = NODES_MSG_DEFAULT_LEN;
  pEncoder->offset = 0;
  pEncoder->tlvCount = 0;
  pEncoder->pBuf = taosMemoryMalloc(pEncoder->allocSize);
  return NULL == pEncoder->pBuf ? TSDB_CODE_OUT_OF_MEMORY : TSDB_CODE_SUCCESS;
}

static void clearTlvEncoder(STlvEncoder* pEncoder) { taosMemoryFree(pEncoder->pBuf); }

static void endTlvEncode(STlvEncoder* pEncoder, char** pMsg, int32_t* pLen) {
  *pMsg = pEncoder->pBuf;
  pEncoder->pBuf = NULL;
  *pLen = pEncoder->offset;
  // nodesWarn("encode tlv count = %d, tl size = %d", pEncoder->tlvCount, sizeof(STlv) * pEncoder->tlvCount);
}

static int32_t tlvEncodeImpl(STlvEncoder* pEncoder, int16_t type, const void* pValue, int16_t len) {
  int32_t tlvLen = sizeof(STlv) + len;
  if (pEncoder->offset + tlvLen > pEncoder->allocSize) {
    void* pNewBuf = taosMemoryRealloc(pEncoder->pBuf, pEncoder->allocSize * 2);
    if (NULL == pNewBuf) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pEncoder->pBuf = pNewBuf;
    pEncoder->allocSize = pEncoder->allocSize * 2;
  }
  STlv* pTlv = (STlv*)(pEncoder->pBuf + pEncoder->offset);
  pTlv->type = type;
  pTlv->len = len;
  memcpy(pTlv->value, pValue, len);
  pEncoder->offset += tlvLen;
  ++(pEncoder->tlvCount);
  return TSDB_CODE_SUCCESS;
}

static int32_t tlvEncodeI8(STlvEncoder* pEncoder, int16_t type, int8_t value) {
  return tlvEncodeImpl(pEncoder, type, &value, sizeof(value));
}

static int32_t tlvEncodeI16(STlvEncoder* pEncoder, int16_t type, int16_t value) {
  return tlvEncodeImpl(pEncoder, type, &value, sizeof(value));
}

static int32_t tlvEncodeI32(STlvEncoder* pEncoder, int16_t type, int32_t value) {
  return tlvEncodeImpl(pEncoder, type, &value, sizeof(value));
}

static int32_t tlvEncodeI64(STlvEncoder* pEncoder, int16_t type, int64_t value) {
  return tlvEncodeImpl(pEncoder, type, &value, sizeof(value));
}

static int32_t tlvEncodeU8(STlvEncoder* pEncoder, int16_t type, uint8_t value) {
  return tlvEncodeImpl(pEncoder, type, &value, sizeof(value));
}

static int32_t tlvEncodeU16(STlvEncoder* pEncoder, int16_t type, uint16_t value) {
  return tlvEncodeImpl(pEncoder, type, &value, sizeof(value));
}

static int32_t tlvEncodeU64(STlvEncoder* pEncoder, int16_t type, uint64_t value) {
  return tlvEncodeImpl(pEncoder, type, &value, sizeof(value));
}

static int32_t tlvEncodeDouble(STlvEncoder* pEncoder, int16_t type, double value) {
  return tlvEncodeImpl(pEncoder, type, &value, sizeof(value));
}

static int32_t tlvEncodeEnum(STlvEncoder* pEncoder, int16_t type, int32_t value) {
  return tlvEncodeImpl(pEncoder, type, &value, sizeof(value));
}

static int32_t tlvEncodeBool(STlvEncoder* pEncoder, int16_t type, bool value) {
  return tlvEncodeImpl(pEncoder, type, &value, sizeof(value));
}

static int32_t tlvEncodeCStr(STlvEncoder* pEncoder, int16_t type, const char* pValue) {
  return tlvEncodeImpl(pEncoder, type, pValue, strlen(pValue));
}

static int32_t tlvEncodeBinary(STlvEncoder* pEncoder, int16_t type, const void* pValue, int32_t len) {
  return tlvEncodeImpl(pEncoder, type, pValue, len);
}

static int32_t tlvEncodeObj(STlvEncoder* pEncoder, int16_t type, FToMsg func, const void* pObj) {
  if (NULL == pObj) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t start = pEncoder->offset;
  pEncoder->offset += sizeof(STlv);
  int32_t code = func(pObj, pEncoder);
  if (TSDB_CODE_SUCCESS == code) {
    STlv* pTlv = (STlv*)(pEncoder->pBuf + start);
    pTlv->type = type;
    pTlv->len = pEncoder->offset - start - sizeof(STlv);
  }
  return code;
}

static int32_t tlvEncodeObjArray(STlvEncoder* pEncoder, int16_t type, FToMsg func, const void* pArray, int32_t itemSize,
                                 int32_t num) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (num > 0) {
    int32_t start = pEncoder->offset;
    pEncoder->offset += sizeof(STlv);
    for (size_t i = 0; TSDB_CODE_SUCCESS == code && i < num; ++i) {
      code = tlvEncodeObj(pEncoder, 0, func, (const char*)pArray + i * itemSize);
    }
    if (TSDB_CODE_SUCCESS == code) {
      STlv* pTlv = (STlv*)(pEncoder->pBuf + start);
      pTlv->type = type;
      pTlv->len = pEncoder->offset - start - sizeof(STlv);
    }
  }
  return code;
}

static int32_t tlvGetNextTlv(STlvDecoder* pDecoder, STlv** pTlv) {
  if (pDecoder->offset == pDecoder->bufSize) {
    *pTlv = NULL;
    return TSDB_CODE_SUCCESS;
  }

  *pTlv = (STlv*)(pDecoder->pBuf + pDecoder->offset);
  if ((*pTlv)->len + pDecoder->offset > pDecoder->bufSize) {
    return TSDB_CODE_FAILED;
  }
  pDecoder->offset += sizeof(STlv) + (*pTlv)->len;
  return TSDB_CODE_SUCCESS;
}

static bool tlvDecodeEnd(STlvDecoder* pDecoder) { return pDecoder->offset == pDecoder->bufSize; }

static int32_t tlvDecodeImpl(STlv* pTlv, void* pValue, int16_t len) {
  if (pTlv->len != len) {
    return TSDB_CODE_FAILED;
  }
  memcpy(pValue, pTlv->value, len);
  return TSDB_CODE_SUCCESS;
}

static int32_t tlvDecodeI8(STlv* pTlv, int8_t* pValue) { return tlvDecodeImpl(pTlv, pValue, sizeof(*pValue)); }

static int32_t tlvDecodeI16(STlv* pTlv, int16_t* pValue) { return tlvDecodeImpl(pTlv, pValue, sizeof(*pValue)); }

static int32_t tlvDecodeI32(STlv* pTlv, int32_t* pValue) { return tlvDecodeImpl(pTlv, pValue, sizeof(*pValue)); }

static int32_t tlvDecodeI64(STlv* pTlv, int64_t* pValue) { return tlvDecodeImpl(pTlv, pValue, sizeof(*pValue)); }

static int32_t tlvDecodeU8(STlv* pTlv, uint8_t* pValue) { return tlvDecodeImpl(pTlv, pValue, sizeof(*pValue)); }

static int32_t tlvDecodeU16(STlv* pTlv, uint16_t* pValue) { return tlvDecodeImpl(pTlv, pValue, sizeof(*pValue)); }

static int32_t tlvDecodeU64(STlv* pTlv, uint64_t* pValue) { return tlvDecodeImpl(pTlv, pValue, sizeof(*pValue)); }

static int32_t tlvDecodeDouble(STlv* pTlv, double* pValue) { return tlvDecodeImpl(pTlv, pValue, sizeof(*pValue)); }

static int32_t tlvDecodeBool(STlv* pTlv, bool* pValue) { return tlvDecodeImpl(pTlv, pValue, sizeof(*pValue)); }

static int32_t tlvDecodeEnum(STlv* pTlv, void* pValue, int16_t len) {
  int32_t value = 0;
  memcpy(&value, pTlv->value, pTlv->len);
  switch (len) {
    case 1:
      *(int8_t*)pValue = value;
      break;
    case 2:
      *(int16_t*)pValue = value;
      break;
    case 4:
      *(int32_t*)pValue = value;
      break;
    default:
      return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t tlvDecodeCStr(STlv* pTlv, char* pValue) {
  memcpy(pValue, pTlv->value, pTlv->len);
  return TSDB_CODE_SUCCESS;
}

static int32_t tlvDecodeDynBinary(STlv* pTlv, void** pValue) {
  *pValue = taosMemoryMalloc(pTlv->len);
  if (NULL == *pValue) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  memcpy(*pValue, pTlv->value, pTlv->len);
  return TSDB_CODE_SUCCESS;
}

static int32_t tlvDecodeObjFromTlv(STlv* pTlv, FToObject func, void* pObj) {
  STlvDecoder decoder = {.bufSize = pTlv->len, .offset = 0, .pBuf = pTlv->value};
  return func(&decoder, pObj);
}

static int32_t tlvDecodeObj(STlvDecoder* pDecoder, FToObject func, void* pObj) {
  STlv*   pTlv = NULL;
  int32_t code = tlvGetNextTlv(pDecoder, &pTlv);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeObjFromTlv(pTlv, func, pObj);
  }
  return code;
}

static int32_t tlvDecodeObjArray(STlvDecoder* pDecoder, FToObject func, void* pArray, int32_t itemSize) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t i = 0;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) { code = tlvDecodeObjFromTlv(pTlv, func, (char*)pArray + itemSize * i++); }
  return code;
}

static int32_t tlvDecodeObjArrayFromTlv(STlv* pTlv, FToObject func, void* pArray, int32_t itemSize) {
  STlvDecoder decoder = {.bufSize = pTlv->len, .offset = 0, .pBuf = pTlv->value};
  return tlvDecodeObjArray(&decoder, func, pArray, itemSize);
}

static int32_t tlvDecodeDynObjFromTlv(STlv* pTlv, FMakeObject makeFunc, FToObject toFunc, void** pObj) {
  *pObj = makeFunc(pTlv->type);
  if (NULL == *pObj) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return tlvDecodeObjFromTlv(pTlv, toFunc, *pObj);
}

static int32_t tlvDecodeDynObj(STlvDecoder* pDecoder, FMakeObject makeFunc, FToObject toFunc, void** pObj) {
  STlv*   pTlv = NULL;
  int32_t code = tlvGetNextTlv(pDecoder, &pTlv);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeDynObjFromTlv(pTlv, makeFunc, toFunc, pObj);
  }
  return code;
}

enum { DATA_TYPE_CODE_TYPE = 1, DATA_TYPE_CODE_PRECISION, DATA_TYPE_CODE_SCALE, DATA_TYPE_CODE_BYTES };

static int32_t dataTypeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SDataType* pNode = (const SDataType*)pObj;

  int32_t code = tlvEncodeI8(pEncoder, DATA_TYPE_CODE_TYPE, pNode->type);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeU8(pEncoder, DATA_TYPE_CODE_PRECISION, pNode->precision);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeU8(pEncoder, DATA_TYPE_CODE_SCALE, pNode->scale);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, DATA_TYPE_CODE_BYTES, pNode->bytes);
  }

  return code;
}

static int32_t msgToDataType(STlvDecoder* pDecoder, void* pObj) {
  SDataType* pNode = (SDataType*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case DATA_TYPE_CODE_TYPE:
        code = tlvDecodeI8(pTlv, &pNode->type);
        break;
      case DATA_TYPE_CODE_PRECISION:
        code = tlvDecodeU8(pTlv, &pNode->precision);
        break;
      case DATA_TYPE_CODE_SCALE:
        code = tlvDecodeU8(pTlv, &pNode->scale);
        break;
      case DATA_TYPE_CODE_BYTES:
        code = tlvDecodeI32(pTlv, &pNode->bytes);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { EXPR_CODE_RES_TYPE = 1 };

static int32_t exprNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SExprNode* pNode = (const SExprNode*)pObj;
  return tlvEncodeObj(pEncoder, EXPR_CODE_RES_TYPE, dataTypeToMsg, &pNode->resType);
}

static int32_t msgToExprNode(STlvDecoder* pDecoder, void* pObj) {
  SExprNode* pNode = (SExprNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case EXPR_CODE_RES_TYPE:
        code = tlvDecodeObjFromTlv(pTlv, msgToDataType, &pNode->resType);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  COLUMN_CODE_EXPR_BASE = 1,
  COLUMN_CODE_TABLE_ID,
  COLUMN_CODE_TABLE_TYPE,
  COLUMN_CODE_COLUMN_ID,
  COLUMN_CODE_COLUMN_TYPE,
  COLUMN_CODE_DATABLOCK_ID,
  COLUMN_CODE_SLOT_ID
};

static int32_t columnNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SColumnNode* pNode = (const SColumnNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, COLUMN_CODE_EXPR_BASE, exprNodeToMsg, pNode);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeU64(pEncoder, COLUMN_CODE_TABLE_ID, pNode->tableId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI8(pEncoder, COLUMN_CODE_TABLE_TYPE, pNode->tableType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI16(pEncoder, COLUMN_CODE_COLUMN_ID, pNode->colId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeEnum(pEncoder, COLUMN_CODE_COLUMN_TYPE, pNode->colType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI16(pEncoder, COLUMN_CODE_DATABLOCK_ID, pNode->dataBlockId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI16(pEncoder, COLUMN_CODE_SLOT_ID, pNode->slotId);
  }

  return code;
}

static int32_t msgToColumnNode(STlvDecoder* pDecoder, void* pObj) {
  SColumnNode* pNode = (SColumnNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case COLUMN_CODE_EXPR_BASE:
        code = tlvDecodeObjFromTlv(pTlv, msgToExprNode, &pNode->node);
        break;
      case COLUMN_CODE_TABLE_ID:
        code = tlvDecodeU64(pTlv, &pNode->tableId);
        break;
      case COLUMN_CODE_TABLE_TYPE:
        code = tlvDecodeI8(pTlv, &pNode->tableType);
        break;
      case COLUMN_CODE_COLUMN_ID:
        code = tlvDecodeI16(pTlv, &pNode->colId);
        break;
      case COLUMN_CODE_COLUMN_TYPE:
        code = tlvDecodeEnum(pTlv, &pNode->colType, sizeof(pNode->colType));
        break;
      case COLUMN_CODE_DATABLOCK_ID:
        code = tlvDecodeI16(pTlv, &pNode->dataBlockId);
        break;
      case COLUMN_CODE_SLOT_ID:
        code = tlvDecodeI16(pTlv, &pNode->slotId);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { VALUE_CODE_EXPR_BASE = 1, VALUE_CODE_IS_NULL, VALUE_CODE_DATUM };

static int32_t datumToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SValueNode* pNode = (const SValueNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  switch (pNode->node.resType.type) {
    case TSDB_DATA_TYPE_NULL:
      break;
    case TSDB_DATA_TYPE_BOOL:
      code = tlvEncodeBool(pEncoder, VALUE_CODE_DATUM, pNode->datum.b);
      break;
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      code = tlvEncodeI64(pEncoder, VALUE_CODE_DATUM, pNode->datum.i);
      break;
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
      code = tlvEncodeU64(pEncoder, VALUE_CODE_DATUM, pNode->datum.u);
      break;
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
      code = tlvEncodeDouble(pEncoder, VALUE_CODE_DATUM, pNode->datum.d);
      break;
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_NCHAR:
      code = tlvEncodeBinary(pEncoder, VALUE_CODE_DATUM, pNode->datum.p, varDataTLen(pNode->datum.p));
      break;
    case TSDB_DATA_TYPE_JSON:
      code = tlvEncodeBinary(pEncoder, VALUE_CODE_DATUM, pNode->datum.p, getJsonValueLen(pNode->datum.p));
      break;
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_BLOB:
      // todo
    default:
      break;
  }

  return code;
}

static int32_t valueNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SValueNode* pNode = (const SValueNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, VALUE_CODE_EXPR_BASE, exprNodeToMsg, pNode);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, VALUE_CODE_IS_NULL, pNode->isNull);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = datumToMsg(pNode, pEncoder);
  }

  return code;
}

static int32_t msgToDatum(STlv* pTlv, void* pObj) {
  SValueNode* pNode = (SValueNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  switch (pNode->node.resType.type) {
    case TSDB_DATA_TYPE_NULL:
      break;
    case TSDB_DATA_TYPE_BOOL:
      code = tlvDecodeBool(pTlv, &pNode->datum.b);
      *(bool*)&pNode->typeData = pNode->datum.b;
      break;
    case TSDB_DATA_TYPE_TINYINT:
      code = tlvDecodeI64(pTlv, &pNode->datum.i);
      *(int8_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      code = tlvDecodeI64(pTlv, &pNode->datum.i);
      *(int16_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_INT:
      code = tlvDecodeI64(pTlv, &pNode->datum.i);
      *(int32_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      code = tlvDecodeI64(pTlv, &pNode->datum.i);
      *(int64_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      code = tlvDecodeI64(pTlv, &pNode->datum.i);
      *(int64_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      code = tlvDecodeU64(pTlv, &pNode->datum.u);
      *(uint8_t*)&pNode->typeData = pNode->datum.u;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      code = tlvDecodeU64(pTlv, &pNode->datum.u);
      *(uint16_t*)&pNode->typeData = pNode->datum.u;
      break;
    case TSDB_DATA_TYPE_UINT:
      code = tlvDecodeU64(pTlv, &pNode->datum.u);
      *(uint32_t*)&pNode->typeData = pNode->datum.u;
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      code = tlvDecodeU64(pTlv, &pNode->datum.u);
      *(uint64_t*)&pNode->typeData = pNode->datum.u;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      code = tlvDecodeDouble(pTlv, &pNode->datum.d);
      *(float*)&pNode->typeData = pNode->datum.d;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      code = tlvDecodeDouble(pTlv, &pNode->datum.d);
      *(double*)&pNode->typeData = pNode->datum.d;
      break;
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
      code = tlvDecodeDynBinary(pTlv, (void**)&pNode->datum.p);
      if (TSDB_CODE_SUCCESS == code) {
        varDataSetLen(pNode->datum.p, pNode->node.resType.bytes - VARSTR_HEADER_SIZE);
      }
      break;
    case TSDB_DATA_TYPE_JSON:
      code = tlvDecodeDynBinary(pTlv, (void**)&pNode->datum.p);
      break;
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_BLOB:
      // todo
    default:
      break;
  }

  return code;
}

static int32_t msgToValueNode(STlvDecoder* pDecoder, void* pObj) {
  SValueNode* pNode = (SValueNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case VALUE_CODE_EXPR_BASE:
        code = tlvDecodeObjFromTlv(pTlv, msgToExprNode, &pNode->node);
        break;
      case VALUE_CODE_IS_NULL:
        code = tlvDecodeBool(pTlv, &pNode->isNull);
        break;
      case VALUE_CODE_DATUM:
        code = msgToDatum(pTlv, pNode);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { OPERATOR_CODE_EXPR_BASE = 1, OPERATOR_CODE_OP_TYPE, OPERATOR_CODE_LEFT, OPERATOR_CODE_RIGHT };

static int32_t operatorNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SOperatorNode* pNode = (const SOperatorNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, OPERATOR_CODE_EXPR_BASE, exprNodeToMsg, pNode);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeEnum(pEncoder, OPERATOR_CODE_OP_TYPE, pNode->opType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, OPERATOR_CODE_LEFT, nodeToMsg, pNode->pLeft);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, OPERATOR_CODE_RIGHT, nodeToMsg, pNode->pRight);
  }

  return code;
}

static int32_t msgToOperatorNode(STlvDecoder* pDecoder, void* pObj) {
  SOperatorNode* pNode = (SOperatorNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case OPERATOR_CODE_EXPR_BASE:
        code = tlvDecodeObjFromTlv(pTlv, msgToExprNode, &pNode->node);
        break;
      case OPERATOR_CODE_OP_TYPE:
        code = tlvDecodeEnum(pTlv, &pNode->opType, sizeof(pNode->opType));
        break;
      case OPERATOR_CODE_LEFT:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pLeft);
        break;
      case OPERATOR_CODE_RIGHT:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pRight);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { LOGIC_COND_CODE_EXPR_BASE = 1, LOGIC_COND_CODE_COND_TYPE, LOGIC_COND_CODE_PARAMETERS };

static int32_t logicConditionNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SLogicConditionNode* pNode = (const SLogicConditionNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, LOGIC_COND_CODE_EXPR_BASE, exprNodeToMsg, pNode);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeEnum(pEncoder, LOGIC_COND_CODE_COND_TYPE, pNode->condType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, LOGIC_COND_CODE_PARAMETERS, nodeListToMsg, pNode->pParameterList);
  }

  return code;
}

static int32_t msgToLogicConditionNode(STlvDecoder* pDecoder, void* pObj) {
  SLogicConditionNode* pNode = (SLogicConditionNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case LOGIC_COND_CODE_EXPR_BASE:
        code = tlvDecodeObjFromTlv(pTlv, msgToExprNode, &pNode->node);
        break;
      case LOGIC_COND_CODE_COND_TYPE:
        code = tlvDecodeEnum(pTlv, &pNode->condType, sizeof(pNode->condType));
        break;
      case LOGIC_COND_CODE_PARAMETERS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pParameterList);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  FUNCTION_CODE_EXPR_BASE = 1,
  FUNCTION_CODE_FUNCTION_ID,
  FUNCTION_CODE_FUNCTION_TYPE,
  FUNCTION_CODE_PARAMETERS,
  FUNCTION_CODE_UDF_BUF_SIZE
};

static int32_t functionNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SFunctionNode* pNode = (const SFunctionNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, FUNCTION_CODE_EXPR_BASE, exprNodeToMsg, pNode);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, FUNCTION_CODE_FUNCTION_ID, pNode->funcId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, FUNCTION_CODE_FUNCTION_TYPE, pNode->funcType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, FUNCTION_CODE_PARAMETERS, nodeListToMsg, pNode->pParameterList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, FUNCTION_CODE_UDF_BUF_SIZE, pNode->udfBufSize);
  }

  return code;
}

static int32_t msgToFunctionNode(STlvDecoder* pDecoder, void* pObj) {
  SFunctionNode* pNode = (SFunctionNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case FUNCTION_CODE_EXPR_BASE:
        code = tlvDecodeObjFromTlv(pTlv, msgToExprNode, &pNode->node);
        break;
      case FUNCTION_CODE_FUNCTION_ID:
        code = tlvDecodeI32(pTlv, &pNode->funcId);
        break;
      case FUNCTION_CODE_FUNCTION_TYPE:
        code = tlvDecodeI32(pTlv, &pNode->funcType);
        break;
      case FUNCTION_CODE_PARAMETERS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pParameterList);
        break;
      case FUNCTION_CODE_UDF_BUF_SIZE:
        code = tlvDecodeI32(pTlv, &pNode->udfBufSize);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { ORDER_BY_EXPR_CODE_EXPR = 1, ORDER_BY_EXPR_CODE_ORDER, ORDER_BY_EXPR_CODE_NULL_ORDER };

static int32_t orderByExprNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SOrderByExprNode* pNode = (const SOrderByExprNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, ORDER_BY_EXPR_CODE_EXPR, nodeToMsg, pNode->pExpr);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeEnum(pEncoder, ORDER_BY_EXPR_CODE_ORDER, pNode->order);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeEnum(pEncoder, ORDER_BY_EXPR_CODE_NULL_ORDER, pNode->nullOrder);
  }

  return code;
}

static int32_t msgToOrderByExprNode(STlvDecoder* pDecoder, void* pObj) {
  SOrderByExprNode* pNode = (SOrderByExprNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case ORDER_BY_EXPR_CODE_EXPR:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pExpr);
        break;
      case ORDER_BY_EXPR_CODE_ORDER:
        code = tlvDecodeEnum(pTlv, &pNode->order, sizeof(pNode->order));
        break;
      case ORDER_BY_EXPR_CODE_NULL_ORDER:
        code = tlvDecodeEnum(pTlv, &pNode->nullOrder, sizeof(pNode->nullOrder));
        break;
      default:
        break;
    }
  }

  return code;
}

enum { LIMIT_CODE_LIMIT = 1, LIMIT_CODE_OFFSET };

static int32_t limitNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SLimitNode* pNode = (const SLimitNode*)pObj;

  int32_t code = tlvEncodeI64(pEncoder, LIMIT_CODE_LIMIT, pNode->limit);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI64(pEncoder, LIMIT_CODE_OFFSET, pNode->offset);
  }

  return code;
}

static int32_t msgToLimitNode(STlvDecoder* pDecoder, void* pObj) {
  SLimitNode* pNode = (SLimitNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case LIMIT_CODE_LIMIT:
        code = tlvDecodeI64(pTlv, &pNode->limit);
        break;
      case LIMIT_CODE_OFFSET:
        code = tlvDecodeI64(pTlv, &pNode->offset);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { NAME_CODE_TYPE = 1, NAME_CODE_ACCT_ID, NAME_CODE_DB_NAME, NAME_CODE_TABLE_NAME };

static int32_t nameToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SName* pNode = (const SName*)pObj;

  int32_t code = tlvEncodeU8(pEncoder, NAME_CODE_TYPE, pNode->type);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, NAME_CODE_ACCT_ID, pNode->acctId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeCStr(pEncoder, NAME_CODE_DB_NAME, pNode->dbname);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeCStr(pEncoder, NAME_CODE_TABLE_NAME, pNode->tname);
  }

  return code;
}

static int32_t msgToName(STlvDecoder* pDecoder, void* pObj) {
  SName* pNode = (SName*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case NAME_CODE_TYPE:
        code = tlvDecodeU8(pTlv, &pNode->type);
        break;
      case NAME_CODE_ACCT_ID:
        code = tlvDecodeI32(pTlv, &pNode->acctId);
        break;
      case NAME_CODE_DB_NAME:
        code = tlvDecodeCStr(pTlv, pNode->dbname);
        break;
      case NAME_CODE_TABLE_NAME:
        code = tlvDecodeCStr(pTlv, pNode->tname);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { TIME_WINDOW_CODE_START_KEY = 1, TIME_WINDOW_CODE_END_KEY };

static int32_t timeWindowToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const STimeWindow* pNode = (const STimeWindow*)pObj;

  int32_t code = tlvEncodeI64(pEncoder, TIME_WINDOW_CODE_START_KEY, pNode->skey);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI64(pEncoder, TIME_WINDOW_CODE_END_KEY, pNode->ekey);
  }

  return code;
}

static int32_t msgToTimeWindow(STlvDecoder* pDecoder, void* pObj) {
  STimeWindow* pNode = (STimeWindow*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case TIME_WINDOW_CODE_START_KEY:
        code = tlvDecodeI64(pTlv, &pNode->skey);
        break;
      case TIME_WINDOW_CODE_END_KEY:
        code = tlvDecodeI64(pTlv, &pNode->ekey);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { NODE_LIST_CODE_DATA_TYPE = 1, NODE_LIST_CODE_NODE_LIST };

static int32_t nodeListNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SNodeListNode* pNode = (const SNodeListNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, NODE_LIST_CODE_DATA_TYPE, dataTypeToMsg, &pNode->dataType);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, NODE_LIST_CODE_NODE_LIST, nodeListToMsg, pNode->pNodeList);
  }

  return code;
}

static int32_t msgToNodeListNode(STlvDecoder* pDecoder, void* pObj) {
  SNodeListNode* pNode = (SNodeListNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case NODE_LIST_CODE_DATA_TYPE:
        code = tlvDecodeObjFromTlv(pTlv, msgToDataType, &pNode->dataType);
        break;
      case NODE_LIST_CODE_NODE_LIST:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pNodeList);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { TARGET_CODE_DATA_BLOCK_ID = 1, TARGET_CODE_SLOT_ID, TARGET_CODE_EXPR };

static int32_t targetNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const STargetNode* pNode = (const STargetNode*)pObj;

  int32_t code = tlvEncodeI16(pEncoder, TARGET_CODE_DATA_BLOCK_ID, pNode->dataBlockId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI16(pEncoder, TARGET_CODE_SLOT_ID, pNode->slotId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, TARGET_CODE_EXPR, nodeToMsg, pNode->pExpr);
  }

  return code;
}

static int32_t msgToTargetNode(STlvDecoder* pDecoder, void* pObj) {
  STargetNode* pNode = (STargetNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case TARGET_CODE_DATA_BLOCK_ID:
        code = tlvDecodeI16(pTlv, &pNode->dataBlockId);
        break;
      case TARGET_CODE_SLOT_ID:
        code = tlvDecodeI16(pTlv, &pNode->slotId);
        break;
      case TARGET_CODE_EXPR:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pExpr);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  DATA_BLOCK_DESC_CODE_DATA_BLOCK_ID = 1,
  DATA_BLOCK_DESC_CODE_SLOTS,
  DATA_BLOCK_DESC_CODE_TOTAL_ROW_SIZE,
  DATA_BLOCK_DESC_CODE_OUTPUT_ROW_SIZE,
  DATA_BLOCK_DESC_CODE_PRECISION
};

static int32_t dataBlockDescNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SDataBlockDescNode* pNode = (const SDataBlockDescNode*)pObj;

  int32_t code = tlvEncodeI16(pEncoder, DATA_BLOCK_DESC_CODE_DATA_BLOCK_ID, pNode->dataBlockId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, DATA_BLOCK_DESC_CODE_SLOTS, nodeListToMsg, pNode->pSlots);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, DATA_BLOCK_DESC_CODE_TOTAL_ROW_SIZE, pNode->totalRowSize);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, DATA_BLOCK_DESC_CODE_OUTPUT_ROW_SIZE, pNode->outputRowSize);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeU8(pEncoder, DATA_BLOCK_DESC_CODE_PRECISION, pNode->precision);
  }

  return code;
}

static int32_t msgToDataBlockDescNode(STlvDecoder* pDecoder, void* pObj) {
  SDataBlockDescNode* pNode = (SDataBlockDescNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case DATA_BLOCK_DESC_CODE_DATA_BLOCK_ID:
        code = tlvDecodeI16(pTlv, &pNode->dataBlockId);
        break;
      case DATA_BLOCK_DESC_CODE_SLOTS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pSlots);
        break;
      case DATA_BLOCK_DESC_CODE_TOTAL_ROW_SIZE:
        code = tlvDecodeI32(pTlv, &pNode->totalRowSize);
        break;
      case DATA_BLOCK_DESC_CODE_OUTPUT_ROW_SIZE:
        code = tlvDecodeI32(pTlv, &pNode->outputRowSize);
        break;
      case DATA_BLOCK_DESC_CODE_PRECISION:
        code = tlvDecodeU8(pTlv, &pNode->precision);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  SLOT_DESC_CODE_SLOT_ID = 1,
  SLOT_DESC_CODE_DATA_TYPE,
  SLOT_DESC_CODE_RESERVE,
  SLOT_DESC_CODE_OUTPUT,
  SLOT_DESC_CODE_TAG
};

static int32_t slotDescNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SSlotDescNode* pNode = (const SSlotDescNode*)pObj;

  int32_t code = tlvEncodeI16(pEncoder, SLOT_DESC_CODE_SLOT_ID, pNode->slotId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, SLOT_DESC_CODE_DATA_TYPE, dataTypeToMsg, &pNode->dataType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, SLOT_DESC_CODE_RESERVE, pNode->reserve);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, SLOT_DESC_CODE_OUTPUT, pNode->output);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, SLOT_DESC_CODE_TAG, pNode->tag);
  }

  return code;
}

static int32_t msgToSlotDescNode(STlvDecoder* pDecoder, void* pObj) {
  SSlotDescNode* pNode = (SSlotDescNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case SLOT_DESC_CODE_SLOT_ID:
        code = tlvDecodeI16(pTlv, &pNode->slotId);
        break;
      case SLOT_DESC_CODE_DATA_TYPE:
        code = tlvDecodeObjFromTlv(pTlv, msgToDataType, &pNode->dataType);
        break;
      case SLOT_DESC_CODE_RESERVE:
        code = tlvDecodeBool(pTlv, &pNode->reserve);
        break;
      case SLOT_DESC_CODE_OUTPUT:
        code = tlvDecodeBool(pTlv, &pNode->output);
        break;
      case SLOT_DESC_CODE_TAG:
        code = tlvDecodeBool(pTlv, &pNode->tag);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  PHY_NODE_CODE_OUTPUT_DESC = 1,
  PHY_NODE_CODE_CONDITIONS,
  PHY_NODE_CODE_CHILDREN,
  PHY_NODE_CODE_LIMIT,
  PHY_NODE_CODE_SLIMIT
};

static int32_t physiNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SPhysiNode* pNode = (const SPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_NODE_CODE_OUTPUT_DESC, nodeToMsg, pNode->pOutputDataBlockDesc);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_NODE_CODE_CONDITIONS, nodeToMsg, pNode->pConditions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_NODE_CODE_CHILDREN, nodeListToMsg, pNode->pChildren);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_NODE_CODE_LIMIT, nodeToMsg, pNode->pLimit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_NODE_CODE_SLIMIT, nodeToMsg, pNode->pSlimit);
  }

  return code;
}

static int32_t msgToPhysiNode(STlvDecoder* pDecoder, void* pObj) {
  SPhysiNode* pNode = (SPhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_NODE_CODE_OUTPUT_DESC:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pOutputDataBlockDesc);
        break;
      case PHY_NODE_CODE_CONDITIONS:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pConditions);
        break;
      case PHY_NODE_CODE_CHILDREN:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pChildren);
        break;
      case PHY_NODE_CODE_LIMIT:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pLimit);
        break;
      case PHY_NODE_CODE_SLIMIT:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pSlimit);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  PHY_SCAN_CODE_BASE_NODE = 1,
  PHY_SCAN_CODE_SCAN_COLS,
  PHY_SCAN_CODE_SCAN_PSEUDO_COLS,
  PHY_SCAN_CODE_BASE_UID,
  PHY_SCAN_CODE_BASE_SUID,
  PHY_SCAN_CODE_BASE_TABLE_TYPE,
  PHY_SCAN_CODE_BASE_TABLE_NAME
};

static int32_t physiScanNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SScanPhysiNode* pNode = (const SScanPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_SCAN_CODE_BASE_NODE, physiNodeToMsg, &pNode->node);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_SCAN_CODE_SCAN_COLS, nodeListToMsg, pNode->pScanCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_SCAN_CODE_SCAN_PSEUDO_COLS, nodeListToMsg, pNode->pScanPseudoCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeU64(pEncoder, PHY_SCAN_CODE_BASE_UID, pNode->uid);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeU64(pEncoder, PHY_SCAN_CODE_BASE_SUID, pNode->suid);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI8(pEncoder, PHY_SCAN_CODE_BASE_TABLE_TYPE, pNode->tableType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_SCAN_CODE_BASE_TABLE_NAME, nameToMsg, &pNode->tableName);
  }

  return code;
}

static int32_t msgToPhysiScanNode(STlvDecoder* pDecoder, void* pObj) {
  SScanPhysiNode* pNode = (SScanPhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_SCAN_CODE_BASE_NODE:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiNode, &pNode->node);
        break;
      case PHY_SCAN_CODE_SCAN_COLS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pScanCols);
        break;
      case PHY_SCAN_CODE_SCAN_PSEUDO_COLS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pScanPseudoCols);
        break;
      case PHY_SCAN_CODE_BASE_UID:
        code = tlvDecodeU64(pTlv, &pNode->uid);
        break;
      case PHY_SCAN_CODE_BASE_SUID:
        code = tlvDecodeU64(pTlv, &pNode->suid);
        break;
      case PHY_SCAN_CODE_BASE_TABLE_TYPE:
        code = tlvDecodeI8(pTlv, &pNode->tableType);
        break;
      case PHY_SCAN_CODE_BASE_TABLE_NAME:
        code = tlvDecodeObjFromTlv(pTlv, msgToName, &pNode->tableName);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { PHY_LAST_ROW_SCAN_CODE_SCAN = 1, PHY_LAST_ROW_SCAN_CODE_GROUP_TAGS, PHY_LAST_ROW_SCAN_CODE_GROUP_SORT };

static int32_t physiLastRowScanNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SLastRowScanPhysiNode* pNode = (const SLastRowScanPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_LAST_ROW_SCAN_CODE_SCAN, physiScanNodeToMsg, &pNode->scan);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_LAST_ROW_SCAN_CODE_GROUP_TAGS, nodeListToMsg, pNode->pGroupTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_LAST_ROW_SCAN_CODE_GROUP_SORT, pNode->groupSort);
  }

  return code;
}

static int32_t msgToPhysiLastRowScanNode(STlvDecoder* pDecoder, void* pObj) {
  SLastRowScanPhysiNode* pNode = (SLastRowScanPhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_LAST_ROW_SCAN_CODE_SCAN:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiScanNode, &pNode->scan);
        break;
      case PHY_LAST_ROW_SCAN_CODE_GROUP_TAGS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pGroupTags);
        break;
      case PHY_LAST_ROW_SCAN_CODE_GROUP_SORT:
        code = tlvDecodeBool(pTlv, &pNode->groupSort);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  PHY_TABLE_SCAN_CODE_SCAN = 1,
  PHY_TABLE_SCAN_CODE_SCAN_COUNT,
  PHY_TABLE_SCAN_CODE_REVERSE_SCAN_COUNT,
  PHY_TABLE_SCAN_CODE_SCAN_RANGE,
  PHY_TABLE_SCAN_CODE_RATIO,
  PHY_TABLE_SCAN_CODE_DATA_REQUIRED,
  PHY_TABLE_SCAN_CODE_DYN_SCAN_FUNCS,
  PHY_TABLE_SCAN_CODE_GROUP_TAGS,
  PHY_TABLE_SCAN_CODE_GROUP_SORT,
  PHY_TABLE_SCAN_CODE_INTERVAL,
  PHY_TABLE_SCAN_CODE_OFFSET,
  PHY_TABLE_SCAN_CODE_SLIDING,
  PHY_TABLE_SCAN_CODE_INTERVAL_UNIT,
  PHY_TABLE_SCAN_CODE_SLIDING_UNIT,
  PHY_TABLE_SCAN_CODE_TRIGGER_TYPE,
  PHY_TABLE_SCAN_CODE_WATERMARK,
  PHY_TABLE_SCAN_CODE_IG_EXPIRED,
  PHY_TABLE_SCAN_CODE_ASSIGN_BLOCK_UID,
};

static int32_t physiTableScanNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const STableScanPhysiNode* pNode = (const STableScanPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_TABLE_SCAN_CODE_SCAN, physiScanNodeToMsg, &pNode->scan);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeU8(pEncoder, PHY_TABLE_SCAN_CODE_SCAN_COUNT, pNode->scanSeq[0]);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeU8(pEncoder, PHY_TABLE_SCAN_CODE_REVERSE_SCAN_COUNT, pNode->scanSeq[1]);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_TABLE_SCAN_CODE_SCAN_RANGE, timeWindowToMsg, &pNode->scanRange);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeDouble(pEncoder, PHY_TABLE_SCAN_CODE_RATIO, pNode->ratio);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, PHY_TABLE_SCAN_CODE_DATA_REQUIRED, pNode->dataRequired);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_TABLE_SCAN_CODE_DYN_SCAN_FUNCS, nodeListToMsg, pNode->pDynamicScanFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_TABLE_SCAN_CODE_GROUP_TAGS, nodeListToMsg, pNode->pGroupTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_TABLE_SCAN_CODE_GROUP_SORT, pNode->groupSort);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI64(pEncoder, PHY_TABLE_SCAN_CODE_INTERVAL, pNode->interval);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI64(pEncoder, PHY_TABLE_SCAN_CODE_OFFSET, pNode->offset);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI64(pEncoder, PHY_TABLE_SCAN_CODE_SLIDING, pNode->sliding);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI8(pEncoder, PHY_TABLE_SCAN_CODE_INTERVAL_UNIT, pNode->intervalUnit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI8(pEncoder, PHY_TABLE_SCAN_CODE_SLIDING_UNIT, pNode->slidingUnit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI8(pEncoder, PHY_TABLE_SCAN_CODE_TRIGGER_TYPE, pNode->triggerType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI64(pEncoder, PHY_TABLE_SCAN_CODE_WATERMARK, pNode->watermark);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI8(pEncoder, PHY_TABLE_SCAN_CODE_IG_EXPIRED, pNode->igExpired);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_TABLE_SCAN_CODE_ASSIGN_BLOCK_UID, pNode->assignBlockUid);
  }

  return code;
}

static int32_t msgToPhysiTableScanNode(STlvDecoder* pDecoder, void* pObj) {
  STableScanPhysiNode* pNode = (STableScanPhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_TABLE_SCAN_CODE_SCAN:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiScanNode, &pNode->scan);
        break;
      case PHY_TABLE_SCAN_CODE_SCAN_COUNT:
        code = tlvDecodeU8(pTlv, pNode->scanSeq);
        break;
      case PHY_TABLE_SCAN_CODE_REVERSE_SCAN_COUNT:
        code = tlvDecodeU8(pTlv, pNode->scanSeq + 1);
        break;
      case PHY_TABLE_SCAN_CODE_SCAN_RANGE:
        code = tlvDecodeObjFromTlv(pTlv, msgToTimeWindow, &pNode->scanRange);
        break;
      case PHY_TABLE_SCAN_CODE_RATIO:
        code = tlvDecodeDouble(pTlv, &pNode->ratio);
        break;
      case PHY_TABLE_SCAN_CODE_DATA_REQUIRED:
        code = tlvDecodeI32(pTlv, &pNode->dataRequired);
        break;
      case PHY_TABLE_SCAN_CODE_DYN_SCAN_FUNCS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pDynamicScanFuncs);
        break;
      case PHY_TABLE_SCAN_CODE_GROUP_TAGS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pGroupTags);
        break;
      case PHY_TABLE_SCAN_CODE_GROUP_SORT:
        code = tlvDecodeBool(pTlv, &pNode->groupSort);
        break;
      case PHY_TABLE_SCAN_CODE_INTERVAL:
        code = tlvDecodeI64(pTlv, &pNode->interval);
        break;
      case PHY_TABLE_SCAN_CODE_OFFSET:
        code = tlvDecodeI64(pTlv, &pNode->offset);
        break;
      case PHY_TABLE_SCAN_CODE_SLIDING:
        code = tlvDecodeI64(pTlv, &pNode->sliding);
        break;
      case PHY_TABLE_SCAN_CODE_INTERVAL_UNIT:
        code = tlvDecodeI8(pTlv, &pNode->intervalUnit);
        break;
      case PHY_TABLE_SCAN_CODE_SLIDING_UNIT:
        code = tlvDecodeI8(pTlv, &pNode->slidingUnit);
        break;
      case PHY_TABLE_SCAN_CODE_TRIGGER_TYPE:
        code = tlvDecodeI8(pTlv, &pNode->triggerType);
        break;
      case PHY_TABLE_SCAN_CODE_WATERMARK:
        code = tlvDecodeI64(pTlv, &pNode->watermark);
        break;
      case PHY_TABLE_SCAN_CODE_IG_EXPIRED:
        code = tlvDecodeI8(pTlv, &pNode->igExpired);
        break;
      case PHY_TABLE_SCAN_CODE_ASSIGN_BLOCK_UID:
        code = tlvDecodeBool(pTlv, &pNode->assignBlockUid);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { EP_CODE_FQDN = 1, EP_CODE_port };

static int32_t epToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SEp* pNode = (const SEp*)pObj;

  int32_t code = tlvEncodeCStr(pEncoder, EP_CODE_FQDN, pNode->fqdn);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeU16(pEncoder, EP_CODE_port, pNode->port);
  }

  return code;
}

static int32_t msgToEp(STlvDecoder* pDecoder, void* pObj) {
  SEp* pNode = (SEp*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case EP_CODE_FQDN:
        code = tlvDecodeCStr(pTlv, pNode->fqdn);
        break;
      case EP_CODE_port:
        code = tlvDecodeU16(pTlv, &pNode->port);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { EP_SET_CODE_IN_USE = 1, EP_SET_CODE_NUM_OF_EPS, EP_SET_CODE_EPS };

static int32_t epSetToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SEpSet* pNode = (const SEpSet*)pObj;

  int32_t code = tlvEncodeI8(pEncoder, EP_SET_CODE_IN_USE, pNode->inUse);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI8(pEncoder, EP_SET_CODE_NUM_OF_EPS, pNode->numOfEps);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObjArray(pEncoder, EP_SET_CODE_EPS, epToMsg, pNode->eps, sizeof(SEp), pNode->numOfEps);
  }

  return code;
}

static int32_t msgToEpSet(STlvDecoder* pDecoder, void* pObj) {
  SEpSet* pNode = (SEpSet*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case EP_SET_CODE_IN_USE:
        code = tlvDecodeI8(pTlv, &pNode->inUse);
        break;
      case EP_SET_CODE_NUM_OF_EPS:
        code = tlvDecodeI8(pTlv, &pNode->numOfEps);
        break;
      case EP_SET_CODE_EPS:
        code = tlvDecodeObjArrayFromTlv(pTlv, msgToEp, pNode->eps, sizeof(SEp));
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  PHY_SYSTABLE_SCAN_CODE_SCAN = 1,
  PHY_SYSTABLE_SCAN_CODE_MGMT_EP_SET,
  PHY_SYSTABLE_SCAN_CODE_SHOW_REWRITE,
  PHY_SYSTABLE_SCAN_CODE_ACCOUNT_ID,
  PHY_SYSTABLE_SCAN_CODE_SYS_INFO
};

static int32_t physiSysTableScanNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SSystemTableScanPhysiNode* pNode = (const SSystemTableScanPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_SYSTABLE_SCAN_CODE_SCAN, physiScanNodeToMsg, &pNode->scan);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_SYSTABLE_SCAN_CODE_MGMT_EP_SET, epSetToMsg, &pNode->mgmtEpSet);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_SYSTABLE_SCAN_CODE_SHOW_REWRITE, pNode->showRewrite);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, PHY_SYSTABLE_SCAN_CODE_ACCOUNT_ID, pNode->accountId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_SYSTABLE_SCAN_CODE_SYS_INFO, pNode->sysInfo);
  }

  return code;
}

static int32_t msgToPhysiSysTableScanNode(STlvDecoder* pDecoder, void* pObj) {
  SSystemTableScanPhysiNode* pNode = (SSystemTableScanPhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_SYSTABLE_SCAN_CODE_SCAN:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiScanNode, &pNode->scan);
        break;
      case PHY_SYSTABLE_SCAN_CODE_MGMT_EP_SET:
        code = tlvDecodeObjFromTlv(pTlv, msgToEpSet, &pNode->mgmtEpSet);
        break;
      case PHY_SYSTABLE_SCAN_CODE_SHOW_REWRITE:
        code = tlvDecodeBool(pTlv, &pNode->showRewrite);
        break;
      case PHY_SYSTABLE_SCAN_CODE_ACCOUNT_ID:
        code = tlvDecodeI32(pTlv, &pNode->accountId);
        break;
      case PHY_SYSTABLE_SCAN_CODE_SYS_INFO:
        code = tlvDecodeBool(pTlv, &pNode->sysInfo);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  PHY_PROJECT_CODE_BASE_NODE = 1,
  PHY_PROJECT_CODE_PROJECTIONS,
  PHY_PROJECT_CODE_MERGE_DATA_BLOCK,
  PHY_PROJECT_CODE_IGNORE_GROUP_ID
};

static int32_t physiProjectNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SProjectPhysiNode* pNode = (const SProjectPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_PROJECT_CODE_BASE_NODE, physiNodeToMsg, &pNode->node);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_PROJECT_CODE_PROJECTIONS, nodeListToMsg, pNode->pProjections);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_PROJECT_CODE_MERGE_DATA_BLOCK, pNode->mergeDataBlock);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_PROJECT_CODE_IGNORE_GROUP_ID, pNode->ignoreGroupId);
  }

  return code;
}

static int32_t msgToPhysiProjectNode(STlvDecoder* pDecoder, void* pObj) {
  SProjectPhysiNode* pNode = (SProjectPhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_PROJECT_CODE_BASE_NODE:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiNode, &pNode->node);
        break;
      case PHY_PROJECT_CODE_PROJECTIONS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pProjections);
        break;
      case PHY_PROJECT_CODE_MERGE_DATA_BLOCK:
        code = tlvDecodeBool(pTlv, &pNode->mergeDataBlock);
        break;
      case PHY_PROJECT_CODE_IGNORE_GROUP_ID:
        code = tlvDecodeBool(pTlv, &pNode->ignoreGroupId);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  PHY_SORT_MERGE_JOIN_CODE_BASE_NODE = 1,
  PHY_SORT_MERGE_JOIN_CODE_JOIN_TYPE,
  PHY_SORT_MERGE_JOIN_CODE_MERGE_CONDITION,
  PHY_SORT_MERGE_JOIN_CODE_ON_CONDITIONS,
  PHY_SORT_MERGE_JOIN_CODE_TARGETS,
  PHY_SORT_MERGE_JOIN_CODE_INPUT_TS_ORDER
};

static int32_t physiJoinNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SSortMergeJoinPhysiNode* pNode = (const SSortMergeJoinPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_SORT_MERGE_JOIN_CODE_BASE_NODE, physiNodeToMsg, &pNode->node);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeEnum(pEncoder, PHY_SORT_MERGE_JOIN_CODE_JOIN_TYPE, pNode->joinType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_SORT_MERGE_JOIN_CODE_MERGE_CONDITION, nodeToMsg, pNode->pMergeCondition);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_SORT_MERGE_JOIN_CODE_ON_CONDITIONS, nodeToMsg, pNode->pOnConditions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_SORT_MERGE_JOIN_CODE_TARGETS, nodeListToMsg, pNode->pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeEnum(pEncoder, PHY_SORT_MERGE_JOIN_CODE_INPUT_TS_ORDER, pNode->inputTsOrder);
  }

  return code;
}

static int32_t msgToPhysiJoinNode(STlvDecoder* pDecoder, void* pObj) {
  SSortMergeJoinPhysiNode* pNode = (SSortMergeJoinPhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_SORT_MERGE_JOIN_CODE_BASE_NODE:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiNode, &pNode->node);
        break;
      case PHY_SORT_MERGE_JOIN_CODE_JOIN_TYPE:
        code = tlvDecodeEnum(pTlv, &pNode->joinType, sizeof(pNode->joinType));
        break;
      case PHY_SORT_MERGE_JOIN_CODE_MERGE_CONDITION:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pMergeCondition);
        break;
      case PHY_SORT_MERGE_JOIN_CODE_ON_CONDITIONS:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pOnConditions);
        break;
      case PHY_SORT_MERGE_JOIN_CODE_TARGETS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pTargets);
        break;
      case PHY_SORT_MERGE_JOIN_CODE_INPUT_TS_ORDER:
        code = tlvDecodeEnum(pTlv, &pNode->inputTsOrder, sizeof(pNode->inputTsOrder));
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  PHY_AGG_CODE_BASE_NODE = 1,
  PHY_AGG_CODE_EXPR,
  PHY_AGG_CODE_GROUP_KEYS,
  PHY_AGG_CODE_AGG_FUNCS,
  PHY_AGG_CODE_MERGE_DATA_BLOCK
};

static int32_t physiAggNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SAggPhysiNode* pNode = (const SAggPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_AGG_CODE_BASE_NODE, physiNodeToMsg, &pNode->node);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_AGG_CODE_EXPR, nodeListToMsg, pNode->pExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_AGG_CODE_GROUP_KEYS, nodeListToMsg, pNode->pGroupKeys);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_AGG_CODE_AGG_FUNCS, nodeListToMsg, pNode->pAggFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_AGG_CODE_MERGE_DATA_BLOCK, pNode->mergeDataBlock);
  }

  return code;
}

static int32_t msgToPhysiAggNode(STlvDecoder* pDecoder, void* pObj) {
  SAggPhysiNode* pNode = (SAggPhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_AGG_CODE_BASE_NODE:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiNode, &pNode->node);
        break;
      case PHY_AGG_CODE_EXPR:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pExprs);
        break;
      case PHY_AGG_CODE_GROUP_KEYS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pGroupKeys);
        break;
      case PHY_AGG_CODE_AGG_FUNCS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pAggFuncs);
        break;
      case PHY_AGG_CODE_MERGE_DATA_BLOCK:
        code = tlvDecodeBool(pTlv, &pNode->mergeDataBlock);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  PHY_EXCHANGE_CODE_BASE_NODE = 1,
  PHY_EXCHANGE_CODE_SRC_GROUP_ID,
  PHY_EXCHANGE_CODE_SINGLE_CHANNEL,
  PHY_EXCHANGE_CODE_SRC_ENDPOINTS
};

static int32_t physiExchangeNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SExchangePhysiNode* pNode = (const SExchangePhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_EXCHANGE_CODE_BASE_NODE, physiNodeToMsg, &pNode->node);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, PHY_EXCHANGE_CODE_SRC_GROUP_ID, pNode->srcGroupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_EXCHANGE_CODE_SINGLE_CHANNEL, pNode->singleChannel);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_EXCHANGE_CODE_SRC_ENDPOINTS, nodeListToMsg, pNode->pSrcEndPoints);
  }

  return code;
}

static int32_t msgToPhysiExchangeNode(STlvDecoder* pDecoder, void* pObj) {
  SExchangePhysiNode* pNode = (SExchangePhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_EXCHANGE_CODE_BASE_NODE:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiNode, &pNode->node);
        break;
      case PHY_EXCHANGE_CODE_SRC_GROUP_ID:
        code = tlvDecodeI32(pTlv, &pNode->srcGroupId);
        break;
      case PHY_EXCHANGE_CODE_SINGLE_CHANNEL:
        code = tlvDecodeBool(pTlv, &pNode->singleChannel);
        break;
      case PHY_EXCHANGE_CODE_SRC_ENDPOINTS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pSrcEndPoints);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  PHY_MERGE_CODE_BASE_NODE = 1,
  PHY_MERGE_CODE_MERGE_KEYS,
  PHY_MERGE_CODE_TARGETS,
  PHY_MERGE_CODE_NUM_OF_CHANNELS,
  PHY_MERGE_CODE_SRC_GROUP_ID,
  PHY_MERGE_CODE_GROUP_SORT
};

static int32_t physiMergeNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SMergePhysiNode* pNode = (const SMergePhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_MERGE_CODE_BASE_NODE, physiNodeToMsg, &pNode->node);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_MERGE_CODE_MERGE_KEYS, nodeListToMsg, pNode->pMergeKeys);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_MERGE_CODE_TARGETS, nodeListToMsg, pNode->pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, PHY_MERGE_CODE_NUM_OF_CHANNELS, pNode->numOfChannels);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, PHY_MERGE_CODE_SRC_GROUP_ID, pNode->srcGroupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_MERGE_CODE_GROUP_SORT, pNode->groupSort);
  }

  return code;
}

static int32_t msgToPhysiMergeNode(STlvDecoder* pDecoder, void* pObj) {
  SMergePhysiNode* pNode = (SMergePhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_MERGE_CODE_BASE_NODE:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiNode, &pNode->node);
        break;
      case PHY_MERGE_CODE_MERGE_KEYS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pMergeKeys);
        break;
      case PHY_MERGE_CODE_TARGETS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pTargets);
        break;
      case PHY_MERGE_CODE_NUM_OF_CHANNELS:
        code = tlvDecodeI32(pTlv, &pNode->numOfChannels);
        break;
      case PHY_MERGE_CODE_SRC_GROUP_ID:
        code = tlvDecodeI32(pTlv, &pNode->srcGroupId);
        break;
      case PHY_MERGE_CODE_GROUP_SORT:
        code = tlvDecodeBool(pTlv, &pNode->groupSort);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { PHY_SORT_CODE_BASE_NODE = 1, PHY_SORT_CODE_EXPR, PHY_SORT_CODE_SORT_KEYS, PHY_SORT_CODE_TARGETS };

static int32_t physiSortNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SSortPhysiNode* pNode = (const SSortPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_SORT_CODE_BASE_NODE, physiNodeToMsg, &pNode->node);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_SORT_CODE_EXPR, nodeListToMsg, pNode->pExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_SORT_CODE_SORT_KEYS, nodeListToMsg, pNode->pSortKeys);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_SORT_CODE_TARGETS, nodeListToMsg, pNode->pTargets);
  }

  return code;
}

static int32_t msgToPhysiSortNode(STlvDecoder* pDecoder, void* pObj) {
  SSortPhysiNode* pNode = (SSortPhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_SORT_CODE_BASE_NODE:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiNode, &pNode->node);
        break;
      case PHY_SORT_CODE_EXPR:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pExprs);
        break;
      case PHY_SORT_CODE_SORT_KEYS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pSortKeys);
        break;
      case PHY_SORT_CODE_TARGETS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pTargets);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  PHY_WINDOW_CODE_BASE_NODE = 1,
  PHY_WINDOW_CODE_EXPR,
  PHY_WINDOW_CODE_FUNCS,
  PHY_WINDOW_CODE_TS_PK,
  PHY_WINDOW_CODE_TS_END,
  PHY_WINDOW_CODE_TRIGGER_TYPE,
  PHY_WINDOW_CODE_WATERMARK,
  PHY_WINDOW_CODE_IG_EXPIRED,
  PHY_WINDOW_CODE_INPUT_TS_ORDER,
  PHY_WINDOW_CODE_OUTPUT_TS_ORDER,
  PHY_WINDOW_CODE_MERGE_DATA_BLOCK
};

static int32_t physiWindowNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SWinodwPhysiNode* pNode = (const SWinodwPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_WINDOW_CODE_BASE_NODE, physiNodeToMsg, &pNode->node);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_WINDOW_CODE_EXPR, nodeListToMsg, pNode->pExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_WINDOW_CODE_FUNCS, nodeListToMsg, pNode->pFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_WINDOW_CODE_TS_PK, nodeToMsg, pNode->pTspk);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_WINDOW_CODE_TS_END, nodeToMsg, pNode->pTsEnd);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI8(pEncoder, PHY_WINDOW_CODE_TRIGGER_TYPE, pNode->triggerType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI64(pEncoder, PHY_WINDOW_CODE_WATERMARK, pNode->watermark);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI8(pEncoder, PHY_WINDOW_CODE_IG_EXPIRED, pNode->igExpired);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeEnum(pEncoder, PHY_WINDOW_CODE_INPUT_TS_ORDER, pNode->inputTsOrder);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeEnum(pEncoder, PHY_WINDOW_CODE_OUTPUT_TS_ORDER, pNode->outputTsOrder);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_WINDOW_CODE_MERGE_DATA_BLOCK, pNode->mergeDataBlock);
  }

  return code;
}

static int32_t msgToPhysiWindowNode(STlvDecoder* pDecoder, void* pObj) {
  SWinodwPhysiNode* pNode = (SWinodwPhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_WINDOW_CODE_BASE_NODE:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiNode, &pNode->node);
        break;
      case PHY_WINDOW_CODE_EXPR:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pExprs);
        break;
      case PHY_WINDOW_CODE_FUNCS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pFuncs);
        break;
      case PHY_WINDOW_CODE_TS_PK:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pTspk);
        break;
      case PHY_WINDOW_CODE_TS_END:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pTsEnd);
        break;
      case PHY_WINDOW_CODE_TRIGGER_TYPE:
        code = tlvDecodeI8(pTlv, &pNode->triggerType);
        break;
      case PHY_WINDOW_CODE_WATERMARK:
        code = tlvDecodeI64(pTlv, &pNode->watermark);
        break;
      case PHY_WINDOW_CODE_IG_EXPIRED:
        code = tlvDecodeI8(pTlv, &pNode->igExpired);
        break;
      case PHY_WINDOW_CODE_INPUT_TS_ORDER:
        code = tlvDecodeEnum(pTlv, &pNode->inputTsOrder, sizeof(pNode->inputTsOrder));
        break;
      case PHY_WINDOW_CODE_OUTPUT_TS_ORDER:
        code = tlvDecodeEnum(pTlv, &pNode->outputTsOrder, sizeof(pNode->outputTsOrder));
        break;
      case PHY_WINDOW_CODE_MERGE_DATA_BLOCK:
        code = tlvDecodeBool(pTlv, &pNode->mergeDataBlock);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  PHY_INTERVAL_CODE_WINDOW = 1,
  PHY_INTERVAL_CODE_INTERVAL,
  PHY_INTERVAL_CODE_OFFSET,
  PHY_INTERVAL_CODE_SLIDING,
  PHY_INTERVAL_CODE_INTERVAL_UNIT,
  PHY_INTERVAL_CODE_SLIDING_UNIT
};

static int32_t physiIntervalNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SIntervalPhysiNode* pNode = (const SIntervalPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_INTERVAL_CODE_WINDOW, physiWindowNodeToMsg, &pNode->window);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI64(pEncoder, PHY_INTERVAL_CODE_INTERVAL, pNode->interval);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI64(pEncoder, PHY_INTERVAL_CODE_OFFSET, pNode->offset);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI64(pEncoder, PHY_INTERVAL_CODE_SLIDING, pNode->sliding);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI8(pEncoder, PHY_INTERVAL_CODE_INTERVAL_UNIT, pNode->intervalUnit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI8(pEncoder, PHY_INTERVAL_CODE_SLIDING_UNIT, pNode->slidingUnit);
  }

  return code;
}

static int32_t msgToPhysiIntervalNode(STlvDecoder* pDecoder, void* pObj) {
  SIntervalPhysiNode* pNode = (SIntervalPhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_INTERVAL_CODE_WINDOW:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiWindowNode, &pNode->window);
        break;
      case PHY_INTERVAL_CODE_INTERVAL:
        code = tlvDecodeI64(pTlv, &pNode->interval);
        break;
      case PHY_INTERVAL_CODE_OFFSET:
        code = tlvDecodeI64(pTlv, &pNode->offset);
        break;
      case PHY_INTERVAL_CODE_SLIDING:
        code = tlvDecodeI64(pTlv, &pNode->sliding);
        break;
      case PHY_INTERVAL_CODE_INTERVAL_UNIT:
        code = tlvDecodeI8(pTlv, &pNode->intervalUnit);
        break;
      case PHY_INTERVAL_CODE_SLIDING_UNIT:
        code = tlvDecodeI8(pTlv, &pNode->slidingUnit);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  PHY_FILL_CODE_BASE_NODE = 1,
  PHY_FILL_CODE_MODE,
  PHY_FILL_CODE_FILL_EXPRS,
  PHY_FILL_CODE_NOT_FILL_EXPRS,
  PHY_FILL_CODE_WSTART,
  PHY_FILL_CODE_VALUES,
  PHY_FILL_CODE_TIME_RANGE,
  PHY_FILL_CODE_INPUT_TS_ORDER
};

static int32_t physiFillNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SFillPhysiNode* pNode = (const SFillPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_FILL_CODE_BASE_NODE, physiNodeToMsg, &pNode->node);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeEnum(pEncoder, PHY_FILL_CODE_MODE, pNode->mode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_FILL_CODE_FILL_EXPRS, nodeListToMsg, pNode->pFillExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_FILL_CODE_NOT_FILL_EXPRS, nodeListToMsg, pNode->pNotFillExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_FILL_CODE_WSTART, nodeToMsg, pNode->pWStartTs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_FILL_CODE_VALUES, nodeToMsg, pNode->pValues);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_FILL_CODE_TIME_RANGE, timeWindowToMsg, &pNode->timeRange);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeEnum(pEncoder, PHY_FILL_CODE_INPUT_TS_ORDER, pNode->inputTsOrder);
  }

  return code;
}

static int32_t msgToPhysiFillNode(STlvDecoder* pDecoder, void* pObj) {
  SFillPhysiNode* pNode = (SFillPhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_FILL_CODE_BASE_NODE:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiNode, &pNode->node);
        break;
      case PHY_FILL_CODE_MODE:
        code = tlvDecodeEnum(pTlv, &pNode->mode, sizeof(pNode->mode));
        break;
      case PHY_FILL_CODE_FILL_EXPRS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pFillExprs);
        break;
      case PHY_FILL_CODE_NOT_FILL_EXPRS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pNotFillExprs);
        break;
      case PHY_FILL_CODE_WSTART:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pWStartTs);
        break;
      case PHY_FILL_CODE_VALUES:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pValues);
        break;
      case PHY_FILL_CODE_TIME_RANGE:
        code = tlvDecodeObjFromTlv(pTlv, msgToTimeWindow, (void**)&pNode->timeRange);
        break;
      case PHY_FILL_CODE_INPUT_TS_ORDER:
        code = tlvDecodeEnum(pTlv, &pNode->inputTsOrder, sizeof(pNode->inputTsOrder));
        break;
      default:
        break;
    }
  }

  return code;
}

enum { PHY_SESSION_CODE_WINDOW = 1, PHY_SESSION_CODE_GAP };

static int32_t physiSessionWindowNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SSessionWinodwPhysiNode* pNode = (const SSessionWinodwPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_SESSION_CODE_WINDOW, physiWindowNodeToMsg, &pNode->window);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI64(pEncoder, PHY_SESSION_CODE_GAP, pNode->gap);
  }

  return code;
}

static int32_t msgToPhysiSessionWindowNode(STlvDecoder* pDecoder, void* pObj) {
  SSessionWinodwPhysiNode* pNode = (SSessionWinodwPhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_SESSION_CODE_WINDOW:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiWindowNode, &pNode->window);
        break;
      case PHY_SESSION_CODE_GAP:
        code = tlvDecodeI64(pTlv, &pNode->gap);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { PHY_STATE_CODE_WINDOW = 1, PHY_STATE_CODE_KEY };

static int32_t physiStateWindowNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SStateWinodwPhysiNode* pNode = (const SStateWinodwPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_STATE_CODE_WINDOW, physiWindowNodeToMsg, &pNode->window);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_STATE_CODE_KEY, nodeToMsg, pNode->pStateKey);
  }

  return code;
}

static int32_t msgToPhysiStateWindowNode(STlvDecoder* pDecoder, void* pObj) {
  SStateWinodwPhysiNode* pNode = (SStateWinodwPhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_STATE_CODE_WINDOW:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiWindowNode, &pNode->window);
        break;
      case PHY_STATE_CODE_KEY:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pStateKey);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { PHY_PARTITION_CODE_BASE_NODE = 1, PHY_PARTITION_CODE_EXPR, PHY_PARTITION_CODE_KEYS, PHY_PARTITION_CODE_TARGETS };

static int32_t physiPartitionNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SPartitionPhysiNode* pNode = (const SPartitionPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_PARTITION_CODE_BASE_NODE, physiNodeToMsg, &pNode->node);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_PARTITION_CODE_EXPR, nodeListToMsg, pNode->pExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_PARTITION_CODE_KEYS, nodeListToMsg, pNode->pPartitionKeys);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_PARTITION_CODE_TARGETS, nodeListToMsg, pNode->pTargets);
  }

  return code;
}

static int32_t msgToPhysiPartitionNode(STlvDecoder* pDecoder, void* pObj) {
  SPartitionPhysiNode* pNode = (SPartitionPhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_PARTITION_CODE_BASE_NODE:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiNode, &pNode->node);
        break;
      case PHY_PARTITION_CODE_EXPR:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pExprs);
        break;
      case PHY_PARTITION_CODE_KEYS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pPartitionKeys);
        break;
      case PHY_PARTITION_CODE_TARGETS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pTargets);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { PHY_INDEF_ROWS_FUNC_CODE_BASE_NODE = 1, PHY_INDEF_ROWS_FUNC_CODE_EXPRS, PHY_INDEF_ROWS_FUNC_CODE_FUNCS };

static int32_t physiIndefRowsFuncNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SIndefRowsFuncPhysiNode* pNode = (const SIndefRowsFuncPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_INDEF_ROWS_FUNC_CODE_BASE_NODE, physiNodeToMsg, &pNode->node);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_INDEF_ROWS_FUNC_CODE_EXPRS, nodeListToMsg, pNode->pExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_INDEF_ROWS_FUNC_CODE_FUNCS, nodeListToMsg, pNode->pFuncs);
  }

  return code;
}

static int32_t msgToPhysiIndefRowsFuncNode(STlvDecoder* pDecoder, void* pObj) {
  SIndefRowsFuncPhysiNode* pNode = (SIndefRowsFuncPhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_INDEF_ROWS_FUNC_CODE_BASE_NODE:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiNode, &pNode->node);
        break;
      case PHY_INDEF_ROWS_FUNC_CODE_EXPRS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pExprs);
        break;
      case PHY_INDEF_ROWS_FUNC_CODE_FUNCS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pFuncs);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  PHY_INERP_FUNC_CODE_BASE_NODE = 1,
  PHY_INERP_FUNC_CODE_EXPR,
  PHY_INERP_FUNC_CODE_FUNCS,
  PHY_INERP_FUNC_CODE_TIME_RANGE,
  PHY_INERP_FUNC_CODE_INTERVAL,
  PHY_INERP_FUNC_CODE_INTERVAL_UNIT,
  PHY_INERP_FUNC_CODE_FILL_MODE,
  PHY_INERP_FUNC_CODE_FILL_VALUES,
  PHY_INERP_FUNC_CODE_TIME_SERIES
};

static int32_t physiInterpFuncNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SInterpFuncPhysiNode* pNode = (const SInterpFuncPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_INERP_FUNC_CODE_BASE_NODE, physiNodeToMsg, &pNode->node);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_INERP_FUNC_CODE_EXPR, nodeListToMsg, pNode->pExprs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_INERP_FUNC_CODE_FUNCS, nodeListToMsg, pNode->pFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_INERP_FUNC_CODE_TIME_RANGE, timeWindowToMsg, &pNode->timeRange);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI64(pEncoder, PHY_INERP_FUNC_CODE_INTERVAL, pNode->interval);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI8(pEncoder, PHY_INERP_FUNC_CODE_INTERVAL_UNIT, pNode->intervalUnit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeEnum(pEncoder, PHY_INERP_FUNC_CODE_FILL_MODE, pNode->fillMode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_INERP_FUNC_CODE_FILL_VALUES, nodeToMsg, pNode->pFillValues);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_INERP_FUNC_CODE_TIME_SERIES, nodeToMsg, pNode->pTimeSeries);
  }

  return code;
}

static int32_t msgToPhysiInterpFuncNode(STlvDecoder* pDecoder, void* pObj) {
  SInterpFuncPhysiNode* pNode = (SInterpFuncPhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_INERP_FUNC_CODE_BASE_NODE:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiNode, &pNode->node);
        break;
      case PHY_INERP_FUNC_CODE_EXPR:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pExprs);
        break;
      case PHY_INERP_FUNC_CODE_FUNCS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pFuncs);
        break;
      case PHY_INERP_FUNC_CODE_TIME_RANGE:
        code = tlvDecodeObjFromTlv(pTlv, msgToTimeWindow, &pNode->timeRange);
        break;
      case PHY_INERP_FUNC_CODE_INTERVAL:
        code = tlvDecodeI64(pTlv, &pNode->interval);
        break;
      case PHY_INERP_FUNC_CODE_INTERVAL_UNIT:
        code = tlvDecodeI8(pTlv, &pNode->intervalUnit);
        break;
      case PHY_INERP_FUNC_CODE_FILL_MODE:
        code = tlvDecodeEnum(pTlv, &pNode->fillMode, sizeof(pNode->fillMode));
        break;
      case PHY_INERP_FUNC_CODE_FILL_VALUES:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pFillValues);
        break;
      case PHY_INERP_FUNC_CODE_TIME_SERIES:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pTimeSeries);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { PHY_DATA_SINK_CODE_INPUT_DESC = 1 };

static int32_t physicDataSinkNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SDataSinkNode* pNode = (const SDataSinkNode*)pObj;
  return tlvEncodeObj(pEncoder, PHY_DATA_SINK_CODE_INPUT_DESC, nodeToMsg, pNode->pInputDataBlockDesc);
}

static int32_t msgToPhysicDataSinkNode(STlvDecoder* pDecoder, void* pObj) {
  SDataSinkNode* pNode = (SDataSinkNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_DATA_SINK_CODE_INPUT_DESC:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pInputDataBlockDesc);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { PHY_DISPATCH_CODE_SINK = 1 };

static int32_t physiDispatchNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SDataDispatcherNode* pNode = (const SDataDispatcherNode*)pObj;
  return tlvEncodeObj(pEncoder, PHY_DISPATCH_CODE_SINK, physicDataSinkNodeToMsg, &pNode->sink);
}

static int32_t msgToPhysiDispatchNode(STlvDecoder* pDecoder, void* pObj) {
  SDataDispatcherNode* pNode = (SDataDispatcherNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_DISPATCH_CODE_SINK:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysicDataSinkNode, &pNode->sink);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  PHY_QUERY_INSERT_CODE_SINK = 1,
  PHY_QUERY_INSERT_CODE_COLS,
  PHY_QUERY_INSERT_CODE_TABLE_ID,
  PHY_QUERY_INSERT_CODE_STABLE_ID,
  PHY_QUERY_INSERT_CODE_TABLE_TYPE,
  PHY_QUERY_INSERT_CODE_TABLE_NAME,
  PHY_QUERY_INSERT_CODE_VG_ID,
  PHY_QUERY_INSERT_CODE_EP_SET
};

static int32_t physiQueryInsertNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SQueryInserterNode* pNode = (const SQueryInserterNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_QUERY_INSERT_CODE_SINK, physicDataSinkNodeToMsg, &pNode->sink);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_QUERY_INSERT_CODE_COLS, nodeListToMsg, pNode->pCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeU64(pEncoder, PHY_QUERY_INSERT_CODE_TABLE_ID, pNode->tableId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeU64(pEncoder, PHY_QUERY_INSERT_CODE_STABLE_ID, pNode->stableId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI8(pEncoder, PHY_QUERY_INSERT_CODE_TABLE_TYPE, pNode->tableType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeCStr(pEncoder, PHY_QUERY_INSERT_CODE_TABLE_NAME, pNode->tableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, PHY_QUERY_INSERT_CODE_VG_ID, pNode->vgId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_QUERY_INSERT_CODE_EP_SET, epSetToMsg, &pNode->epSet);
  }

  return code;
}

static int32_t msgToPhysiQueryInsertNode(STlvDecoder* pDecoder, void* pObj) {
  SQueryInserterNode* pNode = (SQueryInserterNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_QUERY_INSERT_CODE_SINK:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysicDataSinkNode, &pNode->sink);
        break;
      case PHY_QUERY_INSERT_CODE_COLS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pCols);
        break;
      case PHY_QUERY_INSERT_CODE_TABLE_ID:
        code = tlvDecodeU64(pTlv, &pNode->tableId);
        break;
      case PHY_QUERY_INSERT_CODE_STABLE_ID:
        code = tlvDecodeU64(pTlv, &pNode->stableId);
        break;
      case PHY_QUERY_INSERT_CODE_TABLE_TYPE:
        code = tlvDecodeI8(pTlv, &pNode->tableType);
        break;
      case PHY_QUERY_INSERT_CODE_TABLE_NAME:
        code = tlvDecodeCStr(pTlv, pNode->tableName);
        break;
      case PHY_QUERY_INSERT_CODE_VG_ID:
        code = tlvDecodeI32(pTlv, &pNode->vgId);
        break;
      case PHY_QUERY_INSERT_CODE_EP_SET:
        code = tlvDecodeObjFromTlv(pTlv, msgToEpSet, &pNode->epSet);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  PHY_DELETER_CODE_SINK = 1,
  PHY_DELETER_CODE_TABLE_ID,
  PHY_DELETER_CODE_TABLE_TYPE,
  PHY_DELETER_CODE_TABLE_FNAME,
  PHY_DELETER_CODE_TS_COL_NAME,
  PHY_DELETER_CODE_DELETE_TIME_RANGE,
  PHY_DELETER_CODE_AFFECTED_ROWS
};

static int32_t physiDeleteNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SDataDeleterNode* pNode = (const SDataDeleterNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_DELETER_CODE_SINK, physicDataSinkNodeToMsg, &pNode->sink);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeU64(pEncoder, PHY_DELETER_CODE_TABLE_ID, pNode->tableId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI8(pEncoder, PHY_DELETER_CODE_TABLE_TYPE, pNode->tableType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeCStr(pEncoder, PHY_DELETER_CODE_TABLE_FNAME, pNode->tableFName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeCStr(pEncoder, PHY_DELETER_CODE_TS_COL_NAME, pNode->tsColName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_DELETER_CODE_DELETE_TIME_RANGE, timeWindowToMsg, &pNode->deleteTimeRange);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_DELETER_CODE_AFFECTED_ROWS, nodeToMsg, pNode->pAffectedRows);
  }

  return code;
}

static int32_t msgToPhysiDeleteNode(STlvDecoder* pDecoder, void* pObj) {
  SDataDeleterNode* pNode = (SDataDeleterNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_DELETER_CODE_SINK:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysicDataSinkNode, &pNode->sink);
        break;
      case PHY_DELETER_CODE_TABLE_ID:
        code = tlvDecodeU64(pTlv, &pNode->tableId);
        break;
      case PHY_DELETER_CODE_TABLE_TYPE:
        code = tlvDecodeI8(pTlv, &pNode->tableType);
        break;
      case PHY_DELETER_CODE_TABLE_FNAME:
        code = tlvDecodeCStr(pTlv, pNode->tableFName);
        break;
      case PHY_DELETER_CODE_TS_COL_NAME:
        code = tlvDecodeCStr(pTlv, pNode->tsColName);
        break;
      case PHY_DELETER_CODE_DELETE_TIME_RANGE:
        code = tlvDecodeObjFromTlv(pTlv, msgToTimeWindow, &pNode->deleteTimeRange);
        break;
      case PHY_DELETER_CODE_AFFECTED_ROWS:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pAffectedRows);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { SUBPLAN_ID_CODE_QUERY_ID = 1, SUBPLAN_ID_CODE_GROUP_ID, SUBPLAN_ID_CODE_SUBPLAN_ID };

static int32_t subplanIdToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SSubplanId* pNode = (const SSubplanId*)pObj;

  int32_t code = tlvEncodeU64(pEncoder, SUBPLAN_ID_CODE_QUERY_ID, pNode->queryId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, SUBPLAN_ID_CODE_GROUP_ID, pNode->groupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, SUBPLAN_ID_CODE_SUBPLAN_ID, pNode->subplanId);
  }

  return code;
}

static int32_t msgToSubplanId(STlvDecoder* pDecoder, void* pObj) {
  SSubplanId* pNode = (SSubplanId*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case SUBPLAN_ID_CODE_QUERY_ID:
        code = tlvDecodeU64(pTlv, &pNode->queryId);
        break;
      case SUBPLAN_ID_CODE_GROUP_ID:
        code = tlvDecodeI32(pTlv, &pNode->groupId);
        break;
      case SUBPLAN_ID_CODE_SUBPLAN_ID:
        code = tlvDecodeI32(pTlv, &pNode->subplanId);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { QUERY_NODE_ADDR_CODE_NODE_ID = 1, QUERY_NODE_ADDR_CODE_EP_SET };

static int32_t queryNodeAddrToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SQueryNodeAddr* pNode = (const SQueryNodeAddr*)pObj;

  int32_t code = tlvEncodeI32(pEncoder, QUERY_NODE_ADDR_CODE_NODE_ID, pNode->nodeId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, QUERY_NODE_ADDR_CODE_EP_SET, epSetToMsg, &pNode->epSet);
  }

  return code;
}

static int32_t msgToQueryNodeAddr(STlvDecoder* pDecoder, void* pObj) {
  SQueryNodeAddr* pNode = (SQueryNodeAddr*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case QUERY_NODE_ADDR_CODE_NODE_ID:
        code = tlvDecodeI32(pTlv, &pNode->nodeId);
        break;
      case QUERY_NODE_ADDR_CODE_EP_SET:
        code = tlvDecodeObjFromTlv(pTlv, msgToEpSet, &pNode->epSet);
        break;
    }
  }

  return code;
}

enum {
  SUBPLAN_CODE_SUBPLAN_ID = 1,
  SUBPLAN_CODE_SUBPLAN_TYPE,
  SUBPLAN_CODE_MSG_TYPE,
  SUBPLAN_CODE_LEVEL,
  SUBPLAN_CODE_DBFNAME,
  SUBPLAN_CODE_USER,
  SUBPLAN_CODE_EXECNODE,
  SUBPLAN_CODE_ROOT_NODE,
  SUBPLAN_CODE_DATA_SINK,
  SUBPLAN_CODE_TAG_COND,
  SUBPLAN_CODE_TAG_INDEX_COND
};

static int32_t subplanToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SSubplan* pNode = (const SSubplan*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, SUBPLAN_CODE_SUBPLAN_ID, subplanIdToMsg, &pNode->id);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeEnum(pEncoder, SUBPLAN_CODE_SUBPLAN_TYPE, pNode->subplanType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, SUBPLAN_CODE_MSG_TYPE, pNode->msgType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, SUBPLAN_CODE_LEVEL, pNode->level);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeCStr(pEncoder, SUBPLAN_CODE_DBFNAME, pNode->dbFName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeCStr(pEncoder, SUBPLAN_CODE_USER, pNode->user);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, SUBPLAN_CODE_EXECNODE, queryNodeAddrToMsg, &pNode->execNode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, SUBPLAN_CODE_ROOT_NODE, nodeToMsg, pNode->pNode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, SUBPLAN_CODE_DATA_SINK, nodeToMsg, pNode->pDataSink);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, SUBPLAN_CODE_TAG_COND, nodeToMsg, pNode->pTagCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, SUBPLAN_CODE_TAG_INDEX_COND, nodeToMsg, pNode->pTagIndexCond);
  }

  return code;
}

static int32_t msgToSubplan(STlvDecoder* pDecoder, void* pObj) {
  SSubplan* pNode = (SSubplan*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case SUBPLAN_CODE_SUBPLAN_ID:
        code = tlvDecodeObjFromTlv(pTlv, msgToSubplanId, &pNode->id);
        break;
      case SUBPLAN_CODE_SUBPLAN_TYPE:
        code = tlvDecodeEnum(pTlv, &pNode->subplanType, sizeof(pNode->subplanType));
        break;
      case SUBPLAN_CODE_MSG_TYPE:
        code = tlvDecodeI32(pTlv, &pNode->msgType);
        break;
      case SUBPLAN_CODE_LEVEL:
        code = tlvDecodeI32(pTlv, &pNode->level);
        break;
      case SUBPLAN_CODE_DBFNAME:
        code = tlvDecodeCStr(pTlv, pNode->dbFName);
        break;
      case SUBPLAN_CODE_USER:
        code = tlvDecodeCStr(pTlv, pNode->user);
        break;
      case SUBPLAN_CODE_EXECNODE:
        code = tlvDecodeObjFromTlv(pTlv, msgToQueryNodeAddr, &pNode->execNode);
        break;
      case SUBPLAN_CODE_ROOT_NODE:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pNode);
        break;
      case SUBPLAN_CODE_DATA_SINK:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pDataSink);
        break;
      case SUBPLAN_CODE_TAG_COND:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pTagCond);
        break;
      case SUBPLAN_CODE_TAG_INDEX_COND:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pTagIndexCond);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { QUERY_PLAN_CODE_QUERY_ID = 1, QUERY_PLAN_CODE_NUM_OF_SUBPLANS, QUERY_PLAN_CODE_SUBPLANS };

static int32_t queryPlanToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SQueryPlan* pNode = (const SQueryPlan*)pObj;

  int32_t code = tlvEncodeU64(pEncoder, QUERY_PLAN_CODE_QUERY_ID, pNode->queryId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, QUERY_PLAN_CODE_NUM_OF_SUBPLANS, pNode->numOfSubplans);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, QUERY_PLAN_CODE_SUBPLANS, nodeListToMsg, pNode->pSubplans);
  }

  return code;
}

static int32_t msgToQueryPlan(STlvDecoder* pDecoder, void* pObj) {
  SQueryPlan* pNode = (SQueryPlan*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case QUERY_PLAN_CODE_QUERY_ID:
        code = tlvDecodeU64(pTlv, &pNode->queryId);
        break;
      case QUERY_PLAN_CODE_NUM_OF_SUBPLANS:
        code = tlvDecodeI32(pTlv, &pNode->numOfSubplans);
        break;
      case QUERY_PLAN_CODE_SUBPLANS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pSubplans);
        break;
      default:
        break;
    }
  }

  return code;
}

static int32_t specificNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pObj)) {
    case QUERY_NODE_COLUMN:
      code = columnNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_VALUE:
      code = valueNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_OPERATOR:
      code = operatorNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_LOGIC_CONDITION:
      code = logicConditionNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_FUNCTION:
      code = functionNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_ORDER_BY_EXPR:
      code = orderByExprNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_LIMIT:
      code = limitNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_NODE_LIST:
      code = nodeListNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_TARGET:
      code = targetNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_DATABLOCK_DESC:
      code = dataBlockDescNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_SLOT_DESC:
      code = slotDescNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_LEFT_VALUE:
      break;
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN:
      code = physiScanNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN:
      code = physiLastRowScanNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN:
      code = physiTableScanNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN:
      code = physiSysTableScanNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT:
      code = physiProjectNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN:
      code = physiJoinNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_HASH_AGG:
      code = physiAggNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_EXCHANGE:
      code = physiExchangeNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_MERGE:
      code = physiMergeNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_SORT:
    case QUERY_NODE_PHYSICAL_PLAN_GROUP_SORT:
      code = physiSortNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL:
      code = physiIntervalNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_FILL:
      code = physiFillNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_SESSION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_SESSION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION:
      code = physiSessionWindowNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE:
      code = physiStateWindowNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_PARTITION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION:
      code = physiPartitionNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_INDEF_ROWS_FUNC:
      code = physiIndefRowsFuncNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC:
      code = physiInterpFuncNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_DISPATCH:
      code = physiDispatchNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_QUERY_INSERT:
      code = physiQueryInsertNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_DELETE:
      code = physiDeleteNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_SUBPLAN:
      code = subplanToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN:
      code = queryPlanToMsg(pObj, pEncoder);
      break;
    default:
      nodesWarn("specificNodeToMsg unknown node = %s", nodesNodeName(nodeType(pObj)));
      break;
  }
  if (TSDB_CODE_SUCCESS != code) {
    nodesError("specificNodeToMsg error node = %s", nodesNodeName(nodeType(pObj)));
  }
  return code;
}

static int32_t msgToSpecificNode(STlvDecoder* pDecoder, void* pObj) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pObj)) {
    case QUERY_NODE_COLUMN:
      code = msgToColumnNode(pDecoder, pObj);
      break;
    case QUERY_NODE_VALUE:
      code = msgToValueNode(pDecoder, pObj);
      break;
    case QUERY_NODE_OPERATOR:
      code = msgToOperatorNode(pDecoder, pObj);
      break;
    case QUERY_NODE_LOGIC_CONDITION:
      code = msgToLogicConditionNode(pDecoder, pObj);
      break;
    case QUERY_NODE_FUNCTION:
      code = msgToFunctionNode(pDecoder, pObj);
      break;
    case QUERY_NODE_ORDER_BY_EXPR:
      code = msgToOrderByExprNode(pDecoder, pObj);
      break;
    case QUERY_NODE_LIMIT:
      code = msgToLimitNode(pDecoder, pObj);
      break;
    case QUERY_NODE_NODE_LIST:
      code = msgToNodeListNode(pDecoder, pObj);
      break;
    case QUERY_NODE_TARGET:
      code = msgToTargetNode(pDecoder, pObj);
      break;
    case QUERY_NODE_DATABLOCK_DESC:
      code = msgToDataBlockDescNode(pDecoder, pObj);
      break;
    case QUERY_NODE_SLOT_DESC:
      code = msgToSlotDescNode(pDecoder, pObj);
      break;
    case QUERY_NODE_LEFT_VALUE:
      break;
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN:
      code = msgToPhysiScanNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN:
      code = msgToPhysiLastRowScanNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN:
      code = msgToPhysiTableScanNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN:
      code = msgToPhysiSysTableScanNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT:
      code = msgToPhysiProjectNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN:
      code = msgToPhysiJoinNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_HASH_AGG:
      code = msgToPhysiAggNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_EXCHANGE:
      code = msgToPhysiExchangeNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_MERGE:
      code = msgToPhysiMergeNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_SORT:
    case QUERY_NODE_PHYSICAL_PLAN_GROUP_SORT:
      code = msgToPhysiSortNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL:
      code = msgToPhysiIntervalNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_FILL:
      code = msgToPhysiFillNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_SESSION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_SESSION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION:
      code = msgToPhysiSessionWindowNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE:
      code = msgToPhysiStateWindowNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_PARTITION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION:
      code = msgToPhysiPartitionNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_INDEF_ROWS_FUNC:
      code = msgToPhysiIndefRowsFuncNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC:
      code = msgToPhysiInterpFuncNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_DISPATCH:
      code = msgToPhysiDispatchNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_QUERY_INSERT:
      code = msgToPhysiQueryInsertNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_DELETE:
      code = msgToPhysiDeleteNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_SUBPLAN:
      code = msgToSubplan(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN:
      code = msgToQueryPlan(pDecoder, pObj);
      break;
    default:
      nodesWarn("msgToSpecificNode unknown node = %s", nodesNodeName(nodeType(pObj)));
      break;
  }
  if (TSDB_CODE_SUCCESS != code) {
    nodesError("msgToSpecificNode error node = %s", nodesNodeName(nodeType(pObj)));
  }
  return code;
}

static int32_t nodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  return tlvEncodeObj(pEncoder, nodeType(pObj), specificNodeToMsg, pObj);
}

static int32_t msgToNode(STlvDecoder* pDecoder, void** pObj) {
  return tlvDecodeDynObj(pDecoder, (FMakeObject)nodesMakeNode, msgToSpecificNode, pObj);
}

static int32_t msgToNodeFromTlv(STlv* pTlv, void** pObj) {
  STlvDecoder decoder = {.bufSize = pTlv->len, .offset = 0, .pBuf = pTlv->value};
  return msgToNode(&decoder, pObj);
}

static int32_t nodeListToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SNodeList* pList = (const SNodeList*)pObj;

  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    int32_t code = nodeToMsg(pNode, pEncoder);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t msgToNodeList(STlvDecoder* pDecoder, void** pObj) {
  SNodeList* pList = nodesMakeList();

  int32_t code = TSDB_CODE_SUCCESS;
  while (TSDB_CODE_SUCCESS == code && !tlvDecodeEnd(pDecoder)) {
    SNode* pNode = NULL;
    code = msgToNode(pDecoder, (void**)&pNode);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListAppend(pList, pNode);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    *pObj = pList;
  } else {
    nodesDestroyList(pList);
  }
  return code;
}

static int32_t msgToNodeListFromTlv(STlv* pTlv, void** pObj) {
  STlvDecoder decoder = {.bufSize = pTlv->len, .offset = 0, .pBuf = pTlv->value};
  return msgToNodeList(&decoder, pObj);
}

int32_t nodesNodeToMsg(const SNode* pNode, char** pMsg, int32_t* pLen) {
  if (NULL == pNode || NULL == pMsg || NULL == pLen) {
    terrno = TSDB_CODE_FAILED;
    return TSDB_CODE_FAILED;
  }

  STlvEncoder encoder;
  int32_t     code = initTlvEncoder(&encoder);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodeToMsg(pNode, &encoder);
  }
  if (TSDB_CODE_SUCCESS == code) {
    endTlvEncode(&encoder, pMsg, pLen);
  }
  clearTlvEncoder(&encoder);

  terrno = code;
  return code;
}

int32_t nodesMsgToNode(const char* pMsg, int32_t len, SNode** pNode) {
  if (NULL == pMsg || NULL == pNode) {
    return TSDB_CODE_SUCCESS;
  }

  STlvDecoder decoder = {.bufSize = len, .offset = 0, .pBuf = pMsg};
  int32_t     code = msgToNode(&decoder, (void**)pNode);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(*pNode);
    *pNode = NULL;
  }

  terrno = code;
  return code;
}
