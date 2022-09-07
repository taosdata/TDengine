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
  if (pEncoder->offset + len > pEncoder->allocSize) {
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
  pEncoder->offset += sizeof(STlv) + pTlv->len;
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
      code = tlvEncodeObj(pEncoder, 0, func, pArray + i * itemSize);
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
  tlvForEach(pDecoder, pTlv, code) { code = tlvDecodeObjFromTlv(pTlv, func, pArray + itemSize * i++); }
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

enum { PHY_DISPATCH_CODE_BASE_CODE = 1 };

static int32_t physiDispatchNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SDataDispatcherNode* pNode = (const SDataDispatcherNode*)pObj;
  return tlvEncodeObj(pEncoder, PHY_DISPATCH_CODE_BASE_CODE, physicDataSinkNodeToMsg, &pNode->sink);
}

static int32_t msgToPhysiDispatchNode(STlvDecoder* pDecoder, void* pObj) {
  SDataDispatcherNode* pNode = (SDataDispatcherNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_DISPATCH_CODE_BASE_CODE:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysicDataSinkNode, &pNode->sink);
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

enum {
  QUERY_NODE_ADDR_CODE_NODE_ID = 1,
  QUERY_NODE_ADDR_CODE_IN_USE,
  QUERY_NODE_ADDR_CODE_NUM_OF_EPS,
  QUERY_NODE_ADDR_CODE_EPS
};

static int32_t queryNodeAddrToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SQueryNodeAddr* pNode = (const SQueryNodeAddr*)pObj;

  int32_t code = tlvEncodeI32(pEncoder, QUERY_NODE_ADDR_CODE_NODE_ID, pNode->nodeId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI8(pEncoder, QUERY_NODE_ADDR_CODE_IN_USE, pNode->epSet.inUse);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI8(pEncoder, QUERY_NODE_ADDR_CODE_NUM_OF_EPS, pNode->epSet.numOfEps);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObjArray(pEncoder, QUERY_NODE_ADDR_CODE_EPS, epToMsg, pNode->epSet.eps, sizeof(SEp),
                             pNode->epSet.numOfEps);
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
      case QUERY_NODE_ADDR_CODE_IN_USE:
        code = tlvDecodeI8(pTlv, &pNode->epSet.inUse);
        break;
      case QUERY_NODE_ADDR_CODE_NUM_OF_EPS:
        code = tlvDecodeI8(pTlv, &pNode->epSet.numOfEps);
        break;
      case QUERY_NODE_ADDR_CODE_EPS:
        code = tlvDecodeObjArrayFromTlv(pTlv, msgToEp, pNode->epSet.eps, sizeof(SEp));
        break;
      default:
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
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:
      code = physiTableScanNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_DISPATCH:
      code = physiDispatchNodeToMsg(pObj, pEncoder);
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
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:
      code = msgToPhysiTableScanNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_DISPATCH:
      code = msgToPhysiDispatchNode(pDecoder, pObj);
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
