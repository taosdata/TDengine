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

#ifndef htonll

#define htonll(x)                                                                                   \
  (((int64_t)x & 0x00000000000000ff) << 7 * 8) | (((int64_t)x & 0x000000000000ff00) << 5 * 8) |     \
      (((int64_t)x & 0x0000000000ff0000) << 3 * 8) | (((int64_t)x & 0x00000000ff000000) << 1 * 8) | \
      (((int64_t)x & 0x000000ff00000000) >> 1 * 8) | (((int64_t)x & 0x0000ff0000000000) >> 3 * 8) | \
      (((int64_t)x & 0x00ff000000000000) >> 5 * 8) | (((int64_t)x & 0xff00000000000000) >> 7 * 8)

#define ntohll(x) htonll(x)

#endif

#define NODES_MSG_DEFAULT_LEN 1024
#define TLV_TYPE_ARRAY_ELEM   0

#define tlvForEach(pDecoder, pTlv, code) \
  while (TSDB_CODE_SUCCESS == code && TSDB_CODE_SUCCESS == (code = tlvGetNextTlv(pDecoder, &pTlv)) && NULL != pTlv)

#pragma pack(push, 1)

typedef struct STlv {
  int16_t type;
  int32_t len;
  char    value[0];
} STlv;

#pragma pack(pop)

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
static int32_t SArrayToMsg(const void* pObj, STlvEncoder* pEncoder);

static int32_t msgToNode(STlvDecoder* pDecoder, void** pObj);
static int32_t msgToNodeFromTlv(STlv* pTlv, void** pObj);
static int32_t msgToNodeList(STlvDecoder* pDecoder, void** pObj);
static int32_t msgToNodeListFromTlv(STlv* pTlv, void** pObj);
static int32_t msgToSArray(STlv* pTlv, void** pObj);


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
}

static int32_t tlvEncodeImpl(STlvEncoder* pEncoder, int16_t type, const void* pValue, int32_t len) {
  int32_t tlvLen = sizeof(STlv) + len;
  if (pEncoder->offset + tlvLen > pEncoder->allocSize) {
    pEncoder->allocSize = TMAX(pEncoder->allocSize * 2, pEncoder->allocSize + tlvLen);
    void* pNewBuf = taosMemoryRealloc(pEncoder->pBuf, pEncoder->allocSize);
    if (NULL == pNewBuf) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pEncoder->pBuf = pNewBuf;
  }
  STlv* pTlv = (STlv*)(pEncoder->pBuf + pEncoder->offset);
  pTlv->type = htons(type);
  pTlv->len = htonl(len);
  memcpy(pTlv->value, pValue, len);
  pEncoder->offset += tlvLen;
  ++(pEncoder->tlvCount);
  return TSDB_CODE_SUCCESS;
}

static int32_t tlvEncodeValueImpl(STlvEncoder* pEncoder, const void* pValue, int32_t len) {
  if (pEncoder->offset + len > pEncoder->allocSize) {
    void* pNewBuf = taosMemoryRealloc(pEncoder->pBuf, pEncoder->allocSize * 2);
    if (NULL == pNewBuf) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pEncoder->pBuf = pNewBuf;
    pEncoder->allocSize = pEncoder->allocSize * 2;
  }
  memcpy(pEncoder->pBuf + pEncoder->offset, pValue, len);
  pEncoder->offset += len;
  return TSDB_CODE_SUCCESS;
}

static int32_t tlvEncodeI8(STlvEncoder* pEncoder, int16_t type, int8_t value) {
  return tlvEncodeImpl(pEncoder, type, &value, sizeof(value));
}

static int32_t tlvEncodeValueI8(STlvEncoder* pEncoder, int8_t value) {
  return tlvEncodeValueImpl(pEncoder, &value, sizeof(value));
}

static int32_t tlvEncodeI16(STlvEncoder* pEncoder, int16_t type, int16_t value) {
  value = htons(value);
  return tlvEncodeImpl(pEncoder, type, &value, sizeof(value));
}

static int32_t tlvEncodeValueI16(STlvEncoder* pEncoder, int16_t value) {
  value = htons(value);
  return tlvEncodeValueImpl(pEncoder, &value, sizeof(value));
}

static int32_t tlvEncodeI32(STlvEncoder* pEncoder, int16_t type, int32_t value) {
  value = htonl(value);
  return tlvEncodeImpl(pEncoder, type, &value, sizeof(value));
}

static int32_t tlvEncodeValueI32(STlvEncoder* pEncoder, int32_t value) {
  value = htonl(value);
  return tlvEncodeValueImpl(pEncoder, &value, sizeof(value));
}

static int32_t tlvEncodeI64(STlvEncoder* pEncoder, int16_t type, int64_t value) {
  value = htonll(value);
  return tlvEncodeImpl(pEncoder, type, &value, sizeof(value));
}

static int32_t tlvEncodeValueI64(STlvEncoder* pEncoder, int64_t value) {
  value = htonll(value);
  return tlvEncodeValueImpl(pEncoder, &value, sizeof(value));
}

static int32_t tlvEncodeU8(STlvEncoder* pEncoder, int16_t type, uint8_t value) {
  return tlvEncodeImpl(pEncoder, type, &value, sizeof(value));
}

static int32_t tlvEncodeValueU8(STlvEncoder* pEncoder, uint8_t value) {
  return tlvEncodeValueImpl(pEncoder, &value, sizeof(value));
}

static int32_t tlvEncodeU16(STlvEncoder* pEncoder, int16_t type, uint16_t value) {
  value = htons(value);
  return tlvEncodeImpl(pEncoder, type, &value, sizeof(value));
}

static int32_t tlvEncodeValueU16(STlvEncoder* pEncoder, uint16_t value) {
  value = htons(value);
  return tlvEncodeValueImpl(pEncoder, &value, sizeof(value));
}

static int32_t tlvEncodeU64(STlvEncoder* pEncoder, int16_t type, uint64_t value) {
  value = htonll(value);
  return tlvEncodeImpl(pEncoder, type, &value, sizeof(value));
}

static int32_t tlvEncodeValueU64(STlvEncoder* pEncoder, uint64_t value) {
  value = htonll(value);
  return tlvEncodeValueImpl(pEncoder, &value, sizeof(value));
}

static int32_t tlvEncodeDouble(STlvEncoder* pEncoder, int16_t type, double value) {
  int64_t temp = *(int64_t*)&value;
  temp = htonll(temp);
  return tlvEncodeImpl(pEncoder, type, &temp, sizeof(temp));
}

static int32_t tlvEncodeValueDouble(STlvEncoder* pEncoder, double value) {
  int64_t temp = *(int64_t*)&value;
  temp = htonll(temp);
  return tlvEncodeValueImpl(pEncoder, &temp, sizeof(temp));
}

static int32_t tlvEncodeEnum(STlvEncoder* pEncoder, int16_t type, int32_t value) {
  value = htonl(value);
  return tlvEncodeImpl(pEncoder, type, &value, sizeof(value));
}

static int32_t tlvEncodeValueEnum(STlvEncoder* pEncoder, int32_t value) {
  value = htonl(value);
  return tlvEncodeValueImpl(pEncoder, &value, sizeof(value));
}

static int32_t tlvEncodeBool(STlvEncoder* pEncoder, int16_t type, int8_t value) {
  return tlvEncodeImpl(pEncoder, type, &value, sizeof(value));
}

static int32_t tlvEncodeValueBool(STlvEncoder* pEncoder, int8_t value) {
  return tlvEncodeValueImpl(pEncoder, &value, sizeof(value));
}

static int32_t tlvEncodeCStr(STlvEncoder* pEncoder, int16_t type, const char* pValue) {
  if (NULL == pValue) {
    return TSDB_CODE_SUCCESS;
  }
  return tlvEncodeImpl(pEncoder, type, pValue, strlen(pValue));
}

static int32_t tlvEncodeValueCStr(STlvEncoder* pEncoder, const char* pValue) {
  int16_t len = strlen(pValue);
  int32_t code = tlvEncodeValueI16(pEncoder, len);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueImpl(pEncoder, pValue, len);
  }
  return code;
}

static int32_t tlvEncodeBinary(STlvEncoder* pEncoder, int16_t type, const void* pValue, int32_t len) {
  return tlvEncodeImpl(pEncoder, type, pValue, len);
}

static int32_t tlvEncodeObj(STlvEncoder* pEncoder, int16_t type, FToMsg func, const void* pObj) {
  if (NULL == pObj) {
    return TSDB_CODE_SUCCESS;
  }

  if (pEncoder->offset + sizeof(STlv) > pEncoder->allocSize) {
    pEncoder->allocSize = TMAX(pEncoder->allocSize * 2, pEncoder->allocSize + sizeof(STlv));
    void* pNewBuf = taosMemoryRealloc(pEncoder->pBuf, pEncoder->allocSize);
    if (NULL == pNewBuf) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pEncoder->pBuf = pNewBuf;
  }

  int32_t start = pEncoder->offset;
  pEncoder->offset += sizeof(STlv);
  int32_t code = func(pObj, pEncoder);
  if (TSDB_CODE_SUCCESS == code) {
    STlv* pTlv = (STlv*)(pEncoder->pBuf + start);
    pTlv->type = htons(type);
    pTlv->len = htonl(pEncoder->offset - start - sizeof(STlv));
  }
  ++(pEncoder->tlvCount);
  return code;
}

static int32_t tlvEncodeObjArray(STlvEncoder* pEncoder, int16_t type, FToMsg func, const void* pArray, int32_t itemSize,
                                 int32_t num) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (num > 0) {
    int32_t start = pEncoder->offset;
    pEncoder->offset += sizeof(STlv);
    for (size_t i = 0; TSDB_CODE_SUCCESS == code && i < num; ++i) {
      code = tlvEncodeObj(pEncoder, TLV_TYPE_ARRAY_ELEM, func, (const char*)pArray + i * itemSize);
    }
    if (TSDB_CODE_SUCCESS == code) {
      STlv* pTlv = (STlv*)(pEncoder->pBuf + start);
      pTlv->type = htons(type);
      pTlv->len = htonl(pEncoder->offset - start - sizeof(STlv));
    }
  }
  return code;
}

static int32_t tlvEncodeValueArray(STlvEncoder* pEncoder, FToMsg func, const void* pArray, int32_t itemSize,
                                   int32_t num) {
  int32_t code = tlvEncodeValueI32(pEncoder, num);
  for (size_t i = 0; TSDB_CODE_SUCCESS == code && i < num; ++i) {
    code = func((const char*)pArray + i * itemSize, pEncoder);
  }
  return code;
}

static int32_t tlvGetNextTlv(STlvDecoder* pDecoder, STlv** pTlv) {
  if (pDecoder->offset == pDecoder->bufSize) {
    *pTlv = NULL;
    return TSDB_CODE_SUCCESS;
  }

  *pTlv = (STlv*)(pDecoder->pBuf + pDecoder->offset);
  (*pTlv)->type = ntohs((*pTlv)->type);
  (*pTlv)->len = ntohl((*pTlv)->len);
  if ((*pTlv)->len + pDecoder->offset > pDecoder->bufSize) {
    return TSDB_CODE_FAILED;
  }
  pDecoder->offset += sizeof(STlv) + (*pTlv)->len;
  return TSDB_CODE_SUCCESS;
}

static bool tlvDecodeEnd(STlvDecoder* pDecoder) { return pDecoder->offset == pDecoder->bufSize; }

static int32_t tlvDecodeImpl(STlv* pTlv, void* pValue, int32_t len) {
  if (pTlv->len != len) {
    return TSDB_CODE_FAILED;
  }
  memcpy(pValue, pTlv->value, len);
  return TSDB_CODE_SUCCESS;
}

static int32_t tlvDecodeValueImpl(STlvDecoder* pDecoder, void* pValue, int32_t len) {
  // compatible with lower version messages
  if (pDecoder->bufSize == pDecoder->offset) {
    memset(pValue, 0, len);
    return TSDB_CODE_SUCCESS;
  }
  if (len > pDecoder->bufSize - pDecoder->offset) {
    return TSDB_CODE_FAILED;
  }
  memcpy(pValue, pDecoder->pBuf + pDecoder->offset, len);
  pDecoder->offset += len;
  return TSDB_CODE_SUCCESS;
}

static int32_t tlvDecodeI8(STlv* pTlv, int8_t* pValue) { return tlvDecodeImpl(pTlv, pValue, sizeof(*pValue)); }

static int32_t tlvDecodeValueI8(STlvDecoder* pDecoder, int8_t* pValue) {
  return tlvDecodeValueImpl(pDecoder, pValue, sizeof(*pValue));
}

static int32_t tlvDecodeI16(STlv* pTlv, int16_t* pValue) {
  int32_t code = tlvDecodeImpl(pTlv, pValue, sizeof(*pValue));
  if (TSDB_CODE_SUCCESS == code) {
    *pValue = ntohs(*pValue);
  }
  return code;
}

static int32_t tlvDecodeValueI16(STlvDecoder* pDecoder, int16_t* pValue) {
  int32_t code = tlvDecodeValueImpl(pDecoder, pValue, sizeof(*pValue));
  if (TSDB_CODE_SUCCESS == code) {
    *pValue = ntohs(*pValue);
  }
  return code;
}

static int32_t tlvDecodeI32(STlv* pTlv, int32_t* pValue) {
  int32_t code = tlvDecodeImpl(pTlv, pValue, sizeof(*pValue));
  if (TSDB_CODE_SUCCESS == code) {
    *pValue = ntohl(*pValue);
  }
  return code;
}

static int32_t tlvDecodeValueI32(STlvDecoder* pDecoder, int32_t* pValue) {
  int32_t code = tlvDecodeValueImpl(pDecoder, pValue, sizeof(*pValue));
  if (TSDB_CODE_SUCCESS == code) {
    *pValue = ntohl(*pValue);
  }
  return code;
}

static int32_t tlvDecodeI64(STlv* pTlv, int64_t* pValue) {
  int32_t code = tlvDecodeImpl(pTlv, pValue, sizeof(*pValue));
  if (TSDB_CODE_SUCCESS == code) {
    *pValue = ntohll(*pValue);
  }
  return code;
}

static int32_t tlvDecodeValueI64(STlvDecoder* pDecoder, int64_t* pValue) {
  int32_t code = tlvDecodeValueImpl(pDecoder, pValue, sizeof(*pValue));
  if (TSDB_CODE_SUCCESS == code) {
    *pValue = ntohll(*pValue);
  }
  return code;
}

static int32_t tlvDecodeU8(STlv* pTlv, uint8_t* pValue) { return tlvDecodeImpl(pTlv, pValue, sizeof(*pValue)); }

static int32_t tlvDecodeValueU8(STlvDecoder* pDecoder, uint8_t* pValue) {
  return tlvDecodeValueImpl(pDecoder, pValue, sizeof(*pValue));
}

static int32_t tlvDecodeU16(STlv* pTlv, uint16_t* pValue) {
  int32_t code = tlvDecodeImpl(pTlv, pValue, sizeof(*pValue));
  if (TSDB_CODE_SUCCESS == code) {
    *pValue = ntohs(*pValue);
  }
  return code;
}

static int32_t tlvDecodeValueU16(STlvDecoder* pDecoder, uint16_t* pValue) {
  int32_t code = tlvDecodeValueImpl(pDecoder, pValue, sizeof(*pValue));
  if (TSDB_CODE_SUCCESS == code) {
    *pValue = ntohs(*pValue);
  }
  return code;
}

static int32_t tlvDecodeU64(STlv* pTlv, uint64_t* pValue) {
  int32_t code = tlvDecodeImpl(pTlv, pValue, sizeof(*pValue));
  if (TSDB_CODE_SUCCESS == code) {
    *pValue = ntohll(*pValue);
  }
  return code;
}

static int32_t tlvDecodeValueU64(STlvDecoder* pDecoder, uint64_t* pValue) {
  int32_t code = tlvDecodeValueImpl(pDecoder, pValue, sizeof(*pValue));
  if (TSDB_CODE_SUCCESS == code) {
    *pValue = ntohll(*pValue);
  }
  return code;
}

static int32_t tlvDecodeDouble(STlv* pTlv, double* pValue) {
  int64_t temp = 0;
  int32_t code = tlvDecodeI64(pTlv, &temp);
  if (TSDB_CODE_SUCCESS == code) {
    *pValue = *(double*)&temp;
  }
  return code;
}

static int32_t tlvDecodeValueDouble(STlvDecoder* pDecoder, double* pValue) {
  int64_t temp = 0;
  int32_t code = tlvDecodeValueI64(pDecoder, &temp);
  if (TSDB_CODE_SUCCESS == code) {
    *pValue = *(double*)&temp;
  }
  return code;
}

static int32_t convertIntegerType(int32_t value, void* pValue, int16_t len) {
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

static int32_t tlvDecodeBool(STlv* pTlv, bool* pValue) {
  int8_t  value = 0;
  int32_t code = tlvDecodeI8(pTlv, &value);
  if (TSDB_CODE_SUCCESS == code) {
    code = convertIntegerType(value, pValue, sizeof(bool));
  }
  return code;
}

static int32_t tlvDecodeValueBool(STlvDecoder* pDecoder, bool* pValue) {
  int8_t  value = 0;
  int32_t code = tlvDecodeValueI8(pDecoder, &value);
  if (TSDB_CODE_SUCCESS == code) {
    code = convertIntegerType(value, pValue, sizeof(bool));
  }
  return code;
}

static int32_t tlvDecodeEnum(STlv* pTlv, void* pValue, int16_t len) {
  int32_t value = 0;
  int32_t code = tlvDecodeI32(pTlv, &value);
  if (TSDB_CODE_SUCCESS == code) {
    code = convertIntegerType(value, pValue, len);
  }
  return code;
}

static int32_t tlvDecodeValueEnum(STlvDecoder* pDecoder, void* pValue, int16_t len) {
  int32_t value = 0;
  int32_t code = tlvDecodeValueI32(pDecoder, &value);
  if (TSDB_CODE_SUCCESS == code) {
    code = convertIntegerType(value, pValue, len);
  }
  return code;
}

static int32_t tlvDecodeCStr(STlv* pTlv, char* pValue, int32_t size) {
  if (pTlv->len > size - 1) {
    return TSDB_CODE_FAILED;
  }
  memcpy(pValue, pTlv->value, pTlv->len);
  return TSDB_CODE_SUCCESS;
}

static int32_t tlvDecodeValueCStr(STlvDecoder* pDecoder, char* pValue) {
  int16_t len = 0;
  int32_t code = tlvDecodeValueI16(pDecoder, &len);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueImpl(pDecoder, pValue, len);
  }
  return code;
}

static int32_t tlvDecodeCStrP(STlv* pTlv, char** pValue) {
  *pValue = strndup(pTlv->value, pTlv->len);
  return NULL == *pValue ? TSDB_CODE_OUT_OF_MEMORY : TSDB_CODE_SUCCESS;
}

static int32_t tlvDecodeDynBinary(STlv* pTlv, void** pValue) {
  *pValue = taosMemoryMalloc(pTlv->len);
  if (NULL == *pValue) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  memcpy(*pValue, pTlv->value, pTlv->len);
  return TSDB_CODE_SUCCESS;
}

static int32_t tlvDecodeBinary(STlv* pTlv, void* pValue) {
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
  tlvForEach(pDecoder, pTlv, code) { code = tlvDecodeObjFromTlv(pTlv, func, (char*)pArray + itemSize * i++); }
  return code;
}

static int32_t tlvDecodeValueArray(STlvDecoder* pDecoder, FToObject func, void* pArray, int32_t itemSize,
                                   int32_t* pNum) {
  int32_t code = tlvDecodeValueI32(pDecoder, pNum);
  for (size_t i = 0; TSDB_CODE_SUCCESS == code && i < *pNum; ++i) {
    code = func(pDecoder, (char*)pArray + i * itemSize);
  }
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

static int32_t dataTypeInlineToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SDataType* pNode = (const SDataType*)pObj;

  int32_t code = tlvEncodeValueI8(pEncoder, pNode->type);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueU8(pEncoder, pNode->precision);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueU8(pEncoder, pNode->scale);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI32(pEncoder, pNode->bytes);
  }

  return code;
}

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

static int32_t msgToDataTypeInline(STlvDecoder* pDecoder, void* pObj) {
  SDataType* pNode = (SDataType*)pObj;

  int32_t code = tlvDecodeValueI8(pDecoder, &pNode->type);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueU8(pDecoder, &pNode->precision);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueU8(pDecoder, &pNode->scale);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI32(pDecoder, &pNode->bytes);
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

enum { COLUMN_CODE_INLINE_ATTRS = 1 };

static int32_t columnNodeInlineToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SColumnNode* pNode = (const SColumnNode*)pObj;

  int32_t code = dataTypeInlineToMsg(&pNode->node.resType, pEncoder);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueU64(pEncoder, pNode->tableId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI8(pEncoder, pNode->tableType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI16(pEncoder, pNode->colId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueEnum(pEncoder, pNode->colType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueCStr(pEncoder, pNode->dbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueCStr(pEncoder, pNode->tableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueCStr(pEncoder, pNode->tableAlias);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueCStr(pEncoder, pNode->colName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI16(pEncoder, pNode->dataBlockId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI16(pEncoder, pNode->slotId);
  }

  return code;
}

static int32_t columnNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  return tlvEncodeObj(pEncoder, COLUMN_CODE_INLINE_ATTRS, columnNodeInlineToMsg, pObj);
}

static int32_t msgToColumnNodeInline(STlvDecoder* pDecoder, void* pObj) {
  SColumnNode* pNode = (SColumnNode*)pObj;

  int32_t code = msgToDataTypeInline(pDecoder, &pNode->node.resType);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueU64(pDecoder, &pNode->tableId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI8(pDecoder, &pNode->tableType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI16(pDecoder, &pNode->colId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueEnum(pDecoder, &pNode->colType, sizeof(pNode->colType));
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueCStr(pDecoder, pNode->dbName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueCStr(pDecoder, pNode->tableName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueCStr(pDecoder, pNode->tableAlias);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueCStr(pDecoder, pNode->colName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI16(pDecoder, &pNode->dataBlockId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI16(pDecoder, &pNode->slotId);
  }

  return code;
}

static int32_t msgToColumnNode(STlvDecoder* pDecoder, void* pObj) {
  SColumnNode* pNode = (SColumnNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case COLUMN_CODE_INLINE_ATTRS:
        code = tlvDecodeObjFromTlv(pTlv, msgToColumnNodeInline, pNode);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  VALUE_CODE_EXPR_BASE = 1,
  VALUE_CODE_LITERAL,
  VALUE_CODE_IS_DURATION,
  VALUE_CODE_TRANSLATE,
  VALUE_CODE_NOT_RESERVED,
  VALUE_CODE_IS_NULL,
  VALUE_CODE_DATUM
};

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
    case TSDB_DATA_TYPE_GEOMETRY:
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
    code = tlvEncodeCStr(pEncoder, VALUE_CODE_LITERAL, pNode->literal);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, VALUE_CODE_IS_DURATION, pNode->isDuration);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, VALUE_CODE_TRANSLATE, pNode->translate);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, VALUE_CODE_NOT_RESERVED, pNode->notReserved);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, VALUE_CODE_IS_NULL, pNode->isNull);
  }
  if (TSDB_CODE_SUCCESS == code && !pNode->isNull) {
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
    case TSDB_DATA_TYPE_GEOMETRY: {
      if (pTlv->len > pNode->node.resType.bytes + VARSTR_HEADER_SIZE) {
        code = TSDB_CODE_FAILED;
        break;
      }
      pNode->datum.p = taosMemoryCalloc(1, pNode->node.resType.bytes + 1);
      if (NULL == pNode->datum.p) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }
      code = tlvDecodeBinary(pTlv, pNode->datum.p);
      if (TSDB_CODE_SUCCESS == code) {
        varDataSetLen(pNode->datum.p, pTlv->len - VARSTR_HEADER_SIZE);
      }
      break;
    }
    case TSDB_DATA_TYPE_JSON: {
      if (pTlv->len <= 0 || pTlv->len > TSDB_MAX_JSON_TAG_LEN) {
        code = TSDB_CODE_FAILED;
        break;
      }
      code = tlvDecodeDynBinary(pTlv, (void**)&pNode->datum.p);
      break;
    }
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
      case VALUE_CODE_LITERAL:
        code = tlvDecodeCStrP(pTlv, &pNode->literal);
        break;
      case VALUE_CODE_IS_DURATION:
        code = tlvDecodeBool(pTlv, &pNode->isDuration);
        break;
      case VALUE_CODE_TRANSLATE:
        code = tlvDecodeBool(pTlv, &pNode->translate);
        break;
      case VALUE_CODE_NOT_RESERVED:
        code = tlvDecodeBool(pTlv, &pNode->notReserved);
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
  FUNCTION_CODE_FUNCTION_NAME,
  FUNCTION_CODE_FUNCTION_ID,
  FUNCTION_CODE_FUNCTION_TYPE,
  FUNCTION_CODE_PARAMETERS,
  FUNCTION_CODE_UDF_BUF_SIZE
};

static int32_t functionNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SFunctionNode* pNode = (const SFunctionNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, FUNCTION_CODE_EXPR_BASE, exprNodeToMsg, pNode);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeCStr(pEncoder, FUNCTION_CODE_FUNCTION_NAME, pNode->functionName);
  }
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
      case FUNCTION_CODE_FUNCTION_NAME:
        code = tlvDecodeCStr(pTlv, pNode->functionName, sizeof(pNode->functionName));
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
        code = tlvDecodeCStr(pTlv, pNode->dbname, sizeof(pNode->dbname));
        break;
      case NAME_CODE_TABLE_NAME:
        code = tlvDecodeCStr(pTlv, pNode->tname, sizeof(pNode->tname));
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

  int32_t code = tlvEncodeObj(pEncoder, NODE_LIST_CODE_DATA_TYPE, dataTypeInlineToMsg, &pNode->node.resType);
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
        code = tlvDecodeObjFromTlv(pTlv, msgToDataTypeInline, &pNode->node.resType);
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

enum { TARGET_CODE_INLINE_ATTRS = 1, TARGET_CODE_EXPR };

static int32_t targetNodeInlineToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const STargetNode* pNode = (const STargetNode*)pObj;

  int32_t code = tlvEncodeValueI16(pEncoder, pNode->dataBlockId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI16(pEncoder, pNode->slotId);
  }

  return code;
}

static int32_t targetNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const STargetNode* pNode = (const STargetNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, TARGET_CODE_INLINE_ATTRS, targetNodeInlineToMsg, pNode);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, TARGET_CODE_EXPR, nodeToMsg, pNode->pExpr);
  }

  return code;
}

static int32_t msgToTargetNodeInline(STlvDecoder* pDecoder, void* pObj) {
  STargetNode* pNode = (STargetNode*)pObj;

  int32_t code = tlvDecodeValueI16(pDecoder, &pNode->dataBlockId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI16(pDecoder, &pNode->slotId);
  }

  return code;
}

static int32_t msgToTargetNode(STlvDecoder* pDecoder, void* pObj) {
  STargetNode* pNode = (STargetNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case TARGET_CODE_INLINE_ATTRS:
        code = tlvDecodeObjFromTlv(pTlv, msgToTargetNodeInline, pNode);
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

enum { DATA_BLOCK_DESC_CODE_INLINE_ATTRS = 1, DATA_BLOCK_DESC_CODE_SLOTS };

static int32_t dataBlockDescNodeInlineToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SDataBlockDescNode* pNode = (const SDataBlockDescNode*)pObj;

  int32_t code = tlvEncodeValueI16(pEncoder, pNode->dataBlockId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI32(pEncoder, pNode->totalRowSize);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI32(pEncoder, pNode->outputRowSize);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueU8(pEncoder, pNode->precision);
  }

  return code;
}

static int32_t dataBlockDescNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SDataBlockDescNode* pNode = (const SDataBlockDescNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, DATA_BLOCK_DESC_CODE_INLINE_ATTRS, dataBlockDescNodeInlineToMsg, pNode);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, DATA_BLOCK_DESC_CODE_SLOTS, nodeListToMsg, pNode->pSlots);
  }

  return code;
}

static int32_t msgToDataBlockDescNodeInline(STlvDecoder* pDecoder, void* pObj) {
  SDataBlockDescNode* pNode = (SDataBlockDescNode*)pObj;

  int32_t code = tlvDecodeValueI16(pDecoder, &pNode->dataBlockId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI32(pDecoder, &pNode->totalRowSize);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI32(pDecoder, &pNode->outputRowSize);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueU8(pDecoder, &pNode->precision);
  }

  return code;
}

static int32_t msgToDataBlockDescNode(STlvDecoder* pDecoder, void* pObj) {
  SDataBlockDescNode* pNode = (SDataBlockDescNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case DATA_BLOCK_DESC_CODE_INLINE_ATTRS:
        code = tlvDecodeObjFromTlv(pTlv, msgToDataBlockDescNodeInline, pNode);
        break;
      case DATA_BLOCK_DESC_CODE_SLOTS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pSlots);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { SLOT_DESC_CODE_INLINE_ATTRS = 1 };

static int32_t slotDescNodeInlineToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SSlotDescNode* pNode = (const SSlotDescNode*)pObj;

  int32_t code = tlvEncodeValueI16(pEncoder, pNode->slotId);
  if (TSDB_CODE_SUCCESS == code) {
    code = dataTypeInlineToMsg(&pNode->dataType, pEncoder);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueBool(pEncoder, pNode->reserve);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueBool(pEncoder, pNode->output);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueBool(pEncoder, pNode->tag);
  }

  return code;
}

static int32_t slotDescNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  return tlvEncodeObj(pEncoder, SLOT_DESC_CODE_INLINE_ATTRS, slotDescNodeInlineToMsg, pObj);
}

static int32_t msgToSlotDescNodeInline(STlvDecoder* pDecoder, void* pObj) {
  SSlotDescNode* pNode = (SSlotDescNode*)pObj;

  int32_t code = tlvDecodeValueI16(pDecoder, &pNode->slotId);
  if (TSDB_CODE_SUCCESS == code) {
    code = msgToDataTypeInline(pDecoder, &pNode->dataType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueBool(pDecoder, &pNode->reserve);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueBool(pDecoder, &pNode->output);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueBool(pDecoder, &pNode->tag);
  }

  return code;
}

static int32_t msgToSlotDescNode(STlvDecoder* pDecoder, void* pObj) {
  SSlotDescNode* pNode = (SSlotDescNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case SLOT_DESC_CODE_INLINE_ATTRS:
        code = tlvDecodeObjFromTlv(pTlv, msgToSlotDescNodeInline, pNode);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { EP_CODE_FQDN = 1, EP_CODE_port };

static int32_t epInlineToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SEp* pNode = (const SEp*)pObj;

  int32_t code = tlvEncodeValueCStr(pEncoder, pNode->fqdn);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueU16(pEncoder, pNode->port);
  }

  return code;
}

static int32_t epToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SEp* pNode = (const SEp*)pObj;

  int32_t code = tlvEncodeCStr(pEncoder, EP_CODE_FQDN, pNode->fqdn);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeU16(pEncoder, EP_CODE_port, pNode->port);
  }

  return code;
}

static int32_t msgToEpInline(STlvDecoder* pDecoder, void* pObj) {
  SEp* pNode = (SEp*)pObj;

  int32_t code = tlvDecodeValueCStr(pDecoder, pNode->fqdn);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueU16(pDecoder, &pNode->port);
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
        code = tlvDecodeCStr(pTlv, pNode->fqdn, sizeof(pNode->fqdn));
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

static int32_t epSetInlineToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SEpSet* pNode = (const SEpSet*)pObj;

  int32_t code = tlvEncodeValueI8(pEncoder, pNode->inUse);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueArray(pEncoder, epInlineToMsg, pNode->eps, sizeof(SEp), pNode->numOfEps);
  }

  return code;
}

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

static int32_t msgToEpSetInline(STlvDecoder* pDecoder, void* pObj) {
  SEpSet* pNode = (SEpSet*)pObj;

  int32_t code = tlvDecodeValueI8(pDecoder, &pNode->inUse);
  if (TSDB_CODE_SUCCESS == code) {
    int32_t numOfEps = 0;
    code = tlvDecodeValueArray(pDecoder, msgToEpInline, pNode->eps, sizeof(SEp), &numOfEps);
    pNode->numOfEps = numOfEps;
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

enum { QUERY_NODE_ADDR_CODE_NODE_ID = 1, QUERY_NODE_ADDR_CODE_EP_SET };

static int32_t queryNodeAddrInlineToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SQueryNodeAddr* pNode = (const SQueryNodeAddr*)pObj;

  int32_t code = tlvEncodeValueI32(pEncoder, pNode->nodeId);
  if (TSDB_CODE_SUCCESS == code) {
    code = epSetInlineToMsg(&pNode->epSet, pEncoder);
  }

  return code;
}

static int32_t queryNodeAddrToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SQueryNodeAddr* pNode = (const SQueryNodeAddr*)pObj;

  int32_t code = tlvEncodeI32(pEncoder, QUERY_NODE_ADDR_CODE_NODE_ID, pNode->nodeId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, QUERY_NODE_ADDR_CODE_EP_SET, epSetToMsg, &pNode->epSet);
  }

  return code;
}

static int32_t msgToQueryNodeAddrInline(STlvDecoder* pDecoder, void* pObj) {
  SQueryNodeAddr* pNode = (SQueryNodeAddr*)pObj;

  int32_t code = tlvDecodeValueI32(pDecoder, &pNode->nodeId);
  if (TSDB_CODE_SUCCESS == code) {
    code = msgToEpSetInline(pDecoder, &pNode->epSet);
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

enum { DOWNSTREAM_SOURCE_CODE_INLINE_ATTRS = 1 };

static int32_t downstreamSourceNodeInlineToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SDownstreamSourceNode* pNode = (const SDownstreamSourceNode*)pObj;

  int32_t code = queryNodeAddrInlineToMsg(&pNode->addr, pEncoder);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueU64(pEncoder, pNode->taskId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueU64(pEncoder, pNode->schedId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI32(pEncoder, pNode->execId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI32(pEncoder, pNode->fetchMsgType);
  }

  return code;
}

static int32_t downstreamSourceNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  return tlvEncodeObj(pEncoder, DOWNSTREAM_SOURCE_CODE_INLINE_ATTRS, downstreamSourceNodeInlineToMsg, pObj);
}

static int32_t msgToDownstreamSourceNodeInlineToMsg(STlvDecoder* pDecoder, void* pObj) {
  SDownstreamSourceNode* pNode = (SDownstreamSourceNode*)pObj;

  int32_t code = msgToQueryNodeAddrInline(pDecoder, &pNode->addr);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueU64(pDecoder, &pNode->taskId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueU64(pDecoder, &pNode->schedId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI32(pDecoder, &pNode->execId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI32(pDecoder, &pNode->fetchMsgType);
  }

  return code;
}

static int32_t msgToDownstreamSourceNode(STlvDecoder* pDecoder, void* pObj) {
  SDownstreamSourceNode* pNode = (SDownstreamSourceNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case DOWNSTREAM_SOURCE_CODE_INLINE_ATTRS:
        code = tlvDecodeObjFromTlv(pTlv, msgToDownstreamSourceNodeInlineToMsg, pNode);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { WHEN_THEN_CODE_EXPR_BASE = 1, WHEN_THEN_CODE_WHEN, WHEN_THEN_CODE_THEN };

static int32_t whenThenNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SWhenThenNode* pNode = (const SWhenThenNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, WHEN_THEN_CODE_EXPR_BASE, exprNodeToMsg, pNode);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, WHEN_THEN_CODE_WHEN, nodeToMsg, pNode->pWhen);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, WHEN_THEN_CODE_THEN, nodeToMsg, pNode->pThen);
  }

  return code;
}

static int32_t msgToWhenThenNode(STlvDecoder* pDecoder, void* pObj) {
  SWhenThenNode* pNode = (SWhenThenNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case WHEN_THEN_CODE_EXPR_BASE:
        code = tlvDecodeObjFromTlv(pTlv, msgToExprNode, &pNode->node);
        break;
      case WHEN_THEN_CODE_WHEN:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pWhen);
        break;
      case WHEN_THEN_CODE_THEN:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pThen);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { CASE_WHEN_CODE_EXPR_BASE = 1, CASE_WHEN_CODE_CASE, CASE_WHEN_CODE_ELSE, CASE_WHEN_CODE_WHEN_THEN_LIST };

static int32_t caseWhenNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SCaseWhenNode* pNode = (const SCaseWhenNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, CASE_WHEN_CODE_EXPR_BASE, exprNodeToMsg, pNode);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, CASE_WHEN_CODE_CASE, nodeToMsg, pNode->pCase);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, CASE_WHEN_CODE_ELSE, nodeToMsg, pNode->pElse);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, CASE_WHEN_CODE_WHEN_THEN_LIST, nodeListToMsg, pNode->pWhenThenList);
  }

  return code;
}

static int32_t msgToCaseWhenNode(STlvDecoder* pDecoder, void* pObj) {
  SCaseWhenNode* pNode = (SCaseWhenNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case CASE_WHEN_CODE_EXPR_BASE:
        code = tlvDecodeObjFromTlv(pTlv, msgToExprNode, &pNode->node);
        break;
      case CASE_WHEN_CODE_CASE:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pCase);
        break;
      case CASE_WHEN_CODE_ELSE:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pElse);
        break;
      case CASE_WHEN_CODE_WHEN_THEN_LIST:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pWhenThenList);
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
  PHY_NODE_CODE_SLIMIT,
  PHY_NODE_CODE_INPUT_TS_ORDER,
  PHY_NODE_CODE_OUTPUT_TS_ORDER,
  PHY_NODE_CODE_DYNAMIC_OP,
  PHY_NODE_CODE_FORCE_NONBLOCKING_OPTR
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
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeEnum(pEncoder, PHY_NODE_CODE_INPUT_TS_ORDER, pNode->inputTsOrder);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeEnum(pEncoder, PHY_NODE_CODE_OUTPUT_TS_ORDER, pNode->outputTsOrder);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_NODE_CODE_DYNAMIC_OP, pNode->dynamicOp);
  }
  if (TSDB_CODE_SUCCESS == code) { 
    code = tlvEncodeBool(pEncoder, PHY_NODE_CODE_FORCE_NONBLOCKING_OPTR, pNode->forceCreateNonBlockingOptr);
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
      case PHY_NODE_CODE_INPUT_TS_ORDER:
        code = tlvDecodeEnum(pTlv, &pNode->inputTsOrder, sizeof(pNode->inputTsOrder));
        break;
      case PHY_NODE_CODE_OUTPUT_TS_ORDER:
        code = tlvDecodeEnum(pTlv, &pNode->outputTsOrder, sizeof(pNode->outputTsOrder));
        break;
      case PHY_NODE_CODE_DYNAMIC_OP:
        code = tlvDecodeBool(pTlv, &pNode->dynamicOp);
        break;
      case PHY_NODE_CODE_FORCE_NONBLOCKING_OPTR:
        code = tlvDecodeBool(pTlv, &pNode->forceCreateNonBlockingOptr);
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
  PHY_SCAN_CODE_BASE_TABLE_NAME,
  PHY_SCAN_CODE_BASE_GROUP_ORDER_SCAN
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
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_SCAN_CODE_BASE_GROUP_ORDER_SCAN, pNode->groupOrderScan);
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
      case PHY_SCAN_CODE_BASE_GROUP_ORDER_SCAN:
        code = tlvDecodeBool(pTlv, &pNode->groupOrderScan);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  PHY_TAG_SCAN_CODE_SCAN = 1,
  PHY_TAG_SCAN_CODE_ONLY_META_CTB_IDX
};

static int32_t physiTagScanNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const STagScanPhysiNode* pNode = (const STagScanPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_TAG_SCAN_CODE_SCAN, physiScanNodeToMsg, &pNode->scan);

  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_TAG_SCAN_CODE_ONLY_META_CTB_IDX, pNode->onlyMetaCtbIdx);
  }
  return code;
}

static int32_t msgToPhysiTagScanNode(STlvDecoder* pDecoder, void* pObj) {
  STagScanPhysiNode* pNode = (STagScanPhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_TAG_SCAN_CODE_SCAN:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiScanNode, &pNode->scan);
        break;
      case PHY_TAG_SCAN_CODE_ONLY_META_CTB_IDX:
        code = tlvDecodeBool(pTlv, &pNode->onlyMetaCtbIdx);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  PHY_LAST_ROW_SCAN_CODE_SCAN = 1,
  PHY_LAST_ROW_SCAN_CODE_GROUP_TAGS,
  PHY_LAST_ROW_SCAN_CODE_GROUP_SORT,
  PHY_LAST_ROW_SCAN_CODE_IGNULL,
  PHY_LAST_ROW_SCAN_CODE_TARGETS,
  PHY_LAST_ROW_SCAN_CODE_FUNCTYPES
};

static int32_t physiLastRowScanNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SLastRowScanPhysiNode* pNode = (const SLastRowScanPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_LAST_ROW_SCAN_CODE_SCAN, physiScanNodeToMsg, &pNode->scan);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_LAST_ROW_SCAN_CODE_GROUP_TAGS, nodeListToMsg, pNode->pGroupTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_LAST_ROW_SCAN_CODE_GROUP_SORT, pNode->groupSort);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_LAST_ROW_SCAN_CODE_IGNULL, pNode->ignoreNull);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_LAST_ROW_SCAN_CODE_TARGETS, nodeListToMsg, pNode->pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_LAST_ROW_SCAN_CODE_FUNCTYPES, SArrayToMsg, pNode->pFuncTypes);
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
      case PHY_LAST_ROW_SCAN_CODE_IGNULL:
        code = tlvDecodeBool(pTlv, &pNode->ignoreNull);
        break;
      case PHY_LAST_ROW_SCAN_CODE_TARGETS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pTargets);
        break;
      case PHY_LAST_ROW_SCAN_CODE_FUNCTYPES:
        code = msgToSArray(pTlv, (void**)&pNode->pFuncTypes);
        break;

      default:
        break;
    }
  }

  return code;
}

enum {
  PHY_TABLE_SCAN_CODE_SCAN = 1,
  PHY_TABLE_SCAN_CODE_INLINE_ATTRS,
  PHY_TABLE_SCAN_CODE_DYN_SCAN_FUNCS,
  PHY_TABLE_SCAN_CODE_GROUP_TAGS,
  PHY_TABLE_SCAN_CODE_TAGS,
  PHY_TABLE_SCAN_CODE_SUBTABLE
};

static int32_t physiTableScanNodeInlineToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const STableScanPhysiNode* pNode = (const STableScanPhysiNode*)pObj;

  int32_t code = tlvEncodeValueU8(pEncoder, pNode->scanSeq[0]);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueU8(pEncoder, pNode->scanSeq[1]);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI64(pEncoder, pNode->scanRange.skey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI64(pEncoder, pNode->scanRange.ekey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueDouble(pEncoder, pNode->ratio);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI32(pEncoder, pNode->dataRequired);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueBool(pEncoder, pNode->groupSort);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI64(pEncoder, pNode->interval);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI64(pEncoder, pNode->offset);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI64(pEncoder, pNode->sliding);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI8(pEncoder, pNode->intervalUnit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI8(pEncoder, pNode->slidingUnit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI8(pEncoder, pNode->triggerType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI64(pEncoder, pNode->watermark);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI8(pEncoder, pNode->igExpired);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueBool(pEncoder, pNode->assignBlockUid);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI8(pEncoder, pNode->igCheckUpdate);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueBool(pEncoder, pNode->filesetDelimited);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueBool(pEncoder, pNode->needCountEmptyTable);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueBool(pEncoder, pNode->paraTablesSort);
  }
  return code;
}

static int32_t physiTableScanNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const STableScanPhysiNode* pNode = (const STableScanPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_TABLE_SCAN_CODE_SCAN, physiScanNodeToMsg, &pNode->scan);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_TABLE_SCAN_CODE_INLINE_ATTRS, physiTableScanNodeInlineToMsg, pNode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_TABLE_SCAN_CODE_DYN_SCAN_FUNCS, nodeListToMsg, pNode->pDynamicScanFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_TABLE_SCAN_CODE_GROUP_TAGS, nodeListToMsg, pNode->pGroupTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_TABLE_SCAN_CODE_TAGS, nodeListToMsg, pNode->pTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_TABLE_SCAN_CODE_SUBTABLE, nodeToMsg, pNode->pSubtable);
  }

  return code;
}

static int32_t msgToPhysiTableScanNodeInline(STlvDecoder* pDecoder, void* pObj) {
  STableScanPhysiNode* pNode = (STableScanPhysiNode*)pObj;

  int32_t code = tlvDecodeValueU8(pDecoder, pNode->scanSeq);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueU8(pDecoder, pNode->scanSeq + 1);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI64(pDecoder, &pNode->scanRange.skey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI64(pDecoder, &pNode->scanRange.ekey);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueDouble(pDecoder, &pNode->ratio);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI32(pDecoder, &pNode->dataRequired);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueBool(pDecoder, &pNode->groupSort);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI64(pDecoder, &pNode->interval);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI64(pDecoder, &pNode->offset);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI64(pDecoder, &pNode->sliding);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI8(pDecoder, &pNode->intervalUnit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI8(pDecoder, &pNode->slidingUnit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI8(pDecoder, &pNode->triggerType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI64(pDecoder, &pNode->watermark);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI8(pDecoder, &pNode->igExpired);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueBool(pDecoder, &pNode->assignBlockUid);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI8(pDecoder, &pNode->igCheckUpdate);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueBool(pDecoder, &pNode->filesetDelimited);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueBool(pDecoder, &pNode->needCountEmptyTable);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueBool(pDecoder, &pNode->paraTablesSort);
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
      case PHY_TABLE_SCAN_CODE_INLINE_ATTRS:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiTableScanNodeInline, pNode);
        break;
      case PHY_TABLE_SCAN_CODE_DYN_SCAN_FUNCS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pDynamicScanFuncs);
        break;
      case PHY_TABLE_SCAN_CODE_GROUP_TAGS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pGroupTags);
        break;
      case PHY_TABLE_SCAN_CODE_TAGS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pTags);
        break;
      case PHY_TABLE_SCAN_CODE_SUBTABLE:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pSubtable);
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
  PHY_PROJECT_CODE_IGNORE_GROUP_ID,
  PHY_PROJECT_CODE_INPUT_IGNORE_GROUP
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
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_PROJECT_CODE_INPUT_IGNORE_GROUP, pNode->inputIgnoreGroup);
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
      case PHY_PROJECT_CODE_INPUT_IGNORE_GROUP:
        code = tlvDecodeBool(pTlv, &pNode->inputIgnoreGroup);
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
  PHY_SORT_MERGE_JOIN_CODE_PRIM_KEY_CONDITION,
  PHY_SORT_MERGE_JOIN_CODE_ON_CONDITIONS,
  PHY_SORT_MERGE_JOIN_CODE_TARGETS,
  PHY_SORT_MERGE_JOIN_CODE_INPUT_TS_ORDER,
  PHY_SORT_MERGE_JOIN_CODE_TAG_EQUAL_CONDITIONS
};

static int32_t physiMergeJoinNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SSortMergeJoinPhysiNode* pNode = (const SSortMergeJoinPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_SORT_MERGE_JOIN_CODE_BASE_NODE, physiNodeToMsg, &pNode->node);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeEnum(pEncoder, PHY_SORT_MERGE_JOIN_CODE_JOIN_TYPE, pNode->joinType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_SORT_MERGE_JOIN_CODE_PRIM_KEY_CONDITION, nodeToMsg, pNode->pPrimKeyCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_SORT_MERGE_JOIN_CODE_ON_CONDITIONS, nodeToMsg, pNode->pOtherOnCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_SORT_MERGE_JOIN_CODE_TARGETS, nodeListToMsg, pNode->pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_SORT_MERGE_JOIN_CODE_TAG_EQUAL_CONDITIONS, nodeToMsg, pNode->pColEqCond);
  }
  return code;
}

static int32_t msgToPhysiMergeJoinNode(STlvDecoder* pDecoder, void* pObj) {
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
      case PHY_SORT_MERGE_JOIN_CODE_PRIM_KEY_CONDITION:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pPrimKeyCond);
        break;
      case PHY_SORT_MERGE_JOIN_CODE_ON_CONDITIONS:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pOtherOnCond);
        break;
      case PHY_SORT_MERGE_JOIN_CODE_TARGETS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pTargets);
        break;
      case PHY_SORT_MERGE_JOIN_CODE_TAG_EQUAL_CONDITIONS:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pColEqCond);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  PHY_HASH_JOIN_CODE_BASE_NODE = 1,
  PHY_HASH_JOIN_CODE_JOIN_TYPE,
  PHY_HASH_JOIN_CODE_ON_LEFT_COLUMN,
  PHY_HASH_JOIN_CODE_ON_RIGHT_COLUMN,
  PHY_HASH_JOIN_CODE_ON_CONDITIONS,
  PHY_HASH_JOIN_CODE_TARGETS,
  PHY_HASH_JOIN_CODE_INPUT_ROW_NUM0,
  PHY_HASH_JOIN_CODE_INPUT_ROW_SIZE0,
  PHY_HASH_JOIN_CODE_INPUT_ROW_NUM1,
  PHY_HASH_JOIN_CODE_INPUT_ROW_SIZE1
};

static int32_t physiHashJoinNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SHashJoinPhysiNode* pNode = (const SHashJoinPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_HASH_JOIN_CODE_BASE_NODE, physiNodeToMsg, &pNode->node);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeEnum(pEncoder, PHY_HASH_JOIN_CODE_JOIN_TYPE, pNode->joinType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_HASH_JOIN_CODE_ON_LEFT_COLUMN, nodeListToMsg, pNode->pOnLeft);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_HASH_JOIN_CODE_ON_RIGHT_COLUMN, nodeListToMsg, pNode->pOnRight);
  }  
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_HASH_JOIN_CODE_ON_CONDITIONS, nodeToMsg, pNode->pFilterConditions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_HASH_JOIN_CODE_TARGETS, nodeListToMsg, pNode->pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI64(pEncoder, PHY_HASH_JOIN_CODE_INPUT_ROW_NUM0, pNode->inputStat[0].inputRowNum);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI64(pEncoder, PHY_HASH_JOIN_CODE_INPUT_ROW_NUM1, pNode->inputStat[1].inputRowNum);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, PHY_HASH_JOIN_CODE_INPUT_ROW_SIZE0, pNode->inputStat[0].inputRowSize);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, PHY_HASH_JOIN_CODE_INPUT_ROW_SIZE1, pNode->inputStat[1].inputRowSize);
  }
  return code;
}


static int32_t msgToPhysiHashJoinNode(STlvDecoder* pDecoder, void* pObj) {
  SHashJoinPhysiNode* pNode = (SHashJoinPhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_HASH_JOIN_CODE_BASE_NODE:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiNode, &pNode->node);
        break;
      case PHY_HASH_JOIN_CODE_JOIN_TYPE:
        code = tlvDecodeEnum(pTlv, &pNode->joinType, sizeof(pNode->joinType));
        break;
      case PHY_HASH_JOIN_CODE_ON_LEFT_COLUMN:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pOnLeft);
        break;
      case PHY_HASH_JOIN_CODE_ON_RIGHT_COLUMN:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pOnRight);
        break;
      case PHY_HASH_JOIN_CODE_ON_CONDITIONS:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pFilterConditions);
        break;
      case PHY_HASH_JOIN_CODE_TARGETS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pTargets);
        break;
      case PHY_HASH_JOIN_CODE_INPUT_ROW_NUM0:
        code = tlvDecodeI64(pTlv, &pNode->inputStat[0].inputRowNum);
        break;
      case PHY_HASH_JOIN_CODE_INPUT_ROW_NUM1:
        code = tlvDecodeI64(pTlv, &pNode->inputStat[1].inputRowNum);
        break;
      case PHY_HASH_JOIN_CODE_INPUT_ROW_SIZE0:
        code = tlvDecodeI32(pTlv, &pNode->inputStat[0].inputRowSize);
        break;
      case PHY_HASH_JOIN_CODE_INPUT_ROW_SIZE1:
        code = tlvDecodeI32(pTlv, &pNode->inputStat[1].inputRowSize);
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
  PHY_AGG_CODE_MERGE_DATA_BLOCK,
  PHY_AGG_CODE_GROUP_KEY_OPTIMIZE
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
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_AGG_CODE_GROUP_KEY_OPTIMIZE, pNode->groupKeyOptimized);
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
      case PHY_AGG_CODE_GROUP_KEY_OPTIMIZE:
        code = tlvDecodeBool(pTlv, &pNode->groupKeyOptimized);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  PHY_EXCHANGE_CODE_BASE_NODE = 1,
  PHY_EXCHANGE_CODE_SRC_START_GROUP_ID,
  PHY_EXCHANGE_CODE_SRC_END_GROUP_ID,
  PHY_EXCHANGE_CODE_SINGLE_CHANNEL,
  PHY_EXCHANGE_CODE_SRC_ENDPOINTS,
  PHY_EXCHANGE_CODE_SEQ_RECV_DATA
};

static int32_t physiExchangeNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SExchangePhysiNode* pNode = (const SExchangePhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_EXCHANGE_CODE_BASE_NODE, physiNodeToMsg, &pNode->node);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, PHY_EXCHANGE_CODE_SRC_START_GROUP_ID, pNode->srcStartGroupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, PHY_EXCHANGE_CODE_SRC_END_GROUP_ID, pNode->srcEndGroupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_EXCHANGE_CODE_SINGLE_CHANNEL, pNode->singleChannel);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_EXCHANGE_CODE_SRC_ENDPOINTS, nodeListToMsg, pNode->pSrcEndPoints);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_EXCHANGE_CODE_SEQ_RECV_DATA, pNode->seqRecvData);
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
      case PHY_EXCHANGE_CODE_SRC_START_GROUP_ID:
        code = tlvDecodeI32(pTlv, &pNode->srcStartGroupId);
        break;
      case PHY_EXCHANGE_CODE_SRC_END_GROUP_ID:
        code = tlvDecodeI32(pTlv, &pNode->srcEndGroupId);
        break;
      case PHY_EXCHANGE_CODE_SINGLE_CHANNEL:
        code = tlvDecodeBool(pTlv, &pNode->singleChannel);
        break;
      case PHY_EXCHANGE_CODE_SRC_ENDPOINTS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pSrcEndPoints);
        break;
      case PHY_EXCHANGE_CODE_SEQ_RECV_DATA:
        code = tlvDecodeBool(pTlv, &pNode->seqRecvData);
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
  PHY_MERGE_CODE_GROUP_SORT,
  PHY_MERGE_CODE_IGNORE_GROUP_ID,
  PHY_MERGE_CODE_INPUT_WITH_GROUP_ID,
  PHY_MERGE_CODE_TYPE,
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
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_MERGE_CODE_IGNORE_GROUP_ID, pNode->ignoreGroupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_MERGE_CODE_INPUT_WITH_GROUP_ID, pNode->inputWithGroupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, PHY_MERGE_CODE_TYPE, pNode->type);
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
      case PHY_MERGE_CODE_IGNORE_GROUP_ID:
        code = tlvDecodeBool(pTlv, &pNode->ignoreGroupId);
        break;
      case PHY_MERGE_CODE_INPUT_WITH_GROUP_ID:
        code = tlvDecodeBool(pTlv, &pNode->inputWithGroupId);
        break;
      case PHY_MERGE_CODE_TYPE:
        code = tlvDecodeI32(pTlv, (int32_t*)&pNode->type);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  PHY_SORT_CODE_BASE_NODE = 1,
  PHY_SORT_CODE_EXPR,
  PHY_SORT_CODE_SORT_KEYS,
  PHY_SORT_CODE_TARGETS,
  PHY_SORT_CODE_CALC_GROUPID,
  PHY_SORT_CODE_EXCLUDE_PK_COL
};

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
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_SORT_CODE_CALC_GROUPID, pNode->calcGroupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_SORT_CODE_EXCLUDE_PK_COL, pNode->excludePkCol);
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
      case PHY_SORT_CODE_CALC_GROUPID:
        code = tlvDecodeBool(pTlv, &pNode->calcGroupId);
        break;
      case PHY_SORT_CODE_EXCLUDE_PK_COL:
        code = tlvDecodeBool(pTlv, &pNode->excludePkCol);
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
  PHY_WINDOW_CODE_DELETE_MARK,
  PHY_WINDOW_CODE_IG_EXPIRED,
  PHY_WINDOW_CODE_INPUT_TS_ORDER,
  PHY_WINDOW_CODE_OUTPUT_TS_ORDER,
  PHY_WINDOW_CODE_MERGE_DATA_BLOCK
};

static int32_t physiWindowNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SWindowPhysiNode* pNode = (const SWindowPhysiNode*)pObj;

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
    code = tlvEncodeI64(pEncoder, PHY_WINDOW_CODE_DELETE_MARK, pNode->deleteMark);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI8(pEncoder, PHY_WINDOW_CODE_IG_EXPIRED, pNode->igExpired);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_WINDOW_CODE_MERGE_DATA_BLOCK, pNode->mergeDataBlock);
  }

  return code;
}

static int32_t msgToPhysiWindowNode(STlvDecoder* pDecoder, void* pObj) {
  SWindowPhysiNode* pNode = (SWindowPhysiNode*)pObj;

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
      case PHY_WINDOW_CODE_DELETE_MARK:
        code = tlvDecodeI64(pTlv, &pNode->deleteMark);
        break;
      case PHY_WINDOW_CODE_IG_EXPIRED:
        code = tlvDecodeI8(pTlv, &pNode->igExpired);
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

enum { PHY_INTERVAL_CODE_WINDOW = 1, PHY_INTERVAL_CODE_INLINE_ATTRS };

static int32_t physiIntervalNodeInlineToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SIntervalPhysiNode* pNode = (const SIntervalPhysiNode*)pObj;

  int32_t code = tlvEncodeValueI64(pEncoder, pNode->interval);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI64(pEncoder, pNode->offset);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI64(pEncoder, pNode->sliding);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI8(pEncoder, pNode->intervalUnit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI8(pEncoder, pNode->slidingUnit);
  }

  return code;
}

static int32_t physiIntervalNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SIntervalPhysiNode* pNode = (const SIntervalPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_INTERVAL_CODE_WINDOW, physiWindowNodeToMsg, &pNode->window);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_INTERVAL_CODE_INLINE_ATTRS, physiIntervalNodeInlineToMsg, pNode);
  }

  return code;
}

static int32_t msgToPhysiIntervalNodeInline(STlvDecoder* pDecoder, void* pObj) {
  SIntervalPhysiNode* pNode = (SIntervalPhysiNode*)pObj;

  int32_t code = tlvDecodeValueI64(pDecoder, &pNode->interval);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI64(pDecoder, &pNode->offset);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI64(pDecoder, &pNode->sliding);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI8(pDecoder, &pNode->intervalUnit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI8(pDecoder, &pNode->slidingUnit);
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
      case PHY_INTERVAL_CODE_INLINE_ATTRS:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiIntervalNodeInline, pNode);
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

enum { PHY_EVENT_CODE_WINDOW = 1, PHY_EVENT_CODE_START_COND, PHY_EVENT_CODE_END_COND };

static int32_t physiEventWindowNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SEventWinodwPhysiNode* pNode = (const SEventWinodwPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_EVENT_CODE_WINDOW, physiWindowNodeToMsg, &pNode->window);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_EVENT_CODE_START_COND, nodeToMsg, pNode->pStartCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_EVENT_CODE_END_COND, nodeToMsg, pNode->pEndCond);
  }

  return code;
}

static int32_t msgToPhysiEventWindowNode(STlvDecoder* pDecoder, void* pObj) {
  SEventWinodwPhysiNode* pNode = (SEventWinodwPhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_EVENT_CODE_WINDOW:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiWindowNode, &pNode->window);
        break;
      case PHY_EVENT_CODE_START_COND:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pStartCond);
        break;
      case PHY_EVENT_CODE_END_COND:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pEndCond);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { PHY_COUNT_CODE_WINDOW = 1, PHY_COUNT_CODE_WINDOW_COUNT, PHY_COUNT_CODE_WINDOW_SLIDING };

static int32_t physiCountWindowNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SCountWinodwPhysiNode* pNode = (const SCountWinodwPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_COUNT_CODE_WINDOW, physiWindowNodeToMsg, &pNode->window);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI64(pEncoder, PHY_COUNT_CODE_WINDOW_COUNT, pNode->windowCount);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI64(pEncoder, PHY_COUNT_CODE_WINDOW_SLIDING, pNode->windowSliding);
  }

  return code;
}

static int32_t msgToPhysiCountWindowNode(STlvDecoder* pDecoder, void* pObj) {
  SCountWinodwPhysiNode* pNode = (SCountWinodwPhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_COUNT_CODE_WINDOW:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiWindowNode, &pNode->window);
        break;
      case PHY_COUNT_CODE_WINDOW_COUNT:
        code = tlvDecodeI64(pTlv, &pNode->windowCount);
        break;
      case PHY_COUNT_CODE_WINDOW_SLIDING:
        code = tlvDecodeI64(pTlv, &pNode->windowSliding);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  PHY_PARTITION_CODE_BASE_NODE = 1,
  PHY_PARTITION_CODE_EXPR,
  PHY_PARTITION_CODE_KEYS,
  PHY_PARTITION_CODE_TARGETS,
  PHY_PARTITION_CODE_HAS_OUTPUT_TS_ORDER,
  PHY_PARTITION_CODE_TS_SLOTID
};

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
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_PARTITION_CODE_HAS_OUTPUT_TS_ORDER, pNode->needBlockOutputTsOrder);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, PHY_PARTITION_CODE_TS_SLOTID, pNode->tsSlotId);
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
      case PHY_PARTITION_CODE_HAS_OUTPUT_TS_ORDER:
        code = tlvDecodeBool(pTlv, &pNode->needBlockOutputTsOrder);
        break;
      case PHY_PARTITION_CODE_TS_SLOTID:
        code = tlvDecodeI32(pTlv, &pNode->tsSlotId);
        break;
      default:
        break;
    }
  }

  return code;
}

enum { PHY_STREAM_PARTITION_CODE_BASE_NODE = 1, PHY_STREAM_PARTITION_CODE_TAGS, PHY_STREAM_PARTITION_CODE_SUBTABLE };

static int32_t physiStreamPartitionNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SStreamPartitionPhysiNode* pNode = (const SStreamPartitionPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_STREAM_PARTITION_CODE_BASE_NODE, physiPartitionNodeToMsg, &pNode->part);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_STREAM_PARTITION_CODE_TAGS, nodeListToMsg, pNode->pTags);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_STREAM_PARTITION_CODE_SUBTABLE, nodeToMsg, pNode->pSubtable);
  }

  return code;
}

static int32_t msgToPhysiStreamPartitionNode(STlvDecoder* pDecoder, void* pObj) {
  SStreamPartitionPhysiNode* pNode = (SStreamPartitionPhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_STREAM_PARTITION_CODE_BASE_NODE:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiPartitionNode, &pNode->part);
        break;
      case PHY_STREAM_PARTITION_CODE_TAGS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pTags);
        break;
      case PHY_STREAM_PARTITION_CODE_SUBTABLE:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pSubtable);
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
  PHY_QUERY_INSERT_CODE_EP_SET,
  PHY_QUERY_INSERT_CODE_EXPLAIN
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
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_QUERY_INSERT_CODE_EXPLAIN, pNode->explain);
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
        code = tlvDecodeCStr(pTlv, pNode->tableName, sizeof(pNode->tableName));
        break;
      case PHY_QUERY_INSERT_CODE_VG_ID:
        code = tlvDecodeI32(pTlv, &pNode->vgId);
        break;
      case PHY_QUERY_INSERT_CODE_EP_SET:
        code = tlvDecodeObjFromTlv(pTlv, msgToEpSet, &pNode->epSet);
        break;
      case PHY_QUERY_INSERT_CODE_EXPLAIN:
        code = tlvDecodeBool(pTlv, &pNode->explain);
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
  PHY_DELETER_CODE_AFFECTED_ROWS,
  PHY_DELETER_CODE_START_TS,
  PHY_DELETER_CODE_END_TS
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
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_DELETER_CODE_START_TS, nodeToMsg, pNode->pStartTs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_DELETER_CODE_END_TS, nodeToMsg, pNode->pEndTs);
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
        code = tlvDecodeCStr(pTlv, pNode->tableFName, sizeof(pNode->tableFName));
        break;
      case PHY_DELETER_CODE_TS_COL_NAME:
        code = tlvDecodeCStr(pTlv, pNode->tsColName, sizeof(pNode->tsColName));
        break;
      case PHY_DELETER_CODE_DELETE_TIME_RANGE:
        code = tlvDecodeObjFromTlv(pTlv, msgToTimeWindow, &pNode->deleteTimeRange);
        break;
      case PHY_DELETER_CODE_AFFECTED_ROWS:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pAffectedRows);
        break;
      case PHY_DELETER_CODE_START_TS:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pStartTs);
        break;
      case PHY_DELETER_CODE_END_TS:
        code = msgToNodeFromTlv(pTlv, (void**)&pNode->pEndTs);
        break;
      default:
        break;
    }
  }

  return code;
}

enum {
  PHY_GROUP_CACHE_CODE_BASE_NODE = 1,
  PHY_GROUP_CACHE_CODE_GROUP_COLS_MAY_BE_NULL,
  PHY_GROUP_CACHE_CODE_GROUP_BY_UID,
  PHY_GROUP_CACHE_CODE_GLOBAL_GROUP,
  PHY_GROUP_CACHE_CODE_BATCH_FETCH,
  PHY_GROUP_CACHE_CODE_GROUP_COLUMNS
};

static int32_t physiGroupCacheNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SGroupCachePhysiNode* pNode = (const SGroupCachePhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_GROUP_CACHE_CODE_BASE_NODE, physiNodeToMsg, &pNode->node);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, PHY_GROUP_CACHE_CODE_GROUP_COLUMNS, nodeListToMsg, pNode->pGroupCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_GROUP_CACHE_CODE_GROUP_COLS_MAY_BE_NULL, pNode->grpColsMayBeNull);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_GROUP_CACHE_CODE_GROUP_BY_UID, pNode->grpByUid);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_GROUP_CACHE_CODE_GLOBAL_GROUP, pNode->globalGrp);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeBool(pEncoder, PHY_GROUP_CACHE_CODE_BATCH_FETCH, pNode->batchFetch);
  }

  return code;
}

static int32_t msgToPhysiGroupCacheNode(STlvDecoder* pDecoder, void* pObj) {
  SGroupCachePhysiNode* pNode = (SGroupCachePhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_GROUP_CACHE_CODE_BASE_NODE:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiNode, &pNode->node);
        break;
      case PHY_GROUP_CACHE_CODE_GROUP_COLUMNS:
        code = msgToNodeListFromTlv(pTlv, (void**)&pNode->pGroupCols);
        break;
      case PHY_GROUP_CACHE_CODE_GROUP_COLS_MAY_BE_NULL:
        code = tlvDecodeBool(pTlv, &pNode->grpColsMayBeNull);
        break;    
      case PHY_GROUP_CACHE_CODE_GROUP_BY_UID:
        code = tlvDecodeBool(pTlv, &pNode->grpByUid);
        break;    
      case PHY_GROUP_CACHE_CODE_GLOBAL_GROUP:
        code = tlvDecodeBool(pTlv, &pNode->globalGrp);
        break;    
      case PHY_GROUP_CACHE_CODE_BATCH_FETCH:
        code = tlvDecodeBool(pTlv, &pNode->batchFetch);
        break;    
      default:
        break;
    }
  }

  return code;
}


enum {
  PHY_DYN_QUERY_CTRL_CODE_BASE_NODE = 1,
  PHY_DYN_QUERY_CTRL_CODE_QUERY_TYPE,
  PHY_DYN_QUERY_CTRL_CODE_STB_JOIN_BATCH_FETCH,
  PHY_DYN_QUERY_CTRL_CODE_STB_JOIN_VG_SLOT0,
  PHY_DYN_QUERY_CTRL_CODE_STB_JOIN_VG_SLOT1,
  PHY_DYN_QUERY_CTRL_CODE_STB_JOIN_UID_SLOT0,
  PHY_DYN_QUERY_CTRL_CODE_STB_JOIN_UID_SLOT1,
  PHY_DYN_QUERY_CTRL_CODE_STB_JOIN_SRC_SCAN0,
  PHY_DYN_QUERY_CTRL_CODE_STB_JOIN_SRC_SCAN1,
};

static int32_t physiDynQueryCtrlNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SDynQueryCtrlPhysiNode* pNode = (const SDynQueryCtrlPhysiNode*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, PHY_DYN_QUERY_CTRL_CODE_BASE_NODE, physiNodeToMsg, &pNode->node);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeEnum(pEncoder, PHY_DYN_QUERY_CTRL_CODE_QUERY_TYPE, pNode->qType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    switch (pNode->qType) {
      case DYN_QTYPE_STB_HASH: {
        code = tlvEncodeBool(pEncoder, PHY_DYN_QUERY_CTRL_CODE_STB_JOIN_BATCH_FETCH, pNode->stbJoin.batchFetch);
        if (TSDB_CODE_SUCCESS == code) {
          code = tlvEncodeEnum(pEncoder, PHY_DYN_QUERY_CTRL_CODE_STB_JOIN_VG_SLOT0, pNode->stbJoin.vgSlot[0]);
        }
        if (TSDB_CODE_SUCCESS == code) {
          code = tlvEncodeEnum(pEncoder, PHY_DYN_QUERY_CTRL_CODE_STB_JOIN_VG_SLOT1, pNode->stbJoin.vgSlot[1]);
        }
        if (TSDB_CODE_SUCCESS == code) {
          code = tlvEncodeEnum(pEncoder, PHY_DYN_QUERY_CTRL_CODE_STB_JOIN_UID_SLOT0, pNode->stbJoin.uidSlot[0]);
        }
        if (TSDB_CODE_SUCCESS == code) {
          code = tlvEncodeEnum(pEncoder, PHY_DYN_QUERY_CTRL_CODE_STB_JOIN_UID_SLOT1, pNode->stbJoin.uidSlot[1]);
        }
        if (TSDB_CODE_SUCCESS == code) {
          code = tlvEncodeBool(pEncoder, PHY_DYN_QUERY_CTRL_CODE_STB_JOIN_SRC_SCAN0, pNode->stbJoin.srcScan[0]);
        }
        if (TSDB_CODE_SUCCESS == code) {
          code = tlvEncodeBool(pEncoder, PHY_DYN_QUERY_CTRL_CODE_STB_JOIN_SRC_SCAN1, pNode->stbJoin.srcScan[1]);
        }
        break;
      }
      default:
        return TSDB_CODE_INVALID_PARA;
    }
  }
  return code;
}

static int32_t msgToPhysiDynQueryCtrlNode(STlvDecoder* pDecoder, void* pObj) {
  SDynQueryCtrlPhysiNode* pNode = (SDynQueryCtrlPhysiNode*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case PHY_DYN_QUERY_CTRL_CODE_BASE_NODE:
        code = tlvDecodeObjFromTlv(pTlv, msgToPhysiNode, &pNode->node);
        break;
      case PHY_DYN_QUERY_CTRL_CODE_QUERY_TYPE:
        code = tlvDecodeEnum(pTlv, &pNode->qType, sizeof(pNode->qType));
        break;
      case PHY_DYN_QUERY_CTRL_CODE_STB_JOIN_BATCH_FETCH:
        code = tlvDecodeBool(pTlv, &pNode->stbJoin.batchFetch);
        break;
      case PHY_DYN_QUERY_CTRL_CODE_STB_JOIN_VG_SLOT0:
        code = tlvDecodeEnum(pTlv, &pNode->stbJoin.vgSlot[0], sizeof(pNode->stbJoin.vgSlot[0]));
        break;
      case PHY_DYN_QUERY_CTRL_CODE_STB_JOIN_VG_SLOT1:
        code = tlvDecodeEnum(pTlv, &pNode->stbJoin.vgSlot[1], sizeof(pNode->stbJoin.vgSlot[1]));
        break;
      case PHY_DYN_QUERY_CTRL_CODE_STB_JOIN_UID_SLOT0:
        code = tlvDecodeEnum(pTlv, &pNode->stbJoin.uidSlot[0], sizeof(pNode->stbJoin.uidSlot[0]));
        break;
      case PHY_DYN_QUERY_CTRL_CODE_STB_JOIN_UID_SLOT1:
        code = tlvDecodeEnum(pTlv, &pNode->stbJoin.uidSlot[1], sizeof(pNode->stbJoin.uidSlot[1]));
        break;      
      case PHY_DYN_QUERY_CTRL_CODE_STB_JOIN_SRC_SCAN0:
        code = tlvDecodeBool(pTlv, &pNode->stbJoin.srcScan[0]);
        break;
      case PHY_DYN_QUERY_CTRL_CODE_STB_JOIN_SRC_SCAN1:
        code = tlvDecodeBool(pTlv, &pNode->stbJoin.srcScan[1]);
        break;      
      default:
        break;
    }
  }

  return code;
}



enum { SUBPLAN_ID_CODE_QUERY_ID = 1, SUBPLAN_ID_CODE_GROUP_ID, SUBPLAN_ID_CODE_SUBPLAN_ID };

static int32_t subplanIdInlineToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SSubplanId* pNode = (const SSubplanId*)pObj;

  int32_t code = tlvEncodeValueU64(pEncoder, pNode->queryId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI32(pEncoder, pNode->groupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI32(pEncoder, pNode->subplanId);
  }

  return code;
}

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

static int32_t msgToSubplanIdInline(STlvDecoder* pDecoder, void* pObj) {
  SSubplanId* pNode = (SSubplanId*)pObj;

  int32_t code = tlvDecodeValueU64(pDecoder, &pNode->queryId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI32(pDecoder, &pNode->groupId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI32(pDecoder, &pNode->subplanId);
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

enum {
  SUBPLAN_CODE_INLINE_ATTRS = 1,
  SUBPLAN_CODE_ROOT_NODE,
  SUBPLAN_CODE_DATA_SINK,
  SUBPLAN_CODE_TAG_COND,
  SUBPLAN_CODE_TAG_INDEX_COND
};

static int32_t subplanInlineToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SSubplan* pNode = (const SSubplan*)pObj;

  int32_t code = subplanIdInlineToMsg(&pNode->id, pEncoder);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueEnum(pEncoder, pNode->subplanType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI32(pEncoder, pNode->msgType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI32(pEncoder, pNode->level);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueCStr(pEncoder, pNode->dbFName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueCStr(pEncoder, pNode->user);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = queryNodeAddrInlineToMsg(&pNode->execNode, pEncoder);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueBool(pEncoder, pNode->showRewrite);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI32(pEncoder, pNode->rowsThreshold);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueBool(pEncoder, pNode->dynamicRowThreshold);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueBool(pEncoder, pNode->isView);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueBool(pEncoder, pNode->isAudit);
  }

  return code;
}

static int32_t subplanToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SSubplan* pNode = (const SSubplan*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, SUBPLAN_CODE_INLINE_ATTRS, subplanInlineToMsg, pNode);
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

static int32_t msgToSubplanInline(STlvDecoder* pDecoder, void* pObj) {
  SSubplan* pNode = (SSubplan*)pObj;

  int32_t code = msgToSubplanIdInline(pDecoder, &pNode->id);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueEnum(pDecoder, &pNode->subplanType, sizeof(pNode->subplanType));
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI32(pDecoder, &pNode->msgType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI32(pDecoder, &pNode->level);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueCStr(pDecoder, pNode->dbFName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueCStr(pDecoder, pNode->user);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = msgToQueryNodeAddrInline(pDecoder, &pNode->execNode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueBool(pDecoder, &pNode->showRewrite);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI32(pDecoder, &pNode->rowsThreshold);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueBool(pDecoder, &pNode->dynamicRowThreshold);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueBool(pDecoder, &pNode->isView);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueBool(pDecoder, &pNode->isAudit);
  }
  return code;
}

static int32_t msgToSubplan(STlvDecoder* pDecoder, void* pObj) {
  SSubplan* pNode = (SSubplan*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case SUBPLAN_CODE_INLINE_ATTRS:
        code = tlvDecodeObjFromTlv(pTlv, msgToSubplanInline, pNode);
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

enum { QUERY_PLAN_CODE_INLINE_ATTRS = 1, QUERY_PLAN_CODE_SUBPLANS };

static int32_t queryPlanInlineToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SQueryPlan* pNode = (const SQueryPlan*)pObj;

  int32_t code = tlvEncodeValueU64(pEncoder, pNode->queryId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeValueI32(pEncoder, pNode->numOfSubplans);
  }

  return code;
}

static int32_t queryPlanToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SQueryPlan* pNode = (const SQueryPlan*)pObj;

  int32_t code = tlvEncodeObj(pEncoder, QUERY_PLAN_CODE_INLINE_ATTRS, queryPlanInlineToMsg, pNode);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObj(pEncoder, QUERY_PLAN_CODE_SUBPLANS, nodeListToMsg, pNode->pSubplans);
  }

  return code;
}

static int32_t msgToQueryPlanInline(STlvDecoder* pDecoder, void* pObj) {
  SQueryPlan* pNode = (SQueryPlan*)pObj;

  int32_t code = tlvDecodeValueU64(pDecoder, &pNode->queryId);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvDecodeValueI32(pDecoder, &pNode->numOfSubplans);
  }

  return code;
}

static int32_t msgToQueryPlan(STlvDecoder* pDecoder, void* pObj) {
  SQueryPlan* pNode = (SQueryPlan*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  STlv*   pTlv = NULL;
  tlvForEach(pDecoder, pTlv, code) {
    switch (pTlv->type) {
      case QUERY_PLAN_CODE_INLINE_ATTRS:
        code = tlvDecodeObjFromTlv(pTlv, msgToQueryPlanInline, pNode);
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
  // nodesWarn("specificNodeToMsg node = %s, before tlv count = %d", nodesNodeName(nodeType(pObj)), pEncoder->tlvCount);
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
    case QUERY_NODE_DOWNSTREAM_SOURCE:
      code = downstreamSourceNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_LEFT_VALUE:
      break;
    case QUERY_NODE_WHEN_THEN:
      code = whenThenNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_CASE_WHEN:
      code = caseWhenNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN:
      code = physiTagScanNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN:
      code = physiScanNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN:
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
      code = physiMergeJoinNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN:
      code = physiHashJoinNodeToMsg(pObj, pEncoder);
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
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_MID_INTERVAL:
      code = physiIntervalNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_FILL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FILL:
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
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_EVENT:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_EVENT:
      code = physiEventWindowNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_COUNT:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_COUNT:
      code = physiCountWindowNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_PARTITION:
      code = physiPartitionNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION:
      code = physiStreamPartitionNodeToMsg(pObj, pEncoder);
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
    case QUERY_NODE_PHYSICAL_PLAN_GROUP_CACHE:
      code = physiGroupCacheNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_DYN_QUERY_CTRL:
      code = physiDynQueryCtrlNodeToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_SUBPLAN:
      code = subplanToMsg(pObj, pEncoder);
      break;
    case QUERY_NODE_PHYSICAL_PLAN:
      code = queryPlanToMsg(pObj, pEncoder);
      break;
    default:
      break;
  }
  if (TSDB_CODE_SUCCESS != code) {
    nodesError("specificNodeToMsg error node = %s", nodesNodeName(nodeType(pObj)));
  }
  // nodesWarn("specificNodeToMsg node = %s, after tlv count = %d", nodesNodeName(nodeType(pObj)), pEncoder->tlvCount);
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
    case QUERY_NODE_DOWNSTREAM_SOURCE:
      code = msgToDownstreamSourceNode(pDecoder, pObj);
    case QUERY_NODE_LEFT_VALUE:
      break;
    case QUERY_NODE_WHEN_THEN:
      code = msgToWhenThenNode(pDecoder, pObj);
      break;
    case QUERY_NODE_CASE_WHEN:
      code = msgToCaseWhenNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN:
      code = msgToPhysiTagScanNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN:
      code = msgToPhysiScanNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN:
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
      code = msgToPhysiMergeJoinNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN:
      code = msgToPhysiHashJoinNode(pDecoder, pObj);
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
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_MID_INTERVAL:
      code = msgToPhysiIntervalNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_FILL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FILL:
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
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_EVENT:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_EVENT:
      code = msgToPhysiEventWindowNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_COUNT:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_COUNT:
      code = msgToPhysiCountWindowNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_PARTITION:
      code = msgToPhysiPartitionNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION:
      code = msgToPhysiStreamPartitionNode(pDecoder, pObj);
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
    case QUERY_NODE_PHYSICAL_PLAN_GROUP_CACHE:
      code = msgToPhysiGroupCacheNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_DYN_QUERY_CTRL:
      code = msgToPhysiDynQueryCtrlNode(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_SUBPLAN:
      code = msgToSubplan(pDecoder, pObj);
      break;
    case QUERY_NODE_PHYSICAL_PLAN:
      code = msgToQueryPlan(pDecoder, pObj);
      break;
    default:
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
enum {
  SARRAY_CODE_CAPACITY = 1,
  SARRAY_CODE_ELEMSIZE,
  SARRAY_CODE_SIZE,
  SARRAY_CODE_PDATA
};

static int32_t SArrayToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SArray* pArray = (const SArray*)pObj;
  int32_t code = TSDB_CODE_SUCCESS;
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, SARRAY_CODE_CAPACITY, pArray->capacity);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, SARRAY_CODE_ELEMSIZE, pArray->elemSize);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, SARRAY_CODE_SIZE, pArray->size);
  }
  if (TSDB_CODE_SUCCESS == code && pArray->capacity * pArray->elemSize > 0 && pArray->pData != NULL) {
    code = tlvEncodeBinary(pEncoder, SARRAY_CODE_PDATA, pArray->pData, pArray->capacity * pArray->elemSize);
  }
  return code;
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

static int32_t msgToSArray(STlv* pTlv, void** pObj){
  SArray* pArray = NULL;
  uint32_t capacity = 0;
  uint32_t elemSize = 0;
  uint32_t actualSize;
  int32_t decodeFieldNum = 0;;
  int32_t code = TSDB_CODE_SUCCESS;
  STlvDecoder decoder = {.bufSize = pTlv->len, .offset = 0, .pBuf = pTlv->value};
  STlv*   pTlvTemp = NULL;
  STlv*   pDataTlv = NULL;

  tlvForEach(&decoder, pTlvTemp, code) {
    switch (pTlvTemp->type) {
      case SARRAY_CODE_CAPACITY:
        code = tlvDecodeI32(pTlvTemp, &capacity);
        break;
      case SARRAY_CODE_ELEMSIZE:
        code = tlvDecodeI32(pTlvTemp, &elemSize);
        break;
      case SARRAY_CODE_SIZE:
        code = tlvDecodeI32(pTlvTemp, &actualSize);
        break;
      case SARRAY_CODE_PDATA:
        if (decodeFieldNum < 3) {
          pDataTlv = pTlvTemp;
          break;
        }
        pArray = taosArrayInit(capacity, elemSize);
        if (NULL == pArray) {
          return TSDB_CODE_OUT_OF_MEMORY;
        }
        pArray->size = actualSize;
        if (TSDB_CODE_SUCCESS != code || pTlvTemp == NULL) {
          taosArrayDestroy(pArray);
          return TSDB_CODE_OUT_OF_MEMORY;
        }
        code = tlvDecodeBinary(pTlvTemp, pArray->pData);
        break;
      default:
        break;
    }
    decodeFieldNum++;
  }

  if (pDataTlv != NULL) {
    pArray = taosArrayInit(capacity, elemSize);
    if (NULL == pArray) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pArray->size = actualSize;
    if (TSDB_CODE_SUCCESS != code || pTlvTemp == NULL) {
      taosArrayDestroy(pArray);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    code = tlvDecodeBinary(pDataTlv, pArray->pData);
  }
  *pObj = pArray;
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
