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

#define tlvDecodeEnum(pTlv, value) tlvDecodeImpl(pTlv, &(value), sizeof(value))

#define tlvForEach(pDecoder, pTlv, code) \
  while (TSDB_CODE_SUCCESS == (code = tlvGetNextTlv(pDecoder, &pTlv)) && NULL != pTlv)

typedef struct STlv {
  int16_t type;
  int16_t len;
  char    value[0];
} STlv;

typedef struct STlvEncoder {
  int32_t allocSize;
  int32_t offset;
  char*   pBuf;
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
  pEncoder->pBuf = taosMemoryMalloc(pEncoder->allocSize);
  return NULL == pEncoder->pBuf ? TSDB_CODE_OUT_OF_MEMORY : TSDB_CODE_SUCCESS;
}

static void clearTlvEncoder(STlvEncoder* pEncoder) { taosMemoryFree(pEncoder->pBuf); }

static void endTlvEncode(STlvEncoder* pEncoder, char** pMsg, int32_t* pLen) {
  *pMsg = pEncoder->pBuf;
  pEncoder->pBuf = NULL;
  *pLen = pEncoder->offset;
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

static int32_t tlvEncodeU8(STlvEncoder* pEncoder, int16_t type, uint8_t value) {
  return tlvEncodeImpl(pEncoder, type, &value, sizeof(value));
}

static int32_t tlvEncodeU16(STlvEncoder* pEncoder, int16_t type, uint16_t value) {
  return tlvEncodeImpl(pEncoder, type, &value, sizeof(value));
}

static int32_t tlvEncodeU64(STlvEncoder* pEncoder, int16_t type, uint64_t value) {
  return tlvEncodeImpl(pEncoder, type, &value, sizeof(value));
}

static int32_t tlvEncodeCStr(STlvEncoder* pEncoder, int16_t type, const char* pValue) {
  return tlvEncodeImpl(pEncoder, type, pValue, strlen(pValue));
}

static int32_t tlvEncodeObjArray(STlvEncoder* pEncoder, int16_t type, FToMsg func, const void* pArray, int32_t itemSize,
                                 int32_t num) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (num > 0) {
    int32_t start = pEncoder->offset;
    pEncoder->offset += sizeof(STlv);
    for (size_t i = 0; TSDB_CODE_SUCCESS == code && i < num; ++i) {
      code = func(pArray + i * itemSize, pEncoder);
    }
    if (TSDB_CODE_SUCCESS == code) {
      STlv* pTlv = (STlv*)(pEncoder->pBuf + start);
      pTlv->type = type;
      pTlv->len = pEncoder->offset - start - sizeof(STlv);
    }
  }
  return code;
}

static int32_t tlvEncodeObj(STlvEncoder* pEncoder, int16_t type, FToMsg func, const void* pObj) {
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

static int32_t tlvGetNextTlv(STlvDecoder* pDecoder, STlv** pTlv) {
  if (pDecoder->offset == pDecoder->bufSize) {
    *pTlv = NULL;
    return TSDB_CODE_SUCCESS;
  }

  *pTlv = (STlv*)(pDecoder->pBuf + pDecoder->offset);
  if ((*pTlv)->len + pDecoder->offset > pDecoder->bufSize) {
    return TSDB_CODE_FAILED;
  }
  pDecoder->offset += (*pTlv)->len;
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

static int32_t tlvDecodeU8(STlv* pTlv, uint8_t* pValue) { return tlvDecodeImpl(pTlv, pValue, sizeof(*pValue)); }

static int32_t tlvDecodeU64(STlv* pTlv, uint64_t* pValue) { return tlvDecodeImpl(pTlv, pValue, sizeof(*pValue)); }

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

static void* makeNodeList(int16_t type) { return nodesMakeList(); }

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
        code = TSDB_CODE_FAILED;
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
        code = TSDB_CODE_FAILED;
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
    code = tlvEncodeI8(pEncoder, COLUMN_CODE_COLUMN_TYPE, pNode->colType);
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
        code = tlvDecodeEnum(pTlv, pNode->colType);
        break;
      case COLUMN_CODE_DATABLOCK_ID:
        code = tlvDecodeI16(pTlv, &pNode->dataBlockId);
        break;
      case COLUMN_CODE_SLOT_ID:
        code = tlvDecodeI16(pTlv, &pNode->slotId);
        break;
      default:
        code = TSDB_CODE_FAILED;
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

enum { EP_CODE_FQDN = 1, EP_CODE_port };

static int32_t epToMsg(const void* pObj, STlvEncoder* pEncoder) {
  const SEp* pNode = (const SEp*)pObj;

  int32_t code = tlvEncodeCStr(pEncoder, EP_CODE_FQDN, pNode->fqdn);
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeU16(pEncoder, EP_CODE_port, pNode->port);
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
    code = tlvEncodeI32(pEncoder, QUERY_NODE_ADDR_CODE_IN_USE, pNode->epSet.inUse);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeI32(pEncoder, QUERY_NODE_ADDR_CODE_NUM_OF_EPS, pNode->epSet.numOfEps);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tlvEncodeObjArray(pEncoder, QUERY_NODE_ADDR_CODE_EPS, epToMsg, pNode->epSet.eps, sizeof(SEp),
                             pNode->epSet.numOfEps);
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
    code = tlvEncodeI8(pEncoder, SUBPLAN_CODE_SUBPLAN_TYPE, pNode->subplanType);
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
        // code = tlvDecodeObjFromTlv(pTlv, msgToSubplanId, &pNode->id);
        break;
      case SUBPLAN_CODE_SUBPLAN_TYPE:
        code = tlvDecodeEnum(pTlv, pNode->subplanType);
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
        // code = tlvDecodeObjFromTlv(pTlv, msgToQueryNodeAddr, &pNode->execNode);
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
        code = TSDB_CODE_FAILED;
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
        code = TSDB_CODE_FAILED;
        break;
    }
  }

  return code;
}

static int32_t specificNodeToMsg(const void* pObj, STlvEncoder* pEncoder) {
  switch (nodeType(pObj)) {
    case QUERY_NODE_COLUMN:
      return columnNodeToMsg(pObj, pEncoder);
    case QUERY_NODE_PHYSICAL_SUBPLAN:
      return subplanToMsg(pObj, pEncoder);
    case QUERY_NODE_PHYSICAL_PLAN:
      return queryPlanToMsg(pObj, pEncoder);
    default:
      break;
  };
  nodesWarn("specificNodeToMsg unknown node = %s", nodesNodeName(nodeType(pObj)));
  return TSDB_CODE_SUCCESS;
}

static int32_t msgToSpecificNode(STlvDecoder* pDecoder, void* pObj) {
  switch (nodeType(pObj)) {
    case QUERY_NODE_COLUMN:
      return msgToColumnNode(pDecoder, pObj);
    case QUERY_NODE_PHYSICAL_SUBPLAN:
      return msgToSubplan(pDecoder, pObj);
    case QUERY_NODE_PHYSICAL_PLAN:
      return msgToQueryPlan(pDecoder, pObj);
    default:
      break;
  };
  nodesWarn("msgToSpecificNode unknown node = %s", nodesNodeName(nodeType(pObj)));
  return TSDB_CODE_SUCCESS;
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

static int32_t msgToNodeListImpl(STlvDecoder* pDecoder, void* pObj) {
  SNodeList* pList = (SNodeList*)pObj;

  int32_t code = TSDB_CODE_SUCCESS;
  while (TSDB_CODE_SUCCESS == code && !tlvDecodeEnd(pDecoder)) {
    SNode* pNode = NULL;
    code = msgToNode(pDecoder, (void**)&pNode);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListAppend(pList, pNode);
    }
  }
  return code;
}

static int32_t msgToNodeList(STlvDecoder* pDecoder, void** pObj) {
  return tlvDecodeDynObj(pDecoder, makeNodeList, msgToNodeListImpl, pObj);
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
