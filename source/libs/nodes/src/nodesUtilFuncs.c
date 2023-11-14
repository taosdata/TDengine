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

#include "cmdnodes.h"
#include "nodesUtil.h"
#include "plannodes.h"
#include "querynodes.h"
#include "taos.h"
#include "taoserror.h"
#include "tdatablock.h"
#include "thash.h"
#include "tref.h"
#include "functionMgt.h"

typedef struct SNodeMemChunk {
  int32_t               availableSize;
  int32_t               usedSize;
  char*                 pBuf;
  struct SNodeMemChunk* pNext;
} SNodeMemChunk;

typedef struct SNodeAllocator {
  int64_t        self;
  int64_t        queryId;
  int32_t        chunkSize;
  int32_t        chunkNum;
  SNodeMemChunk* pCurrChunk;
  SNodeMemChunk* pChunks;
  TdThreadMutex  mutex;
} SNodeAllocator;

static threadlocal SNodeAllocator* g_pNodeAllocator;
static int32_t                     g_allocatorReqRefPool = -1;

static SNodeMemChunk* callocNodeChunk(SNodeAllocator* pAllocator) {
  SNodeMemChunk* pNewChunk = taosMemoryCalloc(1, sizeof(SNodeMemChunk) + pAllocator->chunkSize);
  if (NULL == pNewChunk) {
    return NULL;
  }
  pNewChunk->pBuf = (char*)(pNewChunk + 1);
  pNewChunk->availableSize = pAllocator->chunkSize;
  pNewChunk->usedSize = 0;
  pNewChunk->pNext = NULL;
  if (NULL != pAllocator->pCurrChunk) {
    pAllocator->pCurrChunk->pNext = pNewChunk;
  }
  pAllocator->pCurrChunk = pNewChunk;
  if (NULL == pAllocator->pChunks) {
    pAllocator->pChunks = pNewChunk;
  }
  ++(pAllocator->chunkNum);
  return pNewChunk;
}

static void* nodesCallocImpl(int32_t size) {
  if (NULL == g_pNodeAllocator) {
    return taosMemoryCalloc(1, size);
  }

  if (g_pNodeAllocator->pCurrChunk->usedSize + size > g_pNodeAllocator->pCurrChunk->availableSize) {
    if (NULL == callocNodeChunk(g_pNodeAllocator)) {
      return NULL;
    }
  }
  void* p = g_pNodeAllocator->pCurrChunk->pBuf + g_pNodeAllocator->pCurrChunk->usedSize;
  g_pNodeAllocator->pCurrChunk->usedSize += size;
  return p;
}

static void* nodesCalloc(int32_t num, int32_t size) {
  void* p = nodesCallocImpl(num * size + 1);
  if (NULL == p) {
    return NULL;
  }
  *(char*)p = (NULL != g_pNodeAllocator) ? 1 : 0;
  return (char*)p + 1;
}

void nodesFree(void* p) {
  char* ptr = (char*)p - 1;
  if (0 == *ptr) {
    taosMemoryFree(ptr);
  }
  return;
}

static int32_t createNodeAllocator(int32_t chunkSize, SNodeAllocator** pAllocator) {
  *pAllocator = taosMemoryCalloc(1, sizeof(SNodeAllocator));
  if (NULL == *pAllocator) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  (*pAllocator)->chunkSize = chunkSize;
  if (NULL == callocNodeChunk(*pAllocator)) {
    taosMemoryFreeClear(*pAllocator);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  taosThreadMutexInit(&(*pAllocator)->mutex, NULL);
  return TSDB_CODE_SUCCESS;
}

static void destroyNodeAllocator(void* p) {
  if (NULL == p) {
    return;
  }

  SNodeAllocator* pAllocator = p;

  nodesDebug("query id %" PRIx64 " allocator id %" PRIx64 " alloc chunkNum: %d, chunkTotakSize: %d",
             pAllocator->queryId, pAllocator->self, pAllocator->chunkNum, pAllocator->chunkNum * pAllocator->chunkSize);

  SNodeMemChunk* pChunk = pAllocator->pChunks;
  while (NULL != pChunk) {
    SNodeMemChunk* pTemp = pChunk->pNext;
    taosMemoryFree(pChunk);
    pChunk = pTemp;
  }
  taosThreadMutexDestroy(&pAllocator->mutex);
  taosMemoryFree(pAllocator);
}

int32_t nodesInitAllocatorSet() {
  nodesInit();
  if (g_allocatorReqRefPool >= 0) {
    nodesWarn("nodes already initialized");
    return TSDB_CODE_SUCCESS;
  }

  g_allocatorReqRefPool = taosOpenRef(1024, destroyNodeAllocator);
  if (g_allocatorReqRefPool < 0) {
    nodesError("init nodes failed");
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

void nodesDestroyAllocatorSet() {
  if (g_allocatorReqRefPool >= 0) {
    SNodeAllocator* pAllocator = taosIterateRef(g_allocatorReqRefPool, 0);
    int64_t         refId = 0;
    while (NULL != pAllocator) {
      refId = pAllocator->self;
      taosRemoveRef(g_allocatorReqRefPool, refId);
      pAllocator = taosIterateRef(g_allocatorReqRefPool, refId);
    }
    taosCloseRef(g_allocatorReqRefPool);
  }
}

int32_t nodesCreateAllocator(int64_t queryId, int32_t chunkSize, int64_t* pAllocatorId) {
  SNodeAllocator* pAllocator = NULL;
  int32_t         code = createNodeAllocator(chunkSize, &pAllocator);
  if (TSDB_CODE_SUCCESS == code) {
    pAllocator->self = taosAddRef(g_allocatorReqRefPool, pAllocator);
    if (pAllocator->self <= 0) {
      return terrno;
    }
    pAllocator->queryId = queryId;
    *pAllocatorId = pAllocator->self;
  }
  return code;
}

int32_t nodesAcquireAllocator(int64_t allocatorId) {
  if (allocatorId <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  SNodeAllocator* pAllocator = taosAcquireRef(g_allocatorReqRefPool, allocatorId);
  if (NULL == pAllocator) {
    return terrno;
  }
  taosThreadMutexLock(&pAllocator->mutex);
  g_pNodeAllocator = pAllocator;
  return TSDB_CODE_SUCCESS;
}

int32_t nodesReleaseAllocator(int64_t allocatorId) {
  if (allocatorId <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (NULL == g_pNodeAllocator) {
    nodesError("allocator id %" PRIx64
               " release failed: The nodesReleaseAllocator function needs to be called after the nodesAcquireAllocator "
               "function is called!",
               allocatorId);
    return TSDB_CODE_FAILED;
  }
  SNodeAllocator* pAllocator = g_pNodeAllocator;
  g_pNodeAllocator = NULL;
  taosThreadMutexUnlock(&pAllocator->mutex);
  return taosReleaseRef(g_allocatorReqRefPool, allocatorId);
}

int64_t nodesMakeAllocatorWeakRef(int64_t allocatorId) {
  if (allocatorId <= 0) {
    return 0;
  }

  SNodeAllocator* pAllocator = taosAcquireRef(g_allocatorReqRefPool, allocatorId);
  if (NULL == pAllocator) {
    nodesError("allocator id %" PRIx64 " weak reference failed", allocatorId);
    return -1;
  }
  return pAllocator->self;
}

int64_t nodesReleaseAllocatorWeakRef(int64_t allocatorId) { return taosReleaseRef(g_allocatorReqRefPool, allocatorId); }

void nodesDestroyAllocator(int64_t allocatorId) {
  if (allocatorId <= 0) {
    return;
  }

  taosRemoveRef(g_allocatorReqRefPool, allocatorId);
}

static SNode* makeNode(ENodeType type, int32_t size) {
  SNode* p = nodesCalloc(1, size);
  if (NULL == p) {
    return NULL;
  }
  setNodeType(p, type);
  return p;
}

SNode* nodesMakeNode(ENodeType type) {
  int32_t size = getNodeSize(type);
  if (size > 0) {
    return makeNode(type, size);
  }
  nodesError("nodesMakeNode unsupported type = %d", type);
  return NULL;
}


SNodeList* nodesMakeList() {
  SNodeList* p = nodesCalloc(1, sizeof(SNodeList));
  if (NULL == p) {
    return NULL;
  }
  return p;
}

int32_t nodesListAppend(SNodeList* pList, SNode* pNode) {
  if (NULL == pList || NULL == pNode) {
    return TSDB_CODE_FAILED;
  }
  SListCell* p = nodesCalloc(1, sizeof(SListCell));
  if (NULL == p) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  p->pNode = pNode;
  if (NULL == pList->pHead) {
    pList->pHead = p;
  }
  if (NULL != pList->pTail) {
    pList->pTail->pNext = p;
  }
  p->pPrev = pList->pTail;
  pList->pTail = p;
  ++(pList->length);
  return TSDB_CODE_SUCCESS;
}

int32_t nodesListStrictAppend(SNodeList* pList, SNode* pNode) {
  if (NULL == pNode) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  int32_t code = nodesListAppend(pList, pNode);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pNode);
  }
  return code;
}

int32_t nodesListMakeAppend(SNodeList** pList, SNode* pNode) {
  if (NULL == *pList) {
    *pList = nodesMakeList();
    if (NULL == *pList) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return nodesListAppend(*pList, pNode);
}

int32_t nodesListMakeStrictAppend(SNodeList** pList, SNode* pNode) {
  if (NULL == *pList) {
    *pList = nodesMakeList();
    if (NULL == *pList) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return nodesListStrictAppend(*pList, pNode);
}

int32_t nodesListAppendList(SNodeList* pTarget, SNodeList* pSrc) {
  if (NULL == pTarget || NULL == pSrc) {
    return TSDB_CODE_FAILED;
  }

  if (NULL == pTarget->pHead) {
    pTarget->pHead = pSrc->pHead;
  } else {
    pTarget->pTail->pNext = pSrc->pHead;
    if (NULL != pSrc->pHead) {
      pSrc->pHead->pPrev = pTarget->pTail;
    }
  }
  pTarget->pTail = pSrc->pTail;
  pTarget->length += pSrc->length;
  nodesFree(pSrc);

  return TSDB_CODE_SUCCESS;
}

int32_t nodesListStrictAppendList(SNodeList* pTarget, SNodeList* pSrc) {
  if (NULL == pSrc) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  int32_t code = nodesListAppendList(pTarget, pSrc);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyList(pSrc);
  }
  return code;
}


int32_t nodesListMakeStrictAppendList(SNodeList** pTarget, SNodeList* pSrc) {
  if (NULL == *pTarget) {
    *pTarget = nodesMakeList();
    if (NULL == *pTarget) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return nodesListStrictAppendList(*pTarget, pSrc);
}


int32_t nodesListPushFront(SNodeList* pList, SNode* pNode) {
  if (NULL == pList || NULL == pNode) {
    return TSDB_CODE_FAILED;
  }
  SListCell* p = nodesCalloc(1, sizeof(SListCell));
  if (NULL == p) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  p->pNode = pNode;
  if (NULL != pList->pHead) {
    pList->pHead->pPrev = p;
    p->pNext = pList->pHead;
  }
  pList->pHead = p;
  ++(pList->length);
  return TSDB_CODE_SUCCESS;
}

SListCell* nodesListErase(SNodeList* pList, SListCell* pCell) {
  if (NULL == pCell->pPrev) {
    pList->pHead = pCell->pNext;
  } else {
    pCell->pPrev->pNext = pCell->pNext;
  }
  if (NULL == pCell->pNext) {
    pList->pTail = pCell->pPrev;
  } else {
    pCell->pNext->pPrev = pCell->pPrev;
  }
  SListCell* pNext = pCell->pNext;
  nodesDestroyNode(pCell->pNode);
  nodesFree(pCell);
  --(pList->length);
  return pNext;
}

void nodesListInsertList(SNodeList* pTarget, SListCell* pPos, SNodeList* pSrc) {
  if (NULL == pTarget || NULL == pPos || NULL == pSrc || NULL == pSrc->pHead) {
    return;
  }

  if (NULL == pPos->pPrev) {
    pTarget->pHead = pSrc->pHead;
  } else {
    pPos->pPrev->pNext = pSrc->pHead;
  }
  pSrc->pHead->pPrev = pPos->pPrev;
  pSrc->pTail->pNext = pPos;
  pPos->pPrev = pSrc->pTail;

  pTarget->length += pSrc->length;
  nodesFree(pSrc);
}

void nodesListInsertListAfterPos(SNodeList* pTarget, SListCell* pPos, SNodeList* pSrc) {
  if (NULL == pTarget || NULL == pPos || NULL == pSrc || NULL == pSrc->pHead) {
    return;
  }

  if (NULL == pPos->pNext) {
    pTarget->pTail = pSrc->pHead;
  } else {
    pPos->pNext->pPrev = pSrc->pHead;
  }

  pSrc->pHead->pPrev = pPos;
  pSrc->pTail->pNext = pPos->pNext;

  pPos->pNext = pSrc->pHead;
  
  pTarget->length += pSrc->length;
  nodesFree(pSrc);  
}

SNode* nodesListGetNode(SNodeList* pList, int32_t index) {
  SNode* node;
  FOREACH(node, pList) {
    if (0 == index--) {
      return node;
    }
  }
  return NULL;
}

SListCell* nodesListGetCell(SNodeList* pList, int32_t index) {
  SNode* node;
  FOREACH(node, pList) {
    if (0 == index--) {
      return cell;
    }
  }
  return NULL;
}

void nodesDestroyList(SNodeList* pList) {
  if (NULL == pList) {
    return;
  }

  SListCell* pNext = pList->pHead;
  while (NULL != pNext) {
    pNext = nodesListErase(pList, pNext);
  }
  nodesFree(pList);
}

void nodesClearList(SNodeList* pList) {
  if (NULL == pList) {
    return;
  }

  SListCell* pNext = pList->pHead;
  while (NULL != pNext) {
    SListCell* tmp = pNext;
    pNext = pNext->pNext;
    nodesFree(tmp);
  }
  nodesFree(pList);
}

void* nodesGetValueFromNode(SValueNode* pNode) {
  switch (pNode->node.resType.type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
      return (void*)&pNode->typeData;
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_GEOMETRY:
      return (void*)pNode->datum.p;
    default:
      break;
  }

  return NULL;
}

int32_t nodesSetValueNodeValue(SValueNode* pNode, void* value) {
  switch (pNode->node.resType.type) {
    case TSDB_DATA_TYPE_BOOL:
      pNode->datum.b = *(bool*)value;
      *(bool*)&pNode->typeData = pNode->datum.b;
      break;
    case TSDB_DATA_TYPE_TINYINT:
      pNode->datum.i = *(int8_t*)value;
      *(int8_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      pNode->datum.i = *(int16_t*)value;
      *(int16_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_INT:
      pNode->datum.i = *(int32_t*)value;
      *(int32_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      pNode->datum.i = *(int64_t*)value;
      *(int64_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      pNode->datum.i = *(int64_t*)value;
      *(int64_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      pNode->datum.u = *(int8_t*)value;
      *(int8_t*)&pNode->typeData = pNode->datum.u;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      pNode->datum.u = *(int16_t*)value;
      *(int16_t*)&pNode->typeData = pNode->datum.u;
      break;
    case TSDB_DATA_TYPE_UINT:
      pNode->datum.u = *(int32_t*)value;
      *(int32_t*)&pNode->typeData = pNode->datum.u;
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      pNode->datum.u = *(uint64_t*)value;
      *(uint64_t*)&pNode->typeData = pNode->datum.u;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      pNode->datum.d = *(float*)value;
      *(float*)&pNode->typeData = pNode->datum.d;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      pNode->datum.d = *(double*)value;
      *(double*)&pNode->typeData = pNode->datum.d;
      break;
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_GEOMETRY:
      pNode->datum.p = (char*)value;
      break;
    default:
      return TSDB_CODE_APP_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}

char* nodesGetStrValueFromNode(SValueNode* pNode) {
  switch (pNode->node.resType.type) {
    case TSDB_DATA_TYPE_BOOL: {
      void* buf = taosMemoryMalloc(MAX_NUM_STR_SIZE);
      if (NULL == buf) {
        return NULL;
      }

      sprintf(buf, "%s", pNode->datum.b ? "true" : "false");
      return buf;
    }
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP: {
      void* buf = taosMemoryMalloc(MAX_NUM_STR_SIZE);
      if (NULL == buf) {
        return NULL;
      }

      sprintf(buf, "%" PRId64, pNode->datum.i);
      return buf;
    }
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT: {
      void* buf = taosMemoryMalloc(MAX_NUM_STR_SIZE);
      if (NULL == buf) {
        return NULL;
      }

      sprintf(buf, "%" PRIu64, pNode->datum.u);
      return buf;
    }
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE: {
      void* buf = taosMemoryMalloc(MAX_NUM_STR_SIZE);
      if (NULL == buf) {
        return NULL;
      }

      sprintf(buf, "%e", pNode->datum.d);
      return buf;
    }
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_GEOMETRY: {
      int32_t bufSize = varDataLen(pNode->datum.p) + 2 + 1;
      void*   buf = taosMemoryMalloc(bufSize);
      if (NULL == buf) {
        return NULL;
      }

      snprintf(buf, bufSize, "'%s'", varDataVal(pNode->datum.p));
      return buf;
    }
    default:
      break;
  }

  return NULL;
}

bool nodesIsExprNode(const SNode* pNode) {
  ENodeType type = nodeType(pNode);
  return (QUERY_NODE_COLUMN == type || QUERY_NODE_VALUE == type || QUERY_NODE_OPERATOR == type ||
          QUERY_NODE_FUNCTION == type || QUERY_NODE_LOGIC_CONDITION == type || QUERY_NODE_CASE_WHEN == type);
}

bool nodesIsUnaryOp(const SOperatorNode* pOp) {
  switch (pOp->opType) {
    case OP_TYPE_MINUS:
    case OP_TYPE_IS_NULL:
    case OP_TYPE_IS_NOT_NULL:
    case OP_TYPE_IS_TRUE:
    case OP_TYPE_IS_FALSE:
    case OP_TYPE_IS_UNKNOWN:
    case OP_TYPE_IS_NOT_TRUE:
    case OP_TYPE_IS_NOT_FALSE:
    case OP_TYPE_IS_NOT_UNKNOWN:
      return true;
    default:
      break;
  }
  return false;
}

bool nodesIsArithmeticOp(const SOperatorNode* pOp) {
  switch (pOp->opType) {
    case OP_TYPE_ADD:
    case OP_TYPE_SUB:
    case OP_TYPE_MULTI:
    case OP_TYPE_DIV:
    case OP_TYPE_REM:
      return true;
    default:
      break;
  }
  return false;
}

bool nodesIsComparisonOp(const SOperatorNode* pOp) {
  switch (pOp->opType) {
    case OP_TYPE_GREATER_THAN:
    case OP_TYPE_GREATER_EQUAL:
    case OP_TYPE_LOWER_THAN:
    case OP_TYPE_LOWER_EQUAL:
    case OP_TYPE_EQUAL:
    case OP_TYPE_NOT_EQUAL:
    case OP_TYPE_IN:
    case OP_TYPE_NOT_IN:
    case OP_TYPE_LIKE:
    case OP_TYPE_NOT_LIKE:
    case OP_TYPE_MATCH:
    case OP_TYPE_NMATCH:
    case OP_TYPE_JSON_CONTAINS:
    case OP_TYPE_IS_NULL:
    case OP_TYPE_IS_NOT_NULL:
    case OP_TYPE_IS_TRUE:
    case OP_TYPE_IS_FALSE:
    case OP_TYPE_IS_UNKNOWN:
    case OP_TYPE_IS_NOT_TRUE:
    case OP_TYPE_IS_NOT_FALSE:
    case OP_TYPE_IS_NOT_UNKNOWN:
      return true;
    default:
      break;
  }
  return false;
}

bool nodesIsJsonOp(const SOperatorNode* pOp) {
  switch (pOp->opType) {
    case OP_TYPE_JSON_GET_VALUE:
    case OP_TYPE_JSON_CONTAINS:
      return true;
    default:
      break;
  }
  return false;
}

bool nodesIsRegularOp(const SOperatorNode* pOp) {
  switch (pOp->opType) {
    case OP_TYPE_LIKE:
    case OP_TYPE_NOT_LIKE:
    case OP_TYPE_MATCH:
    case OP_TYPE_NMATCH:
      return true;
    default:
      break;
  }
  return false;
}

bool nodesIsBitwiseOp(const SOperatorNode* pOp) {
  switch (pOp->opType) {
    case OP_TYPE_BIT_AND:
    case OP_TYPE_BIT_OR:
      return true;
    default:
      break;
  }
  return false;
}

typedef struct SCollectColumnsCxt {
  int32_t         errCode;
  const char*     pTableAlias;
  ECollectColType collectType;
  SNodeList*      pCols;
  SHashObj*       pColHash;
} SCollectColumnsCxt;

static EDealRes doCollect(SCollectColumnsCxt* pCxt, SColumnNode* pCol, SNode* pNode) {
  char    name[TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN];
  int32_t len = 0;
  if ('\0' == pCol->tableAlias[0]) {
    len = snprintf(name, sizeof(name), "%s", pCol->colName);
  } else {
    len = snprintf(name, sizeof(name), "%s.%s", pCol->tableAlias, pCol->colName);
  }
  if (NULL == taosHashGet(pCxt->pColHash, name, len)) {
    pCxt->errCode = taosHashPut(pCxt->pColHash, name, len, NULL, 0);
    if (TSDB_CODE_SUCCESS == pCxt->errCode) {
      pCxt->errCode = nodesListStrictAppend(pCxt->pCols, nodesCloneNode(pNode));
    }
    return (TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_IGNORE_CHILD : DEAL_RES_ERROR);
  }
  return DEAL_RES_CONTINUE;
}

static bool isCollectType(ECollectColType collectType, EColumnType colType) {
  return COLLECT_COL_TYPE_ALL == collectType
             ? true
             : (COLLECT_COL_TYPE_TAG == collectType ? COLUMN_TYPE_TAG == colType : (COLUMN_TYPE_TAG != colType && COLUMN_TYPE_TBNAME != colType));
}

static EDealRes collectColumns(SNode* pNode, void* pContext) {
  SCollectColumnsCxt* pCxt = (SCollectColumnsCxt*)pContext;
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    if (isCollectType(pCxt->collectType, pCol->colType) && 0 != strcmp(pCol->colName, "*") &&
        (NULL == pCxt->pTableAlias || 0 == strcmp(pCxt->pTableAlias, pCol->tableAlias))) {
      return doCollect(pCxt, pCol, pNode);
    }
  }
  return DEAL_RES_CONTINUE;
}

int32_t nodesCollectColumns(SSelectStmt* pSelect, ESqlClause clause, const char* pTableAlias, ECollectColType type,
                            SNodeList** pCols) {
  if (NULL == pSelect || NULL == pCols) {
    return TSDB_CODE_FAILED;
  }

  SCollectColumnsCxt cxt = {
      .errCode = TSDB_CODE_SUCCESS,
      .pTableAlias = pTableAlias,
      .collectType = type,
      .pCols = (NULL == *pCols ? nodesMakeList() : *pCols),
      .pColHash = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK)};
  if (NULL == cxt.pCols || NULL == cxt.pColHash) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  *pCols = NULL;
  nodesWalkSelectStmt(pSelect, clause, collectColumns, &cxt);
  taosHashCleanup(cxt.pColHash);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(cxt.pCols);
    return cxt.errCode;
  }
  if (LIST_LENGTH(cxt.pCols) > 0) {
    *pCols = cxt.pCols;
  } else {
    nodesDestroyList(cxt.pCols);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t nodesCollectColumnsFromNode(SNode* node, const char* pTableAlias, ECollectColType type, SNodeList** pCols) {
  if (NULL == pCols) {
    return TSDB_CODE_FAILED;
  }
  SCollectColumnsCxt cxt = {
      .errCode = TSDB_CODE_SUCCESS,
      .pTableAlias = pTableAlias,
      .collectType = type,
      .pCols = (NULL == *pCols ? nodesMakeList() : *pCols),
      .pColHash = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK)};
  if (NULL == cxt.pCols || NULL == cxt.pColHash) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  *pCols = NULL;

  nodesWalkExpr(node, collectColumns, &cxt);

  taosHashCleanup(cxt.pColHash);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(cxt.pCols);
    return cxt.errCode;
  }
  if (LIST_LENGTH(cxt.pCols) > 0) {
    *pCols = cxt.pCols;
  } else {
    nodesDestroyList(cxt.pCols);
  }

  return TSDB_CODE_SUCCESS;
}

typedef struct SCollectFuncsCxt {
  int32_t         errCode;
  char*           tableAlias;
  FFuncClassifier classifier;
  SNodeList*      pFuncs;
} SCollectFuncsCxt;

static EDealRes collectFuncs(SNode* pNode, void* pContext) {
  SCollectFuncsCxt* pCxt = (SCollectFuncsCxt*)pContext;
  if (QUERY_NODE_FUNCTION == nodeType(pNode) && pCxt->classifier(((SFunctionNode*)pNode)->funcId) &&
      !(((SExprNode*)pNode)->orderAlias)) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (FUNCTION_TYPE_TBNAME == pFunc->funcType && pCxt->tableAlias) {
      SValueNode* pVal = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 0);
      if (pVal && strcmp(pVal->literal, pCxt->tableAlias)) {
        return DEAL_RES_CONTINUE;
      }
    }
    SExprNode* pExpr = (SExprNode*)pNode;
    bool bFound = false;
    SNode* pn = NULL;
    FOREACH(pn, pCxt->pFuncs) {
      if (nodesEqualNode(pn, pNode)) {
        bFound = true;
      }
    }
    if (!bFound) {
      pCxt->errCode = nodesListStrictAppend(pCxt->pFuncs, nodesCloneNode(pNode));
    }
    return (TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_IGNORE_CHILD : DEAL_RES_ERROR);
  }
  return DEAL_RES_CONTINUE;
}

static uint32_t funcNodeHash(const char* pKey, uint32_t len) {
  SExprNode* pExpr = *(SExprNode**)pKey;
  return MurmurHash3_32(pExpr->aliasName, strlen(pExpr->aliasName));
}

static int32_t funcNodeEqual(const void* pLeft, const void* pRight, size_t len) {
  // if (0 != strcmp((*(const SExprNode**)pLeft)->aliasName, (*(const SExprNode**)pRight)->aliasName)) {
  //   return 1;
  // }
  return nodesEqualNode(*(const SNode**)pLeft, *(const SNode**)pRight) ? 0 : 1;
}

int32_t nodesCollectFuncs(SSelectStmt* pSelect, ESqlClause clause, char* tableAlias, FFuncClassifier classifier, SNodeList** pFuncs) {
  if (NULL == pSelect || NULL == pFuncs) {
    return TSDB_CODE_FAILED;
  }

  SCollectFuncsCxt cxt = {.errCode = TSDB_CODE_SUCCESS,
                          .classifier = classifier,
                          .tableAlias = tableAlias,
                          .pFuncs = (NULL == *pFuncs ? nodesMakeList() : *pFuncs)};
  if (NULL == cxt.pFuncs) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  *pFuncs = NULL;
  nodesWalkSelectStmt(pSelect, clause, collectFuncs, &cxt);
  if (TSDB_CODE_SUCCESS == cxt.errCode) {
    if (LIST_LENGTH(cxt.pFuncs) > 0) {
      *pFuncs = cxt.pFuncs;
    } else {
      nodesDestroyList(cxt.pFuncs);
    }
  } else {
    nodesDestroyList(cxt.pFuncs);
  }

  return cxt.errCode;
}

typedef struct SCollectSpecialNodesCxt {
  int32_t    errCode;
  ENodeType  type;
  SNodeList* pNodes;
} SCollectSpecialNodesCxt;

static EDealRes collectSpecialNodes(SNode* pNode, void* pContext) {
  SCollectSpecialNodesCxt* pCxt = (SCollectSpecialNodesCxt*)pContext;
  if (pCxt->type == nodeType(pNode)) {
    pCxt->errCode = nodesListStrictAppend(pCxt->pNodes, nodesCloneNode(pNode));
    return (TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_IGNORE_CHILD : DEAL_RES_ERROR);
  }
  return DEAL_RES_CONTINUE;
}

int32_t nodesCollectSpecialNodes(SSelectStmt* pSelect, ESqlClause clause, ENodeType type, SNodeList** pNodes) {
  if (NULL == pSelect || NULL == pNodes) {
    return TSDB_CODE_FAILED;
  }

  SCollectSpecialNodesCxt cxt = {
      .errCode = TSDB_CODE_SUCCESS, .type = type, .pNodes = (NULL == *pNodes ? nodesMakeList() : *pNodes)};
  if (NULL == cxt.pNodes) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  *pNodes = NULL;
  nodesWalkSelectStmt(pSelect, SQL_CLAUSE_GROUP_BY, collectSpecialNodes, &cxt);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(cxt.pNodes);
    return cxt.errCode;
  }
  if (LIST_LENGTH(cxt.pNodes) > 0) {
    *pNodes = cxt.pNodes;
  } else {
    nodesDestroyList(cxt.pNodes);
  }

  return TSDB_CODE_SUCCESS;
}

static EDealRes hasColumn(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    *(bool*)pContext = true;
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

bool nodesExprHasColumn(SNode* pNode) {
  bool hasCol = false;
  nodesWalkExprPostOrder(pNode, hasColumn, &hasCol);
  return hasCol;
}

bool nodesExprsHasColumn(SNodeList* pList) {
  bool hasCol = false;
  nodesWalkExprsPostOrder(pList, hasColumn, &hasCol);
  return hasCol;
}

char* nodesGetFillModeString(EFillMode mode) {
  switch (mode) {
    case FILL_MODE_NONE:
      return "none";
    case FILL_MODE_VALUE:
      return "value";
    case FILL_MODE_VALUE_F:
      return "value_f";
    case FILL_MODE_PREV:
      return "prev";
    case FILL_MODE_NULL:
      return "null";
    case FILL_MODE_NULL_F:
      return "null_f";
    case FILL_MODE_LINEAR:
      return "linear";
    case FILL_MODE_NEXT:
      return "next";
    default:
      return "unknown";
  }
}

char* nodesGetNameFromColumnNode(SNode* pNode) {
  if (NULL == pNode || QUERY_NODE_COLUMN != pNode->type) {
    return "NULL";
  }

  return ((SColumnNode*)pNode)->node.userAlias;
}

int32_t nodesGetOutputNumFromSlotList(SNodeList* pSlots) {
  if (NULL == pSlots || pSlots->length <= 0) {
    return 0;
  }

  SNode*  pNode = NULL;
  int32_t num = 0;
  FOREACH(pNode, pSlots) {
    if (QUERY_NODE_SLOT_DESC != pNode->type) {
      continue;
    }

    SSlotDescNode* descNode = (SSlotDescNode*)pNode;
    if (descNode->output) {
      ++num;
    }
  }

  return num;
}

void nodesValueNodeToVariant(const SValueNode* pNode, SVariant* pVal) {
  if (pNode->isNull) {
    pVal->nType = TSDB_DATA_TYPE_NULL;
    pVal->nLen = tDataTypes[TSDB_DATA_TYPE_NULL].bytes;
    return;
  }
  pVal->nType = pNode->node.resType.type;
  pVal->nLen = pNode->node.resType.bytes;
  switch (pNode->node.resType.type) {
    case TSDB_DATA_TYPE_NULL:
      break;
    case TSDB_DATA_TYPE_BOOL:
      pVal->i = pNode->datum.b;
      break;
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      pVal->i = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
      pVal->u = pNode->datum.u;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      pVal->f = pNode->datum.d;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      pVal->d = pNode->datum.d;
      break;
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_GEOMETRY:
      pVal->pz = taosMemoryMalloc(pVal->nLen + 1);
      memcpy(pVal->pz, pNode->datum.p, pVal->nLen);
      pVal->pz[pVal->nLen] = 0;
      break;
    case TSDB_DATA_TYPE_JSON:
      pVal->nLen = getJsonValueLen(pNode->datum.p);
      pVal->pz = taosMemoryMalloc(pVal->nLen);
      memcpy(pVal->pz, pNode->datum.p, pVal->nLen);
      break;
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_BLOB:
      // todo
    default:
      break;
  }
}

int32_t nodesMergeConds(SNode** pDst, SNodeList** pSrc) {
  if (NULL == *pSrc) {
    return TSDB_CODE_SUCCESS;
  }

  if (1 == LIST_LENGTH(*pSrc)) {
    *pDst = nodesListGetNode(*pSrc, 0);
    nodesClearList(*pSrc);
  } else {
    SLogicConditionNode* pLogicCond = (SLogicConditionNode*)nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
    if (NULL == pLogicCond) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pLogicCond->node.resType.type = TSDB_DATA_TYPE_BOOL;
    pLogicCond->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
    pLogicCond->condType = LOGIC_COND_TYPE_AND;
    pLogicCond->pParameterList = *pSrc;
    *pDst = (SNode*)pLogicCond;
  }
  *pSrc = NULL;

  return TSDB_CODE_SUCCESS;
}

const char* dataOrderStr(EDataOrderLevel order) {
  switch (order) {
    case DATA_ORDER_LEVEL_NONE:
      return "no order required";
    case DATA_ORDER_LEVEL_IN_BLOCK:
      return "in-datablock order";
    case DATA_ORDER_LEVEL_IN_GROUP:
      return "in-group order";
    case DATA_ORDER_LEVEL_GLOBAL:
      return "global order";
    default:
      break;
  }
  return "unknown";
}

SValueNode* nodesMakeValueNodeFromString(char* literal) {
  int32_t lenStr = strlen(literal);
  SValueNode* pValNode = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  if (pValNode) {
    pValNode->node.resType.type = TSDB_DATA_TYPE_VARCHAR;
    pValNode->node.resType.bytes = lenStr + VARSTR_HEADER_SIZE;
    char* p = taosMemoryMalloc(lenStr + 1  + VARSTR_HEADER_SIZE);
    if (p == NULL) {
      return NULL;
    }
    varDataSetLen(p, lenStr);
    memcpy(varDataVal(p), literal, lenStr + 1);
    pValNode->datum.p = p;
    pValNode->literal = tstrdup(literal);
    pValNode->translate = true;
    pValNode->isDuration = false;
    pValNode->isNull = false;
  }
  return pValNode;
}

SValueNode* nodesMakeValueNodeFromBool(bool b) {
  SValueNode* pValNode = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  if (pValNode) {
    pValNode->node.resType.type = TSDB_DATA_TYPE_BOOL;
    pValNode->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
    nodesSetValueNodeValue(pValNode, &b);
    pValNode->translate = true;
    pValNode->isDuration = false;
    pValNode->isNull = false;
  }
  return pValNode;
}

bool nodesIsStar(SNode* pNode) {
  return (QUERY_NODE_COLUMN == nodeType(pNode)) && ('\0' == ((SColumnNode*)pNode)->tableAlias[0]) &&
         (0 == strcmp(((SColumnNode*)pNode)->colName, "*"));
}

bool nodesIsTableStar(SNode* pNode) {
  return (QUERY_NODE_COLUMN == nodeType(pNode)) && ('\0' != ((SColumnNode*)pNode)->tableAlias[0]) &&
         (0 == strcmp(((SColumnNode*)pNode)->colName, "*"));
}
