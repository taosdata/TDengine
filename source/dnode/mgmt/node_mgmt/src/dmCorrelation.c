/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#define _DEFAULT_SOURCE
#include "dmCorrelation.h"

#include "nodes.h"
#include "osMemPool.h"
#include "planner.h"
#include "scheduler.h"
#include "tcompression.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tmsg.h"
#include "tuuid.h"

typedef struct SCorrelationRootState {
  SCorrelationRootKey key;
  int32_t refs;
  int32_t activeCalls;
  struct SCorrelationRootState* pNext;
} SCorrelationRootState;

struct SCorrelationRuntime {
  void*                    clientRpc;
  volatile int32_t         refs;
  volatile int32_t         closing;
  TdThreadMutex            lock;
  tsem_t                   drained;
  int32_t                  activeCalls;
  int32_t                  blockingCalls;
  SCorrelationRootState*   pRoots;
  SCorrelationTaskSession* pSessions;
};

enum {
  CORRELATION_MAX_ROOT_CALLS = 32,
  CORRELATION_MAX_BLOCKING_CALLS = 64,
};

struct SCorrelationTaskSession {
  volatile int32_t       refs;
  TdThreadMutex          lock;
  bool                   closed;
  int32_t                closeCode;
  uint64_t               nextSequence;
  SCorrelationRuntime*   pRuntime;
  SCorrelationTaskContext context;
  char                   timezoneName[TD_TIMEZONE_LEN];
  char                   charsetName[TD_LOCALE_LEN];
  SNodeList*             pTemplates;
  SArray*                pInheritedParams;
  SArray*                pNodeList;
  SCorrelationCall*      pCalls;
  SCorrelationRootState* pRoot;
  SCorrelationTaskSession* pRuntimeNext;
  int64_t                chargedBytes;
};

typedef enum {
  CORRELATION_CALL_CREATED,
  CORRELATION_CALL_SUBMITTING,
  CORRELATION_CALL_JOB_PUBLISHED,
  CORRELATION_CALL_FETCHING,
  CORRELATION_CALL_TERMINAL,
  CORRELATION_CALL_DRAINING,
} ECorrelationCallState;

struct SCorrelationCall {
  TdThreadMutex          lock;
  tsem_t                 ready;
  volatile int32_t       refs;
  SCorrelationCallKey    key;
  SCorrelationTaskSession* pSession;
  SCorrelationCall*      pNext;
  int64_t                jobId;
  int32_t                code;
  ECorrelationCallState  state;
  bool                   execDone;
  bool                   fetchInFlight;
  bool                   fetchDone;
  bool                   jobReleased;
  bool                   cancelled;
  bool                   activeReserved;
  bool                   blockingReserved;
  SRetrieveTableRsp*     pFetchRsp;
  int64_t                chargedBytes;
  int64_t                fetchCharge;
};

static int32_t dmReserveCorrelationMemory(void* pJob, int64_t bytes) {
  if (bytes < 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  return NULL == gMemPoolHandle ? TSDB_CODE_SUCCESS
                                : taosMemPoolReserveJobMemory(gMemPoolHandle, pJob, bytes);
}

static void dmFreeCorrelationParamValue(void* pElement) {
  SCorrelationParamValue* pValue = pElement;
  if (NULL != pValue) {
    taosMemoryFree(pValue->pData);
  }
}

static void dmFreeCorrelationWireValue(void* pElement) {
  SCorrelationWireValue* pValue = pElement;
  if (NULL != pValue) {
    taosMemoryFree(pValue->pData);
  }
}

static void dmRetainCorrelationRuntime(SCorrelationRuntime* pRuntime) {
  (void)atomic_add_fetch_32(&pRuntime->refs, 1);
}

static void dmReleaseCorrelationRuntime(SCorrelationRuntime* pRuntime) {
  if (NULL != pRuntime && 0 == atomic_sub_fetch_32(&pRuntime->refs, 1)) {
    int32_t code = tsem_destroy(&pRuntime->drained);
    if (TSDB_CODE_SUCCESS != code) {
      dError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    }
    (void)taosThreadMutexDestroy(&pRuntime->lock);
    taosMemFree(pRuntime);
  }
}

static int32_t dmCorrelationSessionCharge(const SCorrelationTaskContext* pContext, int64_t* pBytes) {
  SNodeListNode templates = {0};
  templates.node.type = QUERY_NODE_NODE_LIST;
  templates.pNodeList = (SNodeList*)pContext->pTemplates;
  char* pEncoded = NULL;
  int32_t encodedLen = 0;
  int32_t code = nodesNodeToMsg((SNode*)&templates, &pEncoded, &encodedLen);
  taosMemoryFree(pEncoded);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  if (encodedLen <= 0) {
    return TSDB_CODE_QRY_INVALID_CORRELATION_NODE;
  }

  int64_t bytes = sizeof(SCorrelationTaskSession) + sizeof(SCorrelationRootState) + encodedLen;
  if (NULL != pContext->pNodeList) {
    int64_t nodeBytes = sizeof(SArray) + (int64_t)taosArrayGetSize(pContext->pNodeList) * sizeof(SQueryNodeLoad);
    if (bytes > INT64_MAX - nodeBytes) {
      return TSDB_CODE_QRY_INVALID_CORRELATION_NODE;
    }
    bytes += nodeBytes;
  }
  if (NULL != pContext->pInheritedParams) {
    int32_t count = taosArrayGetSize(pContext->pInheritedParams);
    bytes += (int64_t)count * sizeof(SCorrelationParamValue);
    for (int32_t i = 0; i < count; ++i) {
      const SCorrelationParamValue* pValue = taosArrayGet(pContext->pInheritedParams, i);
      if (NULL == pValue || pValue->length < 0 || (pValue->length > 0 && NULL == pValue->pData) ||
          bytes > INT64_MAX - pValue->length) {
        return TSDB_CODE_QRY_INVALID_CORRELATION_NODE;
      }
      bytes += pValue->length;
    }
  }
  *pBytes = bytes;
  return TSDB_CODE_SUCCESS;
}

static int32_t dmRegisterCorrelationSession(SCorrelationRuntime* pRuntime, SCorrelationTaskSession* pSession) {
  int32_t code = TSDB_CODE_SUCCESS;
  (void)taosThreadMutexLock(&pRuntime->lock);
  if (pRuntime->closing) {
    code = TSDB_CODE_QRY_TASK_CANCELLED;
  } else {
    SCorrelationRootState* pRoot = pRuntime->pRoots;
    while (NULL != pRoot && (pRoot->key.executionId != pSession->context.root.executionId ||
                            pRoot->key.queryId != pSession->context.root.queryId ||
                            pRoot->key.clientId != pSession->context.root.clientId ||
                            pRoot->key.generation != pSession->context.root.generation)) {
      pRoot = pRoot->pNext;
    }
    if (NULL == pRoot) {
      pRoot = taosMemCalloc(1, sizeof(SCorrelationRootState));
      if (NULL == pRoot) {
        code = terrno;
      } else {
        pRoot->key = pSession->context.root;
        pRoot->pNext = pRuntime->pRoots;
        pRuntime->pRoots = pRoot;
      }
    }
    if (TSDB_CODE_SUCCESS == code) {
      ++pRoot->refs;
      pSession->pRoot = pRoot;
      pSession->pRuntimeNext = pRuntime->pSessions;
      pRuntime->pSessions = pSession;
      dmRetainCorrelationRuntime(pRuntime);
    }
  }
  (void)taosThreadMutexUnlock(&pRuntime->lock);
  return code;
}

static int32_t dmCopyCorrelationParams(const SArray* pSource, SArray** ppTarget) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SArray* pTarget = NULL;
  void*   pPendingData = NULL;
  QUERY_CHECK_NULL(ppTarget, code, lino, _return, TSDB_CODE_INVALID_PARA);
  *ppTarget = NULL;
  if (NULL == pSource) {
    goto _return;
  }

  pTarget = taosArrayInit(taosArrayGetSize(pSource), sizeof(SCorrelationParamValue));
  QUERY_CHECK_NULL(pTarget, code, lino, _return, terrno);
  for (int32_t i = 0; i < taosArrayGetSize(pSource); ++i) {
    const SCorrelationParamValue* pSourceValue = taosArrayGet(pSource, i);
    QUERY_CHECK_NULL(pSourceValue, code, lino, _return, TSDB_CODE_QRY_INVALID_CORRELATION_NODE);
    SCorrelationParamValue value = *pSourceValue;
    value.pData = NULL;
    if (value.length > 0) {
      value.pData = taosMemoryMalloc(value.length);
      QUERY_CHECK_NULL(value.pData, code, lino, _return, terrno);
      pPendingData = value.pData;
      memcpy(value.pData, pSourceValue->pData, value.length);
    }
    QUERY_CHECK_NULL(taosArrayPush(pTarget, &value), code, lino, _return, terrno);
    pPendingData = NULL;
  }

  *ppTarget = pTarget;
  pTarget = NULL;

_return:
  taosMemoryFree(pPendingData);
  taosArrayDestroyEx(pTarget, dmFreeCorrelationParamValue);
  if (TSDB_CODE_SUCCESS != code) {
    dError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t dmCreateCorrelationTaskSession(SCorrelationRuntime* pRuntime,
                                              const SCorrelationTaskContext* pContext,
                                              SCorrelationTaskSession** ppSession) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SCorrelationTaskSession* pSession = NULL;
  bool                     mutexInitialized = false;
  int64_t                  chargeBytes = 0;
  bool                     charged = false;
  bool                     jobRetained = false;
  QUERY_CHECK_NULL(pRuntime, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pContext, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(ppSession, code, lino, _return, TSDB_CODE_INVALID_PARA);
  *ppSession = NULL;
  QUERY_CHECK_CONDITION(0 == atomic_load_32(&pRuntime->closing), code, lino, _return,
                        TSDB_CODE_QRY_TASK_CANCELLED);
  QUERY_CHECK_CONDITION(0 != pContext->root.executionId && NULL != pContext->pTemplates, code, lino, _return,
                        TSDB_CODE_QRY_INVALID_CORRELATION_NODE);
  QUERY_CHECK_CONDITION(NULL == gMemPoolHandle || NULL != pContext->pMemPoolJob, code, lino, _return,
                        TSDB_CODE_QRY_MEMORY_POOL_NOT_INITIALIZED);
  taosMemPoolRetainJob(pContext->pMemPoolJob);
  jobRetained = true;
  code = dmCorrelationSessionCharge(pContext, &chargeBytes);
  QUERY_CHECK_CODE(code, lino, _return);
  code = dmReserveCorrelationMemory(pContext->pMemPoolJob, chargeBytes);
  QUERY_CHECK_CODE(code, lino, _return);
  charged = true;

  pSession = taosMemCalloc(1, sizeof(SCorrelationTaskSession));
  QUERY_CHECK_NULL(pSession, code, lino, _return, terrno);
  pSession->chargedBytes = chargeBytes;
  charged = false;
  code = taosThreadMutexInit(&pSession->lock, NULL);
  QUERY_CHECK_CODE(code, lino, _return);
  mutexInitialized = true;
  pSession->refs = 1;
  pSession->pRuntime = pRuntime;
  pSession->context = *pContext;
  jobRetained = false;
  QUERY_CHECK_NULL(pContext->pTimezoneName, code, lino, _return, TSDB_CODE_QRY_INVALID_CORRELATION_NODE);
  QUERY_CHECK_NULL(pContext->pCharsetName, code, lino, _return, TSDB_CODE_QRY_INVALID_CORRELATION_NODE);
  tstrncpy(pSession->timezoneName, pContext->pTimezoneName, sizeof(pSession->timezoneName));
  tstrncpy(pSession->charsetName, pContext->pCharsetName, sizeof(pSession->charsetName));
  pSession->context.pTimezoneName = pSession->timezoneName;
  pSession->context.pCharsetName = pSession->charsetName;
  pSession->context.pTemplates = NULL;
  pSession->context.pInheritedParams = NULL;
  pSession->context.pNodeList = NULL;
  code = nodesCloneList(pContext->pTemplates, &pSession->pTemplates);
  QUERY_CHECK_CODE(code, lino, _return);
  code = dmCopyCorrelationParams(pContext->pInheritedParams, &pSession->pInheritedParams);
  QUERY_CHECK_CODE(code, lino, _return);
  if (NULL != pContext->pNodeList) {
    pSession->pNodeList = taosArrayDup(pContext->pNodeList, NULL);
    QUERY_CHECK_NULL(pSession->pNodeList, code, lino, _return, terrno);
  }
  pSession->context.pTemplates = pSession->pTemplates;
  pSession->context.pInheritedParams = pSession->pInheritedParams;
  pSession->context.pNodeList = pSession->pNodeList;
  code = dmRegisterCorrelationSession(pRuntime, pSession);
  QUERY_CHECK_CODE(code, lino, _return);
  *ppSession = pSession;
  pSession = NULL;

_return:
  if (NULL != pSession) {
    nodesDestroyList(pSession->pTemplates);
    taosArrayDestroyEx(pSession->pInheritedParams, dmFreeCorrelationParamValue);
    taosArrayDestroy(pSession->pNodeList);
    taosMemPoolReleaseJobMemory(gMemPoolHandle, pContext->pMemPoolJob, pSession->chargedBytes);
    taosMemPoolReleaseJob(pSession->context.pMemPoolJob);
    if (mutexInitialized) {
      (void)taosThreadMutexDestroy(&pSession->lock);
    }
    taosMemFree(pSession);
  }
  if (charged) {
    taosMemPoolReleaseJobMemory(gMemPoolHandle, pContext->pMemPoolJob, chargeBytes);
  }
  if (jobRetained) {
    taosMemPoolReleaseJob(pContext->pMemPoolJob);
  }
  if (TSDB_CODE_SUCCESS != code) {
    dError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void dmRetainCorrelationTaskSession(SCorrelationTaskSession* pSession) {
  if (NULL != pSession) {
    (void)atomic_add_fetch_32(&pSession->refs, 1);
  }
}

static void dmReleaseCorrelationTaskSession(SCorrelationTaskSession* pSession) {
  if (NULL == pSession || 0 != atomic_sub_fetch_32(&pSession->refs, 1)) {
    return;
  }

  SCorrelationRuntime* pRuntime = pSession->pRuntime;
  (void)taosThreadMutexLock(&pRuntime->lock);
  SCorrelationTaskSession** ppSession = &pRuntime->pSessions;
  while (NULL != *ppSession && *ppSession != pSession) {
    ppSession = &(*ppSession)->pRuntimeNext;
  }
  if (NULL != *ppSession) {
    *ppSession = pSession->pRuntimeNext;
  }
  if (0 == --pSession->pRoot->refs) {
    SCorrelationRootState** ppRoot = &pRuntime->pRoots;
    while (NULL != *ppRoot && *ppRoot != pSession->pRoot) {
      ppRoot = &(*ppRoot)->pNext;
    }
    if (NULL != *ppRoot) {
      *ppRoot = pSession->pRoot->pNext;
    }
    taosMemFree(pSession->pRoot);
  }
  (void)taosThreadMutexUnlock(&pRuntime->lock);

  nodesDestroyList(pSession->pTemplates);
  taosArrayDestroyEx(pSession->pInheritedParams, dmFreeCorrelationParamValue);
  taosArrayDestroy(pSession->pNodeList);
  taosMemPoolReleaseJobMemory(gMemPoolHandle, pSession->context.pMemPoolJob, pSession->chargedBytes);
  taosMemPoolReleaseJob(pSession->context.pMemPoolJob);
  (void)taosThreadMutexDestroy(&pSession->lock);
  dmReleaseCorrelationRuntime(pRuntime);
  taosMemFree(pSession);
}

static void dmReleaseCorrelationCall(SCorrelationCall* pCall) {
  if (0 != atomic_sub_fetch_32(&pCall->refs, 1)) {
    return;
  }

  SCorrelationTaskSession* pSession = pCall->pSession;
  SCorrelationRuntime* pRuntime = pSession->pRuntime;
  bool activeReserved = pCall->activeReserved;
  dmRetainCorrelationRuntime(pRuntime);
  taosMemoryFree(pCall->pFetchRsp);
  taosMemPoolReleaseJobMemory(gMemPoolHandle, pSession->context.pMemPoolJob, pCall->fetchCharge);
  taosMemPoolReleaseJobMemory(gMemPoolHandle, pSession->context.pMemPoolJob, pCall->chargedBytes);
  int32_t code = tsem_destroy(&pCall->ready);
  if (TSDB_CODE_SUCCESS != code) {
    dError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  }
  (void)taosThreadMutexDestroy(&pCall->lock);

  (void)taosThreadMutexLock(&pRuntime->lock);
  if (pCall->blockingReserved) {
    --pRuntime->blockingCalls;
  }
  if (activeReserved) {
    --pSession->pRoot->activeCalls;
  }
  (void)taosThreadMutexUnlock(&pRuntime->lock);
  dmReleaseCorrelationTaskSession(pSession);
  taosMemFree(pCall);
  if (activeReserved) {
    (void)taosThreadMutexLock(&pRuntime->lock);
    if (0 == --pRuntime->activeCalls) {
      code = tsem_post(&pRuntime->drained);
      if (TSDB_CODE_SUCCESS != code) {
        dError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      }
    }
    (void)taosThreadMutexUnlock(&pRuntime->lock);
  }
  dmReleaseCorrelationRuntime(pRuntime);
}

static int32_t dmReserveCorrelationCall(SCorrelationCall* pCall) {
  SCorrelationTaskSession* pSession = pCall->pSession;
  SCorrelationRuntime*     pRuntime = pSession->pRuntime;
  int32_t                  code = TSDB_CODE_SUCCESS;
  (void)taosThreadMutexLock(&pRuntime->lock);
  if (pRuntime->closing) {
    code = TSDB_CODE_QRY_TASK_CANCELLED;
  } else if (pSession->pRoot->activeCalls >= CORRELATION_MAX_ROOT_CALLS) {
    code = TSDB_CODE_QRY_CORRELATION_CALL_LIMIT;
  } else if (pRuntime->blockingCalls >= CORRELATION_MAX_BLOCKING_CALLS) {
    code = TSDB_CODE_QRY_CORRELATION_BLOCK_LIMIT;
  } else {
    ++pSession->pRoot->activeCalls;
    ++pRuntime->activeCalls;
    ++pRuntime->blockingCalls;
    pCall->activeReserved = true;
    pCall->blockingReserved = true;
  }
  (void)taosThreadMutexUnlock(&pRuntime->lock);
  return code;
}

static void dmReleaseCorrelationBlockingReservation(SCorrelationCall* pCall) {
  SCorrelationRuntime* pRuntime = pCall->pSession->pRuntime;
  (void)taosThreadMutexLock(&pRuntime->lock);
  if (pCall->blockingReserved) {
    --pRuntime->blockingCalls;
    pCall->blockingReserved = false;
  }
  (void)taosThreadMutexUnlock(&pRuntime->lock);
}

static void dmCorrelationJobReleased(void* pParam) {
  SCorrelationCall* pCall = pParam;
  (void)taosThreadMutexLock(&pCall->lock);
  pCall->jobReleased = true;
  (void)taosThreadMutexUnlock(&pCall->lock);
  int32_t code = tsem_post(&pCall->ready);
  if (TSDB_CODE_SUCCESS != code) {
    dError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  }
  dmReleaseCorrelationCall(pCall);
}

static void dmCloseCorrelationTaskSession(SCorrelationTaskSession* pSession, int32_t code) {
  if (NULL == pSession) {
    return;
  }

  int32_t closeCode = TSDB_CODE_SUCCESS == code ? TSDB_CODE_QRY_TASK_CANCELLED : code;
  (void)taosThreadMutexLock(&pSession->lock);
  if (!pSession->closed) {
    pSession->closed = true;
    pSession->closeCode = closeCode;
  }
  for (SCorrelationCall* pCall = pSession->pCalls; NULL != pCall; pCall = pCall->pNext) {
    (void)taosThreadMutexLock(&pCall->lock);
    pCall->cancelled = true;
    if (pCall->state < CORRELATION_CALL_TERMINAL && TSDB_CODE_SUCCESS == pCall->code) {
      pCall->code = pSession->closeCode;
      pCall->state = CORRELATION_CALL_TERMINAL;
    }
    (void)taosThreadMutexUnlock(&pCall->lock);
    int32_t postCode = tsem_post(&pCall->ready);
    if (TSDB_CODE_SUCCESS != postCode) {
      dError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(postCode));
    }
  }
  (void)taosThreadMutexUnlock(&pSession->lock);
}

static const SCorrelationTemplateNode* dmFindCorrelationTemplate(const SCorrelationTaskSession* pSession,
                                                                 int32_t templateId) {
  SNode* pNode = NULL;
  FOREACH(pNode, pSession->pTemplates) {
    const SCorrelationTemplateNode* pTemplate = (const SCorrelationTemplateNode*)pNode;
    if (QUERY_NODE_CORRELATION_TEMPLATE == nodeType(pNode) && pTemplate->templateId == templateId) {
      return pTemplate;
    }
  }
  return NULL;
}

static const SCorrelationParamValue* dmFindCorrelationParam(const SArray* pParams, int32_t paramId) {
  if (NULL == pParams) {
    return NULL;
  }
  for (int32_t i = 0; i < taosArrayGetSize(pParams); ++i) {
    const SCorrelationParamValue* pValue = taosArrayGet(pParams, i);
    if (NULL != pValue && pValue->paramId == paramId) {
      return pValue;
    }
  }
  return NULL;
}

static int32_t dmCorrelationCallCharge(const SCorrelationTaskSession* pSession,
                                       const SCorrelationTemplateNode* pTemplate,
                                       const SCorrelationScalarRequest* pRequest, int64_t* pBytes) {
  if (pTemplate->planLen <= 0 || NULL == pTemplate->pPlan) {
    return TSDB_CODE_QRY_INVALID_CORRELATION_NODE;
  }

  int64_t bytes = sizeof(SCorrelationCall) + sizeof(SQueryPlan) + pTemplate->planLen;
  int32_t paramCount = LIST_LENGTH(pTemplate->pParams);
  bytes += sizeof(SArray) * 2 +
           (int64_t)paramCount * (sizeof(SCorrelationParamDef) + sizeof(SCorrelationWireValue));
  SNode* pNode = NULL;
  FOREACH(pNode, pTemplate->pParams) {
    if (QUERY_NODE_CORRELATION_PARAM != nodeType(pNode)) {
      return TSDB_CODE_QRY_INVALID_CORRELATION_NODE;
    }
    const SCorrelationParamNode* pParam = (const SCorrelationParamNode*)pNode;
    const SCorrelationParamValue* pValue = dmFindCorrelationParam(pRequest->pParams, pParam->paramId);
    if (NULL == pValue) {
      pValue = dmFindCorrelationParam(pSession->pInheritedParams, pParam->paramId);
    }
    if (NULL == pValue || pValue->length < 0 || (pValue->length > 0 && NULL == pValue->pData) ||
        bytes > INT64_MAX - pValue->length) {
      return TSDB_CODE_QRY_INVALID_CORRELATION_NODE;
    }
    bytes += pValue->length;
  }
  *pBytes = bytes;
  return TSDB_CODE_SUCCESS;
}

static int32_t dmBuildCorrelationParams(const SCorrelationTaskSession* pSession,
                                        const SCorrelationTemplateNode* pTemplate, const SArray* pParams,
                                        SArray** ppDefs, SArray** ppValues) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SArray* pDefs = NULL;
  SArray* pValues = NULL;
  void*   pPendingData = NULL;
  QUERY_CHECK_NULL(pSession, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pTemplate, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(ppDefs, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(ppValues, code, lino, _return, TSDB_CODE_INVALID_PARA);
  *ppDefs = NULL;
  *ppValues = NULL;

  int32_t paramCount = LIST_LENGTH(pTemplate->pParams);
  pDefs = taosArrayInit(paramCount, sizeof(SCorrelationParamDef));
  QUERY_CHECK_NULL(pDefs, code, lino, _return, terrno);
  pValues = taosArrayInit(paramCount, sizeof(SCorrelationWireValue));
  QUERY_CHECK_NULL(pValues, code, lino, _return, terrno);

  SNode* pNode = NULL;
  FOREACH(pNode, pTemplate->pParams) {
    QUERY_CHECK_CONDITION(QUERY_NODE_CORRELATION_PARAM == nodeType(pNode), code, lino, _return,
                          TSDB_CODE_QRY_INVALID_CORRELATION_NODE);
    const SCorrelationParamNode* pParam = (const SCorrelationParamNode*)pNode;
    const SCorrelationParamValue* pValue = dmFindCorrelationParam(pParams, pParam->paramId);
    if (NULL == pValue) {
      pValue = dmFindCorrelationParam(pSession->pInheritedParams, pParam->paramId);
    }
    QUERY_CHECK_NULL(pValue, code, lino, _return, TSDB_CODE_QRY_INVALID_CORRELATION_NODE);

    SCorrelationParamDef def = {.paramId = pParam->paramId, .dataType = pParam->node.resType};
    QUERY_CHECK_NULL(taosArrayPush(pDefs, &def), code, lino, _return, terrno);
    SCorrelationWireValue value = {
        .paramId = pValue->paramId,
        .dataType = pValue->dataType,
        .isNull = pValue->isNull,
        .dataLen = pValue->length,
    };
    if (value.dataLen > 0) {
      value.pData = taosMemoryMalloc(value.dataLen);
      QUERY_CHECK_NULL(value.pData, code, lino, _return, terrno);
      pPendingData = value.pData;
      memcpy(value.pData, pValue->pData, value.dataLen);
    }
    QUERY_CHECK_NULL(taosArrayPush(pValues, &value), code, lino, _return, terrno);
    pPendingData = NULL;
  }
  code = tValidateCorrelationParams(pDefs, pValues);
  QUERY_CHECK_CODE(code, lino, _return);
  *ppDefs = pDefs;
  *ppValues = pValues;
  pDefs = NULL;
  pValues = NULL;

_return:
  taosMemoryFree(pPendingData);
  taosArrayDestroy(pDefs);
  taosArrayDestroyEx(pValues, dmFreeCorrelationWireValue);
  if (TSDB_CODE_SUCCESS != code) {
    dError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void dmSetCorrelationPlanQueryId(SQueryPlan* pPlan, uint64_t queryId) {
  pPlan->queryId = queryId;
  SNode* pLevelNode = NULL;
  FOREACH(pLevelNode, pPlan->pSubplans) {
    SNode* pSubplanNode = NULL;
    FOREACH(pSubplanNode, ((SNodeListNode*)pLevelNode)->pNodeList) {
      ((SSubplan*)pSubplanNode)->id.queryId = queryId;
    }
  }
  SNode* pChildNode = NULL;
  FOREACH(pChildNode, pPlan->pChildren) {
    dmSetCorrelationPlanQueryId((SQueryPlan*)pChildNode, queryId);
  }
}

static int32_t dmInstantiateCorrelationPlan(const SCorrelationTemplateNode* pTemplate, SQueryPlan** ppPlan) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  SQueryPlan* pPlan = NULL;
  char*       pPlanMsg = NULL;
  QUERY_CHECK_NULL(pTemplate, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(ppPlan, code, lino, _return, TSDB_CODE_INVALID_PARA);
  *ppPlan = NULL;
  QUERY_CHECK_CONDITION(pTemplate->planLen > 0 && NULL != pTemplate->pPlan, code, lino, _return,
                        TSDB_CODE_QRY_INVALID_CORRELATION_NODE);
  pPlanMsg = taosMemoryMalloc(pTemplate->planLen);
  QUERY_CHECK_NULL(pPlanMsg, code, lino, _return, terrno);
  memcpy(pPlanMsg, pTemplate->pPlan, pTemplate->planLen);
  code = nodesMsgToNode(pPlanMsg, pTemplate->planLen, (SNode**)&pPlan);
  QUERY_CHECK_CODE(code, lino, _return);
  QUERY_CHECK_CONDITION(NULL != pPlan && QUERY_NODE_PHYSICAL_PLAN == nodeType(pPlan), code, lino, _return,
                        TSDB_CODE_QRY_INVALID_CORRELATION_NODE);
  QUERY_CHECK_CONDITION(NULL == pPlan->pCorrelationTemplates, code, lino, _return,
                        TSDB_CODE_QRY_INVALID_CORRELATION_NODE);
  code = nodesCloneList(pTemplate->pNestedTemplates, &pPlan->pCorrelationTemplates);
  QUERY_CHECK_CODE(code, lino, _return);
  dmSetCorrelationPlanQueryId(pPlan, (uint64_t)tGenIdPI64());
  *ppPlan = pPlan;
  pPlan = NULL;

_return:
  taosMemoryFree(pPlanMsg);
  qDestroyQueryPlan(pPlan);
  if (TSDB_CODE_SUCCESS != code) {
    dError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t dmAttachCorrelationCall(SCorrelationTaskSession* pSession, SCorrelationCall* pCall,
                                       int32_t templateId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  QUERY_CHECK_NULL(pSession, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pCall, code, lino, _return, TSDB_CODE_INVALID_PARA);
  (void)taosThreadMutexLock(&pSession->lock);
  if (pSession->closed || 0 != atomic_load_32(&pSession->pRuntime->closing)) {
    code = pSession->closed ? pSession->closeCode : TSDB_CODE_QRY_TASK_CANCELLED;
    lino = __LINE__;
  } else {
    pCall->key.root = pSession->context.root;
    pCall->key.parent = pSession->context.task;
    pCall->key.templateId = templateId;
    pCall->key.sequence = ++pSession->nextSequence;
    pCall->pNext = pSession->pCalls;
    pSession->pCalls = pCall;
  }
  (void)taosThreadMutexUnlock(&pSession->lock);

_return:
  if (TSDB_CODE_SUCCESS != code) {
    dError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void dmDetachCorrelationCall(SCorrelationCall* pCall) {
  SCorrelationTaskSession* pSession = pCall->pSession;
  (void)taosThreadMutexLock(&pSession->lock);
  SCorrelationCall** ppCall = &pSession->pCalls;
  while (NULL != *ppCall) {
    if (*ppCall == pCall) {
      *ppCall = pCall->pNext;
      break;
    }
    ppCall = &(*ppCall)->pNext;
  }
  (void)taosThreadMutexUnlock(&pSession->lock);
}

static bool dmCorrelationCallKilled(void* pParam) {
  SCorrelationCall* pCall = pParam;
  bool              killed = false;
  (void)taosThreadMutexLock(&pCall->lock);
  killed = pCall->cancelled;
  (void)taosThreadMutexUnlock(&pCall->lock);
  return killed;
}

static void dmCorrelationExecCallback(SExecResult* pResult, void* pParam, int32_t code) {
  SCorrelationCall* pCall = pParam;
  (void)taosThreadMutexLock(&pCall->lock);
  if (pCall->state < CORRELATION_CALL_TERMINAL && !pCall->execDone) {
    pCall->execDone = true;
    if (TSDB_CODE_SUCCESS != code && TSDB_CODE_SUCCESS == pCall->code) {
      pCall->code = code;
      pCall->state = CORRELATION_CALL_TERMINAL;
    }
  }
  (void)taosThreadMutexUnlock(&pCall->lock);
  destroyQueryExecRes(pResult);
  taosMemoryFree(pResult);
  code = tsem_post(&pCall->ready);
  if (TSDB_CODE_SUCCESS != code) {
    dError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  }
}

static void dmCorrelationFetchCallback(void* pResult, void* pParam, int32_t code) {
  SCorrelationCall* pCall = pParam;
  bool              accepted = false;
  int64_t           charge = 0;
  if (TSDB_CODE_SUCCESS == code && NULL != pResult && NULL != pCall->pSession) {
    const SRetrieveTableRsp* pRsp = pResult;
    charge = sizeof(SRetrieveTableRsp) + (uint32_t)ntohl((uint32_t)pRsp->compLen);
    if (pRsp->compressed) {
      charge += (uint32_t)ntohl((uint32_t)pRsp->payloadLen);
    }
    code = dmReserveCorrelationMemory(pCall->pSession->context.pMemPoolJob, charge);
    if (TSDB_CODE_SUCCESS != code) {
      charge = 0;
    }
  }
  (void)taosThreadMutexLock(&pCall->lock);
  if (pCall->state == CORRELATION_CALL_FETCHING && pCall->fetchInFlight && !pCall->fetchDone) {
    pCall->fetchDone = true;
    pCall->fetchInFlight = false;
    if (TSDB_CODE_SUCCESS != code && TSDB_CODE_SUCCESS == pCall->code) {
      pCall->code = code;
      pCall->state = CORRELATION_CALL_TERMINAL;
    } else if (TSDB_CODE_SUCCESS == code) {
      pCall->pFetchRsp = pResult;
      pCall->fetchCharge = charge;
      accepted = true;
    }
  }
  (void)taosThreadMutexUnlock(&pCall->lock);
  if (!accepted) {
    taosMemoryFree(pResult);
    if (NULL != pCall->pSession) {
      taosMemPoolReleaseJobMemory(gMemPoolHandle, pCall->pSession->context.pMemPoolJob, charge);
    }
  }
  code = tsem_post(&pCall->ready);
  if (TSDB_CODE_SUCCESS != code) {
    dError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  }
}

typedef enum { CORRELATION_WAIT_EXEC, CORRELATION_WAIT_FETCH, CORRELATION_WAIT_DRAIN } ECorrelationWait;

static int32_t dmWaitCorrelationCall(SCorrelationCall* pCall, ECorrelationWait waitFor,
                                     const SCorrelationWaitHooks* pHooks, bool* pBlocking) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  while (true) {
    (void)taosThreadMutexLock(&pCall->lock);
    bool done = CORRELATION_WAIT_DRAIN == waitFor ? pCall->jobReleased
                : CORRELATION_WAIT_FETCH == waitFor ? pCall->fetchDone : pCall->execDone;
    code = CORRELATION_WAIT_DRAIN == waitFor ? TSDB_CODE_SUCCESS : pCall->code;
    if (!done && pCall->jobReleased && CORRELATION_WAIT_DRAIN != waitFor && TSDB_CODE_SUCCESS == code) {
      code = TSDB_CODE_QRY_JOB_NOT_EXIST;
    }
    (void)taosThreadMutexUnlock(&pCall->lock);
    if (done || TSDB_CODE_SUCCESS != code) {
      break;
    }

    if (!*pBlocking && NULL != pHooks->beforeBlocking) {
      code = pHooks->beforeBlocking(pHooks->pContext);
      QUERY_CHECK_CODE(code, lino, _return);
      *pBlocking = true;
    }
    code = tsem_wait(&pCall->ready);
    QUERY_CHECK_CODE(code, lino, _return);
  }

_return:
  if (TSDB_CODE_SUCCESS != code) {
    dError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t dmCollectCorrelationBatch(const SCorrelationTemplateNode* pTemplate, SRetrieveTableRsp* pRsp,
                                         int64_t* pRows, SValueNode** ppCandidate) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SValueNode*  pCandidate = NULL;
  SSDataBlock* pBlock = NULL;
  void*        pDecompressed = NULL;
  QUERY_CHECK_NULL(pTemplate, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pRsp, code, lino, _return, TSDB_CODE_QRY_INVALID_MSG);
  QUERY_CHECK_NULL(pRows, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(ppCandidate, code, lino, _return, TSDB_CODE_INVALID_PARA);

  pRsp->numOfRows = be64toh((uint64_t)pRsp->numOfRows);
  pRsp->compLen = ntohl((uint32_t)pRsp->compLen);
  pRsp->payloadLen = ntohl((uint32_t)pRsp->payloadLen);
  pRsp->numOfBlocks = ntohl((uint32_t)pRsp->numOfBlocks);
  pRsp->numOfCols = (int64_t)ntohl((uint32_t)pRsp->numOfCols);
  QUERY_CHECK_CONDITION(pRsp->numOfRows >= 0 && pRsp->numOfBlocks >= 0 && pRsp->compLen >= 0 &&
                            pRsp->payloadLen >= pRsp->compLen &&
                            pRsp->numOfBlocks <= pRsp->compLen / PAYLOAD_PREFIX_LEN,
                        code, lino, _return, TSDB_CODE_QRY_INVALID_MSG);
  QUERY_CHECK_CONDITION(*pRows >= 0 && *pRows <= 1 && pRsp->numOfRows <= 1 - *pRows, code, lino, _return,
                        TSDB_CODE_PAR_INVALID_SCALAR_SUBQ_RES_ROWS);
  QUERY_CHECK_CONDITION(0 == pRsp->numOfRows || 1 == pRsp->numOfCols, code, lino, _return,
                        TSDB_CODE_PAR_INVALID_SCALAR_SUBQ_RES_COLS);

  int32_t compOffset = 0;
  int32_t rawOffset = 0;
  int64_t batchRows = 0;
  for (int32_t i = 0; i < pRsp->numOfBlocks; ++i) {
    QUERY_CHECK_CONDITION(pRsp->compLen - compOffset >= PAYLOAD_PREFIX_LEN, code, lino, _return,
                          TSDB_CODE_QRY_INVALID_MSG);
    int32_t compLen = 0;
    int32_t rawLen = 0;
    memcpy(&compLen, pRsp->data + compOffset, sizeof(compLen));
    memcpy(&rawLen, pRsp->data + compOffset + sizeof(compLen), sizeof(rawLen));
    compOffset += PAYLOAD_PREFIX_LEN;
    rawOffset += PAYLOAD_PREFIX_LEN;
    QUERY_CHECK_CONDITION(compLen > 0 && rawLen >= compLen && compLen <= pRsp->compLen - compOffset &&
                              rawLen <= pRsp->payloadLen - rawOffset && (pRsp->compressed || compLen == rawLen),
                          code, lino, _return, TSDB_CODE_QRY_INVALID_MSG);

    const char* pData = pRsp->data + compOffset;
    if (pRsp->compressed && compLen < rawLen) {
      pDecompressed = taosMemoryMalloc(rawLen);
      QUERY_CHECK_NULL(pDecompressed, code, lino, _return, terrno);
      int32_t length = tsDecompressString((void*)pData, compLen, 1, pDecompressed, rawLen, ONE_STAGE_COMP, NULL, 0);
      QUERY_CHECK_CONDITION(length == rawLen, code, lino, _return, TSDB_CODE_QRY_INVALID_MSG);
      pData = pDecompressed;
    }

    code = createDataBlock(&pBlock);
    QUERY_CHECK_CODE(code, lino, _return);
    SColumnInfoData column = createColumnInfoData(pTemplate->resultType.type, pTemplate->resultType.bytes, 0);
    column.info.precision = pTemplate->resultType.precision;
    column.info.scale = pTemplate->resultType.scale;
    code = blockDataAppendColInfo(pBlock, &column);
    QUERY_CHECK_CODE(code, lino, _return);
    const char* pEnd = NULL;
    code = blockDecodeInternalBounded(pBlock, pData, rawLen, &pEnd);
    QUERY_CHECK_CODE(code, lino, _return);
    QUERY_CHECK_CONDITION(pEnd == pData + rawLen && pBlock->info.rows >= 0 &&
                              batchRows + pBlock->info.rows <= pRsp->numOfRows,
                          code, lino, _return, TSDB_CODE_QRY_INVALID_MSG);
    if (pBlock->info.rows > 0) {
      QUERY_CHECK_CONDITION(1 == blockDataGetNumOfCols(pBlock), code, lino, _return,
                            TSDB_CODE_PAR_INVALID_SCALAR_SUBQ_RES_COLS);
      QUERY_CHECK_CONDITION(NULL == *ppCandidate && NULL == pCandidate, code, lino, _return,
                            TSDB_CODE_PAR_INVALID_SCALAR_SUBQ_RES_ROWS);
      code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pCandidate);
      QUERY_CHECK_CODE(code, lino, _return);
      pCandidate->node.resType = pTemplate->resultType;
      pCandidate->translate = true;
      pCandidate->flag &= ~VALUE_FLAG_VAL_UNSET;
      pCandidate->isNull = true;
      SColumnInfoData* pColumn = taosArrayGet(pBlock->pDataBlock, 0);
      QUERY_CHECK_NULL(pColumn, code, lino, _return, TSDB_CODE_PAR_INVALID_SCALAR_SUBQ_RES_COLS);
      if (!colDataIsNull_s(pColumn, 0)) {
        bool needFree = true;
        code = nodesSetValueNodeValueExt(pCandidate, colDataGetData(pColumn, 0), &needFree);
        QUERY_CHECK_CODE(code, lino, _return);
        pCandidate->isNull = false;
        if (!needFree) {
          pColumn->pData = NULL;
        }
      }
    }
    batchRows += pBlock->info.rows;
    blockDataDestroy(pBlock);
    pBlock = NULL;
    taosMemoryFreeClear(pDecompressed);
    compOffset += compLen;
    rawOffset += rawLen;
  }
  QUERY_CHECK_CONDITION(compOffset == pRsp->compLen && rawOffset == pRsp->payloadLen &&
                            batchRows == pRsp->numOfRows,
                        code, lino, _return, TSDB_CODE_QRY_INVALID_MSG);
  *pRows += batchRows;
  if (NULL != pCandidate) {
    *ppCandidate = pCandidate;
    pCandidate = NULL;
  }

_return:
  taosMemoryFree(pDecompressed);
  blockDataDestroy(pBlock);
  nodesDestroyNode((SNode*)pCandidate);
  if (TSDB_CODE_SUCCESS != code) {
    dError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int64_t dmCorrelationCandidateSize(SValueNode* pCandidate) {
  int64_t size = sizeof(SValueNode) + 4;
  if (!pCandidate->isNull &&
      (IS_VAR_DATA_TYPE(pCandidate->node.resType.type) ||
       TSDB_DATA_TYPE_DECIMAL == pCandidate->node.resType.type) &&
      NULL != pCandidate->datum.p) {
    size += taosMemSize(pCandidate->datum.p);
  }
  return size;
}

static int32_t dmInvokeCorrelationScalar(SCorrelationTaskSession* pSession,
                                         const SCorrelationScalarRequest* pRequest, SValueNode** ppResult) {
  int32_t                         code = TSDB_CODE_SUCCESS;
  int32_t                         lino = 0;
  SCorrelationCall*               pCall = NULL;
  const SCorrelationTemplateNode* pTemplate = NULL;
  SQueryPlan*                     pPlan = NULL;
  SArray*                         pDefs = NULL;
  SArray*                         pValues = NULL;
  SNodeList*                      pTimeRangeInit = NULL;
  SValueNode*                     pCandidate = NULL;
  SRetrieveTableRsp*              pFetchRsp = NULL;
  int64_t                         fetchCharge = 0;
  int64_t                         candidateCharge = 0;
  int64_t                         rows = 0;
  bool                            planTaken = false;
  bool                            attached = false;
  bool                            semInitialized = false;
  bool                            mutexInitialized = false;
  bool                            sessionRetained = false;
  bool                            blocking = false;
  bool                            previousPoolEnabled = false;
  bool                            previousRandErr = false;
  bool                            poolDisabled = false;
  int64_t                         chargeBytes = 0;
  SCorrelationTaskContext         logContext = {0};
  uint64_t                        logCallOrdinal = 0;
  int32_t                         logTemplateId = -1;
  QUERY_CHECK_NULL(pSession, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pRequest, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(ppResult, code, lino, _return, TSDB_CODE_INVALID_PARA);
  *ppResult = NULL;
  taosSaveDisableMemPoolUsage(previousPoolEnabled, previousRandErr);
  poolDisabled = true;
  dmRetainCorrelationTaskSession(pSession);
  sessionRetained = true;

  pTemplate = dmFindCorrelationTemplate(pSession, pRequest->templateId);
  QUERY_CHECK_NULL(pTemplate, code, lino, _return, TSDB_CODE_QRY_INVALID_CORRELATION_NODE);
  pCall = taosMemCalloc(1, sizeof(SCorrelationCall));
  QUERY_CHECK_NULL(pCall, code, lino, _return, terrno);
  pCall->pSession = pSession;
  code = taosThreadMutexInit(&pCall->lock, NULL);
  QUERY_CHECK_CODE(code, lino, _return);
  mutexInitialized = true;
  code = tsem_init(&pCall->ready, 0, 0);
  QUERY_CHECK_CODE(code, lino, _return);
  semInitialized = true;
  pCall->refs = 1;
  sessionRetained = false;
  code = dmReserveCorrelationCall(pCall);
  QUERY_CHECK_CODE(code, lino, _return);
  code = dmCorrelationCallCharge(pSession, pTemplate, pRequest, &chargeBytes);
  QUERY_CHECK_CODE(code, lino, _return);
  code = dmReserveCorrelationMemory(pSession->context.pMemPoolJob, chargeBytes);
  QUERY_CHECK_CODE(code, lino, _return);
  pCall->chargedBytes = chargeBytes;
  code = dmAttachCorrelationCall(pSession, pCall, pRequest->templateId);
  QUERY_CHECK_CODE(code, lino, _return);
  attached = true;
  code = dmBuildCorrelationParams(pSession, pTemplate, pRequest->pParams, &pDefs, &pValues);
  QUERY_CHECK_CODE(code, lino, _return);
  code = dmInstantiateCorrelationPlan(pTemplate, &pPlan);
  QUERY_CHECK_CODE(code, lino, _return);
  code = nodesCloneList(pTemplate->pTimeRangeInit, &pTimeRangeInit);
  QUERY_CHECK_CODE(code, lino, _return);
  SNode* pTimeInit = NULL;
  FOREACH(pTimeInit, pTimeRangeInit) {
    QUERY_CHECK_CONDITION(QUERY_NODE_CORRELATION_TIME_RANGE_INIT == nodeType(pTimeInit), code, lino, _return,
                          TSDB_CODE_QRY_INVALID_CORRELATION_NODE);
    ((SCorrelationTimeRangeInitNode*)pTimeInit)->targetSubplanId.queryId = pPlan->queryId;
  }

  SRequestConnInfo conn = {
      .pTrans = pSession->pRuntime->clientRpc,
      .requestId = pPlan->queryId,
      .requestObjRefId = 0,
      .txnId = pSession->context.txnId,
  };
  SSchedulerReq schedulerReq = {
      .syncReq = false,
      .localReq = false,
      .inlineTaskLaunch = true,
      .pConn = &conn,
      .pNodeList = pSession->pNodeList,
      .pDag = pPlan,
      .startTs = taosGetTimestampUs(),
      .txnId = pSession->context.txnId,
      .execFp = dmCorrelationExecCallback,
      .cbParam = pCall,
      .cbParamFreeFp = dmCorrelationJobReleased,
      .chkKillFp = dmCorrelationCallKilled,
      .chkKillParam = pCall,
      .pWorkerCb = pSession->context.pWorkerCb,
      .firstDayOfWeek = pSession->context.firstDayOfWeek,
      .pCorrelationParamDefs = pDefs,
      .pCorrelationParamValues = pValues,
      .pCorrelationTimeRangeInit = pTimeRangeInit,
      .pExternalSubquerySources = pRequest->pSubquerySources,
      .pPlanTaken = &planTaken,
  };
  schedulerReq.correlation.rootExecutionId = pSession->context.root.executionId;
  schedulerReq.correlation.rootQueryId = pSession->context.root.queryId;
  schedulerReq.correlation.rootClientId = pSession->context.root.clientId;
  schedulerReq.correlation.executionGeneration = pSession->context.root.generation;
  schedulerReq.correlation.parentQueryId = pSession->context.task.queryId;
  schedulerReq.correlation.parentClientId = pSession->context.task.clientId;
  schedulerReq.correlation.parentTaskId = pSession->context.task.taskId;
  schedulerReq.correlation.parentExecId = pSession->context.task.execId;
  schedulerReq.correlation.parentSeriesId = pSession->context.task.seriesId;
  schedulerReq.correlation.templateId = pRequest->templateId;
  schedulerReq.correlation.callOrdinal = pCall->key.sequence;
  tstrncpy(schedulerReq.correlation.user, pSession->context.user, sizeof(schedulerReq.correlation.user));
  tstrncpy(schedulerReq.correlation.dbFName, pSession->context.db, sizeof(schedulerReq.correlation.dbFName));
  tstrncpy(schedulerReq.correlation.timezoneName, pSession->context.pTimezoneName,
           sizeof(schedulerReq.correlation.timezoneName));
  tstrncpy(schedulerReq.correlation.charsetName, pSession->context.pCharsetName,
           sizeof(schedulerReq.correlation.charsetName));

  (void)taosThreadMutexLock(&pCall->lock);
  code = pCall->code;
  if (TSDB_CODE_SUCCESS == code && pCall->state < CORRELATION_CALL_TERMINAL) {
    pCall->state = CORRELATION_CALL_SUBMITTING;
  }
  (void)taosThreadMutexUnlock(&pCall->lock);
  QUERY_CHECK_CODE(code, lino, _return);
  (void)atomic_add_fetch_32(&pCall->refs, 1);
  code = schedulerExecJob(&schedulerReq, &pCall->jobId);
  if (planTaken) {
    pPlan = NULL;
  } else {
    dmReleaseCorrelationCall(pCall);
  }
  QUERY_CHECK_CODE(code, lino, _return);
  (void)taosThreadMutexLock(&pCall->lock);
  if (pCall->state < CORRELATION_CALL_TERMINAL) {
    pCall->state = CORRELATION_CALL_JOB_PUBLISHED;
  }
  (void)taosThreadMutexUnlock(&pCall->lock);
  code = dmWaitCorrelationCall(pCall, CORRELATION_WAIT_EXEC, &pRequest->wait, &blocking);
  QUERY_CHECK_CODE(code, lino, _return);

  SSchedulerReq fetchReq = {
      .syncReq = false,
      .fetchFp = dmCorrelationFetchCallback,
      .cbParam = pCall,
  };
  while (true) {
    (void)taosThreadMutexLock(&pCall->lock);
    code = pCall->code;
    if (TSDB_CODE_SUCCESS == code && pCall->state < CORRELATION_CALL_TERMINAL) {
      pCall->state = CORRELATION_CALL_FETCHING;
      pCall->fetchInFlight = true;
      pCall->fetchDone = false;
    }
    (void)taosThreadMutexUnlock(&pCall->lock);
    QUERY_CHECK_CODE(code, lino, _return);

    code = schedulerFetchRows(pCall->jobId, &fetchReq);
    QUERY_CHECK_CODE(code, lino, _return);
    code = dmWaitCorrelationCall(pCall, CORRELATION_WAIT_FETCH, &pRequest->wait, &blocking);
    QUERY_CHECK_CODE(code, lino, _return);
    (void)taosThreadMutexLock(&pCall->lock);
    pFetchRsp = pCall->pFetchRsp;
    pCall->pFetchRsp = NULL;
    fetchCharge = pCall->fetchCharge;
    pCall->fetchCharge = 0;
    if (pCall->state == CORRELATION_CALL_FETCHING) {
      pCall->state = CORRELATION_CALL_JOB_PUBLISHED;
    }
    (void)taosThreadMutexUnlock(&pCall->lock);
    QUERY_CHECK_NULL(pFetchRsp, code, lino, _return, TSDB_CODE_QRY_INVALID_MSG);
    bool completed = pFetchRsp->completed;
    code = dmCollectCorrelationBatch(pTemplate, pFetchRsp, &rows, &pCandidate);
    taosMemoryFreeClear(pFetchRsp);
    taosMemPoolReleaseJobMemory(gMemPoolHandle, pSession->context.pMemPoolJob, fetchCharge);
    fetchCharge = 0;
    QUERY_CHECK_CODE(code, lino, _return);
    if (NULL != pCandidate && 0 == candidateCharge) {
      candidateCharge = dmCorrelationCandidateSize(pCandidate);
      code = dmReserveCorrelationMemory(pSession->context.pMemPoolJob, candidateCharge);
      if (TSDB_CODE_SUCCESS != code) {
        candidateCharge = 0;
      }
      QUERY_CHECK_CODE(code, lino, _return);
    }
    if (completed) {
      break;
    }
  }

  if (NULL == pCandidate) {
    code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pCandidate);
    QUERY_CHECK_CODE(code, lino, _return);
    pCandidate->node.resType = pTemplate->resultType;
    pCandidate->translate = true;
    pCandidate->flag &= ~VALUE_FLAG_VAL_UNSET;
    pCandidate->isNull = true;
    candidateCharge = dmCorrelationCandidateSize(pCandidate);
    code = dmReserveCorrelationMemory(pSession->context.pMemPoolJob, candidateCharge);
    if (TSDB_CODE_SUCCESS != code) {
      candidateCharge = 0;
    }
    QUERY_CHECK_CODE(code, lino, _return);
  }
  (void)taosThreadMutexLock(&pCall->lock);
  code = pCall->code;
  if (TSDB_CODE_SUCCESS == code && pCall->state < CORRELATION_CALL_TERMINAL) {
    pCall->state = CORRELATION_CALL_TERMINAL;
  }
  (void)taosThreadMutexUnlock(&pCall->lock);
  QUERY_CHECK_CODE(code, lino, _return);

_return:
  if (NULL != pSession) {
    logContext = pSession->context;
  }
  if (NULL != pRequest) {
    logTemplateId = pRequest->templateId;
  }
  if (NULL != pCall) {
    logCallOrdinal = pCall->key.sequence;
  }
  taosMemoryFree(pFetchRsp);
  taosMemPoolReleaseJobMemory(gMemPoolHandle, NULL != pSession ? pSession->context.pMemPoolJob : NULL, fetchCharge);
  if (NULL != pCall && 0 != pCall->jobId) {
    (void)taosThreadMutexLock(&pCall->lock);
    if (TSDB_CODE_SUCCESS != code && TSDB_CODE_SUCCESS == pCall->code) {
      pCall->code = code;
    }
    pCall->cancelled = true;
    pCall->state = CORRELATION_CALL_DRAINING;
    (void)taosThreadMutexUnlock(&pCall->lock);
    schedulerFreeJob(&pCall->jobId, code);
  }
  qDestroyQueryPlan(pPlan);
  nodesDestroyList(pTimeRangeInit);
  taosArrayDestroy(pDefs);
  taosArrayDestroyEx(pValues, dmFreeCorrelationWireValue);
  if (NULL != pCall) {
    if (planTaken) {
      int32_t drainCode = dmWaitCorrelationCall(pCall, CORRELATION_WAIT_DRAIN, &pRequest->wait, &blocking);
      if (TSDB_CODE_SUCCESS == code) {
        code = drainCode;
      }
    }
    if (attached) {
      dmDetachCorrelationCall(pCall);
    }
    if (blocking && NULL != pRequest->wait.afterRecoverFromBlocking) {
      pRequest->wait.afterRecoverFromBlocking(pRequest->wait.pContext);
    }
    if (semInitialized) {
      dmReleaseCorrelationBlockingReservation(pCall);
      dmReleaseCorrelationCall(pCall);
    } else {
      if (mutexInitialized) {
        (void)taosThreadMutexDestroy(&pCall->lock);
      }
      taosMemPoolReleaseJobMemory(gMemPoolHandle, pSession->context.pMemPoolJob, pCall->chargedBytes);
      taosMemFree(pCall);
    }
  }
  if (sessionRetained) {
    dmReleaseCorrelationTaskSession(pSession);
  }
  if (TSDB_CODE_SUCCESS == code) {
    *ppResult = pCandidate;
    pCandidate = NULL;
  }
  nodesDestroyNode((SNode*)pCandidate);
  taosMemPoolReleaseJobMemory(gMemPoolHandle, NULL != pSession ? pSession->context.pMemPoolJob : NULL,
                              candidateCharge);
  if (poolDisabled) {
    taosRestoreEnableMemPoolUsage(previousPoolEnabled, previousRandErr);
  }
  if (TSDB_CODE_SUCCESS != code) {
    dError("rootQID:0x%" PRIx64 " rootExec:0x%" PRIx64 " parentQID:0x%" PRIx64
           " parentTID:0x%" PRIx64 " parentEID:%d template:%d call:%" PRIu64
           " %s failed at line %d since %s",
           logContext.root.queryId, logContext.root.executionId, logContext.task.queryId,
           logContext.task.taskId, logContext.task.execId, logTemplateId, logCallOrdinal,
           __func__, lino, tstrerror(code));
  }
  return code;
}

static const SArray* dmGetCorrelationInheritedParams(const SCorrelationTaskSession* pSession) {
  return pSession->pInheritedParams;
}

static int32_t dmReserveCorrelationResultMemory(SCorrelationTaskSession* pSession, int64_t bytes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  QUERY_CHECK_NULL(pSession, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(bytes >= 0, code, lino, _return, TSDB_CODE_INVALID_PARA);
  code = dmReserveCorrelationMemory(pSession->context.pMemPoolJob, bytes);
  QUERY_CHECK_CODE(code, lino, _return);

_return:
  if (TSDB_CODE_SUCCESS != code) {
    dError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void dmReleaseCorrelationResultMemory(SCorrelationTaskSession* pSession, int64_t bytes) {
  if (NULL != pSession && bytes > 0) {
    taosMemPoolReleaseJobMemory(gMemPoolHandle, pSession->context.pMemPoolJob, bytes);
  }
}

const SCorrelationRuntimeOps gDmCorrelationRuntimeOps = {
    .createTaskSession = dmCreateCorrelationTaskSession,
    .retainTaskSession = dmRetainCorrelationTaskSession,
    .releaseTaskSession = dmReleaseCorrelationTaskSession,
    .invokeScalar = dmInvokeCorrelationScalar,
    .getInheritedParams = dmGetCorrelationInheritedParams,
    .reserveResultMemory = dmReserveCorrelationResultMemory,
    .releaseResultMemory = dmReleaseCorrelationResultMemory,
    .closeTaskSession = dmCloseCorrelationTaskSession,
};

int32_t dmInitCorrelation(SDnode* pDnode) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  SCorrelationRuntime* pRuntime = NULL;
  bool                 mutexInitialized = false;
  bool                 semInitialized = false;
  QUERY_CHECK_NULL(pDnode, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pDnode->trans.clientRpc, code, lino, _return, TSDB_CODE_QRY_INVALID_INPUT);
  pRuntime = taosMemCalloc(1, sizeof(SCorrelationRuntime));
  QUERY_CHECK_NULL(pRuntime, code, lino, _return, terrno);
  code = taosThreadMutexInit(&pRuntime->lock, NULL);
  QUERY_CHECK_CODE(code, lino, _return);
  mutexInitialized = true;
  code = tsem_init(&pRuntime->drained, 0, 0);
  QUERY_CHECK_CODE(code, lino, _return);
  semInitialized = true;
  pRuntime->clientRpc = pDnode->trans.clientRpc;
  pRuntime->refs = 1;
  code = schedulerInit();
  QUERY_CHECK_CODE(code, lino, _return);
  pDnode->pCorrelationRuntime = pRuntime;
  pRuntime = NULL;

_return:
  if (NULL != pRuntime) {
    if (semInitialized) {
      int32_t cleanupCode = tsem_destroy(&pRuntime->drained);
      if (TSDB_CODE_SUCCESS != cleanupCode) {
        dError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(cleanupCode));
      }
    }
    if (mutexInitialized) {
      (void)taosThreadMutexDestroy(&pRuntime->lock);
    }
    taosMemFree(pRuntime);
  }
  if (TSDB_CODE_SUCCESS != code) {
    dError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void dmCleanupCorrelation(SDnode* pDnode) {
  if (NULL == pDnode || NULL == pDnode->pCorrelationRuntime) {
    return;
  }

  SCorrelationRuntime* pRuntime = pDnode->pCorrelationRuntime;
  pDnode->pCorrelationRuntime = NULL;
  (void)taosThreadMutexLock(&pRuntime->lock);
  atomic_store_32(&pRuntime->closing, 1);
  for (SCorrelationTaskSession* pSession = pRuntime->pSessions; NULL != pSession;
       pSession = pSession->pRuntimeNext) {
    dmCloseCorrelationTaskSession(pSession, TSDB_CODE_VND_STOPPED);
  }
  (void)taosThreadMutexUnlock(&pRuntime->lock);

  while (true) {
    (void)taosThreadMutexLock(&pRuntime->lock);
    bool drained = 0 == pRuntime->activeCalls;
    (void)taosThreadMutexUnlock(&pRuntime->lock);
    if (drained) {
      break;
    }
    int32_t code = tsem_wait(&pRuntime->drained);
    if (TSDB_CODE_SUCCESS != code) {
      dError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    }
  }
  schedulerDestroy();
  dmReleaseCorrelationRuntime(pRuntime);
}
