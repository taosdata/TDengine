/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 */

#include "streamWindowChain.h"

#include "cJSON.h"
#include "nodes.h"
#include "querynodes.h"
#include "streamInt.h"
#include "streamTriggerTask.h"
#include "tdatablock.h"
#include "ttime.h"

typedef struct {
  TSKEY    start;
  TSKEY    end;
  int64_t  rownum;
  int64_t  prevProcTimeNs;
  int64_t  nativeDiscriminator;
  uint64_t generation;
  bool     suppressed;
  bool     openEmitted;
  char    *pOpenNotifyContent;
} SWindowChainInstance;

typedef struct {
  int8_t  type;
  int32_t bytes;
  bool    defined;
  void   *pData;
} SWindowChainStateValue;

typedef struct {
  const SStreamWindowLayerSpec *pSpec;
  EStreamTriggerType            runtimeType;
  bool                          pureSliding;
  bool                          timeCursorInitialized;
  STimeWindow                   nextTimeRange;
  SArray                       *pInstances;
  SArray                       *pScratchRanges;
  SArray                       *pStateValues;
  SArray                       *pPendingStateValues;
  bool                         *pPendingStateTouched;
  int64_t                       totalCount;
  int64_t                       pendingNullCount;
  TSKEY                         pendingNullStart;
  int64_t                       deferredPartialNullCount;
  int64_t                       deferredTailAllNullCount;
  TSKEY                         firstDeferredPartialNullTs;
  TSKEY                         lastDeferredPartialNullTs;
  bool                          hasPendingPartialNull;
  bool                          multipleEventStarts;
  int32_t                       eventConditionIndex;
  int32_t                       eventSubwindowCount;
  bool                          eventParentActive;
  SWindowChainInstance          eventParent;
  int32_t                       eventStartConditionCount;
  TSKEY                         eventStartConditionFirstTs;
  int32_t                       eventEndConditionCount;
  TSKEY                         eventEndConditionFirstTs;
} SWindowChainLayerState;

typedef struct SWindowChainMutationJournal SWindowChainMutationJournal;

struct SWindowChainState {
  const SStreamWindowPlan     *pPlan;
  int64_t                      gid;
  SWindowChainPolicy           policy;
  int32_t                      numLayers;
  uint64_t                     nextInstanceGeneration;
  SWindowChainMutationJournal *pJournal;
  SWindowChainLayerState       layers[STREAM_WINDOW_MAX_LAYERS];
};

typedef enum {
  WINDOW_CHAIN_INSTANCE_MODIFIED,
  WINDOW_CHAIN_INSTANCE_INSERTED,
  WINDOW_CHAIN_INSTANCE_REMOVED,
} EWindowChainInstanceMutation;

typedef struct {
  EWindowChainInstanceMutation type;
  int32_t                      layerIndex;
  int32_t                      instanceIndex;
  SWindowChainInstance         instance;
} SWindowChainInstanceMutation;

struct SWindowChainMutationJournal {
  SWindowChainState     *pState;
  uint64_t               nextInstanceGeneration;
  SWindowChainLayerState layers[STREAM_WINDOW_MAX_LAYERS];
  SArray                *pInstanceMutations;
};

struct SWindowChainPeerGroupTxn {
  SWindowChainMutationJournal journal;
};

typedef enum {
  WINDOW_LAYER_INPUT_ROW,
  WINDOW_LAYER_INPUT_FRONTIER,
  WINDOW_LAYER_INPUT_ANCESTOR_END,
} EWindowLayerInputType;

typedef struct {
  EWindowLayerInputType type;
  TSKEY                 ts;
} SWindowLayerInput;

typedef struct {
  bool    resetBeforeRoute;
  bool    resetAfterRoute;
  SArray *pRanges;
} SWindowTransitionList;

typedef struct {
  SWindowChainInstance instance;
  SWindowChainInstance ancestors[STREAM_WINDOW_MAX_LAYERS - 1];
  int32_t              eventType;
  int64_t              nowNs;
  char                *pNotifyContent;
} SWindowChainCandidateIntent;

typedef struct {
  SWindowChainInstance ancestors[STREAM_WINDOW_MAX_LAYERS - 1];
  SArray              *pRows;
} SWindowChainAcceptedIntent;

static void stCopyWindowChainInstanceSnapshot(const SWindowChainInstance *pSource, SWindowChainInstance *pTarget) {
  *pTarget = *pSource;
  pTarget->pOpenNotifyContent = NULL;
}

static void stDestroyCandidateIntent(void *pValue) {
  SWindowChainCandidateIntent *pIntent = pValue;
  if (pIntent == NULL) return;
  taosMemoryFreeClear(pIntent->pNotifyContent);
}

static void stDestroyAcceptedIntent(void *pValue) {
  SWindowChainAcceptedIntent *pIntent = pValue;
  if (pIntent == NULL) return;
  taosArrayDestroy(pIntent->pRows);
  pIntent->pRows = NULL;
}

static int32_t stWindowChainAllocError(void) { return terrno == TSDB_CODE_SUCCESS ? TSDB_CODE_OUT_OF_MEMORY : terrno; }

void stDestroyRecalcImpactDomain(SRecalcImpactDomain *pDomain) {
  if (pDomain == NULL) return;
  taosArrayDestroy(pDomain->pRootExtents);
  *pDomain = (SRecalcImpactDomain){0};
}

int32_t stCloneRecalcImpactDomain(const SRecalcImpactDomain *pSrc, SRecalcImpactDomain *pDst) {
  if (pSrc == NULL || pDst == NULL) return TSDB_CODE_INVALID_PARA;
  SRecalcImpactDomain cloned = *pSrc;
  cloned.pRootExtents = taosArrayDup(pSrc->pRootExtents, NULL);
  if (pSrc->pRootExtents != NULL && cloned.pRootExtents == NULL) return stWindowChainAllocError();
  *pDst = cloned;
  return TSDB_CODE_SUCCESS;
}

static int32_t stAppendDomainExtent(SArray *pExtents, STimeWindow extent) {
  int32_t size = taosArrayGetSize(pExtents);
  if (size > 0) {
    STimeWindow *pLast = taosArrayGet(pExtents, size - 1);
    if (extent.skey <= pLast->ekey || (pLast->ekey != INT64_MAX && extent.skey == pLast->ekey + 1)) {
      pLast->ekey = TMAX(pLast->ekey, extent.ekey);
      return TSDB_CODE_SUCCESS;
    }
  }
  return taosArrayPush(pExtents, &extent) == NULL ? stWindowChainAllocError() : TSDB_CODE_SUCCESS;
}

int32_t stUnionRecalcImpactDomains(const SRecalcImpactDomain *pLeft, const SRecalcImpactDomain *pRight,
                                   SRecalcImpactDomain *pUnion) {
  if (pLeft == NULL || pRight == NULL || pUnion == NULL || pLeft->gid != pRight->gid) {
    return TSDB_CODE_INVALID_PARA;
  }
  SRecalcImpactDomain result = {
      .gid = pLeft->gid,
      .replayAnchor = TMIN(pLeft->replayAnchor, pRight->replayAnchor),
      .capturedFrontier = TMAX(pLeft->capturedFrontier, pRight->capturedFrontier),
      .pRootExtents = taosArrayInit(
          TMAX(taosArrayGetSize(pLeft->pRootExtents) + taosArrayGetSize(pRight->pRootExtents), 1), sizeof(STimeWindow)),
  };
  if (result.pRootExtents == NULL) return stWindowChainAllocError();

  int32_t left = 0;
  int32_t right = 0;
  int32_t code = TSDB_CODE_SUCCESS;
  while (left < taosArrayGetSize(pLeft->pRootExtents) || right < taosArrayGetSize(pRight->pRootExtents)) {
    const STimeWindow *pLeftExtent =
        left < taosArrayGetSize(pLeft->pRootExtents) ? taosArrayGet(pLeft->pRootExtents, left) : NULL;
    const STimeWindow *pRightExtent =
        right < taosArrayGetSize(pRight->pRootExtents) ? taosArrayGet(pRight->pRootExtents, right) : NULL;
    STimeWindow extent;
    if (pRightExtent == NULL || (pLeftExtent != NULL && pLeftExtent->skey <= pRightExtent->skey)) {
      extent = *pLeftExtent;
      ++left;
    } else {
      extent = *pRightExtent;
      ++right;
    }
    code = stAppendDomainExtent(result.pRootExtents, extent);
    if (code != TSDB_CODE_SUCCESS) break;
  }
  if (code != TSDB_CODE_SUCCESS) {
    stDestroyRecalcImpactDomain(&result);
    return code;
  }
  *pUnion = result;
  return TSDB_CODE_SUCCESS;
}

static void stDestroyWindowLineage(SWindowLineage *pLineage) {
  if (pLineage == NULL) return;
  taosArrayDestroy(pLineage->pScopes);
  pLineage->pScopes = NULL;
}

static int32_t stCloneWindowLineage(const SWindowLineage *pSource, SWindowLineage *pTarget) {
  pTarget->pScopes = taosArrayDup(pSource == NULL ? NULL : pSource->pScopes, NULL);
  if (pSource != NULL && pSource->pScopes != NULL && pTarget->pScopes == NULL) return stWindowChainAllocError();
  return TSDB_CODE_SUCCESS;
}

static void stDestroyAcceptedBatch(void *pValue) {
  SWindowChainAcceptedBatch *pBatch = pValue;
  if (pBatch == NULL) return;
  stDestroyWindowLineage(&pBatch->cacheScope.lineage);
  taosArrayDestroy(pBatch->pRows);
  pBatch->pRows = NULL;
}

static void stDestroyStateValue(void *pValue) {
  SWindowChainStateValue *pStateValue = pValue;
  if (pStateValue == NULL) return;
  taosMemoryFreeClear(pStateValue->pData);
}

static void stDestroyWindowChainInstance(void *pValue) {
  SWindowChainInstance *pInstance = pValue;
  if (pInstance == NULL) return;
  taosMemoryFreeClear(pInstance->pOpenNotifyContent);
}

void stDestroyLeafEventCandidate(void *pValue) {
  SLeafEventCandidate *pCandidate = pValue;
  if (pCandidate == NULL) return;
  stDestroyWindowLineage(&pCandidate->instanceId.lineage);
  stDestroyWindowLineage(&pCandidate->lineage);
  stDestroyWindowLineage(&pCandidate->cacheScope.lineage);
  taosArrayDestroy(pCandidate->pAncestorSnapshots);
  pCandidate->pAncestorSnapshots = NULL;
  tDestroySSTriggerCalcParam(&pCandidate->leafParam);
}

void stDestroyWindowChainSubmitResult(SWindowChainSubmitResult *pResult) {
  if (pResult == NULL) return;
  taosArrayDestroyEx(pResult->pAcceptedBatches, stDestroyAcceptedBatch);
  taosArrayDestroyEx(pResult->pCandidates, stDestroyLeafEventCandidate);
  pResult->pAcceptedBatches = NULL;
  pResult->pCandidates = NULL;
}

static void stDestroyLayerStates(SWindowChainState *pState) {
  if (pState == NULL) return;
  for (int32_t i = 0; i < pState->numLayers; ++i) {
    taosArrayDestroyEx(pState->layers[i].pInstances, stDestroyWindowChainInstance);
    stDestroyWindowChainInstance(&pState->layers[i].eventParent);
    taosArrayDestroy(pState->layers[i].pScratchRanges);
    taosArrayDestroyEx(pState->layers[i].pStateValues, stDestroyStateValue);
    taosArrayDestroyEx(pState->layers[i].pPendingStateValues, stDestroyStateValue);
    taosMemoryFreeClear(pState->layers[i].pPendingStateTouched);
    pState->layers[i].pInstances = NULL;
    pState->layers[i].pScratchRanges = NULL;
    pState->layers[i].pStateValues = NULL;
    pState->layers[i].pPendingStateValues = NULL;
  }
}

void stWindowChainDestroy(SWindowChainState **ppState) {
  if (ppState == NULL || *ppState == NULL) return;
  stDestroyLayerStates(*ppState);
  taosMemoryFree(*ppState);
  *ppState = NULL;
}

bool stWindowChainGetInputRetentionRange(const SWindowChainState *pState, STimeWindow *pRange) {
  if (pState == NULL || pRange == NULL || pState->numLayers <= 0) return false;

  const SWindowChainLayerState *pLeaf = &pState->layers[pState->numLayers - 1];
  STimeWindow                   range = {.skey = INT64_MAX, .ekey = INT64_MIN};
  for (int32_t i = 0; i < taosArrayGetSize(pLeaf->pInstances); ++i) {
    const SWindowChainInstance *pInstance = taosArrayGet(pLeaf->pInstances, i);
    if (pInstance == NULL || pInstance->rownum <= 0) continue;
    range.skey = TMIN(range.skey, pInstance->start);
    range.ekey = TMAX(range.ekey, pInstance->end);
  }
  if (pLeaf->eventParentActive && pLeaf->eventParent.rownum > 0) {
    range.skey = TMIN(range.skey, pLeaf->eventParent.start);
    range.ekey = TMAX(range.ekey, pLeaf->eventParent.end);
  }
  if (pLeaf->eventStartConditionCount > 0 && pLeaf->eventStartConditionFirstTs != INT64_MIN) {
    range.skey = TMIN(range.skey, pLeaf->eventStartConditionFirstTs);
    range.ekey = TMAX(range.ekey, pLeaf->eventStartConditionFirstTs);
  }
  if (pLeaf->runtimeType == STREAM_TRIGGER_STATE &&
      pLeaf->pSpec->trigger.stateWin.extend == STATE_WIN_EXTEND_OPTION_FORWARD && pLeaf->pendingNullCount > 0) {
    range.skey = TMIN(range.skey, pLeaf->pendingNullStart);
    range.ekey = TMAX(range.ekey, pLeaf->pendingNullStart);
  }
  if (range.skey > range.ekey) return false;
  *pRange = range;
  return true;
}

bool stWindowChainGetFirstOpenCountLeafRange(const SWindowChainState *pState, STimeWindow *pRange) {
  if (pState == NULL || pRange == NULL || pState->numLayers <= 0) return false;

  const SWindowChainLayerState *pLeaf = &pState->layers[pState->numLayers - 1];
  if (pLeaf->runtimeType != STREAM_TRIGGER_COUNT) return false;

  const SWindowChainInstance *pInstance = taosArrayGet(pLeaf->pInstances, 0);
  if (pInstance == NULL) return false;

  *pRange = (STimeWindow){.skey = pInstance->start, .ekey = pInstance->end};
  return true;
}

bool stWindowChainCanRepairCountDisorder(const SWindowChainState *pState, const STimeWindow *pRange) {
  if (pState == NULL || pRange == NULL || pRange->skey != pRange->ekey || pState->numLayers < 2) return false;

  const SWindowChainLayerState *pLeaf = &pState->layers[pState->numLayers - 1];
  if (pLeaf->runtimeType != STREAM_TRIGGER_COUNT || pLeaf->pSpec->trigger.count.sliding != 1) return false;

  const int32_t instanceCount = taosArrayGetSize(pLeaf->pInstances);
  if (instanceCount < 2) return false;
  const SWindowChainInstance *pFirst = taosArrayGet(pLeaf->pInstances, 0);
  const SWindowChainInstance *pLast = taosArrayGet(pLeaf->pInstances, instanceCount - 1);
  if (pFirst == NULL || pLast == NULL || pRange->skey <= pFirst->start || pRange->ekey >= pLast->end) return false;
  for (int32_t i = 0; i < instanceCount; ++i) {
    const SWindowChainInstance *pInstance = taosArrayGet(pLeaf->pInstances, i);
    if (pInstance == NULL || pInstance->start == pRange->skey) return false;
  }

  for (int32_t i = 0; i < pState->numLayers - 1; ++i) {
    const SWindowChainLayerState *pLayer = &pState->layers[i];
    const SSlidingTrigger        *pSliding = &pLayer->pSpec->trigger.sliding;
    const SWindowChainInstance   *pInstance = taosArrayGet(pLayer->pInstances, 0);
    if (pLayer->runtimeType != STREAM_TRIGGER_SLIDING || pLayer->pureSliding || pSliding->interval <= 0 ||
        pSliding->interval != pSliding->sliding || pSliding->intervalUnit != pSliding->slidingUnit ||
        taosArrayGetSize(pLayer->pInstances) != 1 || pInstance == NULL || pRange->skey < pInstance->start ||
        pRange->ekey > pInstance->end) {
      return false;
    }
  }
  return true;
}

void stWindowChainSuppressOpenCountLeafBefore(SWindowChainState *pState, TSKEY firstUnaffectedStart) {
  if (pState == NULL || pState->numLayers <= 0) return;

  SWindowChainLayerState *pLeaf = &pState->layers[pState->numLayers - 1];
  if (pLeaf->runtimeType != STREAM_TRIGGER_COUNT) return;

  for (int32_t i = 0; i < taosArrayGetSize(pLeaf->pInstances); ++i) {
    SWindowChainInstance *pInstance = taosArrayGet(pLeaf->pInstances, i);
    if (pInstance != NULL && pInstance->start < firstUnaffectedStart) {
      pInstance->suppressed = true;
    }
  }
}

static int32_t stWindowChainEventHasMultipleStarts(const SEventTrigger *pEvent, bool *pMultiple) {
  *pMultiple = false;
  if (pEvent->startCond == NULL) return TSDB_CODE_INVALID_PARA;
  SNode  *pNode = NULL;
  int32_t code = nodesStringToNode(pEvent->startCond, &pNode);
  if (code != TSDB_CODE_SUCCESS) return code;
  *pMultiple = nodeType(pNode) == QUERY_NODE_NODE_LIST;
  nodesDestroyNode(pNode);
  return TSDB_CODE_SUCCESS;
}

int32_t stWindowChainCreate(const SStreamWindowPlan *pPlan, int64_t gid, const SWindowChainPolicy *pPolicy,
                            SWindowChainState **ppState) {
  if (pPlan == NULL || pPolicy == NULL || ppState == NULL || *ppState != NULL || pPolicy->maxDelayNs < 0 ||
      pPlan->version != STREAM_WINDOW_PLAN_VERSION || pPlan->pLayers == NULL ||
      pPlan->pLayers->elemSize != sizeof(SStreamWindowLayerSpec)) {
    return TSDB_CODE_INVALID_PARA;
  }

  const int32_t numLayers = taosArrayGetSize(pPlan->pLayers);
  if (numLayers < 2 || numLayers > STREAM_WINDOW_MAX_LAYERS) return TSDB_CODE_INVALID_PARA;

  SStreamWindowPlanValidationCtx validationCtx = {0};
  int32_t                        code = tValidateStreamWindowPlan(pPlan, &validationCtx);
  if (code != TSDB_CODE_SUCCESS) return code;

  SWindowChainState *pState = taosMemoryCalloc(1, sizeof(*pState));
  if (pState == NULL) return stWindowChainAllocError();
  pState->pPlan = pPlan;
  pState->gid = gid;
  pState->policy = *pPolicy;
  pState->numLayers = numLayers;

  for (int32_t i = 0; i < numLayers; ++i) {
    SWindowChainLayerState       *pRuntime = &pState->layers[i];
    const SStreamWindowLayerSpec *pLayer = taosArrayGet(pPlan->pLayers, i);
    pRuntime->pSpec = pLayer;
    pRuntime->pureSliding = pLayer->triggerType == WINDOW_TYPE_INTERVAL && pLayer->trigger.sliding.interval == 0;
    const int32_t runtimeType = stWindowPlanTypeToTriggerType((EWindowType)pLayer->triggerType, &pLayer->trigger);
    if (runtimeType < 0) {
      code = TSDB_CODE_INVALID_PARA;
      goto _exit;
    }
    pRuntime->runtimeType = (EStreamTriggerType)runtimeType;
    pRuntime->pInstances = taosArrayInit(2, sizeof(SWindowChainInstance));
    pRuntime->pScratchRanges = taosArrayInit(4, sizeof(STimeWindow));
    if (pRuntime->pInstances == NULL || pRuntime->pScratchRanges == NULL) {
      code = stWindowChainAllocError();
      goto _exit;
    }
    if (pLayer->triggerType == WINDOW_TYPE_STATE) {
      const int32_t stateCount = taosArrayGetSize(pLayer->input.pConditionSlotIds);
      pRuntime->pStateValues = taosArrayInit(stateCount, sizeof(SWindowChainStateValue));
      pRuntime->pPendingStateValues = taosArrayInit(stateCount, sizeof(SWindowChainStateValue));
      pRuntime->pPendingStateTouched = taosMemoryCalloc(stateCount, sizeof(bool));
      if (pRuntime->pStateValues == NULL || pRuntime->pPendingStateValues == NULL ||
          pRuntime->pPendingStateTouched == NULL) {
        code = stWindowChainAllocError();
        goto _exit;
      }
      for (int32_t j = 0; j < stateCount; ++j) {
        const SWindowChainStateValue value = {0};
        if (taosArrayPush(pRuntime->pStateValues, &value) == NULL ||
            taosArrayPush(pRuntime->pPendingStateValues, &value) == NULL) {
          code = stWindowChainAllocError();
          goto _exit;
        }
      }
    } else if (pLayer->triggerType == WINDOW_TYPE_EVENT) {
      code = stWindowChainEventHasMultipleStarts(&pLayer->trigger.event, &pRuntime->multipleEventStarts);
      if (code != TSDB_CODE_SUCCESS || (i < numLayers - 1 && pRuntime->multipleEventStarts)) {
        if (code == TSDB_CODE_SUCCESS) code = TSDB_CODE_INVALID_PARA;
        goto _exit;
      }
    }
  }

  *ppState = pState;
  return TSDB_CODE_SUCCESS;

_exit:
  stWindowChainDestroy(&pState);
  return code;
}

static int32_t stCloneStateValues(const SArray *pSource, SArray **ppTarget) {
  if (pSource == NULL) return TSDB_CODE_SUCCESS;
  SArray *pTarget = taosArrayInit(taosArrayGetSize(pSource), sizeof(SWindowChainStateValue));
  if (pTarget == NULL) return stWindowChainAllocError();
  for (int32_t i = 0; i < taosArrayGetSize(pSource); ++i) {
    const SWindowChainStateValue *pValue = taosArrayGet(pSource, i);
    SWindowChainStateValue        copy = *pValue;
    copy.pData = NULL;
    if (pValue->pData != NULL && pValue->bytes > 0) {
      copy.pData = taosMemoryMalloc(pValue->bytes);
      if (copy.pData == NULL) {
        taosArrayDestroyEx(pTarget, stDestroyStateValue);
        return stWindowChainAllocError();
      }
      TAOS_MEMCPY(copy.pData, pValue->pData, pValue->bytes);
    }
    if (taosArrayPush(pTarget, &copy) == NULL) {
      stDestroyStateValue(&copy);
      taosArrayDestroyEx(pTarget, stDestroyStateValue);
      return stWindowChainAllocError();
    }
  }
  *ppTarget = pTarget;
  return TSDB_CODE_SUCCESS;
}

static int32_t stCloneWindowChainInstance(const SWindowChainInstance *pSource, SWindowChainInstance *pTarget) {
  *pTarget = *pSource;
  pTarget->pOpenNotifyContent = NULL;
  if (pSource->pOpenNotifyContent != NULL) {
    pTarget->pOpenNotifyContent = taosStrdup(pSource->pOpenNotifyContent);
    if (pTarget->pOpenNotifyContent == NULL) return stWindowChainAllocError();
  }
  return TSDB_CODE_SUCCESS;
}

static void stDestroyWindowChainInstanceMutation(void *pValue) {
  SWindowChainInstanceMutation *pMutation = pValue;
  if (pMutation == NULL || pMutation->type == WINDOW_CHAIN_INSTANCE_INSERTED) return;
  stDestroyWindowChainInstance(&pMutation->instance);
}

static int32_t stWindowChainBeginMutation(SWindowChainState *pState, SWindowChainMutationJournal *pJournal) {
  if (pState->pJournal != NULL) return TSDB_CODE_INTERNAL_ERROR;
  *pJournal =
      (SWindowChainMutationJournal){.pState = pState,
                                    .nextInstanceGeneration = pState->nextInstanceGeneration,
                                    .pInstanceMutations = taosArrayInit(8, sizeof(SWindowChainInstanceMutation))};
  if (pJournal->pInstanceMutations == NULL) return stWindowChainAllocError();
  for (int32_t i = 0; i < pState->numLayers; ++i) {
    pJournal->layers[i] = pState->layers[i];
    pJournal->layers[i].eventParent.pOpenNotifyContent = NULL;
    int32_t code = stCloneWindowChainInstance(&pState->layers[i].eventParent, &pJournal->layers[i].eventParent);
    if (code != TSDB_CODE_SUCCESS) {
      for (int32_t j = 0; j < i; ++j) stDestroyWindowChainInstance(&pJournal->layers[j].eventParent);
      taosArrayDestroy(pJournal->pInstanceMutations);
      *pJournal = (SWindowChainMutationJournal){0};
      return code;
    }
  }
  pState->pJournal = pJournal;
  return TSDB_CODE_SUCCESS;
}

static int32_t stWindowChainLayerIndex(const SWindowChainState *pState, const SArray *pInstances) {
  for (int32_t i = 0; i < pState->numLayers; ++i) {
    if (pState->layers[i].pInstances == pInstances) return i;
  }
  return -1;
}

static int32_t stWindowChainReserveInstanceMutation(SWindowChainState *pState) {
  if (pState->pJournal == NULL) return TSDB_CODE_SUCCESS;
  SArray *pMutations = pState->pJournal->pInstanceMutations;
  return taosArrayEnsureCap(pMutations, taosArrayGetSize(pMutations) + 1);
}

static void stWindowChainAppendReservedInstanceMutation(SWindowChainMutationJournal        *pJournal,
                                                        const SWindowChainInstanceMutation *pMutation) {
  TAOS_MEMCPY(TARRAY_GET_ELEM(pJournal->pInstanceMutations, pJournal->pInstanceMutations->size), pMutation,
              sizeof(*pMutation));
  ++pJournal->pInstanceMutations->size;
}

static int32_t stWindowChainTouchInstanceAt(SWindowChainState *pState, int32_t layerIndex, int32_t instanceIndex) {
  if (pState->pJournal == NULL) return TSDB_CODE_SUCCESS;
  int32_t code = stWindowChainReserveInstanceMutation(pState);
  if (code != TSDB_CODE_SUCCESS) return code;
  SWindowChainInstanceMutation mutation = {
      .type = WINDOW_CHAIN_INSTANCE_MODIFIED, .layerIndex = layerIndex, .instanceIndex = instanceIndex};
  const SWindowChainInstance *pInstance = taosArrayGet(pState->layers[layerIndex].pInstances, instanceIndex);
  if (pInstance == NULL) return TSDB_CODE_INTERNAL_ERROR;
  code = stCloneWindowChainInstance(pInstance, &mutation.instance);
  if (code != TSDB_CODE_SUCCESS) return code;
  stWindowChainAppendReservedInstanceMutation(pState->pJournal, &mutation);
  return TSDB_CODE_SUCCESS;
}

static int32_t stWindowChainTouchInstance(SWindowChainState *pState, SWindowChainInstance *pInstance) {
  if (pState->pJournal == NULL || pInstance == NULL) return TSDB_CODE_SUCCESS;
  for (int32_t i = 0; i < pState->numLayers; ++i) {
    SArray *pInstances = pState->layers[i].pInstances;
    if (pInstances->size == 0) continue;
    SWindowChainInstance *pFirst = TARRAY_GET_ELEM(pInstances, 0);
    SWindowChainInstance *pEnd = TARRAY_GET_ELEM(pInstances, pInstances->size);
    if ((uintptr_t)pInstance >= (uintptr_t)pFirst && (uintptr_t)pInstance < (uintptr_t)pEnd) {
      return stWindowChainTouchInstanceAt(pState, i, TARRAY_ELEM_IDX(pInstances, pInstance));
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t stWindowChainInsertInstance(SWindowChainState *pState, int32_t layerIndex, int32_t instanceIndex,
                                           const SWindowChainInstance *pInstance, SWindowChainInstance **ppInserted) {
  int32_t code = stWindowChainReserveInstanceMutation(pState);
  if (code != TSDB_CODE_SUCCESS) return code;
  SArray               *pInstances = pState->layers[layerIndex].pInstances;
  SWindowChainInstance *pInserted = taosArrayInsert(pInstances, instanceIndex, pInstance);
  if (pInserted == NULL) return stWindowChainAllocError();
  if (pState->pJournal != NULL) {
    SWindowChainInstanceMutation mutation = {
        .type = WINDOW_CHAIN_INSTANCE_INSERTED, .layerIndex = layerIndex, .instanceIndex = instanceIndex};
    stWindowChainAppendReservedInstanceMutation(pState->pJournal, &mutation);
  }
  if (ppInserted != NULL) *ppInserted = taosArrayGet(pInstances, instanceIndex);
  return TSDB_CODE_SUCCESS;
}

static int32_t stWindowChainRemoveInstance(SWindowChainState *pState, int32_t layerIndex, int32_t instanceIndex) {
  SArray               *pInstances = pState->layers[layerIndex].pInstances;
  SWindowChainInstance *pInstance = taosArrayGet(pInstances, instanceIndex);
  if (pInstance == NULL) return TSDB_CODE_INTERNAL_ERROR;
  if (pState->pJournal == NULL) {
    taosArrayRemoveBatch(pInstances, instanceIndex, 1, stDestroyWindowChainInstance);
    return TSDB_CODE_SUCCESS;
  }
  int32_t code = stWindowChainReserveInstanceMutation(pState);
  if (code != TSDB_CODE_SUCCESS) return code;
  SWindowChainInstanceMutation mutation = {.type = WINDOW_CHAIN_INSTANCE_REMOVED,
                                           .layerIndex = layerIndex,
                                           .instanceIndex = instanceIndex,
                                           .instance = *pInstance};
  stWindowChainAppendReservedInstanceMutation(pState->pJournal, &mutation);
  taosArrayRemove(pInstances, instanceIndex);
  return TSDB_CODE_SUCCESS;
}

static int32_t stWindowChainClearInstances(SWindowChainState *pState, int32_t layerIndex) {
  SArray *pInstances = pState->layers[layerIndex].pInstances;
  for (int32_t i = taosArrayGetSize(pInstances) - 1; i >= 0; --i) {
    int32_t code = stWindowChainRemoveInstance(pState, layerIndex, i);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t stWindowChainCowStateValues(SWindowChainState *pState, int32_t layerIndex) {
  SWindowChainMutationJournal *pJournal = pState->pJournal;
  if (pJournal == NULL) return TSDB_CODE_SUCCESS;
  SWindowChainLayerState       *pLayer = &pState->layers[layerIndex];
  const SWindowChainLayerState *pOriginal = &pJournal->layers[layerIndex];
  if (pLayer->pStateValues != pOriginal->pStateValues) return TSDB_CODE_SUCCESS;

  SArray *pStateValues = NULL;
  SArray *pPendingValues = NULL;
  bool   *pTouched = NULL;
  int32_t code = stCloneStateValues(pLayer->pStateValues, &pStateValues);
  if (code == TSDB_CODE_SUCCESS) code = stCloneStateValues(pLayer->pPendingStateValues, &pPendingValues);
  if (code == TSDB_CODE_SUCCESS && pLayer->pPendingStateTouched != NULL) {
    const int32_t stateCount = taosArrayGetSize(pLayer->pStateValues);
    pTouched = taosMemoryMalloc(stateCount * sizeof(bool));
    if (pTouched == NULL) {
      code = stWindowChainAllocError();
    } else {
      TAOS_MEMCPY(pTouched, pLayer->pPendingStateTouched, stateCount * sizeof(bool));
    }
  }
  if (code != TSDB_CODE_SUCCESS) {
    taosArrayDestroyEx(pStateValues, stDestroyStateValue);
    taosArrayDestroyEx(pPendingValues, stDestroyStateValue);
    taosMemoryFree(pTouched);
    return code;
  }
  pLayer->pStateValues = pStateValues;
  pLayer->pPendingStateValues = pPendingValues;
  pLayer->pPendingStateTouched = pTouched;
  return TSDB_CODE_SUCCESS;
}

static void stWindowChainFinishMutation(SWindowChainMutationJournal *pJournal, bool commit) {
  SWindowChainState *pState = pJournal->pState;
  if (!commit) {
    for (int32_t i = taosArrayGetSize(pJournal->pInstanceMutations) - 1; i >= 0; --i) {
      SWindowChainInstanceMutation *pMutation = taosArrayGet(pJournal->pInstanceMutations, i);
      SArray                       *pInstances = pState->layers[pMutation->layerIndex].pInstances;
      if (pMutation->type == WINDOW_CHAIN_INSTANCE_MODIFIED) {
        SWindowChainInstance *pInstance = taosArrayGet(pInstances, pMutation->instanceIndex);
        stDestroyWindowChainInstance(pInstance);
        *pInstance = pMutation->instance;
        pMutation->instance.pOpenNotifyContent = NULL;
      } else if (pMutation->type == WINDOW_CHAIN_INSTANCE_INSERTED) {
        stDestroyWindowChainInstance(taosArrayGet(pInstances, pMutation->instanceIndex));
        taosArrayRemove(pInstances, pMutation->instanceIndex);
      } else {
        void *pTarget = TARRAY_GET_ELEM(pInstances, pMutation->instanceIndex);
        memmove((char *)pTarget + pInstances->elemSize, pTarget,
                (pInstances->size - pMutation->instanceIndex) * pInstances->elemSize);
        TAOS_MEMCPY(pTarget, &pMutation->instance, sizeof(pMutation->instance));
        ++pInstances->size;
        pMutation->instance.pOpenNotifyContent = NULL;
      }
    }
    pState->nextInstanceGeneration = pJournal->nextInstanceGeneration;
  }

  for (int32_t i = 0; i < pState->numLayers; ++i) {
    SWindowChainLayerState *pLayer = &pState->layers[i];
    SWindowChainLayerState *pOriginal = &pJournal->layers[i];
    if (pLayer->pStateValues != pOriginal->pStateValues) {
      if (commit) {
        taosArrayDestroyEx(pOriginal->pStateValues, stDestroyStateValue);
        taosArrayDestroyEx(pOriginal->pPendingStateValues, stDestroyStateValue);
        taosMemoryFree(pOriginal->pPendingStateTouched);
      } else {
        taosArrayDestroyEx(pLayer->pStateValues, stDestroyStateValue);
        taosArrayDestroyEx(pLayer->pPendingStateValues, stDestroyStateValue);
        taosMemoryFree(pLayer->pPendingStateTouched);
      }
    }
    if (commit) {
      stDestroyWindowChainInstance(&pOriginal->eventParent);
      continue;
    }
    SArray *pInstances = pLayer->pInstances;
    SArray *pScratchRanges = pLayer->pScratchRanges;
    stDestroyWindowChainInstance(&pLayer->eventParent);
    *pLayer = *pOriginal;
    pLayer->pInstances = pInstances;
    pLayer->pScratchRanges = pScratchRanges;
    pOriginal->eventParent.pOpenNotifyContent = NULL;
  }
  pState->pJournal = NULL;
  taosArrayDestroyEx(pJournal->pInstanceMutations, stDestroyWindowChainInstanceMutation);
  *pJournal = (SWindowChainMutationJournal){0};
}

static SInterval stBuildInterval(const SSlidingTrigger *pSliding, bool pureSliding) {
  SInterval interval = {0};
  interval.timezone = NULL;
  interval.intervalUnit = pureSliding ? pSliding->slidingUnit : pSliding->intervalUnit;
  interval.slidingUnit = pSliding->slidingUnit;
  interval.offsetUnit = pureSliding ? pSliding->soffsetUnit : pSliding->offsetUnit;
  interval.precision = pSliding->precision;
  interval.interval = pureSliding ? pSliding->sliding : pSliding->interval;
  interval.sliding = pSliding->sliding;
  interval.offset = pureSliding ? pSliding->soffset : pSliding->offset;
  interval.timeRange = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MIN};
  return interval;
}

static int32_t stPushRange(SArray *pRanges, TSKEY start, TSKEY end) {
  const STimeWindow range = {.skey = start, .ekey = end};
  return taosArrayPush(pRanges, &range) == NULL ? stWindowChainAllocError() : TSDB_CODE_SUCCESS;
}

static int32_t stPureSlidingRange(const SSlidingTrigger *pSliding, TSKEY ts, STimeWindow *pRange) {
  SInterval interval = stBuildInterval(pSliding, true);
  TSKEY     boundary = taosTimeTruncate(ts, &interval);
  if (boundary == ts) {
    TSKEY previous = getNextTimeWindowStart(&interval, boundary, TSDB_ORDER_DESC);
    if (previous == INT64_MAX || previous == INT64_MIN || previous == boundary) return TSDB_CODE_INVALID_PARA;
    pRange->skey = previous == INT64_MAX ? INT64_MAX : previous + 1;
    pRange->ekey = boundary;
  } else {
    TSKEY next = getNextTimeWindowStart(&interval, boundary, TSDB_ORDER_ASC);
    if (next == INT64_MAX || next == INT64_MIN || next == boundary) return TSDB_CODE_INVALID_PARA;
    pRange->skey = boundary == INT64_MAX ? INT64_MAX : boundary + 1;
    pRange->ekey = next;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t stIntervalRanges(const SWindowChainLayerState *pLayer, TSKEY ts, SArray *pRanges) {
  const SSlidingTrigger *pSliding = &pLayer->pSpec->trigger.sliding;
  if (pLayer->pureSliding) {
    STimeWindow range = {0};
    int32_t     code = stPureSlidingRange(pSliding, ts, &range);
    if (code != TSDB_CODE_SUCCESS) return code;
    return stPushRange(pRanges, range.skey, range.ekey);
  }

  SInterval interval = stBuildInterval(pSliding, false);
  TSKEY     start = taosTimeTruncate(ts, &interval);
  while (true) {
    TSKEY end = taosTimeGetIntervalEnd(start, &interval);
    if (start <= ts && ts <= end) {
      int32_t code = stPushRange(pRanges, start, end);
      if (code != TSDB_CODE_SUCCESS) return code;
    }
    TSKEY next = getNextTimeWindowStart(&interval, start, TSDB_ORDER_ASC);
    if (next <= start || next > ts) break;
    start = next;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t stNextTimeRange(const SWindowChainLayerState *pLayer, const STimeWindow *pCurrent, STimeWindow *pNext) {
  const SSlidingTrigger *pSliding = &pLayer->pSpec->trigger.sliding;
  if (pLayer->pureSliding) {
    if (pCurrent->ekey == INT64_MAX) return TSDB_CODE_INVALID_PARA;
    int32_t code = stPureSlidingRange(pSliding, pCurrent->ekey + 1, pNext);
    if (code != TSDB_CODE_SUCCESS) return code;
  } else {
    SInterval interval = stBuildInterval(pSliding, false);
    TSKEY     start = getNextTimeWindowStart(&interval, pCurrent->skey, TSDB_ORDER_ASC);
    if (start == INT64_MIN || start == INT64_MAX || start <= pCurrent->skey) return TSDB_CODE_INVALID_PARA;
    *pNext = (STimeWindow){.skey = start, .ekey = taosTimeGetIntervalEnd(start, &interval)};
  }
  if (pNext->skey <= pCurrent->skey || pNext->ekey < pNext->skey) return TSDB_CODE_INVALID_PARA;
  return TSDB_CODE_SUCCESS;
}

static int32_t stWindowLayerApply(SWindowChainState *pState, int32_t layerIndex, const SWindowLayerInput *pInput,
                                  SWindowTransitionList *pTransitions) {
  SWindowChainLayerState *pLayer = &pState->layers[layerIndex];
  SWindowChainInstance   *pCurrent = taosArrayGet(pLayer->pInstances, 0);

  switch (pLayer->runtimeType) {
    case STREAM_TRIGGER_SLIDING: {
      int32_t code = stIntervalRanges(pLayer, pInput->ts, pTransitions->pRanges);
      if (code != TSDB_CODE_SUCCESS || pInput->type == WINDOW_LAYER_INPUT_FRONTIER) return code;
      const STimeWindow *pTarget = taosArrayGet(pTransitions->pRanges, 0);
      if (pCurrent != NULL && (pTarget == NULL || pCurrent->start != pTarget->skey)) {
        pTransitions->resetBeforeRoute = true;
      }
      if (pLayer->pureSliding && pTarget != NULL && pInput->ts == pTarget->ekey) {
        pTransitions->resetAfterRoute = true;
      }
      return TSDB_CODE_SUCCESS;
    }
    case STREAM_TRIGGER_SESSION:
      if (pInput->type == WINDOW_LAYER_INPUT_ROW && pCurrent != NULL &&
          pCurrent->end <= INT64_MAX - pLayer->pSpec->trigger.session.sessionVal &&
          pInput->ts > pCurrent->end + pLayer->pSpec->trigger.session.sessionVal) {
        pTransitions->resetBeforeRoute = true;
      }
      return TSDB_CODE_SUCCESS;
    default:
      return TSDB_CODE_INVALID_PARA;
  }
}

static bool stWindowChainDataDriven(EStreamTriggerType type) {
  return type == STREAM_TRIGGER_STATE || type == STREAM_TRIGGER_COUNT || type == STREAM_TRIGGER_EVENT;
}

static const SColumnInfoData *stWindowChainGetColumn(const SWindowChainRowRef *pRow, int16_t slotId) {
  if (pRow == NULL || pRow->pBlock == NULL || slotId < 0 || slotId >= taosArrayGetSize(pRow->pBlock->pDataBlock)) {
    return NULL;
  }
  return taosArrayGet(pRow->pBlock->pDataBlock, slotId);
}

static int32_t stWindowChainGetTimestamp(const SWindowChainLayerState *pLayer, const SWindowChainRowRef *pRow,
                                         TSKEY *pTs) {
  const SColumnInfoData *pColumn = stWindowChainGetColumn(pRow, pLayer->pSpec->input.tsSlotId);
  if (pColumn == NULL || pColumn->info.type != TSDB_DATA_TYPE_TIMESTAMP || colDataIsNull_s(pColumn, pRow->rowIndex)) {
    return TSDB_CODE_INVALID_PARA;
  }
  *pTs = *(const TSKEY *)colDataGetData(pColumn, pRow->rowIndex);
  return TSDB_CODE_SUCCESS;
}

static int32_t stWindowChainStateDatumBytes(const SColumnInfoData *pColumn, const char *pData) {
  return IS_VAR_DATA_TYPE(pColumn->info.type) ? varDataTLen(pData) : pColumn->info.bytes;
}

static int32_t stWindowChainCountRowMatches(const SWindowChainLayerState *pLayer, const SWindowChainRowRef *pRow,
                                            bool *pMatches) {
  *pMatches = true;
  const SArray *pSlots = pLayer->pSpec->input.pConditionSlotIds;
  if (pSlots == NULL || taosArrayGetSize(pSlots) == 0) return TSDB_CODE_SUCCESS;

  *pMatches = false;
  for (int32_t i = 0; i < taosArrayGetSize(pSlots); ++i) {
    const int16_t          slot = *(const int16_t *)taosArrayGet(pSlots, i);
    const SColumnInfoData *pColumn = stWindowChainGetColumn(pRow, slot);
    if (pColumn == NULL) return TSDB_CODE_INVALID_PARA;
    if (!colDataIsNull_s(pColumn, pRow->rowIndex)) {
      *pMatches = true;
      return TSDB_CODE_SUCCESS;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static bool stWindowChainStateRowAllNull(const SWindowChainLayerState *pLayer, const SWindowChainRowRef *pRow) {
  for (int32_t i = 0; i < taosArrayGetSize(pLayer->pSpec->input.pConditionSlotIds); ++i) {
    const int16_t          slot = *(const int16_t *)taosArrayGet(pLayer->pSpec->input.pConditionSlotIds, i);
    const SColumnInfoData *pColumn = stWindowChainGetColumn(pRow, slot);
    if (pColumn == NULL || !colDataIsNull_s(pColumn, pRow->rowIndex)) return false;
  }
  return true;
}

static int32_t stWindowChainStateRowEqual(SWindowChainLayerState *pLayer, const SWindowChainRowRef *pRow,
                                          bool *pEqual) {
  *pEqual = true;
  for (int32_t i = 0; i < taosArrayGetSize(pLayer->pSpec->input.pConditionSlotIds); ++i) {
    const int16_t           slot = *(const int16_t *)taosArrayGet(pLayer->pSpec->input.pConditionSlotIds, i);
    const SColumnInfoData  *pColumn = stWindowChainGetColumn(pRow, slot);
    SWindowChainStateValue *pValue = taosArrayGet(pLayer->pStateValues, i);
    if (pColumn == NULL || pValue == NULL) return TSDB_CODE_INVALID_PARA;
    if (colDataIsNull_s(pColumn, pRow->rowIndex)) continue;
    const char   *pData = colDataGetData(pColumn, pRow->rowIndex);
    const int32_t bytes = stWindowChainStateDatumBytes(pColumn, pData);
    if (pValue->defined &&
        (pValue->type != pColumn->info.type || pValue->bytes != bytes || memcmp(pValue->pData, pData, bytes) != 0)) {
      *pEqual = false;
      return TSDB_CODE_SUCCESS;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t stWindowChainAssignStateRowToValues(SWindowChainLayerState *pLayer, const SWindowChainRowRef *pRow,
                                                   SArray *pValues, bool clearFirst) {
  if (clearFirst) {
    for (int32_t i = 0; i < taosArrayGetSize(pValues); ++i) {
      SWindowChainStateValue *pValue = taosArrayGet(pValues, i);
      pValue->defined = false;
    }
  }
  for (int32_t i = 0; i < taosArrayGetSize(pLayer->pSpec->input.pConditionSlotIds); ++i) {
    const int16_t           slot = *(const int16_t *)taosArrayGet(pLayer->pSpec->input.pConditionSlotIds, i);
    const SColumnInfoData  *pColumn = stWindowChainGetColumn(pRow, slot);
    SWindowChainStateValue *pValue = taosArrayGet(pValues, i);
    if (pColumn == NULL || pValue == NULL) return TSDB_CODE_INVALID_PARA;
    if (colDataIsNull_s(pColumn, pRow->rowIndex)) continue;
    const char   *pData = colDataGetData(pColumn, pRow->rowIndex);
    const int32_t bytes = stWindowChainStateDatumBytes(pColumn, pData);
    if (bytes <= 0) return TSDB_CODE_INVALID_PARA;
    if (pValue->bytes < bytes) {
      void *pNew = taosMemoryRealloc(pValue->pData, bytes);
      if (pNew == NULL) return stWindowChainAllocError();
      pValue->pData = pNew;
    }
    TAOS_MEMCPY(pValue->pData, pData, bytes);
    pValue->type = pColumn->info.type;
    pValue->bytes = bytes;
    pValue->defined = true;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t stWindowChainAssignStateRow(SWindowChainLayerState *pLayer, const SWindowChainRowRef *pRow,
                                           bool clearFirst) {
  return stWindowChainAssignStateRowToValues(pLayer, pRow, pLayer->pStateValues, clearFirst);
}

static int32_t stWindowChainCopyStateValue(const SWindowChainStateValue *pSource, SWindowChainStateValue *pTarget) {
  if (!pSource->defined) {
    pTarget->defined = false;
    return TSDB_CODE_SUCCESS;
  }
  if (pTarget->bytes < pSource->bytes) {
    void *pNew = taosMemoryRealloc(pTarget->pData, pSource->bytes);
    if (pNew == NULL) return stWindowChainAllocError();
    pTarget->pData = pNew;
  }
  TAOS_MEMCPY(pTarget->pData, pSource->pData, pSource->bytes);
  pTarget->type = pSource->type;
  pTarget->bytes = pSource->bytes;
  pTarget->defined = true;
  return TSDB_CODE_SUCCESS;
}

static int32_t stWindowChainSyncPendingState(SWindowChainLayerState *pLayer) {
  const int32_t count = taosArrayGetSize(pLayer->pStateValues);
  for (int32_t i = 0; i < count; ++i) {
    int32_t code = stWindowChainCopyStateValue(taosArrayGet(pLayer->pStateValues, i),
                                               taosArrayGet(pLayer->pPendingStateValues, i));
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  memset(pLayer->pPendingStateTouched, 0, count * sizeof(bool));
  pLayer->hasPendingPartialNull = false;
  return TSDB_CODE_SUCCESS;
}

static int32_t stWindowChainPendingStateRowEqual(SWindowChainLayerState *pLayer, const SWindowChainRowRef *pRow,
                                                 bool initializeUndefined, bool *pEqual) {
  *pEqual = false;
  const int32_t count = taosArrayGetSize(pLayer->pPendingStateValues);
  for (int32_t i = 0; i < count; ++i) {
    const int16_t           slot = *(const int16_t *)taosArrayGet(pLayer->pSpec->input.pConditionSlotIds, i);
    const SColumnInfoData  *pColumn = stWindowChainGetColumn(pRow, slot);
    SWindowChainStateValue *pValue = taosArrayGet(pLayer->pPendingStateValues, i);
    if (pColumn == NULL || pValue == NULL) return TSDB_CODE_INVALID_PARA;
    if (colDataIsNull_s(pColumn, pRow->rowIndex) || !pValue->defined) continue;
    const char   *pData = colDataGetData(pColumn, pRow->rowIndex);
    const int32_t bytes = stWindowChainStateDatumBytes(pColumn, pData);
    if (pValue->type != pColumn->info.type || pValue->bytes != bytes || memcmp(pValue->pData, pData, bytes) != 0) {
      return TSDB_CODE_SUCCESS;
    }
  }
  if (initializeUndefined) {
    int32_t code = stWindowChainAssignStateRowToValues(pLayer, pRow, pLayer->pPendingStateValues, false);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  *pEqual = true;
  return TSDB_CODE_SUCCESS;
}

static int32_t stWindowChainAccumulatePartialState(SWindowChainLayerState *pLayer, const SWindowChainRowRef *pRow,
                                                   TSKEY ts) {
  int32_t code = stWindowChainAssignStateRowToValues(pLayer, pRow, pLayer->pPendingStateValues, false);
  if (code != TSDB_CODE_SUCCESS) return code;
  const int32_t count = taosArrayGetSize(pLayer->pPendingStateValues);
  for (int32_t i = 0; i < count; ++i) {
    const int16_t          slot = *(const int16_t *)taosArrayGet(pLayer->pSpec->input.pConditionSlotIds, i);
    const SColumnInfoData *pColumn = stWindowChainGetColumn(pRow, slot);
    if (pColumn == NULL) return TSDB_CODE_INVALID_PARA;
    if (!colDataIsNull_s(pColumn, pRow->rowIndex)) pLayer->pPendingStateTouched[i] = true;
  }
  pLayer->hasPendingPartialNull = true;
  if (pLayer->deferredTailAllNullCount > 0) {
    pLayer->deferredPartialNullCount += pLayer->deferredTailAllNullCount;
    pLayer->deferredTailAllNullCount = 0;
  }
  if (pLayer->pendingNullCount == 0) pLayer->pendingNullStart = ts;
  ++pLayer->pendingNullCount;
  if (pLayer->deferredPartialNullCount == 0) pLayer->firstDeferredPartialNullTs = ts;
  ++pLayer->deferredPartialNullCount;
  pLayer->lastDeferredPartialNullTs = ts;
  return TSDB_CODE_SUCCESS;
}

static int32_t stWindowChainPendingStateDualSide(const SWindowChainLayerState *pLayer, const SWindowChainRowRef *pRow,
                                                 bool *pCompatible) {
  *pCompatible = true;
  if (!pLayer->hasPendingPartialNull) return TSDB_CODE_SUCCESS;
  const int32_t count = taosArrayGetSize(pLayer->pPendingStateValues);
  for (int32_t i = 0; i < count; ++i) {
    if (!pLayer->pPendingStateTouched[i]) continue;
    const int16_t                 slot = *(const int16_t *)taosArrayGet(pLayer->pSpec->input.pConditionSlotIds, i);
    const SColumnInfoData        *pColumn = stWindowChainGetColumn(pRow, slot);
    const SWindowChainStateValue *pValue = taosArrayGet(pLayer->pPendingStateValues, i);
    if (pColumn == NULL || pValue == NULL) return TSDB_CODE_INVALID_PARA;
    if (colDataIsNull_s(pColumn, pRow->rowIndex)) continue;
    const char   *pData = colDataGetData(pColumn, pRow->rowIndex);
    const int32_t bytes = stWindowChainStateDatumBytes(pColumn, pData);
    if (!pValue->defined || pValue->type != pColumn->info.type || pValue->bytes != bytes ||
        memcmp(pValue->pData, pData, bytes) != 0) {
      *pCompatible = false;
      return TSDB_CODE_SUCCESS;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t stWindowChainCommitPendingState(SWindowChainLayerState *pLayer) {
  const int32_t count = taosArrayGetSize(pLayer->pPendingStateValues);
  for (int32_t i = 0; i < count; ++i) {
    int32_t code = stWindowChainCopyStateValue(taosArrayGet(pLayer->pPendingStateValues, i),
                                               taosArrayGet(pLayer->pStateValues, i));
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  memset(pLayer->pPendingStateTouched, 0, count * sizeof(bool));
  pLayer->hasPendingPartialNull = false;
  return TSDB_CODE_SUCCESS;
}

static void stWindowChainResetDeferredState(SWindowChainLayerState *pLayer) {
  pLayer->deferredPartialNullCount = 0;
  pLayer->deferredTailAllNullCount = 0;
  pLayer->firstDeferredPartialNullTs = 0;
  pLayer->lastDeferredPartialNullTs = 0;
}

static bool stWindowChainStateAllDefined(const SWindowChainLayerState *pLayer) {
  for (int32_t i = 0; i < taosArrayGetSize(pLayer->pStateValues); ++i) {
    const SWindowChainStateValue *pValue = taosArrayGet(pLayer->pStateValues, i);
    if (pValue == NULL || !pValue->defined) return false;
  }
  return true;
}

static int32_t stWindowChainPushStateValueView(const SColumnInfoData *pColumn, const void *pData, int32_t bytes,
                                               SArray *pValues) {
  SValue value = {.type = pColumn->info.type};
  if (pData != NULL) {
    if (bytes <= 0) return TSDB_CODE_INVALID_PARA;
    value.nData = bytes;
    if (IS_VAR_DATA_TYPE(value.type) || value.type == TSDB_DATA_TYPE_DECIMAL) {
      value.pData = (uint8_t *)pData;
    } else {
      if (bytes > sizeof(value.val)) return TSDB_CODE_INVALID_PARA;
      TAOS_MEMCPY(&value.val, pData, bytes);
    }
  } else if (bytes != 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  return taosArrayPush(pValues, &value) == NULL ? stWindowChainAllocError() : TSDB_CODE_SUCCESS;
}

static int32_t stWindowChainCopyStateDefined(const SArray *pStateValues, bool **ppDefined) {
  const int32_t count = taosArrayGetSize(pStateValues);
  bool         *pDefined = taosMemoryCalloc(count, sizeof(bool));
  if (pDefined == NULL) return stWindowChainAllocError();
  for (int32_t i = 0; i < count; ++i) {
    const SWindowChainStateValue *pValue = taosArrayGet(pStateValues, i);
    if (pValue == NULL) {
      taosMemoryFree(pDefined);
      return TSDB_CODE_INVALID_PARA;
    }
    pDefined[i] = pValue->defined;
  }
  *ppDefined = pDefined;
  return TSDB_CODE_SUCCESS;
}

static int32_t stWindowChainBuildStateNotifyContent(const SWindowChainLayerState *pLayer, ESTriggerEventType eventType,
                                                    const SArray *pFromStateValues, const bool *pFromDefined,
                                                    const SWindowChainRowRef *pToRow, char **ppContent) {
  if (pLayer == NULL || pToRow == NULL || ppContent == NULL ||
      (eventType != STRIGGER_EVENT_WINDOW_OPEN && eventType != STRIGGER_EVENT_WINDOW_CLOSE)) {
    return TSDB_CODE_INVALID_PARA;
  }
  *ppContent = NULL;
  const int32_t count = taosArrayGetSize(pLayer->pSpec->input.pConditionSlotIds);
  if (count <= 0 || (pFromStateValues != NULL && taosArrayGetSize(pFromStateValues) != count)) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SArray *pStateCols = taosArrayInit(count, POINTER_BYTES);
  SArray *pFromValues = pFromStateValues == NULL ? NULL : taosArrayInit(count, sizeof(SValue));
  SArray *pToValues = taosArrayInit(count, sizeof(SValue));
  bool   *pOwnedFromDefined = pFromStateValues == NULL ? NULL : taosMemoryCalloc(count, sizeof(bool));
  bool   *pToDefined = taosMemoryCalloc(count, sizeof(bool));
  if (pStateCols == NULL || (pFromStateValues != NULL && (pFromValues == NULL || pOwnedFromDefined == NULL)) ||
      pToValues == NULL || pToDefined == NULL) {
    code = stWindowChainAllocError();
    goto _exit;
  }

  for (int32_t i = 0; i < count; ++i) {
    const int16_t          slot = *(const int16_t *)taosArrayGet(pLayer->pSpec->input.pConditionSlotIds, i);
    const SColumnInfoData *pColumn = stWindowChainGetColumn(pToRow, slot);
    if (pColumn == NULL) {
      code = TSDB_CODE_INVALID_PARA;
      goto _exit;
    }
    SColumnInfoData *pStateColumn = (SColumnInfoData *)pColumn;
    if (taosArrayPush(pStateCols, &pStateColumn) == NULL) {
      code = stWindowChainAllocError();
      goto _exit;
    }

    pToDefined[i] = !colDataIsNull_s(pColumn, pToRow->rowIndex);
    const void   *pToData = pToDefined[i] ? colDataGetData(pColumn, pToRow->rowIndex) : NULL;
    const int32_t toBytes = pToDefined[i] ? stWindowChainStateDatumBytes(pColumn, pToData) : 0;
    code = stWindowChainPushStateValueView(pColumn, pToData, toBytes, pToValues);
    if (code != TSDB_CODE_SUCCESS) goto _exit;

    if (pFromStateValues != NULL) {
      const SWindowChainStateValue *pFromValue = taosArrayGet(pFromStateValues, i);
      if (pFromValue == NULL ||
          (pFromValue->defined &&
           (pFromValue->type != pColumn->info.type || pFromValue->pData == NULL || pFromValue->bytes <= 0)) ||
          (pFromDefined != NULL && pFromDefined[i] && !pFromValue->defined)) {
        code = TSDB_CODE_INVALID_PARA;
        goto _exit;
      }
      pOwnedFromDefined[i] = pFromValue->defined;
      code = stWindowChainPushStateValueView(pColumn, pFromValue->defined ? pFromValue->pData : NULL,
                                             pFromValue->defined ? pFromValue->bytes : 0, pFromValues);
      if (code != TSDB_CODE_SUCCESS) goto _exit;
    }
  }

  /* These SValue arrays borrow chain and input-row data only for this synchronous helper call. */
  code = streamBuildMultiStateNotifyContent(eventType, pStateCols, pFromValues,
                                            pFromDefined == NULL ? pOwnedFromDefined : pFromDefined, pToValues,
                                            pToDefined, ppContent);

_exit:
  taosArrayDestroy(pStateCols);
  taosArrayDestroy(pFromValues);
  taosArrayDestroy(pToValues);
  taosMemoryFree(pOwnedFromDefined);
  taosMemoryFree(pToDefined);
  if (code != TSDB_CODE_SUCCESS) taosMemoryFreeClear(*ppContent);
  return code;
}

typedef struct {
  bool  splitStandalone;
  TSKEY standaloneStart;
  TSKEY standaloneEnd;
  TSKEY nextStart;
} SWindowChainStateCut;

static int32_t stWindowChainResolveStateCut(SWindowChainLayerState *pLayer, SWindowChainInstance *pCurrent, TSKEY ts,
                                            bool dualSide, SWindowChainStateCut *pCut) {
  const int16_t extend = pLayer->pSpec->trigger.stateWin.extend;
  pCut->splitStandalone = !dualSide && extend == STATE_WIN_EXTEND_OPTION_FORWARD &&
                          pLayer->deferredPartialNullCount > 0 && stWindowChainStateAllDefined(pLayer);
  const TSKEY lastDeferred = pLayer->lastDeferredPartialNullTs;
  bool        committedDeferred = false;
  if (!dualSide && !pCut->splitStandalone) {
    committedDeferred = pLayer->deferredPartialNullCount > 0;
    pCurrent->rownum += pLayer->deferredPartialNullCount;
    pLayer->pendingNullCount -= pLayer->deferredPartialNullCount;
    stWindowChainResetDeferredState(pLayer);
    int32_t code = stWindowChainCommitPendingState(pLayer);
    if (code != TSDB_CODE_SUCCESS) return code;
  }

  pCut->nextStart = pLayer->pendingNullCount > 0 ? pLayer->pendingNullStart : ts;
  if (extend == STATE_WIN_EXTEND_OPTION_BACKWARD) {
    pCurrent->rownum += pLayer->pendingNullCount;
    pCurrent->end = ts == INT64_MIN ? ts : ts - 1;
  } else if (extend == STATE_WIN_EXTEND_OPTION_FORWARD) {
    if (committedDeferred) pCurrent->end = lastDeferred;
    pCut->nextStart = pCurrent->end == INT64_MAX ? pCurrent->end : pCurrent->end + 1;
    if (pCut->splitStandalone) {
      pCut->standaloneStart = pCut->nextStart;
      pCut->standaloneEnd = lastDeferred;
    }
  } else if (committedDeferred || pLayer->deferredPartialNullCount > 0) {
    pCurrent->rownum += pLayer->deferredPartialNullCount;
    pCurrent->end = lastDeferred;
    pLayer->pendingNullCount -= pLayer->deferredPartialNullCount;
    stWindowChainResetDeferredState(pLayer);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t stWindowChainStateIsZeroth(const SWindowChainLayerState *pLayer, bool *pIsZeroth) {
  *pIsZeroth = false;
  if (pLayer->pSpec->trigger.stateWin.zeroth == NULL) return TSDB_CODE_SUCCESS;
  SNodeList *pZeroths = NULL;
  int32_t    code = nodesStringToList(pLayer->pSpec->trigger.stateWin.zeroth, &pZeroths);
  if (code != TSDB_CODE_SUCCESS) return code;
  const int32_t count = taosArrayGetSize(pLayer->pSpec->input.pConditionSlotIds);
  if (LIST_LENGTH(pZeroths) != count) {
    code = TSDB_CODE_INVALID_PARA;
    goto _exit;
  }
  bool hasZeroth = false;
  for (int32_t i = 0; i < count; ++i) {
    const SValueNode *pZeroth = (const SValueNode *)nodesListGetNode(pZeroths, i);
    if (pZeroth == NULL) {
      code = TSDB_CODE_INVALID_PARA;
      goto _exit;
    }
    if (pZeroth->isNull) continue;
    hasZeroth = true;
    const SWindowChainStateValue *pValue = taosArrayGet(pLayer->pStateValues, i);
    if (pValue == NULL || !pValue->defined) goto _exit;
    bool equal = false;
    code = stIsStateEqualZeroth(pValue->pData, (void *)pZeroth, &equal);
    if (code != TSDB_CODE_SUCCESS || !equal) goto _exit;
  }
  *pIsZeroth = hasZeroth;

_exit:
  nodesDestroyList(pZeroths);
  return code;
}

static bool stWindowChainTrueForSatisfied(const SWindowChainLayerState *pLeaf, const SWindowChainInstance *pInstance) {
  STrueForInfo info = {0};
  if (pLeaf->runtimeType == STREAM_TRIGGER_STATE) {
    info.trueForType = pLeaf->pSpec->trigger.stateWin.trueForType;
    info.count = pLeaf->pSpec->trigger.stateWin.trueForCount;
    info.duration = pLeaf->pSpec->trigger.stateWin.trueForDuration;
  } else if (pLeaf->runtimeType == STREAM_TRIGGER_EVENT) {
    info.trueForType = pLeaf->pSpec->trigger.event.trueForType;
    info.count = pLeaf->pSpec->trigger.event.trueForCount;
    info.duration = pLeaf->pSpec->trigger.event.trueForDuration;
  } else {
    return true;
  }
  if (info.count <= 0 && info.duration <= 0) return true;
  return isTrueForSatisfied(&info, pInstance->start, pInstance->end, pInstance->rownum);
}

static int32_t stBuildLineageFromInstances(const SWindowChainState    *pState,
                                           const SWindowChainInstance *pAncestorInstances, SWindowLineage *pLineage) {
  const int32_t ancestors = pState->numLayers - 1;
  pLineage->pScopes = taosArrayInit(ancestors, sizeof(SScopeInstanceId));
  if (pLineage->pScopes == NULL) return stWindowChainAllocError();

  for (int32_t i = 0; i < ancestors; ++i) {
    const SWindowChainLayerState *pLayer = &pState->layers[i];
    const SWindowChainInstance   *pInstance =
        pAncestorInstances == NULL ? taosArrayGet(pLayer->pInstances, 0) : &pAncestorInstances[i];
    if (pInstance == NULL) return TSDB_CODE_INTERNAL_ERROR;
    const SScopeInstanceId id = {.layerIndex = i,
                                 .triggerType = pLayer->pSpec->triggerType,
                                 .openingTs = pInstance->start,
                                 .nativeDiscriminator = 0};
    if (taosArrayPush(pLineage->pScopes, &id) == NULL) return stWindowChainAllocError();
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t stBuildLineage(const SWindowChainState *pState, SWindowLineage *pLineage) {
  return stBuildLineageFromInstances(pState, NULL, pLineage);
}

static int32_t stWindowChainBuildEventParentTriggerId(const SWindowChainState *pState, TSKEY parentWindowStart,
                                                      char parentTriggerId[STREAM_NESTED_TRIGGER_ID_LEN]) {
  SLeafInstanceId parentIdentity = {
      .gid = pState->gid,
      .triggerType = WINDOW_TYPE_EVENT,
      .openingTs = parentWindowStart,
      .nativeDiscriminator = -1,
  };
  int32_t code = stBuildLineage(pState, &parentIdentity.lineage);
  if (code == TSDB_CODE_SUCCESS) {
    code = stBuildNestedTriggerId(parentIdentity.gid, &parentIdentity.lineage, parentIdentity.openingTs, -1,
                                  parentTriggerId);
  }
  stDestroyWindowLineage(&parentIdentity.lineage);
  return code;
}

static int32_t stWindowChainBuildForcedEventNotifyContent(const SWindowChainState *pState, int32_t windowIndex,
                                                          TSKEY parentWindowStart, char **ppContent) {
  *ppContent = NULL;
  cJSON *pObject = cJSON_CreateObject();
  if (pObject == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  int32_t code = TSDB_CODE_SUCCESS;
  if (cJSON_AddNumberToObject(pObject, "windowIndex", windowIndex) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  if (windowIndex >= 0) {
    char parentTriggerId[STREAM_NESTED_TRIGGER_ID_LEN] = {0};
    code = stWindowChainBuildEventParentTriggerId(pState, parentWindowStart, parentTriggerId);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
    if (cJSON_AddStringToObject(pObject, "parentTriggerId", parentTriggerId) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
  }
  *ppContent = cJSON_PrintUnformatted(pObject);
  if (*ppContent == NULL) code = TSDB_CODE_OUT_OF_MEMORY;

_exit:
  cJSON_Delete(pObject);
  return code;
}

static int32_t stBuildAncestorSnapshotsFromInstances(const SWindowChainState    *pState,
                                                     const SWindowChainInstance *pAncestorInstances,
                                                     SArray                    **ppSnapshots) {
  const int32_t ancestors = pState->numLayers - 1;
  SArray       *pSnapshots = taosArrayInit(ancestors, sizeof(SWindowAncestorSnapshot));
  if (pSnapshots == NULL) return stWindowChainAllocError();

  for (int32_t i = 0; i < ancestors; ++i) {
    const SWindowChainLayerState *pLayer = &pState->layers[i];
    const SWindowChainInstance   *pInstance =
        pAncestorInstances == NULL ? taosArrayGet(pLayer->pInstances, 0) : &pAncestorInstances[i];
    if (pInstance == NULL) {
      taosArrayDestroy(pSnapshots);
      return TSDB_CODE_INTERNAL_ERROR;
    }
    SWindowAncestorSnapshot snapshot = {
        .layerIndex = i, .triggerType = pLayer->pSpec->triggerType, .placeholderMask = pLayer->pSpec->placeholderMask};
    if (pLayer->pureSliding) {
      STimeWindow next = {0};
      int32_t     code = stPureSlidingRange(&pLayer->pSpec->trigger.sliding,
                                        pInstance->end == INT64_MAX ? pInstance->end : pInstance->end + 1, &next);
      if (code != TSDB_CODE_SUCCESS) {
        taosArrayDestroy(pSnapshots);
        return code;
      }
      snapshot.values.sliding.prevTs = pInstance->start;
      snapshot.values.sliding.currentTs = pInstance->end;
      snapshot.values.sliding.nextTs = next.ekey;
    } else {
      snapshot.values.window.start = pInstance->start;
      snapshot.values.window.end = pInstance->end;
      snapshot.values.window.duration = pInstance->end - pInstance->start;
      snapshot.values.window.rownum = pInstance->rownum;
    }
    if (taosArrayPush(pSnapshots, &snapshot) == NULL) {
      int32_t code = stWindowChainAllocError();
      taosArrayDestroy(pSnapshots);
      return code;
    }
  }
  *ppSnapshots = pSnapshots;
  return TSDB_CODE_SUCCESS;
}

static void stFillLeafParam(const SWindowChainLayerState *pLeaf, const SWindowChainInstance *pInstance,
                            int32_t eventType, int64_t nowNs, SSTriggerCalcParam *pParam) {
  pParam->triggerTime = nowNs;
  pParam->notifyType = eventType;
  if (pLeaf->pureSliding) {
    STimeWindow next = {0};
    if (pInstance->end != INT64_MAX &&
        stPureSlidingRange(&pLeaf->pSpec->trigger.sliding, pInstance->end + 1, &next) == TSDB_CODE_SUCCESS) {
      pParam->nextTs = next.ekey;
    }
    pParam->prevTs = pInstance->start;
    pParam->currentTs = pInstance->end;
  } else {
    pParam->wstart = pInstance->start;
    const bool dataOpen = eventType == STRIGGER_EVENT_WINDOW_OPEN && stWindowChainDataDriven(pLeaf->runtimeType);
    pParam->wend = dataOpen ? pInstance->start : pInstance->end;
    pParam->wduration = pParam->wend - pParam->wstart;
    pParam->wrownum = pInstance->rownum;
  }
}

static int32_t stBuildCandidateCalcDataRange(const SWindowChainState *pState, const SWindowChainInstance *pLeafInstance,
                                             const SArray *pAncestorSnapshots, STimeWindow *pRange) {
  if (pState == NULL || pLeafInstance == NULL || pRange == NULL) return TSDB_CODE_INVALID_PARA;

  const STimeWindow leafRange = {.skey = pLeafInstance->start, .ekey = pLeafInstance->end};
  *pRange = leafRange;
  for (int32_t i = 0; i < taosArrayGetSize(pAncestorSnapshots); ++i) {
    const SWindowAncestorSnapshot *pSnapshot = taosArrayGet(pAncestorSnapshots, i);
    if (pSnapshot == NULL || pSnapshot->layerIndex != i || i >= pState->numLayers - 1) {
      return TSDB_CODE_INTERNAL_ERROR;
    }
    const SWindowChainLayerState *pLayer = &pState->layers[i];
    STimeWindow                   ancestorRange =
        pLayer->pureSliding
                              ? (STimeWindow){.skey = pSnapshot->values.sliding.prevTs, .ekey = pSnapshot->values.sliding.currentTs}
                              : (STimeWindow){.skey = pSnapshot->values.window.start, .ekey = pSnapshot->values.window.end};
    pRange->skey = TMAX(pRange->skey, ancestorRange.skey);
    pRange->ekey = TMIN(pRange->ekey, ancestorRange.ekey);
  }
  if (pRange->skey > pRange->ekey && pLeafInstance->rownum == 0) {
    *pRange = leafRange;
  }
  return pRange->skey <= pRange->ekey ? TSDB_CODE_SUCCESS : TSDB_CODE_INVALID_PARA;
}

static bool stWindowChainCandidateIntentArray(const SArray *pCandidates) {
  return pCandidates != NULL && pCandidates->elemSize == sizeof(SWindowChainCandidateIntent);
}

static bool stWindowChainAcceptedIntentArray(const SArray *pBatches) {
  return pBatches != NULL && pBatches->elemSize == sizeof(SWindowChainAcceptedIntent);
}

static int32_t stCaptureAncestorInstances(const SWindowChainState *pState, SWindowChainInstance *pAncestorInstances) {
  for (int32_t i = 0; i < pState->numLayers - 1; ++i) {
    const SWindowChainInstance *pInstance = taosArrayGet(pState->layers[i].pInstances, 0);
    if (pInstance == NULL) return TSDB_CODE_INTERNAL_ERROR;
    stCopyWindowChainInstanceSnapshot(pInstance, &pAncestorInstances[i]);
  }
  return TSDB_CODE_SUCCESS;
}

static bool stWindowChainSameInstance(const SWindowChainInstance *pLeft, const SWindowChainInstance *pRight) {
  return pLeft->generation != 0 && pLeft->generation == pRight->generation;
}

static const SWindowChainInstance *stFindMatchingInstance(const SWindowChainLayerState *pLayer,
                                                          const SWindowChainInstance   *pSnapshot) {
  for (int32_t i = 0; i < taosArrayGetSize(pLayer->pInstances); ++i) {
    const SWindowChainInstance *pInstance = taosArrayGet(pLayer->pInstances, i);
    if (pInstance != NULL && stWindowChainSameInstance(pSnapshot, pInstance)) return pInstance;
  }
  if (pLayer->eventParentActive && stWindowChainSameInstance(pSnapshot, &pLayer->eventParent)) {
    return &pLayer->eventParent;
  }
  return NULL;
}

static void stRefreshCandidateIntents(const SWindowChainState *pState, SArray *pCandidates) {
  if (!stWindowChainCandidateIntentArray(pCandidates)) return;

  const SWindowChainLayerState *pLeaf = &pState->layers[pState->numLayers - 1];
  for (int32_t i = 0; i < taosArrayGetSize(pCandidates); ++i) {
    SWindowChainCandidateIntent *pIntent = taosArrayGet(pCandidates, i);
    const SWindowChainInstance  *pInstance = stFindMatchingInstance(pLeaf, &pIntent->instance);
    if (pInstance != NULL) stCopyWindowChainInstanceSnapshot(pInstance, &pIntent->instance);
    for (int32_t j = 0; j < pState->numLayers - 1; ++j) {
      pInstance = stFindMatchingInstance(&pState->layers[j], &pIntent->ancestors[j]);
      if (pInstance != NULL) stCopyWindowChainInstanceSnapshot(pInstance, &pIntent->ancestors[j]);
    }
  }
}

static int32_t stAppendCandidateIntent(const SWindowChainState *pState, const SWindowChainInstance *pInstance,
                                       int32_t eventType, int64_t nowNs, const char *pNotifyContent,
                                       SArray *pCandidates) {
  SWindowChainCandidateIntent intent = {.eventType = eventType, .nowNs = nowNs};
  stCopyWindowChainInstanceSnapshot(pInstance, &intent.instance);
  int32_t code = stCaptureAncestorInstances(pState, intent.ancestors);
  if (code != TSDB_CODE_SUCCESS) return code;
  if (pNotifyContent != NULL) {
    intent.pNotifyContent = taosStrdup(pNotifyContent);
    if (intent.pNotifyContent == NULL) return stWindowChainAllocError();
  }
  if (taosArrayPush(pCandidates, &intent) == NULL) {
    code = stWindowChainAllocError();
    stDestroyCandidateIntent(&intent);
    return code;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t stAppendMaterializedCandidateWithContent(const SWindowChainState    *pState,
                                                        const SWindowChainInstance *pInstance,
                                                        const SWindowChainInstance *pAncestorInstances,
                                                        int32_t eventType, int64_t nowNs, const char *pNotifyContent,
                                                        SArray *pCandidates) {
  const SWindowChainLayerState *pLeaf = &pState->layers[pState->numLayers - 1];
  if (pInstance->suppressed || !stWindowChainTrueForSatisfied(pLeaf, pInstance)) return TSDB_CODE_SUCCESS;
  SLeafEventCandidate candidate = {0};
  int32_t code = stBuildAncestorSnapshotsFromInstances(pState, pAncestorInstances, &candidate.pAncestorSnapshots);
  if (code != TSDB_CODE_SUCCESS) goto _exit;

  code = stBuildLineageFromInstances(pState, pAncestorInstances, &candidate.instanceId.lineage);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  code = stCloneWindowLineage(&candidate.instanceId.lineage, &candidate.lineage);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  code = stCloneWindowLineage(&candidate.instanceId.lineage, &candidate.cacheScope.lineage);
  if (code != TSDB_CODE_SUCCESS) goto _exit;

  candidate.instanceId.gid = pState->gid;
  candidate.instanceId.triggerType = pLeaf->pSpec->triggerType;
  candidate.instanceId.openingTs = pInstance->start;
  candidate.instanceId.nativeDiscriminator = pInstance->nativeDiscriminator;
  candidate.cacheScope.gid = pState->gid;
  const SWindowChainInstance *pRoot = pInstance;
  if (pState->numLayers > 1) {
    pRoot = pAncestorInstances == NULL ? taosArrayGet(pState->layers[0].pInstances, 0) : &pAncestorInstances[0];
    if (pRoot == NULL) {
      code = TSDB_CODE_INTERNAL_ERROR;
      goto _exit;
    }
  }
  candidate.rootImpactExtent = (STimeWindow){.skey = pRoot->start, .ekey = pRoot->end};
  candidate.eventType = eventType;
  candidate.rowCount = pInstance->rownum;
  stFillLeafParam(pLeaf, pInstance, eventType, nowNs, &candidate.leafParam);
  code = stBuildCandidateCalcDataRange(pState, pInstance, candidate.pAncestorSnapshots, &candidate.calcDataRange);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  if (pNotifyContent != NULL) {
    candidate.leafParam.extraNotifyContent = taosStrdup(pNotifyContent);
    if (candidate.leafParam.extraNotifyContent == NULL) {
      code = stWindowChainAllocError();
      goto _exit;
    }
  }
  if (taosArrayPush(pCandidates, &candidate) == NULL) {
    code = stWindowChainAllocError();
    goto _exit;
  }
  return TSDB_CODE_SUCCESS;

_exit:
  stDestroyLeafEventCandidate(&candidate);
  return code;
}

int32_t stWindowChainBuildRecalcImpactDomain(const SStreamWindowPlan *pPlan, int64_t gid, const STimeWindow *pScanRange,
                                             const STimeWindow *pCalcRange, SRecalcImpactDomain *pDomain) {
  if (pPlan == NULL || pPlan->pLayers == NULL || taosArrayGetSize(pPlan->pLayers) <= 0 || pScanRange == NULL ||
      pCalcRange == NULL || pDomain == NULL || pScanRange->skey > pScanRange->ekey ||
      pCalcRange->skey > pCalcRange->ekey) {
    return TSDB_CODE_INVALID_PARA;
  }

  SRecalcImpactDomain result = {
      .gid = gid,
      .replayAnchor = pScanRange->skey,
      .capturedFrontier = pScanRange->ekey,
      .pRootExtents = taosArrayInit(2, sizeof(STimeWindow)),
  };
  if (result.pRootExtents == NULL) return stWindowChainAllocError();

  const SStreamWindowLayerSpec *pRoot = taosArrayGet(pPlan->pLayers, 0);
  int32_t                       code = TSDB_CODE_SUCCESS;
  if (pRoot->triggerType != WINDOW_TYPE_INTERVAL) {
    code = stPushRange(result.pRootExtents, pScanRange->skey, pScanRange->ekey);
  } else {
    const bool  pureSliding = pRoot->trigger.sliding.interval == 0;
    STimeWindow first = {0};
    STimeWindow last = {0};
    if (pureSliding) {
      code = stPureSlidingRange(&pRoot->trigger.sliding, pCalcRange->skey, &first);
      if (code == TSDB_CODE_SUCCESS) {
        code = stPureSlidingRange(&pRoot->trigger.sliding, pCalcRange->ekey, &last);
      }
    } else {
      SInterval interval = stBuildInterval(&pRoot->trigger.sliding, false);
      first.skey = taosTimeTruncate(pCalcRange->skey, &interval);
      first.ekey = taosTimeGetIntervalEnd(first.skey, &interval);
      last.skey = taosTimeTruncate(pCalcRange->ekey, &interval);
      last.ekey = taosTimeGetIntervalEnd(last.skey, &interval);
    }
    if (code == TSDB_CODE_SUCCESS) {
      code = stPushRange(result.pRootExtents, first.skey, last.ekey);
    }
  }
  if (code != TSDB_CODE_SUCCESS) {
    stDestroyRecalcImpactDomain(&result);
    return code;
  }
  *pDomain = result;
  return TSDB_CODE_SUCCESS;
}

static int32_t stAppendCandidateWithContent(const SWindowChainState *pState, const SWindowChainInstance *pInstance,
                                            int32_t eventType, int64_t nowNs, const char *pNotifyContent,
                                            SArray *pCandidates) {
  if (stWindowChainCandidateIntentArray(pCandidates)) {
    stRefreshCandidateIntents(pState, pCandidates);
    const SWindowChainLayerState *pLeaf = &pState->layers[pState->numLayers - 1];
    if (pInstance->suppressed || !stWindowChainTrueForSatisfied(pLeaf, pInstance)) return TSDB_CODE_SUCCESS;
    return stAppendCandidateIntent(pState, pInstance, eventType, nowNs, pNotifyContent, pCandidates);
  }
  return stAppendMaterializedCandidateWithContent(pState, pInstance, NULL, eventType, nowNs, pNotifyContent,
                                                  pCandidates);
}

static int32_t stAppendCandidate(const SWindowChainState *pState, const SWindowChainInstance *pInstance,
                                 int32_t eventType, int64_t nowNs, SArray *pCandidates) {
  return stAppendCandidateWithContent(pState, pInstance, eventType, nowNs, NULL, pCandidates);
}

static int32_t stAppendForcedCloseCandidate(const SWindowChainState *pState, const SWindowChainInstance *pInstance,
                                            int64_t nowNs, SArray *pCandidates) {
  const SWindowChainLayerState *pLeaf = &pState->layers[pState->numLayers - 1];
  char                         *pNotifyContent = NULL;
  int32_t                       code = TSDB_CODE_SUCCESS;
  if (pLeaf->runtimeType == STREAM_TRIGGER_EVENT &&
      (pState->policy.leafNotifyEventTypes & STRIGGER_EVENT_WINDOW_CLOSE) != 0) {
    const int32_t windowIndex = (int32_t)pInstance->nativeDiscriminator;
    const TSKEY   parentWindowStart = windowIndex >= 0 ? pLeaf->eventParent.start : 0;
    code = stWindowChainBuildForcedEventNotifyContent(pState, windowIndex, parentWindowStart, &pNotifyContent);
  }
  if (code == TSDB_CODE_SUCCESS) {
    code = stAppendCandidateWithContent(pState, pInstance, STRIGGER_EVENT_WINDOW_CLOSE, nowNs, pNotifyContent,
                                        pCandidates);
  }
  taosMemoryFree(pNotifyContent);
  return code;
}

static int32_t stAppendOpenCandidateWithContent(SWindowChainState *pState, SWindowChainInstance *pInstance,
                                                int64_t nowNs, const char *pNotifyContent, SArray *pCandidates) {
  stRefreshCandidateIntents(pState, pCandidates);
  if (pInstance->openEmitted || pInstance->suppressed ||
      (pState->policy.leafEventTypes & STRIGGER_EVENT_WINDOW_OPEN) == 0 ||
      !stWindowChainTrueForSatisfied(&pState->layers[pState->numLayers - 1], pInstance)) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t code =
      stAppendCandidateWithContent(pState, pInstance, STRIGGER_EVENT_WINDOW_OPEN, nowNs, pNotifyContent, pCandidates);
  if (code == TSDB_CODE_SUCCESS) code = stWindowChainTouchInstance(pState, pInstance);
  if (code == TSDB_CODE_SUCCESS) pInstance->openEmitted = true;
  return code;
}

static int32_t stAppendOpenCandidate(SWindowChainState *pState, SWindowChainInstance *pInstance, int64_t nowNs,
                                     SArray *pCandidates) {
  return stAppendOpenCandidateWithContent(pState, pInstance, nowNs, NULL, pCandidates);
}

static int32_t stAppendAcceptedBatch(const SWindowChainState *pState, const SArray *pRows, SArray *pBatches) {
  if (stWindowChainAcceptedIntentArray(pBatches)) {
    SWindowChainAcceptedIntent intent = {0};
    int32_t                    code = stCaptureAncestorInstances(pState, intent.ancestors);
    if (code != TSDB_CODE_SUCCESS) return code;
    intent.pRows = taosArrayDup(pRows, NULL);
    if (intent.pRows == NULL) return stWindowChainAllocError();
    if (taosArrayPush(pBatches, &intent) == NULL) {
      code = stWindowChainAllocError();
      stDestroyAcceptedIntent(&intent);
      return code;
    }
    return TSDB_CODE_SUCCESS;
  }

  SWindowChainAcceptedBatch batch = {0};
  int32_t                   code = stBuildLineage(pState, &batch.cacheScope.lineage);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  batch.cacheScope.gid = pState->gid;
  batch.pRows = taosArrayDup(pRows, NULL);
  if (batch.pRows == NULL) {
    code = stWindowChainAllocError();
    goto _exit;
  }
  if (taosArrayPush(pBatches, &batch) == NULL) {
    code = stWindowChainAllocError();
    goto _exit;
  }
  return TSDB_CODE_SUCCESS;

_exit:
  stDestroyAcceptedBatch(&batch);
  return code;
}

static int32_t stAppendAcceptedRow(const SWindowChainState *pState, const SWindowChainRowRef *pRow, SArray *pBatches) {
  SArray *pRows = taosArrayInit(1, sizeof(SWindowChainRowRef));
  if (pRows == NULL) return stWindowChainAllocError();
  int32_t code = TSDB_CODE_SUCCESS;
  if (taosArrayPush(pRows, pRow) == NULL) {
    code = stWindowChainAllocError();
  } else {
    code = stAppendAcceptedBatch(pState, pRows, pBatches);
  }
  taosArrayDestroy(pRows);
  return code;
}

static int32_t stResetLayerRuntime(SWindowChainState *pState, int32_t layerIndex) {
  SWindowChainLayerState *pLayer = &pState->layers[layerIndex];
  int32_t                 code = stWindowChainCowStateValues(pState, layerIndex);
  if (code != TSDB_CODE_SUCCESS) return code;
  code = stWindowChainClearInstances(pState, layerIndex);
  if (code != TSDB_CODE_SUCCESS) return code;
  pLayer->timeCursorInitialized = false;
  pLayer->nextTimeRange = (STimeWindow){0};
  for (int32_t i = 0; i < taosArrayGetSize(pLayer->pStateValues); ++i) {
    SWindowChainStateValue *pValue = taosArrayGet(pLayer->pStateValues, i);
    pValue->defined = false;
    SWindowChainStateValue *pPending = taosArrayGet(pLayer->pPendingStateValues, i);
    if (pPending != NULL) pPending->defined = false;
  }
  if (pLayer->pPendingStateTouched != NULL) {
    memset(pLayer->pPendingStateTouched, 0, taosArrayGetSize(pLayer->pStateValues) * sizeof(bool));
  }
  pLayer->totalCount = 0;
  pLayer->pendingNullCount = 0;
  pLayer->pendingNullStart = 0;
  pLayer->deferredPartialNullCount = 0;
  pLayer->deferredTailAllNullCount = 0;
  pLayer->firstDeferredPartialNullTs = 0;
  pLayer->lastDeferredPartialNullTs = 0;
  pLayer->hasPendingPartialNull = false;
  pLayer->eventConditionIndex = 0;
  pLayer->eventSubwindowCount = 0;
  pLayer->eventParentActive = false;
  stDestroyWindowChainInstance(&pLayer->eventParent);
  pLayer->eventParent = (SWindowChainInstance){0};
  pLayer->eventStartConditionCount = 0;
  pLayer->eventStartConditionFirstTs = 0;
  pLayer->eventEndConditionCount = 0;
  pLayer->eventEndConditionFirstTs = 0;
  return TSDB_CODE_SUCCESS;
}

static int32_t stClearDescendants(SWindowChainState *pState, int32_t layerIndex) {
  for (int32_t i = layerIndex + 1; i < pState->numLayers; ++i) {
    int32_t code = stResetLayerRuntime(pState, i);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t stFlushLeaf(SWindowChainState *pState, int64_t nowNs, SArray *pCandidates) {
  if (!pState->policy.flushOnOuterClose || (pState->policy.leafEventTypes & STRIGGER_EVENT_WINDOW_CLOSE) == 0) {
    return TSDB_CODE_SUCCESS;
  }
  SWindowChainLayerState *pLeaf = &pState->layers[pState->numLayers - 1];
  SArray                 *pLeaves = pLeaf->pInstances;
  if (pLeaf->runtimeType == STREAM_TRIGGER_EVENT && pLeaf->multipleEventStarts && pLeaf->eventParentActive &&
      pLeaf->eventSubwindowCount == 1) {
    SWindowChainInstance *pOnly = taosArrayGet(pLeaves, 0);
    if (pOnly != NULL) {
      int32_t code = stWindowChainTouchInstance(pState, pOnly);
      if (code != TSDB_CODE_SUCCESS) return code;
      pOnly->nativeDiscriminator = -1;
    }
  }
  for (int32_t i = 0; i < taosArrayGetSize(pLeaves); ++i) {
    int32_t code = stAppendForcedCloseCandidate(pState, taosArrayGet(pLeaves, i), nowNs, pCandidates);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  if (pLeaf->runtimeType == STREAM_TRIGGER_EVENT && pLeaf->eventParentActive && pLeaf->eventSubwindowCount > 1) {
    return stAppendForcedCloseCandidate(pState, &pLeaf->eventParent, nowNs, pCandidates);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t stResetDescendants(SWindowChainState *pState, int32_t layerIndex, int64_t nowNs, SArray *pCandidates) {
  int32_t code = stFlushLeaf(pState, nowNs, pCandidates);
  if (code != TSDB_CODE_SUCCESS) return code;
  stRefreshCandidateIntents(pState, pCandidates);
  return stClearDescendants(pState, layerIndex);
}

static int32_t stAdvanceLayer(SWindowChainState *pState, int32_t layerIndex, TSKEY frontier,
                              EWindowLayerInputType inputType, int64_t nowNs, SArray *pCandidates);

static bool stLayerHasRuntimeState(const SWindowChainLayerState *pLayer) {
  return taosArrayGetSize(pLayer->pInstances) > 0 || pLayer->timeCursorInitialized;
}

static int32_t stSettleAndResetDescendants(SWindowChainState *pState, int32_t layerIndex,
                                           const SWindowChainInstance *pInstance, int64_t nowNs, SArray *pCandidates) {
  if (stLayerHasRuntimeState(&pState->layers[layerIndex + 1])) {
    int32_t code =
        stAdvanceLayer(pState, layerIndex + 1, pInstance->end, WINDOW_LAYER_INPUT_ANCESTOR_END, nowNs, pCandidates);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  return stResetDescendants(pState, layerIndex, nowNs, pCandidates);
}

static int32_t stFindInstanceIndex(const SArray *pInstances, TSKEY start) {
  for (int32_t i = 0; i < taosArrayGetSize(pInstances); ++i) {
    const SWindowChainInstance *pInstance = taosArrayGet(pInstances, i);
    if (pInstance->start == start) return i;
  }
  return -1;
}

static SWindowChainInstance *stFindInstance(SArray *pInstances, TSKEY start) {
  int32_t index = stFindInstanceIndex(pInstances, start);
  return index < 0 ? NULL : taosArrayGet(pInstances, index);
}

static const SWindowChainInstance *stFindOrderedInstance(const SArray *pInstances, TSKEY start) {
  int32_t left = 0;
  int32_t right = taosArrayGetSize(pInstances) - 1;
  while (left <= right) {
    int32_t                     middle = left + (right - left) / 2;
    const SWindowChainInstance *pInstance = taosArrayGet(pInstances, middle);
    if (pInstance == NULL || pInstance->start == start) return pInstance;
    if (pInstance->start < start) {
      left = middle + 1;
    } else {
      right = middle - 1;
    }
  }
  return NULL;
}

static int32_t stAllocateInstanceGeneration(SWindowChainState *pState, uint64_t *pGeneration) {
  if (pState->nextInstanceGeneration == UINT64_MAX) return TSDB_CODE_OUT_OF_RANGE;
  *pGeneration = ++pState->nextInstanceGeneration;
  return TSDB_CODE_SUCCESS;
}

static int32_t stOpenInstance(SWindowChainState *pState, SArray *pInstances, const STimeWindow *pRange, int64_t nowNs,
                              SWindowChainInstance **ppInstance) {
  const int32_t layerIndex = stWindowChainLayerIndex(pState, pInstances);
  if (layerIndex < 0) return TSDB_CODE_INTERNAL_ERROR;
  uint64_t generation = 0;
  int32_t  code = stAllocateInstanceGeneration(pState, &generation);
  if (code != TSDB_CODE_SUCCESS) return code;
  const SWindowChainInstance instance = {
      .start = pRange->skey,
      .end = pRange->ekey,
      .rownum = 0,
      .prevProcTimeNs = nowNs,
      .generation = generation,
  };
  return stWindowChainInsertInstance(pState, layerIndex, taosArrayGetSize(pInstances), &instance, ppInstance);
}

static bool stTimeRangeComplete(const SWindowChainLayerState *pLeaf, TSKEY progress, EWindowLayerInputType inputType,
                                bool includeBoundary) {
  if (inputType == WINDOW_LAYER_INPUT_ANCESTOR_END) return progress >= pLeaf->nextTimeRange.ekey;
  if (inputType == WINDOW_LAYER_INPUT_ROW) {
    return includeBoundary ? progress >= pLeaf->nextTimeRange.ekey : progress > pLeaf->nextTimeRange.ekey;
  }
  return pLeaf->pureSliding ? progress >= pLeaf->nextTimeRange.ekey : progress > pLeaf->nextTimeRange.ekey;
}

static int32_t stAdvanceTimeLeafRanges(SWindowChainState *pState, TSKEY progress, EWindowLayerInputType inputType,
                                       bool includeBoundary, int64_t nowNs, SArray *pCandidates) {
  SWindowChainLayerState *pLeaf = &pState->layers[pState->numLayers - 1];
  const int32_t           numInstances = taosArrayGetSize(pLeaf->pInstances);
  int32_t                 instanceIndex = 0;
  while (pLeaf->timeCursorInitialized && stTimeRangeComplete(pLeaf, progress, inputType, includeBoundary)) {
    SWindowChainInstance  empty = {.start = pLeaf->nextTimeRange.skey, .end = pLeaf->nextTimeRange.ekey, .rownum = 0};
    SWindowChainInstance *pInstance = &empty;
    while (instanceIndex < numInstances) {
      SWindowChainInstance *pStored = taosArrayGet(pLeaf->pInstances, instanceIndex);
      if (pStored->start >= pLeaf->nextTimeRange.skey) {
        if (pStored->start == pLeaf->nextTimeRange.skey) {
          pInstance = pStored;
          ++instanceIndex;
        }
        break;
      }
      ++instanceIndex;
    }
    if ((pState->policy.leafEventTypes & STRIGGER_EVENT_WINDOW_CLOSE) != 0) {
      int32_t code = stAppendCandidate(pState, pInstance, STRIGGER_EVENT_WINDOW_CLOSE, nowNs, pCandidates);
      if (code != TSDB_CODE_SUCCESS) return code;
    }
    if (pLeaf->nextTimeRange.skey == INT64_MAX || pLeaf->nextTimeRange.ekey == INT64_MAX) {
      pLeaf->timeCursorInitialized = false;
      pLeaf->nextTimeRange = (STimeWindow){0};
      break;
    }
    STimeWindow next = {0};
    int32_t     code = stNextTimeRange(pLeaf, &pLeaf->nextTimeRange, &next);
    if (code != TSDB_CODE_SUCCESS) return code;
    pLeaf->nextTimeRange = next;
  }
  for (int32_t i = 0; i < instanceIndex; ++i) {
    int32_t code = stWindowChainRemoveInstance(pState, pState->numLayers - 1, 0);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t stRouteRow(SWindowChainState *pState, int32_t layerIndex, const SWindowChainPeerGroup *pGroup,
                          int64_t nowNs, SArray *pBatches, SArray *pCandidates);

static int32_t stRouteStateLeaf(SWindowChainState *pState, int32_t layerIndex, const SWindowChainPeerGroup *pGroup,
                                int64_t nowNs, SArray *pBatches, SArray *pCandidates) {
  SWindowChainLayerState   *pLayer = &pState->layers[layerIndex];
  const SWindowChainRowRef *pRow = taosArrayGet(pGroup->pRows, 0);
  TSKEY                     ts = 0;
  int32_t                   code = stWindowChainGetTimestamp(pLayer, pRow, &ts);
  if (code != TSDB_CODE_SUCCESS) return code;

  bool allNull = true;
  bool hasNull = false;
  for (int32_t i = 0; i < taosArrayGetSize(pLayer->pSpec->input.pConditionSlotIds); ++i) {
    const int16_t          slot = *(const int16_t *)taosArrayGet(pLayer->pSpec->input.pConditionSlotIds, i);
    const SColumnInfoData *pColumn = stWindowChainGetColumn(pRow, slot);
    if (pColumn == NULL) return TSDB_CODE_INVALID_PARA;
    const bool isNull = colDataIsNull_s(pColumn, pRow->rowIndex);
    allNull = allNull && isNull;
    hasNull = hasNull || isNull;
  }

  SWindowChainInstance *pCurrent =
      taosArrayGetSize(pLayer->pInstances) == 0 ? NULL : taosArrayGet(pLayer->pInstances, 0);
  if (pCurrent != NULL) {
    code = stWindowChainTouchInstance(pState, pCurrent);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  if (allNull) {
    if (pLayer->pendingNullCount == 0) pLayer->pendingNullStart = ts;
    ++pLayer->pendingNullCount;
    if (pLayer->deferredPartialNullCount > 0) ++pLayer->deferredTailAllNullCount;
    return stAppendAcceptedRow(pState, pRow, pBatches);
  }

  bool equal = false;
  if (pCurrent != NULL) {
    if (!pLayer->hasPendingPartialNull) {
      code = stWindowChainSyncPendingState(pLayer);
      if (code != TSDB_CODE_SUCCESS) return code;
    }
    code = stWindowChainPendingStateRowEqual(pLayer, pRow, !hasNull, &equal);
    if (code != TSDB_CODE_SUCCESS) return code;
    if (equal && hasNull) {
      code = stWindowChainAccumulatePartialState(pLayer, pRow, ts);
      if (code != TSDB_CODE_SUCCESS) return code;
      return stAppendAcceptedRow(pState, pRow, pBatches);
    }
    if (equal) {
      code = stWindowChainCommitPendingState(pLayer);
      if (code != TSDB_CODE_SUCCESS) return code;
      pCurrent->end = ts;
      pCurrent->rownum += pLayer->pendingNullCount + 1;
      pLayer->pendingNullCount = 0;
      pLayer->pendingNullStart = 0;
      stWindowChainResetDeferredState(pLayer);
      code = stWindowChainStateIsZeroth(pLayer, &pCurrent->suppressed);
      if (code != TSDB_CODE_SUCCESS) return code;
      code = stAppendAcceptedRow(pState, pRow, pBatches);
      if (code == TSDB_CODE_SUCCESS) {
        code = stAppendOpenCandidateWithContent(pState, pCurrent, nowNs, pCurrent->pOpenNotifyContent, pCandidates);
      }
      return code;
    }
  }

  bool                *pOldDefined = NULL;
  char                *pCloseNotifyContent = NULL;
  const SArray        *pPreviousStateValues = NULL;
  SWindowChainStateCut cut = {.nextStart = pLayer->pendingNullCount > 0 ? pLayer->pendingNullStart : ts};
  if (pCurrent != NULL) {
    bool dualSide = false;
    code = stWindowChainPendingStateDualSide(pLayer, pRow, &dualSide);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
    if ((pState->policy.leafNotifyEventTypes & STRIGGER_EVENT_WINDOW_OPEN) != 0) {
      code = stWindowChainCopyStateDefined(pLayer->pStateValues, &pOldDefined);
      if (code != TSDB_CODE_SUCCESS) goto _exit;
    }
    const bool usePendingStateValues = dualSide && pLayer->hasPendingPartialNull;
    code = stWindowChainResolveStateCut(pLayer, pCurrent, ts, dualSide, &cut);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
    pPreviousStateValues = usePendingStateValues ? pLayer->pPendingStateValues : pLayer->pStateValues;
    code = stWindowChainStateIsZeroth(pLayer, &pCurrent->suppressed);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
    if (!pCurrent->suppressed && stWindowChainTrueForSatisfied(pLayer, pCurrent) &&
        (pState->policy.leafEventTypes & STRIGGER_EVENT_WINDOW_CLOSE) != 0 &&
        (pState->policy.leafNotifyEventTypes & STRIGGER_EVENT_WINDOW_CLOSE) != 0) {
      code = stWindowChainBuildStateNotifyContent(pLayer, STRIGGER_EVENT_WINDOW_CLOSE, pPreviousStateValues, NULL, pRow,
                                                  &pCloseNotifyContent);
      if (code != TSDB_CODE_SUCCESS) goto _exit;
    }
    code = stAppendOpenCandidateWithContent(pState, pCurrent, nowNs, pCurrent->pOpenNotifyContent, pCandidates);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
    if ((pState->policy.leafEventTypes & STRIGGER_EVENT_WINDOW_CLOSE) != 0) {
      code = stAppendCandidateWithContent(pState, pCurrent, STRIGGER_EVENT_WINDOW_CLOSE, nowNs, pCloseNotifyContent,
                                          pCandidates);
      if (code != TSDB_CODE_SUCCESS) goto _exit;
    }
    if (cut.splitStandalone) {
      SWindowChainInstance standalone = {.start = cut.standaloneStart,
                                         .end = cut.standaloneEnd,
                                         .rownum = pLayer->deferredPartialNullCount,
                                         .prevProcTimeNs = nowNs};
      code = stAppendOpenCandidate(pState, &standalone, nowNs, pCandidates);
      if (code != TSDB_CODE_SUCCESS) goto _exit;
      if ((pState->policy.leafEventTypes & STRIGGER_EVENT_WINDOW_CLOSE) != 0) {
        code = stAppendCandidate(pState, &standalone, STRIGGER_EVENT_WINDOW_CLOSE, nowNs, pCandidates);
        if (code != TSDB_CODE_SUCCESS) goto _exit;
      }
    }
    stRefreshCandidateIntents(pState, pCandidates);
    code = stWindowChainClearInstances(pState, layerIndex);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
    pCurrent = NULL;
    if (cut.splitStandalone) {
      pLayer->pendingNullCount -= pLayer->deferredPartialNullCount;
      stWindowChainResetDeferredState(pLayer);
      cut.nextStart = cut.standaloneEnd == INT64_MAX ? cut.standaloneEnd : cut.standaloneEnd + 1;
    }
  }

  TSKEY   start = ts;
  int64_t rows = 1;
  if (pLayer->pSpec->trigger.stateWin.extend == STATE_WIN_EXTEND_OPTION_FORWARD) {
    start = cut.nextStart;
    rows += pLayer->pendingNullCount;
  }
  const STimeWindow range = {.skey = start, .ekey = ts};
  code = stOpenInstance(pState, pLayer->pInstances, &range, nowNs, &pCurrent);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  pCurrent->rownum = rows;
  if ((pState->policy.leafNotifyEventTypes & STRIGGER_EVENT_WINDOW_OPEN) != 0) {
    code = stWindowChainBuildStateNotifyContent(pLayer, STRIGGER_EVENT_WINDOW_OPEN, pPreviousStateValues, pOldDefined,
                                                pRow, &pCurrent->pOpenNotifyContent);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
  }
  code = stWindowChainAssignStateRow(pLayer, pRow, true);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  code = stWindowChainStateIsZeroth(pLayer, &pCurrent->suppressed);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  memset(pLayer->pPendingStateTouched, 0, taosArrayGetSize(pLayer->pStateValues) * sizeof(bool));
  pLayer->hasPendingPartialNull = false;
  pLayer->pendingNullCount = 0;
  pLayer->pendingNullStart = 0;
  stWindowChainResetDeferredState(pLayer);
  code = stAppendAcceptedRow(pState, pRow, pBatches);
  if (code == TSDB_CODE_SUCCESS) {
    code = stAppendOpenCandidateWithContent(pState, pCurrent, nowNs, pCurrent->pOpenNotifyContent, pCandidates);
  }

_exit:
  taosMemoryFree(pOldDefined);
  taosMemoryFree(pCloseNotifyContent);
  return code;
}

static int32_t stRouteStateLayer(SWindowChainState *pState, int32_t layerIndex, const SWindowChainPeerGroup *pGroup,
                                 int64_t nowNs, SArray *pBatches, SArray *pCandidates) {
  SWindowChainLayerState *pLayer = &pState->layers[layerIndex];
  const bool              leaf = layerIndex == pState->numLayers - 1;
  int32_t                 code = stWindowChainCowStateValues(pState, layerIndex);
  if (code != TSDB_CODE_SUCCESS) return code;
  if (leaf) return stRouteStateLeaf(pState, layerIndex, pGroup, nowNs, pBatches, pCandidates);
  const SWindowChainRowRef *pRow = taosArrayGet(pGroup->pRows, 0);
  TSKEY                     ts = 0;
  code = stWindowChainGetTimestamp(pLayer, pRow, &ts);
  if (code != TSDB_CODE_SUCCESS) return code;
  SWindowChainInstance *pCurrent =
      taosArrayGetSize(pLayer->pInstances) == 0 ? NULL : taosArrayGet(pLayer->pInstances, 0);
  if (pCurrent != NULL) {
    code = stWindowChainTouchInstance(pState, pCurrent);
    if (code != TSDB_CODE_SUCCESS) return code;
  }

  if (stWindowChainStateRowAllNull(pLayer, pRow)) {
    if (pLayer->pendingNullCount == 0) pLayer->pendingNullStart = ts;
    ++pLayer->pendingNullCount;
    if (leaf) return stAppendAcceptedRow(pState, pRow, pBatches);
    if (pCurrent == NULL) return TSDB_CODE_SUCCESS;
    pCurrent->end = ts;
    ++pCurrent->rownum;
    return stRouteRow(pState, layerIndex + 1, pGroup, nowNs, pBatches, pCandidates);
  }

  bool equal = false;
  if (pCurrent != NULL) {
    code = stWindowChainStateRowEqual(pLayer, pRow, &equal);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  if (pCurrent != NULL && !equal) {
    const int16_t extend = pLayer->pSpec->trigger.stateWin.extend;
    const TSKEY   oldEnd = pCurrent->end;
    if (extend == STATE_WIN_EXTEND_OPTION_BACKWARD) {
      pCurrent->end = ts == INT64_MIN ? ts : ts - 1;
      pCurrent->rownum += pLayer->pendingNullCount;
    }
    if (leaf && (pState->policy.leafEventTypes & STRIGGER_EVENT_WINDOW_CLOSE) != 0) {
      code = stAppendCandidate(pState, pCurrent, STRIGGER_EVENT_WINDOW_CLOSE, nowNs, pCandidates);
    } else if (!leaf) {
      code = stSettleAndResetDescendants(pState, layerIndex, pCurrent, nowNs, pCandidates);
    }
    if (code != TSDB_CODE_SUCCESS) return code;
    stRefreshCandidateIntents(pState, pCandidates);
    code = stWindowChainClearInstances(pState, layerIndex);
    if (code != TSDB_CODE_SUCCESS) return code;
    pCurrent = NULL;

    TSKEY   start = ts;
    int64_t rows = 1;
    if (extend == STATE_WIN_EXTEND_OPTION_FORWARD) {
      start = oldEnd == INT64_MAX ? oldEnd : oldEnd + 1;
      rows += pLayer->pendingNullCount;
    }
    const STimeWindow range = {.skey = start, .ekey = ts};
    code = stOpenInstance(pState, pLayer->pInstances, &range, nowNs, &pCurrent);
    if (code != TSDB_CODE_SUCCESS) return code;
    pCurrent->rownum = rows;
    code = stWindowChainAssignStateRow(pLayer, pRow, true);
    if (code != TSDB_CODE_SUCCESS) return code;
    if (leaf) {
      code = stWindowChainStateIsZeroth(pLayer, &pCurrent->suppressed);
      if (code != TSDB_CODE_SUCCESS) return code;
    }
  } else if (pCurrent == NULL) {
    TSKEY   start = ts;
    int64_t rows = 1;
    if (pLayer->pSpec->trigger.stateWin.extend == STATE_WIN_EXTEND_OPTION_FORWARD && pLayer->pendingNullCount > 0) {
      start = pLayer->pendingNullStart;
      rows += pLayer->pendingNullCount;
    }
    const STimeWindow range = {.skey = start, .ekey = ts};
    code = stOpenInstance(pState, pLayer->pInstances, &range, nowNs, &pCurrent);
    if (code != TSDB_CODE_SUCCESS) return code;
    pCurrent->rownum = rows;
    code = stWindowChainAssignStateRow(pLayer, pRow, true);
    if (code != TSDB_CODE_SUCCESS) return code;
    if (leaf) {
      code = stWindowChainStateIsZeroth(pLayer, &pCurrent->suppressed);
      if (code != TSDB_CODE_SUCCESS) return code;
    }
  } else {
    pCurrent->end = ts;
    pCurrent->rownum += pLayer->pendingNullCount + 1;
    code = stWindowChainAssignStateRow(pLayer, pRow, false);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  pLayer->pendingNullCount = 0;
  pLayer->pendingNullStart = 0;

  if (leaf) {
    code = stAppendAcceptedRow(pState, pRow, pBatches);
    if (code == TSDB_CODE_SUCCESS) code = stAppendOpenCandidate(pState, pCurrent, nowNs, pCandidates);
    return code;
  }
  return stRouteRow(pState, layerIndex + 1, pGroup, nowNs, pBatches, pCandidates);
}

static int32_t stRouteCountLayer(SWindowChainState *pState, int32_t layerIndex, const SWindowChainPeerGroup *pGroup,
                                 int64_t nowNs, SArray *pBatches, SArray *pCandidates) {
  SWindowChainLayerState   *pLayer = &pState->layers[layerIndex];
  const bool                leaf = layerIndex == pState->numLayers - 1;
  const SWindowChainRowRef *pRow = taosArrayGet(pGroup->pRows, 0);
  TSKEY                     ts = 0;
  int32_t                   code = stWindowChainGetTimestamp(pLayer, pRow, &ts);
  if (code != TSDB_CODE_SUCCESS) return code;
  bool matches = false;
  code = stWindowChainCountRowMatches(pLayer, pRow, &matches);
  if (code != TSDB_CODE_SUCCESS || !matches) return code;

  SWindowChainInstance *pOpened = NULL;
  if (pLayer->totalCount % pLayer->pSpec->trigger.count.sliding == 0) {
    const STimeWindow range = {.skey = ts, .ekey = ts};
    code = stOpenInstance(pState, pLayer->pInstances, &range, nowNs, &pOpened);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  ++pLayer->totalCount;
  if (taosArrayGetSize(pLayer->pInstances) == 0) return TSDB_CODE_SUCCESS;
  for (int32_t i = 0; i < taosArrayGetSize(pLayer->pInstances); ++i) {
    SWindowChainInstance *pInstance = taosArrayGet(pLayer->pInstances, i);
    code = stWindowChainTouchInstanceAt(pState, layerIndex, i);
    if (code != TSDB_CODE_SUCCESS) return code;
    pInstance = taosArrayGet(pLayer->pInstances, i);
    pInstance->end = ts;
    ++pInstance->rownum;
  }

  if (!leaf) {
    SWindowChainInstance *pCurrent = taosArrayGet(pLayer->pInstances, 0);
    code = stRouteRow(pState, layerIndex + 1, pGroup, nowNs, pBatches, pCandidates);
    if (code != TSDB_CODE_SUCCESS) return code;
    if (pCurrent->rownum == pLayer->pSpec->trigger.count.countVal) {
      code = stSettleAndResetDescendants(pState, layerIndex, pCurrent, nowNs, pCandidates);
      if (code != TSDB_CODE_SUCCESS) return code;
      stRefreshCandidateIntents(pState, pCandidates);
      code = stWindowChainRemoveInstance(pState, layerIndex, 0);
      if (code != TSDB_CODE_SUCCESS) return code;
    }
    return TSDB_CODE_SUCCESS;
  }

  code = stAppendAcceptedRow(pState, pRow, pBatches);
  if (code != TSDB_CODE_SUCCESS) return code;
  if (pOpened != NULL) {
    code = stAppendOpenCandidate(pState, pOpened, nowNs, pCandidates);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  for (int32_t i = taosArrayGetSize(pLayer->pInstances) - 1; i >= 0; --i) {
    SWindowChainInstance *pInstance = taosArrayGet(pLayer->pInstances, i);
    if (pInstance->rownum != pLayer->pSpec->trigger.count.countVal) continue;
    if ((pState->policy.leafEventTypes & STRIGGER_EVENT_WINDOW_CLOSE) != 0) {
      code = stAppendCandidate(pState, pInstance, STRIGGER_EVENT_WINDOW_CLOSE, nowNs, pCandidates);
      if (code != TSDB_CODE_SUCCESS) return code;
    }
    stRefreshCandidateIntents(pState, pCandidates);
    code = stWindowChainRemoveInstance(pState, layerIndex, i);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t stRouteCountDisorderRow(SWindowChainState *pState, const SWindowChainPeerGroup *pGroup, int64_t nowNs,
                                       SArray *pBatches, SArray *pCandidates) {
  for (int32_t i = 0; i < pState->numLayers - 1; ++i) {
    SWindowChainInstance *pAncestor = taosArrayGet(pState->layers[i].pInstances, 0);
    if (pAncestor == NULL) return TSDB_CODE_INTERNAL_ERROR;
    int32_t code = stWindowChainTouchInstanceAt(pState, i, 0);
    if (code != TSDB_CODE_SUCCESS) return code;
    pAncestor = taosArrayGet(pState->layers[i].pInstances, 0);
    ++pAncestor->rownum;
  }

  SWindowChainLayerState   *pLeaf = &pState->layers[pState->numLayers - 1];
  const SWindowChainRowRef *pRow = taosArrayGet(pGroup->pRows, 0);
  TSKEY                     ts = 0;
  int32_t                   code = stWindowChainGetTimestamp(pLeaf, pRow, &ts);
  if (code != TSDB_CODE_SUCCESS) return code;
  bool matches = false;
  code = stWindowChainCountRowMatches(pLeaf, pRow, &matches);
  if (code != TSDB_CODE_SUCCESS || !matches) return code;

  const int32_t instanceCount = taosArrayGetSize(pLeaf->pInstances);
  int32_t       insertIndex = 0;
  while (insertIndex < instanceCount) {
    const SWindowChainInstance *pInstance = taosArrayGet(pLeaf->pInstances, insertIndex);
    if (pInstance == NULL) return TSDB_CODE_INTERNAL_ERROR;
    if (pInstance->start > ts) break;
    ++insertIndex;
  }
  const SWindowChainInstance *pLast = taosArrayGet(pLeaf->pInstances, instanceCount - 1);
  if (insertIndex <= 0 || insertIndex >= instanceCount || pLast == NULL) return TSDB_CODE_INVALID_PARA;

  for (int32_t i = 0; i < insertIndex; ++i) {
    SWindowChainInstance *pInstance = taosArrayGet(pLeaf->pInstances, i);
    if (pInstance == NULL) return TSDB_CODE_INTERNAL_ERROR;
    code = stWindowChainTouchInstanceAt(pState, pState->numLayers - 1, i);
    if (code != TSDB_CODE_SUCCESS) return code;
    pInstance = taosArrayGet(pLeaf->pInstances, i);
    ++pInstance->rownum;
  }
  uint64_t generation = 0;
  code = stAllocateInstanceGeneration(pState, &generation);
  if (code != TSDB_CODE_SUCCESS) return code;
  const SWindowChainInstance inserted = {
      .start = ts,
      .end = pLast->end,
      .rownum = instanceCount - insertIndex + 1,
      .prevProcTimeNs = nowNs,
      .generation = generation,
  };
  code = stWindowChainInsertInstance(pState, pState->numLayers - 1, insertIndex, &inserted, NULL);
  if (code != TSDB_CODE_SUCCESS) return code;
  ++pLeaf->totalCount;

  code = stAppendAcceptedRow(pState, pRow, pBatches);
  if (code != TSDB_CODE_SUCCESS) return code;
  for (int32_t i = taosArrayGetSize(pLeaf->pInstances) - 1; i >= 0; --i) {
    SWindowChainInstance *pInstance = taosArrayGet(pLeaf->pInstances, i);
    if (pInstance == NULL || pInstance->rownum != pLeaf->pSpec->trigger.count.countVal) continue;
    if ((pState->policy.leafEventTypes & STRIGGER_EVENT_WINDOW_CLOSE) != 0) {
      code = stAppendCandidate(pState, pInstance, STRIGGER_EVENT_WINDOW_CLOSE, nowNs, pCandidates);
      if (code != TSDB_CODE_SUCCESS) return code;
    }
    stRefreshCandidateIntents(pState, pCandidates);
    code = stWindowChainRemoveInstance(pState, pState->numLayers - 1, i);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t stWindowChainEventValue(const SWindowChainLayerState *pLayer, const SWindowChainRowRef *pRow,
                                       int16_t slotId, uint8_t *pValue) {
  *pValue = 0;
  if (slotId < 0) return TSDB_CODE_SUCCESS;
  const SColumnInfoData *pColumn = stWindowChainGetColumn(pRow, slotId);
  if (pColumn == NULL) return TSDB_CODE_INVALID_PARA;
  if (!colDataIsNull_s(pColumn, pRow->rowIndex)) *pValue = *(const uint8_t *)colDataGetData(pColumn, pRow->rowIndex);
  return TSDB_CODE_SUCCESS;
}

static int32_t stWindowChainBuildEventNotifyContent(const SWindowChainState *pState, const SWindowChainRowRef *pRow,
                                                    const SNodeList *pCondCols, int32_t conditionIndex,
                                                    const SWindowChainInstance *pInstance, int32_t windowIndex,
                                                    TSKEY parentWindowStart, char **ppContent) {
  int32_t code = streamBuildEventNotifyContent(pRow->pBlock, pCondCols, pRow->rowIndex, conditionIndex, windowIndex,
                                               pState->gid, pInstance->start, parentWindowStart, ppContent);
  if (code != TSDB_CODE_SUCCESS || windowIndex < 0) return code;

  cJSON *pObject = NULL;
  char  *pCanonicalContent = NULL;
  cJSON *pCanonicalParentId = NULL;
  char   parentTriggerId[STREAM_NESTED_TRIGGER_ID_LEN] = {0};
  code = stWindowChainBuildEventParentTriggerId(pState, parentWindowStart, parentTriggerId);
  if (code != TSDB_CODE_SUCCESS) goto _exit;

  pObject = cJSON_Parse(*ppContent);
  if (!cJSON_IsObject(pObject)) {
    code = stWindowChainAllocError();
    goto _exit;
  }
  pCanonicalParentId = cJSON_CreateString(parentTriggerId);
  if (pCanonicalParentId == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  cJSON *pLegacyParentId = cJSON_GetObjectItemCaseSensitive(pObject, "parentTriggerId");
  if (pLegacyParentId == NULL ||
      !cJSON_ReplaceItemInObjectCaseSensitive(pObject, "parentTriggerId", pCanonicalParentId)) {
    code = TSDB_CODE_INVALID_PARA;
    goto _exit;
  }
  pCanonicalParentId = NULL;
  pCanonicalContent = cJSON_PrintUnformatted(pObject);
  if (pCanonicalContent == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  taosMemoryFree(*ppContent);
  *ppContent = pCanonicalContent;
  pCanonicalContent = NULL;

_exit:
  cJSON_Delete(pCanonicalParentId);
  cJSON_Delete(pObject);
  cJSON_free(pCanonicalContent);
  return code;
}

static int32_t stWindowChainFreezeEventOpenNotifyContent(SWindowChainState *pState, const SWindowChainRowRef *pRow,
                                                         int32_t conditionIndex, int32_t windowIndex,
                                                         TSKEY parentWindowStart, SWindowChainInstance *pInstance) {
  if ((pState->policy.leafNotifyEventTypes & STRIGGER_EVENT_WINDOW_OPEN) == 0) return TSDB_CODE_SUCCESS;
  int32_t code = stWindowChainTouchInstance(pState, pInstance);
  if (code != TSDB_CODE_SUCCESS) return code;
  return stWindowChainBuildEventNotifyContent(pState, pRow, pState->policy.pEventStartCondCols, conditionIndex,
                                              pInstance, windowIndex, parentWindowStart,
                                              &pInstance->pOpenNotifyContent);
}

static int32_t stWindowChainAppendEventCloseCandidate(const SWindowChainState *pState, const SWindowChainRowRef *pRow,
                                                      const SWindowChainInstance *pInstance, int32_t windowIndex,
                                                      TSKEY parentWindowStart, int64_t nowNs, SArray *pCandidates) {
  char   *pNotifyContent = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  if ((pState->policy.leafNotifyEventTypes & STRIGGER_EVENT_WINDOW_CLOSE) != 0) {
    code = stWindowChainBuildEventNotifyContent(pState, pRow, pState->policy.pEventEndCondCols, 0, pInstance,
                                                windowIndex, parentWindowStart, &pNotifyContent);
  }
  if (code == TSDB_CODE_SUCCESS) {
    code = stAppendCandidateWithContent(pState, pInstance, STRIGGER_EVENT_WINDOW_CLOSE, nowNs, pNotifyContent,
                                        pCandidates);
  }
  taosMemoryFree(pNotifyContent);
  return code;
}

static int32_t stRouteEventLayer(SWindowChainState *pState, int32_t layerIndex, const SWindowChainPeerGroup *pGroup,
                                 int64_t nowNs, SArray *pBatches, SArray *pCandidates) {
  SWindowChainLayerState   *pLayer = &pState->layers[layerIndex];
  const bool                leaf = layerIndex == pState->numLayers - 1;
  const SWindowChainRowRef *pRow = taosArrayGet(pGroup->pRows, 0);
  TSKEY                     ts = 0;
  uint8_t                   start = 0;
  uint8_t                   end = 0;
  int32_t                   code = stWindowChainGetTimestamp(pLayer, pRow, &ts);
  if (code == TSDB_CODE_SUCCESS)
    code = stWindowChainEventValue(pLayer, pRow, pLayer->pSpec->input.eventStartSlotId, &start);
  if (code == TSDB_CODE_SUCCESS)
    code = stWindowChainEventValue(pLayer, pRow, pLayer->pSpec->input.eventEndSlotId, &end);
  if (code != TSDB_CODE_SUCCESS) return code;

  SWindowChainInstance *pCurrent = taosArrayGet(pLayer->pInstances, 0);
  if (pCurrent != NULL) {
    code = stWindowChainTouchInstance(pState, pCurrent);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  if (pCurrent != NULL && pLayer->multipleEventStarts && start != 0 && start != pLayer->eventConditionIndex) {
    if (leaf) {
      code = stAppendOpenCandidateWithContent(pState, pCurrent, nowNs, pCurrent->pOpenNotifyContent, pCandidates);
      if (code == TSDB_CODE_SUCCESS && (pState->policy.leafEventTypes & STRIGGER_EVENT_WINDOW_CLOSE) != 0) {
        code = stWindowChainAppendEventCloseCandidate(pState, pRow, pCurrent, (int32_t)pCurrent->nativeDiscriminator,
                                                      pLayer->eventParent.start, nowNs, pCandidates);
      }
      if (code != TSDB_CODE_SUCCESS) return code;
    }
    stRefreshCandidateIntents(pState, pCandidates);
    code = stWindowChainClearInstances(pState, layerIndex);
    if (code != TSDB_CODE_SUCCESS) return code;
    pCurrent = NULL;
  }
  if (pCurrent == NULL) {
    bool  openNow = start != 0;
    TSKEY openingTs = ts;
    if (start != 0 &&
        (pLayer->pSpec->trigger.event.startTrueForCount > 0 || pLayer->pSpec->trigger.event.startTrueForDuration > 0)) {
      if (pLayer->eventStartConditionCount == 0) pLayer->eventStartConditionFirstTs = ts;
      ++pLayer->eventStartConditionCount;
      STrueForInfo info = {.trueForType = pLayer->pSpec->trigger.event.startTrueForType,
                           .count = pLayer->pSpec->trigger.event.startTrueForCount,
                           .duration = pLayer->pSpec->trigger.event.startTrueForDuration};
      openNow = isTrueForSatisfied(&info, pLayer->eventStartConditionFirstTs, ts, pLayer->eventStartConditionCount);
      if (openNow) {
        openingTs = pLayer->eventStartConditionFirstTs;
        pLayer->eventStartConditionCount = 0;
        pLayer->eventStartConditionFirstTs = 0;
      }
    } else if (start == 0) {
      pLayer->eventStartConditionCount = 0;
      pLayer->eventStartConditionFirstTs = 0;
    }
    if (openNow) {
      const STimeWindow range = {.skey = openingTs, .ekey = ts};
      code = stOpenInstance(pState, pLayer->pInstances, &range, nowNs, &pCurrent);
      if (code != TSDB_CODE_SUCCESS) return code;
      if (pLayer->multipleEventStarts) {
        if (!pLayer->eventParentActive) {
          uint64_t generation = 0;
          code = stAllocateInstanceGeneration(pState, &generation);
          if (code != TSDB_CODE_SUCCESS) return code;
          pLayer->eventParentActive = true;
          stDestroyWindowChainInstance(&pLayer->eventParent);
          pLayer->eventParent = (SWindowChainInstance){
              .start = openingTs, .end = ts, .nativeDiscriminator = -1, .generation = generation};
          if (leaf) {
            code = stWindowChainFreezeEventOpenNotifyContent(pState, pRow, start - 1, -1, 0, &pLayer->eventParent);
            if (code != TSDB_CODE_SUCCESS) return code;
          }
        }
        pCurrent->nativeDiscriminator = pLayer->eventSubwindowCount++;
        pLayer->eventConditionIndex = start;
        if (leaf) {
          code = stWindowChainFreezeEventOpenNotifyContent(
              pState, pRow, start - 1, (int32_t)pCurrent->nativeDiscriminator, pLayer->eventParent.start, pCurrent);
          if (code != TSDB_CODE_SUCCESS) return code;
        }
      } else {
        pCurrent->nativeDiscriminator = -1;
        if (leaf) {
          code = stWindowChainFreezeEventOpenNotifyContent(pState, pRow, start - 1, -1, 0, pCurrent);
          if (code != TSDB_CODE_SUCCESS) return code;
        }
      }
    }
  }
  if (pCurrent == NULL) return TSDB_CODE_SUCCESS;

  pCurrent->end = ts;
  ++pCurrent->rownum;
  if (pLayer->eventParentActive) {
    pLayer->eventParent.end = ts;
    ++pLayer->eventParent.rownum;
  }
  if (leaf) {
    code = stAppendAcceptedRow(pState, pRow, pBatches);
    if (code == TSDB_CODE_SUCCESS && (!pLayer->multipleEventStarts || pLayer->eventSubwindowCount > 1)) {
      code = stAppendOpenCandidateWithContent(pState, pCurrent, nowNs, pCurrent->pOpenNotifyContent, pCandidates);
    }
    if (code == TSDB_CODE_SUCCESS && pLayer->multipleEventStarts && pLayer->eventSubwindowCount == 1) {
      code = stAppendOpenCandidateWithContent(pState, &pLayer->eventParent, nowNs,
                                              pLayer->eventParent.pOpenNotifyContent, pCandidates);
    }
  } else {
    code = stRouteRow(pState, layerIndex + 1, pGroup, nowNs, pBatches, pCandidates);
  }
  if (code != TSDB_CODE_SUCCESS) return code;

  bool  close = pLayer->multipleEventStarts && start == 0;
  TSKEY closingTs = ts;
  if (end != 0) {
    if (pLayer->pSpec->trigger.event.endTrueForCount > 0 || pLayer->pSpec->trigger.event.endTrueForDuration > 0) {
      if (pLayer->eventEndConditionCount == 0) pLayer->eventEndConditionFirstTs = ts;
      ++pLayer->eventEndConditionCount;
      STrueForInfo info = {.trueForType = pLayer->pSpec->trigger.event.endTrueForType,
                           .count = pLayer->pSpec->trigger.event.endTrueForCount,
                           .duration = pLayer->pSpec->trigger.event.endTrueForDuration};
      if (isTrueForSatisfied(&info, pLayer->eventEndConditionFirstTs, ts, pLayer->eventEndConditionCount)) {
        closingTs = pLayer->eventEndConditionFirstTs;
        pLayer->eventEndConditionCount = 0;
        pLayer->eventEndConditionFirstTs = 0;
        close = true;
      }
    } else {
      close = true;
    }
  } else {
    pLayer->eventEndConditionCount = 0;
    pLayer->eventEndConditionFirstTs = 0;
  }
  if (!close) return TSDB_CODE_SUCCESS;
  pCurrent->end = closingTs;
  if (pLayer->eventParentActive) pLayer->eventParent.end = closingTs;
  if (leaf) {
    if (pLayer->multipleEventStarts && pLayer->eventSubwindowCount == 1) {
      pCurrent->nativeDiscriminator = -1;
      pCurrent->openEmitted = pLayer->eventParent.openEmitted;
    }
    code = stAppendOpenCandidateWithContent(pState, pCurrent, nowNs, pCurrent->pOpenNotifyContent, pCandidates);
    if (code == TSDB_CODE_SUCCESS && (pState->policy.leafEventTypes & STRIGGER_EVENT_WINDOW_CLOSE) != 0) {
      const int32_t windowIndex = (int32_t)pCurrent->nativeDiscriminator;
      const TSKEY   parentWindowStart = windowIndex >= 0 ? pLayer->eventParent.start : 0;
      code = stWindowChainAppendEventCloseCandidate(pState, pRow, pCurrent, windowIndex, parentWindowStart, nowNs,
                                                    pCandidates);
    }
    if (code == TSDB_CODE_SUCCESS && pLayer->eventParentActive && pLayer->eventSubwindowCount > 1) {
      code = stAppendOpenCandidateWithContent(pState, &pLayer->eventParent, nowNs,
                                              pLayer->eventParent.pOpenNotifyContent, pCandidates);
      if (code == TSDB_CODE_SUCCESS && (pState->policy.leafEventTypes & STRIGGER_EVENT_WINDOW_CLOSE) != 0) {
        code = stWindowChainAppendEventCloseCandidate(pState, pRow, &pLayer->eventParent, -1, 0, nowNs, pCandidates);
      }
    }
  } else if (!leaf) {
    code = stSettleAndResetDescendants(pState, layerIndex, pCurrent, nowNs, pCandidates);
  }
  if (code != TSDB_CODE_SUCCESS) return code;
  stRefreshCandidateIntents(pState, pCandidates);
  code = stWindowChainClearInstances(pState, layerIndex);
  if (code != TSDB_CODE_SUCCESS) return code;
  pLayer->eventConditionIndex = 0;
  pLayer->eventSubwindowCount = 0;
  pLayer->eventParentActive = false;
  stDestroyWindowChainInstance(&pLayer->eventParent);
  pLayer->eventParent = (SWindowChainInstance){0};
  pLayer->eventStartConditionCount = 0;
  pLayer->eventStartConditionFirstTs = 0;
  pLayer->eventEndConditionCount = 0;
  pLayer->eventEndConditionFirstTs = 0;
  return TSDB_CODE_SUCCESS;
}

static int32_t stRouteTimeLeaf(SWindowChainState *pState, const SWindowChainPeerGroup *pGroup, int64_t nowNs,
                               SArray *pBatches, SArray *pCandidates) {
  const int32_t           leafIndex = pState->numLayers - 1;
  SWindowChainLayerState *pLeaf = &pState->layers[leafIndex];
  taosArrayClear(pLeaf->pScratchRanges);
  SWindowTransitionList   transitions = {.pRanges = pLeaf->pScratchRanges};
  const SWindowLayerInput input = {.type = WINDOW_LAYER_INPUT_ROW, .ts = pGroup->ts};
  int32_t                 code = stWindowLayerApply(pState, leafIndex, &input, &transitions);
  if (code != TSDB_CODE_SUCCESS) goto _exit;

  if (pLeaf->runtimeType == STREAM_TRIGGER_SESSION) {
    SWindowChainInstance *pCurrent = taosArrayGet(pLeaf->pInstances, 0);
    if (pCurrent != NULL) {
      code = stWindowChainTouchInstanceAt(pState, leafIndex, 0);
      if (code != TSDB_CODE_SUCCESS) goto _exit;
      pCurrent = taosArrayGet(pLeaf->pInstances, 0);
    }
    if (transitions.resetBeforeRoute && pCurrent != NULL) {
      if ((pState->policy.leafEventTypes & STRIGGER_EVENT_WINDOW_CLOSE) != 0) {
        code = stAppendCandidate(pState, pCurrent, STRIGGER_EVENT_WINDOW_CLOSE, nowNs, pCandidates);
        if (code != TSDB_CODE_SUCCESS) goto _exit;
      }
      code = stWindowChainClearInstances(pState, leafIndex);
      if (code != TSDB_CODE_SUCCESS) goto _exit;
      pCurrent = NULL;
    }
    bool opened = false;
    if (pCurrent == NULL) {
      const STimeWindow range = {.skey = pGroup->ts, .ekey = pGroup->ts};
      code = stOpenInstance(pState, pLeaf->pInstances, &range, nowNs, &pCurrent);
      if (code != TSDB_CODE_SUCCESS) goto _exit;
      opened = true;
    }
    pCurrent->end = pGroup->ts;
    pCurrent->rownum += taosArrayGetSize(pGroup->pRows);
    code = stAppendAcceptedBatch(pState, pGroup->pRows, pBatches);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
    if (opened && (pState->policy.leafEventTypes & STRIGGER_EVENT_WINDOW_OPEN) != 0) {
      code = stAppendCandidate(pState, pCurrent, STRIGGER_EVENT_WINDOW_OPEN, nowNs, pCandidates);
    }
    goto _exit;
  }

  code = stAdvanceTimeLeafRanges(pState, pGroup->ts, WINDOW_LAYER_INPUT_ROW, false, nowNs, pCandidates);
  if (code != TSDB_CODE_SUCCESS) goto _exit;

  int32_t instanceIndex = 0;
  for (int32_t i = 0; i < taosArrayGetSize(transitions.pRanges); ++i) {
    const STimeWindow    *pRange = taosArrayGet(transitions.pRanges, i);
    SWindowChainInstance *pInstance = NULL;
    while (instanceIndex < taosArrayGetSize(pLeaf->pInstances)) {
      pInstance = taosArrayGet(pLeaf->pInstances, instanceIndex);
      if (pInstance->start >= pRange->skey) break;
      ++instanceIndex;
    }
    if (instanceIndex == taosArrayGetSize(pLeaf->pInstances)) pInstance = NULL;
    bool opened = false;
    if (pInstance == NULL || pInstance->start != pRange->skey) {
      if (instanceIndex != taosArrayGetSize(pLeaf->pInstances)) {
        code = TSDB_CODE_INTERNAL_ERROR;
        goto _exit;
      }
      code = stOpenInstance(pState, pLeaf->pInstances, pRange, nowNs, &pInstance);
      if (code != TSDB_CODE_SUCCESS) goto _exit;
      opened = true;
    }
    ++instanceIndex;
    code = stWindowChainTouchInstance(pState, pInstance);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
    pInstance = taosArrayGet(pLeaf->pInstances, instanceIndex - 1);
    pInstance->rownum += taosArrayGetSize(pGroup->pRows);
    if (opened && (pState->policy.leafEventTypes & STRIGGER_EVENT_WINDOW_OPEN) != 0) {
      code = stAppendCandidate(pState, pInstance, STRIGGER_EVENT_WINDOW_OPEN, nowNs, pCandidates);
      if (code != TSDB_CODE_SUCCESS) goto _exit;
    }
  }
  if (taosArrayGetSize(transitions.pRanges) > 0) {
    if (!pLeaf->timeCursorInitialized) {
      pLeaf->nextTimeRange = *(STimeWindow *)taosArrayGet(transitions.pRanges, 0);
      pLeaf->timeCursorInitialized = true;
    }
    code = stAppendAcceptedBatch(pState, pGroup->pRows, pBatches);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
  }
  if (pLeaf->pureSliding) {
    code = stAdvanceTimeLeafRanges(pState, pGroup->ts, WINDOW_LAYER_INPUT_ROW, true, nowNs, pCandidates);
  }

_exit:
  return code;
}

static int32_t stRouteLeaf(SWindowChainState *pState, const SWindowChainPeerGroup *pGroup, int64_t nowNs,
                           SArray *pBatches, SArray *pCandidates) {
  const int32_t type = pState->layers[pState->numLayers - 1].runtimeType;
  switch (type) {
    case STREAM_TRIGGER_STATE:
      return stRouteStateLayer(pState, pState->numLayers - 1, pGroup, nowNs, pBatches, pCandidates);
    case STREAM_TRIGGER_COUNT:
      return stRouteCountLayer(pState, pState->numLayers - 1, pGroup, nowNs, pBatches, pCandidates);
    case STREAM_TRIGGER_EVENT:
      return stRouteEventLayer(pState, pState->numLayers - 1, pGroup, nowNs, pBatches, pCandidates);
    default:
      return stRouteTimeLeaf(pState, pGroup, nowNs, pBatches, pCandidates);
  }
}

static int32_t stRouteRow(SWindowChainState *pState, int32_t layerIndex, const SWindowChainPeerGroup *pGroup,
                          int64_t nowNs, SArray *pBatches, SArray *pCandidates) {
  if (layerIndex == pState->numLayers - 1) return stRouteLeaf(pState, pGroup, nowNs, pBatches, pCandidates);

  SWindowChainLayerState *pLayer = &pState->layers[layerIndex];
  if (pLayer->runtimeType == STREAM_TRIGGER_STATE) {
    return stRouteStateLayer(pState, layerIndex, pGroup, nowNs, pBatches, pCandidates);
  }
  if (pLayer->runtimeType == STREAM_TRIGGER_COUNT) {
    return stRouteCountLayer(pState, layerIndex, pGroup, nowNs, pBatches, pCandidates);
  }
  if (pLayer->runtimeType == STREAM_TRIGGER_EVENT) {
    return stRouteEventLayer(pState, layerIndex, pGroup, nowNs, pBatches, pCandidates);
  }
  taosArrayClear(pLayer->pScratchRanges);
  SWindowTransitionList   transitions = {.pRanges = pLayer->pScratchRanges};
  const SWindowLayerInput input = {.type = WINDOW_LAYER_INPUT_ROW, .ts = pGroup->ts};
  int32_t                 code = stWindowLayerApply(pState, layerIndex, &input, &transitions);
  if (code != TSDB_CODE_SUCCESS) goto _exit;

  SWindowChainInstance *pCurrent = taosArrayGet(pLayer->pInstances, 0);
  if (pCurrent != NULL) {
    code = stWindowChainTouchInstanceAt(pState, layerIndex, 0);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
    pCurrent = taosArrayGet(pLayer->pInstances, 0);
  }
  if (transitions.resetBeforeRoute && pCurrent != NULL) {
    code = stSettleAndResetDescendants(pState, layerIndex, pCurrent, nowNs, pCandidates);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
    stRefreshCandidateIntents(pState, pCandidates);
    code = stWindowChainClearInstances(pState, layerIndex);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
    pCurrent = NULL;
  }

  if (pLayer->runtimeType == STREAM_TRIGGER_SESSION) {
    if (pCurrent == NULL) {
      const STimeWindow range = {.skey = pGroup->ts, .ekey = pGroup->ts};
      code = stOpenInstance(pState, pLayer->pInstances, &range, nowNs, &pCurrent);
      if (code != TSDB_CODE_SUCCESS) goto _exit;
    }
    pCurrent->end = pGroup->ts;
  } else {
    const STimeWindow *pRange = taosArrayGet(transitions.pRanges, 0);
    if (pRange == NULL) goto _exit;
    if (pCurrent == NULL) {
      code = stOpenInstance(pState, pLayer->pInstances, pRange, nowNs, &pCurrent);
      if (code != TSDB_CODE_SUCCESS) goto _exit;
    }
  }
  pCurrent->rownum += taosArrayGetSize(pGroup->pRows);
  code = stRouteRow(pState, layerIndex + 1, pGroup, nowNs, pBatches, pCandidates);
  if (code != TSDB_CODE_SUCCESS) goto _exit;

  if (transitions.resetAfterRoute) {
    code = stSettleAndResetDescendants(pState, layerIndex, pCurrent, nowNs, pCandidates);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
    STimeWindow next = {0};
    code = stPureSlidingRange(&pLayer->pSpec->trigger.sliding,
                              pCurrent->end == INT64_MAX ? pCurrent->end : pCurrent->end + 1, &next);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
    uint64_t generation = 0;
    code = stAllocateInstanceGeneration(pState, &generation);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
    stRefreshCandidateIntents(pState, pCandidates);
    stDestroyWindowChainInstance(pCurrent);
    *pCurrent = (SWindowChainInstance){.start = next.skey, .end = next.ekey, .generation = generation};
  }

_exit:
  return code;
}

static int32_t stValidatePeerGroup(const SWindowChainState *pState, const SWindowChainPeerGroup *pGroup) {
  if (pGroup == NULL || pGroup->gid != pState->gid || pGroup->pRows == NULL ||
      pGroup->pRows->elemSize != sizeof(SWindowChainRowRef) || taosArrayGetSize(pGroup->pRows) <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  for (int32_t i = 0; i < taosArrayGetSize(pGroup->pRows); ++i) {
    const SWindowChainRowRef *pRef = taosArrayGet(pGroup->pRows, i);
    if (pRef == NULL || pRef->pBlock == NULL || pRef->rowIndex < 0 || pRef->rowIndex >= pRef->pBlock->info.rows) {
      return TSDB_CODE_INVALID_PARA;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t stMaterializeCandidateIntents(const SWindowChainState *pState, const SArray *pIntents,
                                             SArray **ppCandidates) {
  *ppCandidates = NULL;
  SArray *pCandidates = taosArrayInit(2, sizeof(SLeafEventCandidate));
  if (pCandidates == NULL) return stWindowChainAllocError();

  for (int32_t i = 0; i < taosArrayGetSize(pIntents); ++i) {
    const SWindowChainCandidateIntent *pIntent = taosArrayGet(pIntents, i);
    int32_t                            code =
        stAppendMaterializedCandidateWithContent(pState, &pIntent->instance, pIntent->ancestors, pIntent->eventType,
                                                 pIntent->nowNs, pIntent->pNotifyContent, pCandidates);
    if (code != TSDB_CODE_SUCCESS) {
      taosArrayDestroyEx(pCandidates, stDestroyLeafEventCandidate);
      return code;
    }
  }
  *ppCandidates = pCandidates;
  return TSDB_CODE_SUCCESS;
}

static int32_t stMaterializeAcceptedIntents(const SWindowChainState *pState, SArray *pIntents, SArray **ppBatches) {
  *ppBatches = NULL;
  SArray *pBatches = taosArrayInit(2, sizeof(SWindowChainAcceptedBatch));
  if (pBatches == NULL) return stWindowChainAllocError();

  for (int32_t i = 0; i < taosArrayGetSize(pIntents); ++i) {
    SWindowChainAcceptedIntent *pIntent = taosArrayGet(pIntents, i);
    SWindowChainAcceptedBatch   batch = {.cacheScope = {.gid = pState->gid}};
    int32_t code = stBuildLineageFromInstances(pState, pIntent->ancestors, &batch.cacheScope.lineage);
    if (code != TSDB_CODE_SUCCESS) {
      stDestroyAcceptedBatch(&batch);
      taosArrayDestroyEx(pBatches, stDestroyAcceptedBatch);
      return code;
    }
    batch.pRows = pIntent->pRows;
    pIntent->pRows = NULL;
    if (taosArrayPush(pBatches, &batch) == NULL) {
      code = stWindowChainAllocError();
      stDestroyAcceptedBatch(&batch);
      taosArrayDestroyEx(pBatches, stDestroyAcceptedBatch);
      return code;
    }
  }
  *ppBatches = pBatches;
  return TSDB_CODE_SUCCESS;
}

int32_t stWindowChainPreparePeerGroup(SWindowChainState *pState, const SWindowChainPeerGroup *pGroup, int64_t nowNs,
                                      SWindowChainSubmitResult *pResult, SWindowChainPeerGroupTxn **ppTxn) {
  if (pState == NULL || pResult == NULL || pResult->pAcceptedBatches != NULL || pResult->pCandidates != NULL ||
      ppTxn == NULL || *ppTxn != NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t code = stValidatePeerGroup(pState, pGroup);
  if (code != TSDB_CODE_SUCCESS) return code;

  SWindowChainPeerGroupTxn *pTxn = taosMemoryCalloc(1, sizeof(SWindowChainPeerGroupTxn));
  if (pTxn == NULL) return stWindowChainAllocError();
  SWindowChainMutationJournal *pJournal = &pTxn->journal;
  SArray                     *pRouteBatches = NULL;
  SArray                     *pRouteCandidates = NULL;
  SArray                     *pBatches = NULL;
  SArray                     *pCandidates = NULL;
  bool                        dataDriven = false;
  bool                        mutationStarted = false;
  const STimeWindow           peerRange = {.skey = pGroup->ts, .ekey = pGroup->ts};
  const bool                  repairCountDisorder =
      taosArrayGetSize(pGroup->pRows) == 1 && stWindowChainCanRepairCountDisorder(pState, &peerRange);
  code = stWindowChainBeginMutation(pState, pJournal);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  mutationStarted = true;
  for (int32_t i = 0; i < pState->numLayers; ++i) {
    if (stWindowChainDataDriven(pState->layers[i].runtimeType)) {
      dataDriven = true;
      break;
    }
  }
  pRouteBatches = taosArrayInit(2, dataDriven ? sizeof(SWindowChainAcceptedIntent) : sizeof(SWindowChainAcceptedBatch));
  pRouteCandidates = taosArrayInit(2, dataDriven ? sizeof(SWindowChainCandidateIntent) : sizeof(SLeafEventCandidate));
  if (pRouteBatches == NULL || pRouteCandidates == NULL) {
    code = stWindowChainAllocError();
    goto _exit;
  }
  if (dataDriven) {
    SArray *pSingleRow = taosArrayInit(1, sizeof(SWindowChainRowRef));
    if (pSingleRow == NULL) {
      code = stWindowChainAllocError();
      goto _exit;
    }
    for (int32_t i = 0; i < taosArrayGetSize(pGroup->pRows); ++i) {
      taosArrayClear(pSingleRow);
      const SWindowChainRowRef *pRow = taosArrayGet(pGroup->pRows, i);
      if (taosArrayPush(pSingleRow, pRow) == NULL) {
        code = stWindowChainAllocError();
        break;
      }
      const SWindowChainPeerGroup single = {.gid = pGroup->gid, .ts = pGroup->ts, .pRows = pSingleRow};
      code = repairCountDisorder ? stRouteCountDisorderRow(pState, &single, nowNs, pRouteBatches, pRouteCandidates)
                                 : stRouteRow(pState, 0, &single, nowNs, pRouteBatches, pRouteCandidates);
      if (code != TSDB_CODE_SUCCESS) break;
    }
    taosArrayDestroy(pSingleRow);
  } else {
    code = stRouteRow(pState, 0, pGroup, nowNs, pRouteBatches, pRouteCandidates);
  }
  if (code != TSDB_CODE_SUCCESS) goto _exit;

  if (dataDriven) {
    stRefreshCandidateIntents(pState, pRouteCandidates);
    code = stMaterializeCandidateIntents(pState, pRouteCandidates, &pCandidates);
    if (code == TSDB_CODE_SUCCESS) code = stMaterializeAcceptedIntents(pState, pRouteBatches, &pBatches);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
    taosArrayDestroyEx(pRouteCandidates, stDestroyCandidateIntent);
    taosArrayDestroyEx(pRouteBatches, stDestroyAcceptedIntent);
    pRouteCandidates = NULL;
    pRouteBatches = NULL;
  } else {
    pCandidates = pRouteCandidates;
    pBatches = pRouteBatches;
    pRouteCandidates = NULL;
    pRouteBatches = NULL;
  }

  mutationStarted = false;
  pResult->pAcceptedBatches = pBatches;
  pResult->pCandidates = pCandidates;
  *ppTxn = pTxn;
  return TSDB_CODE_SUCCESS;

_exit:
  if (mutationStarted) stWindowChainFinishMutation(pJournal, false);
  if (dataDriven) {
    taosArrayDestroyEx(pRouteBatches, stDestroyAcceptedIntent);
    taosArrayDestroyEx(pRouteCandidates, stDestroyCandidateIntent);
  } else {
    taosArrayDestroyEx(pRouteBatches, stDestroyAcceptedBatch);
    taosArrayDestroyEx(pRouteCandidates, stDestroyLeafEventCandidate);
  }
  taosArrayDestroyEx(pBatches, stDestroyAcceptedBatch);
  taosArrayDestroyEx(pCandidates, stDestroyLeafEventCandidate);
  taosMemoryFree(pTxn);
  return code;
}

void stWindowChainCommitPeerGroup(SWindowChainPeerGroupTxn **ppTxn) {
  if (ppTxn == NULL || *ppTxn == NULL) return;
  SWindowChainPeerGroupTxn *pTxn = *ppTxn;
  stWindowChainFinishMutation(&pTxn->journal, true);
  taosMemoryFree(pTxn);
  *ppTxn = NULL;
}

void stWindowChainAbortPeerGroup(SWindowChainPeerGroupTxn **ppTxn) {
  if (ppTxn == NULL || *ppTxn == NULL) return;
  SWindowChainPeerGroupTxn *pTxn = *ppTxn;
  stWindowChainFinishMutation(&pTxn->journal, false);
  taosMemoryFree(pTxn);
  *ppTxn = NULL;
}

int32_t stWindowChainSubmitPeerGroup(SWindowChainState *pState, const SWindowChainPeerGroup *pGroup, int64_t nowNs,
                                     SWindowChainSubmitResult *pResult) {
  SWindowChainPeerGroupTxn *pTxn = NULL;
  int32_t                   code = stWindowChainPreparePeerGroup(pState, pGroup, nowNs, pResult, &pTxn);
  if (code == TSDB_CODE_SUCCESS) stWindowChainCommitPeerGroup(&pTxn);
  return code;
}

static int32_t stAdvanceLeaf(SWindowChainState *pState, TSKEY frontier, EWindowLayerInputType inputType, int64_t nowNs,
                             SArray *pCandidates) {
  SWindowChainLayerState *pLeaf = &pState->layers[pState->numLayers - 1];
  if (stWindowChainDataDriven(pLeaf->runtimeType)) return TSDB_CODE_SUCCESS;
  if (pLeaf->runtimeType != STREAM_TRIGGER_SESSION) {
    if (!pLeaf->timeCursorInitialized) return TSDB_CODE_SUCCESS;
    return stAdvanceTimeLeafRanges(pState, frontier, inputType, false, nowNs, pCandidates);
  }
  for (int32_t i = taosArrayGetSize(pLeaf->pInstances) - 1; i >= 0; --i) {
    SWindowChainInstance *pInstance = taosArrayGet(pLeaf->pInstances, i);
    const int64_t         gap = pLeaf->pSpec->trigger.session.sessionVal;
    bool                  close = pInstance->end <= INT64_MAX - gap && frontier > pInstance->end + gap;
    if (!close) continue;
    if ((pState->policy.leafEventTypes & STRIGGER_EVENT_WINDOW_CLOSE) != 0) {
      int32_t code = stAppendCandidate(pState, pInstance, STRIGGER_EVENT_WINDOW_CLOSE, nowNs, pCandidates);
      if (code != TSDB_CODE_SUCCESS) return code;
    }
    int32_t code = stWindowChainRemoveInstance(pState, pState->numLayers - 1, i);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t stAdvanceLayer(SWindowChainState *pState, int32_t layerIndex, TSKEY frontier,
                              EWindowLayerInputType inputType, int64_t nowNs, SArray *pCandidates) {
  if (layerIndex == pState->numLayers - 1) return stAdvanceLeaf(pState, frontier, inputType, nowNs, pCandidates);

  SWindowChainLayerState *pLayer = &pState->layers[layerIndex];
  SWindowChainInstance   *pCurrent = taosArrayGet(pLayer->pInstances, 0);
  if (stWindowChainDataDriven(pLayer->runtimeType)) {
    if (pCurrent == NULL || !stLayerHasRuntimeState(&pState->layers[layerIndex + 1])) {
      return TSDB_CODE_SUCCESS;
    }
    if (inputType == WINDOW_LAYER_INPUT_ANCESTOR_END) {
      return stSettleAndResetDescendants(pState, layerIndex, pCurrent, nowNs, pCandidates);
    }
    return stAdvanceLayer(pState, layerIndex + 1, frontier, inputType, nowNs, pCandidates);
  }
  if (pLayer->pureSliding) {
    if (pCurrent == NULL) {
      STimeWindow range = {0};
      int32_t     code = stPureSlidingRange(&pLayer->pSpec->trigger.sliding, frontier, &range);
      if (code != TSDB_CODE_SUCCESS) return code;
      code = stOpenInstance(pState, pLayer->pInstances, &range, nowNs, &pCurrent);
      if (code != TSDB_CODE_SUCCESS) return code;
    } else {
      int32_t code = stWindowChainTouchInstanceAt(pState, layerIndex, 0);
      if (code != TSDB_CODE_SUCCESS) return code;
      pCurrent = taosArrayGet(pLayer->pInstances, 0);
    }
    while (frontier >= pCurrent->end) {
      int32_t code = stSettleAndResetDescendants(pState, layerIndex, pCurrent, nowNs, pCandidates);
      if (code != TSDB_CODE_SUCCESS) return code;
      STimeWindow next = {0};
      code = stPureSlidingRange(&pLayer->pSpec->trigger.sliding,
                                pCurrent->end == INT64_MAX ? pCurrent->end : pCurrent->end + 1, &next);
      if (code != TSDB_CODE_SUCCESS) return code;
      uint64_t generation = 0;
      code = stAllocateInstanceGeneration(pState, &generation);
      if (code != TSDB_CODE_SUCCESS) return code;
      stDestroyWindowChainInstance(pCurrent);
      *pCurrent = (SWindowChainInstance){.start = next.skey, .end = next.ekey, .generation = generation};
      if (pCurrent->end == INT64_MAX) break;
    }
    if (!stLayerHasRuntimeState(&pState->layers[layerIndex + 1])) return TSDB_CODE_SUCCESS;
    return stAdvanceLayer(pState, layerIndex + 1, frontier, inputType, nowNs, pCandidates);
  }
  if (pCurrent == NULL) return TSDB_CODE_SUCCESS;

  if (pLayer->runtimeType == STREAM_TRIGGER_SESSION) {
    const int64_t gap = pLayer->pSpec->trigger.session.sessionVal;
    if (pCurrent->end <= INT64_MAX - gap && frontier > pCurrent->end + gap) {
      int32_t code = stSettleAndResetDescendants(pState, layerIndex, pCurrent, nowNs, pCandidates);
      if (code != TSDB_CODE_SUCCESS) return code;
      code = stWindowChainClearInstances(pState, layerIndex);
      if (code != TSDB_CODE_SUCCESS) return code;
    } else {
      return stAdvanceLayer(pState, layerIndex + 1, TMIN(frontier, pCurrent->end), inputType, nowNs, pCandidates);
    }
    return TSDB_CODE_SUCCESS;
  }

  if (frontier > pCurrent->end || (inputType == WINDOW_LAYER_INPUT_ANCESTOR_END && frontier == pCurrent->end)) {
    int32_t code = stSettleAndResetDescendants(pState, layerIndex, pCurrent, nowNs, pCandidates);
    if (code != TSDB_CODE_SUCCESS) return code;
    code = stWindowChainClearInstances(pState, layerIndex);
    if (code != TSDB_CODE_SUCCESS) return code;
    return TSDB_CODE_SUCCESS;
  }
  return stAdvanceLayer(pState, layerIndex + 1, frontier, inputType, nowNs, pCandidates);
}

static int32_t stMergeCandidates(SArray *pTarget, SArray *pSource) {
  if (pTarget == NULL || pTarget->elemSize != sizeof(SLeafEventCandidate)) return TSDB_CODE_INVALID_PARA;
  const int32_t count = taosArrayGetSize(pSource);
  int32_t       code = taosArrayEnsureCap(pTarget, taosArrayGetSize(pTarget) + count);
  if (code != TSDB_CODE_SUCCESS) return code;
  if (count > 0) {
    TAOS_MEMCPY(TARRAY_GET_ELEM(pTarget, pTarget->size), pSource->pData, count * sizeof(SLeafEventCandidate));
    pTarget->size += count;
    pSource->size = 0;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t stWindowChainAdvanceFrontier(SWindowChainState *pState, TSKEY frontier, int64_t nowNs, SArray *pCandidates) {
  if (pState == NULL || pCandidates == NULL || pCandidates->elemSize != sizeof(SLeafEventCandidate)) {
    return TSDB_CODE_INVALID_PARA;
  }
  SWindowChainMutationJournal journal = {0};
  SArray                     *pStaged = NULL;
  bool                        mutationStarted = false;
  int32_t                     code = stWindowChainBeginMutation(pState, &journal);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  mutationStarted = true;
  pStaged = taosArrayInit(2, sizeof(SLeafEventCandidate));
  if (pStaged == NULL) {
    code = stWindowChainAllocError();
    goto _exit;
  }
  code = stAdvanceLayer(pState, 0, frontier, WINDOW_LAYER_INPUT_FRONTIER, nowNs, pStaged);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  code = stMergeCandidates(pCandidates, pStaged);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  stWindowChainFinishMutation(&journal, true);
  mutationStarted = false;

_exit:
  if (mutationStarted) stWindowChainFinishMutation(&journal, false);
  taosArrayDestroyEx(pStaged, stDestroyLeafEventCandidate);
  return code;
}

bool stWindowChainHasHistoryLeafTail(const SWindowChainState *pState) {
  if (pState == NULL) return false;

  const SWindowChainLayerState *pLeaf = &pState->layers[pState->numLayers - 1];
  if (pLeaf->runtimeType != STREAM_TRIGGER_SLIDING && pLeaf->runtimeType != STREAM_TRIGGER_SESSION &&
      pLeaf->runtimeType != STREAM_TRIGGER_STATE) {
    return false;
  }

  return (pState->policy.leafEventTypes & STRIGGER_EVENT_WINDOW_CLOSE) != 0 && taosArrayGetSize(pLeaf->pInstances) > 0;
}

int32_t stWindowChainPrepareHistoryLeafTail(const SWindowChainState *pState, int64_t nowNs, SArray *pCandidates) {
  if (pState == NULL || pCandidates == NULL || pCandidates->elemSize != sizeof(SLeafEventCandidate)) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (!stWindowChainHasHistoryLeafTail(pState)) return TSDB_CODE_SUCCESS;

  SArray *pStaged = taosArrayInit(2, sizeof(SLeafEventCandidate));
  if (pStaged == NULL) return stWindowChainAllocError();

  const SWindowChainLayerState *pLeaf = &pState->layers[pState->numLayers - 1];
  int32_t                       code = TSDB_CODE_SUCCESS;
  for (int32_t i = 0; i < taosArrayGetSize(pLeaf->pInstances); ++i) {
    code = stAppendCandidate(pState, taosArrayGet(pLeaf->pInstances, i), STRIGGER_EVENT_WINDOW_CLOSE, nowNs, pStaged);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
  }

  code = stMergeCandidates(pCandidates, pStaged);

_exit:
  taosArrayDestroyEx(pStaged, stDestroyLeafEventCandidate);
  return code;
}

void stWindowChainCommitHistoryLeafTail(SWindowChainState *pState) {
  if (pState == NULL) return;
  SWindowChainLayerState *pLeaf = &pState->layers[pState->numLayers - 1];
  if (pLeaf->runtimeType == STREAM_TRIGGER_SLIDING || pLeaf->runtimeType == STREAM_TRIGGER_SESSION ||
      pLeaf->runtimeType == STREAM_TRIGGER_STATE) {
    (void)stResetLayerRuntime(pState, pState->numLayers - 1);
  }
}

static int64_t stDelayDeadline(const SWindowChainState *pState, const SWindowChainInstance *pInstance) {
  if (pState->policy.maxDelayNs == 0 || pInstance->prevProcTimeNs > INT64_MAX - pState->policy.maxDelayNs) {
    return INT64_MAX;
  }
  return pInstance->prevProcTimeNs + pState->policy.maxDelayNs;
}

int64_t stWindowChainNextDelayDeadline(const SWindowChainState *pState) {
  if (pState == NULL || pState->policy.maxDelayNs == 0) return INT64_MAX;
  const SArray *pLeaves = pState->layers[pState->numLayers - 1].pInstances;
  int64_t       deadline = INT64_MAX;
  for (int32_t i = 0; i < taosArrayGetSize(pLeaves); ++i) {
    deadline = TMIN(deadline, stDelayDeadline(pState, taosArrayGet(pLeaves, i)));
  }
  return deadline;
}

int32_t stWindowChainCollectDelayedCandidates(SWindowChainState *pState, int64_t nowNs, SArray *pCandidates) {
  if (pState == NULL || pCandidates == NULL || pCandidates->elemSize != sizeof(SLeafEventCandidate)) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pState->policy.maxDelayNs == 0) return TSDB_CODE_SUCCESS;

  SArray *pStaged = taosArrayInit(2, sizeof(SLeafEventCandidate));
  SArray *pMaturedIndexes = taosArrayInit(2, sizeof(int32_t));
  if (pStaged == NULL || pMaturedIndexes == NULL) {
    int32_t code = stWindowChainAllocError();
    taosArrayDestroyEx(pStaged, stDestroyLeafEventCandidate);
    taosArrayDestroy(pMaturedIndexes);
    return code;
  }

  SArray *pLeaves = pState->layers[pState->numLayers - 1].pInstances;
  int32_t code = TSDB_CODE_SUCCESS;
  for (int32_t i = 0; i < taosArrayGetSize(pLeaves); ++i) {
    SWindowChainInstance *pInstance = taosArrayGet(pLeaves, i);
    if (stDelayDeadline(pState, pInstance) > nowNs) continue;
    code = stAppendCandidate(pState, pInstance, STRIGGER_EVENT_WINDOW_NONE, nowNs, pStaged);
    if (code != TSDB_CODE_SUCCESS || taosArrayPush(pMaturedIndexes, &i) == NULL) {
      if (code == TSDB_CODE_SUCCESS) code = stWindowChainAllocError();
      goto _exit;
    }
  }
  code = stMergeCandidates(pCandidates, pStaged);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  for (int32_t i = 0; i < taosArrayGetSize(pMaturedIndexes); ++i) {
    int32_t index = *(int32_t *)taosArrayGet(pMaturedIndexes, i);
    ((SWindowChainInstance *)taosArrayGet(pLeaves, index))->prevProcTimeNs = nowNs;
  }

_exit:
  taosArrayDestroyEx(pStaged, stDestroyLeafEventCandidate);
  taosArrayDestroy(pMaturedIndexes);
  return code;
}

void stWindowChainRearmDelayClocks(SWindowChainState *pState, int64_t nowNs) {
  if (pState == NULL) return;
  SArray *pLeaves = pState->layers[pState->numLayers - 1].pInstances;
  for (int32_t i = 0; i < taosArrayGetSize(pLeaves); ++i) {
    ((SWindowChainInstance *)taosArrayGet(pLeaves, i))->prevProcTimeNs = nowNs;
  }
}

bool stWindowChainHasCacheScope(const SWindowChainState *pState, const SStreamCacheScope *pScope) {
  if (pState == NULL || pScope == NULL || pScope->gid != pState->gid || pScope->lineage.pScopes == NULL ||
      pScope->lineage.pScopes->elemSize != sizeof(SScopeInstanceId) ||
      taosArrayGetSize(pScope->lineage.pScopes) != pState->numLayers - 1) {
    return false;
  }
  for (int32_t i = 0; i < taosArrayGetSize(pScope->lineage.pScopes); ++i) {
    const SScopeInstanceId       *pScopeId = taosArrayGet(pScope->lineage.pScopes, i);
    const SWindowChainLayerState *pLayer = &pState->layers[i];
    if (pScopeId == NULL || pScopeId->layerIndex != i || pScopeId->triggerType != pLayer->pSpec->triggerType ||
        stFindOrderedInstance(pLayer->pInstances, pScopeId->openingTs) == NULL) {
      return false;
    }
  }
  return true;
}
