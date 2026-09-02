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

#ifndef TDENGINE_STREAM_WINDOW_CHAIN_H
#define TDENGINE_STREAM_WINDOW_CHAIN_H

#include "nodes.h"
#include "streamMsg.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SWindowChainState SWindowChainState;
typedef struct SWindowChainPeerGroupTxn SWindowChainPeerGroupTxn;

typedef struct {
  int64_t gid;
  TSKEY   replayAnchor;
  TSKEY   capturedFrontier;
  SArray *pRootExtents;
} SRecalcImpactDomain;

static inline int32_t stWindowPlanTypeToTriggerType(EWindowType type, const SStreamTrigger *pTrigger) {
  if (pTrigger == NULL) return -1;

  switch (type) {
    case WINDOW_TYPE_INTERVAL:
      return STREAM_TRIGGER_SLIDING;
    case WINDOW_TYPE_SESSION:
      return STREAM_TRIGGER_SESSION;
    case WINDOW_TYPE_STATE:
      return STREAM_TRIGGER_STATE;
    case WINDOW_TYPE_COUNT:
      return STREAM_TRIGGER_COUNT;
    case WINDOW_TYPE_EVENT:
      return STREAM_TRIGGER_EVENT;
    default:
      return -1;
  }
}

typedef struct {
  const SSDataBlock *pBlock; /* Borrowed for one submit call. */
  int32_t            rowIndex;
  int64_t            tableUid;
} SWindowChainRowRef;

typedef struct {
  int64_t gid;
  TSKEY   ts;
  SArray *pRows; /* Borrowed SArray<SWindowChainRowRef>. */
} SWindowChainPeerGroup;

typedef struct {
  bool             flushOnOuterClose;
  int64_t          leafEventTypes;
  int64_t          leafNotifyEventTypes;
  int64_t          maxDelayNs;
  const SNodeList *pEventStartCondCols; /* Borrowed from the trigger task. */
  const SNodeList *pEventEndCondCols;   /* Borrowed from the trigger task. */
} SWindowChainPolicy;

typedef struct {
  SLeafInstanceId    instanceId;
  int32_t            eventType;
  int64_t            rowCount;
  SSTriggerCalcParam leafParam;
  SArray            *pAncestorSnapshots;
  SWindowLineage     lineage;
  SStreamCacheScope  cacheScope;
  STimeWindow        calcDataRange;
  STimeWindow        rootImpactExtent;
} SLeafEventCandidate;

typedef struct {
  SStreamCacheScope cacheScope;
  SArray           *pRows; /* Owned refs; referenced blocks remain borrowed. */
} SWindowChainAcceptedBatch;

typedef struct {
  SArray *pAcceptedBatches; /* Owned SArray<SWindowChainAcceptedBatch>. */
  SArray *pCandidates;      /* Owned SArray<SLeafEventCandidate>. */
} SWindowChainSubmitResult;

/* pPlan is borrowed and must outlive the returned state. */
int32_t stWindowChainCreate(const SStreamWindowPlan *pPlan, int64_t gid, const SWindowChainPolicy *pPolicy,
                            SWindowChainState **ppState);
void    stWindowChainDestroy(SWindowChainState **ppState);
int32_t stWindowChainSubmitPeerGroup(SWindowChainState *pState, const SWindowChainPeerGroup *pGroup, int64_t nowNs,
                                     SWindowChainSubmitResult *pResult);
/* A successful prepare must be paired with exactly one commit or abort. */
int32_t stWindowChainPreparePeerGroup(SWindowChainState *pState, const SWindowChainPeerGroup *pGroup, int64_t nowNs,
                                      SWindowChainSubmitResult *pResult, SWindowChainPeerGroupTxn **ppTxn);
void    stWindowChainCommitPeerGroup(SWindowChainPeerGroupTxn **ppTxn);
void    stWindowChainAbortPeerGroup(SWindowChainPeerGroupTxn **ppTxn);
int32_t stWindowChainAdvanceFrontier(SWindowChainState *pState, TSKEY frontier, int64_t nowNs, SArray *pCandidates);
bool    stWindowChainHasHistoryLeafTail(const SWindowChainState *pState);
int32_t stWindowChainPrepareHistoryLeafTail(const SWindowChainState *pState, int64_t nowNs, SArray *pCandidates);
void    stWindowChainCommitHistoryLeafTail(SWindowChainState *pState);
int32_t stWindowChainCollectDelayedCandidates(SWindowChainState *pState, int64_t nowNs, SArray *pCandidates);
int64_t stWindowChainNextDelayDeadline(const SWindowChainState *pState);
void    stWindowChainRearmDelayClocks(SWindowChainState *pState, int64_t nowNs);
bool    stWindowChainHasCacheScope(const SWindowChainState *pState, const SStreamCacheScope *pScope);
bool    stWindowChainGetInputRetentionRange(const SWindowChainState *pState, STimeWindow *pRange);
bool    stWindowChainGetFirstOpenCountLeafRange(const SWindowChainState *pState, STimeWindow *pRange);
bool    stWindowChainCanRepairCountDisorder(const SWindowChainState *pState, const STimeWindow *pRange);
void    stWindowChainSuppressOpenCountLeafBefore(SWindowChainState *pState, TSKEY firstUnaffectedStart);
int32_t stWindowChainBuildRecalcImpactDomain(const SStreamWindowPlan *pPlan, int64_t gid, const STimeWindow *pScanRange,
                                             const STimeWindow *pCalcRange, SRecalcImpactDomain *pDomain);
int32_t stCloneRecalcImpactDomain(const SRecalcImpactDomain *pSrc, SRecalcImpactDomain *pDst);
int32_t stUnionRecalcImpactDomains(const SRecalcImpactDomain *pLeft, const SRecalcImpactDomain *pRight,
                                   SRecalcImpactDomain *pUnion);
void    stDestroyRecalcImpactDomain(SRecalcImpactDomain *pDomain);
void    stDestroyLeafEventCandidate(void *pCandidate);
void    stDestroyWindowChainSubmitResult(SWindowChainSubmitResult *pResult);

#ifdef __cplusplus
}
#endif

#endif /* TDENGINE_STREAM_WINDOW_CHAIN_H */
