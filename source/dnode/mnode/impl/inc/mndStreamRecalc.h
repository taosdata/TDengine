/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 */

#ifndef _TD_MND_STREAM_RECALC_H_
#define _TD_MND_STREAM_RECALC_H_

#include "mndStream.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SStreamRecalcTerminalCandidate {
  SStreamRecalcSnapshot snapshot;
  int32_t               recordIndexHint;
  int32_t               retryOrdinal;
  int32_t               errorCode;
  const char           *errorText;
} SStreamRecalcTerminalCandidate;

int32_t mndStreamRecalcAccept(SMnode *pMnode, SStreamObj *pStream, SStmStatus *pStatus, const STimeWindow *pRange,
                              const SRpcMsg *pReq);
int32_t mndStreamRecalcFinish(SMnode *pMnode, int64_t streamId, const SStreamRecalcTerminalCandidate *pCandidate);
int32_t mndStreamRecalcSnapshot(const SStreamObj *pStream, SArray **ppRequests);
int32_t mndStreamRecalcRestore(const SStreamObj *pStream, SStmStatus *pStatus, int64_t triggerTaskId,
                               int64_t triggerSeriousId);
int32_t mndStreamRecalcBuildDispatch(SStmStatus *pStatus, SArray **ppRequests);
int32_t mndStreamRecalcApplySnapshot(SMnode *pMnode, int64_t streamId, SStmStatus *pStatus, int64_t triggerTaskId,
                                     int64_t triggerSeriousId, bool completeSnapshot,
                                     EStreamRecalcDetailState detailState, const SArray *pSnapshots,
                                     const SArray *pDetails);
/**
 * Apply a heartbeat snapshot while the caller holds runtimeLock(R). The caller
 * initializes pStartDeferred to false and schedules pull-up after unlocking if
 * this function changes it to true.
 */
int32_t mndStreamRecalcApplySnapshotDeferred(SMnode *pMnode, int64_t streamId, SStmStatus *pStatus,
                                             int64_t triggerTaskId, int64_t triggerSeriousId, bool completeSnapshot,
                                             EStreamRecalcDetailState detailState, const SArray *pSnapshots,
                                             const SArray *pDetails, bool *pStartDeferred);
void    mndStreamRecalcInitStatus(SStmStatus *pStatus);
void    mndStreamRecalcCancelPending(SStmStatus *pStatus, int32_t code);
void    mndStreamRecalcTransStopped(SMnode *pMnode, void *param, int32_t paramLen);
void    mndStreamRecalcSchedulePullupPostUnlock(SMnode *pMnode);
void    mndStreamRecalcPullup(SMnode *pMnode);

#ifdef __cplusplus
}
#endif

#endif /* _TD_MND_STREAM_RECALC_H_ */
