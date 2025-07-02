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

#ifndef TDENGINE_TSTREAM_H
#define TDENGINE_TSTREAM_H

#include "common/tmsg.h"
#include "streamTriggerMerger.h"
#include "tringbuf.h"
#include "tsimplehash.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SSTriggerVirTableInfo {
  int64_t tbGid;
  int64_t tbUid;
  int64_t tbVer;
  int32_t vgId;
  SArray *pTrigColRefs;  // SArray<SSTriggerTableColRef>
  SArray *pCalcColRefs;  // SArray<SSTriggerTableColRef>
} SSTriggerVirTableInfo;

typedef struct SSTriggerWindow {
  STimeWindow range;
  int64_t     wrownum;
  int64_t     prevProcTime;
} SSTriggerWindow;

typedef TRINGBUF(SSTriggerWindow) TriggerWindowBuf;

typedef struct SSTriggerRealtimeGroup {
  struct SSTriggerRealtimeContext *pContext;
  int64_t                          gid;
  TD_DLIST_NODE(SSTriggerRealtimeGroup);

  SArray    *pVirTableInfos;  // SArray<SSTriggerVirTableInfo *>
  SSHashObj *pTableMetas;     // SSHashObj<tbUid, SSTriggerTableMeta>

  int64_t oldThreshold;
  int64_t newThreshold;

  int32_t                tbIter;
  SSTriggerVirTableInfo *pCurVirTable;  // only for virtual tables
  SSTriggerTableMeta    *pCurTableMeta;

  TriggerWindowBuf winBuf;
  STimeWindow      nextWindow;  // for period trigger and sliding window trigger
  SValue           stateVal;    // for state window trigger
} SSTriggerRealtimeGroup;

typedef enum ESTriggerContextStatus {
  STRIGGER_CONTEXT_IDLE = 0,
  STRIGGER_CONTEXT_GATHER_VTABLE_INFO,
  STRIGGER_CONTEXT_DETERMINE_BOUND,
  STRIGGER_CONTEXT_FETCH_META,
  STRIGGER_CONTEXT_ACQUIRE_REQUEST,
  STRIGGER_CONTEXT_CHECK_CONDITION,
  STRIGGER_CONTEXT_SEND_CALC_REQ,
} ESTriggerContextStatus;

typedef struct SSTriggerWalProgress {
  SStreamTaskAddr *pTaskAddr;    // reader task address
  int64_t          lastScanVer;  // version of the last committed record in previous scan
  int64_t          latestVer;    // latest version of committed records in the vnode WAL
} SSTriggerWalProgress;

typedef struct SSTriggerRealtimeContext {
  struct SStreamTriggerTask *pTask;
  int64_t                    sessionId;
  ESTriggerContextStatus     status;

  int32_t maxMetaDelta;      // tables lagging behind the fastest table by this threshold will be ignored
  int32_t minMetaThreshold;  // window check start when any single table's metadata number exceed the threshold

  SSHashObj *pReaderWalProgress;  // SSHashObj<vgId, SSTriggerWalProgress>
  int32_t    curReaderIdx;
  bool       getWalMetaThisRound;

  SSHashObj *pGroups;
  TD_DLIST(SSTriggerRealtimeGroup) groupsToCheck;
  TD_DLIST(SSTriggerRealtimeGroup) groupsMaxDelay;

  SSTriggerTimestampSorter *pSorter;
  SSTriggerVtableMerger    *pMerger;
  SSTriggerMetaData        *pMetaToFetch;
  SSTriggerTableColRef     *pColRefToFetch;

  SArray      *pSavedWindows;  // for interval window trigger and session window trigger
  SArray      *pInitWindows;   // for interval window trigger and session window trigger
  SFilterInfo *pStartCond;     // for event window trigger
  SFilterInfo *pEndCond;       // for event window trigger

  SArray                   *pNotifyParams;  // SArray<SSTriggerCalcParam>
  SSTriggerPullRequestUnion pullReq;
  SArray                   *reqCids;  // SArray<col_id_t>
  SArray                   *reqCols;  // SArray<OTableInfo>
  SSDataBlock              *pullRes[STRIGGER_PULL_TYPE_MAX];
  SSTriggerCalcRequest     *pCalcReq;

  void     *pCalcDataCache;
  SHashObj *pCalcDataCacheIters;

  bool        retryPull;
  bool        haveReadCheckpoint;
  int64_t     lastCheckpointTime;
  STimeWindow periodWindow;  // for period trigger
} SSTriggerRealtimeContext;

typedef enum EStreamTriggerType {
  STREAM_TRIGGER_PERIOD = 0,
  STREAM_TRIGGER_SLIDING,  // sliding is 1 , can not change, because used in doOpenExternalWindow
  STREAM_TRIGGER_SESSION,
  STREAM_TRIGGER_COUNT,
  STREAM_TRIGGER_STATE,
  STREAM_TRIGGER_EVENT,
} EStreamTriggerType;

typedef enum ESTriggerEventType {
  STRIGGER_EVENT_WINDOW_NONE = 0,
  STRIGGER_EVENT_WINDOW_CLOSE = 1 << 0,
  STRIGGER_EVENT_WINDOW_OPEN = 1 << 1,
} ESTriggerEventType;

typedef struct SSTriggerCalcSlot {
  SSTriggerCalcRequest req;  // keep in the first place
  TD_DLIST_NODE(SSTriggerCalcSlot);
} SSTriggerCalcSlot;

typedef struct SSTriggerCalcNode {
  SArray *pSlots;  // SArray<SSTriggerCalcSlot>
  TD_DLIST(SSTriggerCalcSlot) idleSlots;
} SSTriggerCalcNode;

typedef struct SStreamTriggerTask {
  SStreamTask task;

  // trigger options
  EStreamTriggerType triggerType;
  union {
    SInterval interval;  // for sliding window
    int64_t   gap;       // for session window
    struct {             // for count window
      int64_t windowCount;
      int64_t windowSliding;
    };
    struct {  // for state window
      int64_t stateSlotId;
      int64_t stateTrueFor;
    };
    struct {  // for event window
      SNode     *pStartCond;
      SNode     *pEndCond;
      SNodeList *pStartCondCols;
      SNodeList *pEndCondCols;
      int64_t    eventTrueFor;
    };
  };
  int32_t trigTsIndex;
  int32_t calcTsIndex;
  int64_t maxDelay;  // precision is ms
  int64_t fillHistoryStartTime;
  int64_t watermark;
  int64_t expiredTime;
  bool    ignoreDisorder;
  bool    fillHistory;
  bool    fillHistoryFirst;
  bool    lowLatencyCalc;
  bool    hasPartitionBy;
  bool    isVirtualTable;
  int64_t placeHolderBitmap;
  SNode  *triggerFilter;
  // notify options
  ESTriggerEventType calcEventType;
  ESTriggerEventType notifyEventType;
  SArray            *pNotifyAddrUrls;
  int32_t            notifyErrorHandle;
  bool               notifyHistory;
  // reader and runner info
  SArray *readerList;      // SArray<SStreamTaskAddr>
  SArray *virtReaderList;  // SArray<SStreamTaskAddr>
  SArray *runnerList;      // SArray<SStreamRunnerTarget>

  // runtime info
  int32_t                   leaderSnodeId;
  SSTriggerRealtimeContext *pRealtimeContext;
  SSHashObj                *pRealtimeStartVer;
  SSHashObj                *pHistoryCutoffTime;

  SSDataBlock *pVirDataBlock;
  SArray      *pVirTrigSlots;     // SArray<int32_t>
  SArray      *pVirCalcSlots;     // SArray<int32_t>
  SArray      *pVirTableInfoRsp;  // SArray<VTableInfo>
  SSHashObj   *pOrigTableCols;    // SSHashObj<dbname, SSHashObj<tbname, SSTriggerOrigTableInfo>>
  SSHashObj   *pReaderUidMap;     // SSHashObj<vgId, SArray<int64_t, int64_t>>
  SSHashObj   *pVirTableInfos;    // SSHashObj<tbUid, SSTriggerVirTableInfo>
  bool         virTableInfoReady;

  // calc request pool
  SRWLatch   calcPoolLock;
  SArray    *pCalcNodes;     // SArray<SSTriggerCalcNode>
  SSHashObj *pGroupRunning;  // SSHashObj<gid, bool[]>

  // checkpoint
  bool isCheckpointReady;
  volatile int64_t mgmtReqId;
} SStreamTriggerTask;

// interfaces called by stream trigger thread
int32_t stTriggerTaskAcquireRequest(SStreamTriggerTask *pTask, int64_t sessionId, int64_t gid,
                                    SSTriggerCalcRequest **ppRequest);
int32_t stTriggerTaskReleaseRequest(SStreamTriggerTask *pTask, SSTriggerCalcRequest **ppRequest);
int32_t stTriggerTaskMarkRecalc(SStreamTriggerTask *pTask, int64_t gid, int64_t skey, int64_t ekey);

// interfaces called by stream mgmt thread
int32_t stTriggerTaskDeploy(SStreamTriggerTask *pTask, SStreamTriggerDeployMsg *pMsg);
int32_t stTriggerTaskUndeploy(SStreamTriggerTask **ppTask, bool force);
int32_t stTriggerTaskExecute(SStreamTriggerTask *pTask, const SStreamMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /* ifndef TDENGINE_TSTREAM_H */
