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

#define TRIGGER_USE_HISTORY_META 0  // todo(kjq): remove the flag

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
  int64_t     prevProcTime;  // only used in realtime group for max_delay check
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

  TriggerWindowBuf winBuf;
  STimeWindow      nextWindow;  // for period trigger and sliding window trigger
  SValue           stateVal;    // for state window trigger

  bool    recalcNextWindow;
  int64_t prevCalcTime;        // only used in batch window mode (lowLatencyCalc is false)
  SArray *pPendingCalcParams;  // SArray<SSTriggerCalcParam>
} SSTriggerRealtimeGroup;

typedef struct SSTriggerHistoryGroup {
  struct SSTriggerHistoryContext *pContext;
  int64_t                         gid;
  TD_DLIST_NODE(SSTriggerHistoryGroup);

  SArray    *pVirTableInfos;  // SArray<SSTriggerVirTableInfo *>
  SSHashObj *pTableMetas;     // SSHashObj<tbUid, SSTriggerTableMeta>

  bool finished;

  TriggerWindowBuf winBuf;
  STimeWindow      nextWindow;
  SValue           stateVal;

  SArray *pPendingCalcParams;  // SArray<SSTriggerCalcParam>
} SSTriggerHistoryGroup;

typedef enum ESTriggerContextStatus {
  STRIGGER_CONTEXT_IDLE = 0,
  STRIGGER_CONTEXT_GATHER_VTABLE_INFO,
  STRIGGER_CONTEXT_DETERMINE_BOUND,
  STRIGGER_CONTEXT_ADJUST_START,
  STRIGGER_CONTEXT_FETCH_META,
  STRIGGER_CONTEXT_ACQUIRE_REQUEST,
  STRIGGER_CONTEXT_CHECK_CONDITION,
  STRIGGER_CONTEXT_SEND_CALC_REQ,
  STRIGGER_CONTEXT_WAIT_RECALC_REQ,
} ESTriggerContextStatus;

typedef struct SSTriggerWalProgress {
  SStreamTaskAddr          *pTaskAddr;    // reader task address
  int64_t                   lastScanVer;  // version of the last committed record in previous scan
  int64_t                   latestVer;    // latest version of committed records in the vnode WAL
  SSTriggerPullRequestUnion pullReq;
  SArray                   *reqCids;     // SArray<col_id_t>
  SArray                   *reqCols;     // SArray<OTableInfo>
  SArray                   *pMetadatas;  // SArray<SSDataBlock*>
} SSTriggerWalProgress;

typedef struct SSTriggerRealtimeContext {
  struct SStreamTriggerTask *pTask;
  int64_t                    sessionId;
  ESTriggerContextStatus     status;

  SSHashObj *pReaderWalProgress;  // SSHashObj<vgId, SSTriggerWalProgress>
  int32_t    curReaderIdx;
  bool       getWalMetaThisRound;

  SSHashObj *pGroups;
  TD_DLIST(SSTriggerRealtimeGroup) groupsToCheck;
  TD_DLIST(SSTriggerRealtimeGroup) groupsMaxDelay;

  // these fields are shared by all groups and do not need to be destroyed
  bool                   reenterCheck;
  bool                   needCheckAgain;
  int32_t                tbIter;
  STimeWindow            periodWindow;   // for period trigger
  SSTriggerVirTableInfo *pCurVirTable;   // only for virtual tables
  SSTriggerTableMeta    *pCurTableMeta;  // only for non-virtual tables
  SSTriggerMetaData     *pMetaToFetch;
  SSTriggerTableColRef  *pColRefToFetch;
  SSTriggerCalcParam    *pParamToFetch;
  SSTriggerCalcRequest  *pCalcReq;
  // these fields are shared by all groups and need to be destroyed
  SSTriggerTimestampSorter *pSorter;
  SSTriggerVtableMerger    *pMerger;
  SArray                   *pSavedWindows;  // for sliding trigger and session window trigger
  SArray                   *pInitWindows;   // for sliding trigger and session window trigger
  SFilterInfo              *pStartCond;     // for event window trigger
  SFilterInfo              *pEndCond;       // for event window trigger
  SArray                   *pNotifyParams;  // SArray<SSTriggerCalcParam>

  void     *pCalcDataCache;
  SHashObj *pCalcDataCacheIters;

  SList retryPullReqs;  // SList<SSTriggerPullRequest*>
  SList retryCalcReqs;  // SList<SSTriggerCalcRequest*>

  bool    haveReadCheckpoint;
  int64_t lastCheckpointTime;
  int64_t lastVirtTableInfoTime;
} SSTriggerRealtimeContext;

typedef struct SSTriggerTsdbProgress {
  SStreamTaskAddr          *pTaskAddr;  // reader task address
  SSTriggerPullRequestUnion pullReq;
  SArray                   *reqCids;     // SArray<col_id_t>
  SArray                   *pMetadatas;  // SArray<SSDataBlock*>
  int64_t                   version;
} SSTriggerTsdbProgress;

typedef struct SSTriggerHistoryContext {
  struct SStreamTriggerTask *pTask;
  int64_t                    sessionId;
  ESTriggerContextStatus     status;

  int64_t     gid;
  STimeWindow scanRange;
  STimeWindow calcRange;
  STimeWindow stepRange;
  bool        isHistory;
  bool        needTsdbMeta;
  bool        pendingToFinish;

  SSHashObj *pReaderTsdbProgress;  // SSHashObj<vgId, SSTriggerTsdbProgress>
  int32_t    curReaderIdx;

  SSHashObj *pFirstTsMap;  // SSHashObj<gid, int64_t>, for sliding trigger
  int32_t    trigDataBlockIdx;
  SArray    *pTrigDataBlocks;  // SArray<SSDataBlock*>
  int32_t    calcDataBlockIdx;
  SArray    *pCalcDataBlocks;  // SArray<SSDataBlock*>

  SSHashObj *pGroups;
  TD_DLIST(SSTriggerHistoryGroup) groupsToCheck;
  TD_DLIST(SSTriggerHistoryGroup) groupsForceClose;

  // these fields are shared by all groups and do not need to be destroyed
  bool                   reenterCheck;
  int32_t                tbIter;
  SSTriggerVirTableInfo *pCurVirTable;   // only for virtual tables
  SSTriggerTableMeta    *pCurTableMeta;  // only for non-virtual tables
  SSTriggerMetaData     *pMetaToFetch;
  SSTriggerTableColRef  *pColRefToFetch;
  SSTriggerCalcParam    *pParamToFetch;
  SSTriggerCalcRequest  *pCalcReq;
  // these fields are shared by all groups and need to be destroyed
  SSTriggerTimestampSorter *pSorter;
  SSTriggerVtableMerger    *pMerger;
  SArray                   *pSavedWindows;  // for sliding trigger and session window trigger
  SArray                   *pInitWindows;   // for sliding trigger and session window trigger
  SFilterInfo              *pStartCond;     // for event window trigger
  SFilterInfo              *pEndCond;       // for event window trigger
  SArray                   *pNotifyParams;  // SArray<SSTriggerCalcParam>

  void     *pCalcDataCache;
  SHashObj *pCalcDataCacheIters;

  SList retryPullReqs;  // SList<SSTriggerPullRequest*>
  SList retryCalcReqs;  // SList<SSTriggerCalcRequest*>
} SSTriggerHistoryContext;

typedef enum ESTriggerEventType {
  STRIGGER_EVENT_WINDOW_NONE = 0,
  STRIGGER_EVENT_WINDOW_CLOSE = 1 << 0,
  STRIGGER_EVENT_WINDOW_OPEN = 1 << 1,
  STRIGGER_EVENT_ON_TIME = 1 << 2,
} ESTriggerEventType;

typedef struct SSTriggerCalcSlot {
  SSTriggerCalcRequest req;  // keep in the first place
  TD_DLIST_NODE(SSTriggerCalcSlot);
} SSTriggerCalcSlot;

typedef struct SSTriggerCalcNode {
  SArray *pSlots;  // SArray<SSTriggerCalcSlot>
  TD_DLIST(SSTriggerCalcSlot) idleSlots;
} SSTriggerCalcNode;

typedef struct SSTriggerRecalcRequest {
  int64_t     gid;
  STimeWindow scanRange;
  STimeWindow calcRange;
  SSHashObj  *pTsdbVersions;
  bool        isHistory;
} SSTriggerRecalcRequest;

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
  int64_t maxDelayNs;
  int64_t fillHistoryStartTime;
  int64_t watermark;
  int64_t expiredTime;
  bool    ignoreDisorder;
  bool    fillHistory;
  bool    fillHistoryFirst;
  bool    lowLatencyCalc;
  bool    hasPartitionBy;
  bool    isVirtualTable;
  bool    ignoreNoDataTrigger;
  bool    hasTriggerFilter;
  int64_t placeHolderBitmap;
  SNode  *triggerFilter;
  // notify options
  ESTriggerEventType calcEventType;
  ESTriggerEventType notifyEventType;
  SArray            *pNotifyAddrUrls;
  int32_t            notifyErrorHandle;
  bool               notifyHistory;

  // task info
  int32_t leaderSnodeId;
  SArray *readerList;      // SArray<SStreamTaskAddr>
  SArray *virtReaderList;  // SArray<SStreamTaskAddr>
  SArray *runnerList;      // SArray<SStreamRunnerTarget>

  // virtual table info
  SSDataBlock *pVirDataBlock;
  int32_t      nVirDataCols;      // number of non-pseudo data columns in pVirDataBlock
  SArray      *pVirTrigSlots;     // SArray<int32_t>
  SArray      *pVirCalcSlots;     // SArray<int32_t>
  SArray      *pVirTableInfoRsp;  // SArray<VTableInfo>
  SSHashObj   *pOrigTableCols;    // SSHashObj<dbname, SSHashObj<tbname, SSTriggerOrigTableInfo>>
  SSHashObj   *pReaderUidMap;     // SSHashObj<vgId, SArray<int64_t, int64_t>>
  SSHashObj   *pOrigTableGroups;  // SSHashObj<otbUid, SArray<vtbUid>>
  SSHashObj   *pVirTableInfos;    // SSHashObj<vtbUid, SSTriggerVirTableInfo>
  bool         virTableInfoReady;

  // boundary between realtime and history
  SSHashObj *pRealtimeStartVer;  // SSHashObj<vgId, int64_t>
  SSHashObj *pHistoryCutoffTime;

  // calc request pool
  SRWLatch   calcPoolLock;
  SArray    *pCalcNodes;     // SArray<SSTriggerCalcNode>
  SSHashObj *pGroupRunning;  // SSHashObj<gid, bool[]>

  SRWLatch recalcRequestLock;
  SList   *pRecalcRequests;  // SList<SSTriggerRecalcRequest>

  // runtime status
  volatile int8_t           isCheckpointReady;
  volatile int32_t          checkpointVersion;
  volatile int64_t          mgmtReqId;
  bool                      historyCalcStarted;
  char                     *streamName;
  SSTriggerRealtimeContext *pRealtimeContext;
  SSTriggerHistoryContext  *pHistoryContext;
} SStreamTriggerTask;

// interfaces called by stream trigger thread
int32_t stTriggerTaskAcquireRequest(SStreamTriggerTask *pTask, int64_t sessionId, int64_t gid,
                                    SSTriggerCalcRequest **ppRequest);
int32_t stTriggerTaskReleaseRequest(SStreamTriggerTask *pTask, SSTriggerCalcRequest **ppRequest);

int32_t stTriggerTaskAddRecalcRequest(SStreamTriggerTask *pTask, SSTriggerRealtimeGroup *pGroup,
                                      STimeWindow *pCalcRange, SSHashObj *pWalProgress, bool isHistory);
int32_t stTriggerTaskFetchRecalcRequest(SStreamTriggerTask *pTask, SSTriggerRecalcRequest **ppReq);

// interfaces called by stream mgmt thread
int32_t stTriggerTaskDeploy(SStreamTriggerTask *pTask, SStreamTriggerDeployMsg *pMsg);
int32_t stTriggerTaskUndeploy(SStreamTriggerTask **ppTask, bool force);
int32_t stTriggerTaskExecute(SStreamTriggerTask *pTask, const SStreamMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /* ifndef TDENGINE_TSTREAM_H */
