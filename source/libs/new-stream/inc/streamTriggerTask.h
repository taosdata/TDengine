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
#include "theap.h"
#include "tobjpool.h"
#include "tringbuf.h"
#include "tsimplehash.h"

#ifdef __cplusplus
extern "C" {
#endif

// #define SKIP_SEND_CALC_REQUEST

typedef struct SSTriggerVirtTableInfo {
  int64_t tbGid;
  int64_t tbUid;
  int64_t tbVer;
  int32_t vgId;
  SArray *pTrigColRefs;  // SArray<SSTriggerTableColRef>
  SArray *pCalcColRefs;  // SArray<SSTriggerTableColRef>
} SSTriggerVirtTableInfo;

typedef struct SSTriggerOrigTableInfo {
  int64_t    tbSuid;
  int64_t    tbUid;
  int32_t    vgId;
  SSHashObj *pTrigColMap;  // SSHashObj<colId, slotId>
  SSHashObj *pCalcColMap;  // SSHashObj<colId, slotId>
  SArray    *pVtbUids;     // SArray<int64_t>
} SSTriggerOrigTableInfo;

typedef struct SSTriggerWindow {
  STimeWindow range;
  int64_t     wrownum;
  int64_t     prevProcTime;  // only used in realtime group for max_delay check
} SSTriggerWindow;

typedef struct SSTriggerNotifyWindow {
  STimeWindow range;
  int64_t     wrownum;
  char       *pWinOpenNotify;
  char       *pWinCloseNotify;
} SSTriggerNotifyWindow;

typedef TRINGBUF(SSTriggerWindow) TriggerWindowBuf;

typedef struct SSTriggerRealtimeGroup {
  struct SSTriggerRealtimeContext *pContext;
  int64_t                          gid;
  int32_t                          vgId;
  bool                             recalcNextWindow;
  TD_DLIST_NODE(SSTriggerRealtimeGroup);

  SSHashObj *pWalMetas;  // SSHashObj<vgId, SObjList<SSTriggerMetaData>>
  SObjList   tableUids;  // SObjList<{uid, vgId}>, tables having data to check
  int64_t    oldThreshold;
  int64_t    newThreshold;

  SValue      stateVal;            // for state window trigger
  int64_t     pendingNullStart;    // for state window trigger
  int32_t     numPendingNull;      // for state window trigger
  STimeWindow prevWindow;          // the last closed window, for sliding trigger
  SObjList    windows;             // SObjList<SSTriggerWindow>, windows not yet closed
  SObjList    pPendingCalcParams;  // SObjList<SSTriggerCalcParam>
  SSHashObj  *pDoneVersions;       // SSHashObj<vgId, SObjList<{skey, ver}>>

  int64_t  nextExecTime;  // used for max delay and batch window mode
  HeapNode heapNode;      // used for max delay and batch window mode
} SSTriggerRealtimeGroup;

typedef struct SSTriggerHistoryGroup {
  struct SSTriggerHistoryContext *pContext;
  int64_t                         gid;
  TD_DLIST_NODE(SSTriggerHistoryGroup);

  SArray    *pVirtTableInfos;  // SArray<SSTriggerVirtTableInfo *>
  SSHashObj *pTableMetas;      // SSHashObj<tbUid, SSTriggerTableMeta>

  bool finished;

  TriggerWindowBuf winBuf;
  STimeWindow      nextWindow;
  SValue           stateVal;
  int64_t          pendingNullStart;
  int64_t          numPendingNull;

  SObjList pPendingCalcParams;  // SObjList<SSTriggerCalcParam>
  HeapNode heapNode;
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
  STRIGGER_CONTEXT_SEND_DROP_REQ,
} ESTriggerContextStatus;

typedef struct SSTriggerWalProgress {
  SStreamTaskAddr          *pTaskAddr;    // reader task address
  int64_t                   startVer;     // version to start realtime check
  int64_t                   savedVer;     // version saved in checkpoint
  int64_t                   doneVer;      // version already processed
  int64_t                   lastScanVer;  // version of the last committed record in previous scan
  int64_t                   verTime;      // commit time of the last commit record in previous scan
  SSTriggerPullRequestUnion pullReq;
  SArray                   *reqCids;         // SArray<col_id_t>
  SArray                   *reqCols;         // SArray<OTableInfo>
  SSHashObj                *uidInfoTrigger;  // SSHashObj<uid, SSHashObj<slotId, colId>>
  SSHashObj                *uidInfoCalc;     // SSHashObj<uid, SSHashObj<slotId, colId>>
  SArray                   *pVersions;       // SArray<int64_t>
  SSDataBlock              *pTrigBlock;
  SSDataBlock              *pCalcBlock;
} SSTriggerWalProgress;

typedef enum ESTriggerWalMode {
  STRIGGER_WAL_META_ONLY,
  STRIGGER_WAL_META_WITH_DATA,
  STRIGGER_WAL_META_THEN_DATA,
} ESTriggerWalMode;

typedef struct SSTriggerRealtimeContext {
  struct SStreamTriggerTask *pTask;
  int64_t                    sessionId;
  ESTriggerWalMode           walMode;
  ESTriggerContextStatus     status;

  SSHashObj *pReaderWalProgress;  // SSHashObj<vgId, SSTriggerWalProgress>
  int32_t    curReaderIdx;
  bool       catchUp;  // whether all readers have caught up the latest wal data
  bool       continueToFetch;

  SSDataBlock *pMetaBlock;
  SSDataBlock *pDeleteBlock;
  SSDataBlock *pDropBlock;
  SArray      *pTempSlices;  // SSArray<{gid, uid, startIdx, endIdx}>
  SSHashObj   *pRanges;      // SSHashObj<gid, STimeWindow>

  SSHashObj *pGroups;  // SSHashObj<gid, SSTriggerRealtimeGroup*>
  TD_DLIST(SSTriggerRealtimeGroup) groupsToCheck;
  Heap                   *pMaxDelayHeap;
  SSTriggerRealtimeGroup *pMinGroup;
  SArray                 *groupsToDelete;
  SSHashObj              *pGroupColVals;  // SSHashObj<gid, SArray<SStreamGroupValue>*>

  // these fields need to be cleared each round
  SSHashObj *pSlices;  // SSHashObj<uid, SSTriggerDataSlice>
  // these fields are shared by all groups and need to reset for each group
  bool                         needPseudoCols;
  bool                         needMergeWindow;
  bool                         needCheckAgain;
  SSTriggerNewTimestampSorter *pSorter;
  SSTriggerNewVtableMerger    *pMerger;
  SArray                      *pWindows;       // SArray<SSTriggerNotifyWindow>, valid windows in this round
  SArray                      *pNotifyParams;  // SArray<SSTriggerCalcParam>
  STimeWindow                  calcRange;
  SSTriggerCalcParam          *pCurParam;
  int64_t                      curParamRows;
  int64_t                      lastSentWinEnd;
  SObjList                     pAllCalcTableUids;  // SObjList<{uid, vgId}>
  SObjList                     pCalcTableUids;     // SObjList<{uid, vgId}>
  SSTriggerNewTimestampSorter *pCalcSorter;
  SSTriggerNewVtableMerger    *pCalcMerger;
  // these fields are shared by all groups and do not need to reset for each group
  STimeWindow           periodWindow;  // for period trigger
  SSTriggerCalcRequest *pCalcReq;
  SColumnInfoData       stateCol;       // for state window trigger with expr
  SColumnInfoData       eventStartCol;  // for event window trigger
  SColumnInfoData       eventEndCol;    // for event window trigger
  SObjPool              metaPool;       // SObjPool<SSTriggerMetaData>
  SObjPool              tableUidPool;   // SObjPool<{uid, vgId}>
  SObjPool              windowPool;     // SObjPool<SSTriggerWindow>
  SObjPool              calcParamPool;  // SObjPool<SSTriggerCalcParam>

  void     *pCalcDataCache;
  SHashObj *pCalcDataCacheIters;

  SList   retryPullReqs;  // SList<SSTriggerPullRequest*>
  SList   retryCalcReqs;  // SList<SSTriggerCalcRequest*>
  SList   dropTableReqs;  // SList<SSTriggerDropRequest*>
  int32_t dropReqIndex;

  bool    haveReadCheckpoint;
  bool    boundDetermined;
  bool    recovering;
  int64_t lastCheckpointTime;
  int64_t lastVirtTableInfoTime;
  int64_t lastReportTime;
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
  bool        finishCheck;
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
  Heap                  *pMaxDelayHeap;
  SSTriggerHistoryGroup *pMinGroup;
  SSHashObj             *pGroupColVals;  // SSHashObj<gid, SArray<SStreamGroupValue>*>

  // these fields are shared by all groups and do not need to be destroyed
  bool                    reenterCheck;
  int32_t                 tbIter;
  SSTriggerVirtTableInfo *pCurVirTable;   // only for virtual tables
  SSTriggerTableMeta     *pCurTableMeta;  // only for non-virtual tables
  SSTriggerMetaData      *pMetaToFetch;
  SSTriggerTableColRef   *pColRefToFetch;
  SSTriggerCalcParam     *pParamToFetch;
  SSTriggerCalcRequest   *pCalcReq;
  int64_t                 lastSentWinEnd;
  // these fields are shared by all groups and need to be destroyed
  SSTriggerTimestampSorter *pSorter;
  SSTriggerVtableMerger    *pMerger;
  SArray                   *pSavedWindows;  // for sliding trigger and session window trigger
  SArray                   *pInitWindows;   // for sliding trigger and session window trigger
  SFilterInfo              *pStartCond;     // for event window trigger
  SFilterInfo              *pEndCond;       // for event window trigger
  SArray                   *pNotifyParams;  // SArray<SSTriggerCalcParam>
  SObjPool                  calcParamPool;  // SObjPool<SSTriggerCalcParam>

  void     *pCalcDataCache;
  SHashObj *pCalcDataCacheIters;

  SList retryPullReqs;  // SList<SSTriggerPullRequest*>
  SList retryCalcReqs;  // SList<SSTriggerCalcRequest*>

  int64_t lastReportTime;
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
  TD_DLIST_NODE(SSTriggerRecalcRequest);
} SSTriggerRecalcRequest;

typedef TD_DLIST(SSTriggerRecalcRequest) TriggerRecalcRequestList;

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
      int64_t stateExtend;
      int64_t stateTrueFor;
      SNode  *pStateExpr;
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
  bool    isStbPartitionByTag;
  bool    ignoreNoDataTrigger;
  bool    hasTriggerFilter;
  int8_t  precision;
  int64_t historyStep;
  int64_t placeHolderBitmap;
  SNode  *triggerFilter;
  // trigger options: old version, to be removed
  int32_t histTrigTsIndex;
  int32_t histCalcTsIndex;
  int64_t histStateSlotId;
  SNode  *histTriggerFilter;
  SNode  *histStateExpr;
  SNode  *histStartCond;
  SNode  *histEndCond;
  // notify options
  ESTriggerEventType calcEventType;
  ESTriggerEventType notifyEventType;
  SArray            *pNotifyAddrUrls;
  int32_t            addOptions;
  bool               notifyHistory;

  // task info
  int32_t leaderSnodeId;
  SArray *readerList;      // SArray<SStreamTaskAddr>
  SArray *virtReaderList;  // SArray<SStreamTaskAddr>
  SArray *runnerList;      // SArray<SStreamRunnerTarget>

  // virtual table info: old version, to be removed
  SSDataBlock *pVirDataBlock;
  int32_t      nVirDataCols;   // number of non-pseudo data columns in pVirDataBlock
  SArray      *pVirTrigSlots;  // SArray<int32_t>
  SArray      *pVirCalcSlots;  // SArray<int32_t>
  // virtual table info: new version
  SSDataBlock *pVirtTrigBlock;    // trigger data schema for virtual table
  SSDataBlock *pVirtCalcBlock;    // calc data schema for virtual table
  SArray      *pTrigIsPseudoCol;  // SArray<bool>, whether the column in trig data is pseudo column, could be NULL
  SArray      *pCalcIsPseudoCol;  // SArray<bool>, whether the column in calc data is pseudo column, could be NULL
  col_id_t     trigTsSlotId;
  col_id_t     calcTsSlotId;
  bool         virScanTsOnly;    // whether the trigger and calc data only contains timestamp column
  SSHashObj   *pVirtTableInfos;  // SSHashObj<vtbUid, SSTriggerVirtTableInfo>
  SSHashObj   *pOrigTableInfos;  // SSHashObj<otbUid, SSTriggerOrigTableInfo>
  // virtual table info response
  bool       virTableInfoReady;
  SArray    *pVirTableInfoRsp;  // SArray<VTableInfo>
  SSHashObj *pOrigTableCols;    // SSHashObj<dbname, SSHashObj<tbname, SSTriggerOrigColumnInfo>*>

  // boundary between realtime and history
  SSHashObj *pHistoryCutoffTime;

  // calc request pool
  SRWLatch   calcPoolLock;
  SArray    *pCalcNodes;       // SArray<SSTriggerCalcNode>
  SSHashObj *pGroupRunning;    // SSHashObj<gid, bool[]>
  SSHashObj *pSessionRunning;  // SSHashObj<sessionId, cnt>

  SRWLatch   recalcRequestLock;
  SList     *pRecalcRequests;    // SList<SSTriggerRecalcRequest>
  SSHashObj *pRecalcRequestMap;  // SSHashObj<gid, TriggerRecalcRequestList>

  // runtime status
  volatile int8_t           isCheckpointReady;
  volatile int8_t           historyFinished;
  volatile int8_t           realtimeStarted;
  volatile int32_t          checkpointVersion;
  volatile int64_t          mgmtReqId;
  volatile int64_t          waitingMgmtReqId;
  volatile int64_t          latestVersionTime;
  bool                      historyCalcStarted;
  char                     *streamName;
  SSTriggerRealtimeContext *pRealtimeContext;
  SSTriggerHistoryContext  *pHistoryContext;
} SStreamTriggerTask;

// interfaces called by stream trigger thread
int32_t stTriggerTaskAcquireRequest(SStreamTriggerTask *pTask, int64_t sessionId, int64_t gid,
                                    SSTriggerCalcRequest **ppRequest);
int32_t stTriggerTaskReleaseRequest(SStreamTriggerTask *pTask, SSTriggerCalcRequest **ppRequest);
int32_t stTriggerTaskGetRunningReq(SStreamTriggerTask *pTask, int64_t sessionId, int64_t *pNumRunningReq);

int32_t stTriggerTaskAddRecalcRequest(SStreamTriggerTask *pTask, SSTriggerRealtimeGroup *pGroup,
                                      STimeWindow *pCalcRange, SSHashObj *pWalProgress, bool isHistory,
                                      bool shrinkRange);
int32_t stTriggerTaskFetchRecalcRequest(SStreamTriggerTask *pTask, SSTriggerRecalcRequest **ppReq);

// interfaces called by stream mgmt thread
int32_t stTriggerTaskDeploy(SStreamTriggerTask *pTask, SStreamTriggerDeployMsg *pMsg);
int32_t stTriggerTaskUndeploy(SStreamTriggerTask **ppTask, bool force);
int32_t stTriggerTaskExecute(SStreamTriggerTask *pTask, const SStreamMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /* ifndef TDENGINE_TSTREAM_H */
