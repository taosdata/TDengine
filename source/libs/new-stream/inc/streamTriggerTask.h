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
#include "streamRecalcTracker.h"
#include "streamTaskStats.h"
#include "streamTriggerMerger.h"
#include "streamWindowChain.h"
#include "tcompare.h"
#include "theap.h"
#include "tobjpool.h"
#include "tringbuf.h"
#include "tsimplehash.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SWindowChainState          SWindowChainState;
typedef struct SStreamTriggerPeerMerger   SStreamTriggerPeerMerger;
typedef struct SStreamTriggerNoticeBatch  SStreamTriggerNoticeBatch;
typedef struct SSTriggerNestedRecovery    SSTriggerNestedRecovery;
typedef struct SSTriggerRealtimeContext   SSTriggerRealtimeContext;
typedef struct SStagedNestedLeafEffects   SStagedNestedLeafEffects;

typedef struct {
  SSTriggerRealtimeContext *pContext;
} SSTriggerRealtimeBundle;

typedef enum {
  STRIGGER_NESTED_RECOVERY_VALIDATE = 1,
  STRIGGER_NESTED_RECOVERY_REPLAY_PENDING,
  STRIGGER_NESTED_RECOVERY_REPLAY,
  STRIGGER_NESTED_RECOVERY_REPLAY_DRAINED,
} ESTriggerNestedRecoveryPhase;

typedef enum {
  STRIGGER_NESTED_RECOVERY_ACTIVE = 1,
  STRIGGER_NESTED_RECOVERY_STALE,
} ESTriggerNestedRecoveryLifecycle;

typedef struct {
  uint64_t                     generation;
  ESTriggerNestedRecoveryPhase phase;
  int32_t                      vgId;
  int64_t                      taskId;
  SEpSet                       endpoint;
  uint32_t                     pageSeq;
  int64_t                      requestCursor;
} SSTriggerNestedWalToken;

typedef struct {
  int64_t requestCursor;
  int64_t endpoint;
} SSTriggerNestedWalPage;

typedef struct {
  SStreamTaskAddr         addr;
  int64_t                 startVer;
  int64_t                 savedVer;
  int64_t                 cursor;
  uint32_t                nextPageSeq;
  uint64_t                activeAttemptSerial;
  bool                    completed;
  bool                    expectedValid;
  bool                    roundParticipant;
  bool                    roundResponded;
  int64_t                 roundEndpoint;
  SSTriggerNestedWalToken expected;
  SArray                 *pValidateLedger;  // SArray<SSTriggerNestedWalPage>
} SSTriggerNestedWalReader;

struct SSTriggerNestedRecovery {
  volatile int32_t         refCount;
  volatile int32_t         inFlight;
  uint64_t                 generation;
  uint64_t                 nextAttemptSerial;
  uint64_t                 replayRoundSeq;
  volatile int8_t          lifecycle;
  int8_t                   phase;
  int32_t                  replayWalPending;
  volatile int32_t         replayPseudoInFlight;
  bool                     replayRoundActive;
  SArray                  *pReaders;     // immutable SArray<SStreamTaskAddr>
  SArray                  *pWalReaders;  // SArray<SSTriggerNestedWalReader>
  SSTriggerRealtimeBundle *pShadowBundle;
  SSTriggerNestedRecovery *pNextRetired;
};

typedef struct {
  SSTriggerNestedRecovery  *pRecovery;
  SSTriggerNestedWalToken   token;
  SSTriggerPullRequestUnion request;
  uint8_t                  *pRequestPayload;
  int32_t                   requestPayloadLen;
  int32_t                   readerIndex;
  uint64_t                  attemptSerial;
  bool                      handled;
} SSTriggerNestedWalAttempt;

typedef struct {
  SSTriggerNestedRecovery          *pRecovery;
  uint64_t                          recoveryGeneration;
  ESTriggerNestedRecoveryPhase      recoveryPhase;
  uint64_t                          replayRoundSeq;
  SStreamNestedInputRoute           route;
  SSTriggerVirTablePseudoColRequest request;
  uint8_t                          *pRequestPayload;
  int32_t                           requestPayloadLen;
  SEpSet                            endpoint;
  int32_t                           readerIndex;
  uint64_t                          attemptSerial;
  bool                              handled;
} SSTriggerNestedPseudoAttempt;

#define STREAM_MANUAL_RECALC_RETRY_BACKOFF_MS 100

// #define SKIP_SEND_CALC_REQUEST

typedef struct SSTriggerVirtTableInfo {
  int64_t tbUid;
  int64_t tbVer;
  int32_t vgId;
  SArray *pTrigColRefs;  // SArray<SSTriggerTableColRef>
  SArray *pCalcColRefs;  // SArray<SSTriggerTableColRef>
  SArray *tbGids;        // SArray<int64_t>
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
  bool        forceWinOpen;
  char       *pWinOpenNotify;
  char       *pWinCloseNotify;
} SSTriggerNotifyWindow;

typedef TRINGBUF(SSTriggerWindow) TriggerWindowBuf;

/*
 * State-window deferred/pending NULL tracking fields shared by both
 * realtime and history group structs.  Extracted so that helpers like
 * stResolveDeferredOnCut() can accept a single pointer instead of 11+
 * individual arguments.
 */
typedef struct SStateDeferredState {
  SArray *pStateVals;          /* SArray<SValue>, state column values */
  bool   *stateKeyDefined;    /* per-column defined flags */
  int64_t pendingNullStart;
  int32_t numPendingNull;
  int32_t numDeferredPartialNull;
  int32_t numDeferredTailAllNull;
  TSKEY   firstDeferredPartialNullTs;
  TSKEY   lastDeferredPartialNullTs;
  SArray *pPendingStateVals;   /* shadow state for deferred partial-NULL rows */
  bool   *pendingColTouched;   /* per-column: non-NULL seen in pending row */
  bool    hasPendingPartialNull;
} SStateDeferredState;

typedef struct SSTriggerNestedPeerSource {
  int64_t tableUid;
  int64_t readerTaskId;
  int32_t sourceIndex;
  int32_t subSourceIndex;

  const SSDataBlock        *pSliceBlock;
  SSDataBlock              *pOutputBlock;  // borrowed alias to pVtableMerger output
  SSTriggerNewVtableMerger *pVtableMerger;
  SArray                   *pTrigColRefs;  // owned SArray<SSTriggerTableColRef>
  int32_t                   rowIndex;
  int32_t                   endRowIndex;
  int32_t                   eventTsIndex;
  int32_t                   errorCode;

  SSTriggerVirTablePseudoColRequest request;
  bool                              needsInput;
  bool                              inFlight;
  bool                              dataBound;
  bool                              eof;
  bool                              failed;
} SSTriggerNestedPeerSource;

typedef struct SSTriggerNestedInputRetry {
  SStreamNestedInputRoute           route;
  SSTriggerVirTablePseudoColRequest request;
} SSTriggerNestedInputRetry;

typedef struct SSTriggerRealtimeGroup {
  struct SSTriggerRealtimeContext *pContext;
  SWindowChainState               *pWindowChain;
  SStreamTriggerPeerMerger        *pPeerMerger;
  const SWindowChainPeerGroup     *pNestedPeerGroup;           // borrowed until explicitly released before merger reset
  SArray                          *pNestedPeerSources;         // SArray<SSTriggerNestedPeerSource>
  SList                            nestedInputRetries;         // SList<SSTriggerNestedInputRetry>
  SList                            pendingNestedEvents;        // SList<SStreamNestedPendingCalcEvent>
  SList                            pendingNestedParWinEvents;  // SList<SStreamNestedPendingCalcEvent>
  SSHashObj                       *pNestedCacheScopes;         // SSHashObj<scope key, SStreamCacheScope>
  uint64_t                         nestedInputGeneration;
  bool                             nestedInputRegistered;
  bool                             nestedInputFailed;
  bool                             nestedInputCancelled;
  bool                             nestedPeerGroupPinned;
  int64_t                          gid;
  int32_t                          vgId;
  int32_t                          activeVtableCount;
  bool                             recalcNextWindow;
  bool                             repairCountDisorder;
  STimeWindow                      countDisorderRange;
  // For EXT/federated groups only (vgId == 0): the taskId of the SSTriggerExtProgress
  // that owns this group, so group-col-value pulls can be routed to the right reader.
  int64_t                          extReaderTaskId;
  TD_DLIST_NODE(SSTriggerRealtimeGroup);

  SSHashObj *pWalMetas;  // SSHashObj<vgId, SObjList<SSTriggerMetaData>>
  SObjList   tableUids;  // SObjList<{uid, vgId}>, tables having data to check
  int64_t    oldThreshold;
  int64_t    newThreshold;

  union {
    SStateDeferredState ds;  /* for state window trigger */
    struct {  // for event window trigger with sub-event
      SSTriggerNotifyWindow parentWindow;
      char                 *pFirstSubWinOpenNotify;
      int32_t               numSubWindows;
      int32_t               conditionIdx;
      // streak state (start/end, always scalar — sub-event + start/end is rejected at parse time)
      int32_t startCondCount;
      TSKEY   startCondFirstTs;
      int32_t endCondCount;
      TSKEY   endCondFirstTs;
    };
    int64_t totalCount;  // for count window trigger
  };
  STimeWindow prevWindow;                // the last closed window
  SObjList    windows;                   // SObjList<SSTriggerWindow>, windows not yet closed
  SObjList    pPendingParWinCalcParams;  // SObjList<SSTriggerCalcParam>
  SObjList    pPendingCalcParams;        // SObjList<SSTriggerCalcParam>
  int64_t     prevParentWinStart;        // for event window trigger with parent windows
  bool        pendingWinOpen;            // for event window trigger and state window trigger
  char       *pPendWinOpenNotify;        // for event window trigger and state window trigger

  int64_t  nextExecTime;  // used for max delay and batch window mode
  HeapNode heapNode;      // used for max delay and batch window mode
  int64_t  nestedCalcDeadline;
  int64_t  nestedPendingParamCount;
  bool     nestedCalcIndexed;
  HeapNode nestedCalcHeapNode;

  // Idle trigger fields
  int64_t lastRecvTimeMono;                        // last receive time (monotonic clock ms)
  int64_t lastRecvTimeWall;                        // last receive time (wall clock ns)
  uint8_t idleState;                               // 0=ACTIVE, 1=IDLE
  TD_DLIST_NODE(SSTriggerRealtimeGroup) idleNode;  // for groupsToCheckIdle list
} SSTriggerRealtimeGroup;

typedef struct SSTriggerHistoryGroup {
  struct SSTriggerHistoryContext *pContext;
  int64_t                         gid;
  TD_DLIST_NODE(SSTriggerHistoryGroup);

  SArray    *pVirtTableInfos;  // SArray<SSTriggerVirtTableInfo *>
  SSHashObj *pTableMetas;      // SSHashObj<tbUid, SSTriggerTableMeta>

  int64_t finishTs;

  TriggerWindowBuf winBuf;
  union {
    STimeWindow nextWindow;  // for sliding/period trigger
    SStateDeferredState ds;  /* for state window trigger */
    struct {  // for event window trigger with sub-event
      SSTriggerNotifyWindow parentWindow;
      int32_t               numSubWindows;
      int32_t               conditionIdx;
    };
  };

  SObjList           pPendingParWinCalcParams;  // SObjList<SSTriggerCalcParam>
  SObjList           pPendingCalcParams;        // SObjList<SSTriggerCalcParam>
  SWindowChainState        *pWindowChain;
  SStreamTriggerPeerMerger *pPeerMerger;
  SArray                   *pNestedPeerSources;
  SList                     pendingNestedEvents;
  SList                     pendingNestedParWinEvents;
  SSHashObj                *pNestedCacheScopes;        // SSHashObj<scope key, SStreamCacheScope>
  int64_t            prevParentWinStart;        // for event window trigger with parent windows
  bool               pendingWinOpen;            // for event window trigger and state window trigger
  SSTriggerCalcParam pendingWinParam;           // for event window trigger and state window trigger
  HeapNode           heapNode;
  bool               inMaxDelayHeap;
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
  // nodelay GROUP_COL_VALUE owns pullReq until its logical request finishes.
  bool                      groupColValuePullInFlight;
  int64_t                   groupColValuePullGid;
  SArray                   *reqCids;         // SArray<col_id_t>
  SArray                   *reqCols;         // SArray<OTableInfo>
  SArray                   *reqUids;         // SArray<int64_t>
  SSHashObj                *uidInfoTrigger;  // SSHashObj<uid, SSHashObj<slotId, colId>>
  SSHashObj                *uidInfoCalc;     // SSHashObj<uid, SSHashObj<slotId, colId>>
  SArray                   *pVersions;       // SArray<int64_t>
  SSDataBlock              *pTrigBlock;
  SSDataBlock              *pCalcBlock;
} SSTriggerWalProgress;

typedef struct SStreamReaderProgressSnapshot {
  int64_t taskId;
  int32_t nodeId;
  int64_t startVer;
  int64_t savedVer;
  int64_t doneVer;
  int64_t lastScanVer;
  int64_t verTime;
  int64_t lastInvalidVerTime;
  bool    externalSource;
} SStreamReaderProgressSnapshot;

typedef enum EStreamTriggerDebugGauge {
  STREAM_TRIGGER_GAUGE_REALTIME_SESSION = 1ULL << 0,
  STREAM_TRIGGER_GAUGE_HISTORY_SESSION = 1ULL << 1,
  STREAM_TRIGGER_GAUGE_ACTIVE_RECALC = 1ULL << 2,
  STREAM_TRIGGER_GAUGE_HISTORY_PROGRESS = 1ULL << 3,
  STREAM_TRIGGER_GAUGE_PENDING_PULL_RETRY = 1ULL << 4,
  STREAM_TRIGGER_GAUGE_PENDING_CALC_RETRY = 1ULL << 5,
  STREAM_TRIGGER_GAUGE_PENDING_RECALC_REQUEST = 1ULL << 6,
  STREAM_TRIGGER_GAUGE_LAST_CHECKPOINT = 1ULL << 7,
  STREAM_TRIGGER_GAUGE_CHECKPOINT_LOADED = 1ULL << 8,
  STREAM_TRIGGER_GAUGE_RECOVERING = 1ULL << 9,
  STREAM_TRIGGER_GAUGE_REALTIME_GROUP = 1ULL << 10,
  STREAM_TRIGGER_GAUGE_HISTORY_GROUP = 1ULL << 11,
  STREAM_TRIGGER_GAUGE_PENDING_NOTIFY = 1ULL << 12,
  STREAM_TRIGGER_GAUGE_META_POOL = 1ULL << 13,
  STREAM_TRIGGER_GAUGE_TABLE_UID_POOL = 1ULL << 14,
  STREAM_TRIGGER_GAUGE_WINDOW_POOL = 1ULL << 15,
  STREAM_TRIGGER_GAUGE_CALC_PARAM_POOL = 1ULL << 16,
} EStreamTriggerDebugGauge;

typedef struct SStreamTriggerDebugGauges {
  int64_t  realtimeSessionCount;
  int64_t  historySessionCount;
  int64_t  activeRecalcCount;
  int64_t  pendingPullRetryCount;
  int64_t  pendingCalcRetryCount;
  int64_t  pendingRecalcRequestCount;
  int64_t  lastCheckpointAtMs;
  bool     checkpointLoaded;
  bool     recovering;
  int64_t  realtimeGroupCount;
  int64_t  historyGroupCount;
  int64_t  pendingNotifyCount;
  int64_t  metaPoolUsed;
  int64_t  metaPoolCapacity;
  int64_t  metaPoolBytes;
  int64_t  tableUidPoolUsed;
  int64_t  tableUidPoolCapacity;
  int64_t  tableUidPoolBytes;
  int64_t  windowPoolUsed;
  int64_t  windowPoolCapacity;
  int64_t  windowPoolBytes;
  int64_t  calcParamPoolUsed;
  int64_t  calcParamPoolCapacity;
  int64_t  calcParamPoolBytes;
  int32_t  historyProgressPct;
  uint64_t validMask;
} SStreamTriggerDebugGauges;

typedef struct SStreamTriggerCommonDebugGauges {
  int64_t  activeRecalcCount;
  int64_t  pendingRecalcRequestCount;
  int32_t  historyProgressPct;
  uint64_t validMask;
} SStreamTriggerCommonDebugGauges;

typedef struct SStreamTriggerRealtimeDebugGauges {
  bool     present;
  int64_t  pendingPullRetryCount;
  int64_t  pendingCalcRetryCount;
  int64_t  lastCheckpointAtMs;
  bool     checkpointLoaded;
  bool     recovering;
  int64_t  realtimeGroupCount;
  int64_t  pendingNotifyCount;
  int64_t  metaPoolUsed;
  int64_t  metaPoolCapacity;
  int64_t  metaPoolBytes;
  int64_t  tableUidPoolUsed;
  int64_t  tableUidPoolCapacity;
  int64_t  tableUidPoolBytes;
  int64_t  windowPoolUsed;
  int64_t  windowPoolCapacity;
  int64_t  windowPoolBytes;
  int64_t  calcParamPoolUsed;
  int64_t  calcParamPoolCapacity;
  int64_t  calcParamPoolBytes;
  uint64_t validMask;
} SStreamTriggerRealtimeDebugGauges;

typedef struct SStreamTriggerHistoryDebugGauges {
  bool     present;
  int64_t  pendingPullRetryCount;
  int64_t  pendingCalcRetryCount;
  int64_t  historyGroupCount;
  int64_t  pendingNotifyCount;
  int64_t  calcParamPoolUsed;
  int64_t  calcParamPoolCapacity;
  int64_t  calcParamPoolBytes;
  uint64_t validMask;
} SStreamTriggerHistoryDebugGauges;

/* Per (trigger task, EXT reader task) progress maintained by the trigger
 * driver. Sibling of SSTriggerWalProgress; lives in SSTriggerRealtimeContext
 * .pReaderExtProgress as SSHashObj<int64_t taskId, SSTriggerExtProgress*>.
 * P0 only defines the struct; producer/consumer is added in P3.
 * See DS §6.2.2 for full field semantics. */
typedef struct SSTriggerExtProgress {
  SStreamTaskAddr *pTaskAddr;            // EXT reader task address
  SSHashObj       *triggerSideUidMaxTs;  // SSHashObj<int64_t uid, int64_t maxTs>;
                                         // trigger-side watermark map; sent on
                                         // every PULL so reader never stores it.
  SSHashObj       *pUidWindow;           // SSHashObj<int64_t uid, SExtUidWindow>;
                                         // uid -> {skey,ekey} collected from the latest
                                         // META_EXT response; sent with DATA_EXT pull;
                                         // owned by this struct, freed after DATA pull is sent.
  SSHashObj       *pCalcUidWindow;       // SSHashObj<int64_t uid, SExtUidWindow>;
                                         // accumulated uid -> {skey,ekey} across all
                                         // DATA_EXT rounds that arrived while CALC_DATA_EXT
                                         // was deferred by maxDelay; each new DATA_EXT round
                                         // merges its pUidWindow here (skey=min, ekey=max)
                                         // instead of replacing.  Freed after CALC_DATA_EXT
                                         // response.
  SSHashObj       *pUidToGid;            // SSHashObj<int64_t uid, int64_t groupId>;
                                         // populated from META_COL_GROUP_ID in every
                                         // META_EXT/META_DATA_EXT response so that trigger
                                         // groups can be keyed by the reader's PARTITION BY
                                         // groupId (which may merge several uids) instead of
                                         // by raw uid.  Persists for the lifetime of this
                                         // progress (not cleared per-request like pUidWindow).
  SSDataBlock     *pTrigDataBlock;       // owned data block returned by DATA_EXT/META_DATA_EXT;
                                         // borrowed by pSlices during trigger-side checks;
                                         // freed in stExtProgressDestroy and on next trigger
                                         // data response.
  SSDataBlock     *pCalcDataBlock;       // owned data block returned by CALC_DATA_EXT;
                                         // borrowed by pSlices during %%trows calc;
                                         // freed in stExtProgressDestroy and on next CALC_DATA_EXT.
  int64_t          lastTsNs;             // driver-internal heuristic only;
                                         // NOT persisted, NOT in heartbeat,
                                         // undefined for InfluxDB multi-uid.
  void            *pullReq;              // in-flight pull request handle, or NULL
  ESTriggerPullType pullType;            // last in-flight pull type; used when
                                         // reader returns NO_DATA without payload.
  struct SStreamTriggerTask *pOwnerTask; // back ref for async PULL callback
  int32_t          sessionId;            // back ref for stTriggerTaskAddWaitSession
  int64_t          pendingGroupColValGid; // gid of the in-flight GROUP_COL_VALUE_EXT pull, if pullType == STRIGGER_PULL_GROUP_COL_VALUE_EXT
} SSTriggerExtProgress;

// (gid, pProgress) for nodelay create-table; each gid must be pulled from its owning reader
typedef struct {
  int64_t               gid;
  SSTriggerWalProgress *pProgress;
  int32_t               attemptCount;  // 1-based, incremented on each NEED_RETRY for logging
} SSTriggerPendingCreateTableEntry;

typedef enum ESTriggerWalMode {
  STRIGGER_WAL_META_ONLY,
  STRIGGER_WAL_META_WITH_DATA,
  STRIGGER_WAL_META_THEN_DATA,
} ESTriggerWalMode;

typedef struct SSTriggerVTablePatchItem {
  int64_t         vtbUid;
  int64_t         ver;
  int32_t         vgId;
  ETableBlockType op;
} SSTriggerVTablePatchItem;

typedef struct SSTriggerVTablePatchContext {
  SSHashObj *pPatchItems;          // SSHashObj<vtbUid, SSTriggerVTablePatchItem>
  SArray    *pVirTableInfoRsp;     // SArray<VTableInfo>
  SSHashObj *pOrigTableCols;       // SSHashObj<dbname, SSHashObj<tbname, SSTriggerOrigColumnInfo>*>
} SSTriggerVTablePatchContext;

struct SSTriggerRealtimeContext {
  struct SStreamTriggerTask *pTask;
  SSTriggerNestedRecovery   *pRecovery;
  EStreamTriggerType         triggerType;
  bool                       nestedWindowPlan;
  bool                       suppressOutput;
  int64_t                    sessionId;
  uint64_t                   nextNestedInputGeneration;
  ESTriggerWalMode           walMode;
  ESTriggerContextStatus     status;

  SSHashObj                  *pReaderWalProgress;  // SSHashObj<vgId, SSTriggerWalProgress>
  SSHashObj                  *pReaderExtProgress;  // SSHashObj<int64_t taskId, SSTriggerExtProgress*>; NULL until P3 producer is introduced (DS §6.1.3)
  int32_t                     curReaderIdx;
  bool                        catchUp;  // whether all readers have caught up the latest wal data
  bool                        continueToFetch;
  SSTriggerVTablePatchContext patchContext;

  SSDataBlock *pMetaBlock;
  SSDataBlock *pDeleteBlock;
  SSDataBlock *pTableBlock;
  SArray      *pTempSlices;  // SSArray<{gid, uid, startIdx, endIdx}>
  SSHashObj   *pRanges;      // SSHashObj<gid, STimeWindow>

  SSHashObj *pGroups;  // SSHashObj<gid, SSTriggerRealtimeGroup*>
  TD_DLIST(SSTriggerRealtimeGroup) groupsToCheck;
  TD_DLIST(SSTriggerRealtimeGroup) groupsToCheckIdle;  // groups to check for idle timeout
  Heap                   *pMaxDelayHeap;
  Heap                   *pNestedCalcHeap;
  int64_t                 nestedPendingParamCount;
  SSTriggerRealtimeGroup *pMinGroup;
  SArray                 *groupsToDelete;
  SSHashObj              *pGroupColVals;  // SSHashObj<gid, SArray<SStreamGroupValue>*>
  SSHashObj              *pRollupTbCount;      // SSHashObj<gid, int32_t>
  SSHashObj              *pRollupTbCountSeen;  // SSHashObj<{vgId, gid}, int32_t>

  // these fields need to be cleared each round
  bool       needCheckAgain;
  SSHashObj *pSlices;  // SSHashObj<uid, SSTriggerDataSlice>
  SObjList   dumpTableUids;  // SObjList<{uid, vgId}>, backup ids for repeated check in one round
  // these fields are shared by all groups and need to reset for each group
  bool                         needPseudoCols;
  bool                         needMergeWindow;
  SSTriggerNewTimestampSorter *pSorter;
  SSTriggerNewVtableMerger    *pMerger;
  SArray                      *pParentWindows;  // SArray<SSTriggerNotifyWindow>, valid parent windows in this round
  SArray                      *pWindows;        // SArray<SSTriggerNotifyWindow>, valid windows in this round
  SArray                      *pNotifyParams;   // SArray<SSTriggerCalcParam>
  STimeWindow                  calcRange;
  SSTriggerCalcParam          *pCurParam;
  int64_t                      curParamRows;
  int64_t                      lastSentWinEnd;
  SObjList                     pAllCalcTableUids;  // SObjList<{uid, vgId}>
  SObjList                     pCalcTableUids;     // SObjList<{uid, vgId}>
  SSTriggerNewTimestampSorter *pCalcSorter;
  SSTriggerNewVtableMerger    *pCalcMerger;
  void                        *pNestedCalcBatch;  // SStagedNestedCalcBatch, owned while %%trows pull is in flight
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

  // LAST_TS create-table: need groupInfo before send create-table req; pull GROUP_COL_VALUE first
  SArray *pPendingCreateTableGids;  // SArray<SSTriggerPendingCreateTableEntry>, (gid, pProgress) per reader
};

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

  int64_t              gid;
  STimeWindow          scanRange;
  STimeWindow          calcRange;
  STimeWindow          stepRange;
  bool                 isHistory;
  bool                 needTsdbMeta;
  bool                 finishCheck;
  bool                 pendingToFinish;
  SArray              *pContributors;  // SArray<SStreamRecalcContributor>
  struct SSTriggerRecalcRequest *pManualRecalcRequest;
  uint64_t             progressStepId;
  uint64_t             nextProgressRequestToken;
  SStreamProgressRange historyProgressStepRange;
  bool                 historyProgressTriggerDone;
  bool                 historyProgressReaderCompleting;
  TD_DLIST(SStagedNestedLeafEffects) pendingNestedLeafEffects;

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
  SSHashObj             *pRollupTbCount;      // SSHashObj<gid, int32_t>
  SSHashObj             *pRollupTbCountSeen;  // SSHashObj<{vgId, gid}, int32_t>

  // these fields are shared by all groups and do not need to be destroyed
  bool                    reenterCheck;
  int32_t                 tbIter;
  SSTriggerVirtTableInfo *pCurVirTable;   // only for virtual tables
  SSTriggerTableMeta     *pCurTableMeta;  // only for non-virtual tables
  SSTriggerMetaData      *pMetaToFetch;
  SSTriggerTableColRef   *pColRefToFetch;
  SSTriggerCalcParam     *pParamToFetch;
  STimeWindow             paramCalcRange;
  SSTriggerCalcRequest   *pCalcReq;
  int64_t                 lastSentWinEnd;
  // these fields are shared by all groups and need to be destroyed
  SSTriggerTimestampSorter *pSorter;
  SSTriggerVtableMerger    *pMerger;
  SArray                   *pSavedWindows;  // for sliding trigger and session window trigger
  SArray                   *pInitWindows;   // for sliding trigger and session window trigger
  SArray                   *pNotifyParams;  // SArray<SSTriggerCalcParam>
  SObjPool                  calcParamPool;  // SObjPool<SSTriggerCalcParam>

  void     *pCalcDataCache;
  SHashObj *pCalcDataCacheIters;

  SList retryPullReqs;  // SList<SSTriggerPullRequest*>
  SList retryCalcReqs;  // SList<SSTriggerCalcRequest*>

} SSTriggerHistoryContext;

typedef enum ESTriggerEventType {
  STRIGGER_EVENT_WINDOW_NONE = 0,
  STRIGGER_EVENT_WINDOW_CLOSE = 1 << 0,
  STRIGGER_EVENT_WINDOW_OPEN = 1 << 1,
  STRIGGER_EVENT_IDLE = 1 << 2,
  STRIGGER_EVENT_RESUME = 1 << 3,
  STRIGGER_EVENT_ON_TIME = 1 << 15,
} ESTriggerEventType;

typedef struct {
  int32_t vgId;
  int64_t version;
} SStreamNestedReaderVersion;

typedef struct {
  SSTriggerCalcParam calcParam;
  int8_t             contextPolicy;
  SLeafInstanceId    leafIdentity;
  SArray            *pSnapshots;
  STimeWindow        calcDataRange;
  SArray            *pReaderVersions;  // SArray<SStreamNestedReaderVersion>
} SStreamNestedPendingCalcEvent;

int32_t stAllocNestedPendingCalcNode(const SStreamNestedPendingCalcEvent *pSrc, SListNode **ppNode);
void    stDestroyNestedPendingCalcNode(SListNode **ppNode);

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
  SArray     *pContributors;  // SArray<SStreamRecalcContributor>
  bool        isHistory;
  SRecalcImpactDomain        impactDomain;
  bool                       frozen;
  bool                       noMerge;
  bool                       retryScheduled;
  SStreamRecalcAttemptState *pPreparedAttempt;
  SStreamRecalcAttemptRef    attempt;
  SListNode                 *pRetryWaitNode;
  TD_DLIST_NODE(SSTriggerRecalcRequest);
} SSTriggerRecalcRequest;

typedef TD_DLIST(SSTriggerRecalcRequest) SSTriggerRecalcReqList;

typedef struct SSTriggerGroupPendingRecalc {
  int64_t                progressTs;
  SSTriggerRecalcReqList pendingRequests;
} SSTriggerGroupPendingRecalc;

typedef struct SStreamTriggerTask {
  SStreamTask                       task;
  SStreamTaskStats                 *pStats;
  SRWLatch                          readerProgressLock;
  SArray                           *pReaderProgressSnapshots;
  SRWLatch                          debugGaugesLock;
  SStreamTriggerCommonDebugGauges   commonDebugGauges;
  SStreamTriggerRealtimeDebugGauges realtimeDebugGauges;
  SStreamTriggerHistoryDebugGauges  historyDebugGauges;

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
      SArray      *pStateSlotIds;  // SArray<int16_t>
      int64_t      stateExtend;
      SNodeList   *pStateZeroths;
      STrueForInfo stateTrueForInfo;
      SNodeList   *pStateExprs;
    };
    struct {  // for event window
      SNode       *pStartCond;
      SNode       *pEndCond;
      SNodeList   *pStartCondCols;
      SNodeList   *pEndCondCols;
      STrueForInfo eventTrueForInfo;
      STrueForInfo startTrueForInfo;  // start condition consecutive-streak limit
      STrueForInfo endTrueForInfo;    // end condition consecutive-streak limit
    };
  };
  int32_t trigTsIndex;
  int32_t calcTsIndex;
  int32_t trigPkIndex;
  int32_t calcPkIndex;
  int64_t maxDelayNs;
  int64_t fillHistoryStartTime;
  int64_t watermark;
  int64_t expiredTime;
  int64_t idleTimeoutMs;  // idle timeout in milliseconds (0 = disabled)
  bool    ignoreDisorder;
  bool               countStepForcesOrderedInput;
  bool    fillHistory;
  bool    fillHistoryFirst;
  bool    lowLatencyCalc;
  bool    hasPartitionBy;
  bool    isRollup;
  bool    isVirtualTable;
  bool    isSuperTable;
  bool    stbPartByTbname;
  bool    ignoreNoDataTrigger;
  bool    hasTriggerFilter;
  int8_t  precision;
  int64_t historyStep;
  bool    multiGroupBatch;
  int64_t placeHolderBitmap;
  SNode  *triggerFilter;
  SStreamWindowPlan *pWindowPlan;
  SArray            *pNestedInputProjections;
  bool               flushOnOuterClose;
  // trigger options: old version, to be removed
  int32_t    histTrigTsIndex;
  int32_t    histCalcTsIndex;
  int32_t    histTrigPkIndex;
  int32_t    histCalcPkIndex;
  int64_t    histStateSlotId;
  SArray     *pHistStateSlotIds;  // SArray<int16_t>
  SNode     *histTriggerFilter;
  SNode     *histStateExpr;
  SNodeList *histStateExprs;
  SNode     *histStartCond;
  SNode     *histEndCond;
  SNodeList *histStartCondCols;
  SNodeList *histEndCondCols;
  // notify options
  ESTriggerEventType calcEventType;
  ESTriggerEventType notifyEventType;
  SArray            *pNotifyAddrUrls;
  int32_t            addOptions;
  bool               notifyHistory;
  int8_t             nodelayCreateSubtable;  // 1 = sub-tables created at stream create; 0 = create on the fly

  // task info
  int32_t leaderSnodeId;
  SArray *readerList;  // SArray<SStreamTaskAddr>
  SArray *runnerList;  // SArray<SStreamRunnerTarget>

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
  bool         virScanTsOnly;  // whether the trigger and calc data only contains timestamp column
  SSDataBlock *pNestedCalcCacheBlock;
  SArray      *pNestedCalcCacheProjection;  // SArray<SStreamDataCacheColumnProjection>
  SRWLatch     virTableInfoLock;
  SSHashObj   *pVirtTableInfos;  // SSHashObj<vtbUid, SSTriggerVirtTableInfo>
  SSHashObj   *pOrigTableInfos;  // SSHashObj<otbUid, SSTriggerOrigTableInfo>
  // virtual table info response
  bool       virTableInfoReady;
  SArray    *pVirTableInfoRsp;  // SArray<VTableInfo>
  SSHashObj *pOrigTableCols;    // SSHashObj<dbname, SSHashObj<tbname, SSTriggerOrigColumnInfo>*>

  // boundary between realtime and history
  SSHashObj           *pHistoryCutoffTime;
  SStreamProgressRange historyOriginalRange;
  bool                 historyOriginalRangeValid;

  // calc request pool
  SRWLatch   calcPoolLock;
  SRWLatch   calcDataCacheIterLock;
  SArray    *pCalcNodes;       // SArray<SSTriggerCalcNode>
  SSHashObj *pGroupRunning;    // SSHashObj<gid, bool[]>
  SSHashObj *pSessionRunning;  // SSHashObj<sessionId, cnt>

  SRWLatch   recalcRequestLock;
  SList     *pRecalcRequests;       // SList<SSTriggerRecalcRequest>
  SSHashObj *pRecalcRequestMap;     // SSHashObj<gid, SSTriggerRecalcRequestList>
  SSHashObj *pGroupPendingRecalcs;  // SSHashObj<gid, SSTriggerGroupPendingRecalc>
  SSTriggerRecalcReqList backoffRecalcRequests;

  SRWLatch userRecalcRequestLock;
  SArray  *pUserRecalcRequests;        // SArray<SStreamRecalcReq>
  SArray  *pUserRecalcConversionGids;  // SArray<int64_t>, frozen groups for the head raw request
  int32_t  userRecalcConversionIndex;
  SStreamRecalcTracker *pRecalcTracker;

  // runtime status
  volatile int8_t           isCheckpointReady;
  volatile int8_t           historyFinished;
  volatile int8_t           realtimeStarted;
  volatile int32_t          checkpointVersion;
  volatile int64_t          mgmtReqId;
  volatile int64_t          waitingMgmtReqId;
  volatile int64_t          latestVersionTime;
  bool                      historyCalcStarted;
  bool                      pendingPeriodicCheckpoint;
  char                     *streamName;
  uint64_t                  nestedRecoveryGeneration;
  int32_t                   nestedRecoveryError;
  SSTriggerNestedRecovery  *pNestedRecovery;
  SSTriggerNestedRecovery  *pRetiredNestedRecoveries;
  SSTriggerRealtimeBundle  *pRealtimeBundle;
  SSTriggerRealtimeContext *pRealtimeContext;
  SSTriggerHistoryContext  *pHistoryContext;
} SStreamTriggerTask;

// interfaces called by stream trigger thread
int32_t stTriggerTaskAcquireRequest(SStreamTriggerTask *pTask, int64_t sessionId, int64_t gid,
                                    SSTriggerCalcRequest **ppRequest);
int32_t stTriggerTaskReleaseRequest(SStreamTriggerTask *pTask, SSTriggerCalcRequest **ppRequest, bool completed);
int32_t stTriggerTaskGetRunningReq(SStreamTriggerTask *pTask, int64_t sessionId, int64_t *pNumRunningReq);
int32_t stTriggerTaskCheckCreate(SStreamTriggerTask *pTask, SSTriggerCalcRequest *pRequest, int64_t sessionId,
                                 int64_t gid);

int32_t stTriggerTaskAddRecalcRequest(SStreamTriggerTask *pTask, SSTriggerRealtimeGroup *pGroup,
                                      STimeWindow *pCalcRange, bool isHistory, bool isUserRecalc, bool isDetermined,
                                      int64_t recalcId);
int32_t stTriggerTaskTryAddNextWindowRecalc(SStreamTriggerTask *pTask, SSTriggerRealtimeGroup *pGroup,
                                            const STimeWindow *pRecalcRange);
int32_t stTriggerTaskReadyRecalcRequest(SStreamTriggerTask *pTask, SSTriggerRealtimeGroup *pGroup);
int32_t stTriggerTaskFetchRecalcRequest(SStreamTriggerTask *pTask, SSTriggerRecalcRequest **ppReq);
void    stTriggerTaskDestroyRecalcRequest(SSTriggerRecalcRequest **ppReq);

// interfaces called by stream mgmt thread
int32_t stTriggerTaskDeploy(SStreamTriggerTask *pTask, SStreamTriggerDeployMsg *pMsg);
int32_t stTriggerTaskUndeploy(SStreamTriggerTask **ppTask, bool force);
int32_t stTriggerTaskExecute(SStreamTriggerTask *pTask, const SStreamMsg *pMsg);
int32_t stTriggerTaskCopyReaderProgress(SStreamTriggerTask *pTask, SArray **ppSnapshots);
int32_t stTriggerTaskRefreshRealtimeLag(SStreamTriggerTask *pTask, int64_t nowMs);
int32_t stTriggerTaskGetMetrics(SStreamTriggerTask *pTask, SStreamTaskMetricsSnapshot *pSnapshot);
/** A NULL snapshot drains terminal recalculation events only. */
int32_t stTriggerTaskLogStats(SStreamTriggerTask *pTask, const SStreamTaskPeriodSnapshot *pSnapshot);
void    stTriggerTaskRefreshCommonDebugGauges(SStreamTriggerTask *pTask);
void    stTriggerTaskPublishRealtimeDebugGauges(SStreamTriggerTask *pTask, SSTriggerRealtimeContext *pContext);
void    stTriggerTaskUpdateRealtimeDebugGauges(SStreamTriggerTask *pTask, SSTriggerRealtimeContext *pContext);
void    stTriggerTaskPublishHistoryDebugGauges(SStreamTriggerTask *pTask, SSTriggerHistoryContext *pContext);
void    stTriggerTaskUpdateHistoryDebugGauges(SStreamTriggerTask *pTask, SSTriggerHistoryContext *pContext);
void    stTriggerTaskClearRealtimeDebugGauges(SStreamTriggerTask *pTask);
void    stTriggerTaskClearHistoryDebugGauges(SStreamTriggerTask *pTask);

uint64_t           streamTaskMetricMask(const SStreamTask *pTask);
SStreamTaskStats **streamTaskGetStatsSlot(SStreamTask *pTask);
SStreamTaskStats  *streamTaskGetStats(SStreamTask *pTask);
int32_t            streamTaskStatsInit(SStreamTask *pTask, SStreamTaskStats **ppStats);
int64_t            streamTaskGetMonotonicUs(void);

// helper function in trigger task
// check whether the state data equals to the zeroth state
int32_t     stIsStateEqualZeroth(void *pStateData, void *pZeroth, bool *pIsEqual);
STimeWindow stTriggerTaskGetTimeWindow(SStreamTriggerTask *pTask, int64_t ts);
void        stTriggerTaskPrevTimeWindow(SStreamTriggerTask *pTask, STimeWindow *pWindow);
void        stTriggerTaskNextTimeWindow(SStreamTriggerTask *pTask, STimeWindow *pWindow);
SStreamProgressRange stTriggerTaskProgressRangeFromClosed(STimeWindow range);

#ifdef __cplusplus
}
#endif

#endif /* ifndef TDENGINE_TSTREAM_H */
