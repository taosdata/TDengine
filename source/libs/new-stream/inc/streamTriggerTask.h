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
#include "filter.h"
#include "stream.h"
#include "tcommon.h"
#include "tlosertree.h"
#include "tmsgcb.h"
#include "tringbuf.h"
#include "tsimplehash.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum EStreamTriggerType {
  STREAM_TRIGGER_PERIOD = 0,
  STREAM_TRIGGER_SLIDING,
  STREAM_TRIGGER_SESSION,
  STREAM_TRIGGER_COUNT,
  STREAM_TRIGGER_STATE,
  STREAM_TRIGGER_EVENT,
} EStreamTriggerType;

typedef enum ESTriggerEventType {
  STRIGGER_EVENT_WINDOW_NONE = 0,
  STRIGGER_EVENT_WINDOW_OPEN = 1 << 0,
  STRIGGER_EVENT_WINDOW_CLOSE = 1 << 1,
} ESTriggerEventType;

typedef enum ESTriggerGroupStatus {
  STRIGGER_GROUP_WAITING_META = 0,
  STRIGGER_GROUP_WAITING_CALC,
  STRIGGER_GROUP_WAITING_TDATA,
} ESTriggerGroupStatus;

typedef enum ESTriggerWindowStatus {
  STRIGGER_WINDOW_INITIALIZED = 0,
  STRIGGER_WINDOW_OPENED,
  STRIGGER_WINDOW_CLOSED,
} ESTriggerWindowStatus;

typedef enum ESTriggerCalcStatus {
  STRIGGER_CALC_IDLE = 0,
  STRIGGER_CALC_TO_RUN,
  STRIGGER_CALC_RUNNING,
} ESTriggerCalcStatus;

typedef TRINGBUF(int64_t) TWstartBuf;

/// structure definitions for trigger real-time calculation

typedef struct SSTriggerWalMetaStat {
  int64_t readerTaskId;
  int64_t numHoldMetas;
  int64_t threshold;
} SSTriggerWalMetaStat;

typedef struct SSTriggerWalMeta {
  int64_t uid;
  int64_t skey;
  int64_t ekey;
  int64_t ver;
  int64_t nrows;
} SSTriggerWalMeta;

typedef struct SSTriggerRealtimeGroup {
  struct SSTriggerRealtimeContext *pContext;
  int64_t                          groupId;
  ESTriggerGroupStatus             status;
  union {
    SSTriggerWalMetaStat *pMetaStat;   // for single table per group
    SSHashObj            *pMetaStats;  // for multiple tables per group
  };
  int32_t maxMetaDelta;      // vnodes lagging behind the fastest vnode by this threshold will be ignored
  int32_t minMetaThreshold;  // minimum number of metas to do window check
  int64_t oldThreshold;
  int64_t newThreshold;

  SArray               *pMetas;
  int32_t               metaIdx;
  STimeWindow           curWindow;
  ESTriggerWindowStatus winStatus;
  int32_t               nrowsInWindow;  // not work for sliding/session window
  union {
    TWstartBuf wstartBuf;  // for count window
    SValue     stateVal;   // for state window
  };
} SSTriggerRealtimeGroup;

typedef struct SSTriggerWalMetaNode {
  SSTriggerWalMeta            *pMeta;
  struct SSTriggerWalMetaNode *next;
} SSTriggerWalMetaNode;

typedef struct SSTriggerWalMetaList {
  SSTriggerWalMetaNode *head;
  SSTriggerWalMetaNode *tail;

  SSTriggerWalMetaNode *nextSessNode;
  STimeWindow           curSessWin;

  int64_t      nextTs;
  int64_t      nextIdx;
  SSDataBlock *pDataBlock;
} SSTriggerWalMetaList;

typedef struct SSTriggerWalMetaMerger {
  struct SSTriggerRealtimeContext *pContext;
  SArray                          *pMetaNodeBuf;
  SArray                          *pMetaLists;

  SMultiwayMergeTreeInfo *pSessMerger;
  STimeWindow             sessRange;

  SMultiwayMergeTreeInfo *pDataMerger;
  STimeWindow             dataReadRange;
  STimeWindow             stepReadRange;
} SSTriggerWalMetaMerger;

typedef struct SSTriggerWalProgress {
  SStreamTaskAddr *pTaskAddr;     // reader task address
  int64_t          histVerBound;  // boundary version between historical and real-time calculations
  int64_t          lastScanVer;   // version of the last committed record in previous scan
  int64_t          latestVer;     // latest version of committed records in the vnode WAL
} SSTriggerWalProgress;

typedef struct SSTriggerRealtimeContext {
  struct SStreamTriggerTask *pTask;
  int64_t                    sessionId;
  SSHashObj                 *pReaderWalProgress;
  int32_t                    curReaderIdx;
  SSDataBlock               *pWalMetaData;  // wal meta pull response
  union {
    SSTriggerPullRequest           base;
    SSTriggerWalMetaRequest        walMetaReq;
    SSTriggerWalTsDataRequest      walTsReq;
    SSTriggerWalTriggerDataRequest walTriggerDataReq;
    SSTriggerWalCalcDataRequest    walCalcDataReq;
  } pullReq;

  SSHashObj              *pGroups;
  SSTriggerRealtimeGroup *pCurGroup;
  SSTriggerWalMetaMerger *pMerger;

  ESTriggerCalcStatus     calcStatus;
  SSTriggerCalcRequest    calcReq;
  SSTriggerRealtimeGroup *pCalcGroup;
} SSTriggerRealtimeContext;

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
      int64_t stateColId;
      int64_t stateTrueFor;
    };
    struct {  // for event window
      SFilterInfo *pStartCond;
      SFilterInfo *pEndCond;
      int64_t      eventTrueFor;
    };
  };
  int64_t maxDelay;  // precision is ms
  int64_t fillHistoryStartTime;
  int64_t watermark;
  int64_t expiredTime;
  int64_t primaryTsIndex;
  bool    ignoreDisorder;
  bool    fillHistory;
  bool    fillHistoryFirst;
  bool    lowLatencyCalc;
  // notify options
  ESTriggerEventType calcEventType;
  ESTriggerEventType notifyEventType;
  SArray            *pNotifyAddrUrls;
  int32_t            notifyErrorHandle;
  bool               notifyHistory;
  // reader and runner info
  SArray *readerList;  // SArray<SStreamTaskAddr>
  SArray *runnerList;  // SArray<SStreamRunnerTarget>
  // extra info
  bool singleTableGroup;
  bool needRowNumber;
  bool needCacheData;
  bool needWend;

  // runtime info
  // default: 10, todo(kjq): adjust dynamically
  int32_t                   calcParamLimit;  // max number of params in each calculation request
  SSTriggerRealtimeContext *pRealtimeCtx;
} SStreamTriggerTask;

// interfaces called by stream trigger thread
int32_t stTriggerTaskProcessPullRsp(SStreamTriggerTask *pTask, SSTriggerPullResponse *pRsp);
int32_t stTriggerTaskProcessCalcRsp(SStreamTriggerTask *pTask, SSTriggerCalcResponse *pRsp);
int32_t stTriggerTaskMarkRecalc(SStreamTriggerTask *pTask, int64_t groupId, int64_t skey, int64_t ekey);

// interfaces called by stream mgmt thread
int32_t stTriggerTaskDeploy(SStreamTriggerTask *pTask, const SStreamTriggerDeployMsg *pMsg);
int32_t stTriggerTaskUndeploy(SStreamTriggerTask *pTask, const SStreamUndeployTaskMsg *pMsg);
int32_t stTriggerTaskExecute(SStreamTriggerTask *pTask, const SStreamMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /* ifndef TDENGINE_TSTREAM_H */
