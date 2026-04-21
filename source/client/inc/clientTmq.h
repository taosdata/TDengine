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

#ifndef TDENGINE_CLIENT_TMQ_H
#define TDENGINE_CLIENT_TMQ_H

#include "clientInt.h"
#include "clientLog.h"
#include "tarray.h"
#include "tdef.h"
#include "tqueue.h"
#include "tref.h"
#include "ttimer.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================
// log macros
// ============================================================
#define tqErrorC(...) do { if (cDebugFlag & DEBUG_ERROR || tqClientDebugFlag & DEBUG_ERROR) { taosPrintLog("TQ  ERROR ", DEBUG_ERROR, tqClientDebugFlag|cDebugFlag, __VA_ARGS__); }} while(0)
#define tqInfoC(...)  do { if (cDebugFlag & DEBUG_INFO  || tqClientDebugFlag & DEBUG_INFO)  { taosPrintLog("TQ  INFO  ", DEBUG_INFO,  tqClientDebugFlag|cDebugFlag, __VA_ARGS__); }} while(0)
#define tqDebugC(...) do { if (cDebugFlag & DEBUG_DEBUG || tqClientDebugFlag & DEBUG_DEBUG) { taosPrintLog("TQ  DEBUG ", DEBUG_DEBUG, tqClientDebugFlag|cDebugFlag, __VA_ARGS__); }} while(0)
#define tqTraceC(...) do { if (cDebugFlag & DEBUG_TRACE || tqClientDebugFlag & DEBUG_TRACE) { taosPrintLog("TQ  TRACE ", DEBUG_TRACE, tqClientDebugFlag|cDebugFlag, __VA_ARGS__); }} while(0)
#define tqWarnC(...)  do { if (cDebugFlag & DEBUG_WARN  || tqClientDebugFlag & DEBUG_WARN)  { taosPrintLog("TQ  WARN  ", DEBUG_WARN,  tqClientDebugFlag|cDebugFlag, __VA_ARGS__); }} while(0)

// ============================================================
// constants
// ============================================================
#define EMPTY_BLOCK_POLL_IDLE_DURATION 10
#define DEFAULT_AUTO_COMMIT_INTERVAL   5000
#define DEFAULT_HEARTBEAT_INTERVAL     3000
#define DEFAULT_ASKEP_INTERVAL         1000
#define DEFAULT_COMMIT_CNT             1
#define SUBSCRIBE_RETRY_MAX_COUNT      240
#define SUBSCRIBE_RETRY_INTERVAL       500

#define SET_ERROR_MSG_TMQ(MSG) \
  if (errstr != NULL && errstrLen > 0) (void)snprintf(errstr, errstrLen, "%s", MSG);

#define PROCESS_POLL_RSP(FUNC,DATA) \
  SDecoder decoder = {0}; \
  tDecoderInit(&decoder, POINTER_SHIFT(pRspWrapper->pollRsp.data, sizeof(SMqRspHead)), pRspWrapper->pollRsp.len - sizeof(SMqRspHead)); \
  if (FUNC(&decoder, DATA) < 0) { \
    tDecoderClear(&decoder); \
    code = terrno; \
    goto END;\
  }\
  tDecoderClear(&decoder);\
  (void)memcpy(DATA, pRspWrapper->pollRsp.data, sizeof(SMqRspHead));

#define DELETE_POLL_RSP(FUNC,DATA) \
  SMqPollRspWrapper* pRsp = &rspWrapper->pollRsp;\
  taosMemoryFreeClear(pRsp->pEpset);             \
  taosMemoryFreeClear(pRsp->data);               \
  FUNC(DATA);

// ============================================================
// opaque struct definitions (originally in clientTmq.c)
// ============================================================
struct tmq_list_t {
  SArray container;
};

struct tmq_conf_t {
  char           clientId[TSDB_CLIENT_ID_LEN];
  char           groupId[TSDB_CGROUP_LEN];
  int8_t         autoCommit;
  int8_t         enableWalMarker;
  int8_t         resetOffset;
  int8_t         withTbName;
  int8_t         snapEnable;
  int8_t         replayEnable;
  int8_t         sourceExcluded;  // do not consume, bit
  int8_t         rawData;         // fetch raw data
  int32_t        maxPollWaitTime;
  int32_t        minPollRows;
  uint16_t       port;
  int32_t        autoCommitInterval;
  int32_t        sessionTimeoutMs;
  int32_t        heartBeatIntervalMs;
  int32_t        maxPollIntervalMs;
  char*          ip;
  char*          user;
  char*          pass;
  tmq_commit_cb* commitCb;
  void*          commitCbUserParam;
  int8_t         enableBatchMeta;
  char*          token;
};

struct tmq_t {
  int64_t        refId;
  char           groupId[TSDB_CGROUP_LEN];
  char           clientId[TSDB_CLIENT_ID_LEN];
  char           user[TSDB_USER_LEN];
  char           fqdn[TSDB_FQDN_LEN];
  int8_t         withTbName;
  int8_t         useSnapshot;
  int8_t         autoCommit;
  int8_t         enableWalMarker;
  int32_t        autoCommitInterval;
  int32_t        sessionTimeoutMs;
  int32_t        heartBeatIntervalMs;
  int32_t        maxPollIntervalMs;
  int8_t         resetOffsetCfg;
  int8_t         replayEnable;
  int8_t         sourceExcluded;  // do not consume, bit
  int8_t         rawData;         // fetch raw data
  int32_t        maxPollWaitTime;
  int32_t        minPollRows;
  int64_t        consumerId;
  tmq_commit_cb* commitCb;
  void*          commitCbUserParam;
  int8_t         enableBatchMeta;

  // status
  SRWLatch lock;
  int8_t   status;
  int32_t  epoch;
  // poll info
  int64_t pollCnt;
  int64_t totalRows;
  int8_t  pollFlag;

  // timer
  tmr_h       hbLiveTimer;
  tmr_h       epTimer;
  tmr_h       commitTimer;
  STscObj*    pTscObj;       // connection
  SArray*     clientTopics;  // SArray<SMqClientTopic>
  STaosQueue* mqueue;        // queue of rsp
  STaosQueue* delayedTask;  // delayed task queue for heartbeat and auto commit
  tsem2_t     rspSem;

  // token
  int32_t tokenCode;
};

// ============================================================
// enums
// ============================================================
enum {
  TMQ_VG_STATUS__IDLE = 0,
  TMQ_VG_STATUS__WAIT,
};

enum {
  TMQ_CONSUMER_STATUS__INIT = 0,
  TMQ_CONSUMER_STATUS__READY,
  TMQ_CONSUMER_STATUS__CLOSED,
};

enum {
  TMQ_DELAYED_TASK__ASK_EP = 1,
  TMQ_DELAYED_TASK__COMMIT,
};

// ============================================================
// types
// ============================================================
typedef struct {
  tmr_h               timer;
  int32_t             rsetId;
  TdThreadMutex       lock;
} SMqMgmt;

typedef struct {
  int32_t code;
  tsem2_t sem;
} SAskEpInfo;

typedef struct {
  STqOffsetVal committedOffset;
  STqOffsetVal endOffset;    // the last version in TAOS_RES + 1
  STqOffsetVal beginOffset;  // the first version in TAOS_RES
  int64_t      walVerBegin;
  int64_t      walVerEnd;
} SVgOffsetInfo;

typedef struct {
  int64_t       pollCnt;
  int64_t       numOfRows;
  SVgOffsetInfo offsetInfo;
  int32_t       vgId;
  int32_t       vgStatus;
  int32_t       vgSkipCnt;            // here used to mark the slow vgroups
  int64_t       emptyBlockReceiveTs;  // once empty block is received, idle for ignoreCnt then start to poll data
  int64_t       blockReceiveTs;       // once empty block is received, idle for ignoreCnt then start to poll data
  int64_t       blockSleepForReplay;  // once empty block is received, idle for ignoreCnt then start to poll data
  bool          seekUpdated;          // offset is updated by seek operator, therefore, not update by vnode rsp.
  SEpSet        epSet;
} SMqClientVg;

typedef struct {
  char           topicName[TSDB_TOPIC_FNAME_LEN];
  char           db[TSDB_DB_FNAME_LEN];
  SArray*        vgs;  // SArray<SMqClientVg>
  int8_t         noPrivilege;
} SMqClientTopic;

typedef struct {
  int32_t         vgId;
  char            topicName[TSDB_TOPIC_FNAME_LEN];
  SMqClientTopic* topicHandle;
  uint64_t        reqId;
  SEpSet*         pEpset;
  void*           data;
  uint32_t        len;
  union {
    struct{
      SMqRspHead   head;
      STqOffsetVal rspOffset;
    };
    SMqDataRsp      dataRsp;
    SMqMetaRsp      metaRsp;
    SMqBatchMetaRsp batchMetaRsp;
  };
} SMqPollRspWrapper;

typedef struct {
  int32_t code;
  int8_t  tmqRspType;
  int32_t epoch;
  union{
    SMqPollRspWrapper pollRsp;
    SMqAskEpRsp       epRsp;
  };
} SMqRspWrapper;

typedef struct {
  tsem2_t rspSem;
  int32_t rspErr;
} SMqSubscribeCbParam;

typedef struct {
  int64_t refId;
  bool    sync;
  void*   pParam;
} SMqAskEpCbParam;

typedef struct {
  int64_t  refId;
  char     topicName[TSDB_TOPIC_FNAME_LEN];
  int32_t  vgId;
  uint64_t requestId;  // request id for debug purpose
} SMqPollCbParam;

typedef struct {
  tsem2_t       rsp;
  int32_t       numOfRsp;
  SArray*       pList;
  TdThreadMutex mutex;
  int64_t       consumerId;
  char*         pTopicName;
  int32_t       code;
} SMqVgCommon;

typedef struct {
  tsem2_t sem;
  int32_t code;
} SMqSeekParam;

typedef struct {
  tsem2_t     sem;
  int32_t     code;
  SMqVgOffset vgOffset;
} SMqCommittedParam;

typedef struct {
  int32_t      vgId;
  int32_t      epoch;
  int32_t      totalReq;
  SMqVgCommon* pCommon;
} SMqVgWalInfoParam;

typedef struct {
  int64_t        refId;
  int32_t        epoch;
  int32_t        waitingRspNum;
  int32_t        code;
  tmq_commit_cb* callbackFn;
  void*          userParam;
} SMqCommitCbParamSet;

typedef struct {
  SMqCommitCbParamSet* params;
  char                 topicName[TSDB_TOPIC_FNAME_LEN];
  int32_t              vgId;
  int64_t              consumerId;
} SMqCommitCbParam;

typedef struct {
  tsem2_t sem;
  int32_t code;
} SSyncCommitInfo;

typedef struct {
  STqOffsetVal currentOffset;
  STqOffsetVal commitOffset;
  STqOffsetVal seekOffset;
  int64_t      numOfRows;
  int32_t      vgStatus;
  int64_t      walVerBegin;
  int64_t      walVerEnd;
} SVgroupSaveInfo;

// ============================================================
// global variables (defined in clientTmq.c)
// ============================================================
extern TdThreadOnce   tmqInit;
extern volatile int32_t tmqInitRes;
extern SMqMgmt        tmqMgmt;

// ============================================================
// lock helpers
// ============================================================
static inline void tmqRlock(tmq_t* tmq){
  tqTraceC("tmqRlock: %p %" PRIx64 ", tmq lock: %p", tmq, tmq->consumerId, &tmq->lock);
  taosRLockLatch(&tmq->lock);
}

static inline void tmqRUnlock(tmq_t* tmq){
  tqTraceC("tmqRUnlock: %p %" PRIx64 ", tmq lock: %p", tmq, tmq->consumerId, &tmq->lock);
  taosRUnLockLatch(&tmq->lock);
}

static inline void tmqWlock(tmq_t* tmq){
  tqTraceC("tmqWlock: %p %" PRIx64 ", tmq lock: %p", tmq, tmq->consumerId, &tmq->lock);
  taosWLockLatch(&tmq->lock);
}

static inline void tmqWUnlock(tmq_t* tmq){
  tqTraceC("tmqWUnlock: %p %" PRIx64 ", tmq lock: %p", tmq, tmq->consumerId, &tmq->lock);
  taosWUnLockLatch(&tmq->lock);
}

// ============================================================
// helper to build topic full name (replaces repeated snprintf pattern)
// ============================================================
static inline void buildTopicFullName(tmq_t* tmq, const char* pTopicName, char* tname) {
  (void)snprintf(tname, TSDB_TOPIC_FNAME_LEN, "%d.%s", tmq->pTscObj->acctId, pTopicName);
}

// ============================================================
// shared internal functions (implemented in clientTmq.c)
// ============================================================
int32_t         getTopicByName(tmq_t* tmq, const char* pTopicName, SMqClientTopic** topic);
int32_t         getClientVg(tmq_t* tmq, char* pTopicName, int32_t vgId, SMqClientVg** pVg);
void            getVgInfo(tmq_t* tmq, char* topicName, int32_t vgId, SMqClientVg** pVg);
SMqClientTopic* getTopicInfo(tmq_t* tmq, char* topicName);
void            freeClientVg(void* param);
void            freeClientTopic(void* param);
void            tmqFreeRspWrapper(SMqRspWrapper* rspWrapper);
void            tmqFreeImpl(void* handle);
void            tmqClearUnhandleMsg(tmq_t* tmq);
void            tmqMgmtInit(void);
int32_t         checkWalRange(SVgOffsetInfo* offset, int64_t value);
bool            isInSnapshotMode(int8_t type, bool useSnapshot);

// commit functions (implemented in clientTmqCommit.c)
int32_t         tmqCommitDone(SMqCommitCbParamSet* pParamSet);
int32_t         commitRspCountDown(SMqCommitCbParamSet* pParamSet, int64_t consumerId, const char* pTopic, int32_t vgId);
int32_t         doSendCommitMsg(tmq_t* tmq, int32_t vgId, SEpSet* epSet, STqOffsetVal* offset, const char* pTopicName,
                                SMqCommitCbParamSet* pParamSet);
int32_t         prepareCommitCbParamSet(tmq_t* tmq, tmq_commit_cb* pCommitFp, void* userParam, int32_t rspNum,
                                        SMqCommitCbParamSet** ppParamSet);
int32_t         innerCommit(tmq_t* tmq, char* pTopicName, STqOffsetVal* offsetVal, SMqClientVg* pVg, SMqCommitCbParamSet* pParamSet);
int32_t         asyncCommitOffset(tmq_t* tmq, char* pTopicName, int32_t vgId, STqOffsetVal* offsetVal,
                                  tmq_commit_cb* pCommitFp, void* userParam);
void            asyncCommitFromResult(tmq_t* tmq, const TAOS_RES* pRes, tmq_commit_cb* pCommitFp, void* userParam);
void            asyncCommitAllOffsets(tmq_t* tmq, tmq_commit_cb* pCommitFp, void* userParam);
void            asyncSendWalMarkMsgToMnode(tmq_t* tmq, int32_t vgId, int64_t keepVersion);
void            commitCallBackFn(tmq_t* tmq, int32_t code, void* param);

// offset functions (implemented in clientTmqOffset.c)
int32_t         getCommittedFromServer(tmq_t* tmq, char* tname, int32_t vgId, SEpSet* epSet, int64_t* committed);
void            destroyCommonInfo(SMqVgCommon* pCommon);

// poll functions (shared)
void            tmqBuildConsumeReqImpl(SMqPollReq* pReq, tmq_t* tmq, SMqClientTopic* pTopic, SMqClientVg* pVg);

// askEp functions (shared)
int32_t         askEp(tmq_t* pTmq, void* param, bool sync, bool updateEpSet);
int32_t         syncAskEp(tmq_t* pTmq);

// timer callbacks
void            tmqAssignAskEpTask(void* param, void* tmrId);
void            tmqReplayTask(void* param, void* tmrId);
void            tmqAssignDelayedCommitTask(void* param, void* tmrId);
void            tmqSendHbReq(void* param, void* tmrId);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_CLIENT_TMQ_H
