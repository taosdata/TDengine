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

#ifndef TDENGINE_STREAMMSG_H
#define TDENGINE_STREAMMSG_H

#include "tarray.h"
#include "tmsg.h"

#ifdef __cplusplus
extern "C" {
#endif


typedef enum EStreamTriggerType {
  STREAM_TRIGGER_PERIOD = 0,
  STREAM_TRIGGER_SLIDING,  // sliding is 1 , can not change, because used in doOpenExternalWindow
  STREAM_TRIGGER_SESSION,
  STREAM_TRIGGER_COUNT,
  STREAM_TRIGGER_STATE,
  STREAM_TRIGGER_EVENT,
} EStreamTriggerType;

typedef struct SStreamRetrieveReq SStreamRetrieveReq;
typedef struct SStreamDispatchReq SStreamDispatchReq;
typedef struct STokenBucket       STokenBucket;

#define COPY_STR(_p) ((_p) ? (taosStrdup(_p)) : NULL)

typedef struct SNodeUpdateInfo {
  int32_t nodeId;
  SEpSet  prevEp;
  SEpSet  newEp;
} SNodeUpdateInfo;

typedef struct SStreamUpstreamEpInfo {
  int32_t nodeId;
  int32_t childId;
  int32_t taskId;
  SEpSet  epSet;
  bool    dataAllowed;  // denote if the data from this upstream task is allowed to put into inputQ, not serialize it
  int64_t stage;  // upstream task stage value, to denote if the upstream node has restart/replica changed/transfer
  int64_t lastMsgId;
} SStreamUpstreamEpInfo;

int32_t tEncodeStreamEpInfo(SEncoder* pEncoder, const SStreamUpstreamEpInfo* pInfo);
int32_t tDecodeStreamEpInfo(SDecoder* pDecoder, SStreamUpstreamEpInfo* pInfo);

typedef struct SStreamTaskNodeUpdateMsg {
  int32_t transId;  // to identify the msg
  int64_t streamId;
  int32_t taskId;
  SArray* pNodeList;  // SArray<SNodeUpdateInfo>
  SArray* pTaskList;  // SArray<int32_t>, taskId list
} SStreamTaskNodeUpdateMsg;

int32_t tEncodeStreamTaskUpdateMsg(SEncoder* pEncoder, const SStreamTaskNodeUpdateMsg* pMsg);
int32_t tDecodeStreamTaskUpdateMsg(SDecoder* pDecoder, SStreamTaskNodeUpdateMsg* pMsg);
void    tDestroyNodeUpdateMsg(SStreamTaskNodeUpdateMsg* pMsg);

typedef struct {
  int64_t reqId;
  int64_t stage;
  int64_t streamId;
  int32_t upstreamNodeId;
  int32_t upstreamTaskId;
  int32_t downstreamNodeId;
  int32_t downstreamTaskId;
  int32_t childId;
} SStreamTaskCheckReq;

int32_t tEncodeStreamTaskCheckReq(SEncoder* pEncoder, const SStreamTaskCheckReq* pReq);
int32_t tDecodeStreamTaskCheckReq(SDecoder* pDecoder, SStreamTaskCheckReq* pReq);

typedef struct {
  int64_t reqId;
  int64_t streamId;
  int32_t upstreamNodeId;
  int32_t upstreamTaskId;
  int32_t downstreamNodeId;
  int32_t downstreamTaskId;
  int32_t childId;
  int64_t oldStage;
  int8_t  status;
} SStreamTaskCheckRsp;

int32_t tEncodeStreamTaskCheckRsp(SEncoder* pEncoder, const SStreamTaskCheckRsp* pRsp);
int32_t tDecodeStreamTaskCheckRsp(SDecoder* pDecoder, SStreamTaskCheckRsp* pRsp);

struct SStreamDispatchReq {
  int32_t type;
  int64_t stage;  // nodeId from upstream task
  int64_t streamId;
  int32_t taskId;
  int32_t msgId;  // msg id to identify if the incoming msg from the same sender
  int32_t srcVgId;
  int32_t upstreamTaskId;
  int32_t upstreamChildId;
  int32_t upstreamNodeId;
  int32_t upstreamRelTaskId;
  int32_t blockNum;
  int64_t totalLen;
  SArray* dataLen;  // SArray<int32_t>
  SArray* data;     // SArray<SRetrieveTableRsp*>
};

int32_t tEncodeStreamDispatchReq(SEncoder* pEncoder, const struct SStreamDispatchReq* pReq);
int32_t tDecodeStreamDispatchReq(SDecoder* pDecoder, struct SStreamDispatchReq* pReq);
void    tCleanupStreamDispatchReq(struct SStreamDispatchReq* pReq);

struct SStreamRetrieveReq {
  int64_t            streamId;
  int64_t            reqId;
  int32_t            srcTaskId;
  int32_t            srcNodeId;
  int32_t            dstTaskId;
  int32_t            dstNodeId;
  int32_t            retrieveLen;
  SRetrieveTableRsp* pRetrieve;
};

int32_t tEncodeStreamRetrieveReq(SEncoder* pEncoder, const struct SStreamRetrieveReq* pReq);
int32_t tDecodeStreamRetrieveReq(SDecoder* pDecoder, struct SStreamRetrieveReq* pReq);
void    tCleanupStreamRetrieveReq(struct SStreamRetrieveReq* pReq);

#define BIT_FLAG_MASK(n)               (1 << n)
#define BIT_FLAG_SET_MASK(val, mask)   ((val) |= (mask))
#define BIT_FLAG_UNSET_MASK(val, mask) ((val) &= ~(mask))
#define BIT_FLAG_TEST_MASK(val, mask)  (((val) & (mask)) != 0)

#define PLACE_HOLDER_NONE             0
#define PLACE_HOLDER_PREV_TS          BIT_FLAG_MASK(0)
#define PLACE_HOLDER_CURRENT_TS       BIT_FLAG_MASK(1)
#define PLACE_HOLDER_NEXT_TS          BIT_FLAG_MASK(2)
#define PLACE_HOLDER_WSTART           BIT_FLAG_MASK(3)
#define PLACE_HOLDER_WEND             BIT_FLAG_MASK(4)
#define PLACE_HOLDER_WDURATION        BIT_FLAG_MASK(5)
#define PLACE_HOLDER_WROWNUM          BIT_FLAG_MASK(6)
#define PLACE_HOLDER_PREV_LOCAL       BIT_FLAG_MASK(7)
#define PLACE_HOLDER_NEXT_LOCAL       BIT_FLAG_MASK(8)
#define PLACE_HOLDER_LOCALTIME        BIT_FLAG_MASK(9)
#define PLACE_HOLDER_PARTITION_IDX    BIT_FLAG_MASK(10)
#define PLACE_HOLDER_PARTITION_TBNAME BIT_FLAG_MASK(11)
#define PLACE_HOLDER_PARTITION_ROWS   BIT_FLAG_MASK(12)
#define PLACE_HOLDER_GRPID            BIT_FLAG_MASK(13)

#define CREATE_STREAM_FLAG_NONE                     0
#define CREATE_STREAM_FLAG_TRIGGER_VIRTUAL_STB      BIT_FLAG_MASK(0)

typedef enum EStreamPlaceholder {
  SP_NONE = 0,
  SP_CURRENT_TS = 1,
  SP_WSTART,
  SP_WEND,
  SP_WDURATION,
  SP_WROWNUM,
  SP_LOCALTIME,
  SP_PARTITION_IDX,
  SP_PARTITION_TBNAME,
  SP_PARTITION_ROWS
} EStreamPlaceholder;

typedef struct SStreamOutCol {
  void*     expr;
  SDataType type;
} SStreamOutCol;

void destroySStreamOutCols(void* p);
typedef struct SSessionTrigger {
  int16_t slotId;
  int64_t sessionVal;
} SSessionTrigger;

typedef struct SStateWinTrigger {
  int16_t slotId;
  int16_t extend;
  void*   zeroth;
  int64_t trueForDuration;
  void*   expr;
} SStateWinTrigger;

typedef struct SSlidingTrigger {
  int8_t  intervalUnit;
  int8_t  slidingUnit;
  int8_t  offsetUnit;
  int8_t  soffsetUnit;
  int8_t  precision;
  int64_t interval;
  int64_t offset;
  int64_t sliding;
  int64_t soffset;
  int8_t  overlap;
} SSlidingTrigger;

typedef struct SEventTrigger {
  void*   startCond;
  void*   endCond;
  int64_t trueForDuration;
} SEventTrigger;

typedef struct SCountTrigger {
  int64_t countVal;
  int64_t sliding;
  void*   condCols;
} SCountTrigger;

typedef struct SPeriodTrigger {
  char    periodUnit;
  char    offsetUnit;
  int8_t  precision;
  int64_t period;
  int64_t offset;
} SPeriodTrigger;

typedef union {
  SSessionTrigger  session;
  SStateWinTrigger stateWin;
  SSlidingTrigger  sliding;
  SEventTrigger    event;
  SCountTrigger    count;
  SPeriodTrigger   period;
} SStreamTrigger;

typedef struct {
  SArray* vgList;  // vgId, SArray<int32>
  int8_t  readFromCache;
  void*   scanPlan;
} SStreamCalcScan;

typedef struct {
  char*   name;         // full name
  int64_t streamId;
  char*   sql;

  char*   streamDB;    // db full name
  char*   triggerDB;   // db full name
  char*   outDB;       // db full name
  SArray* calcDB;      // char*, db full name

  char* triggerTblName;  // table name
  char* outTblName;      // table name

  int8_t igExists;
  int8_t triggerType;
  int8_t igDisorder;
  int8_t deleteReCalc;
  int8_t deleteOutTbl;
  int8_t fillHistory;
  int8_t fillHistoryFirst;
  int8_t calcNotifyOnly;
  int8_t lowLatencyCalc;
  int8_t igNoDataTrigger;

  // notify options
  SArray* pNotifyAddrUrls;
  int32_t notifyEventTypes;
  int32_t addOptions;
  int8_t  notifyHistory;

  void*          triggerFilterCols;     // nodelist of SColumnNode
  void*          triggerCols;           // nodelist of SColumnNode
  void*          partitionCols;         // nodelist of SColumnNode
  SArray*        outCols;               // array of SFieldWithOptions
  SArray*        outTags;               // array of SFieldWithOptions
  int64_t        maxDelay;              // precision is ms
  int64_t        fillHistoryStartTime;  // precision same with triggerDB, INT64_MIN for no value specified
  int64_t        watermark;             // precision same with triggerDB
  int64_t        expiredTime;           // precision same with triggerDB
  SStreamTrigger trigger;

  int8_t   triggerTblType;
  uint64_t triggerTblUid;  // suid or uid
  uint64_t triggerTblSuid;
  uint8_t  triggerPrec;
  int8_t   vtableCalc;     // virtual table calc exits
  int8_t   outTblType;
  int8_t   outStbExists;
  uint64_t outStbUid;
  int32_t  outStbSversion;
  int64_t  eventTypes;
  int64_t  flags;
  int64_t  tsmaId;
  int64_t  placeHolderBitmap;
  int16_t  calcTsSlotId; // only used when using %%trows
  int16_t  triTsSlotId; // only used when using %%trows

  // only for (virtual) child table and normal table
  int32_t triggerTblVgId;
  int32_t outTblVgId;

  // reader part
  void*   triggerScanPlan;   // block include all
                             // preFilter<>triggerPrevFilter/partitionCols<>subTblNameExpr+tagValueExpr/triggerCols<>triggerCond/calcRows
  SArray* calcScanPlanList;  // for calc action, SArray<SStreamCalcScan>

  // trigger part
  int8_t  triggerHasPF;       // Since some filter will be processed in trigger's reader, triggerPrevFilter will be NULL.
                              // Use this flag to mark whether trigger has preFilter.
  void*   triggerPrevFilter;  // filter for trigger table

  // runner part
  int32_t numOfCalcSubplan;
  void*   calcPlan;        // for calc action
  void*   subTblNameExpr;
  void*   tagValueExpr;
  SArray* forceOutCols;  // array of SStreamOutCol, only available when forceOutput is true
} SCMCreateStreamReq;

typedef enum SStreamMsgType {
  STREAM_MSG_START,
  STREAM_MSG_UNDEPLOY,
  STREAM_MSG_ORIGTBL_READER_INFO,
  STREAM_MSG_UPDATE_RUNNER,
  STREAM_MSG_USER_RECALC,
  STREAM_MSG_RUNNER_ORIGTBL_READER,
} SStreamMsgType;

typedef struct SStreamMsg {
  SStreamMsgType msgType;
} SStreamMsg;

int32_t tEncodeSStreamMsg(SEncoder* pEncoder, const SStreamMsg* pMsg);
int32_t tDecodeSStreamMsg(SDecoder* pDecoder, SStreamMsg* pMsg);

typedef struct SStreamStartTaskMsg {
  SStreamMsg header;
} SStreamStartTaskMsg;

int32_t tEncodeSStreamStartTaskMsg(SEncoder* pEncoder, const SStreamStartTaskMsg* pMsg);
int32_t tDecodeSStreamStartTaskMsg(SDecoder* pDecoder, SStreamStartTaskMsg* pMsg);

typedef struct SStreamUndeployTaskMsg {
  SStreamMsg header;
  int8_t     doCheckpoint;
  int8_t     doCleanup;
} SStreamUndeployTaskMsg;

int32_t tEncodeSStreamUndeployTaskMsg(SEncoder* pEncoder, const SStreamUndeployTaskMsg* pMsg);
int32_t tDecodeSStreamUndeployTaskMsg(SDecoder* pDecoder, SStreamUndeployTaskMsg* pMsg);

void tFreeSStreamMgmtRsp(void* param);

typedef enum {
  STREAM_STATUS_UNDEPLOYED = 0,
  STREAM_STATUS_INIT = 1,
  STREAM_STATUS_RUNNING,
  STREAM_STATUS_STOPPED,
  STREAM_STATUS_FAILED,
  STREAM_STATUS_DROPPING,
} EStreamStatus;

static const char* gStreamStatusStr[] = {"Undeployed", "Idle", "Running", "Stopped", "Failed", "Dropping"};

typedef enum EStreamTaskType {
  STREAM_READER_TASK = 0,
  STREAM_TRIGGER_TASK,
  STREAM_RUNNER_TASK,
} EStreamTaskType;

static const char* gStreamTaskTypeStr[] = {"Reader", "Trigger", "Runner"};



typedef enum SStreamMgmtReqType {
  STREAM_MGMT_REQ_TRIGGER_ORIGTBL_READER = 0,
  STREAM_MGMT_REQ_RUNNER_ORIGTBL_READER
} SStreamMgmtReqType;

typedef struct SStreamDbTableName {
  char dbFName[TSDB_DB_FNAME_LEN];
  char tbName[TSDB_TABLE_NAME_LEN];
} SStreamDbTableName;

typedef struct SStreamOReaderDeployReq {
  int32_t execId;
  int64_t uid;
  SArray* vgIds;
} SStreamOReaderDeployReq;

typedef struct SStreamOReaderDeployRsp {
  int32_t execId;
  SArray* vgList;   // SArray<SStreamTaskAddr>
} SStreamOReaderDeployRsp;


typedef struct SStreamMgmtReqCont {
  SArray*            pReqs;  // for trigger SArray<SStreamDbTableName>, full table names of the original tables
                             // for runner  SArray<SStreamOReaderDeployReq>, original tables groups
} SStreamMgmtReqCont;

typedef struct SStreamMgmtReq {
  int64_t            reqId;
  SStreamMgmtReqType type;
  SStreamMgmtReqCont cont;
} SStreamMgmtReq;

void tFreeSStreamMgmtReq(SStreamMgmtReq* pReq);
int32_t tCloneSStreamMgmtReq(SStreamMgmtReq* pSrc, SStreamMgmtReq** ppDst);
void tFreeRunnerOReaderDeployReq(void* param);

typedef void (*taskUndeplyCallback)(void*);

typedef struct SStreamTask {
  EStreamTaskType type;

  /** KEEP TOGETHER **/
  int64_t streamId;  // ID of the stream
  int64_t taskId;    // ID of the current task
  int64_t seriousId;  // task deploy idx
  /** KEEP TOGETHER **/

  int64_t       flags;
  int32_t       deployId;   // runner task's deploy id
  int32_t       nodeId;     // ID of the vgroup/snode
  int64_t       sessionId;  // ID of the current session (real-time, historical, or recalculation)
  int32_t       taskIdx;

  EStreamStatus status;
  int32_t       detailStatus; // status index in pTriggerStatus
  int32_t       errorCode;

  SStreamMgmtReq* pMgmtReq;  // request that should be handled by stream mgmt thread

  // FOR LOCAL PART
  SRWLatch        mgmtReqLock;
  SRWLatch        entryLock;       

  SStreamUndeployTaskMsg undeployMsg;
  taskUndeplyCallback    undeployCb;
  
  int8_t          deployed;      // concurrent undeloy
} SStreamTask;

typedef struct SStreamMgmtRspCont {
  // FOR STREAM_MSG_ORIGTBL_READER_INFO
  SArray*    vgIds;       // SArray<int32_t>, same size and order as fullTableNames in SStreamMgmtReqCont
  SArray*    readerList;  // SArray<SStreamTaskAddr>, each SStreamTaskAddr has an unique nodeId

  // FOR STREAM_MSG_UPDATE_RUNNER
  SArray*    runnerList;  // SArray<SStreamRunnerTarget>, full runner list

  // FOR STREAM_MSG_USER_RECALC
  SArray*    recalcList;  // SArray<SStreamRecalcReq>

  // FOR STREAM_MSG_RUNNER_ORIGTBL_READER
  SArray*    execRspList;  // SArray<SStreamOReaderDeployRsp>
} SStreamMgmtRspCont;

typedef struct SStreamMgmtRsp {
  SStreamMsg         header;
  int64_t            reqId;
  int32_t            code;
  SStreamTask        task;
  SStreamMgmtRspCont cont;
} SStreamMgmtRsp;

typedef struct SStreamRecalcReq {
  int64_t recalcId;
  TSKEY   start;
  TSKEY   end;
} SStreamRecalcReq;

typedef struct SSTriggerRecalcProgress {
  int64_t recalcId;  // same with SStreamRecalcReq in stTriggerTaskExecute
  int32_t progress;  // 0-100, 0 means not started, 100 means finished
  TSKEY   start;
  TSKEY   end;
} SSTriggerRecalcProgress;

typedef struct SSTriggerRuntimeStatus {
  int32_t autoRecalcNum;
  int32_t realtimeSessionNum;
  int32_t historySessionNum;
  int32_t recalcSessionNum;
  int32_t histroyProgress; // 0-100, 0 means not started, 100 means finished
  SArray* userRecalcs;  // SArray<SSTriggerRecalcProgress>
} SSTriggerRuntimeStatus;


typedef SStreamTask SStmTaskStatusMsg;

typedef struct SStreamHbMsg {
  int32_t dnodeId;
  int32_t streamGId;
  int32_t snodeId;
  int32_t runnerThreadNum;
  SArray* pVgLeaders;     // SArray<int32_t>
  SArray* pStreamStatus;  // SArray<SStmTaskStatusMsg>
  SArray* pStreamReq;     // SArray<int32_t>, task index in pStreamStatus
  SArray* pTriggerStatus; // SArray<SSTriggerRuntimeStatus>
} SStreamHbMsg;

int32_t tEncodeStreamHbMsg(SEncoder* pEncoder, const SStreamHbMsg* pReq);
int32_t tDecodeStreamHbMsg(SDecoder* pDecoder, SStreamHbMsg* pReq);
void    tCleanupStreamHbMsg(SStreamHbMsg* pMsg, bool deepClean);

typedef struct {
  char*   triggerTblName;
  int64_t triggerTblUid;  // suid or uid
  int64_t triggerTblSuid;
  int8_t  triggerTblType;
  int8_t  deleteReCalc;
  int8_t  deleteOutTbl;
  void*   partitionCols;  // nodelist of SColumnNode
  void*   triggerCols;    // nodelist of SColumnNode
  // void*   triggerPrevFilter;
  void* triggerScanPlan;
  void* calcCacheScanPlan;
} SStreamReaderDeployFromTrigger;

typedef struct {
  int32_t execReplica;
  void*   calcScanPlan;
} SStreamReaderDeployFromCalc;

typedef union {
  SStreamReaderDeployFromTrigger trigger;
  SStreamReaderDeployFromCalc    calc;
} SStreamReaderDeploy;

typedef struct SStreamReaderDeployMsg {
  int8_t              triggerReader;
  SStreamReaderDeploy msg;
} SStreamReaderDeployMsg;

typedef struct SStreamTaskAddr {
  int64_t taskId;
  int32_t nodeId;
  SEpSet  epset;
} SStreamTaskAddr;

int32_t tDecodeSStreamTaskAddr(SDecoder* pDecoder, SStreamTaskAddr* pMsg);
int32_t tEncodeSStreamTaskAddr(SEncoder* pEncoder, const SStreamTaskAddr* pMsg);

typedef struct SStreamRunnerTarget {
  SStreamTaskAddr addr;
  int32_t         execReplica;
} SStreamRunnerTarget;

typedef struct SStreamSnodeInfo {
  int32_t leaderSnodeId;
  int32_t replicaSnodeId;
  SEpSet  leaderEpSet;
  SEpSet  replicaEpSet; // may be empty
} SStreamSnodeInfo;

typedef struct {
  int8_t triggerType;
  int8_t igDisorder;
  int8_t fillHistory;
  int8_t fillHistoryFirst;
  int8_t lowLatencyCalc;
  int8_t igNoDataTrigger;
  int8_t isTriggerTblVirt;
  int8_t triggerHasPF;
  int8_t isTriggerTblStb;
  int8_t precision;
  void*  partitionCols;

  // notify options
  SArray* pNotifyAddrUrls;
  int32_t notifyEventTypes;
  int32_t addOptions;
  int8_t  notifyHistory;

  int64_t        maxDelay;              // precision is ms
  int64_t        fillHistoryStartTime;  // precision same with triggerDB, INT64_MIN for no value specified
  int64_t        watermark;             // precision same with triggerDB
  int64_t        expiredTime;           // precision same with triggerDB
  SStreamTrigger trigger;

  int64_t eventTypes;
  int64_t placeHolderBitmap;
  int16_t calcTsSlotId;  // only used when using %%trows
  int16_t triTsSlotId;  // only used when using %%trows
  void*   triggerPrevFilter;
  void*   triggerScanPlan;    // only used for virtual tables
  void*   calcCacheScanPlan;  // only used for virtual tables

  SArray* readerList;  // SArray<SStreamTaskAddr>
  SArray* runnerList;  // SArray<SStreamRunnerTarget>

  int32_t leaderSnodeId;
  char*   streamName;  
} SStreamTriggerDeployMsg;

typedef struct SStreamRunnerDeployMsg {
  int32_t execReplica;

  char*  streamName;
  void*  pPlan;
  char*  outDBFName;
  char*  outTblName;
  int8_t outTblType;
  int8_t lowLatencyCalc;
  int8_t calcNotifyOnly;
  int8_t topPlan;

  // notify options
  SArray* pNotifyAddrUrls;
  int32_t addOptions;

  SArray*  outCols;  // array of SFieldWithOptions
  SArray*  outTags;  // array of SFieldWithOptions
  uint64_t outStbUid;
  int64_t  outStbSversion;

  void*   subTblNameExpr;
  void*   tagValueExpr;
  SArray* forceOutCols;  // array of SStreamOutCol, only available when forceOutput is true
} SStreamRunnerDeployMsg;

typedef union {
  SStreamReaderDeployMsg  reader;
  SStreamTriggerDeployMsg trigger;
  SStreamRunnerDeployMsg  runner;
} SStreamDeployTaskMsg;

typedef struct {
  SStreamTask          task;
  SStreamDeployTaskMsg msg;
} SStmTaskDeploy;

typedef struct {
  int64_t         streamId;
  SArray*         readerTasks;  // SArray<SStmTaskDeploy>
  SStmTaskDeploy* triggerTask;
  SArray*         runnerTasks;  // SArray<SStmTaskDeploy>
} SStmStreamDeploy;


void tFreeSStmStreamDeploy(void* param);
void tDeepFreeSStmStreamDeploy(void* param);

typedef struct {
  SArray* streamList;  // SArray<SStmStreamDeploy>
} SStreamDeployActions;

typedef struct {
  SStreamTask         task;
  SStreamStartTaskMsg startMsg;
} SStreamTaskStart;

typedef struct {
  SArray* taskList;  // SArray<SStreamTaskStart>
} SStreamStartActions;

typedef struct {
  SStreamTask            task;
  SStreamUndeployTaskMsg undeployMsg;
} SStreamTaskUndeploy;

typedef struct {
  int8_t  undeployAll;
  SArray* taskList;  // SArray<SStreamTaskUndeploy>
} SStreamUndeployActions;

typedef struct {
  SArray* rspList;   // SArray<SStreamMgmtRsp>
} SStreamMgmtRsps;

typedef struct {
  int32_t streamGid;
} SStreamMsgGrpHeader;

typedef struct {
  int32_t                streamGId;
  SStreamDeployActions   deploy;
  SStreamStartActions    start;
  SStreamUndeployActions undeploy;
  SStreamMgmtRsps        rsps;
} SMStreamHbRspMsg;


void tFreeSMStreamHbRspMsg(SMStreamHbRspMsg* pRsp);
void tDeepFreeSMStreamHbRspMsg(SMStreamHbRspMsg* pRsp);
int32_t tEncodeStreamHbRsp(SEncoder* pEncoder, const SMStreamHbRspMsg* pRsp);
int32_t tDecodeStreamHbRsp(SDecoder* pDecoder, SMStreamHbRspMsg* pRsp);

typedef struct {
  SMsgHead head;
  int64_t  streamId;
  int32_t  taskId;
  int32_t  reqType;
} SStreamTaskRunReq;

int32_t tEncodeStreamTaskRunReq(SEncoder* pEncoder, const SStreamTaskRunReq* pReq);
int32_t tDecodeStreamTaskRunReq(SDecoder* pDecoder, SStreamTaskRunReq* pReq);

typedef struct {
  SMsgHead head;
  int64_t  streamId;
} SStreamTaskStopReq;

int32_t tEncodeStreamTaskStopReq(SEncoder* pEncoder, const SStreamTaskStopReq* pReq);
int32_t tDecodeStreamTaskStopReq(SDecoder* pDecoder, SStreamTaskStopReq* pReq);

typedef struct SStreamProgressReq {
  int64_t streamId;
  int64_t taskId;
  int32_t fetchIdx;
} SStreamProgressReq;

int32_t tSerializeStreamProgressReq(void* buf, int32_t bufLen, const SStreamProgressReq* pReq);
int32_t tDeserializeStreamProgressReq(void* buf, int32_t bufLen, SStreamProgressReq* pReq);

typedef struct SStreamProgressRsp {
  int64_t streamId;
  bool    fillHisFinished;
  int64_t progressDelay;
  int32_t fetchIdx;
} SStreamProgressRsp;

int32_t tSerializeStreamProgressRsp(void* buf, int32_t bufLen, const SStreamProgressRsp* pRsp);
int32_t tDeserializeSStreamProgressRsp(void* buf, int32_t bufLen, SStreamProgressRsp* pRsp);

typedef struct {
  int64_t streamId;
} SCMCreateStreamRsp;

void tFreeStreamOutCol(void* pCol);
int32_t tSerializeSCMCreateStreamReq(void* buf, int32_t bufLen, const SCMCreateStreamReq* pReq);
int32_t tDeserializeSCMCreateStreamReq(void* buf, int32_t bufLen, SCMCreateStreamReq* pReq);
void    tFreeSCMCreateStreamReq(SCMCreateStreamReq* pReq);
int32_t tCloneStreamCreateDeployPointers(SCMCreateStreamReq *pSrc, SCMCreateStreamReq** ppDst);

int32_t tSerializeSCMCreateStreamReqImpl(SEncoder* pEncoder, const SCMCreateStreamReq* pReq);
int32_t tDeserializeSCMCreateStreamReqImpl(SDecoder* pDecoder, SCMCreateStreamReq* pReq);

typedef enum ESTriggerPullType {
  STRIGGER_PULL_SET_TABLE,
  STRIGGER_PULL_LAST_TS,
  STRIGGER_PULL_FIRST_TS,
  STRIGGER_PULL_TSDB_META,
  STRIGGER_PULL_TSDB_META_NEXT,
  STRIGGER_PULL_TSDB_TS_DATA,
  STRIGGER_PULL_TSDB_TRIGGER_DATA,
  STRIGGER_PULL_TSDB_TRIGGER_DATA_NEXT,
  STRIGGER_PULL_TSDB_CALC_DATA,
  STRIGGER_PULL_TSDB_CALC_DATA_NEXT,
  STRIGGER_PULL_TSDB_DATA, //10
  STRIGGER_PULL_TSDB_DATA_NEXT,
  STRIGGER_PULL_GROUP_COL_VALUE,
  STRIGGER_PULL_VTABLE_INFO,
  STRIGGER_PULL_VTABLE_PSEUDO_COL,
  STRIGGER_PULL_OTABLE_INFO,
  STRIGGER_PULL_WAL_META_NEW,
  STRIGGER_PULL_WAL_DATA_NEW,
  STRIGGER_PULL_WAL_META_DATA_NEW,
  STRIGGER_PULL_WAL_CALC_DATA_NEW,
  STRIGGER_PULL_TYPE_MAX,
} ESTriggerPullType;

typedef struct SSTriggerPullRequest {
  ESTriggerPullType type;
  int64_t           streamId;
  int64_t           readerTaskId;
  int64_t           sessionId;
  int64_t           triggerTaskId;  // does not serialize
} SSTriggerPullRequest;

typedef struct SSTriggerSetTableRequest {
  SSTriggerPullRequest base;
  SSHashObj*           uidInfoTrigger;    // < uid->SHashObj<slotId->colId> >
  SSHashObj*           uidInfoCalc;    // < uid->SHashObj<slotId->colId> >
} SSTriggerSetTableRequest;

typedef struct SSTriggerLastTsRequest {
  SSTriggerPullRequest base;
} SSTriggerLastTsRequest;

typedef struct SSTriggerFirstTsRequest {
  SSTriggerPullRequest base;
  int64_t              gid;  // optional, 0 by default
  int64_t              startTime;
  int64_t              ver;
} SSTriggerFirstTsRequest;

typedef struct SSTriggerTsdbMetaRequest {
  SSTriggerPullRequest base;
  int64_t              startTime;
  int64_t              endTime;
  int64_t              gid;    // optional, 0 by default
  int8_t               order;  // 1 for asc, 2 for desc
  int64_t              ver;
} SSTriggerTsdbMetaRequest;

typedef struct SSTriggerTsdbTsDataRequest {
  SSTriggerPullRequest base;
  int64_t              suid;
  int64_t              uid;
  int64_t              skey;
  int64_t              ekey;
  int64_t              ver;
} SSTriggerTsdbTsDataRequest;

typedef struct SSTriggerTsdbTriggerDataRequest {
  SSTriggerPullRequest base;
  int64_t              startTime;
  int64_t              gid;    // optional, 0 by default
  int8_t               order;  // 1 for asc, 2 for desc
  int64_t              ver;
} SSTriggerTsdbTriggerDataRequest;

typedef struct SSTriggerTsdbCalcDataRequest {
  SSTriggerPullRequest base;
  int64_t              gid;
  int64_t              skey;
  int64_t              ekey;
  int64_t              ver;
} SSTriggerTsdbCalcDataRequest;

typedef struct SSTriggerTsdbDataRequest {
  SSTriggerPullRequest base;
  int64_t              suid;
  int64_t              uid;
  int64_t              skey;
  int64_t              ekey;
  SArray*              cids;   // SArray<col_id_t>, col_id starts from 0
  int8_t               order;  // 1 for asc, 2 for desc
  int64_t              ver;
} SSTriggerTsdbDataRequest;

typedef struct SSTriggerWalMetaNewRequest {
  SSTriggerPullRequest base;
  int64_t              lastVer;
  int64_t              ctime;
} SSTriggerWalMetaNewRequest;

typedef struct SSTriggerWalNewRsp {
  SSHashObj*           indexHash;
  SSHashObj*           uidHash;
  void*                dataBlock;
  void*                metaBlock;
  void*                deleteBlock;
  void*                dropBlock;
  int64_t              ver;
  int64_t              verTime;
  int32_t              totalRows;
  bool                 isCalc;
} SSTriggerWalNewRsp;

typedef struct SSTriggerWalDataNewRequest {
  SSTriggerPullRequest base;
  SArray*              versions;  // SArray<int64_t>
  SSHashObj*           ranges;    // SSHash<gid, {skey, ekey}>
} SSTriggerWalDataNewRequest;

typedef struct SSTriggerWalMetaDataNewRequest {
  SSTriggerPullRequest base;
  int64_t              lastVer;
} SSTriggerWalMetaDataNewRequest;

typedef struct SSTriggerGroupColValueRequest {
  SSTriggerPullRequest base;
  int64_t              gid;
} SSTriggerGroupColValueRequest;

typedef struct SSTriggerVirTableInfoRequest {
  SSTriggerPullRequest base;
  SArray*              cids;  // SArray<col_id_t>, col ids of the virtual table
} SSTriggerVirTableInfoRequest;

typedef struct SSTriggerVirTablePseudoColRequest {
  SSTriggerPullRequest base;
  int64_t              uid;
  SArray*              cids;  // SArray<col_id_t>, -1 means tbname
} SSTriggerVirTablePseudoColRequest;
typedef struct OTableInfoRsp {
  int64_t  suid;
  int64_t  uid;
  col_id_t cid;
} OTableInfoRsp;

typedef struct OTableInfo {
  char     refTableName[TSDB_TABLE_NAME_LEN];
  char     refColName[TSDB_COL_NAME_LEN];
} OTableInfo;

typedef struct SSTriggerOrigTableInfoRequest {
  SSTriggerPullRequest base;
  SArray*              cols;  // SArray<OTableInfo>
} SSTriggerOrigTableInfoRequest;

typedef struct SSTriggerOrigTableInfoRsp {
  SArray*              cols;  // SArray<OTableInfoRsp>
} SSTriggerOrigTableInfoRsp;

int32_t tSerializeSTriggerOrigTableInfoRsp(void* buf, int32_t bufLen, const SSTriggerOrigTableInfoRsp* pReq);
int32_t tDserializeSTriggerOrigTableInfoRsp(void* buf, int32_t bufLen, SSTriggerOrigTableInfoRsp* pReq);
void    tDestroySTriggerOrigTableInfoRsp(SSTriggerOrigTableInfoRsp* pReq);

typedef union SSTriggerPullRequestUnion {
  SSTriggerPullRequest                base;
  SSTriggerSetTableRequest            setTableReq;
  SSTriggerLastTsRequest              lastTsReq;
  SSTriggerFirstTsRequest             firstTsReq;
  SSTriggerTsdbMetaRequest            tsdbMetaReq;
  SSTriggerTsdbTsDataRequest          tsdbTsDataReq;
  SSTriggerTsdbTriggerDataRequest     tsdbTriggerDataReq;
  SSTriggerTsdbCalcDataRequest        tsdbCalcDataReq;
  SSTriggerTsdbDataRequest            tsdbDataReq;
  SSTriggerWalMetaNewRequest          walMetaNewReq;
  SSTriggerWalDataNewRequest          walDataNewReq;
  SSTriggerWalMetaDataNewRequest      walMetaDataNewReq;
  SSTriggerGroupColValueRequest       groupColValueReq;
  SSTriggerVirTableInfoRequest        virTableInfoReq;
  SSTriggerVirTablePseudoColRequest   virTablePseudoColReq;
  SSTriggerOrigTableInfoRequest       origTableInfoReq;
} SSTriggerPullRequestUnion;

int32_t tSerializeSTriggerPullRequest(void* buf, int32_t bufLen, const SSTriggerPullRequest* pReq);
int32_t tDeserializeSTriggerPullRequest(void* buf, int32_t bufLen, SSTriggerPullRequestUnion* pReq);
void    tDestroySTriggerPullRequest(SSTriggerPullRequestUnion* pReq);

typedef struct SSTriggerCalcParam {
  union {
    struct {
      // Placeholder for Sliding Trigger
      int64_t prevTs;
      int64_t currentTs;
      int64_t nextTs;
    };
    struct {
      // Placeholder for Window Trigger
      int64_t wstart;
      int64_t wend;
      int64_t wduration;
      int64_t wrownum;
    };
    struct {
      // Placeholder for Period Trigger
      int64_t prevLocalTime;
      int64_t nextLocalTime;
    };
  };

  // General Placeholder
  int64_t triggerTime;  // _tlocaltime

  int32_t notifyType;           // See also: ESTriggerEventType
  char*   extraNotifyContent;   // NULL if not available
  char*   resultNotifyContent;  // does not serialize
} SSTriggerCalcParam;

typedef struct SSTriggerCalcRequest {
  int64_t streamId;
  int64_t runnerTaskId;
  int64_t sessionId;
  bool    isWindowTrigger;
  int8_t  precision;
  int32_t triggerType;    // See also: EStreamTriggerType
  int64_t triggerTaskId;  // does not serialize
  
  int64_t gid;
  SArray* params;        // SArray<SSTriggerCalcParam>
  SArray* groupColVals;  // SArray<SStreamGroupValue>, only provided at the first calculation of the group

  // The following fields are not serialized and only used by the runner task
  int8_t  createTable;
  bool    brandNew;   // no serialize
  int32_t execId;     // no serialize
  int32_t curWinIdx;  // no serialize
  void*   pOutBlock;  // no serialize
} SSTriggerCalcRequest;

int32_t tSerializeSTriggerCalcRequest(void* buf, int32_t bufLen, const SSTriggerCalcRequest* pReq);
int32_t tDeserializeSTriggerCalcRequest(void* buf, int32_t bufLen, SSTriggerCalcRequest* pReq);
void    tDestroySSTriggerCalcParam(void* ptr);
void    tDestroySTriggerCalcRequest(SSTriggerCalcRequest* pReq);

typedef struct SSTriggerDropRequest {
  int64_t streamId;
  int64_t runnerTaskId;
  int64_t sessionId;
  int64_t triggerTaskId;  // does not serialize

  int64_t gid;
  SArray* groupColVals;  // SArray<SStreamGroupValue>
} SSTriggerDropRequest;

int32_t tSerializeSTriggerDropTableRequest(void* buf, int32_t bufLen, const SSTriggerDropRequest* pReq);
int32_t tDeserializeSTriggerDropTableRequest(void* buf, int32_t bufLen, SSTriggerDropRequest* pReq);
void    tDestroySSTriggerDropRequest(SSTriggerDropRequest* pReq);

typedef enum ESTriggerCtrlType {
  STRIGGER_CTRL_START = 0,
  STRIGGER_CTRL_STOP = 1,
} ESTriggerCtrlType;

typedef struct SSTriggerCtrlRequest {
  ESTriggerCtrlType type;
  int64_t           streamId;
  int64_t           taskId;
  int64_t           sessionId;
} SSTriggerCtrlRequest;

int32_t tSerializeSTriggerCtrlRequest(void* buf, int32_t bufLen, const SSTriggerCtrlRequest* pReq);
int32_t tDeserializeSTriggerCtrlRequest(void* buf, int32_t bufLen, SSTriggerCtrlRequest* pReq);

typedef struct SStreamRuntimeFuncInfo {
  SArray* pStreamPesudoFuncVals;
  SArray* pStreamPartColVals;
  SArray* pStreamBlkWinIdx;  // no serialize, SArray<int64_t->winOutIdx+rowStartIdx>
  STimeWindow curWindow;
//  STimeWindow wholeWindow;
  int64_t groupId;
  int32_t curIdx; // for pesudo func calculation
  int64_t sessionId;
  bool    withExternalWindow;
  bool    isWindowTrigger;
  int8_t  precision;
  int32_t curOutIdx; // to indicate the window index for current block, valid value start from 1
  int32_t triggerType;
  int32_t addOptions;
  bool    hasPlaceHolder;
} SStreamRuntimeFuncInfo;

int32_t tSerializeStRtFuncInfo(SEncoder* pEncoder, const SStreamRuntimeFuncInfo* pInfo, bool full);
int32_t tDeserializeStRtFuncInfo(SDecoder* pDecoder, SStreamRuntimeFuncInfo* pInfo);
void    tDestroyStRtFuncInfo(SStreamRuntimeFuncInfo* pInfo);
typedef struct STsInfo {
  int64_t gId;
  int64_t  ts;
} STsInfo;

typedef struct VTableInfo {
  int64_t gId;        // group id
  int64_t uid;        // table uid
  SColRefWrapper cols;    
} VTableInfo;

typedef struct SStreamMsgVTableInfo {
  SArray*        infos;     // SArray<VTableInfo>
} SStreamMsgVTableInfo;

void tDestroyVTableInfo(void *ptr);
int32_t tSerializeSStreamMsgVTableInfo(void* buf, int32_t bufLen, const SStreamMsgVTableInfo* pRsp);
int32_t tDeserializeSStreamMsgVTableInfo(void* buf, int32_t bufLen, SStreamMsgVTableInfo *pBlock);
void    tDestroySStreamMsgVTableInfo(SStreamMsgVTableInfo *ptr);


typedef struct SStreamTsResponse {
  int64_t ver;
  SArray* tsInfo;  // SArray<STsInfo>
} SStreamTsResponse;

int32_t tSerializeSStreamTsResponse(void* buf, int32_t bufLen, const SStreamTsResponse* pRsp);
int32_t tDeserializeSStreamTsResponse(void* buf, int32_t bufLen, void *pBlock);

typedef struct SStreamWalDataSlice {
  uint64_t gId;
  int32_t startRowIdx;  // start row index of current slice in DataBlock
  int32_t currentRowIdx;
  int32_t numRows;      // number of rows in current slice
} SStreamWalDataSlice;

typedef struct SStreamWalDataResponse {
  void*      pDataBlock;
  SSHashObj* pSlices;  // SSHash<uid, SStreamWalDataSlice>
} SStreamWalDataResponse;

int32_t tSerializeSStreamWalDataResponse(void* buf, int32_t bufLen, SSTriggerWalNewRsp* metaBlock);
int32_t tDeserializeSStreamWalDataResponse(void* buf, int32_t bufLen, SSTriggerWalNewRsp* pRsp, SArray* pSlices);

typedef struct SStreamGroupValue {
  SValue        data;
  bool          isNull;
  bool          isTbname;
  int64_t       uid;
  int32_t       vgId;
} SStreamGroupValue;

typedef struct SStreamGroupInfo {
  SArray* gInfo;  // SArray<SStreamGroupValue>
} SStreamGroupInfo;

int32_t tSerializeSStreamGroupInfo(void* buf, int32_t bufLen, const SStreamGroupInfo* gInfo, int32_t vgId);
int32_t tDeserializeSStreamGroupInfo(void* buf, int32_t bufLen, SStreamGroupInfo* gInfo);
void    tDestroySStreamGroupValue(void *ptr);

typedef enum EValueType {
  SCL_VALUE_TYPE_NULL = 0,
  SCL_VALUE_TYPE_START,
  SCL_VALUE_TYPE_END,
} EValueType;
typedef struct SStreamTSRangeParas { // used for stream
  EOperatorType      opType;    
  EValueType         eType;   
  int64_t            timeValue;
} SStreamTSRangeParas;

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STREAMMSG_H
