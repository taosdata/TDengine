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

#include "tmsg.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SStreamRetrieveReq SStreamRetrieveReq;
typedef struct SStreamDispatchReq SStreamDispatchReq;
typedef struct STokenBucket       STokenBucket;

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
#define PLACE_HOLDER_CURRENT_TS       BIT_FLAG_MASK(0)
#define PLACE_HOLDER_WSTART           BIT_FLAG_MASK(1)
#define PLACE_HOLDER_WEND             BIT_FLAG_MASK(2)
#define PLACE_HOLDER_WDURATION        BIT_FLAG_MASK(3)
#define PLACE_HOLDER_WROWNUM          BIT_FLAG_MASK(4)
#define PLACE_HOLDER_LOCALTIME        BIT_FLAG_MASK(5)
#define PLACE_HOLDER_PARTITION_IDX    BIT_FLAG_MASK(6)
#define PLACE_HOLDER_PARTITION_TBNAME BIT_FLAG_MASK(7)
#define PLACE_HOLDER_PARTITION_ROWS   BIT_FLAG_MASK(8)
#define PLACE_HOLDER_GRPID            BIT_FLAG_MASK(9)

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

typedef struct SSessionTrigger {
  int16_t slotId;
  int64_t sessionVal;
} SSessionTrigger;

typedef struct SStateWinTrigger {
  int16_t slotId;
  int64_t trueForDuration;
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
  char*   name;
  int64_t streamId;
  char*   sql;

  char*   streamDB;
  char*   triggerDB;
  char*   outDB;
  SArray* calcDB;  // char*

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

  // notify options
  SArray* pNotifyAddrUrls;
  int32_t notifyEventTypes;
  int32_t notifyErrorHandle;
  int8_t  notifyHistory;

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
  int8_t   outTblType;
  int8_t   outStbExists;
  uint64_t outStbUid;
  int64_t  eventTypes;
  int64_t  flags;
  int64_t  tsmaId;
  int64_t  placeHolderBitmap;

  // only for child table and normal table
  int32_t triggerTblVgId;
  int32_t outTblVgId;

  // reader part
  void*
      triggerScanPlan;  // block include all
                        // preFilter<>triggerPrevFilter/partitionCols<>subTblNameExpr+tagValueExpr/triggerCols<>triggerCond
  SArray* calcScanPlanList;  // for calc action, SArray<SStreamCalcScan>

  // trigger part
  SArray* pVSubTables;  // array of SVSubTablesRsp

  // runner part
  void*   calcPlan;  // for calc action
  void*   subTblNameExpr;
  void*   tagValueExpr;
  SArray* forceOutCols;  // array of SStreamOutCol, only available when forceOutput is true
} SCMCreateStreamReq;

typedef struct SStreamMsg {
  int32_t msgType;
} SStreamMsg;

typedef enum {
  STREAM_STATUS_NA = 0,
  STREAM_STATUS_INIT = 1,
  STREAM_STATUS_RUNNING,
  STREAM_STATUS_STOPPED,
  STREAM_STATUS_FAILED,
} EStreamStatus;

typedef enum EStreamTaskType {
  STREAM_READER_TASK = 0,
  STREAM_TRIGGER_TASK,
  STREAM_RUNNER_TASK,
} EStreamTaskType;

static const char* gStreamTaskTypeStr[] = {"Reader", "Trigger", "Runner"};

typedef struct SStreamTask {
  EStreamTaskType type;

  /** KEEP TOGETHER **/
  int64_t streamId;  // ID of the stream
  int64_t taskId;    // ID of the current task
  /** KEEP TOGETHER **/

  int32_t       nodeId;     // ID of the vgroup/snode
  int64_t       sessionId;  // ID of the current session (real-time, historical, or recalculation)
  int16_t       taskIdx;
  EStreamStatus status;
} SStreamTask;

typedef SStreamTask SStmTaskStatusMsg;

typedef struct SStreamHbMsg {
  int32_t dnodeId;
  int32_t streamGId;
  int32_t snodeId;
  SArray* pVgLeaders;     // SArray<int32_t>
  SArray* pStreamStatus;  // SArray<SStmTaskStatusMsg>
} SStreamHbMsg;

int32_t tEncodeStreamHbMsg(SEncoder* pEncoder, const SStreamHbMsg* pReq);
int32_t tDecodeStreamHbMsg(SDecoder* pDecoder, SStreamHbMsg* pReq);
void    tCleanupStreamHbMsg(SStreamHbMsg* pMsg);

typedef struct {
  char*   triggerTblName;
  int64_t triggerTblUid;  // suid or uid
  int8_t  triggerTblType;
  int8_t  deleteReCalc;
  int8_t  deleteOutTbl;
  void*   partitionCols;  // nodelist of SColumnNode
  void*   triggerCols;    // nodelist of SColumnNode
  // void*   triggerPrevFilter;
  void* triggerScanPlan;
} SStreamReaderDeployFromTrigger;

typedef struct {
  void* calcScanPlan;
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

typedef struct SStreamRunnerTarget {
  SStreamTaskAddr addr;
  int32_t         execReplica;
} SStreamRunnerTarget;

typedef struct {
  int8_t triggerType;
  int8_t igDisorder;
  int8_t fillHistory;
  int8_t fillHistoryFirst;
  int8_t lowLatencyCalc;

  // notify options
  SArray* pNotifyAddrUrls;
  int32_t notifyEventTypes;
  int32_t notifyErrorHandle;
  int8_t  notifyHistory;

  int64_t        maxDelay;              // precision is ms
  int64_t        fillHistoryStartTime;  // precision same with triggerDB, INT64_MIN for no value specified
  int64_t        watermark;             // precision same with triggerDB
  int64_t        expiredTime;           // precision same with triggerDB
  SStreamTrigger trigger;

  int64_t eventTypes;
  int64_t placeHolderBitmap;

  SArray* readerList;  // SArray<SStreamTaskAddr>
  SArray* runnerList;  // SArray<SStreamRunnerTarget>

  SArray* pVSubTables;
} SStreamTriggerDeployMsg;

typedef struct SStreamRunnerDeployMsg {
  int32_t execReplica;

  void*  pPlan;
  char*  outDBFName;
  char*  outTblName;
  int8_t outTblType;
  int8_t calcNotifyOnly;

  // notify options
  SArray* pNotifyAddrUrls;
  int32_t notifyErrorHandle;

  SArray*  outCols;  // array of SFieldWithOptions
  SArray*  outTags;  // array of SFieldWithOptions
  uint64_t outStbUid;

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
  SArray*         readerTasks;  // SArray<SStmTaskDeploy> in v/sNode and SArray<SStmTaskDeploy*> in mNode
  SStmTaskDeploy* triggerTask;
  SArray*         runnerTasks;  // SArray<SStmTaskDeploy> in v/sNode and SArray<SStmTaskDeploy*> in mNode
} SStmStreamDeploy;

typedef struct {
  SArray* streamList;  // SArray<SStmStreamDeploy>
} SStreamDeployActions;

typedef struct {
  SStreamMsg header;

} SStreamStartTaskMsg;

typedef struct {
  SStreamTask         task;
  SStreamStartTaskMsg startMsg;
} SStreamTaskStart;

typedef struct {
  SArray* taskList;  // SArray<SStreamTaskStart>
} SStreamStartActions;

typedef struct {
  SStreamMsg header;
  int8_t     doCheckpoint;
  int8_t     doCleanup;
} SStreamUndeployTaskMsg;

typedef struct {
  SStreamTask            task;
  SStreamUndeployTaskMsg undeployMsg;
} SStreamTaskUndeploy;

typedef struct {
  int8_t  undeployAll;
  SArray* taskList;  // SArray<SStreamTaskUndeploy>
} SStreamUndeployActions;

typedef struct {
  int32_t streamGid;
} SStreamMsgGrpHeader;

typedef struct {
  int32_t                streamGId;
  SStreamDeployActions   deploy;
  SStreamStartActions    start;
  SStreamUndeployActions undeploy;
} SMStreamHbRspMsg;

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
  int32_t vgId;
  int32_t fetchIdx;
  int32_t subFetchIdx;
} SStreamProgressReq;

int32_t tSerializeStreamProgressReq(void* buf, int32_t bufLen, const SStreamProgressReq* pReq);
int32_t tDeserializeStreamProgressReq(void* buf, int32_t bufLen, SStreamProgressReq* pReq);

typedef struct SStreamProgressRsp {
  int64_t streamId;
  int32_t vgId;
  bool    fillHisFinished;
  int64_t progressDelay;
  int32_t fetchIdx;
  int32_t subFetchIdx;
} SStreamProgressRsp;

int32_t tSerializeStreamProgressRsp(void* buf, int32_t bufLen, const SStreamProgressRsp* pRsp);
int32_t tDeserializeSStreamProgressRsp(void* buf, int32_t bufLen, SStreamProgressRsp* pRsp);

typedef struct {
  int64_t streamId;
} SCMCreateStreamRsp;

int32_t tSerializeSCMCreateStreamReq(void* buf, int32_t bufLen, const SCMCreateStreamReq* pReq);
int32_t tDeserializeSCMCreateStreamReq(void* buf, int32_t bufLen, SCMCreateStreamReq* pReq);
void    tFreeSCMCreateStreamReq(SCMCreateStreamReq* pReq);

int32_t tSerializeSCMCreateStreamReqImpl(SEncoder* pEncoder, const SCMCreateStreamReq* pReq);
int32_t tDeserializeSCMCreateStreamReqImpl(SDecoder* pDecoder, SCMCreateStreamReq* pReq);

typedef enum ESTriggerPullType {
  STRIGGER_PULL_LAST_TS,
  STRIGGER_PULL_FIRST_TS,
  STRIGGER_PULL_TSDB_META,
  STRIGGER_PULL_TSDB_META_NEXT,
  STRIGGER_PULL_TSDB_TS_DATA,
  STRIGGER_PULL_TSDB_TRIGGER_DATA,
  STRIGGER_PULL_TSDB_TRIGGER_DATA_NEXT,
  STRIGGER_PULL_TSDB_CALC_DATA,
  STRIGGER_PULL_TSDB_CALC_DATA_NEXT,
  STRIGGER_PULL_WAL_META,
  STRIGGER_PULL_WAL_TS_DATA,
  STRIGGER_PULL_WAL_TRIGGER_DATA,
  STRIGGER_PULL_WAL_CALC_DATA,
} ESTriggerPullType;

typedef struct SSTriggerPullRequest {
  ESTriggerPullType type;
  int64_t           streamId;
  int64_t           readerTaskId;
  int64_t           sessionId;
  int64_t           triggerTaskId;  // does not serialize
} SSTriggerPullRequest;

typedef struct SSTriggerLastTsRequest {
  SSTriggerPullRequest base;
} SSTriggerLastTsRequest;

typedef struct SSTriggerFirstTsRequest {
  SSTriggerPullRequest base;
  int64_t              startTime;
} SSTriggerFirstTsRequest;

typedef struct SSTriggerTsdbMetaRequest {
  SSTriggerPullRequest base;
  int64_t              startTime;
} SSTriggerTsdbMetaRequest;

typedef struct SSTriggerTsdbMetaNextRequest {
  SSTriggerPullRequest base;
} SSTriggerTsdbMetaNextRequest;

typedef struct SSTriggerTsdbTsDataRequest {
  SSTriggerPullRequest base;
  int64_t              uid;
  int64_t              skey;
  int64_t              ekey;
} SSTriggerTsdbTsDataRequest;

typedef struct SSTriggerTsdbTriggerDataRequest {
  SSTriggerPullRequest base;
  int64_t              startTime;
} SSTriggerTsdbTriggerDataRequest;

typedef struct SSTriggerTsdbTriggerDataNextRequest {
  SSTriggerPullRequest base;
} SSTriggerTsdbTriggerDataNextRequest;

typedef struct SSTriggerTsdbCalcDataRequest {
  SSTriggerPullRequest base;
  int64_t              gid;
  int64_t              skey;
  int64_t              ekey;
} SSTriggerTsdbCalcDataRequest;

typedef struct SSTriggerTsdbCalcDataNextRequest {
  SSTriggerPullRequest base;
} SSTriggerTsdbCalcDataNextRequest;

typedef struct SSTriggerWalMetaRequest {
  SSTriggerPullRequest base;
  int64_t              lastVer;
} SSTriggerWalMetaRequest;

typedef struct SSTriggerWalTsDataRequest {
  SSTriggerPullRequest base;
  int64_t              uid;
  int64_t              ver;
  int64_t              uid;
} SSTriggerWalTsDataRequest;

typedef struct SSTriggerWalTriggerDataRequest {
  SSTriggerPullRequest base;
  int64_t              uid;
  int64_t              ver;
  int64_t              uid;
} SSTriggerWalTriggerDataRequest;

typedef struct SSTriggerWalCalcDataRequest {
  SSTriggerPullRequest base;
  int64_t              uid;
  int64_t              ver;
  int64_t              skey;
  int64_t              ekey;
  int64_t              uid;
} SSTriggerWalCalcDataRequest;

typedef union SSTriggerPullRequestUnion {
  SSTriggerPullRequest                base;
  SSTriggerLastTsRequest              lastTsReq;
  SSTriggerFirstTsRequest             firstTsReq;
  SSTriggerTsdbMetaRequest            tsdbMetaReq;
  SSTriggerTsdbMetaNextRequest        tsdbMetaNextReq;
  SSTriggerTsdbTsDataRequest          tsdbTsDataReq;
  SSTriggerTsdbTriggerDataRequest     tsdbTriggerDataReq;
  SSTriggerTsdbTriggerDataNextRequest tsdbTriggerDataNextReq;
  SSTriggerTsdbCalcDataRequest        tsdbCalcDataReq;
  SSTriggerTsdbCalcDataNextRequest    tsdbCalcDataNextReq;
  SSTriggerWalMetaRequest             walMetaReq;
  SSTriggerWalTsDataRequest           walTsDataReq;
  SSTriggerWalTriggerDataRequest      walTriggerDataReq;
  SSTriggerWalCalcDataRequest         walCalcDataReq;
} SSTriggerPullRequestUnion;

int32_t tSerializeSTriggerPullRequest(void* buf, int32_t bufLen, const SSTriggerPullRequest* pReq);
int32_t tDserializeSTriggerPullRequest(void* buf, int32_t bufLen, SSTriggerPullRequestUnion* pReq);

typedef struct SSTriggerCalcParam {
  // These fields only have values when used in the statement, otherwise they are 0
  int64_t currentTs;
  int64_t wstart;
  int64_t wend;
  int64_t wduration;
  int64_t wrownum;
  int64_t triggerTime;

  int32_t notifyType;          // See also: ESTriggerEventType
  char*   extraNotifyContent;  // NULL if not available
} SSTriggerCalcParam;

typedef struct SSTriggerCalcRequest {
  int64_t streamId;
  int64_t runnerTaskId;
  int64_t sessionId;
  int64_t triggerTaskId;  // does not serialize
  int32_t execId;

  int64_t gid;
  SArray* params;
  SArray* groupColVals;  // only provided at the first calculation of the group
  bool    brandNew;      // TODO wjm remove it
} SSTriggerCalcRequest;

int32_t tSerializeSTriggerCalcRequest(void* buf, int32_t bufLen, const SSTriggerCalcRequest* pReq);
int32_t tDeserializeSTriggerCalcRequest(void* buf, int32_t bufLen, SSTriggerCalcRequest* pReq);
void    tDestroySTriggerCalcRequest(SSTriggerCalcRequest* pReq);

typedef struct SSTriggerPullResponse {
  void* pDataBlock;
} SSTriggerPullResponse;

typedef struct SSTriggerCalcResponse {
  int64_t sessionId;
  int32_t code;
} SSTriggerCalcResponse;

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STREAMMSG_H
