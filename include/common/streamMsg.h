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

// mndTrigger: denote if this checkpoint is triggered by mnode or as requested from tasks when transfer-state finished
typedef struct {
  int64_t streamId;
  int64_t checkpointId;
  int32_t taskId;
  int32_t nodeId;
  SEpSet  mgmtEps;
  int32_t mnodeId;
  int32_t transId;
  int8_t  mndTrigger;
  int64_t expireTime;
} SStreamCheckpointSourceReq;

int32_t tEncodeStreamCheckpointSourceReq(SEncoder* pEncoder, const SStreamCheckpointSourceReq* pReq);
int32_t tDecodeStreamCheckpointSourceReq(SDecoder* pDecoder, SStreamCheckpointSourceReq* pReq);

typedef struct {
  int64_t streamId;
  int64_t checkpointId;
  int32_t taskId;
  int32_t nodeId;
  int32_t mnodeId;
  int64_t expireTime;
  int8_t  success;
} SStreamCheckpointSourceRsp;

int32_t tEncodeStreamCheckpointSourceRsp(SEncoder* pEncoder, const SStreamCheckpointSourceRsp* pRsp);

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

typedef struct {
  SMsgHead msgHead;
  int64_t  streamId;
  int64_t  checkpointId;
  int32_t  downstreamTaskId;
  int32_t  downstreamNodeId;
  int32_t  upstreamTaskId;
  int32_t  upstreamNodeId;
  int32_t  childId;
} SStreamCheckpointReadyMsg;

int32_t tEncodeStreamCheckpointReadyMsg(SEncoder* pEncoder, const SStreamCheckpointReadyMsg* pRsp);
int32_t tDecodeStreamCheckpointReadyMsg(SDecoder* pDecoder, SStreamCheckpointReadyMsg* pRsp);

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

typedef struct SStreamTaskCheckpointReq {
  int64_t streamId;
  int32_t taskId;
  int32_t nodeId;
} SStreamTaskCheckpointReq;

int32_t tEncodeStreamTaskCheckpointReq(SEncoder* pEncoder, const SStreamTaskCheckpointReq* pReq);
int32_t tDecodeStreamTaskCheckpointReq(SDecoder* pDecoder, SStreamTaskCheckpointReq* pReq);

typedef struct SStreamMsg {
  int32_t msgType;
} SStreamMsg;

typedef struct SStreamHbMsg {
  int32_t dnodeId;
  int32_t streamGId;
  int32_t snodeId;
  SArray* pVgLeaders;     // SArray<int32_t>
  SArray* pStreamStatus;  // SArray<SStreamStatus>
} SStreamHbMsg;

int32_t tEncodeStreamHbMsg(SEncoder* pEncoder, const SStreamHbMsg* pReq);
int32_t tDecodeStreamHbMsg(SDecoder* pDecoder, SStreamHbMsg* pReq);
void    tCleanupStreamHbMsg(SStreamHbMsg* pMsg);

typedef struct {

} SStreamReaderDeployMsg;

typedef struct {

} SStreamTriggerDeployMsg;

typedef struct {

} SStreamRunnerDeployMsg;

typedef union {
  SStreamReaderDeployMsg  reader;
  SStreamTriggerDeployMsg trigger;
  SStreamRunnerDeployMsg  runner;
} SStreamDeployTaskMsg;

typedef struct {
  SStreamTask          task;
  SStreamDeployTaskMsg msg;
} SStreamDeployTaskInfo;

typedef struct {
  int64_t streamId;
  SArray* readerTasks;   // SArray<SStreamDeployTaskInfo>
  SArray* triggerTasks;
  SArray* runnerTasks;
} SStreamTasksDeploy;

typedef struct {
  SArray* taskList;      // SArray<SStreamTasksDeploy>
} SStreamDeployActions;

typedef struct {
  SStreamMsg  header;
  
} SStreamStartTaskMsg;

typedef struct {
  SStreamTask         task;
  SStreamStartTaskMsg startMsg;
} SStreamTasksStart;

typedef struct {
  SArray* taskList;      // SArray<SStreamTasksStart>
} SStreamStartActions;

typedef struct {
  SStreamMsg  header;
  
} SStreamUndeployTaskMsg;

typedef struct {
  SStreamTask             task;
  SStreamUndeployTaskMsg  undeployMsg;
} SStreamTasksUndeploy;

typedef struct {
  bool    undeployAll;
  SArray* taskList;      // SArray<SStreamTasksUndeploy>
} SStreamUndeployActions;


typedef struct {
  int32_t  msgId;
  SStreamDeployActions   deploy;
  SStreamStartActions    start;
  SStreamUndeployActions undeploy;
  SArray*              nodesVerion;
} SMStreamHbRspMsg;

int32_t tEncodeStreamHbRsp(SEncoder* pEncoder, const SMStreamHbRspMsg* pRsp);
int32_t tDecodeStreamHbRsp(SDecoder* pDecoder, SMStreamHbRspMsg* pRsp);

typedef struct SRetrieveChkptTriggerReq {
  SMsgHead head;
  int64_t  streamId;
  int64_t  checkpointId;
  int32_t  upstreamNodeId;
  int32_t  upstreamTaskId;
  int32_t  downstreamNodeId;
  int64_t  downstreamTaskId;
} SRetrieveChkptTriggerReq;

int32_t tEncodeRetrieveChkptTriggerReq(SEncoder* pEncoder, const SRetrieveChkptTriggerReq* pReq);
int32_t tDecodeRetrieveChkptTriggerReq(SDecoder* pDecoder, SRetrieveChkptTriggerReq* pReq);

typedef struct SCheckpointTriggerRsp {
  int64_t streamId;
  int64_t checkpointId;
  int32_t upstreamTaskId;
  int32_t taskId;
  int32_t transId;
  int32_t rspCode;
} SCheckpointTriggerRsp;

int32_t tEncodeCheckpointTriggerRsp(SEncoder* pEncoder, const SCheckpointTriggerRsp* pRsp);
int32_t tDecodeCheckpointTriggerRsp(SDecoder* pDecoder, SCheckpointTriggerRsp* pRsp);

typedef struct SCheckpointReport {
  int64_t streamId;
  int32_t taskId;
  int32_t nodeId;
  int64_t checkpointId;
  int64_t checkpointVer;
  int64_t checkpointTs;
  int32_t transId;
  int8_t  dropHTask;
} SCheckpointReport;

int32_t tEncodeStreamTaskChkptReport(SEncoder* pEncoder, const SCheckpointReport* pReq);
int32_t tDecodeStreamTaskChkptReport(SDecoder* pDecoder, SCheckpointReport* pReq);

typedef struct SRestoreCheckpointInfo {
  SMsgHead head;
  int64_t  startTs;
  int64_t  streamId;
  int64_t  checkpointId;   // latest checkpoint id
  int32_t  transId;        // transaction id of the update the consensus-checkpointId transaction
  int32_t  taskId;
  int32_t  nodeId;
  int32_t  term;
} SRestoreCheckpointInfo;

int32_t tEncodeRestoreCheckpointInfo(SEncoder* pEncoder, const SRestoreCheckpointInfo* pReq);
int32_t tDecodeRestoreCheckpointInfo(SDecoder* pDecoder, SRestoreCheckpointInfo* pReq);

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

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STREAMMSG_H
