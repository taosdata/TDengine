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
#include "trpc.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SStreamUpstreamEpInfo {
  int32_t nodeId;
  int32_t childId;
  int32_t taskId;
  SEpSet  epSet;
  bool    dataAllowed;  // denote if the data from this upstream task is allowed to put into inputQ, not serialize it
  int64_t stage;  // upstream task stage value, to denote if the upstream node has restart/replica changed/transfer
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
} SStreamTaskNodeUpdateMsg;

int32_t tEncodeStreamTaskUpdateMsg(SEncoder* pEncoder, const SStreamTaskNodeUpdateMsg* pMsg);
int32_t tDecodeStreamTaskUpdateMsg(SDecoder* pDecoder, SStreamTaskNodeUpdateMsg* pMsg);

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

typedef struct SStreamHbMsg {
  int32_t vgId;
  int32_t msgId;
  int64_t ts;
  int32_t numOfTasks;
  SArray* pTaskStatus;   // SArray<STaskStatusEntry>
  SArray* pUpdateNodes;  // SArray<int32_t>, needs update the epsets in stream tasks for those nodes.
} SStreamHbMsg;

int32_t tEncodeStreamHbMsg(SEncoder* pEncoder, const SStreamHbMsg* pRsp);
int32_t tDecodeStreamHbMsg(SDecoder* pDecoder, SStreamHbMsg* pRsp);
void    tCleanupStreamHbMsg(SStreamHbMsg* pMsg);

typedef struct {
  SMsgHead head;
  int32_t  msgId;
} SMStreamHbRspMsg;

typedef struct SRetrieveChkptTriggerReq {
  SMsgHead head;
  int64_t  streamId;
  int64_t  checkpointId;
  int32_t  upstreamNodeId;
  int32_t  upstreamTaskId;
  int32_t  downstreamNodeId;
  int64_t  downstreamTaskId;
} SRetrieveChkptTriggerReq;

typedef struct SCheckpointTriggerRsp {
  int64_t streamId;
  int64_t checkpointId;
  int32_t upstreamTaskId;
  int32_t taskId;
  int32_t transId;
  int32_t rspCode;
} SCheckpointTriggerRsp;

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
} SRestoreCheckpointInfo;

int32_t tEncodeRestoreCheckpointInfo (SEncoder* pEncoder, const SRestoreCheckpointInfo* pReq);
int32_t tDecodeRestoreCheckpointInfo(SDecoder* pDecoder, SRestoreCheckpointInfo* pReq);

typedef struct {
  SMsgHead head;
  int64_t  streamId;
  int32_t  taskId;
  int32_t  reqType;
} SStreamTaskRunReq;

typedef struct SCheckpointConsensusEntry {
  SRestoreCheckpointInfo req;
  int64_t                ts;
} SCheckpointConsensusEntry;

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STREAMMSG_H
