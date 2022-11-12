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

#ifndef _TD_LIBS_SYNC_MESSAGE_H
#define _TD_LIBS_SYNC_MESSAGE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "syncInt.h"

typedef enum ESyncTimeoutType {
  SYNC_TIMEOUT_PING = 100,
  SYNC_TIMEOUT_ELECTION,
  SYNC_TIMEOUT_HEARTBEAT,
} ESyncTimeoutType;

typedef struct SyncTimeout {
  uint32_t         bytes;
  int32_t          vgId;
  uint32_t         msgType;
  ESyncTimeoutType timeoutType;
  uint64_t         logicClock;
  int32_t          timerMS;
  void*            data;  // need optimized
} SyncTimeout;

typedef struct SyncClientRequest {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;          // TDMT_SYNC_CLIENT_REQUEST
  uint32_t originalRpcType;  // origin RpcMsg msgType
  uint64_t seqNum;
  bool     isWeak;
  uint32_t dataLen;  // origin RpcMsg.contLen
  char     data[];   // origin RpcMsg.pCont
} SyncClientRequest;

typedef struct SyncClientRequestReply {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  int32_t  errCode;
  SRaftId  leaderHint;
} SyncClientRequestReply;

typedef struct SyncRequestVote {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;
  // private data
  SyncTerm  term;
  SyncIndex lastLogIndex;
  SyncTerm  lastLogTerm;
} SyncRequestVote;

typedef struct SyncRequestVoteReply {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;
  // private data
  SyncTerm term;
  bool     voteGranted;
} SyncRequestVoteReply;

typedef struct SyncAppendEntries {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;

  // private data
  SyncTerm  term;
  SyncIndex prevLogIndex;
  SyncTerm  prevLogTerm;
  SyncIndex commitIndex;
  SyncTerm  privateTerm;
  uint32_t  dataLen;
  char      data[];
} SyncAppendEntries;

typedef struct SyncAppendEntriesReply {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;
  // private data
  SyncTerm  term;
  SyncTerm  privateTerm;
  bool      success;
  SyncIndex matchIndex;
  SyncIndex lastSendIndex;
  int64_t   startTime;
} SyncAppendEntriesReply;

// ---------------------------------------------
typedef struct SyncHeartbeat {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;

  // private data
  SyncTerm  term;
  SyncIndex commitIndex;
  SyncTerm  privateTerm;
  SyncTerm  minMatchIndex;

} SyncHeartbeat;

SyncHeartbeat* syncHeartbeatBuild(int32_t vgId);
void           syncHeartbeatDestroy(SyncHeartbeat* pMsg);
void           syncHeartbeatSerialize(const SyncHeartbeat* pMsg, char* buf, uint32_t bufLen);
void           syncHeartbeatDeserialize(const char* buf, uint32_t len, SyncHeartbeat* pMsg);
char*          syncHeartbeatSerialize2(const SyncHeartbeat* pMsg, uint32_t* len);
SyncHeartbeat* syncHeartbeatDeserialize2(const char* buf, uint32_t len);
void           syncHeartbeat2RpcMsg(const SyncHeartbeat* pMsg, SRpcMsg* pRpcMsg);
void           syncHeartbeatFromRpcMsg(const SRpcMsg* pRpcMsg, SyncHeartbeat* pMsg);
SyncHeartbeat* syncHeartbeatFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*         syncHeartbeat2Json(const SyncHeartbeat* pMsg);
char*          syncHeartbeat2Str(const SyncHeartbeat* pMsg);

// for debug ----------------------
void syncHeartbeatPrint(const SyncHeartbeat* pMsg);
void syncHeartbeatPrint2(char* s, const SyncHeartbeat* pMsg);
void syncHeartbeatLog(const SyncHeartbeat* pMsg);
void syncHeartbeatLog2(char* s, const SyncHeartbeat* pMsg);

// ---------------------------------------------
typedef struct SyncHeartbeatReply {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;

  // private data
  SyncTerm term;
  SyncTerm privateTerm;
  int64_t  startTime;
} SyncHeartbeatReply;

SyncHeartbeatReply* syncHeartbeatReplyBuild(int32_t vgId);
void                syncHeartbeatReplyDestroy(SyncHeartbeatReply* pMsg);
void                syncHeartbeatReplySerialize(const SyncHeartbeatReply* pMsg, char* buf, uint32_t bufLen);
void                syncHeartbeatReplyDeserialize(const char* buf, uint32_t len, SyncHeartbeatReply* pMsg);
char*               syncHeartbeatReplySerialize2(const SyncHeartbeatReply* pMsg, uint32_t* len);
SyncHeartbeatReply* syncHeartbeatReplyDeserialize2(const char* buf, uint32_t len);
void                syncHeartbeatReply2RpcMsg(const SyncHeartbeatReply* pMsg, SRpcMsg* pRpcMsg);
void                syncHeartbeatReplyFromRpcMsg(const SRpcMsg* pRpcMsg, SyncHeartbeatReply* pMsg);
SyncHeartbeatReply* syncHeartbeatReplyFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*              syncHeartbeatReply2Json(const SyncHeartbeatReply* pMsg);
char*               syncHeartbeatReply2Str(const SyncHeartbeatReply* pMsg);

// for debug ----------------------
void syncHeartbeatReplyPrint(const SyncHeartbeatReply* pMsg);
void syncHeartbeatReplyPrint2(char* s, const SyncHeartbeatReply* pMsg);
void syncHeartbeatReplyLog(const SyncHeartbeatReply* pMsg);
void syncHeartbeatReplyLog2(char* s, const SyncHeartbeatReply* pMsg);

// ---------------------------------------------
typedef struct SyncPreSnapshot {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;

  // private data
  SyncTerm term;

} SyncPreSnapshot;

SyncPreSnapshot* syncPreSnapshotBuild(int32_t vgId);
void             syncPreSnapshotDestroy(SyncPreSnapshot* pMsg);
void             syncPreSnapshotSerialize(const SyncPreSnapshot* pMsg, char* buf, uint32_t bufLen);
void             syncPreSnapshotDeserialize(const char* buf, uint32_t len, SyncPreSnapshot* pMsg);
char*            syncPreSnapshotSerialize2(const SyncPreSnapshot* pMsg, uint32_t* len);
SyncPreSnapshot* syncPreSnapshotDeserialize2(const char* buf, uint32_t len);
void             syncPreSnapshot2RpcMsg(const SyncPreSnapshot* pMsg, SRpcMsg* pRpcMsg);
void             syncPreSnapshotFromRpcMsg(const SRpcMsg* pRpcMsg, SyncPreSnapshot* pMsg);
SyncPreSnapshot* syncPreSnapshotFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*           syncPreSnapshot2Json(const SyncPreSnapshot* pMsg);
char*            syncPreSnapshot2Str(const SyncPreSnapshot* pMsg);

// for debug ----------------------
void syncPreSnapshotPrint(const SyncPreSnapshot* pMsg);
void syncPreSnapshotPrint2(char* s, const SyncPreSnapshot* pMsg);
void syncPreSnapshotLog(const SyncPreSnapshot* pMsg);
void syncPreSnapshotLog2(char* s, const SyncPreSnapshot* pMsg);

// ---------------------------------------------
typedef struct SyncPreSnapshotReply {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;

  // private data
  SyncTerm  term;
  SyncIndex snapStart;

} SyncPreSnapshotReply;

SyncPreSnapshotReply* syncPreSnapshotReplyBuild(int32_t vgId);
void                  syncPreSnapshotReplyDestroy(SyncPreSnapshotReply* pMsg);
void                  syncPreSnapshotReplySerialize(const SyncPreSnapshotReply* pMsg, char* buf, uint32_t bufLen);
void                  syncPreSnapshotReplyDeserialize(const char* buf, uint32_t len, SyncPreSnapshotReply* pMsg);
char*                 syncPreSnapshotReplySerialize2(const SyncPreSnapshotReply* pMsg, uint32_t* len);
SyncPreSnapshotReply* syncPreSnapshotReplyDeserialize2(const char* buf, uint32_t len);
void                  syncPreSnapshotReply2RpcMsg(const SyncPreSnapshotReply* pMsg, SRpcMsg* pRpcMsg);
void                  syncPreSnapshotReplyFromRpcMsg(const SRpcMsg* pRpcMsg, SyncPreSnapshotReply* pMsg);
SyncPreSnapshotReply* syncPreSnapshotReplyFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*                syncPreSnapshotReply2Json(const SyncPreSnapshotReply* pMsg);
char*                 syncPreSnapshotReply2Str(const SyncPreSnapshotReply* pMsg);

// for debug ----------------------
void syncPreSnapshotReplyPrint(const SyncPreSnapshotReply* pMsg);
void syncPreSnapshotReplyPrint2(char* s, const SyncPreSnapshotReply* pMsg);
void syncPreSnapshotReplyLog(const SyncPreSnapshotReply* pMsg);
void syncPreSnapshotReplyLog2(char* s, const SyncPreSnapshotReply* pMsg);

// ---------------------------------------------
typedef struct SyncApplyMsg {
  uint32_t   bytes;
  int32_t    vgId;
  uint32_t   msgType;          // user SyncApplyMsg msgType
  uint32_t   originalRpcType;  // user RpcMsg msgType
  SFsmCbMeta fsmMeta;
  uint32_t   dataLen;  // user RpcMsg.contLen
  char       data[];   // user RpcMsg.pCont
} SyncApplyMsg;

SyncApplyMsg* syncApplyMsgBuild(uint32_t dataLen);
SyncApplyMsg* syncApplyMsgBuild2(const SRpcMsg* pOriginalRpcMsg, int32_t vgId, SFsmCbMeta* pMeta);
void          syncApplyMsgDestroy(SyncApplyMsg* pMsg);
void          syncApplyMsgSerialize(const SyncApplyMsg* pMsg, char* buf, uint32_t bufLen);
void          syncApplyMsgDeserialize(const char* buf, uint32_t len, SyncApplyMsg* pMsg);
char*         syncApplyMsgSerialize2(const SyncApplyMsg* pMsg, uint32_t* len);
SyncApplyMsg* syncApplyMsgDeserialize2(const char* buf, uint32_t len);
void syncApplyMsg2RpcMsg(const SyncApplyMsg* pMsg, SRpcMsg* pRpcMsg);     // SyncApplyMsg to SRpcMsg, put it into ApplyQ
void syncApplyMsgFromRpcMsg(const SRpcMsg* pRpcMsg, SyncApplyMsg* pMsg);  // get SRpcMsg from ApplyQ, to SyncApplyMsg
SyncApplyMsg* syncApplyMsgFromRpcMsg2(const SRpcMsg* pRpcMsg);
void syncApplyMsg2OriginalRpcMsg(const SyncApplyMsg* pMsg, SRpcMsg* pOriginalRpcMsg);  // SyncApplyMsg to OriginalRpcMsg
cJSON* syncApplyMsg2Json(const SyncApplyMsg* pMsg);
char*  syncApplyMsg2Str(const SyncApplyMsg* pMsg);

// for debug ----------------------
void syncApplyMsgPrint(const SyncApplyMsg* pMsg);
void syncApplyMsgPrint2(char* s, const SyncApplyMsg* pMsg);
void syncApplyMsgLog(const SyncApplyMsg* pMsg);
void syncApplyMsgLog2(char* s, const SyncApplyMsg* pMsg);

// ---------------------------------------------
typedef struct SyncSnapshotSend {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;

  SyncTerm  term;
  SyncIndex beginIndex;       // snapshot.beginIndex
  SyncIndex lastIndex;        // snapshot.lastIndex
  SyncTerm  lastTerm;         // snapshot.lastTerm
  SyncIndex lastConfigIndex;  // snapshot.lastConfigIndex
  SSyncCfg  lastConfig;
  int64_t   startTime;
  int32_t   seq;
  uint32_t  dataLen;
  char      data[];
} SyncSnapshotSend;

SyncSnapshotSend* syncSnapshotSendBuild(uint32_t dataLen, int32_t vgId);
void              syncSnapshotSendDestroy(SyncSnapshotSend* pMsg);
void              syncSnapshotSendSerialize(const SyncSnapshotSend* pMsg, char* buf, uint32_t bufLen);
void              syncSnapshotSendDeserialize(const char* buf, uint32_t len, SyncSnapshotSend* pMsg);
char*             syncSnapshotSendSerialize2(const SyncSnapshotSend* pMsg, uint32_t* len);
SyncSnapshotSend* syncSnapshotSendDeserialize2(const char* buf, uint32_t len);
void              syncSnapshotSend2RpcMsg(const SyncSnapshotSend* pMsg, SRpcMsg* pRpcMsg);
void              syncSnapshotSendFromRpcMsg(const SRpcMsg* pRpcMsg, SyncSnapshotSend* pMsg);
SyncSnapshotSend* syncSnapshotSendFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*            syncSnapshotSend2Json(const SyncSnapshotSend* pMsg);
char*             syncSnapshotSend2Str(const SyncSnapshotSend* pMsg);

// for debug ----------------------
void syncSnapshotSendPrint(const SyncSnapshotSend* pMsg);
void syncSnapshotSendPrint2(char* s, const SyncSnapshotSend* pMsg);
void syncSnapshotSendLog(const SyncSnapshotSend* pMsg);
void syncSnapshotSendLog2(char* s, const SyncSnapshotSend* pMsg);

// ---------------------------------------------
typedef struct SyncSnapshotRsp {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;

  SyncTerm  term;
  SyncIndex lastIndex;
  SyncTerm  lastTerm;
  int64_t   startTime;
  int32_t   ack;
  int32_t   code;
  SyncIndex snapBeginIndex;  // when ack = SYNC_SNAPSHOT_SEQ_BEGIN, it's valid
} SyncSnapshotRsp;

SyncSnapshotRsp* syncSnapshotRspBuild(int32_t vgId);
void             syncSnapshotRspDestroy(SyncSnapshotRsp* pMsg);
void             syncSnapshotRspSerialize(const SyncSnapshotRsp* pMsg, char* buf, uint32_t bufLen);
void             syncSnapshotRspDeserialize(const char* buf, uint32_t len, SyncSnapshotRsp* pMsg);
char*            syncSnapshotRspSerialize2(const SyncSnapshotRsp* pMsg, uint32_t* len);
SyncSnapshotRsp* syncSnapshotRspDeserialize2(const char* buf, uint32_t len);
void             syncSnapshotRsp2RpcMsg(const SyncSnapshotRsp* pMsg, SRpcMsg* pRpcMsg);
void             syncSnapshotRspFromRpcMsg(const SRpcMsg* pRpcMsg, SyncSnapshotRsp* pMsg);
SyncSnapshotRsp* syncSnapshotRspFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*           syncSnapshotRsp2Json(const SyncSnapshotRsp* pMsg);
char*            syncSnapshotRsp2Str(const SyncSnapshotRsp* pMsg);

// for debug ----------------------
void syncSnapshotRspPrint(const SyncSnapshotRsp* pMsg);
void syncSnapshotRspPrint2(char* s, const SyncSnapshotRsp* pMsg);
void syncSnapshotRspLog(const SyncSnapshotRsp* pMsg);
void syncSnapshotRspLog2(char* s, const SyncSnapshotRsp* pMsg);

// ---------------------------------------------
typedef struct SyncLeaderTransfer {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  /*
   SRaftId  srcId;
   SRaftId  destId;
   */
  SNodeInfo newNodeInfo;
  SRaftId   newLeaderId;
} SyncLeaderTransfer;

SyncLeaderTransfer* syncLeaderTransferBuild(int32_t vgId);
void                syncLeaderTransferDestroy(SyncLeaderTransfer* pMsg);
void                syncLeaderTransferSerialize(const SyncLeaderTransfer* pMsg, char* buf, uint32_t bufLen);
void                syncLeaderTransferDeserialize(const char* buf, uint32_t len, SyncLeaderTransfer* pMsg);
char*               syncLeaderTransferSerialize2(const SyncLeaderTransfer* pMsg, uint32_t* len);
SyncLeaderTransfer* syncLeaderTransferDeserialize2(const char* buf, uint32_t len);
void                syncLeaderTransfer2RpcMsg(const SyncLeaderTransfer* pMsg, SRpcMsg* pRpcMsg);
void                syncLeaderTransferFromRpcMsg(const SRpcMsg* pRpcMsg, SyncLeaderTransfer* pMsg);
SyncLeaderTransfer* syncLeaderTransferFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*              syncLeaderTransfer2Json(const SyncLeaderTransfer* pMsg);
char*               syncLeaderTransfer2Str(const SyncLeaderTransfer* pMsg);

typedef enum {
  SYNC_LOCAL_CMD_STEP_DOWN = 100,
  SYNC_LOCAL_CMD_FOLLOWER_CMT,
} ESyncLocalCmd;

const char* syncLocalCmdGetStr(int32_t cmd);

typedef struct SyncLocalCmd {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;

  int32_t   cmd;
  SyncTerm  sdNewTerm;  // step down new term
  SyncIndex fcIndex;    // follower commit index

} SyncLocalCmd;

SyncLocalCmd* syncLocalCmdBuild(int32_t vgId);
void          syncLocalCmdDestroy(SyncLocalCmd* pMsg);
void          syncLocalCmdSerialize(const SyncLocalCmd* pMsg, char* buf, uint32_t bufLen);
void          syncLocalCmdDeserialize(const char* buf, uint32_t len, SyncLocalCmd* pMsg);
char*         syncLocalCmdSerialize2(const SyncLocalCmd* pMsg, uint32_t* len);
SyncLocalCmd* syncLocalCmdDeserialize2(const char* buf, uint32_t len);
void          syncLocalCmd2RpcMsg(const SyncLocalCmd* pMsg, SRpcMsg* pRpcMsg);
void          syncLocalCmdFromRpcMsg(const SRpcMsg* pRpcMsg, SyncLocalCmd* pMsg);
SyncLocalCmd* syncLocalCmdFromRpcMsg2(const SRpcMsg* pRpcMsg);
cJSON*        syncLocalCmd2Json(const SyncLocalCmd* pMsg);
char*         syncLocalCmd2Str(const SyncLocalCmd* pMsg);

// for debug ----------------------
void syncLocalCmdPrint(const SyncLocalCmd* pMsg);
void syncLocalCmdPrint2(char* s, const SyncLocalCmd* pMsg);
void syncLocalCmdLog(const SyncLocalCmd* pMsg);
void syncLocalCmdLog2(char* s, const SyncLocalCmd* pMsg);

// on message ----------------------
int32_t syncNodeOnRequestVote(SSyncNode* pNode, const SRpcMsg* pMsg);
int32_t syncNodeOnRequestVoteReply(SSyncNode* pNode, const SRpcMsg* pMsg);
int32_t syncNodeOnAppendEntries(SSyncNode* pNode, const SRpcMsg* pMsg);
int32_t syncNodeOnAppendEntriesReply(SSyncNode* ths, const SRpcMsg* pMsg);

int32_t syncNodeOnPreSnapshot(SSyncNode* ths, SyncPreSnapshot* pMsg);
int32_t syncNodeOnPreSnapshotReply(SSyncNode* ths, SyncPreSnapshotReply* pMsg);

int32_t syncNodeOnSnapshot(SSyncNode* ths, SyncSnapshotSend* pMsg);
int32_t syncNodeOnSnapshotReply(SSyncNode* ths, SyncSnapshotRsp* pMsg);

int32_t syncNodeOnHeartbeat(SSyncNode* ths, SyncHeartbeat* pMsg);
int32_t syncNodeOnHeartbeatReply(SSyncNode* ths, SyncHeartbeatReply* pMsg);

int32_t syncNodeOnClientRequest(SSyncNode* ths, SRpcMsg* pMsg, SyncIndex* pRetIndex);
int32_t syncNodeOnLocalCmd(SSyncNode* ths, SyncLocalCmd* pMsg);

// -----------------------------------------

// option ----------------------------------
bool          syncNodeSnapshotEnable(SSyncNode* pSyncNode);
ESyncStrategy syncNodeStrategy(SSyncNode* pSyncNode);

const char* syncTimerTypeStr(enum ESyncTimeoutType timerType);

int32_t syncTimeoutBuild(SRpcMsg* pTimeoutRpcMsg, ESyncTimeoutType timeoutType, uint64_t logicClock, int32_t timerMS,
                         SSyncNode* pNode);
int32_t syncClientRequestBuildFromRpcMsg(SRpcMsg* pClientRequestRpcMsg, const SRpcMsg* pOriginalRpcMsg, uint64_t seqNum,
                                         bool isWeak, int32_t vgId);
int32_t syncClientRequestBuildFromNoopEntry(SRpcMsg* pClientRequestRpcMsg, const SSyncRaftEntry* pEntry, int32_t vgId);
int32_t syncBuildRequestVote(SRpcMsg* pMsg, int32_t vgId);
int32_t syncBuildRequestVoteReply(SRpcMsg* pMsg, int32_t vgId);
int32_t syncBuildAppendEntries(SRpcMsg* pMsg, int32_t dataLen, int32_t vgId);
int32_t syncBuildAppendEntriesReply(SRpcMsg* pMsg, int32_t vgId);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_MESSAGE_H*/
