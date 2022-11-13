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

#ifndef _TD_LIBS_SYNC_UTIL_H
#define _TD_LIBS_SYNC_UTIL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "syncInt.h"

uint64_t syncUtilAddr2U64(const char* host, uint16_t port);
void     syncUtilU642Addr(uint64_t u64, char* host, int64_t len, uint16_t* port);
void     syncUtilNodeInfo2EpSet(const SNodeInfo* pInfo, SEpSet* pEpSet);
void     syncUtilRaftId2EpSet(const SRaftId* raftId, SEpSet* pEpSet);
bool     syncUtilNodeInfo2RaftId(const SNodeInfo* pInfo, SyncGroupId vgId, SRaftId* raftId);
bool     syncUtilSameId(const SRaftId* pId1, const SRaftId* pId2);
bool     syncUtilEmptyId(const SRaftId* pId);

int32_t     syncUtilElectRandomMS(int32_t min, int32_t max);
int32_t     syncUtilQuorum(int32_t replicaNum);
cJSON*      syncUtilRaftId2Json(const SRaftId* p);
const char* syncStr(ESyncState state);
char*       syncUtilPrintBin(char* ptr, uint32_t len);
char*       syncUtilPrintBin2(char* ptr, uint32_t len);
void        syncUtilMsgHtoN(void* msg);
void        syncUtilMsgNtoH(void* msg);
bool        syncUtilUserPreCommit(tmsg_t msgType);
bool        syncUtilUserCommit(tmsg_t msgType);
bool        syncUtilUserRollback(tmsg_t msgType);

void syncPrintNodeLog(const char* flags, ELogLevel level, int32_t dflag, SSyncNode* pNode, const char* format, ...);
void syncPrintSnapshotSenderLog(const char* flags, ELogLevel level, int32_t dflag, SSyncSnapshotSender* pSender,
                                const char* format, ...);
void syncPrintSnapshotReceiverLog(const char* flags, ELogLevel level, int32_t dflag, SSyncSnapshotReceiver* pReceiver,
                                  const char* format, ...);

void syncLogRecvTimer(SSyncNode* pSyncNode, const SyncTimeout* pMsg, const char* s);
void syncLogRecvLocalCmd(SSyncNode* pSyncNode, const SyncLocalCmd* pMsg, const char* s);

void syncLogSendAppendEntriesReply(SSyncNode* pSyncNode, const SyncAppendEntriesReply* pMsg, const char* s);
void syncLogRecvAppendEntriesReply(SSyncNode* pSyncNode, const SyncAppendEntriesReply* pMsg, const char* s);

void syncLogSendHeartbeat(SSyncNode* pSyncNode, const SyncHeartbeat* pMsg, const char* s);
void syncLogRecvHeartbeat(SSyncNode* pSyncNode, const SyncHeartbeat* pMsg, const char* s);

void syncLogSendHeartbeatReply(SSyncNode* pSyncNode, const SyncHeartbeatReply* pMsg, const char* s);
void syncLogRecvHeartbeatReply(SSyncNode* pSyncNode, const SyncHeartbeatReply* pMsg, const char* s);

void syncLogSendSyncPreSnapshot(SSyncNode* pSyncNode, const SyncPreSnapshot* pMsg, const char* s);
void syncLogRecvSyncPreSnapshot(SSyncNode* pSyncNode, const SyncPreSnapshot* pMsg, const char* s);

void syncLogSendSyncPreSnapshotReply(SSyncNode* pSyncNode, const SyncPreSnapshotReply* pMsg, const char* s);
void syncLogRecvSyncPreSnapshotReply(SSyncNode* pSyncNode, const SyncPreSnapshotReply* pMsg, const char* s);

void syncLogSendSyncSnapshotSend(SSyncNode* pSyncNode, const SyncSnapshotSend* pMsg, const char* s);
void syncLogRecvSyncSnapshotSend(SSyncNode* pSyncNode, const SyncSnapshotSend* pMsg, const char* s);

void syncLogSendSyncSnapshotRsp(SSyncNode* pSyncNode, const SyncSnapshotRsp* pMsg, const char* s);
void syncLogRecvSyncSnapshotRsp(SSyncNode* pSyncNode, const SyncSnapshotRsp* pMsg, const char* s);

void syncLogRecvAppendEntries(SSyncNode* pSyncNode, const SyncAppendEntries* pMsg, const char* s);
void syncLogSendAppendEntries(SSyncNode* pSyncNode, const SyncAppendEntries* pMsg, const char* s);

void syncLogRecvRequestVote(SSyncNode* pSyncNode, const SyncRequestVote* pMsg, const char* s);
void syncLogSendRequestVote(SSyncNode* pNode, const SyncRequestVote* pMsg, const char* s);

void syncLogRecvRequestVoteReply(SSyncNode* pSyncNode, const SyncRequestVoteReply* pMsg, const char* s);
void syncLogSendRequestVoteReply(SSyncNode* pSyncNode, const SyncRequestVoteReply* pMsg, const char* s);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_UTIL_H*/
