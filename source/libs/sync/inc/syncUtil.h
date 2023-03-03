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
#include "tlog.h"

// clang-format off

#define sFatal(...) if (sDebugFlag & DEBUG_FATAL) { taosPrintLog("SYN FATAL ", DEBUG_FATAL, 255,        __VA_ARGS__); }
#define sError(...) if (sDebugFlag & DEBUG_ERROR) { taosPrintLog("SYN ERROR ", DEBUG_ERROR, 255,        __VA_ARGS__); }
#define sWarn(...)  if (sDebugFlag & DEBUG_WARN)  { taosPrintLog("SYN WARN ",  DEBUG_WARN,  255,        __VA_ARGS__); }
#define sInfo(...)  if (sDebugFlag & DEBUG_INFO)  { taosPrintLog("SYN ",       DEBUG_INFO,  255,        __VA_ARGS__); }
#define sDebug(...) if (sDebugFlag & DEBUG_DEBUG) { taosPrintLog("SYN ",       DEBUG_DEBUG, sDebugFlag, __VA_ARGS__); }
#define sTrace(...) if (sDebugFlag & DEBUG_TRACE) { taosPrintLog("SYN ",       DEBUG_TRACE, sDebugFlag, __VA_ARGS__); }

#define sLFatal(...) if (sDebugFlag & DEBUG_FATAL) { taosPrintLongString("SYN FATAL ", DEBUG_FATAL, 255,        __VA_ARGS__); }
#define sLError(...) if (sDebugFlag & DEBUG_ERROR) { taosPrintLongString("SYN ERROR ", DEBUG_ERROR, 255,        __VA_ARGS__); }
#define sLWarn(...)  if (sDebugFlag & DEBUG_WARN)  { taosPrintLongString("SYN WARN ",  DEBUG_WARN,  255,        __VA_ARGS__); }
#define sLInfo(...)  if (sDebugFlag & DEBUG_INFO)  { taosPrintLongString("SYN ",       DEBUG_INFO,  255,        __VA_ARGS__); }
#define sLDebug(...) if (sDebugFlag & DEBUG_DEBUG) { taosPrintLongString("SYN ",       DEBUG_DEBUG, sDebugFlag, __VA_ARGS__); }
#define sLTrace(...) if (sDebugFlag & DEBUG_TRACE) { taosPrintLongString("SYN ",       DEBUG_TRACE, sDebugFlag, __VA_ARGS__); }

#define sNFatal(pNode, ...)  if (sDebugFlag & DEBUG_FATAL) { syncPrintNodeLog("SYN FATAL ", DEBUG_FATAL, 255,        pNode, __VA_ARGS__); }
#define sNError(pNode, ...)  if (sDebugFlag & DEBUG_ERROR) { syncPrintNodeLog("SYN ERROR ", DEBUG_ERROR, 255,        pNode, __VA_ARGS__); }
#define sNWarn(pNode, ...)   if (sDebugFlag & DEBUG_WARN)  { syncPrintNodeLog("SYN WARN ",  DEBUG_WARN,  255,        pNode, __VA_ARGS__); }
#define sNInfo(pNode, ...)   if (sDebugFlag & DEBUG_INFO)  { syncPrintNodeLog("SYN ",       DEBUG_INFO,  255,        pNode, __VA_ARGS__); }
#define sNDebug(pNode, ...)  if (sDebugFlag & DEBUG_DEBUG) { syncPrintNodeLog("SYN ",       DEBUG_DEBUG, sDebugFlag, pNode, __VA_ARGS__); }
#define sNTrace(pNode, ...)  if (sDebugFlag & DEBUG_TRACE) { syncPrintNodeLog("SYN ",       DEBUG_TRACE, sDebugFlag, pNode, __VA_ARGS__); }

#define sSFatal(pSender, ...)  if (sDebugFlag & DEBUG_FATAL) { syncPrintSnapshotSenderLog("SYN FATAL ", DEBUG_FATAL, 255,        pSender, __VA_ARGS__); }
#define sSError(pSender, ...)  if (sDebugFlag & DEBUG_ERROR) { syncPrintSnapshotSenderLog("SYN ERROR ", DEBUG_ERROR, 255,        pSender, __VA_ARGS__); }
#define sSWarn(pSender, ...)   if (sDebugFlag & DEBUG_WARN)  { syncPrintSnapshotSenderLog("SYN WARN ",  DEBUG_WARN,  255,        pSender, __VA_ARGS__); }
#define sSInfo(pSender, ...)   if (sDebugFlag & DEBUG_INFO)  { syncPrintSnapshotSenderLog("SYN ",       DEBUG_INFO,  255,        pSender, __VA_ARGS__); }
#define sSDebug(pSender, ...)  if (sDebugFlag & DEBUG_DEBUG) { syncPrintSnapshotSenderLog("SYN ",       DEBUG_DEBUG, sDebugFlag, pSender, __VA_ARGS__); }
#define sSTrace(pSender, ...)  if (sDebugFlag & DEBUG_TRACE) { syncPrintSnapshotSenderLog("SYN ",       DEBUG_TRACE, sDebugFlag, pSender, __VA_ARGS__); }

#define sRFatal(pReceiver, ...)  if (sDebugFlag & DEBUG_FATAL) { syncPrintSnapshotReceiverLog("SYN FATAL ", DEBUG_FATAL, 255,        pReceiver, __VA_ARGS__); }
#define sRError(pReceiver, ...)  if (sDebugFlag & DEBUG_ERROR) { syncPrintSnapshotReceiverLog("SYN ERROR ", DEBUG_ERROR, 255,        pReceiver, __VA_ARGS__); }
#define sRWarn(pReceiver, ...)   if (sDebugFlag & DEBUG_WARN)  { syncPrintSnapshotReceiverLog("SYN WARN ",  DEBUG_WARN,  255,        pReceiver, __VA_ARGS__); }
#define sRInfo(pReceiver, ...)   if (sDebugFlag & DEBUG_INFO)  { syncPrintSnapshotReceiverLog("SYN ",       DEBUG_INFO,  255,        pReceiver, __VA_ARGS__); }
#define sRDebug(pReceiver, ...)  if (sDebugFlag & DEBUG_DEBUG) { syncPrintSnapshotReceiverLog("SYN ",       DEBUG_DEBUG, sDebugFlag, pReceiver, __VA_ARGS__); }
#define sRTrace(pReceiver, ...)  if (sDebugFlag & DEBUG_TRACE) { syncPrintSnapshotReceiverLog("SYN ",       DEBUG_TRACE, sDebugFlag, pReceiver, __VA_ARGS__); }

// clang-format on

#define CID(pRaftId)     (int32_t)(((pRaftId)->addr) >> 32)
#define DID(pRaftId)     (int32_t)((pRaftId)->addr)
#define SYNC_ADDR(pInfo) (int64_t)(((pInfo)->clusterId << 32) | (pInfo)->nodeId)

void syncUtilNodeInfo2EpSet(const SNodeInfo* pInfo, SEpSet* pEpSet);
bool syncUtilNodeInfo2RaftId(const SNodeInfo* pInfo, SyncGroupId vgId, SRaftId* raftId);
bool syncUtilSameId(const SRaftId* pId1, const SRaftId* pId2);
bool syncUtilEmptyId(const SRaftId* pId);

int32_t     syncUtilElectRandomMS(int32_t min, int32_t max);
int32_t     syncUtilQuorum(int32_t replicaNum);
const char* syncStr(ESyncState state);
void        syncUtilMsgHtoN(void* msg);
bool        syncUtilUserPreCommit(tmsg_t msgType);
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

void syncLogSendHeartbeat(SSyncNode* pSyncNode, const SyncHeartbeat* pMsg, bool printX, int64_t timerElapsed,
                          int64_t execTime);
void syncLogRecvHeartbeat(SSyncNode* pSyncNode, const SyncHeartbeat* pMsg, int64_t timeDiff, const char* s);

void syncLogSendHeartbeatReply(SSyncNode* pSyncNode, const SyncHeartbeatReply* pMsg, const char* s);
void syncLogRecvHeartbeatReply(SSyncNode* pSyncNode, const SyncHeartbeatReply* pMsg, int64_t timeDiff, const char* s);

void syncLogSendSyncSnapshotSend(SSyncNode* pSyncNode, const SyncSnapshotSend* pMsg, const char* s);
void syncLogRecvSyncSnapshotSend(SSyncNode* pSyncNode, const SyncSnapshotSend* pMsg, const char* s);

void syncLogSendSyncSnapshotRsp(SSyncNode* pSyncNode, const SyncSnapshotRsp* pMsg, const char* s);
void syncLogRecvSyncSnapshotRsp(SSyncNode* pSyncNode, const SyncSnapshotRsp* pMsg, const char* s);

void syncLogRecvAppendEntries(SSyncNode* pSyncNode, const SyncAppendEntries* pMsg, const char* s);
void syncLogSendAppendEntries(SSyncNode* pSyncNode, const SyncAppendEntries* pMsg, const char* s);

void syncLogRecvRequestVote(SSyncNode* pSyncNode, const SyncRequestVote* pMsg, int32_t voteGranted, const char* s);
void syncLogSendRequestVote(SSyncNode* pNode, const SyncRequestVote* pMsg, const char* s);

void syncLogRecvRequestVoteReply(SSyncNode* pSyncNode, const SyncRequestVoteReply* pMsg, const char* s);
void syncLogSendRequestVoteReply(SSyncNode* pSyncNode, const SyncRequestVoteReply* pMsg, const char* s);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_UTIL_H*/
