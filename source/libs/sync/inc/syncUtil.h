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
#define sWarn(...)  if (sDebugFlag & DEBUG_WARN)  { taosPrintLog("SYN WARN  ", DEBUG_WARN,  255,        __VA_ARGS__); }
#define sInfo(...)  if (sDebugFlag & DEBUG_INFO)  { taosPrintLog("SYN INFO  ", DEBUG_INFO,  255,        __VA_ARGS__); }
#define sDebug(...) if (sDebugFlag & DEBUG_DEBUG) { taosPrintLog("SYN DEBUG ", DEBUG_DEBUG, sDebugFlag, __VA_ARGS__); }
#define sTrace(...) if (sDebugFlag & DEBUG_TRACE) { taosPrintLog("SYN TRACE ", DEBUG_TRACE, sDebugFlag, __VA_ARGS__); }

#define sGTrace(trace, param, ...) do { if (sDebugFlag & DEBUG_TRACE) { sTrace(param ", QID:0x%" PRIx64 ":0x%" PRIx64, __VA_ARGS__, (trace) ? (trace)->rootId : 0, (trace) ? (trace)->msgId : 0);}} while(0)
#define sGFatal(trace, param, ...) do { if (sDebugFlag & DEBUG_FATAL) { sFatal(param ", QID:0x%" PRIx64 ":0x%" PRIx64, __VA_ARGS__, (trace) ? (trace)->rootId : 0, (trace) ? (trace)->msgId : 0);}} while(0)
#define sGError(trace, param, ...) do { if (sDebugFlag & DEBUG_ERROR) { sError(param ", QID:0x%" PRIx64 ":0x%" PRIx64, __VA_ARGS__, (trace) ? (trace)->rootId : 0, (trace) ? (trace)->msgId : 0);}} while(0)
#define sGWarn(trace, param, ...)  do { if (sDebugFlag & DEBUG_WARN)  { sWarn(param  ", QID:0x%" PRIx64 ":0x%" PRIx64, __VA_ARGS__, (trace) ? (trace)->rootId : 0, (trace) ? (trace)->msgId : 0);}} while(0)
#define sGInfo(trace, param, ...)  do { if (sDebugFlag & DEBUG_INFO)  { sInfo(param  ", QID:0x%" PRIx64 ":0x%" PRIx64, __VA_ARGS__, (trace) ? (trace)->rootId : 0, (trace) ? (trace)->msgId : 0);}} while(0)
#define sGDebug(trace, param, ...) do { if (sDebugFlag & DEBUG_DEBUG) { sDebug(param ", QID:0x%" PRIx64 ":0x%" PRIx64, __VA_ARGS__, (trace) ? (trace)->rootId : 0, (trace) ? (trace)->msgId : 0);}} while(0)

#define sLFatal(...) if (sDebugFlag & DEBUG_FATAL) { taosPrintLongString("SYN FATAL ", DEBUG_FATAL, 255,        __VA_ARGS__); }
#define sLError(...) if (sDebugFlag & DEBUG_ERROR) { taosPrintLongString("SYN ERROR ", DEBUG_ERROR, 255,        __VA_ARGS__); }
#define sLWarn(...)  if (sDebugFlag & DEBUG_WARN)  { taosPrintLongString("SYN WARN  ", DEBUG_WARN,  255,        __VA_ARGS__); }
#define sLInfo(...)  if (sDebugFlag & DEBUG_INFO)  { taosPrintLongString("SYN INFO  ", DEBUG_INFO,  255,        __VA_ARGS__); }
#define sLDebug(...) if (sDebugFlag & DEBUG_DEBUG) { taosPrintLongString("SYN DEBUG ", DEBUG_DEBUG, sDebugFlag, __VA_ARGS__); }
#define sLTrace(...) if (sDebugFlag & DEBUG_TRACE) { taosPrintLongString("SYN TRACE ", DEBUG_TRACE, sDebugFlag, __VA_ARGS__); }

#define sNFatal(pNode, ...)  if (sDebugFlag & DEBUG_FATAL) { syncPrintNodeLog("SYN FATAL ", DEBUG_FATAL, 255, true,        pNode,  __VA_ARGS__); }
#define sNError(pNode, ...)  if (sDebugFlag & DEBUG_ERROR) { syncPrintNodeLog("SYN ERROR ", DEBUG_ERROR, 255, true,        pNode,  __VA_ARGS__); }
#define sNWarn(pNode, ...)   if (sDebugFlag & DEBUG_WARN)  { syncPrintNodeLog("SYN WARN  ", DEBUG_WARN,  255, true,        pNode,  __VA_ARGS__); }
#define sNInfo(pNode, ...)   if (sDebugFlag & DEBUG_INFO)  { syncPrintNodeLog("SYN INFO  ", DEBUG_INFO,  255, true,        pNode, __VA_ARGS__); }
#define sNDebug(pNode, ...)  if (sDebugFlag & DEBUG_DEBUG) { syncPrintNodeLog("SYN DEBUG ", DEBUG_DEBUG, sDebugFlag, true, pNode,  __VA_ARGS__); }
#define sNTrace(pNode, ...)  if (sDebugFlag & DEBUG_TRACE) { syncPrintNodeLog("SYN TRACE ", DEBUG_TRACE, sDebugFlag, true, pNode,  __VA_ARGS__); }

#define sHFatal(pNode, ...)  if (sDebugFlag & DEBUG_FATAL) { syncPrintHbLog("SYN FATAL ", DEBUG_FATAL, 255, true,        pNode,  __VA_ARGS__); }
#define sHError(pNode, ...)  if (sDebugFlag & DEBUG_ERROR) { syncPrintHbLog("SYN ERROR ", DEBUG_ERROR, 255, true,        pNode,  __VA_ARGS__); }
#define sHWarn(pNode, ...)   if (sDebugFlag & DEBUG_WARN)  { syncPrintHbLog("SYN WARN  ", DEBUG_WARN,  255, true,        pNode,  __VA_ARGS__); }
#define sHInfo(pNode, ...)   if (sDebugFlag & DEBUG_INFO)  { syncPrintHbLog("SYN INFO  ", DEBUG_INFO,  255, true,        pNode, __VA_ARGS__); }
#define sHDebug(pNode, ...)  if (sDebugFlag & DEBUG_DEBUG) { syncPrintHbLog("SYN DEBUG ", DEBUG_DEBUG, sDebugFlag, true, pNode,  __VA_ARGS__); }
#define sHTrace(pNode, ...)  if (sDebugFlag & DEBUG_TRACE) { syncPrintHbLog("SYN TRACE ", DEBUG_TRACE, sDebugFlag, true, pNode,  __VA_ARGS__); }

#define sSFatal(pSender, ...)  if (sDebugFlag & DEBUG_FATAL) { syncPrintSnapshotSenderLog("SYN FATAL ", DEBUG_FATAL, 255,        pSender, __VA_ARGS__); }
#define sSError(pSender, ...)  if (sDebugFlag & DEBUG_ERROR) { syncPrintSnapshotSenderLog("SYN ERROR ", DEBUG_ERROR, 255,        pSender, __VA_ARGS__); }
#define sSWarn(pSender, ...)   if (sDebugFlag & DEBUG_WARN)  { syncPrintSnapshotSenderLog("SYN WARN  ", DEBUG_WARN,  255,        pSender, __VA_ARGS__); }
#define sSInfo(pSender, ...)   if (sDebugFlag & DEBUG_INFO)  { syncPrintSnapshotSenderLog("SYN INFO  ", DEBUG_INFO,  255,        pSender, __VA_ARGS__); }
#define sSDebug(pSender, ...)  if (sDebugFlag & DEBUG_DEBUG) { syncPrintSnapshotSenderLog("SYN DEBUG ", DEBUG_DEBUG, sDebugFlag, pSender, __VA_ARGS__); }
#define sSTrace(pSender, ...)  if (sDebugFlag & DEBUG_TRACE) { syncPrintSnapshotSenderLog("SYN TRACE ", DEBUG_TRACE, sDebugFlag, pSender, __VA_ARGS__); }

#define sRFatal(pReceiver, ...)  if (sDebugFlag & DEBUG_FATAL) { syncPrintSnapshotReceiverLog("SYN FATAL ", DEBUG_FATAL, 255,        pReceiver, __VA_ARGS__); }
#define sRError(pReceiver, ...)  if (sDebugFlag & DEBUG_ERROR) { syncPrintSnapshotReceiverLog("SYN ERROR ", DEBUG_ERROR, 255,        pReceiver, __VA_ARGS__); }
#define sRWarn(pReceiver, ...)   if (sDebugFlag & DEBUG_WARN)  { syncPrintSnapshotReceiverLog("SYN WARN  ", DEBUG_WARN,  255,        pReceiver, __VA_ARGS__); }
#define sRInfo(pReceiver, ...)   if (sDebugFlag & DEBUG_INFO)  { syncPrintSnapshotReceiverLog("SYN INFO  ", DEBUG_INFO,  255,        pReceiver, __VA_ARGS__); }
#define sRDebug(pReceiver, ...)  if (sDebugFlag & DEBUG_DEBUG) { syncPrintSnapshotReceiverLog("SYN DEBUG ", DEBUG_DEBUG, sDebugFlag, pReceiver, __VA_ARGS__); }
#define sRTrace(pReceiver, ...)  if (sDebugFlag & DEBUG_TRACE) { syncPrintSnapshotReceiverLog("SYN TRACE ", DEBUG_TRACE, sDebugFlag, pReceiver, __VA_ARGS__); }

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

void syncUtilGenerateArbToken(int32_t nodeId, int32_t groupId, char* buf);

void syncPrintNodeLog(const char* flags, ELogLevel level, int32_t dflag, bool formatTime, SSyncNode* pNode,
                      const char* format, ...);
void syncPrintHbLog(const char* flags, ELogLevel level, int32_t dflag, bool formatTime, SSyncNode* pNode,
                    const char* format, ...);
void syncPrintSnapshotSenderLog(const char* flags, ELogLevel level, int32_t dflag, SSyncSnapshotSender* pSender,
                                const char* format, ...);
void syncPrintSnapshotReceiverLog(const char* flags, ELogLevel level, int32_t dflag, SSyncSnapshotReceiver* pReceiver,
                                  const char* format, ...);

void syncLogRecvTimer(SSyncNode* pSyncNode, const SyncTimeout* pMsg, const STraceId* trace);
void syncLogRecvLocalCmd(SSyncNode* pSyncNode, const SyncLocalCmd* pMsg, const STraceId* trace);

void syncLogSendAppendEntriesReply(SSyncNode* pSyncNode, const SyncAppendEntriesReply* pMsg, const char* s,
                                   const STraceId* trace);
void syncLogRecvAppendEntriesReply(SSyncNode* pSyncNode, const SyncAppendEntriesReply* pMsg, const char* s,
                                   const STraceId* trace);

void syncLogSendHeartbeat(SSyncNode* pSyncNode, const SyncHeartbeat* pMsg, bool printX, int64_t timerElapsed,
                          int64_t execTime);
void syncLogRecvHeartbeat(SSyncNode* pSyncNode, const SyncHeartbeat* pMsg, int64_t netElapsed, const STraceId* trace,
                          int64_t timeDiff);

void syncLogSendHeartbeatReply(SSyncNode* pSyncNode, const SyncHeartbeatReply* pMsg, const char* s,
                               const STraceId* trace);
void syncLogRecvHeartbeatReply(SSyncNode* pSyncNode, const SyncHeartbeatReply* pMsg, int64_t netElapse,
                               const STraceId* trace, int64_t timeDiff);

void syncLogSendSyncSnapshotSend(SSyncNode* pSyncNode, const SyncSnapshotSend* pMsg, const char* s,
                                 const STraceId* trace);
void syncLogRecvSyncSnapshotSend(SSyncNode* pSyncNode, const SyncSnapshotSend* pMsg, const char* s,
                                 const STraceId* trace);

void syncLogSendSyncSnapshotRsp(SSyncNode* pSyncNode, const SyncSnapshotRsp* pMsg, const char* s,
                                const STraceId* trace);
void syncLogRecvSyncSnapshotRsp(SSyncNode* pSyncNode, const SyncSnapshotRsp* pMsg, const char* s,
                                const STraceId* trace);

void syncLogRecvAppendEntries(SSyncNode* pSyncNode, const SyncAppendEntries* pMsg, const char* s,
                              const STraceId* trace);
void syncLogSendAppendEntries(SSyncNode* pSyncNode, const SyncAppendEntries* pMsg, const char* s,
                              const STraceId* trace);

void syncLogRecvRequestVote(SSyncNode* pSyncNode, const SyncRequestVote* pMsg, int32_t voteGranted, const char* s,
                            const char* opt, const STraceId* trace);
void syncLogSendRequestVote(SSyncNode* pSyncNode, const SyncRequestVote* pMsg, const char* s, const STraceId* trace);

void syncLogRecvRequestVoteReply(SSyncNode* pSyncNode, const SyncRequestVoteReply* pMsg, const char* s,
                                 const STraceId* trace);
void syncLogSendRequestVoteReply(SSyncNode* pSyncNode, const SyncRequestVoteReply* pMsg, const char* s,
                                 const STraceId* trace);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_UTIL_H*/
