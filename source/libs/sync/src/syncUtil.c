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

#define _DEFAULT_SOURCE
#include "syncUtil.h"
#include "syncIndexMgr.h"
#include "syncMessage.h"
#include "syncRaftCfg.h"
#include "syncRaftStore.h"
#include "syncSnapshot.h"

void syncCfg2SimpleStr(const SSyncCfg* pCfg, char* buf, int32_t bufLen) {
  int32_t len = snprintf(buf, bufLen, "{r-num:%d, my:%d, ", pCfg->replicaNum, pCfg->myIndex);

  for (int32_t i = 0; i < pCfg->replicaNum; ++i) {
    if (i < pCfg->replicaNum - 1) {
      len += snprintf(buf + len, bufLen - len, "%s:%d, ", pCfg->nodeInfo[i].nodeFqdn, pCfg->nodeInfo[i].nodePort);
    } else {
      len += snprintf(buf + len, bufLen - len, "%s:%d}", pCfg->nodeInfo[i].nodeFqdn, pCfg->nodeInfo[i].nodePort);
    }
  }
}

void syncUtilNodeInfo2EpSet(const SNodeInfo* pInfo, SEpSet* pEpSet) {
  pEpSet->inUse = 0;
  pEpSet->numOfEps = 1;
  pEpSet->eps[0].port = pInfo->nodePort;
  tstrncpy(pEpSet->eps[0].fqdn, pInfo->nodeFqdn, TSDB_FQDN_LEN);
}

bool syncUtilNodeInfo2RaftId(const SNodeInfo* pInfo, SyncGroupId vgId, SRaftId* raftId) {
  uint32_t ipv4 = taosGetIpv4FromFqdn(pInfo->nodeFqdn);
  if (ipv4 == 0xFFFFFFFF || ipv4 == 1) {
    sError("failed to resolve ipv4 addr, fqdn: %s", pInfo->nodeFqdn);
    terrno = TSDB_CODE_TSC_INVALID_FQDN;
    return false;
  }

  char ipbuf[128] = {0};
  tinet_ntoa(ipbuf, ipv4);
  raftId->addr = SYNC_ADDR(pInfo);
  raftId->vgId = vgId;

  sInfo("vgId:%d, sync addr:%" PRIu64 ", dnode:%d cluster:%" PRId64 " fqdn:%s ip:%s port:%u ipv4:%u", vgId,
        raftId->addr, pInfo->nodeId, pInfo->clusterId, pInfo->nodeFqdn, ipbuf, pInfo->nodePort, ipv4);
  return true;
}

bool syncUtilSameId(const SRaftId* pId1, const SRaftId* pId2) {
  if (pId1->addr == pId2->addr && pId1->vgId == pId2->vgId) return true;
  if ((CID(pId1) == 0 || CID(pId2) == 0) && (DID(pId1) == DID(pId2)) && pId1->vgId == pId2->vgId) return true;
  return false;
}

bool syncUtilEmptyId(const SRaftId* pId) { return (pId->addr == 0 && pId->vgId == 0); }

static inline int32_t syncUtilRand(int32_t max) { return taosRand() % max; }

int32_t syncUtilElectRandomMS(int32_t min, int32_t max) {
  int32_t rdm = min + syncUtilRand(max - min);

  // sDebug("random min:%d, max:%d, rdm:%d", min, max, rdm);
  return rdm;
}

int32_t syncUtilQuorum(int32_t replicaNum) { return replicaNum / 2 + 1; }

void syncUtilMsgHtoN(void* msg) {
  SMsgHead* pHead = msg;
  pHead->contLen = htonl(pHead->contLen);
  pHead->vgId = htonl(pHead->vgId);
}

bool syncUtilUserPreCommit(tmsg_t msgType) { return msgType != TDMT_SYNC_NOOP && msgType != TDMT_SYNC_LEADER_TRANSFER; }

bool syncUtilUserRollback(tmsg_t msgType) { return msgType != TDMT_SYNC_NOOP && msgType != TDMT_SYNC_LEADER_TRANSFER; }

// for leader
static void syncHearbeatReplyTime2Str(SSyncNode* pSyncNode, char* buf, int32_t bufLen) {
  int32_t len = 5;

  for (int32_t i = 0; i < pSyncNode->replicaNum; ++i) {
    int64_t tsMs = syncIndexMgrGetRecvTime(pSyncNode->pMatchIndex, &(pSyncNode->replicasId[i]));

    if (i < pSyncNode->replicaNum - 1) {
      len += snprintf(buf + len, bufLen - len, "%d:%" PRId64 ",", i, tsMs);
    } else {
      len += snprintf(buf + len, bufLen - len, "%d:%" PRId64 "}", i, tsMs);
    }
  }
}

// for follower
static void syncHearbeatTime2Str(SSyncNode* pSyncNode, char* buf, int32_t bufLen) {
  int32_t len = 4;

  for (int32_t i = 0; i < pSyncNode->replicaNum; ++i) {
    int64_t tsMs = syncIndexMgrGetRecvTime(pSyncNode->pNextIndex, &(pSyncNode->replicasId[i]));

    if (i < pSyncNode->replicaNum - 1) {
      len += snprintf(buf + len, bufLen - len, "%d:%" PRId64 ",", i, tsMs);
    } else {
      len += snprintf(buf + len, bufLen - len, "%d:%" PRId64 "}", i, tsMs);
    }
  }
}

static void syncPeerState2Str(SSyncNode* pSyncNode, char* buf, int32_t bufLen) {
  int32_t len = 1;

  for (int32_t i = 0; i < pSyncNode->replicaNum; ++i) {
    SPeerState* pState = syncNodeGetPeerState(pSyncNode, &(pSyncNode->replicasId[i]));
    if (pState == NULL) break;

    if (i < pSyncNode->replicaNum - 1) {
      len += snprintf(buf + len, bufLen - len, "%d:%" PRId64 " %" PRId64 ", ", i, pState->lastSendIndex,
                      pState->lastSendTime);
    } else {
      len += snprintf(buf + len, bufLen - len, "%d:%" PRId64 " %" PRId64 "}", i, pState->lastSendIndex,
                      pState->lastSendTime);
    }
  }
}

void syncPrintNodeLog(const char* flags, ELogLevel level, int32_t dflag, SSyncNode* pNode, const char* format, ...) {
  if (pNode == NULL || pNode->pRaftStore == NULL || pNode->pLogStore == NULL) return;
  int64_t currentTerm = pNode->pRaftStore->currentTerm;

  // save error code, otherwise it will be overwritten
  int32_t errCode = terrno;

  SSnapshot snapshot = {.data = NULL, .lastApplyIndex = -1, .lastApplyTerm = 0};
  if (pNode->pFsm != NULL && pNode->pFsm->FpGetSnapshotInfo != NULL) {
    pNode->pFsm->FpGetSnapshotInfo(pNode->pFsm, &snapshot);
  }

  SyncIndex logLastIndex = SYNC_INDEX_INVALID;
  SyncIndex logBeginIndex = SYNC_INDEX_INVALID;
  if (pNode->pLogStore != NULL) {
    logLastIndex = pNode->pLogStore->syncLogLastIndex(pNode->pLogStore);
    logBeginIndex = pNode->pLogStore->syncLogBeginIndex(pNode->pLogStore);
  }

  int32_t cacheHit = pNode->pLogStore->cacheHit;
  int32_t cacheMiss = pNode->pLogStore->cacheMiss;

  char cfgStr[1024];
  syncCfg2SimpleStr(&pNode->raftCfg.cfg, cfgStr, sizeof(cfgStr));

  char peerStr[1024] = "{";
  syncPeerState2Str(pNode, peerStr, sizeof(peerStr));

  char hbrTimeStr[256] = "hbr:{";
  syncHearbeatReplyTime2Str(pNode, hbrTimeStr, sizeof(hbrTimeStr));

  char hbTimeStr[256] = "hb:{";
  syncHearbeatTime2Str(pNode, hbTimeStr, sizeof(hbTimeStr));

  char    eventLog[512];  // {0};
  va_list argpointer;
  va_start(argpointer, format);
  int32_t writeLen = vsnprintf(eventLog, sizeof(eventLog), format, argpointer);
  va_end(argpointer);

  int32_t aqItems = 0;
  if (pNode != NULL && pNode->pFsm != NULL && pNode->pFsm->FpApplyQueueItems != NULL) {
    aqItems = pNode->pFsm->FpApplyQueueItems(pNode->pFsm);
  }

  // restore error code
  terrno = errCode;

  if (pNode != NULL) {
    taosPrintLog(flags, level, dflag,
                 "vgId:%d, %s, sync:%s, term:%" PRIu64 ", commit-index:%" PRId64 ", first-ver:%" PRId64
                 ", last-ver:%" PRId64 ", min:%" PRId64 ", snap:%" PRId64 ", snap-term:%" PRIu64
                 ", elect-times:%d, as-leader-times:%d, cfg-ch-times:%d, hit:%d, mis:%d, hb-slow:%d, hbr-slow:%d, "
                 "aq-items:%d, snaping:%" PRId64 ", replicas:%d, last-cfg:%" PRId64
                 ", chging:%d, restore:%d, quorum:%d, elect-lc-timer:%" PRId64 ", hb:%" PRId64 ", %s, %s, %s, %s",
                 pNode->vgId, eventLog, syncStr(pNode->state), currentTerm, pNode->commitIndex, logBeginIndex,
                 logLastIndex, pNode->minMatchIndex, snapshot.lastApplyIndex, snapshot.lastApplyTerm, pNode->electNum,
                 pNode->becomeLeaderNum, pNode->configChangeNum, cacheHit, cacheMiss, pNode->hbSlowNum,
                 pNode->hbrSlowNum, aqItems, pNode->snapshottingIndex, pNode->replicaNum,
                 pNode->raftCfg.lastConfigIndex, pNode->changing, pNode->restoreFinish, syncNodeDynamicQuorum(pNode),
                 pNode->electTimerLogicClock, pNode->heartbeatTimerLogicClockUser, peerStr, cfgStr, hbTimeStr,
                 hbrTimeStr);
  }
}

void syncPrintSnapshotSenderLog(const char* flags, ELogLevel level, int32_t dflag, SSyncSnapshotSender* pSender,
                                const char* format, ...) {
  SSyncNode* pNode = pSender->pSyncNode;
  if (pNode == NULL || pNode->pRaftStore == NULL || pNode->pLogStore == NULL) return;

  SSnapshot snapshot = {.data = NULL, .lastApplyIndex = -1, .lastApplyTerm = 0};
  if (pNode->pFsm != NULL && pNode->pFsm->FpGetSnapshotInfo != NULL) {
    pNode->pFsm->FpGetSnapshotInfo(pNode->pFsm, &snapshot);
  }

  SyncIndex logLastIndex = SYNC_INDEX_INVALID;
  SyncIndex logBeginIndex = SYNC_INDEX_INVALID;
  if (pNode->pLogStore != NULL) {
    logLastIndex = pNode->pLogStore->syncLogLastIndex(pNode->pLogStore);
    logBeginIndex = pNode->pLogStore->syncLogBeginIndex(pNode->pLogStore);
  }

  char cfgStr[1024];
  syncCfg2SimpleStr(&pNode->raftCfg.cfg, cfgStr, sizeof(cfgStr));

  char peerStr[1024] = "{";
  syncPeerState2Str(pNode, peerStr, sizeof(peerStr));

  char    eventLog[512];  // {0};
  va_list argpointer;
  va_start(argpointer, format);
  int32_t writeLen = vsnprintf(eventLog, sizeof(eventLog), format, argpointer);
  va_end(argpointer);

  taosPrintLog(flags, level, dflag,
               "vgId:%d, %s, sync:%s, {%p s-param:%" PRId64 " e-param:%" PRId64 " laindex:%" PRId64 " laterm:%" PRIu64
               " lcindex:%" PRId64
               " seq:%d ack:%d finish:%d replica-index:%d dnode:%d}"
               ", tm:%" PRIu64 ", cmt:%" PRId64 ", fst:%" PRId64 ", lst:%" PRId64 ", min:%" PRId64 ", snap:%" PRId64
               ", snap-tm:%" PRIu64 ", sby:%d, stgy:%d, bch:%d, r-num:%d, lcfg:%" PRId64
               ", chging:%d, rsto:%d, dquorum:%d, elt:%" PRId64 ", hb:%" PRId64 ", %s, %s",
               pNode->vgId, eventLog, syncStr(pNode->state), pSender, pSender->snapshotParam.start,
               pSender->snapshotParam.end, pSender->snapshot.lastApplyIndex, pSender->snapshot.lastApplyTerm,
               pSender->snapshot.lastConfigIndex, pSender->seq, pSender->ack, pSender->finish, pSender->replicaIndex,
               DID(&pNode->replicasId[pSender->replicaIndex]), pNode->pRaftStore->currentTerm, pNode->commitIndex,
               logBeginIndex, logLastIndex, pNode->minMatchIndex, snapshot.lastApplyIndex, snapshot.lastApplyTerm,
               pNode->raftCfg.isStandBy, pNode->raftCfg.snapshotStrategy, pNode->raftCfg.batchSize, pNode->replicaNum,
               pNode->raftCfg.lastConfigIndex, pNode->changing, pNode->restoreFinish, syncNodeDynamicQuorum(pNode),
               pNode->electTimerLogicClock, pNode->heartbeatTimerLogicClockUser, peerStr, cfgStr);
}

void syncPrintSnapshotReceiverLog(const char* flags, ELogLevel level, int32_t dflag, SSyncSnapshotReceiver* pReceiver,
                                  const char* format, ...) {
  SSyncNode* pNode = pReceiver->pSyncNode;
  if (pNode == NULL || pNode->pRaftStore == NULL || pNode->pLogStore == NULL) return;

  SSnapshot snapshot = {.data = NULL, .lastApplyIndex = -1, .lastApplyTerm = 0};
  if (pNode->pFsm != NULL && pNode->pFsm->FpGetSnapshotInfo != NULL) {
    pNode->pFsm->FpGetSnapshotInfo(pNode->pFsm, &snapshot);
  }

  SyncIndex logLastIndex = SYNC_INDEX_INVALID;
  SyncIndex logBeginIndex = SYNC_INDEX_INVALID;
  if (pNode->pLogStore != NULL) {
    logLastIndex = pNode->pLogStore->syncLogLastIndex(pNode->pLogStore);
    logBeginIndex = pNode->pLogStore->syncLogBeginIndex(pNode->pLogStore);
  }

  char cfgStr[1024];
  syncCfg2SimpleStr(&pNode->raftCfg.cfg, cfgStr, sizeof(cfgStr));

  char peerStr[1024] = "{";
  syncPeerState2Str(pNode, peerStr, sizeof(peerStr));

  char    eventLog[512];  // {0};
  va_list argpointer;
  va_start(argpointer, format);
  int32_t writeLen = vsnprintf(eventLog, sizeof(eventLog), format, argpointer);
  va_end(argpointer);

  taosPrintLog(flags, level, dflag,
               "vgId:%d, %s, sync:%s,"
               " {%p start:%d ack:%d term:%" PRIu64 " start-time:%" PRId64 " from dnode:%d s-param:%" PRId64
               " e-param:%" PRId64 " laindex:%" PRId64 " laterm:%" PRIu64 " lcindex:%" PRId64
               "}"
               ", tm:%" PRIu64 ", cmt:%" PRId64 ", fst:%" PRId64 ", lst:%" PRId64 ", min:%" PRId64 ", snap:%" PRId64
               ", snap-tm:%" PRIu64 ", sby:%d, stgy:%d, bch:%d, r-num:%d, lcfg:%" PRId64
               ", chging:%d, rsto:%d, dquorum:%d, elt:%" PRId64 ", hb:%" PRId64 ", %s, %s",
               pNode->vgId, eventLog, syncStr(pNode->state), pReceiver, pReceiver->start, pReceiver->ack,
               pReceiver->term, pReceiver->startTime, DID(&pReceiver->fromId), pReceiver->snapshotParam.start,
               pReceiver->snapshotParam.end, pReceiver->snapshot.lastApplyIndex, pReceiver->snapshot.lastApplyTerm,
               pReceiver->snapshot.lastConfigIndex, pNode->pRaftStore->currentTerm, pNode->commitIndex, logBeginIndex,
               logLastIndex, pNode->minMatchIndex, snapshot.lastApplyIndex, snapshot.lastApplyTerm,
               pNode->raftCfg.isStandBy, pNode->raftCfg.snapshotStrategy, pNode->raftCfg.batchSize, pNode->replicaNum,
               pNode->raftCfg.lastConfigIndex, pNode->changing, pNode->restoreFinish, syncNodeDynamicQuorum(pNode),
               pNode->electTimerLogicClock, pNode->heartbeatTimerLogicClockUser, peerStr, cfgStr);
}

void syncLogRecvTimer(SSyncNode* pSyncNode, const SyncTimeout* pMsg, const char* s) {
  if (!(sDebugFlag & DEBUG_TRACE)) return;

  int64_t tsNow = taosGetTimestampMs();
  int64_t timeDIff = tsNow - pMsg->timeStamp;
  sNTrace(
      pSyncNode, "recv sync-timer {type:%s, lc:%" PRId64 ", ms:%d, ts:%" PRId64 ", elapsed:%" PRId64 ", data:%p}, %s",
      syncTimerTypeStr(pMsg->timeoutType), pMsg->logicClock, pMsg->timerMS, pMsg->timeStamp, timeDIff, pMsg->data, s);
}

void syncLogRecvLocalCmd(SSyncNode* pSyncNode, const SyncLocalCmd* pMsg, const char* s) {
  sNTrace(pSyncNode, "recv sync-local-cmd {cmd:%d-%s, sd-new-term:%" PRId64 ", fc-index:%" PRId64 "}, %s", pMsg->cmd,
          syncLocalCmdGetStr(pMsg->cmd), pMsg->sdNewTerm, pMsg->fcIndex, s);
}

void syncLogSendAppendEntriesReply(SSyncNode* pSyncNode, const SyncAppendEntriesReply* pMsg, const char* s) {
  sNTrace(pSyncNode,
          "send sync-append-entries-reply to dnode:%d, {term:%" PRId64 ", pterm:%" PRId64
          ", success:%d, lsend-index:%" PRId64 ", match:%" PRId64 "}, %s",
          DID(&pMsg->destId), pMsg->term, pMsg->lastMatchTerm, pMsg->success, pMsg->lastSendIndex, pMsg->matchIndex, s);
}

void syncLogRecvAppendEntriesReply(SSyncNode* pSyncNode, const SyncAppendEntriesReply* pMsg, const char* s) {
  sNTrace(pSyncNode,
          "recv sync-append-entries-reply from dnode:%d {term:%" PRId64 ", pterm:%" PRId64
          ", success:%d, lsend-index:%" PRId64 ", match:%" PRId64 "}, %s",
          DID(&pMsg->srcId), pMsg->term, pMsg->lastMatchTerm, pMsg->success, pMsg->lastSendIndex, pMsg->matchIndex, s);
}

void syncLogSendHeartbeat(SSyncNode* pSyncNode, const SyncHeartbeat* pMsg, bool printX, int64_t timerElapsed,
                          int64_t execTime) {
  if (printX) {
    sNTrace(pSyncNode,
            "send sync-heartbeat to dnode:%d {term:%" PRId64 ", cmt:%" PRId64 ", min-match:%" PRId64 ", ts:%" PRId64
            "}, x",
            DID(&pMsg->destId), pMsg->term, pMsg->commitIndex, pMsg->minMatchIndex, pMsg->timeStamp);
  } else {
    sNTrace(pSyncNode,
            "send sync-heartbeat to dnode:%d {term:%" PRId64 ", cmt:%" PRId64 ", min-match:%" PRId64 ", ts:%" PRId64
            "}, timer-elapsed:%" PRId64 ", next-exec:%" PRId64,
            DID(&pMsg->destId), pMsg->term, pMsg->commitIndex, pMsg->minMatchIndex, pMsg->timeStamp, timerElapsed,
            execTime);
  }
}

void syncLogRecvHeartbeat(SSyncNode* pSyncNode, const SyncHeartbeat* pMsg, int64_t timeDiff, const char* s) {
  if (timeDiff > SYNC_HEARTBEAT_SLOW_MS) {
    pSyncNode->hbSlowNum++;

    sNInfo(pSyncNode,
           "recv sync-heartbeat from dnode:%d slow {term:%" PRId64 ", cmt:%" PRId64 ", min-match:%" PRId64
           ", ts:%" PRId64 "}, %s, net elapsed:%" PRId64,
           DID(&pMsg->srcId), pMsg->term, pMsg->commitIndex, pMsg->minMatchIndex, pMsg->timeStamp, s, timeDiff);
  }

  sNTrace(pSyncNode,
          "recv sync-heartbeat from dnode:%d {term:%" PRId64 ", cmt:%" PRId64 ", min-match:%" PRId64 ", ts:%" PRId64
          "}, %s, net elapsed:%" PRId64,
          DID(&pMsg->srcId), pMsg->term, pMsg->commitIndex, pMsg->minMatchIndex, pMsg->timeStamp, s, timeDiff);
}

void syncLogSendHeartbeatReply(SSyncNode* pSyncNode, const SyncHeartbeatReply* pMsg, const char* s) {
  sNTrace(pSyncNode, "send sync-heartbeat-reply from dnode:%d {term:%" PRId64 ", ts:%" PRId64 "}, %s",
          DID(&pMsg->destId), pMsg->term, pMsg->timeStamp, s);
}

void syncLogRecvHeartbeatReply(SSyncNode* pSyncNode, const SyncHeartbeatReply* pMsg, int64_t timeDiff, const char* s) {
  if (timeDiff > SYNC_HEARTBEAT_REPLY_SLOW_MS) {
    pSyncNode->hbrSlowNum++;

    sNTrace(pSyncNode,
            "recv sync-heartbeat-reply from dnode:%d slow {term:%" PRId64 ", ts:%" PRId64 "}, %s, net elapsed:%" PRId64,
            DID(&pMsg->srcId), pMsg->term, pMsg->timeStamp, s, timeDiff);
  }

  sNTrace(pSyncNode,
          "recv sync-heartbeat-reply from dnode:%d {term:%" PRId64 ", ts:%" PRId64 "}, %s, net elapsed:%" PRId64,
          DID(&pMsg->srcId), pMsg->term, pMsg->timeStamp, s, timeDiff);
}

void syncLogSendSyncSnapshotSend(SSyncNode* pSyncNode, const SyncSnapshotSend* pMsg, const char* s) {
  sNDebug(pSyncNode,
          "send sync-snapshot-send to dnode:%d, %s, seq:%d, term:%" PRId64 ", begin:%" PRId64 ", end:%" PRId64
          ", lterm:%" PRId64 ", stime:%" PRId64,
          DID(&pMsg->destId), s, pMsg->seq, pMsg->term, pMsg->beginIndex, pMsg->lastIndex, pMsg->lastTerm,
          pMsg->startTime);
}

void syncLogRecvSyncSnapshotSend(SSyncNode* pSyncNode, const SyncSnapshotSend* pMsg, const char* s) {
  sNDebug(pSyncNode,
          "recv sync-snapshot-send from dnode:%d, %s, seq:%d, term:%" PRId64 ", begin:%" PRId64 ", lst:%" PRId64
          ", lterm:%" PRId64 ", stime:%" PRId64 ", len:%u",
          DID(&pMsg->srcId), s, pMsg->seq, pMsg->term, pMsg->beginIndex, pMsg->lastIndex, pMsg->lastTerm,
          pMsg->startTime, pMsg->dataLen);
}

void syncLogSendSyncSnapshotRsp(SSyncNode* pSyncNode, const SyncSnapshotRsp* pMsg, const char* s) {
  sNDebug(pSyncNode,
          "send sync-snapshot-rsp to dnode:%d, %s, ack:%d, term:%" PRId64 ", begin:%" PRId64 ", lst:%" PRId64
          ", lterm:%" PRId64 ", stime:%" PRId64,
          DID(&pMsg->destId), s, pMsg->ack, pMsg->term, pMsg->snapBeginIndex, pMsg->lastIndex, pMsg->lastTerm,
          pMsg->startTime);
}

void syncLogRecvSyncSnapshotRsp(SSyncNode* pSyncNode, const SyncSnapshotRsp* pMsg, const char* s) {
  sNDebug(pSyncNode,
          "recv sync-snapshot-rsp from dnode:%d, %s, ack:%d, term:%" PRId64 ", begin:%" PRId64 ", lst:%" PRId64
          ", lterm:%" PRId64 ", stime:%" PRId64,
          DID(&pMsg->srcId), s, pMsg->ack, pMsg->term, pMsg->snapBeginIndex, pMsg->lastIndex, pMsg->lastTerm,
          pMsg->startTime);
}

void syncLogRecvAppendEntries(SSyncNode* pSyncNode, const SyncAppendEntries* pMsg, const char* s) {
  sNTrace(pSyncNode,
          "recv sync-append-entries from dnode:%d {term:%" PRId64 ", pre-index:%" PRId64 ", pre-term:%" PRId64
          ", cmt:%" PRId64 ", pterm:%" PRId64 ", datalen:%d}, %s",
          DID(&pMsg->srcId), pMsg->term, pMsg->prevLogIndex, pMsg->prevLogTerm, pMsg->commitIndex, pMsg->privateTerm,
          pMsg->dataLen, s);
}

void syncLogSendAppendEntries(SSyncNode* pSyncNode, const SyncAppendEntries* pMsg, const char* s) {
  sNTrace(pSyncNode,
          "send sync-append-entries to dnode:%d, {term:%" PRId64 ", pre-index:%" PRId64 ", pre-term:%" PRId64
          ", lsend-index:%" PRId64 ", cmt:%" PRId64 ", datalen:%d}, %s",
          DID(&pMsg->destId), pMsg->term, pMsg->prevLogIndex, pMsg->prevLogTerm, (pMsg->prevLogIndex + 1),
          pMsg->commitIndex, pMsg->dataLen, s);
}

void syncLogRecvRequestVote(SSyncNode* pSyncNode, const SyncRequestVote* pMsg, int32_t voteGranted, const char* s) {
  if (voteGranted == -1) {
    sNInfo(pSyncNode,
           "recv sync-request-vote from dnode:%d, {term:%" PRId64 ", lindex:%" PRId64 ", lterm:%" PRId64 "}, %s",
           DID(&pMsg->srcId), pMsg->term, pMsg->lastLogIndex, pMsg->lastLogTerm, s);
  } else {
    sNInfo(pSyncNode,
           "recv sync-request-vote from dnode:%d, {term:%" PRId64 ", lindex:%" PRId64 ", lterm:%" PRId64
           "}, granted:%d",
           DID(&pMsg->srcId), pMsg->term, pMsg->lastLogIndex, pMsg->lastLogTerm, voteGranted);
  }
}

void syncLogSendRequestVote(SSyncNode* pNode, const SyncRequestVote* pMsg, const char* s) {
  sNInfo(pNode, "send sync-request-vote to dnode:%d {term:%" PRId64 ", lindex:%" PRId64 ", lterm:%" PRId64 "}, %s",
         DID(&pMsg->destId), pMsg->term, pMsg->lastLogIndex, pMsg->lastLogTerm, s);
}

void syncLogRecvRequestVoteReply(SSyncNode* pSyncNode, const SyncRequestVoteReply* pMsg, const char* s) {
  sNInfo(pSyncNode, "recv sync-request-vote-reply from dnode:%d {term:%" PRId64 ", grant:%d}, %s", DID(&pMsg->srcId),
         pMsg->term, pMsg->voteGranted, s);
}

void syncLogSendRequestVoteReply(SSyncNode* pSyncNode, const SyncRequestVoteReply* pMsg, const char* s) {
  sNInfo(pSyncNode, "send sync-request-vote-reply to dnode:%d {term:%" PRId64 ", grant:%d}, %s", DID(&pMsg->destId),
         pMsg->term, pMsg->voteGranted, s);
}
