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
#include "syncPipeline.h"
#include "syncRaftCfg.h"
#include "syncRaftStore.h"
#include "syncSnapshot.h"
#include "tglobal.h"

void syncCfg2SimpleStr(const SSyncCfg* pCfg, char* buf, int32_t bufLen) {
  int32_t len = snprintf(buf, bufLen, "{num:%d, as:%d, [", pCfg->replicaNum, pCfg->myIndex);
  for (int32_t i = 0; i < pCfg->replicaNum; ++i) {
    len += snprintf(buf + len, bufLen - len, "%s:%d", pCfg->nodeInfo[i].nodeFqdn, pCfg->nodeInfo[i].nodePort);
    if (i < pCfg->replicaNum - 1) {
      len += snprintf(buf + len, bufLen - len, "%s", ", ");
    }
  }
  len += snprintf(buf + len, bufLen - len, "%s", "]}");
}

void syncUtilNodeInfo2EpSet(const SNodeInfo* pInfo, SEpSet* pEpSet) {
  pEpSet->inUse = 0;
  pEpSet->numOfEps = 1;
  pEpSet->eps[0].port = pInfo->nodePort;
  tstrncpy(pEpSet->eps[0].fqdn, pInfo->nodeFqdn, TSDB_FQDN_LEN);
}

bool syncUtilNodeInfo2RaftId(const SNodeInfo* pInfo, SyncGroupId vgId, SRaftId* raftId) {
  uint32_t ipv4 = 0xFFFFFFFF;
  sDebug("vgId:%d, start to resolve sync addr fqdn in %d seconds, "
        "dnode:%d cluster:%" PRId64 " fqdn:%s port:%u ", 
        vgId, tsResolveFQDNRetryTime,
        pInfo->nodeId, pInfo->clusterId, pInfo->nodeFqdn, pInfo->nodePort);
  for(int i = 0; i < tsResolveFQDNRetryTime; i++){
    ipv4 = taosGetIpv4FromFqdn(pInfo->nodeFqdn);
    if (ipv4 == 0xFFFFFFFF || ipv4 == 1) {
      sError("failed to resolve ipv4 addr, fqdn:%s, wait one second", pInfo->nodeFqdn);
      taosSsleep(1);
    }
    else{
      break;
    }
  }
  
  if (ipv4 == 0xFFFFFFFF || ipv4 == 1) {
    sError("failed to resolve ipv4 addr, fqdn:%s", pInfo->nodeFqdn);
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
  int32_t len = 0;
  len += snprintf(buf + len, bufLen - len, "%s", "{");
  for (int32_t i = 0; i < pSyncNode->replicaNum; ++i) {
    int64_t tsMs = syncIndexMgrGetRecvTime(pSyncNode->pMatchIndex, &(pSyncNode->replicasId[i]));
    len += snprintf(buf + len, bufLen - len, "%d:%" PRId64, i, tsMs);
    if (i < pSyncNode->replicaNum - 1) {
      len += snprintf(buf + len, bufLen - len, "%s", ",");
    }
  }
  len += snprintf(buf + len, bufLen - len, "%s", "}");
}

// for follower
static void syncHearbeatTime2Str(SSyncNode* pSyncNode, char* buf, int32_t bufLen) {
  int32_t len = 0;
  len += snprintf(buf + len, bufLen - len, "%s", "{");
  for (int32_t i = 0; i < pSyncNode->replicaNum; ++i) {
    int64_t tsMs = syncIndexMgrGetRecvTime(pSyncNode->pNextIndex, &(pSyncNode->replicasId[i]));
    len += snprintf(buf + len, bufLen - len, "%d:%" PRId64, i, tsMs);
    if (i < pSyncNode->replicaNum - 1) {
      len += snprintf(buf + len, bufLen - len, "%s", ",");
    }
  }
  len += snprintf(buf + len, bufLen - len, "%s", "}");
}

static void syncLogBufferStates2Str(SSyncNode* pSyncNode, char* buf, int32_t bufLen) {
  SSyncLogBuffer* pBuf = pSyncNode->pLogBuf;
  if (pBuf == NULL) {
    return;
  }
  int len = 0;
  len += snprintf(buf + len, bufLen - len, "[%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 ")", pBuf->startIndex,
                  pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);
}

static void syncLogReplStates2Str(SSyncNode* pSyncNode, char* buf, int32_t bufLen) {
  int len = 0;
  len += snprintf(buf + len, bufLen - len, "%s", "{");
  for (int32_t i = 0; i < pSyncNode->replicaNum; i++) {
    SSyncLogReplMgr* pMgr = pSyncNode->logReplMgrs[i];
    if (pMgr == NULL) break;
    len += snprintf(buf + len, bufLen - len, "%d:%d [%" PRId64 " %" PRId64 ", %" PRId64 ")", i, pMgr->restored,
                    pMgr->startIndex, pMgr->matchIndex, pMgr->endIndex);
    if (i + 1 < pSyncNode->replicaNum) {
      len += snprintf(buf + len, bufLen - len, "%s", ", ");
    }
  }
  len += snprintf(buf + len, bufLen - len, "%s", "}");
}

static void syncPeerState2Str(SSyncNode* pSyncNode, char* buf, int32_t bufLen) {
  int32_t len = 0;
  len += snprintf(buf + len, bufLen - len, "%s", "{");
  for (int32_t i = 0; i < pSyncNode->replicaNum; ++i) {
    SPeerState* pState = syncNodeGetPeerState(pSyncNode, &(pSyncNode->replicasId[i]));
    if (pState == NULL) break;
    len += snprintf(buf + len, bufLen - len, "%d:%" PRId64 " %" PRId64 "%s", i, pState->lastSendIndex,
                    pState->lastSendTime, (i < pSyncNode->replicaNum - 1) ? ", " : "");
  }
  len += snprintf(buf + len, bufLen - len, "%s", "}");
}

void syncPrintNodeLog(const char* flags, ELogLevel level, int32_t dflag, SSyncNode* pNode, const char* format, ...) {
  if (pNode == NULL || pNode->pLogStore == NULL) return;
  int64_t currentTerm = raftStoreGetTerm(pNode);

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

  char cfgStr[1024] = "";
  syncCfg2SimpleStr(&pNode->raftCfg.cfg, cfgStr, sizeof(cfgStr));

  char replMgrStatesStr[1024] = "";
  syncLogReplStates2Str(pNode, replMgrStatesStr, sizeof(replMgrStatesStr));

  char bufferStatesStr[256] = "";
  syncLogBufferStates2Str(pNode, bufferStatesStr, sizeof(bufferStatesStr));

  char hbrTimeStr[256] = "";
  syncHearbeatReplyTime2Str(pNode, hbrTimeStr, sizeof(hbrTimeStr));

  char hbTimeStr[256] = "";
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
  SyncIndex appliedIndex = pNode->pFsm->FpAppliedIndexCb(pNode->pFsm);

  if (pNode != NULL) {
    taosPrintLog(flags, level, dflag,
                 "vgId:%d, %s, sync:%s, term:%" PRIu64 ", commit-index:%" PRId64 ", applied-index:%" PRId64
                 ", first-ver:%" PRId64 ", last-ver:%" PRId64 ", min:%" PRId64 ", snap:%" PRId64 ", snap-term:%" PRIu64
                 ", elect-times:%d, as-leader-times:%d, cfg-ch-times:%d, hb-slow:%d, hbr-slow:%d, "
                 "aq-items:%d, snaping:%" PRId64 ", replicas:%d, last-cfg:%" PRId64
                 ", chging:%d, restore:%d, quorum:%d, elect-lc-timer:%" PRId64 ", hb:%" PRId64
                 ", buffer:%s, repl-mgrs:%s, members:%s, hb:%s, hb-reply:%s",
                 pNode->vgId, eventLog, syncStr(pNode->state), currentTerm, pNode->commitIndex, appliedIndex,
                 logBeginIndex, logLastIndex, pNode->minMatchIndex, snapshot.lastApplyIndex, snapshot.lastApplyTerm,
                 pNode->electNum, pNode->becomeLeaderNum, pNode->configChangeNum, pNode->hbSlowNum, pNode->hbrSlowNum,
                 aqItems, pNode->snapshottingIndex, pNode->replicaNum, pNode->raftCfg.lastConfigIndex, pNode->changing,
                 pNode->restoreFinish, syncNodeDynamicQuorum(pNode), pNode->electTimerLogicClock,
                 pNode->heartbeatTimerLogicClockUser, bufferStatesStr, replMgrStatesStr, cfgStr, hbTimeStr, hbrTimeStr);
  }
}

void syncPrintSnapshotSenderLog(const char* flags, ELogLevel level, int32_t dflag, SSyncSnapshotSender* pSender,
                                const char* format, ...) {
  SSyncNode* pNode = pSender->pSyncNode;
  if (pNode == NULL || pNode->pLogStore == NULL) return;

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

  char cfgStr[1024] = "";
  syncCfg2SimpleStr(&pNode->raftCfg.cfg, cfgStr, sizeof(cfgStr));

  char peerStr[1024] = "";
  syncPeerState2Str(pNode, peerStr, sizeof(peerStr));

  char    eventLog[512];  // {0};
  va_list argpointer;
  va_start(argpointer, format);
  int32_t writeLen = vsnprintf(eventLog, sizeof(eventLog), format, argpointer);
  va_end(argpointer);

  taosPrintLog(flags, level, dflag,
               "vgId:%d, %s, sync:%s, snap-sender:%p signature:(%" PRId64 ", %" PRId64 "), {start:%" PRId64
               " end:%" PRId64 " last-index:%" PRId64 " last-term:%" PRId64 " last-cfg:%" PRId64
               ", seq:%d, ack:%d, "
               " buf:[%" PRId64 " %" PRId64 ", %" PRId64
               "), finish:%d, as:%d, to-dnode:%d}"
               ", term:%" PRIu64 ", commit-index:%" PRId64 ", firstver:%" PRId64 ", lastver:%" PRId64
               ", min-match:%" PRId64 ", snap:{last-index:%" PRId64 ", term:%" PRIu64
               "}, standby:%d, batch-sz:%d, replicas:%d, last-cfg:%" PRId64
               ", chging:%d, restore:%d, quorum:%d, peer:%s, cfg:%s",
               pNode->vgId, eventLog, syncStr(pNode->state), pSender, pSender->term, pSender->startTime,
               pSender->snapshotParam.start, pSender->snapshotParam.end, pSender->snapshot.lastApplyIndex,
               pSender->snapshot.lastApplyTerm, pSender->snapshot.lastConfigIndex, pSender->seq, pSender->ack,
               pSender->pSndBuf->start, pSender->pSndBuf->cursor, pSender->pSndBuf->end, pSender->finish,
               pSender->replicaIndex, DID(&pNode->replicasId[pSender->replicaIndex]), raftStoreGetTerm(pNode),
               pNode->commitIndex, logBeginIndex, logLastIndex, pNode->minMatchIndex, snapshot.lastApplyIndex,
               snapshot.lastApplyTerm, pNode->raftCfg.isStandBy, pNode->raftCfg.batchSize, pNode->replicaNum,
               pNode->raftCfg.lastConfigIndex, pNode->changing, pNode->restoreFinish, pNode->quorum, peerStr, cfgStr);
}

void syncPrintSnapshotReceiverLog(const char* flags, ELogLevel level, int32_t dflag, SSyncSnapshotReceiver* pReceiver,
                                  const char* format, ...) {
  SSyncNode* pNode = pReceiver->pSyncNode;
  if (pNode == NULL || pNode->pLogStore == NULL) return;

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

  char cfgStr[1024] = "";
  syncCfg2SimpleStr(&pNode->raftCfg.cfg, cfgStr, sizeof(cfgStr));

  char peerStr[1024] = "";
  syncPeerState2Str(pNode, peerStr, sizeof(peerStr));

  char    eventLog[512];  // {0};
  va_list argpointer;
  va_start(argpointer, format);
  int32_t writeLen = vsnprintf(eventLog, sizeof(eventLog), format, argpointer);
  va_end(argpointer);

  taosPrintLog(
      flags, level, dflag,
      "vgId:%d, %s, sync:%s,"
      " snap-receiver:%p signature:(%" PRId64 ", %" PRId64 "), {start:%d ack:%d buf:[%" PRId64 " %" PRId64 ", %" PRId64
      ")"
      " from-dnode:%d, start:%" PRId64 " end:%" PRId64 " last-index:%" PRId64 " last-term:%" PRIu64 " last-cfg:%" PRId64
      "}"
      ", term:%" PRIu64 ", commit-index:%" PRId64 ", firstver:%" PRId64 ", lastver:%" PRId64 ", min-match:%" PRId64
      ", snap:{last-index:%" PRId64 ", last-term:%" PRIu64 "}, standby:%d, batch-sz:%d, replicas:%d, last-cfg:%" PRId64
      ", chging:%d, restore:%d, quorum:%d, peer:%s, cfg:%s",
      pNode->vgId, eventLog, syncStr(pNode->state), pReceiver, pReceiver->term, pReceiver->startTime, pReceiver->start,
      pReceiver->ack, pReceiver->pRcvBuf->start, pReceiver->pRcvBuf->cursor, pReceiver->pRcvBuf->end,
      DID(&pReceiver->fromId), pReceiver->snapshotParam.start, pReceiver->snapshotParam.end,
      pReceiver->snapshot.lastApplyIndex, pReceiver->snapshot.lastApplyTerm, pReceiver->snapshot.lastConfigIndex,
      raftStoreGetTerm(pNode), pNode->commitIndex, logBeginIndex, logLastIndex, pNode->minMatchIndex,
      snapshot.lastApplyIndex, snapshot.lastApplyTerm, pNode->raftCfg.isStandBy, pNode->raftCfg.batchSize,
      pNode->replicaNum, pNode->raftCfg.lastConfigIndex, pNode->changing, pNode->restoreFinish, pNode->quorum, peerStr,
      cfgStr);
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
          syncLocalCmdGetStr(pMsg->cmd), pMsg->currentTerm, pMsg->commitIndex, s);
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
            "send sync-heartbeat to dnode:%d {term:%" PRId64 ", commit-index:%" PRId64 ", min-match:%" PRId64
            ", ts:%" PRId64 "}, x",
            DID(&pMsg->destId), pMsg->term, pMsg->commitIndex, pMsg->minMatchIndex, pMsg->timeStamp);
  } else {
    sNTrace(pSyncNode,
            "send sync-heartbeat to dnode:%d {term:%" PRId64 ", commit-index:%" PRId64 ", min-match:%" PRId64
            ", ts:%" PRId64 "}, timer-elapsed:%" PRId64 ", next-exec:%" PRId64,
            DID(&pMsg->destId), pMsg->term, pMsg->commitIndex, pMsg->minMatchIndex, pMsg->timeStamp, timerElapsed,
            execTime);
  }
}

void syncLogRecvHeartbeat(SSyncNode* pSyncNode, const SyncHeartbeat* pMsg, int64_t timeDiff, const char* s) {
  if (timeDiff > SYNC_HEARTBEAT_SLOW_MS) {
    pSyncNode->hbSlowNum++;

    sNTrace(pSyncNode,
            "recv sync-heartbeat from dnode:%d slow {term:%" PRId64 ", commit-index:%" PRId64 ", min-match:%" PRId64
            ", ts:%" PRId64 "}, %s, net elapsed:%" PRId64,
            DID(&pMsg->srcId), pMsg->term, pMsg->commitIndex, pMsg->minMatchIndex, pMsg->timeStamp, s, timeDiff);
  }

  sNTrace(pSyncNode,
          "recv sync-heartbeat from dnode:%d {term:%" PRId64 ", commit-index:%" PRId64 ", min-match:%" PRId64
          ", ts:%" PRId64 "}, %s, net elapsed:%" PRId64,
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
          "send sync-snapshot-send to dnode:%d, %s, seq:%d, term:%" PRId64 ", begin-index:%" PRId64
          ", last-index:%" PRId64 ", last-term:%" PRId64 ", start-time:%" PRId64,
          DID(&pMsg->destId), s, pMsg->seq, pMsg->term, pMsg->beginIndex, pMsg->lastIndex, pMsg->lastTerm,
          pMsg->startTime);
}

void syncLogRecvSyncSnapshotSend(SSyncNode* pSyncNode, const SyncSnapshotSend* pMsg, const char* s) {
  sNDebug(pSyncNode,
          "recv sync-snapshot-send from dnode:%d, %s, seq:%d, term:%" PRId64 ", begin-index:%" PRId64
          ", last-index:%" PRId64 ", last-term:%" PRId64 ", start-time:%" PRId64 ", data-len:%u",
          DID(&pMsg->srcId), s, pMsg->seq, pMsg->term, pMsg->beginIndex, pMsg->lastIndex, pMsg->lastTerm,
          pMsg->startTime, pMsg->dataLen);
}

void syncLogSendSyncSnapshotRsp(SSyncNode* pSyncNode, const SyncSnapshotRsp* pMsg, const char* s) {
  sNDebug(pSyncNode,
          "send sync-snapshot-rsp to dnode:%d, %s, acked:%d, term:%" PRId64 ", begin-index:%" PRId64
          ", last-index:%" PRId64 ", last-term:%" PRId64 ", start-time:%" PRId64,
          DID(&pMsg->destId), s, pMsg->ack, pMsg->term, pMsg->snapBeginIndex, pMsg->lastIndex, pMsg->lastTerm,
          pMsg->startTime);
}

void syncLogRecvSyncSnapshotRsp(SSyncNode* pSyncNode, const SyncSnapshotRsp* pMsg, const char* s) {
  sNDebug(pSyncNode,
          "recv sync-snapshot-rsp from dnode:%d, %s, ack:%d, term:%" PRId64 ", begin-index:%" PRId64
          ", last-index:%" PRId64 ", last-term:%" PRId64 ", start-time:%" PRId64,
          DID(&pMsg->srcId), s, pMsg->ack, pMsg->term, pMsg->snapBeginIndex, pMsg->lastIndex, pMsg->lastTerm,
          pMsg->startTime);
}

void syncLogRecvAppendEntries(SSyncNode* pSyncNode, const SyncAppendEntries* pMsg, const char* s) {
  sNTrace(pSyncNode,
          "recv sync-append-entries from dnode:%d {term:%" PRId64 ", prev-log:{index:%" PRId64 ", term:%" PRId64
          "}, commit-index:%" PRId64 ", datalen:%d}, %s",
          DID(&pMsg->srcId), pMsg->term, pMsg->prevLogIndex, pMsg->prevLogTerm, pMsg->commitIndex, pMsg->dataLen, s);
}

void syncLogSendAppendEntries(SSyncNode* pSyncNode, const SyncAppendEntries* pMsg, const char* s) {
  sNTrace(pSyncNode,
          "send sync-append-entries to dnode:%d, {term:%" PRId64 ", prev-log:{index:%" PRId64 ", term:%" PRId64
          "}, index:%" PRId64 ", commit-index:%" PRId64 ", datalen:%d}, %s",
          DID(&pMsg->destId), pMsg->term, pMsg->prevLogIndex, pMsg->prevLogTerm, (pMsg->prevLogIndex + 1),
          pMsg->commitIndex, pMsg->dataLen, s);
}

void syncLogRecvRequestVote(SSyncNode* pSyncNode, const SyncRequestVote* pMsg, int32_t voteGranted,
                            const char* errmsg) {
  char statusMsg[64];
  snprintf(statusMsg, sizeof(statusMsg), "granted:%d", voteGranted);
  sNInfo(pSyncNode,
         "recv sync-request-vote from dnode:%d, {term:%" PRId64 ", last-index:%" PRId64 ", last-term:%" PRId64 "}, %s",
         DID(&pMsg->srcId), pMsg->term, pMsg->lastLogIndex, pMsg->lastLogTerm,
         (voteGranted != -1) ? statusMsg : errmsg);
}

void syncLogSendRequestVote(SSyncNode* pNode, const SyncRequestVote* pMsg, const char* s) {
  sNInfo(pNode,
         "send sync-request-vote to dnode:%d {term:%" PRId64 ", last-index:%" PRId64 ", last-term:%" PRId64 "}, %s",
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

int32_t syncSnapInfoDataRealloc(SSnapshot* pSnap, int32_t size) {
  void* data = taosMemoryRealloc(pSnap->data, size);
  if (data == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  pSnap->data = data;
  return 0;
}
