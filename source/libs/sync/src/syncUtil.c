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
#include "syncRaftCfg.h"
#include "syncRaftStore.h"
#include "syncSnapshot.h"

extern void addEpIntoEpSet(SEpSet* pEpSet, const char* fqdn, uint16_t port);

uint64_t syncUtilAddr2U64(const char* host, uint16_t port) {
  uint32_t hostU32 = taosGetIpv4FromFqdn(host);
  if (hostU32 == (uint32_t)-1) {
    sError("failed to resolve ipv4 addr, host:%s", host);
    terrno = TSDB_CODE_TSC_INVALID_FQDN;
    return -1;
  }

  uint64_t u64 = (((uint64_t)hostU32) << 32) | (((uint32_t)port) << 16);
  return u64;
}

void syncUtilU642Addr(uint64_t u64, char* host, int64_t len, uint16_t* port) {
  uint32_t hostU32 = (uint32_t)((u64 >> 32) & 0x00000000FFFFFFFF);

  struct in_addr addr = {.s_addr = hostU32};
  taosInetNtoa(addr, host, len);
  *port = (uint16_t)((u64 & 0x00000000FFFF0000) >> 16);
}

void syncUtilnodeInfo2EpSet(const SNodeInfo* pInfo, SEpSet* pEpSet) {
  pEpSet->inUse = 0;
  pEpSet->numOfEps = 0;
  addEpIntoEpSet(pEpSet, pInfo->nodeFqdn, pInfo->nodePort);
}

void syncUtilraftId2EpSet(const SRaftId* raftId, SEpSet* pEpSet) {
  char     host[TSDB_FQDN_LEN] = {0};
  uint16_t port;

  syncUtilU642Addr(raftId->addr, host, sizeof(host), &port);
  pEpSet->inUse = 0;
  pEpSet->numOfEps = 0;
  addEpIntoEpSet(pEpSet, host, port);
}

bool syncUtilnodeInfo2raftId(const SNodeInfo* pInfo, SyncGroupId vgId, SRaftId* raftId) {
  uint32_t ipv4 = taosGetIpv4FromFqdn(pInfo->nodeFqdn);
  if (ipv4 == 0xFFFFFFFF || ipv4 == 1) {
    sError("failed to resolve ipv4 addr, fqdn: %s", pInfo->nodeFqdn);
    terrno = TSDB_CODE_TSC_INVALID_FQDN;
    return false;
  }

  char ipbuf[128] = {0};
  tinet_ntoa(ipbuf, ipv4);
  raftId->addr = syncUtilAddr2U64(ipbuf, pInfo->nodePort);
  raftId->vgId = vgId;
  return true;
}

bool syncUtilSameId(const SRaftId* pId1, const SRaftId* pId2) {
  bool ret = pId1->addr == pId2->addr && pId1->vgId == pId2->vgId;
  return ret;
}

bool syncUtilEmptyId(const SRaftId* pId) { return (pId->addr == 0 && pId->vgId == 0); }

static inline int32_t syncUtilRand(int32_t max) { return taosRand() % max; }

int32_t syncUtilElectRandomMS(int32_t min, int32_t max) {
  int32_t rdm = min + syncUtilRand(max - min);

  // sDebug("random min:%d, max:%d, rdm:%d", min, max, rdm);
  return rdm;
}

int32_t syncUtilQuorum(int32_t replicaNum) { return replicaNum / 2 + 1; }

cJSON* syncUtilNodeInfo2Json(const SNodeInfo* p) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  cJSON_AddStringToObject(pRoot, "nodeFqdn", p->nodeFqdn);
  cJSON_AddNumberToObject(pRoot, "nodePort", p->nodePort);

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SNodeInfo", pRoot);
  return pJson;
}

cJSON* syncUtilRaftId2Json(const SRaftId* p) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", p->addr);
  cJSON_AddStringToObject(pRoot, "addr", u64buf);
  char     host[128] = {0};
  uint16_t port;
  syncUtilU642Addr(p->addr, host, sizeof(host), &port);
  cJSON_AddStringToObject(pRoot, "host", host);
  cJSON_AddNumberToObject(pRoot, "port", port);
  cJSON_AddNumberToObject(pRoot, "vgId", p->vgId);

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SRaftId", pRoot);
  return pJson;
}

char* syncUtilRaftId2Str(const SRaftId* p) {
  cJSON* pJson = syncUtilRaftId2Json(p);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

static inline bool syncUtilCanPrint(char c) {
  if (c >= 32 && c <= 126) {
    return true;
  } else {
    return false;
  }
}

char* syncUtilPrintBin(char* ptr, uint32_t len) {
  int64_t memLen = (int64_t)(len + 1);
  char*   s = taosMemoryMalloc(memLen);
  ASSERT(s != NULL);
  memset(s, 0, len + 1);
  memcpy(s, ptr, len);

  for (int32_t i = 0; i < len; ++i) {
    if (!syncUtilCanPrint(s[i])) {
      s[i] = '.';
    }
  }
  return s;
}

char* syncUtilPrintBin2(char* ptr, uint32_t len) {
  uint32_t len2 = len * 4 + 1;
  char*    s = taosMemoryMalloc(len2);
  ASSERT(s != NULL);
  memset(s, 0, len2);

  char* p = s;
  for (int32_t i = 0; i < len; ++i) {
    int32_t n = sprintf(p, "%d,", ptr[i]);
    p += n;
  }
  return s;
}

void syncUtilMsgHtoN(void* msg) {
  // htonl
  SMsgHead* pHead = msg;
  pHead->contLen = htonl(pHead->contLen);
  pHead->vgId = htonl(pHead->vgId);
}

void syncUtilMsgNtoH(void* msg) {
  // ntohl
  SMsgHead* pHead = msg;
  pHead->contLen = ntohl(pHead->contLen);
  pHead->vgId = ntohl(pHead->vgId);
}

bool syncUtilUserPreCommit(tmsg_t msgType) { return msgType != TDMT_SYNC_NOOP && msgType != TDMT_SYNC_LEADER_TRANSFER; }

bool syncUtilUserCommit(tmsg_t msgType) { return msgType != TDMT_SYNC_NOOP && msgType != TDMT_SYNC_LEADER_TRANSFER; }

bool syncUtilUserRollback(tmsg_t msgType) { return msgType != TDMT_SYNC_NOOP && msgType != TDMT_SYNC_LEADER_TRANSFER; }

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
  if (pNode == NULL || pNode->pRaftCfg != NULL && pNode->pRaftStore == NULL || pNode->pLogStore == NULL) return;

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
  syncCfg2SimpleStr(&(pNode->pRaftCfg->cfg), cfgStr, sizeof(cfgStr));

  char peerStr[1024] = "{";
  syncPeerState2Str(pNode, peerStr, sizeof(peerStr));

  int32_t quorum = syncNodeDynamicQuorum(pNode);

  char    eventLog[512];  // {0};
  va_list argpointer;
  va_start(argpointer, format);
  int32_t writeLen = vsnprintf(eventLog, sizeof(eventLog), format, argpointer);
  va_end(argpointer);

  taosPrintLog(flags, level, dflag,
               "vgId:%d, sync %s "
               "%s"
               ", tm:%" PRIu64 ", cmt:%" PRId64 ", fst:%" PRId64 ", lst:%" PRId64 ", min:%" PRId64 ", snap:%" PRId64
               ", snap-tm:%" PRIu64 ", sby:%d, stgy:%d, bch:%d, r-num:%d, lcfg:%" PRId64
               ", chging:%d, rsto:%d, dquorum:%d, elt:%" PRId64 ", hb:%" PRId64 ", %s, %s",
               pNode->vgId, syncStr(pNode->state), eventLog, pNode->pRaftStore->currentTerm, pNode->commitIndex,
               logBeginIndex, logLastIndex, pNode->minMatchIndex, snapshot.lastApplyIndex, snapshot.lastApplyTerm,
               pNode->pRaftCfg->isStandBy, pNode->pRaftCfg->snapshotStrategy, pNode->pRaftCfg->batchSize,
               pNode->replicaNum, pNode->pRaftCfg->lastConfigIndex, pNode->changing, pNode->restoreFinish, quorum,
               pNode->electTimerLogicClock, pNode->heartbeatTimerLogicClockUser, peerStr, cfgStr);
}

void syncPrintSnapshotSenderLog(const char* flags, ELogLevel level, int32_t dflag, SSyncSnapshotSender* pSender,
                                const char* format, ...) {
  SSyncNode* pNode = pSender->pSyncNode;
  if (pNode == NULL || pNode->pRaftCfg != NULL && pNode->pRaftStore == NULL || pNode->pLogStore == NULL) return;

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
  syncCfg2SimpleStr(&(pNode->pRaftCfg->cfg), cfgStr, sizeof(cfgStr));

  char peerStr[1024] = "{";
  syncPeerState2Str(pNode, peerStr, sizeof(peerStr));

  int32_t  quorum = syncNodeDynamicQuorum(pNode);
  SRaftId  destId = pNode->replicasId[pSender->replicaIndex];
  char     host[64];
  uint16_t port;
  syncUtilU642Addr(destId.addr, host, sizeof(host), &port);

  char    eventLog[512];  // {0};
  va_list argpointer;
  va_start(argpointer, format);
  int32_t writeLen = vsnprintf(eventLog, sizeof(eventLog), format, argpointer);
  va_end(argpointer);

  taosPrintLog(flags, level, dflag,
               "vgId:%d, sync %s "
               "%s {%p s-param:%" PRId64 " e-param:%" PRId64 " laindex:%" PRId64 " laterm:%" PRIu64 " lcindex:%" PRId64
               " seq:%d ack:%d finish:%d replica-index:%d %s:%d}"
               ", tm:%" PRIu64 ", cmt:%" PRId64 ", fst:%" PRId64 ", lst:%" PRId64 ", min:%" PRId64 ", snap:%" PRId64
               ", snap-tm:%" PRIu64 ", sby:%d, stgy:%d, bch:%d, r-num:%d, lcfg:%" PRId64
               ", chging:%d, rsto:%d, dquorum:%d, elt:%" PRId64 ", hb:%" PRId64 ", %s, %s",
               pNode->vgId, syncStr(pNode->state), eventLog, pSender, pSender->snapshotParam.start,
               pSender->snapshotParam.end, pSender->snapshot.lastApplyIndex, pSender->snapshot.lastApplyTerm,
               pSender->snapshot.lastConfigIndex, pSender->seq, pSender->ack, pSender->finish, pSender->replicaIndex,
               host, port, pNode->pRaftStore->currentTerm, pNode->commitIndex, logBeginIndex, logLastIndex,
               pNode->minMatchIndex, snapshot.lastApplyIndex, snapshot.lastApplyTerm, pNode->pRaftCfg->isStandBy,
               pNode->pRaftCfg->snapshotStrategy, pNode->pRaftCfg->batchSize, pNode->replicaNum,
               pNode->pRaftCfg->lastConfigIndex, pNode->changing, pNode->restoreFinish, quorum,
               pNode->electTimerLogicClock, pNode->heartbeatTimerLogicClockUser, peerStr, cfgStr);
}

void syncPrintSnapshotReceiverLog(const char* flags, ELogLevel level, int32_t dflag, SSyncSnapshotReceiver* pReceiver,
                                  const char* format, ...) {
  SSyncNode* pNode = pReceiver->pSyncNode;
  if (pNode == NULL || pNode->pRaftCfg != NULL && pNode->pRaftStore == NULL || pNode->pLogStore == NULL) return;

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
  syncCfg2SimpleStr(&(pNode->pRaftCfg->cfg), cfgStr, sizeof(cfgStr));

  char peerStr[1024] = "{";
  syncPeerState2Str(pNode, peerStr, sizeof(peerStr));

  int32_t  quorum = syncNodeDynamicQuorum(pNode);
  SRaftId  fromId = pReceiver->fromId;
  char     host[128];
  uint16_t port;
  syncUtilU642Addr(fromId.addr, host, sizeof(host), &port);

  char    eventLog[512];  // {0};
  va_list argpointer;
  va_start(argpointer, format);
  int32_t writeLen = vsnprintf(eventLog, sizeof(eventLog), format, argpointer);
  va_end(argpointer);

  taosPrintLog(flags, level, dflag,
               "vgId:%d, sync %s "
               "%s {%p start:%d ack:%d term:%" PRIu64 " start-time:%" PRId64 " from:%s:%d s-param:%" PRId64
               " e-param:%" PRId64 " laindex:%" PRId64 " laterm:%" PRIu64 " lcindex:%" PRId64
               "}"
               ", tm:%" PRIu64 ", cmt:%" PRId64 ", fst:%" PRId64 ", lst:%" PRId64 ", min:%" PRId64 ", snap:%" PRId64
               ", snap-tm:%" PRIu64 ", sby:%d, stgy:%d, bch:%d, r-num:%d, lcfg:%" PRId64
               ", chging:%d, rsto:%d, dquorum:%d, elt:%" PRId64 ", hb:%" PRId64 ", %s, %s",
               pNode->vgId, syncStr(pNode->state), eventLog, pReceiver, pReceiver->start, pReceiver->ack,
               pReceiver->term, pReceiver->startTime, host, port, pReceiver->snapshotParam.start,
               pReceiver->snapshotParam.end, pReceiver->snapshot.lastApplyIndex, pReceiver->snapshot.lastApplyTerm,
               pReceiver->snapshot.lastConfigIndex, pNode->pRaftStore->currentTerm, pNode->commitIndex, logBeginIndex,
               logLastIndex, pNode->minMatchIndex, snapshot.lastApplyIndex, snapshot.lastApplyTerm,
               pNode->pRaftCfg->isStandBy, pNode->pRaftCfg->snapshotStrategy, pNode->pRaftCfg->batchSize,
               pNode->replicaNum, pNode->pRaftCfg->lastConfigIndex, pNode->changing, pNode->restoreFinish, quorum,
               pNode->electTimerLogicClock, pNode->heartbeatTimerLogicClockUser, peerStr, cfgStr);
}
