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

#include "sync.h"
#include "syncAppendEntries.h"
#include "syncAppendEntriesReply.h"
#include "syncCommit.h"
#include "syncElection.h"
#include "syncEnv.h"
#include "syncIndexMgr.h"
#include "syncInt.h"
#include "syncMessage.h"
#include "syncRaftCfg.h"
#include "syncRaftLog.h"
#include "syncRaftStore.h"
#include "syncReplication.h"
#include "syncRequestVote.h"
#include "syncRequestVoteReply.h"
#include "syncRespMgr.h"
#include "syncSnapshot.h"
#include "syncTimeout.h"
#include "syncUtil.h"
#include "syncVoteMgr.h"
#include "tref.h"

bool gRaftDetailLog = false;

static int32_t tsNodeRefId = -1;

// ------ local funciton ---------
// enqueue message ----
static void    syncNodeEqPingTimer(void* param, void* tmrId);
static void    syncNodeEqElectTimer(void* param, void* tmrId);
static void    syncNodeEqHeartbeatTimer(void* param, void* tmrId);
static int32_t syncNodeEqNoop(SSyncNode* ths);
static int32_t syncNodeAppendNoop(SSyncNode* ths);

// process message ----
int32_t syncNodeOnPingCb(SSyncNode* ths, SyncPing* pMsg);
int32_t syncNodeOnPingReplyCb(SSyncNode* ths, SyncPingReply* pMsg);

// life cycle
static void syncFreeNode(void* param);
// ---------------------------------

int32_t syncInit() {
  int32_t ret = 0;

  if (!syncEnvIsStart()) {
    tsNodeRefId = taosOpenRef(200, syncFreeNode);
    if (tsNodeRefId < 0) {
      sError("failed to init node ref");
      syncCleanUp();
      ret = -1;
    } else {
      sDebug("sync rsetId:%" PRId64 " is open", tsNodeRefId);
      ret = syncEnvStart();
    }
  }

  return ret;
}

void syncCleanUp() {
  int32_t ret = syncEnvStop();
  ASSERT(ret == 0);

  if (tsNodeRefId != -1) {
    sDebug("sync rsetId:%" PRId64 " is closed", tsNodeRefId);
    taosCloseRef(tsNodeRefId);
    tsNodeRefId = -1;
  }
}

int64_t syncOpen(const SSyncInfo* pSyncInfo) {
  SSyncNode* pSyncNode = syncNodeOpen(pSyncInfo);
  ASSERT(pSyncNode != NULL);

  pSyncNode->rid = taosAddRef(tsNodeRefId, pSyncNode);
  if (pSyncNode->rid < 0) {
    syncFreeNode(pSyncNode);
    return -1;
  }

  sDebug("vgId:%d, sync rid:%" PRId64 " is added to rsetId:%" PRId64, pSyncInfo->vgId, pSyncNode->rid, tsNodeRefId);
  return pSyncNode->rid;
}

void syncStart(int64_t rid) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return;
  }

  if (pSyncNode->pRaftCfg->isStandBy) {
    syncNodeStartStandBy(pSyncNode);
  } else {
    syncNodeStart(pSyncNode);
  }

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
}

void syncStartNormal(int64_t rid) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return;
  }
  syncNodeStart(pSyncNode);

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
}

void syncStartStandBy(int64_t rid) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return;
  }
  syncNodeStartStandBy(pSyncNode);

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
}

void syncStop(int64_t rid) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) return;

  int32_t vgId = pSyncNode->vgId;
  syncNodeClose(pSyncNode);

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  taosRemoveRef(tsNodeRefId, rid);
  sDebug("vgId:%d, sync rid:%" PRId64 " is removed from rsetId:%" PRId64, vgId, rid, tsNodeRefId);
}

int32_t syncSetStandby(int64_t rid) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    sError("failed to set standby since accquire ref error, rid:%" PRId64, rid);
    return -1;
  }

  if (pSyncNode->state != TAOS_SYNC_STATE_FOLLOWER) {
    if (pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
      terrno = TSDB_CODE_SYN_IS_LEADER;
    } else {
      terrno = TSDB_CODE_SYN_STANDBY_NOT_READY;
    }
    sError("failed to set standby since it is not follower, state:%s rid:%" PRId64, syncStr(pSyncNode->state), rid);
    taosReleaseRef(tsNodeRefId, pSyncNode->rid);
    return -1;
  }

  // state change
  pSyncNode->state = TAOS_SYNC_STATE_FOLLOWER;
  syncNodeStopHeartbeatTimer(pSyncNode);

  // reset elect timer, long enough
  int32_t electMS = TIMER_MAX_MS;
  int32_t ret = syncNodeRestartElectTimer(pSyncNode, electMS);
  ASSERT(ret == 0);

  pSyncNode->pRaftCfg->isStandBy = 1;
  raftCfgPersist(pSyncNode->pRaftCfg);

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  sInfo("vgId:%d, set to standby", pSyncNode->vgId);
  return 0;
}

bool syncNodeCheckNewConfig(SSyncNode* pSyncNode, const SSyncCfg* pNewCfg) {
  bool IamInNew = syncNodeInConfig(pSyncNode, pNewCfg);
  if (!IamInNew) {
    return false;
  }

  if (pNewCfg->replicaNum > pSyncNode->replicaNum + 1) {
    return false;
  }

  if (pNewCfg->replicaNum < pSyncNode->replicaNum - 1) {
    return false;
  }

  return true;
}

int32_t syncReconfigBuild(int64_t rid, const SSyncCfg* pNewCfg, SRpcMsg* pRpcMsg) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }
  ASSERT(rid == pSyncNode->rid);
  int32_t ret = 0;

  if (!syncNodeCheckNewConfig(pSyncNode, pNewCfg)) {
    taosReleaseRef(tsNodeRefId, pSyncNode->rid);
    terrno = TSDB_CODE_SYN_NEW_CONFIG_ERROR;
    sError("syncNodeCheckNewConfig error");
    return -1;
  }

  char* newconfig = syncCfg2Str((SSyncCfg*)pNewCfg);
  pRpcMsg->msgType = TDMT_SYNC_CONFIG_CHANGE;
  pRpcMsg->info.noResp = 1;
  pRpcMsg->contLen = strlen(newconfig) + 1;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  snprintf(pRpcMsg->pCont, pRpcMsg->contLen, "%s", newconfig);
  taosMemoryFree(newconfig);

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  return ret;
}

int32_t syncReconfig(int64_t rid, const SSyncCfg* pNewCfg) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }
  ASSERT(rid == pSyncNode->rid);

  if (!syncNodeCheckNewConfig(pSyncNode, pNewCfg)) {
    taosReleaseRef(tsNodeRefId, pSyncNode->rid);
    terrno = TSDB_CODE_SYN_NEW_CONFIG_ERROR;
    sError("syncNodeCheckNewConfig error");
    return -1;
  }

  char*   newconfig = syncCfg2Str((SSyncCfg*)pNewCfg);
  int32_t ret = 0;

  SRpcMsg rpcMsg = {0};
  rpcMsg.msgType = TDMT_SYNC_CONFIG_CHANGE;
  rpcMsg.info.noResp = 1;
  rpcMsg.contLen = strlen(newconfig) + 1;
  rpcMsg.pCont = rpcMallocCont(rpcMsg.contLen);
  snprintf(rpcMsg.pCont, rpcMsg.contLen, "%s", newconfig);
  taosMemoryFree(newconfig);
  ret = syncNodePropose(pSyncNode, &rpcMsg, false);

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  return ret;
}

int32_t syncLeaderTransfer(int64_t rid) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }
  ASSERT(rid == pSyncNode->rid);

  int32_t ret = syncNodeLeaderTransfer(pSyncNode);
  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  return ret;
}

int32_t syncLeaderTransferTo(int64_t rid, SNodeInfo newLeader) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }
  ASSERT(rid == pSyncNode->rid);

  int32_t ret = syncNodeLeaderTransferTo(pSyncNode, newLeader);
  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  return ret;
}

int32_t syncNodeLeaderTransfer(SSyncNode* pSyncNode) {
  if (pSyncNode->peersNum == 0) {
    sDebug("only one replica, cannot leader transfer");
    terrno = TSDB_CODE_SYN_ONE_REPLICA;
    return -1;
  }

  SNodeInfo newLeader = (pSyncNode->peersNodeInfo)[0];
  int32_t   ret = syncNodeLeaderTransferTo(pSyncNode, newLeader);
  return ret;
}

int32_t syncNodeLeaderTransferTo(SSyncNode* pSyncNode, SNodeInfo newLeader) {
  int32_t ret = 0;

  if (pSyncNode->replicaNum == 1) {
    sDebug("only one replica, cannot leader transfer");
    terrno = TSDB_CODE_SYN_ONE_REPLICA;
    return -1;
  }

  do {
    char logBuf[128];
    snprintf(logBuf, sizeof(logBuf), "begin leader transfer to %s:%u", newLeader.nodeFqdn, newLeader.nodePort);
    syncNodeEventLog(pSyncNode, logBuf);
  } while (0);

  SyncLeaderTransfer* pMsg = syncLeaderTransferBuild(pSyncNode->vgId);
  pMsg->newLeaderId.addr = syncUtilAddr2U64(newLeader.nodeFqdn, newLeader.nodePort);
  pMsg->newLeaderId.vgId = pSyncNode->vgId;
  pMsg->newNodeInfo = newLeader;
  ASSERT(pMsg != NULL);
  SRpcMsg rpcMsg = {0};
  syncLeaderTransfer2RpcMsg(pMsg, &rpcMsg);
  syncLeaderTransferDestroy(pMsg);

  ret = syncNodePropose(pSyncNode, &rpcMsg, false);
  return ret;
}

bool syncCanLeaderTransfer(int64_t rid) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return false;
  }
  ASSERT(rid == pSyncNode->rid);

  if (pSyncNode->replicaNum == 1) {
    taosReleaseRef(tsNodeRefId, pSyncNode->rid);
    return false;
  }

  if (pSyncNode->state == TAOS_SYNC_STATE_FOLLOWER) {
    taosReleaseRef(tsNodeRefId, pSyncNode->rid);
    return true;
  }

  bool matchOK = true;
  if (pSyncNode->state == TAOS_SYNC_STATE_CANDIDATE || pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
    SyncIndex myCommitIndex = pSyncNode->commitIndex;
    for (int i = 0; i < pSyncNode->peersNum; ++i) {
      SyncIndex peerMatchIndex = syncIndexMgrGetIndex(pSyncNode->pMatchIndex, &(pSyncNode->peersId)[i]);
      if (peerMatchIndex < myCommitIndex) {
        matchOK = false;
      }
    }
  }

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  return matchOK;
}

int32_t syncForwardToPeer(int64_t rid, SRpcMsg* pMsg, bool isWeak) {
  int32_t ret = syncPropose(rid, pMsg, isWeak);
  return ret;
}

ESyncState syncGetMyRole(int64_t rid) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return TAOS_SYNC_STATE_ERROR;
  }
  ASSERT(rid == pSyncNode->rid);
  ESyncState state = pSyncNode->state;

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  return state;
}

bool syncIsReady(int64_t rid) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return false;
  }
  ASSERT(rid == pSyncNode->rid);
  bool b = (pSyncNode->state == TAOS_SYNC_STATE_LEADER) && pSyncNode->restoreFinish;
  taosReleaseRef(tsNodeRefId, pSyncNode->rid);

  // if false, set error code
  if (false == b) {
    if (pSyncNode->state != TAOS_SYNC_STATE_LEADER) {
      terrno = TSDB_CODE_SYN_NOT_LEADER;
    } else {
      terrno = TSDB_CODE_APP_NOT_READY;
    }
  }
  return b;
}

bool syncIsRestoreFinish(int64_t rid) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return false;
  }
  ASSERT(rid == pSyncNode->rid);
  bool b = pSyncNode->restoreFinish;

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  return b;
}

int32_t syncGetSnapshotByIndex(int64_t rid, SyncIndex index, SSnapshot* pSnapshot) {
  if (index < SYNC_INDEX_BEGIN) {
    return -1;
  }

  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return -1;
  }
  ASSERT(rid == pSyncNode->rid);

  SSyncRaftEntry* pEntry = NULL;
  int32_t         code = pSyncNode->pLogStore->syncLogGetEntry(pSyncNode->pLogStore, index, &pEntry);
  if (code != 0) {
    if (pEntry != NULL) {
      syncEntryDestory(pEntry);
    }
    taosReleaseRef(tsNodeRefId, pSyncNode->rid);
    return -1;
  }
  ASSERT(pEntry != NULL);

  pSnapshot->data = NULL;
  pSnapshot->lastApplyIndex = index;
  pSnapshot->lastApplyTerm = pEntry->term;
  pSnapshot->lastConfigIndex = syncNodeGetSnapshotConfigIndex(pSyncNode, index);

  syncEntryDestory(pEntry);
  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  return 0;
}

int32_t syncGetSnapshotMeta(int64_t rid, struct SSnapshotMeta* sMeta) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return -1;
  }
  ASSERT(rid == pSyncNode->rid);
  sMeta->lastConfigIndex = pSyncNode->pRaftCfg->lastConfigIndex;

  sTrace("vgId:%d, get snapshot meta, lastConfigIndex:%" PRId64, pSyncNode->vgId, pSyncNode->pRaftCfg->lastConfigIndex);

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  return 0;
}

int32_t syncGetSnapshotMetaByIndex(int64_t rid, SyncIndex snapshotIndex, struct SSnapshotMeta* sMeta) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return -1;
  }
  ASSERT(rid == pSyncNode->rid);

  ASSERT(pSyncNode->pRaftCfg->configIndexCount >= 1);
  SyncIndex lastIndex = (pSyncNode->pRaftCfg->configIndexArr)[0];

  for (int i = 0; i < pSyncNode->pRaftCfg->configIndexCount; ++i) {
    if ((pSyncNode->pRaftCfg->configIndexArr)[i] > lastIndex &&
        (pSyncNode->pRaftCfg->configIndexArr)[i] <= snapshotIndex) {
      lastIndex = (pSyncNode->pRaftCfg->configIndexArr)[i];
    }
  }
  sMeta->lastConfigIndex = lastIndex;
  sTrace("vgId:%d, get snapshot meta by index:%" PRId64 " lcindex:%" PRId64, pSyncNode->vgId, snapshotIndex,
         sMeta->lastConfigIndex);

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  return 0;
}

SyncIndex syncNodeGetSnapshotConfigIndex(SSyncNode* pSyncNode, SyncIndex snapshotLastApplyIndex) {
  ASSERT(pSyncNode->pRaftCfg->configIndexCount >= 1);
  SyncIndex lastIndex = (pSyncNode->pRaftCfg->configIndexArr)[0];

  for (int i = 0; i < pSyncNode->pRaftCfg->configIndexCount; ++i) {
    if ((pSyncNode->pRaftCfg->configIndexArr)[i] > lastIndex &&
        (pSyncNode->pRaftCfg->configIndexArr)[i] <= snapshotLastApplyIndex) {
      lastIndex = (pSyncNode->pRaftCfg->configIndexArr)[i];
    }
  }
  sTrace("vgId:%d, sync get last config index, index:%" PRId64 " lcindex:%" PRId64, pSyncNode->vgId,
         snapshotLastApplyIndex, lastIndex);

  return lastIndex;
}

const char* syncGetMyRoleStr(int64_t rid) {
  const char* s = syncUtilState2String(syncGetMyRole(rid));
  return s;
}

SyncTerm syncGetMyTerm(int64_t rid) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return TAOS_SYNC_STATE_ERROR;
  }
  ASSERT(rid == pSyncNode->rid);
  SyncTerm term = pSyncNode->pRaftStore->currentTerm;

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  return term;
}

SyncGroupId syncGetVgId(int64_t rid) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return TAOS_SYNC_STATE_ERROR;
  }
  ASSERT(rid == pSyncNode->rid);
  SyncGroupId vgId = pSyncNode->vgId;

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  return vgId;
}

void syncGetEpSet(int64_t rid, SEpSet* pEpSet) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    memset(pEpSet, 0, sizeof(*pEpSet));
    return;
  }
  ASSERT(rid == pSyncNode->rid);
  pEpSet->numOfEps = 0;
  for (int i = 0; i < pSyncNode->pRaftCfg->cfg.replicaNum; ++i) {
    snprintf(pEpSet->eps[i].fqdn, sizeof(pEpSet->eps[i].fqdn), "%s", (pSyncNode->pRaftCfg->cfg.nodeInfo)[i].nodeFqdn);
    pEpSet->eps[i].port = (pSyncNode->pRaftCfg->cfg.nodeInfo)[i].nodePort;
    (pEpSet->numOfEps)++;
    sInfo("vgId:%d, sync get epset: index:%d %s:%d", pSyncNode->vgId, i, pEpSet->eps[i].fqdn, pEpSet->eps[i].port);
  }
  pEpSet->inUse = pSyncNode->pRaftCfg->cfg.myIndex;
  sInfo("vgId:%d, sync get epset in-use:%d", pSyncNode->vgId, pEpSet->inUse);

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
}

void syncGetRetryEpSet(int64_t rid, SEpSet* pEpSet) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    memset(pEpSet, 0, sizeof(*pEpSet));
    return;
  }
  ASSERT(rid == pSyncNode->rid);
  pEpSet->numOfEps = 0;
  for (int i = 0; i < pSyncNode->pRaftCfg->cfg.replicaNum; ++i) {
    snprintf(pEpSet->eps[i].fqdn, sizeof(pEpSet->eps[i].fqdn), "%s", (pSyncNode->pRaftCfg->cfg.nodeInfo)[i].nodeFqdn);
    pEpSet->eps[i].port = (pSyncNode->pRaftCfg->cfg.nodeInfo)[i].nodePort;
    (pEpSet->numOfEps)++;
    sInfo("vgId:%d, sync get retry epset: index:%d %s:%d", pSyncNode->vgId, i, pEpSet->eps[i].fqdn,
          pEpSet->eps[i].port);
  }
  pEpSet->inUse = (pSyncNode->pRaftCfg->cfg.myIndex + 1) % pEpSet->numOfEps;
  sInfo("vgId:%d, sync get retry epset in-use:%d", pSyncNode->vgId, pEpSet->inUse);

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
}

int32_t syncGetRespRpc(int64_t rid, uint64_t index, SRpcMsg* msg) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return TAOS_SYNC_STATE_ERROR;
  }
  ASSERT(rid == pSyncNode->rid);

  SRespStub stub;
  int32_t   ret = syncRespMgrGet(pSyncNode->pSyncRespMgr, index, &stub);
  if (ret == 1) {
    memcpy(msg, &(stub.rpcMsg), sizeof(SRpcMsg));
  }

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  return ret;
}

int32_t syncGetAndDelRespRpc(int64_t rid, uint64_t index, SRpcHandleInfo* pInfo) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return TAOS_SYNC_STATE_ERROR;
  }
  ASSERT(rid == pSyncNode->rid);

  SRespStub stub;
  int32_t   ret = syncRespMgrGetAndDel(pSyncNode->pSyncRespMgr, index, &stub);
  if (ret == 1) {
    *pInfo = stub.rpcMsg.info;
  }

  sTrace("vgId:%d, get seq:%" PRIu64 " rpc handle:%p", pSyncNode->vgId, index, pInfo->handle);
  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  return ret;
}

void syncSetMsgCb(int64_t rid, const SMsgCb* msgcb) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    sTrace("syncSetQ get pSyncNode is NULL, rid:%" PRId64, rid);
    return;
  }
  ASSERT(rid == pSyncNode->rid);
  pSyncNode->msgcb = msgcb;

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
}

char* sync2SimpleStr(int64_t rid) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    sTrace("syncSetRpc get pSyncNode is NULL, rid:%" PRId64, rid);
    return NULL;
  }
  ASSERT(rid == pSyncNode->rid);
  char* s = syncNode2SimpleStr(pSyncNode);
  taosReleaseRef(tsNodeRefId, pSyncNode->rid);

  return s;
}

void setPingTimerMS(int64_t rid, int32_t pingTimerMS) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return;
  }
  ASSERT(rid == pSyncNode->rid);
  pSyncNode->pingBaseLine = pingTimerMS;
  pSyncNode->pingTimerMS = pingTimerMS;

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
}

void setElectTimerMS(int64_t rid, int32_t electTimerMS) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return;
  }
  ASSERT(rid == pSyncNode->rid);
  pSyncNode->electBaseLine = electTimerMS;

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
}

void setHeartbeatTimerMS(int64_t rid, int32_t hbTimerMS) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return;
  }
  ASSERT(rid == pSyncNode->rid);
  pSyncNode->hbBaseLine = hbTimerMS;
  pSyncNode->heartbeatTimerMS = hbTimerMS;

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
}

int32_t syncPropose(int64_t rid, SRpcMsg* pMsg, bool isWeak) {
  SSyncNode* pSyncNode = taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    taosReleaseRef(tsNodeRefId, rid);
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }
  ASSERT(rid == pSyncNode->rid);

  int32_t ret = syncNodePropose(pSyncNode, pMsg, isWeak);
  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  return ret;
}

int32_t syncProposeBatch(int64_t rid, SRpcMsg** pMsgPArr, bool* pIsWeakArr, int32_t arrSize) {
  if (arrSize < 0) {
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }

  SSyncNode* pSyncNode = taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }
  ASSERT(rid == pSyncNode->rid);

  int32_t ret = syncNodeProposeBatch(pSyncNode, pMsgPArr, pIsWeakArr, arrSize);
  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  return ret;
}

static bool syncNodeBatchOK(SRpcMsg** pMsgPArr, int32_t arrSize) {
  for (int32_t i = 0; i < arrSize; ++i) {
    if (pMsgPArr[i]->msgType == TDMT_SYNC_CONFIG_CHANGE) {
      return false;
    }

    if (pMsgPArr[i]->msgType == TDMT_SYNC_CONFIG_CHANGE_FINISH) {
      return false;
    }
  }

  return true;
}

int32_t syncNodeProposeBatch(SSyncNode* pSyncNode, SRpcMsg** pMsgPArr, bool* pIsWeakArr, int32_t arrSize) {
  if (!syncNodeBatchOK(pMsgPArr, arrSize)) {
    syncNodeErrorLog(pSyncNode, "sync propose batch error");
    terrno = TSDB_CODE_SYN_BATCH_ERROR;
    return -1;
  }

  if (arrSize > SYNC_MAX_BATCH_SIZE) {
    syncNodeErrorLog(pSyncNode, "sync propose batch error");
    terrno = TSDB_CODE_SYN_BATCH_ERROR;
    return -1;
  }

  if (pSyncNode->state != TAOS_SYNC_STATE_LEADER) {
    syncNodeErrorLog(pSyncNode, "sync propose not leader");
    terrno = TSDB_CODE_SYN_NOT_LEADER;
    return -1;
  }

  if (pSyncNode->changing) {
    syncNodeErrorLog(pSyncNode, "sync propose not ready");
    terrno = TSDB_CODE_SYN_PROPOSE_NOT_READY;
    return -1;
  }

  SRaftMeta raftArr[SYNC_MAX_BATCH_SIZE];
  for (int i = 0; i < arrSize; ++i) {
    do {
      char eventLog[128];
      snprintf(eventLog, sizeof(eventLog), "propose message, type:%s batch:%d", TMSG_INFO(pMsgPArr[i]->msgType),
               arrSize);
      syncNodeEventLog(pSyncNode, eventLog);
    } while (0);

    SRespStub stub;
    stub.createTime = taosGetTimestampMs();
    stub.rpcMsg = *(pMsgPArr[i]);
    uint64_t seqNum = syncRespMgrAdd(pSyncNode->pSyncRespMgr, &stub);

    raftArr[i].isWeak = pIsWeakArr[i];
    raftArr[i].seqNum = seqNum;
  }

  SyncClientRequestBatch* pSyncMsg = syncClientRequestBatchBuild(pMsgPArr, raftArr, arrSize, pSyncNode->vgId);
  ASSERT(pSyncMsg != NULL);

  SRpcMsg rpcMsg;
  syncClientRequestBatch2RpcMsg(pSyncMsg, &rpcMsg);
  taosMemoryFree(pSyncMsg);  // only free msg body, do not free rpc msg content

  if (pSyncNode->replicaNum == 1 && pSyncNode->vgId != 1) {
    int32_t code = syncNodeOnClientRequestBatchCb(pSyncNode, pSyncMsg);
    if (code == 0) {
      // update rpc msg applyIndex
      SRpcMsg* msgArr = syncClientRequestBatchRpcMsgArr(pSyncMsg);
      ASSERT(arrSize == pSyncMsg->dataCount);
      for (int i = 0; i < arrSize; ++i) {
        pMsgPArr[i]->info.conn.applyIndex = msgArr[i].info.conn.applyIndex;
        syncRespMgrDel(pSyncNode->pSyncRespMgr, raftArr[i].seqNum);
      }

      rpcFreeCont(rpcMsg.pCont);
      terrno = 0;
      return 1;

    } else {
      terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
      return -1;
    }

  } else {
    if (pSyncNode->FpEqMsg != NULL && (*pSyncNode->FpEqMsg)(pSyncNode->msgcb, &rpcMsg) == 0) {
      // enqueue msg ok
      return 0;

    } else {
      sError("vgId:%d, enqueue msg error, FpEqMsg is NULL", pSyncNode->vgId);
      terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
      return -1;
    }
  }

  return 0;
}

int32_t syncNodePropose(SSyncNode* pSyncNode, SRpcMsg* pMsg, bool isWeak) {
  int32_t ret = 0;

  do {
    char eventLog[128];
    snprintf(eventLog, sizeof(eventLog), "propose message, type:%s", TMSG_INFO(pMsg->msgType));
    syncNodeEventLog(pSyncNode, eventLog);
  } while (0);

  if (pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
    if (pSyncNode->changing && pMsg->msgType != TDMT_SYNC_CONFIG_CHANGE_FINISH) {
      ret = -1;
      terrno = TSDB_CODE_SYN_PROPOSE_NOT_READY;
      sError("vgId:%d, failed to sync propose since not ready, type:%s", pSyncNode->vgId, TMSG_INFO(pMsg->msgType));
      goto _END;
    }

    // config change
    if (pMsg->msgType == TDMT_SYNC_CONFIG_CHANGE) {
      if (!syncNodeCanChange(pSyncNode)) {
        ret = -1;
        terrno = TSDB_CODE_SYN_RECONFIG_NOT_READY;
        sError("vgId:%d, failed to sync reconfig since not ready, type:%s", pSyncNode->vgId, TMSG_INFO(pMsg->msgType));
        goto _END;
      }

      ASSERT(!pSyncNode->changing);
      pSyncNode->changing = true;
    }

    SRespStub stub;
    stub.createTime = taosGetTimestampMs();
    stub.rpcMsg = *pMsg;
    uint64_t seqNum = syncRespMgrAdd(pSyncNode->pSyncRespMgr, &stub);

    SyncClientRequest* pSyncMsg = syncClientRequestBuild2(pMsg, seqNum, isWeak, pSyncNode->vgId);
    SRpcMsg            rpcMsg;
    syncClientRequest2RpcMsg(pSyncMsg, &rpcMsg);

    // optimized one replica
    if (syncNodeIsOptimizedOneReplica(pSyncNode, pMsg)) {
      SyncIndex retIndex;
      int32_t   code = syncNodeOnClientRequestCb(pSyncNode, pSyncMsg, &retIndex);
      if (code == 0) {
        pMsg->info.conn.applyIndex = retIndex;
        pMsg->info.conn.applyTerm = pSyncNode->pRaftStore->currentTerm;
        rpcFreeCont(rpcMsg.pCont);
        syncRespMgrDel(pSyncNode->pSyncRespMgr, seqNum);
        ret = 1;
        sDebug("vgId:%d, sync optimize index:%" PRId64 ", type:%s", pSyncNode->vgId, retIndex,
               TMSG_INFO(pMsg->msgType));
      } else {
        ret = -1;
        terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
        sError("vgId:%d, failed to sync optimize index:%" PRId64 ", type:%s", pSyncNode->vgId, retIndex,
               TMSG_INFO(pMsg->msgType));
      }

    } else {
      if (pSyncNode->FpEqMsg != NULL && (*pSyncNode->FpEqMsg)(pSyncNode->msgcb, &rpcMsg) == 0) {
        ret = 0;
      } else {
        ret = -1;
        terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
        sError("vgId:%d, failed to enqueue msg since its null", pSyncNode->vgId);
      }
    }

    syncClientRequestDestroy(pSyncMsg);
    goto _END;

  } else {
    ret = -1;
    terrno = TSDB_CODE_SYN_NOT_LEADER;
    sError("vgId:%d, sync propose not leader, %s, type:%s", pSyncNode->vgId, syncUtilState2String(pSyncNode->state),
           TMSG_INFO(pMsg->msgType));
    goto _END;
  }

_END:
  return ret;
}

// open/close --------------
SSyncNode* syncNodeOpen(const SSyncInfo* pOldSyncInfo) {
  SSyncInfo* pSyncInfo = (SSyncInfo*)pOldSyncInfo;

  SSyncNode* pSyncNode = (SSyncNode*)taosMemoryMalloc(sizeof(SSyncNode));
  ASSERT(pSyncNode != NULL);
  memset(pSyncNode, 0, sizeof(SSyncNode));

  int32_t ret = 0;
  if (!taosDirExist((char*)(pSyncInfo->path))) {
    if (taosMkDir(pSyncInfo->path) != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      sError("failed to create dir:%s since %s", pSyncInfo->path, terrstr());
      return NULL;
    }
  }

  snprintf(pSyncNode->configPath, sizeof(pSyncNode->configPath), "%s%sraft_config.json", pSyncInfo->path, TD_DIRSEP);
  if (!taosCheckExistFile(pSyncNode->configPath)) {
    // create a new raft config file
    SRaftCfgMeta meta;
    meta.isStandBy = pSyncInfo->isStandBy;
    meta.snapshotStrategy = pSyncInfo->snapshotStrategy;
    meta.lastConfigIndex = SYNC_INDEX_INVALID;
    meta.batchSize = pSyncInfo->batchSize;
    ret = raftCfgCreateFile((SSyncCfg*)&(pSyncInfo->syncCfg), meta, pSyncNode->configPath);
    ASSERT(ret == 0);

  } else {
    // update syncCfg by raft_config.json
    pSyncNode->pRaftCfg = raftCfgOpen(pSyncNode->configPath);
    ASSERT(pSyncNode->pRaftCfg != NULL);
    pSyncInfo->syncCfg = pSyncNode->pRaftCfg->cfg;

    raftCfgClose(pSyncNode->pRaftCfg);
  }

  // init by SSyncInfo
  pSyncNode->vgId = pSyncInfo->vgId;
  memcpy(pSyncNode->path, pSyncInfo->path, sizeof(pSyncNode->path));
  snprintf(pSyncNode->raftStorePath, sizeof(pSyncNode->raftStorePath), "%s%sraft_store.json", pSyncInfo->path,
           TD_DIRSEP);
  snprintf(pSyncNode->configPath, sizeof(pSyncNode->configPath), "%s%sraft_config.json", pSyncInfo->path, TD_DIRSEP);

  pSyncNode->pWal = pSyncInfo->pWal;
  pSyncNode->msgcb = pSyncInfo->msgcb;
  pSyncNode->FpSendMsg = pSyncInfo->FpSendMsg;
  pSyncNode->FpEqMsg = pSyncInfo->FpEqMsg;

  // init raft config
  pSyncNode->pRaftCfg = raftCfgOpen(pSyncNode->configPath);
  ASSERT(pSyncNode->pRaftCfg != NULL);

  // init internal
  pSyncNode->myNodeInfo = pSyncNode->pRaftCfg->cfg.nodeInfo[pSyncNode->pRaftCfg->cfg.myIndex];
  syncUtilnodeInfo2raftId(&pSyncNode->myNodeInfo, pSyncNode->vgId, &pSyncNode->myRaftId);

  // init peersNum, peers, peersId
  pSyncNode->peersNum = pSyncNode->pRaftCfg->cfg.replicaNum - 1;
  int j = 0;
  for (int i = 0; i < pSyncNode->pRaftCfg->cfg.replicaNum; ++i) {
    if (i != pSyncNode->pRaftCfg->cfg.myIndex) {
      pSyncNode->peersNodeInfo[j] = pSyncNode->pRaftCfg->cfg.nodeInfo[i];
      j++;
    }
  }
  for (int i = 0; i < pSyncNode->peersNum; ++i) {
    syncUtilnodeInfo2raftId(&pSyncNode->peersNodeInfo[i], pSyncNode->vgId, &pSyncNode->peersId[i]);
  }

  // init replicaNum, replicasId
  pSyncNode->replicaNum = pSyncNode->pRaftCfg->cfg.replicaNum;
  for (int i = 0; i < pSyncNode->pRaftCfg->cfg.replicaNum; ++i) {
    syncUtilnodeInfo2raftId(&pSyncNode->pRaftCfg->cfg.nodeInfo[i], pSyncNode->vgId, &pSyncNode->replicasId[i]);
  }

  // init raft algorithm
  pSyncNode->pFsm = pSyncInfo->pFsm;
  pSyncNode->quorum = syncUtilQuorum(pSyncNode->pRaftCfg->cfg.replicaNum);
  pSyncNode->leaderCache = EMPTY_RAFT_ID;

  // init life cycle outside

  // TLA+ Spec
  // InitHistoryVars == /\ elections = {}
  //                    /\ allLogs   = {}
  //                    /\ voterLog  = [i \in Server |-> [j \in {} |-> <<>>]]
  // InitServerVars == /\ currentTerm = [i \in Server |-> 1]
  //                   /\ state       = [i \in Server |-> Follower]
  //                   /\ votedFor    = [i \in Server |-> Nil]
  // InitCandidateVars == /\ votesResponded = [i \in Server |-> {}]
  //                      /\ votesGranted   = [i \in Server |-> {}]
  // \* The values nextIndex[i][i] and matchIndex[i][i] are never read, since the
  // \* leader does not send itself messages. It's still easier to include these
  // \* in the functions.
  // InitLeaderVars == /\ nextIndex  = [i \in Server |-> [j \in Server |-> 1]]
  //                   /\ matchIndex = [i \in Server |-> [j \in Server |-> 0]]
  // InitLogVars == /\ log          = [i \in Server |-> << >>]
  //                /\ commitIndex  = [i \in Server |-> 0]
  // Init == /\ messages = [m \in {} |-> 0]
  //         /\ InitHistoryVars
  //         /\ InitServerVars
  //         /\ InitCandidateVars
  //         /\ InitLeaderVars
  //         /\ InitLogVars
  //

  // init TLA+ server vars
  pSyncNode->state = TAOS_SYNC_STATE_FOLLOWER;
  pSyncNode->pRaftStore = raftStoreOpen(pSyncNode->raftStorePath);
  ASSERT(pSyncNode->pRaftStore != NULL);

  // init TLA+ candidate vars
  pSyncNode->pVotesGranted = voteGrantedCreate(pSyncNode);
  ASSERT(pSyncNode->pVotesGranted != NULL);
  pSyncNode->pVotesRespond = votesRespondCreate(pSyncNode);
  ASSERT(pSyncNode->pVotesRespond != NULL);

  // init TLA+ leader vars
  pSyncNode->pNextIndex = syncIndexMgrCreate(pSyncNode);
  ASSERT(pSyncNode->pNextIndex != NULL);
  pSyncNode->pMatchIndex = syncIndexMgrCreate(pSyncNode);
  ASSERT(pSyncNode->pMatchIndex != NULL);

  // init TLA+ log vars
  pSyncNode->pLogStore = logStoreCreate(pSyncNode);
  ASSERT(pSyncNode->pLogStore != NULL);

  SyncIndex commitIndex = SYNC_INDEX_INVALID;
  if (pSyncNode->pFsm != NULL && pSyncNode->pFsm->FpGetSnapshotInfo != NULL) {
    SSnapshot snapshot = {0};
    int32_t   code = pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
    ASSERT(code == 0);
    if (snapshot.lastApplyIndex > commitIndex) {
      commitIndex = snapshot.lastApplyIndex;
      syncNodeEventLog(pSyncNode, "reset commit index by snapshot");
    }
  }
  pSyncNode->commitIndex = commitIndex;

  // timer ms init
  pSyncNode->pingBaseLine = PING_TIMER_MS;
  pSyncNode->electBaseLine = ELECT_TIMER_MS_MIN;
  pSyncNode->hbBaseLine = HEARTBEAT_TIMER_MS;

  // init ping timer
  pSyncNode->pPingTimer = NULL;
  pSyncNode->pingTimerMS = pSyncNode->pingBaseLine;
  atomic_store_64(&pSyncNode->pingTimerLogicClock, 0);
  atomic_store_64(&pSyncNode->pingTimerLogicClockUser, 0);
  pSyncNode->FpPingTimerCB = syncNodeEqPingTimer;
  pSyncNode->pingTimerCounter = 0;

  // init elect timer
  pSyncNode->pElectTimer = NULL;
  pSyncNode->electTimerMS = syncUtilElectRandomMS(pSyncNode->electBaseLine, 2 * pSyncNode->electBaseLine);
  atomic_store_64(&pSyncNode->electTimerLogicClock, 0);
  atomic_store_64(&pSyncNode->electTimerLogicClockUser, 0);
  pSyncNode->FpElectTimerCB = syncNodeEqElectTimer;
  pSyncNode->electTimerCounter = 0;

  // init heartbeat timer
  pSyncNode->pHeartbeatTimer = NULL;
  pSyncNode->heartbeatTimerMS = pSyncNode->hbBaseLine;
  atomic_store_64(&pSyncNode->heartbeatTimerLogicClock, 0);
  atomic_store_64(&pSyncNode->heartbeatTimerLogicClockUser, 0);
  pSyncNode->FpHeartbeatTimerCB = syncNodeEqHeartbeatTimer;
  pSyncNode->heartbeatTimerCounter = 0;

  // init callback
  pSyncNode->FpOnPing = syncNodeOnPingCb;
  pSyncNode->FpOnPingReply = syncNodeOnPingReplyCb;
  pSyncNode->FpOnClientRequest = syncNodeOnClientRequestCb;
  pSyncNode->FpOnTimeout = syncNodeOnTimeoutCb;

  pSyncNode->FpOnSnapshotSend = syncNodeOnSnapshotSendCb;
  pSyncNode->FpOnSnapshotRsp = syncNodeOnSnapshotRspCb;

  if (pSyncNode->pRaftCfg->snapshotStrategy) {
    sInfo("vgId:%d, sync node use snapshot", pSyncNode->vgId);
    pSyncNode->FpOnRequestVote = syncNodeOnRequestVoteSnapshotCb;
    pSyncNode->FpOnRequestVoteReply = syncNodeOnRequestVoteReplySnapshotCb;
    pSyncNode->FpOnAppendEntries = syncNodeOnAppendEntriesSnapshotCb;
    pSyncNode->FpOnAppendEntriesReply = syncNodeOnAppendEntriesReplySnapshotCb;

  } else {
    sInfo("vgId:%d, sync node do not use snapshot", pSyncNode->vgId);
    pSyncNode->FpOnRequestVote = syncNodeOnRequestVoteCb;
    pSyncNode->FpOnRequestVoteReply = syncNodeOnRequestVoteReplyCb;
    pSyncNode->FpOnAppendEntries = syncNodeOnAppendEntriesCb;
    pSyncNode->FpOnAppendEntriesReply = syncNodeOnAppendEntriesReplyCb;
  }

  // tools
  pSyncNode->pSyncRespMgr = syncRespMgrCreate(pSyncNode, SYNC_RESP_TTL_MS);
  ASSERT(pSyncNode->pSyncRespMgr != NULL);

  // restore state
  pSyncNode->restoreFinish = false;

  // snapshot senders
  for (int i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SSyncSnapshotSender* pSender = snapshotSenderCreate(pSyncNode, i);
    // ASSERT(pSender != NULL);
    (pSyncNode->senders)[i] = pSender;
  }

  // snapshot receivers
  pSyncNode->pNewNodeReceiver = snapshotReceiverCreate(pSyncNode, EMPTY_RAFT_ID);

  // is config changing
  pSyncNode->changing = false;

  // start in syncNodeStart
  // start raft
  // syncNodeBecomeFollower(pSyncNode);

  syncNodeEventLog(pSyncNode, "sync open");

  return pSyncNode;
}

void syncNodeMaybeUpdateCommitBySnapshot(SSyncNode* pSyncNode) {
  if (pSyncNode->pFsm != NULL && pSyncNode->pFsm->FpGetSnapshotInfo != NULL) {
    SSnapshot snapshot;
    int32_t   code = pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
    ASSERT(code == 0);
    if (snapshot.lastApplyIndex > pSyncNode->commitIndex) {
      pSyncNode->commitIndex = snapshot.lastApplyIndex;
    }
  }
}

void syncNodeStart(SSyncNode* pSyncNode) {
  // start raft
  if (pSyncNode->replicaNum == 1) {
    raftStoreNextTerm(pSyncNode->pRaftStore);
    syncNodeBecomeLeader(pSyncNode, "one replica start");

    // Raft 3.6.2 Committing entries from previous terms
    syncNodeAppendNoop(pSyncNode);
    syncMaybeAdvanceCommitIndex(pSyncNode);
  } else {
    syncNodeBecomeFollower(pSyncNode, "first start");
  }

  int32_t ret = 0;
  // ret = syncNodeStartPingTimer(pSyncNode);
  ASSERT(ret == 0);
}

void syncNodeStartStandBy(SSyncNode* pSyncNode) {
  // state change
  pSyncNode->state = TAOS_SYNC_STATE_FOLLOWER;
  syncNodeStopHeartbeatTimer(pSyncNode);

  // reset elect timer, long enough
  int32_t electMS = TIMER_MAX_MS;
  int32_t ret = syncNodeRestartElectTimer(pSyncNode, electMS);
  ASSERT(ret == 0);
}

void syncNodeClose(SSyncNode* pSyncNode) {
  syncNodeEventLog(pSyncNode, "sync close");

  int32_t ret;
  ASSERT(pSyncNode != NULL);

  ret = raftStoreClose(pSyncNode->pRaftStore);
  ASSERT(ret == 0);

  syncRespMgrDestroy(pSyncNode->pSyncRespMgr);
  voteGrantedDestroy(pSyncNode->pVotesGranted);
  votesRespondDestory(pSyncNode->pVotesRespond);
  syncIndexMgrDestroy(pSyncNode->pNextIndex);
  syncIndexMgrDestroy(pSyncNode->pMatchIndex);
  logStoreDestory(pSyncNode->pLogStore);
  raftCfgClose(pSyncNode->pRaftCfg);

  syncNodeStopPingTimer(pSyncNode);
  syncNodeStopElectTimer(pSyncNode);
  syncNodeStopHeartbeatTimer(pSyncNode);

  if (pSyncNode->pFsm != NULL) {
    taosMemoryFree(pSyncNode->pFsm);
  }

  for (int i = 0; i < TSDB_MAX_REPLICA; ++i) {
    if ((pSyncNode->senders)[i] != NULL) {
      snapshotSenderDestroy((pSyncNode->senders)[i]);
      (pSyncNode->senders)[i] = NULL;
    }
  }

  if (pSyncNode->pNewNodeReceiver != NULL) {
    snapshotReceiverDestroy(pSyncNode->pNewNodeReceiver);
    pSyncNode->pNewNodeReceiver = NULL;
  }

  // free memory in syncFreeNode
  // taosMemoryFree(pSyncNode);
}

// option
// bool syncNodeSnapshotEnable(SSyncNode* pSyncNode) { return pSyncNode->pRaftCfg->snapshotEnable; }

ESyncStrategy syncNodeStrategy(SSyncNode* pSyncNode) { return pSyncNode->pRaftCfg->snapshotStrategy; }

// ping --------------
int32_t syncNodePing(SSyncNode* pSyncNode, const SRaftId* destRaftId, SyncPing* pMsg) {
  syncPingLog2((char*)"==syncNodePing==", pMsg);
  int32_t ret = 0;

  SRpcMsg rpcMsg;
  syncPing2RpcMsg(pMsg, &rpcMsg);
  syncRpcMsgLog2((char*)"==syncNodePing==", &rpcMsg);

  ret = syncNodeSendMsgById(destRaftId, pSyncNode, &rpcMsg);
  return ret;
}

int32_t syncNodePingSelf(SSyncNode* pSyncNode) {
  int32_t   ret = 0;
  SyncPing* pMsg = syncPingBuild3(&pSyncNode->myRaftId, &pSyncNode->myRaftId, pSyncNode->vgId);
  ret = syncNodePing(pSyncNode, &pMsg->destId, pMsg);
  ASSERT(ret == 0);

  syncPingDestroy(pMsg);
  return ret;
}

int32_t syncNodePingPeers(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  for (int i = 0; i < pSyncNode->peersNum; ++i) {
    SRaftId*  destId = &(pSyncNode->peersId[i]);
    SyncPing* pMsg = syncPingBuild3(&pSyncNode->myRaftId, destId, pSyncNode->vgId);
    ret = syncNodePing(pSyncNode, destId, pMsg);
    ASSERT(ret == 0);
    syncPingDestroy(pMsg);
  }
  return ret;
}

int32_t syncNodePingAll(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  for (int i = 0; i < pSyncNode->pRaftCfg->cfg.replicaNum; ++i) {
    SRaftId*  destId = &(pSyncNode->replicasId[i]);
    SyncPing* pMsg = syncPingBuild3(&pSyncNode->myRaftId, destId, pSyncNode->vgId);
    ret = syncNodePing(pSyncNode, destId, pMsg);
    ASSERT(ret == 0);
    syncPingDestroy(pMsg);
  }
  return ret;
}

// timer control --------------
int32_t syncNodeStartPingTimer(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  if (syncEnvIsStart()) {
    taosTmrReset(pSyncNode->FpPingTimerCB, pSyncNode->pingTimerMS, pSyncNode, gSyncEnv->pTimerManager,
                 &pSyncNode->pPingTimer);
    atomic_store_64(&pSyncNode->pingTimerLogicClock, pSyncNode->pingTimerLogicClockUser);
  } else {
    sError("vgId:%d, start ping timer error, sync env is stop", pSyncNode->vgId);
  }
  return ret;
}

int32_t syncNodeStopPingTimer(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  atomic_add_fetch_64(&pSyncNode->pingTimerLogicClockUser, 1);
  taosTmrStop(pSyncNode->pPingTimer);
  pSyncNode->pPingTimer = NULL;
  return ret;
}

int32_t syncNodeStartElectTimer(SSyncNode* pSyncNode, int32_t ms) {
  int32_t ret = 0;
  if (syncEnvIsStart()) {
    pSyncNode->electTimerMS = ms;
    taosTmrReset(pSyncNode->FpElectTimerCB, pSyncNode->electTimerMS, pSyncNode, gSyncEnv->pTimerManager,
                 &pSyncNode->pElectTimer);
    atomic_store_64(&pSyncNode->electTimerLogicClock, pSyncNode->electTimerLogicClockUser);

    do {
      char logBuf[128];
      snprintf(logBuf, sizeof(logBuf), "elect timer reset, ms:%d", ms);
      syncNodeEventLog(pSyncNode, logBuf);
    } while (0);

  } else {
    sError("vgId:%d, start elect timer error, sync env is stop", pSyncNode->vgId);
  }
  return ret;
}

int32_t syncNodeStopElectTimer(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  atomic_add_fetch_64(&pSyncNode->electTimerLogicClockUser, 1);
  taosTmrStop(pSyncNode->pElectTimer);
  pSyncNode->pElectTimer = NULL;
  return ret;
}

int32_t syncNodeRestartElectTimer(SSyncNode* pSyncNode, int32_t ms) {
  int32_t ret = 0;
  syncNodeStopElectTimer(pSyncNode);
  syncNodeStartElectTimer(pSyncNode, ms);
  return ret;
}

int32_t syncNodeResetElectTimer(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  int32_t electMS;

  if (pSyncNode->pRaftCfg->isStandBy) {
    electMS = TIMER_MAX_MS;
  } else {
    electMS = syncUtilElectRandomMS(pSyncNode->electBaseLine, 2 * pSyncNode->electBaseLine);
  }
  ret = syncNodeRestartElectTimer(pSyncNode, electMS);

  do {
    char logBuf[128];
    snprintf(logBuf, sizeof(logBuf), "reset elect timer, min:%d, max:%d, ms:%d", pSyncNode->electBaseLine,
             2 * pSyncNode->electBaseLine, electMS);
    syncNodeEventLog(pSyncNode, logBuf);
  } while (0);

  return ret;
}

int32_t syncNodeStartHeartbeatTimer(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  if (syncEnvIsStart()) {
    taosTmrReset(pSyncNode->FpHeartbeatTimerCB, pSyncNode->heartbeatTimerMS, pSyncNode, gSyncEnv->pTimerManager,
                 &pSyncNode->pHeartbeatTimer);
    atomic_store_64(&pSyncNode->heartbeatTimerLogicClock, pSyncNode->heartbeatTimerLogicClockUser);
  } else {
    sError("vgId:%d, start heartbeat timer error, sync env is stop", pSyncNode->vgId);
  }

  do {
    char logBuf[128];
    snprintf(logBuf, sizeof(logBuf), "start heartbeat timer, ms:%d", pSyncNode->heartbeatTimerMS);
    syncNodeEventLog(pSyncNode, logBuf);
  } while (0);

  return ret;
}

int32_t syncNodeStartNowHeartbeatTimer(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  if (syncEnvIsStart()) {
    taosTmrReset(pSyncNode->FpHeartbeatTimerCB, 1, pSyncNode, gSyncEnv->pTimerManager, &pSyncNode->pHeartbeatTimer);
    atomic_store_64(&pSyncNode->heartbeatTimerLogicClock, pSyncNode->heartbeatTimerLogicClockUser);
  } else {
    sError("vgId:%d, start heartbeat timer error, sync env is stop", pSyncNode->vgId);
  }

  do {
    char logBuf[128];
    snprintf(logBuf, sizeof(logBuf), "start heartbeat timer, ms:%d", 1);
    syncNodeEventLog(pSyncNode, logBuf);
  } while (0);

  return ret;
}

int32_t syncNodeStopHeartbeatTimer(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  atomic_add_fetch_64(&pSyncNode->heartbeatTimerLogicClockUser, 1);
  taosTmrStop(pSyncNode->pHeartbeatTimer);
  pSyncNode->pHeartbeatTimer = NULL;
  sTrace("vgId:%d, stop heartbeat timer", pSyncNode->vgId);

  return ret;
}

int32_t syncNodeRestartHeartbeatTimer(SSyncNode* pSyncNode) {
  syncNodeStopHeartbeatTimer(pSyncNode);
  syncNodeStartHeartbeatTimer(pSyncNode);
  return 0;
}

int32_t syncNodeRestartNowHeartbeatTimer(SSyncNode* pSyncNode) {
  syncNodeStopHeartbeatTimer(pSyncNode);
  syncNodeStartNowHeartbeatTimer(pSyncNode);
  return 0;
}

// utils --------------
int32_t syncNodeSendMsgById(const SRaftId* destRaftId, SSyncNode* pSyncNode, SRpcMsg* pMsg) {
  SEpSet epSet;
  syncUtilraftId2EpSet(destRaftId, &epSet);
  if (pSyncNode->FpSendMsg != NULL) {
    // htonl
    syncUtilMsgHtoN(pMsg->pCont);

    pMsg->info.noResp = 1;
    pSyncNode->FpSendMsg(&epSet, pMsg);
  } else {
    sError("vgId:%d, sync send msg by id error, fp-send-msg is null", pSyncNode->vgId);
    return -1;
  }

  return 0;
}

int32_t syncNodeSendMsgByInfo(const SNodeInfo* nodeInfo, SSyncNode* pSyncNode, SRpcMsg* pMsg) {
  SEpSet epSet;
  syncUtilnodeInfo2EpSet(nodeInfo, &epSet);
  if (pSyncNode->FpSendMsg != NULL) {
    // htonl
    syncUtilMsgHtoN(pMsg->pCont);

    pMsg->info.noResp = 1;
    pSyncNode->FpSendMsg(&epSet, pMsg);
  } else {
    sError("vgId:%d, sync send msg by info error, fp-send-msg is null", pSyncNode->vgId);
  }
  return 0;
}

cJSON* syncNode2Json(const SSyncNode* pSyncNode) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  if (pSyncNode != NULL) {
    // init by SSyncInfo
    cJSON_AddNumberToObject(pRoot, "vgId", pSyncNode->vgId);
    cJSON_AddItemToObject(pRoot, "SRaftCfg", raftCfg2Json(pSyncNode->pRaftCfg));
    cJSON_AddStringToObject(pRoot, "path", pSyncNode->path);
    cJSON_AddStringToObject(pRoot, "raftStorePath", pSyncNode->raftStorePath);
    cJSON_AddStringToObject(pRoot, "configPath", pSyncNode->configPath);

    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->pWal);
    cJSON_AddStringToObject(pRoot, "pWal", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->msgcb);
    cJSON_AddStringToObject(pRoot, "rpcClient", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpSendMsg);
    cJSON_AddStringToObject(pRoot, "FpSendMsg", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->msgcb);
    cJSON_AddStringToObject(pRoot, "queue", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpEqMsg);
    cJSON_AddStringToObject(pRoot, "FpEqMsg", u64buf);

    // init internal
    cJSON* pMe = syncUtilNodeInfo2Json(&pSyncNode->myNodeInfo);
    cJSON_AddItemToObject(pRoot, "myNodeInfo", pMe);
    cJSON* pRaftId = syncUtilRaftId2Json(&pSyncNode->myRaftId);
    cJSON_AddItemToObject(pRoot, "myRaftId", pRaftId);

    cJSON_AddNumberToObject(pRoot, "peersNum", pSyncNode->peersNum);
    cJSON* pPeers = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "peersNodeInfo", pPeers);
    for (int i = 0; i < pSyncNode->peersNum; ++i) {
      cJSON_AddItemToArray(pPeers, syncUtilNodeInfo2Json(&pSyncNode->peersNodeInfo[i]));
    }
    cJSON* pPeersId = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "peersId", pPeersId);
    for (int i = 0; i < pSyncNode->peersNum; ++i) {
      cJSON_AddItemToArray(pPeersId, syncUtilRaftId2Json(&pSyncNode->peersId[i]));
    }

    cJSON_AddNumberToObject(pRoot, "replicaNum", pSyncNode->replicaNum);
    cJSON* pReplicasId = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "replicasId", pReplicasId);
    for (int i = 0; i < pSyncNode->replicaNum; ++i) {
      cJSON_AddItemToArray(pReplicasId, syncUtilRaftId2Json(&pSyncNode->replicasId[i]));
    }

    // raft algorithm
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->pFsm);
    cJSON_AddStringToObject(pRoot, "pFsm", u64buf);
    cJSON_AddNumberToObject(pRoot, "quorum", pSyncNode->quorum);
    cJSON* pLaderCache = syncUtilRaftId2Json(&pSyncNode->leaderCache);
    cJSON_AddItemToObject(pRoot, "leaderCache", pLaderCache);

    // life cycle
    snprintf(u64buf, sizeof(u64buf), "%" PRId64, pSyncNode->rid);
    cJSON_AddStringToObject(pRoot, "rid", u64buf);

    // tla+ server vars
    cJSON_AddNumberToObject(pRoot, "state", pSyncNode->state);
    cJSON_AddStringToObject(pRoot, "state_str", syncUtilState2String(pSyncNode->state));
    cJSON_AddItemToObject(pRoot, "pRaftStore", raftStore2Json(pSyncNode->pRaftStore));

    // tla+ candidate vars
    cJSON_AddItemToObject(pRoot, "pVotesGranted", voteGranted2Json(pSyncNode->pVotesGranted));
    cJSON_AddItemToObject(pRoot, "pVotesRespond", votesRespond2Json(pSyncNode->pVotesRespond));

    // tla+ leader vars
    cJSON_AddItemToObject(pRoot, "pNextIndex", syncIndexMgr2Json(pSyncNode->pNextIndex));
    cJSON_AddItemToObject(pRoot, "pMatchIndex", syncIndexMgr2Json(pSyncNode->pMatchIndex));

    // tla+ log vars
    cJSON_AddItemToObject(pRoot, "pLogStore", logStore2Json(pSyncNode->pLogStore));
    snprintf(u64buf, sizeof(u64buf), "%" PRId64, pSyncNode->commitIndex);
    cJSON_AddStringToObject(pRoot, "commitIndex", u64buf);

    // timer ms init
    cJSON_AddNumberToObject(pRoot, "pingBaseLine", pSyncNode->pingBaseLine);
    cJSON_AddNumberToObject(pRoot, "electBaseLine", pSyncNode->electBaseLine);
    cJSON_AddNumberToObject(pRoot, "hbBaseLine", pSyncNode->hbBaseLine);

    // ping timer
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->pPingTimer);
    cJSON_AddStringToObject(pRoot, "pPingTimer", u64buf);
    cJSON_AddNumberToObject(pRoot, "pingTimerMS", pSyncNode->pingTimerMS);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pSyncNode->pingTimerLogicClock);
    cJSON_AddStringToObject(pRoot, "pingTimerLogicClock", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pSyncNode->pingTimerLogicClockUser);
    cJSON_AddStringToObject(pRoot, "pingTimerLogicClockUser", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpPingTimerCB);
    cJSON_AddStringToObject(pRoot, "FpPingTimerCB", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pSyncNode->pingTimerCounter);
    cJSON_AddStringToObject(pRoot, "pingTimerCounter", u64buf);

    // elect timer
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->pElectTimer);
    cJSON_AddStringToObject(pRoot, "pElectTimer", u64buf);
    cJSON_AddNumberToObject(pRoot, "electTimerMS", pSyncNode->electTimerMS);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pSyncNode->electTimerLogicClock);
    cJSON_AddStringToObject(pRoot, "electTimerLogicClock", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pSyncNode->electTimerLogicClockUser);
    cJSON_AddStringToObject(pRoot, "electTimerLogicClockUser", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpElectTimerCB);
    cJSON_AddStringToObject(pRoot, "FpElectTimerCB", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pSyncNode->electTimerCounter);
    cJSON_AddStringToObject(pRoot, "electTimerCounter", u64buf);

    // heartbeat timer
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->pHeartbeatTimer);
    cJSON_AddStringToObject(pRoot, "pHeartbeatTimer", u64buf);
    cJSON_AddNumberToObject(pRoot, "heartbeatTimerMS", pSyncNode->heartbeatTimerMS);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pSyncNode->heartbeatTimerLogicClock);
    cJSON_AddStringToObject(pRoot, "heartbeatTimerLogicClock", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pSyncNode->heartbeatTimerLogicClockUser);
    cJSON_AddStringToObject(pRoot, "heartbeatTimerLogicClockUser", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpHeartbeatTimerCB);
    cJSON_AddStringToObject(pRoot, "FpHeartbeatTimerCB", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pSyncNode->heartbeatTimerCounter);
    cJSON_AddStringToObject(pRoot, "heartbeatTimerCounter", u64buf);

    // callback
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpOnPing);
    cJSON_AddStringToObject(pRoot, "FpOnPing", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpOnPingReply);
    cJSON_AddStringToObject(pRoot, "FpOnPingReply", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpOnRequestVote);
    cJSON_AddStringToObject(pRoot, "FpOnRequestVote", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpOnRequestVoteReply);
    cJSON_AddStringToObject(pRoot, "FpOnRequestVoteReply", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpOnAppendEntries);
    cJSON_AddStringToObject(pRoot, "FpOnAppendEntries", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpOnAppendEntriesReply);
    cJSON_AddStringToObject(pRoot, "FpOnAppendEntriesReply", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpOnTimeout);
    cJSON_AddStringToObject(pRoot, "FpOnTimeout", u64buf);

    // restoreFinish
    cJSON_AddNumberToObject(pRoot, "restoreFinish", pSyncNode->restoreFinish);

    // snapshot senders
    cJSON* pSenders = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "senders", pSenders);
    for (int i = 0; i < TSDB_MAX_REPLICA; ++i) {
      cJSON_AddItemToArray(pSenders, snapshotSender2Json((pSyncNode->senders)[i]));
    }

    // snapshot receivers
    cJSON* pReceivers = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "receiver", snapshotReceiver2Json(pSyncNode->pNewNodeReceiver));

    // changing
    cJSON_AddNumberToObject(pRoot, "changing", pSyncNode->changing);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SSyncNode", pRoot);
  return pJson;
}

char* syncNode2Str(const SSyncNode* pSyncNode) {
  cJSON* pJson = syncNode2Json(pSyncNode);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

void syncNodeEventLog(const SSyncNode* pSyncNode, char* str) {
  int32_t userStrLen = strlen(str);

  SSnapshot snapshot = {.data = NULL, .lastApplyIndex = -1, .lastApplyTerm = 0};
  if (pSyncNode->pFsm != NULL && pSyncNode->pFsm->FpGetSnapshotInfo != NULL) {
    pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
  }

  SyncIndex logLastIndex = SYNC_INDEX_INVALID;
  SyncIndex logBeginIndex = SYNC_INDEX_INVALID;
  if (pSyncNode->pLogStore != NULL) {
    logLastIndex = pSyncNode->pLogStore->syncLogLastIndex(pSyncNode->pLogStore);
    logBeginIndex = pSyncNode->pLogStore->syncLogBeginIndex(pSyncNode->pLogStore);
  }

  char* pCfgStr = syncCfg2SimpleStr(&(pSyncNode->pRaftCfg->cfg));
  char* printStr = "";
  if (pCfgStr != NULL) {
    printStr = pCfgStr;
  }

  if (userStrLen < 256) {
    char logBuf[256 + 256];
    if (pSyncNode != NULL && pSyncNode->pRaftCfg != NULL && pSyncNode->pRaftStore != NULL) {
      snprintf(logBuf, sizeof(logBuf),
               "vgId:%d, sync %s %s, tm:%" PRIu64 ", cmt:%" PRId64 ", fst:%" PRId64 ", lst:%" PRId64 ", snap:%" PRId64
               ", snap-tm:%" PRIu64
               ", sby:%d, "
               "stgy:%d, bch:%d, "
               "r-num:%d, "
               "lcfg:%" PRId64 ", chging:%d, rsto:%d, elt:%" PRId64 ", hb:%" PRId64 ", %s",
               pSyncNode->vgId, syncUtilState2String(pSyncNode->state), str, pSyncNode->pRaftStore->currentTerm,
               pSyncNode->commitIndex, logBeginIndex, logLastIndex, snapshot.lastApplyIndex, snapshot.lastApplyTerm,
               pSyncNode->pRaftCfg->isStandBy, pSyncNode->pRaftCfg->snapshotStrategy, pSyncNode->pRaftCfg->batchSize,
               pSyncNode->replicaNum, pSyncNode->pRaftCfg->lastConfigIndex, pSyncNode->changing,
               pSyncNode->restoreFinish, pSyncNode->electTimerLogicClockUser, pSyncNode->heartbeatTimerLogicClockUser,
               printStr);
    } else {
      snprintf(logBuf, sizeof(logBuf), "%s", str);
    }
    // sDebug("%s", logBuf);
    // sInfo("%s", logBuf);
    sTrace("%s", logBuf);

  } else {
    int   len = 256 + userStrLen;
    char* s = (char*)taosMemoryMalloc(len);
    if (pSyncNode != NULL && pSyncNode->pRaftCfg != NULL && pSyncNode->pRaftStore != NULL) {
      snprintf(s, len,
               "vgId:%d, sync %s %s, tm:%" PRIu64 ", cmt:%" PRId64 ", fst:%" PRId64 ", lst:%" PRId64 ", snap:%" PRId64
               ", snap-tm:%" PRIu64
               ", sby:%d, "
               "stgy:%d, bch:%d, "
               "r-num:%d, "
               "lcfg:%" PRId64 ", chging:%d, rsto:%d, %s",
               pSyncNode->vgId, syncUtilState2String(pSyncNode->state), str, pSyncNode->pRaftStore->currentTerm,
               pSyncNode->commitIndex, logBeginIndex, logLastIndex, snapshot.lastApplyIndex, snapshot.lastApplyTerm,
               pSyncNode->pRaftCfg->isStandBy, pSyncNode->pRaftCfg->snapshotStrategy, pSyncNode->pRaftCfg->batchSize,
               pSyncNode->replicaNum, pSyncNode->pRaftCfg->lastConfigIndex, pSyncNode->changing,
               pSyncNode->restoreFinish, printStr);
    } else {
      snprintf(s, len, "%s", str);
    }
    // sDebug("%s", s);
    // sInfo("%s", s);
    sTrace("%s", s);
    taosMemoryFree(s);
  }

  taosMemoryFree(pCfgStr);
}

void syncNodeErrorLog(const SSyncNode* pSyncNode, char* str) {
  int32_t userStrLen = strlen(str);

  SSnapshot snapshot = {.data = NULL, .lastApplyIndex = -1, .lastApplyTerm = 0};
  if (pSyncNode->pFsm != NULL && pSyncNode->pFsm->FpGetSnapshotInfo != NULL) {
    pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
  }

  SyncIndex logLastIndex = SYNC_INDEX_INVALID;
  SyncIndex logBeginIndex = SYNC_INDEX_INVALID;
  if (pSyncNode->pLogStore != NULL) {
    logLastIndex = pSyncNode->pLogStore->syncLogLastIndex(pSyncNode->pLogStore);
    logBeginIndex = pSyncNode->pLogStore->syncLogBeginIndex(pSyncNode->pLogStore);
  }

  char* pCfgStr = syncCfg2SimpleStr(&(pSyncNode->pRaftCfg->cfg));
  char* printStr = "";
  if (pCfgStr != NULL) {
    printStr = pCfgStr;
  }

  if (userStrLen < 256) {
    char logBuf[256 + 256];
    if (pSyncNode != NULL && pSyncNode->pRaftCfg != NULL && pSyncNode->pRaftStore != NULL) {
      snprintf(logBuf, sizeof(logBuf),
               "vgId:%d, sync %s %s, tm:%" PRIu64 ", cmt:%" PRId64 ", fst:%" PRId64 ", lst:%" PRId64 ", snap:%" PRId64
               ", snap-tm:%" PRIu64
               ", sby:%d, "
               "stgy:%d, bch:%d, "
               "r-num:%d, "
               "lcfg:%" PRId64 ", chging:%d, rsto:%d, %s",
               pSyncNode->vgId, syncUtilState2String(pSyncNode->state), str, pSyncNode->pRaftStore->currentTerm,
               pSyncNode->commitIndex, logBeginIndex, logLastIndex, snapshot.lastApplyIndex, snapshot.lastApplyTerm,
               pSyncNode->pRaftCfg->isStandBy, pSyncNode->pRaftCfg->snapshotStrategy, pSyncNode->pRaftCfg->batchSize,
               pSyncNode->replicaNum, pSyncNode->pRaftCfg->lastConfigIndex, pSyncNode->changing,
               pSyncNode->restoreFinish, printStr);
    } else {
      snprintf(logBuf, sizeof(logBuf), "%s", str);
    }
    sError("%s", logBuf);

  } else {
    int   len = 256 + userStrLen;
    char* s = (char*)taosMemoryMalloc(len);
    if (pSyncNode != NULL && pSyncNode->pRaftCfg != NULL && pSyncNode->pRaftStore != NULL) {
      snprintf(s, len,
               "vgId:%d, sync %s %s, tm:%" PRIu64 ", cmt:%" PRId64 ", fst:%" PRId64 ", lst:%" PRId64 ", snap:%" PRId64
               ", snap-tm:%" PRIu64
               ", sby:%d, "
               "stgy:%d, bch:%d, "
               "r-num:%d, "
               "lcfg:%" PRId64 ", chging:%d, rsto:%d, %s",
               pSyncNode->vgId, syncUtilState2String(pSyncNode->state), str, pSyncNode->pRaftStore->currentTerm,
               pSyncNode->commitIndex, logBeginIndex, logLastIndex, snapshot.lastApplyIndex, snapshot.lastApplyTerm,
               pSyncNode->pRaftCfg->isStandBy, pSyncNode->pRaftCfg->snapshotStrategy, pSyncNode->pRaftCfg->batchSize,
               pSyncNode->replicaNum, pSyncNode->pRaftCfg->lastConfigIndex, pSyncNode->changing,
               pSyncNode->restoreFinish, printStr);
    } else {
      snprintf(s, len, "%s", str);
    }
    sError("%s", s);
    taosMemoryFree(s);
  }

  taosMemoryFree(pCfgStr);
}

char* syncNode2SimpleStr(const SSyncNode* pSyncNode) {
  int   len = 256;
  char* s = (char*)taosMemoryMalloc(len);

  SSnapshot snapshot = {.data = NULL, .lastApplyIndex = -1, .lastApplyTerm = 0};
  if (pSyncNode->pFsm->FpGetSnapshotInfo != NULL) {
    pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
  }
  SyncIndex logLastIndex = pSyncNode->pLogStore->syncLogLastIndex(pSyncNode->pLogStore);
  SyncIndex logBeginIndex = pSyncNode->pLogStore->syncLogBeginIndex(pSyncNode->pLogStore);

  snprintf(s, len,
           "vgId:%d, sync %s, tm:%" PRIu64 ", cmt:%" PRId64 ", fst:%" PRId64 ", lst:%" PRId64 ", snap:%" PRId64
           ", sby:%d, "
           "r-num:%d, "
           "lcfg:%" PRId64 ", chging:%d, rsto:%d",
           pSyncNode->vgId, syncUtilState2String(pSyncNode->state), pSyncNode->pRaftStore->currentTerm,
           pSyncNode->commitIndex, logBeginIndex, logLastIndex, snapshot.lastApplyIndex, pSyncNode->pRaftCfg->isStandBy,
           pSyncNode->replicaNum, pSyncNode->pRaftCfg->lastConfigIndex, pSyncNode->changing, pSyncNode->restoreFinish);

  return s;
}

bool syncNodeInConfig(SSyncNode* pSyncNode, const SSyncCfg* config) {
  bool b1 = false;
  bool b2 = false;

  for (int i = 0; i < config->replicaNum; ++i) {
    if (strcmp((config->nodeInfo)[i].nodeFqdn, pSyncNode->myNodeInfo.nodeFqdn) == 0 &&
        (config->nodeInfo)[i].nodePort == pSyncNode->myNodeInfo.nodePort) {
      b1 = true;
      break;
    }
  }

  for (int i = 0; i < config->replicaNum; ++i) {
    SRaftId raftId;
    raftId.addr = syncUtilAddr2U64((config->nodeInfo)[i].nodeFqdn, (config->nodeInfo)[i].nodePort);
    raftId.vgId = pSyncNode->vgId;

    if (syncUtilSameId(&raftId, &(pSyncNode->myRaftId))) {
      b2 = true;
      break;
    }
  }

  ASSERT(b1 == b2);
  return b1;
}

void syncNodeDoConfigChange(SSyncNode* pSyncNode, SSyncCfg* pNewConfig, SyncIndex lastConfigChangeIndex) {
  SSyncCfg oldConfig = pSyncNode->pRaftCfg->cfg;
  pSyncNode->pRaftCfg->cfg = *pNewConfig;
  pSyncNode->pRaftCfg->lastConfigIndex = lastConfigChangeIndex;

  bool IamInOld = syncNodeInConfig(pSyncNode, &oldConfig);
  bool IamInNew = syncNodeInConfig(pSyncNode, pNewConfig);

  bool isDrop = false;
  bool isAdd = false;

  if (IamInOld && !IamInNew) {
    isDrop = true;
  } else {
    isDrop = false;
  }

  if (!IamInOld && IamInNew) {
    isAdd = true;
  } else {
    isAdd = false;
  }

  // log begin config change
  do {
    char  eventLog[256];
    char* pOldCfgStr = syncCfg2SimpleStr(&oldConfig);
    char* pNewCfgStr = syncCfg2SimpleStr(pNewConfig);
    snprintf(eventLog, sizeof(eventLog), "begin do config change, from %s to %s", pOldCfgStr, pNewCfgStr);
    syncNodeEventLog(pSyncNode, eventLog);
    taosMemoryFree(pOldCfgStr);
    taosMemoryFree(pNewCfgStr);
  } while (0);

  if (IamInNew) {
    pSyncNode->pRaftCfg->isStandBy = 0;  // change isStandBy to normal
  }
  if (isDrop) {
    pSyncNode->pRaftCfg->isStandBy = 1;  // set standby
  }

  // add last config index
  raftCfgAddConfigIndex(pSyncNode->pRaftCfg, lastConfigChangeIndex);

  if (IamInNew) {
    //-----------------------------------------
    int32_t ret = 0;

    // save snapshot senders
    int32_t oldReplicaNum = pSyncNode->replicaNum;
    SRaftId oldReplicasId[TSDB_MAX_REPLICA];
    memcpy(oldReplicasId, pSyncNode->replicasId, sizeof(oldReplicasId));
    SSyncSnapshotSender* oldSenders[TSDB_MAX_REPLICA];
    for (int i = 0; i < TSDB_MAX_REPLICA; ++i) {
      oldSenders[i] = (pSyncNode->senders)[i];

      char* eventLog = snapshotSender2SimpleStr(oldSenders[i], "snapshot sender save old");
      syncNodeEventLog(pSyncNode, eventLog);
      taosMemoryFree(eventLog);
    }

    // init internal
    pSyncNode->myNodeInfo = pSyncNode->pRaftCfg->cfg.nodeInfo[pSyncNode->pRaftCfg->cfg.myIndex];
    syncUtilnodeInfo2raftId(&pSyncNode->myNodeInfo, pSyncNode->vgId, &pSyncNode->myRaftId);

    // init peersNum, peers, peersId
    pSyncNode->peersNum = pSyncNode->pRaftCfg->cfg.replicaNum - 1;
    int j = 0;
    for (int i = 0; i < pSyncNode->pRaftCfg->cfg.replicaNum; ++i) {
      if (i != pSyncNode->pRaftCfg->cfg.myIndex) {
        pSyncNode->peersNodeInfo[j] = pSyncNode->pRaftCfg->cfg.nodeInfo[i];
        j++;
      }
    }
    for (int i = 0; i < pSyncNode->peersNum; ++i) {
      syncUtilnodeInfo2raftId(&pSyncNode->peersNodeInfo[i], pSyncNode->vgId, &pSyncNode->peersId[i]);
    }

    // init replicaNum, replicasId
    pSyncNode->replicaNum = pSyncNode->pRaftCfg->cfg.replicaNum;
    for (int i = 0; i < pSyncNode->pRaftCfg->cfg.replicaNum; ++i) {
      syncUtilnodeInfo2raftId(&pSyncNode->pRaftCfg->cfg.nodeInfo[i], pSyncNode->vgId, &pSyncNode->replicasId[i]);
    }

    syncIndexMgrUpdate(pSyncNode->pNextIndex, pSyncNode);
    syncIndexMgrUpdate(pSyncNode->pMatchIndex, pSyncNode);
    voteGrantedUpdate(pSyncNode->pVotesGranted, pSyncNode);
    votesRespondUpdate(pSyncNode->pVotesRespond, pSyncNode);

    pSyncNode->quorum = syncUtilQuorum(pSyncNode->pRaftCfg->cfg.replicaNum);

    // reset snapshot senders

    // clear new
    for (int i = 0; i < TSDB_MAX_REPLICA; ++i) {
      (pSyncNode->senders)[i] = NULL;
    }

    // reset new
    for (int i = 0; i < pSyncNode->replicaNum; ++i) {
      // reset sender
      bool reset = false;
      for (int j = 0; j < TSDB_MAX_REPLICA; ++j) {
        if (syncUtilSameId(&(pSyncNode->replicasId)[i], &oldReplicasId[j])) {
          char     host[128];
          uint16_t port;
          syncUtilU642Addr((pSyncNode->replicasId)[i].addr, host, sizeof(host), &port);

          do {
            char eventLog[256];
            snprintf(eventLog, sizeof(eventLog), "snapshot sender reset for: %" PRIu64 ", newIndex:%d, %s:%d, %p",
                     (pSyncNode->replicasId)[i].addr, i, host, port, oldSenders[j]);
            syncNodeEventLog(pSyncNode, eventLog);
          } while (0);

          (pSyncNode->senders)[i] = oldSenders[j];
          oldSenders[j] = NULL;
          reset = true;

          // reset replicaIndex
          int32_t oldreplicaIndex = (pSyncNode->senders)[i]->replicaIndex;
          (pSyncNode->senders)[i]->replicaIndex = i;

          do {
            char eventLog[256];
            snprintf(eventLog, sizeof(eventLog),
                     "snapshot sender udpate replicaIndex from %d to %d, %s:%d, %p, reset:%d", oldreplicaIndex, i, host,
                     port, (pSyncNode->senders)[i], reset);
            syncNodeEventLog(pSyncNode, eventLog);
          } while (0);
        }
      }
    }

    // create new
    for (int i = 0; i < TSDB_MAX_REPLICA; ++i) {
      if ((pSyncNode->senders)[i] == NULL) {
        (pSyncNode->senders)[i] = snapshotSenderCreate(pSyncNode, i);

        char* eventLog = snapshotSender2SimpleStr((pSyncNode->senders)[i], "snapshot sender create new");
        syncNodeEventLog(pSyncNode, eventLog);
        taosMemoryFree(eventLog);
      }
    }

    // free old
    for (int i = 0; i < TSDB_MAX_REPLICA; ++i) {
      if (oldSenders[i] != NULL) {
        snapshotSenderDestroy(oldSenders[i]);

        do {
          char eventLog[128];
          snprintf(eventLog, sizeof(eventLog), "snapshot sender delete old %p replica-index:%d", oldSenders[i], i);
          syncNodeEventLog(pSyncNode, eventLog);
        } while (0);

        oldSenders[i] = NULL;
      }
    }

    // persist cfg
    raftCfgPersist(pSyncNode->pRaftCfg);

    char  tmpbuf[512];
    char* oldStr = syncCfg2SimpleStr(&oldConfig);
    char* newStr = syncCfg2SimpleStr(pNewConfig);
    snprintf(tmpbuf, sizeof(tmpbuf), "config change from %d to %d, index:%" PRId64 ", %s  -->  %s",
             oldConfig.replicaNum, pNewConfig->replicaNum, lastConfigChangeIndex, oldStr, newStr);
    taosMemoryFree(oldStr);
    taosMemoryFree(newStr);

    // change isStandBy to normal (election timeout)
    if (pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
      syncNodeBecomeLeader(pSyncNode, tmpbuf);

      // Raft 3.6.2 Committing entries from previous terms
      syncNodeAppendNoop(pSyncNode);
#if 0  // simon
      syncNodeReplicate(pSyncNode);
#endif
      syncMaybeAdvanceCommitIndex(pSyncNode);

    } else {
      syncNodeBecomeFollower(pSyncNode, tmpbuf);
    }
  } else {
    // persist cfg
    raftCfgPersist(pSyncNode->pRaftCfg);

    char  tmpbuf[512];
    char* oldStr = syncCfg2SimpleStr(&oldConfig);
    char* newStr = syncCfg2SimpleStr(pNewConfig);
    snprintf(tmpbuf, sizeof(tmpbuf), "do not config change from %d to %d, index:%" PRId64 ", %s  -->  %s",
             oldConfig.replicaNum, pNewConfig->replicaNum, lastConfigChangeIndex, oldStr, newStr);
    taosMemoryFree(oldStr);
    taosMemoryFree(newStr);
    syncNodeEventLog(pSyncNode, tmpbuf);
  }

_END:

  // log end config change
  do {
    char  eventLog[256];
    char* pOldCfgStr = syncCfg2SimpleStr(&oldConfig);
    char* pNewCfgStr = syncCfg2SimpleStr(pNewConfig);
    snprintf(eventLog, sizeof(eventLog), "end do config change, from %s to %s", pOldCfgStr, pNewCfgStr);
    syncNodeEventLog(pSyncNode, eventLog);
    taosMemoryFree(pOldCfgStr);
    taosMemoryFree(pNewCfgStr);
  } while (0);
  return;
}

SSyncNode* syncNodeAcquire(int64_t rid) {
  SSyncNode* pNode = taosAcquireRef(tsNodeRefId, rid);
  if (pNode == NULL) {
    sTrace("failed to acquire node from refId:%" PRId64, rid);
  }

  return pNode;
}

void syncNodeRelease(SSyncNode* pNode) { taosReleaseRef(tsNodeRefId, pNode->rid); }

// raft state change --------------
void syncNodeUpdateTerm(SSyncNode* pSyncNode, SyncTerm term) {
  if (term > pSyncNode->pRaftStore->currentTerm) {
    raftStoreSetTerm(pSyncNode->pRaftStore, term);
    char tmpBuf[64];
    snprintf(tmpBuf, sizeof(tmpBuf), "update term to %" PRIu64, term);
    syncNodeBecomeFollower(pSyncNode, tmpBuf);
    raftStoreClearVote(pSyncNode->pRaftStore);
  }
}

void syncNodeBecomeFollower(SSyncNode* pSyncNode, const char* debugStr) {
  // maybe clear leader cache
  if (pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
    pSyncNode->leaderCache = EMPTY_RAFT_ID;
  }

  // state change
  pSyncNode->state = TAOS_SYNC_STATE_FOLLOWER;
  syncNodeStopHeartbeatTimer(pSyncNode);

  // reset elect timer
  syncNodeResetElectTimer(pSyncNode);

  // trace log
  do {
    int32_t debugStrLen = strlen(debugStr);
    if (debugStrLen < 256) {
      char eventLog[256 + 64];
      snprintf(eventLog, sizeof(eventLog), "become follower %s", debugStr);
      syncNodeEventLog(pSyncNode, eventLog);
    } else {
      char* eventLog = taosMemoryMalloc(debugStrLen + 64);
      snprintf(eventLog, debugStrLen, "become follower %s", debugStr);
      syncNodeEventLog(pSyncNode, eventLog);
      taosMemoryFree(eventLog);
    }
  } while (0);
}

// TLA+ Spec
// \* Candidate i transitions to leader.
// BecomeLeader(i) ==
//     /\ state[i] = Candidate
//     /\ votesGranted[i] \in Quorum
//     /\ state'      = [state EXCEPT ![i] = Leader]
//     /\ nextIndex'  = [nextIndex EXCEPT ![i] =
//                          [j \in Server |-> Len(log[i]) + 1]]
//     /\ matchIndex' = [matchIndex EXCEPT ![i] =
//                          [j \in Server |-> 0]]
//     /\ elections'  = elections \cup
//                          {[eterm     |-> currentTerm[i],
//                            eleader   |-> i,
//                            elog      |-> log[i],
//                            evotes    |-> votesGranted[i],
//                            evoterLog |-> voterLog[i]]}
//     /\ UNCHANGED <<messages, currentTerm, votedFor, candidateVars, logVars>>
//
void syncNodeBecomeLeader(SSyncNode* pSyncNode, const char* debugStr) {
  // reset restoreFinish
  pSyncNode->restoreFinish = false;

  // state change
  pSyncNode->state = TAOS_SYNC_STATE_LEADER;

  // set leader cache
  pSyncNode->leaderCache = pSyncNode->myRaftId;

  for (int i = 0; i < pSyncNode->pNextIndex->replicaNum; ++i) {
    // maybe overwrite myself, no harm
    // just do it!

    // pSyncNode->pNextIndex->index[i] = pSyncNode->pLogStore->getLastIndex(pSyncNode->pLogStore) + 1;

    // maybe wal is deleted
    SyncIndex lastIndex;
    SyncTerm  lastTerm;
    int32_t   code = syncNodeGetLastIndexTerm(pSyncNode, &lastIndex, &lastTerm);
    ASSERT(code == 0);
    pSyncNode->pNextIndex->index[i] = lastIndex + 1;
  }

  for (int i = 0; i < pSyncNode->pMatchIndex->replicaNum; ++i) {
    // maybe overwrite myself, no harm
    // just do it!
    pSyncNode->pMatchIndex->index[i] = SYNC_INDEX_INVALID;
  }

  // update sender private term
  SSyncSnapshotSender* pMySender = syncNodeGetSnapshotSender(pSyncNode, &(pSyncNode->myRaftId));
  if (pMySender != NULL) {
    for (int i = 0; i < pSyncNode->pMatchIndex->replicaNum; ++i) {
      if ((pSyncNode->senders)[i]->privateTerm > pMySender->privateTerm) {
        pMySender->privateTerm = (pSyncNode->senders)[i]->privateTerm;
      }
    }
    (pMySender->privateTerm) += 100;
  }

  // stop elect timer
  syncNodeStopElectTimer(pSyncNode);

  // start heartbeat timer
  syncNodeStartHeartbeatTimer(pSyncNode);

  // trace log
  do {
    int32_t debugStrLen = strlen(debugStr);
    if (debugStrLen < 256) {
      char eventLog[256 + 64];
      snprintf(eventLog, sizeof(eventLog), "become leader %s", debugStr);
      syncNodeEventLog(pSyncNode, eventLog);
    } else {
      char* eventLog = taosMemoryMalloc(debugStrLen + 64);
      snprintf(eventLog, debugStrLen, "become leader %s", debugStr);
      syncNodeEventLog(pSyncNode, eventLog);
      taosMemoryFree(eventLog);
    }
  } while (0);
}

void syncNodeCandidate2Leader(SSyncNode* pSyncNode) {
  ASSERT(pSyncNode->state == TAOS_SYNC_STATE_CANDIDATE);
  ASSERT(voteGrantedMajority(pSyncNode->pVotesGranted));
  syncNodeBecomeLeader(pSyncNode, "candidate to leader");

  syncNodeLog2("==state change syncNodeCandidate2Leader==", pSyncNode);

  // Raft 3.6.2 Committing entries from previous terms
  syncNodeAppendNoop(pSyncNode);
#if 0  // simon
  syncNodeReplicate(pSyncNode);
#endif
  syncMaybeAdvanceCommitIndex(pSyncNode);
}

void syncNodeFollower2Candidate(SSyncNode* pSyncNode) {
  ASSERT(pSyncNode->state == TAOS_SYNC_STATE_FOLLOWER);
  pSyncNode->state = TAOS_SYNC_STATE_CANDIDATE;

  syncNodeEventLog(pSyncNode, "follower to candidate");
}

void syncNodeLeader2Follower(SSyncNode* pSyncNode) {
  ASSERT(pSyncNode->state == TAOS_SYNC_STATE_LEADER);
  syncNodeBecomeFollower(pSyncNode, "leader to follower");

  syncNodeEventLog(pSyncNode, "leader to follower");
}

void syncNodeCandidate2Follower(SSyncNode* pSyncNode) {
  ASSERT(pSyncNode->state == TAOS_SYNC_STATE_CANDIDATE);
  syncNodeBecomeFollower(pSyncNode, "candidate to follower");

  syncNodeEventLog(pSyncNode, "candidate to follower");
}

// raft vote --------------

// just called by syncNodeVoteForSelf
// need assert
void syncNodeVoteForTerm(SSyncNode* pSyncNode, SyncTerm term, SRaftId* pRaftId) {
  ASSERT(term == pSyncNode->pRaftStore->currentTerm);
  ASSERT(!raftStoreHasVoted(pSyncNode->pRaftStore));

  raftStoreVote(pSyncNode->pRaftStore, pRaftId);
}

// simulate get vote from outside
void syncNodeVoteForSelf(SSyncNode* pSyncNode) {
  syncNodeVoteForTerm(pSyncNode, pSyncNode->pRaftStore->currentTerm, &(pSyncNode->myRaftId));

  SyncRequestVoteReply* pMsg = syncRequestVoteReplyBuild(pSyncNode->vgId);
  pMsg->srcId = pSyncNode->myRaftId;
  pMsg->destId = pSyncNode->myRaftId;
  pMsg->term = pSyncNode->pRaftStore->currentTerm;
  pMsg->voteGranted = true;

  voteGrantedVote(pSyncNode->pVotesGranted, pMsg);
  votesRespondAdd(pSyncNode->pVotesRespond, pMsg);
  syncRequestVoteReplyDestroy(pMsg);
}

// snapshot --------------

// return if has a snapshot
bool syncNodeHasSnapshot(SSyncNode* pSyncNode) {
  bool      ret = false;
  SSnapshot snapshot = {.data = NULL, .lastApplyIndex = -1, .lastApplyTerm = 0, .lastConfigIndex = -1};
  if (pSyncNode->pFsm->FpGetSnapshotInfo != NULL) {
    pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
    if (snapshot.lastApplyIndex >= SYNC_INDEX_BEGIN) {
      ret = true;
    }
  }
  return ret;
}

// return max(logLastIndex, snapshotLastIndex)
// if no snapshot and log, return -1
SyncIndex syncNodeGetLastIndex(SSyncNode* pSyncNode) {
  SSnapshot snapshot = {.data = NULL, .lastApplyIndex = -1, .lastApplyTerm = 0, .lastConfigIndex = -1};
  if (pSyncNode->pFsm->FpGetSnapshotInfo != NULL) {
    pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
  }
  SyncIndex logLastIndex = pSyncNode->pLogStore->syncLogLastIndex(pSyncNode->pLogStore);

  SyncIndex lastIndex = logLastIndex > snapshot.lastApplyIndex ? logLastIndex : snapshot.lastApplyIndex;
  return lastIndex;
}

// return the last term of snapshot and log
// if error, return SYNC_TERM_INVALID (by syncLogLastTerm)
SyncTerm syncNodeGetLastTerm(SSyncNode* pSyncNode) {
  SyncTerm lastTerm = 0;
  if (syncNodeHasSnapshot(pSyncNode)) {
    // has snapshot
    SSnapshot snapshot = {.data = NULL, .lastApplyIndex = -1, .lastApplyTerm = 0, .lastConfigIndex = -1};
    if (pSyncNode->pFsm->FpGetSnapshotInfo != NULL) {
      pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
    }

    SyncIndex logLastIndex = pSyncNode->pLogStore->syncLogLastIndex(pSyncNode->pLogStore);
    if (logLastIndex > snapshot.lastApplyIndex) {
      lastTerm = pSyncNode->pLogStore->syncLogLastTerm(pSyncNode->pLogStore);
    } else {
      lastTerm = snapshot.lastApplyTerm;
    }

  } else {
    // no snapshot
    lastTerm = pSyncNode->pLogStore->syncLogLastTerm(pSyncNode->pLogStore);
  }

  return lastTerm;
}

// get last index and term along with snapshot
int32_t syncNodeGetLastIndexTerm(SSyncNode* pSyncNode, SyncIndex* pLastIndex, SyncTerm* pLastTerm) {
  *pLastIndex = syncNodeGetLastIndex(pSyncNode);
  *pLastTerm = syncNodeGetLastTerm(pSyncNode);
  return 0;
}

// return append-entries first try index
SyncIndex syncNodeSyncStartIndex(SSyncNode* pSyncNode) {
  SyncIndex syncStartIndex = syncNodeGetLastIndex(pSyncNode) + 1;
  return syncStartIndex;
}

// if index > 0, return index - 1
// else, return -1
SyncIndex syncNodeGetPreIndex(SSyncNode* pSyncNode, SyncIndex index) {
  SyncIndex preIndex = index - 1;
  if (preIndex < SYNC_INDEX_INVALID) {
    preIndex = SYNC_INDEX_INVALID;
  }

  return preIndex;
}

// if index < 0, return SYNC_TERM_INVALID
// if index == 0, return 0
// if index > 0, return preTerm
// if error, return SYNC_TERM_INVALID
SyncTerm syncNodeGetPreTerm(SSyncNode* pSyncNode, SyncIndex index) {
  if (index < SYNC_INDEX_BEGIN) {
    return SYNC_TERM_INVALID;
  }

  if (index == SYNC_INDEX_BEGIN) {
    return 0;
  }

  SyncTerm        preTerm = 0;
  SyncIndex       preIndex = index - 1;
  SSyncRaftEntry* pPreEntry = NULL;
  int32_t         code = pSyncNode->pLogStore->syncLogGetEntry(pSyncNode->pLogStore, preIndex, &pPreEntry);

  SSnapshot snapshot = {.data = NULL,
                        .lastApplyIndex = SYNC_INDEX_INVALID,
                        .lastApplyTerm = SYNC_TERM_INVALID,
                        .lastConfigIndex = SYNC_INDEX_INVALID};

  if (code == 0) {
    ASSERT(pPreEntry != NULL);
    preTerm = pPreEntry->term;
    taosMemoryFree(pPreEntry);
    return preTerm;
  } else {
    if (pSyncNode->pFsm->FpGetSnapshotInfo != NULL) {
      pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
      if (snapshot.lastApplyIndex == preIndex) {
        return snapshot.lastApplyTerm;
      }
    }
  }

  do {
    char logBuf[128];
    snprintf(logBuf, sizeof(logBuf),
             "sync node get pre term error, index:%" PRId64 ", snap-index:%" PRId64 ", snap-term:%" PRIu64, index,
             snapshot.lastApplyIndex, snapshot.lastApplyTerm);
    syncNodeErrorLog(pSyncNode, logBuf);
  } while (0);

  return SYNC_TERM_INVALID;
}

// get pre index and term of "index"
int32_t syncNodeGetPreIndexTerm(SSyncNode* pSyncNode, SyncIndex index, SyncIndex* pPreIndex, SyncTerm* pPreTerm) {
  *pPreIndex = syncNodeGetPreIndex(pSyncNode, index);
  *pPreTerm = syncNodeGetPreTerm(pSyncNode, index);
  return 0;
}

// for debug --------------
void syncNodePrint(SSyncNode* pObj) {
  char* serialized = syncNode2Str(pObj);
  printf("syncNodePrint | len:%" PRIu64 " | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncNodePrint2(char* s, SSyncNode* pObj) {
  char* serialized = syncNode2Str(pObj);
  printf("syncNodePrint2 | len:%" PRIu64 " | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncNodeLog(SSyncNode* pObj) {
  char* serialized = syncNode2Str(pObj);
  sTraceLong("syncNodeLog | len:%" PRIu64 " | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncNodeLog2(char* s, SSyncNode* pObj) {
  if (gRaftDetailLog) {
    char* serialized = syncNode2Str(pObj);
    sTraceLong("syncNodeLog2 | len:%" PRIu64 " | %s | %s", strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
}

void syncNodeLog3(char* s, SSyncNode* pObj) {
  char* serialized = syncNode2Str(pObj);
  sTraceLong("syncNodeLog3 | len:%" PRIu64 " | %s | %s", strlen(serialized), s, serialized);
  taosMemoryFree(serialized);
}

// ------ local funciton ---------
// enqueue message ----
static void syncNodeEqPingTimer(void* param, void* tmrId) {
  SSyncNode* pSyncNode = (SSyncNode*)param;
  if (atomic_load_64(&pSyncNode->pingTimerLogicClockUser) <= atomic_load_64(&pSyncNode->pingTimerLogicClock)) {
    SyncTimeout* pSyncMsg = syncTimeoutBuild2(SYNC_TIMEOUT_PING, atomic_load_64(&pSyncNode->pingTimerLogicClock),
                                              pSyncNode->pingTimerMS, pSyncNode->vgId, pSyncNode);
    SRpcMsg      rpcMsg;
    syncTimeout2RpcMsg(pSyncMsg, &rpcMsg);
    syncRpcMsgLog2((char*)"==syncNodeEqPingTimer==", &rpcMsg);
    if (pSyncNode->FpEqMsg != NULL) {
      int32_t code = pSyncNode->FpEqMsg(pSyncNode->msgcb, &rpcMsg);
      if (code != 0) {
        sError("vgId:%d, sync enqueue ping msg error, code:%d", pSyncNode->vgId, code);
        rpcFreeCont(rpcMsg.pCont);
        syncTimeoutDestroy(pSyncMsg);
        return;
      }
    } else {
      sTrace("syncNodeEqPingTimer pSyncNode->FpEqMsg is NULL");
    }
    syncTimeoutDestroy(pSyncMsg);

    if (syncEnvIsStart()) {
      taosTmrReset(syncNodeEqPingTimer, pSyncNode->pingTimerMS, pSyncNode, gSyncEnv->pTimerManager,
                   &pSyncNode->pPingTimer);
    } else {
      sError("sync env is stop, syncNodeEqPingTimer");
    }

  } else {
    sTrace("==syncNodeEqPingTimer== pingTimerLogicClock:%" PRIu64 ", pingTimerLogicClockUser:%" PRIu64,
           pSyncNode->pingTimerLogicClock, pSyncNode->pingTimerLogicClockUser);
  }
}

static void syncNodeEqElectTimer(void* param, void* tmrId) {
  SSyncNode* pSyncNode = (SSyncNode*)param;
  if (atomic_load_64(&pSyncNode->electTimerLogicClockUser) <= atomic_load_64(&pSyncNode->electTimerLogicClock)) {
    SyncTimeout* pSyncMsg = syncTimeoutBuild2(SYNC_TIMEOUT_ELECTION, atomic_load_64(&pSyncNode->electTimerLogicClock),
                                              pSyncNode->electTimerMS, pSyncNode->vgId, pSyncNode);
    SRpcMsg      rpcMsg;
    syncTimeout2RpcMsg(pSyncMsg, &rpcMsg);
    syncRpcMsgLog2((char*)"==syncNodeEqElectTimer==", &rpcMsg);
    if (pSyncNode->FpEqMsg != NULL) {
      int32_t code = pSyncNode->FpEqMsg(pSyncNode->msgcb, &rpcMsg);
      if (code != 0) {
        sError("vgId:%d, sync enqueue elect msg error, code:%d", pSyncNode->vgId, code);
        rpcFreeCont(rpcMsg.pCont);
        syncTimeoutDestroy(pSyncMsg);
        return;
      }
    } else {
      sTrace("syncNodeEqElectTimer FpEqMsg is NULL");
    }
    syncTimeoutDestroy(pSyncMsg);

    // reset timer ms
    if (syncEnvIsStart()) {
      pSyncNode->electTimerMS = syncUtilElectRandomMS(pSyncNode->electBaseLine, 2 * pSyncNode->electBaseLine);
      taosTmrReset(syncNodeEqElectTimer, pSyncNode->electTimerMS, pSyncNode, gSyncEnv->pTimerManager,
                   &pSyncNode->pElectTimer);
    } else {
      sError("sync env is stop, syncNodeEqElectTimer");
    }
  } else {
    sTrace("==syncNodeEqElectTimer== electTimerLogicClock:%" PRIu64 ", electTimerLogicClockUser:%" PRIu64,
           pSyncNode->electTimerLogicClock, pSyncNode->electTimerLogicClockUser);
  }
}

static void syncNodeEqHeartbeatTimer(void* param, void* tmrId) {
  SSyncNode* pSyncNode = (SSyncNode*)param;
  if (pSyncNode->replicaNum > 1) {
    if (atomic_load_64(&pSyncNode->heartbeatTimerLogicClockUser) <=
        atomic_load_64(&pSyncNode->heartbeatTimerLogicClock)) {
      SyncTimeout* pSyncMsg =
          syncTimeoutBuild2(SYNC_TIMEOUT_HEARTBEAT, atomic_load_64(&pSyncNode->heartbeatTimerLogicClock),
                            pSyncNode->heartbeatTimerMS, pSyncNode->vgId, pSyncNode);
      SRpcMsg rpcMsg;
      syncTimeout2RpcMsg(pSyncMsg, &rpcMsg);
      syncRpcMsgLog2((char*)"==syncNodeEqHeartbeatTimer==", &rpcMsg);
      if (pSyncNode->FpEqMsg != NULL) {
        int32_t code = pSyncNode->FpEqMsg(pSyncNode->msgcb, &rpcMsg);
        if (code != 0) {
          sError("vgId:%d, sync enqueue timer msg error, code:%d", pSyncNode->vgId, code);
          rpcFreeCont(rpcMsg.pCont);
          syncTimeoutDestroy(pSyncMsg);
          return;
        }
      } else {
        sError("syncNodeEqHeartbeatTimer FpEqMsg is NULL");
      }
      syncTimeoutDestroy(pSyncMsg);

      if (syncEnvIsStart()) {
        taosTmrReset(syncNodeEqHeartbeatTimer, pSyncNode->heartbeatTimerMS, pSyncNode, gSyncEnv->pTimerManager,
                     &pSyncNode->pHeartbeatTimer);
      } else {
        sError("sync env is stop, syncNodeEqHeartbeatTimer");
      }
    } else {
      sTrace("==syncNodeEqHeartbeatTimer== heartbeatTimerLogicClock:%" PRIu64 ", heartbeatTimerLogicClockUser:%" PRIu64
             "",
             pSyncNode->heartbeatTimerLogicClock, pSyncNode->heartbeatTimerLogicClockUser);
    }
  }
}

static int32_t syncNodeEqNoop(SSyncNode* ths) {
  int32_t ret = 0;
  ASSERT(ths->state == TAOS_SYNC_STATE_LEADER);

  SyncIndex       index = ths->pLogStore->syncLogWriteIndex(ths->pLogStore);
  SyncTerm        term = ths->pRaftStore->currentTerm;
  SSyncRaftEntry* pEntry = syncEntryBuildNoop(term, index, ths->vgId);
  ASSERT(pEntry != NULL);

  uint32_t           entryLen;
  char*              serialized = syncEntrySerialize(pEntry, &entryLen);
  SyncClientRequest* pSyncMsg = syncClientRequestBuild(entryLen);
  ASSERT(pSyncMsg->dataLen == entryLen);
  memcpy(pSyncMsg->data, serialized, entryLen);

  SRpcMsg rpcMsg = {0};
  syncClientRequest2RpcMsg(pSyncMsg, &rpcMsg);
  if (ths->FpEqMsg != NULL) {
    ths->FpEqMsg(ths->msgcb, &rpcMsg);
  } else {
    sTrace("syncNodeEqNoop pSyncNode->FpEqMsg is NULL");
  }

  taosMemoryFree(serialized);
  syncClientRequestDestroy(pSyncMsg);

  return ret;
}

static int32_t syncNodeAppendNoop(SSyncNode* ths) {
  int32_t ret = 0;

  SyncIndex       index = ths->pLogStore->syncLogWriteIndex(ths->pLogStore);
  SyncTerm        term = ths->pRaftStore->currentTerm;
  SSyncRaftEntry* pEntry = syncEntryBuildNoop(term, index, ths->vgId);
  ASSERT(pEntry != NULL);

  if (ths->state == TAOS_SYNC_STATE_LEADER) {
    int32_t code = ths->pLogStore->syncLogAppendEntry(ths->pLogStore, pEntry);
    ASSERT(code == 0);
    syncNodeReplicate(ths);
  }

  syncEntryDestory(pEntry);
  return ret;
}

// on message ----
int32_t syncNodeOnPingCb(SSyncNode* ths, SyncPing* pMsg) {
  // log state
  char logBuf[1024] = {0};
  snprintf(logBuf, sizeof(logBuf),
           "==syncNodeOnPingCb== vgId:%d, state: %d, %s, term:%" PRIu64 " electTimerLogicClock:%" PRIu64
           ", "
           "electTimerLogicClockUser:%" PRIu64 ", electTimerMS:%d",
           ths->vgId, ths->state, syncUtilState2String(ths->state), ths->pRaftStore->currentTerm,
           ths->electTimerLogicClock, ths->electTimerLogicClockUser, ths->electTimerMS);

  int32_t ret = 0;
  syncPingLog2(logBuf, pMsg);
  SyncPingReply* pMsgReply = syncPingReplyBuild3(&ths->myRaftId, &pMsg->srcId, ths->vgId);
  SRpcMsg        rpcMsg;
  syncPingReply2RpcMsg(pMsgReply, &rpcMsg);

  /*
    // htonl
    SMsgHead* pHead = rpcMsg.pCont;
    pHead->contLen = htonl(pHead->contLen);
    pHead->vgId = htonl(pHead->vgId);
  */

  syncNodeSendMsgById(&pMsgReply->destId, ths, &rpcMsg);

  return ret;
}

int32_t syncNodeOnPingReplyCb(SSyncNode* ths, SyncPingReply* pMsg) {
  int32_t ret = 0;
  syncPingReplyLog2("==syncNodeOnPingReplyCb==", pMsg);
  return ret;
}

// TLA+ Spec
// ClientRequest(i, v) ==
//     /\ state[i] = Leader
//     /\ LET entry == [term  |-> currentTerm[i],
//                      value |-> v]
//            newLog == Append(log[i], entry)
//        IN  log' = [log EXCEPT ![i] = newLog]
//     /\ UNCHANGED <<messages, serverVars, candidateVars,
//                    leaderVars, commitIndex>>
//
int32_t syncNodeOnClientRequestCb(SSyncNode* ths, SyncClientRequest* pMsg, SyncIndex* pRetIndex) {
  int32_t ret = 0;
  int32_t code = 0;
  syncClientRequestLog2("==syncNodeOnClientRequestCb==", pMsg);

  SyncIndex       index = ths->pLogStore->syncLogWriteIndex(ths->pLogStore);
  SyncTerm        term = ths->pRaftStore->currentTerm;
  SSyncRaftEntry* pEntry = syncEntryBuild2((SyncClientRequest*)pMsg, term, index);
  ASSERT(pEntry != NULL);

  if (ths->state == TAOS_SYNC_STATE_LEADER) {
    // append entry
    code = ths->pLogStore->syncLogAppendEntry(ths->pLogStore, pEntry);
    if (code != 0) {
      // del resp mgr, call FpCommitCb
      ASSERT(0);
      return -1;
    }

    // if mulit replica, start replicate right now
    if (ths->replicaNum > 1) {
      syncNodeReplicate(ths);

      // pre commit
      syncNodePreCommit(ths, pEntry, 0);
    }

    // if only myself, maybe commit right now
    if (ths->replicaNum == 1) {
      syncMaybeAdvanceCommitIndex(ths);
    }
  }

  if (pRetIndex != NULL) {
    if (ret == 0 && pEntry != NULL) {
      *pRetIndex = pEntry->index;
    } else {
      *pRetIndex = SYNC_INDEX_INVALID;
    }
  }

  syncEntryDestory(pEntry);
  return ret;
}

int32_t syncNodeOnClientRequestBatchCb(SSyncNode* ths, SyncClientRequestBatch* pMsg) {
  int32_t code = 0;

  if (ths->state != TAOS_SYNC_STATE_LEADER) {
    // call FpCommitCb, delete resp mgr
    return -1;
  }

  SyncIndex index = ths->pLogStore->syncLogWriteIndex(ths->pLogStore);
  SyncTerm  term = ths->pRaftStore->currentTerm;

  int32_t    raftMetaArrayLen = sizeof(SRaftMeta) * pMsg->dataCount;
  int32_t    rpcArrayLen = sizeof(SRpcMsg) * pMsg->dataCount;
  SRaftMeta* raftMetaArr = (SRaftMeta*)(pMsg->data);
  SRpcMsg*   msgArr = (SRpcMsg*)((char*)(pMsg->data) + raftMetaArrayLen);
  for (int32_t i = 0; i < pMsg->dataCount; ++i) {
    SSyncRaftEntry* pEntry = syncEntryBuild(msgArr[i].contLen);
    ASSERT(pEntry != NULL);

    pEntry->originalRpcType = msgArr[i].msgType;
    pEntry->seqNum = raftMetaArr[i].seqNum;
    pEntry->isWeak = raftMetaArr[i].isWeak;
    pEntry->term = term;
    pEntry->index = index;
    memcpy(pEntry->data, msgArr[i].pCont, msgArr[i].contLen);
    ASSERT(msgArr[i].contLen == pEntry->dataLen);

    code = ths->pLogStore->syncLogAppendEntry(ths->pLogStore, pEntry);
    if (code != 0) {
      // del resp mgr, call FpCommitCb
      ASSERT(0);
      return -1;
    }

    // update rpc msg conn apply.index
    msgArr[i].info.conn.applyIndex = pEntry->index;
  }

  // fsync once
  SSyncLogStoreData* pData = ths->pLogStore->data;
  SWal*              pWal = pData->pWal;
  walFsync(pWal, true);

  if (ths->replicaNum > 1) {
    // if multi replica, start replicate right now
    syncNodeReplicate(ths);

  } else if (ths->replicaNum == 1) {
    // one replica
    syncMaybeAdvanceCommitIndex(ths);
  }

  return 0;
}

static void syncFreeNode(void* param) {
  SSyncNode* pNode = param;
  // inner object already free
  // syncNodePrint2((char*)"==syncFreeNode==", pNode);

  taosMemoryFree(pNode);
}

const char* syncStr(ESyncState state) {
  switch (state) {
    case TAOS_SYNC_STATE_FOLLOWER:
      return "follower";
    case TAOS_SYNC_STATE_CANDIDATE:
      return "candidate";
    case TAOS_SYNC_STATE_LEADER:
      return "leader";
    default:
      return "error";
  }
}

int32_t syncDoLeaderTransfer(SSyncNode* ths, SRpcMsg* pRpcMsg, SSyncRaftEntry* pEntry) {
  SyncLeaderTransfer* pSyncLeaderTransfer = syncLeaderTransferFromRpcMsg2(pRpcMsg);

  if (ths->state != TAOS_SYNC_STATE_FOLLOWER) {
    syncNodeEventLog(ths, "I am not follower, can not do leader transfer");
    return 0;
  }
  syncNodeEventLog(ths, "do leader transfer");

  bool sameId = syncUtilSameId(&(pSyncLeaderTransfer->newLeaderId), &(ths->myRaftId));
  bool sameNodeInfo = strcmp(pSyncLeaderTransfer->newNodeInfo.nodeFqdn, ths->myNodeInfo.nodeFqdn) == 0 &&
                      pSyncLeaderTransfer->newNodeInfo.nodePort == ths->myNodeInfo.nodePort;

  bool same = sameId || sameNodeInfo;
  if (same) {
    // reset elect timer now!
    int32_t electMS = 1;
    int32_t ret = syncNodeRestartElectTimer(ths, electMS);
    ASSERT(ret == 0);

    char eventLog[256];
    snprintf(eventLog, sizeof(eventLog), "maybe leader transfer to %s:%d %" PRIu64,
             pSyncLeaderTransfer->newNodeInfo.nodeFqdn, pSyncLeaderTransfer->newNodeInfo.nodePort,
             pSyncLeaderTransfer->newLeaderId.addr);
    syncNodeEventLog(ths, eventLog);
  }

  if (ths->pFsm->FpLeaderTransferCb != NULL) {
    SFsmCbMeta cbMeta = {0};
    cbMeta.code = 0;
    cbMeta.currentTerm = ths->pRaftStore->currentTerm;
    cbMeta.flag = 0;
    cbMeta.index = pEntry->index;
    cbMeta.lastConfigIndex = syncNodeGetSnapshotConfigIndex(ths, cbMeta.index);
    cbMeta.isWeak = pEntry->isWeak;
    cbMeta.seqNum = pEntry->seqNum;
    cbMeta.state = ths->state;
    cbMeta.term = pEntry->term;
    ths->pFsm->FpLeaderTransferCb(ths->pFsm, pRpcMsg, cbMeta);
  }

  syncLeaderTransferDestroy(pSyncLeaderTransfer);
  return 0;
}

int32_t syncNodeUpdateNewConfigIndex(SSyncNode* ths, SSyncCfg* pNewCfg) {
  for (int i = 0; i < pNewCfg->replicaNum; ++i) {
    SRaftId raftId;
    raftId.addr = syncUtilAddr2U64((pNewCfg->nodeInfo)[i].nodeFqdn, (pNewCfg->nodeInfo)[i].nodePort);
    raftId.vgId = ths->vgId;

    if (syncUtilSameId(&(ths->myRaftId), &raftId)) {
      pNewCfg->myIndex = i;
      return 0;
    }
  }

  return -1;
}

static int32_t syncNodeConfigChangeFinish(SSyncNode* ths, SRpcMsg* pRpcMsg, SSyncRaftEntry* pEntry) {
  SyncReconfigFinish* pFinish = syncReconfigFinishFromRpcMsg2(pRpcMsg);
  ASSERT(pFinish);

  if (ths->pFsm->FpReConfigCb != NULL) {
    SReConfigCbMeta cbMeta = {0};
    cbMeta.code = 0;
    cbMeta.index = pEntry->index;
    cbMeta.term = pEntry->term;
    cbMeta.seqNum = pEntry->seqNum;
    cbMeta.lastConfigIndex = syncNodeGetSnapshotConfigIndex(ths, pEntry->index);
    cbMeta.state = ths->state;
    cbMeta.currentTerm = ths->pRaftStore->currentTerm;
    cbMeta.isWeak = pEntry->isWeak;
    cbMeta.flag = 0;

    cbMeta.oldCfg = pFinish->oldCfg;
    cbMeta.newCfg = pFinish->newCfg;
    cbMeta.newCfgIndex = pFinish->newCfgIndex;
    cbMeta.newCfgTerm = pFinish->newCfgTerm;
    cbMeta.newCfgSeqNum = pFinish->newCfgSeqNum;

    ths->pFsm->FpReConfigCb(ths->pFsm, pRpcMsg, cbMeta);
  }

  // clear changing
  ths->changing = false;

  char  tmpbuf[512];
  char* oldStr = syncCfg2SimpleStr(&(pFinish->oldCfg));
  char* newStr = syncCfg2SimpleStr(&(pFinish->newCfg));
  snprintf(tmpbuf, sizeof(tmpbuf), "config change finish from %d to %d, index:%" PRId64 ", %s  -->  %s",
           pFinish->oldCfg.replicaNum, pFinish->newCfg.replicaNum, pFinish->newCfgIndex, oldStr, newStr);
  taosMemoryFree(oldStr);
  taosMemoryFree(newStr);
  syncNodeEventLog(ths, tmpbuf);

  syncReconfigFinishDestroy(pFinish);

  return 0;
}

static int32_t syncNodeConfigChange(SSyncNode* ths, SRpcMsg* pRpcMsg, SSyncRaftEntry* pEntry,
                                    SyncReconfigFinish* pFinish) {
  // set changing
  ths->changing = true;

  // old config
  SSyncCfg oldSyncCfg = ths->pRaftCfg->cfg;

  // new config
  SSyncCfg newSyncCfg;
  int32_t  ret = syncCfgFromStr(pRpcMsg->pCont, &newSyncCfg);
  ASSERT(ret == 0);

  // update new config myIndex
  syncNodeUpdateNewConfigIndex(ths, &newSyncCfg);

  // do config change
  syncNodeDoConfigChange(ths, &newSyncCfg, pEntry->index);

  // set pFinish
  pFinish->oldCfg = oldSyncCfg;
  pFinish->newCfg = newSyncCfg;
  pFinish->newCfgIndex = pEntry->index;
  pFinish->newCfgTerm = pEntry->term;
  pFinish->newCfgSeqNum = pEntry->seqNum;

  return 0;
}

static int32_t syncNodeProposeConfigChangeFinish(SSyncNode* ths, SyncReconfigFinish* pFinish) {
  SRpcMsg rpcMsg;
  syncReconfigFinish2RpcMsg(pFinish, &rpcMsg);

  int32_t code = syncNodePropose(ths, &rpcMsg, false);
  if (code != 0) {
    sError("syncNodeProposeConfigChangeFinish error");
    ths->changing = false;
  }
  return 0;
}

bool syncNodeIsOptimizedOneReplica(SSyncNode* ths, SRpcMsg* pMsg) {
  return (ths->replicaNum == 1 && syncUtilUserCommit(pMsg->msgType) && ths->vgId != 1);
}

int32_t syncNodeCommit(SSyncNode* ths, SyncIndex beginIndex, SyncIndex endIndex, uint64_t flag) {
  int32_t    code = 0;
  ESyncState state = flag;

  char eventLog[128];
  snprintf(eventLog, sizeof(eventLog), "commit wal from index:%" PRId64 " to index:%" PRId64, beginIndex, endIndex);
  syncNodeEventLog(ths, eventLog);

  // execute fsm
  if (ths->pFsm != NULL) {
    for (SyncIndex i = beginIndex; i <= endIndex; ++i) {
      if (i != SYNC_INDEX_INVALID) {
        SSyncRaftEntry* pEntry;
        code = ths->pLogStore->syncLogGetEntry(ths->pLogStore, i, &pEntry);
        ASSERT(code == 0);
        ASSERT(pEntry != NULL);

        SRpcMsg rpcMsg;
        syncEntry2OriginalRpc(pEntry, &rpcMsg);

        // user commit
        if ((ths->pFsm->FpCommitCb != NULL) && syncUtilUserCommit(pEntry->originalRpcType)) {
          bool internalExecute = true;
          if ((ths->replicaNum == 1) && ths->restoreFinish && ths->vgId != 1) {
            internalExecute = false;
          }

          do {
            char logBuf[128];
            snprintf(logBuf, sizeof(logBuf), "commit index:%" PRId64 ", internal:%d", i, internalExecute);
            syncNodeEventLog(ths, logBuf);
          } while (0);

          // execute fsm in apply thread, or execute outside syncPropose
          if (internalExecute) {
            SFsmCbMeta cbMeta = {0};
            cbMeta.index = pEntry->index;
            cbMeta.lastConfigIndex = syncNodeGetSnapshotConfigIndex(ths, cbMeta.index);
            cbMeta.isWeak = pEntry->isWeak;
            cbMeta.code = 0;
            cbMeta.state = ths->state;
            cbMeta.seqNum = pEntry->seqNum;
            cbMeta.term = pEntry->term;
            cbMeta.currentTerm = ths->pRaftStore->currentTerm;
            cbMeta.flag = flag;

            ths->pFsm->FpCommitCb(ths->pFsm, &rpcMsg, cbMeta);
          }
        }

        // config change
        if (pEntry->originalRpcType == TDMT_SYNC_CONFIG_CHANGE) {
          SyncReconfigFinish* pFinish = syncReconfigFinishBuild(ths->vgId);
          ASSERT(pFinish != NULL);

          code = syncNodeConfigChange(ths, &rpcMsg, pEntry, pFinish);
          ASSERT(code == 0);

          if (ths->state == TAOS_SYNC_STATE_LEADER) {
            syncNodeProposeConfigChangeFinish(ths, pFinish);
          }
          syncReconfigFinishDestroy(pFinish);
        }

        // config change finish
        if (pEntry->originalRpcType == TDMT_SYNC_CONFIG_CHANGE_FINISH) {
          code = syncNodeConfigChangeFinish(ths, &rpcMsg, pEntry);
          ASSERT(code == 0);
        }

#if 0
        // execute in pre-commit
        // leader transfer
        if (pEntry->originalRpcType == TDMT_SYNC_LEADER_TRANSFER) {
          code = syncDoLeaderTransfer(ths, &rpcMsg, pEntry);
          ASSERT(code == 0);
        }
#endif

        // restore finish
        // if only snapshot, a noop entry will be append, so syncLogLastIndex is always ok
        if (pEntry->index == ths->pLogStore->syncLogLastIndex(ths->pLogStore)) {
          if (ths->restoreFinish == false) {
            if (ths->pFsm->FpRestoreFinishCb != NULL) {
              ths->pFsm->FpRestoreFinishCb(ths->pFsm);
            }
            ths->restoreFinish = true;

            char eventLog[128];
            snprintf(eventLog, sizeof(eventLog), "restore finish, index:%" PRId64, pEntry->index);
            syncNodeEventLog(ths, eventLog);
          }
        }

        rpcFreeCont(rpcMsg.pCont);
        syncEntryDestory(pEntry);
      }
    }
  }
  return 0;
}

bool syncNodeInRaftGroup(SSyncNode* ths, SRaftId* pRaftId) {
  for (int i = 0; i < ths->replicaNum; ++i) {
    if (syncUtilSameId(&((ths->replicasId)[i]), pRaftId)) {
      return true;
    }
  }
  return false;
}

SSyncSnapshotSender* syncNodeGetSnapshotSender(SSyncNode* ths, SRaftId* pDestId) {
  SSyncSnapshotSender* pSender = NULL;
  for (int i = 0; i < ths->replicaNum; ++i) {
    if (syncUtilSameId(pDestId, &((ths->replicasId)[i]))) {
      pSender = (ths->senders)[i];
    }
  }
  return pSender;
}

bool syncNodeCanChange(SSyncNode* pSyncNode) {
  if (pSyncNode->changing) {
    sError("sync cannot change");
    return false;
  }

  if ((pSyncNode->commitIndex >= SYNC_INDEX_BEGIN)) {
    SyncIndex lastIndex = syncNodeGetLastIndex(pSyncNode);
    if (pSyncNode->commitIndex != lastIndex) {
      sError("sync cannot change2");
      return false;
    }
  }

  for (int i = 0; i < pSyncNode->peersNum; ++i) {
    SSyncSnapshotSender* pSender = syncNodeGetSnapshotSender(pSyncNode, &(pSyncNode->peersId)[i]);
    if (pSender->start) {
      sError("sync cannot change3");
      return false;
    }
  }

  return true;
}

void syncLogSendRequestVote(SSyncNode* pSyncNode, const SyncRequestVote* pMsg, const char* s) {
  char     host[64];
  uint16_t port;
  syncUtilU642Addr(pMsg->destId.addr, host, sizeof(host), &port);
  char logBuf[256];
  snprintf(logBuf, sizeof(logBuf),
           "send sync-request-vote to %s:%d {term:%" PRIu64 ", lindex:%" PRId64 ", lterm:%" PRIu64 "}, %s", host, port,
           pMsg->term, pMsg->lastLogIndex, pMsg->lastLogTerm, s);
  syncNodeEventLog(pSyncNode, logBuf);
}

void syncLogRecvRequestVote(SSyncNode* pSyncNode, const SyncRequestVote* pMsg, const char* s) {
  char     logBuf[256];
  char     host[64];
  uint16_t port;
  syncUtilU642Addr(pMsg->srcId.addr, host, sizeof(host), &port);
  snprintf(logBuf, sizeof(logBuf),
           "recv sync-request-vote from %s:%d, {term:%" PRIu64 ", lindex:%" PRId64 ", lterm:%" PRIu64 "}, %s", host,
           port, pMsg->term, pMsg->lastLogIndex, pMsg->lastLogTerm, s);
  syncNodeEventLog(pSyncNode, logBuf);
}

void syncLogSendRequestVoteReply(SSyncNode* pSyncNode, const SyncRequestVoteReply* pMsg, const char* s) {
  char     host[64];
  uint16_t port;
  syncUtilU642Addr(pMsg->destId.addr, host, sizeof(host), &port);
  char logBuf[256];
  snprintf(logBuf, sizeof(logBuf), "send sync-request-vote-reply to %s:%d {term:%" PRIu64 ", grant:%d}, %s", host, port,
           pMsg->term, pMsg->voteGranted, s);
  syncNodeEventLog(pSyncNode, logBuf);
}

void syncLogRecvRequestVoteReply(SSyncNode* pSyncNode, const SyncRequestVoteReply* pMsg, const char* s) {
  char     host[64];
  uint16_t port;
  syncUtilU642Addr(pMsg->srcId.addr, host, sizeof(host), &port);
  char logBuf[256];
  snprintf(logBuf, sizeof(logBuf), "recv sync-request-vote-reply from %s:%d {term:%" PRIu64 ", grant:%d}, %s", host,
           port, pMsg->term, pMsg->voteGranted, s);
  syncNodeEventLog(pSyncNode, logBuf);
}

void syncLogSendAppendEntries(SSyncNode* pSyncNode, const SyncAppendEntries* pMsg, const char* s) {
  char     host[64];
  uint16_t port;
  syncUtilU642Addr(pMsg->destId.addr, host, sizeof(host), &port);
  char logBuf[256];
  snprintf(logBuf, sizeof(logBuf),
           "send sync-append-entries to %s:%d, {term:%" PRIu64 ", pre-index:%" PRId64 ", pre-term:%" PRIu64
           ", pterm:%" PRIu64 ", cmt:%" PRId64
           ", "
           "datalen:%d}, %s",
           host, port, pMsg->term, pMsg->prevLogIndex, pMsg->prevLogTerm, pMsg->privateTerm, pMsg->commitIndex,
           pMsg->dataLen, s);
  syncNodeEventLog(pSyncNode, logBuf);
}

void syncLogRecvAppendEntries(SSyncNode* pSyncNode, const SyncAppendEntries* pMsg, const char* s) {
  char     host[64];
  uint16_t port;
  syncUtilU642Addr(pMsg->srcId.addr, host, sizeof(host), &port);
  char logBuf[256];
  snprintf(logBuf, sizeof(logBuf),
           "recv sync-append-entries from %s:%d {term:%" PRIu64 ", pre-index:%" PRIu64 ", pre-term:%" PRIu64
           ", cmt:%" PRIu64 ", pterm:%" PRIu64
           ", "
           "datalen:%d}, %s",
           host, port, pMsg->term, pMsg->prevLogIndex, pMsg->prevLogTerm, pMsg->commitIndex, pMsg->privateTerm,
           pMsg->dataLen, s);
  syncNodeEventLog(pSyncNode, logBuf);
}

void syncLogSendAppendEntriesBatch(SSyncNode* pSyncNode, const SyncAppendEntriesBatch* pMsg, const char* s) {
  char     host[64];
  uint16_t port;
  syncUtilU642Addr(pMsg->destId.addr, host, sizeof(host), &port);
  char logBuf[256];
  snprintf(logBuf, sizeof(logBuf),
           "send sync-append-entries-batch to %s:%d, {term:%" PRIu64 ", pre-index:%" PRId64 ", pre-term:%" PRIu64
           ", pterm:%" PRIu64 ", cmt:%" PRId64 ", datalen:%d, count:%d}, %s",
           host, port, pMsg->term, pMsg->prevLogIndex, pMsg->prevLogTerm, pMsg->privateTerm, pMsg->commitIndex,
           pMsg->dataLen, pMsg->dataCount, s);
  syncNodeEventLog(pSyncNode, logBuf);
}

void syncLogRecvAppendEntriesBatch(SSyncNode* pSyncNode, const SyncAppendEntriesBatch* pMsg, const char* s) {
  char     host[64];
  uint16_t port;
  syncUtilU642Addr(pMsg->srcId.addr, host, sizeof(host), &port);
  char logBuf[256];
  snprintf(logBuf, sizeof(logBuf),
           "recv sync-append-entries-batch from %s:%d, {term:%" PRIu64 ", pre-index:%" PRId64 ", pre-term:%" PRIu64
           ", pterm:%" PRIu64 ", cmt:%" PRId64 ", datalen:%d, count:%d}, %s",
           host, port, pMsg->term, pMsg->prevLogIndex, pMsg->prevLogTerm, pMsg->privateTerm, pMsg->commitIndex,
           pMsg->dataLen, pMsg->dataCount, s);
  syncNodeEventLog(pSyncNode, logBuf);
}

void syncLogSendAppendEntriesReply(SSyncNode* pSyncNode, const SyncAppendEntriesReply* pMsg, const char* s) {
  char     host[64];
  uint16_t port;
  syncUtilU642Addr(pMsg->destId.addr, host, sizeof(host), &port);
  char logBuf[256];
  snprintf(logBuf, sizeof(logBuf),
           "send sync-append-entries-reply to %s:%d, {term:%" PRIu64 ", pterm:%" PRIu64 ", success:%d, match:%" PRId64
           "}, %s",
           host, port, pMsg->term, pMsg->privateTerm, pMsg->success, pMsg->matchIndex, s);
  syncNodeEventLog(pSyncNode, logBuf);
}

void syncLogRecvAppendEntriesReply(SSyncNode* pSyncNode, const SyncAppendEntriesReply* pMsg, const char* s) {
  char     host[64];
  uint16_t port;
  syncUtilU642Addr(pMsg->srcId.addr, host, sizeof(host), &port);
  char logBuf[256];
  snprintf(logBuf, sizeof(logBuf),
           "recv sync-append-entries-reply from %s:%d {term:%" PRIu64 ", pterm:%" PRIu64 ", success:%d, match:%" PRId64
           "}, %s",
           host, port, pMsg->term, pMsg->privateTerm, pMsg->success, pMsg->matchIndex, s);
  syncNodeEventLog(pSyncNode, logBuf);
}
