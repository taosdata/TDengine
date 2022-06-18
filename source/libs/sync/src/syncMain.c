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
int32_t syncNodeOnClientRequestCb(SSyncNode* ths, SyncClientRequest* pMsg);

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
      ret = syncEnvStart();
    }
  }

  return ret;
}

void syncCleanUp() {
  int32_t ret = syncEnvStop();
  assert(ret == 0);

  if (tsNodeRefId != -1) {
    taosCloseRef(tsNodeRefId);
    tsNodeRefId = -1;
  }
}

int64_t syncOpen(const SSyncInfo* pSyncInfo) {
  SSyncNode* pSyncNode = syncNodeOpen(pSyncInfo);
  assert(pSyncNode != NULL);

  if (gRaftDetailLog) {
    syncNodeLog2("syncNodeOpen open success", pSyncNode);
  }

  pSyncNode->rid = taosAddRef(tsNodeRefId, pSyncNode);
  if (pSyncNode->rid < 0) {
    syncFreeNode(pSyncNode);
    return -1;
  }

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
  if (pSyncNode == NULL) {
    return;
  }
  syncNodeClose(pSyncNode);

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  taosRemoveRef(tsNodeRefId, rid);
}

int32_t syncSetStandby(int64_t rid) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    sError("failed to set standby since accquire ref error, rid:%" PRId64, rid);
    return -1;
  }

  if (pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
    taosReleaseRef(tsNodeRefId, pSyncNode->rid);
    terrno = TSDB_CODE_SYN_IS_LEADER;
    sError("failed to set standby since it is leader, rid:%" PRId64, rid);
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

int32_t syncReconfigBuild(int64_t rid, const SSyncCfg* pNewCfg, SRpcMsg* pRpcMsg) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }
  ASSERT(rid == pSyncNode->rid);

  int32_t ret = 0;
  bool    IamInNew = syncNodeInConfig(pSyncNode, pNewCfg);

  if (!IamInNew) {
    taosReleaseRef(tsNodeRefId, pSyncNode->rid);
    terrno = TSDB_CODE_SYN_NOT_IN_NEW_CONFIG;
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

  bool IamInNew = syncNodeInConfig(pSyncNode, pNewCfg);

  if (!IamInNew) {
    sError("sync reconfig error, not in new config");
    taosReleaseRef(tsNodeRefId, pSyncNode->rid);
    terrno = TSDB_CODE_SYN_NOT_IN_NEW_CONFIG;
    return -1;
  }

  char* newconfig = syncCfg2Str((SSyncCfg*)pNewCfg);
  if (gRaftDetailLog) {
    sInfo("==syncReconfig== newconfig:%s", newconfig);
  }

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

  if (pSyncNode->peersNum == 0) {
    taosReleaseRef(tsNodeRefId, pSyncNode->rid);
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }

  SNodeInfo newLeader = (pSyncNode->peersNodeInfo)[0];
  taosReleaseRef(tsNodeRefId, pSyncNode->rid);

  int32_t ret = syncLeaderTransferTo(rid, newLeader);
  return ret;
}

int32_t syncLeaderTransferTo(int64_t rid, SNodeInfo newLeader) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }
  ASSERT(rid == pSyncNode->rid);
  int32_t ret = 0;

  if (pSyncNode->replicaNum == 1) {
    sError("only one replica, cannot drop leader");
    taosReleaseRef(tsNodeRefId, pSyncNode->rid);
    terrno = TSDB_CODE_SYN_ONE_REPLICA;
    return -1;
  }

  SyncLeaderTransfer* pMsg = syncLeaderTransferBuild(pSyncNode->vgId);
  pMsg->newLeaderId.addr = syncUtilAddr2U64(newLeader.nodeFqdn, newLeader.nodePort);
  pMsg->newLeaderId.vgId = pSyncNode->vgId;
  pMsg->newNodeInfo = newLeader;
  ASSERT(pMsg != NULL);
  SRpcMsg rpcMsg = {0};
  syncLeaderTransfer2RpcMsg(pMsg, &rpcMsg);
  syncLeaderTransferDestroy(pMsg);

  ret = syncNodePropose(pSyncNode, &rpcMsg, false);
  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  return ret;
}

bool syncCanLeaderTransfer(int64_t rid) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return false;
  }
  assert(rid == pSyncNode->rid);

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

int32_t syncForwardToPeer(int64_t rid, const SRpcMsg* pMsg, bool isWeak) {
  int32_t ret = syncPropose(rid, pMsg, isWeak);
  return ret;
}

ESyncState syncGetMyRole(int64_t rid) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return TAOS_SYNC_STATE_ERROR;
  }
  assert(rid == pSyncNode->rid);
  ESyncState state = pSyncNode->state;

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  return state;
}

bool syncIsReady(int64_t rid) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return false;
  }
  assert(rid == pSyncNode->rid);
  bool b = (pSyncNode->state == TAOS_SYNC_STATE_LEADER) && pSyncNode->restoreFinish;
  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  return b;
}

bool syncIsRestoreFinish(int64_t rid) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return false;
  }
  assert(rid == pSyncNode->rid);
  bool b = pSyncNode->restoreFinish;

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  return b;
}

int32_t syncGetSnapshotMeta(int64_t rid, struct SSnapshotMeta* sMeta) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return -1;
  }
  assert(rid == pSyncNode->rid);
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
  assert(rid == pSyncNode->rid);

  ASSERT(pSyncNode->pRaftCfg->configIndexCount >= 1);
  SyncIndex lastIndex = (pSyncNode->pRaftCfg->configIndexArr)[0];

  for (int i = 0; i < pSyncNode->pRaftCfg->configIndexCount; ++i) {
    if ((pSyncNode->pRaftCfg->configIndexArr)[i] > lastIndex &&
        (pSyncNode->pRaftCfg->configIndexArr)[i] <= snapshotIndex) {
      lastIndex = (pSyncNode->pRaftCfg->configIndexArr)[i];
    }
  }
  sMeta->lastConfigIndex = lastIndex;
  sTrace("vgId:%d, get snapshot meta by index:%" PRId64 " lastConfigIndex:%" PRId64, pSyncNode->vgId, snapshotIndex,
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

  sTrace("sync syncNodeGetSnapshotConfigIndex index:%ld lastConfigIndex:%ld", snapshotLastApplyIndex, lastIndex);
  return lastIndex;
}

const char* syncGetMyRoleStr(int64_t rid) {
  const char* s = syncUtilState2String(syncGetMyRole(rid));
  return s;
}

int32_t syncGetVgId(int64_t rid) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return TAOS_SYNC_STATE_ERROR;
  }
  assert(rid == pSyncNode->rid);
  int32_t vgId = pSyncNode->vgId;

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  return vgId;
}

SyncTerm syncGetMyTerm(int64_t rid) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return TAOS_SYNC_STATE_ERROR;
  }
  assert(rid == pSyncNode->rid);
  SyncTerm term = pSyncNode->pRaftStore->currentTerm;

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  return term;
}

void syncGetEpSet(int64_t rid, SEpSet* pEpSet) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    memset(pEpSet, 0, sizeof(*pEpSet));
    return;
  }
  assert(rid == pSyncNode->rid);
  pEpSet->numOfEps = 0;
  for (int i = 0; i < pSyncNode->pRaftCfg->cfg.replicaNum; ++i) {
    snprintf(pEpSet->eps[i].fqdn, sizeof(pEpSet->eps[i].fqdn), "%s", (pSyncNode->pRaftCfg->cfg.nodeInfo)[i].nodeFqdn);
    pEpSet->eps[i].port = (pSyncNode->pRaftCfg->cfg.nodeInfo)[i].nodePort;
    (pEpSet->numOfEps)++;

    sInfo("syncGetEpSet index:%d %s:%d", i, pEpSet->eps[i].fqdn, pEpSet->eps[i].port);
  }
  pEpSet->inUse = pSyncNode->pRaftCfg->cfg.myIndex;

  sInfo("syncGetEpSet pEpSet->inUse:%d ", pEpSet->inUse);

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
}

int32_t syncGetRespRpc(int64_t rid, uint64_t index, SRpcMsg* msg) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return TAOS_SYNC_STATE_ERROR;
  }
  assert(rid == pSyncNode->rid);

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
  assert(rid == pSyncNode->rid);

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
    sTrace("syncSetQ get pSyncNode is NULL, rid:%ld", rid);
    return;
  }
  assert(rid == pSyncNode->rid);
  pSyncNode->msgcb = msgcb;

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
}

char* sync2SimpleStr(int64_t rid) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    sTrace("syncSetRpc get pSyncNode is NULL, rid:%ld", rid);
    return NULL;
  }
  assert(rid == pSyncNode->rid);
  char* s = syncNode2SimpleStr(pSyncNode);
  taosReleaseRef(tsNodeRefId, pSyncNode->rid);

  return s;
}

void setPingTimerMS(int64_t rid, int32_t pingTimerMS) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return;
  }
  assert(rid == pSyncNode->rid);
  pSyncNode->pingBaseLine = pingTimerMS;
  pSyncNode->pingTimerMS = pingTimerMS;

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
}

void setElectTimerMS(int64_t rid, int32_t electTimerMS) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return;
  }
  assert(rid == pSyncNode->rid);
  pSyncNode->electBaseLine = electTimerMS;

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
}

void setHeartbeatTimerMS(int64_t rid, int32_t hbTimerMS) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return;
  }
  assert(rid == pSyncNode->rid);
  pSyncNode->hbBaseLine = hbTimerMS;
  pSyncNode->heartbeatTimerMS = hbTimerMS;

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
}

int32_t syncPropose(int64_t rid, const SRpcMsg* pMsg, bool isWeak) {
  int32_t ret = 0;

  SSyncNode* pSyncNode = taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }
  assert(rid == pSyncNode->rid);
  ret = syncNodePropose(pSyncNode, pMsg, isWeak);

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  return ret;
}

int32_t syncNodePropose(SSyncNode* pSyncNode, const SRpcMsg* pMsg, bool isWeak) {
  int32_t ret = 0;

  char eventLog[128];
  snprintf(eventLog, sizeof(eventLog), "propose type:%s,%d", TMSG_INFO(pMsg->msgType), pMsg->msgType);
  syncNodeEventLog(pSyncNode, eventLog);

  if (pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
    SRespStub stub;
    stub.createTime = taosGetTimestampMs();
    stub.rpcMsg = *pMsg;
    uint64_t seqNum = syncRespMgrAdd(pSyncNode->pSyncRespMgr, &stub);

    SyncClientRequest* pSyncMsg = syncClientRequestBuild2(pMsg, seqNum, isWeak, pSyncNode->vgId);
    SRpcMsg            rpcMsg;
    syncClientRequest2RpcMsg(pSyncMsg, &rpcMsg);

    if (pSyncNode->FpEqMsg != NULL && (*pSyncNode->FpEqMsg)(pSyncNode->msgcb, &rpcMsg) == 0) {
      ret = 0;
    } else {
      ret = -1;
      terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
      sError("syncPropose pSyncNode->FpEqMsg is NULL");
    }
    syncClientRequestDestroy(pSyncMsg);
  } else {
    ret = -1;
    terrno = TSDB_CODE_SYN_NOT_LEADER;
    sError("syncPropose not leader, %s", syncUtilState2String(pSyncNode->state));
  }

  return ret;
}

// open/close --------------
SSyncNode* syncNodeOpen(const SSyncInfo* pOldSyncInfo) {
  SSyncInfo* pSyncInfo = (SSyncInfo*)pOldSyncInfo;

  SSyncNode* pSyncNode = (SSyncNode*)taosMemoryMalloc(sizeof(SSyncNode));
  assert(pSyncNode != NULL);
  memset(pSyncNode, 0, sizeof(SSyncNode));

  int32_t ret = 0;
  if (!taosDirExist((char*)(pSyncInfo->path))) {
    if (taosMkDir(pSyncInfo->path) != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      sError("failed to create dir:%s since %s", pSyncInfo->path, terrstr());
      return NULL;
    }
  }

  snprintf(pSyncNode->configPath, sizeof(pSyncNode->configPath), "%s/raft_config.json", pSyncInfo->path);
  if (!taosCheckExistFile(pSyncNode->configPath)) {
    // create a new raft config file
    SRaftCfgMeta meta;
    meta.isStandBy = pSyncInfo->isStandBy;
    meta.snapshotEnable = pSyncInfo->snapshotEnable;
    meta.lastConfigIndex = SYNC_INDEX_INVALID;
    ret = raftCfgCreateFile((SSyncCfg*)&(pSyncInfo->syncCfg), meta, pSyncNode->configPath);
    assert(ret == 0);

  } else {
    // update syncCfg by raft_config.json
    pSyncNode->pRaftCfg = raftCfgOpen(pSyncNode->configPath);
    assert(pSyncNode->pRaftCfg != NULL);
    pSyncInfo->syncCfg = pSyncNode->pRaftCfg->cfg;

    if (gRaftDetailLog) {
      char* seralized = raftCfg2Str(pSyncNode->pRaftCfg);
      sInfo("syncNodeOpen update config :%s", seralized);
      taosMemoryFree(seralized);
    }

    raftCfgClose(pSyncNode->pRaftCfg);
  }

  // init by SSyncInfo
  pSyncNode->vgId = pSyncInfo->vgId;
  memcpy(pSyncNode->path, pSyncInfo->path, sizeof(pSyncNode->path));
  snprintf(pSyncNode->raftStorePath, sizeof(pSyncNode->raftStorePath), "%s/raft_store.json", pSyncInfo->path);
  snprintf(pSyncNode->configPath, sizeof(pSyncNode->configPath), "%s/raft_config.json", pSyncInfo->path);

  pSyncNode->pWal = pSyncInfo->pWal;
  pSyncNode->msgcb = pSyncInfo->msgcb;
  pSyncNode->FpSendMsg = pSyncInfo->FpSendMsg;
  pSyncNode->FpEqMsg = pSyncInfo->FpEqMsg;

  // init raft config
  pSyncNode->pRaftCfg = raftCfgOpen(pSyncNode->configPath);
  assert(pSyncNode->pRaftCfg != NULL);

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
  assert(pSyncNode->pRaftStore != NULL);

  // init TLA+ candidate vars
  pSyncNode->pVotesGranted = voteGrantedCreate(pSyncNode);
  assert(pSyncNode->pVotesGranted != NULL);
  pSyncNode->pVotesRespond = votesRespondCreate(pSyncNode);
  assert(pSyncNode->pVotesRespond != NULL);

  // init TLA+ leader vars
  pSyncNode->pNextIndex = syncIndexMgrCreate(pSyncNode);
  assert(pSyncNode->pNextIndex != NULL);
  pSyncNode->pMatchIndex = syncIndexMgrCreate(pSyncNode);
  assert(pSyncNode->pMatchIndex != NULL);

  // init TLA+ log vars
  pSyncNode->pLogStore = logStoreCreate(pSyncNode);
  assert(pSyncNode->pLogStore != NULL);
  pSyncNode->commitIndex = SYNC_INDEX_INVALID;

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

  if (pSyncNode->pRaftCfg->snapshotEnable) {
    sInfo("sync node use snapshot");
    pSyncNode->FpOnRequestVote = syncNodeOnRequestVoteSnapshotCb;
    pSyncNode->FpOnRequestVoteReply = syncNodeOnRequestVoteReplySnapshotCb;
    pSyncNode->FpOnAppendEntries = syncNodeOnAppendEntriesSnapshotCb;
    pSyncNode->FpOnAppendEntriesReply = syncNodeOnAppendEntriesReplySnapshotCb;

  } else {
    sInfo("sync node do not use snapshot");
    pSyncNode->FpOnRequestVote = syncNodeOnRequestVoteCb;
    pSyncNode->FpOnRequestVoteReply = syncNodeOnRequestVoteReplyCb;
    pSyncNode->FpOnAppendEntries = syncNodeOnAppendEntriesCb;
    pSyncNode->FpOnAppendEntriesReply = syncNodeOnAppendEntriesReplyCb;
  }

  // tools
  pSyncNode->pSyncRespMgr = syncRespMgrCreate(pSyncNode, 0);
  assert(pSyncNode->pSyncRespMgr != NULL);

  // restore state
  pSyncNode->restoreFinish = false;

  // pSyncNode->pSnapshot = NULL;
  // if (pSyncNode->pFsm->FpGetSnapshot != NULL) {
  //   pSyncNode->pSnapshot = taosMemoryMalloc(sizeof(SSnapshot));
  //   pSyncNode->pFsm->FpGetSnapshot(pSyncNode->pFsm, pSyncNode->pSnapshot);
  // }
  // tsem_init(&(pSyncNode->restoreSem), 0, 0);

  // snapshot senders
  for (int i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SSyncSnapshotSender* pSender = snapshotSenderCreate(pSyncNode, i);
    // ASSERT(pSender != NULL);
    (pSyncNode->senders)[i] = pSender;
  }

  // snapshot receivers
  pSyncNode->pNewNodeReceiver = snapshotReceiverCreate(pSyncNode, EMPTY_RAFT_ID);

  // start in syncNodeStart
  // start raft
  // syncNodeBecomeFollower(pSyncNode);

  syncNodeEventLog(pSyncNode, "sync open");

  return pSyncNode;
}

void syncNodeStart(SSyncNode* pSyncNode) {
  // start raft
  if (pSyncNode->replicaNum == 1) {
    raftStoreNextTerm(pSyncNode->pRaftStore);
    syncNodeBecomeLeader(pSyncNode, "one replica start");

    // Raft 3.6.2 Committing entries from previous terms

    // use this now
    syncNodeAppendNoop(pSyncNode);
    syncMaybeAdvanceCommitIndex(pSyncNode);  // maybe only one replica

    if (gRaftDetailLog) {
      syncNodeLog2("==state change become leader immediately==", pSyncNode);
    }

    return;
  }

  syncNodeBecomeFollower(pSyncNode, "first start");

  // int32_t ret = 0;
  // ret = syncNodeStartPingTimer(pSyncNode);
  // assert(ret == 0);

  if (gRaftDetailLog) {
    syncNodeLog2("==state change become leader immediately==", pSyncNode);
  }
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
  assert(pSyncNode != NULL);

  ret = raftStoreClose(pSyncNode->pRaftStore);
  assert(ret == 0);

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

  /*
  if (pSyncNode->pSnapshot != NULL) {
    taosMemoryFree(pSyncNode->pSnapshot);
  }
  */

  // tsem_destroy(&pSyncNode->restoreSem);

  // free memory in syncFreeNode
  // taosMemoryFree(pSyncNode);
}

// option
bool syncNodeSnapshotEnable(SSyncNode* pSyncNode) { return pSyncNode->pRaftCfg->snapshotEnable; }

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
  assert(ret == 0);

  syncPingDestroy(pMsg);
  return ret;
}

int32_t syncNodePingPeers(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  for (int i = 0; i < pSyncNode->peersNum; ++i) {
    SRaftId*  destId = &(pSyncNode->peersId[i]);
    SyncPing* pMsg = syncPingBuild3(&pSyncNode->myRaftId, destId, pSyncNode->vgId);
    ret = syncNodePing(pSyncNode, destId, pMsg);
    assert(ret == 0);
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
    assert(ret == 0);
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
    sError("sync env is stop, syncNodeStartPingTimer");
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
  } else {
    sError("sync env is stop, syncNodeStartElectTimer");
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
  return ret;
}

int32_t syncNodeStartHeartbeatTimer(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  if (syncEnvIsStart()) {
    taosTmrReset(pSyncNode->FpHeartbeatTimerCB, pSyncNode->heartbeatTimerMS, pSyncNode, gSyncEnv->pTimerManager,
                 &pSyncNode->pHeartbeatTimer);
    atomic_store_64(&pSyncNode->heartbeatTimerLogicClock, pSyncNode->heartbeatTimerLogicClockUser);
  } else {
    sError("sync env is stop, syncNodeStartHeartbeatTimer");
  }
  return ret;
}

int32_t syncNodeStopHeartbeatTimer(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  atomic_add_fetch_64(&pSyncNode->heartbeatTimerLogicClockUser, 1);
  taosTmrStop(pSyncNode->pHeartbeatTimer);
  pSyncNode->pHeartbeatTimer = NULL;
  return ret;
}

// utils --------------
int32_t syncNodeSendMsgById(const SRaftId* destRaftId, SSyncNode* pSyncNode, SRpcMsg* pMsg) {
  SEpSet epSet;
  syncUtilraftId2EpSet(destRaftId, &epSet);
  if (pSyncNode->FpSendMsg != NULL) {
    if (gRaftDetailLog) {
      char* JsonStr = syncRpcMsg2Str(pMsg);
      syncUtilJson2Line(JsonStr);
      sTrace("sync send msg, vgId:%d, type:%d, msg:%s", pSyncNode->vgId, pMsg->msgType, JsonStr);
      taosMemoryFree(JsonStr);
    }

    // htonl
    syncUtilMsgHtoN(pMsg->pCont);

    pMsg->info.noResp = 1;
    pSyncNode->FpSendMsg(&epSet, pMsg);
  } else {
    sTrace("syncNodeSendMsgById pSyncNode->FpSendMsg is NULL");
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
    sTrace("syncNodeSendMsgByInfo pSyncNode->FpSendMsg is NULL");
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
    snprintf(u64buf, sizeof(u64buf), "%ld", pSyncNode->rid);
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
    snprintf(u64buf, sizeof(u64buf), "%" PRId64 "", pSyncNode->commitIndex);
    cJSON_AddStringToObject(pRoot, "commitIndex", u64buf);

    // timer ms init
    cJSON_AddNumberToObject(pRoot, "pingBaseLine", pSyncNode->pingBaseLine);
    cJSON_AddNumberToObject(pRoot, "electBaseLine", pSyncNode->electBaseLine);
    cJSON_AddNumberToObject(pRoot, "hbBaseLine", pSyncNode->hbBaseLine);

    // ping timer
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->pPingTimer);
    cJSON_AddStringToObject(pRoot, "pPingTimer", u64buf);
    cJSON_AddNumberToObject(pRoot, "pingTimerMS", pSyncNode->pingTimerMS);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", pSyncNode->pingTimerLogicClock);
    cJSON_AddStringToObject(pRoot, "pingTimerLogicClock", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", pSyncNode->pingTimerLogicClockUser);
    cJSON_AddStringToObject(pRoot, "pingTimerLogicClockUser", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpPingTimerCB);
    cJSON_AddStringToObject(pRoot, "FpPingTimerCB", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", pSyncNode->pingTimerCounter);
    cJSON_AddStringToObject(pRoot, "pingTimerCounter", u64buf);

    // elect timer
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->pElectTimer);
    cJSON_AddStringToObject(pRoot, "pElectTimer", u64buf);
    cJSON_AddNumberToObject(pRoot, "electTimerMS", pSyncNode->electTimerMS);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", pSyncNode->electTimerLogicClock);
    cJSON_AddStringToObject(pRoot, "electTimerLogicClock", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", pSyncNode->electTimerLogicClockUser);
    cJSON_AddStringToObject(pRoot, "electTimerLogicClockUser", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpElectTimerCB);
    cJSON_AddStringToObject(pRoot, "FpElectTimerCB", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", pSyncNode->electTimerCounter);
    cJSON_AddStringToObject(pRoot, "electTimerCounter", u64buf);

    // heartbeat timer
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->pHeartbeatTimer);
    cJSON_AddStringToObject(pRoot, "pHeartbeatTimer", u64buf);
    cJSON_AddNumberToObject(pRoot, "heartbeatTimerMS", pSyncNode->heartbeatTimerMS);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", pSyncNode->heartbeatTimerLogicClock);
    cJSON_AddStringToObject(pRoot, "heartbeatTimerLogicClock", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", pSyncNode->heartbeatTimerLogicClockUser);
    cJSON_AddStringToObject(pRoot, "heartbeatTimerLogicClockUser", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpHeartbeatTimerCB);
    cJSON_AddStringToObject(pRoot, "FpHeartbeatTimerCB", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", pSyncNode->heartbeatTimerCounter);
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
  if (userStrLen < 256) {
    char logBuf[128 + 256];
    snprintf(logBuf, sizeof(logBuf),
             "vgId:%d %s term:%lu commit:%ld standby:%d replica-num:%d lconfig:%ld sync event %s", pSyncNode->vgId,
             syncUtilState2String(pSyncNode->state), pSyncNode->pRaftStore->currentTerm, pSyncNode->commitIndex,
             pSyncNode->pRaftCfg->isStandBy, pSyncNode->replicaNum, pSyncNode->pRaftCfg->lastConfigIndex, str);
    sDebug("%s", logBuf);
  } else {
    int   len = 128 + userStrLen;
    char* s = (char*)taosMemoryMalloc(len);
    snprintf(s, len, "vgId:%d %s term:%lu commit:%ld standby:%d replica-num:%d lconfig:%ld sync event %s",
             pSyncNode->vgId, syncUtilState2String(pSyncNode->state), pSyncNode->pRaftStore->currentTerm,
             pSyncNode->commitIndex, pSyncNode->pRaftCfg->isStandBy, pSyncNode->replicaNum,
             pSyncNode->pRaftCfg->lastConfigIndex, str);
    sDebug("%s", s);
    taosMemoryFree(s);
  }
}

char* syncNode2SimpleStr(const SSyncNode* pSyncNode) {
  int   len = 256;
  char* s = (char*)taosMemoryMalloc(len);
  snprintf(s, len,
           "syncNode: vgId:%d, currentTerm:%lu, commitIndex:%ld, state:%d %s, isStandBy:%d, "
           "electTimerLogicClock:%lu, "
           "electTimerLogicClockUser:%lu, "
           "electTimerMS:%d, replicaNum:%d",
           pSyncNode->vgId, pSyncNode->pRaftStore->currentTerm, pSyncNode->commitIndex, pSyncNode->state,
           syncUtilState2String(pSyncNode->state), pSyncNode->pRaftCfg->isStandBy, pSyncNode->electTimerLogicClock,
           pSyncNode->electTimerLogicClockUser, pSyncNode->electTimerMS, pSyncNode->replicaNum);
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

void syncNodeUpdateConfig(SSyncNode* pSyncNode, SSyncCfg* pNewConfig, SyncIndex lastConfigChangeIndex, bool* isDrop) {
  SSyncCfg oldConfig = pSyncNode->pRaftCfg->cfg;
  pSyncNode->pRaftCfg->cfg = *pNewConfig;
  pSyncNode->pRaftCfg->lastConfigIndex = lastConfigChangeIndex;

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
          char eventLog[128];
          snprintf(eventLog, sizeof(eventLog), "snapshot sender reset for %lu, newIndex:%d, %s:%d, %p",
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
          char eventLog[128];
          snprintf(eventLog, sizeof(eventLog), "snapshot sender udpate replicaIndex from %d to %d, %s:%d, %p, reset:%d",
                   oldreplicaIndex, i, host, port, (pSyncNode->senders)[i], reset);
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

  bool IamInOld = syncNodeInConfig(pSyncNode, &oldConfig);
  bool IamInNew = syncNodeInConfig(pSyncNode, pNewConfig);

  *isDrop = true;
  if (IamInOld && !IamInNew) {
    *isDrop = true;
  } else {
    *isDrop = false;
  }

  // may be add me to a new raft group
  if (IamInOld && IamInNew && oldConfig.replicaNum == 1) {
  }

  if (IamInNew) {
    pSyncNode->pRaftCfg->isStandBy = 0;  // change isStandBy to normal
  }
  raftCfgPersist(pSyncNode->pRaftCfg);

  if (gRaftDetailLog) {
    syncNodeLog2("==syncNodeUpdateConfig==", pSyncNode);
  }
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
    snprintf(tmpBuf, sizeof(tmpBuf), "update term to %lu", term);
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

  // start replicate right now!
  syncNodeReplicate(pSyncNode);

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
  assert(pSyncNode->state == TAOS_SYNC_STATE_CANDIDATE);
  assert(voteGrantedMajority(pSyncNode->pVotesGranted));
  syncNodeBecomeLeader(pSyncNode, "candidate to leader");

  syncNodeLog2("==state change syncNodeCandidate2Leader==", pSyncNode);

  // Raft 3.6.2 Committing entries from previous terms

  // use this now
  syncNodeAppendNoop(pSyncNode);
  syncMaybeAdvanceCommitIndex(pSyncNode);  // maybe only one replica

  // do not use this
  // syncNodeEqNoop(pSyncNode);
}

void syncNodeFollower2Candidate(SSyncNode* pSyncNode) {
  assert(pSyncNode->state == TAOS_SYNC_STATE_FOLLOWER);
  pSyncNode->state = TAOS_SYNC_STATE_CANDIDATE;

  syncNodeLog2("==state change syncNodeFollower2Candidate==", pSyncNode);
}

void syncNodeLeader2Follower(SSyncNode* pSyncNode) {
  assert(pSyncNode->state == TAOS_SYNC_STATE_LEADER);
  syncNodeBecomeFollower(pSyncNode, "leader to follower");

  syncNodeLog2("==state change syncNodeLeader2Follower==", pSyncNode);
}

void syncNodeCandidate2Follower(SSyncNode* pSyncNode) {
  assert(pSyncNode->state == TAOS_SYNC_STATE_CANDIDATE);
  syncNodeBecomeFollower(pSyncNode, "candidate to follower");

  syncNodeLog2("==state change syncNodeCandidate2Follower==", pSyncNode);
}

// raft vote --------------

// just called by syncNodeVoteForSelf
// need assert
void syncNodeVoteForTerm(SSyncNode* pSyncNode, SyncTerm term, SRaftId* pRaftId) {
  assert(term == pSyncNode->pRaftStore->currentTerm);
  assert(!raftStoreHasVoted(pSyncNode->pRaftStore));

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
bool syncNodeHasSnapshot(SSyncNode* pSyncNode) {
  bool      ret = false;
  SSnapshot snapshot = {.data = NULL, .lastApplyIndex = -1, .lastApplyTerm = 0};
  if (pSyncNode->pFsm->FpGetSnapshot != NULL) {
    pSyncNode->pFsm->FpGetSnapshot(pSyncNode->pFsm, &snapshot);
    if (snapshot.lastApplyIndex >= SYNC_INDEX_BEGIN) {
      ret = true;
    }
  }
  return ret;
}

bool syncNodeIsIndexInSnapshot(SSyncNode* pSyncNode, SyncIndex index) {
  ASSERT(syncNodeHasSnapshot(pSyncNode));
  ASSERT(pSyncNode->pFsm->FpGetSnapshot != NULL);
  ASSERT(index >= SYNC_INDEX_BEGIN);

  SSnapshot snapshot;
  pSyncNode->pFsm->FpGetSnapshot(pSyncNode->pFsm, &snapshot);
  bool b = (index <= snapshot.lastApplyIndex);
  return b;
}

SyncIndex syncNodeGetLastIndex(SSyncNode* pSyncNode) {
  SSnapshot snapshot = {.data = NULL, .lastApplyIndex = -1, .lastApplyTerm = 0};
  if (pSyncNode->pFsm->FpGetSnapshot != NULL) {
    pSyncNode->pFsm->FpGetSnapshot(pSyncNode->pFsm, &snapshot);
  }
  SyncIndex logLastIndex = pSyncNode->pLogStore->syncLogLastIndex(pSyncNode->pLogStore);

  SyncIndex lastIndex = logLastIndex > snapshot.lastApplyIndex ? logLastIndex : snapshot.lastApplyIndex;
  return lastIndex;
}

SyncTerm syncNodeGetLastTerm(SSyncNode* pSyncNode) {
  SyncTerm lastTerm = 0;
  if (syncNodeHasSnapshot(pSyncNode)) {
    // has snapshot
    SSnapshot snapshot = {.data = NULL, .lastApplyIndex = -1, .lastApplyTerm = 0};
    if (pSyncNode->pFsm->FpGetSnapshot != NULL) {
      pSyncNode->pFsm->FpGetSnapshot(pSyncNode->pFsm, &snapshot);
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

SyncIndex syncNodeSyncStartIndex(SSyncNode* pSyncNode) {
  SyncIndex syncStartIndex = syncNodeGetLastIndex(pSyncNode) + 1;
  return syncStartIndex;
}

SyncIndex syncNodeGetPreIndex(SSyncNode* pSyncNode, SyncIndex index) {
  ASSERT(index >= SYNC_INDEX_BEGIN);
  SyncIndex syncStartIndex = syncNodeSyncStartIndex(pSyncNode);
  ASSERT(index <= syncStartIndex);

  SyncIndex preIndex = index - 1;
  return preIndex;
}

SyncTerm syncNodeGetPreTerm(SSyncNode* pSyncNode, SyncIndex index) {
  ASSERT(index >= SYNC_INDEX_BEGIN);
  SyncIndex syncStartIndex = syncNodeSyncStartIndex(pSyncNode);
  ASSERT(index <= syncStartIndex);

  if (index == SYNC_INDEX_BEGIN) {
    return 0;
  }

  SyncTerm preTerm = 0;
  if (syncNodeHasSnapshot(pSyncNode)) {
    // has snapshot
    SSnapshot snapshot = {.data = NULL, .lastApplyIndex = -1, .lastApplyTerm = 0};
    if (pSyncNode->pFsm->FpGetSnapshot != NULL) {
      pSyncNode->pFsm->FpGetSnapshot(pSyncNode->pFsm, &snapshot);
    }

    if (index > snapshot.lastApplyIndex + 1) {
      // should be log preTerm
      SSyncRaftEntry* pPreEntry = NULL;
      int32_t         code = pSyncNode->pLogStore->syncLogGetEntry(pSyncNode->pLogStore, index - 1, &pPreEntry);
      ASSERT(code == 0);
      ASSERT(pPreEntry != NULL);

      preTerm = pPreEntry->term;
      taosMemoryFree(pPreEntry);

    } else if (index == snapshot.lastApplyIndex + 1) {
      preTerm = snapshot.lastApplyTerm;

    } else {
      // maybe snapshot change
      sError("sync get pre term, bad scene. index:%ld", index);
      logStoreLog2("sync get pre term, bad scene", pSyncNode->pLogStore);

      SSyncRaftEntry* pPreEntry = NULL;
      int32_t         code = pSyncNode->pLogStore->syncLogGetEntry(pSyncNode->pLogStore, index - 1, &pPreEntry);
      ASSERT(code == 0);
      ASSERT(pPreEntry != NULL);

      preTerm = pPreEntry->term;
      taosMemoryFree(pPreEntry);
    }

  } else {
    // no snapshot
    ASSERT(index > SYNC_INDEX_BEGIN);

    SSyncRaftEntry* pPreEntry = NULL;
    int32_t         code = pSyncNode->pLogStore->syncLogGetEntry(pSyncNode->pLogStore, index - 1, &pPreEntry);
    ASSERT(code == 0);
    ASSERT(pPreEntry != NULL);

    preTerm = pPreEntry->term;
    taosMemoryFree(pPreEntry);
  }

  return preTerm;
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
  printf("syncNodePrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncNodePrint2(char* s, SSyncNode* pObj) {
  char* serialized = syncNode2Str(pObj);
  printf("syncNodePrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncNodeLog(SSyncNode* pObj) {
  char* serialized = syncNode2Str(pObj);
  sTraceLong("syncNodeLog | len:%lu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncNodeLog2(char* s, SSyncNode* pObj) {
  if (gRaftDetailLog) {
    char* serialized = syncNode2Str(pObj);
    sTraceLong("syncNodeLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
    taosMemoryFree(serialized);
  }
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
    sTrace("==syncNodeEqPingTimer== pingTimerLogicClock:%" PRIu64 ", pingTimerLogicClockUser:%" PRIu64 "",
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
    sTrace("==syncNodeEqElectTimer== electTimerLogicClock:%" PRIu64 ", electTimerLogicClockUser:%" PRIu64 "",
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
  assert(ths->state == TAOS_SYNC_STATE_LEADER);

  SyncIndex       index = ths->pLogStore->getLastIndex(ths->pLogStore) + 1;
  SyncTerm        term = ths->pRaftStore->currentTerm;
  SSyncRaftEntry* pEntry = syncEntryBuildNoop(term, index, ths->vgId);
  assert(pEntry != NULL);

  uint32_t           entryLen;
  char*              serialized = syncEntrySerialize(pEntry, &entryLen);
  SyncClientRequest* pSyncMsg = syncClientRequestBuild(entryLen);
  assert(pSyncMsg->dataLen == entryLen);
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

  SyncIndex       index = ths->pLogStore->getLastIndex(ths->pLogStore) + 1;
  SyncTerm        term = ths->pRaftStore->currentTerm;
  SSyncRaftEntry* pEntry = syncEntryBuildNoop(term, index, ths->vgId);
  assert(pEntry != NULL);

  if (ths->state == TAOS_SYNC_STATE_LEADER) {
    // ths->pLogStore->appendEntry(ths->pLogStore, pEntry);
    ths->pLogStore->syncLogAppendEntry(ths->pLogStore, pEntry);
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
           "==syncNodeOnPingCb== vgId:%d, state: %d, %s, term:%lu electTimerLogicClock:%lu, "
           "electTimerLogicClockUser:%lu, electTimerMS:%d",
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
int32_t syncNodeOnClientRequestCb(SSyncNode* ths, SyncClientRequest* pMsg) {
  int32_t ret = 0;
  syncClientRequestLog2("==syncNodeOnClientRequestCb==", pMsg);

  SyncIndex       index = ths->pLogStore->syncLogWriteIndex(ths->pLogStore);
  SyncTerm        term = ths->pRaftStore->currentTerm;
  SSyncRaftEntry* pEntry = syncEntryBuild2((SyncClientRequest*)pMsg, term, index);
  assert(pEntry != NULL);

  if (ths->state == TAOS_SYNC_STATE_LEADER) {
    // ths->pLogStore->appendEntry(ths->pLogStore, pEntry);
    ths->pLogStore->syncLogAppendEntry(ths->pLogStore, pEntry);

    // start replicate right now!
    syncNodeReplicate(ths);

    // pre commit
    SRpcMsg rpcMsg;
    syncEntry2OriginalRpc(pEntry, &rpcMsg);

    if (ths->pFsm != NULL) {
      // if (ths->pFsm->FpPreCommitCb != NULL && pEntry->originalRpcType != TDMT_SYNC_NOOP) {
      if (ths->pFsm->FpPreCommitCb != NULL && syncUtilUserPreCommit(pEntry->originalRpcType)) {
        SFsmCbMeta cbMeta = {0};
        cbMeta.index = pEntry->index;
        cbMeta.lastConfigIndex = syncNodeGetSnapshotConfigIndex(ths, cbMeta.index);
        cbMeta.isWeak = pEntry->isWeak;
        cbMeta.code = 0;
        cbMeta.state = ths->state;
        cbMeta.seqNum = pEntry->seqNum;
        ths->pFsm->FpPreCommitCb(ths->pFsm, &rpcMsg, cbMeta);
      }
    }
    rpcFreeCont(rpcMsg.pCont);

    // only myself, maybe commit
    syncMaybeAdvanceCommitIndex(ths);

  } else {
    // pre commit
    SRpcMsg rpcMsg;
    syncEntry2OriginalRpc(pEntry, &rpcMsg);

    if (ths->pFsm != NULL) {
      // if (ths->pFsm->FpPreCommitCb != NULL && pEntry->originalRpcType != TDMT_SYNC_NOOP) {
      if (ths->pFsm->FpPreCommitCb != NULL && syncUtilUserPreCommit(pEntry->originalRpcType)) {
        SFsmCbMeta cbMeta = {0};
        cbMeta.index = pEntry->index;
        cbMeta.lastConfigIndex = syncNodeGetSnapshotConfigIndex(ths, cbMeta.index);
        cbMeta.isWeak = pEntry->isWeak;
        cbMeta.code = 1;
        cbMeta.state = ths->state;
        cbMeta.seqNum = pEntry->seqNum;
        ths->pFsm->FpPreCommitCb(ths->pFsm, &rpcMsg, cbMeta);
      }
    }
    rpcFreeCont(rpcMsg.pCont);
  }

  syncEntryDestory(pEntry);
  return ret;
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

static int32_t syncDoLeaderTransfer(SSyncNode* ths, SRpcMsg* pRpcMsg, SSyncRaftEntry* pEntry) {
  SyncLeaderTransfer* pSyncLeaderTransfer = syncLeaderTransferFromRpcMsg2(pRpcMsg);

  syncNodeEventLog(ths, "begin leader transfer");

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
    snprintf(eventLog, sizeof(eventLog), "maybe leader transfer to %s:%d %lu",
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

static int32_t syncNodeConfigChange(SSyncNode* ths, SRpcMsg* pRpcMsg, SSyncRaftEntry* pEntry) {
  SSyncCfg oldSyncCfg = ths->pRaftCfg->cfg;

  SSyncCfg newSyncCfg;
  int32_t  ret = syncCfgFromStr(pRpcMsg->pCont, &newSyncCfg);
  ASSERT(ret == 0);

  // update new config myIndex
  syncNodeUpdateNewConfigIndex(ths, &newSyncCfg);

  bool IamInNew = syncNodeInConfig(ths, &newSyncCfg);

  /*
   for (int i = 0; i < newSyncCfg.replicaNum; ++i) {
     if (strcmp(ths->myNodeInfo.nodeFqdn, (newSyncCfg.nodeInfo)[i].nodeFqdn) == 0 &&
         ths->myNodeInfo.nodePort == (newSyncCfg.nodeInfo)[i].nodePort) {
       newSyncCfg.myIndex = i;
       IamInNew = true;
       break;
     }
   }
 */

  bool isDrop;

  if (IamInNew) {
    syncNodeUpdateConfig(ths, &newSyncCfg, pEntry->index, &isDrop);

    // change isStandBy to normal
    if (!isDrop) {
      char  tmpbuf[512];
      char* oldStr = syncCfg2SimpleStr(&oldSyncCfg);
      char* newStr = syncCfg2SimpleStr(&newSyncCfg);
      snprintf(tmpbuf, sizeof(tmpbuf), "config change from %d to %d, index:%ld, %s  -->  %s", oldSyncCfg.replicaNum,
               newSyncCfg.replicaNum, pEntry->index, oldStr, newStr);
      taosMemoryFree(oldStr);
      taosMemoryFree(newStr);

      if (ths->state == TAOS_SYNC_STATE_LEADER) {
        syncNodeBecomeLeader(ths, tmpbuf);
      } else {
        syncNodeBecomeFollower(ths, tmpbuf);
      }
    }
  } else {
    char  tmpbuf[512];
    char* oldStr = syncCfg2SimpleStr(&oldSyncCfg);
    char* newStr = syncCfg2SimpleStr(&newSyncCfg);
    snprintf(tmpbuf, sizeof(tmpbuf), "config change2 from %d to %d, index:%ld, %s  -->  %s", oldSyncCfg.replicaNum,
             newSyncCfg.replicaNum, pEntry->index, oldStr, newStr);
    taosMemoryFree(oldStr);
    taosMemoryFree(newStr);

    syncNodeBecomeFollower(ths, tmpbuf);
  }

  if (gRaftDetailLog) {
    char* sOld = syncCfg2Str(&oldSyncCfg);
    char* sNew = syncCfg2Str(&newSyncCfg);
    sInfo("==config change== 0x11 old:%s new:%s isDrop:%d index:%ld IamInNew:%d \n", sOld, sNew, isDrop, pEntry->index,
          IamInNew);
    taosMemoryFree(sOld);
    taosMemoryFree(sNew);
  }

  // always call FpReConfigCb
  if (ths->pFsm->FpReConfigCb != NULL) {
    SReConfigCbMeta cbMeta = {0};
    cbMeta.code = 0;
    cbMeta.currentTerm = ths->pRaftStore->currentTerm;
    cbMeta.index = pEntry->index;
    cbMeta.lastConfigIndex = syncNodeGetSnapshotConfigIndex(ths, pEntry->index);
    cbMeta.term = pEntry->term;
    cbMeta.newCfg = newSyncCfg;
    cbMeta.oldCfg = oldSyncCfg;
    cbMeta.seqNum = pEntry->seqNum;
    cbMeta.flag = 0x11;
    cbMeta.isDrop = isDrop;
    ths->pFsm->FpReConfigCb(ths->pFsm, pRpcMsg, cbMeta);
  }

  return 0;
}

int32_t syncNodeCommit(SSyncNode* ths, SyncIndex beginIndex, SyncIndex endIndex, uint64_t flag) {
  int32_t    code = 0;
  ESyncState state = flag;

  char eventLog[128];
  snprintf(eventLog, sizeof(eventLog), "commit by wal from index:%ld to index:%ld", beginIndex, endIndex);
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
        if (ths->pFsm->FpCommitCb != NULL && syncUtilUserCommit(pEntry->originalRpcType)) {
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

        // config change
        if (pEntry->originalRpcType == TDMT_SYNC_CONFIG_CHANGE) {
          raftCfgAddConfigIndex(ths->pRaftCfg, pEntry->index);
          raftCfgPersist(ths->pRaftCfg);
          code = syncNodeConfigChange(ths, &rpcMsg, pEntry);
          ASSERT(code == 0);
        }

        // leader transfer
        if (pEntry->originalRpcType == TDMT_SYNC_LEADER_TRANSFER) {
          code = syncDoLeaderTransfer(ths, &rpcMsg, pEntry);
          ASSERT(code == 0);
        }

        // restore finish
        // if only snapshot, a noop entry will be append, so syncLogLastIndex is always ok
        if (pEntry->index == ths->pLogStore->syncLogLastIndex(ths->pLogStore)) {
          if (ths->restoreFinish == false) {
            if (ths->pFsm->FpRestoreFinishCb != NULL) {
              ths->pFsm->FpRestoreFinishCb(ths->pFsm);
            }
            ths->restoreFinish = true;

            char eventLog[128];
            snprintf(eventLog, sizeof(eventLog), "restore finish, index:%ld", pEntry->index);
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
