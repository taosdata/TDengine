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
#include "syncTimeout.h"
#include "syncUtil.h"
#include "syncVoteMgr.h"
#include "tref.h"

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

  syncNodeLog2("syncNodeOpen open success", pSyncNode);

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
    return -1;
  }

  if (pSyncNode->state != TAOS_SYNC_STATE_LEADER) {
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
  return 0;
}

int32_t syncReconfig(int64_t rid, const SSyncCfg* pSyncCfg) {
  int32_t ret = 0;
  char*   configChange = syncCfg2Str((SSyncCfg*)pSyncCfg);
  sInfo("==syncReconfig== newconfig:%s", configChange);

  SRpcMsg rpcMsg = {0};
  rpcMsg.msgType = TDMT_VND_SYNC_CONFIG_CHANGE;
  rpcMsg.info.noResp = 1;
  rpcMsg.contLen = strlen(configChange) + 1;
  rpcMsg.pCont = rpcMallocCont(rpcMsg.contLen);
  snprintf(rpcMsg.pCont, rpcMsg.contLen, "%s", configChange);
  taosMemoryFree(configChange);
  ret = syncPropose(rid, &rpcMsg, false);
  return ret;
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

int32_t syncGetAndDelRespRpc(int64_t rid, uint64_t index, SRpcMsg* msg) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return TAOS_SYNC_STATE_ERROR;
  }
  assert(rid == pSyncNode->rid);

  SRespStub stub;
  int32_t   ret = syncRespMgrGetAndDel(pSyncNode->pSyncRespMgr, index, &stub);
  if (ret == 1) {
    memcpy(msg, &(stub.rpcMsg), sizeof(SRpcMsg));
  }

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
  sTrace("syncPropose msgType:%d ", pMsg->msgType);

  int32_t    ret = TAOS_SYNC_PROPOSE_SUCCESS;
  SSyncNode* pSyncNode = taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) return TAOS_SYNC_PROPOSE_OTHER_ERROR;

  assert(rid == pSyncNode->rid);

  if (pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
    SRespStub stub;
    stub.createTime = taosGetTimestampMs();
    stub.rpcMsg = *pMsg;
    uint64_t seqNum = syncRespMgrAdd(pSyncNode->pSyncRespMgr, &stub);

    SyncClientRequest* pSyncMsg = syncClientRequestBuild2(pMsg, seqNum, isWeak, pSyncNode->vgId);
    SRpcMsg            rpcMsg;
    syncClientRequest2RpcMsg(pSyncMsg, &rpcMsg);

    if (pSyncNode->FpEqMsg != NULL && (*pSyncNode->FpEqMsg)(pSyncNode->msgcb, &rpcMsg) == 0) {
      ret = TAOS_SYNC_PROPOSE_SUCCESS;
    } else {
      sTrace("syncPropose pSyncNode->FpEqMsg is NULL");
    }
    syncClientRequestDestroy(pSyncMsg);
  } else {
    sTrace("syncPropose not leader, %s", syncUtilState2String(pSyncNode->state));
    ret = TAOS_SYNC_PROPOSE_NOT_LEADER;
  }

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
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
    // create raft config file
    ret = raftCfgCreateFile((SSyncCfg*)&(pSyncInfo->syncCfg), pSyncInfo->isStandBy, pSyncNode->configPath);
    assert(ret == 0);

  } else {
    // update syncCfg by raft_config.json
    pSyncNode->pRaftCfg = raftCfgOpen(pSyncNode->configPath);
    assert(pSyncNode->pRaftCfg != NULL);
    pSyncInfo->syncCfg = pSyncNode->pRaftCfg->cfg;

    char* seralized = raftCfg2Str(pSyncNode->pRaftCfg);
    sInfo("syncNodeOpen update config :%s", seralized);
    taosMemoryFree(seralized);

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
  pSyncNode->FpOnRequestVote = syncNodeOnRequestVoteCb;
  pSyncNode->FpOnRequestVoteReply = syncNodeOnRequestVoteReplyCb;
  pSyncNode->FpOnAppendEntries = syncNodeOnAppendEntriesCb;
  pSyncNode->FpOnAppendEntriesReply = syncNodeOnAppendEntriesReplyCb;
  pSyncNode->FpOnTimeout = syncNodeOnTimeoutCb;

  // tools
  pSyncNode->pSyncRespMgr = syncRespMgrCreate(NULL, 0);
  assert(pSyncNode->pSyncRespMgr != NULL);

  // restore state
  pSyncNode->restoreFinish = false;
  pSyncNode->pSnapshot = NULL;
  if (pSyncNode->pFsm->FpGetSnapshot != NULL) {
    pSyncNode->pSnapshot = taosMemoryMalloc(sizeof(SSnapshot));
    pSyncNode->pFsm->FpGetSnapshot(pSyncNode->pFsm, pSyncNode->pSnapshot);
  }
  // tsem_init(&(pSyncNode->restoreSem), 0, 0);

  // start in syncNodeStart
  // start raft
  // syncNodeBecomeFollower(pSyncNode);

  return pSyncNode;
}

void syncNodeStart(SSyncNode* pSyncNode) {
  // start raft
  if (pSyncNode->replicaNum == 1) {
    syncNodeBecomeLeader(pSyncNode);

    syncNodeLog2("==state change become leader immediately==", pSyncNode);

    // Raft 3.6.2 Committing entries from previous terms

    // use this now
    syncNodeAppendNoop(pSyncNode);
    syncMaybeAdvanceCommitIndex(pSyncNode);  // maybe only one replica

    /*
    sInfo("==syncNodeStart== RestoreFinish begin 1 replica tsem_wait %p", pSyncNode);
    tsem_wait(&pSyncNode->restoreSem);
    sInfo("==syncNodeStart== RestoreFinish end 1 replica tsem_wait %p", pSyncNode);
    */

    /*
    while (pSyncNode->restoreFinish != true) {
      taosMsleep(10);
    }
    */

    sInfo("==syncNodeStart== restoreFinish ok 1 replica %p vgId:%d", pSyncNode, pSyncNode->vgId);
    return;
  }

  syncNodeBecomeFollower(pSyncNode);

  // for test
  int32_t ret = 0;
  // ret = syncNodeStartPingTimer(pSyncNode);
  assert(ret == 0);

  /*
  sInfo("==syncNodeStart== RestoreFinish begin multi replica tsem_wait %p", pSyncNode);
  tsem_wait(&pSyncNode->restoreSem);
  sInfo("==syncNodeStart== RestoreFinish end multi replica tsem_wait %p", pSyncNode);
  */

  /*
  while (pSyncNode->restoreFinish != true) {
    taosMsleep(10);
  }
  */
  sInfo("==syncNodeStart== restoreFinish ok multi replica %p vgId:%d", pSyncNode, pSyncNode->vgId);
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

  if (pSyncNode->pSnapshot != NULL) {
    taosMemoryFree(pSyncNode->pSnapshot);
  }

  // tsem_destroy(&pSyncNode->restoreSem);

  // free memory in syncFreeNode
  // taosMemoryFree(pSyncNode);
}

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
  taosTmrReset(pSyncNode->FpPingTimerCB, pSyncNode->pingTimerMS, pSyncNode, gSyncEnv->pTimerManager,
               &pSyncNode->pPingTimer);
  atomic_store_64(&pSyncNode->pingTimerLogicClock, pSyncNode->pingTimerLogicClockUser);
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
  pSyncNode->electTimerMS = ms;
  taosTmrReset(pSyncNode->FpElectTimerCB, pSyncNode->electTimerMS, pSyncNode, gSyncEnv->pTimerManager,
               &pSyncNode->pElectTimer);
  atomic_store_64(&pSyncNode->electTimerLogicClock, pSyncNode->electTimerLogicClockUser);
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
  int32_t electMS = syncUtilElectRandomMS(pSyncNode->electBaseLine, 2 * pSyncNode->electBaseLine);
  ret = syncNodeRestartElectTimer(pSyncNode, electMS);
  return ret;
}

int32_t syncNodeStartHeartbeatTimer(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  taosTmrReset(pSyncNode->FpHeartbeatTimerCB, pSyncNode->heartbeatTimerMS, pSyncNode, gSyncEnv->pTimerManager,
               &pSyncNode->pHeartbeatTimer);
  atomic_store_64(&pSyncNode->heartbeatTimerLogicClock, pSyncNode->heartbeatTimerLogicClockUser);
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

char* syncNode2SimpleStr(const SSyncNode* pSyncNode) {
  int   len = 256;
  char* s = (char*)taosMemoryMalloc(len);
  snprintf(s, len,
           "syncNode2SimpleStr vgId:%d currentTerm:%lu, commitIndex:%ld, state:%d %s, isStandBy:%d, "
           "electTimerLogicClock:%lu, "
           "electTimerLogicClockUser:%lu, "
           "electTimerMS:%d",
           pSyncNode->vgId, pSyncNode->pRaftStore->currentTerm, pSyncNode->commitIndex, pSyncNode->state,
           syncUtilState2String(pSyncNode->state), pSyncNode->pRaftCfg->isStandBy, pSyncNode->electTimerLogicClock,
           pSyncNode->electTimerLogicClockUser, pSyncNode->electTimerMS);
  return s;
}

void syncNodeUpdateConfig(SSyncNode* pSyncNode, SSyncCfg* newConfig, bool* isDrop) {
  SSyncCfg oldConfig = pSyncNode->pRaftCfg->cfg;
  pSyncNode->pRaftCfg->cfg = *newConfig;
  int32_t ret = 0;

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

  // isDrop
  *isDrop = true;
  bool IamInOld, IamInNew;
  for (int i = 0; i < oldConfig.replicaNum; ++i) {
    if (strcmp((oldConfig.nodeInfo)[i].nodeFqdn, pSyncNode->myNodeInfo.nodeFqdn) == 0 &&
        (oldConfig.nodeInfo)[i].nodePort == pSyncNode->myNodeInfo.nodePort) {
      *isDrop = false;
      break;
    }
  }

  for (int i = 0; i < newConfig->replicaNum; ++i) {
    if (strcmp((newConfig->nodeInfo)[i].nodeFqdn, pSyncNode->myNodeInfo.nodeFqdn) == 0 &&
        (newConfig->nodeInfo)[i].nodePort == pSyncNode->myNodeInfo.nodePort) {
      *isDrop = false;
      break;
    }
  }

  if (!(*isDrop)) {
    // change isStandBy to normal
    pSyncNode->pRaftCfg->isStandBy = 0;
  }

  raftCfgPersist(pSyncNode->pRaftCfg);
  syncNodeLog2("==syncNodeUpdateConfig==", pSyncNode);
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
    syncNodeBecomeFollower(pSyncNode);
    raftStoreClearVote(pSyncNode->pRaftStore);
  }
}

void syncNodeBecomeFollower(SSyncNode* pSyncNode) {
  // maybe clear leader cache
  if (pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
    pSyncNode->leaderCache = EMPTY_RAFT_ID;
  }

  // state change
  pSyncNode->state = TAOS_SYNC_STATE_FOLLOWER;
  syncNodeStopHeartbeatTimer(pSyncNode);

  // reset elect timer
  syncNodeResetElectTimer(pSyncNode);
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
void syncNodeBecomeLeader(SSyncNode* pSyncNode) {
  // state change
  pSyncNode->state = TAOS_SYNC_STATE_LEADER;

  // set leader cache
  pSyncNode->leaderCache = pSyncNode->myRaftId;

  for (int i = 0; i < pSyncNode->pNextIndex->replicaNum; ++i) {
    // maybe overwrite myself, no harm
    // just do it!
    pSyncNode->pNextIndex->index[i] = pSyncNode->pLogStore->getLastIndex(pSyncNode->pLogStore) + 1;
  }

  for (int i = 0; i < pSyncNode->pMatchIndex->replicaNum; ++i) {
    // maybe overwrite myself, no harm
    // just do it!
    pSyncNode->pMatchIndex->index[i] = SYNC_INDEX_INVALID;
  }

  // stop elect timer
  syncNodeStopElectTimer(pSyncNode);

  // start replicate right now!
  syncNodeReplicate(pSyncNode);

  // start heartbeat timer
  syncNodeStartHeartbeatTimer(pSyncNode);
}

void syncNodeCandidate2Leader(SSyncNode* pSyncNode) {
  assert(pSyncNode->state == TAOS_SYNC_STATE_CANDIDATE);
  assert(voteGrantedMajority(pSyncNode->pVotesGranted));
  syncNodeBecomeLeader(pSyncNode);

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
  syncNodeBecomeFollower(pSyncNode);

  syncNodeLog2("==state change syncNodeLeader2Follower==", pSyncNode);
}

void syncNodeCandidate2Follower(SSyncNode* pSyncNode) {
  assert(pSyncNode->state == TAOS_SYNC_STATE_CANDIDATE);
  syncNodeBecomeFollower(pSyncNode);

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
  char* serialized = syncNode2Str(pObj);
  sTraceLong("syncNodeLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
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
      pSyncNode->FpEqMsg(pSyncNode->msgcb, &rpcMsg);
    } else {
      sTrace("syncNodeEqPingTimer pSyncNode->FpEqMsg is NULL");
    }
    syncTimeoutDestroy(pSyncMsg);

    taosTmrReset(syncNodeEqPingTimer, pSyncNode->pingTimerMS, pSyncNode, gSyncEnv->pTimerManager,
                 &pSyncNode->pPingTimer);
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
      pSyncNode->FpEqMsg(pSyncNode->msgcb, &rpcMsg);
    } else {
      sTrace("syncNodeEqElectTimer pSyncNode->FpEqMsg is NULL");
    }
    syncTimeoutDestroy(pSyncMsg);

    // reset timer ms
    pSyncNode->electTimerMS = syncUtilElectRandomMS(pSyncNode->electBaseLine, 2 * pSyncNode->electBaseLine);
    taosTmrReset(syncNodeEqElectTimer, pSyncNode->electTimerMS, pSyncNode, gSyncEnv->pTimerManager,
                 &pSyncNode->pElectTimer);
  } else {
    sTrace("==syncNodeEqElectTimer== electTimerLogicClock:%" PRIu64 ", electTimerLogicClockUser:%" PRIu64 "",
           pSyncNode->electTimerLogicClock, pSyncNode->electTimerLogicClockUser);
  }
}

static void syncNodeEqHeartbeatTimer(void* param, void* tmrId) {
  SSyncNode* pSyncNode = (SSyncNode*)param;
  if (atomic_load_64(&pSyncNode->heartbeatTimerLogicClockUser) <=
      atomic_load_64(&pSyncNode->heartbeatTimerLogicClock)) {
    SyncTimeout* pSyncMsg =
        syncTimeoutBuild2(SYNC_TIMEOUT_HEARTBEAT, atomic_load_64(&pSyncNode->heartbeatTimerLogicClock),
                          pSyncNode->heartbeatTimerMS, pSyncNode->vgId, pSyncNode);
    SRpcMsg rpcMsg;
    syncTimeout2RpcMsg(pSyncMsg, &rpcMsg);
    syncRpcMsgLog2((char*)"==syncNodeEqHeartbeatTimer==", &rpcMsg);
    if (pSyncNode->FpEqMsg != NULL) {
      pSyncNode->FpEqMsg(pSyncNode->msgcb, &rpcMsg);
    } else {
      sTrace("syncNodeEqHeartbeatTimer pSyncNode->FpEqMsg is NULL");
    }
    syncTimeoutDestroy(pSyncMsg);

    taosTmrReset(syncNodeEqHeartbeatTimer, pSyncNode->heartbeatTimerMS, pSyncNode, gSyncEnv->pTimerManager,
                 &pSyncNode->pHeartbeatTimer);
  } else {
    sTrace("==syncNodeEqHeartbeatTimer== heartbeatTimerLogicClock:%" PRIu64 ", heartbeatTimerLogicClockUser:%" PRIu64
           "",
           pSyncNode->heartbeatTimerLogicClock, pSyncNode->heartbeatTimerLogicClockUser);
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
    ths->pLogStore->appendEntry(ths->pLogStore, pEntry);
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

  SyncIndex       index = ths->pLogStore->getLastIndex(ths->pLogStore) + 1;
  SyncTerm        term = ths->pRaftStore->currentTerm;
  SSyncRaftEntry* pEntry = syncEntryBuild2((SyncClientRequest*)pMsg, term, index);
  assert(pEntry != NULL);

  if (ths->state == TAOS_SYNC_STATE_LEADER) {
    ths->pLogStore->appendEntry(ths->pLogStore, pEntry);

    // start replicate right now!
    syncNodeReplicate(ths);

    // pre commit
    SRpcMsg rpcMsg;
    syncEntry2OriginalRpc(pEntry, &rpcMsg);

    if (ths->pFsm != NULL) {
      // if (ths->pFsm->FpPreCommitCb != NULL && pEntry->originalRpcType != TDMT_VND_SYNC_NOOP) {
      if (ths->pFsm->FpPreCommitCb != NULL && syncUtilUserPreCommit(pEntry->originalRpcType)) {
        SFsmCbMeta cbMeta;
        cbMeta.index = pEntry->index;
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
      // if (ths->pFsm->FpPreCommitCb != NULL && pEntry->originalRpcType != TDMT_VND_SYNC_NOOP) {
      if (ths->pFsm->FpPreCommitCb != NULL && syncUtilUserPreCommit(pEntry->originalRpcType)) {
        SFsmCbMeta cbMeta;
        cbMeta.index = pEntry->index;
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
      return "FOLLOWER";
    case TAOS_SYNC_STATE_CANDIDATE:
      return "CANDIDATE";
    case TAOS_SYNC_STATE_LEADER:
      return "LEADER";
    default:
      return "ERROR";
  }
}
