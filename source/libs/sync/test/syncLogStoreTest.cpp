#include <gtest/gtest.h>
#include <stdio.h>
#include "syncEnv.h"
#include "syncIO.h"
#include "syncInt.h"
#include "syncRaftLog.h"
#include "syncRaftStore.h"
#include "syncUtil.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

uint16_t ports[] = {7010, 7110, 7210, 7310, 7410};
int32_t  replicaNum = 1;
int32_t  myIndex = 0;

SRaftId    ids[TSDB_MAX_REPLICA];
SSyncInfo  syncInfo;
SSyncFSM*  pFsm;
SWal*      pWal;
SSyncNode* pSyncNode;

SSyncNode* syncNodeInit() {
  syncInfo.vgId = 1234;
  syncInfo.rpcClient = gSyncIO->clientRpc;
  syncInfo.FpSendMsg = syncIOSendMsg;
  syncInfo.queue = gSyncIO->pMsgQ;
  syncInfo.FpEqMsg = syncIOEqMsg;
  syncInfo.pFsm = pFsm;
  snprintf(syncInfo.path, sizeof(syncInfo.path), "%s", "./");

  int code = walInit();
  assert(code == 0);
  SWalCfg walCfg;
  memset(&walCfg, 0, sizeof(SWalCfg));
  walCfg.vgId = syncInfo.vgId;
  walCfg.fsyncPeriod = 1000;
  walCfg.retentionPeriod = 1000;
  walCfg.rollPeriod = 1000;
  walCfg.retentionSize = 1000;
  walCfg.segSize = 1000;
  walCfg.level = TAOS_WAL_FSYNC;
  pWal = walOpen("./wal_test", &walCfg);
  assert(pWal != NULL);

  syncInfo.pWal = pWal;

  SSyncCfg* pCfg = &syncInfo.syncCfg;
  pCfg->myIndex = myIndex;
  pCfg->replicaNum = replicaNum;

  for (int i = 0; i < replicaNum; ++i) {
    pCfg->nodeInfo[i].nodePort = ports[i];
    snprintf(pCfg->nodeInfo[i].nodeFqdn, sizeof(pCfg->nodeInfo[i].nodeFqdn), "%s", "127.0.0.1");
    // taosGetFqdn(pCfg->nodeInfo[0].nodeFqdn);
  }

  pSyncNode = syncNodeOpen(&syncInfo);
  assert(pSyncNode != NULL);

  gSyncIO->FpOnSyncPing = pSyncNode->FpOnPing;
  gSyncIO->FpOnSyncPingReply = pSyncNode->FpOnPingReply;
  gSyncIO->FpOnSyncRequestVote = pSyncNode->FpOnRequestVote;
  gSyncIO->FpOnSyncRequestVoteReply = pSyncNode->FpOnRequestVoteReply;
  gSyncIO->FpOnSyncAppendEntries = pSyncNode->FpOnAppendEntries;
  gSyncIO->FpOnSyncAppendEntriesReply = pSyncNode->FpOnAppendEntriesReply;
  gSyncIO->FpOnSyncPing = pSyncNode->FpOnPing;
  gSyncIO->FpOnSyncPingReply = pSyncNode->FpOnPingReply;
  gSyncIO->FpOnSyncTimeout = pSyncNode->FpOnTimeout;
  gSyncIO->pSyncNode = pSyncNode;

  return pSyncNode;
}

SSyncNode* syncInitTest() { return syncNodeInit(); }

void logStoreTest() {
  logStorePrint2((char*)"logStoreTest2", pSyncNode->pLogStore);

  assert(pSyncNode->pLogStore->getLastIndex(pSyncNode->pLogStore) == SYNC_INDEX_INVALID);

  for (int i = 0; i < 5; ++i) {
    int32_t         dataLen = 10;
    SSyncRaftEntry* pEntry = syncEntryBuild(dataLen);
    assert(pEntry != NULL);
    pEntry->msgType = 1;
    pEntry->originalRpcType = 2;
    pEntry->seqNum = 3;
    pEntry->isWeak = true;
    pEntry->term = 100;
    pEntry->index = pSyncNode->pLogStore->getLastIndex(pSyncNode->pLogStore) + 1;
    snprintf(pEntry->data, dataLen, "value%d", i);

    // syncEntryPrint2((char*)"write entry:", pEntry);
    pSyncNode->pLogStore->appendEntry(pSyncNode->pLogStore, pEntry);
    syncEntryDestory(pEntry);

    if (i == 0) {
      assert(pSyncNode->pLogStore->getLastIndex(pSyncNode->pLogStore) == SYNC_INDEX_BEGIN);
    }
  }
  logStorePrint(pSyncNode->pLogStore);

  pSyncNode->pLogStore->truncate(pSyncNode->pLogStore, 3);
  logStorePrint(pSyncNode->pLogStore);
}

void initRaftId(SSyncNode* pSyncNode) {
  for (int i = 0; i < replicaNum; ++i) {
    ids[i] = pSyncNode->replicasId[i];
    char* s = syncUtilRaftId2Str(&ids[i]);
    printf("raftId[%d] : %s\n", i, s);
    free(s);
  }
}

int main(int argc, char** argv) {
  // taosInitLog((char *)"syncTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;

  myIndex = 0;
  if (argc >= 2) {
    myIndex = atoi(argv[1]);
  }

  int32_t ret = syncIOStart((char*)"127.0.0.1", ports[myIndex]);
  assert(ret == 0);

  ret = syncEnvStart();
  assert(ret == 0);

  taosRemoveDir("./wal_test");

  pSyncNode = syncInitTest();
  assert(pSyncNode != NULL);

  // syncNodePrint((char*)"syncLogStoreTest", pSyncNode);
  // initRaftId(pSyncNode);

  logStoreTest();

  return 0;
}
