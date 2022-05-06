#include "syncIndexMgr.h"
//#include <gtest/gtest.h>
#include <stdio.h>
#include "syncEnv.h"
#include "syncIO.h"
#include "syncInt.h"
#include "syncRaftStore.h"
#include "syncUtil.h"
#include "syncVoteMgr.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

uint16_t ports[] = {7010, 7110, 7210, 7310, 7410};
int32_t  replicaNum = 3;
int32_t  myIndex = 0;

SRaftId    ids[TSDB_MAX_REPLICA];
SSyncInfo  syncInfo;
SSyncFSM*  pFsm;
SSyncNode* pSyncNode;

SSyncNode* syncNodeInit() {
  syncInfo.vgId = 1234;
  syncInfo.rpcClient = gSyncIO->clientRpc;
  syncInfo.FpSendMsg = syncIOSendMsg;
  syncInfo.queue = gSyncIO->pMsgQ;
  syncInfo.FpEqMsg = syncIOEqMsg;
  syncInfo.pFsm = pFsm;
  snprintf(syncInfo.path, sizeof(syncInfo.path), "%s", "./");

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
  gSyncIO->pSyncNode = pSyncNode;

  return pSyncNode;
}

SSyncNode* syncInitTest() { return syncNodeInit(); }

void initRaftId(SSyncNode* pSyncNode) {
  for (int i = 0; i < replicaNum; ++i) {
    ids[i] = pSyncNode->replicasId[i];
    char* s = syncUtilRaftId2Str(&ids[i]);
    printf("raftId[%d] : %s\n", i, s);
    taosMemoryFree(s);
  }
}

int main(int argc, char** argv) {
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE;

  myIndex = 0;
  if (argc >= 2) {
    myIndex = atoi(argv[1]);
  }

  int32_t ret = syncIOStart((char*)"127.0.0.1", ports[myIndex]);
  assert(ret == 0);

  ret = syncEnvStart();
  assert(ret == 0);

  SSyncNode* pSyncNode = syncInitTest();
  assert(pSyncNode != NULL);

  char* serialized = syncNode2Str(pSyncNode);
  printf("%s\n", serialized);
  taosMemoryFree(serialized);

  initRaftId(pSyncNode);

  SSyncIndexMgr* pSyncIndexMgr = syncIndexMgrCreate(pSyncNode);
  assert(pSyncIndexMgr != NULL);

  printf("---------------------------------------\n");
  {
    char* serialized = syncIndexMgr2Str(pSyncIndexMgr);
    assert(serialized != NULL);
    printf("%s\n", serialized);
    taosMemoryFree(serialized);
  }

  syncIndexMgrSetIndex(pSyncIndexMgr, &ids[0], 100);
  syncIndexMgrSetIndex(pSyncIndexMgr, &ids[1], 200);
  syncIndexMgrSetIndex(pSyncIndexMgr, &ids[2], 300);

  printf("---------------------------------------\n");
  {
    char* serialized = syncIndexMgr2Str(pSyncIndexMgr);
    assert(serialized != NULL);
    printf("%s\n", serialized);
    taosMemoryFree(serialized);
  }

  printf("---------------------------------------\n");
  for (int i = 0; i < pSyncIndexMgr->replicaNum; ++i) {
    SyncIndex idx = syncIndexMgrGetIndex(pSyncIndexMgr, &ids[i]);
    printf("index %d : %lu \n", i, idx);
  }

  syncIndexMgrClear(pSyncIndexMgr);
  printf("---------------------------------------\n");
  {
    char* serialized = syncIndexMgr2Str(pSyncIndexMgr);
    assert(serialized != NULL);
    printf("%s\n", serialized);
    taosMemoryFree(serialized);
  }

  syncIndexMgrDestroy(pSyncIndexMgr);
  return 0;
}
