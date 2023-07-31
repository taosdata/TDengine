#include "syncRaftStore.h"
#include "syncTest.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

uint16_t ports[] = {7010, 7110, 7210, 7310, 7410};
int32_t  replicaNum = 5;
int32_t  myIndex = 0;
SRaftId  ids[TSDB_MAX_REPLICA];

void initRaftId() {
  for (int i = 0; i < replicaNum; ++i) {
    ids[i].addr = syncUtilAddr2U64("127.0.0.1", ports[i]);
    ids[i].vgId = 1234;
    char* s = syncUtilRaftId2Str(&ids[i]);
    printf("raftId[%d] : %s\n", i, s);
    taosMemoryFree(s);
  }
}

int main() {
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE;

  logTest();

  initRaftId();

  // SRaftStore* pRaftStore = raftStoreOpen("./test_raft_store.json");
  // assert(pRaftStore != NULL);
  // raftStoreLog2((char*)"==raftStoreOpen==", pRaftStore);

  // raftStoreSetTerm(pRaftStore, 100);
  // raftStoreLog2((char*)"==raftStoreSetTerm==", pRaftStore);

  // raftStoreVote(pRaftStore, &ids[0]);
  // raftStoreLog2((char*)"==raftStoreVote==", pRaftStore);

  // raftStoreClearVote(pRaftStore);
  // raftStoreLog2((char*)"==raftStoreClearVote==", pRaftStore);

  // raftStoreVote(pRaftStore, &ids[1]);
  // raftStoreLog2((char*)"==raftStoreVote==", pRaftStore);

  // raftStoreNextTerm(pRaftStore);
  // raftStoreLog2((char*)"==raftStoreNextTerm==", pRaftStore);

  // raftStoreNextTerm(pRaftStore);
  // raftStoreLog2((char*)"==raftStoreNextTerm==", pRaftStore);

  // raftStoreNextTerm(pRaftStore);
  // raftStoreLog2((char*)"==raftStoreNextTerm==", pRaftStore);

  // raftStoreNextTerm(pRaftStore);
  // raftStoreLog2((char*)"==raftStoreNextTerm==", pRaftStore);

  // raftStoreClose(pRaftStore);

  return 0;
}
