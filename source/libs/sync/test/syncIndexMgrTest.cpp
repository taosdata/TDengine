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
int32_t  replicaNum = 3;
int32_t  myIndex = 0;

SRaftId    ids[TSDB_MAX_REPLICA];
SSyncNode* pSyncNode;

SSyncNode* syncNodeInit() {
  pSyncNode = (SSyncNode*)taosMemoryMalloc(sizeof(SSyncNode));
  memset(pSyncNode, 0, sizeof(SSyncNode));
  pSyncNode->replicaNum = replicaNum;
  for (int i = 0; i < replicaNum; ++i) {
    pSyncNode->replicasId[i].addr = syncUtilAddr2U64("127.0.0.1", ports[i]);
    pSyncNode->replicasId[i].vgId = 1234;

    ids[i].addr = pSyncNode->replicasId[i].addr;
    ids[i].vgId = pSyncNode->replicasId[i].vgId;
  }

  return pSyncNode;
}

int main(int argc, char** argv) {
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE;

  myIndex = 0;
  if (argc >= 2) {
    myIndex = atoi(argv[1]);
  }

  SSyncNode* pSyncNode = syncNodeInit();
  assert(pSyncNode != NULL);

  printf("---------------------------------------\n");
  SSyncIndexMgr* pSyncIndexMgr = syncIndexMgrCreate(pSyncNode);
  assert(pSyncIndexMgr != NULL);
  {
    char* serialized = syncIndexMgr2Str(pSyncIndexMgr);
    assert(serialized != NULL);
    printf("%s\n", serialized);
    taosMemoryFree(serialized);
  }
  printf("---------------------------------------\n");

  printf("---------------------------------------\n");
  syncIndexMgrSetIndex(pSyncIndexMgr, &ids[0], 100);
  syncIndexMgrSetIndex(pSyncIndexMgr, &ids[1], 200);
  syncIndexMgrSetIndex(pSyncIndexMgr, &ids[2], 300);
  // syncIndexMgrSetTerm(pSyncIndexMgr, &ids[0], 700);
  // syncIndexMgrSetTerm(pSyncIndexMgr, &ids[1], 800);
  // syncIndexMgrSetTerm(pSyncIndexMgr, &ids[2], 900);
  {
    char* serialized = syncIndexMgr2Str(pSyncIndexMgr);
    assert(serialized != NULL);
    printf("%s\n", serialized);
    taosMemoryFree(serialized);
  }
  printf("---------------------------------------\n");

  printf("---------------------------------------\n");
  for (int i = 0; i < pSyncIndexMgr->replicaNum; ++i) {
    SyncIndex idx = syncIndexMgrGetIndex(pSyncIndexMgr, &ids[i]);
    // SyncTerm  term = syncIndexMgrGetTerm(pSyncIndexMgr, &ids[i]);
    // printf("%d: index:%" PRId64 " term:%" PRIu64 " \n", i, idx, term);
  }
  printf("---------------------------------------\n");

  printf("---------------------------------------\n");
  syncIndexMgrClear(pSyncIndexMgr);
  {
    char* serialized = syncIndexMgr2Str(pSyncIndexMgr);
    assert(serialized != NULL);
    printf("%s\n", serialized);
    taosMemoryFree(serialized);
  }
  printf("---------------------------------------\n");

  syncIndexMgrDestroy(pSyncIndexMgr);
  return 0;
}
