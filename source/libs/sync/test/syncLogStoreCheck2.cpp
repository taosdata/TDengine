#include <gtest/gtest.h>
#include "syncTest.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

void init() {
  int code = walInit();
  assert(code == 0);
}

void cleanup() { walCleanUp(); }

SWal* createWal(char* path, int32_t vgId) {
  SWalCfg walCfg;
  memset(&walCfg, 0, sizeof(SWalCfg));
  walCfg.vgId = vgId;
  walCfg.fsyncPeriod = 1000;
  walCfg.retentionPeriod = 1000;
  walCfg.rollPeriod = 1000;
  walCfg.retentionSize = 1000;
  walCfg.segSize = 1000;
  walCfg.level = TAOS_WAL_FSYNC;
  SWal* pWal = walOpen(path, &walCfg);
  assert(pWal != NULL);
  return pWal;
}

SSyncNode* createSyncNode(SWal* pWal) {
  SSyncNode* pSyncNode = (SSyncNode*)taosMemoryMalloc(sizeof(SSyncNode));
  memset(pSyncNode, 0, sizeof(SSyncNode));
  pSyncNode->pWal = pWal;
  return pSyncNode;
}

void usage(char* exe) { printf("usage: %s path vgId \n", exe); }

int main(int argc, char** argv) {
  if (argc != 3) {
    usage(argv[0]);
    exit(-1);
  }
  char*   path = argv[1];
  int32_t vgId = atoi(argv[2]);

  init();
  SWal* pWal = createWal(path, vgId);
  assert(pWal != NULL);
  SSyncNode* pSyncNode = createSyncNode(pWal);
  assert(pSyncNode != NULL);

  SSyncLogStore* pLog = logStoreCreate(pSyncNode);
  assert(pLog != NULL);

  logStoreSimplePrint2((char*)"==syncLogStoreCheck2==", pLog);

  walClose(pWal);
  logStoreDestory(pLog);
  taosMemoryFree(pSyncNode);

  cleanup();
  return 0;
}
