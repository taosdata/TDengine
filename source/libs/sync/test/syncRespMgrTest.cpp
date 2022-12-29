#include "syncTest.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}
SSyncRespMgr *pMgr = NULL;

void syncRespMgrInsert(uint64_t count) {
  for (uint64_t i = 0; i < count; ++i) {
    SRespStub stub;
    memset(&stub, 0, sizeof(SRespStub));
    stub.createTime = taosGetTimestampMs();
    stub.rpcMsg.code = (pMgr->seqNum + 1);
    stub.rpcMsg.info.ahandle = (void *)(200 + i);
    stub.rpcMsg.info.handle = (void *)(300 + i);
    uint64_t ret = syncRespMgrAdd(pMgr, &stub);
    printf("insert: %" PRIu64 " \n", ret);
  }
}

void syncRespMgrDelTest(uint64_t begin, uint64_t end) {
  for (uint64_t i = begin; i <= end; ++i) {
    int32_t ret = syncRespMgrDel(pMgr, i);
    assert(ret == 0);
  }
}

void printStub(SRespStub *p) {
  printf("createTime:%" PRId64 ", rpcMsg.code:%d rpcMsg.ahandle:%" PRId64 " rpcMsg.handle:%" PRId64 " \n",
         p->createTime, p->rpcMsg.code, (int64_t)(p->rpcMsg.info.ahandle), (int64_t)(p->rpcMsg.info.handle));
}
void syncRespMgrPrint() {
  printf("\n----------------syncRespMgrPrint--------------\n");
  taosThreadMutexLock(&(pMgr->mutex));

  SRespStub *p = (SRespStub *)taosHashIterate(pMgr->pRespHash, NULL);
  while (p) {
    printStub(p);
    p = (SRespStub *)taosHashIterate(pMgr->pRespHash, p);
  }

  taosThreadMutexUnlock(&(pMgr->mutex));
}

void syncRespMgrGetTest(uint64_t i) {
  printf("------syncRespMgrGetTest-------: %" PRIu64 " -- \n", i);
  SRespStub stub;
  int32_t   ret = syncRespMgrGet(pMgr, i, &stub);
  if (ret == 1) {
    printStub(&stub);
  } else if (ret == 0) {
    printf("%" PRId64 " notFound \n", i);
  }
}

void syncRespMgrGetAndDelTest(uint64_t i) {
  printf("------syncRespMgrGetAndDelTest-------%" PRIu64 "-- \n", i);
  SRpcHandleInfo stub;
  int32_t   ret = syncRespMgrGetAndDel(pMgr, i, &stub);
  if (ret == 1) {
    // printStub(&stub);
  } else if (ret == 0) {
    printf("%" PRId64 " notFound \n", i);
  }
}

SSyncNode *createSyncNode() {
  SSyncNode *pSyncNode = (SSyncNode *)taosMemoryMalloc(sizeof(SSyncNode));
  memset(pSyncNode, 0, sizeof(SSyncNode));
  return pSyncNode;
}

void test1() {
  printf("------- test1 ---------\n");
  pMgr = syncRespMgrCreate(createSyncNode(), 0);
  assert(pMgr != NULL);

  syncRespMgrInsert(10);
  syncRespMgrPrint();

  printf("====== get print \n");
  for (uint64_t i = 1; i <= 10; ++i) {
    syncRespMgrGetTest(i);
  }

  printf("===== delete 5 - 7 \n");
  syncRespMgrDelTest(5, 7);
  syncRespMgrPrint();

  printf("====== get print \n");
  for (uint64_t i = 1; i <= 10; ++i) {
    syncRespMgrGetTest(i);
  }

  syncRespMgrDestroy(pMgr);
}

void test2() {
  printf("------- test2 ---------\n");
  pMgr = syncRespMgrCreate(createSyncNode(), 0);
  assert(pMgr != NULL);

  syncRespMgrInsert(10);
  syncRespMgrPrint();

  printf("====== get and delete 3 - 7 \n");
  for (uint64_t i = 3; i <= 7; ++i) {
    syncRespMgrGetAndDelTest(i);
  }

  syncRespMgrPrint();
  syncRespMgrDestroy(pMgr);
}

void test3() {
  printf("------- test3 ---------\n");
  pMgr = syncRespMgrCreate(createSyncNode(), 0);
  assert(pMgr != NULL);

  syncRespMgrInsert(10);
  syncRespMgrPrint();

  printf("====== get and delete 0 - 20 \n");
  for (uint64_t i = 0; i <= 20; ++i) {
    syncRespMgrGetAndDelTest(i);
  }

  syncRespMgrPrint();
  syncRespMgrDestroy(pMgr);
}

void test4() {
  printf("------- test4 ---------\n");
  pMgr = syncRespMgrCreate(createSyncNode(), 2);
  assert(pMgr != NULL);

  syncRespMgrInsert(5);
  syncRespMgrPrint();

  taosMsleep(3000);

  syncRespMgrInsert(3);
  syncRespMgrPrint();

  printf("====== after clean ttl \n");
  syncRespClean(pMgr);
  syncRespMgrPrint();

  syncRespMgrDestroy(pMgr);
}

int main() {
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_DEBUG + DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE;
  logTest();
  test1();
  test2();
  test3();
  test4();

  return 0;
}
