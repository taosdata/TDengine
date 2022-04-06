/**
 * @file queue.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief UTIL module queue tests
 * @version 1.0
 * @date 2022-01-27
 *
 * @copyright Copyright (c) 2022
 *
 */

#include <gtest/gtest.h>
#include "sut.h"
#include "tprocess.h"
#include "tqueue.h"
#include "trpc.h"

class UtilTesProc : public ::testing::Test {
 public:
  void SetUp() override {
    test.InitLog("/tmp/td");
    // uDebugFlag = 207;
    shm.id = -1;
    for (int32_t i = 0; i < 4000; ++i) {
      body[i] = i % 26 + 'a';
    }
    head.pCont = body;
    head.code = 1;
    head.msgType = 2;
    head.noResp = 3;
    head.persistHandle = 4;
  }
  void TearDown() override { taosDropShm(&shm); }

 public:
  static SRpcMsg  head;
  static char     body[4000];
  static Testbase test;
  static SShm     shm;
  static void     SetUpTestSuite() {}
  static void     TearDownTestSuite() {}
};

Testbase UtilTesProc::test;
SShm     UtilTesProc::shm;
char     UtilTesProc::body[4000];
SRpcMsg  UtilTesProc::head;

TEST_F(UtilTesProc, 00_Init_Cleanup) {
  ASSERT_EQ(taosCreateShm(&shm, 1234, 1024 * 1024 * 2), 0);

  shm.size = 1023;
  SProcCfg  cfg = {.childConsumeFp = (ProcConsumeFp)NULL,
                   .childMallocHeadFp = (ProcMallocFp)taosAllocateQitem,
                   .childFreeHeadFp = (ProcFreeFp)taosFreeQitem,
                   .childMallocBodyFp = (ProcMallocFp)rpcMallocCont,
                   .childFreeBodyFp = (ProcFreeFp)rpcFreeCont,
                   .parentConsumeFp = (ProcConsumeFp)NULL,
                   .parentMallocHeadFp = (ProcMallocFp)taosMemoryMalloc,
                   .parentFreeHeadFp = (ProcFreeFp)taosMemoryFree,
                   .parentMallocBodyFp = (ProcMallocFp)rpcMallocCont,
                   .parentFreeBodyFp = (ProcFreeFp)rpcFreeCont,
                   .shm = shm,
                   .parent = &shm,
                   .name = "1234"};
  SProcObj *proc = taosProcInit(&cfg);
  ASSERT_EQ(proc, nullptr);

  shm.size = 2468;
  cfg.shm = shm;
  proc = taosProcInit(&cfg);
  ASSERT_NE(proc, nullptr);

  ASSERT_EQ(taosProcRun(proc), 0);
  taosProcCleanup(proc);
  taosDropShm(&shm);
}

void ConsumeChild1(void *parent, void *pHead, int16_t headLen, void *pBody, int32_t bodyLen, ProcFuncType ftype) {
  SRpcMsg msg;
  memcpy(&msg, pHead, headLen);
  char body[2000] = {0};
  memcpy(body, pBody, bodyLen);

  uDebug("====> parent:%" PRId64 " ftype:%d, headLen:%d bodyLen:%d head:%d:%d:%d:%d body:%s <====", (int64_t)parent,
         ftype, headLen, bodyLen, msg.code, msg.msgType, msg.noResp, msg.persistHandle, body);
  rpcFreeCont(pBody);
  taosFreeQitem(pHead);
}

TEST_F(UtilTesProc, 01_Push_Pop_Child) {
  shm.size = 3000;
  ASSERT_EQ(taosCreateShm(&shm, 1235, shm.size), 0);
  SProcCfg  cfg = {.childConsumeFp = (ProcConsumeFp)ConsumeChild1,
                   .childMallocHeadFp = (ProcMallocFp)taosAllocateQitem,
                   .childFreeHeadFp = (ProcFreeFp)taosFreeQitem,
                   .childMallocBodyFp = (ProcMallocFp)rpcMallocCont,
                   .childFreeBodyFp = (ProcFreeFp)rpcFreeCont,
                   .parentConsumeFp = (ProcConsumeFp)NULL,
                   .parentMallocHeadFp = (ProcMallocFp)taosMemoryMalloc,
                   .parentFreeHeadFp = (ProcFreeFp)taosMemoryFree,
                   .parentMallocBodyFp = (ProcMallocFp)rpcMallocCont,
                   .parentFreeBodyFp = (ProcFreeFp)rpcFreeCont,
                   .shm = shm,
                   .parent = (void *)((int64_t)1235),
                   .name = "child_queue"};
  SProcObj *cproc = taosProcInit(&cfg);
  ASSERT_NE(cproc, nullptr);

  ASSERT_NE(taosProcPutToChildQ(cproc, &head, 0, body, 0, 0, PROC_RSP), 0);
  ASSERT_NE(taosProcPutToChildQ(cproc, &head, 0, body, 0, 0, PROC_REGIST), 0);
  ASSERT_NE(taosProcPutToChildQ(cproc, &head, 0, body, 0, 0, PROC_RELEASE), 0);
  ASSERT_NE(taosProcPutToChildQ(cproc, NULL, 12, body, 0, 0, PROC_REQ), 0);
  ASSERT_NE(taosProcPutToChildQ(cproc, &head, 0, body, 0, 0, PROC_REQ), 0);
  ASSERT_NE(taosProcPutToChildQ(cproc, &head, shm.size, body, 0, 0, PROC_REQ), 0);
  ASSERT_NE(taosProcPutToChildQ(cproc, &head, sizeof(SRpcMsg), body, shm.size, 0, PROC_REQ), 0);

  for (int32_t j = 0; j < 1000; j++) {
    int32_t i = 0;
    for (i = 0; i < 20; ++i) {
      ASSERT_EQ(taosProcPutToChildQ(cproc, &head, sizeof(SRpcMsg), body, i, 0, PROC_REQ), 0);
    }
    ASSERT_NE(taosProcPutToChildQ(cproc, &head, sizeof(SRpcMsg), body, i, 0, PROC_REQ), 0);

    cfg.isChild = true;
    cfg.name = "child_queue";
    SProcObj *pproc = taosProcInit(&cfg);
    ASSERT_NE(pproc, nullptr);
    taosProcRun(pproc);
    taosProcCleanup(pproc);
  }

  taosProcCleanup(cproc);
  taosDropShm(&shm);
}

void ConsumeParent1(void *parent, void *pHead, int16_t headLen, void *pBody, int32_t bodyLen, ProcFuncType ftype) {
  SRpcMsg msg;
  memcpy(&msg, pHead, headLen);
  char body[2000] = {0};
  memcpy(body, pBody, bodyLen);

  uDebug("----> parent:%" PRId64 " ftype:%d, headLen:%d bodyLen:%d head:%d:%d:%d:%d body:%s <----", (int64_t)parent,
         ftype, headLen, bodyLen, msg.code, msg.msgType, msg.noResp, msg.persistHandle, body);
  rpcFreeCont(pBody);
  taosMemoryFree(pHead);
}

TEST_F(UtilTesProc, 02_Push_Pop_Parent) {
  shm.size = 3000;
  ASSERT_EQ(taosCreateShm(&shm, 1236, shm.size), 0);
  SProcCfg  cfg = {.childConsumeFp = (ProcConsumeFp)NULL,
                   .childMallocHeadFp = (ProcMallocFp)taosAllocateQitem,
                   .childFreeHeadFp = (ProcFreeFp)taosFreeQitem,
                   .childMallocBodyFp = (ProcMallocFp)rpcMallocCont,
                   .childFreeBodyFp = (ProcFreeFp)rpcFreeCont,
                   .parentConsumeFp = (ProcConsumeFp)ConsumeParent1,
                   .parentMallocHeadFp = (ProcMallocFp)taosMemoryMalloc,
                   .parentFreeHeadFp = (ProcFreeFp)taosMemoryFree,
                   .parentMallocBodyFp = (ProcMallocFp)rpcMallocCont,
                   .parentFreeBodyFp = (ProcFreeFp)rpcFreeCont,
                   .shm = shm,
                   .parent = (void *)((int64_t)1235),
                   .name = "child_queue"};
  SProcObj *cproc = taosProcInit(&cfg);
  ASSERT_NE(cproc, nullptr);

  cfg.name = "parent_queue";
  cfg.isChild = true;
  SProcObj *pproc = taosProcInit(&cfg);
  ASSERT_NE(pproc, nullptr);

  for (int32_t j = 0; j < 1000; j++) {
    int32_t i = 0;
    for (i = 0; i < 20; ++i) {
      taosProcPutToParentQ(pproc, &head, sizeof(SRpcMsg), body, i, PROC_REQ);
    }

    taosProcRun(cproc);
    taosProcStop(cproc);
  }

  taosProcCleanup(pproc);
  taosProcCleanup(cproc);
  taosDropShm(&shm);
}
