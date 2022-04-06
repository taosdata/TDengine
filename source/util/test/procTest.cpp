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
#include "tprocess.h"
#include "tqueue.h"
#include "trpc.h"
#include "sut.h"

class UtilTesProc : public ::testing::Test {
 public:
  void SetUp() override {
    test.InitLog("/tmp/td");
    uDebugFlag = 207;
    shm.id = -1;
  }
  void TearDown() override {
    taosDropShm(&shm);
  }

 public:
  static Testbase test;
  static SShm     shm;
  static void     SetUpTestSuite() {}
  static void     TearDownTestSuite() {}
};

Testbase UtilTesProc::test;
SShm     UtilTesProc::shm;

TEST_F(UtilTesProc, 01_Create_Drop_Proc) {
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

  taosDropShm(&shm);
}