#include <gtest/gtest.h>
#include <cstring>
#include <iostream>
#include <queue>

#include "walInt.h"

class WalCleanEnv : public ::testing::Test {
  protected:
    static void SetUpTestCase() {
      int code = walInit();
      ASSERT(code == 0);
    }

    static void TearDownTestCase() {
      walCleanUp();
    }

    void SetUp() override {
      taosRemoveDir(pathName);
      SWalCfg* pCfg = (SWalCfg*)malloc(sizeof(SWal));
      memset(pCfg, 0, sizeof(SWalCfg));
      pCfg->rollPeriod = -1;
      pCfg->segSize = -1;
      pCfg->walLevel = TAOS_WAL_FSYNC;
      pWal = walOpen(pathName, pCfg);
      ASSERT(pWal != NULL);
    }

    void TearDown() override {
      walClose(pWal);
      pWal = NULL;
    }

    SWal* pWal = NULL;
    const char* pathName = "/tmp/wal_test";
};

class WalKeepEnv : public ::testing::Test {
  protected:
    static void SetUpTestCase() {
      int code = walInit();
      ASSERT(code == 0);
    }

    static void TearDownTestCase() {
      walCleanUp();
    }

    void SetUp() override {
      SWalCfg* pCfg = (SWalCfg*)malloc(sizeof(SWal)); 
      memset(pCfg, 0, sizeof(SWalCfg));
      pCfg->rollPeriod = -1;
      pCfg->segSize = -1;
      pCfg->walLevel = TAOS_WAL_FSYNC;
      pWal = walOpen(pathName, pCfg);
      ASSERT(pWal != NULL);
    }

    void TearDown() override {
      walClose(pWal);
      pWal = NULL;
    }

    SWal* pWal = NULL;
    const char* pathName = "/tmp/wal_test";
};

TEST_F(WalCleanEnv, createNew) {
  walRollFileInfo(pWal);
  ASSERT(pWal->fileInfoSet != NULL);
  ASSERT_EQ(pWal->fileInfoSet->size, 1);
  WalFileInfo* pInfo = (WalFileInfo*)taosArrayGetLast(pWal->fileInfoSet);
  ASSERT_EQ(pInfo->firstVer, 0);
  ASSERT_EQ(pInfo->lastVer, -1);
  ASSERT_EQ(pInfo->closeTs, -1);
  ASSERT_EQ(pInfo->fileSize, 0);
}

TEST_F(WalCleanEnv, serialize) {
  int code = walRollFileInfo(pWal);
  ASSERT(code == 0);
  ASSERT(pWal->fileInfoSet != NULL);

  code = walRollFileInfo(pWal);
  ASSERT(code == 0);
  code = walRollFileInfo(pWal);
  ASSERT(code == 0);
  code = walRollFileInfo(pWal);
  ASSERT(code == 0);
  code = walRollFileInfo(pWal);
  ASSERT(code == 0);
  code = walRollFileInfo(pWal);
  ASSERT(code == 0);
  char*ss = walMetaSerialize(pWal);
  printf("%s\n", ss);
  code = walWriteMeta(pWal);
  ASSERT(code == 0);
}

TEST_F(WalCleanEnv, removeOldMeta) {
  int code = walRollFileInfo(pWal);
  ASSERT(code == 0);
  ASSERT(pWal->fileInfoSet != NULL);
  code = walWriteMeta(pWal);
  ASSERT(code == 0);
  code = walRollFileInfo(pWal);
  ASSERT(code == 0);
  code = walWriteMeta(pWal);
  ASSERT(code == 0);
}

TEST_F(WalKeepEnv, readOldMeta) {
  int code = walRollFileInfo(pWal);
  ASSERT(code == 0);
  code = walWriteMeta(pWal);
  ASSERT(code == 0);
  code = walRollFileInfo(pWal);
  ASSERT(code == 0);
  code = walWriteMeta(pWal);
  ASSERT(code == 0);
  char*oldss = walMetaSerialize(pWal);

  TearDown();
  SetUp();
  code = walReadMeta(pWal);
  ASSERT(code == 0);
  char* newss = walMetaSerialize(pWal);

  int len = strlen(oldss);
  ASSERT_EQ(len, strlen(newss));
  for(int i = 0; i < len; i++) {
    EXPECT_EQ(oldss[i], newss[i]);
  }
}

TEST_F(WalKeepEnv, write) {
  const char* ranStr = "tvapq02tcp";
  const int len = strlen(ranStr);
  int code;
  for(int i = 0; i < 10; i++) {
    code = walWrite(pWal, i, i+1, (void*)ranStr, len); 
    ASSERT_EQ(code, 0);
    code = walWrite(pWal, i+2, i, (void*)ranStr, len);
    ASSERT_EQ(code, -1);
  }
  code = walWriteMeta(pWal);
  ASSERT_EQ(code, 0);
}
