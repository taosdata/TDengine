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
      SWalCfg* pCfg = (SWalCfg*)malloc(sizeof(SWalCfg));
      memset(pCfg, 0, sizeof(SWalCfg));
      pCfg->rollPeriod = -1;
      pCfg->segSize = -1;
      pCfg->retentionPeriod = 0;
      pCfg->retentionSize = 0;
      pCfg->level = TAOS_WAL_FSYNC;
      pWal = walOpen(pathName, pCfg);
      free(pCfg);
      ASSERT(pWal != NULL);
    }

    void TearDown() override {
      walClose(pWal);
      pWal = NULL;
    }

    SWal* pWal = NULL;
    const char* pathName = "/tmp/wal_test";
};

class WalCleanDeleteEnv : public ::testing::Test {
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
      SWalCfg* pCfg = (SWalCfg*)malloc(sizeof(SWalCfg));
      memset(pCfg, 0, sizeof(SWalCfg));
      pCfg->retentionPeriod = 0;
      pCfg->retentionSize = 0;
      pCfg->level = TAOS_WAL_FSYNC;
      pWal = walOpen(pathName, pCfg);
      free(pCfg);
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

    void walResetEnv() {
      TearDown();
      taosRemoveDir(pathName);
      SetUp();
    }

    void SetUp() override {
      SWalCfg* pCfg = (SWalCfg*)malloc(sizeof(SWalCfg)); 
      memset(pCfg, 0, sizeof(SWalCfg));
      pCfg->rollPeriod = -1;
      pCfg->segSize = -1;
      pCfg->retentionPeriod = 0;
      pCfg->retentionSize = 0;
      pCfg->level = TAOS_WAL_FSYNC;
      pWal = walOpen(pathName, pCfg);
      free(pCfg);
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
  free(ss);
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
  walResetEnv();
  const char* ranStr = "tvapq02tcp";
  int len = strlen(ranStr);
  int code;

  for(int i = 0; i < 10; i++) {
    code = walWrite(pWal, i, i+1, (void*)ranStr, len); 
    ASSERT_EQ(code, 0);
    ASSERT_EQ(pWal->vers.lastVer, i);
    code = walWrite(pWal, i+2, i, (void*)ranStr, len);
    ASSERT_EQ(code, -1);
    ASSERT_EQ(pWal->vers.lastVer, i);
  }
  char* oldss = walMetaSerialize(pWal);

  TearDown();
  SetUp();

  ASSERT_EQ(pWal->vers.firstVer, 0);
  ASSERT_EQ(pWal->vers.lastVer, 9);

  char* newss = walMetaSerialize(pWal);

  len = strlen(oldss);
  ASSERT_EQ(len, strlen(newss));
  for(int i = 0; i < len; i++) {
    EXPECT_EQ(oldss[i], newss[i]);
  }
  free(oldss);
  free(newss);
}

TEST_F(WalCleanEnv, write) {
  const char* ranStr = "tvapq02tcp";
  const int len = strlen(ranStr);
  int code;
  for(int i = 0; i < 10; i++) {
    code = walWrite(pWal, i, i+1, (void*)ranStr, len); 
    ASSERT_EQ(code, 0);
    ASSERT_EQ(pWal->vers.lastVer, i);
    code = walWrite(pWal, i+2, i, (void*)ranStr, len);
    ASSERT_EQ(code, -1);
    ASSERT_EQ(pWal->vers.lastVer, i);
  }
  code = walWriteMeta(pWal);
  ASSERT_EQ(code, 0);
}

TEST_F(WalCleanEnv, rollback) {
  const char* ranStr = "tvapq02tcp";
  const int len = strlen(ranStr);
  int code;
  for(int i = 0; i < 10; i++) {
    code = walWrite(pWal, i, i+1, (void*)ranStr, len); 
    ASSERT_EQ(code, 0);
    ASSERT_EQ(pWal->vers.lastVer, i);
  }
  code = walRollback(pWal, 5);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(pWal->vers.lastVer, 4);
  code = walRollback(pWal, 3);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(pWal->vers.lastVer, 2);
  code = walWriteMeta(pWal);
  ASSERT_EQ(code, 0);
}

TEST_F(WalCleanDeleteEnv, roll) {
  const char* ranStr = "tvapq02tcp";
  const int len = strlen(ranStr);
  int code;
  int i;
  for(i = 0; i < 100; i++) {
    code = walWrite(pWal, i, 0, (void*)ranStr, len);
    ASSERT_EQ(code, 0);
    ASSERT_EQ(pWal->vers.lastVer, i);
    code = walCommit(pWal, i);
    ASSERT_EQ(pWal->vers.commitVer, i);
  }

  walBeginTakeSnapshot(pWal, i-1);
  ASSERT_EQ(pWal->vers.verInSnapshotting, i-1);
  walEndTakeSnapshot(pWal);
  ASSERT_EQ(pWal->vers.snapshotVer, i-1);
  ASSERT_EQ(pWal->vers.verInSnapshotting, -1);

  code = walWrite(pWal, 5, 0, (void*)ranStr, len);
  ASSERT_NE(code, 0);

  for(; i < 200; i++) {
    code = walWrite(pWal, i, 0, (void*)ranStr, len);
    ASSERT_EQ(code, 0);
    code = walCommit(pWal, i);
    ASSERT_EQ(pWal->vers.commitVer, i);
  }

  //code = walWriteMeta(pWal);
  code = walBeginTakeSnapshot(pWal, i - 1);
  ASSERT_EQ(code, 0);
  code = walEndTakeSnapshot(pWal);
  ASSERT_EQ(code, 0);
}
