#include <gtest/gtest.h>
#include <cstring>
#include <iostream>
#include <queue>

#include "walInt.h"

const char* ranStr = "tvapq02tcp";
const int   ranStrLen = strlen(ranStr);

class WalCleanEnv : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    int code = walInit();
    ASSERT(code == 0);
  }

  static void TearDownTestCase() { walCleanUp(); }

  void SetUp() override {
    taosRemoveDir(pathName);
    SWalCfg* pCfg = (SWalCfg*)taosMemoryMalloc(sizeof(SWalCfg));
    memset(pCfg, 0, sizeof(SWalCfg));
    pCfg->rollPeriod = -1;
    pCfg->segSize = -1;
    pCfg->retentionPeriod = 0;
    pCfg->retentionSize = 0;
    pCfg->level = TAOS_WAL_FSYNC;
    pWal = walOpen(pathName, pCfg);
    taosMemoryFree(pCfg);
    ASSERT(pWal != NULL);
  }

  void TearDown() override {
    walClose(pWal);
    pWal = NULL;
  }

  SWal*       pWal = NULL;
  const char* pathName = TD_TMP_DIR_PATH "wal_test";
};

class WalCleanDeleteEnv : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    int code = walInit();
    ASSERT(code == 0);
  }

  static void TearDownTestCase() { walCleanUp(); }

  void SetUp() override {
    taosRemoveDir(pathName);
    SWalCfg* pCfg = (SWalCfg*)taosMemoryMalloc(sizeof(SWalCfg));
    memset(pCfg, 0, sizeof(SWalCfg));
    pCfg->retentionPeriod = 0;
    pCfg->retentionSize = 0;
    pCfg->level = TAOS_WAL_FSYNC;
    pWal = walOpen(pathName, pCfg);
    taosMemoryFree(pCfg);
    ASSERT(pWal != NULL);
  }

  void TearDown() override {
    walClose(pWal);
    pWal = NULL;
  }

  SWal*       pWal = NULL;
  const char* pathName = TD_TMP_DIR_PATH "wal_test";
};

class WalKeepEnv : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    int code = walInit();
    ASSERT(code == 0);
  }

  static void TearDownTestCase() { walCleanUp(); }

  void walResetEnv() {
    TearDown();
    taosRemoveDir(pathName);
    SetUp();
  }

  void SetUp() override {
    SWalCfg* pCfg = (SWalCfg*)taosMemoryMalloc(sizeof(SWalCfg));
    memset(pCfg, 0, sizeof(SWalCfg));
    pCfg->rollPeriod = -1;
    pCfg->segSize = -1;
    pCfg->retentionPeriod = 0;
    pCfg->retentionSize = 0;
    pCfg->level = TAOS_WAL_FSYNC;
    pWal = walOpen(pathName, pCfg);
    taosMemoryFree(pCfg);
    ASSERT(pWal != NULL);
  }

  void TearDown() override {
    walClose(pWal);
    pWal = NULL;
  }

  SWal*       pWal = NULL;
  const char* pathName = TD_TMP_DIR_PATH "wal_test";
};

class WalRetentionEnv : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    int code = walInit();
    ASSERT(code == 0);
  }

  static void TearDownTestCase() { walCleanUp(); }

  void walResetEnv() {
    TearDown();
    taosRemoveDir(pathName);
    SetUp();
  }

  void SetUp() override {
    SWalCfg cfg;
    cfg.rollPeriod = -1;
    cfg.segSize = -1;
    cfg.retentionPeriod = -1;
    cfg.retentionSize = 0;
    cfg.rollPeriod = 0;
    cfg.vgId = 0;
    cfg.level = TAOS_WAL_FSYNC;
    pWal = walOpen(pathName, &cfg);
    ASSERT(pWal != NULL);
  }

  void TearDown() override {
    walClose(pWal);
    pWal = NULL;
  }

  SWal*       pWal = NULL;
  const char* pathName = TD_TMP_DIR_PATH "wal_test";
};

TEST_F(WalCleanEnv, createNew) {
  walRollFileInfo(pWal);
  ASSERT(pWal->fileInfoSet != NULL);
  ASSERT_EQ(pWal->fileInfoSet->size, 1);
  SWalFileInfo* pInfo = (SWalFileInfo*)taosArrayGetLast(pWal->fileInfoSet);
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
  char* ss = walMetaSerialize(pWal);
  printf("%s\n", ss);
  taosMemoryFree(ss);
  code = walSaveMeta(pWal);
  ASSERT(code == 0);
}

TEST_F(WalCleanEnv, removeOldMeta) {
  int code = walRollFileInfo(pWal);
  ASSERT(code == 0);
  ASSERT(pWal->fileInfoSet != NULL);
  code = walSaveMeta(pWal);
  ASSERT(code == 0);
  code = walRollFileInfo(pWal);
  ASSERT(code == 0);
  code = walSaveMeta(pWal);
  ASSERT(code == 0);
}

TEST_F(WalKeepEnv, readOldMeta) {
  walResetEnv();
  int code;

  for (int i = 0; i < 10; i++) {
    code = walWrite(pWal, i, i + 1, (void*)ranStr, ranStrLen);
    ASSERT_EQ(code, 0);
    ASSERT_EQ(pWal->vers.lastVer, i);
    code = walWrite(pWal, i + 2, i, (void*)ranStr, ranStrLen);
    ASSERT_EQ(code, -1);
    ASSERT_EQ(pWal->vers.lastVer, i);
  }
  char* oldss = walMetaSerialize(pWal);

  TearDown();
  SetUp();

  ASSERT_EQ(pWal->vers.firstVer, 0);
  ASSERT_EQ(pWal->vers.lastVer, 9);

  char* newss = walMetaSerialize(pWal);

  int len = strlen(oldss);
  ASSERT_EQ(len, strlen(newss));
  for (int i = 0; i < len; i++) {
    EXPECT_EQ(oldss[i], newss[i]);
  }
  taosMemoryFree(oldss);
  taosMemoryFree(newss);
}

TEST_F(WalCleanEnv, write) {
  int code;
  for (int i = 0; i < 10; i++) {
    code = walWrite(pWal, i, i + 1, (void*)ranStr, ranStrLen);
    ASSERT_EQ(code, 0);
    ASSERT_EQ(pWal->vers.lastVer, i);
    code = walWrite(pWal, i + 2, i, (void*)ranStr, ranStrLen);
    ASSERT_EQ(code, -1);
    ASSERT_EQ(pWal->vers.lastVer, i);
  }
  code = walSaveMeta(pWal);
  ASSERT_EQ(code, 0);
}

TEST_F(WalCleanEnv, rollback) {
  int code;
  for (int i = 0; i < 10; i++) {
    code = walWrite(pWal, i, i + 1, (void*)ranStr, ranStrLen);
    ASSERT_EQ(code, 0);
    ASSERT_EQ(pWal->vers.lastVer, i);
  }
  code = walRollback(pWal, 12);
  ASSERT_NE(code, 0);
  ASSERT_EQ(pWal->vers.lastVer, 9);
  code = walRollback(pWal, 9);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(pWal->vers.lastVer, 8);
  code = walRollback(pWal, 5);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(pWal->vers.lastVer, 4);
  code = walRollback(pWal, 3);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(pWal->vers.lastVer, 2);
  code = walSaveMeta(pWal);
  ASSERT_EQ(code, 0);
}

TEST_F(WalCleanEnv, rollbackMultiFile) {
  int code;
  for (int i = 0; i < 10; i++) {
    code = walWrite(pWal, i, i + 1, (void*)ranStr, ranStrLen);
    ASSERT_EQ(code, 0);
    ASSERT_EQ(pWal->vers.lastVer, i);
    if (i == 5) {
      walBeginSnapshot(pWal, i, 0);
      walEndSnapshot(pWal);
    }
  }
  code = walRollback(pWal, 12);
  ASSERT_NE(code, 0);
  ASSERT_EQ(pWal->vers.lastVer, 9);
  code = walRollback(pWal, 9);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(pWal->vers.lastVer, 8);
  code = walRollback(pWal, 6);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(pWal->vers.lastVer, 5);
  code = walRollback(pWal, 5);
  ASSERT_EQ(code, -1);

  ASSERT_EQ(pWal->vers.lastVer, 5);

  code = walWrite(pWal, 6, 6, (void*)ranStr, ranStrLen);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(pWal->vers.lastVer, 6);

  code = walSaveMeta(pWal);
  ASSERT_EQ(code, 0);
}

TEST_F(WalCleanDeleteEnv, roll) {
  int code;
  int i;
  for (i = 0; i < 100; i++) {
    code = walWrite(pWal, i, 0, (void*)ranStr, ranStrLen);
    ASSERT_EQ(code, 0);
    ASSERT_EQ(pWal->vers.lastVer, i);
    code = walCommit(pWal, i);
    ASSERT_EQ(pWal->vers.commitVer, i);
  }

  walBeginSnapshot(pWal, i - 1, 0);
  ASSERT_EQ(pWal->vers.verInSnapshotting, i - 1);
  walEndSnapshot(pWal);
  ASSERT_EQ(pWal->vers.snapshotVer, i - 1);
  ASSERT_EQ(pWal->vers.verInSnapshotting, -1);

  code = walWrite(pWal, 5, 0, (void*)ranStr, ranStrLen);
  ASSERT_NE(code, 0);

  for (; i < 200; i++) {
    code = walWrite(pWal, i, 0, (void*)ranStr, ranStrLen);
    ASSERT_EQ(code, 0);
    code = walCommit(pWal, i);
    ASSERT_EQ(pWal->vers.commitVer, i);
  }

  code = walBeginSnapshot(pWal, i - 1, 0);
  ASSERT_EQ(code, 0);
  code = walEndSnapshot(pWal);
  ASSERT_EQ(code, 0);
}

TEST_F(WalKeepEnv, readHandleRead) {
  walResetEnv();
  int         code;
  SWalReader* pRead = walOpenReader(pWal, NULL, 0);
  ASSERT(pRead != NULL);

  int i;
  for (i = 0; i < 100; i++) {
    char newStr[100];
    sprintf(newStr, "%s-%d", ranStr, i);
    int len = strlen(newStr);
    code = walWrite(pWal, i, 0, newStr, len);
    ASSERT_EQ(code, 0);
  }
  for (int i = 0; i < 1000; i++) {
    int ver = taosRand() % 100;
    code = walReadVer(pRead, ver);
    ASSERT_EQ(code, 0);

    // printf("rrbody: \n");
    // for(int i = 0; i < pRead->pHead->head.len; i++) {
    // printf("%d ", pRead->pHead->head.body[i]);
    //}
    // printf("\n");

    ASSERT_EQ(pRead->pHead->head.version, ver);
    ASSERT_EQ(pRead->curVersion, ver + 1);
    char newStr[100];
    sprintf(newStr, "%s-%d", ranStr, ver);
    int len = strlen(newStr);
    ASSERT_EQ(pRead->pHead->head.bodyLen, len);
    for (int j = 0; j < len; j++) {
      EXPECT_EQ(newStr[j], pRead->pHead->head.body[j]);
    }
  }
  walCloseReader(pRead);
}

TEST_F(WalRetentionEnv, repairMeta1) {
  walResetEnv();
  int code;

  int i;
  for (i = 0; i < 100; i++) {
    char newStr[100];
    sprintf(newStr, "%s-%d", ranStr, i);
    int len = strlen(newStr);
    code = walWrite(pWal, i, 0, newStr, len);
    ASSERT_EQ(code, 0);
  }

  TearDown();

  // getchar();
  char buf[100];
  sprintf(buf, "%s/meta-ver%d", pathName, 0);
  taosRemoveFile(buf);
  sprintf(buf, "%s/meta-ver%d", pathName, 1);
  taosRemoveFile(buf);
  SetUp();
  // getchar();

  ASSERT_EQ(pWal->vers.lastVer, 99);

  SWalReader* pRead = walOpenReader(pWal, NULL, 0);
  ASSERT(pRead != NULL);

  for (int i = 0; i < 1000; i++) {
    int ver = taosRand() % 100;
    code = walReadVer(pRead, ver);
    ASSERT_EQ(code, 0);

    // printf("rrbody: \n");
    // for(int i = 0; i < pRead->pHead->head.len; i++) {
    // printf("%d ", pRead->pHead->head.body[i]);
    //}
    // printf("\n");

    ASSERT_EQ(pRead->pHead->head.version, ver);
    ASSERT_EQ(pRead->curVersion, ver + 1);
    char newStr[100];
    sprintf(newStr, "%s-%d", ranStr, ver);
    int len = strlen(newStr);
    ASSERT_EQ(pRead->pHead->head.bodyLen, len);
    for (int j = 0; j < len; j++) {
      EXPECT_EQ(newStr[j], pRead->pHead->head.body[j]);
    }
  }

  for (i = 100; i < 200; i++) {
    char newStr[100];
    sprintf(newStr, "%s-%d", ranStr, i);
    int len = strlen(newStr);
    code = walWrite(pWal, i, 0, newStr, len);
    ASSERT_EQ(code, 0);
  }

  for (int i = 0; i < 1000; i++) {
    int ver = taosRand() % 200;
    code = walReadVer(pRead, ver);
    ASSERT_EQ(code, 0);

    // printf("rrbody: \n");
    // for(int i = 0; i < pRead->pHead->head.len; i++) {
    // printf("%d ", pRead->pHead->head.body[i]);
    //}
    // printf("\n");

    ASSERT_EQ(pRead->pHead->head.version, ver);
    ASSERT_EQ(pRead->curVersion, ver + 1);
    char newStr[100];
    sprintf(newStr, "%s-%d", ranStr, ver);
    int len = strlen(newStr);
    ASSERT_EQ(pRead->pHead->head.bodyLen, len);
    for (int j = 0; j < len; j++) {
      EXPECT_EQ(newStr[j], pRead->pHead->head.body[j]);
    }
  }
  walCloseReader(pRead);
}
