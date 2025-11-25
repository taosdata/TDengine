#include <gtest/gtest.h>
#include <cstring>
#include <iostream>
#include <queue>
#include <thread>
#include <vector>
#include <atomic>

#include "walInt.h"

const char*  ranStr = "tvapq02tcp";
const int    ranStrLen = strlen(ranStr);
SWalSyncInfo syncMeta = {0};

class WalCleanEnv : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    int code = walInit(NULL);
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
    int code = walInit(NULL);
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
    int code = walInit(NULL);
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
    int code = walInit(NULL);
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
    cfg.committed = -1;
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

class WalSkipLevel : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    int code = walInit(NULL);
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
    cfg.committed = -1;
    cfg.retentionPeriod = -1;
    cfg.retentionSize = 0;
    cfg.rollPeriod = 0;
    cfg.vgId = 1;
    cfg.level = TAOS_WAL_SKIP;
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

class WalEncrypted : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    int code = walInit(NULL);
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
    cfg.committed = -1;
    cfg.retentionPeriod = -1;
    cfg.retentionSize = 0;
    cfg.rollPeriod = 0;
    cfg.vgId = 0;
    cfg.level = TAOS_WAL_FSYNC;
    cfg.encryptAlgorithm = 1;
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
  char* ss = NULL;
  code = walMetaSerialize(pWal, &ss);
  ASSERT(code == 0);
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

  syncMeta.isWeek = -1;
  syncMeta.seqNum = UINT64_MAX;
  syncMeta.term = UINT64_MAX;

  for (int i = 0; i < 10; i++) {
    code = walAppendLog(pWal, i, i + 1, syncMeta, (void*)ranStr, ranStrLen, NULL);
    ASSERT_EQ(code, 0);
    ASSERT_EQ(pWal->vers.lastVer, i);
    code = walAppendLog(pWal, i + 2, i, syncMeta, (void*)ranStr, ranStrLen, NULL);
    ASSERT_EQ(code, TSDB_CODE_WAL_INVALID_VER);
    ASSERT_EQ(pWal->vers.lastVer, i);
  }
  char* oldss = NULL;
  code = walMetaSerialize(pWal, &oldss);
  ASSERT(code == 0);

  TearDown();
  SetUp();

  ASSERT_EQ(pWal->vers.firstVer, 0);
  ASSERT_EQ(pWal->vers.lastVer, 9);

  char* newss = NULL;
  code = walMetaSerialize(pWal, &newss);
  ASSERT(code == 0);

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
    code = walAppendLog(pWal, i, i + 1, syncMeta, (void*)ranStr, ranStrLen, NULL);
    ASSERT_EQ(code, 0);
    ASSERT_EQ(pWal->vers.lastVer, i);
    code = walAppendLog(pWal, i + 2, i, syncMeta, (void*)ranStr, ranStrLen, NULL);
    ASSERT_EQ(code, TSDB_CODE_WAL_INVALID_VER);
    ASSERT_EQ(pWal->vers.lastVer, i);
  }
  code = walSaveMeta(pWal);
  ASSERT_EQ(code, 0);
}

TEST_F(WalCleanEnv, rollback) {
  int code;
  for (int i = 0; i < 10; i++) {
    code = walAppendLog(pWal, i, i + 1, syncMeta, (void*)ranStr, ranStrLen, NULL);
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
    code = walAppendLog(pWal, i, i + 1, syncMeta, (void*)ranStr, ranStrLen, NULL);
    ASSERT_EQ(code, 0);
    ASSERT_EQ(pWal->vers.lastVer, i);
    if (i == 5) {
      walBeginSnapshot(pWal, i, 0);
      walEndSnapshot(pWal, false);
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
  ASSERT_NE(code, 0);

  ASSERT_EQ(pWal->vers.lastVer, 5);

  code = walAppendLog(pWal, 6, 6, syncMeta, (void*)ranStr, ranStrLen, NULL);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(pWal->vers.lastVer, 6);

  code = walSaveMeta(pWal);
  ASSERT_EQ(code, 0);
}

TEST_F(WalCleanDeleteEnv, roll) {
  int code;
  int i;
  for (i = 0; i < 100; i++) {
    code = walAppendLog(pWal, i, 0, syncMeta, (void*)ranStr, ranStrLen, NULL);
    ASSERT_EQ(code, 0);
    ASSERT_EQ(pWal->vers.lastVer, i);
    code = walCommit(pWal, i);
    ASSERT_EQ(pWal->vers.commitVer, i);
  }

  walBeginSnapshot(pWal, i - 1, 0);
  ASSERT_EQ(pWal->vers.verInSnapshotting, i - 1);
  walEndSnapshot(pWal, false);
  ASSERT_EQ(pWal->vers.snapshotVer, i - 1);
  ASSERT_EQ(pWal->vers.verInSnapshotting, -1);

  code = walAppendLog(pWal, 5, 0, syncMeta, (void*)ranStr, ranStrLen, NULL);
  ASSERT_NE(code, 0);

  for (; i < 200; i++) {
    code = walAppendLog(pWal, i, 0, syncMeta, (void*)ranStr, ranStrLen, NULL);
    ASSERT_EQ(code, 0);
    code = walCommit(pWal, i);
    ASSERT_EQ(pWal->vers.commitVer, i);
  }

  code = walBeginSnapshot(pWal, i - 1, 0);
  ASSERT_EQ(code, 0);
  code = walEndSnapshot(pWal, false);
  ASSERT_EQ(code, 0);
}

TEST_F(WalKeepEnv, readHandleRead) {
  walResetEnv();
  int         code;
  SWalReader* pRead = walOpenReader(pWal, 0);
  ASSERT(pRead != NULL);

  int i;
  for (i = 0; i < 100; i++) {
    char newStr[100];
    sprintf(newStr, "%s-%d", ranStr, i);
    int len = strlen(newStr);
    code = walAppendLog(pWal, i, 0, syncMeta, newStr, len, NULL);
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

TEST_F(WalKeepEnv, walLogExist) {
  walResetEnv();
  int         code;
  SWalReader* pRead = walOpenReader(pWal, 0);
  ASSERT(pRead != NULL);

  int i;
  for (i = 0; i < 100; i++) {
    char newStr[100];
    sprintf(newStr, "%s-%d", ranStr, i);
    int len = strlen(newStr);
    code = walAppendLog(pWal, i, 0, syncMeta, newStr, len, NULL);
    ASSERT_EQ(code, 0);
  }
  walLogExist(pWal, 0);
  ASSERT_EQ(code, 0);
  walCloseReader(pRead);
}

TEST_F(WalKeepEnv, walScanLogGetLastVerHeadMissMatch) {
  walResetEnv();
  int code;
  do {
    char newStr[100];
    sprintf(newStr, "%s-%d", ranStr, 0);
    sprintf(newStr, "%s-%d", ranStr, 0);
    int len = strlen(newStr);
    code = walAppendLog(pWal, 0, 0, syncMeta, newStr, len, NULL);
  } while (0);

  int  i = 0;
  char newStr[100];
  sprintf(newStr, "%s-%d", ranStr, i);
  int           len = strlen(newStr);
  int64_t       offset = walGetCurFileOffset(pWal);
  SWalFileInfo* pFileInfo = walGetCurFileInfo(pWal);

  pWal->writeHead.head.version = i;
  pWal->writeHead.head.bodyLen = len;
  pWal->writeHead.head.msgType = 0;
  pWal->writeHead.head.ingestTs = taosGetTimestampUs();

  pWal->writeHead.head.syncMeta = syncMeta;

  pWal->writeHead.cksumHead = 1;
  pWal->writeHead.cksumBody = walCalcBodyCksum(newStr, len);
  taosWriteFile(pWal->pLogFile, &pWal->writeHead, sizeof(SWalCkHead));
  taosWriteFile(pWal->pLogFile, newStr, len);

  int64_t lastVer = 0;
  code = walScanLogGetLastVer(pWal, 0, &lastVer);
  ASSERT_EQ(code, TSDB_CODE_WAL_CHKSUM_MISMATCH);
}

TEST_F(WalKeepEnv, walScanLogGetLastVerBodyMissMatch) {
  walResetEnv();
  int code;
  do {
    char newStr[100];
    sprintf(newStr, "%s-%d", ranStr, 0);
    sprintf(newStr, "%s-%d", ranStr, 0);
    int len = strlen(newStr);
    code = walAppendLog(pWal, 0, 0, syncMeta, newStr, len, NULL);
  } while (0);

  int  i = 0;
  char newStr[100];
  sprintf(newStr, "%s-%d", ranStr, i);
  int           len = strlen(newStr);
  int64_t       offset = walGetCurFileOffset(pWal);
  SWalFileInfo* pFileInfo = walGetCurFileInfo(pWal);

  pWal->writeHead.head.version = i;
  pWal->writeHead.head.bodyLen = len;
  pWal->writeHead.head.msgType = 0;
  pWal->writeHead.head.ingestTs = taosGetTimestampUs();

  pWal->writeHead.head.syncMeta = syncMeta;

  pWal->writeHead.cksumHead = walCalcHeadCksum(&pWal->writeHead);
  pWal->writeHead.cksumBody = 1;
  taosWriteFile(pWal->pLogFile, &pWal->writeHead, sizeof(SWalCkHead));
  taosWriteFile(pWal->pLogFile, newStr, len);

  int64_t lastVer = 0;
  code = walScanLogGetLastVer(pWal, 0, &lastVer);
  ASSERT_EQ(code, TSDB_CODE_WAL_CHKSUM_MISMATCH);
}

TEST_F(WalKeepEnv, walCheckAndRepairIdxFile) {
  walResetEnv();
  int code;
  do {
    char newStr[100];
    sprintf(newStr, "%s-%d", ranStr, 0);
    sprintf(newStr, "%s-%d", ranStr, 0);
    int len = strlen(newStr);
    code = walAppendLog(pWal, 0, 0, syncMeta, newStr, len, NULL);
  } while (0);
  SWalFileInfo* pFileInfo = walGetCurFileInfo(pWal);
  for (int i = 1; i < 100; i++) {
    char newStr[100];
    sprintf(newStr, "%s-%d", ranStr, i);
    int len = strlen(newStr);
    pWal->writeHead.head.version = i;
    pWal->writeHead.head.bodyLen = len;
    pWal->writeHead.head.msgType = 0;
    pWal->writeHead.head.ingestTs = taosGetTimestampUs();
    pWal->writeHead.head.syncMeta = syncMeta;
    pWal->writeHead.cksumHead = walCalcHeadCksum(&pWal->writeHead);
    pWal->writeHead.cksumBody = walCalcBodyCksum(newStr, len);
    taosWriteFile(pWal->pLogFile, &pWal->writeHead, sizeof(SWalCkHead));
    taosWriteFile(pWal->pLogFile, newStr, len);
  }
  pWal->vers.lastVer = 99;
  pFileInfo->lastVer = 99;
  code = walCheckAndRepairIdx(pWal);
  ASSERT_EQ(code, 0);
}

TEST_F(WalKeepEnv, walRestoreFromSnapshot1) {
  walResetEnv();
  int code;

  int i;
  for (i = 0; i < 100; i++) {
    char newStr[100];
    sprintf(newStr, "%s-%d", ranStr, i);
    int len = strlen(newStr);
    code = walAppendLog(pWal, i, 0, syncMeta, newStr, len, NULL);
    ASSERT_EQ(code, 0);
  }
  code = walRestoreFromSnapshot(pWal, 50);
  ASSERT_EQ(code, 0);
}

TEST_F(WalKeepEnv, walRestoreFromSnapshot2) {
  walResetEnv();
  int code;

  int i;
  for (i = 0; i < 100; i++) {
    char newStr[100];
    sprintf(newStr, "%s-%d", ranStr, i);
    int len = strlen(newStr);
    code = walAppendLog(pWal, i, 0, syncMeta, newStr, len, NULL);
    ASSERT_EQ(code, 0);
  }
  SWalRef* ref = walOpenRef(pWal);
  ref->refVer = 10;
  code = walRestoreFromSnapshot(pWal, 99);
  ASSERT_EQ(code, -1);
}

TEST_F(WalKeepEnv, walRollback) {
  walResetEnv();
  int code;

  int i;
  for (i = 0; i < 100; i++) {
    char newStr[100];
    sprintf(newStr, "%s-%d", ranStr, i);
    int len = strlen(newStr);
    code = walAppendLog(pWal, i, 0, syncMeta, newStr, len, NULL);
    ASSERT_EQ(code, 0);
  }
  code = walRollback(pWal, -1);
  ASSERT_EQ(code, TSDB_CODE_WAL_INVALID_VER);
  pWal->vers.lastVer = 50;
  pWal->vers.commitVer = 40;
  pWal->vers.snapshotVer = 40;
  SWalFileInfo* fileInfo = walGetCurFileInfo(pWal);

  code = walRollback(pWal, 48);
  ASSERT_EQ(code, 0);
}

TEST_F(WalKeepEnv, walChangeWrite) {
  walResetEnv();
  int code;

  int i;
  for (i = 0; i < 100; i++) {
    char newStr[100];
    sprintf(newStr, "%s-%d", ranStr, i);
    int len = strlen(newStr);
    code = walAppendLog(pWal, i, 0, syncMeta, newStr, len, NULL);
    ASSERT_EQ(code, 0);
  }
  
  code = walChangeWrite(pWal, 50);
  ASSERT_EQ(code, 0);
}

TEST_F(WalCleanEnv, walRepairLogFileTs2) {
  int code;

  int i;
  for (i = 0; i < 100; i++) {
    char newStr[100];
    sprintf(newStr, "%s-%d", ranStr, i);
    int len = strlen(newStr);
    code = walAppendLog(pWal, i, 0, syncMeta, newStr, len, NULL);
    ASSERT_EQ(code, 0);
  }

  code = walRollImpl(pWal);
  ASSERT_EQ(code, 0);

  for (i = 100; i < 200; i++) {
    char newStr[100];
    sprintf(newStr, "%s-%d", ranStr, i);
    int len = strlen(newStr);
    code = walAppendLog(pWal, i, 0, syncMeta, newStr, len, NULL);
    ASSERT_EQ(code, 0);
  }

  code = walRollImpl(pWal);
  ASSERT_EQ(code, 0);

  for (i = 200; i < 300; i++) {
    char newStr[100];
    sprintf(newStr, "%s-%d", ranStr, i);
    int len = strlen(newStr);
    code = walAppendLog(pWal, i, 0, syncMeta, newStr, len, NULL);
    ASSERT_EQ(code, 0);
  }

  code = walRollImpl(pWal);
  ASSERT_EQ(code, 0);

  // Try to step in ts repair logic.
  SWalFileInfo* pFileInfo = (SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, 2);
  pFileInfo->closeTs = -1;

  code = walCheckAndRepairMeta(pWal);
  ASSERT_EQ(code, 0);
}

TEST_F(WalRetentionEnv, repairMeta1) {
  walResetEnv();
  int code;

  int i;
  for (i = 0; i < 100; i++) {
    char newStr[100];
    sprintf(newStr, "%s-%d", ranStr, i);
    int len = strlen(newStr);
    code = walAppendLog(pWal, i, 0, syncMeta, newStr, len, NULL);
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

  SWalReader* pRead = walOpenReader(pWal, 0);
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
    code = walAppendLog(pWal, i, 0, syncMeta, newStr, len, NULL);
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

TEST_F(WalSkipLevel, restart) {
  walResetEnv();
  int code;

  int i;
  for (i = 0; i < 100; i++) {
    char newStr[100];
    sprintf(newStr, "%s-%d", ranStr, i);
    int len = strlen(newStr);
    code = walAppendLog(pWal, i, 0, syncMeta, newStr, len, NULL);
    ASSERT_EQ(code, 0);
  }

  TearDown();

  SetUp();
}

TEST_F(WalSkipLevel, roll) {
  int code;
  int i;
  for (i = 0; i < 100; i++) {
    code = walAppendLog(pWal, i, 0, syncMeta, (void*)ranStr, ranStrLen, NULL);
    ASSERT_EQ(code, 0);
    code = walCommit(pWal, i);
  }
  walBeginSnapshot(pWal, i - 1, 0);
  walEndSnapshot(pWal, false);
  code = walAppendLog(pWal, 5, 0, syncMeta, (void*)ranStr, ranStrLen, NULL);
  ASSERT_NE(code, 0);
  for (; i < 200; i++) {
    code = walAppendLog(pWal, i, 0, syncMeta, (void*)ranStr, ranStrLen, NULL);
    ASSERT_EQ(code, 0);
    code = walCommit(pWal, i);
  }
  code = walBeginSnapshot(pWal, i - 1, 0);
  ASSERT_EQ(code, 0);
  code = walEndSnapshot(pWal, false);
  ASSERT_EQ(code, 0);
}

TEST_F(WalEncrypted, write) {
  int code;
  for (int i = 0; i < 100; i++) {
    code = walAppendLog(pWal, i, i + 1, syncMeta, (void*)ranStr, ranStrLen, NULL);
    ASSERT_EQ(code, 0);
    ASSERT_EQ(pWal->vers.lastVer, i);
  }
  code = walSaveMeta(pWal);
  ASSERT_EQ(code, 0);
}

TEST_F(WalKeepEnv, walSetKeepVersionBasic) {
  walResetEnv();
  int code;

  // Test setting keep version
  code = walSetKeepVersion(pWal, 50);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(pWal->keepVersion, 50);

  // Test setting different keep version
  code = walSetKeepVersion(pWal, 100);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(pWal->keepVersion, 100);

  // Test invalid parameter (negative version)
  code = walSetKeepVersion(pWal, -5);
  ASSERT_NE(code, 0);

  // Test NULL pointer
  code = walSetKeepVersion(NULL, 50);
  ASSERT_NE(code, 0);
}

TEST_F(WalCleanDeleteEnv, walSetKeepVersionWithDeletion) {
  int code;
  int i;
  
  // Write 200 logs
  for (i = 0; i < 200; i++) {
    code = walAppendLog(pWal, i, 0, syncMeta, (void*)ranStr, ranStrLen, NULL);
    ASSERT_EQ(code, 0);
    ASSERT_EQ(pWal->vers.lastVer, i);
    code = walCommit(pWal, i);
    ASSERT_EQ(pWal->vers.commitVer, i);
  }

  // Set keep version to 50, so versions >= 50 should not be deleted
  code = walSetKeepVersion(pWal, 50);
  ASSERT_EQ(code, 0);

  // Trigger snapshot, this should not delete logs with version >= 50
  walBeginSnapshot(pWal, i - 1, 0);
  ASSERT_EQ(pWal->vers.verInSnapshotting, i - 1);
  walEndSnapshot(pWal, false);
  ASSERT_EQ(pWal->vers.snapshotVer, i - 1);
  ASSERT_EQ(pWal->vers.verInSnapshotting, -1);

  // The firstVer should be no greater than 50
  ASSERT_LE(pWal->vers.firstVer, 50);

  // Continue writing more logs
  for (; i < 300; i++) {
    code = walAppendLog(pWal, i, 0, syncMeta, (void*)ranStr, ranStrLen, NULL);
    ASSERT_EQ(code, 0);
    code = walCommit(pWal, i);
    ASSERT_EQ(pWal->vers.commitVer, i);
  }

  // Update keep version to 150
  code = walSetKeepVersion(pWal, 150);
  ASSERT_EQ(code, 0);

  // Trigger another snapshot
  code = walBeginSnapshot(pWal, i - 1, 0);
  ASSERT_EQ(code, 0);
  code = walEndSnapshot(pWal, false);
  ASSERT_EQ(code, 0);

  // The firstVer should be no greater than 150
  ASSERT_LE(pWal->vers.firstVer, 150);
}

TEST_F(WalKeepEnv, walSetKeepVersionConcurrent) {
  walResetEnv();
  int code;

  // Write some logs first
  for (int i = 0; i < 100; i++) {
    char newStr[100];
    sprintf(newStr, "%s-%d", ranStr, i);
    int len = strlen(newStr);
    code = walAppendLog(pWal, i, 0, syncMeta, newStr, len, NULL);
    ASSERT_EQ(code, 0);
  }

  // Test concurrent calls to walSetKeepVersion with multiple threads
  const int numThreads = 10;
  const int callsPerThread = 100;
  std::vector<std::thread> threads;
  std::atomic<int> successCount(0);
  
  for (int i = 0; i < numThreads; i++) {
    threads.push_back(std::thread([this, i, &successCount, callsPerThread]() {
      for (int j = 0; j < callsPerThread; j++) {
        int64_t ver = i * callsPerThread + j;
        int code = walSetKeepVersion(pWal, ver);
        if (code == 0) {
          successCount.fetch_add(1);
        }
      }
    }));
  }

  // Wait for all threads to complete
  for (auto& thread : threads) {
    thread.join();
  }

  // All calls should succeed
  ASSERT_EQ(successCount.load(), numThreads * callsPerThread);
  
  // The final keepVersion should be one of the values set by the threads
  // We can't predict which one exactly due to race conditions, but it should be valid
  int64_t finalVersion = pWal->keepVersion;
  ASSERT_GE(finalVersion, 0);
  ASSERT_LT(finalVersion, numThreads * callsPerThread);
}