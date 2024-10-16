#include <gtest/gtest.h>
#include <cstring>
#include <iostream>
#include <queue>

#include "tcs.h"
#include "tcsInt.h"

int32_t tcsInitEnv(int8_t isBlob) {
  int32_t code = 0;

  extern int8_t tsS3EpNum;

  extern char tsS3Hostname[][TSDB_FQDN_LEN];
  extern char tsS3AccessKeyId[][TSDB_FQDN_LEN];
  extern char tsS3AccessKeySecret[][TSDB_FQDN_LEN];
  extern char tsS3BucketName[TSDB_FQDN_LEN];

  /* TCS parameter format
  tsS3Hostname[0] = "endpoint/<account-name>.blob.core.windows.net";
  tsS3AccessKeyId[0] = "<access-key-id/account-name>";
  tsS3AccessKeySecret[0] = "<access-key-secret/account-key>";
  tsS3BucketName = "<bucket/container-name>";
  */

  tsS3Ablob = isBlob;
  if (isBlob) {
    const char *hostname = "endpoint/<account-name>.blob.core.windows.net";
    const char *accessKeyId = "<access-key-id/account-name>";
    const char *accessKeySecret = "<access-key-secret/account-key>";
    const char *bucketName = "<bucket/container-name>";

    tstrncpy(&tsS3Hostname[0][0], hostname, TSDB_FQDN_LEN);
    tstrncpy(&tsS3AccessKeyId[0][0], accessKeyId, TSDB_FQDN_LEN);
    tstrncpy(&tsS3AccessKeySecret[0][0], accessKeySecret, TSDB_FQDN_LEN);
    tstrncpy(tsS3BucketName, bucketName, TSDB_FQDN_LEN);

  } else {
    const char *hostname = "endpoint/<account-name>.blob.core.windows.net";
    const char *accessKeyId = "<access-key-id/account-name>";
    const char *accessKeySecret = "<access-key-secret/account-key>";
    const char *bucketName = "<bucket/container-name>";

    tstrncpy(&tsS3Hostname[0][0], hostname, TSDB_FQDN_LEN);
    tstrncpy(&tsS3AccessKeyId[0][0], accessKeyId, TSDB_FQDN_LEN);
    tstrncpy(&tsS3AccessKeySecret[0][0], accessKeySecret, TSDB_FQDN_LEN);
    tstrncpy(tsS3BucketName, bucketName, TSDB_FQDN_LEN);

    // setup s3 env
    tsS3EpNum = 1;
  }

  tstrncpy(tsTempDir, "/tmp/", PATH_MAX);

  tsS3Enabled = true;
  if (!tsS3Ablob) {
  }

  return code;
}

// TEST(TcsTest, DISABLE_InterfaceTest) {
TEST(TcsTest, InterfaceTest) {
  int code = 0;

  code = tcsInitEnv(true);
  GTEST_ASSERT_EQ(code, 0);
  GTEST_ASSERT_EQ(tsS3Enabled, 1);
  GTEST_ASSERT_EQ(tsS3Ablob, 1);

  code = tcsInit();
  GTEST_ASSERT_EQ(code, 0);

  code = tcsCheckCfg();
  GTEST_ASSERT_EQ(code, 0);
  /*
  code = tcsPutObjectFromFileOffset(file, object_name, offset, size);
  GTEST_ASSERT_EQ(code, 0);
  code = tcsGetObjectBlock(object_name, offset, size, check, ppBlock);
  GTEST_ASSERT_EQ(code, 0);

  tcsDeleteObjectsByPrefix(prefix);
  // list object to check

  code = tcsPutObjectFromFile2(file, object, withcp);
  GTEST_ASSERT_EQ(code, 0);
  code = tcsGetObjectsByPrefix(prefix, path);
  GTEST_ASSERT_EQ(code, 0);
  code = tcsDeleteObjects(object_name, nobject);
  GTEST_ASSERT_EQ(code, 0);
  code = tcsGetObjectToFile(object_name, fileName);
  GTEST_ASSERT_EQ(code, 0);

  // GTEST_ASSERT_NE(pEnv, nullptr);
  */

  tcsUninit();
}

// TEST(TcsTest, DISABLE_InterfaceNonBlobTest) {
TEST(TcsTest, InterfaceNonBlobTest) {
  int code = 0;

  code = tcsInitEnv(false);
  GTEST_ASSERT_EQ(code, 0);
  GTEST_ASSERT_EQ(tsS3Enabled, 1);
  GTEST_ASSERT_EQ(tsS3Ablob, 0);

  code = tcsInit();
  GTEST_ASSERT_EQ(code, 0);

  code = tcsCheckCfg();
  GTEST_ASSERT_EQ(code, 0);
  /*
  code = tcsPutObjectFromFileOffset(file, object_name, offset, size);
  GTEST_ASSERT_EQ(code, 0);
  code = tcsGetObjectBlock(object_name, offset, size, check, ppBlock);
  GTEST_ASSERT_EQ(code, 0);

  tcsDeleteObjectsByPrefix(prefix);
  // list object to check

  code = tcsPutObjectFromFile2(file, object, withcp);
  GTEST_ASSERT_EQ(code, 0);
  code = tcsGetObjectsByPrefix(prefix, path);
  GTEST_ASSERT_EQ(code, 0);
  code = tcsDeleteObjects(object_name, nobject);
  GTEST_ASSERT_EQ(code, 0);
  code = tcsGetObjectToFile(object_name, fileName);
  GTEST_ASSERT_EQ(code, 0);

  // GTEST_ASSERT_NE(pEnv, nullptr);
  */

  tcsUninit();
}

/*
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
    code = walAppendLog(pWal, i, i + 1, syncMeta, (void*)ranStr, ranStrLen);
    ASSERT_EQ(code, 0);
    ASSERT_EQ(pWal->vers.lastVer, i);
    code = walAppendLog(pWal, i + 2, i, syncMeta, (void*)ranStr, ranStrLen);
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
    code = walAppendLog(pWal, i, i + 1, syncMeta, (void*)ranStr, ranStrLen);
    ASSERT_EQ(code, 0);
    ASSERT_EQ(pWal->vers.lastVer, i);
    code = walAppendLog(pWal, i + 2, i, syncMeta, (void*)ranStr, ranStrLen);
    ASSERT_EQ(code, TSDB_CODE_WAL_INVALID_VER);
    ASSERT_EQ(pWal->vers.lastVer, i);
  }
  code = walSaveMeta(pWal);
  ASSERT_EQ(code, 0);
}
TEST_F(WalCleanEnv, rollback) {
  int code;
  for (int i = 0; i < 10; i++) {
    code = walAppendLog(pWal, i, i + 1, syncMeta, (void*)ranStr, ranStrLen);
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
    code = walAppendLog(pWal, i, i + 1, syncMeta, (void*)ranStr, ranStrLen);
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
  ASSERT_NE(code, 0);
  ASSERT_EQ(pWal->vers.lastVer, 5);
  code = walAppendLog(pWal, 6, 6, syncMeta, (void*)ranStr, ranStrLen);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(pWal->vers.lastVer, 6);
  code = walSaveMeta(pWal);
  ASSERT_EQ(code, 0);
}
TEST_F(WalCleanDeleteEnv, roll) {
  int code;
  int i;
  for (i = 0; i < 100; i++) {
    code = walAppendLog(pWal, i, 0, syncMeta, (void*)ranStr, ranStrLen);
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
  code = walAppendLog(pWal, 5, 0, syncMeta, (void*)ranStr, ranStrLen);
  ASSERT_NE(code, 0);
  for (; i < 200; i++) {
    code = walAppendLog(pWal, i, 0, syncMeta, (void*)ranStr, ranStrLen);
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
    code = walAppendLog(pWal, i, 0, syncMeta, newStr, len);
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
    code = walAppendLog(pWal, i, 0, syncMeta, newStr, len);
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
    code = walAppendLog(pWal, i, 0, syncMeta, newStr, len);
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
*/
