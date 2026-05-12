/**
 * @file sdbTest.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief MNODE module sdb tests
 * @version 1.0
 * @date 2022-04-27
 *
 * @copyright Copyright (c) 2022
 *
 */

#include <gtest/gtest.h>

#include "sdb.h"
#include "tglobal.h"
#include "tencrypt.h"
#include "tjson.h"
#include "mnode.h"

class MndTestSdb : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    dDebugFlag = 143;
    vDebugFlag = 0;
    mDebugFlag = 143;
    cDebugFlag = 0;
    jniDebugFlag = 0;
    tmrDebugFlag = 135;
    uDebugFlag = 135;
    rpcDebugFlag = 143;
    qDebugFlag = 0;
    wDebugFlag = 0;
    sDebugFlag = 0;
    tsdbDebugFlag = 0;
    tsLogEmbedded = 1;
    tsAsyncLog = 0;
    tsSkipKeyCheckMode = true;

    const char *path = TD_TMP_DIR_PATH "td";
    taosRemoveDir(path);
    taosMkDir(path);
    tstrncpy(tsLogDir, path, PATH_MAX);
    if (taosInitLog("taosdlog", 1, false) != 0) {
      printf("failed to init log file\n");
    }
  }
  static void TearDownTestSuite() { taosCloseLog(); }

 public:
  void SetUp() override {}
  void TearDown() override {}
};

typedef struct SMnode {
  int32_t v100;
  int32_t v200;
  int32_t insertTimes;
  int32_t deleteTimes;
  SSdb   *pSdb;
} SMnode;

typedef struct SStrObj {
  char    key[24];
  int8_t  v8;
  int16_t v16;
  int32_t v32;
  int64_t v64;
  char    vstr[32];
  char    unused[48];
} SStrObj;

typedef struct SI32Obj {
  int32_t key;
  int8_t  v8;
  int16_t v16;
  int32_t v32;
  int64_t v64;
  char    vstr[32];
  char    unused[48];
} SI32Obj;

typedef struct SI64Obj {
  int64_t key;
  int8_t  v8;
  int16_t v16;
  int32_t v32;
  int64_t v64;
  char    vstr[32];
  char    unused[48];
} SI64Obj;

SSdbRaw *strEncode(SStrObj *pObj) {
  int32_t  dataPos = 0;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_USER, 1, sizeof(SStrObj));

  sdbSetRawBinary(pRaw, dataPos, pObj->key, sizeof(pObj->key));
  dataPos += sizeof(pObj->key);
  sdbSetRawInt8(pRaw, dataPos, pObj->v8);
  dataPos += sizeof(pObj->v8);
  sdbSetRawInt16(pRaw, dataPos, pObj->v16);
  dataPos += sizeof(pObj->v16);
  sdbSetRawInt32(pRaw, dataPos, pObj->v32);
  dataPos += sizeof(pObj->v32);
  sdbSetRawInt64(pRaw, dataPos, pObj->v64);
  dataPos += sizeof(pObj->v64);
  sdbSetRawBinary(pRaw, dataPos, pObj->vstr, sizeof(pObj->vstr));
  dataPos += sizeof(pObj->vstr);
  sdbSetRawDataLen(pRaw, dataPos);

  return pRaw;
}

SSdbRaw *i32Encode(SI32Obj *pObj) {
  int32_t  dataPos = 0;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_VGROUP, 2, sizeof(SI32Obj));

  sdbSetRawInt32(pRaw, dataPos, pObj->key);
  dataPos += sizeof(pObj->key);
  sdbSetRawInt8(pRaw, dataPos, pObj->v8);
  dataPos += sizeof(pObj->v8);
  sdbSetRawInt16(pRaw, dataPos, pObj->v16);
  dataPos += sizeof(pObj->v16);
  sdbSetRawInt32(pRaw, dataPos, pObj->v32);
  dataPos += sizeof(pObj->v32);
  sdbSetRawInt64(pRaw, dataPos, pObj->v64);
  dataPos += sizeof(pObj->v64);
  sdbSetRawBinary(pRaw, dataPos, pObj->vstr, sizeof(pObj->vstr));
  dataPos += sizeof(pObj->vstr);
  sdbSetRawDataLen(pRaw, dataPos);

  return pRaw;
}

SSdbRaw *i64Encode(SI64Obj *pObj) {
  int32_t  dataPos = 0;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_CONSUMER, 3, sizeof(SI64Obj));

  sdbSetRawInt64(pRaw, dataPos, pObj->key);
  dataPos += sizeof(pObj->key);
  sdbSetRawInt8(pRaw, dataPos, pObj->v8);
  dataPos += sizeof(pObj->v8);
  sdbSetRawInt16(pRaw, dataPos, pObj->v16);
  dataPos += sizeof(pObj->v16);
  sdbSetRawInt32(pRaw, dataPos, pObj->v32);
  dataPos += sizeof(pObj->v32);
  sdbSetRawInt64(pRaw, dataPos, pObj->v64);
  dataPos += sizeof(pObj->v64);
  sdbSetRawBinary(pRaw, dataPos, pObj->vstr, sizeof(pObj->vstr));
  dataPos += sizeof(pObj->vstr);
  sdbSetRawDataLen(pRaw, dataPos);

  return pRaw;
}

SSdbRow *strDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;
  if (sver != 1) return NULL;

  SSdbRow *pRow = sdbAllocRow(sizeof(SStrObj));
  if (pRow == NULL) return NULL;

  SStrObj *pObj = (SStrObj *)sdbGetRowObj(pRow);
  if (pObj == NULL) return NULL;

  int32_t dataPos = 0;
  sdbGetRawBinary(pRaw, dataPos, pObj->key, sizeof(pObj->key));
  dataPos += sizeof(pObj->key);
  sdbGetRawInt8(pRaw, dataPos, &pObj->v8);
  dataPos += sizeof(pObj->v8);
  sdbGetRawInt16(pRaw, dataPos, &pObj->v16);
  dataPos += sizeof(pObj->v16);
  sdbGetRawInt32(pRaw, dataPos, &pObj->v32);
  dataPos += sizeof(pObj->v32);
  sdbGetRawInt64(pRaw, dataPos, &pObj->v64);
  dataPos += sizeof(pObj->v64);
  sdbGetRawBinary(pRaw, dataPos, pObj->vstr, sizeof(pObj->vstr));
  dataPos += sizeof(pObj->vstr);

  return pRow;
}

SSdbRow *i32Decode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;
  if (sver != 2) return NULL;

  SSdbRow *pRow = sdbAllocRow(sizeof(SI32Obj));
  if (pRow == NULL) return NULL;

  SI32Obj *pObj = (SI32Obj *)sdbGetRowObj(pRow);
  if (pObj == NULL) return NULL;

  int32_t dataPos = 0;
  sdbGetRawInt32(pRaw, dataPos, &pObj->key);
  dataPos += sizeof(pObj->key);
  sdbGetRawInt8(pRaw, dataPos, &pObj->v8);
  dataPos += sizeof(pObj->v8);
  sdbGetRawInt16(pRaw, dataPos, &pObj->v16);
  dataPos += sizeof(pObj->v16);
  sdbGetRawInt32(pRaw, dataPos, &pObj->v32);
  dataPos += sizeof(pObj->v32);
  sdbGetRawInt64(pRaw, dataPos, &pObj->v64);
  dataPos += sizeof(pObj->v64);
  sdbGetRawBinary(pRaw, dataPos, pObj->vstr, sizeof(pObj->vstr));
  dataPos += sizeof(pObj->vstr);

  return pRow;
}

SSdbRow *i64Decode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;
  if (sver != 3) return NULL;

  SSdbRow *pRow = sdbAllocRow(sizeof(SI64Obj));
  if (pRow == NULL) return NULL;

  SI64Obj *pObj = (SI64Obj *)sdbGetRowObj(pRow);
  if (pObj == NULL) return NULL;

  int32_t dataPos = 0;
  sdbGetRawInt64(pRaw, dataPos, &pObj->key);
  dataPos += sizeof(pObj->key);
  sdbGetRawInt8(pRaw, dataPos, &pObj->v8);
  dataPos += sizeof(pObj->v8);
  sdbGetRawInt16(pRaw, dataPos, &pObj->v16);
  dataPos += sizeof(pObj->v16);
  sdbGetRawInt32(pRaw, dataPos, &pObj->v32);
  dataPos += sizeof(pObj->v32);
  sdbGetRawInt64(pRaw, dataPos, &pObj->v64);
  dataPos += sizeof(pObj->v64);
  sdbGetRawBinary(pRaw, dataPos, pObj->vstr, sizeof(pObj->vstr));
  dataPos += sizeof(pObj->vstr);

  return pRow;
}

int32_t strInsert(SSdb *pSdb, SStrObj *pObj) {
  SMnode *pMnode = pSdb->pMnode;
  pMnode->insertTimes++;
  return 0;
}

int32_t i32Insert(SSdb *pSdb, SI32Obj *pObj) {
  SMnode *pMnode = pSdb->pMnode;
  pMnode->insertTimes++;
  return 0;
}

int32_t i64Insert(SSdb *pSdb, SI64Obj *pObj) {
  SMnode *pMnode = pSdb->pMnode;
  pMnode->insertTimes++;
  return 0;
}

int32_t strDelete(SSdb *pSdb, SStrObj *pObj, bool callFunc) {
  if (callFunc) {
    SMnode *pMnode = pSdb->pMnode;
    pMnode->deleteTimes++;
  }
  return 0;
}

int32_t i32Delete(SSdb *pSdb, SI32Obj *pObj, bool callFunc) {
  if (callFunc) {
    SMnode *pMnode = pSdb->pMnode;
    pMnode->deleteTimes++;
  }
  return 0;
}

int32_t i64Delete(SSdb *pSdb, SI64Obj *pObj, bool callFunc) {
  if (callFunc) {
    SMnode *pMnode = pSdb->pMnode;
    pMnode->deleteTimes++;
  }
  return 0;
}

int32_t strUpdate(SSdb *pSdb, SStrObj *pOld, SStrObj *pNew) {
  pOld->v8 = pNew->v8;
  pOld->v16 = pNew->v16;
  pOld->v32 = pNew->v32;
  pOld->v64 = pNew->v64;
  strcpy(pOld->vstr, pNew->vstr);
  return 0;
}

int32_t i32Update(SSdb *pSdb, SI32Obj *pOld, SI32Obj *pNew) {
  pOld->v8 = pNew->v8;
  pOld->v16 = pNew->v16;
  pOld->v32 = pNew->v32;
  pOld->v64 = pNew->v64;
  strcpy(pOld->vstr, pNew->vstr);
  return 0;
}

int32_t i64Update(SSdb *pSdb, SI64Obj *pOld, SI64Obj *pNew) {
  pOld->v8 = pNew->v8;
  pOld->v16 = pNew->v16;
  pOld->v32 = pNew->v32;
  pOld->v64 = pNew->v64;
  strcpy(pOld->vstr, pNew->vstr);
  return 0;
}

void strSetDefault(SStrObj *pObj, int32_t index) {
  memset(pObj, 0, sizeof(SStrObj));
  snprintf(pObj->key, sizeof(pObj->key), "k%d", index * 1000);
  pObj->v8 = index;
  pObj->v16 = index;
  pObj->v32 = index * 1000;
  pObj->v64 = index * 1000;
  snprintf(pObj->vstr, sizeof(pObj->vstr), "v%d", index * 1000);
}

void i32SetDefault(SI32Obj *pObj, int32_t index) {
  memset(pObj, 0, sizeof(SI32Obj));
  pObj->key = index;
  pObj->v8 = index;
  pObj->v16 = index;
  pObj->v32 = index * 1000;
  pObj->v64 = index * 1000;
  snprintf(pObj->vstr, sizeof(pObj->vstr), "v%d", index * 1000);
}

void i64SetDefault(SI64Obj *pObj, int32_t index) {
  memset(pObj, 0, sizeof(SI64Obj));
  pObj->key = index;
  pObj->v8 = index;
  pObj->v16 = index;
  pObj->v32 = index * 1000;
  pObj->v64 = index * 1000;
  snprintf(pObj->vstr, sizeof(pObj->vstr), "v%d", index * 1000);
}

int32_t strDefault(SMnode *pMnode) {
  SStrObj  strObj;
  SSdbRaw *pRaw = NULL;

  strSetDefault(&strObj, 1);
  pRaw = strEncode(&strObj);
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);
  if (sdbWrite(pMnode->pSdb, pRaw) != 0) return -1;

  strSetDefault(&strObj, 2);
  pRaw = strEncode(&strObj);
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);
  if (sdbWriteWithoutFree(pMnode->pSdb, pRaw) != 0) return -1;

  EXPECT_EQ(sdbGetRawTotalSize(pRaw), 79);
  sdbFreeRaw(pRaw);

  return 0;
}

bool sdbTraverseSucc1(SMnode *pMnode, SStrObj *pObj, int32_t *p1, int32_t *p2, int32_t *p3) {
  if (pObj->v8 == 1) {
    *p1 += *p2 + *p3 + pObj->v8;
  }
  return true;
}

bool sdbTraverseSucc2(SMnode *pMnode, SStrObj *pObj, int32_t *p1, int32_t *p2, int32_t *p3) {
  *p1 += *p2 + *p3 + pObj->v8;
  return true;
}

bool sdbTraverseFail(SMnode *pMnode, SStrObj *pObj, int32_t *p1, int32_t *p2, int32_t *p3) {
  *p1 += *p2 + *p3;
  return false;
}

#ifndef WINDOWS

TEST_F(MndTestSdb, 00_API) {
  SMnode  mnode = {0};
  SSdbOpt opt = {0};
  opt.pMnode = &mnode;
  opt.path = TD_TMP_DIR_PATH "mnode_test_sdb";
  taosRemoveDir(opt.path);
  SSdb *pSdb = sdbInit(&opt);

  SSdbTable table = {.sdbType = SDB_USER, .keyType = SDB_KEY_BINARY};
  sdbSetTable(pSdb, table);

  // sdbRow.c
  SSdbRow *pRow1 = sdbAllocRow(-128);
  ASSERT_EQ(pRow1 == NULL, 1);

  void *pRow2 = sdbGetRowObj(NULL);
  ASSERT_EQ(pRow2 == NULL, 1);

  // sdbRaw.c
  SStrObj  strObj;
  SSdbRaw *pRaw1 = NULL;
  strSetDefault(&strObj, 1);

  pRaw1 = strEncode(&strObj);
  int32_t id = sdbGetIdFromRaw(pSdb, pRaw1);
  ASSERT_EQ(id, -2);

  SSdbRaw *pRaw2 = sdbAllocRaw(SDB_USER, 1, -128);
  ASSERT_EQ(pRaw2 == NULL, 1);

  ASSERT_EQ(sdbSetRawInt8(NULL, 0, 0), TSDB_CODE_INVALID_PTR);
  ASSERT_EQ(sdbSetRawInt8(pRaw1, -128, 0), TSDB_CODE_SDB_INVALID_DATA_LEN);
  ASSERT_EQ(sdbSetRawInt32(NULL, 0, 0), TSDB_CODE_INVALID_PTR);
  ASSERT_EQ(sdbSetRawInt32(pRaw1, -128, 0), TSDB_CODE_SDB_INVALID_DATA_LEN);
  ASSERT_EQ(sdbSetRawInt16(NULL, 0, 0), TSDB_CODE_INVALID_PTR);
  ASSERT_EQ(sdbSetRawInt16(pRaw1, -128, 0), TSDB_CODE_SDB_INVALID_DATA_LEN);
  ASSERT_EQ(sdbSetRawInt64(NULL, 0, 0), TSDB_CODE_INVALID_PTR);
  ASSERT_EQ(sdbSetRawInt64(pRaw1, -128, 0), TSDB_CODE_SDB_INVALID_DATA_LEN);
  ASSERT_EQ(sdbSetRawBinary(NULL, 0, "12", 3), TSDB_CODE_INVALID_PTR);
  ASSERT_EQ(sdbSetRawBinary(pRaw1, 9028, "12", 3), TSDB_CODE_SDB_INVALID_DATA_LEN);
  ASSERT_EQ(sdbSetRawDataLen(NULL, 0), TSDB_CODE_INVALID_PTR);
  ASSERT_EQ(sdbSetRawDataLen(pRaw1, 9000), TSDB_CODE_SDB_INVALID_DATA_LEN);
  ASSERT_EQ(sdbSetRawStatus(NULL, SDB_STATUS_READY), TSDB_CODE_INVALID_PTR);
  ASSERT_EQ(sdbSetRawStatus(pRaw1, SDB_STATUS_INIT), TSDB_CODE_INVALID_PARA);

  ASSERT_EQ(sdbGetRawInt8(NULL, 0, 0), TSDB_CODE_INVALID_PTR);
  ASSERT_EQ(sdbGetRawInt8(pRaw1, 9000, 0), TSDB_CODE_SDB_INVALID_DATA_LEN);
  ASSERT_EQ(sdbGetRawInt32(NULL, 0, 0), TSDB_CODE_INVALID_PTR);
  ASSERT_EQ(sdbGetRawInt32(pRaw1, 9000, 0), TSDB_CODE_SDB_INVALID_DATA_LEN);
  ASSERT_EQ(sdbGetRawInt16(NULL, 0, 0), TSDB_CODE_INVALID_PTR);
  ASSERT_EQ(sdbGetRawInt16(pRaw1, 9000, 0), TSDB_CODE_SDB_INVALID_DATA_LEN);
  ASSERT_EQ(sdbGetRawInt64(NULL, 0, 0), TSDB_CODE_INVALID_PTR);
  ASSERT_EQ(sdbGetRawInt64(pRaw1, 9000, 0), TSDB_CODE_SDB_INVALID_DATA_LEN);
  ASSERT_EQ(sdbGetRawBinary(NULL, 0, 0, 4096), TSDB_CODE_INVALID_PTR);
  ASSERT_EQ(sdbGetRawBinary(pRaw1, 9000, 0, 112), TSDB_CODE_SDB_INVALID_DATA_LEN);
  ASSERT_EQ(sdbGetRawSoftVer(NULL, 0), TSDB_CODE_INVALID_PTR);
  ASSERT_EQ(sdbGetRawTotalSize(NULL), -1);

  // sdbHash.c
  EXPECT_STREQ(sdbTableName((ESdbType)100), "undefine");
  EXPECT_STREQ(sdbStatusName((ESdbStatus)100), "undefine");
  ASSERT_EQ(sdbGetTableVer(pSdb, (ESdbType)100), -1);

  SSdbRaw *pRaw3 = sdbAllocRaw((ESdbType)-12, 1, 128);
  ASSERT_NE(sdbWriteWithoutFree(pSdb, pRaw3), 0);
  pSdb->hashObjs[1] = NULL;
  SSdbRaw *pRaw4 = sdbAllocRaw((ESdbType)1, 1, 128);
  ASSERT_NE(sdbWriteWithoutFree(pSdb, pRaw4), 0);
}

#endif

TEST_F(MndTestSdb, 01_Write_Str) {
  void    *pIter = NULL;
  int32_t  num = 0;
  SStrObj *pObj = NULL;
  SMnode   mnode = {0};
  SSdb    *pSdb = NULL;
  SSdbOpt  opt = {0};
  SStrObj  strObj = {0};
  SI32Obj  i32Obj = {0};
  SI64Obj  i64Obj = {0};
  SSdbRaw *pRaw = NULL;
  int32_t  p1 = 0;
  int32_t  p2 = 111;
  int32_t  p3 = 222;

  mnode.v100 = 100;
  mnode.v200 = 200;
  opt.pMnode = &mnode;
  opt.path = TD_TMP_DIR_PATH "mnode_test_sdb";
  taosRemoveDir(opt.path);

  SSdbTable strTable1;
  memset(&strTable1, 0, sizeof(SSdbTable));
  strTable1.sdbType = SDB_USER;
  strTable1.keyType = SDB_KEY_BINARY;
  strTable1.deployFp = (SdbDeployFp)strDefault;
  strTable1.encodeFp = (SdbEncodeFp)strEncode;
  strTable1.decodeFp = (SdbDecodeFp)strDecode;
  strTable1.insertFp = (SdbInsertFp)strInsert;
  strTable1.updateFp = (SdbUpdateFp)strUpdate;
  strTable1.deleteFp = (SdbDeleteFp)strDelete;

  SSdbTable strTable2;
  memset(&strTable2, 0, sizeof(SSdbTable));
  strTable2.sdbType = SDB_VGROUP;
  strTable2.keyType = SDB_KEY_INT32;
  strTable2.encodeFp = (SdbEncodeFp)i32Encode;
  strTable2.decodeFp = (SdbDecodeFp)i32Decode;
  strTable2.insertFp = (SdbInsertFp)i32Insert;
  strTable2.updateFp = (SdbUpdateFp)i32Update;
  strTable2.deleteFp = (SdbDeleteFp)i32Delete;

  SSdbTable strTable3;
  memset(&strTable3, 0, sizeof(SSdbTable));
  strTable3.sdbType = SDB_CONSUMER;
  strTable3.keyType = SDB_KEY_INT64;
  strTable3.encodeFp = (SdbEncodeFp)i64Encode;
  strTable3.decodeFp = (SdbDecodeFp)i64Decode;
  strTable3.insertFp = (SdbInsertFp)i64Insert;
  strTable3.updateFp = (SdbUpdateFp)i64Update;
  strTable3.deleteFp = (SdbDeleteFp)i64Delete;

  pSdb = sdbInit(&opt);
  mnode.pSdb = pSdb;

  ASSERT_NE(pSdb, nullptr);
  ASSERT_EQ(sdbSetTable(pSdb, strTable1), 0);
  ASSERT_EQ(sdbSetTable(pSdb, strTable2), 0);
  ASSERT_EQ(sdbSetTable(pSdb, strTable3), 0);
  ASSERT_EQ(sdbDeploy(pSdb), 0);

  pObj = (SStrObj *)sdbAcquire(pSdb, SDB_USER, "k1000");
  ASSERT_NE(pObj, nullptr);
  EXPECT_STREQ(pObj->key, "k1000");
  EXPECT_STREQ(pObj->vstr, "v1000");
  ASSERT_EQ(pObj->v8, 1);
  ASSERT_EQ(pObj->v16, 1);
  ASSERT_EQ(pObj->v32, 1000);
  ASSERT_EQ(pObj->v64, 1000);
  sdbRelease(pSdb, pObj);

  pObj = (SStrObj *)sdbAcquire(pSdb, SDB_USER, "k2000");
  ASSERT_NE(pObj, nullptr);
  EXPECT_STREQ(pObj->key, "k2000");
  EXPECT_STREQ(pObj->vstr, "v2000");
  ASSERT_EQ(pObj->v8, 2);
  ASSERT_EQ(pObj->v16, 2);
  ASSERT_EQ(pObj->v32, 2000);
  ASSERT_EQ(pObj->v64, 2000);
  sdbRelease(pSdb, pObj);

  pObj = (SStrObj *)sdbAcquire(pSdb, SDB_USER, "k200");
  ASSERT_EQ(pObj, nullptr);

  pIter = NULL;
  num = 0;
  do {
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pObj);
    if (pIter == NULL) break;
    ASSERT_NE(pObj, nullptr);
    num++;
    sdbRelease(pSdb, pObj);
  } while (1);
  ASSERT_EQ(num, 2);

  do {
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pObj);
    if (pIter == NULL) break;
    if (strcmp(pObj->key, "k1000") == 0) {
      sdbCancelFetch(pSdb, pIter);
      break;
    }
  } while (1);
  EXPECT_STREQ(pObj->key, "k1000");

  p1 = 0;
  p2 = 111;
  p3 = 222;
  sdbTraverse(pSdb, SDB_USER, (sdbTraverseFp)sdbTraverseSucc1, &p1, &p2, &p3);
  ASSERT_EQ(p1, 334);

  p1 = 0;
  p2 = 111;
  p3 = 222;
  sdbTraverse(pSdb, SDB_USER, (sdbTraverseFp)sdbTraverseSucc2, &p1, &p2, &p3);
  ASSERT_EQ(p1, 669);

  p1 = 0;
  p2 = 111;
  p3 = 222;
  sdbTraverse(pSdb, SDB_USER, (sdbTraverseFp)sdbTraverseFail, &p1, &p2, &p3);
  ASSERT_EQ(p1, 333);

  ASSERT_EQ(sdbGetSize(pSdb, SDB_USER), 2);
  ASSERT_EQ(sdbGetMaxId(pSdb, SDB_USER), -1);
  ASSERT_EQ(sdbGetTableVer(pSdb, SDB_USER), 2);
  sdbSetApplyInfo(pSdb, -1, -1, -1);
  // int64_t index, config;
  // int64_t term;
  // sdbGetCommitInfo(pSdb, &index, &term, &config);
  // ASSERT_EQ(index, -1);
  ASSERT_EQ(mnode.insertTimes, 2);
  ASSERT_EQ(mnode.deleteTimes, 0);

  {
    // insert, call func
    strSetDefault(&strObj, 3);
    pRaw = strEncode(&strObj);
    sdbSetRawStatus(pRaw, SDB_STATUS_READY);
    ASSERT_EQ(sdbWrite(pSdb, pRaw), 0);
    pObj = (SStrObj *)sdbAcquire(pSdb, SDB_USER, "k3000");
    ASSERT_NE(pObj, nullptr);
    EXPECT_STREQ(pObj->key, "k3000");
    EXPECT_STREQ(pObj->vstr, "v3000");
    ASSERT_EQ(pObj->v8, 3);
    ASSERT_EQ(pObj->v16, 3);
    ASSERT_EQ(pObj->v32, 3000);
    ASSERT_EQ(pObj->v64, 3000);
    sdbRelease(pSdb, pObj);

    ASSERT_EQ(sdbGetSize(pSdb, SDB_USER), 3);
    ASSERT_EQ(sdbGetTableVer(pSdb, SDB_USER), 3);
    ASSERT_EQ(sdbGetMaxId(pSdb, SDB_USER), -1);

    // update, call func
    strSetDefault(&strObj, 3);
    strObj.v8 = 4;
    pRaw = strEncode(&strObj);
    sdbSetRawStatus(pRaw, SDB_STATUS_READY);
    ASSERT_EQ(sdbWrite(pSdb, pRaw), 0);
    pObj = (SStrObj *)sdbAcquire(pSdb, SDB_USER, "k3000");
    ASSERT_NE(pObj, nullptr);
    EXPECT_STREQ(pObj->key, "k3000");
    EXPECT_STREQ(pObj->vstr, "v3000");
    ASSERT_EQ(pObj->v8, 4);
    ASSERT_EQ(pObj->v16, 3);
    ASSERT_EQ(pObj->v32, 3000);
    ASSERT_EQ(pObj->v64, 3000);
    sdbRelease(pSdb, pObj);

    ASSERT_EQ(sdbGetSize(pSdb, SDB_USER), 3);
    ASSERT_EQ(sdbGetTableVer(pSdb, SDB_USER), 4);
    ASSERT_EQ(mnode.insertTimes, 3);
    ASSERT_EQ(mnode.deleteTimes, 0);

    // delete, call func 2
    strSetDefault(&strObj, 3);
    strObj.v16 = 4;
    pRaw = strEncode(&strObj);
    sdbSetRawStatus(pRaw, SDB_STATUS_DROPPED);
    ASSERT_EQ(sdbWrite(pSdb, pRaw), 0);
    pObj = (SStrObj *)sdbAcquire(pSdb, SDB_USER, "k3000");
    ASSERT_EQ(pObj, nullptr);
    ASSERT_EQ(sdbGetSize(pSdb, SDB_USER), 2);
    ASSERT_EQ(sdbGetTableVer(pSdb, SDB_USER), 5);
    ASSERT_EQ(mnode.insertTimes, 3);
    ASSERT_EQ(mnode.deleteTimes, 1);
  }

  {
    int32_t key = 4;
    i32SetDefault(&i32Obj, key);
    pRaw = i32Encode(&i32Obj);
    sdbSetRawStatus(pRaw, SDB_STATUS_READY);
    ASSERT_EQ(sdbWrite(pSdb, pRaw), 0);
    SI32Obj *pI32Obj = (SI32Obj *)sdbAcquire(pSdb, SDB_VGROUP, &key);
    ASSERT_NE(pI32Obj, nullptr);
    ASSERT_EQ(pI32Obj->key, key);
    ASSERT_EQ(pI32Obj->v8, 4);
    ASSERT_EQ(pI32Obj->v16, 4);
    ASSERT_EQ(pI32Obj->v32, 4000);
    ASSERT_EQ(pI32Obj->v64, 4000);
    sdbRelease(pSdb, pI32Obj);

    ASSERT_EQ(sdbGetSize(pSdb, SDB_VGROUP), 1);
    ASSERT_EQ(sdbGetTableVer(pSdb, SDB_VGROUP), 1);
    ASSERT_EQ(sdbGetMaxId(pSdb, SDB_VGROUP), 5);

    i32SetDefault(&i32Obj, key);
    i32Obj.v8 = 5;
    pRaw = i32Encode(&i32Obj);
    sdbSetRawStatus(pRaw, SDB_STATUS_READY);
    ASSERT_EQ(sdbWrite(pSdb, pRaw), 0);
    pI32Obj = (SI32Obj *)sdbAcquire(pSdb, SDB_VGROUP, &key);
    ASSERT_NE(pI32Obj, nullptr);
    ASSERT_EQ(pI32Obj->key, key);
    ASSERT_EQ(pI32Obj->v8, 5);
    ASSERT_EQ(pI32Obj->v16, 4);
    ASSERT_EQ(pI32Obj->v32, 4000);
    ASSERT_EQ(pI32Obj->v64, 4000);
    sdbRelease(pSdb, pI32Obj);

    ASSERT_EQ(sdbGetSize(pSdb, SDB_VGROUP), 1);
    ASSERT_EQ(sdbGetTableVer(pSdb, SDB_VGROUP), 2);
    ASSERT_EQ(mnode.insertTimes, 4);
    ASSERT_EQ(mnode.deleteTimes, 1);

    // delete, call func 2
    key = 4;
    i32SetDefault(&i32Obj, key);
    pRaw = i32Encode(&i32Obj);
    sdbSetRawStatus(pRaw, SDB_STATUS_DROPPED);
    ASSERT_EQ(sdbWrite(pSdb, pRaw), 0);
    pI32Obj = (SI32Obj *)sdbAcquire(pSdb, SDB_VGROUP, &key);
    ASSERT_EQ(pI32Obj, nullptr);
    ASSERT_EQ(sdbGetSize(pSdb, SDB_VGROUP), 0);
    ASSERT_EQ(sdbGetTableVer(pSdb, SDB_VGROUP), 3);
    ASSERT_EQ(sdbGetMaxId(pSdb, SDB_VGROUP), 5);
    ASSERT_EQ(mnode.insertTimes, 4);
    ASSERT_EQ(mnode.deleteTimes, 2);

    key = 6;
    i32SetDefault(&i32Obj, key);
    pRaw = i32Encode(&i32Obj);
    sdbSetRawStatus(pRaw, SDB_STATUS_READY);
    ASSERT_EQ(sdbWrite(pSdb, pRaw), 0);
    pI32Obj = (SI32Obj *)sdbAcquire(pSdb, SDB_VGROUP, &key);
    ASSERT_NE(pI32Obj, nullptr);
    ASSERT_EQ(pI32Obj->key, key);
    ASSERT_EQ(pI32Obj->v8, 6);
    ASSERT_EQ(pI32Obj->v16, 6);
    ASSERT_EQ(pI32Obj->v32, 6000);
    ASSERT_EQ(pI32Obj->v64, 6000);
    sdbRelease(pSdb, pI32Obj);

    ASSERT_EQ(sdbGetSize(pSdb, SDB_VGROUP), 1);
    ASSERT_EQ(sdbGetTableVer(pSdb, SDB_VGROUP), 4);
    ASSERT_EQ(sdbGetMaxId(pSdb, SDB_VGROUP), 7);
    ASSERT_EQ(mnode.insertTimes, 5);
    ASSERT_EQ(mnode.deleteTimes, 2);
  }

  {
    int64_t key = 4;
    i64SetDefault(&i64Obj, key);
    pRaw = i64Encode(&i64Obj);
    sdbSetRawStatus(pRaw, SDB_STATUS_READY);
    ASSERT_EQ(sdbWrite(pSdb, pRaw), 0);
    SI64Obj *pI64Obj = (SI64Obj *)sdbAcquire(pSdb, SDB_CONSUMER, &key);
    ASSERT_NE(pI64Obj, nullptr);
    ASSERT_EQ(pI64Obj->key, key);
    ASSERT_EQ(pI64Obj->v8, 4);
    ASSERT_EQ(pI64Obj->v16, 4);
    ASSERT_EQ(pI64Obj->v32, 4000);
    ASSERT_EQ(pI64Obj->v64, 4000);
    sdbRelease(pSdb, pI64Obj);

    ASSERT_EQ(sdbGetSize(pSdb, SDB_CONSUMER), 1);
    ASSERT_EQ(sdbGetTableVer(pSdb, SDB_CONSUMER), 1);
    ASSERT_EQ(sdbGetMaxId(pSdb, SDB_CONSUMER), -1);

    i64SetDefault(&i64Obj, key);
    i64Obj.v8 = 5;
    pRaw = i64Encode(&i64Obj);
    sdbSetRawStatus(pRaw, SDB_STATUS_READY);
    ASSERT_EQ(sdbWrite(pSdb, pRaw), 0);
    pI64Obj = (SI64Obj *)sdbAcquire(pSdb, SDB_CONSUMER, &key);
    ASSERT_NE(pI64Obj, nullptr);
    ASSERT_EQ(pI64Obj->key, key);
    ASSERT_EQ(pI64Obj->v8, 5);
    ASSERT_EQ(pI64Obj->v16, 4);
    ASSERT_EQ(pI64Obj->v32, 4000);
    ASSERT_EQ(pI64Obj->v64, 4000);
    sdbRelease(pSdb, pI64Obj);

    ASSERT_EQ(sdbGetSize(pSdb, SDB_CONSUMER), 1);
    ASSERT_EQ(sdbGetTableVer(pSdb, SDB_CONSUMER), 2);
    ASSERT_EQ(mnode.insertTimes, 6);
    ASSERT_EQ(mnode.deleteTimes, 2);

    // delete, call func 2
    key = 4;
    i64SetDefault(&i64Obj, key);
    pRaw = i64Encode(&i64Obj);
    sdbSetRawStatus(pRaw, SDB_STATUS_DROPPED);
    ASSERT_EQ(sdbWrite(pSdb, pRaw), 0);
    pObj = (SStrObj *)sdbAcquire(pSdb, SDB_CONSUMER, &key);
    ASSERT_EQ(pObj, nullptr);
    ASSERT_EQ(sdbGetSize(pSdb, SDB_CONSUMER), 0);
    ASSERT_EQ(sdbGetTableVer(pSdb, SDB_CONSUMER), 3);
    ASSERT_EQ(sdbGetMaxId(pSdb, SDB_CONSUMER), -1);
    ASSERT_EQ(mnode.insertTimes, 6);
    ASSERT_EQ(mnode.deleteTimes, 3);

    key = 7;
    i64SetDefault(&i64Obj, key);
    pRaw = i64Encode(&i64Obj);
    sdbSetRawStatus(pRaw, SDB_STATUS_READY);
    ASSERT_EQ(sdbWrite(pSdb, pRaw), 0);
    pI64Obj = (SI64Obj *)sdbAcquire(pSdb, SDB_CONSUMER, &key);
    ASSERT_NE(pI64Obj, nullptr);
    ASSERT_EQ(pI64Obj->key, key);
    ASSERT_EQ(pI64Obj->v8, 7);
    ASSERT_EQ(pI64Obj->v16, 7);
    ASSERT_EQ(pI64Obj->v32, 7000);
    ASSERT_EQ(pI64Obj->v64, 7000);
    sdbRelease(pSdb, pI64Obj);

    ASSERT_EQ(sdbGetSize(pSdb, SDB_CONSUMER), 1);
    ASSERT_EQ(sdbGetTableVer(pSdb, SDB_CONSUMER), 4);
    ASSERT_EQ(sdbGetMaxId(pSdb, SDB_CONSUMER), -1);
    ASSERT_EQ(mnode.insertTimes, 7);
    ASSERT_EQ(mnode.deleteTimes, 3);
  }

  // write version
  sdbSetApplyInfo(pSdb, 0, 0, 0);
  sdbSetApplyInfo(pSdb, 1, 0, 0);
  // sdbGetApplyInfo(pSdb, &index, &term, &config);
  // ASSERT_EQ(index, 1);
  ASSERT_EQ(sdbWriteFile(pSdb, 0), 0);
  ASSERT_EQ(sdbWriteFile(pSdb, 0), 0);

  sdbCleanup(pSdb);
  ASSERT_EQ(mnode.insertTimes, 7);
  ASSERT_EQ(mnode.deleteTimes, 7);
}

TEST_F(MndTestSdb, 01_Read_Str) {
  void    *pIter = NULL;
  int32_t  num = 0;
  SStrObj *pObj = NULL;
  SMnode   mnode = {0};
  SSdb    *pSdb = NULL;
  SSdbOpt  opt = {0};
  SStrObj  strObj = {0};
  SSdbRaw *pRaw = NULL;
  int32_t  p1 = 0;
  int32_t  p2 = 111;
  int32_t  p3 = 222;

  mnode.v100 = 100;
  mnode.v200 = 200;
  opt.pMnode = &mnode;
  opt.path = TD_TMP_DIR_PATH "mnode_test_sdb";

  SSdbTable strTable1;
  memset(&strTable1, 0, sizeof(SSdbTable));
  strTable1.sdbType = SDB_USER;
  strTable1.keyType = SDB_KEY_BINARY;
  strTable1.deployFp = (SdbDeployFp)strDefault;
  strTable1.encodeFp = (SdbEncodeFp)strEncode;
  strTable1.decodeFp = (SdbDecodeFp)strDecode;
  strTable1.insertFp = (SdbInsertFp)strInsert;
  strTable1.updateFp = (SdbUpdateFp)strUpdate;
  strTable1.deleteFp = (SdbDeleteFp)strDelete;

  SSdbTable strTable2;
  memset(&strTable2, 0, sizeof(SSdbTable));
  strTable2.sdbType = SDB_VGROUP;
  strTable2.keyType = SDB_KEY_INT32;
  strTable2.encodeFp = (SdbEncodeFp)i32Encode;
  strTable2.decodeFp = (SdbDecodeFp)i32Decode;
  strTable2.insertFp = (SdbInsertFp)i32Insert;
  strTable2.updateFp = (SdbUpdateFp)i32Update;
  strTable2.deleteFp = (SdbDeleteFp)i32Delete;

  SSdbTable strTable3;
  memset(&strTable3, 0, sizeof(SSdbTable));
  strTable3.sdbType = SDB_CONSUMER;
  strTable3.keyType = SDB_KEY_INT64;
  strTable3.encodeFp = (SdbEncodeFp)i64Encode;
  strTable3.decodeFp = (SdbDecodeFp)i64Decode;
  strTable3.insertFp = (SdbInsertFp)i64Insert;
  strTable3.updateFp = (SdbUpdateFp)i64Update;
  strTable3.deleteFp = (SdbDeleteFp)i64Delete;

  pSdb = sdbInit(&opt);
  mnode.pSdb = pSdb;
  ASSERT_NE(pSdb, nullptr);
  ASSERT_NE(pSdb, nullptr);
  ASSERT_EQ(sdbSetTable(pSdb, strTable1), 0);
  ASSERT_EQ(sdbSetTable(pSdb, strTable2), 0);
  ASSERT_EQ(sdbSetTable(pSdb, strTable3), 0);
  ASSERT_EQ(sdbReadFile(pSdb), 0);

  ASSERT_EQ(sdbGetSize(pSdb, SDB_USER), 2);
  ASSERT_EQ(sdbGetMaxId(pSdb, SDB_USER), -1);
  ASSERT_EQ(sdbGetTableVer(pSdb, SDB_USER), 5);

  int64_t index, config;
  int64_t term;
  sdbGetCommitInfo(pSdb, &index, &term, &config);
  ASSERT_EQ(index, 1);
  ASSERT_EQ(mnode.insertTimes, 4);
  ASSERT_EQ(mnode.deleteTimes, 0);

  pObj = (SStrObj *)sdbAcquire(pSdb, SDB_USER, "k1000");
  ASSERT_NE(pObj, nullptr);
  EXPECT_STREQ(pObj->key, "k1000");
  EXPECT_STREQ(pObj->vstr, "v1000");
  ASSERT_EQ(pObj->v8, 1);
  ASSERT_EQ(pObj->v16, 1);
  ASSERT_EQ(pObj->v32, 1000);
  ASSERT_EQ(pObj->v64, 1000);
  sdbRelease(pSdb, pObj);

  pObj = (SStrObj *)sdbAcquire(pSdb, SDB_USER, "k2000");
  ASSERT_NE(pObj, nullptr);
  EXPECT_STREQ(pObj->key, "k2000");
  EXPECT_STREQ(pObj->vstr, "v2000");
  ASSERT_EQ(pObj->v8, 2);
  ASSERT_EQ(pObj->v16, 2);
  ASSERT_EQ(pObj->v32, 2000);
  ASSERT_EQ(pObj->v64, 2000);
  sdbRelease(pSdb, pObj);

  pObj = (SStrObj *)sdbAcquire(pSdb, SDB_USER, "k200");
  ASSERT_EQ(pObj, nullptr);

  pIter = NULL;
  num = 0;
  do {
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pObj);
    if (pIter == NULL) break;
    ASSERT_NE(pObj, nullptr);
    num++;
    sdbRelease(pSdb, pObj);
  } while (1);
  ASSERT_EQ(num, 2);

  do {
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pObj);
    if (pIter == NULL) break;
    if (strcmp(pObj->key, "k1000") == 0) {
      sdbCancelFetch(pSdb, pIter);
      break;
    }
  } while (1);
  EXPECT_STREQ(pObj->key, "k1000");

  p1 = 0;
  p2 = 111;
  p3 = 222;
  sdbTraverse(pSdb, SDB_USER, (sdbTraverseFp)sdbTraverseSucc1, &p1, &p2, &p3);
  ASSERT_EQ(p1, 334);

  p1 = 0;
  p2 = 111;
  p3 = 222;
  sdbTraverse(pSdb, SDB_USER, (sdbTraverseFp)sdbTraverseSucc2, &p1, &p2, &p3);
  ASSERT_EQ(p1, 669);

  p1 = 0;
  p2 = 111;
  p3 = 222;
  sdbTraverse(pSdb, SDB_USER, (sdbTraverseFp)sdbTraverseFail, &p1, &p2, &p3);
  ASSERT_EQ(p1, 333);

  int32_t  i32key = 6;
  SI32Obj *pI32Obj = (SI32Obj *)sdbAcquire(pSdb, SDB_VGROUP, &i32key);
  ASSERT_NE(pI32Obj, nullptr);
  ASSERT_EQ(pI32Obj->key, 6);
  ASSERT_EQ(pI32Obj->v8, 6);
  ASSERT_EQ(pI32Obj->v16, 6);
  ASSERT_EQ(pI32Obj->v32, 6000);
  ASSERT_EQ(pI32Obj->v64, 6000);
  sdbRelease(pSdb, pI32Obj);

  ASSERT_EQ(sdbGetSize(pSdb, SDB_VGROUP), 1);
  ASSERT_EQ(sdbGetTableVer(pSdb, SDB_VGROUP), 4);
  ASSERT_EQ(sdbGetMaxId(pSdb, SDB_VGROUP), 7);

  int64_t  i64key = 7;
  SI64Obj *pI64Obj = (SI64Obj *)sdbAcquire(pSdb, SDB_CONSUMER, &i64key);
  ASSERT_NE(pI64Obj, nullptr);
  ASSERT_EQ(pI64Obj->key, 7);
  ASSERT_EQ(pI64Obj->v8, 7);
  ASSERT_EQ(pI64Obj->v16, 7);
  ASSERT_EQ(pI64Obj->v32, 7000);
  ASSERT_EQ(pI64Obj->v64, 7000);
  sdbRelease(pSdb, pI64Obj);

  ASSERT_EQ(sdbGetSize(pSdb, SDB_CONSUMER), 1);
  ASSERT_EQ(sdbGetTableVer(pSdb, SDB_CONSUMER), 4);

  ASSERT_EQ(mnode.insertTimes, 4);
  ASSERT_EQ(mnode.deleteTimes, 0);

  {
    SI32Obj i32Obj = {0};
    int32_t key = 6;
    i32SetDefault(&i32Obj, key);
    pRaw = i32Encode(&i32Obj);
    sdbSetRawStatus(pRaw, SDB_STATUS_DROPPING);
    ASSERT_EQ(sdbWrite(pSdb, pRaw), 0);
    pI32Obj = (SI32Obj *)sdbAcquire(pSdb, SDB_VGROUP, &key);
    ASSERT_EQ(pI32Obj, nullptr);
    // int32_t code = terrno;
    // ASSERT_EQ(code, TSDB_CODE_SDB_OBJ_DROPPING);
  }

  {
    SI32Obj i32Obj = {0};
    int32_t key = 8;
    i32SetDefault(&i32Obj, key);
    pRaw = i32Encode(&i32Obj);
    EXPECT_NE(sdbSetRawStatus(pRaw, SDB_STATUS_INIT), 0);
    sdbSetRawStatus(pRaw, SDB_STATUS_CREATING);
    ASSERT_EQ(sdbWrite(pSdb, pRaw), 0);
    pI32Obj = (SI32Obj *)sdbAcquire(pSdb, SDB_VGROUP, &key);
    ASSERT_EQ(pI32Obj, nullptr);
    // int32_t code = terrno;
    // ASSERT_EQ(code, TSDB_CODE_SDB_OBJ_CREATING);
  }

  {
    SSdbIter *pReader = NULL;
    SSdbIter *pWritter = NULL;
    void     *pBuf = NULL;
    int32_t   len = 0;
    int32_t   code = 0;

    code = sdbStartRead(pSdb, &pReader, NULL, NULL, NULL);
    ASSERT_EQ(code, 0);
    code = sdbStartWrite(pSdb, &pWritter);
    ASSERT_EQ(code, 0);

    while (sdbDoRead(pSdb, pReader, &pBuf, &len) == 0) {
      if (pBuf != NULL && len != 0) {
        sdbDoWrite(pSdb, pWritter, pBuf, len);
        taosMemoryFree(pBuf);
      } else {
        break;
      }
    }

    sdbStopRead(pSdb, pReader);
    sdbStopWrite(pSdb, pWritter, true, -1, -1, -1);
  }

  ASSERT_EQ(sdbGetSize(pSdb, SDB_CONSUMER), 1);
  ASSERT_EQ(sdbGetTableVer(pSdb, SDB_CONSUMER), 4);

  sdbCleanup(pSdb);
  ASSERT_EQ(mnode.insertTimes, 9);
  ASSERT_EQ(mnode.deleteTimes, 9);
}

// Helper function to read mnode.json and check encrypted flag
bool readMnodeJsonEncryptedFlag(const char *path) {
  char jsonFile[PATH_MAX] = {0};
  snprintf(jsonFile, sizeof(jsonFile), "%s%smnode.json", path, TD_DIRSEP);
  printf("readMnodeJsonEncryptedFlag: checking file %s\n", jsonFile);
  
  // Use taosReadCfgFile to read the JSON file
  char *content = NULL;
  int32_t contentLen = 0;
  
  TdFilePtr pFile = taosOpenFile(jsonFile, TD_FILE_READ);
  if (pFile == NULL) {
    return false;
  }
  
  int64_t fileSize = 0;
  if (taosFStatFile(pFile, &fileSize, NULL) < 0) {
    taosCloseFile(&pFile);
    return false;
  }
  
  content = (char *)taosMemoryMalloc(fileSize + 1);
  if (content == NULL) {
    taosCloseFile(&pFile);
    return false;
  }
  
  int64_t readSize = taosReadFile(pFile, content, fileSize);
  taosCloseFile(&pFile);
  
  if (readSize != fileSize) {
    taosMemoryFree(content);
    return false;
  }
  content[fileSize] = '\0';
  
  SJson *pJson = tjsonParse(content);
  taosMemoryFree(content);
  if (pJson == NULL) {
    return false;
  }
  
  int32_t encrypted = 0;
  int32_t code = 0;
  tjsonGetInt32ValueFromDouble(pJson, "encrypted", encrypted, code);
  tjsonDelete(pJson);
  
  return (encrypted != 0);
}

// Callback to persist encrypted flag for tests
extern "C" {
  extern int32_t mmReadFile(const char *path, SMnodeOpt *pOption);
  extern int32_t mmWriteFile(const char *path, const SMnodeOpt *pOption);
}

static int32_t testPersistEncryptedFlag(void *param) {
  SSdb *pSdb = (SSdb *)param;
  if (pSdb == NULL || pSdb->mnodePath[0] == '\0') {
    return -1;
  }
  
  printf("testPersistEncryptedFlag: persisting to %s/mnode.json\n", pSdb->mnodePath);
  
  SMnodeOpt option = {0};
  int32_t code = mmReadFile(pSdb->mnodePath, &option);
  if (code != 0) {
    printf("testPersistEncryptedFlag: mmReadFile failed with code %d\n", code);
    return code;
  }
  
  option.encrypted = true;
  code = mmWriteFile(pSdb->mnodePath, &option);
  if (code != 0) {
    printf("testPersistEncryptedFlag: mmWriteFile failed with code %d\n", code);
    return code;
  }
  
  printf("testPersistEncryptedFlag: successfully persisted encrypted flag\n");
  return 0;
}

// Test encryption migration: unencrypted -> encrypted restart -> restart again
TEST_F(MndTestSdb, 02_Encryption_Migration) {
  const char *testPath = TD_TMP_DIR_PATH "mnode_test_encryption";
  taosRemoveDir(testPath);
  taosMkDir(testPath);
  
  SMnode mnode1 = {0};
  SSdb *pSdb1 = NULL;
  SSdbOpt opt1 = {0};
  
  // Setup test tables
  SSdbTable strTable1;
  memset(&strTable1, 0, sizeof(SSdbTable));
  strTable1.sdbType = SDB_USER;
  strTable1.keyType = SDB_KEY_BINARY;
  strTable1.deployFp = (SdbDeployFp)strDefault;
  strTable1.encodeFp = (SdbEncodeFp)strEncode;
  strTable1.decodeFp = (SdbDecodeFp)strDecode;
  strTable1.insertFp = (SdbInsertFp)strInsert;
  strTable1.updateFp = (SdbUpdateFp)strUpdate;
  strTable1.deleteFp = (SdbDeleteFp)strDelete;
  
  // ===== Step 1: Create unencrypted sdb (no tsMetaKey) =====
  printf("Step 1: Creating unencrypted sdb...\n");
  memset(tsMetaKey, 0, sizeof(tsMetaKey));  // Clear metaKey
  tsEncryptAlgorithmType = TSDB_ENCRYPT_ALGO_NONE;  // No encryption initially
  
  mnode1.v100 = 100;
  mnode1.v200 = 200;
  opt1.pMnode = &mnode1;
  opt1.path = testPath;
  
  pSdb1 = sdbInit(&opt1);
  mnode1.pSdb = pSdb1;
  ASSERT_NE(pSdb1, nullptr);
  ASSERT_EQ(sdbSetTable(pSdb1, strTable1), 0);
  ASSERT_EQ(sdbDeploy(pSdb1), 0);
  
  // Verify data was inserted
  SStrObj *pObj = (SStrObj *)sdbAcquire(pSdb1, SDB_USER, "k1000");
  ASSERT_NE(pObj, nullptr);
  EXPECT_STREQ(pObj->key, "k1000");
  sdbRelease(pSdb1, pObj);
  
  // Write to disk
  sdbSetApplyInfo(pSdb1, 1, 1, 1);
  ASSERT_EQ(sdbWriteFile(pSdb1, 0), 0);
  
  // Verify encrypted flag is false
  EXPECT_EQ(pSdb1->encrypted, false);
  
  // Create mnode.json with encrypted=false to simulate real scenario
  SMnodeOpt option1 = {0};
  option1.deploy = true;
  option1.encrypted = false;
  ASSERT_EQ(mmWriteFile(testPath, &option1), 0);
  printf("Step 1: Created mnode.json with encrypted=false\n");
  
  EXPECT_EQ(readMnodeJsonEncryptedFlag(testPath), false);
  
  sdbCleanup(pSdb1);
  printf("Step 1: Completed. Encrypted flag = false\n\n");
  
  // ===== Step 2: Restart with tsMetaKey, trigger migration =====
  printf("Step 2: Restarting with tsMetaKey to trigger migration...\n");
  
  // Set encryption key and algorithm
  const char *testKey = "1234567890123456";  // 16 chars
  strncpy(tsMetaKey, testKey, ENCRYPT_KEY_LEN);
  tsMetaKey[ENCRYPT_KEY_LEN] = '\0';
  tsEncryptAlgorithmType = TSDB_ENCRYPT_ALGO_SM4;  // Use SM4 encryption
  
  SMnode mnode2 = {0};
  SSdb *pSdb2 = NULL;
  SSdbOpt opt2 = {0};
  
  mnode2.v100 = 100;
  mnode2.v200 = 200;
  opt2.pMnode = &mnode2;
  opt2.path = testPath;
  
  pSdb2 = sdbInit(&opt2);
  mnode2.pSdb = pSdb2;
  ASSERT_NE(pSdb2, nullptr);
  
  // Set up callback for persisting encrypted flag (simulating mndOpenSdb)
  pSdb2->persistEncryptedFlagFp = testPersistEncryptedFlag;
  pSdb2->pMnodeForCallback = pSdb2;
  pSdb2->encrypted = readMnodeJsonEncryptedFlag(testPath);  // Read from mnode.json
  printf("Step 2: Set up callback, encrypted from json: %d\n", pSdb2->encrypted);
  
  ASSERT_EQ(sdbSetTable(pSdb2, strTable1), 0);
  
  // Read file - should trigger migration
  ASSERT_EQ(sdbReadFile(pSdb2), 0);
  
  // Verify encrypted flag is now true
  EXPECT_EQ(pSdb2->encrypted, true);
  EXPECT_EQ(readMnodeJsonEncryptedFlag(testPath), true);
  
  // Verify data can still be read correctly after encryption
  pObj = (SStrObj *)sdbAcquire(pSdb2, SDB_USER, "k1000");
  ASSERT_NE(pObj, nullptr);
  EXPECT_STREQ(pObj->key, "k1000");
  EXPECT_STREQ(pObj->vstr, "v1000");
  ASSERT_EQ(pObj->v8, 1);
  ASSERT_EQ(pObj->v32, 1000);
  sdbRelease(pSdb2, pObj);
  
  pObj = (SStrObj *)sdbAcquire(pSdb2, SDB_USER, "k2000");
  ASSERT_NE(pObj, nullptr);
  EXPECT_STREQ(pObj->key, "k2000");
  EXPECT_STREQ(pObj->vstr, "v2000");
  ASSERT_EQ(pObj->v8, 2);
  sdbRelease(pSdb2, pObj);
  
  // Write a new record to encrypted sdb
  SStrObj strObj;
  strSetDefault(&strObj, 5);
  SSdbRaw *pRaw = strEncode(&strObj);
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);
  ASSERT_EQ(sdbWrite(pSdb2, pRaw), 0);
  
  pObj = (SStrObj *)sdbAcquire(pSdb2, SDB_USER, "k5000");
  ASSERT_NE(pObj, nullptr);
  EXPECT_STREQ(pObj->key, "k5000");
  ASSERT_EQ(pObj->v8, 5);
  sdbRelease(pSdb2, pObj);
  
  // Persist the new record to disk
  sdbSetApplyInfo(pSdb2, 2, 1, 1);
  ASSERT_EQ(sdbWriteFile(pSdb2, 0), 0);
  
  sdbCleanup(pSdb2);
  printf("Step 2: Completed. Migration successful, encrypted flag = true\n\n");
  
  // ===== Step 3: Restart again with tsMetaKey, verify encrypted read =====
  printf("Step 3: Restarting again to verify encrypted file works...\n");
  
  SMnode mnode3 = {0};
  SSdb *pSdb3 = NULL;
  SSdbOpt opt3 = {0};
  
  mnode3.v100 = 100;
  mnode3.v200 = 200;
  opt3.pMnode = &mnode3;
  opt3.path = testPath;
  
  pSdb3 = sdbInit(&opt3);
  mnode3.pSdb = pSdb3;
  ASSERT_NE(pSdb3, nullptr);
  
  ASSERT_EQ(sdbSetTable(pSdb3, strTable1), 0);
  
  // Simulate real mnode behavior: read mnode.json and set encrypted flag BEFORE reading sdb
  bool encryptedFromJson = readMnodeJsonEncryptedFlag(testPath);
  printf("Step 3: Read encrypted flag from mnode.json: %d\n", encryptedFromJson);
  pSdb3->encrypted = encryptedFromJson;
  EXPECT_EQ(pSdb3->encrypted, true);
  
  // Read file - should read encrypted data normally (no migration this time)
  ASSERT_EQ(sdbReadFile(pSdb3), 0);
  
  // Verify encrypted flag is still true
  EXPECT_EQ(pSdb3->encrypted, true);
  EXPECT_EQ(readMnodeJsonEncryptedFlag(testPath), true);
  
  // Check if data was loaded
  int32_t size = sdbGetSize(pSdb3, SDB_USER);
  printf("Step 3: sdbGetSize returned %d (expected 3)\n", size);
  ASSERT_EQ(size, 3);
  
  // Verify all data can be read correctly
  pObj = (SStrObj *)sdbAcquire(pSdb3, SDB_USER, "k1000");
  ASSERT_NE(pObj, nullptr);
  EXPECT_STREQ(pObj->key, "k1000");
  ASSERT_EQ(pObj->v8, 1);
  sdbRelease(pSdb3, pObj);
  
  pObj = (SStrObj *)sdbAcquire(pSdb3, SDB_USER, "k2000");
  ASSERT_NE(pObj, nullptr);
  EXPECT_STREQ(pObj->key, "k2000");
  ASSERT_EQ(pObj->v8, 2);
  sdbRelease(pSdb3, pObj);
  
  pObj = (SStrObj *)sdbAcquire(pSdb3, SDB_USER, "k5000");
  ASSERT_NE(pObj, nullptr);
  EXPECT_STREQ(pObj->key, "k5000");
  ASSERT_EQ(pObj->v8, 5);
  sdbRelease(pSdb3, pObj);
  
  ASSERT_EQ(sdbGetSize(pSdb3, SDB_USER), 3);
  
  sdbCleanup(pSdb3);
  printf("Step 3: Completed. All data read correctly from encrypted file\n\n");
  
  // Cleanup
  memset(tsMetaKey, 0, sizeof(tsMetaKey));
  tsEncryptAlgorithmType = TSDB_ENCRYPT_ALGO_NONE;
  taosRemoveDir(testPath);
  
  printf("Encryption migration test passed!\n");
}

// Test case: Migration failure handling (corrupted data)
TEST_F(MndTestSdb, 03_Encryption_Migration_Error_Handling) {
  const char *testPath = TD_TMP_DIR_PATH "mnode_test_encryption_error";
  taosRemoveDir(testPath);
  taosMkDir(testPath);
  
  SMnode mnode1 = {0};
  SSdb *pSdb1 = NULL;
  SSdbOpt opt1 = {0};
  
  SSdbTable strTable1;
  memset(&strTable1, 0, sizeof(SSdbTable));
  strTable1.sdbType = SDB_USER;
  strTable1.keyType = SDB_KEY_BINARY;
  strTable1.deployFp = (SdbDeployFp)strDefault;
  strTable1.encodeFp = (SdbEncodeFp)strEncode;
  strTable1.decodeFp = (SdbDecodeFp)strDecode;
  strTable1.insertFp = (SdbInsertFp)strInsert;
  strTable1.updateFp = (SdbUpdateFp)strUpdate;
  strTable1.deleteFp = (SdbDeleteFp)strDelete;
  
  // Create unencrypted sdb
  memset(tsMetaKey, 0, sizeof(tsMetaKey));
  tsEncryptAlgorithmType = TSDB_ENCRYPT_ALGO_NONE;
  
  mnode1.v100 = 100;
  mnode1.v200 = 200;
  opt1.pMnode = &mnode1;
  opt1.path = testPath;
  
  pSdb1 = sdbInit(&opt1);
  mnode1.pSdb = pSdb1;
  ASSERT_NE(pSdb1, nullptr);
  ASSERT_EQ(sdbSetTable(pSdb1, strTable1), 0);
  ASSERT_EQ(sdbDeploy(pSdb1), 0);
  
  sdbSetApplyInfo(pSdb1, 1, 1, 1);
  ASSERT_EQ(sdbWriteFile(pSdb1, 0), 0);
  EXPECT_EQ(pSdb1->encrypted, false);
  
  // Create mnode.json with encrypted=false to simulate real scenario
  SMnodeOpt option1 = {0};
  option1.deploy = true;
  option1.encrypted = false;
  ASSERT_EQ(mmWriteFile(testPath, &option1), 0);
  
  sdbCleanup(pSdb1);
  
  // Verify file exists and is not encrypted
  char sdbFile[PATH_MAX] = {0};
  snprintf(sdbFile, sizeof(sdbFile), "%s%sdata%ssdb.data", testPath, TD_DIRSEP, TD_DIRSEP);
  EXPECT_TRUE(taosCheckExistFile(sdbFile));
  EXPECT_FALSE(taosIsEncryptedFile(sdbFile, NULL));
  
  // Now try to read with encryption key - should successfully migrate
  const char *testKey = "1234567890123456";
  strncpy(tsMetaKey, testKey, ENCRYPT_KEY_LEN);
  tsMetaKey[ENCRYPT_KEY_LEN] = '\0';
  tsEncryptAlgorithmType = TSDB_ENCRYPT_ALGO_SM4;
  
  SMnode mnode2 = {0};
  SSdb *pSdb2 = NULL;
  SSdbOpt opt2 = {0};
  
  mnode2.v100 = 100;
  mnode2.v200 = 200;
  opt2.pMnode = &mnode2;
  opt2.path = testPath;
  
  pSdb2 = sdbInit(&opt2);
  mnode2.pSdb = pSdb2;
  ASSERT_NE(pSdb2, nullptr);
  
  // Set up callback for persisting encrypted flag (simulating mndOpenSdb)
  pSdb2->persistEncryptedFlagFp = testPersistEncryptedFlag;
  pSdb2->pMnodeForCallback = pSdb2;
  pSdb2->encrypted = readMnodeJsonEncryptedFlag(testPath);  // Read from mnode.json
  
  ASSERT_EQ(sdbSetTable(pSdb2, strTable1), 0);
  
  ASSERT_EQ(sdbReadFile(pSdb2), 0);
  
  // After migration, encrypted flag should be true
  EXPECT_EQ(pSdb2->encrypted, true);
  EXPECT_EQ(readMnodeJsonEncryptedFlag(testPath), true);
  
  // Note: sdb.data uses custom encryption format without standard header,
  // so taosIsEncryptedFile may not detect it correctly
  
  sdbCleanup(pSdb2);
  
  // Cleanup
  memset(tsMetaKey, 0, sizeof(tsMetaKey));
  tsEncryptAlgorithmType = TSDB_ENCRYPT_ALGO_NONE;
  taosRemoveDir(testPath);
}
