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

class MndTestSdb : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {}
  static void TearDownTestSuite() {}

 public:
  void SetUp() override {}
  void TearDown() override {}
};

typedef struct SMnode {
  int32_t v100;
  int32_t v200;
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

int32_t strInsert(SSdb *pSdb, SStrObj *pObj) { return 0; }

int32_t strDelete(SSdb *pSdb, SStrObj *pObj, bool callFunc) { return 0; }

int32_t strUpdate(SSdb *pSdb, SStrObj *pOld, SStrObj *pNew) {
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
  sdbFreeRaw(pRaw);

  return 0;
}

bool sdbTraverseSucc1(SMnode *pMnode, SStrObj *pObj, int32_t *p1, int32_t *p2, int32_t *p3) {
  if (pObj->v8 == 1) {
    *p1 = *p2 + *p3 + pObj->v8;
  }
  return true;
}

bool sdbTraverseSucc2(SMnode *pMnode, SStrObj *pObj, int32_t *p1, int32_t *p2, int32_t *p3) {
  *p1 = *p2 + *p3 + pObj->v8;
  return true;
}

bool sdbTraverseFail(SMnode *pMnode, SStrObj *pObj, int32_t *p1, int32_t *p2, int32_t *p3) {
  *p1 = *p2 + *p3;
  return false;
}

TEST_F(MndTestSdb, 01_Write) {
  void    *pIter;
  int32_t  num;
  SStrObj *pObj;
  SMnode   mnode;
  SSdb    *pSdb;
  SSdbOpt  opt = {0};
  int32_t  p1 = 0;
  int32_t  p2 = 111;
  int32_t  p3 = 222;

  mnode.v100 = 100;
  mnode.v200 = 200;
  opt.pMnode = &mnode;
  opt.path = "/tmp/mnode_test_sdb";
  taosRemoveDir(opt.path);

  SSdbTable strTable = {
      .sdbType = SDB_USER,
      .keyType = SDB_KEY_BINARY,
      .deployFp = (SdbDeployFp)strDefault,
      .encodeFp = (SdbEncodeFp)strEncode,
      .decodeFp = (SdbDecodeFp)strDecode,
      .insertFp = (SdbInsertFp)strInsert,
      .updateFp = (SdbUpdateFp)strUpdate,
      .deleteFp = (SdbDeleteFp)strDelete,
  };

  pSdb = sdbInit(&opt);
  mnode.pSdb = pSdb;

  ASSERT_NE(pSdb, nullptr);
  ASSERT_EQ(sdbSetTable(pSdb, strTable), 0);
  ASSERT_EQ(sdbDeploy(pSdb), 0);
#if 0
  pObj = (SStrObj *)sdbAcquire(pSdb, SDB_USER, "k1000");
  ASSERT_NE(pObj, nullptr);
  EXPECT_STREQ(pObj->key, "k1000");
  EXPECT_STREQ(pObj->vstr, "v1000");
  EXPECT_EQ(pObj->v8, 1);
  EXPECT_EQ(pObj->v16, 1);
  EXPECT_EQ(pObj->v32, 1000);
  EXPECT_EQ(pObj->v64, 1000);
  sdbRelease(pSdb, pObj);

  pObj = (SStrObj *)sdbAcquire(pSdb, SDB_USER, "k2000");
  ASSERT_NE(pObj, nullptr);
  EXPECT_STREQ(pObj->key, "k2000");
  EXPECT_STREQ(pObj->vstr, "v2000");
  EXPECT_EQ(pObj->v8, 2);
  EXPECT_EQ(pObj->v16, 2);
  EXPECT_EQ(pObj->v32, 2000);
  EXPECT_EQ(pObj->v64, 2000);
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
  EXPECT_EQ(num, 2);

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
  sdbTraverse(pSdb, SDB_USER, (sdbTraverseFp)sdbTraverseSucc2, &p1, &p2, &p3);
  EXPECT_EQ(p1, 334);

  p1 = 0;
  p2 = 111;
  p3 = 222;
  sdbTraverse(pSdb, SDB_USER, (sdbTraverseFp)sdbTraverseSucc2, &p1, &p2, &p3);
  EXPECT_EQ(p1, 669);

  p1 = 0;
  p2 = 111;
  p3 = 222;
  sdbTraverse(pSdb, SDB_USER, (sdbTraverseFp)sdbTraverseFail, &p1, &p2, &p3);
  EXPECT_EQ(p1, 333);

  EXPECT_EQ(sdbGetSize(pSdb, SDB_USER), 2);
  EXPECT_EQ(sdbGetMaxId(pSdb, SDB_USER), -1);
  EXPECT_EQ(sdbGetTableVer(pSdb, SDB_USER), 2);
  EXPECT_EQ(sdbUpdateVer(pSdb, 0), 2);
  EXPECT_EQ(sdbUpdateVer(pSdb, 1), 3);
  EXPECT_EQ(sdbUpdateVer(pSdb, -1), 2);

  // insert, call func

  // update, call func

  // delete, call func 2

  // write version

  // sdb Write ver

  // sdbRead
#endif
  ASSERT_EQ(sdbWriteFile(pSdb), 0);
  sdbCleanup(pSdb);
}

TEST_F(MndTestSdb, 01_Read) {
  void    *pIter;
  int32_t  num;
  SStrObj *pObj;
  SMnode   mnode;
  SSdb    *pSdb;
  SSdbOpt  opt = {0};
  int32_t  p1 = 0;
  int32_t  p2 = 111;
  int32_t  p3 = 222;

  mnode.v100 = 100;
  mnode.v200 = 200;
  opt.pMnode = &mnode;
  opt.path = "/tmp/mnode_test_sdb";
  taosRemoveDir(opt.path);

  SSdbTable strTable = {
      .sdbType = SDB_USER,
      .keyType = SDB_KEY_BINARY,
      .deployFp = (SdbDeployFp)strDefault,
      .encodeFp = (SdbEncodeFp)strEncode,
      .decodeFp = (SdbDecodeFp)strDecode,
      .insertFp = (SdbInsertFp)strInsert,
      .updateFp = (SdbUpdateFp)strDelete,
      .deleteFp = (SdbDeleteFp)strUpdate,
  };

  pSdb = sdbInit(&opt);
  mnode.pSdb = pSdb;

  ASSERT_EQ(sdbReadFile(pSdb), 0);
  sdbCleanup(pSdb);
}