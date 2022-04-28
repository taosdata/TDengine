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
  sdbSetRawBinary(pRaw, dataPos, pObj->key, sizeof(pObj->vstr));
  dataPos += sizeof(pObj->key);
  sdbSetRawDataLen(pRaw, dataPos);

  return pRaw;
}

SSdbRaw *strDecode(SStrObj *pObj) {
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
  sdbSetRawBinary(pRaw, dataPos, pObj->key, sizeof(pObj->vstr));
  dataPos += sizeof(pObj->key);
  sdbSetRawDataLen(pRaw, dataPos);

  return pRaw;
}

int32_t strInsert(SSdb *pSdb, SStrObj *pObj) { return 0; }

int32_t strDelete(SSdb *pSdb, SStrObj *pObj, bool callFunc) { return 0; }

int32_t strUpdate(SSdb *pSdb, SStrObj *pOld, SStrObj *pNew) {
  pOld->v8 = pNew->v8;
  pOld->v16 = pNew->v16;
  pOld->v32 = pNew->v32;
  pOld->v64 = pNew->v64;
  return 0;
}

int32_t strDefault(SMnode *pMnode) {
  SStrObj strObj = {0};
  strcpy(strObj.key, "k1000");
  strObj.v8 = 1;
  strObj.v16 = 1;
  strObj.v32 = 1000;
  strObj.v64 = 1000;
  strcpy(strObj.vstr, "v1000");

  SSdbRaw *pRaw = strEncode(&strObj);
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);
  return sdbWrite(pMnode->pSdb, pRaw);
}

TEST_F(MndTestSdb, 01_Basic) {
  SMnode mnode;
  mnode.v100 = 100;
  mnode.v200 = 200;

  SSdbOpt opt = {0};
  opt.pMnode = &mnode;
  opt.path = "/tmp/mnode_test_sdb";

  SSdb *pSdb = sdbInit(&opt);
  EXPECT_NE(pSdb, nullptr);

  SSdbTable strTable = {
      .sdbType = SDB_USER,
      .keyType = SDB_KEY_BINARY,
      .deployFp = (SdbDeployFp)strEncode,
      .encodeFp = (SdbEncodeFp)strEncode,
      .decodeFp = (SdbDecodeFp)strDecode,
      .insertFp = (SdbInsertFp)strInsert,
      .updateFp = (SdbUpdateFp)strDelete,
      .deleteFp = (SdbDeleteFp)strUpdate,
  };

  sdbSetTable(pSdb, strTable);

  sdbCleanup(pSdb);
}
