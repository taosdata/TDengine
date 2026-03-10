/**
 * @file xnodeEncodeDecodeTest.cpp
 * @brief XNode metadata encode/decode unit tests
 * @version 1.0
 * @date 2025-12-25
 */

#include <gtest/gtest.h>

#include "sdb.h"
#include "tdef.h"
#include "tglobal.h"

// Simplified XNode structures for testing
typedef struct SXnodeObj {
  int32_t id;
  int32_t urlLen;
  char   *url;
  int32_t statusLen;
  char   *status;
  int64_t createTime;
  int64_t updateTime;
} SXnodeObj;

class MndTestXnodeEncodeDecode : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    dDebugFlag = 143;
    mDebugFlag = 143;
    tsLogEmbedded = 1;
    tsAsyncLog = 0;

    const char *path = TD_TMP_DIR_PATH "td_xnode_test";
    taosRemoveDir(path);
    taosMkDir(path);
    tstrncpy(tsLogDir, path, PATH_MAX);
    if (taosInitLog("xnode_test_log", 1, false) != 0) {
      printf("failed to init log file\n");
    }
  }

  static void TearDownTestSuite() { taosCloseLog(); }

 public:
  void SetUp() override {}
  void TearDown() override {}
};

// Helper function to compare XNode objects
bool compareXnodeObj(SXnodeObj *obj1, SXnodeObj *obj2) {
  if (obj1->id != obj2->id) return false;
  if (obj1->urlLen != obj2->urlLen) return false;
  if (obj1->statusLen != obj2->statusLen) return false;
  if (obj1->urlLen > 0 && memcmp(obj1->url, obj2->url, obj1->urlLen) != 0) return false;
  if (obj1->statusLen > 0 && memcmp(obj1->status, obj2->status, obj1->statusLen) != 0) return false;
  if (obj1->createTime != obj2->createTime) return false;
  if (obj1->updateTime != obj2->updateTime) return false;
  return true;
}

TEST_F(MndTestXnodeEncodeDecode, test_xnode_encode_decode) {
  // Create a test XNode object
  SXnodeObj obj = {0};
  obj.id = 1;
  obj.urlLen = strlen("xnode1:6050") + 1;
  obj.url = (char *)taosMemoryCalloc(obj.urlLen, 1);
  strcpy(obj.url, "xnode1:6050");
  obj.statusLen = strlen("online") + 1;
  obj.status = (char *)taosMemoryCalloc(obj.statusLen, 1);
  strcpy(obj.status, "online");
  obj.createTime = 1234567890;
  obj.updateTime = 1234567900;

  // Encode the object (using mndXnodeActionEncode from mndXnode.c)
  // Note: We need to access the internal encode function
  // For now, we'll test the encode/decode logic separately

  // Since we don't have direct access to encode function, we test the raw format
  int32_t  rawDataLen = sizeof(SXnodeObj) + 64 + obj.urlLen;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_XNODE, 1, rawDataLen);
  ASSERT_NE(pRaw, nullptr);

  int32_t dataPos = 0;
  sdbSetRawInt32(pRaw, dataPos, obj.id);
  dataPos += sizeof(obj.id);
  sdbSetRawInt32(pRaw, dataPos, obj.urlLen);
  dataPos += sizeof(obj.urlLen);
  sdbSetRawBinary(pRaw, dataPos, obj.url, obj.urlLen);
  dataPos += obj.urlLen;
  sdbSetRawInt32(pRaw, dataPos, obj.statusLen);
  dataPos += sizeof(obj.statusLen);
  sdbSetRawBinary(pRaw, dataPos, obj.status, obj.statusLen);
  dataPos += obj.statusLen;
  sdbSetRawInt64(pRaw, dataPos, obj.createTime);
  dataPos += sizeof(obj.createTime);
  sdbSetRawInt64(pRaw, dataPos, obj.updateTime);
  dataPos += sizeof(obj.updateTime);
  sdbSetRawDataLen(pRaw, dataPos);

  // Decode the raw data
  int8_t sver = 0;
  ASSERT_EQ(sdbGetRawSoftVer(pRaw, &sver), 0);
  ASSERT_EQ(sver, 1);

  SSdbRow *pRow = sdbAllocRow(sizeof(SXnodeObj));
  ASSERT_NE(pRow, nullptr);

  SXnodeObj *pDecoded = (SXnodeObj *)sdbGetRowObj(pRow);
  ASSERT_NE(pDecoded, nullptr);

  // Reset dataPos for decoding
  dataPos = 0;
  sdbGetRawInt32(pRaw, dataPos, &pDecoded->id);
  dataPos += sizeof(pDecoded->id);
  sdbGetRawInt32(pRaw, dataPos, &pDecoded->urlLen);
  dataPos += sizeof(pDecoded->urlLen);

  if (pDecoded->urlLen > 0) {
    pDecoded->url = (char *)taosMemoryCalloc(pDecoded->urlLen, 1);
    sdbGetRawBinary(pRaw, dataPos, pDecoded->url, pDecoded->urlLen);
    dataPos += pDecoded->urlLen;
  }

  sdbGetRawInt32(pRaw, dataPos, &pDecoded->statusLen);
  dataPos += sizeof(pDecoded->statusLen);

  if (pDecoded->statusLen > 0) {
    pDecoded->status = (char *)taosMemoryCalloc(pDecoded->statusLen, 1);
    sdbGetRawBinary(pRaw, dataPos, pDecoded->status, pDecoded->statusLen);
    dataPos += pDecoded->statusLen;
  }

  sdbGetRawInt64(pRaw, dataPos, &pDecoded->createTime);
  dataPos += sizeof(pDecoded->createTime);
  sdbGetRawInt64(pRaw, dataPos, &pDecoded->updateTime);
  dataPos += sizeof(pDecoded->updateTime);

  // Verify decoded object matches original
  EXPECT_TRUE(compareXnodeObj(&obj, pDecoded));

  // Cleanup
  taosMemoryFree(obj.url);
  taosMemoryFree(obj.status);
  taosMemoryFree(pDecoded->url);
  taosMemoryFree(pDecoded->status);
  taosMemoryFree(pRow);
  sdbFreeRaw(pRaw);
}

TEST_F(MndTestXnodeEncodeDecode, test_encode_decode_with_empty_fields) {
  // Test with empty URL and status
  SXnodeObj obj = {0};
  obj.id = 2;
  obj.urlLen = 0;
  obj.url = NULL;
  obj.statusLen = 0;
  obj.status = NULL;
  obj.createTime = 9876543210;
  obj.updateTime = 9876543220;

  int32_t  rawDataLen = sizeof(SXnodeObj) + 64;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_XNODE, 1, rawDataLen);
  ASSERT_NE(pRaw, nullptr);

  int32_t dataPos = 0;
  sdbSetRawInt32(pRaw, dataPos, obj.id);
  dataPos += sizeof(obj.id);
  sdbSetRawInt32(pRaw, dataPos, obj.urlLen);
  dataPos += sizeof(obj.urlLen);
  sdbSetRawInt32(pRaw, dataPos, obj.statusLen);
  dataPos += sizeof(obj.statusLen);
  sdbSetRawInt64(pRaw, dataPos, obj.createTime);
  dataPos += sizeof(obj.createTime);
  sdbSetRawInt64(pRaw, dataPos, obj.updateTime);
  dataPos += sizeof(obj.updateTime);
  sdbSetRawDataLen(pRaw, dataPos);

  // Decode
  SSdbRow *pRow = sdbAllocRow(sizeof(SXnodeObj));
  ASSERT_NE(pRow, nullptr);

  SXnodeObj *pDecoded = (SXnodeObj *)sdbGetRowObj(pRow);
  ASSERT_NE(pDecoded, nullptr);

  dataPos = 0;
  sdbGetRawInt32(pRaw, dataPos, &pDecoded->id);
  dataPos += sizeof(pDecoded->id);
  sdbGetRawInt32(pRaw, dataPos, &pDecoded->urlLen);
  dataPos += sizeof(pDecoded->urlLen);
  sdbGetRawInt32(pRaw, dataPos, &pDecoded->statusLen);
  dataPos += sizeof(pDecoded->statusLen);
  sdbGetRawInt64(pRaw, dataPos, &pDecoded->createTime);
  dataPos += sizeof(pDecoded->createTime);
  sdbGetRawInt64(pRaw, dataPos, &pDecoded->updateTime);
  dataPos += sizeof(pDecoded->updateTime);

  // Verify
  EXPECT_EQ(obj.id, pDecoded->id);
  EXPECT_EQ(obj.urlLen, pDecoded->urlLen);
  EXPECT_EQ(obj.statusLen, pDecoded->statusLen);
  EXPECT_EQ(obj.createTime, pDecoded->createTime);
  EXPECT_EQ(obj.updateTime, pDecoded->updateTime);

  // Cleanup
  taosMemoryFree(pRow);
  sdbFreeRaw(pRaw);
}

TEST_F(MndTestXnodeEncodeDecode, test_encode_decode_with_max_fields) {
  // Test with maximum length fields
  SXnodeObj obj = {0};
  obj.id = 999;
  obj.urlLen = 256;  // Max URL length
  obj.url = (char *)taosMemoryCalloc(obj.urlLen, 1);
  memset(obj.url, 'X', obj.urlLen - 1);
  obj.url[obj.urlLen - 1] = '\0';

  obj.statusLen = 256;  // Reasonable max status length
  obj.status = (char *)taosMemoryCalloc(obj.statusLen, 1);
  memset(obj.status, 'S', obj.statusLen - 1);
  obj.status[obj.statusLen - 1] = '\0';

  obj.createTime = INT64_MAX - 1000;
  obj.updateTime = INT64_MAX - 500;

  int32_t  rawDataLen = sizeof(SXnodeObj) + 64 + obj.urlLen + obj.statusLen;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_XNODE, 1, rawDataLen);
  ASSERT_NE(pRaw, nullptr);

  int32_t dataPos = 0;
  sdbSetRawInt32(pRaw, dataPos, obj.id);
  dataPos += sizeof(obj.id);
  sdbSetRawInt32(pRaw, dataPos, obj.urlLen);
  dataPos += sizeof(obj.urlLen);
  sdbSetRawBinary(pRaw, dataPos, obj.url, obj.urlLen);
  dataPos += obj.urlLen;
  sdbSetRawInt32(pRaw, dataPos, obj.statusLen);
  dataPos += sizeof(obj.statusLen);
  sdbSetRawBinary(pRaw, dataPos, obj.status, obj.statusLen);
  dataPos += obj.statusLen;
  sdbSetRawInt64(pRaw, dataPos, obj.createTime);
  dataPos += sizeof(obj.createTime);
  sdbSetRawInt64(pRaw, dataPos, obj.updateTime);
  dataPos += sizeof(obj.updateTime);
  sdbSetRawDataLen(pRaw, dataPos);

  // Decode
  SSdbRow *pRow = sdbAllocRow(sizeof(SXnodeObj));
  ASSERT_NE(pRow, nullptr);

  SXnodeObj *pDecoded = (SXnodeObj *)sdbGetRowObj(pRow);
  ASSERT_NE(pDecoded, nullptr);

  dataPos = 0;
  sdbGetRawInt32(pRaw, dataPos, &pDecoded->id);
  dataPos += sizeof(pDecoded->id);
  sdbGetRawInt32(pRaw, dataPos, &pDecoded->urlLen);
  dataPos += sizeof(pDecoded->urlLen);

  pDecoded->url = (char *)taosMemoryCalloc(pDecoded->urlLen, 1);
  sdbGetRawBinary(pRaw, dataPos, pDecoded->url, pDecoded->urlLen);
  dataPos += pDecoded->urlLen;

  sdbGetRawInt32(pRaw, dataPos, &pDecoded->statusLen);
  dataPos += sizeof(pDecoded->statusLen);

  pDecoded->status = (char *)taosMemoryCalloc(pDecoded->statusLen, 1);
  sdbGetRawBinary(pRaw, dataPos, pDecoded->status, pDecoded->statusLen);
  dataPos += pDecoded->statusLen;

  sdbGetRawInt64(pRaw, dataPos, &pDecoded->createTime);
  dataPos += sizeof(pDecoded->createTime);
  sdbGetRawInt64(pRaw, dataPos, &pDecoded->updateTime);
  dataPos += sizeof(pDecoded->updateTime);

  // Verify
  EXPECT_TRUE(compareXnodeObj(&obj, pDecoded));

  // Cleanup
  taosMemoryFree(obj.url);
  taosMemoryFree(obj.status);
  taosMemoryFree(pDecoded->url);
  taosMemoryFree(pDecoded->status);
  taosMemoryFree(pRow);
  sdbFreeRaw(pRaw);
}

TEST_F(MndTestXnodeEncodeDecode, test_xnode_task_basic_encode_decode) {
  // Test basic XNode Task encode/decode
  // Note: This is a simplified test structure
  // In production, use actual SXnodeTaskObj structure from mndDef.h

  struct TestTaskObj {
    int32_t tid;
    char    name[TSDB_TABLE_NAME_LEN];
    int32_t xnodeId;
    int32_t agentId;
    int64_t createTime;
    int64_t updateTime;
  } task = {0};

  task.tid = 100;
  strcpy(task.name, "test_task_1");
  task.xnodeId = 1;
  task.agentId = -1;
  task.createTime = 1000000;
  task.updateTime = 1000100;

  // Encode
  int32_t  rawDataLen = sizeof(TestTaskObj) + 64;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_XNODE_TASK, 1, rawDataLen);
  ASSERT_NE(pRaw, nullptr);

  int32_t dataPos = 0;
  sdbSetRawInt32(pRaw, dataPos, task.tid);
  dataPos += sizeof(task.tid);
  sdbSetRawBinary(pRaw, dataPos, task.name, TSDB_TABLE_NAME_LEN);
  dataPos += TSDB_TABLE_NAME_LEN;
  sdbSetRawInt32(pRaw, dataPos, task.xnodeId);
  dataPos += sizeof(task.xnodeId);
  sdbSetRawInt32(pRaw, dataPos, task.agentId);
  dataPos += sizeof(task.agentId);
  sdbSetRawInt64(pRaw, dataPos, task.createTime);
  dataPos += sizeof(task.createTime);
  sdbSetRawInt64(pRaw, dataPos, task.updateTime);
  dataPos += sizeof(task.updateTime);
  sdbSetRawDataLen(pRaw, dataPos);

  // Decode
  TestTaskObj decoded = {0};
  dataPos = 0;
  sdbGetRawInt32(pRaw, dataPos, &decoded.tid);
  dataPos += sizeof(decoded.tid);
  sdbGetRawBinary(pRaw, dataPos, decoded.name, TSDB_TABLE_NAME_LEN);
  dataPos += TSDB_TABLE_NAME_LEN;
  sdbGetRawInt32(pRaw, dataPos, &decoded.xnodeId);
  dataPos += sizeof(decoded.xnodeId);
  sdbGetRawInt32(pRaw, dataPos, &decoded.agentId);
  dataPos += sizeof(decoded.agentId);
  sdbGetRawInt64(pRaw, dataPos, &decoded.createTime);
  dataPos += sizeof(decoded.createTime);
  sdbGetRawInt64(pRaw, dataPos, &decoded.updateTime);
  dataPos += sizeof(decoded.updateTime);

  // Verify
  EXPECT_EQ(task.tid, decoded.tid);
  EXPECT_STREQ(task.name, decoded.name);
  EXPECT_EQ(task.xnodeId, decoded.xnodeId);
  EXPECT_EQ(task.agentId, decoded.agentId);
  EXPECT_EQ(task.createTime, decoded.createTime);
  EXPECT_EQ(task.updateTime, decoded.updateTime);

  // Cleanup
  sdbFreeRaw(pRaw);
}

TEST_F(MndTestXnodeEncodeDecode, test_xnode_job_basic_encode_decode) {
  // Test basic XNode Job encode/decode
  struct TestJobObj {
    int32_t jid;
    int32_t tid;
    int32_t xnodeId;
    int64_t createTime;
    int64_t updateTime;
  } job = {0};

  job.jid = 500;
  job.tid = 100;
  job.xnodeId = 2;
  job.createTime = 2000000;
  job.updateTime = 2000200;

  // Encode
  int32_t  rawDataLen = sizeof(TestJobObj) + 64;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_XNODE_JOB, 1, rawDataLen);
  ASSERT_NE(pRaw, nullptr);

  int32_t dataPos = 0;
  sdbSetRawInt32(pRaw, dataPos, job.jid);
  dataPos += sizeof(job.jid);
  sdbSetRawInt32(pRaw, dataPos, job.tid);
  dataPos += sizeof(job.tid);
  sdbSetRawInt32(pRaw, dataPos, job.xnodeId);
  dataPos += sizeof(job.xnodeId);
  sdbSetRawInt64(pRaw, dataPos, job.createTime);
  dataPos += sizeof(job.createTime);
  sdbSetRawInt64(pRaw, dataPos, job.updateTime);
  dataPos += sizeof(job.updateTime);
  sdbSetRawDataLen(pRaw, dataPos);

  // Decode
  TestJobObj decoded = {0};
  dataPos = 0;
  sdbGetRawInt32(pRaw, dataPos, &decoded.jid);
  dataPos += sizeof(decoded.jid);
  sdbGetRawInt32(pRaw, dataPos, &decoded.tid);
  dataPos += sizeof(decoded.tid);
  sdbGetRawInt32(pRaw, dataPos, &decoded.xnodeId);
  dataPos += sizeof(decoded.xnodeId);
  sdbGetRawInt64(pRaw, dataPos, &decoded.createTime);
  dataPos += sizeof(decoded.createTime);
  sdbGetRawInt64(pRaw, dataPos, &decoded.updateTime);
  dataPos += sizeof(decoded.updateTime);

  // Verify
  EXPECT_EQ(job.jid, decoded.jid);
  EXPECT_EQ(job.tid, decoded.tid);
  EXPECT_EQ(job.xnodeId, decoded.xnodeId);
  EXPECT_EQ(job.createTime, decoded.createTime);
  EXPECT_EQ(job.updateTime, decoded.updateTime);

  // Cleanup
  sdbFreeRaw(pRaw);
}
