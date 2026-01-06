/**
 * @file xnodeProductionTest.cpp
 * @brief Test Xnode objects using REAL production encode/decode functions from mndXnode.c
 * @version 1.0
 * @date 2025-12-25
 *
 * This test uses actual production encode/decode functions by linking against mnode library.
 * Tests cover all four Xnode object types with real SDB operations.
 */

#include <gtest/gtest.h>
#include <cstdint>
#include <cstring>

extern "C" {
#include "mndDef.h"
#include "mndInt.h"
#include "sdb.h"
#include "taoserror.h"
}

// These are the real production encode functions from mndXnode.c
// We declare them here to test them directly
extern "C" {
SSdbRaw* mndXnodeActionEncode(SXnodeObj* pObj);
SSdbRow* mndXnodeActionDecode(SSdbRaw* pRaw);

SSdbRaw* mndXnodeTaskActionEncode(SXnodeTaskObj* pObj);
SSdbRow* mndXnodeTaskActionDecode(SSdbRaw* pRaw);

SSdbRaw* mndXnodeJobActionEncode(SXnodeJobObj* pObj);
SSdbRow* mndXnodeJobActionDecode(SSdbRaw* pRaw);

SSdbRaw* mndXnodeUserPassActionEncode(SXnodeUserPassObj* pObj);
SSdbRow* mndXnodeUserPassActionDecode(SSdbRaw* pRaw);
}

class XnodeProductionTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}

  // Helper to free XnodeObj
  void freeXnodeObj(SXnodeObj* pObj) {
    if (!pObj) return;
    if (pObj->url) taosMemoryFree(pObj->url);
    if (pObj->status) taosMemoryFree(pObj->status);
  }

  // Helper to free XnodeTaskObj
  void freeXnodeTaskObj(SXnodeTaskObj* pObj) {
    if (!pObj) return;
    if (pObj->name) taosMemoryFree(pObj->name);
    if (pObj->sourceDsn) taosMemoryFree(pObj->sourceDsn);
    if (pObj->sinkDsn) taosMemoryFree(pObj->sinkDsn);
    if (pObj->parser) taosMemoryFree(pObj->parser);
    if (pObj->status) taosMemoryFree(pObj->status);
    if (pObj->reason) taosMemoryFree(pObj->reason);
  }

  // Helper to free XnodeJobObj
  void freeXnodeJobObj(SXnodeJobObj* pObj) {
    if (!pObj) return;
    if (pObj->config) taosMemoryFree(pObj->config);
    if (pObj->status) taosMemoryFree(pObj->status);
    if (pObj->reason) taosMemoryFree(pObj->reason);
  }

  // Helper to free XnodeUserPassObj
  void freeXnodeUserPassObj(SXnodeUserPassObj* pObj) {
    if (!pObj) return;
    if (pObj->user) taosMemoryFree(pObj->user);
    if (pObj->pass) taosMemoryFree(pObj->pass);
  }
};

// ========== SXnodeObj Production Tests ==========

TEST_F(XnodeProductionTest, XnodeObj_RealEncodeDecode_Basic) {
  // Create a real XnodeObj
  SXnodeObj obj = {0};
  obj.id = 1;
  obj.urlLen = 20;
  obj.url = (char*)taosMemoryCalloc(obj.urlLen, 1);
  strcpy(obj.url, "http://xnode1:6051");
  obj.statusLen = 7;
  obj.status = (char*)taosMemoryCalloc(obj.statusLen, 1);
  strcpy(obj.status, "online");
  obj.createTime = 1234567890;
  obj.updateTime = 1234567900;

  // Use REAL production encode function
  SSdbRaw* pRaw = mndXnodeActionEncode(&obj);
  ASSERT_NE(pRaw, nullptr);
  EXPECT_EQ(pRaw->type, SDB_XNODE);

  // Use REAL production decode function
  SSdbRow* pRow = mndXnodeActionDecode(pRaw);
  ASSERT_NE(pRow, nullptr);

  SXnodeObj* pDecoded = (SXnodeObj*)sdbGetRowObj(pRow);
  ASSERT_NE(pDecoded, nullptr);

  // Verify decoded data matches original
  EXPECT_EQ(pDecoded->id, obj.id);
  EXPECT_EQ(pDecoded->urlLen, obj.urlLen);
  EXPECT_STREQ(pDecoded->url, obj.url);
  EXPECT_EQ(pDecoded->statusLen, obj.statusLen);
  EXPECT_STREQ(pDecoded->status, obj.status);
  EXPECT_EQ(pDecoded->createTime, obj.createTime);
  EXPECT_EQ(pDecoded->updateTime, obj.updateTime);

  // Cleanup
  freeXnodeObj(&obj);
  freeXnodeObj(pDecoded);
  taosMemoryFree(pRow);
  sdbFreeRaw(pRaw);
}

TEST_F(XnodeProductionTest, XnodeObj_RealEncodeDecode_EmptyFields) {
  SXnodeObj obj = {0};
  obj.id = 2;
  obj.urlLen = 0;
  obj.url = nullptr;
  obj.statusLen = 0;
  obj.status = nullptr;
  obj.createTime = 1000000;
  obj.updateTime = 2000000;

  SSdbRaw* pRaw = mndXnodeActionEncode(&obj);
  ASSERT_NE(pRaw, nullptr);

  SSdbRow* pRow = mndXnodeActionDecode(pRaw);
  ASSERT_NE(pRow, nullptr);

  SXnodeObj* pDecoded = (SXnodeObj*)sdbGetRowObj(pRow);
  ASSERT_NE(pDecoded, nullptr);
  EXPECT_EQ(pDecoded->id, obj.id);
  EXPECT_EQ(pDecoded->urlLen, 0);
  EXPECT_EQ(pDecoded->url, nullptr);
  EXPECT_EQ(pDecoded->statusLen, 0);
  EXPECT_EQ(pDecoded->status, nullptr);

  freeXnodeObj(pDecoded);
  taosMemoryFree(pRow);
  sdbFreeRaw(pRaw);
}

TEST_F(XnodeProductionTest, XnodeObj_RealEncodeDecode_LongStrings) {
  SXnodeObj obj = {0};
  obj.id = 3;
  obj.urlLen = 256;
  obj.url = (char*)taosMemoryCalloc(obj.urlLen, 1);
  memset(obj.url, 'U', obj.urlLen - 1);
  obj.url[obj.urlLen - 1] = '\0';

  obj.statusLen = 128;
  obj.status = (char*)taosMemoryCalloc(obj.statusLen, 1);
  memset(obj.status, 'S', obj.statusLen - 1);
  obj.status[obj.statusLen - 1] = '\0';

  obj.createTime = INT64_MAX - 1000;
  obj.updateTime = INT64_MAX - 500;

  SSdbRaw* pRaw = mndXnodeActionEncode(&obj);
  ASSERT_NE(pRaw, nullptr);

  SSdbRow* pRow = mndXnodeActionDecode(pRaw);
  ASSERT_NE(pRow, nullptr);

  SXnodeObj* pDecoded = (SXnodeObj*)sdbGetRowObj(pRow);
  ASSERT_NE(pDecoded, nullptr);
  EXPECT_EQ(pDecoded->urlLen, obj.urlLen);
  EXPECT_EQ(memcmp(pDecoded->url, obj.url, obj.urlLen), 0);
  EXPECT_EQ(pDecoded->statusLen, obj.statusLen);
  EXPECT_EQ(memcmp(pDecoded->status, obj.status, obj.statusLen), 0);

  freeXnodeObj(&obj);
  freeXnodeObj(pDecoded);
  taosMemoryFree(pRow);
  sdbFreeRaw(pRaw);
}

// ========== SXnodeTaskObj Production Tests ==========

TEST_F(XnodeProductionTest, XnodeTaskObj_RealEncodeDecode_Complete) {
  SXnodeTaskObj obj = {0};
  obj.id = 100;
  obj.via = 1;
  obj.xnodeId = 10;
  obj.createTime = 5555555;
  obj.updateTime = 6666666;
  obj.sourceType = 1;
  obj.sinkType = 2;

  obj.nameLen = 10;
  obj.name = (char*)taosMemoryCalloc(obj.nameLen, 1);
  strcpy(obj.name, "task_name");

  obj.sourceDsnLen = 15;
  obj.sourceDsn = (char*)taosMemoryCalloc(obj.sourceDsnLen, 1);
  strcpy(obj.sourceDsn, "kafka://source");

  obj.sinkDsnLen = 13;
  obj.sinkDsn = (char*)taosMemoryCalloc(obj.sinkDsnLen, 1);
  strcpy(obj.sinkDsn, "taos://sink");

  obj.parserLen = 11;
  obj.parser = (char*)taosMemoryCalloc(obj.parserLen, 1);
  strcpy(obj.parser, "csv_parser");

  obj.statusLen = 8;
  obj.status = (char*)taosMemoryCalloc(obj.statusLen, 1);
  strcpy(obj.status, "running");

  obj.reasonLen = 0;
  obj.reason = nullptr;

  // Use REAL production encode
  SSdbRaw* pRaw = mndXnodeTaskActionEncode(&obj);
  ASSERT_NE(pRaw, nullptr);
  EXPECT_EQ(pRaw->type, SDB_XNODE_TASK);

  // Use REAL production decode
  SSdbRow* pRow = mndXnodeTaskActionDecode(pRaw);
  ASSERT_NE(pRow, nullptr);

  SXnodeTaskObj* pDecoded = (SXnodeTaskObj*)sdbGetRowObj(pRow);
  ASSERT_NE(pDecoded, nullptr);

  // Verify all fields
  EXPECT_EQ(pDecoded->id, obj.id);
  EXPECT_EQ(pDecoded->via, obj.via);
  EXPECT_EQ(pDecoded->xnodeId, obj.xnodeId);
  EXPECT_EQ(pDecoded->sourceType, obj.sourceType);
  EXPECT_EQ(pDecoded->sinkType, obj.sinkType);
  EXPECT_EQ(pDecoded->nameLen, obj.nameLen);
  EXPECT_STREQ(pDecoded->name, obj.name);
  EXPECT_EQ(pDecoded->sourceDsnLen, obj.sourceDsnLen);
  EXPECT_STREQ(pDecoded->sourceDsn, obj.sourceDsn);
  EXPECT_EQ(pDecoded->sinkDsnLen, obj.sinkDsnLen);
  EXPECT_STREQ(pDecoded->sinkDsn, obj.sinkDsn);
  EXPECT_EQ(pDecoded->parserLen, obj.parserLen);
  EXPECT_STREQ(pDecoded->parser, obj.parser);
  EXPECT_EQ(pDecoded->statusLen, obj.statusLen);
  EXPECT_STREQ(pDecoded->status, obj.status);
  EXPECT_EQ(pDecoded->createTime, obj.createTime);
  EXPECT_EQ(pDecoded->updateTime, obj.updateTime);

  // Cleanup
  freeXnodeTaskObj(&obj);
  freeXnodeTaskObj(pDecoded);
  taosMemoryFree(pRow);
  sdbFreeRaw(pRaw);
}

TEST_F(XnodeProductionTest, XnodeTaskObj_RealEncodeDecode_WithReason) {
  SXnodeTaskObj obj = {0};
  obj.id = 101;
  obj.via = 2;
  obj.xnodeId = 20;
  obj.createTime = 7777777;
  obj.updateTime = 8888888;
  obj.sourceType = 1;
  obj.sinkType = 2;

  obj.nameLen = 6;
  obj.name = (char*)taosMemoryCalloc(obj.nameLen, 1);
  strcpy(obj.name, "task2");

  obj.sourceDsnLen = 5;
  obj.sourceDsn = (char*)taosMemoryCalloc(obj.sourceDsnLen, 1);
  strcpy(obj.sourceDsn, "src1");

  obj.sinkDsnLen = 6;
  obj.sinkDsn = (char*)taosMemoryCalloc(obj.sinkDsnLen, 1);
  strcpy(obj.sinkDsn, "sink1");

  obj.parserLen = 0;
  obj.parser = nullptr;

  obj.statusLen = 7;
  obj.status = (char*)taosMemoryCalloc(obj.statusLen, 1);
  strcpy(obj.status, "failed");

  obj.reasonLen = 15;
  obj.reason = (char*)taosMemoryCalloc(obj.reasonLen, 1);
  strcpy(obj.reason, "timeout_error");

  SSdbRaw* pRaw = mndXnodeTaskActionEncode(&obj);
  ASSERT_NE(pRaw, nullptr);

  SSdbRow* pRow = mndXnodeTaskActionDecode(pRaw);
  ASSERT_NE(pRow, nullptr);

  SXnodeTaskObj* pDecoded = (SXnodeTaskObj*)sdbGetRowObj(pRow);
  ASSERT_NE(pDecoded, nullptr);
  EXPECT_EQ(pDecoded->reasonLen, obj.reasonLen);
  EXPECT_STREQ(pDecoded->reason, obj.reason);
  EXPECT_STREQ(pDecoded->status, obj.status);

  freeXnodeTaskObj(&obj);
  freeXnodeTaskObj(pDecoded);
  taosMemoryFree(pRow);
  sdbFreeRaw(pRaw);
}

// ========== SXnodeJobObj Production Tests ==========

TEST_F(XnodeProductionTest, XnodeJobObj_RealEncodeDecode_Complete) {
  SXnodeJobObj obj = {0};
  obj.id = 200;
  obj.taskId = 100;
  obj.via = 1;
  obj.xnodeId = 10;
  obj.createTime = 3333333;
  obj.updateTime = 4444444;

  obj.configLen = 30;
  obj.config = (char*)taosMemoryCalloc(obj.configLen, 1);
  strcpy(obj.config, "{\"threads\": 4, \"batch\": 10}");

  obj.statusLen = 8;
  obj.status = (char*)taosMemoryCalloc(obj.statusLen, 1);
  strcpy(obj.status, "running");

  obj.reasonLen = 0;
  obj.reason = nullptr;

  // Use REAL production encode
  SSdbRaw* pRaw = mndXnodeJobActionEncode(&obj);
  ASSERT_NE(pRaw, nullptr);
  EXPECT_EQ(pRaw->type, SDB_XNODE_JOB);

  // Use REAL production decode
  SSdbRow* pRow = mndXnodeJobActionDecode(pRaw);
  ASSERT_NE(pRow, nullptr);

  SXnodeJobObj* pDecoded = (SXnodeJobObj*)sdbGetRowObj(pRow);
  ASSERT_NE(pDecoded, nullptr);

  // Verify
  EXPECT_EQ(pDecoded->id, obj.id);
  EXPECT_EQ(pDecoded->taskId, obj.taskId);
  EXPECT_EQ(pDecoded->via, obj.via);
  EXPECT_EQ(pDecoded->xnodeId, obj.xnodeId);
  EXPECT_EQ(pDecoded->configLen, obj.configLen);
  EXPECT_STREQ(pDecoded->config, obj.config);
  EXPECT_EQ(pDecoded->statusLen, obj.statusLen);
  EXPECT_STREQ(pDecoded->status, obj.status);
  EXPECT_EQ(pDecoded->createTime, obj.createTime);
  EXPECT_EQ(pDecoded->updateTime, obj.updateTime);

  freeXnodeJobObj(&obj);
  freeXnodeJobObj(pDecoded);
  taosMemoryFree(pRow);
  sdbFreeRaw(pRaw);
}

TEST_F(XnodeProductionTest, XnodeJobObj_RealEncodeDecode_WithReason) {
  SXnodeJobObj obj = {0};
  obj.id = 201;
  obj.taskId = 101;
  obj.via = 2;
  obj.xnodeId = 20;
  obj.createTime = 5555555;
  obj.updateTime = 6666666;

  obj.configLen = 20;
  obj.config = (char*)taosMemoryCalloc(obj.configLen, 1);
  strcpy(obj.config, "{\"retry\": true}");

  obj.statusLen = 7;
  obj.status = (char*)taosMemoryCalloc(obj.statusLen, 1);
  strcpy(obj.status, "failed");

  obj.reasonLen = 18;
  obj.reason = (char*)taosMemoryCalloc(obj.reasonLen, 1);
  strcpy(obj.reason, "connection_error");

  SSdbRaw* pRaw = mndXnodeJobActionEncode(&obj);
  ASSERT_NE(pRaw, nullptr);

  SSdbRow* pRow = mndXnodeJobActionDecode(pRaw);
  ASSERT_NE(pRow, nullptr);

  SXnodeJobObj* pDecoded = (SXnodeJobObj*)sdbGetRowObj(pRow);
  ASSERT_NE(pDecoded, nullptr);
  EXPECT_EQ(pDecoded->reasonLen, obj.reasonLen);
  EXPECT_STREQ(pDecoded->reason, obj.reason);

  freeXnodeJobObj(&obj);
  freeXnodeJobObj(pDecoded);
  taosMemoryFree(pRow);
  sdbFreeRaw(pRaw);
}

// ========== SXnodeUserPassObj Production Tests ==========

TEST_F(XnodeProductionTest, XnodeUserPassObj_RealEncodeDecode_Complete) {
  SXnodeUserPassObj obj = {0};
  obj.id = 300;
  obj.createTime = 7777777;
  obj.updateTime = 8888888;

  obj.userLen = 10;
  obj.user = (char*)taosMemoryCalloc(obj.userLen, 1);
  strcpy(obj.user, "testuser1");

  obj.passLen = 15;
  obj.pass = (char*)taosMemoryCalloc(obj.passLen, 1);
  strcpy(obj.pass, "securepass123");

  // Use REAL production encode
  SSdbRaw* pRaw = mndXnodeUserPassActionEncode(&obj);
  ASSERT_NE(pRaw, nullptr);
  EXPECT_EQ(pRaw->type, SDB_XNODE_USER_PASS);

  // Use REAL production decode
  SSdbRow* pRow = mndXnodeUserPassActionDecode(pRaw);
  ASSERT_NE(pRow, nullptr);

  SXnodeUserPassObj* pDecoded = (SXnodeUserPassObj*)sdbGetRowObj(pRow);
  ASSERT_NE(pDecoded, nullptr);

  // Verify
  EXPECT_EQ(pDecoded->id, obj.id);
  EXPECT_EQ(pDecoded->userLen, obj.userLen);
  EXPECT_STREQ(pDecoded->user, obj.user);
  EXPECT_EQ(pDecoded->passLen, obj.passLen);
  EXPECT_STREQ(pDecoded->pass, obj.pass);
  EXPECT_EQ(pDecoded->createTime, obj.createTime);
  EXPECT_EQ(pDecoded->updateTime, obj.updateTime);

  freeXnodeUserPassObj(&obj);
  freeXnodeUserPassObj(pDecoded);
  taosMemoryFree(pRow);
  sdbFreeRaw(pRaw);
}

TEST_F(XnodeProductionTest, XnodeUserPassObj_RealEncodeDecode_EmptyFields) {
  SXnodeUserPassObj obj = {0};
  obj.id = 301;
  obj.userLen = 0;
  obj.user = nullptr;
  obj.passLen = 0;
  obj.pass = nullptr;
  obj.createTime = 1111111;
  obj.updateTime = 2222222;

  SSdbRaw* pRaw = mndXnodeUserPassActionEncode(&obj);
  ASSERT_NE(pRaw, nullptr);

  SSdbRow* pRow = mndXnodeUserPassActionDecode(pRaw);
  ASSERT_NE(pRow, nullptr);

  SXnodeUserPassObj* pDecoded = (SXnodeUserPassObj*)sdbGetRowObj(pRow);
  ASSERT_NE(pDecoded, nullptr);
  EXPECT_EQ(pDecoded->id, obj.id);
  EXPECT_EQ(pDecoded->userLen, 0);
  EXPECT_EQ(pDecoded->user, nullptr);
  EXPECT_EQ(pDecoded->passLen, 0);
  EXPECT_EQ(pDecoded->pass, nullptr);

  freeXnodeUserPassObj(pDecoded);
  taosMemoryFree(pRow);
  sdbFreeRaw(pRaw);
}

TEST_F(XnodeProductionTest, XnodeUserPassObj_RealEncodeDecode_LongCredentials) {
  SXnodeUserPassObj obj = {0};
  obj.id = 302;
  obj.createTime = 3333333;
  obj.updateTime = 4444444;

  obj.userLen = 64;
  obj.user = (char*)taosMemoryCalloc(obj.userLen, 1);
  memset(obj.user, 'u', obj.userLen - 1);
  obj.user[obj.userLen - 1] = '\0';

  obj.passLen = 128;
  obj.pass = (char*)taosMemoryCalloc(obj.passLen, 1);
  memset(obj.pass, 'p', obj.passLen - 1);
  obj.pass[obj.passLen - 1] = '\0';

  SSdbRaw* pRaw = mndXnodeUserPassActionEncode(&obj);
  ASSERT_NE(pRaw, nullptr);

  SSdbRow* pRow = mndXnodeUserPassActionDecode(pRaw);
  ASSERT_NE(pRow, nullptr);

  SXnodeUserPassObj* pDecoded = (SXnodeUserPassObj*)sdbGetRowObj(pRow);
  ASSERT_NE(pDecoded, nullptr);
  EXPECT_EQ(pDecoded->userLen, obj.userLen);
  EXPECT_EQ(memcmp(pDecoded->user, obj.user, obj.userLen), 0);
  EXPECT_EQ(pDecoded->passLen, obj.passLen);
  EXPECT_EQ(memcmp(pDecoded->pass, obj.pass, obj.passLen), 0);

  freeXnodeUserPassObj(&obj);
  freeXnodeUserPassObj(pDecoded);
  taosMemoryFree(pRow);
  sdbFreeRaw(pRaw);
}

// ========== Edge Cases and Error Handling ==========

TEST_F(XnodeProductionTest, AllObjects_NullEncodeInput) {
  // Test that encode functions handle nullptr gracefully
  EXPECT_EQ(mndXnodeActionEncode(nullptr), nullptr);
  EXPECT_EQ(mndXnodeTaskActionEncode(nullptr), nullptr);
  EXPECT_EQ(mndXnodeJobActionEncode(nullptr), nullptr);
  EXPECT_EQ(mndXnodeUserPassActionEncode(nullptr), nullptr);
}

TEST_F(XnodeProductionTest, AllObjects_NullDecodeInput) {
  // Test that decode functions handle nullptr gracefully
  EXPECT_EQ(mndXnodeActionDecode(nullptr), nullptr);
  EXPECT_EQ(mndXnodeTaskActionDecode(nullptr), nullptr);
  EXPECT_EQ(mndXnodeJobActionDecode(nullptr), nullptr);
  EXPECT_EQ(mndXnodeUserPassActionDecode(nullptr), nullptr);
}

TEST_F(XnodeProductionTest, MultipleObjects_Sequential) {
  // Test encoding/decoding multiple objects sequentially
  std::vector<SSdbRaw*> rawList;
  std::vector<SSdbRow*> rowList;

  // Create 10 XnodeObj objects
  for (int i = 0; i < 10; i++) {
    SXnodeObj obj = {0};
    obj.id = i;
    obj.urlLen = 15;
    obj.url = (char*)taosMemoryCalloc(obj.urlLen, 1);
    snprintf(obj.url, obj.urlLen, "xnode%d:6051", i);
    obj.statusLen = 7;
    obj.status = (char*)taosMemoryCalloc(obj.statusLen, 1);
    strcpy(obj.status, "online");
    obj.createTime = 1000000 + i;
    obj.updateTime = 2000000 + i;

    SSdbRaw* pRaw = mndXnodeActionEncode(&obj);
    ASSERT_NE(pRaw, nullptr);
    rawList.push_back(pRaw);

    freeXnodeObj(&obj);
  }

  // Decode all objects
  for (auto pRaw : rawList) {
    SSdbRow* pRow = mndXnodeActionDecode(pRaw);
    ASSERT_NE(pRow, nullptr);
    rowList.push_back(pRow);
  }

  // Verify
  for (size_t i = 0; i < rowList.size(); i++) {
    SXnodeObj* pObj = (SXnodeObj*)sdbGetRowObj(rowList[i]);
    ASSERT_NE(pObj, nullptr);
    EXPECT_EQ(pObj->id, (int)i);
    freeXnodeObj(pObj);
  }

  // Cleanup
  for (auto pRaw : rawList) sdbFreeRaw(pRaw);
  for (auto pRow : rowList) taosMemoryFree(pRow);
}
