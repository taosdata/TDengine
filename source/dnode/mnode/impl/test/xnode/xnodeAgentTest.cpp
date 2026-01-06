/**
 * @file xnodeAgentTest.cpp
 * @brief Test XnodeAgent objects using REAL production encode/decode functions from mndXnode.c
 * @version 1.0
 * @date 2025-12-25
 *
 * This test uses actual production encode/decode functions by linking against mnode library.
 * Tests cover all four XnodeAgent object types with real SDB operations.
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
typedef struct {
    int64_t sub;  // agent ID
    int64_t iat;  // issued at time
} agentTokenField;

const unsigned char MNDXNODE_DEFAULT_SECRET[] = {126, 222, 130, 137, 43,  122, 41,  173, 144, 146, 116,
                                                 138, 153, 244, 251, 99,  50,  55,  140, 238, 218, 232,
                                                 15,  161, 226, 54,  130, 40,  211, 234, 111, 171};

SSdbRaw* mndXnodeAgentActionEncode(SXnodeAgentObj* pObj);
SSdbRow* mndXnodeAgentActionDecode(SSdbRaw* pRaw);

char *mndXnodeCreateAgentToken(const agentTokenField *claims, const unsigned char *secret, size_t secret_len);
}

class XnodeAgentTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}

  // Helper to free XnodeObj
  void freeXnodeObj(SXnodeAgentObj* pObj) {
    if (!pObj) return;
    if (pObj->name) taosMemoryFree(pObj->name);
    if (pObj->token) taosMemoryFree(pObj->token);
    if (pObj->status) taosMemoryFree(pObj->status);
  }
};

// ========== SXnodeAgent Tests ==========

TEST_F(XnodeAgentTest, xnode_agent_agent_jwt_token) {
  // Create a real XnodeObj
  SXnodeAgentObj obj = {0};
  obj.id = 12345;
  obj.nameLen = 10;
  obj.name = (char*)taosMemoryCalloc(obj.nameLen, 1);
  strcpy(obj.name, "agent1");
  obj.statusLen = 7;
  obj.status = (char*)taosMemoryCalloc(obj.statusLen, 1);
  strcpy(obj.status, "online");
  obj.createTime = 1767615162;
  obj.updateTime = 1767615162;

  // create agent token
  agentTokenField claims = {
      .sub = obj.id,
      .iat = obj.createTime,
  };
  obj.token = mndXnodeCreateAgentToken(&claims, MNDXNODE_DEFAULT_SECRET, sizeof(MNDXNODE_DEFAULT_SECRET));
  ASSERT_STREQ(obj.token, "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3Njc2MTUxNjIsInN1YiI6MTIzNDV9.wFyF2ydHRxuzb4yDJnMbiIX2Q2d6NlJTPi9YutkS1GI");
  obj.tokenLen = strlen(obj.token) + 1;

  // Use REAL production encode function
  SSdbRaw* pRaw = mndXnodeAgentActionEncode(&obj);
  ASSERT_NE(pRaw, nullptr);
  EXPECT_EQ(pRaw->type, SDB_XNODE_AGENT);

  // Use REAL production decode function
  SSdbRow* pRow = mndXnodeAgentActionDecode(pRaw);
  ASSERT_NE(pRow, nullptr);

  SXnodeAgentObj* pDecoded = (SXnodeAgentObj*)sdbGetRowObj(pRow);
  ASSERT_NE(pDecoded, nullptr);

  // Verify decoded data matches original
  EXPECT_EQ(pDecoded->id, obj.id);
  EXPECT_EQ(pDecoded->nameLen, obj.nameLen);
  EXPECT_STREQ(pDecoded->name, obj.name);
  EXPECT_EQ(pDecoded->tokenLen, obj.tokenLen);
  EXPECT_STREQ(pDecoded->token, obj.token);
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

TEST_F(XnodeAgentTest, xnode_agent_null_test) {
  // Create a real XnodeObj
  SXnodeAgentObj obj = {0};
  obj.id = 7;
  obj.nameLen = 10;
  obj.name = (char*)taosMemoryCalloc(obj.nameLen, 1);
  strcpy(obj.name, "agent2");
  obj.statusLen = 0;
  obj.status = NULL;
  obj.createTime = 1234567890;
  obj.updateTime = 1767615162;

  // create agent token
  agentTokenField claims = {
      .sub = obj.id,
      .iat = obj.createTime,
  };
  obj.token = mndXnodeCreateAgentToken(&claims, MNDXNODE_DEFAULT_SECRET, sizeof(MNDXNODE_DEFAULT_SECRET));
  ASSERT_STREQ(obj.token, "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjEyMzQ1Njc4OTAsInN1YiI6N30.Ig7yhv-EBSlKavSVOQQ-jrVTNhFcn3k7f-bglpWZCHo");
  obj.tokenLen = strlen(obj.token) + 1;

  // Use REAL production encode function
  SSdbRaw* pRaw = mndXnodeAgentActionEncode(&obj);
  ASSERT_NE(pRaw, nullptr);
  EXPECT_EQ(pRaw->type, SDB_XNODE_AGENT);

  // Use REAL production decode function
  SSdbRow* pRow = mndXnodeAgentActionDecode(pRaw);
  ASSERT_NE(pRow, nullptr);

  SXnodeAgentObj* pDecoded = (SXnodeAgentObj*)sdbGetRowObj(pRow);
  ASSERT_NE(pDecoded, nullptr);

  // Verify decoded data matches original
  EXPECT_EQ(pDecoded->id, obj.id);
  EXPECT_EQ(pDecoded->nameLen, obj.nameLen);
  EXPECT_STREQ(pDecoded->name, obj.name);
  EXPECT_EQ(pDecoded->tokenLen, obj.tokenLen);
  EXPECT_STREQ(pDecoded->token, obj.token);
  EXPECT_EQ(pDecoded->statusLen, obj.statusLen);
  EXPECT_EQ(pDecoded->status, nullptr);
  EXPECT_EQ(pDecoded->createTime, obj.createTime);
  EXPECT_EQ(pDecoded->updateTime, obj.updateTime);

  // Cleanup
  freeXnodeObj(&obj);
  freeXnodeObj(pDecoded);
  taosMemoryFree(pRow);
  sdbFreeRaw(pRaw);
}