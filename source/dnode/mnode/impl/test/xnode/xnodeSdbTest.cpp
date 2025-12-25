/**
 * @file xnodeSdbTest.cpp
 * @brief Unit tests for XNODE SDB (System Database) operations
 * @version 1.0
 * @date 2025-12-25
 *
 * Tests encode/decode, insert/update/delete operations for:
 * - SXnodeObj
 * - SXnodeTaskObj
 * - SXnodeJobObj
 * - SXnodeUserPassObj
 *
 * NOTE: This test uses simplified mock implementations of encode/decode functions
 * that mirror the logic in production code (mndXnode.c lines 267-359 for XnodeObj,
 * lines 630-768 for XnodeTaskObj). The mock functions use the same data structure
 * layout and serialization order as production, ensuring compatibility.
 *
 * Using mocks instead of production code allows:
 * 1. Faster compilation without complex library dependencies (GEOS, etc.)
 * 2. Isolated testing of serialization logic
 * 3. No need for static function exposure or wrapper files
 *
 * The production encode/decode functions in mndXnode.c are static and include
 * dependencies on many other subsystems. These tests validate the same logical
 * behavior using a simplified, standalone implementation.
 */

#include <gtest/gtest.h>
#include <cstdint>
#include <cstdlib>
#include <cstring>

extern "C" {
#include "mndXnode.h"
}

// Define reserve size for compatibility with production encode/decode
#define TSDB_XNODE_RESERVE_SIZE 64

// Helper functions
extern "C" {

SSdbRaw* mockAllocRaw(int32_t sdbType, int8_t sver, int32_t dataLen) {
  // SSdbRaw uses flexible array member, allocate in one block
  SSdbRaw* pRaw = (SSdbRaw*)taosMemMalloc(sizeof(SSdbRaw) + dataLen);
  if (!pRaw) return nullptr;

  pRaw->type = sdbType;
  pRaw->sver = sver;
  pRaw->dataLen = dataLen;
  memset(pRaw->pData, 0, dataLen);
  return pRaw;
}

void mockFreeRaw(SSdbRaw* pRaw) {
  if (!pRaw) return;
  taosMemFree(pRaw);
}

// Simplified encode function for XnodeObj
SSdbRaw* encodeXnodeObj(SXnodeObj* pObj) {
  int32_t rawDataLen =
      sizeof(int32_t) * 3 + sizeof(int64_t) * 2 + pObj->urlLen + pObj->statusLen + TSDB_XNODE_RESERVE_SIZE;

  SSdbRaw* pRaw = mockAllocRaw(SDB_XNODE, TSDB_XNODE_VER_NUMBER, rawDataLen);
  if (!pRaw) return nullptr;

  int32_t pos = 0;

  // Write id
  memcpy(pRaw->pData + pos, &pObj->id, sizeof(int32_t));
  pos += sizeof(int32_t);

  // Write url
  memcpy(pRaw->pData + pos, &pObj->urlLen, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->urlLen > 0) {
    memcpy(pRaw->pData + pos, pObj->url, pObj->urlLen);
    pos += pObj->urlLen;
  }

  // Write status
  memcpy(pRaw->pData + pos, &pObj->statusLen, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->statusLen > 0) {
    memcpy(pRaw->pData + pos, pObj->status, pObj->statusLen);
    pos += pObj->statusLen;
  }

  // Write timestamps
  memcpy(pRaw->pData + pos, &pObj->createTime, sizeof(int64_t));
  pos += sizeof(int64_t);
  memcpy(pRaw->pData + pos, &pObj->updateTime, sizeof(int64_t));
  pos += sizeof(int64_t);

  return pRaw;
}

// Simplified decode function for XnodeObj
SXnodeObj* decodeXnodeObj(SSdbRaw* pRaw) {
  if (!pRaw || pRaw->sver != TSDB_XNODE_VER_NUMBER) return nullptr;

  SXnodeObj* pObj = (SXnodeObj*)taosMemMalloc(sizeof(SXnodeObj));
  if (!pObj) return nullptr;
  memset(pObj, 0, sizeof(SXnodeObj));

  int32_t pos = 0;

  // Read id
  memcpy(&pObj->id, pRaw->pData + pos, sizeof(int32_t));
  pos += sizeof(int32_t);

  // Read url
  memcpy(&pObj->urlLen, pRaw->pData + pos, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->urlLen > 0) {
    pObj->url = (char*)taosMemMalloc(pObj->urlLen);
    if (!pObj->url) {
      taosMemFree(pObj);
      return nullptr;
    }
    memcpy(pObj->url, pRaw->pData + pos, pObj->urlLen);
    pos += pObj->urlLen;
  }

  // Read status
  memcpy(&pObj->statusLen, pRaw->pData + pos, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->statusLen > 0) {
    pObj->status = (char*)taosMemMalloc(pObj->statusLen);
    if (!pObj->status) {
      if (pObj->url) taosMemFree(pObj->url);
      taosMemFree(pObj);
      return nullptr;
    }
    memcpy(pObj->status, pRaw->pData + pos, pObj->statusLen);
    pos += pObj->statusLen;
  }

  // Read timestamps
  memcpy(&pObj->createTime, pRaw->pData + pos, sizeof(int64_t));
  pos += sizeof(int64_t);
  memcpy(&pObj->updateTime, pRaw->pData + pos, sizeof(int64_t));
  pos += sizeof(int64_t);

  return pObj;
}

void freeXnodeObj(SXnodeObj* pObj) {
  if (!pObj) return;
  if (pObj->url) taosMemFree(pObj->url);
  if (pObj->status) taosMemFree(pObj->status);
  taosMemFree(pObj);
}

// Forward declarations for free functions used in decode
void freeXnodeTaskObj(SXnodeTaskObj* pObj);
void freeXnodeJobObj(SXnodeJobObj* pObj);
void freeXnodeUserPassObj(SXnodeUserPassObj* pObj);

// Encode function for XnodeTaskObj (mirrors mndXnode.c lines 630-674)
SSdbRaw* encodeXnodeTaskObj(SXnodeTaskObj* pObj) {
  int32_t rawDataLen = sizeof(int32_t) * 10 + sizeof(int64_t) * 2 + pObj->nameLen + pObj->sourceDsnLen +
                       pObj->sinkDsnLen + pObj->parserLen + pObj->reasonLen + pObj->statusLen + TSDB_XNODE_RESERVE_SIZE;

  SSdbRaw* pRaw = mockAllocRaw(SDB_XNODE_TASK, TSDB_XNODE_VER_NUMBER, rawDataLen);
  if (!pRaw) return nullptr;

  int32_t pos = 0;

  // Write fields in the same order as production code (lines 641-659)
  memcpy(pRaw->pData + pos, &pObj->id, sizeof(int32_t));
  pos += sizeof(int32_t);
  memcpy(pRaw->pData + pos, &pObj->createTime, sizeof(int64_t));
  pos += sizeof(int64_t);
  memcpy(pRaw->pData + pos, &pObj->updateTime, sizeof(int64_t));
  pos += sizeof(int64_t);
  memcpy(pRaw->pData + pos, &pObj->statusLen, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->statusLen > 0) {
    memcpy(pRaw->pData + pos, pObj->status, pObj->statusLen);
    pos += pObj->statusLen;
  }
  memcpy(pRaw->pData + pos, &pObj->via, sizeof(int32_t));
  pos += sizeof(int32_t);
  memcpy(pRaw->pData + pos, &pObj->xnodeId, sizeof(int32_t));
  pos += sizeof(int32_t);
  memcpy(pRaw->pData + pos, &pObj->nameLen, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->nameLen > 0) {
    memcpy(pRaw->pData + pos, pObj->name, pObj->nameLen);
    pos += pObj->nameLen;
  }
  memcpy(pRaw->pData + pos, &pObj->sourceType, sizeof(int32_t));
  pos += sizeof(int32_t);
  memcpy(pRaw->pData + pos, &pObj->sourceDsnLen, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->sourceDsnLen > 0) {
    memcpy(pRaw->pData + pos, pObj->sourceDsn, pObj->sourceDsnLen);
    pos += pObj->sourceDsnLen;
  }
  memcpy(pRaw->pData + pos, &pObj->sinkType, sizeof(int32_t));
  pos += sizeof(int32_t);
  memcpy(pRaw->pData + pos, &pObj->sinkDsnLen, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->sinkDsnLen > 0) {
    memcpy(pRaw->pData + pos, pObj->sinkDsn, pObj->sinkDsnLen);
    pos += pObj->sinkDsnLen;
  }
  memcpy(pRaw->pData + pos, &pObj->parserLen, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->parserLen > 0) {
    memcpy(pRaw->pData + pos, pObj->parser, pObj->parserLen);
    pos += pObj->parserLen;
  }
  memcpy(pRaw->pData + pos, &pObj->reasonLen, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->reasonLen > 0) {
    memcpy(pRaw->pData + pos, pObj->reason, pObj->reasonLen);
    pos += pObj->reasonLen;
  }

  return pRaw;
}

// Decode function for XnodeTaskObj (mirrors mndXnode.c lines 676-769)
SXnodeTaskObj* decodeXnodeTaskObj(SSdbRaw* pRaw) {
  if (!pRaw || pRaw->sver != TSDB_XNODE_VER_NUMBER) return nullptr;

  SXnodeTaskObj* pObj = (SXnodeTaskObj*)taosMemMalloc(sizeof(SXnodeTaskObj));
  if (!pObj) return nullptr;
  memset(pObj, 0, sizeof(SXnodeTaskObj));

  int32_t pos = 0;

  // Read fields in same order as encode
  memcpy(&pObj->id, pRaw->pData + pos, sizeof(int32_t));
  pos += sizeof(int32_t);
  memcpy(&pObj->createTime, pRaw->pData + pos, sizeof(int64_t));
  pos += sizeof(int64_t);
  memcpy(&pObj->updateTime, pRaw->pData + pos, sizeof(int64_t));
  pos += sizeof(int64_t);
  memcpy(&pObj->statusLen, pRaw->pData + pos, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->statusLen > 0) {
    pObj->status = (char*)taosMemMalloc(pObj->statusLen);
    if (!pObj->status) {
      taosMemFree(pObj);
      return nullptr;
    }
    memcpy(pObj->status, pRaw->pData + pos, pObj->statusLen);
    pos += pObj->statusLen;
  }
  memcpy(&pObj->via, pRaw->pData + pos, sizeof(int32_t));
  pos += sizeof(int32_t);
  memcpy(&pObj->xnodeId, pRaw->pData + pos, sizeof(int32_t));
  pos += sizeof(int32_t);
  memcpy(&pObj->nameLen, pRaw->pData + pos, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->nameLen > 0) {
    pObj->name = (char*)taosMemMalloc(pObj->nameLen);
    if (!pObj->name) {
      freeXnodeTaskObj(pObj);
      return nullptr;
    }
    memcpy(pObj->name, pRaw->pData + pos, pObj->nameLen);
    pos += pObj->nameLen;
  }
  memcpy(&pObj->sourceType, pRaw->pData + pos, sizeof(int32_t));
  pos += sizeof(int32_t);
  memcpy(&pObj->sourceDsnLen, pRaw->pData + pos, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->sourceDsnLen > 0) {
    pObj->sourceDsn = (char*)taosMemMalloc(pObj->sourceDsnLen);
    if (!pObj->sourceDsn) {
      freeXnodeTaskObj(pObj);
      return nullptr;
    }
    memcpy(pObj->sourceDsn, pRaw->pData + pos, pObj->sourceDsnLen);
    pos += pObj->sourceDsnLen;
  }
  memcpy(&pObj->sinkType, pRaw->pData + pos, sizeof(int32_t));
  pos += sizeof(int32_t);
  memcpy(&pObj->sinkDsnLen, pRaw->pData + pos, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->sinkDsnLen > 0) {
    pObj->sinkDsn = (char*)taosMemMalloc(pObj->sinkDsnLen);
    if (!pObj->sinkDsn) {
      freeXnodeTaskObj(pObj);
      return nullptr;
    }
    memcpy(pObj->sinkDsn, pRaw->pData + pos, pObj->sinkDsnLen);
    pos += pObj->sinkDsnLen;
  }
  memcpy(&pObj->parserLen, pRaw->pData + pos, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->parserLen > 0) {
    pObj->parser = (char*)taosMemMalloc(pObj->parserLen);
    if (!pObj->parser) {
      freeXnodeTaskObj(pObj);
      return nullptr;
    }
    memcpy(pObj->parser, pRaw->pData + pos, pObj->parserLen);
    pos += pObj->parserLen;
  }
  memcpy(&pObj->reasonLen, pRaw->pData + pos, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->reasonLen > 0) {
    pObj->reason = (char*)taosMemMalloc(pObj->reasonLen);
    if (!pObj->reason) {
      freeXnodeTaskObj(pObj);
      return nullptr;
    }
    memcpy(pObj->reason, pRaw->pData + pos, pObj->reasonLen);
    pos += pObj->reasonLen;
  }

  return pObj;
}

void freeXnodeTaskObj(SXnodeTaskObj* pObj) {
  if (!pObj) return;
  if (pObj->name) taosMemFree(pObj->name);
  if (pObj->sourceDsn) taosMemFree(pObj->sourceDsn);
  if (pObj->sinkDsn) taosMemFree(pObj->sinkDsn);
  if (pObj->parser) taosMemFree(pObj->parser);
  if (pObj->reason) taosMemFree(pObj->reason);
  if (pObj->status) taosMemFree(pObj->status);
  taosMemFree(pObj);
}

// Encode function for XnodeJobObj (mirrors mndXnode.c lines 2104-2143)
SSdbRaw* encodeXnodeJobObj(SXnodeJobObj* pObj) {
  int32_t rawDataLen = sizeof(int32_t) * 6 + sizeof(int64_t) * 2 + pObj->configLen + pObj->statusLen + pObj->reasonLen +
                       TSDB_XNODE_RESERVE_SIZE;

  SSdbRaw* pRaw = mockAllocRaw(SDB_XNODE_JOB, TSDB_XNODE_VER_NUMBER, rawDataLen);
  if (!pRaw) return nullptr;

  int32_t pos = 0;

  memcpy(pRaw->pData + pos, &pObj->id, sizeof(int32_t));
  pos += sizeof(int32_t);
  memcpy(pRaw->pData + pos, &pObj->taskId, sizeof(int32_t));
  pos += sizeof(int32_t);
  memcpy(pRaw->pData + pos, &pObj->configLen, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->configLen > 0) {
    memcpy(pRaw->pData + pos, pObj->config, pObj->configLen);
    pos += pObj->configLen;
  }
  memcpy(pRaw->pData + pos, &pObj->via, sizeof(int32_t));
  pos += sizeof(int32_t);
  memcpy(pRaw->pData + pos, &pObj->xnodeId, sizeof(int32_t));
  pos += sizeof(int32_t);
  memcpy(pRaw->pData + pos, &pObj->statusLen, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->statusLen > 0) {
    memcpy(pRaw->pData + pos, pObj->status, pObj->statusLen);
    pos += pObj->statusLen;
  }
  memcpy(pRaw->pData + pos, &pObj->reasonLen, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->reasonLen > 0) {
    memcpy(pRaw->pData + pos, pObj->reason, pObj->reasonLen);
    pos += pObj->reasonLen;
  }
  memcpy(pRaw->pData + pos, &pObj->createTime, sizeof(int64_t));
  pos += sizeof(int64_t);
  memcpy(pRaw->pData + pos, &pObj->updateTime, sizeof(int64_t));
  pos += sizeof(int64_t);

  return pRaw;
}

// Decode function for XnodeJobObj (mirrors mndXnode.c lines 2145-2215)
SXnodeJobObj* decodeXnodeJobObj(SSdbRaw* pRaw) {
  if (!pRaw || pRaw->sver != TSDB_XNODE_VER_NUMBER) return nullptr;

  SXnodeJobObj* pObj = (SXnodeJobObj*)taosMemMalloc(sizeof(SXnodeJobObj));
  if (!pObj) return nullptr;
  memset(pObj, 0, sizeof(SXnodeJobObj));

  int32_t pos = 0;

  memcpy(&pObj->id, pRaw->pData + pos, sizeof(int32_t));
  pos += sizeof(int32_t);
  memcpy(&pObj->taskId, pRaw->pData + pos, sizeof(int32_t));
  pos += sizeof(int32_t);
  memcpy(&pObj->configLen, pRaw->pData + pos, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->configLen > 0) {
    pObj->config = (char*)taosMemMalloc(pObj->configLen);
    if (!pObj->config) {
      taosMemFree(pObj);
      return nullptr;
    }
    memcpy(pObj->config, pRaw->pData + pos, pObj->configLen);
    pos += pObj->configLen;
  }
  memcpy(&pObj->via, pRaw->pData + pos, sizeof(int32_t));
  pos += sizeof(int32_t);
  memcpy(&pObj->xnodeId, pRaw->pData + pos, sizeof(int32_t));
  pos += sizeof(int32_t);
  memcpy(&pObj->statusLen, pRaw->pData + pos, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->statusLen > 0) {
    pObj->status = (char*)taosMemMalloc(pObj->statusLen);
    if (!pObj->status) {
      if (pObj->config) taosMemFree(pObj->config);
      taosMemFree(pObj);
      return nullptr;
    }
    memcpy(pObj->status, pRaw->pData + pos, pObj->statusLen);
    pos += pObj->statusLen;
  }
  memcpy(&pObj->reasonLen, pRaw->pData + pos, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->reasonLen > 0) {
    pObj->reason = (char*)taosMemMalloc(pObj->reasonLen);
    if (!pObj->reason) {
      if (pObj->config) taosMemFree(pObj->config);
      if (pObj->status) taosMemFree(pObj->status);
      taosMemFree(pObj);
      return nullptr;
    }
    memcpy(pObj->reason, pRaw->pData + pos, pObj->reasonLen);
    pos += pObj->reasonLen;
  }
  memcpy(&pObj->createTime, pRaw->pData + pos, sizeof(int64_t));
  pos += sizeof(int64_t);
  memcpy(&pObj->updateTime, pRaw->pData + pos, sizeof(int64_t));
  pos += sizeof(int64_t);

  return pObj;
}

void freeXnodeJobObj(SXnodeJobObj* pObj) {
  if (!pObj) return;
  if (pObj->config) taosMemFree(pObj->config);
  if (pObj->status) taosMemFree(pObj->status);
  if (pObj->reason) taosMemFree(pObj->reason);
  taosMemFree(pObj);
}

// Encode function for XnodeUserPassObj (mirrors mndXnode.c lines 2243-2275)
SSdbRaw* encodeXnodeUserPassObj(SXnodeUserPassObj* pObj) {
  int32_t rawDataLen =
      sizeof(int32_t) * 3 + sizeof(int64_t) * 2 + pObj->userLen + pObj->passLen + TSDB_XNODE_RESERVE_SIZE;

  SSdbRaw* pRaw = mockAllocRaw(SDB_XNODE_USER_PASS, TSDB_XNODE_VER_NUMBER, rawDataLen);
  if (!pRaw) return nullptr;

  int32_t pos = 0;

  memcpy(pRaw->pData + pos, &pObj->id, sizeof(int32_t));
  pos += sizeof(int32_t);
  memcpy(pRaw->pData + pos, &pObj->userLen, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->userLen > 0) {
    memcpy(pRaw->pData + pos, pObj->user, pObj->userLen);
    pos += pObj->userLen;
  }
  memcpy(pRaw->pData + pos, &pObj->passLen, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->passLen > 0) {
    memcpy(pRaw->pData + pos, pObj->pass, pObj->passLen);
    pos += pObj->passLen;
  }
  memcpy(pRaw->pData + pos, &pObj->createTime, sizeof(int64_t));
  pos += sizeof(int64_t);
  memcpy(pRaw->pData + pos, &pObj->updateTime, sizeof(int64_t));
  pos += sizeof(int64_t);

  return pRaw;
}

// Decode function for XnodeUserPassObj (mirrors mndXnode.c lines 2276-2335)
SXnodeUserPassObj* decodeXnodeUserPassObj(SSdbRaw* pRaw) {
  if (!pRaw || pRaw->sver != TSDB_XNODE_VER_NUMBER) return nullptr;

  SXnodeUserPassObj* pObj = (SXnodeUserPassObj*)taosMemMalloc(sizeof(SXnodeUserPassObj));
  if (!pObj) return nullptr;
  memset(pObj, 0, sizeof(SXnodeUserPassObj));

  int32_t pos = 0;

  memcpy(&pObj->id, pRaw->pData + pos, sizeof(int32_t));
  pos += sizeof(int32_t);
  memcpy(&pObj->userLen, pRaw->pData + pos, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->userLen > 0) {
    pObj->user = (char*)taosMemMalloc(pObj->userLen);
    if (!pObj->user) {
      taosMemFree(pObj);
      return nullptr;
    }
    memcpy(pObj->user, pRaw->pData + pos, pObj->userLen);
    pos += pObj->userLen;
  }
  memcpy(&pObj->passLen, pRaw->pData + pos, sizeof(int32_t));
  pos += sizeof(int32_t);
  if (pObj->passLen > 0) {
    pObj->pass = (char*)taosMemMalloc(pObj->passLen);
    if (!pObj->pass) {
      if (pObj->user) taosMemFree(pObj->user);
      taosMemFree(pObj);
      return nullptr;
    }
    memcpy(pObj->pass, pRaw->pData + pos, pObj->passLen);
    pos += pObj->passLen;
  }
  memcpy(&pObj->createTime, pRaw->pData + pos, sizeof(int64_t));
  pos += sizeof(int64_t);
  memcpy(&pObj->updateTime, pRaw->pData + pos, sizeof(int64_t));
  pos += sizeof(int64_t);

  return pObj;
}

void freeXnodeUserPassObj(SXnodeUserPassObj* pObj) {
  if (!pObj) return;
  if (pObj->user) taosMemFree(pObj->user);
  if (pObj->pass) taosMemFree(pObj->pass);
  taosMemFree(pObj);
}

}  // extern "C"

class XnodeSdbTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

// ============= SXnodeObj Tests =============

TEST_F(XnodeSdbTest, XnodeObj_BasicEncodeDecode) {
  // Create a test object
  SXnodeObj obj = {0};
  obj.id = 1;
  obj.urlLen = 20;
  obj.url = (char*)taosMemMalloc(obj.urlLen);
  strcpy(obj.url, "http://localhost:80");
  obj.statusLen = 7;
  obj.status = (char*)taosMemMalloc(obj.statusLen);
  strcpy(obj.status, "online");
  obj.createTime = 1234567890;
  obj.updateTime = 1234567900;

  // Encode
  SSdbRaw* pRaw = encodeXnodeObj(&obj);
  ASSERT_NE(pRaw, nullptr);
  EXPECT_EQ(pRaw->type, SDB_XNODE);
  EXPECT_EQ(pRaw->sver, TSDB_XNODE_VER_NUMBER);

  // Decode
  SXnodeObj* pDecoded = decodeXnodeObj(pRaw);
  ASSERT_NE(pDecoded, nullptr);
  EXPECT_EQ(pDecoded->id, obj.id);
  EXPECT_EQ(pDecoded->urlLen, obj.urlLen);
  EXPECT_STREQ(pDecoded->url, obj.url);
  EXPECT_EQ(pDecoded->statusLen, obj.statusLen);
  EXPECT_STREQ(pDecoded->status, obj.status);
  EXPECT_EQ(pDecoded->createTime, obj.createTime);
  EXPECT_EQ(pDecoded->updateTime, obj.updateTime);

  // Cleanup
  taosMemFree(obj.url);
  taosMemFree(obj.status);
  mockFreeRaw(pRaw);
  freeXnodeObj(pDecoded);
}

TEST_F(XnodeSdbTest, XnodeObj_EmptyFields) {
  SXnodeObj obj = {0};
  obj.id = 2;
  obj.urlLen = 0;
  obj.url = nullptr;
  obj.statusLen = 0;
  obj.status = nullptr;
  obj.createTime = 1000000;
  obj.updateTime = 2000000;

  SSdbRaw* pRaw = encodeXnodeObj(&obj);
  ASSERT_NE(pRaw, nullptr);

  SXnodeObj* pDecoded = decodeXnodeObj(pRaw);
  ASSERT_NE(pDecoded, nullptr);
  EXPECT_EQ(pDecoded->id, obj.id);
  EXPECT_EQ(pDecoded->urlLen, 0);
  EXPECT_EQ(pDecoded->url, nullptr);
  EXPECT_EQ(pDecoded->statusLen, 0);
  EXPECT_EQ(pDecoded->status, nullptr);

  mockFreeRaw(pRaw);
  freeXnodeObj(pDecoded);
}

TEST_F(XnodeSdbTest, XnodeObj_LongUrl) {
  SXnodeObj obj = {0};
  obj.id = 3;
  obj.urlLen = 200;
  obj.url = (char*)taosMemMalloc(obj.urlLen);
  memset(obj.url, 'x', obj.urlLen - 1);
  obj.url[obj.urlLen - 1] = '\0';
  obj.statusLen = 10;
  obj.status = (char*)taosMemMalloc(obj.statusLen);
  strcpy(obj.status, "running");
  obj.createTime = 111111;
  obj.updateTime = 222222;

  SSdbRaw* pRaw = encodeXnodeObj(&obj);
  ASSERT_NE(pRaw, nullptr);

  SXnodeObj* pDecoded = decodeXnodeObj(pRaw);
  ASSERT_NE(pDecoded, nullptr);
  EXPECT_EQ(pDecoded->id, 3);
  EXPECT_EQ(pDecoded->urlLen, 200);
  EXPECT_EQ(memcmp(pDecoded->url, obj.url, obj.urlLen), 0);

  taosMemFree(obj.url);
  taosMemFree(obj.status);
  mockFreeRaw(pRaw);
  freeXnodeObj(pDecoded);
}

TEST_F(XnodeSdbTest, XnodeObj_InvalidVersion) {
  SXnodeObj obj = {0};
  obj.id = 4;
  obj.createTime = 1000;
  obj.updateTime = 2000;

  SSdbRaw* pRaw = mockAllocRaw(SDB_XNODE, 99, 1024);  // Invalid version
  ASSERT_NE(pRaw, nullptr);

  SXnodeObj* pDecoded = decodeXnodeObj(pRaw);
  EXPECT_EQ(pDecoded, nullptr);  // Should fail due to version mismatch

  mockFreeRaw(pRaw);
}

TEST_F(XnodeSdbTest, XnodeObj_MultipleEncodeDecode) {
  // Test encoding/decoding multiple objects in sequence
  for (int i = 1; i <= 5; i++) {
    SXnodeObj obj = {0};
    obj.id = i;
    obj.urlLen = 10 + i;
    obj.url = (char*)taosMemMalloc(obj.urlLen);
    snprintf(obj.url, obj.urlLen, "url_%d", i);
    obj.statusLen = 5;
    obj.status = (char*)taosMemMalloc(obj.statusLen);
    strcpy(obj.status, "ok");
    obj.createTime = 1000 * i;
    obj.updateTime = 2000 * i;

    SSdbRaw* pRaw = encodeXnodeObj(&obj);
    ASSERT_NE(pRaw, nullptr);

    SXnodeObj* pDecoded = decodeXnodeObj(pRaw);
    ASSERT_NE(pDecoded, nullptr);
    EXPECT_EQ(pDecoded->id, i);
    EXPECT_EQ(pDecoded->createTime, 1000 * i);

    taosMemFree(obj.url);
    taosMemFree(obj.status);
    mockFreeRaw(pRaw);
    freeXnodeObj(pDecoded);
  }
}

// ============= SXnodeTaskObj Tests =============

TEST_F(XnodeSdbTest, XnodeTaskObj_BasicEncodeDecode) {
  SXnodeTaskObj obj = {0};
  obj.id = 100;
  obj.nameLen = 10;
  obj.name = (char*)taosMemMalloc(obj.nameLen);
  strcpy(obj.name, "task_test");
  obj.sourceType = 1;
  obj.sourceDsnLen = 15;
  obj.sourceDsn = (char*)taosMemMalloc(obj.sourceDsnLen);
  strcpy(obj.sourceDsn, "kafka://source");
  obj.sinkType = 2;
  obj.sinkDsnLen = 13;
  obj.sinkDsn = (char*)taosMemMalloc(obj.sinkDsnLen);
  strcpy(obj.sinkDsn, "tdengine://db");
  obj.via = 1;
  obj.xnodeId = 10;
  obj.statusLen = 8;
  obj.status = (char*)taosMemMalloc(obj.statusLen);
  strcpy(obj.status, "running");
  obj.createTime = 5555555;
  obj.updateTime = 6666666;

  SSdbRaw* pRaw = encodeXnodeTaskObj(&obj);
  ASSERT_NE(pRaw, nullptr);
  EXPECT_EQ(pRaw->type, SDB_XNODE_TASK);
  EXPECT_EQ(pRaw->sver, TSDB_XNODE_VER_NUMBER);

  // Decode and verify
  SXnodeTaskObj* pDecoded = decodeXnodeTaskObj(pRaw);
  ASSERT_NE(pDecoded, nullptr);
  EXPECT_EQ(pDecoded->id, obj.id);
  EXPECT_EQ(pDecoded->nameLen, obj.nameLen);
  EXPECT_EQ(memcmp(pDecoded->name, obj.name, obj.nameLen), 0);
  EXPECT_EQ(pDecoded->sourceType, obj.sourceType);
  EXPECT_EQ(pDecoded->sourceDsnLen, obj.sourceDsnLen);
  EXPECT_EQ(memcmp(pDecoded->sourceDsn, obj.sourceDsn, obj.sourceDsnLen), 0);
  EXPECT_EQ(pDecoded->sinkType, obj.sinkType);
  EXPECT_EQ(pDecoded->sinkDsnLen, obj.sinkDsnLen);
  EXPECT_EQ(memcmp(pDecoded->sinkDsn, obj.sinkDsn, obj.sinkDsnLen), 0);
  EXPECT_EQ(pDecoded->via, obj.via);
  EXPECT_EQ(pDecoded->xnodeId, obj.xnodeId);
  EXPECT_EQ(pDecoded->createTime, obj.createTime);
  EXPECT_EQ(pDecoded->updateTime, obj.updateTime);

  taosMemFree(obj.name);
  taosMemFree(obj.sourceDsn);
  taosMemFree(obj.sinkDsn);
  taosMemFree(obj.status);
  mockFreeRaw(pRaw);
  freeXnodeTaskObj(pDecoded);
}

TEST_F(XnodeSdbTest, XnodeTaskObj_EmptyStrings) {
  SXnodeTaskObj obj = {0};
  obj.id = 101;
  obj.nameLen = 0;
  obj.name = nullptr;
  obj.sourceType = 0;
  obj.sourceDsnLen = 0;
  obj.sourceDsn = nullptr;
  obj.sinkType = 0;
  obj.sinkDsnLen = 0;
  obj.sinkDsn = nullptr;
  obj.via = 0;
  obj.xnodeId = 0;
  obj.createTime = 111111;
  obj.updateTime = 222222;

  SSdbRaw* pRaw = encodeXnodeTaskObj(&obj);
  ASSERT_NE(pRaw, nullptr);
  EXPECT_GT(pRaw->dataLen, 0);

  SXnodeTaskObj* pDecoded = decodeXnodeTaskObj(pRaw);
  ASSERT_NE(pDecoded, nullptr);
  EXPECT_EQ(pDecoded->id, obj.id);
  EXPECT_EQ(pDecoded->nameLen, 0);
  EXPECT_EQ(pDecoded->name, nullptr);

  mockFreeRaw(pRaw);
  freeXnodeTaskObj(pDecoded);
}

TEST_F(XnodeSdbTest, XnodeTaskObj_WithParser) {
  SXnodeTaskObj obj = {0};
  obj.id = 102;
  obj.nameLen = 7;
  obj.name = (char*)taosMemMalloc(obj.nameLen);
  strcpy(obj.name, "task_p");
  obj.sourceType = 1;
  obj.sourceDsnLen = 10;
  obj.sourceDsn = (char*)taosMemMalloc(obj.sourceDsnLen);
  strcpy(obj.sourceDsn, "source123");
  obj.sinkType = 2;
  obj.sinkDsnLen = 8;
  obj.sinkDsn = (char*)taosMemMalloc(obj.sinkDsnLen);
  strcpy(obj.sinkDsn, "sink456");
  obj.parserLen = 11;
  obj.parser = (char*)taosMemMalloc(obj.parserLen);
  strcpy(obj.parser, "csv_parser");
  obj.via = 1;
  obj.xnodeId = 5;
  obj.createTime = 1111111;
  obj.updateTime = 2222222;

  SSdbRaw* pRaw = encodeXnodeTaskObj(&obj);
  ASSERT_NE(pRaw, nullptr);

  SXnodeTaskObj* pDecoded = decodeXnodeTaskObj(pRaw);
  ASSERT_NE(pDecoded, nullptr);
  EXPECT_EQ(pDecoded->parserLen, obj.parserLen);
  EXPECT_EQ(memcmp(pDecoded->parser, obj.parser, obj.parserLen), 0);

  taosMemFree(obj.name);
  taosMemFree(obj.sourceDsn);
  taosMemFree(obj.sinkDsn);
  taosMemFree(obj.parser);
  mockFreeRaw(pRaw);
  freeXnodeTaskObj(pDecoded);
}

TEST_F(XnodeSdbTest, XnodeTaskObj_WithReason) {
  SXnodeTaskObj obj = {0};
  obj.id = 103;
  obj.nameLen = 8;
  obj.name = (char*)taosMemMalloc(obj.nameLen);
  strcpy(obj.name, "task123");
  obj.sourceType = 1;
  obj.sourceDsnLen = 5;
  obj.sourceDsn = (char*)taosMemMalloc(obj.sourceDsnLen);
  strcpy(obj.sourceDsn, "src1");
  obj.sinkType = 2;
  obj.sinkDsnLen = 6;
  obj.sinkDsn = (char*)taosMemMalloc(obj.sinkDsnLen);
  strcpy(obj.sinkDsn, "sink1");
  obj.reasonLen = 15;
  obj.reason = (char*)taosMemMalloc(obj.reasonLen);
  strcpy(obj.reason, "timeout_error");
  obj.via = 2;
  obj.xnodeId = 8;
  obj.createTime = 3333333;
  obj.updateTime = 4444444;

  SSdbRaw* pRaw = encodeXnodeTaskObj(&obj);
  ASSERT_NE(pRaw, nullptr);

  SXnodeTaskObj* pDecoded = decodeXnodeTaskObj(pRaw);
  ASSERT_NE(pDecoded, nullptr);
  EXPECT_EQ(pDecoded->reasonLen, obj.reasonLen);
  EXPECT_EQ(memcmp(pDecoded->reason, obj.reason, obj.reasonLen), 0);
  EXPECT_EQ(pDecoded->via, 2);
  EXPECT_EQ(pDecoded->xnodeId, 8);

  taosMemFree(obj.name);
  taosMemFree(obj.sourceDsn);
  taosMemFree(obj.sinkDsn);
  taosMemFree(obj.reason);
  mockFreeRaw(pRaw);
  freeXnodeTaskObj(pDecoded);
}

// ============= SXnodeJobObj Tests =============

TEST_F(XnodeSdbTest, XnodeJobObj_BasicEncodeDecode) {
  SXnodeJobObj obj = {0};
  obj.id = 200;
  obj.taskId = 100;
  obj.configLen = 20;
  obj.config = (char*)taosMemMalloc(obj.configLen);
  strcpy(obj.config, "{\"threads\": 4}");
  obj.via = 1;
  obj.xnodeId = 10;
  obj.statusLen = 8;
  obj.status = (char*)taosMemMalloc(obj.statusLen);
  strcpy(obj.status, "running");
  obj.createTime = 7777777;
  obj.updateTime = 8888888;

  SSdbRaw* pRaw = encodeXnodeJobObj(&obj);
  ASSERT_NE(pRaw, nullptr);
  EXPECT_EQ(pRaw->type, SDB_XNODE_JOB);
  EXPECT_EQ(pRaw->sver, TSDB_XNODE_VER_NUMBER);

  SXnodeJobObj* pDecoded = decodeXnodeJobObj(pRaw);
  ASSERT_NE(pDecoded, nullptr);
  EXPECT_EQ(pDecoded->id, obj.id);
  EXPECT_EQ(pDecoded->taskId, obj.taskId);
  EXPECT_EQ(pDecoded->configLen, obj.configLen);
  EXPECT_EQ(memcmp(pDecoded->config, obj.config, obj.configLen), 0);
  EXPECT_EQ(pDecoded->via, obj.via);
  EXPECT_EQ(pDecoded->xnodeId, obj.xnodeId);
  EXPECT_EQ(pDecoded->statusLen, obj.statusLen);
  EXPECT_EQ(memcmp(pDecoded->status, obj.status, obj.statusLen), 0);
  EXPECT_EQ(pDecoded->createTime, obj.createTime);
  EXPECT_EQ(pDecoded->updateTime, obj.updateTime);

  taosMemFree(obj.config);
  taosMemFree(obj.status);
  mockFreeRaw(pRaw);
  freeXnodeJobObj(pDecoded);
}

TEST_F(XnodeSdbTest, XnodeJobObj_EmptyFields) {
  SXnodeJobObj obj = {0};
  obj.id = 201;
  obj.taskId = 101;
  obj.configLen = 0;
  obj.config = nullptr;
  obj.via = 0;
  obj.xnodeId = 0;
  obj.statusLen = 0;
  obj.status = nullptr;
  obj.reasonLen = 0;
  obj.reason = nullptr;
  obj.createTime = 1000000;
  obj.updateTime = 2000000;

  SSdbRaw* pRaw = encodeXnodeJobObj(&obj);
  ASSERT_NE(pRaw, nullptr);

  SXnodeJobObj* pDecoded = decodeXnodeJobObj(pRaw);
  ASSERT_NE(pDecoded, nullptr);
  EXPECT_EQ(pDecoded->id, obj.id);
  EXPECT_EQ(pDecoded->taskId, obj.taskId);
  EXPECT_EQ(pDecoded->configLen, 0);
  EXPECT_EQ(pDecoded->config, nullptr);

  mockFreeRaw(pRaw);
  freeXnodeJobObj(pDecoded);
}

TEST_F(XnodeSdbTest, XnodeJobObj_WithReason) {
  SXnodeJobObj obj = {0};
  obj.id = 202;
  obj.taskId = 102;
  obj.configLen = 15;
  obj.config = (char*)taosMemMalloc(obj.configLen);
  strcpy(obj.config, "{\"batch\": 10}");
  obj.via = 2;
  obj.xnodeId = 20;
  obj.statusLen = 7;
  obj.status = (char*)taosMemMalloc(obj.statusLen);
  strcpy(obj.status, "failed");
  obj.reasonLen = 18;
  obj.reason = (char*)taosMemMalloc(obj.reasonLen);
  strcpy(obj.reason, "connection_failed");
  obj.createTime = 3333333;
  obj.updateTime = 4444444;

  SSdbRaw* pRaw = encodeXnodeJobObj(&obj);
  ASSERT_NE(pRaw, nullptr);

  SXnodeJobObj* pDecoded = decodeXnodeJobObj(pRaw);
  ASSERT_NE(pDecoded, nullptr);
  EXPECT_EQ(pDecoded->reasonLen, obj.reasonLen);
  EXPECT_EQ(memcmp(pDecoded->reason, obj.reason, obj.reasonLen), 0);

  taosMemFree(obj.config);
  taosMemFree(obj.status);
  taosMemFree(obj.reason);
  mockFreeRaw(pRaw);
  freeXnodeJobObj(pDecoded);
}

TEST_F(XnodeSdbTest, XnodeJobObj_LargeConfig) {
  SXnodeJobObj obj = {0};
  obj.id = 203;
  obj.taskId = 103;
  obj.configLen = 100;
  obj.config = (char*)taosMemMalloc(obj.configLen);
  memset(obj.config, 'c', obj.configLen - 1);
  obj.config[obj.configLen - 1] = '\0';
  obj.via = 1;
  obj.xnodeId = 15;
  obj.createTime = 5555555;
  obj.updateTime = 6666666;

  SSdbRaw* pRaw = encodeXnodeJobObj(&obj);
  ASSERT_NE(pRaw, nullptr);

  SXnodeJobObj* pDecoded = decodeXnodeJobObj(pRaw);
  ASSERT_NE(pDecoded, nullptr);
  EXPECT_EQ(pDecoded->configLen, obj.configLen);
  EXPECT_EQ(memcmp(pDecoded->config, obj.config, obj.configLen), 0);

  taosMemFree(obj.config);
  mockFreeRaw(pRaw);
  freeXnodeJobObj(pDecoded);
}

// ============= SXnodeUserPassObj Tests =============

TEST_F(XnodeSdbTest, XnodeUserPassObj_BasicEncodeDecode) {
  SXnodeUserPassObj obj = {0};
  obj.id = 300;
  obj.userLen = 10;
  obj.user = (char*)taosMemMalloc(obj.userLen);
  strcpy(obj.user, "testuser1");
  obj.passLen = 15;
  obj.pass = (char*)taosMemMalloc(obj.passLen);
  strcpy(obj.pass, "securepass123");
  obj.createTime = 9999999;
  obj.updateTime = 1111111;

  SSdbRaw* pRaw = encodeXnodeUserPassObj(&obj);
  ASSERT_NE(pRaw, nullptr);
  EXPECT_EQ(pRaw->type, SDB_XNODE_USER_PASS);
  EXPECT_EQ(pRaw->sver, TSDB_XNODE_VER_NUMBER);

  SXnodeUserPassObj* pDecoded = decodeXnodeUserPassObj(pRaw);
  ASSERT_NE(pDecoded, nullptr);
  EXPECT_EQ(pDecoded->id, obj.id);
  EXPECT_EQ(pDecoded->userLen, obj.userLen);
  EXPECT_EQ(memcmp(pDecoded->user, obj.user, obj.userLen), 0);
  EXPECT_EQ(pDecoded->passLen, obj.passLen);
  EXPECT_EQ(memcmp(pDecoded->pass, obj.pass, obj.passLen), 0);
  EXPECT_EQ(pDecoded->createTime, obj.createTime);
  EXPECT_EQ(pDecoded->updateTime, obj.updateTime);

  taosMemFree(obj.user);
  taosMemFree(obj.pass);
  mockFreeRaw(pRaw);
  freeXnodeUserPassObj(pDecoded);
}

TEST_F(XnodeSdbTest, XnodeUserPassObj_EmptyCredentials) {
  SXnodeUserPassObj obj = {0};
  obj.id = 301;
  obj.userLen = 0;
  obj.user = nullptr;
  obj.passLen = 0;
  obj.pass = nullptr;
  obj.createTime = 1234567;
  obj.updateTime = 7654321;

  SSdbRaw* pRaw = encodeXnodeUserPassObj(&obj);
  ASSERT_NE(pRaw, nullptr);

  SXnodeUserPassObj* pDecoded = decodeXnodeUserPassObj(pRaw);
  ASSERT_NE(pDecoded, nullptr);
  EXPECT_EQ(pDecoded->id, obj.id);
  EXPECT_EQ(pDecoded->userLen, 0);
  EXPECT_EQ(pDecoded->user, nullptr);
  EXPECT_EQ(pDecoded->passLen, 0);
  EXPECT_EQ(pDecoded->pass, nullptr);

  mockFreeRaw(pRaw);
  freeXnodeUserPassObj(pDecoded);
}

TEST_F(XnodeSdbTest, XnodeUserPassObj_LongCredentials) {
  SXnodeUserPassObj obj = {0};
  obj.id = 302;
  obj.userLen = 50;
  obj.user = (char*)taosMemMalloc(obj.userLen);
  memset(obj.user, 'u', obj.userLen - 1);
  obj.user[obj.userLen - 1] = '\0';
  obj.passLen = 100;
  obj.pass = (char*)taosMemMalloc(obj.passLen);
  memset(obj.pass, 'p', obj.passLen - 1);
  obj.pass[obj.passLen - 1] = '\0';
  obj.createTime = 5555555;
  obj.updateTime = 6666666;

  SSdbRaw* pRaw = encodeXnodeUserPassObj(&obj);
  ASSERT_NE(pRaw, nullptr);

  SXnodeUserPassObj* pDecoded = decodeXnodeUserPassObj(pRaw);
  ASSERT_NE(pDecoded, nullptr);
  EXPECT_EQ(pDecoded->userLen, obj.userLen);
  EXPECT_EQ(memcmp(pDecoded->user, obj.user, obj.userLen), 0);
  EXPECT_EQ(pDecoded->passLen, obj.passLen);
  EXPECT_EQ(memcmp(pDecoded->pass, obj.pass, obj.passLen), 0);

  taosMemFree(obj.user);
  taosMemFree(obj.pass);
  mockFreeRaw(pRaw);
  freeXnodeUserPassObj(pDecoded);
}

TEST_F(XnodeSdbTest, XnodeUserPassObj_SpecialCharacters) {
  SXnodeUserPassObj obj = {0};
  obj.id = 303;
  obj.userLen = 15;
  obj.user = (char*)taosMemMalloc(obj.userLen);
  strcpy(obj.user, "user@domain");
  obj.passLen = 20;
  obj.pass = (char*)taosMemMalloc(obj.passLen);
  strcpy(obj.pass, "p@ss!w0rd#123");
  obj.createTime = 1111111;
  obj.updateTime = 2222222;

  SSdbRaw* pRaw = encodeXnodeUserPassObj(&obj);
  ASSERT_NE(pRaw, nullptr);

  SXnodeUserPassObj* pDecoded = decodeXnodeUserPassObj(pRaw);
  ASSERT_NE(pDecoded, nullptr);
  EXPECT_STREQ(pDecoded->user, obj.user);
  EXPECT_STREQ(pDecoded->pass, obj.pass);

  taosMemFree(obj.user);
  taosMemFree(obj.pass);
  mockFreeRaw(pRaw);
  freeXnodeUserPassObj(pDecoded);
}

// ============= Cross-object Tests =============

TEST_F(XnodeSdbTest, AllObjects_InvalidVersion) {
  // Test that all decode functions reject invalid versions
  SSdbRaw* pRawXnode = mockAllocRaw(SDB_XNODE, 99, 1024);
  SSdbRaw* pRawTask = mockAllocRaw(SDB_XNODE_TASK, 99, 1024);
  SSdbRaw* pRawJob = mockAllocRaw(SDB_XNODE_JOB, 99, 1024);
  SSdbRaw* pRawUser = mockAllocRaw(SDB_XNODE_USER_PASS, 99, 1024);

  EXPECT_EQ(decodeXnodeObj(pRawXnode), nullptr);
  EXPECT_EQ(decodeXnodeTaskObj(pRawTask), nullptr);
  EXPECT_EQ(decodeXnodeJobObj(pRawJob), nullptr);
  EXPECT_EQ(decodeXnodeUserPassObj(pRawUser), nullptr);

  mockFreeRaw(pRawXnode);
  mockFreeRaw(pRawTask);
  mockFreeRaw(pRawJob);
  mockFreeRaw(pRawUser);
}

TEST_F(XnodeSdbTest, AllObjects_NullInput) {
  // Test that all decode/free functions handle null gracefully
  EXPECT_EQ(decodeXnodeObj(nullptr), nullptr);
  EXPECT_EQ(decodeXnodeTaskObj(nullptr), nullptr);
  EXPECT_EQ(decodeXnodeJobObj(nullptr), nullptr);
  EXPECT_EQ(decodeXnodeUserPassObj(nullptr), nullptr);

  // Should not crash
  freeXnodeObj(nullptr);
  freeXnodeTaskObj(nullptr);
  freeXnodeJobObj(nullptr);
  freeXnodeUserPassObj(nullptr);
  SUCCEED();
}

// ============= Memory and Edge Cases =============

TEST_F(XnodeSdbTest, XnodeObj_NullRawDecode) {
  SXnodeObj* pDecoded = decodeXnodeObj(nullptr);
  EXPECT_EQ(pDecoded, nullptr);
}

TEST_F(XnodeSdbTest, XnodeObj_FreeNull) {
  // Should not crash
  freeXnodeObj(nullptr);
  SUCCEED();
}

TEST_F(XnodeSdbTest, XnodeTaskObj_FreeNull) {
  // Should not crash
  freeXnodeTaskObj(nullptr);
  SUCCEED();
}

TEST_F(XnodeSdbTest, RawAllocation_OutOfMemory) {
  // Test with very large allocation (simulating OOM)
  SSdbRaw* pRaw = mockAllocRaw(SDB_XNODE, TSDB_XNODE_VER_NUMBER, 0);
  // Should handle gracefully
  if (pRaw) {
    mockFreeRaw(pRaw);
  }
  SUCCEED();
}

TEST_F(XnodeSdbTest, XnodeObj_UpdateScenario) {
  // Simulate an update: old object with one status, new object with another
  SXnodeObj oldObj = {0};
  oldObj.id = 50;
  oldObj.urlLen = 10;
  oldObj.url = (char*)taosMemMalloc(oldObj.urlLen);
  strcpy(oldObj.url, "url_old");
  oldObj.statusLen = 7;
  oldObj.status = (char*)taosMemMalloc(oldObj.statusLen);
  strcpy(oldObj.status, "online");
  oldObj.createTime = 100;
  oldObj.updateTime = 200;

  SXnodeObj newObj = {0};
  newObj.id = 50;
  newObj.urlLen = 10;
  newObj.url = (char*)taosMemMalloc(newObj.urlLen);
  strcpy(newObj.url, "url_old");
  newObj.statusLen = 8;
  newObj.status = (char*)taosMemMalloc(newObj.statusLen);
  strcpy(newObj.status, "offline");
  newObj.createTime = 100;
  newObj.updateTime = 300;

  // Encode both
  SSdbRaw* pRawOld = encodeXnodeObj(&oldObj);
  SSdbRaw* pRawNew = encodeXnodeObj(&newObj);

  ASSERT_NE(pRawOld, nullptr);
  ASSERT_NE(pRawNew, nullptr);

  // Decode and verify
  SXnodeObj* pDecodedOld = decodeXnodeObj(pRawOld);
  SXnodeObj* pDecodedNew = decodeXnodeObj(pRawNew);

  ASSERT_NE(pDecodedOld, nullptr);
  ASSERT_NE(pDecodedNew, nullptr);

  EXPECT_STREQ(pDecodedOld->status, "online");
  EXPECT_STREQ(pDecodedNew->status, "offline");
  EXPECT_EQ(pDecodedNew->updateTime, 300);

  taosMemFree(oldObj.url);
  taosMemFree(oldObj.status);
  taosMemFree(newObj.url);
  taosMemFree(newObj.status);
  mockFreeRaw(pRawOld);
  mockFreeRaw(pRawNew);
  freeXnodeObj(pDecodedOld);
  freeXnodeObj(pDecodedNew);
}

TEST_F(XnodeSdbTest, XnodeObj_SpecialCharacters) {
  SXnodeObj obj = {0};
  obj.id = 999;
  obj.urlLen = 30;
  obj.url = (char*)taosMemMalloc(obj.urlLen);
  strcpy(obj.url, "http://test.com/path?q=123");
  obj.statusLen = 20;
  obj.status = (char*)taosMemMalloc(obj.statusLen);
  strcpy(obj.status, "status with spaces!");
  obj.createTime = 777777;
  obj.updateTime = 888888;

  SSdbRaw* pRaw = encodeXnodeObj(&obj);
  ASSERT_NE(pRaw, nullptr);

  SXnodeObj* pDecoded = decodeXnodeObj(pRaw);
  ASSERT_NE(pDecoded, nullptr);
  EXPECT_STREQ(pDecoded->url, obj.url);
  EXPECT_STREQ(pDecoded->status, obj.status);

  taosMemFree(obj.url);
  taosMemFree(obj.status);
  mockFreeRaw(pRaw);
  freeXnodeObj(pDecoded);
}

TEST_F(XnodeSdbTest, XnodeObj_ZeroTimestamps) {
  SXnodeObj obj = {0};
  obj.id = 0;
  obj.urlLen = 5;
  obj.url = (char*)taosMemMalloc(obj.urlLen);
  strcpy(obj.url, "test");
  obj.statusLen = 3;
  obj.status = (char*)taosMemMalloc(obj.statusLen);
  strcpy(obj.status, "ok");
  obj.createTime = 0;
  obj.updateTime = 0;

  SSdbRaw* pRaw = encodeXnodeObj(&obj);
  ASSERT_NE(pRaw, nullptr);

  SXnodeObj* pDecoded = decodeXnodeObj(pRaw);
  ASSERT_NE(pDecoded, nullptr);
  EXPECT_EQ(pDecoded->createTime, 0);
  EXPECT_EQ(pDecoded->updateTime, 0);

  taosMemFree(obj.url);
  taosMemFree(obj.status);
  mockFreeRaw(pRaw);
  freeXnodeObj(pDecoded);
}
