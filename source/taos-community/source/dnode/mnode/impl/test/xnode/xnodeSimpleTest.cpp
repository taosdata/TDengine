/**
 * @file xnodeSimpleTest.cpp
 * @brief Simplified XNode encode/decode test without complex dependencies
 * @version 1.0
 * @date 2025-12-25
 */

#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>

// Minimal mock for SDB functions
typedef struct SSdbRaw {
  int8_t  sver;
  int32_t dataLen;
  char    data[4096];
} SSdbRaw;

typedef struct SSdbRow {
  char data[1024];
} SSdbRow;

// Mock SDB functions
SSdbRaw* sdbAllocRaw(int type, int version, int dataLen) {
  SSdbRaw* pRaw = (SSdbRaw*)malloc(sizeof(SSdbRaw));
  memset(pRaw, 0, sizeof(SSdbRaw));
  pRaw->sver = version;
  return pRaw;
}

void sdbFreeRaw(SSdbRaw* pRaw) {
  if (pRaw) free(pRaw);
}

SSdbRow* sdbAllocRow(int size) {
  SSdbRow* pRow = (SSdbRow*)malloc(sizeof(SSdbRow));
  memset(pRow, 0, sizeof(SSdbRow));
  return pRow;
}

void* sdbGetRowObj(SSdbRow* pRow) { return pRow ? pRow->data : nullptr; }

int sdbSetRawInt32(SSdbRaw* pRaw, int pos, int32_t val) {
  if (pos + 4 <= 4096) {
    memcpy(pRaw->data + pos, &val, 4);
    return 0;
  }
  return -1;
}

int sdbSetRawInt64(SSdbRaw* pRaw, int pos, int64_t val) {
  if (pos + 8 <= 4096) {
    memcpy(pRaw->data + pos, &val, 8);
    return 0;
  }
  return -1;
}

int sdbSetRawBinary(SSdbRaw* pRaw, int pos, const void* data, int len) {
  if (pos + len <= 4096) {
    memcpy(pRaw->data + pos, data, len);
    return 0;
  }
  return -1;
}

int sdbSetRawDataLen(SSdbRaw* pRaw, int len) {
  pRaw->dataLen = len;
  return 0;
}

int sdbGetRawSoftVer(SSdbRaw* pRaw, int8_t* ver) {
  *ver = pRaw->sver;
  return 0;
}

int sdbGetRawInt32(SSdbRaw* pRaw, int pos, int32_t* val) {
  if (pos + 4 <= pRaw->dataLen) {
    memcpy(val, pRaw->data + pos, 4);
    return 0;
  }
  return -1;
}

int sdbGetRawInt64(SSdbRaw* pRaw, int pos, int64_t* val) {
  if (pos + 8 <= pRaw->dataLen) {
    memcpy(val, pRaw->data + pos, 8);
    return 0;
  }
  return -1;
}

int sdbGetRawBinary(SSdbRaw* pRaw, int pos, void* data, int len) {
  if (pos + len <= pRaw->dataLen) {
    memcpy(data, pRaw->data + pos, len);
    return 0;
  }
  return -1;
}

// Test structure
typedef struct SXnodeObj {
  int32_t id;
  int32_t urlLen;
  char*   url;
  int32_t statusLen;
  char*   status;
  int64_t createTime;
  int64_t updateTime;
} SXnodeObj;

class XnodeSimpleTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(XnodeSimpleTest, BasicEncodeDecodeTest) {
  // Create test object
  SXnodeObj obj = {0};
  obj.id = 1;
  obj.urlLen = 12;
  obj.url = (char*)malloc(obj.urlLen);
  strcpy(obj.url, "node1:6050");
  obj.statusLen = 7;
  obj.status = (char*)malloc(obj.statusLen);
  strcpy(obj.status, "online");
  obj.createTime = 1234567890;
  obj.updateTime = 1234567900;

  // Encode
  SSdbRaw* pRaw = sdbAllocRaw(1, 1, 256);
  ASSERT_NE(pRaw, nullptr);

  int32_t dataPos = 0;
  EXPECT_EQ(sdbSetRawInt32(pRaw, dataPos, obj.id), 0);
  dataPos += 4;
  EXPECT_EQ(sdbSetRawInt32(pRaw, dataPos, obj.urlLen), 0);
  dataPos += 4;
  EXPECT_EQ(sdbSetRawBinary(pRaw, dataPos, obj.url, obj.urlLen), 0);
  dataPos += obj.urlLen;
  EXPECT_EQ(sdbSetRawInt32(pRaw, dataPos, obj.statusLen), 0);
  dataPos += 4;
  EXPECT_EQ(sdbSetRawBinary(pRaw, dataPos, obj.status, obj.statusLen), 0);
  dataPos += obj.statusLen;
  EXPECT_EQ(sdbSetRawInt64(pRaw, dataPos, obj.createTime), 0);
  dataPos += 8;
  EXPECT_EQ(sdbSetRawInt64(pRaw, dataPos, obj.updateTime), 0);
  dataPos += 8;
  EXPECT_EQ(sdbSetRawDataLen(pRaw, dataPos), 0);

  // Decode
  int8_t sver = 0;
  EXPECT_EQ(sdbGetRawSoftVer(pRaw, &sver), 0);
  EXPECT_EQ(sver, 1);

  SSdbRow* pRow = sdbAllocRow(sizeof(SXnodeObj));
  ASSERT_NE(pRow, nullptr);

  SXnodeObj* pDecoded = (SXnodeObj*)sdbGetRowObj(pRow);
  ASSERT_NE(pDecoded, nullptr);
  memset(pDecoded, 0, sizeof(SXnodeObj));

  dataPos = 0;
  EXPECT_EQ(sdbGetRawInt32(pRaw, dataPos, &pDecoded->id), 0);
  dataPos += 4;
  EXPECT_EQ(sdbGetRawInt32(pRaw, dataPos, &pDecoded->urlLen), 0);
  dataPos += 4;

  pDecoded->url = (char*)malloc(pDecoded->urlLen);
  EXPECT_EQ(sdbGetRawBinary(pRaw, dataPos, pDecoded->url, pDecoded->urlLen), 0);
  dataPos += pDecoded->urlLen;

  EXPECT_EQ(sdbGetRawInt32(pRaw, dataPos, &pDecoded->statusLen), 0);
  dataPos += 4;

  pDecoded->status = (char*)malloc(pDecoded->statusLen);
  EXPECT_EQ(sdbGetRawBinary(pRaw, dataPos, pDecoded->status, pDecoded->statusLen), 0);
  dataPos += pDecoded->statusLen;

  EXPECT_EQ(sdbGetRawInt64(pRaw, dataPos, &pDecoded->createTime), 0);
  dataPos += 8;
  EXPECT_EQ(sdbGetRawInt64(pRaw, dataPos, &pDecoded->updateTime), 0);

  // Verify
  EXPECT_EQ(obj.id, pDecoded->id);
  EXPECT_EQ(obj.urlLen, pDecoded->urlLen);
  EXPECT_EQ(obj.statusLen, pDecoded->statusLen);
  EXPECT_STREQ(obj.url, pDecoded->url);
  EXPECT_STREQ(obj.status, pDecoded->status);
  EXPECT_EQ(obj.createTime, pDecoded->createTime);
  EXPECT_EQ(obj.updateTime, pDecoded->updateTime);

  // Cleanup
  free(obj.url);
  free(obj.status);
  free(pDecoded->url);
  free(pDecoded->status);
  free(pRow);
  sdbFreeRaw(pRaw);
}

TEST_F(XnodeSimpleTest, EmptyFieldsTest) {
  SXnodeObj obj = {0};
  obj.id = 2;
  obj.urlLen = 0;
  obj.url = nullptr;
  obj.statusLen = 0;
  obj.status = nullptr;
  obj.createTime = 9876543210;
  obj.updateTime = 9876543220;

  SSdbRaw* pRaw = sdbAllocRaw(1, 1, 128);
  ASSERT_NE(pRaw, nullptr);

  int32_t dataPos = 0;
  sdbSetRawInt32(pRaw, dataPos, obj.id);
  dataPos += 4;
  sdbSetRawInt32(pRaw, dataPos, obj.urlLen);
  dataPos += 4;
  sdbSetRawInt32(pRaw, dataPos, obj.statusLen);
  dataPos += 4;
  sdbSetRawInt64(pRaw, dataPos, obj.createTime);
  dataPos += 8;
  sdbSetRawInt64(pRaw, dataPos, obj.updateTime);
  dataPos += 8;
  sdbSetRawDataLen(pRaw, dataPos);

  // Decode
  SSdbRow*   pRow = sdbAllocRow(sizeof(SXnodeObj));
  SXnodeObj* pDecoded = (SXnodeObj*)sdbGetRowObj(pRow);
  memset(pDecoded, 0, sizeof(SXnodeObj));

  dataPos = 0;
  sdbGetRawInt32(pRaw, dataPos, &pDecoded->id);
  dataPos += 4;
  sdbGetRawInt32(pRaw, dataPos, &pDecoded->urlLen);
  dataPos += 4;
  sdbGetRawInt32(pRaw, dataPos, &pDecoded->statusLen);
  dataPos += 4;
  sdbGetRawInt64(pRaw, dataPos, &pDecoded->createTime);
  dataPos += 8;
  sdbGetRawInt64(pRaw, dataPos, &pDecoded->updateTime);

  // Verify
  EXPECT_EQ(obj.id, pDecoded->id);
  EXPECT_EQ(0, pDecoded->urlLen);
  EXPECT_EQ(0, pDecoded->statusLen);
  EXPECT_EQ(obj.createTime, pDecoded->createTime);
  EXPECT_EQ(obj.updateTime, pDecoded->updateTime);

  free(pRow);
  sdbFreeRaw(pRaw);
}

TEST_F(XnodeSimpleTest, MultipleObjectsTest) {
  for (int i = 0; i < 10; i++) {
    SXnodeObj obj = {0};
    obj.id = i;
    obj.urlLen = 15;
    obj.url = (char*)malloc(obj.urlLen);
    snprintf(obj.url, obj.urlLen, "node%d:6050", i);
    obj.createTime = 1000000 + i;
    obj.updateTime = 2000000 + i;

    SSdbRaw* pRaw = sdbAllocRaw(1, 1, 256);
    int32_t  dataPos = 0;
    sdbSetRawInt32(pRaw, dataPos, obj.id);
    dataPos += 4;
    sdbSetRawInt32(pRaw, dataPos, obj.urlLen);
    dataPos += 4;
    sdbSetRawBinary(pRaw, dataPos, obj.url, obj.urlLen);
    dataPos += obj.urlLen;
    sdbSetRawInt64(pRaw, dataPos, obj.createTime);
    dataPos += 8;
    sdbSetRawInt64(pRaw, dataPos, obj.updateTime);
    dataPos += 8;
    sdbSetRawDataLen(pRaw, dataPos);

    // Decode and verify
    SSdbRow*   pRow = sdbAllocRow(sizeof(SXnodeObj));
    SXnodeObj* pDecoded = (SXnodeObj*)sdbGetRowObj(pRow);
    memset(pDecoded, 0, sizeof(SXnodeObj));

    dataPos = 0;
    sdbGetRawInt32(pRaw, dataPos, &pDecoded->id);
    dataPos += 4;
    sdbGetRawInt32(pRaw, dataPos, &pDecoded->urlLen);
    dataPos += 4;
    pDecoded->url = (char*)malloc(pDecoded->urlLen);
    sdbGetRawBinary(pRaw, dataPos, pDecoded->url, pDecoded->urlLen);
    dataPos += pDecoded->urlLen;
    sdbGetRawInt64(pRaw, dataPos, &pDecoded->createTime);
    dataPos += 8;
    sdbGetRawInt64(pRaw, dataPos, &pDecoded->updateTime);

    EXPECT_EQ(i, pDecoded->id);
    EXPECT_EQ(obj.createTime, pDecoded->createTime);
    EXPECT_EQ(obj.updateTime, pDecoded->updateTime);

    free(obj.url);
    free(pDecoded->url);
    free(pRow);
    sdbFreeRaw(pRaw);
  }
}
