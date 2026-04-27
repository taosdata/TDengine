/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

 #include <gtest/gtest.h>

 #ifdef LINUX
 #include <vnodeInt.h>
 
 #include <taoserror.h>
 #include <tglobal.h>
 #include <iostream>
 
 #include <tmsg.h>
 #include <random>
 #include <string>
 
 #pragma GCC diagnostic push
 #pragma GCC diagnostic ignored "-Wwrite-strings"
 #pragma GCC diagnostic ignored "-Wunused-function"
 #pragma GCC diagnostic ignored "-Wunused-variable"
 #pragma GCC diagnostic ignored "-Wsign-compare"
 
 // vnode meta layer expects this global in some builds; unit tests provide a stub.
 SDmNotifyHandle dmNotifyHdl = {.state = 0};
 
 #include "tsdb.h"
 #endif


int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

// Test fixture for tsdbCache security tests
class TsdbCacheSecurityTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

// Keep the in-test binary layout consistent with tsdbCache.c's on-disk V0 prefix.
// This is test-only; production struct lives in tsdbCache.c.
typedef struct {
  TSKEY  ts;
  int8_t dirty;
  struct {
    int16_t cid;
    int8_t  type;
    int8_t  flag;
    union {
      int64_t val;
      struct {
        uint32_t nData;
        uint8_t *pData;
      };
    } value;
  } colVal;
} SLastColV0Test;

// Test case: numOfPKs exceeds TD_MAX_PK_COLS (buffer overflow prevention)
TEST_F(TsdbCacheSecurityTest, numOfPKsBufferOverflow) {
  // Create a malicious payload with numOfPKs = 3 (exceeds TD_MAX_PK_COLS = 2)
  char buffer[256];
  int32_t offset = 0;

  // cid (int16_t)
  int16_t cid = 1;
  memcpy(buffer + offset, &cid, sizeof(int16_t));
  offset += sizeof(int16_t);

  // ts (TSKEY / int64_t)
  int64_t ts = 1234567890;
  memcpy(buffer + offset, &ts, sizeof(int64_t));
  offset += sizeof(int64_t);

  // version (int8_t)
  int8_t version = 1;
  memcpy(buffer + offset, &version, sizeof(int8_t));
  offset += sizeof(int8_t);

  // numOfPKs (uint8_t) - set to 3 to trigger overflow
  uint8_t numOfPKs = 3;
  memcpy(buffer + offset, &numOfPKs, sizeof(uint8_t));
  offset += sizeof(uint8_t);

  SLastCol* pLastCol = nullptr;
  int32_t code = tsdbCacheDeserialize(buffer, offset, &pLastCol);

  // Should return TSDB_CODE_INVALID_DATA_FMT and not crash
  EXPECT_EQ(code, TSDB_CODE_INVALID_DATA_FMT);
  EXPECT_EQ(pLastCol, nullptr);
}

// Test case: numOfPKs at boundary (TD_MAX_PK_COLS = 2, should succeed)
TEST_F(TsdbCacheSecurityTest, numOfPKsAtBoundary) {
  char buffer[512];
  int32_t offset = 0;

  SLastColV0Test v0 = {};
  v0.ts = 1234567890;
  v0.dirty = 0;
  v0.colVal.cid = 1;
  v0.colVal.type = TSDB_DATA_TYPE_INT;
  v0.colVal.flag = 0;
  v0.colVal.value.val = 0;
  memcpy(buffer + offset, &v0, sizeof(v0));
  offset += sizeof(v0);

  int8_t version = 1;
  memcpy(buffer + offset, &version, sizeof(version));
  offset += sizeof(version);

  uint8_t numOfPKs = 2;
  memcpy(buffer + offset, &numOfPKs, sizeof(numOfPKs));
  offset += sizeof(numOfPKs);

  for (int i = 0; i < 2; i++) {
    SValue pk = {};
    pk.type = TSDB_DATA_TYPE_INT;
    pk.nData = 0;
    pk.val = i + 1;
    memcpy(buffer + offset, &pk, sizeof(pk));
    offset += sizeof(pk);
  }

  SLastCol* pLastCol = nullptr;
  int32_t code = tsdbCacheDeserialize(buffer, offset, &pLastCol);

  // Should succeed with numOfPKs = 2
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  if (pLastCol) {
    EXPECT_EQ(pLastCol->rowKey.numOfPKs, 2);
    taosMemoryFree(pLastCol);
  }
}

// Test case: numOfPKs = 0 (valid edge case)
TEST_F(TsdbCacheSecurityTest, numOfPKsZero) {
  char buffer[256];
  int32_t offset = 0;

  SLastColV0Test v0 = {};
  v0.ts = 1234567890;
  v0.dirty = 0;
  v0.colVal.cid = 1;
  v0.colVal.type = TSDB_DATA_TYPE_INT;
  v0.colVal.flag = 0;
  v0.colVal.value.val = 0;
  memcpy(buffer + offset, &v0, sizeof(v0));
  offset += sizeof(v0);

  int8_t version = 1;
  memcpy(buffer + offset, &version, sizeof(version));
  offset += sizeof(version);

  uint8_t numOfPKs = 0;
  memcpy(buffer + offset, &numOfPKs, sizeof(numOfPKs));
  offset += sizeof(numOfPKs);

  SLastCol* pLastCol = nullptr;
  int32_t code = tsdbCacheDeserialize(buffer, offset, &pLastCol);

  // Should succeed with numOfPKs = 0
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  if (pLastCol) {
    EXPECT_EQ(pLastCol->rowKey.numOfPKs, 0);
    taosMemoryFree(pLastCol);
  }
}

// Test case: maximum malicious numOfPKs (255)
TEST_F(TsdbCacheSecurityTest, numOfPKsMaxValue) {
  char buffer[256];
  int32_t offset = 0;

  // cid
  int16_t cid = 1;
  memcpy(buffer + offset, &cid, sizeof(int16_t));
  offset += sizeof(int16_t);

  // ts
  int64_t ts = 1234567890;
  memcpy(buffer + offset, &ts, sizeof(int64_t));
  offset += sizeof(int64_t);

  // version
  int8_t version = 1;
  memcpy(buffer + offset, &version, sizeof(int8_t));
  offset += sizeof(int8_t);

  // numOfPKs = 255 (maximum uint8_t value)
  uint8_t numOfPKs = 255;
  memcpy(buffer + offset, &numOfPKs, sizeof(uint8_t));
  offset += sizeof(uint8_t);

  SLastCol* pLastCol = nullptr;
  int32_t code = tsdbCacheDeserialize(buffer, offset, &pLastCol);

  // Should return TSDB_CODE_INVALID_DATA_FMT
  EXPECT_EQ(code, TSDB_CODE_INVALID_DATA_FMT);
  EXPECT_EQ(pLastCol, nullptr);
}

// Test case: truncated buffer - missing version byte (out-of-bounds read prevention)
TEST_F(TsdbCacheSecurityTest, truncatedBufferMissingVersion) {
  char buffer[256];
  int32_t offset = 0;

  // cid
  int16_t cid = 1;
  memcpy(buffer + offset, &cid, sizeof(int16_t));
  offset += sizeof(int16_t);

  // ts
  int64_t ts = 1234567890;
  memcpy(buffer + offset, &ts, sizeof(int64_t));
  offset += sizeof(int64_t);

  // Truncate here - no version byte
  // This should trigger the validation check before reading version

  SLastCol* pLastCol = nullptr;
  int32_t code = tsdbCacheDeserialize(buffer, offset, &pLastCol);

  // Should return TSDB_CODE_INVALID_DATA_FMT before attempting to read version
  EXPECT_EQ(code, TSDB_CODE_INVALID_DATA_FMT);
  EXPECT_EQ(pLastCol, nullptr);
}

// Test case: truncated buffer - missing numOfPKs byte
TEST_F(TsdbCacheSecurityTest, truncatedBufferMissingNumOfPKs) {
  char buffer[256];
  int32_t offset = 0;

  // cid
  int16_t cid = 1;
  memcpy(buffer + offset, &cid, sizeof(int16_t));
  offset += sizeof(int16_t);

  // ts
  int64_t ts = 1234567890;
  memcpy(buffer + offset, &ts, sizeof(int64_t));
  offset += sizeof(int64_t);

  // version
  int8_t version = 1;
  memcpy(buffer + offset, &version, sizeof(int8_t));
  offset += sizeof(int8_t);

  // Truncate here - no numOfPKs byte

  SLastCol* pLastCol = nullptr;
  int32_t code = tsdbCacheDeserialize(buffer, offset, &pLastCol);

  // Should return TSDB_CODE_INVALID_DATA_FMT before attempting to read numOfPKs
  EXPECT_EQ(code, TSDB_CODE_INVALID_DATA_FMT);
  EXPECT_EQ(pLastCol, nullptr);
}

// Test case: truncated buffer - incomplete SValue structure
TEST_F(TsdbCacheSecurityTest, truncatedBufferIncompleteSValue) {
  char buffer[256];
  int32_t offset = 0;

  // cid
  int16_t cid = 1;
  memcpy(buffer + offset, &cid, sizeof(int16_t));
  offset += sizeof(int16_t);

  // ts
  int64_t ts = 1234567890;
  memcpy(buffer + offset, &ts, sizeof(int64_t));
  offset += sizeof(int64_t);

  // version
  int8_t version = 1;
  memcpy(buffer + offset, &version, sizeof(int8_t));
  offset += sizeof(int8_t);

  // numOfPKs = 1
  uint8_t numOfPKs = 1;
  memcpy(buffer + offset, &numOfPKs, sizeof(uint8_t));
  offset += sizeof(uint8_t);

  // Add only partial SValue (e.g., 4 bytes when sizeof(SValue) is larger)
  uint32_t partialData = 0x12345678;
  memcpy(buffer + offset, &partialData, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  SLastCol* pLastCol = nullptr;
  int32_t code = tsdbCacheDeserialize(buffer, offset, &pLastCol);

  // Should return TSDB_CODE_INVALID_DATA_FMT before attempting to read full SValue
  EXPECT_EQ(code, TSDB_CODE_INVALID_DATA_FMT);
  EXPECT_EQ(pLastCol, nullptr);
}

// Test case: truncated variable-length payload
TEST_F(TsdbCacheSecurityTest, truncatedVariableLengthPayload) {
  char buffer[512];
  int32_t offset = 0;

  // cid
  int16_t cid = 1;
  memcpy(buffer + offset, &cid, sizeof(int16_t));
  offset += sizeof(int16_t);

  // ts
  int64_t ts = 1234567890;
  memcpy(buffer + offset, &ts, sizeof(int64_t));
  offset += sizeof(int64_t);

  // version
  int8_t version = 1;
  memcpy(buffer + offset, &version, sizeof(int8_t));
  offset += sizeof(int8_t);

  // numOfPKs = 1
  uint8_t numOfPKs = 1;
  memcpy(buffer + offset, &numOfPKs, sizeof(uint8_t));
  offset += sizeof(uint8_t);

  // Add SValue with variable-length type and nData = 100
  SValue val;
  memset(&val, 0, sizeof(SValue));
  val.type = TSDB_DATA_TYPE_VARCHAR;  // Variable-length type
  val.nData = 100;  // Claims 100 bytes of data
  memcpy(buffer + offset, &val, sizeof(SValue));
  offset += sizeof(SValue);

  // But only add 10 bytes of actual data (truncated)
  char smallData[10] = "test";
  memcpy(buffer + offset, smallData, 10);
  offset += 10;

  SLastCol* pLastCol = nullptr;
  int32_t code = tsdbCacheDeserialize(buffer, offset, &pLastCol);

  // Should return TSDB_CODE_INVALID_DATA_FMT when validating variable-length payload
  EXPECT_EQ(code, TSDB_CODE_INVALID_DATA_FMT);
  EXPECT_EQ(pLastCol, nullptr);
}

// Test case: truncated cacheStatus byte (version >= 2)
TEST_F(TsdbCacheSecurityTest, truncatedCacheStatus) {
  char buffer[256];
  int32_t offset = 0;

  // cid
  int16_t cid = 1;
  memcpy(buffer + offset, &cid, sizeof(int16_t));
  offset += sizeof(int16_t);

  // ts
  int64_t ts = 1234567890;
  memcpy(buffer + offset, &ts, sizeof(int64_t));
  offset += sizeof(int64_t);

  // version = 2 (LAST_COL_VERSION_2)
  int8_t version = 2;
  memcpy(buffer + offset, &version, sizeof(int8_t));
  offset += sizeof(int8_t);

  // numOfPKs = 0 (no pks to read)
  uint8_t numOfPKs = 0;
  memcpy(buffer + offset, &numOfPKs, sizeof(uint8_t));
  offset += sizeof(uint8_t);

  // Truncate here - no cacheStatus byte
  // Since version >= 2, code will try to read cacheStatus

  SLastCol* pLastCol = nullptr;
  int32_t code = tsdbCacheDeserialize(buffer, offset, &pLastCol);

  // Should return TSDB_CODE_INVALID_DATA_FMT before attempting to read cacheStatus
  EXPECT_EQ(code, TSDB_CODE_INVALID_DATA_FMT);
  EXPECT_EQ(pLastCol, nullptr);
}

