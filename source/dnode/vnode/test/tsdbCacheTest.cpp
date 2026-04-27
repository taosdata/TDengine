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
#include <cstring>

#ifdef LINUX
#include "vnodeInt.h"
#include "taoserror.h"
#include "tdataformat.h"

extern "C" {
// External function to test
int32_t tsdbCacheDeserialize(char const *value, size_t size, SLastCol **pLastCol);
}

// Test fixture for tsdbCache security tests
class TsdbCacheSecurityTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

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

  // numOfPKs = 2 (exactly at TD_MAX_PK_COLS)
  uint8_t numOfPKs = 2;
  memcpy(buffer + offset, &numOfPKs, sizeof(uint8_t));
  offset += sizeof(uint8_t);

  // Add 2 SValue structures (simplified - just type and nData)
  for (int i = 0; i < 2; i++) {
    SValue val;
    memset(&val, 0, sizeof(SValue));
    val.type = TSDB_DATA_TYPE_INT;
    val.nData = 0;
    memcpy(buffer + offset, &val, sizeof(SValue));
    offset += sizeof(SValue);
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

  // numOfPKs = 0
  uint8_t numOfPKs = 0;
  memcpy(buffer + offset, &numOfPKs, sizeof(uint8_t));
  offset += sizeof(uint8_t);

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

#endif  // LINUX

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
