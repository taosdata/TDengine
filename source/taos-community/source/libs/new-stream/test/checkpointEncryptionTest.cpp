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
#include "stream.h"
#include "tglobal.h"
#include "tencrypt.h"

class CheckpointEncryptionTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    dDebugFlag = 143;
    tsLogEmbedded = 1;
    tsAsyncLog = 0;
    tsSkipKeyCheckMode = true;

    const char *path = TD_TMP_DIR_PATH "td_checkpoint_encryption_test";
    taosRemoveDir(path);
    taosMkDir(path);
    tstrncpy(tsLogDir, path, PATH_MAX);
    tstrncpy(tsDataDir, path, PATH_MAX);
    
    // Create full snode/checkpoint directory structure (must be recursive)
    char snodeBase[PATH_MAX];
    snprintf(snodeBase, sizeof(snodeBase), "%s%ssnode", path, TD_DIRSEP);
    taosMkDir(snodeBase);
    
    char checkpointPath[PATH_MAX];
    snprintf(checkpointPath, sizeof(checkpointPath), "%s%scheckpoint", snodeBase, TD_DIRSEP);
    taosMkDir(checkpointPath);
    
    printf("Created checkpoint directory: %s\n", checkpointPath);
    
    if (taosInitLog("checkpoint_test", 1, false) != 0) {
      printf("failed to init log file\n");
    }
  }
  
  static void TearDownTestSuite() { 
    taosCloseLog(); 
  }

 public:
  void SetUp() override {}
  void TearDown() override {}
};

// Test: unencrypted -> encrypted restart -> restart again
TEST_F(CheckpointEncryptionTest, 01_Encryption_Migration) {
  const int64_t testStreamId = 0x1234567890ABCDEFLL;
  
  // ===== Step 1: Create unencrypted checkpoint =====
  printf("\n=== Step 1: Creating unencrypted checkpoint ===\n");
  memset(tsMetaKey, 0, sizeof(tsMetaKey));
  tsEncryptAlgorithmType = TSDB_ENCRYPT_ALGO_NONE;
  
  // Create test data
  char testData[256] = "This is test checkpoint data for encryption testing";
  int64_t dataLen = strlen(testData) + 1;
  
  int32_t code = streamWriteCheckPoint(testStreamId, testData, dataLen);
  ASSERT_EQ(code, 0);
  printf("Step 1: Written unencrypted checkpoint, size=%ld\n", dataLen);
  
  // Read back and verify
  void *readData = NULL;
  int64_t readLen = 0;
  code = streamReadCheckPoint(testStreamId, &readData, &readLen);
  ASSERT_EQ(code, 0);
  ASSERT_NE(readData, nullptr);
  ASSERT_EQ(readLen, dataLen);
  EXPECT_STREQ((char*)readData, testData);
  taosMemoryFree(readData);
  readData = NULL;
  
  printf("Step 1: Verified unencrypted checkpoint\n\n");
  
  // ===== Step 2: Simulate encryption migration =====
  printf("=== Step 2: Manually migrating checkpoint to encrypted format ===\n");
  
  // First read without encryption key (to get the data)
  code = streamReadCheckPoint(testStreamId, &readData, &readLen);
  ASSERT_EQ(code, 0);
  ASSERT_NE(readData, nullptr);
  ASSERT_EQ(readLen, dataLen);
  printf("Step 2: Read unencrypted checkpoint, size=%ld\n", readLen);
  
  // Now set encryption key
  const char *testKey = "1234567890123456";  // 16 chars
  strncpy(tsMetaKey, testKey, ENCRYPT_KEY_LEN);
  tsMetaKey[ENCRYPT_KEY_LEN] = '\0';
  tsEncryptAlgorithmType = TSDB_ENCRYPT_ALGO_SM4;
  
  // Rewrite it with encryption - this will encrypt it
  code = streamWriteCheckPoint(testStreamId, readData, readLen);
  taosMemoryFree(readData);
  readData = NULL;
  ASSERT_EQ(code, 0);
  printf("Step 2: Rewrote checkpoint with encryption (now encrypted)\n\n");
  
  // ===== Step 3: Restart and verify encrypted read ===== 
  printf("=== Step 3: Restarting to verify encrypted checkpoint ===\n");
  
  // Read encrypted checkpoint
  code = streamReadCheckPoint(testStreamId, &readData, &readLen);
  ASSERT_EQ(code, 0);
  ASSERT_NE(readData, nullptr);
  ASSERT_EQ(readLen, dataLen);
  EXPECT_STREQ((char*)readData, testData);
  taosMemoryFree(readData);
  readData = NULL;
  printf("Step 3: Successfully read encrypted checkpoint\n\n");
  
  // ===== Step 4: Write new data to encrypted checkpoint =====
  printf("=== Step 4: Writing new data to encrypted checkpoint ===\n");
  char newTestData[256] = "Updated checkpoint data after encryption";
  int64_t newDataLen = strlen(newTestData) + 1;
  
  code = streamWriteCheckPoint(testStreamId, newTestData, newDataLen);
  ASSERT_EQ(code, 0);
  
  code = streamReadCheckPoint(testStreamId, &readData, &readLen);
  ASSERT_EQ(code, 0);
  ASSERT_NE(readData, nullptr);
  ASSERT_EQ(readLen, newDataLen);
  EXPECT_STREQ((char*)readData, newTestData);
  taosMemoryFree(readData);
  printf("Step 4: Successfully wrote and read new encrypted checkpoint\n\n");
  
  // Cleanup
  streamDeleteCheckPoint(testStreamId);
  memset(tsMetaKey, 0, sizeof(tsMetaKey));
  tsEncryptAlgorithmType = TSDB_ENCRYPT_ALGO_NONE;
  
  printf("=== Checkpoint encryption migration test PASSED ===\n");
}

// Test: Delete checkpoint
TEST_F(CheckpointEncryptionTest, 02_Delete_Checkpoint) {
  const int64_t testStreamId = 0xABCDEF1234567890LL;
  
  // Set encryption
  const char *testKey = "1234567890123456";
  strncpy(tsMetaKey, testKey, ENCRYPT_KEY_LEN);
  tsMetaKey[ENCRYPT_KEY_LEN] = '\0';
  tsEncryptAlgorithmType = TSDB_ENCRYPT_ALGO_SM4;
  
  // Create checkpoint
  char testData[128] = "Test data for deletion";
  int32_t code = streamWriteCheckPoint(testStreamId, testData, strlen(testData) + 1);
  ASSERT_EQ(code, 0);
  
  // Verify it exists
  void *readData = NULL;
  int64_t readLen = 0;
  code = streamReadCheckPoint(testStreamId, &readData, &readLen);
  ASSERT_EQ(code, 0);
  ASSERT_NE(readData, nullptr);
  taosMemoryFree(readData);
  
  // Delete checkpoint
  streamDeleteCheckPoint(testStreamId);
  
  // Verify it's deleted
  readData = NULL;
  code = streamReadCheckPoint(testStreamId, &readData, &readLen);
  EXPECT_EQ(code, 0);  // No error, just returns empty
  EXPECT_EQ(readData, nullptr);  // No data
  
  // Cleanup
  memset(tsMetaKey, 0, sizeof(tsMetaKey));
  tsEncryptAlgorithmType = TSDB_ENCRYPT_ALGO_NONE;
}
