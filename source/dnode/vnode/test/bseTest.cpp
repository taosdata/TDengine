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

#include "bse.h"
#endif

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#ifdef LINUX
static void initLog() {
  dDebugFlag = 143;
  vDebugFlag = 0;
  mDebugFlag = 143;
  cDebugFlag = 0;
  jniDebugFlag = 0;
  tmrDebugFlag = 135;
  uDebugFlag = 135;
  rpcDebugFlag = 143;
  qDebugFlag = 0;
  wDebugFlag = 0;
  sDebugFlag = 0;
  tsdbDebugFlag = 0;
  tsLogEmbedded = 1;
  tsAsyncLog = 0;
  //bseDebugFlag = 143;

  const char *path = TD_TMP_DIR_PATH "td";
  // taosRemoveDir(path);
  taosMkDir(path);
  tstrncpy(tsLogDir, path, PATH_MAX);
  if (taosInitLog("taosdlog", 1, false) != 0) {
    printf("failed to init log file\n");
  }
}
std::string genRandomString(int len) {
  const std::string               characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  std::random_device              rd;               // 用于生成随机种子
  std::mt19937                    generator(rd());  // 随机数生成器
  std::uniform_int_distribution<> distribution(0, characters.size() - 1);

  std::string randomString;
  for (int i = 0; i < len; ++i) {
    randomString += characters[distribution(generator)];
  }

  return randomString;
}
static int32_t putData(SBse *bse, int nItem, int32_t vlen, std::vector<int64_t> *data) {
  SBseBatch *pBatch = NULL;
  bseBatchInit(bse, &pBatch, nItem);
  int32_t code = 0;
  for (int32_t i = 0; i < nItem; i++) {
    std::string value = genRandomString(vlen);
    int64_t     seq = 0;
    code = bseBatchPut(pBatch, &seq, (uint8_t *)value.c_str(), value.size());

    data->push_back(seq);
  }
  printf("put result\n ");
  code = bseCommitBatch(bse, pBatch);
  return code;
}
static int32_t putNoRandomData(SBse *bse, int nItem, int32_t vlen, std::vector<int64_t> *data) {
  SBseBatch *pBatch = NULL;
  bseBatchInit(bse, &pBatch, nItem);
  char *str = (char *)taosMemoryCalloc(1, vlen + 1);
  memset(str, 'a', vlen);
  int32_t code = 0;
  for (int32_t i = 0; i < nItem; i++) {
    // std::string value;
    // value.reserve(vlen);
    int64_t seq = 0;
    code = bseBatchPut(pBatch, &seq, (uint8_t *)str, vlen);

    data->push_back(seq);
  }
  taosMemoryFree(str);
  printf("put result ");
  code = bseCommitBatch(bse, pBatch);
  return code;
}
static int32_t getData(SBse *pBse, std::vector<int64_t> *data, int32_t expectLen) {
  int32_t code = 0;
  for (int32_t i = 0; i < data->size(); i++) {
    uint8_t *value = NULL;
    int32_t  len = 0;
    uint64_t seq = data->at(i);

    code = bseGet(pBse, seq, &value, &len);
    if (code != 0) {
      printf("failed to get key %d error code: %d\n", i, code);
      ASSERT(0);
    } else {
      if (len != expectLen) {
        printf("get key %d len %d, expect %d\n", i, len, expectLen);
        ASSERT(0);
      }
      // std::string str((char *)value, len);
      if (i % 10000 == 0) printf("get result %d\n", i);
    }
    taosMemoryFree(value);
  }
  return code;
}
int32_t putStringData(SBse *pBse, int32_t num, std::string &data, std::vector<int64_t> *seqs) {
  SBseBatch *pBatch = NULL;
  bseBatchInit(pBse, &pBatch, num);
  int32_t code = 0;
  for (int32_t i = 0; i < num; i++) {
    int64_t seq = 0;
    code = bseBatchPut(pBatch, &seq, (uint8_t *)data.c_str(), data.size());
    seqs->push_back(seq);
  }
  code = bseCommitBatch(pBse, pBatch);
  return code;
}

int32_t getDataAndValid(SBse *pBse, std::string &inStr, std::vector<int64_t> *seqs) {
  int32_t code = 0;
  for (int32_t i = 0; i < seqs->size(); i++) {
    uint8_t *value = NULL;
    int32_t  len = 0;
    uint64_t seq = seqs->at(i);

    code = bseGet(pBse, seq, &value, &len);
    if (code != 0) {
      printf("failed to get key %d error code: %d\n", i, code);
    } else {
      if (strncmp((const char *)value, inStr.c_str(), len) != 0) {
        ASSERT(0);
      } else {
        if (i % 10000 == 0) printf("succ to get key %d\n", (int32_t)seq);
      }
    }
    taosMemoryFree(value);
  }
  return code;
}
int32_t testCompress(SBse *bse, int8_t compressType) {
  std::vector<int64_t> data;
  std::string          str = genRandomString(1000);
  SBseCfg              cfg = {.compressType = compressType};
  bseUpdateCfg(bse, &cfg);

  putStringData(bse, 100000, str, &data);
  bseCommit(bse);

  getDataAndValid(bse, str, &data);
  return 0;
}

int32_t testAllCompress(SBse *bse) {
  for (int8_t i = kNoCompres; i <= kZxCompress; i++) {
    testCompress(bse, i);
  }
  return 0;
}
int32_t benchTest() {
  SBse                *bse = NULL;
  std::vector<int64_t> data;
  SBseCfg              cfg = {.vgId = 2};
  taosRemoveDir("/tmp/bse");

  {
    int32_t code = bseOpen("/tmp/bse", &cfg, &bse);
    {
      SBseCfg cfg = {.compressType = kNoCompres};
      bseUpdateCfg(bse, &cfg);
    }

    putData(bse, 10000, 1000, &data);
    // getData(bse, &data);

    bseCommit(bse);
    getData(bse, &data, 1000);

    putData(bse, 10000, 1000, &data);

    bseCommit(bse);

    putData(bse, 10000, 1000, &data);

    getData(bse, &data, 1000);
    bseCommit(bse);

    getData(bse, &data, 1000);

    // test compress
    testAllCompress(bse);

    data.clear();
  }

  bseClose(bse);
  return 0;
}
int32_t funcTest() {
  SBse                *bse = NULL;
  SBseCfg              cfg = {.vgId = 2};
  std::vector<int64_t> data;
  taosRemoveDir("/tmp/bse");
  int32_t code = bseOpen("/tmp/bse", &cfg, &bse);
  putData(bse, 10000, 1000, &data);
  getData(bse, &data, 1000);

  bseCommit(bse);

  getData(bse, &data, 1000);

  bseClose(bse);

  {
    code = bseOpen("/tmp/bse", &cfg, &bse);
    getData(bse, &data, 1000);
    bseClose(bse);
  }

  return 0;
}
int32_t randomGet(SBse *pBse, std::vector<int64_t> *data, int32_t count, int32_t expectLen) {
  int32_t code = 0;
  int32_t i = 0;
  while (i < count) {
    int32_t   idx = taosRand() % data->size();
    uint8_t *value = NULL;
    int32_t   len = 0;
    int64_t   seq = data->at(idx);
    //uInfo("%d get seq %"PRId64"", idx, seq); 
    code = bseGet(pBse, seq, &value, &len);
    if (code != 0) {
      ASSERT(0);
    } else {
      if (len != expectLen){
       uInfo("len %d, expect len %d", len, expectLen);
        ASSERT(0);
      }
    }
    taosMemoryFree(value);
    i++;
  }

  return code;
}
int32_t funcTestSmallData() {
  SBse   *bse = NULL;
  SBseCfg cfg = {.vgId = 2};
  taosRemoveDir("/tmp/bse");

  std::vector<int64_t> data;
  int32_t              code = bseOpen("/tmp/bse", &cfg, &bse);
  int32_t len = 10000;
  putData(bse, 10000, len, &data);
  randomGet(bse, &data, 1000, len);

  bseCommit(bse);

  randomGet(bse, &data, 1000, len);

  putData(bse, 10000, len, &data);

  bseCommit(bse);

  putData(bse, 10000, len, &data);
  randomGet(bse, &data, 100, len);

  bseCommit(bse);

  randomGet(bse, &data, 100, len);

  bseClose(bse);

  return 0;
}

int32_t funcTestWriteSmallData() {
  SBse   *bse = NULL;
  SBseCfg cfg = {.vgId = 2};
  taosRemoveDir("/tmp/bse");

  std::vector<int64_t> data;
  int32_t              code = bseOpen("/tmp/bse", &cfg, &bse);
  putNoRandomData(bse, 10000, 100000, &data);

  bseCommit(bse);

  putNoRandomData(bse, 10000, 100000, &data);

  bseCommit(bse);

  putNoRandomData(bse, 10000, 100000, &data);

  bseCommit(bse);

  bseClose(bse);

  return 0;
}
int32_t snapTest() {
  int32_t              code = 0;
  SBse                *bse = NULL, *bseDst = NULL;
  SBse                *pDstBse = NULL;
  SBseCfg              cfg = {.vgId = 2};
  std::vector<int64_t> data1;
  {
    taosRemoveDir("/tmp/bseSrc");
    taosRemoveDir("/tmp/bseDst");
    int32_t code = bseOpen("/tmp/bseSrc", &cfg, &bse);
    putData(bse, 10000, 1000, &data1);
    bseCommit(bse);
    int64_t  seq = data1[0];
    uint8_t *value = NULL;
    int32_t  len = 0;
    bseGet(bse, seq, &value, &len);
    taosMemoryFree(value);
  }
  {
    int32_t code = bseOpen("/tmp/bseDst", &cfg, &bseDst);
    // putData(bse, 10000, 1000, &data);
    // bseCommit(bse);
    // getData(bse, &data);
  }
  {
    SBseSnapWriter *pWriter = NULL;
    SBseSnapReader *pReader = NULL;

    int32_t code = bseSnapReaderOpen(bse, 0, 0, &pReader);

    code = bseSnapWriterOpen(bseDst, 0, 0, &pWriter);
    uint8_t *data = NULL;
    int32_t  ndata = 0;
    while (bseSnapReaderRead2(pReader, &data, &ndata) == 0) {
      if (data != NULL)
        code = bseSnapWriterWrite(pWriter, data, ndata);
      else {
        break;
      }
      taosMemFreeClear(data);
    }
    taosMemoryFree(data);

    bseSnapReaderClose(&pReader);
    bseSnapWriterClose(&pWriter, 0);

    uint8_t *value = NULL;
    int32_t  len = 0;
    bseReload(bseDst);
    int64_t seq = data1[0];
    for (int32_t i = 0; i < data1.size(); i++) {
      seq = data1[i];
      code = bseGet(bseDst, seq, &value, &len);
      if (code != 0) {
        printf("failed to get key %d error code: %d\n", i, code);
        ASSERT(0);
      } else {
        taosMemoryFree(value);
      }
    }
  }
  bseClose(bse);
  bseClose(bseDst);
  return code;
}

void emptySnapTest() {
  int32_t code = 0;
  SBse   *bse = NULL, *bseDst = NULL;
  SBse   *pDstBse = NULL;
  SBseCfg cfg = {.vgId = 2};
  {
    taosRemoveDir("/tmp/bseSrc");
    taosRemoveDir("/tmp/bseDst");
    int32_t code = bseOpen("/tmp/bseSrc", &cfg, &bse);
    code = bseOpen("/tmp/bseDst", &cfg, &bseDst);

    SBseSnapWriter *pWriter = NULL;
    SBseSnapReader *pReader = NULL;

    code = bseSnapReaderOpen(bse, 0, 0, &pReader);

    code = bseSnapWriterOpen(bseDst, 0, 0, &pWriter);
    uint8_t *data = NULL;
    int32_t  ndata = 0;
    while (bseSnapReaderRead2(pReader, &data, &ndata) == 0) {
      if (data != NULL)
        code = bseSnapWriterWrite(pWriter, data, ndata);
      else {
        break;
      }
      taosMemFreeClear(data);
    }
    bseSnapReaderClose(&pReader);
    bseSnapWriterClose(&pWriter, 0);

    code = bseReload(bseDst);
  }
  bseClose(bse);
  bseClose(bseDst);
}
#endif
TEST(bseCase, emptysnapTest) {
#ifdef LINUX
  initLog();
  emptySnapTest();
#endif
}
TEST(bseCase, snapTest) {
#ifdef LINUX
  initLog();
  snapTest();
#endif
}
TEST(bseCase, benchTest) {
#ifdef LINUX
  initLog();
  benchTest();
#endif
}
TEST(bseCase, funcTest) {
#ifdef LINUX
  initLog();
  funcTest();
#endif
}
TEST(bseCase, smallDataTest) {
#ifdef LINUX
  initLog();
  funcTestSmallData();
#endif
}
TEST(bseCase, smallDataWriteTest) {
#ifdef LINUX
  initLog();
  funcTestWriteSmallData();
#endif
}

TEST(bseCase, multiThreadReadWriteTest) {
  // Implement multi-threaded read/write test
#ifdef LINUX
  initLog();
  SBse   *bse = NULL;
  SBseCfg cfg = {.vgId = 2};
  taosRemoveDir("/tmp/bse");

  int32_t code = bseOpen("/tmp/bse", &cfg, &bse);
  ASSERT_EQ(code, 0);

  std::vector<int64_t> data;
  putData(bse, 10000, 1000, &data);
  bseCommit(bse);

  getData(bse, &data, 1000);

  bseClose(bse);
#endif
}

TEST(bseCase, recover) {
  // Implement multi-threaded read/write test
#ifdef LINUX
  initLog();
  SBse   *bse = NULL;
  SBseCfg cfg = {.vgId = 2};
  taosRemoveDir("/tmp/bse");

  int32_t code = bseOpen("/tmp/bse", &cfg, &bse);
  ASSERT_EQ(code, 0);

  std::vector<int64_t> data;
  putData(bse, 10000, 1000, &data);
  getData(bse, &data, 1000);
  bseCommit(bse);

  getData(bse, &data, 1000);
  putData(bse, 10000, 1000, &data);

  bseCommit(bse);
  bseClose(bse);
  {
    code = bseOpen("/tmp/bse", &cfg, &bse);
    ASSERT_EQ(code, 0);

    getData(bse, &data, 1000);

    bseClose(bse);
  }

#endif
}
TEST(bseCase, emptyNot) {
  // Implement multi-threaded read/write test
#ifdef LINUX
  initLog();
  SBse   *bse = NULL;
  SBseCfg cfg = {.vgId = 2};
  taosRemoveDir("/tmp/bse");

  std::vector<int64_t> data;
  data.push_back(1);
  data.push_back(2);
  data.push_back(3);
  int32_t code = bseOpen("/tmp/bse", &cfg, &bse);
  char   *value = NULL;
  int32_t len = 0;
  for (int32_t i = 0; i < data.size(); i++) {
    code = bseGet(bse, data[i], (uint8_t **)&value, &len);
    if (code != 0) {
      printf("failed to get key %d error code: %d\n", i, code);
    } else {
      // std::string str((char *)value, len);
      // printf("get result %d: %s\n", i, str.c_str());
    }
    taosMemoryFree(value);
  }
  // code = bseGet(bse, 1, &value, &len);
  // code = getData(bse, &data);
  // EXPECT_NE(code, 0);

  bseClose(bse);
  //}

#endif
}
TEST(bseCase, smallData) {
  // Implement multi-threaded read/write test
#ifdef LINUX
  initLog();
  SBse   *bse = NULL;
  SBseCfg cfg = {.vgId = 2};
  taosRemoveDir("/tmp/bse");

  int32_t code = bseOpen("/tmp/bse", &cfg, &bse);
  ASSERT_EQ(code, 0);

  std::vector<int64_t> data;
  putData(bse, 10, 10, &data);
  bseCommit(bse);

  getData(bse, &data, 10);

  bseClose(bse);
#endif
}

// ==================== Encryption Test Cases ====================

TEST(bseCase, basicEncryptionTest) {
#ifdef LINUX
  initLog();
  SBse   *bse = NULL;
  SBseCfg cfg = {.vgId = 2};
  taosRemoveDir("/tmp/bse_encrypt");

  // Set encryption config
  tstrncpy(cfg.encryptAlgrName, TSDB_ENCRYPT_ALGO_SM4_STR, TSDB_ENCRYPT_ALGR_NAME_LEN);
  tstrncpy(cfg.encryptKey, "test_encryption_key_1234567890abcd", ENCRYPT_KEY_LEN + 1);

  int32_t code = bseOpen("/tmp/bse_encrypt", &cfg, &bse);
  ASSERT_EQ(code, 0);

  // Test write with encryption
  std::vector<int64_t> data;
  putData(bse, 1000, 1000, &data);
  
  // Verify data can be read back correctly
  getData(bse, &data, 1000);
  
  bseCommit(bse);
  
  // Verify after commit
  getData(bse, &data, 1000);
  
  bseClose(bse);
  
  // Reopen and verify persistence
  code = bseOpen("/tmp/bse_encrypt", &cfg, &bse);
  ASSERT_EQ(code, 0);
  
  getData(bse, &data, 1000);
  
  bseClose(bse);
#endif
}

TEST(bseCase, encryptionWithCompressionTest) {
#ifdef LINUX
  initLog();
  SBse   *bse = NULL;
  SBseCfg cfg = {.vgId = 2};
  taosRemoveDir("/tmp/bse_encrypt_compress");

  // Set encryption config
  tstrncpy(cfg.encryptAlgrName, TSDB_ENCRYPT_ALGO_SM4_STR, TSDB_ENCRYPT_ALGR_NAME_LEN);
  tstrncpy(cfg.encryptKey, "test_compression_key_1234567890abc", ENCRYPT_KEY_LEN + 1);

  int32_t code = bseOpen("/tmp/bse_encrypt_compress", &cfg, &bse);
  ASSERT_EQ(code, 0);

  // Test with different compression types
  std::vector<int64_t> data;
  std::string str = genRandomString(5000);
  
  for (int8_t compType = kNoCompres; compType <= kZSTDCompres; compType++) {
    SBseCfg updateCfg = {.compressType = compType};
    bseUpdateCfg(bse, &updateCfg);
    
    putStringData(bse, 100, str, &data);
    bseCommit(bse);
    
    getDataAndValid(bse, str, &data);
    data.clear();
  }
  
  bseClose(bse);
#endif
}

TEST(bseCase, encryptionSnapshotTest) {
#ifdef LINUX
  initLog();
  SBse   *bseSrc = NULL, *bseDst = NULL;
  SBseCfg cfg = {.vgId = 2};
  
  // Set encryption config
  tstrncpy(cfg.encryptAlgrName, TSDB_ENCRYPT_ALGO_SM4_STR, TSDB_ENCRYPT_ALGR_NAME_LEN);
  tstrncpy(cfg.encryptKey, "test_snapshot_key_1234567890abcde", ENCRYPT_KEY_LEN + 1);
  
  std::vector<int64_t> data;
  
  taosRemoveDir("/tmp/bse_encrypt_src");
  taosRemoveDir("/tmp/bse_encrypt_dst");
  
  // Create source BSE with encrypted data
  int32_t code = bseOpen("/tmp/bse_encrypt_src", &cfg, &bseSrc);
  ASSERT_EQ(code, 0);
  
  putData(bseSrc, 5000, 2000, &data);
  bseCommit(bseSrc);
  
  // Create destination BSE
  code = bseOpen("/tmp/bse_encrypt_dst", &cfg, &bseDst);
  ASSERT_EQ(code, 0);
  
  // Perform snapshot
  SBseSnapWriter *pWriter = NULL;
  SBseSnapReader *pReader = NULL;
  
  code = bseSnapReaderOpen(bseSrc, 0, 0, &pReader);
  ASSERT_EQ(code, 0);
  
  code = bseSnapWriterOpen(bseDst, 0, 0, &pWriter);
  ASSERT_EQ(code, 0);
  
  uint8_t *snapData = NULL;
  int32_t  ndata = 0;
  while (bseSnapReaderRead2(pReader, &snapData, &ndata) == 0) {
    if (snapData != NULL) {
      code = bseSnapWriterWrite(pWriter, snapData, ndata);
      ASSERT_EQ(code, 0);
    } else {
      break;
    }
    taosMemFreeClear(snapData);
  }
  
  bseSnapReaderClose(&pReader);
  bseSnapWriterClose(&pWriter, 0);
  
  // Reload and verify
  code = bseReload(bseDst);
  ASSERT_EQ(code, 0);
  
  getData(bseDst, &data, 2000);
  
  bseClose(bseSrc);
  bseClose(bseDst);
#endif
}

TEST(bseCase, encryptionLargeDataTest) {
#ifdef LINUX
  initLog();
  SBse   *bse = NULL;
  SBseCfg cfg = {.vgId = 2};
  taosRemoveDir("/tmp/bse_encrypt_large");

  // Set encryption config
  tstrncpy(cfg.encryptAlgrName, TSDB_ENCRYPT_ALGO_SM4_STR, TSDB_ENCRYPT_ALGR_NAME_LEN);
  tstrncpy(cfg.encryptKey, "large_data_encrypt_key_1234567890", ENCRYPT_KEY_LEN + 1);

  int32_t code = bseOpen("/tmp/bse_encrypt_large", &cfg, &bse);
  ASSERT_EQ(code, 0);

  // Test with large data
  std::vector<int64_t> data;
  putData(bse, 10000, 10000, &data);
  
  bseCommit(bse);
  
  // Random access test
  randomGet(bse, &data, 1000, 10000);
  
  // Add more data
  putData(bse, 10000, 10000, &data);
  bseCommit(bse);
  
  randomGet(bse, &data, 2000, 10000);
  
  bseClose(bse);
  
  // Reopen and verify
  code = bseOpen("/tmp/bse_encrypt_large", &cfg, &bse);
  ASSERT_EQ(code, 0);
  
  randomGet(bse, &data, 2000, 10000);
  
  bseClose(bse);
#endif
}

TEST(bseCase, encryptionMultiCommitTest) {
#ifdef LINUX
  initLog();
  SBse   *bse = NULL;
  SBseCfg cfg = {.vgId = 2};
  taosRemoveDir("/tmp/bse_encrypt_multi");

  // Set encryption config
  tstrncpy(cfg.encryptAlgrName, TSDB_ENCRYPT_ALGO_SM4_STR, TSDB_ENCRYPT_ALGR_NAME_LEN);
  tstrncpy(cfg.encryptKey, "multi_commit_key_1234567890abcdef", ENCRYPT_KEY_LEN + 1);

  int32_t code = bseOpen("/tmp/bse_encrypt_multi", &cfg, &bse);
  ASSERT_EQ(code, 0);

  std::vector<int64_t> data;
  
  // Multiple commit cycles
  for (int i = 0; i < 5; i++) {
    putData(bse, 2000, 1000, &data);
    bseCommit(bse);
    getData(bse, &data, 1000);
  }
  
  bseClose(bse);
  
  // Reopen and verify all data
  code = bseOpen("/tmp/bse_encrypt_multi", &cfg, &bse);
  ASSERT_EQ(code, 0);
  
  getData(bse, &data, 1000);
  
  bseClose(bse);
#endif
}

TEST(bseCase, encryptionRecoverTest) {
#ifdef LINUX
  initLog();
  SBse   *bse = NULL;
  SBseCfg cfg = {.vgId = 2};
  taosRemoveDir("/tmp/bse_encrypt_recover");

  // Set encryption config
  tstrncpy(cfg.encryptAlgrName, TSDB_ENCRYPT_ALGO_SM4_STR, TSDB_ENCRYPT_ALGR_NAME_LEN);
  tstrncpy(cfg.encryptKey, "recover_test_key_1234567890abcdef", ENCRYPT_KEY_LEN + 1);

  int32_t code = bseOpen("/tmp/bse_encrypt_recover", &cfg, &bse);
  ASSERT_EQ(code, 0);

  std::vector<int64_t> data;
  putData(bse, 5000, 1000, &data);
  getData(bse, &data, 1000);
  
  bseCommit(bse);
  
  // Add more data without commit (simulate crash)
  putData(bse, 5000, 1000, &data);
  
  bseClose(bse);
  
  // Reopen - should recover committed data
  code = bseOpen("/tmp/bse_encrypt_recover", &cfg, &bse);
  ASSERT_EQ(code, 0);
  
  // Verify first batch is still accessible
  for (int i = 0; i < 5000; i++) {
    uint8_t *value = NULL;
    int32_t  len = 0;
    code = bseGet(bse, data[i], &value, &len);
    ASSERT_EQ(code, 0);
    ASSERT_EQ(len, 1000);
    taosMemoryFree(value);
  }
  
  bseClose(bse);
#endif
}

TEST(bseCase, encryptionNoKeyTest) {
#ifdef LINUX
  initLog();
  SBse   *bse = NULL;
  SBseCfg cfg = {.vgId = 2};
  taosRemoveDir("/tmp/bse_no_encrypt");

  // No encryption key set - should work as normal
  int32_t code = bseOpen("/tmp/bse_no_encrypt", &cfg, &bse);
  ASSERT_EQ(code, 0);

  std::vector<int64_t> data;
  putData(bse, 1000, 1000, &data);
  bseCommit(bse);
  
  getData(bse, &data, 1000);
  
  bseClose(bse);
  
  // Reopen and verify
  code = bseOpen("/tmp/bse_no_encrypt", &cfg, &bse);
  ASSERT_EQ(code, 0);
  
  getData(bse, &data, 1000);
  
  bseClose(bse);
#endif
}

TEST(bseCase, encryptionBoundaryTest) {
#ifdef LINUX
  initLog();
  SBse   *bse = NULL;
  SBseCfg cfg = {.vgId = 2};
  taosRemoveDir("/tmp/bse_encrypt_boundary");

  // Set encryption config
  tstrncpy(cfg.encryptAlgrName, TSDB_ENCRYPT_ALGO_SM4_STR, TSDB_ENCRYPT_ALGR_NAME_LEN);
  tstrncpy(cfg.encryptKey, "boundary_test_key_1234567890abcde", ENCRYPT_KEY_LEN + 1);

  int32_t code = bseOpen("/tmp/bse_encrypt_boundary", &cfg, &bse);
  ASSERT_EQ(code, 0);

  std::vector<int64_t> data;
  
  // Test various data sizes
  int testSizes[] = {1, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 
                     255, 256, 257, 1023, 1024, 1025, 4095, 4096, 4097};
  
  for (int size : testSizes) {
    std::vector<int64_t> testData;
    putData(bse, 10, size, &testData);
    getData(bse, &testData, size);
    data.insert(data.end(), testData.begin(), testData.end());
  }
  
  bseCommit(bse);
  
  // Verify all data after commit
  int idx = 0;
  for (int size : testSizes) {
    for (int i = 0; i < 10; i++) {
      uint8_t *value = NULL;
      int32_t  len = 0;
      code = bseGet(bse, data[idx++], &value, &len);
      ASSERT_EQ(code, 0);
      ASSERT_EQ(len, size);
      taosMemoryFree(value);
    }
  }
  
  bseClose(bse);
#endif
}

TEST(bseCase, encryptionMixedDataTest) {
#ifdef LINUX
  initLog();
  SBse   *bse = NULL;
  SBseCfg cfg = {.vgId = 2};
  taosRemoveDir("/tmp/bse_encrypt_mixed");

  // Set encryption config
  tstrncpy(cfg.encryptAlgrName, TSDB_ENCRYPT_ALGO_SM4_STR, TSDB_ENCRYPT_ALGR_NAME_LEN);
  tstrncpy(cfg.encryptKey, "mixed_data_test_key_123456789abcde", ENCRYPT_KEY_LEN + 1);

  int32_t code = bseOpen("/tmp/bse_encrypt_mixed", &cfg, &bse);
  ASSERT_EQ(code, 0);

  std::vector<int64_t> data;
  
  // Mix of random and non-random data
  putData(bse, 1000, 100, &data);
  putNoRandomData(bse, 1000, 100, &data);
  putData(bse, 1000, 1000, &data);
  putNoRandomData(bse, 1000, 1000, &data);
  
  bseCommit(bse);
  
  // Verify all data
  for (int i = 0; i < data.size(); i++) {
    uint8_t *value = NULL;
    int32_t  len = 0;
    code = bseGet(bse, data[i], &value, &len);
    ASSERT_EQ(code, 0);
    
    // Check expected length
    if (i < 2000) {
      ASSERT_EQ(len, 100);
    } else {
      ASSERT_EQ(len, 1000);
    }
    taosMemoryFree(value);
  }
  
  bseClose(bse);
#endif
}

TEST(bseCase, encryptionPerformanceComparisonTest) {
#ifdef LINUX
  initLog();
  
  // Test without encryption
  {
    SBse   *bse = NULL;
    SBseCfg cfg = {.vgId = 2};
    taosRemoveDir("/tmp/bse_perf_no_encrypt");

    int64_t startTime = taosGetTimestampMs();
    
    int32_t code = bseOpen("/tmp/bse_perf_no_encrypt", &cfg, &bse);
    ASSERT_EQ(code, 0);

    std::vector<int64_t> data;
    putData(bse, 10000, 1000, &data);
    bseCommit(bse);
    getData(bse, &data, 1000);
    
    int64_t endTime = taosGetTimestampMs();
    printf("No encryption time: %ld ms\n", endTime - startTime);
    
    bseClose(bse);
  }
  
  // Test with encryption
  {
    SBse   *bse = NULL;
    SBseCfg cfg = {.vgId = 2};
    taosRemoveDir("/tmp/bse_perf_encrypt");

    tstrncpy(cfg.encryptAlgrName, TSDB_ENCRYPT_ALGO_SM4_STR, TSDB_ENCRYPT_ALGR_NAME_LEN);
    tstrncpy(cfg.encryptKey, "perf_test_key_1234567890abcdefgh", ENCRYPT_KEY_LEN + 1);

    int64_t startTime = taosGetTimestampMs();
    
    int32_t code = bseOpen("/tmp/bse_perf_encrypt", &cfg, &bse);
    ASSERT_EQ(code, 0);

    std::vector<int64_t> data;
    putData(bse, 10000, 1000, &data);
    bseCommit(bse);
    getData(bse, &data, 1000);
    
    int64_t endTime = taosGetTimestampMs();
    printf("With encryption time: %ld ms\n", endTime - startTime);
    
    bseClose(bse);
  }
#endif
}
