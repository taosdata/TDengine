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
  printf("put result ");
  code = bseCommitBatch(bse, pBatch);
  return code;
}
static int32_t getData(SBse *pBse, std::vector<int64_t> *data) {
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
      // std::string str((char *)value, len);
      // printf("get result %d: %s\n", i, str.c_str());
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
        //printf("succ to get key %d\n", (int32_t)seq);
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

  putStringData(bse, 10000, str, &data);
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
    getData(bse, &data);

    putData(bse, 10000, 200, &data);

    bseCommit(bse);

    putData(bse, 10000, 200, &data);

    getData(bse, &data);
    bseCommit(bse);

    getData(bse, &data);

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
  int32_t              code = bseOpen("/tmp/bse", &cfg, &bse);
  putData(bse, 10000, 1000, &data);
  getData(bse, &data);

  bseCommit(bse);

  getData(bse, &data);

  bseClose(bse);

  {
    code = bseOpen("/tmp/bse", &cfg, &bse);
    getData(bse, &data);
    bseClose(bse);
  }

  return 0;
}
int32_t funcTestSmallData() {
  SBse                *bse = NULL;
  SBseCfg              cfg = {.vgId = 2};
  taosRemoveDir("/tmp/bse");

  std::vector<int64_t> data;
  int32_t              code = bseOpen("/tmp/bse", &cfg, &bse);
  putData(bse, 1000, 100000, &data);
  getData(bse, &data);

  bseCommit(bse);

  getData(bse, &data);

  putData(bse, 1000, 100000, &data);

  bseCommit(bse);

  putData(bse, 1000, 100000, &data);
  getData(bse, &data);

  bseCommit(bse);

  getData(bse, &data);

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
    // getData(bse, &data);
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
  // benchTest();
  // funcTest();
  //funcTestSmallData();
#endif
}
TEST(bseCase, snapTest) {
#ifdef LINUX
  initLog();
  snapTest();
  // //snapTest();
  // emptySnapTest();
  // // benchTest();
  // // funcTest();
  // //funcTestSmallData();
#endif
}
TEST(bseCase, benchTest) {
#ifdef LINUX
  initLog();
  benchTest();
  // // funcTest();
  // //funcTestSmallData();
#endif
}
TEST(bseCase, funcTest) {
#ifdef LINUX
  initLog();
  // // benchTest();
  funcTest();
  // //funcTestSmallData();
#endif
}
TEST(bseCase, smallDataTest) {
#ifdef LINUX
  initLog();
  funcTestSmallData();
#endif
}

TEST(bseCase, multiThreadReadWriteTest) {
  // Implement multi-threaded read/write test
#ifdef LINUX
  initLog();
  SBse *bse = NULL;
  SBseCfg cfg = {.vgId = 2};
  taosRemoveDir("/tmp/bse");

  int32_t code = bseOpen("/tmp/bse", &cfg, &bse);
  ASSERT_EQ(code, 0);

  std::vector<int64_t> data;
  putData(bse, 10000, 1000, &data);
  bseCommit(bse);
  
  getData(bse, &data);
  
  bseClose(bse);
#endif
}

TEST(bseCase, recover) {
  // Implement multi-threaded read/write test
#ifdef LINUX
  initLog();
  SBse *bse = NULL;
  SBseCfg cfg = {.vgId = 2};
  taosRemoveDir("/tmp/bse");

  int32_t code = bseOpen("/tmp/bse", &cfg, &bse);
  ASSERT_EQ(code, 0);

  std::vector<int64_t> data;
  putData(bse, 10000, 1000, &data);
  getData(bse, &data);
  bseCommit(bse);
  
  getData(bse, &data);
  putData(bse, 10000, 1000, &data);

  bseCommit(bse);
  bseClose(bse);
  {
    code = bseOpen("/tmp/bse", &cfg, &bse);  
    ASSERT_EQ(code, 0);

    getData(bse, &data); 

    bseClose(bse);
  }
  
#endif
}
TEST(bseCase, emptyNot) {
  // Implement multi-threaded read/write test
#ifdef LINUX
  initLog();
  SBse *bse = NULL;
  SBseCfg cfg = {.vgId = 2};
  taosRemoveDir("/tmp/bse");

  // int32_t code = bseOpen("/tmp/bse", &cfg, &bse);
  // ASSERT_EQ(code, 0);

  // std::vector<int64_t> data;
  // putData(bse, 10000, 1000, &data);
  // bseCommit(bse);
  
  // getData(bse, &data);
  // putData(bse, 10000, 1000, &data);

  // bseCommit(bse);
  // bseClose(bse);
  // {
  std::vector<int64_t> data;
  data.push_back(1); 
  data.push_back(2); 
  data.push_back(3); 
  int32_t code = bseOpen("/tmp/bse", &cfg, &bse);  
  char *value = NULL;
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

  