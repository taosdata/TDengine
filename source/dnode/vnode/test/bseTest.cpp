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
#include <vnodeInt.h>

#include <taoserror.h>
#include <tglobal.h>
#include <iostream>

#include <tmsg.h>
#include <vnodeInt.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

SDmNotifyHandle dmNotifyHdl = {.state = 0};

#include "bse.h"
int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

// void tqWriteOffset() {
//   TdFilePtr pFile = taosOpenFile(TQ_OFFSET_NAME, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);

//   STqOffset offset = {.val = {.type = TMQ_OFFSET__LOG, .version = 8923}};
//   strcpy(offset.subKey, "testtest");
//   int32_t    bodyLen;
//   int32_t    code;
//   tEncodeSize(tEncodeSTqOffset, &offset, bodyLen, code);
//   int32_t totLen = INT_BYTES + bodyLen;
//   void*   buf = taosMemoryCalloc(1, totLen);
//   void*   abuf = POINTER_SHIFT(buf, INT_BYTES);

//   *(int32_t*)buf = htonl(bodyLen);
//   SEncoder encoder;
//   tEncoderInit(&encoder, (uint8_t*)abuf, bodyLen);
//   tEncodeSTqOffset(&encoder, &offset);
//   taosWriteFile(pFile, buf, totLen);

//   taosMemoryFree(buf);

//   taosCloseFile(&pFile);
// }

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
    taosRemoveDir(path);
    taosMkDir(path);
    tstrncpy(tsLogDir, path, PATH_MAX);
    if (taosInitLog("taosdlog", 1, false) != 0) {
      printf("failed to init log file\n");
    }
}
TEST(bseCase, openTest) {
    initLog();

    SBse *bse = NULL;
    std::vector<int64_t> data;
    SBseCfg cfg = {.vgId = 2};

    SBseBatch *pBatch = NULL;
    
    int32_t code = bseOpen("/tmp/bse", &cfg, &bse);
    code = bseBatchInit(bse, &pBatch,1024);
    for (int32_t i = 0; i < 10000; i++) {
      int64_t seq = 0;
      char *buf = "test";
      code = bseBatchPut(pBatch, &seq, (uint8_t *)buf, strlen(buf)); 
      data.push_back(seq);  
    }

    code = bseAppendBatch(bse, pBatch); 
        

    for (int32_t i = 0; i < 10000; i++) {
      char *p = NULL;
      int32_t len = 0;
      int64_t seq = data[i];
      code = bseGet(bse, seq, (uint8_t **)&p, &len);
      taosMemoryFree(p);
      printf("read at index %d\n", i);
      ASSERT_EQ(len, 4);
      //code = bseRead(bse, data[i], NULL, NULL);
    }
    bseCommit(bse);
    
    for (int32_t i = 0; i < 1000; i++) {
      char *p = NULL;
      int32_t len = 0;
      int64_t seq = data[i];
      code = bseGet(bse, seq, (uint8_t **)&p, &len);
      taosMemoryFree(p);
      ASSERT_EQ(len, 4);
        //code = bseAppend(bse, &seq, (uint8_t *)"test", 4);
        //data.push_back(seq); 
    }
    bseCommit(bse);


    for (int32_t i = 1; i < 10000; i++) {
      uint8_t* value = NULL;
      int32_t len = 0;
      uint64_t seq = data[i];
      code = bseGet(bse, seq, &value, &len);
      if (code != 0) {
        printf("failed to get key %d error code: %d\n", i, code);
      }
      taosMemFree(value);
      //ASSERT_EQ(len, 4); 
    }
    bseClose(bse);
    
}

#pragma GCC diagnostic pop
