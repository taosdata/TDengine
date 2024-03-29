/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3 * or later ("AGPL"), as published by the Free
 * Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
#include <gtest/gtest.h>
#include <algorithm>
#include <iostream>
#include <string>
#include <thread>
#include "index.h"
#include "indexCache.h"
#include "indexFst.h"
#include "indexFstUtil.h"
#include "indexInt.h"
#include "indexTfile.h"
#include "indexUtil.h"
#include "tskiplist.h"
#include "tutil.h"
using namespace std;

static std::string logDir = TD_TMP_DIR_PATH "log";

static void initLog() {
  const char   *defaultLogFileNamePrefix = "taoslog";
  const int32_t maxLogFileNum = 10;

  tsAsyncLog = 0;
  idxDebugFlag = 143;
  strcpy(tsLogDir, logDir.c_str());
  taosRemoveDir(tsLogDir);
  taosMkDir(tsLogDir);

  if (taosInitLog(defaultLogFileNamePrefix, maxLogFileNum) < 0) {
    printf("failed to open log file in directory:%s\n", tsLogDir);
  }
}

struct WriteBatch {
  SIndexMultiTerm *terms;
};
class Idx {
 public:
  Idx(int _cacheSize = 1024 * 1024 * 4, const char *_path = "tindex") {
    opts.cacheSize = _cacheSize;
    path += TD_TMP_DIR_PATH;
    path += _path;
  }
  int SetUp(bool remove) {
    initLog();

    if (remove) taosRemoveDir(path.c_str());

    int ret = indexJsonOpen(&opts, path.c_str(), &index);
    return ret;
  }
  int Write(WriteBatch *batch, uint64_t uid) {
    // write batch
    indexJsonPut(index, batch->terms, uid);
    return 0;
  }
  int Read(const char *json, void *key, int64_t *id) {
    // read batch
    return 0;
  }

  void TearDown() { indexJsonClose(index); }

  std::string path;

  SIndexOpts opts;
  SIndex    *index;
};

SIndexTerm *indexTermCreateT(int64_t suid, SIndexOperOnColumn oper, uint8_t colType, const char *colName,
                             int32_t nColName, const char *colVal, int32_t nColVal) {
  char    buf[256] = {0};
  int16_t sz = nColVal;
  memcpy(buf, (uint16_t *)&sz, 2);
  memcpy(buf + 2, colVal, nColVal);
  if (colType == TSDB_DATA_TYPE_BINARY || colType == TSDB_DATA_TYPE_GEOMETRY) {
    return indexTermCreate(suid, oper, colType, colName, nColName, buf, sizeof(buf));
  } else {
    return indexTermCreate(suid, oper, colType, colName, nColName, colVal, nColVal);
  }
  return NULL;
}
int initWriteBatch(WriteBatch *wb, int batchSize) {
  SIndexMultiTerm *terms = indexMultiTermCreate();

  std::string colName;
  std::string colVal;

  for (int i = 0; i < 64; i++) {
    colName += '0' + i;
    colVal += '0' + i;
  }

  for (int i = 0; i < batchSize; i++) {
    colVal[i % colVal.size()] = '0' + i % 128;
    colName[i % colName.size()] = '0' + i % 128;
    SIndexTerm *term = indexTermCreateT(0, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                        colVal.c_str(), colVal.size());
    indexMultiTermAdd(terms, term);
  }

  wb->terms = terms;
  return 0;
}

int BenchWrite(Idx *idx, int batchSize, int limit) {
  for (int i = 0; i < limit; i += batchSize) {
    WriteBatch wb;
    idx->Write(&wb, i);
  }
  return 0;
}

int BenchRead(Idx *idx) { return 0; }

int main() {
  // Idx *idx = new Idx;
  // if (idx->SetUp(true) != 0) {
  //   std::cout << "failed to setup index" << std::endl;
  //   return 0;
  // } else {
  //   std::cout << "succ to setup index" << std::endl;
  // }
  //  BenchWrite(idx, 100, 10000);
  return 1;
}
