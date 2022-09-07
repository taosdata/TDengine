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
using namespace std;

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
    path = TD_TMP_DIR_PATH _path;
  }
  int SetUp(bool remove) {
    initLog();

    if (remove) taosRemoveDir(path);

    int ret = indexJsonOpen(&opts, path, &index);
    return ret;
  }
  int Write(WriteBatch *batch) {
    // write batch
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

int BenchWrite(Idx *idx, int nCol, int Limit) { return 0; }

int BenchRead(Idx *idx) { return 0; }

int main() {
  Idx *idx = new Idx;
  if (idx->SetUp(true) != 0) {
    std::cout << "failed to setup index" << std::endl;
    return 0;
  } else {
    std::cout << "succ to setup index" << std::endl;
  }
  BenchWrite(idx, 10000, 100);
}
