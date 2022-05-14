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

#include <string>

#include <gtest/gtest.h>
#include "getopt.h"
#include "mockCatalog.h"
#include "planTestUtil.h"

class PlannerEnv : public testing::Environment {
 public:
  virtual void SetUp() {
    initMetaDataEnv();
    generateMetaData();
    initLog("/tmp/td");
  }

  virtual void TearDown() { destroyMetaDataEnv(); }

  PlannerEnv() {}
  virtual ~PlannerEnv() {}

 private:
  void initLog(const char* path) {
    dDebugFlag = 143;
    vDebugFlag = 0;
    mDebugFlag = 143;
    cDebugFlag = 0;
    jniDebugFlag = 0;
    tmrDebugFlag = 135;
    uDebugFlag = 135;
    rpcDebugFlag = 143;
    qDebugFlag = 143;
    wDebugFlag = 0;
    sDebugFlag = 0;
    tsdbDebugFlag = 0;
    tsLogEmbedded = 1;
    tsAsyncLog = 0;

    taosRemoveDir(path);
    taosMkDir(path);
    tstrncpy(tsLogDir, path, PATH_MAX);
    if (taosInitLog("taoslog", 1) != 0) {
      std::cout << "failed to init log file" << std::endl;
    }
  }
};

static void parseArg(int argc, char* argv[]) {
  int                  opt = 0;
  const char*          optstring = "";
  static struct option long_options[] = {
      {"dump", optional_argument, NULL, 'd'}, {"skipSql", optional_argument, NULL, 's'}, {0, 0, 0, 0}};
  while ((opt = getopt_long(argc, argv, optstring, long_options, NULL)) != -1) {
    switch (opt) {
      case 'd':
        setDumpModule(optarg);
        break;
      case 's':
        g_skipSql = 1;
        break;
      default:
        break;
    }
  }
}

int main(int argc, char* argv[]) {
  testing::AddGlobalTestEnvironment(new PlannerEnv());
  testing::InitGoogleTest(&argc, argv);
  parseArg(argc, argv);
  return RUN_ALL_TESTS();
}
