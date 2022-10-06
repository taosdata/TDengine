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

#include "functionMgt.h"
#include "getopt.h"
#include "mockCatalog.h"
#include "parser.h"
#include "planTestUtil.h"
#include "tglobal.h"

class PlannerEnv : public testing::Environment {
 public:
  virtual void SetUp() {
    fmFuncMgtInit();
    initMetaDataEnv();
    generateMetaData();
    initLog(TD_TMP_DIR_PATH "td");
    initCfg();
    nodesInitAllocatorSet();
  }

  virtual void TearDown() {
    destroyMetaDataEnv();
    qCleanupKeywordsTable();
    fmFuncMgtDestroy();
    taosCloseLog();
    nodesDestroyAllocatorSet();
  }

  PlannerEnv() {}
  virtual ~PlannerEnv() {}

 private:
  void initLog(const char* path) {
    int32_t logLevel = getLogLevel() | DEBUG_SCREEN;
    dDebugFlag = logLevel;
    vDebugFlag = logLevel;
    mDebugFlag = logLevel;
    cDebugFlag = logLevel;
    jniDebugFlag = logLevel;
    tmrDebugFlag = logLevel;
    uDebugFlag = logLevel;
    rpcDebugFlag = logLevel;
    qDebugFlag = logLevel;
    wDebugFlag = logLevel;
    sDebugFlag = logLevel;
    tsdbDebugFlag = logLevel;
    tsLogEmbedded = 1;
    tsAsyncLog = 0;

    taosRemoveDir(path);
    taosMkDir(path);
    tstrncpy(tsLogDir, path, PATH_MAX);
    if (taosInitLog("taoslog", 1) != 0) {
      std::cout << "failed to init log file" << std::endl;
    }
  }

  void initCfg() { tsQueryPlannerTrace = true; }
};

static void parseArg(int argc, char* argv[]) {
  int         opt = 0;
  const char* optstring = "";
  // clang-format off
  static struct option long_options[] = {
    {"dump", optional_argument, NULL, 'd'},
    {"skipSql", required_argument, NULL, 's'},
    {"limitSql", required_argument, NULL, 'i'},
    {"log", required_argument, NULL, 'l'},
    {"queryPolicy", required_argument, NULL, 'q'},
    {"useNodeAllocator", required_argument, NULL, 'a'},
    {0, 0, 0, 0}
  };
  // clang-format on
  while ((opt = getopt_long(argc, argv, optstring, long_options, NULL)) != -1) {
    switch (opt) {
      case 'd':
        setDumpModule(optarg);
        break;
      case 's':
        setSkipSqlNum(optarg);
        break;
      case 'i':
        setLimitSqlNum(optarg);
        break;
      case 'l':
        setLogLevel(optarg);
        break;
      case 'q':
        setQueryPolicy(optarg);
        break;
      case 'a':
        setUseNodeAllocator(optarg);
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
