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

#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>

#include <gtest/gtest.h>

#ifdef WINDOWS
#define TD_USE_WINSOCK
#endif

#include "functionMgt.h"
#include "mockCatalog.h"
#include "os.h"
#include "parTestUtil.h"
#include "parToken.h"

namespace ParserTest {

class ParserEnv : public testing::Environment {
 public:
  virtual void SetUp() {
    fmFuncMgtInit();
    initMetaDataEnv();
    generateMetaData();
    initLog(TD_TMP_DIR_PATH "td");
  }

  virtual void TearDown() {
    destroyMetaDataEnv();
    taosCleanupKeywordsTable();
    fmFuncMgtDestroy();
    taosCloseLog();
  }

  ParserEnv() {}
  virtual ~ParserEnv() {}

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
};

static void parseArg(int argc, char* argv[]) {
  int         opt = 0;
  const char* optstring = "";
  // clang-format off
  static struct option long_options[] = {
    {"dump", no_argument, NULL, 'd'},
    {"async", required_argument, NULL, 'a'},
    {"skipSql", required_argument, NULL, 's'},
    {"limitSql", required_argument, NULL, 'i'},
    {"log", required_argument, NULL, 'l'},
    {0, 0, 0, 0}
  };
  // clang-format on
  while ((opt = getopt_long(argc, argv, optstring, long_options, NULL)) != -1) {
    switch (opt) {
      case 'd':
        g_dump = true;
        break;
      case 'a':
        setAsyncFlag(optarg);
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
      default:
        break;
    }
  }
}

}  // namespace ParserTest

int main(int argc, char* argv[]) {
  testing::AddGlobalTestEnvironment(new ParserTest::ParserEnv());
  testing::InitGoogleTest(&argc, argv);
  ParserTest::parseArg(argc, argv);
  return RUN_ALL_TESTS();
}
