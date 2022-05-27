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
    initMetaDataEnv();
    generateMetaData();
  }

  virtual void TearDown() {
    destroyMetaDataEnv();
    taosCleanupKeywordsTable();
    fmFuncMgtDestroy();
  }

  ParserEnv() {}
  virtual ~ParserEnv() {}
};

static void parseArg(int argc, char* argv[]) {
  int                  opt = 0;
  const char*          optstring = "";
  static struct option long_options[] = {
      {"dump", no_argument, NULL, 'd'}, {"async", no_argument, NULL, 'a'}, {0, 0, 0, 0}};
  while ((opt = getopt_long(argc, argv, optstring, long_options, NULL)) != -1) {
    switch (opt) {
      case 'd':
        g_dump = true;
        break;
      case 'a':
        g_testAsyncApis = true;
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
