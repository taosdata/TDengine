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
  }

  virtual void TearDown() {
    destroyMetaDataEnv();
  }

  PlannerEnv() {}
  virtual ~PlannerEnv() {}
};

static void parseArg(int argc, char* argv[]) {
  int opt = 0;
  const char *optstring = "";  
  static struct option long_options[] = {
      {"dump", no_argument, NULL, 'd'},
      {0, 0, 0, 0}
  };
  while ((opt = getopt_long(argc, argv, optstring, long_options, NULL)) != -1) {
    switch (opt) {
      case 'd':
        g_isDump = true;
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
