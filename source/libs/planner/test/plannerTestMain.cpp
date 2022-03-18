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

#include "mockCatalog.h"

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

int main(int argc, char* argv[]) {
	testing::AddGlobalTestEnvironment(new PlannerEnv());
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
