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
#include <tglobal.h>
#include <iostream>
#pragma GCC diagnostic ignored "-Wwrite-strings"

#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#include "os.h"

#include "taos.h"
#include "tdef.h"
#include "tvariant.h"
#include "catalog.h"
#include "scheduler.h"
#include "tep.h"
#include "trpc.h"

namespace {
void mockBuildDag(SQueryDag *dag) {
  uint64_t qId = 0x111111111111;
  
  dag->queryId = qId;
  dag->numOfSubplans = 2;
  dag->pSubplans = taosArrayInit(dag->numOfSubplans, POINTER_BYTES);
  SArray *scan = taosArrayInit(1, sizeof(SSubplan));
  SArray *merge = taosArrayInit(1, sizeof(SSubplan));
  
  SSubplan scanPlan = {0};
  SSubplan mergePlan = {0};

  scanPlan.id.queryId = qId;
  scanPlan.id.templateId = 0x2222222222;
  scanPlan.id.subplanId = 0x3333333333;
  scanPlan.type = QUERY_TYPE_SCAN;
  scanPlan.level = 1;
  scanPlan.execEpSet.numOfEps = 1;
  scanPlan.pChildern = NULL;
  scanPlan.pParents = taosArrayInit(1, POINTER_BYTES);

  mergePlan.id.queryId = qId;
  mergePlan.id.templateId = 0x4444444444;
  mergePlan.id.subplanId = 0x5555555555;
  mergePlan.type = QUERY_TYPE_MERGE;
  mergePlan.level = 0;
  mergePlan.execEpSet.numOfEps = 1;
  mergePlan.pChildern = taosArrayInit(1, POINTER_BYTES);
  mergePlan.pParents = NULL;

  SSubplan *mergePointer = (SSubplan *)taosArrayPush(merge, &mergePlan);
  SSubplan *scanPointer = (SSubplan *)taosArrayPush(scan, &scanPlan);

  taosArrayPush(mergePointer->pChildern, &scanPointer);
  taosArrayPush(scanPointer->pParents, &mergePointer);

  taosArrayPush(dag->pSubplans, &merge);  
  taosArrayPush(dag->pSubplans, &scan);
}

}

TEST(testCase, normalCase) {
  void *mockPointer = (void *)0x1;
  char *clusterId = "cluster1";
  char *dbname = "1.db1";
  char *tablename = "table1";
  SVgroupInfo vgInfo = {0};
  void *pJob = NULL;
  SQueryDag dag = {0};
  SArray *qnodeList = taosArrayInit(1, sizeof(SEpAddr));
  
  int32_t code = schedulerInit(NULL);
  ASSERT_EQ(code, 0);

  mockBuildDag(&dag);
  
  code = scheduleExecJob(mockPointer, qnodeList, &dag, &pJob);
  ASSERT_EQ(code, 0);
}


int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}




