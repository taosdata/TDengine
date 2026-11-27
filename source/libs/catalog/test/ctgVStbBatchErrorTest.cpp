/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

/*
 * Regression test for TD-7106982978.
 *
 * A failed vnode response in a batch has reqType == -1.  The virtual-table
 * reference task must still count that response and finish with the original
 * vnode error after every vgroup has replied.
 */

#include <gtest/gtest.h>

#include "catalogInt.h"
#include "taoserror.h"
#include "tref.h"

extern "C" int32_t ctgHandleGetVStbRefDbsRsp(SCtgTaskReq* tReq, int32_t reqType, const SDataBuf* pMsg,
                                                int32_t rspCode);

namespace {

SDataBuf makeEmptyVSubTablesRsp(char** ppBuf) {
  SDataBuf data = {0};
  SVSubTablesRsp rsp = {0};
  rsp.vgId = 1;
  rsp.pTables = taosArrayInit(1, sizeof(SVCTableRefCols*));
  EXPECT_NE(rsp.pTables, nullptr);
  if (rsp.pTables == nullptr) {
    return data;
  }

  int32_t len = tSerializeSVSubTablesRsp(nullptr, 0, &rsp);
  EXPECT_GT(len, 0);
  if (len <= 0) {
    taosArrayDestroy(rsp.pTables);
    return data;
  }

  *ppBuf = static_cast<char*>(taosMemoryMalloc(len));
  EXPECT_NE(*ppBuf, nullptr);
  if (*ppBuf == nullptr) {
    taosArrayDestroy(rsp.pTables);
    return data;
  }
  EXPECT_EQ(tSerializeSVSubTablesRsp(*ppBuf, len, &rsp), len);

  taosArrayDestroy(rsp.pTables);
  data.pData = *ppBuf;
  data.len = static_cast<uint32_t>(len);
  return data;
}

}  // namespace

TEST(ctgVStbRefDbsRsp, failedBatchResponseCompletesTaskOnce) {
  initQueryModuleMsgHandle();

  SCtgVStbRefDbsCtx ctx = {0};
  ctx.vgNum = 2;
  ctx.pSubTablesList = taosArrayInit(ctx.vgNum, sizeof(SVSubTablesRsp));
  ASSERT_NE(ctx.pSubTablesList, nullptr);
  SVSubTablesRsp emptyRsp = {0};
  ASSERT_NE(taosArrayPush(ctx.pSubTablesList, &emptyRsp), nullptr);
  ASSERT_NE(taosArrayPush(ctx.pSubTablesList, &emptyRsp), nullptr);

  SCtgJob job = {0};
  job.pTasks = taosArrayInit(2, sizeof(SCtgTask));
  ASSERT_NE(job.pTasks, nullptr);
  SCtgTask dummyTask = {};
  ASSERT_NE(taosArrayPush(job.pTasks, &dummyTask), nullptr);
  ASSERT_NE(taosArrayPush(job.pTasks, &dummyTask), nullptr);

  SCtgTask task = {};
  task.type = CTG_TASK_GET_V_STBREFDBS;
  task.pJob = &job;
  task.taskCtx = &ctx;

  SCtgTaskReq req = {0};
  req.pTask = &task;
  req.msgIdx = -1;

  char*    rspBuf = nullptr;
  SDataBuf rsp = makeEmptyVSubTablesRsp(&rspBuf);
  ASSERT_NE(rspBuf, nullptr);

  EXPECT_EQ(ctgHandleGetVStbRefDbsRsp(&req, TDMT_VND_VSUBTABLES_META, &rsp, TSDB_CODE_SUCCESS),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(ctx.resDoneNum, 1);
  EXPECT_EQ(ctx.resIdx, 1);
  EXPECT_NE(task.status, CTG_TASK_DONE);
  EXPECT_EQ(job.taskDone, 0);

  SDataBuf failedRsp = {0};
  EXPECT_EQ(ctgHandleGetVStbRefDbsRsp(&req, -1, &failedRsp, TSDB_CODE_VND_INVALID_VGROUP_ID),
            TSDB_CODE_VND_INVALID_VGROUP_ID);
  EXPECT_EQ(ctx.resDoneNum, 2);
  EXPECT_EQ(ctx.resIdx, 2);
  EXPECT_EQ(ctx.resCode, TSDB_CODE_VND_INVALID_VGROUP_ID);
  EXPECT_EQ(task.status, CTG_TASK_DONE);
  EXPECT_EQ(task.code, TSDB_CODE_VND_INVALID_VGROUP_ID);
  EXPECT_EQ(job.taskDone, 1);
  EXPECT_EQ(job.jobResCode, TSDB_CODE_VND_INVALID_VGROUP_ID);

  EXPECT_EQ(ctgHandleGetVStbRefDbsRsp(&req, -1, &failedRsp, TSDB_CODE_VND_INVALID_VGROUP_ID),
            TSDB_CODE_VND_INVALID_VGROUP_ID);
  EXPECT_EQ(job.taskDone, 1) << "the catalog task must only finish once";

  taosMemoryFree(rspBuf);
  taosArrayDestroyEx(ctx.pSubTablesList, tDestroySVSubTablesRsp);
  taosArrayDestroy(job.pTasks);
}

TEST(ctgVStbRefDbsRsp, failedVgroupLookupReachesUserCallback) {
  struct Result {
    tsem_t sem;
    int32_t calls;
    int32_t errors[3];
  } result = {};
  auto callback = [](SMetaData* meta, void* param, int32_t) {
    auto* result = static_cast<Result*>(param);
    ++result->calls;
    EXPECT_EQ(taosArrayGetSize(meta->pVStbRefDbs), 3u);
    for (size_t i = 0; i < 3 && i < taosArrayGetSize(meta->pVStbRefDbs); ++i) {
      result->errors[i] = static_cast<SMetaRes*>(taosArrayGet(meta->pVStbRefDbs, i))->code;
    }
    EXPECT_EQ(tsem_post(&result->sem), 0);
  };

  SCatalog* catalog = nullptr;
  ASSERT_EQ(catalogInit(nullptr), 0);
  ASSERT_EQ(catalogGetHandle(1, &catalog), 0);
  ASSERT_EQ(tsem_init(&result.sem, 0, 0), 0);
  SName leaves[] = {{TSDB_TABLE_NAME_T, 1, "db", "a"}, {TSDB_TABLE_NAME_T, 1, "db", "b"},
                    {TSDB_TABLE_NAME_T, 1, "db", "c"}};
  SCatalogReq req = {};
  req.pVStbRefDbs = taosArrayFromList(leaves, 3, sizeof(SName));
  ASSERT_NE(req.pVStbRefDbs, nullptr);
  SRequestConnInfo conn = {};
  SCtgJob* job = nullptr;
  ASSERT_EQ(ctgInitJob(catalog, &conn, &job, &req, callback, &result), 0);

  // Resume each launched leaf as if its table metadata just arrived. All share a cold DB route.
  auto resume = [&](int32_t id) {
    auto* task = ctgGetTask(job, id);
    auto* meta = static_cast<STableMeta*>(taosMemoryCalloc(1, sizeof(STableMeta)));
    if (!meta) return TSDB_CODE_OUT_OF_MEMORY;
    meta->virtualStb = true;
    meta->tableType = TSDB_SUPER_TABLE;
    task->status = CTG_TASK_LAUNCHED;
    task->subRes.type = CTG_TASK_GET_TB_META;
    task->subRes.res = meta;
    SCtgTaskReq subReq = {};
    subReq.pTask = task;
    subReq.msgIdx = -1;
    return ctgGetVStbRefDbsCb(&subReq);
  };

  auto& ops = gCtgAsyncFps[CTG_TASK_GET_DB_VGROUP];
  auto init = ops.initFp;
  ops.initFp = [](SCtgJob*, int32_t, void*) { return TSDB_CODE_OUT_OF_MEMORY; };
  int32_t initCode = resume(0);
  ops.initFp = init;
  EXPECT_EQ(initCode, 0);
  EXPECT_EQ(job->taskDone, 1);

  auto launch = ops.launchFp;
  ops.launchFp = [](SCtgTask*) { return TSDB_CODE_RPC_NETWORK_UNAVAIL; };
  int32_t launchCode = resume(1);
  ops.launchFp = launch;
  EXPECT_EQ(launchCode, 0);
  EXPECT_EQ(job->taskDone, 3);

  // The last leaf subscribes after the vgroup task failed and has no result to clone.
  EXPECT_EQ(resume(2), 0);
  EXPECT_EQ(tsem_timewait(&result.sem, 5000), 0);
  EXPECT_EQ(result.calls, 1);
  EXPECT_EQ(result.errors[0], TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(result.errors[1], TSDB_CODE_RPC_NETWORK_UNAVAIL);
  EXPECT_EQ(result.errors[2], TSDB_CODE_RPC_NETWORK_UNAVAIL);
  EXPECT_EQ(job->taskDone, 4);
  EXPECT_EQ(taosReleaseRef(gCtgMgmt.jobPool, job->refId), 0);
  taosArrayDestroy(req.pVStbRefDbs);
  EXPECT_EQ(tsem_destroy(&result.sem), 0);
  catalogDestroy();
}
