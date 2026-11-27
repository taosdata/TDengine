/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * Regression test for Feishu defect 7162305557 (taosadapter core, 3.4.3.6).
 *
 * ctgHandleGetTbMetasRsp() used pFetch unconditionally in its _return error
 * path. Whenever a batch response could not be resolved to a fetch of the
 * task - an out-of-range msg index, a batch reply routed to a task whose
 * request contexts are still being built, or an uninitialized task ctx - the
 * handler reached _return with a NULL (or still uninitialized) pFetch and
 * crashed with the exact core signature captured on site:
 *
 *   movslq 0xc(%rax),%rsi    ; pFetch->resIdx, rax == NULL
 *
 * The first two tests drive the real batch dispatcher and the real handler
 * with synthetic task state. Before the fix the dispatcher test dies with
 * that null dereference; after the fix the response fails the task with a
 * catalog error instead of taking the process down, and the request contexts
 * that may still be under construction are left alone.
 */

#include <gtest/gtest.h>

#include <atomic>
#include <thread>

#include "catalogInt.h"
#include "taoserror.h"

extern "C" int32_t ctgHandleBatchRsp(SCtgJob* pJob, SCtgTaskCallbackParam* cbParam, SDataBuf* pMsg, int32_t rspCode);
extern "C" int32_t ctgHandleGetTbMetasRsp(SCtgTaskReq* tReq, int32_t reqType, const SDataBuf* pMsg, int32_t rspCode);
extern "C" int32_t ctgInitTask(SCtgJob* pJob, CTG_TASK_TYPE type, void* param, int32_t* taskId);
extern "C" void    ctgFreeTaskCtx(SCtgTask* pTask);

namespace {

/*
 * Build the minimal task state the handler walks on a response that cannot be
 * mapped to a fetch. The task lives at index 0 of the job task array (the
 * dispatcher resolves tasks by id), and a dummy task keeps the job incomplete
 * so the failed task ends without scheduling the user callback.
 */
void buildTbMetasTask(SCtgJob* pJob, SCtgTbMetasCtx* pCtx) {
  pJob->pTasks = taosArrayInit(2, sizeof(SCtgTask));
  ASSERT_NE(pJob->pTasks, nullptr);

  SCtgTask task = {};
  task.type = CTG_TASK_GET_TB_META_BATCH;
  task.pJob = pJob;
  task.taskCtx = pCtx;
  ASSERT_NE(taosArrayPush(pJob->pTasks, &task), nullptr);

  SCtgTask dummy = {};
  ASSERT_NE(taosArrayPush(pJob->pTasks, &dummy), nullptr);

  pCtx->pResList = taosArrayInit(2, sizeof(SMetaRes));
  ASSERT_NE(pCtx->pResList, nullptr);
  SMetaRes res = {};
  ASSERT_NE(taosArrayPush(pCtx->pResList, &res), nullptr);
  ASSERT_NE(taosArrayPush(pCtx->pResList, &res), nullptr);

  pCtx->pNames = taosArrayInit(1, sizeof(STablesReq));
  ASSERT_NE(pCtx->pNames, nullptr);
  STablesReq tbReq = {};
  tbReq.pTables = taosArrayInit(1, sizeof(SName));
  ASSERT_NE(tbReq.pTables, nullptr);
  SName tbName = {};
  ASSERT_NE(taosArrayPush(tbReq.pTables, &tbName), nullptr);
  ASSERT_NE(taosArrayPush(pCtx->pNames, &tbReq), nullptr);
}

SCtgTask* jobTask(SCtgJob* pJob) { return (SCtgTask*)taosArrayGet(pJob->pTasks, 0); }

void expectTaskFailed(SCtgJob* pJob, int32_t code) {
  SCtgTask* pTask = jobTask(pJob);
  ASSERT_NE(pTask, nullptr);

  EXPECT_EQ(pJob->taskDone, 1);
  EXPECT_EQ(pTask->status, CTG_TASK_DONE);
  EXPECT_EQ(pTask->code, code);
  EXPECT_NE(pTask->res, nullptr) << "a finished batch task must hand a result array to the job dump";
  EXPECT_EQ(taosArrayGetSize((SArray*)pTask->res), 0);
}

void cleanupTbMetasTask(SCtgJob* pJob, SCtgTbMetasCtx* pCtx) {
  SCtgTask* pTask = jobTask(pJob);
  if (pTask) {
    taosArrayDestroy((SArray*)pTask->res);
    taosArrayDestroyEx(pTask->msgCtxs, nullptr);
  }
  taosArrayDestroy(pCtx->pFetchs);
  taosArrayDestroy(pCtx->pResList);
  for (int32_t i = 0; i < taosArrayGetSize(pCtx->pNames); ++i) {
    STablesReq* pReq = (STablesReq*)taosArrayGet(pCtx->pNames, i);
    if (pReq) {
      taosArrayDestroy(pReq->pTables);
    }
  }
  taosArrayDestroy(pCtx->pNames);
  taosArrayDestroy(pJob->pTasks);
}

}  // namespace

// A batch reply is dispatched to a task that has no fetch yet (the state a
// task is in while its request contexts are still being built). This is the
// crash the customer core shows: pFetch == NULL is dereferenced while the
// handler tries to record the failure.
TEST(ctgTbMetasRsp, replyBeforeFetchesExistFailsTask) {
  initQueryModuleMsgHandle();

  SCtgJob        job = {};
  SCtgTbMetasCtx ctx = {};
  buildTbMetasTask(&job, &ctx);

  SArray* pTaskIds = taosArrayInit(1, sizeof(int32_t));
  SArray* pMsgIdxs = taosArrayInit(1, sizeof(int32_t));
  ASSERT_NE(pTaskIds, nullptr);
  ASSERT_NE(pMsgIdxs, nullptr);
  int32_t taskId = 0;
  int32_t msgIdx = 0;
  ASSERT_NE(taosArrayPush(pTaskIds, &taskId), nullptr);
  ASSERT_NE(taosArrayPush(pMsgIdxs, &msgIdx), nullptr);

  SCtgTaskCallbackParam cbParam = {};
  cbParam.taskId = pTaskIds;
  cbParam.msgIdx = pMsgIdxs;
  cbParam.reqType = TDMT_VND_BATCH_META;

  SDataBuf msg = {};
  EXPECT_EQ(ctgHandleBatchRsp(&job, &cbParam, &msg, TSDB_CODE_SUCCESS), TSDB_CODE_SUCCESS);

  expectTaskFailed(&job, TSDB_CODE_CTG_INTERNAL_ERROR);

  taosArrayDestroy(pTaskIds);
  taosArrayDestroy(pMsgIdxs);
  cleanupTbMetasTask(&job, &ctx);
}

// The msg index does not address any fetch of the task.
TEST(ctgTbMetasRsp, outOfRangeFetchFailsTask) {
  initQueryModuleMsgHandle();

  SCtgJob        job = {};
  SCtgTbMetasCtx ctx = {};
  buildTbMetasTask(&job, &ctx);

  ctx.fetchNum = 1;
  ctx.pFetchs = taosArrayInit(1, sizeof(SCtgFetch));
  ASSERT_NE(ctx.pFetchs, nullptr);
  SCtgFetch fetch = {};
  fetch.resIdx = 1;
  ASSERT_NE(taosArrayPush(ctx.pFetchs, &fetch), nullptr);

  SCtgTaskReq req = {};
  req.pTask = jobTask(&job);
  ASSERT_NE(req.pTask, nullptr);
  req.msgIdx = 3;  // the task only has fetch 0

  SDataBuf rsp = {};
  EXPECT_EQ(ctgHandleGetTbMetasRsp(&req, -1, &rsp, TSDB_CODE_SUCCESS), TSDB_CODE_CTG_INTERNAL_ERROR);

  expectTaskFailed(&job, TSDB_CODE_CTG_INTERNAL_ERROR);
  cleanupTbMetasTask(&job, &ctx);
}

// The request context index is out of range, so pMsgCtx is NULL. The error
// jump skips the pFetch initialization; the handler must not read it.
TEST(ctgTbMetasRsp, missingMsgCtxFailsTask) {
  initQueryModuleMsgHandle();

  SCtgJob        job = {};
  SCtgTbMetasCtx ctx = {};
  buildTbMetasTask(&job, &ctx);

  SCtgTask* pTask = jobTask(&job);
  ASSERT_NE(pTask, nullptr);
  pTask->msgCtxs = taosArrayInit_s(sizeof(SCtgMsgCtx), 1);

  SCtgTaskReq req = {};
  req.pTask = pTask;
  req.msgIdx = 5;  // only request context 0 exists

  SDataBuf rsp = {};
  EXPECT_EQ(ctgHandleGetTbMetasRsp(&req, -1, &rsp, TSDB_CODE_SUCCESS), TSDB_CODE_CTG_INTERNAL_ERROR);

  expectTaskFailed(&job, TSDB_CODE_CTG_INTERNAL_ERROR);
  cleanupTbMetasTask(&job, &ctx);
}

// The ordinary error path must keep marking the addressed result and finish
// the task once every fetch has been answered.
TEST(ctgTbMetasRsp, failedResponseMarksFetchAndCompletesTask) {
  initQueryModuleMsgHandle();

  SCtgJob        job = {};
  SCtgTbMetasCtx ctx = {};
  buildTbMetasTask(&job, &ctx);

  ctx.fetchNum = 1;
  ctx.pFetchs = taosArrayInit(1, sizeof(SCtgFetch));
  ASSERT_NE(ctx.pFetchs, nullptr);
  SCtgFetch fetch = {};
  fetch.resIdx = 1;
  ASSERT_NE(taosArrayPush(ctx.pFetchs, &fetch), nullptr);

  SCtgTaskReq req = {};
  req.pTask = jobTask(&job);
  ASSERT_NE(req.pTask, nullptr);
  req.msgIdx = 0;

  SDataBuf rsp = {};
  EXPECT_EQ(ctgHandleGetTbMetasRsp(&req, -1, &rsp, TSDB_CODE_VND_INVALID_VGROUP_ID),
            TSDB_CODE_VND_INVALID_VGROUP_ID);

  SCtgTask* pTask = jobTask(&job);
  EXPECT_EQ(job.taskDone, 1);
  EXPECT_EQ(pTask->status, CTG_TASK_DONE);
  EXPECT_EQ(pTask->code, TSDB_CODE_VND_INVALID_VGROUP_ID);
  ASSERT_NE(pTask->res, nullptr);
  EXPECT_EQ(ctx.pResList, nullptr);

  SArray* pResList = (SArray*)pTask->res;
  ASSERT_EQ(taosArrayGetSize(pResList), 2);
  SMetaRes* pFirst = (SMetaRes*)taosArrayGet(pResList, 0);
  ASSERT_NE(pFirst, nullptr);
  EXPECT_EQ(pFirst->code, TSDB_CODE_SUCCESS) << "results owned by other fetches must not be touched";
  SMetaRes* pSecond = (SMetaRes*)taosArrayGet(pResList, 1);
  ASSERT_NE(pSecond, nullptr);
  EXPECT_EQ(pSecond->code, TSDB_CODE_VND_INVALID_VGROUP_ID);

  cleanupTbMetasTask(&job, &ctx);
}

// Subtasks are created from catalog response handlers, which run on several
// query worker threads. The task id is also the array index the batch
// dispatcher uses, so concurrent initialization must keep taskId equal to the
// position in pJob->pTasks. Before the fix the id was taken outside the task
// lock and the push order could invert, routing a batch reply to the
// neighbouring task - the corrupted dispatch that ends in the pFetch crash.
TEST(ctgTbMetasRsp, concurrentTaskInitKeepsIdAsIndex) {
  initQueryModuleMsgHandle();

  SCtgJob job = {};
  job.pTasks = taosArrayInit(16, sizeof(SCtgTask));
  ASSERT_NE(job.pTasks, nullptr);

  constexpr int32_t kRounds = 500;
  for (int32_t round = 0; round < kRounds; ++round) {
    std::atomic<int32_t> ready{0};
    auto createTask = [&job, &ready]() {
      ready.fetch_add(1);
      while (ready.load() < 2) {
      }
      int32_t tid = -1;
      EXPECT_EQ(ctgInitTask(&job, CTG_TASK_GET_QNODE, nullptr, &tid), TSDB_CODE_SUCCESS);
    };

    std::thread first(createTask);
    std::thread second(createTask);
    first.join();
    second.join();
  }

  int32_t taskNum = (int32_t)taosArrayGetSize(job.pTasks);
  ASSERT_EQ(taskNum, 2 * kRounds);
  for (int32_t i = 0; i < taskNum; ++i) {
    SCtgTask* pTask = (SCtgTask*)taosArrayGet(job.pTasks, i);
    ASSERT_NE(pTask, nullptr);
    EXPECT_EQ(pTask->taskId, i) << "task id must stay aligned with its array index";
  }

  for (int32_t i = 0; i < taskNum; ++i) {
    ctgFreeTaskCtx((SCtgTask*)taosArrayGet(job.pTasks, i));
  }
  taosArrayDestroy(job.pTasks);
}
