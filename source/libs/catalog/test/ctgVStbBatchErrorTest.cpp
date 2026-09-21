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
