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

#include <iostream>
#include <gtest/gtest.h>
#include "nodes.h"
#include "planner.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

#include <libs/stream/tstream.h>
#include <libs/transport/trpc.h>
#include "../../inc/mndStream.h"

namespace {

static int64_t defStreamId = 999;

SRpcMsg buildHbReq() {
  SStreamHbMsg msg = {0};
  msg.vgId = 1;
  msg.numOfTasks = 5;
  msg.pTaskStatus = taosArrayInit(4, sizeof(STaskStatusEntry));

  for (int32_t i = 0; i < 4; ++i) {
    STaskStatusEntry entry = {0};
    entry.nodeId = i + 1;
    entry.stage = 1;
    entry.id.taskId = i + 1;
    entry.id.streamId = defStreamId;

    if (i == 0) {
      entry.stage = 4;
    }

    void* px = taosArrayPush(msg.pTaskStatus, &entry);
    ASSERT(px != NULL);

  }

  // (p->checkpointId != 0) && p->checkpointFailed
  // add failed checkpoint info
  {
    STaskStatusEntry entry = {0};
    entry.nodeId = 5;
    entry.stage = 1;

    entry.id.taskId = 5;
    entry.id.streamId = defStreamId;

    entry.checkpointInfo.activeId = 1;
    entry.checkpointInfo.failed = true;

    void* px = taosArrayPush(msg.pTaskStatus, &entry);
    ASSERT(px != NULL);
  }

  int32_t  tlen = 0;
  int32_t  code = 0;
  SEncoder encoder;
  void*    buf = NULL;
  SRpcMsg  msg1 = {0};
  msg1.info.noResp = 1;

  tEncodeSize(tEncodeStreamHbMsg, &msg, tlen, code);
  if (code < 0) {
    goto _end;
  }

  buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    goto _end;
  }

  tEncoderInit(&encoder, (uint8_t*)buf, tlen);
  if ((code = tEncodeStreamHbMsg(&encoder, &msg)) < 0) {
    rpcFreeCont(buf);
    tEncoderClear(&encoder);
    goto _end;
  }
  tEncoderClear(&encoder);

  {
    msg1.msgType = TDMT_MND_STREAM_HEARTBEAT;
    msg1.pCont = buf;
    msg1.contLen = tlen;
  }

  taosArrayDestroy(msg.pTaskStatus);
  return msg1;

_end:
  return msg1;
}

void setTask(SStreamTask* pTask, int32_t nodeId, int64_t streamId, int32_t taskId) {
  SStreamExecInfo* pExecNode = &execInfo;

  pTask->id.streamId = streamId;
  pTask->id.taskId = taskId;
  pTask->info.nodeId = nodeId;

  STaskId id;
  id.streamId = pTask->id.streamId;
  id.taskId = pTask->id.taskId;

  STaskStatusEntry entry;
  streamTaskStatusInit(&entry, pTask);

  entry.stage = 1;
  entry.status = TASK_STATUS__READY;

  int32_t code = taosHashPut(pExecNode->pTaskMap, &id, sizeof(id), &entry, sizeof(entry));
  ASSERT(code == 0);

  void* px = taosArrayPush(pExecNode->pTaskList, &id);
  ASSERT(px != NULL);
}

void initStreamExecInfo() {
  SStreamExecInfo* pExecNode = &execInfo;

  SStreamTask task = {0};
  setTask(&task, 1, defStreamId, 1);
  setTask(&task, 1, defStreamId, 2);
  setTask(&task, 1, defStreamId, 3);
  setTask(&task, 1, defStreamId, 4);
  setTask(&task, 2, defStreamId, 5);
}

void initNodeInfo() {
  SNodeEntry entry = {0};
  entry.nodeId = 2;
  entry.stageUpdated = true;
  void* px = taosArrayPush(execInfo.pNodeList, &entry);
  ASSERT(px != NULL);
}
}  // namespace

class StreamTest : public testing::Test { // 继承了 testing::Test
 protected:

  static void SetUpTestSuite() {
    int32_t code = mndInitExecInfo();
    ASSERT(code == 0);

    initStreamExecInfo();
    initNodeInfo();

    (void) printf("setup env for streamTest suite");
  }

  static void TearDownTestSuite() {
    (void) printf("tearDown env for streamTest suite");
  }

  virtual void SetUp() override {
  }

  virtual void TearDown() override {
  }
};

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST_F(StreamTest, handle_error_in_hb) {
  SRpcMsg msg = buildHbReq();
  int32_t code = mndProcessStreamHb(&msg);

  rpcFreeCont(msg.pCont);
}

TEST_F(StreamTest, kill_checkpoint_trans) {
  STrans trans;
  trans.id = 100;
  int32_t code = mndStreamRegisterTrans(&trans, MND_STREAM_CHECKPOINT_NAME, defStreamId);
  ASSERT(code == 0);

  SMnode* pMnode = static_cast<SMnode*>(taosMemoryCalloc(1, sizeof(SMnode)));
  {// init sdb
    SSdbOpt opt = {0};
    opt.path = pMnode->path;
    opt.pMnode = pMnode;
    opt.pWal = pMnode->pWal;

    pMnode->pSdb = sdbInit(&opt);
    int32_t code = taosThreadMutexInit(&pMnode->syncMgmt.lock, NULL);
    ASSERT(code == 0);
  }

  SVgroupChangeInfo info;
  info.pUpdateNodeList = taosArrayInit(4, sizeof(SNodeUpdateInfo));
  info.pDBMap = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), true, HASH_NO_LOCK);

  const char* pDbName = "test_db_name";
  int32_t     len = strlen(pDbName);

  code = taosHashPut(info.pDBMap, pDbName, len, NULL, 0);
  ASSERT(code == 0);

  killAllCheckpointTrans(pMnode, &info);

  void* p = alloca(sizeof(SStreamObj) + sizeof(SSdbRow));
  SSdbRow* pRow = static_cast<SSdbRow*>(p);
  pRow->type = SDB_MAX;

  SStreamObj* pStream = (SStreamObj*)((char*)p + sizeof(SSdbRow));

  memset(pStream, 0, sizeof(SStreamObj));

  pStream->uid = defStreamId;
  pStream->lock = 0;
  pStream->tasks = taosArrayInit(1, POINTER_BYTES);
  pStream->pHTasksList = taosArrayInit(1, POINTER_BYTES);

  SArray* pLevel = taosArrayInit(1, POINTER_BYTES);
  SStreamTask* pTask = static_cast<SStreamTask*>(taosMemoryCalloc(1, sizeof(SStreamTask)));
  pTask->id.streamId = defStreamId;
  pTask->id.taskId = 1;
  pTask->exec.qmsg = (char*)taosMemoryCalloc(1,1);
  code = taosThreadMutexInit(&pTask->lock, NULL);
  ASSERT(code == 0);

  void* px = taosArrayPush(pLevel, &pTask);
  ASSERT(px != NULL);

  px = taosArrayPush(pStream->tasks, &pLevel);
  ASSERT(px != NULL);

  code = mndCreateStreamResetStatusTrans(pMnode, pStream);
  ASSERT(code != 0);

  tFreeStreamObj(pStream);
  sdbCleanup(pMnode->pSdb);

  taosMemoryFree(pMnode);

  taosArrayDestroy(info.pUpdateNodeList);
  taosHashCleanup(info.pDBMap);
}

TEST_F(StreamTest, plan_Test) {
  char* ast = "{\"NodeType\":\"101\",\"Name\":\"SelectStmt\",\"SelectStmt\":{\"Distinct\":false,\"Projections\":[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"#expr_1\",\"UserAlias\":\"_wstart\",\"Name\":\"_wstart\",\"Id\":\"89\",\"Type\":\"3505\",\"UdfBufSize\":\"0\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"#expr_2\",\"UserAlias\":\"sum(voltage)\",\"Name\":\"sum\",\"Id\":\"1\",\"Type\":\"14\",\"Parameters\":[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"voltage\",\"UserAlias\":\"voltage\",\"TableId\":\"6555383776122680534\",\"TableType\":\"1\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"meters\",\"TableAlias\":\"meters\",\"ColName\":\"voltage\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"#expr_3\",\"UserAlias\":\"groupid\",\"Name\":\"_group_key\",\"Id\":\"96\",\"Type\":\"3754\",\"Parameters\":[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"#expr_3\",\"UserAlias\":\"groupid\",\"TableId\":\"6555383776122680534\",\"TableType\":\"1\",\"ColId\":\"5\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"test\",\"TableName\":\"meters\",\"TableAlias\":\"meters\",\"ColName\":\"groupid\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}}],\"UdfBufSize\":\"0\"}}],\"From\":{\"NodeType\":\"6\",\"Name\":\"RealTable\",\"RealTable\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"DbName\":\"test\",\"tableName\":\"meters\",\"tableAlias\":\"meters\",\"MetaSize\":\"475\",\"Meta\":{\"VgId\":\"0\",\"TableType\":\"1\",\"Uid\":\"6555383776122680534\",\"Suid\":\"6555383776122680534\",\"Sversion\":\"1\",\"Tversion\":\"1\",\"ComInfo\":{\"NumOfTags\":\"2\",\"Precision\":\"0\",\"NumOfColumns\":\"4\",\"RowSize\":\"20\"},\"ColSchemas\":[{\"Type\":\"9\",\"ColId\":\"1\",\"bytes\":\"8\",\"Name\":\"ts\"},{\"Type\":\"6\",\"ColId\":\"2\",\"bytes\":\"4\",\"Name\":\"current\"},{\"Type\":\"4\",\"ColId\":\"3\",\"bytes\":\"4\",\"Name\":\"voltage\"},{\"Type\":\"6\",\"ColId\":\"4\",\"bytes\":\"4\",\"Name\":\"phase\"},{\"Type\":\"4\",\"ColId\":\"5\",\"bytes\":\"4\",\"Name\":\"groupid\"},{\"Type\":\"8\",\"ColId\":\"6\",\"bytes\":\"26\",\"Name\":\"location\"}]},\"VgroupsInfoSize\":\"1340\",\"VgroupsInfo\":{\"Num\":\"2\",\"Vgroups\":[{\"VgId\":\"2\",\"HashBegin\":\"0\",\"HashEnd\":\"2147483646\",\"EpSet\":{\"InUse\":\"0\",\"NumOfEps\":\"1\",\"Eps\":[{\"Fqdn\":\"localhost\",\"Port\":\"6030\"}]},\"NumOfTable\":\"0\"},{\"VgId\":\"3\",\"HashBegin\":\"2147483647\",\"HashEnd\":\"4294967295\",\"EpSet\":{\"InUse\":\"0\",\"NumOfEps\":\"1\",\"Eps\":[{\"Fqdn\":\"localhost\",\"Port\":\"6030\"}]},\"NumOfTable\":\"0\"}]}}},\"PartitionBy\":[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"groupid\",\"UserAlias\":\"groupid\",\"TableId\":\"6555383776122680534\",\"TableType\":\"1\",\"ColId\":\"5\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"test\",\"TableName\":\"meters\",\"TableAlias\":\"meters\",\"ColName\":\"groupid\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}}],\"Window\":{\"NodeType\":\"14\",\"Name\":\"IntervalWindow\",\"IntervalWindow\":{\"Interval\":{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"115\",\"Bytes\":\"8\"},\"AliasName\":\"c804c3a15ebe05b5baf40ad5ee12be1f\",\"UserAlias\":\"2s\",\"LiteralSize\":\"2\",\"Literal\":\"2s\",\"Duration\":true,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"115\",\"Datum\":\"2000\"}},\"TsPk\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"TableId\":\"6555383776122680534\",\"TableType\":\"1\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"meters\",\"TableAlias\":\"meters\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}}}},\"StmtName\":\"0x1580095ba\",\"HasAggFuncs\":true}}";
  //  char* ast = "{\"NodeType\":\"101\",\"Name\":\"SelectStmt\",\"SelectStmt\":{\"Distinct\":false,\"Projections\":[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"#expr_1\",\"UserAlias\":\"wstart\",\"Name\":\"_wstart\",\"Id\":\"89\",\"Type\":\"3505\",\"UdfBufSize\":\"0\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"2\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"#expr_2\",\"UserAlias\":\"min(c1)\",\"Name\":\"min\",\"Id\":\"2\",\"Type\":\"8\",\"Parameters\":[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"2\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"TableId\":\"5129202035162885657\",\"TableType\":\"1\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"at_once_interval_ext_stb\",\"TableAlias\":\"at_once_interval_ext_stb\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"3\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"2\"},\"AliasName\":\"#expr_3\",\"UserAlias\":\"max(c2)\",\"Name\":\"max\",\"Id\":\"3\",\"Type\":\"7\",\"Parameters\":[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"3\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"2\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"TableId\":\"5129202035162885657\",\"TableType\":\"1\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"at_once_interval_ext_stb\",\"TableAlias\":\"at_once_interval_ext_stb\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"Name\":\"cast\",\"Id\":\"77\",\"Type\":\"2000\",\"Parameters\":[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"#expr_4\",\"UserAlias\":\"sum(c3)\",\"Name\":\"sum\",\"Id\":\"1\",\"Type\":\"14\",\"Parameters\":[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c3\",\"UserAlias\":\"c3\",\"TableId\":\"5129202035162885657\",\"TableType\":\"1\",\"ColId\":\"4\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"at_once_interval_ext_stb\",\"TableAlias\":\"at_once_interval_ext_stb\",\"ColName\":\"c3\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"2\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"LiteralSize\":\"0\",\"Duration\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"#expr_5\",\"UserAlias\":\"first(c4)\",\"Name\":\"first\",\"Id\":\"33\",\"Type\":\"504\",\"Parameters\":[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"c4\",\"UserAlias\":\"c4\",\"TableId\":\"5129202035162885657\",\"TableType\":\"1\",\"ColId\":\"5\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"at_once_interval_ext_stb\",\"TableAlias\":\"at_once_interval_ext_stb\",\"ColName\":\"c4\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"TableId\":\"5129202035162885657\",\"TableType\":\"1\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"at_once_interval_ext_stb\",\"TableAlias\":\"at_once_interval_ext_stb\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"11\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"#expr_6\",\"UserAlias\":\"last(c5)\",\"Name\":\"last\",\"Id\":\"36\",\"Type\":\"506\",\"Parameters\":[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"11\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"c5\",\"UserAlias\":\"c5\",\"TableId\":\"5129202035162885657\",\"TableType\":\"1\",\"ColId\":\"6\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"at_once_interval_ext_stb\",\"TableAlias\":\"at_once_interval_ext_stb\",\"ColName\":\"c5\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"TableId\":\"5129202035162885657\",\"TableType\":\"1\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"at_once_interval_ext_stb\",\"TableAlias\":\"at_once_interval_ext_stb\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"12\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"2\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"Name\":\"cast\",\"Id\":\"77\",\"Type\":\"2000\",\"Parameters\":[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"#expr_7\",\"UserAlias\":\"apercentile(c6, 50)\",\"Name\":\"apercentile\",\"Id\":\"12\",\"Type\":\"1\",\"Parameters\":[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"12\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"2\"},\"AliasName\":\"c6\",\"UserAlias\":\"c6\",\"TableId\":\"5129202035162885657\",\"TableType\":\"1\",\"ColId\":\"7\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"at_once_interval_ext_stb\",\"TableAlias\":\"at_once_interval_ext_stb\",\"ColName\":\"c6\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"c0c7c76d30bd3dcaefc96f40275bdc0a\",\"UserAlias\":\"50\",\"LiteralSize\":\"2\",\"Literal\":\"50\",\"Duration\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"50\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"2\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"LiteralSize\":\"0\",\"Duration\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"13\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"Name\":\"cast\",\"Id\":\"77\",\"Type\":\"2000\",\"Parameters\":[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"#expr_8\",\"UserAlias\":\"avg(c7)\",\"Name\":\"avg\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"13\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c7\",\"UserAlias\":\"c7\",\"TableId\":\"5129202035162885657\",\"TableType\":\"1\",\"ColId\":\"8\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"at_once_interval_ext_stb\",\"TableAlias\":\"at_once_interval_ext_stb\",\"ColName\":\"c7\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"2\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"LiteralSize\":\"0\",\"Duration\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"14\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"Name\":\"cast\",\"Id\":\"77\",\"Type\":\"2000\",\"Parameters\":[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"#expr_9\",\"UserAlias\":\"count(c8)\",\"Name\":\"count\",\"Id\":\"0\",\"Type\":\"3\",\"Parameters\":[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"14\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"c8\",\"UserAlias\":\"c8\",\"TableId\":\"5129202035162885657\",\"TableType\":\"1\",\"ColId\":\"9\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"test\",\"TableName\":\"at_once_interval_ext_stb\",\"TableAlias\":\"at_once_interval_ext_stb\",\"ColName\":\"c8\",\"DataBlockId\":\"0\",\"SlotId\":\"0\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"2\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"LiteralSize\":\"0\",\"Duration\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"6\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"Name\":\"cast\",\"Id\":\"77\",\"Type\":\"2000\",\"Parameters\":[{\"Node
  SNode *     pAst = NULL;
  SQueryPlan *pPlan = NULL;

  if (taosCreateLog("taoslog", 10, "/etc/taos", NULL, NULL, NULL, NULL, LOG_MODE_TAOSC) != 0) {
    // ignore create log failed, only print
    (void) printf(" WARING: Create failed:%s. configDir\n", strerror(errno));
  }

  if (nodesStringToNode(ast, &pAst) < 0) {
    ASSERT(0);
  }

  SPlanContext cxt = {0};
  cxt.pAstRoot = pAst;
  cxt.topicQuery = false;
  cxt.streamQuery = true;
  cxt.triggerType = STREAM_TRIGGER_WINDOW_CLOSE;
  cxt.watermark = 1;
  cxt.igExpired = 1;
  cxt.deleteMark = 1;
  cxt.igCheckUpdate = 1;

  // using ast and param to build physical plan
  if (qCreateQueryPlan(&cxt, &pPlan, NULL) < 0) {
    ASSERT(0);
  }

  if (pAst != NULL) nodesDestroyNode(pAst);
  nodesDestroyNode((SNode*)pPlan);
}

#pragma GCC diagnostic pop