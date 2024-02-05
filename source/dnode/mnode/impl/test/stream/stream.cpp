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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

#include <libs/stream/tstream.h>
#include <libs/transport/trpc.h>
#include "../../inc/mndStream.h"

namespace {
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
    entry.id.streamId = 999;

    if (i == 0) {
      entry.stage = 4;
    }

    taosArrayPush(msg.pTaskStatus, &entry);
  }

  // (p->checkpointId != 0) && p->checkpointFailed
  // add failed checkpoint info
  {
    STaskStatusEntry entry = {0};
    entry.nodeId = 5;
    entry.stage = 1;

    entry.id.taskId = 5;
    entry.id.streamId = 999;

    entry.checkpointId = 1;
    entry.checkpointFailed = true;

    taosArrayPush(msg.pTaskStatus, &entry);
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
    goto _end;
  }
  tEncoderClear(&encoder);

  initRpcMsg(&msg1, TDMT_MND_STREAM_HEARTBEAT, buf, tlen);

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

  STaskId          id = {.streamId = pTask->id.streamId, .taskId = pTask->id.taskId};
  STaskStatusEntry entry = {0};
  streamTaskStatusInit(&entry, pTask);

  entry.stage = 1;
  entry.status = TASK_STATUS__READY;

  taosHashPut(pExecNode->pTaskMap, &id, sizeof(id), &entry, sizeof(entry));
  taosArrayPush(pExecNode->pTaskList, &id);
}
void initStreamExecInfo() {
  SStreamExecInfo* pExecNode = &execInfo;

  SStreamTask task = {0};
  setTask(&task, 1, 999, 1);
  setTask(&task, 1, 999, 2);
  setTask(&task, 1, 999, 3);
  setTask(&task, 1, 999, 4);
  setTask(&task, 2, 999, 5);
}

void initNodeInfo() {
  execInfo.pNodeList = taosArrayInit(4, sizeof(SNodeEntry));
  SNodeEntry entry = {0};
  entry.nodeId = 2;
  entry.stageUpdated = true;
  taosArrayPush(execInfo.pNodeList, &entry);
}
}  // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(mndHbTest, handle_error_in_hb) {
  mndInitExecInfo();
  initStreamExecInfo();
  initNodeInfo();

  SRpcMsg msg = buildHbReq();
  int32_t code = mndProcessStreamHb(&msg);

  rpcFreeCont(msg.pCont);
}

#pragma GCC diagnostic pop