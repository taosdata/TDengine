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

#include "tq.h"

// stream_task:stream_id:task_id
void createStreamTaskOffsetKey(char* dst, uint64_t streamId, uint32_t taskId) {
  int32_t n = 12;
  char* p = dst;

  memcpy(p, "stream_task:", n);
  p += n;

  int32_t inc = tintToHex(streamId, p);
  p += inc;

  *(p++) = ':';
  tintToHex(taskId, p);
}

int32_t tqAddInputBlockNLaunchTask(SStreamTask* pTask, SStreamQueueItem* pQueueItem, int64_t ver) {
  int32_t code = tAppendDataForStream(pTask, pQueueItem);
  if (code < 0) {
    tqError("s-task:%s failed to put into queue, too many, next start ver:%" PRId64, pTask->id.idStr, ver);
    return -1;
  }

  if (streamSchedExec(pTask) < 0) {
    tqError("stream task:%d failed to be launched, code:%s", pTask->id.taskId, tstrerror(terrno));
    return -1;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t launchTaskForWalBlock(SStreamTask* pTask, SFetchRet* pRet, STqOffset* pOffset) {
  SStreamDataBlock* pBlocks = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM, 0);
  if (pBlocks == NULL) { // failed, do nothing
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pRet->data.info.type = STREAM_NORMAL;
  pBlocks->type = STREAM_INPUT__DATA_BLOCK;
  pBlocks->sourceVer = pOffset->val.version;
  pBlocks->blocks = taosArrayInit(0, sizeof(SSDataBlock));
  taosArrayPush(pBlocks->blocks, &pRet->data);

//          int64_t* ts = (int64_t*)(((SColumnInfoData*)ret.data.pDataBlock->pData)->pData);
//          tqDebug("-----------%ld\n", ts[0]);

  int32_t code = tqAddInputBlockNLaunchTask(pTask, (SStreamQueueItem*)pBlocks, pBlocks->sourceVer);
  if (code == TSDB_CODE_SUCCESS) {
    pOffset->val.version = walReaderGetCurrentVer(pTask->exec.pTqReader->pWalReader);
    tqDebug("s-task:%s set the ver:%" PRId64 " from WALReader after extract block from WAL", pTask->id.idStr,
            pOffset->val.version);
  }

  return 0;
}