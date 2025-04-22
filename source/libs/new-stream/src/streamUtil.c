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

#include "streamInt.h"

int32_t streamGetThreadIdx(int32_t threadNum, int64_t streamGId) {
  return streamGId % threadNum;
}

int32_t stmAddFetchStreamGid(void) {
  if (++gStreamMgmt.stmGrpIdx >= STREAM_MAX_GROUP_NUM) {
    gStreamMgmt.stmGrpIdx = 0;
  }

  return gStreamMgmt.stmGrpIdx;
}

int32_t stmAddStreamStatus(SArray** ppStatus, SStreamTasksInfo* pStream) {
  if (pStream->taskNum <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  *ppStatus = taosArrayInit(pStream->taskNum, sizeof(SStmTaskStatusMsg));
  TSDB_CHECK_NULL(*ppStatus, code, lino, _exit, terrno);

  int32_t taskNum = taosArrayGetSize(pStream->readerList);
  for (int32_t i = 0; i < taskNum; ++i) {
    SStreamReaderTask* pReader = taosArrayGet(pStream->readerList, i);
    TSDB_CHECK_NULL(taosArrayPush(*ppStatus, &pReader->task), code, lino, _exit, terrno);
  }

  return code;

_exit:

  stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));

  return code;
}

int32_t stmBuildStreamsStatus(SArray** ppStatus, int32_t gid) {
  SHashObj* pHash = gStreamMgmt.stmGrp[gid];
  if (NULL == pHash) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  void *pIter = NULL;
  while (true) {
    pIter = taosHashIterate(pHash, pIter);
    if (NULL == pIter) {
      break;
    }

    SStreamTasksInfo* pStream = (SStreamTasksInfo*)pIter;

    stmAddStreamStatus(ppStatus, pStream);
  }

  return code;
}

