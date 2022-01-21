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

#include "executor.h"
#include "planner.h"

void qStreamExecTaskSetInput(qTaskInfo_t qHandle, void* input) {}

qTaskInfo_t qCreateStreamExecTaskInfo(SSubQueryMsg* pMsg, void* pStreamBlockReadHandle) {
  if (pMsg == NULL || pStreamBlockReadHandle == NULL) {
    return NULL;
  }

  // print those info into log
  pMsg->sId = be64toh(pMsg->sId);
  pMsg->queryId = be64toh(pMsg->queryId);
  pMsg->taskId = be64toh(pMsg->taskId);
  pMsg->contentLen = ntohl(pMsg->contentLen);

  struct SSubplan* plan = NULL;
  int32_t          code = qStringToSubplan(pMsg->msg, &plan);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return NULL;
  }

  qTaskInfo_t pTaskInfo = NULL;
  code = qCreateExecTask(pStreamBlockReadHandle, 0, plan, &pTaskInfo, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    // TODO: destroy SSubplan & pTaskInfo
    terrno = code;
    return NULL;
  }

  return pTaskInfo;
}
