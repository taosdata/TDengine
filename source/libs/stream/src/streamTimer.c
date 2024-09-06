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
#include "ttimer.h"

void* streamTimer = NULL;

int32_t streamTimerInit() {
  streamTimer = taosTmrInit(1000, 100, 10000, "STREAM");
  if (streamTimer == NULL) {
    stError("init stream timer failed, code:%s", tstrerror(terrno));
    return -1;
  }

  stInfo("init stream timer, %p", streamTimer);
  return 0;
}

void streamTimerCleanUp() {
  stInfo("cleanup stream timer, %p", streamTimer);
  taosTmrCleanUp(streamTimer);
  streamTimer = NULL;
}

tmr_h streamTimerGetInstance() {
  return streamTimer;
}

int32_t streamCleanBeforeQuitTmr(SStreamTmrInfo* pInfo, SStreamTask* pTask) {
  pInfo->activeCounter = 0;
  pInfo->launchChkptId = 0;
  atomic_store_8(&pInfo->isActive, 0);

  int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
  ASSERT(ref >= 0);
  return ref;
}