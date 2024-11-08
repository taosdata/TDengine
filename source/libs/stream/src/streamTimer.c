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

int32_t streamTimerGetInstance(tmr_h* pTmr) {
  *pTmr = streamTimer;
  return TSDB_CODE_SUCCESS;
}

void streamTmrStart(TAOS_TMR_CALLBACK fp, int32_t mseconds, void* pParam, void* pHandle, tmr_h* pTmrId, int32_t vgId,
                    const char* pMsg) {
  if (*pTmrId == NULL) {
    *pTmrId = taosTmrStart(fp, mseconds, pParam, pHandle);
    if (*pTmrId == NULL) {
      stError("vgId:%d start %s tmr failed, code:%s", vgId, pMsg, tstrerror(terrno));
      return;
    }
  } else {
    bool ret = taosTmrReset(fp, mseconds, pParam, pHandle, pTmrId);
    if (ret) {
      stError("vgId:%d start %s tmr failed, code:%s", vgId, pMsg, tstrerror(terrno));
      return;
    }
  }

  stTrace("vgId:%d start %s tmr succ", vgId, pMsg);
}

void streamTmrStop(tmr_h tmrId) {
  bool stop = taosTmrStop(tmrId);
  if (stop) {
    // todo
  }
}

void streamCleanBeforeQuitTmr(SStreamTmrInfo* pInfo, void* param) {
  pInfo->activeCounter = 0;
  pInfo->launchChkptId = 0;
  atomic_store_8(&pInfo->isActive, 0);
  streamTaskFreeRefId(param);
}