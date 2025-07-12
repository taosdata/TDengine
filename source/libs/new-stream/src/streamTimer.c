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

int32_t streamTimerInit(void** ppTimer) {
  *ppTimer = taosTmrInit(1000, 100, 10000, "STREAM");
  if (*ppTimer == NULL) {
    stError("init stream timer failed, code:%s", tstrerror(terrno));
    return -1;
  }

  stInfo("init stream timer, %p", &ppTimer);
  return 0;
}

int32_t streamTimerGetInstance(tmr_h* pTmr) {
  *pTmr = gStreamMgmt.timer;
  return TSDB_CODE_SUCCESS;
}

void streamTmrStart(TAOS_TMR_CALLBACK fp, int32_t mseconds, void* pParam, void* pHandle, tmr_h* pTmrId, const char* pMsg) {
  if (*pTmrId == NULL) {
    *pTmrId = taosTmrStart(fp, mseconds, pParam, pHandle);
    if (*pTmrId == NULL) {
      stError("start %s tmr failed, code:%s", pMsg, tstrerror(terrno));
      return;
    }
  } else {
    bool ret = taosTmrReset(fp, mseconds, pParam, pHandle, pTmrId);
    if (ret) {
      stError("start %s tmr failed, code:%s", pMsg, tstrerror(terrno));
      return;
    }
  }

  stTrace("start %s tmr succ", pMsg);
}

void streamTmrStop(tmr_h tmrId) {
  bool stop = taosTmrStop(tmrId);
  if (stop) {
    // todo
  }
}


void streamTimerCleanUp() {
  stInfo("cleanup stream timer, %p", gStreamMgmt.timer);
  streamTmrStop(gStreamMgmt.hb.hbTmr);
  taosTmrCleanUp(gStreamMgmt.timer);
  gStreamMgmt.timer = NULL;
}

