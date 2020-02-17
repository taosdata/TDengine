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

#define _DEFAULT_SOURCE
#include "os.h"
#include "taoserror.h"
#include "tlog.h"
#include "dnodeWrite.h"
#include "dnode.h"
#include "dnodeRead.h"
#include "dnodeSystem.h"

void dnodeFreeQInfoInQueue(SShellObj *pShellObj) {
}


void dnodeExecuteRetrieveData(SSchedMsg *pSched) {
  SRetrieveMeterMsg *pRetrieve = (SRetrieveMeterMsg *)pSched->msg;
  SDnodeRetrieveCallbackFp callback = (SDnodeRetrieveCallbackFp)pSched->thandle;
  SShellObj *pObj = (SShellObj *)pSched->ahandle;
  SRetrieveMeterRsp result = {0};

  /*
   * in case of server restart, apps may hold qhandle created by server before restart,
   * which is actually invalid, therefore, signature check is required.
   */
  if (pRetrieve->qhandle != (uint64_t)pObj->qhandle) {
    // if free flag is set, client wants to clean the resources
    dError("QInfo:%p, qhandle:%p is not matched with saved:%p", pObj->qhandle, pRetrieve->qhandle, pObj->qhandle);
    int32_t code = TSDB_CODE_INVALID_QHANDLE;
    (*callback)(code, &result, pObj);
  }

  //TODO build response here

  free(pSched->msg);
}

void dnodeRetrieveData(SRetrieveMeterMsg *pMsg, int32_t msgLen, void *pShellObj, SDnodeRetrieveCallbackFp callback) {
  int8_t *msg = malloc(msgLen);
  memcpy(msg, pMsg, msgLen);

  SSchedMsg schedMsg;
  schedMsg.msg     = msg;
  schedMsg.ahandle = pShellObj;
  schedMsg.thandle = callback;
  schedMsg.fp      = dnodeExecuteRetrieveData;
  taosScheduleTask(tsQueryQhandle, &schedMsg);
}
