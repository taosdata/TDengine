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
#include "tsched.h"
#include "dnode.h"
#include "dnodeRead.h"
#include "dnodeSystem.h"

void dnodeQueryData(SQueryTableMsg *pQuery, void *pConn, void (*callback)(int32_t code, void *pQInfo, void *pConn)) {
  dTrace("conn:%p, query msg is disposed", pConn);
  void *pQInfo = 100;
  callback(TSDB_CODE_SUCCESS, pQInfo, pConn);
}

static void dnodeExecuteRetrieveData(SSchedMsg *pSched) {
  SDnodeRetrieveCallbackFp callback = (SDnodeRetrieveCallbackFp)pSched->thandle;
  SRetrieveTableMsg *pRetrieve = pSched->msg;
  void *pConn = pSched->ahandle;

  dTrace("conn:%p, retrieve msg is disposed, qhandle:%" PRId64, pConn, pRetrieve->qhandle);

  //examples
  int32_t code = TSDB_CODE_SUCCESS;
  void *pQInfo = (void*)pRetrieve->qhandle;

  (*callback)(code, pQInfo, pConn);

  free(pSched->msg);
}

void dnodeRetrieveData(SRetrieveTableMsg *pRetrieve, void *pConn, SDnodeRetrieveCallbackFp callbackFp) {
  dTrace("conn:%p, retrieve msg is received", pConn);

  void *msg = malloc(sizeof(SRetrieveTableMsg));
  memcpy(msg, pRetrieve, sizeof(SRetrieveTableMsg));

  SSchedMsg schedMsg;
  schedMsg.msg     = msg;
  schedMsg.ahandle = pConn;
  schedMsg.thandle = callbackFp;
  schedMsg.fp      = dnodeExecuteRetrieveData;
  taosScheduleTask(tsQueryQhandle, &schedMsg);
}

int32_t dnodeGetRetrieveData(void *pQInfo, SRetrieveTableRsp *pRetrieve) {
  dTrace("qInfo:%p, data is retrieved");
  pRetrieve->numOfRows = 0;
  return 0;
}

int32_t dnodeGetRetrieveDataSize(void *pQInfo) {
  dTrace("qInfo:%p, contLen is 100");
  return 100;
}


