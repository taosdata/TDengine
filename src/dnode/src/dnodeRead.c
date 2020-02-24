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

void dnodeFreeQInfo(void *pConn) {}

void dnodeFreeQInfos() {}

void dnodeQueryData(SQueryTableMsg *pQuery, void *pConn, void (*callback)(int32_t code, void *pQInfo, void *pConn)) {
  void *pQInfo = NULL;
  int code = TSDB_CODE_SUCCESS;
  callback(code, pConn, pQInfo);
}

static void dnodeExecuteRetrieveData(SSchedMsg *pSched) {
  //SRetrieveTableMsg *pRetrieve = (SRetrieveTableMsg *)pSched->msg;
  SDnodeRetrieveCallbackFp callback = (SDnodeRetrieveCallbackFp)pSched->thandle;
  void *pConn = pSched->ahandle;

  //examples
  int32_t code = TSDB_CODE_INVALID_QHANDLE;
  void *pQInfo = NULL; //get from pConn
  (*callback)(code, pQInfo, pConn);

  //TODO build response here

  free(pSched->msg);
}

void dnodeRetrieveData(SRetrieveTableMsg *pRetrieve, void *pConn, SDnodeRetrieveCallbackFp callbackFp) {
  int8_t *msg = malloc(sizeof(SRetrieveTableMsg));
  memcpy(msg, pRetrieve, sizeof(SRetrieveTableMsg));

  SSchedMsg schedMsg;
  schedMsg.msg     = msg;
  schedMsg.ahandle = pConn;
  schedMsg.thandle = callbackFp;
  schedMsg.fp      = dnodeExecuteRetrieveData;
  taosScheduleTask(tsQueryQhandle, &schedMsg);
}

int32_t dnodeGetRetrieveData(void *pQInfo, SRetrieveTableRsp *retrievalRsp) {
  return 0;
}

int32_t dnodeGetRetrieveDataSize(void *pQInfo) {
  return 0;
}


