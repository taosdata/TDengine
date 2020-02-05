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

#include "dnodePlugin.h"
#include "dnodeMgmt.h"


#include "vnode.h"
#include "vnodeMgmt.h"


char *taosBuildRspMsgToMnodeWithSizeEdgeImp(SMgmtObj *pObj, char type, int size) {
  char *pStart = (char *)malloc(size);
  if (pStart == NULL) {
    return NULL;
  }

  *pStart = type;
  return pStart + 1;
}

char *taosBuildReqMsgToMnodeWithSizeEdgeImp(SMgmtObj *pObj, char type, int size) {
  char *pStart = (char *)malloc(size);
  if (pStart == NULL) {
    return NULL;
  }

  *pStart = type;
  return pStart + 1;
}

char *taosBuildRspMsgToMnodeEdgeImp(SMgmtObj *pObj, char type) {
  return taosBuildRspMsgToMnodeWithSize(pObj, type, 256);
}

char *taosBuildReqMsgToMnodeEdgeImp(SMgmtObj *pObj, char type) {
  return taosBuildReqMsgToMnodeWithSize(pObj, type, 256);
}

int taosSendMsgToMnodeEdgeImp(SMgmtObj *pObj, char *msg, int msgLen) {
  dTrace("msg:%s is sent to mnode", taosMsg[(uint8_t)(*(msg-1))]);

  /*
   * Lite version has no message header, so minus one
   */
  SSchedMsg schedMsg;
  schedMsg.fp = mgmtProcessMsgFromDnodeSpec;
  schedMsg.msg = msg - 1;
  schedMsg.ahandle = NULL;
  schedMsg.thandle = NULL;
  taosScheduleTask(dmQhandle, &schedMsg);

  return 0;
}

int taosSendSimpleRspToMnodeEdgeImp(SMgmtObj *pObj, char rsptype, char code) {
  char *pStart = taosBuildRspMsgToMnode(0, rsptype);
  if (pStart == NULL) {
    return 0;
  }

  *pStart = code;
  taosSendMsgToMnode(0, pStart, code);

  return 0;
}

void dnodeProcessMsgFromMgmtEdgeImp(SSchedMsg *sched) {
  char  msgType = *sched->msg;
  char *content = sched->msg + 1;

  dTrace("msg:%s is received from mgmt", taosMsg[(uint8_t)msgType]);

  dnodeProcessMsgFromMgmtImp(content, 0, msgType, 0);

  free(sched->msg);
}

int dnodeInitMgmtConnEdgeImp() {
  return 0;
}

void dnodeInitMgmtIpEdgeImp() {}

void dnodeInitPlugin() {
  dnodeInitMgmtConn = dnodeInitMgmtConnEdgeImp;
  dnodeInitMgmtIp = dnodeInitMgmtIpEdgeImp;
  dnodeProcessMsgFromMgmt = dnodeProcessMsgFromMgmtEdgeImp;

  taosBuildRspMsgToMnodeWithSize = taosBuildRspMsgToMnodeWithSizeEdgeImp;
  taosBuildReqMsgToMnodeWithSize = taosBuildReqMsgToMnodeWithSizeEdgeImp;
  taosBuildRspMsgToMnode = taosBuildRspMsgToMnodeEdgeImp;
  taosBuildReqMsgToMnode = taosBuildReqMsgToMnodeEdgeImp;
  taosSendMsgToMnode = taosSendMsgToMnodeEdgeImp;
  taosSendSimpleRspToMnode = taosSendSimpleRspToMnodeEdgeImp;
}



