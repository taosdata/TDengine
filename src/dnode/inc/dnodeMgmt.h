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

#ifndef TDENGINE_DNODE_MGMT_H
#define TDENGINE_DNODE_MGMT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tsched.h"

typedef struct {
  char  id[20];
  char  sid;
  void *thandle;
  int   mgmtIndex;
  char  status;  // 0:offline, 1:online
} SMgmtObj;

int vnodeProcessCreateMeterRequest(char *pMsg, int msgLen, SMgmtObj *pMgmtObj);
int vnodeProcessRemoveMeterRequest(char *pMsg, int msgLen, SMgmtObj *pMgmtObj);

void dnodeDistributeMsgFromMgmt(char *content, int msgLen, int msgType, SMgmtObj *pObj);
void mgmtProcessMsgFromDnodeSpec(SSchedMsg *sched);

extern void *dmQhandle;
extern char *(*taosBuildRspMsgToMnodeWithSize)(SMgmtObj *pObj, char type, int size);
extern char *(*taosBuildReqMsgToMnodeWithSize)(SMgmtObj *pObj, char type, int size);
extern char *(*taosBuildRspMsgToMnode)(SMgmtObj *pObj, char type);
extern char *(*taosBuildReqMsgToMnode)(SMgmtObj *pObj, char type);
extern int (*taosSendMsgToMnode)(SMgmtObj *pObj, char *msg, int msgLen);
extern int (*taosSendSimpleRspToMnode)(SMgmtObj *pObj, char rsptype, char code);
extern void (*dnodeInitMgmtIp)();
extern int (*dnodeInitMgmtConn)();

char *taosBuildRspMsgToMnodeWithSizeEdgeImp(SMgmtObj *pObj, char type, int size);
char *taosBuildReqMsgToMnodeWithSizeEdgeImp(SMgmtObj *pObj, char type, int size);
char *taosBuildRspMsgToMnodeEdgeImp(SMgmtObj *pObj, char type);
char *taosBuildReqMsgToMnodeEdgeImp(SMgmtObj *pObj, char type);
int taosSendMsgToMnodeEdgeImp(SMgmtObj *pObj, char *msg, int msgLen);
int taosSendSimpleRspToMnodeEdgeImp(SMgmtObj *pObj, char rsptype, char code);
void dnodeInitMgmtIpEdgeImp();
void* dnodeProcessMsgFromMgmtEdgeImp(SSchedMsg *sched);
int dnodeInitMgmtConnEdgeImp();

#ifdef __cplusplus
}
#endif

#endif
