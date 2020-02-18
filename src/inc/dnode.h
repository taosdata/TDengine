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

#ifndef TDENGINE_DNODE_H
#define TDENGINE_DNODE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <pthread.h>
#include "tsched.h"

typedef struct {
  int32_t queryReqNum;
  int32_t submitReqNum;
  int32_t httpReqNum;
} SDnodeStatisInfo;

typedef struct {
  char  id[20];
  char  sid;
  void *thandle;
  int   mgmtIndex;
  char  status;  // 0:offline, 1:online
} SMgmtObj;

// global variables
extern uint32_t tsRebootTime;

// dnodeCluster
extern void (*dnodeStartModules)();
extern void (*dnodeParseParameterK)();
extern int32_t (*dnodeCheckSystem)();
extern char *(*taosBuildRspMsgToMnodeWithSize)(SMgmtObj *pObj, char type, int size);
extern char *(*taosBuildReqMsgToMnodeWithSize)(SMgmtObj *pObj, char type, int size);
extern char *(*taosBuildRspMsgToMnode)(SMgmtObj *pObj, char type);
extern char *(*taosBuildReqMsgToMnode)(SMgmtObj *pObj, char type);
extern int (*taosSendSimpleRspToMnode)(SMgmtObj *pObj, char rsptype, char code);
extern void (*dnodeInitMgmtIp)();
extern int (*dnodeInitMgmt)();


// multilevelStorage
extern int32_t (*dnodeInitStorage)();
extern void (*dnodeCleanupStorage)();

void dnodeCheckDataDirOpenned(const char* dir);

void dnodeProcessMsgFromMgmtImp(SSchedMsg *sched);


void dnodeLockVnodes();
void dnodeUnLockVnodes();
SDnodeStatisInfo dnodeGetStatisInfo();

#ifdef __cplusplus
}
#endif

#endif
