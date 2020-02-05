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

#ifndef TDENGINE_DNODE_PLUGIN_H
#define TDENGINE_DNODE_PLUGIN_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <pthread.h>

#include "tsched.h"
#include "mgmt.h"

char *(*taosBuildRspMsgToMnodeWithSize)(SMgmtObj *pObj, char type, int size);
char *(*taosBuildReqMsgToMnodeWithSize)(SMgmtObj *pObj, char type, int size);
char *(*taosBuildRspMsgToMnode)(SMgmtObj *pObj, char type);
char *(*taosBuildReqMsgToMnode)(SMgmtObj *pObj, char type);
int (*taosSendMsgToMnode)(SMgmtObj *pObj, char *msg, int msgLen);
int (*taosSendSimpleRspToMnode)(SMgmtObj *pObj, char rsptype, char code);

void (*dnodeInitMgmtIp)();
void (*dnodeProcessMsgFromMgmt)(SSchedMsg *sched);
int (*dnodeInitMgmtConn)();

#ifdef __cplusplus
}
#endif

#endif
