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

#ifndef TDENGINE_MGMT_DNODE_INT_H
#define TDENGINE_MGMT_DNODE_INT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include "mnode.h"

extern void *mgmtStatusTimer;

int32_t mgmtSendCreateChildTableMsg(SChildTableObj *pTable, SVgObj *pVgroup, int32_t tagDataLen, int8_t *pTagData);
int32_t mgmtSendCreateNormalTableMsg(SNormalTableObj *pTable, SVgObj *pVgroup);
int32_t mgmtSendCreateStreamTableMsg(SStreamTableObj *pTable, SVgObj *pVgroup);

int mgmtSendRemoveMeterMsgToDnode(STabObj *pTable, SVgObj *pVgroup);
int mgmtSendVPeersMsg(SVgObj *pVgroup);
int mgmtSendFreeVnodeMsg(SVgObj *pVgroup);
int mgmtSendOneFreeVnodeMsg(SVnodeGid *pVnodeGid);

char *taosBuildRspMsgToDnode(SDnodeObj *pObj, char type);
char *taosBuildReqMsgToDnode(SDnodeObj *pObj, char type);

extern int32_t (*mgmtSendSimpleRspToDnode)(int32_t msgType, int32_t code);
extern int32_t (*mgmtSendMsgToDnode)(SDnodeObj *pObj, char *msg, int msgLen);
extern int32_t (*mgmtInitDnodeInt)();
extern void    (*mgmtCleanUpDnodeInt)();
extern void    (*mgmtProcessDnodeStatus)(void *handle, void *tmrId);


#ifdef __cplusplus
}
#endif

#endif
