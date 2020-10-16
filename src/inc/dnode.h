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

#include "trpc.h"

typedef struct {
  int32_t queryReqNum;
  int32_t submitReqNum;
  int32_t httpReqNum;
} SDnodeStatisInfo;

typedef enum {
  TSDB_DNODE_RUN_STATUS_INITIALIZE,
  TSDB_DNODE_RUN_STATUS_RUNING,
  TSDB_DNODE_RUN_STATUS_STOPPED
} SDnodeRunStatus;

SDnodeRunStatus dnodeGetRunStatus();
SDnodeStatisInfo dnodeGetStatisInfo();

bool    dnodeIsFirstDeploy();
char *  dnodeGetMnodeMasterEp();
void    dnodeGetMnodeEpSetForPeer(void *epSet);
void    dnodeGetMnodeEpSetForShell(void *epSet);
void *  dnodeGetMnodeInfos();
int32_t dnodeGetDnodeId();
bool    dnodeStartMnode(void *pModes);

void  dnodeAddClientRspHandle(uint8_t msgType, void (*fp)(SRpcMsg *rpcMsg));
void  dnodeSendMsgToDnode(SRpcEpSet *epSet, SRpcMsg *rpcMsg);
void  dnodeSendMsgToMnodeRecv(SRpcMsg *rpcMsg, SRpcMsg *rpcRsp);
void  dnodeSendMsgToDnodeRecv(SRpcMsg *rpcMsg, SRpcMsg *rpcRsp, SRpcEpSet *epSet);
void *dnodeSendCfgTableToRecv(int32_t vgId, int32_t sid);

void *dnodeAllocateVnodeWqueue(void *pVnode);
void  dnodeFreeVnodeWqueue(void *queue);
void *dnodeAllocateVnodeRqueue(void *pVnode);
void  dnodeFreeVnodeRqueue(void *rqueue);
void  dnodeSendRpcVnodeWriteRsp(void *pVnode, void *param, int32_t code);

int32_t dnodeAllocateMnodePqueue();
void    dnodeFreeMnodePqueue();
int32_t dnodeAllocateMnodeRqueue();
void    dnodeFreeMnodeRqueue();
int32_t dnodeAllocateMnodeWqueue();
void    dnodeFreeMnodeWqueue();
void    dnodeSendRpcMnodeWriteRsp(void *pMsg, int32_t code);
void    dnodeReprocessMnodeWriteMsg(void *pMsg);
void    dnodeDelayReprocessMnodeWriteMsg(void *pMsg);

void    dnodeSendStatusMsgToMnode();

#ifdef __cplusplus
}
#endif

#endif
