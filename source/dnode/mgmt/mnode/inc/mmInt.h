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

#ifndef _TD_DND_MNODE_INT_H_
#define _TD_DND_MNODE_INT_H_

#include "mm.h"
#include "mnode.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SMnodeMgmt {
  SMnode       *pMnode;
  SDnode       *pDnode;
  SMgmtWrapper *pWrapper;
  const char   *path;
  SQWorkerAll   readWorker;
  SQWorkerAll   writeWorker;
  SQWorkerAll   syncWorker;
  SReplica      replicas[TSDB_MAX_REPLICA];
  int8_t        replica;
  int8_t        selfIndex;
} SMnodeMgmt;

// mmFile.c
int32_t mmReadFile(SMnodeMgmt *pMgmt, bool *pDeployed);
int32_t mmWriteFile(SMnodeMgmt *pMgmt, bool deployed);

// mmInt.c
int32_t mmOpenFromMsg(SMgmtWrapper *pWrapper, SDCreateMnodeReq *pReq);
int32_t mmDrop(SMgmtWrapper *pWrapper);
int32_t mmAlter(SMnodeMgmt *pMgmt, SDAlterMnodeReq *pReq);

// mmMsg.c
void    mmInitMsgHandles(SMgmtWrapper *pWrapper);
int32_t mmProcessCreateReq(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
int32_t mmProcessDropReq(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
int32_t mmProcessAlterReq(SMnodeMgmt *pMgmt, SNodeMsg *pMsg);

// mmWorker.c
int32_t mmStartWorker(SMnodeMgmt *pMgmt);
void    mmStopWorker(SMnodeMgmt *pMgmt);
int32_t mmProcessWriteMsg(SMnodeMgmt *pMgmt, SNodeMsg *pMsg);
int32_t mmProcessSyncMsg(SMnodeMgmt *pMgmt, SNodeMsg *pMsg);
int32_t mmProcessReadMsg(SMnodeMgmt *pMgmt, SNodeMsg *pMsg);
int32_t mmPutMsgToWriteQueue(SMgmtWrapper *pWrapper, SRpcMsg *pRpcMsg);
int32_t mmPutMsgToReadQueue(SMgmtWrapper *pWrapper, SRpcMsg *pRpcMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_MNODE_INT_H_*/