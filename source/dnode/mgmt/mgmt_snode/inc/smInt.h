/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#ifndef _TD_DND_SNODE_INT_H_
#define _TD_DND_SNODE_INT_H_

#include "dmUtil.h"

#include "snode.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SSnodeMgmt {
  SDnodeData         *pData;
  SSnode             *pSnode;
  SMsgCb              msgCb;
  const char         *path;
  const char         *name;
  SSingleWorker       runnerWorker;
  SDispatchWorkerPool triggerWorkerPool;
} SSnodeMgmt;

typedef struct SSnodeInfo {
  SRWLatch               snodeLock;
  int32_t                snodeId;
  SNodeEpSet             snodeLeaders[2];
  SNodeEpSet             snodeReplica;
  int32_t                encrypted;
} SSnodeInfo;

// smHandle.c
SArray *smGetMsgHandles();
int32_t smProcessCreateReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg);
int32_t smProcessDropReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg);
int32_t smBuildCreateReqFromJson(SJson *pJson, SDCreateSnodeReq *pReq);
void smUpdateSnodeInfo(SDCreateSnodeReq* pReq);

// smWorker.c
int32_t smStartWorker(SSnodeMgmt *pMgmt);
void    smStopWorker(SSnodeMgmt *pMgmt);
/* Queue free hook: releases the cont and, for stream trigger responses, the
 * send handle of the trigger task that issued the request.
 * Exported for unit tests (dsnodeTest); not a general-purpose API. */
void    smFreeRpcQitem(void *pItem);
int32_t smPutMsgToQueue(SSnodeMgmt *pMgmt, EQueueType qtype, SRpcMsg *pMsg);
int32_t smPutMsgToRunnerQueue(SSnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t smPutMsgToTriggerQueue(SSnodeMgmt *pMgmt, SRpcMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_SNODE_INT_H_*/
