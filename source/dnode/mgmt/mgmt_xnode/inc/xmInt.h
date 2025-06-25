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

#ifndef _TD_DND_XNODE_INT_H_
#define _TD_DND_XNODE_INT_H_

#include "dmUtil.h"

#include "xnode.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SXnodeMgmt {
  SDnodeData *pData;
  SBnode     *pBnode;
  SMsgCb      msgCb;
  const char *path;
  const char *name;
} SXnodeMgmt;

// xmHandle.c
SArray *xmGetMsgHandles();
int32_t xmProcessCreateReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg);
int32_t xmProcessDropReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg);

// xmWorker.c
int32_t xmPutRpcMsgToQueue(SXnodeMgmt *pMgmt, EQueueType qtype, SRpcMsg *pMsg);
int32_t xmGetQueueSize(SXnodeMgmt *pMgmt, int32_t vgId, EQueueType qtype);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_XNODE_INT_H_*/
