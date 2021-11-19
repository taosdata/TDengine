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

#ifndef _TD_MNODE_INT_H_
#define _TD_MNODE_INT_H_

#include "mnodeDef.h"
#include "sdb.h"
#include "tstep.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef int32_t (*MnodeRpcFp)(SMnodeMsg *pMsg);

typedef struct SMnode {
  int32_t    dnodeId;
  int64_t    clusterId;
  tmr_h      timer;
  SSteps    *pInitSteps;
  SSteps    *pStartSteps;
  SMnodePara para;
  MnodeRpcFp msgFp[TSDB_MSG_TYPE_MAX];
} SMnode;

tmr_h   mnodeGetTimer();
int32_t mnodeGetDnodeId();
int64_t mnodeGetClusterId();

void mnodeSendMsgToDnode(struct SEpSet *epSet, struct SRpcMsg *rpcMsg);
void mnodeSendMsgToMnode(struct SRpcMsg *rpcMsg);
void mnodeSendRedirectMsg(struct SRpcMsg *rpcMsg, bool forShell);

void mnodeSetMsgFp(int32_t msgType, MnodeRpcFp fp);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MNODE_INT_H_*/
