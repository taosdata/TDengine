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

#ifndef _TD_MND_INT_H_
#define _TD_MND_INT_H_

#include "mndDef.h"
#include "sdb.h"
#include "tqueue.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef int32_t (*MndMsgFp)(SMnode *pMnode, SMnodeMsg *pMsg);
typedef int32_t (*MndInitFp)(SMnode *pMnode);
typedef void (*MndCleanupFp)(SMnode *pMnode);

typedef struct {
  const char  *name;
  MndInitFp    initFp;
  MndCleanupFp cleanupFp;
} SMnodeStep;

typedef struct SMnode {
  int32_t           dnodeId;
  int32_t           clusterId;
  int8_t            replica;
  int8_t            selfIndex;
  SReplica          replicas[TSDB_MAX_REPLICA];
  tmr_h             timer;
  char             *path;
  SSdb             *pSdb;
  SDnode           *pDnode;
  SArray           *pSteps;
  MndMsgFp          msgFp[TSDB_MSG_TYPE_MAX];
  SendMsgToDnodeFp  sendMsgToDnodeFp;
  SendMsgToMnodeFp  sendMsgToMnodeFp;
  SendRedirectMsgFp sendRedirectMsgFp;
  PutMsgToMnodeQFp  putMsgToApplyMsgFp;
  int32_t           sver;
  int32_t           statusInterval;
  int32_t           mnodeEqualVnodeNum;
  char             *timezone;
  char             *locale;
  char             *charset;
} SMnode;

void mndSendMsgToDnode(SMnode *pMnode, SEpSet *pEpSet, SRpcMsg *rpcMsg);
void mndSendMsgToMnode(SMnode *pMnode, SRpcMsg *pMsg);
void mndSendRedirectMsg(SMnode *pMnode, SRpcMsg *pMsg);
void mndSetMsgHandle(SMnode *pMnode, int32_t msgType, MndMsgFp fp);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_INT_H_*/
