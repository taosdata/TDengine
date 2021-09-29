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

#ifndef _TD_DNODE_TRANS_H_
#define _TD_DNODE_TRANS_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "dnodeInt.h"

typedef void (*RpcMsgCfp)(void *owner, SRpcMsg *pMsg, SRpcEpSet *pEpSet);
typedef void (*RpcMsgFp)(void *owner, SRpcMsg *pMsg);

typedef struct DnMsgFp {
  void *   module;
  RpcMsgFp fp;
} DnMsgFp;

typedef struct DnTrans {
  Dnode * dnode;
  void *  serverRpc;
  void *  clientRpc;
  void *  shellRpc;
  int32_t queryReqNum;
  int32_t submitReqNum;
  DnMsgFp fpPeerMsg[TSDB_MSG_TYPE_MAX];
  DnMsgFp fpShellMsg[TSDB_MSG_TYPE_MAX];
} DnTrans;

int32_t dnodeInitTrans(Dnode *dnode, DnTrans **trans);
void    dnodeCleanupTrans(DnTrans **trans);
void    dnodeSendMsgToMnode(Dnode *dnode, SRpcMsg *rpcMsg);
void    dnodeSendMsgToDnode(Dnode *dnode, SRpcEpSet *epSet, SRpcMsg *rpcMsg);
void    dnodeSendMsgToDnodeRecv(Dnode *dnode, SRpcMsg *rpcMsg, SRpcMsg *rpcRsp, SRpcEpSet *epSet);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DNODE_TRANS_H_*/
