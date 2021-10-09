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

typedef void (*RpcMsgFp)( SRpcMsg *pMsg);

typedef struct SDnTrans {
  void *   serverRpc;
  void *   clientRpc;
  void *   shellRpc;
  int32_t  queryReqNum;
  int32_t  submitReqNum;
  RpcMsgFp peerMsgFp[TSDB_MSG_TYPE_MAX];
  RpcMsgFp shellMsgFp[TSDB_MSG_TYPE_MAX];
} SDnTrans;

int32_t dnodeInitTrans(SDnTrans **rans);
void    dnodeCleanupTrans(SDnTrans **trans);
void    dnodeSendMsgToMnode(SRpcMsg *rpcMsg);
void    dnodeSendMsgToDnode(SRpcEpSet *epSet, SRpcMsg *rpcMsg);
void    dnodeSendMsgToDnodeRecv(SRpcMsg *rpcMsg, SRpcMsg *rpcRsp, SRpcEpSet *epSet);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DNODE_TRANS_H_*/
