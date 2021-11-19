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

#ifndef _TD_MNODE_H_
#define _TD_MNODE_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef enum { MN_MSG_TYPE_WRITE = 1, MN_MSG_TYPE_APPLY, MN_MSG_TYPE_SYNC, MN_MSG_TYPE_READ } EMnMsgType;

typedef struct SMnodeMsg SMnodeMsg;

typedef struct {
  int8_t   replica;
  int8_t   selfIndex;
  SReplica replicas[TSDB_MAX_REPLICA];
} SMnodeCfg;

typedef struct {
  int64_t numOfDnode;
  int64_t numOfMnode;
  int64_t numOfVgroup;
  int64_t numOfDatabase;
  int64_t numOfSuperTable;
  int64_t numOfChildTable;
  int64_t numOfColumn;
  int64_t totalPoints;
  int64_t totalStorage;
  int64_t compStorage;
} SMnodeLoad;

typedef struct SMnode SMnode;
typedef struct SServer SServer;

typedef void (*SendMsgToDnodeFp)(SServer *pServer, struct SEpSet *epSet, struct SRpcMsg *rpcMsg);
typedef void (*SendMsgToMnodeFp)(SServer *pServer, struct SRpcMsg *rpcMsg);
typedef void (*SendRedirectMsgFp)(SServer *pServer, struct SRpcMsg *rpcMsg, bool forShell);
typedef int32_t (*PutMsgToMnodeQFp)(SServer *pServer, SMnodeMsg *pMsg);

typedef struct {
  int32_t           dnodeId;
  int64_t           clusterId;
  PutMsgToMnodeQFp  putMsgToApplyMsgFp;
  SendMsgToDnodeFp  sendMsgToDnodeFp;
  SendMsgToMnodeFp  sendMsgToMnodeFp;
  SendRedirectMsgFp sendRedirectMsgFp;
} SMnodePara;

SMnode* mnodeCreate(SMnodePara para);
void    mnodeCleanup();

int32_t mnodeDeploy(SMnodeCfg *pCfg);
void    mnodeUnDeploy();
int32_t mnodeStart(SMnodeCfg *pCfg);
int32_t mnodeAlter(SMnodeCfg *pCfg);
void    mnodeStop();

int32_t mnodeGetLoad(SMnodeLoad *pLoad);
int32_t mnodeRetriveAuth(char *user, char *spi, char *encrypt, char *secret, char *ckey);

SMnodeMsg *mnodeInitMsg(SRpcMsg *pRpcMsg);
void       mnodeCleanupMsg(SMnodeMsg *pMsg);
void       mnodeProcessMsg(SMnodeMsg *pMsg, EMnMsgType msgType);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MNODE_H_*/
