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
} SMnodeStat;

typedef struct {
  void (*SendMsgToDnode)(struct SEpSet *epSet, struct SRpcMsg *rpcMsg);
  void (*SendMsgToMnode)(struct SRpcMsg *rpcMsg);
  void (*SendRedirectMsg)(struct SRpcMsg *rpcMsg, bool forShell);
  void (*GetDnodeEp)(int32_t dnodeId, char *ep, char *fqdn, uint16_t *port);
} SMnodeFp;

typedef struct {
  SMnodeFp fp;
  int64_t  clusterId;
  int32_t  dnodeId;
} SMnodePara;

int32_t mnodeInit(SMnodePara para);
void    mnodeCleanup();
int32_t mnodeDeploy();
void    mnodeUnDeploy();
int32_t mnodeStart();
void    mnodeStop();

int32_t mnodeGetStatistics(SMnodeStat *stat);
int32_t mnodeRetriveAuth(char *user, char *spi, char *encrypt, char *secret, char *ckey);

void mnodeProcessMsg(SRpcMsg *rpcMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MNODE_H_*/
