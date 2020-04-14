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

#ifndef TDENGINE_MPEER_H
#define TDENGINE_MPEER_H

#ifdef __cplusplus
extern "C" {
#endif

struct _mnode_obj;

enum _TAOS_MN_STATUS {
  TAOS_MN_STATUS_OFFLINE,
  TAOS_MN_STATUS_DROPPING,
  TAOS_MN_STATUS_READY
};

// general implementation
int32_t mpeerInit();
void    mpeerCleanup();

// special implementation
int32_t mpeerInitMnodes();
void    mpeerCleanupMnodes();
int32_t mpeerAddMnode(int32_t dnodeId);
int32_t mpeerRemoveMnode(int32_t dnodeId);

void *  mpeerGetMnode(int32_t mnodeId);
int32_t mpeerGetMnodesNum();
void *  mpeerGetNextMnode(void *pNode, struct _mnode_obj **pMnode);
void    mpeerReleaseMnode(struct _mnode_obj *pMnode);

bool    mpeerIsMaster();

void    mpeerGetPrivateIpList(SRpcIpSet *ipSet);
void    mpeerGetPublicIpList(SRpcIpSet *ipSet);
void    mpeerGetMpeerInfos(void *mpeers);

int32_t mpeerForwardReqToPeer(void *pHead);

#ifdef __cplusplus
}
#endif

#endif
