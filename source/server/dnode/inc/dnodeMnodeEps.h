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

#ifndef _TD_DNODE_MNODE_EP_H_
#define _TD_DNODE_MNODE_EP_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "dnodeInt.h"

typedef struct DnMnEps {
  SRpcEpSet       mnodeEpSet;
  SMInfos         mnodeInfos;
  char            file[PATH_MAX + 20];
  pthread_mutex_t mutex;
} DnMnEps;

int32_t dnodeInitMnodeEps(DnMnEps **meps);
void    dnodeCleanupMnodeEps(DnMnEps **meps);
void    dnodeUpdateMnodeFromStatus(DnMnEps *meps, SMInfos *pMinfos);
void    dnodeUpdateMnodeFromPeer(DnMnEps *meps, SRpcEpSet *pEpSet);
void    dnodeGetEpSetForPeer(DnMnEps *meps, SRpcEpSet *epSet);
void    dnodeGetEpSetForShell(DnMnEps *meps, SRpcEpSet *epSet);
void    dnodeSendRedirectMsg(SRpcMsg *rpcMsg, bool forShell);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DNODE_MNODE_EP_H_*/
