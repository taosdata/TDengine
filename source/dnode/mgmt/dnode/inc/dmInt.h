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

#ifndef _TD_DND_DNODE_INT_H_
#define _TD_DND_DNODE_INT_H_

#include "dndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SDnodeMgmt {
  int32_t      dnodeId;
  int32_t      dropped;
  int64_t      clusterId;
  char         localEp[TSDB_EP_LEN];
  char         firstEp[TSDB_EP_LEN];
  int64_t      dver;
  int64_t      updateTime;
  int8_t       statusSent;
  SEpSet       mnodeEpSet;
  SHashObj    *dnodeHash;
  SArray      *pDnodeEps;
  pthread_t   *threadId;
  SRWLatch     latch;
  SDnodeWorker mgmtWorker;
  SDnodeWorker statusWorker;
  const char  *path;
  SDnode      *pDnode;
} SDnodeMgmt;

// dmInt.h
void    dmGetMgmtFp(SMgmtWrapper *pWrapper);
int32_t dmGetDnodeId(SDnode *pDnode);
int64_t dmGetClusterId(SDnode *pDnode);
void    dmGetMnodeEpSet(SDnode *pDnode, SEpSet *pEpSet);
void    dmUpdateMnodeEpSet(SDnode *pDnode, SEpSet *pEpSet);
void    dmGetDnodeEp(SDnode *pDnode, int32_t dnodeId, char *pEp, char *pFqdn, uint16_t *pPort);
void    dmSendRedirectRsp(SDnode *pDnode, SRpcMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_DNODE_INT_H_*/