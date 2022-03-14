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
  SMsgHandle   msgHandles[TDMT_MAX];
  const char  *path;
  SDnode      *pDnode;
} SDnodeMgmt;

// dmFile.h
void dmGetMnodeEpSet(SDnode *pDnode, SEpSet *pEpSet);

// dmHandle.h
void dmProcessStartupReq(SDnode *pDnode, SRpcMsg *pMsg);

// dmInt.h
SMgmtFp dmGetMgmtFp();
int32_t dmGetDnodeId(SDnode *pDnode);
int64_t dmGetClusterId(SDnode *pDnode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_DNODE_INT_H_*/