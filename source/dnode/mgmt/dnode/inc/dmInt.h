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

#include "dm.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SDnodeMgmt {
  int64_t       dver;
  int64_t       updateTime;
  int8_t        statusSent;
  SEpSet        mnodeEpSet;
  SHashObj     *dnodeHash;
  SArray       *dnodeEps;
  TdThread    *threadId;
  SRWLatch      latch;
  SDnodeWorker  mgmtWorker;
  SDnodeWorker  statusWorker;
  const char   *path;
  SDnode       *pDnode;
  SMgmtWrapper *pWrapper;
} SDnodeMgmt;

// dmFile.c
int32_t dmReadFile(SDnodeMgmt *pMgmt);
int32_t dmWriteFile(SDnodeMgmt *pMgmt);
void    dmUpdateDnodeEps(SDnodeMgmt *pMgmt, SArray *pDnodeEps);

// dmMsg.c
void    dmSendStatusReq(SDnodeMgmt *pMgmt);
int32_t dmProcessConfigReq(SDnodeMgmt *pMgmt, SNodeMsg *pMsg);
int32_t dmProcessStatusRsp(SDnodeMgmt *pMgmt, SNodeMsg *pMsg);
int32_t dmProcessAuthRsp(SDnodeMgmt *pMgmt, SNodeMsg *pMsg);
int32_t dmProcessGrantRsp(SDnodeMgmt *pMgmt, SNodeMsg *pMsg);

// dmWorker.c
int32_t dmStartWorker(SDnodeMgmt *pMgmt);
void    dmStopWorker(SDnodeMgmt *pMgmt);
int32_t dmStartThread(SDnodeMgmt *pMgmt);
int32_t dmProcessMgmtMsg(SDnodeMgmt *pMgmt, SNodeMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_DNODE_INT_H_*/