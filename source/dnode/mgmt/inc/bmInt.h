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

#ifndef _TD_DND_BNODE_INT_H_
#define _TD_DND_BNODE_INT_H_

#include "dndInt.h"

#include "bnode.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SBnodeMgmt {
  SBnode       *pBnode;
  SDnode       *pDnode;
  SMgmtWrapper *pWrapper;
  const char   *path;
  SMultiWorker  writeWorker;
  SSingleWorker monitorWorker;
} SBnodeMgmt;

// bmInt.c
int32_t bmOpen(SMgmtWrapper *pWrapper);
int32_t bmDrop(SMgmtWrapper *pWrapper);

// bmHandle.c
void    bmInitMsgHandle(SMgmtWrapper *pWrapper);
int32_t bmProcessCreateReq(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
int32_t bmProcessDropReq(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
int32_t bmProcessGetMonBmInfoReq(SMgmtWrapper *pWrapper, SNodeMsg *pReq);

// bmWorker.c
int32_t bmStartWorker(SBnodeMgmt *pMgmt);
void    bmStopWorker(SBnodeMgmt *pMgmt);
int32_t bmProcessWriteMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);
int32_t bmProcessMonitorMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_BNODE_INT_H_*/