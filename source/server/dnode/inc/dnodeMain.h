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

#ifndef _TD_DNODE_MAIN_H_
#define _TD_DNODE_MAIN_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "dnodeInt.h"

typedef enum {
  TD_RUN_STAT_INIT,
  TD_RUN_STAT_RUNNING,
  TD_RUN_STAT_STOPPED
} RunStat;

typedef struct DnMain {
  Dnode *      dnode;
  RunStat      runStatus;
  void *       dnodeTimer;
  SStartupStep startup;
} DnMain;

int32_t dnodeInitMain(Dnode *dnode, DnMain **main);
void    dnodeCleanupMain(DnMain **main);
int32_t dnodeInitStorage(Dnode *dnode, void **unused);
void    dnodeCleanupStorage(void **unused);
void    dnodeReportStartup(Dnode *dnode, char *name, char *desc);
void    dnodeReportStartupFinished(Dnode *dnode, char *name, char *desc);
void    dnodeProcessStartupReq(Dnode *dnode, SRpcMsg *pMsg);
void    dnodeProcessCreateMnodeReq(Dnode *dnode, SRpcMsg *pMsg);
void    dnodeProcessConfigDnodeReq(Dnode *dnode, SRpcMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DNODE_MAIN_H_*/
