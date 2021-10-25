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
  DN_RUN_STAT_INIT,
  DN_RUN_STAT_RUNNING,
  DN_RUN_STAT_STOPPED
} EDnStat;

int32_t dnodeInitMain();
void    dnodeCleanupMain();
int32_t dnodeInitStorage();
void    dnodeCleanupStorage();
void    dnodeReportStartup(char *name, char *desc);
void    dnodeReportStartupFinished(char *name, char *desc);
void    dnodeProcessStartupReq(SRpcMsg *pMsg);
void    dnodeProcessCreateMnodeReq(SRpcMsg *pMsg);
void    dnodeProcessConfigDnodeReq(SRpcMsg *pMsg);
EDnStat dnodeGetRunStat();
void    dnodeSetRunStat();
void*   dnodeGetTimer();

#ifdef __cplusplus
}
#endif

#endif /*_TD_DNODE_MAIN_H_*/
