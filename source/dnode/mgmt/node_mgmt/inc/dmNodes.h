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

#ifndef _TD_DND_NODES_H_
#define _TD_DND_NODES_H_

#include "dmInt.h"

#ifdef __cplusplus
extern "C" {
#endif

SMgmtFunc dmGetMgmtFunc();
SMgmtFunc qmGetMgmtFunc();
SMgmtFunc smGetMgmtFunc();
SMgmtFunc vmGetMgmtFunc();
SMgmtFunc mmGetMgmtFunc();

void mmGetMonitorInfo(void *pMgmt, SMonMmInfo *pInfo);
void vmGetMonitorInfo(void *pMgmt, SMonVmInfo *pInfo);
void qmGetMonitorInfo(void *pMgmt, SMonQmInfo *pInfo);
void smGetMonitorInfo(void *pMgmt, SMonSmInfo *pInfo);
void bmGetMonitorInfo(void *pMgmt, SMonBmInfo *pInfo);

void vmGetVnodeLoads(void *pMgmt, SMonVloadInfo *pInfo, bool isReset);
void vmGetVnodeLoadsLite(void *pMgmt, SMonVloadInfo *pInfo);
void mmGetMnodeLoads(void *pMgmt, SMonMloadInfo *pInfo);
void qmGetQnodeLoads(void *pMgmt, SQnodeLoad *pInfo);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_NODES_H_*/