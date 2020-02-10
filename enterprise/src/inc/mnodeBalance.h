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

#ifndef TDENGINE_MODULE_MNODE_BALANCE_H
#define TDENGINE_MODULE_MNODE_BALANCE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

void mnodeBalanceInit();

int  mgmtSetDnodeShellRemoving(SDnodeObj *pDnode);
void mgmtSetDnodeUnRemove(SDnodeObj *pDnode);
void mgmtSetDnodeOfflineOnSdbChanged();



void mgmtCreateDnodeOrderList();

void mgmtReleaseDnodeOrderList();

void mgmtMakeDnodeOrderList();

void mgmtCalcSystemScore();

float mgmtTryCalcDnodeScore(SDnodeObj *pDnode, int extraVnode);

bool mgmtCheckDnodeInOfflineState(SDnodeObj *pDnode);

bool mgmtCheckDnodeInRemoveState(SDnodeObj *pDnode);

bool mgmtCheckModuleInDnode(SDnodeObj *pDnode, int moduleType);

void mgmtMonitorDnodeModule();

void mgmtSetModuleInDnode(SDnodeObj *pDnode, int moduleType);

int mgmtUnSetModuleInDnode(SDnodeObj *pDnode, int moduleType);

void mgmtMonitorVgroups();

void mgmtMonitorDnodes();

void mgmtCalcNumOfFreeVnodes(SDnodeObj *pDnode);

extern void *      dnodeSdb;
extern void *      vgSdb;
extern void *      balanceTimer;
extern int         mgmtOrderedDnodesSize;
extern int         mgmtOrderedDnodesMallocSize;
extern SDnodeObj **mgmtOrderedDnodes;
extern uint32_t    mgmtAccessSquence;
extern SMgmtIpList mgmtIpList;

#ifdef __cplusplus
}
#endif

#endif
