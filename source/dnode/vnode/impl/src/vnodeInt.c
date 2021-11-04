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

#define _DEFAULT_SOURCE
#include "vnodeInt.h"

int32_t vnodeInit() { return 0; }
void    vnodeCleanup() {}

int32_t vnodeGetStatistics(SVnode *pVnode, SVnodeStatisic *pStat) { return 0; }
int32_t vnodeGetStatus(SVnode *pVnode, SVnodeStatus *pStatus) { return 0; }

SVnode *vnodeOpen(int32_t vgId, const char *path) { return NULL; }
void    vnodeClose(SVnode *pVnode) {}
int32_t vnodeAlter(SVnode *pVnode, const SVnodeCfg *pCfg) { return 0; }
SVnode *vnodeCreate(int32_t vgId, const char *path, const SVnodeCfg *pCfg) { return NULL; }
int32_t vnodeDrop(SVnode *pVnode) { return 0; }
int32_t vnodeCompact(SVnode *pVnode) { return 0; }
int32_t vnodeSync(SVnode *pVnode) { return 0; }

void vnodeProcessMsg(SVnode *pVnode, SVnodeMsg *pMsg) {}
