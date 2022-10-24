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
#include "monInt.h"
#include "tcoding.h"
#include "tencode.h"

void tFreeSMonMmInfo(SMonMmInfo *pInfo) {
  taosArrayDestroy(pInfo->log.logs);
  taosArrayDestroy(pInfo->cluster.mnodes);
  taosArrayDestroy(pInfo->cluster.dnodes);
  taosArrayDestroy(pInfo->vgroup.vgroups);
  taosArrayDestroy(pInfo->stb.stbs);
  pInfo->cluster.mnodes = NULL;
  pInfo->cluster.dnodes = NULL;
  pInfo->vgroup.vgroups = NULL;
  pInfo->stb.stbs = NULL;
  pInfo->log.logs = NULL;
}

void tFreeSMonVmInfo(SMonVmInfo *pInfo) {
  taosArrayDestroy(pInfo->log.logs);
  taosArrayDestroy(pInfo->tfs.datadirs);
  pInfo->log.logs = NULL;
  pInfo->tfs.datadirs = NULL;
}

void tFreeSMonQmInfo(SMonQmInfo *pInfo) {
  taosArrayDestroy(pInfo->log.logs);
  pInfo->log.logs = NULL;
}

void tFreeSMonSmInfo(SMonSmInfo *pInfo) {
  taosArrayDestroy(pInfo->log.logs);
  pInfo->log.logs = NULL;
}

void tFreeSMonBmInfo(SMonBmInfo *pInfo) {
  taosArrayDestroy(pInfo->log.logs);
  pInfo->log.logs = NULL;
}
