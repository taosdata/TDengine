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

#ifndef TD_TTIER_H
#define TD_TTIER_H

#include "tdisk.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_MAX_DISK_PER_TIER 16

typedef struct {
  int    level;
  int    ndisk;
  SDisk *disks[TSDB_MAX_DISK_PER_TIER];
} STier;

#define DISK_AT_TIER(pTier, id) ((pTier)->disks + (id))

void   tdInitTier(STier *pTier, int level);
void   tdDestroyTier(STier *pTier);
SDisk *tdAddDiskToTier(STier *pTier, SDiskCfg *pCfg);
int    tdUpdateTierInfo(STier *pTier);

#ifdef __cplusplus
}
#endif

#endif