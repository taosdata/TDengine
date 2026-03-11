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

#ifndef _TD_DM_REPAIR_H_
#define _TD_DM_REPAIR_H_

#include "tdef.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  DM_REPAIR_STRATEGY_NONE = 0,
  DM_REPAIR_STRATEGY_META_FROM_UID,
  DM_REPAIR_STRATEGY_META_FROM_REDO,
  DM_REPAIR_STRATEGY_TSDB_DROP_INVALID_ONLY,
  DM_REPAIR_STRATEGY_TSDB_HEAD_ONLY_REBUILD,
  DM_REPAIR_STRATEGY_TSDB_FULL_REBUILD,
} EDmRepairStrategy;

typedef struct {
  EDmRepairStrategy strategy;
} SRepairMetaVnodeOpt;

typedef struct {
  EDmRepairStrategy strategy;
} SRepairTsdbFileOpt;

bool                  dmRepairFlowEnabled();
bool                  dmRepairNodeTypeIsVnode();
bool                  dmRepairModeIsForce();
bool                  dmRepairHasBackupPath();
const char           *dmRepairBackupPath();
const SRepairMetaVnodeOpt *dmRepairGetMetaVnodeOpt(int32_t vnodeId);
bool                       dmRepairNeedTsdbRepair(int32_t vnodeId);
const SRepairTsdbFileOpt  *dmRepairGetTsdbFileOpt(int32_t vnodeId, int32_t fileId);
bool                       dmRepairNeedWalRepair(int32_t vnodeId);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DM_REPAIR_H_*/
