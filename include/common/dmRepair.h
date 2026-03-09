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
  DM_REPAIR_FILE_TYPE_META = 0,
  DM_REPAIR_FILE_TYPE_TSDB,
  DM_REPAIR_FILE_TYPE_WAL,
} EDmRepairFileType;

typedef enum {
  DM_REPAIR_STRATEGY_NONE = 0,
  DM_REPAIR_STRATEGY_META_FROM_UID,
  DM_REPAIR_STRATEGY_META_FROM_REDO,
  DM_REPAIR_STRATEGY_TSDB_SHALLOW_REPAIR,
  DM_REPAIR_STRATEGY_TSDB_DEEP_REPAIR,
} EDmRepairStrategy;

typedef struct {
  EDmRepairFileType fileType;
  int32_t           vnodeId;
  int32_t           fileId;
  EDmRepairStrategy strategy;
} SDmRepairTarget;

bool                  dmRepairFlowEnabled();
int32_t               dmRepairTargetCount();
const SDmRepairTarget *dmRepairTargetAt(int32_t index);
bool                  dmRepairHasBackupPath();
const char           *dmRepairBackupPath();

#ifdef __cplusplus
}
#endif

#endif /*_TD_DM_REPAIR_H_*/
