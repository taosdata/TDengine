/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include <stdbool.h>

#if defined(__GNUC__) || defined(__clang__)
#define TD_WEAK __attribute__((weak))
#else
#define TD_WEAK
#endif

#include "dmRepair.h"

TD_WEAK bool dmRepairFlowEnabled() { return false; }
TD_WEAK bool dmRepairNodeTypeIsVnode() { return false; }
TD_WEAK bool dmRepairModeIsForce() { return false; }
TD_WEAK bool dmRepairHasBackupPath() { return false; }
TD_WEAK const char *dmRepairBackupPath() { return ""; }

TD_WEAK const SRepairMetaVnodeOpt *dmRepairGetMetaVnodeOpt(int32_t vnodeId) {
  TAOS_UNUSED(vnodeId);
  return NULL;
}

TD_WEAK bool dmRepairNeedTsdbRepair(int32_t vnodeId) {
  TAOS_UNUSED(vnodeId);
  return false;
}

TD_WEAK const SRepairTsdbFileOpt *dmRepairGetTsdbFileOpt(int32_t vnodeId, int32_t fileId) {
  TAOS_UNUSED(vnodeId);
  TAOS_UNUSED(fileId);
  return NULL;
}

TD_WEAK bool dmRepairNeedWalRepair(int32_t vnodeId) {
  TAOS_UNUSED(vnodeId);
  return false;
}
