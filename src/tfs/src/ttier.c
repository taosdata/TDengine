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

#include "ttier.h"
#include "tglobal.h"
#include "taoserror.h"

void tdInitTier(STier *pTier, int level) {
  pTier->level = level;
}

void tdDestroyTier(STier *pTier) {
  for (int id = 0; id < TSDB_MAX_DISK_PER_TIER; id++) {
    tdFreeDisk(DISK_AT_TIER(pTier, id));
    pTier->disks[id] = NULL;
  }
  pTier->ndisk = 0;
}

SDisk *tdAddDiskToTier(STier *pTier, SDiskCfg *pCfg) {
  ASSERT(pTier->level == pCfg->level);
  int id = 0;

  if (pTier->ndisk >= TSDB_MAX_DISK_PER_TIER) {
    terrno = TSDB_CODE_FS_TOO_MANY_MOUNT;
    return -1;
  }

  if (pTier->level == 0) {
    if (DISK_AT_TIER(pTier, 0) != NULL) {
      id = pTier->ndisk;
    } else {
      id = pTier->ndisk + 1;
      if (id >= TSDB_MAX_DISK_PER_TIER) {
        terrno = TSDB_CODE_FS_TOO_MANY_MOUNT;
        return -1;
      }
    }
  } else {
    id = pTier->ndisk;
  }

  pTier->disks[id] = tdNewDisk(pCfg->level, id, pCfg->dir);
  if (pTier->disks[id] == NULL) return -1;
  pTier->ndisk++;

  return id;
}

int tdUpdateTierInfo(STier *pTier) {
  for (int id = 0; id < pTier->ndisk; id++) {
    if (tdUpdateDiskInfo(DISK_AT_TIER(pTier, id)) < 0) {
      // TODO: deal with the error here
    }
  }
}