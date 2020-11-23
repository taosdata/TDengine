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
#include "os.h"

#include "tfsint.h"
#include "taoserror.h"

#define TSDB_MAX_DISK_PER_TIER 16
struct STier {
  int    level;
  int    ndisk;
  SDisk *disks[TSDB_MAX_DISK_PER_TIER];
};

// PROTECTED ==========================================
void tfsInitTier(STier *pTier, int level) {
  pTier->level = level;
}

void tfsDestroyTier(STier *pTier) {
  for (int id = 0; id < TSDB_MAX_DISK_PER_TIER; id++) {
    tfsFreeDisk(DISK_AT_TIER(pTier, id));
    pTier->disks[id] = NULL;
  }
  pTier->ndisk = 0;
}

SDisk *tfsMountDiskToTier(STier *pTier, SDiskCfg *pCfg) {
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

  DISK_AT_TIER(pTier, id) = tfsNewDisk(pCfg->level, id, pCfg->dir);
  if (DISK_AT_TIER(pTier, id) == NULL) return -1;
  pTier->ndisk++;

  fDebug("disk %s is mounted to level %d id %d", pCfg->dir, pCfg->level, id);

  return id;
}

int tfsUpdateTierInfo(STier *pTier) {
  for (int id = 0; id < pTier->ndisk; id++) {
    if (tfsUpdateDiskInfo(DISK_AT_TIER(pTier, id)) < 0) {
      return -1;
    }
  }
  return 0;
}