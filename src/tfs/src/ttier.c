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

#include "taosdef.h"
#include "taoserror.h"
#include "tfsint.h"

// PROTECTED ==========================================
void tfsInitTier(STier *pTier, int level) { pTier->level = level; }

void tfsDestroyTier(STier *pTier) {
  for (int id = 0; id < TSDB_MAX_DISKS_PER_TIER; id++) {
    DISK_AT_TIER(pTier, id) = tfsFreeDisk(DISK_AT_TIER(pTier, id));
  }
  pTier->ndisk = 0;
}

SDisk *tfsMountDiskToTier(STier *pTier, SDiskCfg *pCfg) {
  ASSERT(pTier->level == pCfg->level);
  int id = 0;

  if (TIER_NDISKS(pTier) >= TSDB_MAX_DISKS_PER_TIER) {
    terrno = TSDB_CODE_FS_TOO_MANY_MOUNT;
    return NULL;
  }

  if (pTier->level == 0) {
    if (DISK_AT_TIER(pTier, 0) != NULL) {
      id = pTier->ndisk;
    } else {
      if (pCfg->primary) {
        id = 0;
      } else {
        id = pTier->ndisk + 1;
      }
      if (id >= TSDB_MAX_DISKS_PER_TIER) {
        terrno = TSDB_CODE_FS_TOO_MANY_MOUNT;
        return NULL;
      }
    }
  } else {
    id = pTier->ndisk;
  }

  DISK_AT_TIER(pTier, id) = tfsNewDisk(pCfg->level, id, pCfg->dir);
  if (DISK_AT_TIER(pTier, id) == NULL) return NULL;
  pTier->ndisk++;

  fDebug("disk %s is mounted to tier level %d id %d", pCfg->dir, pCfg->level, id);

  return DISK_AT_TIER(pTier, id);
}

void tfsUpdateTierInfo(STier *pTier) {
  STierMeta tmeta = {0};

  for (int id = 0; id < pTier->ndisk; id++) {
    tfsUpdateDiskInfo(DISK_AT_TIER(pTier, id));
    tmeta.size += DISK_SIZE(DISK_AT_TIER(pTier, id));
    tmeta.free += DISK_FREE_SIZE(DISK_AT_TIER(pTier, id));
  }

  pTier->tmeta = tmeta;
}