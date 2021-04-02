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

#define tfsLockTier(pTier) pthread_spin_lock(&((pTier)->lock))
#define tfsUnLockTier(pTier) pthread_spin_unlock(&((pTier)->lock))

// PROTECTED ==========================================
int tfsInitTier(STier *pTier, int level) {
  memset((void *)pTier, 0, sizeof(*pTier));

  int code = pthread_spin_init(&(pTier->lock), 0);
  if (code) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }

  pTier->level = level;

  return 0;
}

void tfsDestroyTier(STier *pTier) {
  for (int id = 0; id < TSDB_MAX_DISKS_PER_TIER; id++) {
    DISK_AT_TIER(pTier, id) = tfsFreeDisk(DISK_AT_TIER(pTier, id));
  }

  pTier->ndisk = 0;

  pthread_spin_destroy(&(pTier->lock));
}

SDisk *tfsMountDiskToTier(STier *pTier, SDiskCfg *pCfg) {
  ASSERT(pTier->level == pCfg->level);

  int    id = 0;
  SDisk *pDisk;

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

  pDisk = tfsNewDisk(pCfg->level, id, pCfg->dir);
  if (pDisk == NULL) return NULL;
  DISK_AT_TIER(pTier, id) = pDisk;
  pTier->ndisk++;

  fInfo("disk %s is mounted to tier level %d id %d", pCfg->dir, pCfg->level, id);

  return DISK_AT_TIER(pTier, id);
}

void tfsUpdateTierInfo(STier *pTier, STierMeta *pTierMeta) {
  STierMeta tmeta;

  if (pTierMeta == NULL) {
    pTierMeta = &tmeta;
  }
  memset(pTierMeta, 0, sizeof(*pTierMeta));

  tfsLockTier(pTier);

  for (int id = 0; id < pTier->ndisk; id++) {
    if (tfsUpdateDiskInfo(DISK_AT_TIER(pTier, id)) < 0) {
      continue;
    }
    pTierMeta->size += DISK_SIZE(DISK_AT_TIER(pTier, id));
    pTierMeta->used += DISK_USED_SIZE(DISK_AT_TIER(pTier, id));
    pTierMeta->free += DISK_FREE_SIZE(DISK_AT_TIER(pTier, id));
    pTierMeta->nAvailDisks++;
  }

  pTier->tmeta = *pTierMeta;

  tfsUnLockTier(pTier);
}

// Round-Robin to allocate disk on a tier
int tfsAllocDiskOnTier(STier *pTier) {
  ASSERT(pTier->ndisk > 0);
  int    id = TFS_UNDECIDED_ID;
  SDisk *pDisk;

  tfsLockTier(pTier);

  if (TIER_AVAIL_DISKS(pTier) <= 0) {
    tfsUnLockTier(pTier);
    return id;
  }

  id = pTier->nextid;
  while (true) {
    pDisk = DISK_AT_TIER(pTier, id);
    ASSERT(pDisk != NULL);

    if (DISK_FREE_SIZE(pDisk) < TFS_MIN_DISK_FREE_SIZE) {
      id = (id + 1) % pTier->ndisk;
      if (id == pTier->nextid) {
        tfsUnLockTier(pTier);
        return TFS_UNDECIDED_ID;
      } else {
        continue;
      }
    } else {
      pTier->nextid = (id + 1) % pTier->ndisk;
      break;
    }
  }

  tfsUnLockTier(pTier);
  return id;
}

void tfsGetTierMeta(STier *pTier, STierMeta *pTierMeta) {
  ASSERT(pTierMeta != NULL);

  tfsLockTier(pTier);
  *pTierMeta = pTier->tmeta;
  tfsUnLockTier(pTier);
}

void tfsPosNextId(STier *pTier) {
  ASSERT(pTier->ndisk > 0);
  int nextid = 0;

  for (int id = 1; id < pTier->ndisk; id++) {
    SDisk *pLDisk = DISK_AT_TIER(pTier, nextid);
    SDisk *pDisk = DISK_AT_TIER(pTier, id);
    if (DISK_FREE_SIZE(pDisk) > TFS_MIN_DISK_FREE_SIZE && DISK_FREE_SIZE(pDisk) > DISK_FREE_SIZE(pLDisk)) {
      nextid = id;
    }
  }

  pTier->nextid = nextid;
}
