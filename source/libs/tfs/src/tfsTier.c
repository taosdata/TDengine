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
#include "tfsInt.h"

#define tfsLockTier(pTier) pthread_spin_lock(&(pTier)->lock)
#define tfsUnLockTier(pTier) pthread_spin_unlock(&(pTier)->lock)

int32_t tfsInitTier(STier *pTier, int32_t level) {
  memset(pTier, 0, sizeof(STier));

  if (pthread_spin_init(&pTier->lock, 0) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  pTier->level = level;
  return 0;
}

void tfsDestroyTier(STier *pTier) {
  for (int32_t id = 0; id < TSDB_MAX_DISKS_PER_TIER; id++) {
    pTier->disks[id] = tfsFreeDisk(pTier->disks[id]);
  }

  pTier->ndisk = 0;
  pthread_spin_destroy(&(pTier->lock));
}

SDisk *tfsMountDiskToTier(STier *pTier, SDiskCfg *pCfg) {
  if (pTier->ndisk >= TSDB_MAX_DISKS_PER_TIER) {
    terrno = TSDB_CODE_FS_TOO_MANY_MOUNT;
    return NULL;
  }

  int32_t id = 0;
  if (pTier->level == 0) {
    if (pTier->disks[0] != NULL) {
      id = pTier->ndisk;
    } else {
      if (pCfg->primary) {
        id = 0;
      } else {
        id = pTier->ndisk + 1;
      }
    }
  } else {
    id = pTier->ndisk;
  }

  if (id >= TSDB_MAX_DISKS_PER_TIER) {
    terrno = TSDB_CODE_FS_TOO_MANY_MOUNT;
    return NULL;
  }

  SDisk *pDisk = tfsNewDisk(pCfg->level, id, pCfg->dir);
  if (pDisk == NULL) return NULL;

  pTier->disks[id] = pDisk;
  pTier->ndisk++;

  fInfo("disk %s is mounted to tier level %d id %d", pCfg->dir, pCfg->level, id);
  return pTier->disks[id];
}

void tfsUpdateTierSize(STier *pTier) {
  SDiskSize size = {0};
  int16_t   nAvailDisks = 0;

  tfsLockTier(pTier);

  for (int32_t id = 0; id < pTier->ndisk; id++) {
    SDisk *pDisk = pTier->disks[id];
    if (pDisk == NULL) continue;

    size.total += pDisk->size.total;
    size.used += pDisk->size.used;
    size.avail += pDisk->size.avail;
    nAvailDisks++;
  }

  pTier->size = size;
  pTier->nAvailDisks = nAvailDisks;

  tfsUnLockTier(pTier);
}

// Round-Robin to allocate disk on a tier
int32_t tfsAllocDiskOnTier(STier *pTier) {
  terrno = TSDB_CODE_FS_NO_VALID_DISK;

  tfsLockTier(pTier);

  if (pTier->ndisk <= 0 || pTier->nAvailDisks <= 0) {
    tfsUnLockTier(pTier);
    return -1;
  }

  int32_t retId = -1;
  for (int32_t id = 0; id < TSDB_MAX_DISKS_PER_TIER; ++id) {
    int32_t diskId = (pTier->nextid + id) % pTier->ndisk;
    SDisk  *pDisk = pTier->disks[diskId];

    if (pDisk == NULL) continue;

    if (pDisk->size.avail < TFS_MIN_DISK_FREE_SIZE) continue;

    retId = diskId;
    terrno = 0;
    pTier->nextid = (diskId + 1) % pTier->ndisk;
    break;
  }

  tfsUnLockTier(pTier);
  return retId;
}

void tfsPosNextId(STier *pTier) {
  int32_t nextid = 0;

  for (int32_t id = 1; id < pTier->ndisk; id++) {
    SDisk *pLDisk = pTier->disks[nextid];
    SDisk *pDisk = pTier->disks[id];
    if (pDisk->size.avail > TFS_MIN_DISK_FREE_SIZE && pDisk->size.avail > pLDisk->size.avail) {
      nextid = id;
    }
  }

  pTier->nextid = nextid;
}
