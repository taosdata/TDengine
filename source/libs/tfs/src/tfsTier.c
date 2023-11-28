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

extern int64_t tsMinDiskFreeSize;

int32_t tfsInitTier(STfsTier *pTier, int32_t level) {
  memset(pTier, 0, sizeof(STfsTier));

  if (taosThreadSpinInit(&pTier->lock, 0) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  pTier->level = level;
  return 0;
}

void tfsDestroyTier(STfsTier *pTier) {
  for (int32_t id = 0; id < TFS_MAX_DISKS_PER_TIER; id++) {
    pTier->disks[id] = tfsFreeDisk(pTier->disks[id]);
  }

  pTier->ndisk = 0;
  taosThreadSpinDestroy(&pTier->lock);
}

STfsDisk *tfsMountDiskToTier(STfsTier *pTier, SDiskCfg *pCfg) {
  if (pTier->ndisk >= TFS_MAX_DISKS_PER_TIER) {
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

  if (id >= TFS_MAX_DISKS_PER_TIER) {
    terrno = TSDB_CODE_FS_TOO_MANY_MOUNT;
    return NULL;
  }

  STfsDisk *pDisk = tfsNewDisk(pCfg->level, id, pCfg->dir);
  if (pDisk == NULL) return NULL;

  pTier->disks[id] = pDisk;
  pTier->ndisk++;

  fDebug("disk %s is mounted to tier level %d id %d", pCfg->dir, pCfg->level, id);
  return pTier->disks[id];
}

void tfsUpdateTierSize(STfsTier *pTier) {
  SDiskSize size = {0};
  int32_t   nAvailDisks = 0;

  tfsLockTier(pTier);

  for (int32_t id = 0; id < pTier->ndisk; id++) {
    STfsDisk *pDisk = pTier->disks[id];
    if (pDisk == NULL) continue;
    if (tfsUpdateDiskSize(pDisk) < 0) continue;

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
int32_t tfsAllocDiskOnTier(STfsTier *pTier) {
  terrno = TSDB_CODE_FS_NO_VALID_DISK;

  tfsLockTier(pTier);

  if (pTier->ndisk <= 0 || pTier->nAvailDisks <= 0) {
    tfsUnLockTier(pTier);
    return -1;
  }

  int32_t retId = -1;
  int64_t avail = 0;
  for (int32_t id = 0; id < TFS_MAX_DISKS_PER_TIER; ++id) {
#if 1  // round-robin
    int32_t   diskId = (pTier->nextid + id) % pTier->ndisk;
    STfsDisk *pDisk = pTier->disks[diskId];

    if (pDisk == NULL) continue;

    if (pDisk->size.avail < tsMinDiskFreeSize) {
      uInfo("disk %s is full and skip it, level:%d id:%d free size:%" PRId64 " min free size:%" PRId64, pDisk->path,
            pDisk->level, pDisk->id, pDisk->size.avail, tsMinDiskFreeSize);
      continue;
    }

    retId = diskId;
    terrno = 0;
    pTier->nextid = (diskId + 1) % pTier->ndisk;
    break;
#else  // select the disk with the most available space
    STfsDisk *pDisk = pTier->disks[id];
    if (pDisk == NULL) continue;

    if (pDisk->size.avail < tsMinDiskFreeSize) continue;

    if (pDisk->size.avail > avail) {
      avail = pDisk->size.avail;
      retId = id;
      terrno = 0;
    }
#endif
  }

  tfsUnLockTier(pTier);
  return retId;
}

void tfsPosNextId(STfsTier *pTier) {
  int32_t nextid = 0;

  for (int32_t id = 1; id < pTier->ndisk; id++) {
    STfsDisk *pLDisk = pTier->disks[nextid];
    STfsDisk *pDisk = pTier->disks[id];
    if (pDisk->size.avail > tsMinDiskFreeSize && pDisk->size.avail > pLDisk->size.avail) {
      nextid = id;
    }
  }

  pTier->nextid = nextid;
}
