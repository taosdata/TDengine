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

typedef struct {
  int32_t nextid;
} SDiskCursor;

static SDiskCursor *tfsDiskCursorNew(int32_t nextid) {
  SDiskCursor *pCursor = (SDiskCursor *)taosMemoryMalloc(sizeof(SDiskCursor));
  if (pCursor == NULL) {
    return NULL;
  }
  pCursor->nextid = nextid;
  return pCursor;
}

static void tfsDiskCursorFree(void *p) {
  if (p) {
    SDiskCursor *pCursor = *(SDiskCursor **)p;
    taosMemoryFree(pCursor);
  }
}

int32_t tfsInitTier(STfsTier *pTier, int32_t level) {
  (void)memset(pTier, 0, sizeof(STfsTier));

  if (taosThreadSpinInit(&pTier->lock, 0) != 0) {
    TAOS_RETURN(TAOS_SYSTEM_ERROR(ERRNO));
  }

  pTier->level = level;
  pTier->hash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (pTier->hash == NULL) {
    TAOS_RETURN(terrno);
  }
  taosHashSetFreeFp(pTier->hash, tfsDiskCursorFree);
  return 0;
}

void tfsDestroyTier(STfsTier *pTier) {
  taosHashCleanup(pTier->hash);
  pTier->hash = NULL;
  for (int32_t id = 0; id < TFS_MAX_DISKS_PER_TIER; id++) {
    pTier->disks[id] = tfsFreeDisk(pTier->disks[id]);
  }

  pTier->ndisk = 0;
  (void)taosThreadSpinDestroy(&pTier->lock);
}

int32_t tfsMountDiskToTier(STfsTier *pTier, SDiskCfg *pCfg, STfsDisk **ppDisk) {
  int32_t   code = 0;
  int32_t   lino = 0;
  int32_t   id = 0;
  STfsDisk *pDisk = NULL;

  if (pTier->ndisk >= TFS_MAX_DISKS_PER_TIER) {
    TAOS_CHECK_GOTO(TSDB_CODE_FS_TOO_MANY_MOUNT, &lino, _exit);
  }

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
    TAOS_CHECK_GOTO(TSDB_CODE_FS_TOO_MANY_MOUNT, &lino, _exit);
  }

  TAOS_CHECK_GOTO(tfsNewDisk(pCfg->level, id, pCfg->disable, pCfg->dir, &pDisk), &lino, _exit);

  pTier->disks[id] = pDisk;
  pTier->ndisk++;

_exit:
  if (code != 0) {
    pDisk = tfsFreeDisk(pDisk);
    fError("%s failed at line %d since %s, disk:%s level:%d id:%d", __func__, lino, tstrerror(code), pCfg->dir,
           pCfg->level, id);
  } else {
    *ppDisk = pTier->disks[id];
    fDebug("disk %s is mounted to tier level %d id %d", pCfg->dir, pCfg->level, id);
  }

  TAOS_RETURN(code);
}

void tfsUpdateTierSize(STfsTier *pTier) {
  SDiskSize size = {0};
  int32_t   nAvailDisks = 0;

  TAOS_UNUSED(tfsLockTier(pTier));

  for (int32_t id = 0; id < pTier->ndisk; id++) {
    STfsDisk *pDisk = pTier->disks[id];
    if (pDisk == NULL) continue;
    if (tfsUpdateDiskSize(pDisk) < 0) continue;

    size.total += pDisk->size.total;
    size.used += pDisk->size.used;
    size.avail += pDisk->size.avail;
    if (pDisk->disable == 0) nAvailDisks++;
  }

  pTier->size = size;
  pTier->nAvailDisks = nAvailDisks;

  TAOS_UNUSED(tfsUnLockTier(pTier));
}

// Round-Robin to allocate disk on a tier
int32_t tfsAllocDiskOnTier(STfsTier *pTier, const char *label) {
  TAOS_UNUSED(tfsLockTier(pTier));

  if (pTier->ndisk <= 0 || pTier->nAvailDisks <= 0) {
    TAOS_UNUSED(tfsUnLockTier(pTier));
    TAOS_RETURN(TSDB_CODE_FS_NO_VALID_DISK);
  }

  // get the existing cursor or create a new one
  SDiskCursor *pCursor = NULL;
  void        *p = taosHashGet(pTier->hash, label, strlen(label));

  if (p) {
    pCursor = *(SDiskCursor **)p;
  } else {
    pCursor = tfsDiskCursorNew(pTier->nextid);
    if (pCursor == NULL) {
      TAOS_UNUSED(tfsUnLockTier(pTier));
      TAOS_RETURN(terrno);
    }

    int32_t code = taosHashPut(pTier->hash, label, strlen(label), &pCursor, sizeof(pCursor));
    if (code != 0) {
      TAOS_UNUSED(tfsUnLockTier(pTier));
      TAOS_RETURN(code);
    }
  }

  // do allocation
  int32_t retId = -1;
  int64_t avail = 0;
  for (int32_t id = 0; id < TFS_MAX_DISKS_PER_TIER; ++id) {
    int32_t   diskId = (pCursor->nextid + id) % pTier->ndisk;
    STfsDisk *pDisk = pTier->disks[diskId];

    if (pDisk == NULL) continue;

    if (pDisk->disable == 1) {
      uTrace("disk %s is disabled and skip it, level:%d id:%d disable:%" PRIi8, pDisk->path, pDisk->level, pDisk->id,
             pDisk->disable);
      continue;
    }

    if (pDisk->size.avail < tsMinDiskFreeSize) {
      uInfo("disk %s is full and skip it, level:%d id:%d free size:%" PRId64 " min free size:%" PRId64, pDisk->path,
            pDisk->level, pDisk->id, pDisk->size.avail, tsMinDiskFreeSize);
      continue;
    }

    retId = diskId;
    terrno = 0;
    pCursor->nextid = (diskId + 1) % pTier->ndisk;
    break;
  }

  TAOS_UNUSED(tfsUnLockTier(pTier));
  if (retId < 0) {
    TAOS_RETURN(TSDB_CODE_FS_NO_VALID_DISK);
  }
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
