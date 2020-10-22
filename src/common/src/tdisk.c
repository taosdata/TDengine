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
#include "tutil.h"
#include "tdisk.h"
#include "tulog.h"

#define DISK_MIN_FREE_SPACE 30 * 1024 * 1024  // disk free space less than 100M will not create new file again
#define DNODE_DISK_AVAIL(pDisk) ((pDisk)->dmeta.free > DISK_MIN_FREE_SPACE)

static int tdFormatDir(char *idir, char *odir);
static int tdCheckDisk(char *dirName, int level, int primary);
static int tdUpdateDiskMeta(SDisk *pDisk);
static int tdAddDisk(SDnodeTier *pDnodeTier, char *dir, int level, int primary);

struct SDnodeTier *tsDnodeTier = NULL;

SDnodeTier *tdNewTier() {
  SDnodeTier *pDnodeTier = (SDnodeTier *)calloc(1, sizeof(*pDnodeTier));
  if (pDnodeTier == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return NULL;
  }

  int ret = pthread_mutex_init(&(pDnodeTier->lock), NULL);
  if (ret != 0) {
    terrno = TAOS_SYSTEM_ERROR(ret);
    tdCloseTier(pDnodeTier);
    return NULL;
  }

  pDnodeTier->map = taosHashInit(TSDB_MAX_TIERS * TSDB_MAX_DISKS_PER_TIER * 2,
                                 taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (pDnodeTier->map == NULL) {
    terrno = TSDB_CODE_COM_OUT_OF_MEMORY;
    tdCloseTier(pDnodeTier);
    return NULL;
  }

  return pDnodeTier;
}

void *tdCloseTier(SDnodeTier *pDnodeTier) {
  if (pDnodeTier) {
    if (pDnodeTier->map) {
      taosHashCleanup(pDnodeTier->map);
      pDnodeTier->map = NULL;
    }

    pthread_mutex_destroy(&(pDnodeTier->lock));

    for (int i = 0; i < pDnodeTier->nTiers; i++) {
      STier *pTier = pDnodeTier->tiers + i;
      for (int j = 0; j < pTier->nDisks; j++) {
        if (pTier->disks[j]) {
          free(pTier->disks[j]);
          pTier->disks[j] = NULL;
        }
      }
    }
    free(pDnodeTier);
  }
  return NULL;
}

int tdAddDisks(SDnodeTier *pDnodeTier, SDiskCfg *pDiskCfgs, int ndisks) {
  ASSERT(ndisks > 0);

  for (int i = 0; i < ndisks; i++) {
    SDiskCfg *pCfg = pDiskCfgs + i;
    tdAddDisk(pDnodeTier, pCfg->dir, pCfg->level, pCfg->primary);
  }

  if (tdCheckTiers(pDnodeTier) < 0) return -1;
  
  return 0;
}

int tdUpdateTiersInfo(SDnodeTier *pDnodeTier) {
  tdLockTiers(pDnodeTier);

  pDnodeTier->meta.tsize = 0;
  pDnodeTier->meta.avail = 0;

  for (int i = 0; i < pDnodeTier->nTiers; i++) {
    STier *pTier = pDnodeTier->tiers + i;

    for (int j = 0; j < pTier->nDisks; j++) {
      SDisk *pDisk = pTier->disks[j];
      if (tdUpdateDiskMeta(pDisk) < 0) return -1;

      pDnodeTier->meta.tsize += pDisk->dmeta.size;
      pDnodeTier->meta.avail += pDisk->dmeta.free;
    }
  }

  tdUnLockTiers(pDnodeTier);
  return 0;
}

int tdCheckTiers(SDnodeTier *pDnodeTier) {
  ASSERT(pDnodeTier->nTiers > 0);
  if (DNODE_PRIMARY_DISK(pDnodeTier) == NULL) {
    terrno = TSDB_CODE_DND_LACK_PRIMARY_DISK;
    return -1;
  }

  for (int i = 0; i < pDnodeTier->nTiers; i++) {
    if (pDnodeTier->tiers[i].nDisks == 0) {
      terrno = TSDB_CODE_DND_NO_DISK_AT_TIER;
      return -1;
    }
  }

  return 0;
}

SDisk *tdAssignDisk(SDnodeTier *pDnodeTier, int level) {
  ASSERT(level < pDnodeTier->nTiers);

  STier *pTier = pDnodeTier->tiers + level;
  SDisk *pDisk = NULL;

  ASSERT(pTier->nDisks > 0);

  tdLockTiers(pDnodeTier);

  for (int i = 0; i < pTier->nDisks; i++) {
    SDisk *iDisk = pTier->disks[i];
    if (tdUpdateDiskMeta(iDisk) < 0) return NULL;
    if (DNODE_DISK_AVAIL(iDisk)) {
      if (pDisk == NULL || pDisk->dmeta.nfiles > iDisk->dmeta.nfiles) {
        pDisk = iDisk;
      }
    }
  }

  if (pDisk == NULL) {
    terrno = TSDB_CODE_DND_NO_DISK_SPACE;
    tdUnLockTiers(pDnodeTier);
    return NULL;
  }

  tdIncDiskFiles(pDnodeTier, pDisk, false);

  tdUnLockTiers(pDnodeTier);

  return NULL;
}

SDisk *tdGetDiskByName(SDnodeTier *pDnodeTier, char *dirName) {
  char     fdirName[TSDB_FILENAME_LEN] = "\0";
  SDiskID *pDiskID = NULL;

  if (tdFormatDir(dirName, fdirName) < 0) {
    return NULL;
  }

  void *ptr = taosHashGet(pDnodeTier->map, (void *)fdirName, strnlen(fdirName, TSDB_FILENAME_LEN));
  if (ptr == NULL) return NULL;
  pDiskID = (SDiskID *)ptr;

  return tdGetDisk(pDnodeTier, pDiskID->level, pDiskID->did);
}

void tdIncDiskFiles(SDnodeTier *pDnodeTier, SDisk *pDisk, bool lock) {
  if (lock) {
    tdLockTiers(pDnodeTier);
  }

  pDisk->dmeta.nfiles++;

  if (lock) {
    tdUnLockTiers(pDnodeTier);
  }
}

void tdDecDiskFiles(SDnodeTier *pDnodeTier, SDisk *pDisk, bool lock) {
  if (lock) {
    tdLockTiers(pDnodeTier);
  }

  pDisk->dmeta.nfiles--;

  if (lock) {
    tdUnLockTiers(pDnodeTier);
  }
}

static int tdFormatDir(char *idir, char *odir) {
  wordexp_t wep;

  int code = wordexp(idir, &wep, 0);
  if (code != 0) {
    uError("failed to format dir %s since %s", idir, strerror(code));
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }

  if (realpath(wep.we_wordv[0], odir) == NULL) {
    uError("failed to format dir %s since %s", idir, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    wordfree(&wep);
    return -1;
  }

  wordfree(&wep);
  return 0;
}

static int tdCheckDisk(char *dirName, int level, int primary) {
  if (access(dirName, W_OK | R_OK | F_OK) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  struct stat pstat;
  if (stat(dirName, &pstat) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (S_ISDIR(pstat.st_mode)) {
    return 0;
  } else {
    terrno = TSDB_CODE_DND_DISK_NOT_DIRECTORY;
    return -1;
  }
}

static int tdUpdateDiskMeta(SDisk *pDisk) {
  struct statvfs dstat;
  if (statvfs(pDisk->dir, &dstat) < 0) {
    uError("failed to get dir %s information since %s", pDisk->dir, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  pDisk->dmeta.size = dstat.f_bsize * dstat.f_blocks;
  pDisk->dmeta.free = dstat.f_bsize * dstat.f_bavail;

  return 0;
}

static int tdAddDisk(SDnodeTier *pDnodeTier, char *dir, int level, int primary) {
  char    dirName[TSDB_FILENAME_LEN] = "\0";
  STier * pTier = NULL;
  SDiskID diskid = {0};
  SDisk * pDisk = NULL;

  if (level < 0 || level >= TSDB_MAX_TIERS) {
    terrno = TSDB_CODE_DND_INVALID_DISK_TIER;
    uError("failed to add disk %s to tier %d level since %s", dir, level, tstrerror(terrno));
    return -1;
  }

  if (tdFormatDir(dir, dirName) < 0) {
    uError("failed to add disk %s to tier %d level since %s", dir, level, tstrerror(terrno));
    return -1;
  }

  pTier = pDnodeTier->tiers + level;
  diskid.level = level;

  if (pTier->nDisks >= TSDB_MAX_DISKS_PER_TIER) {
    terrno = TSDB_CODE_DND_TOO_MANY_DISKS;
    uError("failed to add disk %s to tier %d level since %s", dir, level, tstrerror(terrno));
    return -1;
  }

  if (tdGetDiskByName(pDnodeTier, dirName) != NULL) {
    terrno = TSDB_CODE_DND_DISK_ALREADY_EXISTS;
    uError("failed to add disk %s to tier %d level since %s", dir, level, tstrerror(terrno));
    return -1;
  }

  if (tdCheckDisk(dirName, level, primary) < 0) {
    uError("failed to add disk %s to tier %d level since %s", dir, level, tstrerror(terrno));
    return -1;
  }

  if (primary) {
    if (level != 0) {
      terrno = TSDB_CODE_DND_INVALID_DISK_TIER;
      uError("failed to add disk %s to tier %d level since %s", dir, level, tstrerror(terrno));
      return -1;
    }

    if (DNODE_PRIMARY_DISK(pDnodeTier) != NULL) {
      terrno = TSDB_CODE_DND_DUPLICATE_PRIMARY_DISK;
      uError("failed to add disk %s to tier %d level since %s", dir, level, tstrerror(terrno));
      return -1;
    }

    diskid.did = 0;
  } else {
    if (level == 0) {
      if (DNODE_PRIMARY_DISK(pDnodeTier) != NULL) {
        diskid.did = pTier->nDisks;
      } else {
        diskid.did = pTier->nDisks + 1;
        if (diskid.did >= TSDB_MAX_DISKS_PER_TIER) {
          terrno = TSDB_CODE_DND_TOO_MANY_DISKS;
          uError("failed to add disk %s to tier %d level since %s", dir, level, tstrerror(terrno));
          return -1;
        }
      }
    } else {
      diskid.did = pTier->nDisks;
    }
  }

  pDisk = (SDisk *)calloc(1, sizeof(SDisk));
  if (pDisk == NULL) {
    terrno = TSDB_CODE_DND_OUT_OF_MEMORY;
    uError("failed to add disk %s to tier %d level since %s", dir, level, tstrerror(terrno));
    return -1;
  }

  strncpy(pDisk->dir, dirName, TSDB_FILENAME_LEN);
  pDisk->level = diskid.level;
  pDisk->did = diskid.did;

  if (taosHashPut(pDnodeTier->map, (void *)dirName, strnlen(dirName, TSDB_FILENAME_LEN), (void *)(&diskid),
                  sizeof(diskid)) < 0) {
    free(pDisk);
    terrno = TSDB_CODE_DND_OUT_OF_MEMORY;
    uError("failed to add disk %s to tier %d level since %s", dir, level, tstrerror(terrno));
    return -1;
  }

  pTier->nDisks++;
  pTier->disks[diskid.did] = pDisk;
  pDnodeTier->nTiers = MAX(pDnodeTier->nTiers, level + 1);

  return 0;
}