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
#include "tmount.h"
#include "hash.h"
#include "taosdef.h"
#include "taoserror.h"
#include "tulog.h"
#include "tutil.h"

typedef struct {
  uint64_t size;
  uint64_t free;
  uint64_t nfiles;
} SDiskMeta;

typedef struct {
  int       level;
  int       id;
  char      dir[TSDB_FILENAME_LEN];
  SDiskMeta dmeta;
} SDisk;

typedef struct {
  uint64_t tsize;
  uint64_t avail; // bytes
} STiersMeta;

typedef struct {
  int    level;
  int    nDisks;
  SDisk *disks[TSDB_MAX_DISKS_PER_TIER];
} STier;

typedef struct STiers {
  pthread_mutex_t lock;
  STiersMeta      meta;
  int             nLevel;
  STier           tiers[TSDB_MAX_TIERS];
  SHashObj *      map;
} STiers;

#define DISK_MIN_FREE_SPACE 30 * 1024 * 1024  // disk free space less than 100M will not create new file again
#define DNODE_DISK_AVAIL(pDisk) ((pDisk)->dmeta.free > DISK_MIN_FREE_SPACE)
#define TIER_AT_LEVEL(level) (pTiers->tiers + (level))
#define DISK_AT(level, did) (TIER_AT_LEVEL(level)->disks[(did)])

static struct STiers  tdTiers;
static struct STiers *pTiers = &tdTiers;

int tdInitMount(SDiskCfg *pDiskCfg, int ndisk) {
  ASSERT(ndisk > 0);

  memset((void *)pTiers, 0, sizeof(*pTiers));

  int ret = pthread_mutex_init(&(pTiers->lock), NULL);
  if (ret != 0) {
    terrno = TAOS_SYSTEM_ERROR(ret);
    return -1;
  }

  pTiers->map = taosHashInit(TSDB_MAX_TIERS * TSDB_MAX_DISKS_PER_TIER * 2,
                             taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (pTiers->map == NULL) {
    terrno = TSDB_CODE_COM_OUT_OF_MEMORY;
    tdDestroyMount();
    return -1;
  }

  for (int idisk = 0; idisk < ndisk; idisk++) {
    if (tdAddDisk(pDiskCfg + idisk) < 0) {
      tdDestroyMount();
      return -1;
    }
  }

  if (tdCheckTiers() < 0) {
    tdDestroyMount();
    return -1;
  }

  return 0;
}

void tdDestroyMount() {
  taosHashCleanup(pTiers->map);
  pTiers->map = NULL;

  pthread_mutex_destroy(&(pTiers->lock));

  for (int level = TSDB_MAX_TIERS - 1; level >= 0; --level) {
    for (int did = TSDB_MAX_DISKS_PER_TIER - 1; did >= 0; --did) {
      if (DISK_AT(level, did)) {
        free(DISK_AT(level, did));
        DISK_AT(level, did) = NULL;
      }
    }
  }
}

int tdUpdateDiskInfos() {
  tdLockTiers(pTiers);

  pTiers->meta.tsize = 0;
  pTiers->meta.avail = 0;

  for (int i = 0; i < pTiers->nLevel; i++) {
    STier *pTier = pTiers->tiers + i;

    for (int j = 0; j < pTier->nDisks; j++) {
      SDisk *pDisk = pTier->disks[j];
      if (tdUpdateDiskMeta(pDisk) < 0) {
        tdUnLockTiers(pTiers);
        return -1;
      }

      pTiers->meta.tsize += pDisk->dmeta.size;
      pTiers->meta.avail += pDisk->dmeta.free;
    }
  }

  tdUnLockTiers(pTiers);
  return 0;
}

void tdGetPrimaryPath(char *dst) { strncpy(dst, DISK_AT(0, 0)->dir, TSDB_FILENAME_LEN); }

static SDisk *tdGetPrimaryDisk() { return DISK_AT(0, 0); }

static int tdCheckTiers() {
  ASSERT(pTiers->nLevel > 0);
  if (tdGetPrimaryDisk(pTiers) == NULL) {
    terrno = TSDB_CODE_COM_LACK_PRIMARY_DISK;
    return -1;
  }

  for (int i = 0; i < pTiers->nLevel; i++) {
    if (pTiers->tiers[i].nDisks == 0) {
      terrno = TSDB_CODE_COM_NO_DISK_AT_TIER;
      return -1;
    }
  }

  return 0;
}

static SDisk *tdAssignDisk(int level) {
  ASSERT(level < pTiers->nLevel);

  STier *pTier = pTiers->tiers + level;
  SDisk *pDisk = NULL;

  ASSERT(pTier->nDisks > 0);

  tdLockTiers(pTiers);

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
    terrno = TSDB_CODE_COM_NO_DISK_SPACE;
    tdUnLockTiers(pTiers);
    return NULL;
  }

  tdIncDiskFiles(pTiers, pDisk, false);

  tdUnLockTiers(pTiers);

  return pDisk;
}

static SDisk *tdGetDiskByName(char *dirName) {
  char     fdirName[TSDB_FILENAME_LEN] = "\0";
  SDiskID *pDiskID = NULL;

  if (tfsFormatDir(dirName, fdirName) < 0) {
    return NULL;
  }

  void *ptr = taosHashGet(pTiers->map, (void *)fdirName, strnlen(fdirName, TSDB_FILENAME_LEN));
  if (ptr == NULL) return NULL;
  pDiskID = (SDiskID *)ptr;

  return tdGetDisk(pTiers, pDiskID->level, pDiskID->did);
}

static void tdIncDiskFiles(SDisk *pDisk, bool lock) {
  if (lock) {
    tdLockTiers(pTiers);
  }

  pDisk->dmeta.nfiles++;

  if (lock) {
    tdUnLockTiers(pTiers);
  }
}

static void tdDecDiskFiles(SDisk *pDisk, bool lock) {
  if (lock) {
    tdLockTiers(pTiers);
  }

  pDisk->dmeta.nfiles--;

  if (lock) {
    tdUnLockTiers(pTiers);
  }
}

static int tfsFormatDir(char *idir, char *odir) {
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

static int tdCheckDisk(char *dirName) {
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
    terrno = TSDB_CODE_COM_DISK_NOT_DIRECTORY;
    return -1;
  }
}

static int tdUpdateDiskMeta(SDisk *pDisk) {
  SysDiskSize dstat;
  if (taosGetDiskSize(pDisk->dir, &dstat) < 0) {
    uError("failed to get dir %s information since %s", pDisk->dir, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  pDisk->dmeta.size = dstat.tsize;
  pDisk->dmeta.free = dstat.avail;

  return 0;
}

static int tdAddDisk(SDiskCfg *pCfg) {
  char    dirName[TSDB_FILENAME_LEN] = "\0";
  STier * pTier = NULL;
  SDiskID diskid = {0};
  SDisk * pDisk = NULL;

  if (pCfg->level < 0 || pCfg->level >= TSDB_MAX_TIERS) {
    terrno = TSDB_CODE_COM_INVALID_DISK_TIER;
    uError("failed to add disk %s to tier %d level since %s", pCfg->dir, pCfg->level, tstrerror(terrno));
    return -1;
  }

  if (tfsFormatDir(pCfg->dir, dirName) < 0) {
    uError("failed to add disk %s to tier %d level since %s", pCfg->dir, pCfg->level, tstrerror(terrno));
    return -1;
  }

  pTier = TIER_AT_LEVEL(pCfg->level);
  diskid.level = pCfg->level;

  if (pTier->nDisks >= TSDB_MAX_DISKS_PER_TIER) {
    terrno = TSDB_CODE_COM_TOO_MANY_DISKS;
    uError("failed to add disk %s to tier %d level since %s", pCfg->dir, pCfg->level, tstrerror(terrno));
    return -1;
  }

  if (tdGetDiskByName(dirName) != NULL) {
    terrno = TSDB_CODE_COM_DISK_ALREADY_EXISTS;
    uError("failed to add disk %s to tier %d level since %s", pCfg->dir, pCfg->level, tstrerror(terrno));
    return -1;
  }

  if (tdCheckDisk(dirName) < 0) {
    uError("failed to add disk %s to tier %d level since %s", pCfg->dir, pCfg->level, tstrerror(terrno));
    return -1;
  }

  if (pCfg->primary) {
    if (pCfg->level != 0) {
      terrno = TSDB_CODE_COM_INVALID_DISK_TIER;
      uError("failed to add disk %s to tier %d level since %s", pCfg->dir, pCfg->level, tstrerror(terrno));
      return -1;
    }

    if (tdGetPrimaryDisk() != NULL) {
      terrno = TSDB_CODE_COM_DUPLICATE_PRIMARY_DISK;
      uError("failed to add disk %s to tier %d level since %s", pCfg->dir, pCfg->level, tstrerror(terrno));
      return -1;
    }

    diskid.did = 0;
  } else {
    if (level == 0) {
      if (tdGetPrimaryDisk() != NULL) {
        diskid.did = pTier->nDisks;
      } else {
        diskid.did = pTier->nDisks + 1;
        if (diskid.did >= TSDB_MAX_DISKS_PER_TIER) {
          terrno = TSDB_CODE_COM_TOO_MANY_DISKS;
          uError("failed to add disk %s to tier %d level since %s", pCfg->dir, pCfg->level, tstrerror(terrno));
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
    uError("failed to add disk %s to tier %d level since %s", pCfg->dir, pCfg->level, tstrerror(terrno));
    return -1;
  }

  strncpy(pDisk->dir, dirName, TSDB_FILENAME_LEN);
  pDisk->level = diskid.level;
  pDisk->did = diskid.did;

  if (taosHashPut(pTiers->map, (void *)dirName, strnlen(dirName, TSDB_FILENAME_LEN), (void *)(&diskid),
                  sizeof(diskid)) < 0) {
    free(pDisk);
    terrno = TSDB_CODE_DND_OUT_OF_MEMORY;
    uError("failed to add disk %s to tier %d level since %s", pCfg->dir, pCfg->level, tstrerror(terrno));
    return -1;
  }

  pTier->nDisks++;
  pTier->disks[diskid.did] = pDisk;
  pTiers->nLevel = MAX(pTiers->nLevel, level + 1);

  return 0;
}

static void taosGetDisk() {
  const double unit = 1024 * 1024 * 1024;
  SysDiskSize  diskSize;

  if (tscEmbedded) {
    tdUpdateDiskInfos(tsDnodeTier);
    tsTotalDataDirGB = (float)tsDnodeTier->meta.tsize / unit;
    tsAvailDataDirGB = (float)tsDnodeTier->meta.avail / unit;
  }

  if (taosGetDiskSize(tsLogDir, &diskSize)) {
    tsTotalLogDirGB = (float)diskSize.tsize / unit;
    tsAvailLogDirGB = (float)diskSize.avail / unit;
  }

  if (taosGetDiskSize("/tmp", &diskSize)) {
    tsTotalTmpDirGB = (float)diskSize.tsize / unit;
    tsAvailTmpDirectorySpace = (float)diskSize.avail / unit;
  }
}

static int tdLockTiers() {
  int code = pthread_mutex_lock(&(pTiers->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static int tdUnLockTiers() {
  int code = pthread_mutex_unlock(&(pTiers->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static SDisk *tdGetDisk(int level, int did) {
  if (level < 0 || level >= pTiers->nLevel) return NULL;

  if (did < 0 || did >= pTiers->tiers[level].nDisks) return NULL;

  return pTiers->tiers[level].disks[did];
}