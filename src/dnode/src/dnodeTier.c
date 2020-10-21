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

#include "dnode.h"
#include "dnodeInt.h"
#include "taosdef.h"

#define DISK_MIN_FREE_SPACE 30 * 1024 * 1024  // disk free space less than 100M will not create new file again
#define DNODE_DISK_AVAIL(pDisk) ((pDisk)->dmeta.free > DISK_MIN_FREE_SPACE)

static int dnodeFormatDir(char *idir, char *odir);
static int dnodeCheckDisk(char *dirName, int level, int primary);
static int dnodeUpdateDiskMeta(SDisk *pDisk);
static int dnodeAddDisk(SDnodeTier *pDnodeTier, char *dir, int level, int primary);

SDnodeTier *dnodeNewTier() {
  SDnodeTier *pDnodeTier = (SDnodeTier *)calloc(1, sizeof(*pDnodeTier));
  if (pDnodeTier == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return NULL;
  }

  int ret = pthread_rwlock_init(&(pDnodeTier->rwlock), NULL);
  if (ret != 0) {
    terrno = TAOS_SYSTEM_ERROR(ret);
    dnodeCloseTier(pDnodeTier);
    return NULL;
  }

  pDnodeTier->map = taosHashInit(DNODE_MAX_TIERS * DNODE_MAX_DISKS_PER_TIER * 2,
                                 taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (pDnodeTier->map == NULL) {
    terrno = TSDB_CODE_COM_OUT_OF_MEMORY;
    dnodeCloseTier(pDnodeTier);
    return NULL;
  }

  return pDnodeTier;
}

void *dnodeCloseTier(SDnodeTier *pDnodeTier) {
  if (pDnodeTier) {
    if (pDnodeTier->map) {
      taosHashCleanup(pDnodeTier->map);
      pDnodeTier->map = NULL;
    }

    pthread_rwlock_destroy(&(pDnodeTier->rwlock));

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

int dnodeAddDisks(SDnodeTier *pDnodeTier, SDiskCfg *pDiskCfgs, int ndisks) {
  ASSERT(ndisks > 0);

  for (int i = 0; i < ndisks; i++) {
    SDiskCfg *pCfg = pDiskCfgs + i;
    dnodeAddDisk(pDnodeTier, pCfg->dir, pCfg->level, pCfg->primary);
  }

  if (dnodeCheckTiers(pDnodeTier) < 0) return -1;
  
  return 0;
}

int dnodeUpdateTiersInfo(SDnodeTier *pDnodeTier) {
  for (int i = 0; i < pDnodeTier->nTiers; i++) {
    STier *pTier = pDnodeTier->tiers + i;

    for (int j = 0; j < pTier->nDisks; j++) {
      SDisk *pDisk = pTier->disks[j];
      if (dnodeUpdateDiskMeta(pDisk) < 0) return -1;
    }
  }
  return 0;
}

int dnodeCheckTiers(SDnodeTier *pDnodeTier) {
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

SDisk *dnodeAssignDisk(SDnodeTier *pDnodeTier, int level) {
  ASSERT(level < pDnodeTier->nTiers);

  STier *pTier = pDnodeTier->tiers + level;
  SDisk *pDisk = NULL;

  ASSERT(pTier->nDisks > 0);

  for (int i = 0; i < pTier->nDisks; i++) {
    SDisk *iDisk = pTier->disks[i];
    if (dnodeUpdateDiskMeta(iDisk) < 0) return NULL;
    if (DNODE_DISK_AVAIL(iDisk)) {
      if (pDisk == NULL || pDisk->dmeta.nfiles > iDisk->dmeta.nfiles) {
        pDisk = iDisk;
      }
    }
  }

  if (pDisk == NULL) {
    terrno = TSDB_CODE_DND_NO_DISK_SPACE;
  }

  return NULL;
}

SDisk *dnodeGetDiskByName(SDnodeTier *pDnodeTier, char *dirName) {
  char     fdirName[TSDB_FILENAME_LEN] = "\0";
  SDiskID *pDiskID = NULL;

  if (dnodeFormatDir(dirName, fdirName) < 0) {
    return NULL;
  }

  void *ptr = taosHashGet(pDnodeTier->map, (void *)fdirName, strnlen(fdirName, TSDB_FILENAME_LEN));
  if (ptr == NULL) return NULL;
  pDiskID = (SDiskID *)ptr;

  return dnodeGetDisk(pDnodeTier, pDiskID->level, pDiskID->did);
}

static int dnodeFormatDir(char *idir, char *odir) {
  wordexp_t wep;

  int code = wordexp(idir, &wep, 0);
  if (code != 0) {
    dError("failed to format dir %s since %s", idir, strerror(code));
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }

  if (realpath(wep.we_wordv[0], odir) == NULL) {
    dError("failed to format dir %s since %s", idir, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    wordfree(&wep);
    return -1;
  }

  wordfree(&wep);
  return 0;
}

static int dnodeCheckDisk(char *dirName, int level, int primary) {
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

static int dnodeUpdateDiskMeta(SDisk *pDisk) {
  struct statvfs dstat;
  if (statvfs(pDisk->dir, &dstat) < 0) {
    dError("failed to get dir %s information since %s", pDisk->dir, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  pDisk->dmeta.size = dstat.f_bsize * dstat.f_blocks;
  pDisk->dmeta.free = dstat.f_bsize * dstat.f_bavail;

  return 0;
}

static int dnodeAddDisk(SDnodeTier *pDnodeTier, char *dir, int level, int primary) {
  char    dirName[TSDB_FILENAME_LEN] = "\0";
  STier * pTier = NULL;
  SDiskID diskid = {0};
  SDisk * pDisk = NULL;

  if (level < 0 || level >= DNODE_MAX_TIERS) {
    terrno = TSDB_CODE_DND_INVALID_DISK_TIER;
    dError("failed to add disk %s to tier %d level since %s", dir, level, tstrerror(terrno));
    return -1;
  }

  if (dnodeFormatDir(dir, dirName) < 0) {
    dError("failed to add disk %s to tier %d level since %s", dir, level, tstrerror(terrno));
    return -1;
  }

  pTier = pDnodeTier->tiers + level;
  diskid.level = level;

  if (pTier->nDisks >= DNODE_MAX_DISKS_PER_TIER) {
    terrno = TSDB_CODE_DND_TOO_MANY_DISKS;
    dError("failed to add disk %s to tier %d level since %s", dir, level, tstrerror(terrno));
    return -1;
  }

  if (dnodeGetDiskByName(pDnodeTier, dirName) != NULL) {
    terrno = TSDB_CODE_DND_DISK_ALREADY_EXISTS;
    dError("failed to add disk %s to tier %d level since %s", dir, level, tstrerror(terrno));
    return -1;
  }

  if (dnodeCheckDisk(dirName, level, primary) < 0) {
    dError("failed to add disk %s to tier %d level since %s", dir, level, tstrerror(terrno));
    return -1;
  }

  if (primary) {
    if (level != 0) {
      terrno = TSDB_CODE_DND_INVALID_DISK_TIER;
      dError("failed to add disk %s to tier %d level since %s", dir, level, tstrerror(terrno));
      return -1;
    }

    if (DNODE_PRIMARY_DISK(pDnodeTier) != NULL) {
      terrno = TSDB_CODE_DND_DUPLICATE_PRIMARY_DISK;
      dError("failed to add disk %s to tier %d level since %s", dir, level, tstrerror(terrno));
      return -1;
    }

    diskid.did = 0;
  } else {
    if (level == 0) {
      if (DNODE_PRIMARY_DISK(pDnodeTier) != NULL) {
        diskid.did = pTier->nDisks;
      } else {
        diskid.did = pTier->nDisks + 1;
        if (diskid.did >= DNODE_MAX_DISKS_PER_TIER) {
          terrno = TSDB_CODE_DND_TOO_MANY_DISKS;
          dError("failed to add disk %s to tier %d level since %s", dir, level, tstrerror(terrno));
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
    dError("failed to add disk %s to tier %d level since %s", dir, level, tstrerror(terrno));
    return -1;
  }

  strncpy(pDisk->dir, dirName, TSDB_FILENAME_LEN);

  if (taosHashPut(pDnodeTier->map, (void *)dirName, strnlen(dirName, TSDB_FILENAME_LEN), (void *)(&diskid),
                  sizeof(diskid)) < 0) {
    free(pDisk);
    terrno = TSDB_CODE_DND_OUT_OF_MEMORY;
    dError("failed to add disk %s to tier %d level since %s", dir, level, tstrerror(terrno));
    return -1;
  }

  pTier->nDisks++;
  pTier->disks[diskid.did] = pDisk;
  pDnodeTier->nTiers = MAX(pDnodeTier->nTiers, level);

  return 0;
}