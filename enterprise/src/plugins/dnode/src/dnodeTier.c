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
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/statvfs.h>
#include <wordexp.h>

#include "dnodeInt.h"
#include "hash.h"
#include "os.h"
#include "taosdef.h"
#include "taoserror.h"

#define DNODE_MAX_TIERS 3
#define DNODE_MAX_DISKS_PER_TIER 16

typedef struct {
  uint64_t size;
  uint64_t free;
  uint64_t nfiles;
} SDiskMeta;

typedef struct {
  char      dir[TSDB_FILENAME_LEN];
  SDiskMeta dmeta;
} SDisk;

typedef struct {
  int   level;
  int   nDisks;
  SDisk disks[DNODE_MAX_DISKS_PER_TIER];
} STier;

typedef struct {
  pthread_rwlock_t rwlock;
  int              nTiers;
  STier            tiers[DNODE_MAX_TIERS];
  SHashObj *       map;
} SDnodeTier;

static FORCE_INLINE int dnodeRLockTiers(SDnodeTier *pDnodeTier) {
  int code = pthread_rwlock_rdlock(&(pDnodeTier->rwlock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int dnodeWLockTiers(SDnodeTier *pDnodeTier) {
  int code = pthread_rwlock_wrlock(&(pDnodeTier->rwlock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int dnodeUnLockTiers(SDnodeTier *pDnodeTier) {
  int code = pthread_rwlock_unlock(&(pDnodeTier->rwlock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE SDisk *dnodeGetDisk(SDnodeTier *pDnodeTier, int level, int did) {
  if (level < 0 || level >= pDnodeTier->nTiers) return NULL;

  if (did < 0 || did >= pDnodeTier->tiers[level].nDisks) return NULL;

  return &(pDnodeTier->tiers[level].disks[did]);
}

static FORCE_INLINE SDisk *dnodeGetDiskByName(SDnodeTier *pDnodeTier, char *dirName) {
  // TODO
  return NULL;
}

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

  // TODO
  pDnodeTier->map = taosHashInit();
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
    free(pDnodeTier);
  }
  return NULL;
}

int dnodeAddDisk(SDnodeTier *pDnodeTier, char *dir, int level) {
  char   dirName[TSDB_FILENAME_LEN] = "\0";
  STier *pTier = NULL;

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

  if (dnodeCheckDisk(dirName) < 0) {
    dError("failed to add disk %s to tier %d level since %s", dir, level, tstrerror(terrno));
    return -1;
  }

  strncpy(pTier->disks[pTier->nDisks++].dir, dirName, TSDB_FILENAME_LEN);

  // TODO
  // taosHashPut();

  return 0;
}

int dnodeUpdateTiersInfo(SDnodeTier *pDnodeTier) {
  for (int i == 0; i < pDnodeTier->nTiers; i++) {
    STier *pTier = pDnodeTier->tiers + i;

    for (int j = 0; j < pTier->nDisks; j++) {
      SDisk *pDisk = pTier->disks + j;
      if (dnodeUpdateDiskMeta(pDisk) < 0) return -1;
    }
  }
  return 0;
}

int dnodeCheckTiers(SDnodeTier *pDnodeTier) {
  // TODO
  return 0;
}

SDisk *dnodeAssignDisk(SDnodeTier *pDnodeTier) {
  // TODO
  return NULL;
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

static int dnodeCheckDisk(char *dirName) {
  if (access)
  return 0;
}