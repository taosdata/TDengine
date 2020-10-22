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

#ifndef TD_TDISK_H
#define TD_TDISK_H

#include "taosdef.h"
#include "hash.h"
#include "hash.h"
#include "taoserror.h"
#include "tglobal.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  int level;
  int did;
} SDiskID;

typedef struct {
  uint64_t size;
  uint64_t free;
  uint64_t nfiles;
} SDiskMeta;

typedef struct {
  uint64_t tsize;
  uint64_t avail; // bytes
} STiersMeta;

typedef struct {
  int       level;
  int       did;
  char      dir[TSDB_FILENAME_LEN];
  SDiskMeta dmeta;
} SDisk;

typedef struct {
  int    level;
  int    nDisks;
  SDisk *disks[TSDB_MAX_DISKS_PER_TIER];
} STier;

typedef struct SDnodeTier {
  pthread_mutex_t lock;
  STiersMeta      meta;
  int             nTiers;
  STier           tiers[TSDB_MAX_TIERS];
  SHashObj *      map;
} SDnodeTier;

extern struct SDnodeTier *tsDnodeTier;
#define DNODE_PRIMARY_DISK(pDnodeTier) (pDnodeTier)->tiers[0].disks[0]

static FORCE_INLINE int tdLockTiers(SDnodeTier *pDnodeTier) {
  int code = pthread_mutex_lock(&(pDnodeTier->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int tdUnLockTiers(SDnodeTier *pDnodeTier) {
  int code = pthread_mutex_unlock(&(pDnodeTier->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE SDisk *tdGetDisk(SDnodeTier *pDnodeTier, int level, int did) {
  if (level < 0 || level >= pDnodeTier->nTiers) return NULL;

  if (did < 0 || did >= pDnodeTier->tiers[level].nDisks) return NULL;

  return pDnodeTier->tiers[level].disks[did];
}

SDnodeTier *tdNewTier();
void *      tdCloseTier(SDnodeTier *pDnodeTier);
int         tdAddDisks(SDnodeTier *pDnodeTier, SDiskCfg *pDiskCfgs, int ndisks);
int         tdUpdateTiersInfo(SDnodeTier *pDnodeTier);
int         tdCheckTiers(SDnodeTier *pDnodeTier);
SDisk *     tdAssignDisk(SDnodeTier *pDnodeTier, int level);
SDisk *     tdGetDiskByName(SDnodeTier *pDnodeTier, char *dirName);
void        tdIncDiskFiles(SDnodeTier *pDnodeTier, SDisk *pDisk, bool lock);
void        tdDecDiskFiles(SDnodeTier *pDnodeTier, SDisk *pDisk, bool lock);

#ifdef __cplusplus
}
#endif

#endif
