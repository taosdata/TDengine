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

#include "hash.h"
#include "taoserror.h"
#include "tfs.h"
#include "tglobal.h"
#include "ttier.h"

#define TSDB_MAX_TIER 3

typedef struct {
  uint64_t tsize;
  uint64_t avail;
} SFSMeta;

typedef struct {
  pthread_mutex_t lock;
  SFSMeta         meta;
  int             nlevel;
  STier           tiers[TSDB_MAX_TIER];
  SHashObj *      map;  // name to did map
} SFS;

static SFS  tdFileSystem = {0};
static SFS *pfs = &tdFileSystem;

#define TIER_AT(level) (pfs->tiers + (level))
#define DISK_AT(level, id) DISK_AT_TIER(TIER_AT(level), id)

int tfsInit(SDiskCfg *pDiskCfg, int ndisk) {
  ASSERT(ndisk > 0);

  for (int level = 0; level < TSDB_MAX_TIER; level++) {
    tdInitTier(TIER_AT(level), level);
  }

  int ret = pthread_mutex_init(&(pfs->lock), NULL);
  if (ret != 0) {
    terrno = TAOS_SYSTEM_ERROR(ret);
    return -1;
  }

  pfs->map = taosHashInit(TSDB_MAX_TIER * TSDB_MAX_DISKS_PER_TIER * 2,
                          taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (pfs->map == NULL) {
    terrno = TSDB_CODE_FS_OUT_OF_MEMORY;
    tfsDestroy();
    return -1;
  }

  for (int idisk = 0; idisk < ndisk; idisk++) {
    if (tfsMount(pDiskCfg + idisk) < 0) {
      tfsDestroy();
      return -1;
    }
  }

  if (tfsCheck() < 0) {
    tfsDestroy();
    return -1;
  }

  return 0;
}

void tfsDestroy() {
  taosHashCleanup(pfs->map);
  pfs->map = NULL;
  
  pthread_mutex_destroy(&(pfs->lock));
  for (int level = 0; level < TSDB_MAX_TIER; level++) {
    tdDestroyTier(TIER_AT(level));
  }
}

int tfsUpdateInfo() {
  tfsLock();

  for (int level = 0; level < pfs->nlevel; level++) {
    if (tdUpdateTierInfo(TIER_AT(level)) < 0) {
      // TODO: deal with the error here
    }
  }

  tfsUnLock();
}

void tfsPrimaryPath(char *dst) {
  strncpy(dst, DISK_AT(0, 0)->dir, TSDB_FILENAME_LEN);
}

int tfsCreateDir(char *name) {
  char dirName[TSDB_FILENAME_LEN] = "\0";

  for (int level = 0; level < pfs->nlevel; level++) {
    STier *pTier = TIER_AT(level);
    for (int id = 0; id < pTier->ndisk; id++) {
      SDisk *pDisk = DISK_AT_TIER(pTier, id);

      ASSERT(pDisk != NULL);

      snprintf(dirName, TSDB_FILENAME_LEN, "%s/%s", pDisk->dir, name);

      if (mkdir(dirName, 0755) != 0 && errno != EEXIST) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        return -1;
      }
    }
  }

  return 0;
}

static int tfsMount(SDiskCfg *pCfg) {
  SDiskID did;

  if (tfsCheckAndFormatCfg(pCfg) < 0) return -1;

  did.level = pCfg->level;
  did.id = tdAddDiskToTier(TIER_AT(pCfg->level), pCfg);
  if (did.id < 0) {
    fError("failed to add disk %s to FS since %s", pCfg->dir, tstrerror(terrno));
    return -1;
  }

  taosHashPut(pTiers->map, pCfg->dir, strnlen(pCfg->dir, TSDB_FILENAME_LEN), (void *)(&did), sizeof(did));
  if (pfs->nlevel < pCfg->level + 1) pfs->nlevel = pCfg->level + 1;

  // TODO: update meta info

  return 0;
}

static int tfsCheckAndFormatCfg(SDiskCfg *pCfg) {
  char        dirName[TSDB_FILENAME_LEN] = "\0";
  struct stat pstat;

  if (pCfg->level < 0 || pCfg->level >= TSDB_MAX_TIER) {
    fError("failed to add disk %s to FS since invalid level %d", pCfg->dir, pCfg->level);
    terrno = TSDB_CODE_FS_INVLD_CFG;
    return -1;
  }

  if (pCfg->primary) {
    if (pCfg->level != 0) {
      fError("failed to add disk %s to FS since disk is primary but level %d not 0", pCfg->dir, pCfg->level);
      terrno = TSDB_CODE_FS_INVLD_CFG;
      return -1;
    }

    if (DISK_AT(0, 0) != NULL) {
      fError("failed to add disk %s to FS since duplicate primary mount", pCfg->dir, pCfg->level);
      terrno = TSDB_CODE_FS_DUP_PRIMARY;
      return -1;
    }
  }

  if (tfsFormatDir(pCfg->dir, dirName) < 0) {
    fError("failed to add disk %s to FS since invalid dir format", pCfg->dir);
    terrno = TSDB_CODE_FS_INVLD_CFG;
    return -1;
  }

  if (tfsGetDiskByName(dirName) != NULL) {
    fError("failed to add disk %s to FS since duplicate mount", pCfg->dir);
    terrno = TSDB_CODE_FS_INVLD_CFG;
    return -1;
  }

  if (access(dirName, W_OK | R_OK | F_OK) != 0) {
    fError("failed to add disk %s to FS since no R/W access rights", pCfg->dir);
    terrno = TSDB_CODE_FS_INVLD_CFG;
    return -1;
  }

  if (stat(dirName, &pstat) < 0) {
    fError("failed to add disk %s to FS since %s", pCfg->dir, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (!S_ISDIR(pstat.st_mode)) {
    fError("failed to add disk %s to FS since not a directory", pCfg->dir);
    terrno = TSDB_CODE_FS_INVLD_CFG;
    return -1;
  }

  strncpy(pCfg->dir, dirName, TSDB_FILENAME_LEN);

  return 0;
}

static int tfsFormatDir(char *idir, char *odir) {
  wordexp_t wep = {0};

  int code = wordexp(idir, &wep, 0);
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }

  if (realpath(wep.we_wordv[0], odir) == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wordfree(&wep);
    return -1;
  }

  wordfree(&wep);
  return 0;

}

static int tfsCheck() {
  if (DISK_AT(0, 0) == NULL) {
    fError("no primary disk is set");
    terrno = TSDB_CODE_FS_NO_PRIMARY_DISK;
    return -1;
  }

  int level = 0;
  do {
    if (TIER_AT(level)->ndisk == 0) {
      fError("no disk at level %d", level);
      terrno = TSDB_CODE_FS_NO_MOUNT_AT_TIER;
      return -1;
    }
  } while (level < pfs->nlevel);

  return 0;
}

static int tfsLock() {
  int code = pthread_mutex_lock(&(pfs->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }

  return 0;
}

static tfsUnLock() {
  int code = pthread_mutex_unlock(&(pfs->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }

  return 0;
}

static tfsGetDiskByName(char *dirName) {
}

static SDisk *tfsGetDiskByID(SDiskID did) { return DISK_AT(did.level, did.id); }