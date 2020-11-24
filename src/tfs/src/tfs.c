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
#include "hash.h"
#include "taoserror.h"
#include "tfs.h"
#include "tfsint.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-truncation"

typedef struct {
  int64_t tsize;
  int64_t avail;
} SFSMeta;

typedef struct {
  pthread_mutex_t lock;
  bool            locked;
  SFSMeta         meta;
  int             nlevel;
  STier           tiers[TSDB_MAX_TIERS];
  SHashObj *      map;  // name to did map
} SFS;

typedef struct {
  SDisk *pDisk;
} SDiskIter;

#define TFS_LOCKED() (pfs->locked)
#define TFS_META() (pfs->meta)
#define TFS_NLEVEL() (pfs->nlevel)
#define TFS_TIERS() (pfs->tiers)

#define TFS_TIER_AT(level) (TFS_TIERS() + (level))
#define TFS_DISK_AT(level, id) DISK_AT_TIER(TFS_TIER_AT(level), id)
#define TFS_PRIMARY_DISK() TFS_DISK_AT(TFS_PRIMARY_LEVEL, TFS_PRIMARY_ID)
#define TFS_IS_VALID_LEVEL(level) (((level) >= 0) && ((level) < TFS_NLEVEL()))
#define TFS_IS_VALID_ID(level, id) (((id) >= 0) && ((id) < TIER_NDISKS(TFS_TIER_AT(level))))
#define TFS_IS_VALID_DISK(level, id) (TFS_IS_VALID_LEVEL(level) && TFS_IS_VALID_ID(level, id))

#define TFS_MIN_DISK_FREE_SIZE 50*1024*1024

static SFS  tfs = {0};
static SFS *pfs = &tfs;

// STATIC DECLARATION
static int    tfsMount(SDiskCfg *pCfg);
static int    tfsCheck();
static int    tfsCheckAndFormatCfg(SDiskCfg *pCfg);
static int    tfsFormatDir(char *idir, char *odir);
static SDisk *tfsGetDiskByID(SDiskID did);
static SDisk *tfsGetDiskByName(const char *dir);
static int    tfsLock();
static int    tfsUnLock();
static int    tfsOpendirImpl(TDIR *tdir);
static void   tfsInitDiskIter(SDiskIter *pIter);
static SDisk *tfsNextDisk(SDiskIter *pIter);
static int    tfsAssignDisk(int level);

// FS APIs ====================================
int tfsInit(SDiskCfg *pDiskCfg, int ndisk) {
  ASSERT(ndisk > 0);

  for (int level = 0; level < TSDB_MAX_TIERS; level++) {
    tfsInitTier(TFS_TIER_AT(level), level);
  }

  int ret = pthread_mutex_init(&(pfs->lock), NULL);
  if (ret != 0) {
    terrno = TAOS_SYSTEM_ERROR(ret);
    return -1;
  }

  pfs->map = taosHashInit(TSDB_MAX_TIERS * TSDB_MAX_DISKS_PER_TIER * 2,
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

  tfsUpdateInfo();

  return 0;
}

void tfsDestroy() {
  taosHashCleanup(pfs->map);
  pfs->map = NULL;
  
  pthread_mutex_destroy(&(pfs->lock));
  for (int level = 0; level < TFS_NLEVEL(); level++) {
    tfsDestroyTier(TFS_TIER_AT(level));
  }
}

void tfsUpdateInfo() {
  SFSMeta tmeta = {0};

  tfsLock();

  for (int level = 0; level < TFS_NLEVEL(); level++) {
    STier *pTier = TFS_TIER_AT(level);
    tfsUpdateTierInfo(pTier);
    tmeta.tsize += TIER_SIZE(pTier);
    tmeta.avail += TIER_FREE_SIZE(pTier);
  }

  pfs->meta = tmeta;

  tfsUnLock();
}

void tfsIncDiskFile(int level, int id, int num) {
  tfsLock();
  TFS_DISK_AT(level, id)->dmeta.nfiles += num;
  tfsUnLock();
}

void tfsDecDiskFile(int level, int id, int num) {
  tfsLock();
  TFS_DISK_AT(level, id)->dmeta.nfiles -= num;
  ASSERT(TFS_DISK_AT(level, id)->dmeta.nfiles >= 0);
  tfsUnLock();
}

const char *TFS_PRIMARY_PATH() { return DISK_DIR(TFS_PRIMARY_DISK()); }
const char *TFS_DISK_PATH(int level, int id) { return DISK_DIR(TFS_DISK_AT(level, id)); }

// TFILE APIs ====================================
void tfsInitFile(TFILE *pf, int level, int id, const char *bname) {
  SDisk *pDisk = TFS_DISK_AT(level, id);

  pf->level = level;
  pf->id = id;
  strncpy(pf->rname, bname, TSDB_FILENAME_LEN);
  snprintf(pf->aname, TSDB_FILENAME_LEN, "%s/%s", DISK_DIR(pDisk), pf->rname);
}

int tfsopen(TFILE *pf, int flags) {
  int fd = -1;

  if (flags & O_CREAT) {
    if (pf->level > TFS_NLEVEL()) {
      pf->level = TFS_NLEVEL();
    }

    if (pf->id == TFS_UNDECIDED_ID) {
      pf->id = tfsAssignDisk(pf->level);
      if (pf->id < 0) {
        fError("failed to assign disk at level %d", pf->level);
        return -1;
      }
    }

    tfsIncDiskFile(pf->level, pf->id, 1);
  }

  fd = open(pf->aname, flags);
  if (fd < 0) {
    fError("failed to open file %s since %s", pf->aname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return fd;
}

int tfsclose(int fd) {
  int code = close(fd);
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

int tfsremove(TFILE *pf) { 
  int code = remove(pf->aname);
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  tfsDecDiskFile(pf->level, pf->id, 1);
  return 0; 
}

// DIR APIs ====================================
int tfsMkdir(const char *rname) {
  char aname[TSDB_FILENAME_LEN] = "\0";

  for (int level = 0; level < TFS_NLEVEL(); level++) {
    STier *pTier = TFS_TIER_AT(level);
    for (int id = 0; id < TIER_NDISKS(pTier); id++) {
      SDisk *pDisk = DISK_AT_TIER(pTier, id);
      snprintf(aname, TSDB_FILENAME_LEN, "%s/%s", DISK_DIR(pDisk), rname);

      if (mkdir(aname, 0755) != 0 && errno != EEXIST) {
        fError("failed to create directory %s since %s", aname, strerror(errno));
        terrno = TAOS_SYSTEM_ERROR(errno);
        return -1;
      }
    }
  }

  return 0;
}

int tfsRmdir(const char *rname) {
  char aname[TSDB_FILENAME_LEN] = "\0";

  for (int level = 0; level < TFS_NLEVEL(); level++) {
    STier *pTier = TFS_TIER_AT(level);
    for (int id = 0; id < TIER_NDISKS(pTier); id++) {
      SDisk *pDisk = DISK_AT_TIER(pTier, id);

      snprintf(aname, TSDB_FILENAME_LEN, "%s/%s", DISK_DIR(pDisk), rname);

      taosRemoveDir(aname);
    }
  }

  return 0;
}

int tfsRename(char *orname, char *nrname) {
  char oaname[TSDB_FILENAME_LEN] = "\0";
  char naname[TSDB_FILENAME_LEN] = "\0";

  for (int level = 0; level < pfs->nlevel; level++) {
    STier *pTier = TFS_TIER_AT(level);
    for (int id = 0; id < TIER_NDISKS(pTier); id++) {
      SDisk *pDisk = DISK_AT_TIER(pTier, id);

      snprintf(oaname, TSDB_FILENAME_LEN, "%s/%s", DISK_DIR(pDisk), orname);
      snprintf(naname, TSDB_FILENAME_LEN, "%s/%s", DISK_DIR(pDisk), nrname);

      taosRename(oaname, naname);
    }
  }

  return 0;
}

struct TDIR {
  SDiskIter iter;
  int       level;
  int       id;
  char      dirname[TSDB_FILENAME_LEN];
  TFILE     tfile;
  DIR *     dir;
};

TDIR *tfsOpendir(const char *rname) {
  TDIR *tdir = (TDIR *)calloc(1, sizeof(*tdir));
  if (tdir == NULL) {
    terrno = TSDB_CODE_FS_OUT_OF_MEMORY;
    return NULL;
  }

  tfsInitDiskIter(&(tdir->iter));
  strncpy(tdir->dirname, rname, TSDB_FILENAME_LEN);

  if (tfsOpendirImpl(tdir) < 0) {
    free(tdir);
    return NULL;
  }

  return tdir;
}

const TFILE *tfsReaddir(TDIR *tdir) {
  if (tdir == NULL || tdir->dir == NULL) return NULL;
  char bname[TSDB_FILENAME_LEN] = "\0";

  while (true) {
    struct dirent *dp = NULL;
    dp = readdir(tdir->dir);
    if (dp != NULL) {
      snprintf(bname, TSDB_FILENAME_LEN, "%s/%s", tdir->dirname, dp->d_name);
      tfsInitFile(&(tdir->tfile), tdir->level, tdir->id, bname);
      return &(tdir->tfile);
    }

    if (tfsOpendirImpl(tdir) < 0) {
      return NULL;
    }

    if (tdir->dir == NULL) {
      terrno = TSDB_CODE_SUCCESS;
      return NULL;
    }
  }
}

void tfsClosedir(TDIR *tdir) {
  if (tdir) {
    if (tdir->dir != NULL) {
      closedir(tdir->dir);
      tdir->dir = NULL;
    }
    free(tdir);
  }
}

// Static functions
static int tfsLock() {
  int code = pthread_mutex_lock(&(pfs->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }

  pfs->locked = true;

  return 0;
}

static int tfsUnLock() {
  pfs->locked = false;

  int code = pthread_mutex_unlock(&(pfs->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }

  return 0;
}

// private
static int tfsMount(SDiskCfg *pCfg) {
  SDiskID did;
  SDisk * pDisk = NULL;

  if (tfsCheckAndFormatCfg(pCfg) < 0) return -1;

  did.level = pCfg->level;
  pDisk = tfsMountDiskToTier(TFS_TIER_AT(did.level), pCfg);
  if (pDisk == NULL) {
    fError("failed to mount disk %s to level %d since %s", pCfg->dir, pCfg->level, strerror(terrno));
    return -1;
  }
  did.id = DISK_ID(pDisk);

  taosHashPut(pfs->map, (void *)(pCfg->dir), strnlen(pCfg->dir, TSDB_FILENAME_LEN), (void *)(&did), sizeof(did));
  if (pfs->nlevel < pCfg->level + 1) pfs->nlevel = pCfg->level + 1;

  return 0;
}

static int tfsCheckAndFormatCfg(SDiskCfg *pCfg) {
  char        dirName[TSDB_FILENAME_LEN] = "\0";
  struct stat pstat;

  if (pCfg->level < 0 || pCfg->level >= TSDB_MAX_TIERS) {
    fError("failed to mount %s to FS since invalid level %d", pCfg->dir, pCfg->level);
    terrno = TSDB_CODE_FS_INVLD_CFG;
    return -1;
  }

  if (pCfg->primary) {
    if (pCfg->level != 0) {
      fError("failed to mount %s to FS since disk is primary but level %d not 0", pCfg->dir, pCfg->level);
      terrno = TSDB_CODE_FS_INVLD_CFG;
      return -1;
    }

    if (TFS_PRIMARY_DISK() != NULL) {
      fError("failed to mount %s to FS since duplicate primary mount", pCfg->dir);
      terrno = TSDB_CODE_FS_DUP_PRIMARY;
      return -1;
    }
  }

  if (tfsFormatDir(pCfg->dir, dirName) < 0) {
    fError("failed to mount %s to FS since invalid dir format", pCfg->dir);
    terrno = TSDB_CODE_FS_INVLD_CFG;
    return -1;
  }

  if (tfsGetDiskByName(dirName) != NULL) {
    fError("failed to mount %s to FS since duplicate mount", pCfg->dir);
    terrno = TSDB_CODE_FS_INVLD_CFG;
    return -1;
  }

  if (access(dirName, W_OK | R_OK | F_OK) != 0) {
    fError("failed to mount %s to FS since no R/W access rights", pCfg->dir);
    terrno = TSDB_CODE_FS_INVLD_CFG;
    return -1;
  }

  if (stat(dirName, &pstat) < 0) {
    fError("failed to mount %s to FS since %s", pCfg->dir, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (!S_ISDIR(pstat.st_mode)) {
    fError("failed to mount %s to FS since not a directory", pCfg->dir);
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
  if (TFS_PRIMARY_DISK() == NULL) {
    fError("no primary disk is set");
    terrno = TSDB_CODE_FS_NO_PRIMARY_DISK;
    return -1;
  }

  for (int level = 0; level < TFS_NLEVEL(); level++) {
    if (TIER_NDISKS(TFS_TIER_AT(level)) == 0) {
      fError("no disk at level %d", level);
      terrno = TSDB_CODE_FS_NO_MOUNT_AT_TIER;
      return -1;
    }
  }

  return 0;
}

static SDisk *tfsGetDiskByID(SDiskID did) { return TFS_DISK_AT(did.level, did.id); }
static SDisk *tfsGetDiskByName(const char *dir) {
  SDiskID did;
  SDisk * pDisk = NULL;
  void *  pr = NULL;

  pr = taosHashGet(pfs->map, (void *)dir, strnlen(dir, TSDB_FILENAME_LEN));
  if (pr == NULL) return NULL;

  did = *(SDiskID *)pr;
  pDisk = tfsGetDiskByID(did);
  ASSERT(pDisk != NULL);

  return pDisk;
}

static int tfsOpendirImpl(TDIR *tdir) {
  SDisk *pDisk = NULL;
  char   adir[TSDB_FILENAME_LEN] = "\0";

  if (tdir->dir != NULL) {
    closedir(tdir->dir);
    tdir->dir = NULL;
  }

  while (true) {
    pDisk = tfsNextDisk(&(tdir->iter));
    if (pDisk == NULL) return 0;

    tdir->level = DISK_LEVEL(pDisk);
    tdir->id = DISK_ID(pDisk);

    snprintf(adir, TSDB_FILENAME_LEN, "%s/%s", DISK_DIR(pDisk), tdir->dirname);
    tdir->dir = opendir(adir);
    if (tdir->dir != NULL) break;
  }

  return 0;
}

static void tfsInitDiskIter(SDiskIter *pIter) { pIter->pDisk = TFS_DISK_AT(0, 0); }

static SDisk *tfsNextDisk(SDiskIter *pIter) {
  SDisk *pDisk = pIter->pDisk;

  if (pDisk == NULL) return NULL;

  int level = DISK_LEVEL(pDisk);
  int id = DISK_ID(pDisk);

  id++;
  if (id < TIER_NDISKS(TFS_TIER_AT(level))) {
    pIter->pDisk = TFS_DISK_AT(level, id);
    ASSERT(pIter->pDisk != NULL);
  } else {
    level++;
    id = 0;
    if (level < TFS_NLEVEL()) {
      pIter->pDisk = TFS_DISK_AT(level, id);
      ASSERT(pIter->pDisk != NULL);
    } else {
      pIter->pDisk = NULL;
    }
  }

  return pDisk;
}

static int tfsAssignDisk(int level) {
  if (!TFS_IS_VALID_LEVEL(level)) return -1;

  STier *pTier = TFS_TIER_AT(level);
  int    id = -1;

  tfsLock();

  for (int tid = 0; tid < TIER_NDISKS(pTier); tid++) {
    SDisk *pDisk = DISK_AT_TIER(pTier, tid);

    if (DISK_FREE_SIZE(pDisk) < TFS_MIN_DISK_FREE_SIZE) continue;

    if (id == -1) {
      id = tid;
      continue;
    }

    if (DISK_NFILES(DISK_AT_TIER(pTier, id)) > DISK_NFILES(DISK_AT_TIER(pTier, tid))) {
      id = tid;
    }
  }

  tfsUnLock();

  return id;
}

#pragma GCC diagnostic pop