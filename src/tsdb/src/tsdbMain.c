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

// no test file errors here
#include "tsdbMain.h"
#include "os.h"
#include "talgo.h"
#include "taosdef.h"
#include "tchecksum.h"
#include "tscompression.h"
#include "tsdb.h"
#include "tulog.h"

#define TSDB_CFG_FILE_NAME "config"
#define TSDB_DATA_DIR_NAME "data"
#define TSDB_META_FILE_NAME "meta"
#define TSDB_META_FILE_INDEX 10000000
#define IS_VALID_PRECISION(precision) \
  (((precision) >= TSDB_TIME_PRECISION_MILLI) && ((precision) <= TSDB_TIME_PRECISION_NANO))
#define TSDB_DEFAULT_COMPRESSION TWO_STAGE_COMP
#define IS_VALID_COMPRESSION(compression) (((compression) >= NO_COMPRESSION) && ((compression) <= TWO_STAGE_COMP))

static int32_t     tsdbCheckAndSetDefaultCfg(STsdbCfg *pCfg);
static int32_t     tsdbSetRepoEnv(char *rootDir, STsdbCfg *pCfg);
static int32_t     tsdbUnsetRepoEnv(char *rootDir);
static int32_t     tsdbSaveConfig(char *rootDir, STsdbCfg *pCfg);
static int         tsdbLoadConfig(char *rootDir, STsdbCfg *pCfg);
static char *      tsdbGetCfgFname(char *rootDir);
static STsdbRepo * tsdbNewRepo(char *rootDir, STsdbAppH *pAppH, STsdbCfg *pCfg);
static void        tsdbFreeRepo(STsdbRepo *pRepo);
static int         tsdbRestoreInfo(STsdbRepo *pRepo);
static void        tsdbAlterCompression(STsdbRepo *pRepo, int8_t compression);
static int         tsdbAlterKeep(STsdbRepo *pRepo, int32_t keep);
static int         tsdbAlterCacheTotalBlocks(STsdbRepo *pRepo, int totalBlocks);
static int         keyFGroupCompFunc(const void *key, const void *fgroup);
static int         tsdbEncodeCfg(void **buf, STsdbCfg *pCfg);
static void *      tsdbDecodeCfg(void *buf, STsdbCfg *pCfg);
static void        tsdbStartStream(STsdbRepo *pRepo);
static void        tsdbStopStream(STsdbRepo *pRepo);

// Function declaration
int32_t tsdbCreateRepo(char *rootDir, STsdbCfg *pCfg) {
  char tsdbDir[TSDB_FILENAME_LEN] = "\0";

  snprintf(tsdbDir, TSDB_FILENAME_LEN, "%s/%s", TFS_PRIMARY_PATH(), rootDir);
  DIR *dir = opendir(tsdbDir);
  if (dir) {
    tsdbDebug("repository %s already exists", rootDir);
    closedir(dir);
    return 0;
  } else {
    if (ENOENT != errno) {
      tsdbError("failed to open directory %s since %s", rootDir, strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }
  }

  if (tsdbCheckAndSetDefaultCfg(pCfg) < 0) return -1;

  if (tsdbSetRepoEnv(rootDir, pCfg) < 0) return -1;

  tsdbDebug(
      "vgId:%d tsdb env create succeed! cacheBlockSize %d totalBlocks %d daysPerFile %d keep "
      "%d minRowsPerFileBlock %d maxRowsPerFileBlock %d precision %d compression %d",
      pCfg->tsdbId, pCfg->cacheBlockSize, pCfg->totalBlocks, pCfg->daysPerFile, pCfg->keep, pCfg->minRowsPerFileBlock,
      pCfg->maxRowsPerFileBlock, pCfg->precision, pCfg->compression);
  return 0;
}

int32_t tsdbDropRepo(char *rootDir) { return tsdbUnsetRepoEnv(rootDir); }

TSDB_REPO_T *tsdbOpenRepo(char *rootDir, STsdbAppH *pAppH) {
  STsdbCfg   config = {0};
  STsdbRepo *pRepo = NULL;

  terrno = TSDB_CODE_SUCCESS;

  if (tsdbLoadConfig(rootDir, &config) < 0) {
    tsdbError("failed to open repo in rootDir %s since %s", rootDir, tstrerror(terrno));
    return NULL;
  }

  pRepo = tsdbNewRepo(rootDir, pAppH, &config);
  if (pRepo == NULL) {
    tsdbError("failed to open repo in rootDir %s since %s", rootDir, tstrerror(terrno));
    return NULL;
  }

  if (tsdbOpenMeta(pRepo) < 0) {
    tsdbError("vgId:%d failed to open meta since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  if (tsdbOpenBufPool(pRepo) < 0) {
    tsdbError("vgId:%d failed to open buffer pool since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  if (tsdbOpenFileH(pRepo) < 0) {
    tsdbError("vgId:%d failed to open file handle since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  if (tsdbRestoreInfo(pRepo) < 0) {
    tsdbError("vgId:%d failed to restore info from file since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  tsdbStartStream(pRepo);

  tsdbDebug("vgId:%d open tsdb repository succeed!", REPO_ID(pRepo));

  return (TSDB_REPO_T *)pRepo;

_err:
  tsdbCloseRepo(pRepo, false);
  return NULL;
}

// Note: all working thread and query thread must stopped when calling this function
int tsdbCloseRepo(TSDB_REPO_T *repo, int toCommit) {
  if (repo == NULL) return 0;

  STsdbRepo *pRepo = (STsdbRepo *)repo;
  int        vgId = REPO_ID(pRepo);

  terrno = TSDB_CODE_SUCCESS;

  tsdbStopStream(pRepo);

  if (toCommit) {
    tsdbAsyncCommit(pRepo);
    sem_wait(&(pRepo->readyToCommit));
    terrno = pRepo->code;
  }
  tsdbUnRefMemTable(pRepo, pRepo->mem);
  tsdbUnRefMemTable(pRepo, pRepo->imem);
  pRepo->mem = NULL;
  pRepo->imem = NULL;

  tsdbCloseFileH(pRepo, !toCommit);
  tsdbCloseBufPool(pRepo);
  tsdbCloseMeta(pRepo);
  tsdbFreeRepo(pRepo);
  tsdbDebug("vgId:%d repository is closed", vgId);

  if (terrno != TSDB_CODE_SUCCESS) {
    return -1;
  } else {
    return 0;
  }
}

uint32_t tsdbGetFileInfo(TSDB_REPO_T *repo, char *name, uint32_t *index, uint32_t eindex, int64_t *size) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;
  // STsdbMeta *pMeta = pRepo->tsdbMeta;
  STsdbFileH *pFileH = pRepo->tsdbFileH;
  uint32_t    magic = 0;
  char *      fname = NULL;

  struct stat fState;

  tsdbDebug("vgId:%d name:%s index:%d eindex:%d", pRepo->config.tsdbId, name, *index, eindex);
  ASSERT(*index <= eindex);

  if (name[0] == 0) {  // get the file from index or after, but not larger than eindex
    int fid = (*index) / TSDB_FILE_TYPE_MAX;

    if (pFileH->nFGroups == 0 || fid > pFileH->pFGroup[pFileH->nFGroups - 1].fileId) {
      if (*index <= TSDB_META_FILE_INDEX && TSDB_META_FILE_INDEX <= eindex) {
        fname = tsdbGetMetaFileName(pRepo->rootDir);
        *index = TSDB_META_FILE_INDEX;
        magic = TSDB_META_FILE_MAGIC(pRepo->tsdbMeta);
        sprintf(name, "tsdb/%s", TSDB_META_FILE_NAME);
      } else {
        return 0;
      }
    } else {
      SFileGroup *pFGroup =
          taosbsearch(&fid, pFileH->pFGroup, pFileH->nFGroups, sizeof(SFileGroup), keyFGroupCompFunc, TD_GE);
      if (pFGroup->fileId == fid) {
        SFile *pFile = &pFGroup->files[(*index) % TSDB_FILE_TYPE_MAX];
        fname = strdup(TSDB_FILE_NAME(pFile));
        magic = pFile->info.magic;
        char *tfname = strdup(fname);
        sprintf(name, "tsdb/%s/%s", TSDB_DATA_DIR_NAME, basename(tfname));
        tfree(tfname);
      } else {
        if ((pFGroup->fileId + 1) * TSDB_FILE_TYPE_MAX - 1 < (int)eindex) {
          SFile *pFile = &pFGroup->files[0];
          fname = strdup(TSDB_FILE_NAME(pFile));
          *index = pFGroup->fileId * TSDB_FILE_TYPE_MAX;
          magic = pFile->info.magic;
          char *tfname = strdup(fname);
          sprintf(name, "tsdb/%s/%s", TSDB_DATA_DIR_NAME, basename(tfname));
          tfree(tfname);
        } else {
          return 0;
        }
      }
    }
  } else {  // get the named file at the specified index. If not there, return 0
    fname = malloc(256);
    sprintf(fname, "%s/vnode/vnode%d/%s", TFS_PRIMARY_PATH(), REPO_ID(pRepo), name);
    if (access(fname, F_OK) != 0) {
      tfree(fname);
      return 0;
    }
    if (*index == TSDB_META_FILE_INDEX) {  // get meta file
      tsdbGetStoreInfo(fname, &magic, size);
    } else {
      char tfname[TSDB_FILENAME_LEN] = "\0";
      sprintf(tfname, "vnode/vnode%d/tsdb/%s/%s", REPO_ID(pRepo), TSDB_DATA_DIR_NAME, basename(name));
      tsdbGetFileInfoImpl(tfname, &magic, size);
    }
    tfree(fname);
    return magic;
  }

  if (stat(fname, &fState) < 0) {
    tfree(fname);
    return 0;
  }

  *size = fState.st_size;
  // magic = *size;

  tfree(fname);
  return magic;
}

STsdbCfg *tsdbGetCfg(const TSDB_REPO_T *repo) {
  ASSERT(repo != NULL);
  return &((STsdbRepo *)repo)->config;
}

int32_t tsdbConfigRepo(TSDB_REPO_T *repo, STsdbCfg *pCfg) {
  // TODO: think about multithread cases
  STsdbRepo *pRepo = (STsdbRepo *)repo;
  STsdbCfg   config = pRepo->config;
  STsdbCfg * pRCfg = &pRepo->config;

  if (tsdbCheckAndSetDefaultCfg(pCfg) < 0) return -1;

  ASSERT(pRCfg->tsdbId == pCfg->tsdbId);
  ASSERT(pRCfg->cacheBlockSize == pCfg->cacheBlockSize);
  ASSERT(pRCfg->daysPerFile == pCfg->daysPerFile);
  ASSERT(pRCfg->minRowsPerFileBlock == pCfg->minRowsPerFileBlock);
  ASSERT(pRCfg->maxRowsPerFileBlock == pCfg->maxRowsPerFileBlock);
  ASSERT(pRCfg->precision == pCfg->precision);

  bool configChanged = false;
  if (pRCfg->compression != pCfg->compression) {
    tsdbAlterCompression(pRepo, pCfg->compression);
    config.compression = pCfg->compression;
    configChanged = true;
  }
  if (pRCfg->keep != pCfg->keep) {
    if (tsdbAlterKeep(pRepo, pCfg->keep) < 0) {
      tsdbError("vgId:%d failed to configure repo when alter keep since %s", REPO_ID(pRepo), tstrerror(terrno));
      config.keep = pCfg->keep;
      return -1;
    }
    configChanged = true;
  }
  if (pRCfg->totalBlocks != pCfg->totalBlocks) {
    tsdbAlterCacheTotalBlocks(pRepo, pCfg->totalBlocks);
    config.totalBlocks = pCfg->totalBlocks;
    configChanged = true;
  }

  if (configChanged) {
    if (tsdbSaveConfig(pRepo->rootDir, &config) < 0) {
      tsdbError("vgId:%d failed to configure repository while save config since %s", REPO_ID(pRepo), tstrerror(terrno));
      return -1;
    }
  }

  return 0;
}

void tsdbReportStat(void *repo, int64_t *totalPoints, int64_t *totalStorage, int64_t *compStorage) {
  ASSERT(repo != NULL);
  STsdbRepo *pRepo = repo;
  *totalPoints = pRepo->stat.pointsWritten;
  *totalStorage = pRepo->stat.totalStorage;
  *compStorage = pRepo->stat.compStorage;
}

int tsdbGetState(TSDB_REPO_T *repo) {
  return ((STsdbRepo *)repo)->state;
}

// ----------------- INTERNAL FUNCTIONS -----------------
char *tsdbGetMetaFileName(char *rootDir) {
  int   tlen = (int)(strlen(TFS_PRIMARY_PATH()) + strlen(rootDir) + strlen(TSDB_META_FILE_NAME) + 3);
  char *fname = calloc(1, tlen);
  if (fname == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  snprintf(fname, tlen, "%s/%s/%s", TFS_PRIMARY_PATH(), rootDir, TSDB_META_FILE_NAME);
  return fname;
}

void tsdbGetDataFileName(char *rootDir, int vid, int fid, int type, char *fname) {
  snprintf(fname, TSDB_FILENAME_LEN, "%s/%s/v%df%d%s", rootDir, TSDB_DATA_DIR_NAME, vid, fid, tsdbFileSuffix[type]);
}

int tsdbLockRepo(STsdbRepo *pRepo) {
  int code = pthread_mutex_lock(&pRepo->mutex);
  if (code != 0) {
    tsdbError("vgId:%d failed to lock tsdb since %s", REPO_ID(pRepo), strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  pRepo->repoLocked = true;
  return 0;
}

int tsdbUnlockRepo(STsdbRepo *pRepo) {
  ASSERT(IS_REPO_LOCKED(pRepo));
  pRepo->repoLocked = false;
  int code = pthread_mutex_unlock(&pRepo->mutex);
  if (code != 0) {
    tsdbError("vgId:%d failed to unlock tsdb since %s", REPO_ID(pRepo), strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

char *tsdbGetDataDirName(char *rootDir) {
  int   tlen = (int)(strlen(rootDir) + strlen(TSDB_DATA_DIR_NAME) + 2);
  char *fname = calloc(1, tlen);
  if (fname == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  snprintf(fname, tlen, "%s/%s", rootDir, TSDB_DATA_DIR_NAME);
  return fname;
}

int tsdbGetNextMaxTables(int tid) {
  ASSERT(tid >= 1 && tid <= TSDB_MAX_TABLES);
  int maxTables = TSDB_INIT_NTABLES;
  while (true) {
    maxTables = MIN(maxTables, TSDB_MAX_TABLES);
    if (tid <= maxTables) break;
    maxTables *= 2;
  }

  return maxTables + 1;
}

int tsdbCheckCommit(STsdbRepo *pRepo) {
  ASSERT(pRepo->mem != NULL);
  STsdbCfg *pCfg = &(pRepo->config);

  STsdbBufBlock *pBufBlock = tsdbGetCurrBufBlock(pRepo);
  ASSERT(pBufBlock != NULL);
  if ((pRepo->mem->extraBuffList != NULL) ||
      ((listNEles(pRepo->mem->bufBlockList) >= pCfg->totalBlocks / 3) && (pBufBlock->remain < TSDB_BUFFER_RESERVE))) {
    // trigger commit
    if (tsdbAsyncCommit(pRepo) < 0) return -1;
  }

  return 0;
}

STsdbMeta *    tsdbGetMeta(TSDB_REPO_T *pRepo) { return ((STsdbRepo *)pRepo)->tsdbMeta; }
STsdbFileH *   tsdbGetFile(TSDB_REPO_T *pRepo) { return ((STsdbRepo *)pRepo)->tsdbFileH; }
STsdbRepoInfo *tsdbGetStatus(TSDB_REPO_T *pRepo) { return NULL; }

// ----------------- LOCAL FUNCTIONS -----------------
static int32_t tsdbCheckAndSetDefaultCfg(STsdbCfg *pCfg) {
  // Check precision
  if (pCfg->precision == -1) {
    pCfg->precision = TSDB_DEFAULT_PRECISION;
  } else {
    if (!IS_VALID_PRECISION(pCfg->precision)) {
      tsdbError("vgId:%d invalid precision configuration %d", pCfg->tsdbId, pCfg->precision);
      goto _err;
    }
  }

  // Check compression
  if (pCfg->compression == -1) {
    pCfg->compression = TSDB_DEFAULT_COMPRESSION;
  } else {
    if (!IS_VALID_COMPRESSION(pCfg->compression)) {
      tsdbError("vgId:%d invalid compression configuration %d", pCfg->tsdbId, pCfg->precision);
      goto _err;
    }
  }

  // Check tsdbId
  if (pCfg->tsdbId < 0) {
    tsdbError("vgId:%d invalid vgroup ID", pCfg->tsdbId);
    goto _err;
  }

  // Check daysPerFile
  if (pCfg->daysPerFile == -1) {
    pCfg->daysPerFile = TSDB_DEFAULT_DAYS_PER_FILE;
  } else {
    if (pCfg->daysPerFile < TSDB_MIN_DAYS_PER_FILE || pCfg->daysPerFile > TSDB_MAX_DAYS_PER_FILE) {
      tsdbError(
          "vgId:%d invalid daysPerFile configuration! daysPerFile %d TSDB_MIN_DAYS_PER_FILE %d TSDB_MAX_DAYS_PER_FILE "
          "%d",
          pCfg->tsdbId, pCfg->daysPerFile, TSDB_MIN_DAYS_PER_FILE, TSDB_MAX_DAYS_PER_FILE);
      goto _err;
    }
  }

  // Check minRowsPerFileBlock and maxRowsPerFileBlock
  if (pCfg->minRowsPerFileBlock == -1) {
    pCfg->minRowsPerFileBlock = TSDB_DEFAULT_MIN_ROW_FBLOCK;
  } else {
    if (pCfg->minRowsPerFileBlock < TSDB_MIN_MIN_ROW_FBLOCK || pCfg->minRowsPerFileBlock > TSDB_MAX_MIN_ROW_FBLOCK) {
      tsdbError(
          "vgId:%d invalid minRowsPerFileBlock configuration! minRowsPerFileBlock %d TSDB_MIN_MIN_ROW_FBLOCK %d "
          "TSDB_MAX_MIN_ROW_FBLOCK %d",
          pCfg->tsdbId, pCfg->minRowsPerFileBlock, TSDB_MIN_MIN_ROW_FBLOCK, TSDB_MAX_MIN_ROW_FBLOCK);
      goto _err;
    }
  }

  if (pCfg->maxRowsPerFileBlock == -1) {
    pCfg->maxRowsPerFileBlock = TSDB_DEFAULT_MAX_ROW_FBLOCK;
  } else {
    if (pCfg->maxRowsPerFileBlock < TSDB_MIN_MAX_ROW_FBLOCK || pCfg->maxRowsPerFileBlock > TSDB_MAX_MAX_ROW_FBLOCK) {
      tsdbError(
          "vgId:%d invalid maxRowsPerFileBlock configuration! maxRowsPerFileBlock %d TSDB_MIN_MAX_ROW_FBLOCK %d "
          "TSDB_MAX_MAX_ROW_FBLOCK %d",
          pCfg->tsdbId, pCfg->maxRowsPerFileBlock, TSDB_MIN_MIN_ROW_FBLOCK, TSDB_MAX_MIN_ROW_FBLOCK);
      goto _err;
    }
  }

  if (pCfg->minRowsPerFileBlock > pCfg->maxRowsPerFileBlock) {
    tsdbError("vgId:%d invalid configuration! minRowsPerFileBlock %d maxRowsPerFileBlock %d", pCfg->tsdbId,
              pCfg->minRowsPerFileBlock, pCfg->maxRowsPerFileBlock);
    goto _err;
  }

  // Check keep
  if (pCfg->keep == -1) {
    pCfg->keep = TSDB_DEFAULT_KEEP;
  } else {
    if (pCfg->keep < TSDB_MIN_KEEP || pCfg->keep > TSDB_MAX_KEEP) {
      tsdbError(
          "vgId:%d invalid keep configuration! keep %d TSDB_MIN_KEEP %d "
          "TSDB_MAX_KEEP %d",
          pCfg->tsdbId, pCfg->keep, TSDB_MIN_KEEP, TSDB_MAX_KEEP);
      goto _err;
    }
  }

  // update check
  if (pCfg->update != 0) pCfg->update = 1;

  return 0;

_err:
  terrno = TSDB_CODE_TDB_INVALID_CONFIG;
  return -1;
}

static int32_t tsdbSetRepoEnv(char *rootDir, STsdbCfg *pCfg) {
  if (tfsMkdir(rootDir) < 0) {
    tsdbError("vgId:%d failed to create rootDir %s since %s", pCfg->tsdbId, rootDir, tstrerror(terrno));
    return -1;
  }

  if (tsdbSaveConfig(rootDir, pCfg) < 0) {
    tsdbError("vgId:%d failed to set TSDB environment since %s", pCfg->tsdbId, tstrerror(terrno));
    return -1;
  }

  char *dirName = tsdbGetDataDirName(rootDir);
  if (dirName == NULL) return -1;

  if (tfsMkdir(dirName) < 0) {
    tsdbError("vgId:%d failed to create directory %s since %s", pCfg->tsdbId, dirName, strerror(errno));
    free(dirName);
    return -1;
  }

  free(dirName);

  char *fname = tsdbGetMetaFileName(rootDir);
  if (fname == NULL) return -1;
  if (tdCreateKVStore(fname) < 0) {
    tsdbError("vgId:%d failed to open KV store since %s", pCfg->tsdbId, tstrerror(terrno));
    free(fname);
    return -1;
  }

  free(fname);
  return 0;
}

static int32_t tsdbUnsetRepoEnv(char *rootDir) {
  tfsRmdir(rootDir);
  tsdbDebug("repository %s is removed", rootDir);
  return 0;
}

static int32_t tsdbSaveConfig(char *rootDir, STsdbCfg *pCfg) {
  int   fd = -1;
  char *fname = NULL;
  char  buf[TSDB_FILE_HEAD_SIZE] = "\0";
  char *pBuf = buf;

  fname = tsdbGetCfgFname(rootDir);
  if (fname == NULL) {
    tsdbError("vgId:%d failed to save configuration since %s", pCfg->tsdbId, tstrerror(terrno));
    goto _err;
  }

  fd = open(fname, O_WRONLY | O_CREAT, 0755);
  if (fd < 0) {
    tsdbError("vgId:%d failed to open file %s since %s", pCfg->tsdbId, fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  int tlen = tsdbEncodeCfg((void *)(&pBuf), pCfg);
  ASSERT((tlen + sizeof(TSCKSUM) <= TSDB_FILE_HEAD_SIZE) && (POINTER_DISTANCE(pBuf, buf) == tlen));

  taosCalcChecksumAppend(0, (uint8_t *)buf, TSDB_FILE_HEAD_SIZE);

  if (taosWrite(fd, (void *)buf, TSDB_FILE_HEAD_SIZE) < TSDB_FILE_HEAD_SIZE) {
    tsdbError("vgId:%d failed to write %d bytes to file %s since %s", pCfg->tsdbId, TSDB_FILE_HEAD_SIZE, fname,
              strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (fsync(fd) < 0) {
    tsdbError("vgId:%d failed to fsync file %s since %s", pCfg->tsdbId, fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  free(fname);
  close(fd);
  return 0;

_err:
  tfree(fname);
  if (fd >= 0) close(fd);
  return -1;
}

static int tsdbLoadConfig(char *rootDir, STsdbCfg *pCfg) {
  char *fname = NULL;
  int   fd = -1;
  char  buf[TSDB_FILE_HEAD_SIZE] = "\0";

  fname = tsdbGetCfgFname(rootDir);
  if (fname == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  fd = open(fname, O_RDONLY);
  if (fd < 0) {
    tsdbError("failed to open file %s since %s", fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (taosRead(fd, (void *)buf, TSDB_FILE_HEAD_SIZE) < TSDB_FILE_HEAD_SIZE) {
    tsdbError("failed to read %d bytes from file %s since %s", TSDB_FILE_HEAD_SIZE, fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (!taosCheckChecksumWhole((uint8_t *)buf, TSDB_FILE_HEAD_SIZE)) {
    tsdbError("file %s is corrupted", fname);
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    goto _err;
  }

  tsdbDecodeCfg(buf, pCfg);

  tfree(fname);
  close(fd);

  return 0;

_err:
  tfree(fname);
  if (fd >= 0) close(fd);
  return -1;
}

static char *tsdbGetCfgFname(char *rootDir) {
  int   tlen = (int)(strlen(TFS_PRIMARY_PATH()) + strlen(rootDir) + strlen(TSDB_CFG_FILE_NAME) + 3);
  char *fname = calloc(1, tlen);
  if (fname == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  snprintf(fname, tlen, "%s/%s/%s", TFS_PRIMARY_PATH(), rootDir, TSDB_CFG_FILE_NAME);
  return fname;
}

static STsdbRepo *tsdbNewRepo(char *rootDir, STsdbAppH *pAppH, STsdbCfg *pCfg) {
  STsdbRepo *pRepo = (STsdbRepo *)calloc(1, sizeof(STsdbRepo));
  if (pRepo == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  pRepo->state = TSDB_STATE_OK;
  pRepo->code = TSDB_CODE_SUCCESS;

  int code = pthread_mutex_init(&pRepo->mutex, NULL);
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    goto _err;
  }

  code = sem_init(&(pRepo->readyToCommit), 0, 1);
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    goto _err;
  }

  pRepo->repoLocked = false;

  pRepo->rootDir = strdup(rootDir);
  if (pRepo->rootDir == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  pRepo->config = *pCfg;
  if (pAppH) pRepo->appH = *pAppH;

  pRepo->tsdbMeta = tsdbNewMeta(pCfg);
  if (pRepo->tsdbMeta == NULL) {
    tsdbError("vgId:%d failed to create meta since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  pRepo->pPool = tsdbNewBufPool(pCfg);
  if (pRepo->pPool == NULL) {
    tsdbError("vgId:%d failed to create buffer pool since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  pRepo->tsdbFileH = tsdbNewFileH(pCfg);
  if (pRepo->tsdbFileH == NULL) {
    tsdbError("vgId:%d failed to create file handle since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  return pRepo;

_err:
  tsdbFreeRepo(pRepo);
  return NULL;
}

static void tsdbFreeRepo(STsdbRepo *pRepo) {
  if (pRepo) {
    tsdbFreeFileH(pRepo->tsdbFileH);
    tsdbFreeBufPool(pRepo->pPool);
    tsdbFreeMeta(pRepo->tsdbMeta);
    // tsdbFreeMemTable(pRepo->mem);
    // tsdbFreeMemTable(pRepo->imem);
    tfree(pRepo->rootDir);
    sem_destroy(&(pRepo->readyToCommit));
    pthread_mutex_destroy(&pRepo->mutex);
    free(pRepo);
  }
}

static int tsdbRestoreInfo(STsdbRepo *pRepo) {
  STsdbMeta * pMeta = pRepo->tsdbMeta;
  STsdbFileH *pFileH = pRepo->tsdbFileH;
  SFileGroup *pFGroup = NULL;

  SFileGroupIter iter;
  SRWHelper      rhelper = {0};

  if (tsdbInitReadHelper(&rhelper, pRepo) < 0) goto _err;

  tsdbInitFileGroupIter(pFileH, &iter, TSDB_ORDER_DESC);
  while ((pFGroup = tsdbGetFileGroupNext(&iter)) != NULL) {
    if (pFGroup->state) continue;
    if (tsdbSetAndOpenHelperFile(&rhelper, pFGroup) < 0) goto _err;
    if (tsdbLoadCompIdx(&rhelper, NULL) < 0) goto _err;
    for (int i = 1; i < pMeta->maxTables; i++) {
      STable *pTable = pMeta->tables[i];
      if (pTable == NULL) continue;
      if (tsdbSetHelperTable(&rhelper, pTable, pRepo) < 0) goto _err;
      SBlockIdx *pIdx = &(rhelper.curCompIdx);

      if (pIdx->offset > 0 && pTable->lastKey < pIdx->maxKey) pTable->lastKey = pIdx->maxKey;
    }
  }

  tsdbDestroyHelper(&rhelper);
  return 0;

_err:
  tsdbDestroyHelper(&rhelper);
  return -1;
}

static void tsdbAlterCompression(STsdbRepo *pRepo, int8_t compression) {
  int8_t ocompression = pRepo->config.compression;
  pRepo->config.compression = compression;
  tsdbDebug("vgId:%d tsdb compression is changed from %d to %d", REPO_ID(pRepo), ocompression, compression);
}

static int tsdbAlterKeep(STsdbRepo *pRepo, int32_t keep) {
  STsdbCfg *  pCfg = &pRepo->config;
  STsdbFileH *pFileH = pRepo->tsdbFileH;
  int         okeep = pCfg->keep;
  SFileGroup *pFGroup = NULL;

  ASSERT(pCfg->keep != keep);
  int maxFiles = TSDB_MAX_FILE(keep, pCfg->daysPerFile);
  
  if (maxFiles != pFileH->maxFGroups) {
    pthread_rwlock_wrlock(&(pFileH->fhlock));

    pCfg->keep = keep;
    pFGroup = (SFileGroup *)calloc(maxFiles, sizeof(SFileGroup));
    if (pFGroup == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      pthread_rwlock_unlock(&(pFileH->fhlock));
      return -1;
    }

    int mfid = (int)(TSDB_KEY_FILEID(taosGetTimestamp(pCfg->precision), pCfg->daysPerFile, pCfg->precision) -
               TSDB_MAX_FILE(keep, pCfg->daysPerFile));

    int i = 0;
    for (; i < pFileH->nFGroups; i++) {
      if (pFileH->pFGroup[i].fileId >= mfid) break;
      tsdbRemoveFileGroup(pRepo, &(pFileH->pFGroup[i]));
    }

    for (int j = 0; i < pFileH->nFGroups; i++, j++) {
      pFGroup[j] = pFileH->pFGroup[i];
    }

    free(pFileH->pFGroup);
    pFileH->pFGroup = pFGroup;

    pthread_rwlock_unlock(&(pFileH->fhlock));
  }

  tsdbDebug("vgId:%d keep is changed from %d to %d", REPO_ID(pRepo), okeep, keep);

  return 0;
}

static int keyFGroupCompFunc(const void *key, const void *fgroup) {
  int         fid = *(int *)key;
  SFileGroup *pFGroup = (SFileGroup *)fgroup;
  if (fid == pFGroup->fileId) {
    return 0;
  } else {
    return fid > pFGroup->fileId ? 1 : -1;
  }
}

static int tsdbEncodeCfg(void **buf, STsdbCfg *pCfg) {
  int tlen = 0;

  tlen += taosEncodeVariantI32(buf, pCfg->tsdbId);
  tlen += taosEncodeFixedI32(buf, pCfg->cacheBlockSize);
  tlen += taosEncodeVariantI32(buf, pCfg->totalBlocks);
  tlen += taosEncodeVariantI32(buf, pCfg->daysPerFile);
  tlen += taosEncodeVariantI32(buf, pCfg->keep);
  tlen += taosEncodeVariantI32(buf, pCfg->keep1);
  tlen += taosEncodeVariantI32(buf, pCfg->keep2);
  tlen += taosEncodeVariantI32(buf, pCfg->minRowsPerFileBlock);
  tlen += taosEncodeVariantI32(buf, pCfg->maxRowsPerFileBlock);
  tlen += taosEncodeFixedI8(buf, pCfg->precision);
  tlen += taosEncodeFixedI8(buf, pCfg->compression);
  tlen += taosEncodeFixedI8(buf, pCfg->update);

  return tlen;
}

static void *tsdbDecodeCfg(void *buf, STsdbCfg *pCfg) {
  buf = taosDecodeVariantI32(buf, &(pCfg->tsdbId));
  buf = taosDecodeFixedI32(buf, &(pCfg->cacheBlockSize));
  buf = taosDecodeVariantI32(buf, &(pCfg->totalBlocks));
  buf = taosDecodeVariantI32(buf, &(pCfg->daysPerFile));
  buf = taosDecodeVariantI32(buf, &(pCfg->keep));
  buf = taosDecodeVariantI32(buf, &(pCfg->keep1));
  buf = taosDecodeVariantI32(buf, &(pCfg->keep2));
  buf = taosDecodeVariantI32(buf, &(pCfg->minRowsPerFileBlock));
  buf = taosDecodeVariantI32(buf, &(pCfg->maxRowsPerFileBlock));
  buf = taosDecodeFixedI8(buf, &(pCfg->precision));
  buf = taosDecodeFixedI8(buf, &(pCfg->compression));
  buf = taosDecodeFixedI8(buf, &(pCfg->update));

  return buf;
}

static int tsdbAlterCacheTotalBlocks(STsdbRepo *pRepo, int totalBlocks) {
  // TODO
  // STsdbCache *pCache = pRepo->tsdbCache;
  // int         oldNumOfBlocks = pCache->totalCacheBlocks;

  // tsdbLockRepo((TsdbRepoT *)pRepo);

  // ASSERT(pCache->totalCacheBlocks != totalBlocks);

  // if (pCache->totalCacheBlocks < totalBlocks) {
  //   ASSERT(pCache->totalCacheBlocks == pCache->pool.numOfCacheBlocks);
  //   int blocksToAdd = pCache->totalCacheBlocks - totalBlocks;
  //   pCache->totalCacheBlocks = totalBlocks;
  //   for (int i = 0; i < blocksToAdd; i++) {
  //     if (tsdbAddCacheBlockToPool(pCache) < 0) {
  //       tsdbUnLockRepo((TsdbRepoT *)pRepo);
  //       tsdbError("tsdbId:%d, failed to add cache block to cache pool", pRepo->config.tsdbId);
  //       return -1;
  //     }
  //   }
  // } else {
  //   pCache->totalCacheBlocks = totalBlocks;
  //   tsdbAdjustCacheBlocks(pCache);
  // }
  // pRepo->config.totalBlocks = totalBlocks;

  // tsdbUnLockRepo((TsdbRepoT *)pRepo);
  // tsdbDebug("vgId:%d, tsdb total cache blocks changed from %d to %d", pRepo->config.tsdbId, oldNumOfBlocks,
  // totalBlocks);
  return 0;
}

#if 0

TSKEY tsdbGetTableLastKey(TSDB_REPO_T *repo, uint64_t uid) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;

  STable *pTable = tsdbGetTableByUid(pRepo->tsdbMeta, uid);
  if (pTable == NULL) return -1;

  return TSDB_GET_TABLE_LAST_KEY(pTable);
}

#endif

static void tsdbStartStream(STsdbRepo *pRepo) {
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  for (int i = 0; i < pMeta->maxTables; i++) {
    STable *pTable = pMeta->tables[i];
    if (pTable && pTable->type == TSDB_STREAM_TABLE) {
      pTable->cqhandle = (*pRepo->appH.cqCreateFunc)(pRepo->appH.cqH, TABLE_UID(pTable), TABLE_TID(pTable), TABLE_NAME(pTable)->data, pTable->sql,
                                                     tsdbGetTableSchemaImpl(pTable, false, false, -1));
    }
  }
}


static void tsdbStopStream(STsdbRepo *pRepo) {
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  for (int i = 0; i < pMeta->maxTables; i++) {
    STable *pTable = pMeta->tables[i];
    if (pTable && pTable->type == TSDB_STREAM_TABLE) {
      (*pRepo->appH.cqDropFunc)(pTable->cqhandle);
    }
  }
}
