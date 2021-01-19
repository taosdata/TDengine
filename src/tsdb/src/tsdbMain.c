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
#include "tsdbint.h"

#define IS_VALID_PRECISION(precision) \
  (((precision) >= TSDB_TIME_PRECISION_MILLI) && ((precision) <= TSDB_TIME_PRECISION_NANO))
#define TSDB_DEFAULT_COMPRESSION TWO_STAGE_COMP
#define IS_VALID_COMPRESSION(compression) (((compression) >= NO_COMPRESSION) && ((compression) <= TWO_STAGE_COMP))

static void       tsdbGetDataDir(int repoid, char dirName[]);
static void       tsdbGetRootDir(int repoid, char dirName[]);
static int32_t    tsdbCheckAndSetDefaultCfg(STsdbCfg *pCfg);
static STsdbRepo *tsdbNewRepo(STsdbCfg *pCfg, STsdbAppH *pAppH);
static void       tsdbFreeRepo(STsdbRepo *pRepo);
static void       tsdbStartStream(STsdbRepo *pRepo);
static void       tsdbStopStream(STsdbRepo *pRepo);
static int        tsdbRestoreInfo(STsdbRepo *pRepo);

// Function declaration
int32_t tsdbCreateRepo(int repoid) {
  char tsdbDir[TSDB_FILENAME_LEN] = "\0";
  char dataDir[TSDB_FILENAME_LEN] = "\0";

  tsdbGetRootDir(repoid, tsdbDir);
  if (tfsMkdir(tsdbDir) < 0) {
    goto _err;
  }

  tsdbGetDataDir(repoid, dataDir);
  if (tfsMkdir(dataDir) < 0) {
    goto _err;
  }

  return 0;

_err:
  tsdbError("vgId:%d failed to create TSDB repository since %s", repoid, tstrerror(terrno));
  return -1;
}

int32_t tsdbDropRepo(int repoid) {
  char tsdbDir[TSDB_FILENAME_LEN] = "\0";

  tsdbGetRootDir(repoid, tsdbDir);
  return tfsRmdir(tsdbDir);
}

STsdbRepo *tsdbOpenRepo(STsdbCfg *pCfg, STsdbAppH *pAppH) {
  STsdbRepo *pRepo;
  STsdbCfg   config = *pCfg;

  terrno = TSDB_CODE_SUCCESS;

  // Check and set default configurations
  if (tsdbCheckAndSetDefaultCfg(&config) < 0) {
    tsdbError("vgId:%d failed to open TSDB repository since %s", config.tsdbId, tstrerror(terrno));
    return NULL;
  }

  // Create new TSDB object
  if ((pRepo = tsdbNewRepo(&config, pAppH)) == NULL) {
    tsdbError("vgId:%d failed to open TSDB repository while creating TSDB object since %s", config.tsdbId,
              tstrerror(terrno));
    return NULL;
  }

  // Open meta
  if (tsdbOpenMeta(pRepo) < 0) {
    tsdbError("vgId:%d failed to open TSDB repository while opening Meta since %s", config.tsdbId, tstrerror(terrno));
    tsdbCloseRepo(pRepo, false);
    return NULL;
  }

  if (tsdbOpenBufPool(pRepo) < 0) {
    tsdbError("vgId:%d failed to open TSDB repository while opening buffer pool since %s", config.tsdbId,
              tstrerror(terrno));
    tsdbCloseRepo(pRepo, false);
    return NULL;
  }

  if (tsdbOpenFS(pRepo) < 0) {
    tsdbError("vgId:%d failed to open TSDB repository while opening FS since %s", config.tsdbId, tstrerror(terrno));
    tsdbCloseRepo(pRepo, false);
    return NULL;
  }

  // TODO: Restore information from data
  if (tsdbRestoreInfo(pRepo) < 0) {
    tsdbError("vgId:%d failed to open TSDB repository while restore info since %s", config.tsdbId, tstrerror(terrno));
    tsdbCloseRepo(pRepo, false);
    return NULL;
  }

  tsdbStartStream(pRepo);

  tsdbDebug("vgId:%d, TSDB repository opened", REPO_ID(pRepo));

  return pRepo;
}

// Note: all working thread and query thread must stopped when calling this function
int tsdbCloseRepo(STsdbRepo *repo, int toCommit) {
  if (repo == NULL) return 0;

  STsdbRepo *pRepo = repo;
  int        vgId = REPO_ID(pRepo);

  terrno = TSDB_CODE_SUCCESS;

  tsdbStopStream(pRepo);

  if (toCommit) {
    tsdbSyncCommit(repo);
  }
  tsdbUnRefMemTable(pRepo, pRepo->mem);
  tsdbUnRefMemTable(pRepo, pRepo->imem);
  pRepo->mem = NULL;
  pRepo->imem = NULL;

  tsdbCloseFS(pRepo);
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

STsdbCfg *tsdbGetCfg(const STsdbRepo *repo) {
  ASSERT(repo != NULL);
  return &((STsdbRepo *)repo)->config;
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

STsdbMeta *tsdbGetMeta(STsdbRepo *pRepo) { return pRepo->tsdbMeta; }

STsdbRepoInfo *tsdbGetStatus(STsdbRepo *pRepo) { return NULL; }

int tsdbGetState(STsdbRepo *repo) { return repo->state; }

void tsdbReportStat(void *repo, int64_t *totalPoints, int64_t *totalStorage, int64_t *compStorage) {
  ASSERT(repo != NULL);
  STsdbRepo *pRepo = repo;
  *totalPoints = pRepo->stat.pointsWritten;
  *totalStorage = pRepo->stat.totalStorage;
  *compStorage = pRepo->stat.compStorage;
}

int32_t tsdbConfigRepo(STsdbRepo *repo, STsdbCfg *pCfg) {
  // TODO: think about multithread cases
  return 0;
#if 0
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
  if (pRCfg->cacheLastRow != pCfg->cacheLastRow) {
    config.cacheLastRow = pCfg->cacheLastRow;
    configChanged = true;
  }

  if (configChanged) {
    if (tsdbSaveConfig(pRepo->rootDir, &config) < 0) {
      tsdbError("vgId:%d failed to configure repository while save config since %s", REPO_ID(pRepo), tstrerror(terrno));
      return -1;
    }
  }

  return 0;
#endif
}

uint32_t tsdbGetFileInfo(STsdbRepo *repo, char *name, uint32_t *index, uint32_t eindex, int64_t *size) {
  // TODO
  return 0;
#if 0
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
#endif
}

static void tsdbGetRootDir(int repoid, char dirName[]) {
  snprintf(dirName, TSDB_FILENAME_LEN, "vnode/vnode%d/tsdb", repoid);
}

static void tsdbGetDataDir(int repoid, char dirName[]) {
  snprintf(dirName, TSDB_FILENAME_LEN, "vnode/vnode%d/tsdb/data", repoid);
}

static int32_t tsdbCheckAndSetDefaultCfg(STsdbCfg *pCfg) {
  // Check tsdbId
  if (pCfg->tsdbId < 0) {
    tsdbError("vgId:%d invalid vgroup ID", pCfg->tsdbId);
    terrno = TSDB_CODE_TDB_INVALID_CONFIG;
    return -1;
  }

  // Check precision
  if (pCfg->precision == -1) {
    pCfg->precision = TSDB_DEFAULT_PRECISION;
  } else {
    if (!IS_VALID_PRECISION(pCfg->precision)) {
      tsdbError("vgId:%d invalid precision configuration %d", pCfg->tsdbId, pCfg->precision);
      terrno = TSDB_CODE_TDB_INVALID_CONFIG;
      return -1;
    }
  }

  // Check compression
  if (pCfg->compression == -1) {
    pCfg->compression = TSDB_DEFAULT_COMPRESSION;
  } else {
    if (!IS_VALID_COMPRESSION(pCfg->compression)) {
      tsdbError("vgId:%d invalid compression configuration %d", pCfg->tsdbId, pCfg->precision);
      terrno = TSDB_CODE_TDB_INVALID_CONFIG;
      return -1;
    }
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
      terrno = TSDB_CODE_TDB_INVALID_CONFIG;
      return -1;
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
      terrno = TSDB_CODE_TDB_INVALID_CONFIG;
      return -1;
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
      terrno = TSDB_CODE_TDB_INVALID_CONFIG;
      return -1;
    }
  }

  if (pCfg->minRowsPerFileBlock > pCfg->maxRowsPerFileBlock) {
    tsdbError("vgId:%d invalid configuration! minRowsPerFileBlock %d maxRowsPerFileBlock %d", pCfg->tsdbId,
              pCfg->minRowsPerFileBlock, pCfg->maxRowsPerFileBlock);
    terrno = TSDB_CODE_TDB_INVALID_CONFIG;
    return -1;
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
      terrno = TSDB_CODE_TDB_INVALID_CONFIG;
      return -1;
    }
  }

  if (pCfg->keep1 == 0) {
    pCfg->keep1 = pCfg->keep;
  }

  if (pCfg->keep2 == 0) {
    pCfg->keep2 = pCfg->keep;
  }

  // update check
  if (pCfg->update != 0) pCfg->update = 1;

  // update cacheLastRow
  if (pCfg->cacheLastRow != 0) pCfg->cacheLastRow = 1;

  return 0;
}

static STsdbRepo *tsdbNewRepo(STsdbCfg *pCfg, STsdbAppH *pAppH) {
  STsdbRepo *pRepo = (STsdbRepo *)calloc(1, sizeof(*pRepo));
  if (pRepo == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  pRepo->state = TSDB_STATE_OK;
  pRepo->code = TSDB_CODE_SUCCESS;
  pRepo->config = *pCfg;
  if (pAppH) {
    pRepo->appH = *pAppH;
  }
  pRepo->repoLocked = false;

  int code = pthread_mutex_init(&(pRepo->mutex), NULL);
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    tsdbFreeRepo(pRepo);
    return NULL;
  }

  code = sem_init(&(pRepo->readyToCommit), 0, 1);
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    tsdbFreeRepo(pRepo);
    return NULL;
  }

  pRepo->tsdbMeta = tsdbNewMeta(pCfg);
  if (pRepo->tsdbMeta == NULL) {
    tsdbError("vgId:%d failed to create meta since %s", REPO_ID(pRepo), tstrerror(terrno));
    tsdbFreeRepo(pRepo);
    return NULL;
  }

  pRepo->pPool = tsdbNewBufPool(pCfg);
  if (pRepo->pPool == NULL) {
    tsdbError("vgId:%d failed to create buffer pool since %s", REPO_ID(pRepo), tstrerror(terrno));
    tsdbFreeRepo(pRepo);
    return NULL;
  }

  pRepo->fs = tsdbNewFS(pCfg);
  if (pRepo->fs == NULL) {
    tsdbError("vgId:%d failed to TSDB file system since %s", REPO_ID(pRepo), tstrerror(terrno));
    tsdbFreeRepo(pRepo);
    return NULL;
  }

  return pRepo;
}

static void tsdbFreeRepo(STsdbRepo *pRepo) {
  if (pRepo) {
    tsdbFreeFS(pRepo->fs);
    tsdbFreeBufPool(pRepo->pPool);
    tsdbFreeMeta(pRepo->tsdbMeta);
    sem_destroy(&(pRepo->readyToCommit));
    pthread_mutex_destroy(&pRepo->mutex);
    free(pRepo);
  }
}

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

static int tsdbRestoreInfo(STsdbRepo *pRepo) {
  SFSIter    fsiter;
  SReadH     readh;
  SDFileSet *pSet;
  STsdbMeta *pMeta = pRepo->tsdbMeta;
  STsdbCfg * pCfg = REPO_CFG(pRepo);
  SBlock *   pBlock;

  if (tsdbInitReadH(&readh, pRepo) < 0) {
    return -1;
  }

  tsdbFSIterInit(&fsiter, REPO_FS(pRepo), TSDB_FS_ITER_BACKWARD);

  while ((pSet = tsdbFSIterNext(&fsiter)) != NULL) {
    if (tsdbSetAndOpenReadFSet(&readh, pSet) < 0) {
      tsdbDestroyReadH(&readh);
      return -1;
    }

    if (tsdbLoadBlockIdx(&readh) < 0) {
      tsdbDestroyReadH(&readh);
      return -1;
    }

    for (int i = 1; i < pMeta->maxTables; i++) {
      STable *pTable = pMeta->tables[i];
      if (pTable == NULL) continue;

      if (tsdbSetReadTable(&readh, pTable) < 0) {
        tsdbDestroyReadH(&readh);
        return -1;
      }

      TSKEY      lastKey = tsdbGetTableLastKeyImpl(pTable);
      SBlockIdx *pIdx = readh.pBlkIdx;
      if (pIdx && lastKey < pIdx->maxKey) {
        pTable->lastKey = pIdx->maxKey;

        if (pCfg->cacheLastRow) {
          if (tsdbLoadBlockInfo(&readh, NULL) < 0) {
            tsdbDestroyReadH(&readh);
            return -1;
          }

          pBlock = readh.pBlkInfo->blocks + pIdx->numOfBlocks - 1;

          if (tsdbLoadBlockData(&readh, pBlock, NULL) < 0) {
            tsdbDestroyReadH(&readh);
            return -1;
          }

          // Get the data in row
          ASSERT(pTable->lastRow == NULL);
          STSchema *pSchema = tsdbGetTableSchema(pTable);
          pTable->lastRow = taosTMalloc(schemaTLen(pSchema));
          if (pTable->lastRow == NULL) {
            terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
            tsdbDestroyReadH(&readh);
            return -1;
          }

          tdInitDataRow(pTable->lastRow, pSchema);
          for (int icol = 0; icol < schemaNCols(pSchema); icol++) {
            STColumn *pCol = schemaColAt(pSchema, icol);
            SDataCol *pDataCol = readh.pDCols[0]->cols + icol;
            tdAppendColVal(pTable->lastRow, tdGetColDataOfRow(pDataCol, pBlock->numOfRows - 1), pCol->type, pCol->bytes,
                           pCol->offset);
          }
        }
      }
      
    }
  }

  tsdbDestroyReadH(&readh);
  return 0;
}