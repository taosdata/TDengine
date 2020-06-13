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
#include "tsdbMain.h"
#include "os.h"
#include "talgo.h"
#include "taosdef.h"
#include "tchecksum.h"
#include "tscompression.h"
#include "tsdb.h"
#include "ttime.h"
#include "tulog.h"

#include <pthread.h>
#include <sys/stat.h>

#define TSDB_CFG_FILE_NAME "config"
#define TSDB_DATA_DIR_NAME "data"
#define TSDB_META_FILE_NAME "meta"
#define TSDB_META_FILE_INDEX 10000000

// Function declaration
int32_t tsdbCreateRepo(char *rootDir, STsdbCfg *pCfg) {
  if (mkdir(rootDir, 0755) < 0) {
    tsdbError("vgId:%d failed to create rootDir %s since %s", pCfg->tsdbId, rootDir, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (tsdbCheckAndSetDefaultCfg(pCfg) < 0) return -1;

  if (tsdbSetRepoEnv(rootDir, pCfg) < 0) return -1;

  tsdbTrace(
      "vgId%d tsdb env create succeed! cacheBlockSize %d totalBlocks %d maxTables %d daysPerFile %d keep "
      "%d minRowsPerFileBlock %d maxRowsPerFileBlock %d precision %d compression %d",
      pCfg->tsdbId, pCfg->cacheBlockSize, pCfg->totalBlocks, pCfg->maxTables, pCfg->daysPerFile, pCfg->keep,
      pCfg->minRowsPerFileBlock, pCfg->maxRowsPerFileBlock, pCfg->precision, pCfg->compression);
  return 0;
}

int32_t tsdbDropRepo(char *rootDir) {
  return tsdbUnsetRepoEnv(rootDir);
}

TSDB_REPO_T *tsdbOpenRepo(char *rootDir, STsdbAppH *pAppH) {
  STsdbCfg   config = {0};
  STsdbRepo *pRepo = NULL;

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

  // // Restore key from file
  // if (tsdbRestoreInfo(pRepo) < 0) {
  //   tsdbFreeCache(pRepo->tsdbCache);
  //   tsdbFreeMeta(pRepo->tsdbMeta);
  //   tsdbCloseFileH(pRepo->tsdbFileH);
  //   free(pRepo->rootDir);
  //   free(pRepo);
  //   return NULL;
  // }

  // pRepo->state = TSDB_REPO_STATE_ACTIVE;

  tsdbTrace("vgId:%d open tsdb repository succeed!", REPO_ID(pRepo));

  return (TSDB_REPO_T *)pRepo;

_err:
  tsdbCloseRepo(pRepo, false);
  tsdbFreeRepo(pRepo);
  return NULL;
}

int32_t tsdbCloseRepo(TSDB_REPO_T *repo, int toCommit) {
  // TODO
  // STsdbRepo *pRepo = (STsdbRepo *)repo;
  // if (pRepo == NULL) return 0;
  // int id = pRepo->config.tsdbId;

  // pRepo->state = TSDB_REPO_STATE_CLOSED;
  // tsdbLockRepo(repo);
  // if (pRepo->commit) {
  //   tsdbUnLockRepo(repo);
  //   return -1;
  // }
  // pRepo->commit = 1;
  // // Loop to move pData to iData
  // for (int i = 1; i < pRepo->config.maxTables; i++) {
  //   STable *pTable = pRepo->tsdbMeta->tables[i];
  //   if (pTable != NULL && pTable->mem != NULL) {
  //     pTable->imem = pTable->mem;
  //     pTable->mem = NULL;
  //   }
  // }
  // // TODO: Loop to move mem to imem
  // pRepo->tsdbCache->imem = pRepo->tsdbCache->mem;
  // pRepo->tsdbCache->mem = NULL;
  // pRepo->tsdbCache->curBlock = NULL;
  // tsdbUnLockRepo(repo);

  // if (pRepo->appH.notifyStatus) pRepo->appH.notifyStatus(pRepo->appH.appH, TSDB_STATUS_COMMIT_START);
  // if (toCommit) tsdbCommitData((void *)repo);

  // tsdbCloseFileH(pRepo->tsdbFileH);

  // tsdbFreeMeta(pRepo->tsdbMeta);

  // tsdbFreeCache(pRepo->tsdbCache);

  // tfree(pRepo->rootDir);
  // tfree(pRepo);

  // tsdbTrace("vgId:%d repository is closed!", id);

  return 0;
}

int32_t tsdbInsertData(TSDB_REPO_T *repo, SSubmitMsg *pMsg, SShellSubmitRspMsg *pRsp) {
  STsdbRepo *    pRepo = (STsdbRepo *)repo;
  SSubmitMsgIter msgIter;

  if (tsdbInitSubmitMsgIter(pMsg, &msgIter) < 0) {
    tsdbError("vgId:%d submit message is messed up", REPO_ID(pRepo));
    return terrno;
  }

  SSubmitBlk *pBlock = NULL;
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     affectedrows = 0;

  TSKEY now = taosGetTimestamp(pRepo->config.precision);

  while ((pBlock = tsdbGetSubmitMsgNext(&msgIter)) != NULL) {
    if ((code = tsdbInsertDataToTable(pRepo, pBlock, now, &affectedrows)) != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  pRsp->affectedRows = htonl(affectedrows);
  return code;
}

uint32_t tsdbGetFileInfo(TSDB_REPO_T *repo, char *name, uint32_t *index, uint32_t eindex, int32_t *size) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;
  // STsdbMeta *pMeta = pRepo->tsdbMeta;
  STsdbFileH *pFileH = pRepo->tsdbFileH;
  uint32_t    magic = 0;
  char        fname[256] = "\0";

  struct stat fState;

  tsdbTrace("vgId:%d name:%s index:%d eindex:%d", pRepo->config.tsdbId, name, *index, eindex);
  ASSERT(*index <= eindex);

  char *sdup = strdup(pRepo->rootDir);
  char *prefix = dirname(sdup);

  if (name[0] == 0) {  // get the file from index or after, but not larger than eindex
    int fid = (*index) / 3;

    if (pFileH->numOfFGroups == 0 || fid > pFileH->fGroup[pFileH->numOfFGroups - 1].fileId) {
      if (*index <= TSDB_META_FILE_INDEX && TSDB_META_FILE_INDEX <= eindex) {
        tsdbGetMetaFileName(pRepo->rootDir, fname);
        *index = TSDB_META_FILE_INDEX;
      } else {
        tfree(sdup);
        return 0;
      }
    } else {
      SFileGroup *pFGroup =
          taosbsearch(&fid, pFileH->fGroup, pFileH->numOfFGroups, sizeof(SFileGroup), compFGroupKey, TD_GE);
      if (pFGroup->fileId == fid) {
        strcpy(fname, pFGroup->files[(*index) % 3].fname);
      } else {
        if (pFGroup->fileId * 3 + 2 < eindex) {
          strcpy(fname, pFGroup->files[0].fname);
          *index = pFGroup->fileId * 3;
        } else {
          tfree(sdup);
          return 0;
        }
      }
    }
    strcpy(name, fname + strlen(prefix));
  } else {                                 // get the named file at the specified index. If not there, return 0
    if (*index == TSDB_META_FILE_INDEX) {  // get meta file
      tsdbGetMetaFileName(pRepo->rootDir, fname);
    } else {
      int         fid = (*index) / 3;
      SFileGroup *pFGroup = tsdbSearchFGroup(pFileH, fid);
      if (pFGroup == NULL) {  // not found
        tfree(sdup);
        return 0;
      }

      SFile *pFile = &pFGroup->files[(*index) % 3];
      strcpy(fname, pFile->fname);
    }
  }

  if (stat(fname, &fState) < 0) {
    tfree(sdup);
    return 0;
  }

  tfree(sdup);
  *size = fState.st_size;
  magic = *size;

  return magic;
}

int tsdbUpdateTagValue(TSDB_REPO_T *repo, SUpdateTableTagValMsg *pMsg) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;
  STsdbMeta *pMeta = pRepo->tsdbMeta;
  int16_t    tversion = htons(pMsg->tversion);

  STable *pTable = tsdbGetTableByUid(pMeta, htobe64(pMsg->uid));
  if (pTable == NULL) return TSDB_CODE_TDB_INVALID_TABLE_ID;
  if (pTable->tableId.tid != htonl(pMsg->tid)) return TSDB_CODE_TDB_INVALID_TABLE_ID;

  if (pTable->type != TSDB_CHILD_TABLE) {
    tsdbError("vgId:%d failed to update tag value of table %s since its type is %d", pRepo->config.tsdbId,
              varDataVal(pTable->name), pTable->type);
    return TSDB_CODE_TDB_INVALID_TABLE_TYPE;
  }

  if (schemaVersion(tsdbGetTableTagSchema(pMeta, pTable)) < tversion) {
    tsdbTrace("vgId:%d server tag version %d is older than client tag version %d, try to config", pRepo->config.tsdbId,
              schemaVersion(tsdbGetTableTagSchema(pMeta, pTable)), tversion);
    void *msg = (*pRepo->appH.configFunc)(pRepo->config.tsdbId, htonl(pMsg->tid));
    if (msg == NULL) {
      return terrno;
    }
    // Deal with error her
    STableCfg *pTableCfg = tsdbCreateTableCfgFromMsg(msg);
    STable *   super = tsdbGetTableByUid(pMeta, pTableCfg->superUid);
    ASSERT(super != NULL);

    int32_t code = tsdbUpdateTable(pMeta, super, pTableCfg);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    tsdbClearTableCfg(pTableCfg);
    rpcFreeCont(msg);
  }

  STSchema *pTagSchema = tsdbGetTableTagSchema(pMeta, pTable);

  if (schemaVersion(pTagSchema) > tversion) {
    tsdbError(
        "vgId:%d failed to update tag value of table %s since version out of date, client tag version:%d server tag "
        "version:%d",
        pRepo->config.tsdbId, varDataVal(pTable->name), tversion, schemaVersion(pTable->tagSchema));
    return TSDB_CODE_TDB_TAG_VER_OUT_OF_DATE;
  }
  if (schemaColAt(pTagSchema, DEFAULT_TAG_INDEX_COLUMN)->colId == htons(pMsg->colId)) {
    tsdbRemoveTableFromIndex(pMeta, pTable);
  }
  // TODO: remove table from index if it is the first column of tag
  tdSetKVRowDataOfCol(&pTable->tagVal, htons(pMsg->colId), htons(pMsg->type), pMsg->data);
  if (schemaColAt(pTagSchema, DEFAULT_TAG_INDEX_COLUMN)->colId == htons(pMsg->colId)) {
    tsdbAddTableIntoIndex(pMeta, pTable);
  }
  return TSDB_CODE_SUCCESS;
}

void tsdbStartStream(TSDB_REPO_T *repo) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  for (int i = 0; i < pRepo->config.maxTables; i++) {
    STable *pTable = pMeta->tables[i];
    if (pTable && pTable->type == TSDB_STREAM_TABLE) {
      pTable->cqhandle = (*pRepo->appH.cqCreateFunc)(pRepo->appH.cqH, pTable->tableId.uid, pTable->tableId.tid,
                                                     pTable->sql, tsdbGetTableSchema(pMeta, pTable));
    }
  }
}

STsdbCfg *tsdbGetCfg(const TSDB_REPO_T *repo) {
  ASSERT(repo != NULL);
  return &((STsdbRepo *)repo)->config;
}

// ----------------- INTERNAL FUNCTIONS -----------------
char *tsdbGetMetaFileName(char *rootDir) {
  int   tlen = strlen(rootDir) + strlen(TSDB_META_FILE_NAME) + 2;
  char *fname = calloc(1, tlen);
  if (fname == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  snprintf(fname, tlen, "%s/%s", rootDir, TSDB_META_FILE_NAME);
  return fname;
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
  pRepo->repoLocked = false;
  int code = pthread_mutex_unlock(&pRepo->mutex);
  if (code != 0) {
    tsdbError("vgId:%d failed to unlock tsdb since %s", REPO_ID(pRepo), strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

void *tsdbCommitData(void *arg) {
  STsdbRepo *pRepo = (STsdbRepo *)arg;
  STsdbMeta *pMeta = pRepo->tsdbMeta;
  ASSERT(pRepo->imem != NULL);
  ASSERT(pRepo->commit == 1);

  tsdbPrint("vgId:%d start to commit! keyFirst " PRId64 " keyLast " PRId64 " numOfRows " PRId64, REPO_ID(pRepo),
            pRepo->imem->keyFirst, pRepo->imem->keyLast, pRepo->imem->numOfRows);

  // STsdbMeta * pMeta = pRepo->tsdbMeta;
  // STsdbCache *pCache = pRepo->tsdbCache;
  // STsdbCfg *  pCfg = &(pRepo->config);
  // SDataCols * pDataCols = NULL;
  // SRWHelper   whelper = {{0}};
  // if (pCache->imem == NULL) return NULL;

  tsdbPrint("vgId:%d, starting to commit....", pRepo->config.tsdbId);

  // Create the iterator to read from cache
  SSkipListIterator **iters = tsdbCreateTableIters(pRepo);
  if (iters == NULL) {
    tsdbError("vgId:%d failed to create table iterators since %s", REPO_ID(pRepo), tstrerror(terrno));
    // TODO: deal with the error here
    return NULL;
  }

  if (tsdbInitWriteHelper(&whelper, pRepo) < 0) {
    tsdbError("vgId:%d failed to init write helper since %s", REPO_ID(pRepo), tstrerror(terrno));
    // TODO
    goto _exit;
  }

  if ((pDataCols = tdNewDataCols(pMeta->maxRowBytes, pMeta->maxCols, pCfg->maxRowsPerFileBlock)) == NULL) {
    tsdbError("vgId:%d failed to init data cols with maxRowBytes %d maxCols %d since %s", REPO_ID(pRepo),
              pMeta->maxRowBytes, pMeta->maxCols, tstrerror(terrno));
        // TODO
    goto _exit;
  }

  int sfid = tsdbGetKeyFileId(pCache->imem->keyFirst, pCfg->daysPerFile, pCfg->precision);
  int efid = tsdbGetKeyFileId(pCache->imem->keyLast, pCfg->daysPerFile, pCfg->precision);

  // Loop to commit to each file
  for (int fid = sfid; fid <= efid; fid++) {
    if (tsdbCommitToFile(pRepo, fid, iters, &whelper, pDataCols) < 0) {
      ASSERT(false);
      goto _exit;
    }
  }

  // Do retention actions
  tsdbFitRetention(pRepo);
  if (pRepo->appH.notifyStatus) pRepo->appH.notifyStatus(pRepo->appH.appH, TSDB_STATUS_COMMIT_OVER);

_exit:
  tdFreeDataCols(pDataCols);
  tsdbDestroyTableIters(iters, pCfg->maxTables);
  tsdbDestroyHelper(&whelper);

  tsdbLockRepo(arg);
  tdListMove(pCache->imem->list, pCache->pool.memPool);
  tsdbAdjustCacheBlocks(pCache);
  tdListFree(pCache->imem->list);
  free(pCache->imem);
  pCache->imem = NULL;
  pRepo->commit = 0;
  for (int i = 1; i < pCfg->maxTables; i++) {
    STable *pTable = pMeta->tables[i];
    if (pTable && pTable->imem) {
      tsdbFreeMemTable(pTable->imem);
      pTable->imem = NULL;
    }
  }
  tsdbUnLockRepo(arg);
  tsdbPrint("vgId:%d, commit over....", pRepo->config.tsdbId);

  return NULL;
}

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

  // Check maxTables
  if (pCfg->maxTables == -1) {
    pCfg->maxTables = TSDB_DEFAULT_TABLES;
  } else {
    if (pCfg->maxTables < TSDB_MIN_TABLES || pCfg->maxTables > TSDB_MAX_TABLES) {
      tsdbError("vgId:%d invalid maxTables configuration! maxTables %d TSDB_MIN_TABLES %d TSDB_MAX_TABLES %d",
                pCfg->tsdbId, pCfg->maxTables, TSDB_MIN_TABLES, TSDB_MAX_TABLES);
      goto _err;
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
    tsdbError("vgId:%d invalid configuration! minRowsPerFileBlock %d maxRowsPerFileBlock %d" pCfg->tsdbId,
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

  return 0;

_err:
  terrno = TSDB_CODE_TDB_INVALID_CONFIG;
  return -1;
}

static int32_t tsdbSetRepoEnv(char *rootDir, STsdbCfg *pCfg) {
  if (tsdbSaveConfig(rootDir, pCfg) < 0) {
    tsdbError("vgId:%d failed to set TSDB environment since %s", pCfg->tsdbId, tstrerror(terrno));
    return -1;
  }

  char *dirName = tsdbGetDataDirName(rootDir);
  if (dirName == NULL) return -1;

  if (mkdir(dirName, 0755) < 0) {
    tsdbError("vgId:%d failed to create directory %s since %s", pCfg->tsdbId, dirName, strerror(errno));
    errno = TAOS_SYSTEM_ERROR(errno);
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
  taosRemoveDir(rootDir);
  tsdbTrace("repository %s is removed", rootDir);
  return 0;
}

static int32_t tsdbSaveConfig(char *rootDir, STsdbCfg *pCfg) {
  int   fd = -1;
  char *fname = NULL;

  fname = tsdbGetCfgFname(rootDir);
  if (fname == NULL) {
    tsdbError("vgId:%d failed to save configuration since %s", pCfg->tsdbId, tstrerror(terrno));
    goto _err;
  }

  fd = open(fname, O_WRONLY | O_CREAT, 0755);
  if (fd < 0) {
    tsdbError("vgId:%d failed to open file %s since %s", pCfg->tsdbId, fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err
  }

  if (twrite(fd, (void *)pCfg, sizeof(STsdbCfg)) < sizeof(STsdbCfg)) {
    tsdbError("vgId:%d failed to write %d bytes to file %s since %s", pCfg->tsdbId, sizeof(STsdbCfg), fname,
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
  if (fd > 0) close(fd);
  return -1;
}

static int tsdbLoadConfig(char *rootDir, STsdbCfg *pCfg) {
  char *fname = NULL;
  int   fd = -1;

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

  if (tread(fd, (void *)pCfg, sizeof(*pCfg)) < sizeof(*pCfg)) {
    tsdbError("failed to read %d bytes from file %s since %s", sizeof(*pCfg), fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  tfree(fname);
  close(fd);

  return 0;

_err:
  tfree(fname);
  if (fd > 0) close(fd);
  return -1;
}

static char *tsdbGetCfgFname(char *rootDir) {
  int   tlen = strlen(rootDir) + strlen(TSDB_CFG_FILE_NAME) + 2;
  char *fname = calloc(1, tlen);
  if (fname == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  snprintf(fname, tlen, "%s/%s", rootDir, TSDB_CFG_FILE_NAME);
  return fname;
}

static char *tsdbGetDataDirName(char *rootDir) {
  int   tlen = strlen(rootDir) + strlen(TSDB_DATA_DIR_NAME) + 2;
  char *fname = calloc(1, tlen);
  if (fname == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  snprintf(fname, tlen, "%s/%s", rootDir, TSDB_DATA_DIR_NAME);
  return fname;
}

static STsdbRepo *tsdbNewRepo(char *rootDir, STsdbAppH *pAppH, STsdbCfg *pCfg) {
  STsdbRepo *pRepo = (STsdbRepo *)calloc(1, sizeof(STsdbRepo));
  if (pRepo == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  int code = pthread_mutex_init(&pRepo->mutex, NULL);
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
  pRepo->appH = *pAppH;

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

  pRepo->tsdbFileH = tsdbNewFileH(pRepo);
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
    tsdbFreeMemTable(pRepo->mem);
    tsdbFreeMemTable(pRepo->imem);
    tfree(pRepo->rootDir);
    pthread_mutex_destroy(&pRepo->mutex);
    free(pRepo);
  }
}

static int tsdbInitSubmitMsgIter(SSubmitMsg *pMsg, SSubmitMsgIter *pIter) {
  if (pMsg == NULL) {
    terrno = TSDB_CODE_TDB_SUBMIT_MSG_MSSED_UP;
    return -1;
  }

  pMsg->length = htonl(pMsg->length);
  pMsg->numOfBlocks = htonl(pMsg->numOfBlocks);
  pMsg->compressed = htonl(pMsg->compressed);

  pIter->totalLen = pMsg->length;
  pIter->len = TSDB_SUBMIT_MSG_HEAD_SIZE;
  if (pMsg->length <= TSDB_SUBMIT_MSG_HEAD_SIZE) {
    terrno = TSDB_CODE_TDB_SUBMIT_MSG_MSSED_UP;
    return -1;
  } else {
    pIter->pBlock = pMsg->blocks;
  }

  return 0;
}

static int32_t tsdbInsertDataToTable(STsdbRepo *pRepo, SSubmitBlk *pBlock, TSKEY now, int32_t *affectedrows) {
  STsdbMeta *pMeta = pRepo->tsdbMeta;
  int64_t    points = 0;

  STable *pTable == tsdbGetTableByUid(pMeta, pBlock->uid);
  if (pTable == NULL || TABLE_TID(pTable)) {
    tsdbError("vgId:%d failed to get table to insert data, uid " PRIu64 " tid %d", REPO_ID(pRepo), pBlock->uid,
              pBlock->tid);
    terrno = TSDB_CODE_TDB_INVALID_TABLE_ID;
    return -1;
  }

  if (TABLE_TYPE(pTable) == TSDB_SUPER_TABLE) {
    tsdbError("vgId:%d invalid action trying to insert a super table %s", REPO_ID(pRepo), TABLE_CHAR_NAME(pTable));
    terrno = TSDB_CODE_TDB_INVALID_ACTION;
    return -1;
  }

  // // Check schema version
  // int32_t tversion = pBlock->sversion;
  // STSchema * pSchema = tsdbGetTableSchema(pMeta, pTable);
  // ASSERT(pSchema != NULL);
  // int16_t nversion = schemaVersion(pSchema);
  // if (tversion > nversion) {
  //   tsdbTrace("vgId:%d table:%s tid:%d server schema version %d is older than clien version %d, try to config.",
  //             pRepo->config.tsdbId, varDataVal(pTable->name), pTable->tableId.tid, nversion, tversion);
  //   void *msg = (*pRepo->appH.configFunc)(pRepo->config.tsdbId, pTable->tableId.tid);
  //   if (msg == NULL) {
  //     return terrno;
  //   }
  //   // Deal with error her
  //   STableCfg *pTableCfg = tsdbCreateTableCfgFromMsg(msg);
  //   STable *pTableUpdate = NULL;
  //   if (pTable->type == TSDB_CHILD_TABLE) {
  //     pTableUpdate = tsdbGetTableByUid(pMeta, pTableCfg->superUid);
  //   } else {
  //     pTableUpdate = pTable;
  //   }

  //   int32_t code = tsdbUpdateTable(pMeta, pTableUpdate, pTableCfg);
  //   if (code != TSDB_CODE_SUCCESS) {
  //     return code;
  //   }
  //   tsdbClearTableCfg(pTableCfg);
  //   rpcFreeCont(msg);
  // } else {
  //   if (tsdbGetTableSchemaByVersion(pMeta, pTable, tversion) == NULL) {
  //     tsdbError("vgId:%d table:%s tid:%d invalid schema version %d from client", pRepo->config.tsdbId,
  //               varDataVal(pTable->name), pTable->tableId.tid, tversion);
  //     return TSDB_CODE_TDB_TABLE_SCHEMA_VERSION;
  //   }
  // }

  SSubmitBlkIter blkIter = {0};
  SDataRow       row = NULL;

  TSKEY minKey = now - tsMsPerDay[pRepo->config.precision] * pRepo->config.keep;
  TSKEY maxKey = now + tsMsPerDay[pRepo->config.precision] * pRepo->config.daysPerFile;

  tsdbInitSubmitBlkIter(pBlock, &blkIter);
  while ((row = tsdbGetSubmitBlkNext(&blkIter)) != NULL) {
    if (dataRowKey(row) < minKey || dataRowKey(row) > maxKey) {
      tsdbError("vgId:%d table %s tid %d uid %ld timestamp is out of range! now " PRId64 " maxKey " PRId64
                " minKey " PRId64,
                REPO_ID(pRepo), TABLE_CHAR_NAME(pTable), TABLE_TID(pTable), TALBE_UID(pTable), now, minKey, maxKey);
      return TSDB_CODE_TDB_TIMESTAMP_OUT_OF_RANGE;
    }

    if (tdInsertRowToTable(pRepo, row, pTable) < 0) {
      return -1;
    }
    (*affectedrows)++;
    points++;
  }
  atomic_fetch_add_64(&(pRepo->stat.pointsWritten), points * (pSchema->numOfCols));
  atomic_fetch_add_64(&(pRepo->stat.totalStorage), points * pSchema->vlen);

  return 0;
}

static SSubmitBlk *tsdbGetSubmitMsgNext(SSubmitMsgIter *pIter) {
  SSubmitBlk *pBlock = pIter->pBlock;
  if (pBlock == NULL) return NULL;

  pBlock->len = htonl(pBlock->len);
  pBlock->numOfRows = htons(pBlock->numOfRows);
  pBlock->uid = htobe64(pBlock->uid);
  pBlock->tid = htonl(pBlock->tid);

  pBlock->sversion = htonl(pBlock->sversion);
  pBlock->padding = htonl(pBlock->padding);

  pIter->len = pIter->len + sizeof(SSubmitBlk) + pBlock->len;
  if (pIter->len >= pIter->totalLen) {
    pIter->pBlock = NULL;
  } else {
    pIter->pBlock = (SSubmitBlk *)((char *)pBlock + pBlock->len + sizeof(SSubmitBlk));
  }

  return pBlock;
}

static SDataRow tsdbGetSubmitBlkNext(SSubmitBlkIter *pIter) {
  SDataRow row = pIter->row;
  if (row == NULL) return NULL;

  pIter->len += dataRowLen(row);
  if (pIter->len >= pIter->totalLen) {
    pIter->row = NULL;
  } else {
    pIter->row = (char *)row + dataRowLen(row);
  }

  return row;
}

static int32_t tsdbInsertRowToTable(STsdbRepo *pRepo, SDataRow row, STable *pTable) {
  // TODO
  int32_t level = 0;
  int32_t headSize = 0;

  if (pTable->mem == NULL) {
    pTable->mem = (SMemTable *)calloc(1, sizeof(SMemTable));
    if (pTable->mem == NULL) return -1;
    pTable->mem->pData =
        tSkipListCreate(5, TSDB_DATA_TYPE_TIMESTAMP, TYPE_BYTES[TSDB_DATA_TYPE_TIMESTAMP], 0, 0, 0, getTSTupleKey);
    pTable->mem->keyFirst = INT64_MAX;
    pTable->mem->keyLast = 0;
  }

  tSkipListNewNodeInfo(pTable->mem->pData, &level, &headSize);

  TSKEY key = dataRowKey(row);
  // printf("insert:%lld, size:%d\n", key, pTable->mem->numOfRows);

  // Copy row into the memory
  SSkipListNode *pNode = tsdbAllocFromCache(pRepo->tsdbCache, headSize + dataRowLen(row), key);
  if (pNode == NULL) {
    // TODO: deal with allocate failure
  }

  pNode->level = level;
  dataRowCpy(SL_GET_NODE_DATA(pNode), row);

  // Insert the skiplist node into the data
  if (pTable->mem == NULL) {
    pTable->mem = (SMemTable *)calloc(1, sizeof(SMemTable));
    if (pTable->mem == NULL) return -1;
    pTable->mem->pData =
        tSkipListCreate(5, TSDB_DATA_TYPE_TIMESTAMP, TYPE_BYTES[TSDB_DATA_TYPE_TIMESTAMP], 0, 0, 0, getTSTupleKey);
    pTable->mem->keyFirst = INT64_MAX;
    pTable->mem->keyLast = 0;
  }
  tSkipListPut(pTable->mem->pData, pNode);
  if (key > pTable->mem->keyLast) pTable->mem->keyLast = key;
  if (key < pTable->mem->keyFirst) pTable->mem->keyFirst = key;
  if (key > pTable->lastKey) pTable->lastKey = key;

  pTable->mem->numOfRows = tSkipListGetSize(pTable->mem->pData);

  tsdbTrace("vgId:%d, tid:%d, uid:%" PRId64 ", table:%s a row is inserted to table! key:%" PRId64, pRepo->config.tsdbId,
            pTable->tableId.tid, pTable->tableId.uid, varDataVal(pTable->name), dataRowKey(row));

  return 0;
}

static SSkipListIterator **tsdbCreateTableIters(STsdbRepo *pRepo) {
  STsdbCfg *pCfg = &(pRepo->config);

  SSkipListIterator **iters = (SSkipListIterator **)calloc(pCfg->maxTables, sizeof(SSkipListIterator *));
  if (iters == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  for (int tid = 1; tid < maxTables; tid++) {
    STable *pTable = pMeta->tables[tid];
    if (pTable == NULL || pTable->imem == NULL || pTable->imem->numOfRows == 0) continue;

    iters[tid] = tSkipListCreateIter(pTable->imem->pData);
    if (iters[tid] == NULL) goto _err;

    if (!tSkipListIterNext(iters[tid])) goto _err;
  }

  return iters;

_err:
  tsdbDestroyTableIters(iters, maxTables);
  return NULL;
}

static int tsdbCommitToFile(STsdbRepo *pRepo, int fid, SSkipListIterator **iters, SRWHelper *pHelper,
                            SDataCols *pDataCols) {
  char        dataDir[128] = {0};
  STsdbMeta * pMeta = pRepo->tsdbMeta;
  STsdbFileH *pFileH = pRepo->tsdbFileH;
  STsdbCfg *  pCfg = &pRepo->config;
  SFileGroup *pGroup = NULL;

  TSKEY minKey = 0, maxKey = 0;
  tsdbGetKeyRangeOfFileId(pCfg->daysPerFile, pCfg->precision, fid, &minKey, &maxKey);

  // Check if there are data to commit to this file
  int hasDataToCommit = tsdbHasDataToCommit(iters, pCfg->maxTables, minKey, maxKey);
  if (!hasDataToCommit) return 0;  // No data to commit, just return

  // Create and open files for commit
  tsdbGetDataDirName(pRepo, dataDir);
  if ((pGroup = tsdbCreateFGroup(pFileH, dataDir, fid, pCfg->maxTables)) == NULL) {
    tsdbError("vgId:%d, failed to create file group %d", pRepo->config.tsdbId, fid);
    goto _err;
  }

  // Open files for write/read
  if (tsdbSetAndOpenHelperFile(pHelper, pGroup) < 0) {
    tsdbError("vgId:%d, failed to set helper file", pRepo->config.tsdbId);
    goto _err;
  }

  // Loop to commit data in each table
  for (int tid = 1; tid < pCfg->maxTables; tid++) {
    STable *pTable = pMeta->tables[tid];
    if (pTable == NULL) continue;

    SSkipListIterator *pIter = iters[tid];

    // Set the helper and the buffer dataCols object to help to write this table
    tsdbSetHelperTable(pHelper, pTable, pRepo);
    tdInitDataCols(pDataCols, tsdbGetTableSchema(pMeta, pTable));

    // Loop to write the data in the cache to files. If no data to write, just break the loop
    int maxRowsToRead = pCfg->maxRowsPerFileBlock * 4 / 5;
    int nLoop = 0;
    while (true) {
      int rowsRead = tsdbReadRowsFromCache(pMeta, pTable, pIter, maxKey, maxRowsToRead, pDataCols);
      assert(rowsRead >= 0);
      if (pDataCols->numOfRows == 0) break;
      nLoop++;

      ASSERT(dataColsKeyFirst(pDataCols) >= minKey && dataColsKeyFirst(pDataCols) <= maxKey);
      ASSERT(dataColsKeyLast(pDataCols) >= minKey && dataColsKeyLast(pDataCols) <= maxKey);

      int rowsWritten = tsdbWriteDataBlock(pHelper, pDataCols);
      ASSERT(rowsWritten != 0);
      if (rowsWritten < 0) goto _err;
      ASSERT(rowsWritten <= pDataCols->numOfRows);

      tdPopDataColsPoints(pDataCols, rowsWritten);
      maxRowsToRead = pCfg->maxRowsPerFileBlock * 4 / 5 - pDataCols->numOfRows;
    }

    ASSERT(pDataCols->numOfRows == 0);

    // Move the last block to the new .l file if neccessary
    if (tsdbMoveLastBlockIfNeccessary(pHelper) < 0) {
      tsdbError("vgId:%d, failed to move last block", pRepo->config.tsdbId);
      goto _err;
    }

    // Write the SCompBlock part
    if (tsdbWriteCompInfo(pHelper) < 0) {
      tsdbError("vgId:%d, failed to write compInfo part", pRepo->config.tsdbId);
      goto _err;
    }
  }

  if (tsdbWriteCompIdx(pHelper) < 0) {
    tsdbError("vgId:%d, failed to write compIdx part", pRepo->config.tsdbId);
    goto _err;
  }

  tsdbCloseHelperFile(pHelper, 0);
  // TODO: make it atomic with some methods
  pGroup->files[TSDB_FILE_TYPE_HEAD] = pHelper->files.headF;
  pGroup->files[TSDB_FILE_TYPE_DATA] = pHelper->files.dataF;
  pGroup->files[TSDB_FILE_TYPE_LAST] = pHelper->files.lastF;

  return 0;

_err:
  ASSERT(false);
  tsdbCloseHelperFile(pHelper, 1);
  return -1;
}

#if 0
**
 * Set the default TSDB configuration
 */

static int tsdbRestoreInfo(STsdbRepo *pRepo) {
  STsdbMeta * pMeta = pRepo->tsdbMeta;
  STsdbFileH *pFileH = pRepo->tsdbFileH;
  SFileGroup *pFGroup = NULL;

  SFileGroupIter iter;
  SRWHelper      rhelper = {{0}};

  if (tsdbInitReadHelper(&rhelper, pRepo) < 0) goto _err;
  tsdbInitFileGroupIter(pFileH, &iter, TSDB_ORDER_ASC);
  while ((pFGroup = tsdbGetFileGroupNext(&iter)) != NULL) {
    if (tsdbSetAndOpenHelperFile(&rhelper, pFGroup) < 0) goto _err;
    for (int i = 1; i < pRepo->config.maxTables; i++) {
      STable *  pTable = pMeta->tables[i];
      if (pTable == NULL) continue;
      SCompIdx *pIdx = &rhelper.pCompIdx[i];

      if (pIdx->offset > 0 && pTable->lastKey < pIdx->maxKey) pTable->lastKey = pIdx->maxKey;
    }
  }

  tsdbDestroyHelper(&rhelper);
  return 0;

_err:
  tsdbDestroyHelper(&rhelper);
  return -1;
}

/**
 * Change the configuration of a repository
 * @param pCfg the repository configuration, the upper layer should free the pointer
 *
 * @return 0 for success, -1 for failure and the error number is set
 */
int32_t tsdbConfigRepo(TSDB_REPO_T *repo, STsdbCfg *pCfg) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;
  STsdbCfg * pRCfg = &pRepo->config;

  if (tsdbCheckAndSetDefaultCfg(pCfg) < 0) return TSDB_CODE_TDB_INVALID_CONFIG;

  ASSERT(pRCfg->tsdbId == pCfg->tsdbId);
  ASSERT(pRCfg->cacheBlockSize == pCfg->cacheBlockSize);
  ASSERT(pRCfg->daysPerFile == pCfg->daysPerFile);
  ASSERT(pRCfg->minRowsPerFileBlock == pCfg->minRowsPerFileBlock);
  ASSERT(pRCfg->maxRowsPerFileBlock == pCfg->maxRowsPerFileBlock);
  ASSERT(pRCfg->precision == pCfg->precision);

  bool configChanged = false;
  if (pRCfg->compression != pCfg->compression) {
    configChanged = true;
    tsdbAlterCompression(pRepo, pCfg->compression);
  }
  if (pRCfg->keep != pCfg->keep) {
    configChanged = true;
    tsdbAlterKeep(pRepo, pCfg->keep);
  }
  if (pRCfg->totalBlocks != pCfg->totalBlocks) {
    configChanged = true;
    tsdbAlterCacheTotalBlocks(pRepo, pCfg->totalBlocks);
  }
  if (pRCfg->maxTables != pCfg->maxTables) {
    configChanged = true;
    tsdbAlterMaxTables(pRepo, pCfg->maxTables);
  }

  if (configChanged) tsdbSaveConfig(pRepo);

  return TSDB_CODE_SUCCESS;
}


/**
 * Get the TSDB repository information, including some statistics
 * @param pRepo the TSDB repository handle
 * @param error the error number to set when failure occurs
 *
 * @return a info struct handle on success, NULL for failure and the error number is set. The upper
 *         layers should free the info handle themselves or memory leak will occur
 */
STsdbRepoInfo *tsdbGetStatus(TSDB_REPO_T *pRepo) {
  // TODO
  return NULL;
}


TSKEY tsdbGetTableLastKey(TSDB_REPO_T *repo, uint64_t uid) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;

  STable *pTable = tsdbGetTableByUid(pRepo->tsdbMeta, uid);
  if (pTable == NULL) return -1;

  return TSDB_GET_TABLE_LAST_KEY(pTable);
}


STableInfo *tsdbGetTableInfo(TSDB_REPO_T *pRepo, STableId tableId) {
  // TODO
  return NULL;
}

// TODO: need to return the number of data inserted

void tsdbClearTableCfg(STableCfg *config) {
  if (config) {
    if (config->schema) tdFreeSchema(config->schema);
    if (config->tagSchema) tdFreeSchema(config->tagSchema);
    if (config->tagValues) kvRowFree(config->tagValues);
    tfree(config->name);
    tfree(config->sname);
    tfree(config->sql);
    free(config);
  }
}

int tsdbInitSubmitBlkIter(SSubmitBlk *pBlock, SSubmitBlkIter *pIter) {
  if (pBlock->len <= 0) return -1;
  pIter->totalLen = pBlock->len;
  pIter->len = 0;
  pIter->row = (SDataRow)(pBlock->data);
  return 0;
}




STsdbMeta* tsdbGetMeta(TSDB_REPO_T* pRepo) {
  STsdbRepo *tsdb = (STsdbRepo *)pRepo;
  return tsdb->tsdbMeta;
}

STsdbFileH* tsdbGetFile(TSDB_REPO_T* pRepo) {
  STsdbRepo* tsdb = (STsdbRepo*) pRepo;
  return tsdb->tsdbFileH;
}

// Check the configuration and set default options



static int32_t tsdbRestoreCfg(STsdbRepo *pRepo, STsdbCfg *pCfg) {
  char fname[128] = "\0";

  if (tsdbGetCfgFname(pRepo, fname) < 0) return -1;

  int fd = open(fname, O_RDONLY);
  if (fd < 0) {
    return -1;
  }

  if (read(fd, (void *)pCfg, sizeof(STsdbCfg)) < sizeof(STsdbCfg)) {
    close(fd);
    return -1;
  }

  close(fd);

  return 0;
}



static int32_t tsdbDestroyRepoEnv(STsdbRepo *pRepo) {
  char fname[260];
  if (pRepo == NULL) return 0;
  char *dirName = calloc(1, strlen(pRepo->rootDir) + strlen("tsdb") + 2);
  if (dirName == NULL) {
    return -1;
  }

  sprintf(dirName, "%s/%s", pRepo->rootDir, "tsdb");

  DIR *dir = opendir(dirName);
  if (dir == NULL) return -1;

  struct dirent *dp;
  while ((dp = readdir(dir)) != NULL) {
    if ((strcmp(dp->d_name, ".") == 0) || (strcmp(dp->d_name, "..") == 0)) continue;
    sprintf(fname, "%s/%s", pRepo->rootDir, dp->d_name);
    remove(fname);
  }

  closedir(dir);

  rmdir(dirName);

  return 0;
}


static int tsdbReadRowsFromCache(STsdbMeta *pMeta, STable *pTable, SSkipListIterator *pIter, TSKEY maxKey, int maxRowsToRead, SDataCols *pCols) {
  ASSERT(maxRowsToRead > 0);
  if (pIter == NULL) return 0;
  STSchema *pSchema = NULL;

  int numOfRows = 0;

  do {
    if (numOfRows >= maxRowsToRead) break;

    SSkipListNode *node = tSkipListIterGet(pIter);
    if (node == NULL) break;

    SDataRow row = SL_GET_NODE_DATA(node);
    if (dataRowKey(row) > maxKey) break;

    if (pSchema == NULL || schemaVersion(pSchema) != dataRowVersion(row)) {
      pSchema = tsdbGetTableSchemaByVersion(pMeta, pTable, dataRowVersion(row));
      if (pSchema == NULL) {
        // TODO: deal with the error here
        ASSERT(false);
      }
    }

    tdAppendDataRowToDataCol(row, pSchema, pCols);
    numOfRows++;
  } while (tSkipListIterNext(pIter));

  return numOfRows;
}

static void tsdbDestroyTableIters(SSkipListIterator **iters, int maxTables) {
  if (iters == NULL) return;

  for (int tid = 1; tid < maxTables; tid++) {
    if (iters[tid] == NULL) continue;
    tSkipListDestroyIter(iters[tid]);
  }

  free(iters);
}


static void tsdbFreeMemTable(SMemTable *pMemTable) {
  if (pMemTable) {
    tSkipListDestroy(pMemTable->pData);
    free(pMemTable);
  }
}

// Commit to file


/**
 * Return the next iterator key.
 *
 * @return the next key if iter has
 *         -1 if iter not
 */
static TSKEY tsdbNextIterKey(SSkipListIterator *pIter) {
  if (pIter == NULL) return -1;

  SSkipListNode *node = tSkipListIterGet(pIter);
  if (node == NULL) return -1;

  SDataRow row = SL_GET_NODE_DATA(node);
  return dataRowKey(row);
}

static int tsdbHasDataToCommit(SSkipListIterator **iters, int nIters, TSKEY minKey, TSKEY maxKey) {
  TSKEY nextKey;
  for (int i = 0; i < nIters; i++) {
    SSkipListIterator *pIter = iters[i];
    nextKey = tsdbNextIterKey(pIter);
    if (nextKey > 0 && (nextKey >= minKey && nextKey <= maxKey)) return 1;
  }
  return 0;
}

static void tsdbAlterCompression(STsdbRepo *pRepo, int8_t compression) {
  int8_t oldCompRession = pRepo->config.compression;
  pRepo->config.compression = compression;
  tsdbTrace("vgId:%d, tsdb compression is changed from %d to %d", oldCompRession, compression);
}

static void tsdbAlterKeep(STsdbRepo *pRepo, int32_t keep) {
  STsdbCfg *pCfg = &pRepo->config;
  int oldKeep = pCfg->keep;

  int maxFiles = keep / pCfg->maxTables + 3;
  if (pRepo->config.keep > keep) {
    pRepo->config.keep = keep;
    pRepo->tsdbFileH->maxFGroups = maxFiles;
  } else {
    pRepo->config.keep = keep;
    pRepo->tsdbFileH->fGroup = realloc(pRepo->tsdbFileH->fGroup, sizeof(SFileGroup));
    if (pRepo->tsdbFileH->fGroup == NULL) {
      // TODO: deal with the error
    }
    pRepo->tsdbFileH->maxFGroups = maxFiles;
  }
  tsdbTrace("vgId:%d, keep is changed from %d to %d", pRepo->config.tsdbId, oldKeep, keep);
}

static void tsdbAlterMaxTables(STsdbRepo *pRepo, int32_t maxTables) {
  int oldMaxTables = pRepo->config.maxTables;
  if (oldMaxTables < pRepo->config.maxTables) {
    // TODO
  }

  STsdbMeta *pMeta = pRepo->tsdbMeta;

  pMeta->maxTables = maxTables;
  pMeta->tables = realloc(pMeta->tables, maxTables * sizeof(STable *));
  memset(&pMeta->tables[oldMaxTables], 0, sizeof(STable *) * (maxTables-oldMaxTables));
  pRepo->config.maxTables = maxTables;

  tsdbTrace("vgId:%d, tsdb maxTables is changed from %d to %d!", pRepo->config.tsdbId, oldMaxTables, maxTables);
}


void tsdbReportStat(void *repo, int64_t *totalPoints, int64_t *totalStorage, int64_t *compStorage){
    ASSERT(repo != NULL);
    STsdbRepo * pRepo = repo;
    *totalPoints = pRepo->stat.pointsWritten;
    *totalStorage = pRepo->stat.totalStorage;
    *compStorage = pRepo->stat.compStorage;
}

#endif