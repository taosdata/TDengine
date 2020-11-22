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
#define _DEFAULT_SOURCE
#define TAOS_RANDOM_FILE_FAIL_TEST
#include <regex.h>
#include "os.h"
#include "talgo.h"
#include "tchecksum.h"
#include "tsdbMain.h"
#include "tutil.h"
#include "tfs.h"


const char *tsdbFileSuffix[] = {".head", ".data", ".last", ".stat", ".h", ".d", ".l", ".s"};


// ---------------- INTERNAL FUNCTIONS ----------------
STsdbFileH *tsdbNewFileH(STsdbCfg *pCfg) {
  STsdbFileH *pFileH = (STsdbFileH *)calloc(1, sizeof(*pFileH));
  if (pFileH == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  int code = pthread_rwlock_init(&(pFileH->fhlock), NULL);
  if (code != 0) {
    tsdbError("vgId:%d failed to init file handle lock since %s", pCfg->tsdbId, strerror(code));
    terrno = TAOS_SYSTEM_ERROR(code);
    goto _err;
  }

  pFileH->maxFGroups = TSDB_MAX_FILE(pCfg->keep, pCfg->daysPerFile);

  pFileH->pFGroup = (SFileGroup *)calloc(pFileH->maxFGroups, sizeof(SFileGroup));
  if (pFileH->pFGroup == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  return pFileH;

_err:
  tsdbFreeFileH(pFileH);
  return NULL;
}

void tsdbFreeFileH(STsdbFileH *pFileH) {
  if (pFileH) {
    pthread_rwlock_destroy(&pFileH->fhlock);
    tfree(pFileH->pFGroup);
    free(pFileH);
  }
}

int tsdbOpenFileH(STsdbRepo *pRepo) {
  ASSERT(pRepo != NULL && pRepo->tsdbFileH != NULL);
  char dataDir[TSDB_FILENAME_LEN] = "\0";

  // 1. scan and get all files corresponds
  TFSDIR *tdir = NULL;
  char    fname[TSDB_FILENAME_LEN] = "\0";
  regex_t regex = {0};
  int     code = 0;
  int     vid = 0;
  int     fid = 0;

  const TFSFILE *pfile = NULL;

  code = regcomp(&regex, "^v[0-9]+f[0-9]+\\.(head|data|last|h|d|l)$", REG_EXTENDED);
  if (code != 0) {
    // TODO: deal the error
  }

  snprintf(dataDir, TSDB_FILENAME_LEN, "vnode/vnode%d/tsdb/data", REPO_ID(pRepo));
  tdir = tfsOpenDir(dataDir);
  if (tdir == NULL) {
    // TODO: deal the error
  }

  while ((pfile = tfsReadDir(tdir)) != NULL) {
    tfsBaseName(pfile, fname);

    if (strcmp(fname, ".") == 0 || strcmp(fname, "..") == 0) continue;

    code = regexec(&regex, fname, 0, NULL, 0);
    if (code == 0) {
      sscanf(fname, "v%df%d", &vid, &fid);

      if (vid != REPO_ID(pRepo)) {
        tfsAbsName(pfile, fname);
        tsdbError("vgId:%d invalid file %s exists, ignore", REPO_ID(pRepo), fname);
        continue;
      }

      // TODO 
      {}
    } else if (code == REG_NOMATCH) {
      tfsAbsName(pfile, fname);
      tsdbWarn("vgId:%d unrecognizable file %s exists, ignore", REPO_ID(pRepo), fname);
      continue;
    } else {
      tsdbError("vgId:%d regexec failed since %s", REPO_ID(pRepo), strerror(code));
      // TODO: deal with error
    }
  }

  // 2. Sort all files according to fid

  // 3. Recover all files of each fid
  while (true) {
    // TODO
  }

  return 0;
}

void tsdbCloseFileH(STsdbRepo *pRepo) {
  STsdbFileH *pFileH = pRepo->tsdbFileH;

  for (int i = 0; i < pFileH->nFGroups; i++) {
    SFileGroup *pFGroup = pFileH->pFGroup + i;
    for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
      tsdbDestroyFile(&pFGroup->files[type]);
    }
  }
}

SFileGroup *tsdbCreateFGroup(STsdbRepo *pRepo, int fid) {
  STsdbFileH *pFileH = pRepo->tsdbFileH;
  SFileGroup  fGroup = {0};

  ASSERT(tsdbSearchFGroup(pFileH, fid, TD_EQ) == NULL);

  // TODO: think about if (level == 0) is correct
  SDisk *pDisk = tdAssignDisk(tsDnodeTier, 0);
  if (pDisk == NULL) {
    tsdbError("vgId:%d failed to create file group %d since %s", REPO_ID(pRepo), fid, tstrerror(terrno));
    return NULL;
  }

  fGroup.fileId = fid;
  fGroup.level = pDisk->level;
  fGroup.did = pDisk->did;
  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    if (tsdbCreateFile(&(fGroup.files[type]), pRepo, fid, type, pDisk) < 0) goto _err;
  }

  pthread_rwlock_wrlock(&pFileH->fhlock);
  pFileH->pFGroup[pFileH->nFGroups++] = fGroup;
  qsort((void *)(pFileH->pFGroup), pFileH->nFGroups, sizeof(SFileGroup), compFGroup);
  pthread_rwlock_unlock(&pFileH->fhlock);
  return tsdbSearchFGroup(pFileH, fid, TD_EQ);

_err:
  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    tsdbDestroyFile(&(fGroup.files[type]));
  }
  tdDecDiskFiles(tsDnodeTier, pDisk, true);
  return NULL;
}

void tsdbInitFileGroupIter(STsdbFileH *pFileH, SFileGroupIter *pIter, int direction) {
  pIter->pFileH = pFileH;
  pIter->direction = direction;

  if (pFileH->nFGroups == 0) {
    pIter->index = -1;
    pIter->fileId = -1;
  } else {
    if (direction == TSDB_FGROUP_ITER_FORWARD) {
      pIter->index = 0;
    } else {
      pIter->index = pFileH->nFGroups - 1;
    }
    pIter->fileId = pFileH->pFGroup[pIter->index].fileId;
  }
}

void tsdbSeekFileGroupIter(SFileGroupIter *pIter, int fid) {
  STsdbFileH *pFileH = pIter->pFileH;

  if (pFileH->nFGroups == 0) {
    pIter->index = -1;
    pIter->fileId = -1;
    return;
  }

  int   flags = (pIter->direction == TSDB_FGROUP_ITER_FORWARD) ? TD_GE : TD_LE;
  void *ptr = taosbsearch(&fid, (void *)pFileH->pFGroup, pFileH->nFGroups, sizeof(SFileGroup), keyFGroupCompFunc, flags);
  if (ptr == NULL) {
    pIter->index = -1;
    pIter->fileId = -1;
  } else {
    pIter->index = (int)(POINTER_DISTANCE(ptr, pFileH->pFGroup) / sizeof(SFileGroup));
    pIter->fileId = ((SFileGroup *)ptr)->fileId;
  }
}

SFileGroup *tsdbGetFileGroupNext(SFileGroupIter *pIter) {
  STsdbFileH *pFileH = pIter->pFileH;
  SFileGroup *pFGroup = NULL;

  if (pIter->index < 0 || pIter->index >= pFileH->nFGroups || pIter->fileId < 0) return NULL;

  pFGroup = &pFileH->pFGroup[pIter->index];
  if (pFGroup->fileId != pIter->fileId) {
    tsdbSeekFileGroupIter(pIter, pIter->fileId);
  }

  if (pIter->index < 0) return NULL;

  pFGroup = &pFileH->pFGroup[pIter->index];
  ASSERT(pFGroup->fileId == pIter->fileId);

  if (pIter->direction == TSDB_FGROUP_ITER_FORWARD) {
    pIter->index++;
  } else {
    pIter->index--;
  }

  if (pIter->index >= 0 && pIter->index < pFileH->nFGroups) {
    pIter->fileId = pFileH->pFGroup[pIter->index].fileId;
  } else {
    pIter->fileId = -1;
  }

  return pFGroup;
}

int tsdbOpenFile(SFile *pFile, int oflag) {
  ASSERT(!TSDB_IS_FILE_OPENED(pFile));

  pFile->fd = open(pFile->fname, oflag, 0755);
  if (pFile->fd < 0) {
    tsdbError("failed to open file %s since %s", pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  tsdbTrace("open file %s, fd %d", pFile->fname, pFile->fd);

  return 0;
}

void tsdbCloseFile(SFile *pFile) {
  if (TSDB_IS_FILE_OPENED(pFile)) {
    tsdbTrace("close file %s, fd %d", pFile->fname, pFile->fd);
    close(pFile->fd);
    pFile->fd = -1;
  }
}

int tsdbCreateFile(SFile *pFile, STsdbRepo *pRepo, int fid, int type, SDisk *pDisk) {
  memset((void *)pFile, 0, sizeof(SFile));
  pFile->fd = -1;

  tsdbGetDataFileName(pRepo->rootDir, REPO_ID(pRepo), fid, type, pFile->fname);

  if (access(pFile->fname, F_OK) == 0) {
    tsdbError("vgId:%d file %s already exists", REPO_ID(pRepo), pFile->fname);
    terrno = TSDB_CODE_TDB_FILE_ALREADY_EXISTS;
    goto _err;
  }

  if (tsdbOpenFile(pFile, O_RDWR | O_CREAT) < 0) {
    goto _err;
  }

  pFile->info.size = TSDB_FILE_HEAD_SIZE;
  pFile->info.magic = TSDB_FILE_INIT_MAGIC;

  if (tsdbUpdateFileHeader(pFile) < 0) {
    tsdbCloseFile(pFile);
    return -1;
  }

  tsdbCloseFile(pFile);

  return 0;

_err:
  return -1;
}

SFileGroup *tsdbSearchFGroup(STsdbFileH *pFileH, int fid, int flags) {
  void *ptr =
      taosbsearch((void *)(&fid), (void *)(pFileH->pFGroup), pFileH->nFGroups, sizeof(SFileGroup), keyFGroupCompFunc, flags);
  if (ptr == NULL) return NULL;
  return (SFileGroup *)ptr;
}

void tsdbRemoveFilesBeyondRetention(STsdbRepo *pRepo, SFidGroup *pFidGroup) {
  STsdbFileH *pFileH = pRepo->tsdbFileH;
  SFileGroup *pGroup = pFileH->pFGroup;

  pthread_rwlock_wrlock(&(pFileH->fhlock));

  while (pFileH->nFGroups > 0 && pGroup[0].fileId < pFidGroup->minFid) {
    tsdbRemoveFileGroup(pRepo, pGroup);
  }

  pthread_rwlock_unlock(&(pFileH->fhlock));
}

int tsdbUpdateFileHeader(SFile *pFile) {
  char buf[TSDB_FILE_HEAD_SIZE] = "\0";

  void *pBuf = (void *)buf;
  taosEncodeFixedU32((void *)(&pBuf), TSDB_FILE_VERSION);
  tsdbEncodeSFileInfo((void *)(&pBuf), &(pFile->info));

  taosCalcChecksumAppend(0, (uint8_t *)buf, TSDB_FILE_HEAD_SIZE);

  if (lseek(pFile->fd, 0, SEEK_SET) < 0) {
    tsdbError("failed to lseek file %s since %s", pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
  if (taosWrite(pFile->fd, (void *)buf, TSDB_FILE_HEAD_SIZE) < TSDB_FILE_HEAD_SIZE) {
    tsdbError("failed to write %d bytes to file %s since %s", TSDB_FILE_HEAD_SIZE, pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

int tsdbEncodeSFileInfo(void **buf, const STsdbFileInfo *pInfo) {
  int tlen = 0;
  tlen += taosEncodeFixedU32(buf, pInfo->magic);
  tlen += taosEncodeFixedU32(buf, pInfo->len);
  tlen += taosEncodeFixedU32(buf, pInfo->totalBlocks);
  tlen += taosEncodeFixedU32(buf, pInfo->totalSubBlocks);
  tlen += taosEncodeFixedU32(buf, pInfo->offset);
  tlen += taosEncodeFixedU64(buf, pInfo->size);
  tlen += taosEncodeFixedU64(buf, pInfo->tombSize);

  return tlen;
}

void *tsdbDecodeSFileInfo(void *buf, STsdbFileInfo *pInfo) {
  buf = taosDecodeFixedU32(buf, &(pInfo->magic));
  buf = taosDecodeFixedU32(buf, &(pInfo->len));
  buf = taosDecodeFixedU32(buf, &(pInfo->totalBlocks));
  buf = taosDecodeFixedU32(buf, &(pInfo->totalSubBlocks));
  buf = taosDecodeFixedU32(buf, &(pInfo->offset));
  buf = taosDecodeFixedU64(buf, &(pInfo->size));
  buf = taosDecodeFixedU64(buf, &(pInfo->tombSize));

  return buf;
}

void tsdbRemoveFileGroup(STsdbRepo *pRepo, SFileGroup *pFGroup) {
  ASSERT(pFGroup != NULL);
  STsdbFileH *pFileH = pRepo->tsdbFileH;
  SDisk *     pDisk = NULL;
  char        baseDir[TSDB_FILENAME_LEN] = "\0";

  SFileGroup fileGroup = *pFGroup;
  tsdbGetBaseDirFromFile(fileGroup.files[0].fname, baseDir);
  pDisk = tdGetDiskByName(tsDnodeTier, baseDir);
  ASSERT(pDisk != NULL);

  int nFilesLeft = pFileH->nFGroups - (int)(POINTER_DISTANCE(pFGroup, pFileH->pFGroup) / sizeof(SFileGroup) + 1);
  if (nFilesLeft > 0) {
    memmove((void *)pFGroup, POINTER_SHIFT(pFGroup, sizeof(SFileGroup)), sizeof(SFileGroup) * nFilesLeft);
  }

  pFileH->nFGroups--;
  ASSERT(pFileH->nFGroups >= 0);

  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    if (remove(fileGroup.files[type].fname) < 0) {
      tsdbError("vgId:%d failed to remove file %s", REPO_ID(pRepo), fileGroup.files[type].fname);
    }
    tsdbDestroyFile(&fileGroup.files[type]);
  }

  tdDecDiskFiles(tsDnodeTier, pDisk, true);
}

int tsdbLoadFileHeader(SFile *pFile, uint32_t *version) {
  char buf[TSDB_FILE_HEAD_SIZE] = "\0";

  if (lseek(pFile->fd, 0, SEEK_SET) < 0) {
    tsdbError("failed to lseek file %s to start since %s", pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosRead(pFile->fd, buf, TSDB_FILE_HEAD_SIZE) < TSDB_FILE_HEAD_SIZE) {
    tsdbError("failed to read file %s header part with %d bytes, reason:%s", pFile->fname, TSDB_FILE_HEAD_SIZE,
              strerror(errno));
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    return -1;
  }

  if (!taosCheckChecksumWhole((uint8_t *)buf, TSDB_FILE_HEAD_SIZE)) {
    tsdbError("file %s header part is corrupted with failed checksum", pFile->fname);
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    return -1;
  }

  void *pBuf = (void *)buf;
  pBuf = taosDecodeFixedU32(pBuf, version);
  pBuf = tsdbDecodeSFileInfo(pBuf, &(pFile->info));

  return 0;
}

void tsdbGetFileInfoImpl(char *fname, uint32_t *magic, int64_t *size) {
  uint32_t      version = 0;
  SFile         file;
  SFile *       pFile = &file;

  strncpy(pFile->fname, fname, TSDB_FILENAME_LEN - 1);
  pFile->fd = -1;

  if (tsdbOpenFile(pFile, O_RDONLY) < 0) goto _err;
  if (tsdbLoadFileHeader(pFile, &version) < 0) goto _err;

  off_t offset = lseek(pFile->fd, 0, SEEK_END);
  if (offset < 0) goto _err;
  tsdbCloseFile(pFile);

  *magic = pFile->info.magic;
  *size = offset;

  return;

_err:
  tsdbCloseFile(pFile);
  *magic = TSDB_FILE_INIT_MAGIC;
  *size = 0;
}

void tsdbGetFidGroup(STsdbCfg *pCfg, SFidGroup *pFidGroup) {
  TSKEY now = taosGetTimestamp(pCfg->precision);

  pFidGroup->minFid =
      TSDB_KEY_FILEID(now - pCfg->keep * tsMsPerDay[pCfg->precision], pCfg->daysPerFile, pCfg->precision);
  pFidGroup->midFid =
      TSDB_KEY_FILEID(now - pCfg->keep2 * tsMsPerDay[pCfg->precision], pCfg->daysPerFile, pCfg->precision);
  pFidGroup->maxFid =
      TSDB_KEY_FILEID(now - pCfg->keep1 * tsMsPerDay[pCfg->precision], pCfg->daysPerFile, pCfg->precision);
}

int tsdbGetBaseDirFromFile(char *fname, char *baseDir) {
  char *fdup = strdup(fname);
  if (fdup == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  for (size_t i = 0; i < 5; i++) {
    dirname(fdup);
  }

  strncpy(baseDir, fdup, TSDB_FILENAME_LEN);
  free(fdup);
  return 0;
}

int tsdbApplyRetention(STsdbRepo *pRepo, SFidGroup *pFidGroup) {
  STsdbFileH *pFileH = pRepo->tsdbFileH;
  SFileGroup *pGroup = NULL;
  SFileGroup  nFileGroup = {0};
  SFileGroup  oFileGroup = {0};
  int         level = 0;

  if (tsDnodeTier->nTiers == 1 || (pFidGroup->minFid == pFidGroup->midFid && pFidGroup->midFid == pFidGroup->maxFid)) {
    return 0;
  }

  for (int gidx = pFileH->nFGroups - 1; gidx >= 0; gidx--) {
    pGroup = pFileH->pFGroup + gidx;

    level = tsdbGetFidLevel(pGroup->fileId, pFidGroup);

    if (level == pGroup->level) continue;
    if (level > pGroup->level && level < tsDnodeTier->nTiers) {
      SDisk *pODisk = tdGetDisk(tsDnodeTier, pGroup->level, pGroup->did);
      SDisk *pDisk = tdAssignDisk(tsDnodeTier, level);
      tsdbCreateVnodeDataDir(pDisk->dir, REPO_ID(pRepo));
      oFileGroup = *pGroup;
      nFileGroup = *pGroup;
      nFileGroup.level = level;
      nFileGroup.did = pDisk->did;

      char tsdbRootDir[TSDB_FILENAME_LEN];
      tdGetTsdbRootDir(pDisk->dir, REPO_ID(pRepo), tsdbRootDir);
      for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
        tsdbGetDataFileName(tsdbRootDir, REPO_ID(pRepo), pGroup->fileId, type, nFileGroup.files[type].fname);
      }

      for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
        if (taosCopy(oFileGroup.files[type].fname, nFileGroup.files[type].fname) < 0) return -1;
      }

      pthread_rwlock_wrlock(&(pFileH->fhlock)); 
      *pGroup = nFileGroup;
      pthread_rwlock_unlock(&(pFileH->fhlock));

      for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
        (void)remove(oFileGroup.files[type].fname);
      }

      tdLockTiers(tsDnodeTier);
      tdDecDiskFiles(tsDnodeTier, pODisk, false);
      tdIncDiskFiles(tsDnodeTier, pDisk, false);
      tdUnLockTiers(tsDnodeTier);
    }
  }

  return 0;
}

// ---------------- LOCAL FUNCTIONS ----------------
static void tsdbDestroyFile(SFile *pFile) { tsdbCloseFile(pFile); }

static int compFGroup(const void *arg1, const void *arg2) {
  int val1 = ((SFileGroup *)arg1)->fileId;
  int val2 = ((SFileGroup *)arg2)->fileId;

  if (val1 < val2) {
    return -1;
  } else if (val1 > val2) {
    return 1;
  } else {
    return 0;
  }
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

// static int tsdbLoadFilesFromDisk(STsdbRepo *pRepo, SDisk *pDisk) {
//   char                  tsdbDataDir[TSDB_FILENAME_LEN] = "\0";
//   char                  tsdbRootDir[TSDB_FILENAME_LEN] = "\0";
//   char                  fname[TSDB_FILENAME_LEN] = "\0";
//   SHashObj *            pFids = NULL;
//   SHashMutableIterator *pIter = NULL;
//   STsdbFileH *          pFileH = pRepo->tsdbFileH;
//   SFileGroup            fgroup = {0};
//   STsdbCfg *            pCfg = &(pRepo->config);
//   SFidGroup             fidGroup = {0};
//   int                   mfid = 0;

//   tdGetTsdbRootDir(pDisk->dir, REPO_ID(pRepo), tsdbRootDir);
//   tdGetTsdbDataDir(pDisk->dir, REPO_ID(pRepo), tsdbDataDir);

//   pFids = tsdbGetAllFids(pRepo, tsdbDataDir);
//   if (pFids == NULL) {
//     goto _err;
//   }

//   pIter = taosHashCreateIter(pFids);
//   if (pIter == NULL) {
//     goto _err;
//   }

//   tsdbGetFidGroup(pCfg, &fidGroup);
//   mfid = fidGroup.minFid;

//   while (taosHashIterNext(pIter)) {
//     int32_t fid = *(int32_t *)taosHashIterGet(pIter);

//     if (fid < mfid) {
//       for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
//         tsdbGetDataFileName(tsdbRootDir, REPO_ID(pRepo), fid, type, fname);
//         (void)remove(fname);
//       }

//       tsdbGetDataFileName(tsdbRootDir, REPO_ID(pRepo), fid, TSDB_FILE_TYPE_NHEAD, fname);
//       (void)remove(fname);

//       tsdbGetDataFileName(tsdbRootDir, REPO_ID(pRepo), fid, TSDB_FILE_TYPE_NLAST, fname);
//       (void)remove(fname);

//       continue;
//     }

//     tsdbRestoreFileGroup(pRepo, pDisk, fid, &fgroup);
//     pFileH->pFGroup[pFileH->nFGroups++] = fgroup;
//     qsort((void *)(pFileH->pFGroup), pFileH->nFGroups, sizeof(fgroup), compFGroup);

//     // TODO
//     pDisk->dmeta.nfiles++;
//   }

//   taosHashDestroyIter(pIter);
//   taosHashCleanup(pFids);
//   return 0;

// _err:
//   taosHashDestroyIter(pIter);
//   taosHashCleanup(pFids);
//   return -1;
// }

// static int tsdbRestoreFileGroup(STsdbRepo *pRepo, SDisk *pDisk, int fid, SFileGroup *pFileGroup) {
//   char tsdbRootDir[TSDB_FILENAME_LEN] = "\0";
//   char nheadF[TSDB_FILENAME_LEN] = "\0";
//   char nlastF[TSDB_FILENAME_LEN] = "\0";
//   bool newHeadExists = false;
//   bool newLastExists = false;

//   uint32_t version = 0;

//   terrno = TSDB_CODE_SUCCESS;

//   memset((void *)pFileGroup, 0, sizeof(*pFileGroup));
//   pFileGroup->fileId = fid;
//   for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
//     SFile *pFile = pFileGroup->files + type;
//     pFile->fd = -1;
//   }

//   tdGetTsdbRootDir(pDisk->dir, REPO_ID(pRepo), tsdbRootDir);
//   for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
//     SFile *pFile = pFileGroup->files + type;
//     tsdbGetDataFileName(tsdbRootDir, REPO_ID(pRepo), fid, TSDB_FILE_TYPE_HEAD, pFile->fname);
//     if (access(pFile->fname, F_OK) != 0) {
//       memset(&(pFile->info), 0, sizeof(pFile->info));
//       pFile->info.magic = TSDB_FILE_INIT_MAGIC;
//       pFileGroup->state = 1;
//       terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
//     }
//   }

//   tsdbGetDataFileName(tsdbRootDir, REPO_ID(pRepo), fid, TSDB_FILE_TYPE_NHEAD, nheadF);
//   tsdbGetDataFileName(tsdbRootDir, REPO_ID(pRepo), fid, TSDB_FILE_TYPE_NLAST, nlastF);

//   if (access(nheadF, F_OK) == 0) {
//     newHeadExists = true;
//   }

//   if (access(nlastF, F_OK) == 0) {
//     newLastExists = true;
//   }

//   if (newHeadExists) {
//     (void)remove(nheadF);
//     (void)remove(nlastF);
//   } else {
//     if (newLastExists) {
//       (void)rename(nlastF, pFileGroup->files[TSDB_FILE_TYPE_LAST].fname);
//     }
//   }

//   if (terrno != TSDB_CODE_SUCCESS) {
//     return -1;
//   }

//   for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
//     SFile *pFile = pFileGroup->files + type;
//     if (tsdbOpenFile(pFile, O_RDONLY) < 0) {
//       memset(&(pFile->info), 0, sizeof(pFile->info));
//       pFile->info.magic = TSDB_FILE_INIT_MAGIC;
//       pFileGroup->state = 1;
//       terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
//       continue;
//     }

//     if (tsdbLoadFileHeader(pFile, &version) < 0) {
//       memset(&(pFile->info), 0, sizeof(pFile->info));
//       pFile->info.magic = TSDB_FILE_INIT_MAGIC;
//       pFileGroup->state = 1;
//       terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
//       tsdbCloseFile(pFile);
//       continue;
//     }

//     if (version != TSDB_FILE_VERSION) {
//       tsdbError("vgId:%d file %s version %u is not the same as program version %u which may cause problem",
//                 REPO_ID(pRepo), pFile->fname, version, TSDB_FILE_VERSION);
//     }

//     tsdbCloseFile(pFile);
//   }

//   if (terrno != TSDB_CODE_SUCCESS) {
//     return -1;
//   } else {
//     return 0;
//   }
// }

// static SHashObj *tsdbGetAllFids(STsdbRepo *pRepo, char *dirName) {
//   DIR *     dir = NULL;
//   regex_t   regex = {0};
//   int       code = 0;
//   int32_t   vid, fid;
//   SHashObj *pHash = NULL;

//   code = regcomp(&regex, "^v[0-9]+f[0-9]+\\.(head|data|last|h|d|l)$", REG_EXTENDED);
//   if (code != 0) {
//     terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
//     goto _err;
//   }

//   dir = opendir(dirName);
//   if (dir == NULL) {
//     tsdbError("vgId:%d failed to open directory %s since %s", REPO_ID(pRepo), dirName, strerror(errno));
//     terrno = TAOS_SYSTEM_ERROR(errno);
//     goto _err;
//   }

//   pHash = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
//   if (pHash == NULL) {
//     terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
//     goto _err;
//   }

//   struct dirent *dp = NULL;
//   while ((dp = readdir(dir)) != NULL) {
//     if (strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0) continue;

//     code = regexec(&regex, dp->d_name, 0, NULL, 0);
//     if (code == 0) {
//       sscanf(dp->d_name, "v%df%d", &vid, &fid);

//       if (vid != REPO_ID(pRepo)) {
//         tsdbError("vgId:%d invalid file %s exists, ignore it", REPO_ID(pRepo), dp->d_name);
//         continue;
//       }

//       taosHashPut(pHash, (void *)(&fid), sizeof(fid), (void *)(&fid), sizeof(fid));
//     } else if (code == REG_NOMATCH) {
//       tsdbError("vgId:%d invalid file %s exists, ignore it", REPO_ID(pRepo), dp->d_name);
//       continue;
//     } else {
//       goto _err;
//     }
//   }

//   closedir(dir);
//   regfree(&regex);
//   return pHash;

// _err:
//   taosHashCleanup(pHash);
//   if (dir != NULL) closedir(dir);
//   regfree(&regex);
//   return NULL;
// }

// static int tsdbGetFidLevel(int fid, SFidGroup *pFidGroup) {
//   if (fid >= pFidGroup->maxFid) {
//     return 0;
//   } else if (fid >= pFidGroup->midFid && fid < pFidGroup->maxFid) {
//     return 1;
//   } else {
//     return 2;
//   }
// }

// static int tsdbCreateVnodeDataDir(char *baseDir, int vid) {
//   char dirName[TSDB_FILENAME_LEN] = "\0";
//   char tsdbRootDir[TSDB_FILENAME_LEN] = "\0";

//   tdGetVnodeRootDir(baseDir, dirName);
//   if (taosMkDir(dirName, 0755) < 0 && errno != EEXIST) {
//     terrno = TAOS_SYSTEM_ERROR(errno);
//     return -1;
//   }

//   tdGetVnodeDir(baseDir, vid, dirName);
//   if (taosMkDir(dirName, 0755) < 0 && errno != EEXIST) {
//     terrno = TAOS_SYSTEM_ERROR(errno);
//     return -1;
//   }

//   tdGetTsdbRootDir(baseDir, vid, tsdbRootDir);
//   if (taosMkDir(tsdbRootDir, 0755) < 0 && errno != EEXIST) {
//     terrno = TAOS_SYSTEM_ERROR(errno);
//     return -1;
//   }

//   tdGetTsdbDataDir(baseDir, vid, dirName);
//   if (taosMkDir(dirName, 0755) < 0 && errno != EEXIST) {
//     terrno = TAOS_SYSTEM_ERROR(errno);
//     return -1;
//   }

//   return 0;
// }