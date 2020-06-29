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
#include <dirent.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <libgen.h>

#include "talgo.h"
#include "tchecksum.h"
#include "tsdbMain.h"
#include "tutil.h"
#include "ttime.h"

const char *tsdbFileSuffix[] = {".head", ".data", ".last", "", ".h", ".l"};

static int   tsdbInitFile(SFile *pFile, STsdbRepo *pRepo, int fid, int type);
static void  tsdbDestroyFile(SFile *pFile);
static int   compFGroup(const void *arg1, const void *arg2);
static int   keyFGroupCompFunc(const void *key, const void *fgroup);

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

  char *tDataDir = NULL;
  DIR * dir = NULL;
  int   fid = 0;
  int   vid = 0;

  SFileGroup fileGroup = {0};
  STsdbFileH *pFileH = pRepo->tsdbFileH;

  tDataDir = tsdbGetDataDirName(pRepo->rootDir);
  if (tDataDir == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  dir = opendir(tDataDir);
  if (dir == NULL) {
    tsdbError("vgId:%d failed to open directory %s since %s", REPO_ID(pRepo), tDataDir, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  struct dirent *dp = NULL;
  while ((dp = readdir(dir)) != NULL) {
    if (strncmp(dp->d_name, ".", 1) == 0 || strncmp(dp->d_name, "..", 2) == 0) continue;
    sscanf(dp->d_name, "v%df%d", &vid, &fid);

    if (tsdbSearchFGroup(pRepo->tsdbFileH, fid, TD_EQ) != NULL) continue;

    memset((void *)(&fileGroup), 0, sizeof(SFileGroup));
    fileGroup.fileId = fid;
    for (int type = TSDB_FILE_TYPE_HEAD; type < TSDB_FILE_TYPE_MAX; type++) {
      if (tsdbInitFile(&fileGroup.files[type], pRepo, fid, type) < 0) {
        tsdbError("vgId:%d failed to init file fid %d type %d", REPO_ID(pRepo), fid, type);
        goto _err;
      }
    }

    tsdbDebug("vgId:%d file group %d init", REPO_ID(pRepo), fid);

    pFileH->pFGroup[pFileH->nFGroups++] = fileGroup;
    qsort((void *)(pFileH->pFGroup), pFileH->nFGroups, sizeof(SFileGroup), compFGroup);
  }

  tfree(tDataDir);
  closedir(dir);
  return 0;

_err:
  for (int type = TSDB_FILE_TYPE_HEAD; type < TSDB_FILE_TYPE_MAX; type++) tsdbDestroyFile(&fileGroup.files[type]);

  tfree(tDataDir);
  if (dir != NULL) closedir(dir);
  tsdbCloseFileH(pRepo);
  return -1;
}

void tsdbCloseFileH(STsdbRepo *pRepo) {
  STsdbFileH *pFileH = pRepo->tsdbFileH;

  for (int i = 0; i < pFileH->nFGroups; i++) {
    SFileGroup *pFGroup = pFileH->pFGroup + i;
    for (int type = TSDB_FILE_TYPE_HEAD; type < TSDB_FILE_TYPE_MAX; type++) {
      tsdbDestroyFile(&pFGroup->files[type]);
    }
  }
}

SFileGroup *tsdbCreateFGroupIfNeed(STsdbRepo *pRepo, char *dataDir, int fid, int maxTables) {
  STsdbFileH *pFileH = pRepo->tsdbFileH;

  if (pFileH->nFGroups >= pFileH->maxFGroups) return NULL;

  SFileGroup  fGroup;
  SFileGroup *pFGroup = &fGroup;

  SFileGroup *pGroup = tsdbSearchFGroup(pFileH, fid, TD_EQ);
  if (pGroup == NULL) {  // if not exists, create one
    pFGroup->fileId = fid;
    for (int type = TSDB_FILE_TYPE_HEAD; type < TSDB_FILE_TYPE_MAX; type++) {
      if (tsdbCreateFile(&pFGroup->files[type], pRepo, fid, type) < 0)
        goto _err;
    }

    pFileH->pFGroup[pFileH->nFGroups++] = fGroup;
    qsort((void *)(pFileH->pFGroup), pFileH->nFGroups, sizeof(SFileGroup), compFGroup);
    return tsdbSearchFGroup(pFileH, fid, TD_EQ);
  }

  return pGroup;

_err:
  for (int type = TSDB_FILE_TYPE_HEAD; type < TSDB_FILE_TYPE_MAX; type++) tsdbDestroyFile(&pGroup->files[type]);
  return NULL;
}

void tsdbInitFileGroupIter(STsdbFileH *pFileH, SFileGroupIter *pIter, int direction) { // TODO
  pIter->direction = direction;
  pIter->base = pFileH->pFGroup;
  pIter->numOfFGroups = pFileH->nFGroups;
  if (pFileH->nFGroups == 0) {
    pIter->pFileGroup = NULL;
  } else {
    if (direction == TSDB_FGROUP_ITER_FORWARD) {
      pIter->pFileGroup = pFileH->pFGroup;
    } else {
      pIter->pFileGroup = pFileH->pFGroup + pFileH->nFGroups - 1;
    }
  }
}

void tsdbSeekFileGroupIter(SFileGroupIter *pIter, int fid) { // TODO
  if (pIter->numOfFGroups == 0) {
    assert(pIter->pFileGroup == NULL);
    return;
  }

  int   flags = (pIter->direction == TSDB_FGROUP_ITER_FORWARD) ? TD_GE : TD_LE;
  void *ptr = taosbsearch(&fid, pIter->base, pIter->numOfFGroups, sizeof(SFileGroup), keyFGroupCompFunc, flags);
  if (ptr == NULL) {
    pIter->pFileGroup = NULL;
  } else {
    pIter->pFileGroup = (SFileGroup *)ptr;
  }
}

SFileGroup *tsdbGetFileGroupNext(SFileGroupIter *pIter) {//TODO
  SFileGroup *ret = pIter->pFileGroup;
  if (ret == NULL) return NULL;

  if (pIter->direction == TSDB_FGROUP_ITER_FORWARD) {
    if ((pIter->pFileGroup + 1) == (pIter->base + pIter->numOfFGroups)) {
      pIter->pFileGroup = NULL;
    } else {
      pIter->pFileGroup += 1;
    }
  } else {
    if (pIter->pFileGroup == pIter->base) {
      pIter->pFileGroup = NULL;
    } else {
      pIter->pFileGroup -= 1;
    }
  }
  return ret;
}

int tsdbOpenFile(SFile *pFile, int oflag) {
  ASSERT(!TSDB_IS_FILE_OPENED(pFile));

  pFile->fd = open(pFile->fname, oflag, 0755);
  if (pFile->fd < 0) {
    tsdbError("failed to open file %s since %s", pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

void tsdbCloseFile(SFile *pFile) {
  if (TSDB_IS_FILE_OPENED(pFile)) {
    close(pFile->fd);
    pFile->fd = -1;
  }
}

int tsdbCreateFile(SFile *pFile, STsdbRepo *pRepo, int fid, int type) {
  memset((void *)pFile, 0, sizeof(SFile));
  pFile->fd = -1;

  tsdbGetDataFileName(pRepo, fid, type, pFile->fname);

  if (access(pFile->fname, F_OK) == 0) {
    tsdbError("vgId:%d file %s already exists", REPO_ID(pRepo), pFile->fname);
    terrno = TSDB_CODE_TDB_FILE_ALREADY_EXISTS;
    goto _err;
  }

  if (tsdbOpenFile(pFile, O_RDWR | O_CREAT) < 0) {
    goto _err;
  }

  pFile->info.size = TSDB_FILE_HEAD_SIZE;

  if (tsdbUpdateFileHeader(pFile, 0) < 0) {
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

void tsdbFitRetention(STsdbRepo *pRepo) {
  STsdbCfg *pCfg = &(pRepo->config);
  STsdbFileH *pFileH = pRepo->tsdbFileH;
  SFileGroup *pGroup = pFileH->pFGroup;

  int mfid = TSDB_KEY_FILEID(taosGetTimestamp(pCfg->precision), pCfg->daysPerFile, pCfg->precision) -
             TSDB_MAX_FILE(pCfg->keep, pCfg->daysPerFile);

  pthread_rwlock_wrlock(&(pFileH->fhlock));

  while (pFileH->nFGroups > 0 && pGroup[0].fileId < mfid) {
    tsdbRemoveFileGroup(pRepo, pGroup);
  }

  pthread_rwlock_unlock(&(pFileH->fhlock));
}

int tsdbUpdateFileHeader(SFile *pFile, uint32_t version) {
  char buf[TSDB_FILE_HEAD_SIZE] = "\0";

  void *pBuf = (void *)buf;
  taosEncodeFixedU32((void *)(&pBuf), version);
  tsdbEncodeSFileInfo((void *)(&pBuf), &(pFile->info));

  taosCalcChecksumAppend(0, (uint8_t *)buf, TSDB_FILE_HEAD_SIZE);

  if (lseek(pFile->fd, 0, SEEK_SET) < 0) {
    tsdbError("failed to lseek file %s since %s", pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
  if (twrite(pFile->fd, (void *)buf, TSDB_FILE_HEAD_SIZE) < TSDB_FILE_HEAD_SIZE) {
    tsdbError("failed to write %d bytes to file %s since %s", TSDB_FILE_HEAD_SIZE, pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

int tsdbEncodeSFileInfo(void **buf, const STsdbFileInfo *pInfo) {
  int tlen = 0;
  tlen += taosEncodeFixedU32(buf, pInfo->offset);
  tlen += taosEncodeFixedU32(buf, pInfo->len);
  tlen += taosEncodeFixedU64(buf, pInfo->size);
  tlen += taosEncodeFixedU64(buf, pInfo->tombSize);
  tlen += taosEncodeFixedU32(buf, pInfo->totalBlocks);
  tlen += taosEncodeFixedU32(buf, pInfo->totalSubBlocks);

  return tlen;
}

void *tsdbDecodeSFileInfo(void *buf, STsdbFileInfo *pInfo) {
  buf = taosDecodeFixedU32(buf, &(pInfo->offset));
  buf = taosDecodeFixedU32(buf, &(pInfo->len));
  buf = taosDecodeFixedU64(buf, &(pInfo->size));
  buf = taosDecodeFixedU64(buf, &(pInfo->tombSize));
  buf = taosDecodeFixedU32(buf, &(pInfo->totalBlocks));
  buf = taosDecodeFixedU32(buf, &(pInfo->totalSubBlocks));

  return buf;
}

void tsdbRemoveFileGroup(STsdbRepo *pRepo, SFileGroup *pFGroup) {
  ASSERT(pFGroup != NULL);
  STsdbFileH *pFileH = pRepo->tsdbFileH;

  SFileGroup fileGroup = *pFGroup;

  int nFilesLeft = pFileH->nFGroups - (POINTER_DISTANCE(pFGroup, pFileH->pFGroup) / sizeof(SFileGroup) + 1);
  if (nFilesLeft > 0) {
    memmove((void *)pFGroup, POINTER_SHIFT(pFGroup, sizeof(SFileGroup)), sizeof(SFileGroup) * nFilesLeft);
  }

  pFileH->nFGroups--;
  ASSERT(pFileH->nFGroups >= 0);

  for (int type = TSDB_FILE_TYPE_HEAD; type < TSDB_FILE_TYPE_MAX; type++) {
    if (remove(fileGroup.files[type].fname) < 0) {
      tsdbError("vgId:%d failed to remove file %s", REPO_ID(pRepo), fileGroup.files[type].fname);
    }
    tsdbDestroyFile(&fileGroup.files[type]);
  }
}

// ---------------- LOCAL FUNCTIONS ----------------
static int tsdbInitFile(SFile *pFile, STsdbRepo *pRepo, int fid, int type) {
  uint32_t version;
  char     buf[512] = "\0";

  tsdbGetDataFileName(pRepo, fid, type, pFile->fname);

  pFile->fd = -1;
  if (tsdbOpenFile(pFile, O_RDONLY) < 0) goto _err;

  if (tread(pFile->fd, buf, TSDB_FILE_HEAD_SIZE) < TSDB_FILE_HEAD_SIZE) {
    tsdbError("vgId:%d failed to read %d bytes from file %s since %s", REPO_ID(pRepo), TSDB_FILE_HEAD_SIZE,
              pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  if (!taosCheckChecksumWhole((uint8_t *)buf, TSDB_FILE_HEAD_SIZE)) {
    tsdbError("vgId:%d file %s head part is corrupted", REPO_ID(pRepo), pFile->fname);
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    goto _err;
  }

  void *pBuf = buf;
  pBuf = taosDecodeFixedU32(pBuf, &version);
  pBuf = tsdbDecodeSFileInfo(pBuf, &(pFile->info));

  tsdbCloseFile(pFile);

  return 0;
_err:
  tsdbDestroyFile(pFile);
  return -1;
}

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
