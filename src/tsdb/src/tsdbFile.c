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

const char *tsdbFileSuffix[] = {".head", ".data", ".last", ".h", ".l"};

// ---------------- INTERNAL FUNCTIONS ----------------
STsdbFileH *tsdbNewFileH(STsdbCfg *pCfg) {
  STsdbFileH *pFileH = (STsdbFileH *)calloc(1, sizeof(*pFileH));
  if (pFileH == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  int code = pthread_rwlock_init(&(pFileH->fhlock));
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

int *tsdbOpenFileH(STsdbRepo *pRepo) {
  ASSERT(pRepo != NULL && pRepo->tsdbFileH != NULL);

  char *tDataDir = NULL;
  DIR * dir = NULL;
  int   fid = 0;

  STsdbFileH pFileH = pRepo->tsdbFileH;

  tDataDir = tsdbGetDataDirName(pRepo->rootDir);
  if (tDataDir == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  DIR *dir = opendir(tDataDir);
  if (dir == NULL) {
    tsdbError("vgId:%d failed to open directory %s since %s", REPO_ID(pRepo), tDataDir, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  struct dirent *dp = NULL;
  while ((dp = readdir(dir)) != NULL) {
    if (strncmp(dp->d_name, ".", 1) == 0 || strncmp(dp->d_name, "..", 2) == 0) continue;
    sscanf(dp->d_name, "f%d", &fid);
    
    SFileGroup fileGroup = {0};

    if (tsdbSearchFGroup(pFileH, fid, TD_EQ) != NULL) continue;
    for (int type = TSDB_FILE_TYPE_HEAD; type <= TSDB_FILE_TYPE_LAST; type++) {
    }
    for (int type = TSDB_FILE_TYPE_NHEAD; type <= TSDB_FILE_TYPE_NLAST; type++) {

    }
  }

  tfree(tDataDir);
  closedir(dir);
  return 0;

_err:
  tfree(tDataDir);
  if (dir != NULL) closedir(tDataDir);
  return -1;
}

void tsdbCloseFileH(STsdbRepo *pRepo) {
  STsdbFileH *pFileH = pRepo->tsdbFileH;

  for (int i = 0; i < pFileH->nFGroups; i++) {
    // TODO

  }
}

/**
 * Create the file group if the file group not exists.
 *
 * @return A pointer to
 */
SFileGroup *tsdbCreateFGroup(STsdbFileH *pFileH, char *dataDir, int fid, int maxTables) {
  if (pFileH->numOfFGroups >= pFileH->maxFGroups) return NULL;

  SFileGroup  fGroup;
  SFileGroup *pFGroup = &fGroup;

  SFileGroup *pGroup = tsdbSearchFGroup(pFileH, fid);
  if (pGroup == NULL) {  // if not exists, create one
    pFGroup->fileId = fid;
    for (int type = TSDB_FILE_TYPE_HEAD; type < TSDB_FILE_TYPE_MAX; type++) {
      if (tsdbCreateFile(dataDir, fid, tsdbFileSuffix[type], &(pFGroup->files[type])) < 0)
        goto _err;
    }

    pFileH->fGroup[pFileH->numOfFGroups++] = fGroup;
    qsort((void *)(pFileH->fGroup), pFileH->numOfFGroups, sizeof(SFileGroup), compFGroup);
    return tsdbSearchFGroup(pFileH, fid);
  }

  return pGroup;

_err:
  // TODO: deal with the err here
  return NULL;
}

int tsdbRemoveFileGroup(STsdbFileH *pFileH, int fid) {
  SFileGroup *pGroup =
      bsearch((void *)&fid, (void *)(pFileH->fGroup), pFileH->numOfFGroups, sizeof(SFileGroup), keyFGroupCompFunc);
  if (pGroup == NULL) return -1;

  // Remove from disk
  for (int type = TSDB_FILE_TYPE_HEAD; type < TSDB_FILE_TYPE_MAX; type++) {
    remove(pGroup->files[type].fname);
  }

  // Adjust the memory
  int filesBehind = pFileH->numOfFGroups - (((char *)pGroup - (char *)(pFileH->fGroup)) / sizeof(SFileGroup) + 1);
  if (filesBehind > 0) {
    memmove((void *)pGroup, (void *)((char *)pGroup + sizeof(SFileGroup)), sizeof(SFileGroup) * filesBehind);
  }
  pFileH->numOfFGroups--;

  return 0;
}

void tsdbInitFileGroupIter(STsdbFileH *pFileH, SFileGroupIter *pIter, int direction) {
  pIter->direction = direction;
  pIter->base = pFileH->fGroup;
  pIter->numOfFGroups = pFileH->numOfFGroups;
  if (pFileH->numOfFGroups == 0){
    pIter->pFileGroup = NULL;
  } else {
    if (direction == TSDB_FGROUP_ITER_FORWARD) {
      pIter->pFileGroup = pFileH->fGroup;
    } else {
      pIter->pFileGroup = pFileH->fGroup + pFileH->numOfFGroups - 1;
    }
  }
}

void tsdbFitRetention(STsdbRepo *pRepo) {
  STsdbFileH *pFileH = pRepo->tsdbFileH;
  SFileGroup *pGroup = pFileH->fGroup;

  int mfid =
      tsdbGetKeyFileId(taosGetTimestamp(pRepo->config.precision), pRepo->config.daysPerFile, pRepo->config.precision) - pFileH->maxFGroups + 3;

  while (pFileH->numOfFGroups > 0 && pGroup[0].fileId < mfid) {
    tsdbRemoveFileGroup(pFileH, pGroup[0].fileId);
  }
}

void tsdbSeekFileGroupIter(SFileGroupIter *pIter, int fid) {
  if (pIter->numOfFGroups == 0) {
    assert(pIter->pFileGroup == NULL);
    return;
  }
  
  int flags = (pIter->direction == TSDB_FGROUP_ITER_FORWARD) ? TD_GE : TD_LE;
  void *ptr = taosbsearch(&fid, pIter->base, pIter->numOfFGroups, sizeof(SFileGroup), keyFGroupCompFunc, flags);
  if (ptr == NULL) {
    pIter->pFileGroup = NULL;
  } else {
    pIter->pFileGroup = (SFileGroup *)ptr;
  }
}

SFileGroup *tsdbGetFileGroupNext(SFileGroupIter *pIter) {
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

char *tsdbGetFileName(char *dataDir, int fileId, int type) {
  int tlen = strlen(dataDir) + strlen(tsdbFileSuffix[type]) + 24;

  char *fname = (char *)malloc(tlen);
  if (fname == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  sprintf(fname, "%s/v%df%d%s", dataDir, fileId, tsdbFileSuffix[type]);

  return 0;
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

int tsdbCreateFile(char *dataDir, int fileId, const char *suffix, SFile *pFile) {
  memset((void *)pFile, 0, sizeof(SFile));
  pFile->fd = -1;

  tsdbGetFileName(dataDir, fileId, suffix, pFile->fname);
  
  if (access(pFile->fname, F_OK) == 0) {
    // File already exists
    return -1;
  }

  if (tsdbOpenFile(pFile, O_RDWR | O_CREAT) < 0) {
    // TODO: deal with the ERROR here
    return -1;
  }

  pFile->info.size = TSDB_FILE_HEAD_SIZE;

  if (tsdbUpdateFileHeader(pFile, 0) < 0) {
    tsdbCloseFile(pFile);
    return -1;
  }

  tsdbCloseFile(pFile);

  return 0;
}

SFileGroup *tsdbSearchFGroup(STsdbFileH *pFileH, int fid, int flags) {
  void *ptr =
      taosbsearch((void *)(&fid), (void *)(pFileH->pFGroup), pFileH->nFGroups, sizeof(SFileGroup), keyFGroupCompFunc);
  if (ptr == NULL) return NULL;
  return (SFileGroup *)ptr;
}

// ---------------- LOCAL FUNCTIONS ----------------
static int tsdbInitFile(char *dataDir, int fid, const char *suffix, SFile *pFile) {
  uint32_t version;
  char buf[512] = "\0";

  tsdbGetFileName(dataDir, fid, suffix, pFile->fname);
  if (access(pFile->fname, F_OK|R_OK|W_OK) < 0) return -1;
  pFile->fd = -1;
  if (tsdbOpenFile(pFile, O_RDONLY) < 0) return -1;

  if (tread(pFile->fd, buf, TSDB_FILE_HEAD_SIZE) < TSDB_FILE_HEAD_SIZE) return -1;
  if (!taosCheckChecksumWhole((uint8_t *)buf, TSDB_FILE_HEAD_SIZE)) return -1;

  void *pBuf = buf;
  pBuf = taosDecodeFixedU32(pBuf, &version);
  pBuf = tsdbDecodeSFileInfo(pBuf, &(pFile->info));

  tsdbCloseFile(pFile);

  return 0;
}

// static int tsdbOpenFGroup(STsdbFileH *pFileH, char *dataDir, int fid) {
//   if (tsdbSearchFGroup(pFileH, fid) != NULL) return 0;

//   SFileGroup fGroup = {0};
//   fGroup.fileId = fid;

//   for (int type = TSDB_FILE_TYPE_HEAD; type < TSDB_FILE_TYPE_MAX; type++) {
//     if (tsdbInitFile(dataDir, fid, tsdbFileSuffix[type], &fGroup.files[type]) < 0) return -1;
//   }
//   pFileH->fGroup[pFileH->numOfFGroups++] = fGroup;
//   qsort((void *)(pFileH->fGroup), pFileH->numOfFGroups, sizeof(SFileGroup), compFGroup);
//   return 0;
// }

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