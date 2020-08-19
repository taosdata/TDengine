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
#include <regex.h>

#define TAOS_RANDOM_FILE_FAIL_TEST

#include "os.h"
#include "talgo.h"
#include "tchecksum.h"
#include "tsdbMain.h"
#include "tutil.h"


const char *tsdbFileSuffix[] = {".head", ".data", ".last", ".stat", ".h", ".d", ".l", ".s"};

static int  tsdbInitFile(SFile *pFile, STsdbRepo *pRepo, int fid, int type);
static void tsdbDestroyFile(SFile *pFile);
static int  compFGroup(const void *arg1, const void *arg2);
static int  keyFGroupCompFunc(const void *key, const void *fgroup);
static void tsdbInitFileGroup(SFileGroup *pFGroup, STsdbRepo *pRepo);

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
    taosTFree(pFileH->pFGroup);
    free(pFileH);
  }
}

int tsdbOpenFileH(STsdbRepo *pRepo) {
  ASSERT(pRepo != NULL && pRepo->tsdbFileH != NULL);

  char *  tDataDir = NULL;
  DIR *   dir = NULL;
  int     fid = 0;
  int     vid = 0;
  regex_t regex1, regex2;
  int     code = 0;

  SFileGroup  fileGroup = {0};
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

  code = regcomp(&regex1, "^v[0-9]+f[0-9]+\\.(head|data|last|stat)$", REG_EXTENDED);
  if (code != 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  code = regcomp(&regex2, "^v[0-9]+f[0-9]+\\.(h|d|l|s)$", REG_EXTENDED);
  if (code != 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  struct dirent *dp = NULL;
  while ((dp = readdir(dir)) != NULL) {
    if (strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0) continue;

    code = regexec(&regex1, dp->d_name, 0, NULL, 0);
    if (code == 0) {
      sscanf(dp->d_name, "v%df%d", &vid, &fid);
      if (vid != REPO_ID(pRepo)) {
        tsdbError("vgId:%d invalid file %s exists, ignore it", REPO_ID(pRepo), dp->d_name);
        continue;
      }

      if (tsdbSearchFGroup(pFileH, fid, TD_EQ) != NULL) continue;
      memset((void *)(&fileGroup), 0, sizeof(SFileGroup));
      fileGroup.fileId = fid;

      tsdbInitFileGroup(&fileGroup, pRepo);
    } else if (code == REG_NOMATCH) {
      code = regexec(&regex2, dp->d_name, 0, NULL, 0);
      if (code == 0) {
        tsdbDebug("vgId:%d invalid file %s exists, remove it", REPO_ID(pRepo), dp->d_name);
        char *fname = malloc(strlen(tDataDir) + strlen(dp->d_name) + 2);
        if (fname == NULL) goto _err;
        sprintf(fname, "%s/%s", tDataDir, dp->d_name);
        (void)remove(fname);
        free(fname);
      } else if (code == REG_NOMATCH) {
        tsdbError("vgId:%d invalid file %s exists, ignore it", REPO_ID(pRepo), dp->d_name);
        continue;
      } else {
        goto _err;
      }
    } else {
      goto _err;
    }

    pFileH->pFGroup[pFileH->nFGroups++] = fileGroup;
    qsort((void *)(pFileH->pFGroup), pFileH->nFGroups, sizeof(SFileGroup), compFGroup);
  }

  regfree(&regex1);
  regfree(&regex2);
  taosTFree(tDataDir);
  closedir(dir);
  return 0;

_err:
  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) tsdbDestroyFile(&fileGroup.files[type]);

  regfree(&regex1);
  regfree(&regex2);

  taosTFree(tDataDir);
  if (dir != NULL) closedir(dir);
  tsdbCloseFileH(pRepo);
  return -1;
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

SFileGroup *tsdbCreateFGroupIfNeed(STsdbRepo *pRepo, char *dataDir, int fid) {
  STsdbFileH *pFileH = pRepo->tsdbFileH;

  if (pFileH->nFGroups >= pFileH->maxFGroups) return NULL;

  SFileGroup  fGroup;
  SFileGroup *pFGroup = &fGroup;

  SFileGroup *pGroup = tsdbSearchFGroup(pFileH, fid, TD_EQ);
  if (pGroup == NULL) {  // if not exists, create one
    pFGroup->fileId = fid;
    for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
      if (tsdbCreateFile(&pFGroup->files[type], pRepo, fid, type) < 0)
        goto _err;
    }

    pthread_rwlock_wrlock(&pFileH->fhlock);
    pFileH->pFGroup[pFileH->nFGroups++] = fGroup;
    qsort((void *)(pFileH->pFGroup), pFileH->nFGroups, sizeof(SFileGroup), compFGroup);
    pthread_rwlock_unlock(&pFileH->fhlock);
    return tsdbSearchFGroup(pFileH, fid, TD_EQ);
  }

  return pGroup;

_err:
  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) tsdbDestroyFile(&pGroup->files[type]);
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

void tsdbFitRetention(STsdbRepo *pRepo) {
  STsdbCfg *pCfg = &(pRepo->config);
  STsdbFileH *pFileH = pRepo->tsdbFileH;
  SFileGroup *pGroup = pFileH->pFGroup;

  int mfid = (int)(TSDB_KEY_FILEID(taosGetTimestamp(pCfg->precision), pCfg->daysPerFile, pCfg->precision) -
             TSDB_MAX_FILE(pCfg->keep, pCfg->daysPerFile));

  pthread_rwlock_wrlock(&(pFileH->fhlock));

  while (pFileH->nFGroups > 0 && pGroup[0].fileId < mfid) {
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
  if (taosTWrite(pFile->fd, (void *)buf, TSDB_FILE_HEAD_SIZE) < TSDB_FILE_HEAD_SIZE) {
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

  SFileGroup fileGroup = *pFGroup;

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
}

void tsdbGetFileInfoImpl(char *fname, uint32_t *magic, int32_t *size) {
  char          buf[TSDB_FILE_HEAD_SIZE] = "\0";
  uint32_t      version = 0;
  STsdbFileInfo info = {0};

  int fd = open(fname, O_RDONLY);
  if (fd < 0) goto _err;

  if (taosTRead(fd, buf, TSDB_FILE_HEAD_SIZE) < TSDB_FILE_HEAD_SIZE) goto _err;

  if (!taosCheckChecksumWhole((uint8_t *)buf, TSDB_FILE_HEAD_SIZE)) goto _err;

  void *pBuf = (void *)buf;
  pBuf = taosDecodeFixedU32(pBuf, &version);
  pBuf = tsdbDecodeSFileInfo(pBuf, &info);

  off_t offset = lseek(fd, 0, SEEK_END);
  if (offset < 0) goto _err;
  close(fd);

  *magic = info.magic;
  *size = (int32_t)offset;

  return;

_err:
  if (fd >= 0) close(fd);
  *magic = TSDB_FILE_INIT_MAGIC;
  *size = 0;
}

// ---------------- LOCAL FUNCTIONS ----------------
static int tsdbInitFile(SFile *pFile, STsdbRepo *pRepo, int fid, int type) {
  uint32_t version;
  char     buf[512] = "\0";

  tsdbGetDataFileName(pRepo, fid, type, pFile->fname);

  pFile->fd = -1;
  if (tsdbOpenFile(pFile, O_RDONLY) < 0) goto _err;

  if (taosTRead(pFile->fd, buf, TSDB_FILE_HEAD_SIZE) < TSDB_FILE_HEAD_SIZE) {
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

  if (pFile->info.size == TSDB_FILE_HEAD_SIZE) {
    pFile->info.size = lseek(pFile->fd, 0, SEEK_END);
  }

  if (version != TSDB_FILE_VERSION) {
    tsdbError("vgId:%d file %s version %u is not the same as program version %u which may cause problem",
              REPO_ID(pRepo), pFile->fname, version, TSDB_FILE_VERSION);
  }

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

static void tsdbInitFileGroup(SFileGroup *pFGroup, STsdbRepo *pRepo) {
  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    if (tsdbInitFile(&pFGroup->files[type], pRepo, pFGroup->fileId, type) < 0) {
      memset(&pFGroup->files[type].info, 0, sizeof(STsdbFileInfo));
      pFGroup->files[type].info.magic = TSDB_FILE_INIT_MAGIC;
      pFGroup->state = 1;
      terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    }
  }
}
