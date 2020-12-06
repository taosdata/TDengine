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
#include "tarray.h"

const char *tsdbFileSuffix[] = {".head", ".data", ".last", ".stat", ".h", ".d", ".l", ".s"};

static int   compFGroup(const void *arg1, const void *arg2);
static int   keyFGroupCompFunc(const void *key, const void *fgroup);
static void *tsdbScanAllFiles(STsdbRepo *pRepo);
static int   tsdbCompareFile(const void *arg1, const void *arg2);
static int   tsdbRestoreFile(STsdbRepo *pRepo, TFILE *pfiles, int nfile);
static void  tsdbParseFname(const char *bname, int *vid, int *fid, char *suffix);

// STsdbFileH ===========================================
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

  void *pfArray = NULL;

  // Scan the whole directory and get data
  pfArray = tsdbScanAllFiles(pRepo);
  if (pfArray == NULL) {
    return -1;
  }

  if (taosArrayGetSize(pfArray) == 0) {
    taosArrayDestroy(pfArray);
    return 0;
  }

  // Sort the files
  taosArraySort(pfArray, tsdbCompareFile);

  // Loop to recover the files
  int iter = 0;
  while (true) {
    if (iter >= taosArrayGetSize(pfArray)) break;

    int  vid, fid;
    char bname[TSDB_FILENAME_LEN] = "\0";
    char suffix[TSDB_FILENAME_LEN] = "\0";
    int  count = 0;

    TFILE *pf = taosArrayGet(pfArray, iter);
    tfsbasename(pf, bname);
    tsdbParseFname(bname, &vid, &fid, suffix);
    count++;
    iter++;

    while (true) {
      int nfid = 0;
      if (iter >= taosArrayGetSize(pfArray)) break;
      TFILE *npf = taosArrayGet(pfArray, iter);
      tfsbasename(npf, bname);
      tsdbParseFname(bname, &vid, &nfid, suffix);

      if (nfid != fid) break;
      count++;
      iter++;
    }

    tsdbRestoreFile(pRepo, pf, count);
  }

  taosArrayDestroy(pfArray);
  return 0;
}

void tsdbCloseFileH(STsdbRepo *pRepo, bool isRestart) {
  STsdbFileH *pFileH = pRepo->tsdbFileH;

  for (int i = 0; i < pFileH->nFGroups; i++) {
    SFileGroup *pFGroup = pFileH->pFGroup + i;
    for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
      tsdbCloseFile(&(pFGroup->files[type]));
    }
    if (isRestart) {
      tfsDecDiskFile(pFGroup->files[0].file.level, pFGroup->files[0].file.level, TSDB_FILE_TYPE_MAX);
    }
  }
}

// SFileGroup ===========================================
SFileGroup *tsdbCreateFGroup(STsdbRepo *pRepo, int fid, int level) {
  STsdbFileH *pFileH = pRepo->tsdbFileH;
  SFileGroup  fg = {0};
  int         id = TFS_UNDECIDED_ID;
  char        fname[TSDB_FILENAME_LEN] = "\0";

  ASSERT(tsdbSearchFGroup(pFileH, fid, TD_EQ) == NULL);
  ASSERT(pFileH->nFGroups < pFileH->maxFGroups);

  // SET FILE GROUP
  fg.fileId = fid;

  // CREATE FILES
  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    SFile *pFile = &(fg.files[type]);

    pFile->fd = -1;
    pFile->info.size = TSDB_FILE_HEAD_SIZE;
    pFile->info.magic = TSDB_FILE_INIT_MAGIC;

    tsdbGetDataFileName(pRepo->rootDir, REPO_ID(pRepo), fid, type, fname);
    tfsInitFile(&pFile->file, level, id, fname);

    if (tsdbOpenFile(pFile, O_WRONLY|O_CREAT) < 0) return NULL;

    if (tsdbUpdateFileHeader(pFile) < 0) {
      tsdbCloseFile(pFile);
      return NULL;
    }

    tsdbCloseFile(pFile);

    level = TFILE_LEVEL(&(pFile->file));
    id = TFILE_ID(&(pFile->file));
  }

  // PUT GROUP INTO FILE HANDLE
  pthread_rwlock_wrlock(&pFileH->fhlock);
  pFileH->pFGroup[pFileH->nFGroups++] = fg;
  qsort((void *)(pFileH->pFGroup), pFileH->nFGroups, sizeof(SFileGroup), compFGroup);
  pthread_rwlock_unlock(&pFileH->fhlock);

  SFileGroup *pfg = tsdbSearchFGroup(pFileH, fid, TD_EQ);
  ASSERT(pfg != NULL);
  return pfg;
}

void tsdbRemoveFileGroup(STsdbRepo *pRepo, SFileGroup *pFGroup) {
  ASSERT(pFGroup != NULL);
  STsdbFileH *pFileH = pRepo->tsdbFileH;

  SFileGroup fg = *pFGroup;

  int nFilesLeft = pFileH->nFGroups - (int)(POINTER_DISTANCE(pFGroup, pFileH->pFGroup) / sizeof(SFileGroup) + 1);
  if (nFilesLeft > 0) {
    memmove((void *)pFGroup, POINTER_SHIFT(pFGroup, sizeof(SFileGroup)), sizeof(SFileGroup) * nFilesLeft);
  }

  pFileH->nFGroups--;
  ASSERT(pFileH->nFGroups >= 0);

  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    SFile *pFile = &(fg.files[type]);
    tfsremove(&(pFile->file));
  }
}

SFileGroup *tsdbSearchFGroup(STsdbFileH *pFileH, int fid, int flags) {
  void *ptr = taosbsearch((void *)(&fid), (void *)(pFileH->pFGroup), pFileH->nFGroups, sizeof(SFileGroup),
                          keyFGroupCompFunc, flags);
  if (ptr == NULL) return NULL;
  return (SFileGroup *)ptr;
}

int tsdbGetFidLevel(int fid, SFidGroup fidg) {
  if (fid >= fidg.maxFid) {
    return 0;
  } else if (fid >= fidg.midFid) {
    return 1;
  } else if (fid >= fidg.minFid) {
    return 2;
  } else {
    return -1;
  }
}

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

// SFileGroupIter ===========================================
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

// SFile ===========================================
int tsdbOpenFile(SFile *pFile, int oflag) {
  ASSERT(!TSDB_IS_FILE_OPENED(pFile));

  pFile->fd = tfsopen(&(pFile->file), oflag);
  if (pFile->fd < 0) {
    tsdbError("failed to open file %s since %s", TSDB_FILE_NAME(pFile), tstrerror(terrno));
    return -1;
  }

  tsdbTrace("open file %s, fd %d", TSDB_FILE_NAME(pFile), pFile->fd);

  return 0;
}

void tsdbCloseFile(SFile *pFile) {
  if (TSDB_IS_FILE_OPENED(pFile)) {
    tsdbTrace("close file %s, fd %d", TSDB_FILE_NAME(pFile), pFile->fd);
    tfsclose(pFile->fd);
    pFile->fd = -1;
  }
}

int tsdbUpdateFileHeader(SFile *pFile) {
  char buf[TSDB_FILE_HEAD_SIZE] = "\0";

  void *pBuf = (void *)buf;
  taosEncodeFixedU32((void *)(&pBuf), TSDB_FILE_VERSION);
  tsdbEncodeSFileInfo((void *)(&pBuf), &(pFile->info));

  taosCalcChecksumAppend(0, (uint8_t *)buf, TSDB_FILE_HEAD_SIZE);

  if (lseek(pFile->fd, 0, SEEK_SET) < 0) {
    tsdbError("failed to lseek file %s since %s", TSDB_FILE_NAME(pFile), strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
  if (taosWrite(pFile->fd, (void *)buf, TSDB_FILE_HEAD_SIZE) < TSDB_FILE_HEAD_SIZE) {
    tsdbError("failed to write %d bytes to file %s since %s", TSDB_FILE_HEAD_SIZE, TSDB_FILE_NAME(pFile),
              strerror(errno));
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

int tsdbLoadFileHeader(SFile *pFile, uint32_t *version) {
  char buf[TSDB_FILE_HEAD_SIZE] = "\0";

  if (lseek(pFile->fd, 0, SEEK_SET) < 0) {
    tsdbError("failed to lseek file %s to start since %s", TSDB_FILE_NAME(pFile), strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosRead(pFile->fd, buf, TSDB_FILE_HEAD_SIZE) < TSDB_FILE_HEAD_SIZE) {
    tsdbError("failed to read file %s header part with %d bytes, reason:%s", TSDB_FILE_NAME(pFile), TSDB_FILE_HEAD_SIZE,
              strerror(errno));
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    return -1;
  }

  if (!taosCheckChecksumWhole((uint8_t *)buf, TSDB_FILE_HEAD_SIZE)) {
    tsdbError("file %s header part is corrupted with failed checksum", TSDB_FILE_NAME(pFile));
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    return -1;
  }

  void *pBuf = (void *)buf;
  pBuf = taosDecodeFixedU32(pBuf, version);
  pBuf = tsdbDecodeSFileInfo(pBuf, &(pFile->info));

  return 0;
}

void tsdbGetFileInfoImpl(char *fname, uint32_t *magic, int64_t *size) { // TODO
  uint32_t      version = 0;
  SFile         file;
  SFile *       pFile = &file;

  tfsInitFile(&(pFile->file), TFS_PRIMARY_LEVEL, TFS_PRIMARY_ID, fname);
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

// Retention ===========================================
void tsdbRemoveFilesBeyondRetention(STsdbRepo *pRepo, SFidGroup *pFidGroup) {
  STsdbFileH *pFileH = pRepo->tsdbFileH;
  SFileGroup *pGroup = pFileH->pFGroup;

  pthread_rwlock_wrlock(&(pFileH->fhlock));

  while (pFileH->nFGroups > 0 && pGroup[0].fileId < pFidGroup->minFid) {
    tsdbRemoveFileGroup(pRepo, pGroup);
  }

  pthread_rwlock_unlock(&(pFileH->fhlock));
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

int tsdbApplyRetention(STsdbRepo *pRepo, SFidGroup *pFidGroup) {
  STsdbFileH *pFileH = pRepo->tsdbFileH;

  for (int i = 0; i < pFileH->nFGroups; i++) {
    SFileGroup ofg = pFileH->pFGroup[i];

    int level = tsdbGetFidLevel(ofg.fileId, *pFidGroup);
    ASSERT(level >= 0);

    if (level == ofg.files[0].file.level) continue;

    // COPY THE FILE GROUP TO THE RIGHT LEVEL
    SFileGroup nfg = ofg;
    int        id = TFS_UNDECIDED_ID;
    int        type = 0;
    for (; type < TSDB_FILE_TYPE_MAX; type++) {
      tfsInitFile(&nfg.files[type].file, level, id, nfg.files[type].file.rname);
      if (tfscopy(&(ofg.files[type].file), &(nfg.files[type].file)) < 0) {
        if (terrno == TSDB_CODE_FS_INVLD_LEVEL) break;
        tsdbError("vgId:%d failed to move fid %d from level %d to level %d since %s", REPO_ID(pRepo), ofg.fileId,
                  ofg.files[0].file.level, level, strerror(terrno));
        return -1;
      }

      id = nfg.files[type].file.level;
      id = nfg.files[type].file.id;
    }

    if (type < TSDB_FILE_TYPE_MAX) continue;

    // Register new file into TSDB
    pthread_rwlock_wrlock(&(pFileH->fhlock));
    pFileH->pFGroup[i] = nfg;
    pthread_rwlock_unlock(&(pFileH->fhlock));

    for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
      SFile *pFile = &(ofg.files[type]);
      tfsremove(&(pFile->file));
    }

    tsdbDebug("vgId:%d move file group %d from level %d to level %d", REPO_ID(pRepo), ofg.fileId,
              ofg.files[0].file.level, level);
  }

  return 0;
}

static void *tsdbScanAllFiles(STsdbRepo *pRepo) {
  void *       farray = NULL;
  TDIR *       tdir = NULL;
  char         dirName[TSDB_FILENAME_LEN] = "\0";
  char         bname[TSDB_FILENAME_LEN] = "\0";
  regex_t      regex1 = {0};
  const TFILE *pf = NULL;

  farray = taosArrayInit(256, sizeof(TFILE));
  if (farray == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  regcomp(&regex1, "^v[0-9]+f[0-9]+\\.(head|data|last|stat|l|d|h|s)$", REG_EXTENDED);

  snprintf(dirName, TSDB_FILENAME_LEN, "vnode/vnode%d/tsdb/data", REPO_ID(pRepo));

  tdir = tfsOpendir(dirName);

  while ((pf = tfsReaddir(tdir)) != NULL) {
    tfsbasename(pf, bname);

    int code = regexec(&regex1, bname, 0, NULL, 0);
    if (code != 0) {
      tsdbWarn("vgId:%d file %s exists, ignore it", REPO_ID(pRepo), pf->aname);
      continue;
    }

    taosArrayPush(farray, pf);
  }

  regfree(&regex1);
  tfsClosedir(tdir);

  return farray;
}

static int tsdbCompareFile(const void *arg1, const void *arg2) {
  char   bname1[TSDB_FILENAME_LEN] = "\0";
  char   bname2[TSDB_FILENAME_LEN] = "\0";
  TFILE *pf1 = (TFILE *)arg1;
  TFILE *pf2 = (TFILE *)arg2;
  int    vid1, fid1, vid2, fid2;

  tfsbasename(pf1, bname1);
  tfsbasename(pf2, bname2);

  sscanf(bname1, "v%df%d", &vid1, &fid1);
  sscanf(bname2, "v%df%d", &vid2, &fid2);

  ASSERT(vid1 == vid2);
  if (fid1 < fid2) {
    return -1;
  } else if (fid1 == fid2) {
    return 0;
  } else {
    return 1;
  }
}

static int tsdbRestoreFile(STsdbRepo *pRepo, TFILE *pfiles, int nfile) {
  char        backname[TSDB_FILENAME_LEN*2] = "\0";
  char        bname[TSDB_FILENAME_LEN] = "\0";
  STsdbFileH *pFileH = pRepo->tsdbFileH;
  TFILE *     pfArray[TSDB_FILE_TYPE_MAX] = {0};
  TFILE *     pHf = NULL;
  TFILE *     pLf = NULL;
  SFileGroup  fg = {0};
  int         vid = 0;
  int         fid = 0;
  char        suffix[TSDB_FILENAME_LEN] = "\0";

  for (int i = 0; i < nfile; i++) {
    TFILE *pf = pfiles + i;

    tfsbasename(pf, bname);
    tsdbParseFname(bname, &vid, &fid, suffix);

    if (strcmp(suffix, ".head") == 0) {
      pfArray[TSDB_FILE_TYPE_HEAD] = pf;
    } else if (strcmp(suffix, ".data") == 0) {
      pfArray[TSDB_FILE_TYPE_DATA] = pf;
    } else if (strcmp(suffix, ".last") == 0) {
      pfArray[TSDB_FILE_TYPE_LAST] = pf;
    } else if (strcmp(suffix, ".l") == 0) {
      pLf = pf;
    } else if (strcmp(suffix, ".h") == 0) {
      pHf = pf;
    } else {
      tsdbWarn("vgId:%d invalid file %s exists, ignore it", REPO_ID(pRepo), pf->aname);
    }
  }

  if (pfArray[TSDB_FILE_TYPE_HEAD] == NULL || pfArray[TSDB_FILE_TYPE_DATA] == NULL || pfArray[TSDB_FILE_TYPE_LAST] == NULL) {
    for (int i = 0; i < nfile; i++) {
      snprintf(backname, TSDB_FILENAME_LEN*2, "%s_bak", (pfiles + i)->aname);
      rename((pfiles + i)->aname, backname);
    }

    return -1;
  }

  if (pHf == NULL) {
    if (pLf != NULL) {
      rename(pLf->aname, pfArray[TSDB_FILE_TYPE_LAST]->aname);
    } 
  } else {
    if (pLf != NULL) {
      remove(pLf->aname);
    }
    remove(pHf->aname);
  }

  // Register file
  fg.fileId = fid;

  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    SFile * pFile = fg.files + type;
    uint32_t version = 0;

    pFile->fd = -1;
    pFile->file = *pfArray[type]; // TODO
    tsdbOpenFile(pFile, O_RDONLY);
    tsdbLoadFileHeader(pFile, &version);
    tsdbCloseFile(pFile);
  }

  pFileH->pFGroup[pFileH->nFGroups++] = fg;

  tfsIncDiskFile(pfArray[TSDB_FILE_TYPE_HEAD]->level, pfArray[TSDB_FILE_TYPE_HEAD]->id, TSDB_FILE_TYPE_MAX);

  return 0;
}

static void tsdbParseFname(const char *bname, int *vid, int *fid, char *suffix) {
  sscanf(bname, "v%df%d%s", vid, fid, suffix);
}