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

#include "sma.h"

// =================================================================================================

// static int32_t tdFetchQTaskInfoFiles(SSma *pSma, int64_t version, SArray **output);
static int32_t tdQTaskInfCmprFn1(const void *p1, const void *p2);

static FORCE_INLINE int32_t tPutQTaskF(uint8_t *p, SQTaskFile *pFile) {
  int32_t n = 0;

  n += tPutI8(p ? p + n : p, pFile->level);
  n += tPutI64v(p ? p + n : p, pFile->size);
  n += tPutI64v(p ? p + n : p, pFile->suid);
  n += tPutI64v(p ? p + n : p, pFile->version);

  return n;
}

static int32_t tdRSmaFSToBinary(uint8_t *p, SRSmaFS *pFS) {
  int32_t  n = 0;
  uint32_t size = taosArrayGetSize(pFS->aQTaskInf);

  // version
  n += tPutI8(p ? p + n : p, 0);

  // SArray<SQTaskFile>
  n += tPutU32v(p ? p + n : p, size);
  for (uint32_t i = 0; i < size; ++i) {
    n += tPutQTaskF(p ? p + n : p, taosArrayGet(pFS->aQTaskInf, i));
  }

  return n;
}

int32_t tdRSmaGetQTaskF(uint8_t *p, SQTaskFile *pFile) {
  int32_t n = 0;

  n += tGetI8(p + n, &pFile->level);
  n += tGetI64v(p + n, &pFile->size);
  n += tGetI64v(p + n, &pFile->suid);
  n += tGetI64v(p + n, &pFile->version);

  return n;
}

static int32_t tsdbBinaryToFS(uint8_t *pData, int64_t nData, SRSmaFS *pFS) {
  int32_t code = 0;
  int32_t n = 0;
  int8_t  version = 0;

  // version
  n += tGetI8(pData + n, &version);

  // SArray<SQTaskFile>
  taosArrayClear(pFS->aQTaskInf);
  uint32_t size = 0;
  n += tGetU32v(pData + n, &size);
  for (uint32_t i = 0; i < size; ++i) {
    SQTaskFile qTaskF = {0};

    int32_t nt = tdRSmaGetQTaskF(pData + n, &qTaskF);
    if (nt < 0) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }

    n += nt;
    if (taosArrayPush(pFS->aQTaskInf, &qTaskF) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
  }

  ASSERT(n + sizeof(TSCKSUM) == nData);

_exit:
  return code;
}

static int32_t tdRSmaSaveFSToFile(SRSmaFS *pFS, const char *fname) {
  int32_t code = 0;
  int32_t lino = 0;

  // encode to binary
  int32_t  size = tdRSmaFSToBinary(NULL, pFS) + sizeof(TSCKSUM);
  uint8_t *pData = taosMemoryMalloc(size);
  if (pData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  tdRSmaFSToBinary(pData, pFS);
  taosCalcChecksumAppend(0, pData, size);

  // save to file
  TdFilePtr pFD = taosCreateFile(fname, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (pFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  int64_t n = taosWriteFile(pFD, pData, size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    taosCloseFile(&pFD);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (taosFsyncFile(pFD) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    taosCloseFile(&pFD);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  taosCloseFile(&pFD);

_exit:
  if (pData) taosMemoryFree(pData);
  if (code) {
    smaError("%s failed at line %d since %s, fname:%s", __func__, lino, tstrerror(code), fname);
  }
  return code;
}

static int32_t tdRSmaFSCreate(SRSmaFS *pFS, int32_t size) {
  int32_t code = 0;

  pFS->aQTaskInf = taosArrayInit(size, sizeof(SQTaskFile));
  if (pFS->aQTaskInf == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

_exit:
  return code;
}

static void tdRSmaGetCurrentFName(SSma *pSma, char *current, char *current_t) {
  SVnode *pVnode = pSma->pVnode;
  if (pVnode->pTfs) {
    if (current) {
      snprintf(current, TSDB_FILENAME_LEN - 1, "%s%svnode%svnode%d%srsma%sPRESENT", tfsGetPrimaryPath(pVnode->pTfs),
               TD_DIRSEP, TD_DIRSEP, TD_VID(pVnode), TD_DIRSEP, TD_DIRSEP);
    }
    if (current_t) {
      snprintf(current, TSDB_FILENAME_LEN - 1, "%s%svnode%svnode%d%srsma%sPRESENT.t", tfsGetPrimaryPath(pVnode->pTfs),
               TD_DIRSEP, TD_DIRSEP, TD_VID(pVnode), TD_DIRSEP, TD_DIRSEP);
    }
  } else {
#if 0
    if (current) {
      snprintf(current, TSDB_FILENAME_LEN - 1, "%s%sPRESENT", pTsdb->path, TD_DIRSEP);
    }
    if (current_t) {
      snprintf(current_t, TSDB_FILENAME_LEN - 1, "%s%sPRESENT.t", pTsdb->path, TD_DIRSEP);
    }
#endif
  }
}

static int32_t tdRSmaLoadFSFromFile(const char *fname, SRSmaFS *pFS) {
  int32_t  code = 0;
  int32_t  lino = 0;
  uint8_t *pData = NULL;

  // load binary
  TdFilePtr pFD = taosOpenFile(fname, TD_FILE_READ);
  if (pFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  int64_t size;
  if (taosFStatFile(pFD, &size, NULL) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    taosCloseFile(&pFD);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pData = taosMemoryMalloc(size);
  if (pData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    taosCloseFile(&pFD);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (taosReadFile(pFD, pData, size) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    taosCloseFile(&pFD);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (!taosCheckChecksumWhole(pData, size)) {
    code = TSDB_CODE_FILE_CORRUPTED;
    taosCloseFile(&pFD);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  taosCloseFile(&pFD);

  // decode binary
  code = tsdbBinaryToFS(pData, size, pFS);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (pData) taosMemoryFree(pData);
  if (code) {
    smaError("%s failed at line %d since %s, fname:%s", __func__, lino, tstrerror(code), fname);
  }
  return code;
}

static int32_t tdQTaskInfCmprFn1(const void *p1, const void *p2) {
  const SQTaskFile *q1 = (const SQTaskFile *)p1;
  const SQTaskFile *q2 = (const SQTaskFile *)p2;

  if (q1->suid < q2->suid) {
    return -1;
  } else if (q1->suid > q2->suid) {
    return 1;
  }

  if (q1->level < q2->level) {
    return -1;
  } else if (q1->level > q2->level) {
    return 1;
  }

  if (q1->version < q2->version) {
    return -1;
  } else if (q1->version > q2->version) {
    return 1;
  }

  return 0;
}

static int32_t tdRSmaFSApplyChange(SSma *pSma, SRSmaFS *pFS) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SVnode    *pVnode = pSma->pVnode;
  SSmaEnv   *pEnv = SMA_RSMA_ENV(pSma);
  SRSmaStat *pStat = (SRSmaStat *)SMA_ENV_STAT(pEnv);
  SRSmaFS   *pFSOld = RSMA_FS(pStat);
  int64_t    version = pStat->commitAppliedVer;
  char       fname[TSDB_FILENAME_LEN] = {0};

  // SQTaskFile
  int32_t   nNew = taosArrayGetSize(pFS->aQTaskInf);
  int32_t iNew = 0;
  while (iNew < nNew) {
    SQTaskFile *pQTaskFNew =  TARRAY_GET_ELEM(pFS->aQTaskInf, iNew);

    int32_t idx = taosArraySearchIdx(pFSOld->aQTaskInf, pQTaskFNew, tdQTaskInfCmprFn1, TD_GE);

    if (idx < 0) {
      idx = taosArrayGetSize(pFSOld->aQTaskInf);
      pQTaskFNew->nRef = 1;
    } else {
      SQTaskFile *pTaskF = (SQTaskFile *)taosArrayGet(pFSOld->aQTaskInf, idx);
      int32_t     c = tdQTaskInfCmprFn1(pTaskF, pQTaskFNew);
      if (c < 0) {
        
      } else if(c == 0) {
        ++iNew;
        continue;
      } else {
        ASSERT(0);
      }
    }

    if (taosArrayInsert(pFS->aQTaskInf, idx, qTaskF) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }

  }

_exit:
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

/**
 * @brief Fetch qtaskfiles LE than version
 *
 * @param pSma
 * @param version
 * @param output
 * @return int32_t
 */
#if 0
static int32_t tdFetchQTaskInfoFiles(SSma *pSma, int64_t version, SArray **output) {
  SVnode       *pVnode = pSma->pVnode;
  TdDirPtr      pDir = NULL;
  TdDirEntryPtr pDirEntry = NULL;
  char          dir[TSDB_FILENAME_LEN];
  const char   *pattern = "v[0-9]+qinf\\.v([0-9]+)?$";
  regex_t       regex;
  int           code = 0;

  terrno = TSDB_CODE_SUCCESS;

  tdRSmaGetDirName(TD_VID(pVnode), tfsGetPrimaryPath(pVnode->pTfs), VNODE_RSMA_DIR, true, dir);

  if (!taosCheckExistFile(dir)) {
    smaDebug("vgId:%d, fetch qtask files, no need as dir %s not exist", TD_VID(pVnode), dir);
    return TSDB_CODE_SUCCESS;
  }

  // Resource allocation and init
  if ((code = regcomp(&regex, pattern, REG_EXTENDED)) != 0) {
    terrno = TSDB_CODE_RSMA_REGEX_MATCH;
    char errbuf[128];
    regerror(code, &regex, errbuf, sizeof(errbuf));
    smaWarn("vgId:%d, fetch qtask files, regcomp for %s failed since %s", TD_VID(pVnode), dir, errbuf);
    return TSDB_CODE_FAILED;
  }

  if (!(pDir = taosOpenDir(dir))) {
    regfree(&regex);
    terrno = TAOS_SYSTEM_ERROR(errno);
    smaError("vgId:%d, fetch qtask files, open dir %s failed since %s", TD_VID(pVnode), dir, terrstr());
    return TSDB_CODE_FAILED;
  }

  int32_t    dirLen = strlen(dir);
  char      *dirEnd = POINTER_SHIFT(dir, dirLen);
  regmatch_t regMatch[2];
  while ((pDirEntry = taosReadDir(pDir))) {
    char *entryName = taosGetDirEntryName(pDirEntry);
    if (!entryName) {
      continue;
    }

    code = regexec(&regex, entryName, 2, regMatch, 0);

    if (code == 0) {
      // match
      smaInfo("vgId:%d, fetch qtask files, max ver:%" PRIi64 ", %s found", TD_VID(pVnode), version, entryName);

      int64_t ver = -1;
      sscanf((const char *)POINTER_SHIFT(entryName, regMatch[1].rm_so), "%" PRIi64, &ver);
      if ((ver <= version) && (ver > -1)) {
        if (!(*output)) {
          if (!(*output = taosArrayInit(1, POINTER_BYTES))) {
            terrno = TSDB_CODE_OUT_OF_MEMORY;
            goto _end;
          }
        }
        char *entryDup = strdup(entryName);
        if (!entryDup) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          goto _end;
        }
        if (!taosArrayPush(*output, &entryDup)) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          goto _end;
        }
      } else {
      }
    } else if (code == REG_NOMATCH) {
      // not match
      smaTrace("vgId:%d, fetch qtask files, not match %s", TD_VID(pVnode), entryName);
      continue;
    } else {
      // has other error
      char errbuf[128];
      regerror(code, &regex, errbuf, sizeof(errbuf));
      smaWarn("vgId:%d, fetch qtask files, regexec failed since %s", TD_VID(pVnode), errbuf);
      terrno = TSDB_CODE_RSMA_REGEX_MATCH;
      goto _end;
    }
  }
_end:
  taosCloseDir(&pDir);
  regfree(&regex);
  return terrno == 0 ? TSDB_CODE_SUCCESS : TSDB_CODE_FAILED;
}
#endif

static int32_t tdRSmaFSScanAndTryFix(SSma *pSma) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SVnode    *pVnode = pSma->pVnode;
  SSmaEnv   *pEnv = SMA_RSMA_ENV(pSma);
  SRSmaStat *pStat = (SRSmaStat *)SMA_ENV_STAT(pEnv);
  SRSmaFS   *pFS = RSMA_FS(pStat);
  char       fname[TSDB_FILENAME_LEN] = {0};
  char       fnameVer[TSDB_FILENAME_LEN] = {0};

  // SArray<SQTaskFile>
  int32_t size = taosArrayGetSize(pFS->aQTaskInf);
  for (int32_t i = 0; i < size; ++i) {
    SQTaskFile *pTaskF = (SQTaskFile *)taosArrayGet(pFS->aQTaskInf, i);

    // main.tdb =========
    tdRSmaQTaskInfoGetFullName(TD_VID(pVnode), pTaskF->suid, pTaskF->level, pTaskF->version,
                               tfsGetPrimaryPath(pVnode->pTfs), fnameVer);
    tdRSmaQTaskInfoGetFullName(TD_VID(pVnode), pTaskF->suid, pTaskF->level, -1, tfsGetPrimaryPath(pVnode->pTfs), fname);

    if (taosCheckExistFile(fnameVer)) {
      if (taosRenameFile(fnameVer, fname) < 0) {
        code = TAOS_SYSTEM_ERROR(errno);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
      smaDebug("vgId:%d, %s:%d succeed to to rename %s to %s", TD_VID(pVnode), __func__, lino, fnameVer, fname);
    } else if (taosCheckExistFile(fname)) {
      if (taosRemoveFile(fname) < 0) {
        code = TAOS_SYSTEM_ERROR(errno);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
      smaDebug("vgId:%d, %s:%d succeed to to remove %s", TD_VID(pVnode), __func__, lino, fname);
    }
  }

  {
    // remove those invalid files (todo)
    // main.tdb-journal.5 // TDB should handle its clear for kill -9
  }

_exit:
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

// EXPOSED APIS ====================================================================================

int32_t tdRSmaFSOpen(SSma *pSma, int64_t version, int8_t rollback) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SVnode    *pVnode = pSma->pVnode;
  SSmaEnv   *pEnv = SMA_RSMA_ENV(pSma);
  SRSmaStat *pStat = (SRSmaStat *)SMA_ENV_STAT(pEnv);

  // open handle
  code = tdRSmaFSCreate(RSMA_FS(pStat), 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  // open impl
  char current[TSDB_FILENAME_LEN] = {0};
  char current_t[TSDB_FILENAME_LEN] = {0};
  tdRSmaGetCurrentFName(pSma, current, current_t);

  if (taosCheckExistFile(current)) {
    code = tdRSmaLoadFSFromFile(current, RSMA_FS(pStat));
    TSDB_CHECK_CODE(code, lino, _exit);

    if (taosCheckExistFile(current_t)) {
      if (rollback) {
        code = tdRSmaFSRollback(pSma);
        TSDB_CHECK_CODE(code, lino, _exit);
      } else {
        code = tdRSmaFSCommit(pSma);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }
  } else {
    // 1st open with empty current/qTaskInfoFile
    code = tdRSmaSaveFSToFile(RSMA_FS(pStat), current);
    TSDB_CHECK_CODE(code, lino, _exit);
    ASSERT(!rollback);
  }

  // scan and try fix(remove main.db/main.db.xxx and use the one with version)
  code = tdRSmaFSScanAndTryFix(pSma);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

void tdRSmaFSClose(SRSmaFS *pFS) { taosArrayDestroy(pFS->aQTaskInf); }

int32_t tdRSmaFSPrepareCommit(SSma *pSma, SRSmaFS *pFSNew) {
  int32_t code = 0;
  int32_t lino = 0;
  char    tfname[TSDB_FILENAME_LEN];

  tdRSmaGetCurrentFName(pSma, NULL, tfname);

  // gnrt PRESENT.t
  code = tdRSmaSaveFSToFile(pFSNew, tfname);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s", TD_VID(pSma->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tdRSmaFSCommit(SSma *pSma) {
  int32_t code = 0;
  int32_t lino = 0;
  SRSmaFS fs = {0};

  char current[TSDB_FILENAME_LEN] = {0};
  char current_t[TSDB_FILENAME_LEN] = {0};
  tdRSmaGetCurrentFName(pSma, current, current_t);

  if (!taosCheckExistFile(current_t)) {
    goto _exit;
  }

  // rename the file
  if (taosRenameFile(current_t, current) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // Load the new FS
  code = tdRSmaFSCreate(&fs, 1);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tdRSmaLoadFSFromFile(current, &fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  // apply file change
  code = tdRSmaFSApplyChange(pSma, &fs);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  tdRSmaFSClose(&fs);
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s", SMA_VID(pSma), __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tdRSmaFSFinishCommit(SSma *pSma) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SSmaEnv   *pSmaEnv = SMA_RSMA_ENV(pSma);
  SRSmaStat *pStat = (SRSmaStat *)SMA_ENV_STAT(pSmaEnv);

  taosWLockLatch(RSMA_FS_LOCK(pStat));
  if ((code = tdRSmaFSCommit(pSma)) < 0) {
    taosWUnLockLatch(RSMA_FS_LOCK(pStat));
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  taosWUnLockLatch(RSMA_FS_LOCK(pStat));

_exit:
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s", SMA_VID(pSma), __func__, lino, tstrerror(code));
  } else {
    smaInfo("vgId:%d, rsmaFS finish commit", SMA_VID(pSma));
  }
  return code;
}

int32_t tdRSmaFSRollback(SSma *pSma) {
  int32_t code = 0;
  int32_t lino = 0;

  char current_t[TSDB_FILENAME_LEN] = {0};
  tdRSmaGetCurrentFName(pSma, NULL, current_t);
  (void)taosRemoveFile(current_t);

_exit:
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s", SMA_VID(pSma), __func__, lino, tstrerror(errno));
  }
  return code;
}

int32_t tdRSmaFSUpsertQTaskFile(SRSmaFS *pFS, SQTaskFile *qTaskFile, int32_t size) {
  int32_t code = 0;

  for (int32_t i = 0; i < size; ++i) {
    SQTaskFile *qTaskF = qTaskFile + i;

    int32_t idx = taosArraySearchIdx(pFS->aQTaskInf, qTaskF, tdQTaskInfCmprFn1, TD_GE);

    if (idx < 0) {
      idx = taosArrayGetSize(pFS->aQTaskInf);
    } else {
      SQTaskFile *pTaskF = (SQTaskFile *)taosArrayGet(pFS->aQTaskInf, idx);
      int32_t     c = tdQTaskInfCmprFn1(pTaskF, qTaskF);
      if (c == 0) {
        ASSERT(pTaskF->size == qTaskF->size);
        ASSERT(0);
        goto _exit;
      }
    }

    if (taosArrayInsert(pFS->aQTaskInf, idx, qTaskF) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
  }

_exit:
  return code;
}

int32_t tdRSmaFSRef(SSma *pSma, SRSmaStat *pStat, int64_t suid, int8_t level, int64_t version) {
  SArray     *aQTaskInf = RSMA_FS(pStat)->aQTaskInf;
  SQTaskFile  qTaskF = {.level = level, .suid = suid, .version = version};
  SQTaskFile *pTaskF = NULL;
  int32_t     oldVal = 0;

  taosRLockLatch(RSMA_FS_LOCK(pStat));
  if (suid > 0 && level > 0) {
    ASSERT(version > 0);
    if ((pTaskF = taosArraySearch(aQTaskInf, &qTaskF, tdQTaskInfCmprFn1, TD_EQ))) {
      oldVal = atomic_fetch_add_32(&pTaskF->nRef, 1);
      ASSERT(oldVal > 0);
    }
  } else {
    // ref all
    int32_t size = taosArrayGetSize(aQTaskInf);
    for (int32_t i = 0; i < size; ++i) {
      pTaskF = TARRAY_GET_ELEM(aQTaskInf, i);
      oldVal = atomic_fetch_add_32(&pTaskF->nRef, 1);
      ASSERT(oldVal > 0);
    }
  }
  taosRUnLockLatch(RSMA_FS_LOCK(pStat));
  return oldVal;
}

void tdRSmaFSUnRef(SSma *pSma, SRSmaStat *pStat, int64_t suid, int8_t level, int64_t version) {
  SVnode     *pVnode = pSma->pVnode;
  SArray     *aQTaskInf = RSMA_FS(pStat)->aQTaskInf;
  char        qTaskFullName[TSDB_FILENAME_LEN];
  SQTaskFile  qTaskF = {.level = level, .suid = suid, .version = version};
  SQTaskFile *pTaskF = NULL;
  int32_t     idx = -1;

  taosWLockLatch(RSMA_FS_LOCK(pStat));
  if (suid > 0 && level > 0) {
    ASSERT(version > 0);
    if ((idx = taosArraySearchIdx(aQTaskInf, &qTaskF, tdQTaskInfCmprFn1, TD_EQ)) >= 0) {
      ASSERT(idx < taosArrayGetSize(aQTaskInf));
      pTaskF = taosArrayGet(aQTaskInf, idx);
      if (atomic_sub_fetch_32(&pTaskF->nRef, 1) <= 0) {
        tdRSmaQTaskInfoGetFullName(TD_VID(pVnode), pTaskF->suid, level, pTaskF->version,
                                   tfsGetPrimaryPath(pVnode->pTfs), qTaskFullName);
        if (taosRemoveFile(qTaskFullName) < 0) {
          smaWarn("vgId:%d, failed to remove %s since %s", TD_VID(pVnode), qTaskFullName,
                  tstrerror(TAOS_SYSTEM_ERROR(errno)));
        } else {
          smaDebug("vgId:%d, success to remove %s", TD_VID(pVnode), qTaskFullName);
        }
        taosArrayRemove(aQTaskInf, idx);
      }
    }
  } else {
    for (int32_t i = 0; i < taosArrayGetSize(aQTaskInf);) {
      pTaskF = TARRAY_GET_ELEM(aQTaskInf, i);
      int32_t nRef = INT32_MAX;
      if (pTaskF->version == version) {
        nRef = atomic_sub_fetch_32(&pTaskF->nRef, 1);
      } else if (pTaskF->version < version) {
        nRef = atomic_load_32(&pTaskF->nRef);
      }
      if (nRef <= 0) {
        tdRSmaQTaskInfoGetFullName(TD_VID(pVnode), pTaskF->suid, pTaskF->level, pTaskF->version,
                                   tfsGetPrimaryPath(pVnode->pTfs), qTaskFullName);
        if (taosRemoveFile(qTaskFullName) < 0) {
          smaWarn("vgId:%d, failed to remove %s since %s", TD_VID(pVnode), qTaskFullName,
                  tstrerror(TAOS_SYSTEM_ERROR(errno)));
        } else {
          smaDebug("vgId:%d, success to remove %s", TD_VID(pVnode), qTaskFullName);
        }
        taosArrayRemove(aQTaskInf, i);
        continue;
      }
      ++i;
    }
  }

  taosWUnLockLatch(RSMA_FS_LOCK(pStat));
}

int32_t tdRSmaFSTakeSnapshot(SSma *pSma, SRSmaFS *pFSOut) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SSmaEnv   *pEnv = SMA_RSMA_ENV(pSma);
  SRSmaStat *pStat = (SRSmaStat *)SMA_ENV_STAT(pEnv);

  taosRLockLatch(RSMA_FS_LOCK(pStat));
  code = tdRSmaFSCopy(pSma, pFSOut);
  TSDB_CHECK_CODE(code, lino, _exit);
  taosWUnLockLatch(RSMA_FS_LOCK(pStat));
_exit:
  if (code) {
    taosWUnLockLatch(RSMA_FS_LOCK(pStat));
    smaError("vgId:%d, %s failed at line %d since %s", TD_VID(pSma->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tdRSmaFSCopy(SSma *pSma, SRSmaFS *pFSOut) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SSmaEnv   *pEnv = SMA_RSMA_ENV(pSma);
  SRSmaStat *pStat = (SRSmaStat *)SMA_ENV_STAT(pEnv);
  SRSmaFS   *pFS = RSMA_FS(pStat);
  int32_t    size = 0;

  code = tdRSmaFSCreate(pFSOut, size);
  TSDB_CHECK_CODE(code, lino, _exit);

  taosArraySetSize(pFSOut->aQTaskInf, size);
  memcpy(TARRAY_GET_ELEM(pFSOut->aQTaskInf, 0), TARRAY_GET_ELEM(pFS->aQTaskInf, 0), size * sizeof(SQTaskFile));

_exit:
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s", TD_VID(pSma->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}
