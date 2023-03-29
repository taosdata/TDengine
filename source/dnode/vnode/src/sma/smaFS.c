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
  n += tPutI64v(p ? p + n : p, pFile->mtime);

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
  n += tGetI64v(p + n, &pFile->mtime);

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
      code = TSDB_CODE_FILE_CORRUPTED;
      goto _exit;
    }

    n += nt;
    if (taosArrayPush(pFS->aQTaskInf, &qTaskF) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
  }

  if (ASSERTS(n + sizeof(TSCKSUM) == nData, "n:%d + sizeof(TSCKSUM):%d != nData:%d", n, (int32_t)sizeof(TSCKSUM),
              nData)) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _exit;
  }

_exit:
  return code;
}

static int32_t tdRSmaSaveFSToFile(SRSmaFS *pFS, const char *fname) {
  int32_t   code = 0;
  int32_t   lino = 0;
  TdFilePtr pFD = NULL;

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
  pFD = taosCreateFile(fname, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (!pFD) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  int64_t n = taosWriteFile(pFD, pData, size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (taosFsyncFile(pFD) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  taosCloseFile(&pFD);
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
      snprintf(current_t, TSDB_FILENAME_LEN - 1, "%s%svnode%svnode%d%srsma%sPRESENT.t", tfsGetPrimaryPath(pVnode->pTfs),
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
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pData = taosMemoryMalloc(size);
  if (pData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (taosReadFile(pFD, pData, size) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (!taosCheckChecksumWhole(pData, size)) {
    code = TSDB_CODE_FILE_CORRUPTED;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // decode binary
  code = tsdbBinaryToFS(pData, size, pFS);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  taosCloseFile(&pFD);
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
    return -2;
  } else if (q1->version > q2->version) {
    return 1;
  }

  return 0;
}

static int32_t tdRSmaFSApplyChange(SSma *pSma, SRSmaFS *pFSNew) {
  int32_t    code = 0;
  int32_t    lino = 0;
  int32_t    nRef = 0;
  SVnode    *pVnode = pSma->pVnode;
  SSmaEnv   *pEnv = SMA_RSMA_ENV(pSma);
  SRSmaStat *pStat = (SRSmaStat *)SMA_ENV_STAT(pEnv);
  SRSmaFS   *pFSOld = RSMA_FS(pStat);
  int64_t    version = pStat->commitAppliedVer;
  char       fname[TSDB_FILENAME_LEN] = {0};

  // SQTaskFile
  int32_t nNew = taosArrayGetSize(pFSNew->aQTaskInf);
  int32_t iNew = 0;
  while (iNew < nNew) {
    SQTaskFile *pQTaskFNew = TARRAY_GET_ELEM(pFSNew->aQTaskInf, iNew++);

    int32_t idx = taosArraySearchIdx(pFSOld->aQTaskInf, pQTaskFNew, tdQTaskInfCmprFn1, TD_GE);

    if (idx < 0) {
      idx = taosArrayGetSize(pFSOld->aQTaskInf);
      pQTaskFNew->nRef = 1;
    } else {
      SQTaskFile *pTaskF = TARRAY_GET_ELEM(pFSOld->aQTaskInf, idx);
      int32_t     c1 = tdQTaskInfCmprFn1(pQTaskFNew, pTaskF);
      if (c1 == 0) {
        // utilize the item in pFSOld->qQTaskInf, instead of pFSNew
        continue;
      } else if (c1 < 0) {
        // NOTHING TODO
      } else {
        code = TSDB_CODE_RSMA_FS_UPDATE;
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }

    if (taosArrayInsert(pFSOld->aQTaskInf, idx, pQTaskFNew) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    // remove previous version
    while (--idx >= 0) {
      SQTaskFile *preTaskF = TARRAY_GET_ELEM(pFSOld->aQTaskInf, idx);
      int32_t     c2 = tdQTaskInfCmprFn1(preTaskF, pQTaskFNew);
      if (c2 == 0) {
        code = TSDB_CODE_RSMA_FS_UPDATE;
        TSDB_CHECK_CODE(code, lino, _exit);
      } else if (c2 != -2) {
        break;
      }

      nRef = atomic_sub_fetch_32(&preTaskF->nRef, 1);
      if (nRef <= 0) {
        tdRSmaQTaskInfoGetFullName(TD_VID(pVnode), preTaskF->suid, preTaskF->level, preTaskF->version,
                                   tfsGetPrimaryPath(pVnode->pTfs), fname);
        (void)taosRemoveFile(fname);
        taosArrayRemove(pFSOld->aQTaskInf, idx);
      }
    }
  }

_exit:
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tdRSmaFSScanAndTryFix(SSma *pSma) {
  int32_t code = 0;
#if 0
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
#endif
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
    // 1st time open with empty current/qTaskInfoFile
    code = tdRSmaSaveFSToFile(RSMA_FS(pStat), current);
    TSDB_CHECK_CODE(code, lino, _exit);
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

void tdRSmaFSClose(SRSmaFS *pFS) { pFS->aQTaskInf = taosArrayDestroy(pFS->aQTaskInf); }

int32_t tdRSmaFSPrepareCommit(SSma *pSma, SRSmaFS *pFSNew) {
  int32_t code = 0;
  int32_t lino = 0;
  char    tfname[TSDB_FILENAME_LEN];

  tdRSmaGetCurrentFName(pSma, NULL, tfname);

  // generate PRESENT.t
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

  // load the new FS
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
  code = tdRSmaFSCommit(pSma);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  taosWUnLockLatch(RSMA_FS_LOCK(pStat));
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

int32_t tdRSmaFSUpsertQTaskFile(SSma *pSma, SRSmaFS *pFS, SQTaskFile *qTaskFile, int32_t nSize) {
  int32_t code = 0;

  for (int32_t i = 0; i < nSize; ++i) {
    SQTaskFile *qTaskF = qTaskFile + i;

    int32_t idx = taosArraySearchIdx(pFS->aQTaskInf, qTaskF, tdQTaskInfCmprFn1, TD_GE);

    if (idx < 0) {
      idx = taosArrayGetSize(pFS->aQTaskInf);
    } else {
      SQTaskFile *pTaskF = (SQTaskFile *)taosArrayGet(pFS->aQTaskInf, idx);
      int32_t     c = tdQTaskInfCmprFn1(pTaskF, qTaskF);
      if (c == 0) {
        if (pTaskF->size != qTaskF->size) {
          code = TSDB_CODE_RSMA_FS_UPDATE;
          smaError("vgId:%d, %s failed at line %d since %s, level:%" PRIi8 ", suid:%" PRIi64 ", version:%" PRIi64
                   ", size:%" PRIi64 " != %" PRIi64,
                   SMA_VID(pSma), __func__, __LINE__, tstrerror(code), pTaskF->level, pTaskF->suid, pTaskF->version,
                   pTaskF->size, qTaskF->size);
          goto _exit;
        }
        continue;
      }
    }

    if (!taosArrayInsert(pFS->aQTaskInf, idx, qTaskF)) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
  }

_exit:
  return code;
}

int32_t tdRSmaFSRef(SSma *pSma, SRSmaFS *pFS) {
  int32_t    code = 0;
  int32_t    lino = 0;
  int32_t    nRef = 0;
  SSmaEnv   *pEnv = SMA_RSMA_ENV(pSma);
  SRSmaStat *pStat = (SRSmaStat *)SMA_ENV_STAT(pEnv);
  SRSmaFS   *qFS = RSMA_FS(pStat);
  int32_t    size = taosArrayGetSize(qFS->aQTaskInf);

  pFS->aQTaskInf = taosArrayInit_s(sizeof(SQTaskFile), size);
  if (pFS->aQTaskInf == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  for (int32_t i = 0; i < size; ++i) {
    SQTaskFile *qTaskF = (SQTaskFile *)taosArrayGet(qFS->aQTaskInf, i);
    nRef = atomic_fetch_add_32(&qTaskF->nRef, 1);
    if (nRef <= 0) {
      code = TSDB_CODE_RSMA_FS_REF;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  memcpy(pFS->aQTaskInf->pData, qFS->aQTaskInf->pData, size * sizeof(SQTaskFile));

_exit:
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s, nRef %d", TD_VID(pSma->pVnode), __func__, lino, tstrerror(code),
             nRef);
  }
  return code;
}

void tdRSmaFSUnRef(SSma *pSma, SRSmaFS *pFS) {
  int32_t    nRef = 0;
  char       fname[TSDB_FILENAME_LEN];
  SVnode    *pVnode = pSma->pVnode;
  SSmaEnv   *pEnv = SMA_RSMA_ENV(pSma);
  SRSmaStat *pStat = (SRSmaStat *)SMA_ENV_STAT(pEnv);
  int32_t    size = taosArrayGetSize(pFS->aQTaskInf);

  for (int32_t i = 0; i < size; ++i) {
    SQTaskFile *pTaskF = (SQTaskFile *)taosArrayGet(pFS->aQTaskInf, i);

    nRef = atomic_sub_fetch_32(&pTaskF->nRef, 1);
    if (nRef == 0) {
      tdRSmaQTaskInfoGetFullName(TD_VID(pVnode), pTaskF->suid, pTaskF->level, pTaskF->version,
                                 tfsGetPrimaryPath(pVnode->pTfs), fname);
      if (taosRemoveFile(fname) < 0) {
        smaWarn("vgId:%d, failed to remove %s since %s", TD_VID(pVnode), fname, tstrerror(TAOS_SYSTEM_ERROR(errno)));
      } else {
        smaDebug("vgId:%d, success to remove %s", TD_VID(pVnode), fname);
      }
    } else if (nRef < 0) {
      smaWarn("vgId:%d, abnormal unref %s since %s", TD_VID(pVnode), fname, tstrerror(TSDB_CODE_RSMA_FS_REF));
    }
  }

  taosArrayDestroy(pFS->aQTaskInf);
}

int32_t tdRSmaFSTakeSnapshot(SSma *pSma, SRSmaFS *pFS) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SSmaEnv   *pEnv = SMA_RSMA_ENV(pSma);
  SRSmaStat *pStat = (SRSmaStat *)SMA_ENV_STAT(pEnv);

  taosRLockLatch(RSMA_FS_LOCK(pStat));
  code = tdRSmaFSRef(pSma, pFS);
  TSDB_CHECK_CODE(code, lino, _exit);
_exit:
  taosRUnLockLatch(RSMA_FS_LOCK(pStat));
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s", TD_VID(pSma->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tdRSmaFSCopy(SSma *pSma, SRSmaFS *pFS) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SSmaEnv   *pEnv = SMA_RSMA_ENV(pSma);
  SRSmaStat *pStat = (SRSmaStat *)SMA_ENV_STAT(pEnv);
  SRSmaFS   *qFS = RSMA_FS(pStat);
  int32_t    size = taosArrayGetSize(qFS->aQTaskInf);

  code = tdRSmaFSCreate(pFS, size);
  TSDB_CHECK_CODE(code, lino, _exit);
  taosArrayAddBatch(pFS->aQTaskInf, qFS->aQTaskInf->pData, size);

_exit:
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s", TD_VID(pSma->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}
