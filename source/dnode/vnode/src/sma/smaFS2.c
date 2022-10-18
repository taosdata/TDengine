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
static int32_t tdRSmaEncodeFS(uint8_t *p, SRSmaFS *pFS);
static int32_t tPutQTaskFile(uint8_t *p, SQTaskFile *pFile);
static int32_t tPutQTaskFileSet(uint8_t *p, SQTaskFileSet *pSet);

static int32_t tdFetchQTaskInfoFiles(SSma *pSma, int64_t version, SArray **output);
static int32_t tdQTaskInfCmprFn1(const void *p1, const void *p2);
static int32_t tdQTaskInfCmprFn2(const void *p1, const void *p2);

// =================================================================================================
static int32_t tdRSmaEncodeFS(uint8_t *p, SRSmaFS *pFS) {
  int32_t  n = 0;
  uint32_t nSet = taosArrayGetSize(pFS->aQTaskFSet);

  // version
  n += tPutU8(p ? p + n : p, 0);

  // SArray<SQTaskFileSet>
  n += tPutU32v(p ? p + n : p, nSet);
  for (uint32_t i = 0; i < nSet; ++i) {
    n += tPutQTaskFileSet(p ? p + n : p, (SQTaskFileSet *)taosArrayGet(pFS->aQTaskFSet, i));
  }

  return n;
}

static int32_t tPutQTaskFileSet(uint8_t *p, SQTaskFileSet *pSet) {
  int32_t n = 0;
  int32_t nQTaskF = taosArrayGetSize(pSet->aQTaskF);

  n += tPutI64v(p ? p + n : p, pSet->suid);
  n += tPutI32v(p ? p + n : p, nQTaskF);

  // SQTaskFile
  for (int32_t i = 0; i < nQTaskF; ++i) {
    n += tPutQTaskFile(p ? p + n : p, taosArrayGet(pSet->aQTaskF, i));
  }

  return n;
}

static int32_t tPutQTaskFile(uint8_t *p, SQTaskFile *pFile) {
  int32_t n = 0;

  n += tPutI8(p ? p + n : p, pFile->level);
  n += tPutI64v(p ? p + n : p, pFile->version);
  n += tPutI64v(p ? p + n : p, pFile->mtime);

  return n;
}

static int32_t tGetQTaskFileSet(uint8_t *p, SQTaskFileSet *pSet) {
  int32_t n = 0;
  int32_t nQTaskF = 0;

  n += tGetI64v(p + n, &pSet->suid);
  n += tGetI32v(p + n, &nQTaskF);

  ASSERT(nQTaskF >= 0);

  pSet->aQTaskF = taosArrayInit(nQTaskF, sizeof(SQTaskFile));
  if (!pSet->aQTaskF) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < nQTaskF; ++i) {
    SQTaskFile *pFile = taosArrayGet(pSet->aQTaskF, i);
    pFile->nRef = 1;
    n += tGetQTaskFile(p + n, pFile);
  }

  return n;
}

static int32_t tGetQTaskFile(uint8_t *p, SQTaskFile *pFile) {
  int32_t n = 0;

  n += tGetI8(p + n, &pFile->level);
  n += tGetI64v(p + n, &pFile->version);
  n += tGetI64v(p + n, &pFile->mtime);

  return n;
}

static int32_t tdRSmaGnrtFS(SSma *pSma, SRSmaFS *pFS, char *fname) {
  int32_t   code = 0;
  int64_t   n;
  int64_t   size;
  uint8_t  *pData = NULL;
  TdFilePtr pFD = NULL;

  // to binary
  size = tdRSmaEncodeFS(NULL, pFS) + sizeof(TSCKSUM);
  pData = taosMemoryMalloc(size);
  if (pData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  n = tdRSmaEncodeFS(pData, pFS);
  ASSERT(n + sizeof(TSCKSUM) == size);
  taosCalcChecksumAppend(0, pData, size);

  // create and write
  pFD = taosOpenFile(fname, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (pFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  n = taosWriteFile(pFD, pData, size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (taosFsyncFile(pFD) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  taosCloseFile(&pFD);

  if (pData) taosMemoryFree(pData);
  return code;

_err:
  smaError("vgId:%d, rsma gnrt fs failed since %s", SMA_VID(pSma), tstrerror(code));
  if (pData) taosMemoryFree(pData);
  return code;
}

int32_t tdRSmaFSCommit1(SSma *pSma, SRSmaFS *pFSNew) {
  int32_t code = 0;
  char    tfname[TSDB_FILENAME_LEN];
  char    fname[TSDB_FILENAME_LEN];

  snprintf(tfname, TSDB_FILENAME_LEN - 1, "%s%s%s%sRSMAFS.t", tfsGetPrimaryPath(pSma->pVnode->pTfs), TD_DIRSEP,
           VNODE_RSMA_DIR, TD_DIRSEP);
  snprintf(fname, TSDB_FILENAME_LEN - 1, "%s%s%s%sRSMAFS", tfsGetPrimaryPath(pSma->pVnode->pTfs), TD_DIRSEP,
           VNODE_RSMA_DIR, TD_DIRSEP);

  // gnrt CURRENT.t
  code = tdRSmaGnrtFS(pSma, pFSNew, tfname);
  if (code) goto _err;

  // rename
  code = taosRenameFile(tfname, fname);
  if (code) {
    code = TAOS_SYSTEM_ERROR(code);
    goto _err;
  }

  return code;

_err:
  smaError("vgId:%d, rsma fs commit phase 1 failed since %s", SMA_VID(pSma), tstrerror(code));
  return code;
}

int32_t tdRSmaFSMergeQTaskFSet(SQTaskFileSet *pSetOld, SQTaskFileSet *pSetNew) {
  SQTaskFileSet *

}

int32_t tdRSmaFSCommit2(SSma *pSma, SRSmaFS *pFSNew) {
  SSmaEnv   *pEnv = SMA_RSMA_ENV(pSma);
  SRSmaStat *pStat = NULL;
  SRSmaInfo *pRSmaInfo = NULL;
  SRSmaFS   *pRSmaFS = NULL;
  int32_t    code = 0;
  int32_t    nRef;
  char       fname[TSDB_FILENAME_LEN];

  if (!pEnv) {
    terrno = TSDB_CODE_RSMA_INVALID_ENV;
    return TSDB_CODE_FAILED;
  }

  pStat = (SRSmaStat *)SMA_ENV_STAT(pEnv);
  if (!pStat || !(pRSmaFS = RSMA_FS(pStat))) {
    terrno = TSDB_CODE_RSMA_INVALID_STAT;
    return TSDB_CODE_FAILED;
  }

  // data
  int32_t iOld = 0;
  int32_t iNew = 0;
  while (true) {
    int32_t       nOld = taosArrayGetSize(pRSmaFS->aQTaskFSet);
    int32_t       nNew = taosArrayGetSize(pFSNew->aQTaskFSet);
    SQTaskFileSet fSet;

    if (iOld >= nOld && iNew >= nNew) break;

    SQTaskFileSet *pSetOld = (iOld < nOld) ? taosArrayGet(pRSmaFS->aQTaskFSet, iOld) : NULL;
    SQTaskFileSet *pSetNew = (iNew < nNew) ? taosArrayGet(pFSNew->aQTaskFSet, iNew) : NULL;

    if (pSetOld && pSetNew) {
      if (pSetOld->suid == pSetNew->suid) {
        goto _merge_old_and_new;
      } else if (pSetOld->suid < pSetNew->suid) {
        goto _remove_old;
      } else {
        goto _add_new;
      }
    } else if (pSetOld) {
      goto _remove_old;
    } else {
      goto _add_new;
    }

  _merge_old_and_new:



    // head
    fSet.pHeadF = pSetOld->pHeadF;
    if ((!sameDisk) || (pSetOld->pHeadF->commitID != pSetNew->pHeadF->commitID)) {
      pSetOld->pHeadF = (SHeadFile *)taosMemoryMalloc(sizeof(SHeadFile));
      if (pSetOld->pHeadF == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _err;
      }
      *pSetOld->pHeadF = *pSetNew->pHeadF;
      pSetOld->pHeadF->nRef = 1;

      nRef = atomic_sub_fetch_32(&fSet.pHeadF->nRef, 1);
      if (nRef == 0) {
        tsdbHeadFileName(pTsdb, pSetOld->diskId, pSetOld->fid, fSet.pHeadF, fname);
        taosRemoveFile(fname);
        taosMemoryFree(fSet.pHeadF);
      }
    } else {
      ASSERT(fSet.pHeadF->size == pSetNew->pHeadF->size);
      ASSERT(fSet.pHeadF->offset == pSetNew->pHeadF->offset);
    }

    // data
    fSet.pDataF = pSetOld->pDataF;
    if ((!sameDisk) || (pSetOld->pDataF->commitID != pSetNew->pDataF->commitID)) {
      pSetOld->pDataF = (SDataFile *)taosMemoryMalloc(sizeof(SDataFile));
      if (pSetOld->pDataF == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _err;
      }
      *pSetOld->pDataF = *pSetNew->pDataF;
      pSetOld->pDataF->nRef = 1;

      nRef = atomic_sub_fetch_32(&fSet.pDataF->nRef, 1);
      if (nRef == 0) {
        tsdbDataFileName(pTsdb, pSetOld->diskId, pSetOld->fid, fSet.pDataF, fname);
        taosRemoveFile(fname);
        taosMemoryFree(fSet.pDataF);
      }
    } else {
      ASSERT(pSetOld->pDataF->size <= pSetNew->pDataF->size);
      pSetOld->pDataF->size = pSetNew->pDataF->size;
    }

    // sma
    fSet.pSmaF = pSetOld->pSmaF;
    if ((!sameDisk) || (pSetOld->pSmaF->commitID != pSetNew->pSmaF->commitID)) {
      pSetOld->pSmaF = (SSmaFile *)taosMemoryMalloc(sizeof(SSmaFile));
      if (pSetOld->pSmaF == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _err;
      }
      *pSetOld->pSmaF = *pSetNew->pSmaF;
      pSetOld->pSmaF->nRef = 1;

      nRef = atomic_sub_fetch_32(&fSet.pSmaF->nRef, 1);
      if (nRef == 0) {
        tsdbSmaFileName(pTsdb, pSetOld->diskId, pSetOld->fid, fSet.pSmaF, fname);
        taosRemoveFile(fname);
        taosMemoryFree(fSet.pSmaF);
      }
    } else {
      ASSERT(pSetOld->pSmaF->size <= pSetNew->pSmaF->size);
      pSetOld->pSmaF->size = pSetNew->pSmaF->size;
    }

    // stt
    if (sameDisk) {
      if (pSetNew->nSttF > pSetOld->nSttF) {
        ASSERT(pSetNew->nSttF = pSetOld->nSttF + 1);
        pSetOld->aSttF[pSetOld->nSttF] = (SSttFile *)taosMemoryMalloc(sizeof(SSttFile));
        if (pSetOld->aSttF[pSetOld->nSttF] == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          goto _err;
        }
        *pSetOld->aSttF[pSetOld->nSttF] = *pSetNew->aSttF[pSetOld->nSttF];
        pSetOld->aSttF[pSetOld->nSttF]->nRef = 1;
        pSetOld->nSttF++;
      } else if (pSetNew->nSttF < pSetOld->nSttF) {
        ASSERT(pSetNew->nSttF == 1);
        for (int32_t iStt = 0; iStt < pSetOld->nSttF; iStt++) {
          SSttFile *pSttFile = pSetOld->aSttF[iStt];
          nRef = atomic_sub_fetch_32(&pSttFile->nRef, 1);
          if (nRef == 0) {
            tsdbSttFileName(pTsdb, pSetOld->diskId, pSetOld->fid, pSttFile, fname);
            taosRemoveFile(fname);
            taosMemoryFree(pSttFile);
          }
          pSetOld->aSttF[iStt] = NULL;
        }

        pSetOld->nSttF = 1;
        pSetOld->aSttF[0] = (SSttFile *)taosMemoryMalloc(sizeof(SSttFile));
        if (pSetOld->aSttF[0] == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          goto _err;
        }
        *pSetOld->aSttF[0] = *pSetNew->aSttF[0];
        pSetOld->aSttF[0]->nRef = 1;
      } else {
        for (int32_t iStt = 0; iStt < pSetOld->nSttF; iStt++) {
          if (pSetOld->aSttF[iStt]->commitID != pSetNew->aSttF[iStt]->commitID) {
            SSttFile *pSttFile = pSetOld->aSttF[iStt];
            nRef = atomic_sub_fetch_32(&pSttFile->nRef, 1);
            if (nRef == 0) {
              tsdbSttFileName(pTsdb, pSetOld->diskId, pSetOld->fid, pSttFile, fname);
              taosRemoveFile(fname);
              taosMemoryFree(pSttFile);
            }

            pSetOld->aSttF[iStt] = (SSttFile *)taosMemoryMalloc(sizeof(SSttFile));
            if (pSetOld->aSttF[iStt] == NULL) {
              code = TSDB_CODE_OUT_OF_MEMORY;
              goto _err;
            }
            *pSetOld->aSttF[iStt] = *pSetNew->aSttF[iStt];
            pSetOld->aSttF[iStt]->nRef = 1;
          } else {
            ASSERT(pSetOld->aSttF[iStt]->size == pSetOld->aSttF[iStt]->size);
            ASSERT(pSetOld->aSttF[iStt]->offset == pSetOld->aSttF[iStt]->offset);
          }
        }
      }
    } else {
      ASSERT(pSetOld->nSttF == pSetNew->nSttF);
      for (int32_t iStt = 0; iStt < pSetOld->nSttF; iStt++) {
        SSttFile *pSttFile = pSetOld->aSttF[iStt];
        nRef = atomic_sub_fetch_32(&pSttFile->nRef, 1);
        if (nRef == 0) {
          tsdbSttFileName(pTsdb, pSetOld->diskId, pSetOld->fid, pSttFile, fname);
          taosRemoveFile(fname);
          taosMemoryFree(pSttFile);
        }

        pSetOld->aSttF[iStt] = (SSttFile *)taosMemoryMalloc(sizeof(SSttFile));
        if (pSetOld->aSttF[iStt] == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          goto _err;
        }
        *pSetOld->aSttF[iStt] = *pSetNew->aSttF[iStt];
        pSetOld->aSttF[iStt]->nRef = 1;
      }
    }

    if (!sameDisk) {
      pSetOld->diskId = pSetNew->diskId;
    }

    iOld++;
    iNew++;
    continue;

  _remove_old:
    nRef = atomic_sub_fetch_32(&pSetOld->pHeadF->nRef, 1);
    if (nRef == 0) {
      tsdbHeadFileName(pTsdb, pSetOld->diskId, pSetOld->fid, pSetOld->pHeadF, fname);
      taosRemoveFile(fname);
      taosMemoryFree(pSetOld->pHeadF);
    }

    nRef = atomic_sub_fetch_32(&pSetOld->pDataF->nRef, 1);
    if (nRef == 0) {
      tsdbDataFileName(pTsdb, pSetOld->diskId, pSetOld->fid, pSetOld->pDataF, fname);
      taosRemoveFile(fname);
      taosMemoryFree(pSetOld->pDataF);
    }

    nRef = atomic_sub_fetch_32(&pSetOld->pSmaF->nRef, 1);
    if (nRef == 0) {
      tsdbSmaFileName(pTsdb, pSetOld->diskId, pSetOld->fid, pSetOld->pSmaF, fname);
      taosRemoveFile(fname);
      taosMemoryFree(pSetOld->pSmaF);
    }

    for (int8_t iStt = 0; iStt < pSetOld->nSttF; iStt++) {
      nRef = atomic_sub_fetch_32(&pSetOld->aSttF[iStt]->nRef, 1);
      if (nRef == 0) {
        tsdbSttFileName(pTsdb, pSetOld->diskId, pSetOld->fid, pSetOld->aSttF[iStt], fname);
        taosRemoveFile(fname);
        taosMemoryFree(pSetOld->aSttF[iStt]);
      }
    }

    taosArrayRemove(pTsdb->fs.aDFileSet, iOld);
    continue;

  _add_new:
    fSet = (SDFileSet){.diskId = pSetNew->diskId, .fid = pSetNew->fid, .nSttF = 1};

    // head
    fSet.pHeadF = (SHeadFile *)taosMemoryMalloc(sizeof(SHeadFile));
    if (fSet.pHeadF == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
    *fSet.pHeadF = *pSetNew->pHeadF;
    fSet.pHeadF->nRef = 1;

    // data
    fSet.pDataF = (SDataFile *)taosMemoryMalloc(sizeof(SDataFile));
    if (fSet.pDataF == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
    *fSet.pDataF = *pSetNew->pDataF;
    fSet.pDataF->nRef = 1;

    // sma
    fSet.pSmaF = (SSmaFile *)taosMemoryMalloc(sizeof(SSmaFile));
    if (fSet.pSmaF == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
    *fSet.pSmaF = *pSetNew->pSmaF;
    fSet.pSmaF->nRef = 1;

    // stt
    ASSERT(pSetNew->nSttF == 1);
    fSet.aSttF[0] = (SSttFile *)taosMemoryMalloc(sizeof(SSttFile));
    if (fSet.aSttF[0] == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
    *fSet.aSttF[0] = *pSetNew->aSttF[0];
    fSet.aSttF[0]->nRef = 1;

    if (taosArrayInsert(pTsdb->fs.aDFileSet, iOld, &fSet) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
    iOld++;
    iNew++;
    continue;
  }

  return code;

_err:
  tsdbError("vgId:%d, tsdb fs commit phase 2 failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

/**
 * @brief Open RSma FS from qTaskInfo files
 *
 * @param pSma
 * @param version
 * @return int32_t
 */
int32_t tdRSmaFSOpen(SSma *pSma, int64_t version) {
  SVnode    *pVnode = pSma->pVnode;
  int64_t    commitID = pVnode->state.commitID;
  SSmaEnv   *pEnv = SMA_RSMA_ENV(pSma);
  SRSmaStat *pStat = NULL;
  SArray    *output = NULL;

  terrno = TSDB_CODE_SUCCESS;

  if (!pEnv) {
    return TSDB_CODE_SUCCESS;
  }

  if (tdFetchQTaskInfoFiles(pSma, version, &output) < 0) {
    goto _end;
  }

  pStat = (SRSmaStat *)SMA_ENV_STAT(pEnv);

  for (int32_t i = 0; i < taosArrayGetSize(output); ++i) {
    int32_t vid = 0;
    int64_t version = -1;
    sscanf((const char *)taosArrayGetP(output, i), "v%dqinf.v%" PRIi64, &vid, &version);
    SQTaskFile qTaskFile = {.version = version, .nRef = 1};
    if ((terrno = tdRSmaFSUpsertQTaskFile(RSMA_FS(pStat), &qTaskFile)) < 0) {
      goto _end;
    }
    smaInfo("vgId:%d, open fs, version:%" PRIi64 ", ref:%d", TD_VID(pVnode), qTaskFile.version, qTaskFile.nRef);
  }

_end:
  for (int32_t i = 0; i < taosArrayGetSize(output); ++i) {
    void *ptr = taosArrayGetP(output, i);
    taosMemoryFreeClear(ptr);
  }
  taosArrayDestroy(output);

  if (terrno != TSDB_CODE_SUCCESS) {
    smaError("vgId:%d, open rsma fs failed since %s", TD_VID(pVnode), terrstr());
    return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

void tdRSmaFSClose(SRSmaFS *fs) { taosArrayDestroy(fs->aQTaskFSet); }

static int32_t tdQTaskInfCmprFn1(const void *p1, const void *p2) {
  if (*(int64_t *)p1 < ((SQTaskFile *)p2)->version) {
    return -1;
  } else if (*(int64_t *)p1 > ((SQTaskFile *)p2)->version) {
    return 1;
  }
  return 0;
}

int32_t tdRSmaFSRef(SSma *pSma, SRSmaStat *pStat, int64_t version) {
  SArray     *aQTaskFSet = RSMA_FS(pStat)->aQTaskFSet;
  SQTaskFile *pTaskF = NULL;
  int32_t     oldVal = 0;

  taosRLockLatch(RSMA_FS_LOCK(pStat));
  if ((pTaskF = taosArraySearch(aQTaskFSet, &version, tdQTaskInfCmprFn1, TD_EQ))) {
    oldVal = atomic_fetch_add_32(&pTaskF->nRef, 1);
    ASSERT(oldVal > 0);
  }
  taosRUnLockLatch(RSMA_FS_LOCK(pStat));
  return oldVal;
}

int64_t tdRSmaFSMaxVer(SSma *pSma, SRSmaStat *pStat) {
  SArray *aQTaskFSet = RSMA_FS(pStat)->aQTaskFSet;
  int64_t version = -1;

  taosRLockLatch(RSMA_FS_LOCK(pStat));
  if (taosArrayGetSize(aQTaskFSet) > 0) {
    version = ((SQTaskFile *)taosArrayGetLast(aQTaskFSet))->version;
  }
  taosRUnLockLatch(RSMA_FS_LOCK(pStat));
  return version;
}

void tdRSmaFSUnRef(SSma *pSma, SRSmaStat *pStat, int64_t version) {
  SVnode     *pVnode = pSma->pVnode;
  SArray     *aQTaskFSet = RSMA_FS(pStat)->aQTaskFSet;
  char        qTaskFullName[TSDB_FILENAME_LEN];
  SQTaskFile *pTaskF = NULL;
  int32_t     idx = -1;

  taosWLockLatch(RSMA_FS_LOCK(pStat));
  if ((idx = taosArraySearchIdx(aQTaskFSet, &version, tdQTaskInfCmprFn1, TD_EQ)) >= 0) {
    ASSERT(idx < taosArrayGetSize(aQTaskFSet));
    pTaskF = taosArrayGet(aQTaskFSet, idx);
    if (atomic_sub_fetch_32(&pTaskF->nRef, 1) <= 0) {
      tdRSmaQTaskInfoGetFullName(TD_VID(pVnode), pTaskF->version, tfsGetPrimaryPath(pVnode->pTfs), qTaskFullName);
      if (taosRemoveFile(qTaskFullName) < 0) {
        smaWarn("vgId:%d, failed to remove %s since %s", TD_VID(pVnode), qTaskFullName,
                tstrerror(TAOS_SYSTEM_ERROR(errno)));
      } else {
        smaDebug("vgId:%d, success to remove %s", TD_VID(pVnode), qTaskFullName);
      }
      taosArrayRemove(aQTaskFSet, idx);
    }
  }
  taosWUnLockLatch(RSMA_FS_LOCK(pStat));
}

/**
 * @brief Fetch qtaskfiles LE than version
 *
 * @param pSma
 * @param version
 * @param output
 * @return int32_t
 */
static int32_t tdFetchQTaskInfoFiles(SSma *pSma, int64_t version, SArray **output) {
  SVnode       *pVnode = pSma->pVnode;
  TdDirPtr      pDir = NULL;
  TdDirEntryPtr pDirEntry = NULL;
  char          dir[TSDB_FILENAME_LEN];
  const char   *pattern = "v[0-9]+qinf\\.v([0-9]+)?$";
  regex_t       regex;
  int           code = 0;

  terrno = TSDB_CODE_SUCCESS;

  tdGetVndDirName(TD_VID(pVnode), tfsGetPrimaryPath(pVnode->pTfs), VNODE_RSMA_DIR, true, dir);

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

static int32_t tdQTaskFileCmprFn2(const void *p1, const void *p2) {
  if (((SQTaskFile *)p1)->version < ((SQTaskFile *)p2)->version) {
    return -1;
  } else if (((SQTaskFile *)p1)->version > ((SQTaskFile *)p2)->version) {
    return 1;
  }

  return 0;
}

int32_t tdRSmaFSUpsertQTaskFile(SRSmaFS *pFS, SQTaskFile *qTaskFile) {
  int32_t code = 0;
  int32_t idx = taosArraySearchIdx(pFS->aQTaskFSet, qTaskFile, tdQTaskFileCmprFn2, TD_GE);

  if (idx < 0) {
    idx = taosArrayGetSize(pFS->aQTaskFSet);
  } else {
    SQTaskFile *pTaskF = (SQTaskFile *)taosArrayGet(pFS->aQTaskFSet, idx);
    int32_t     c = tdQTaskFileCmprFn2(pTaskF, qTaskFile);
    if (c == 0) {
      pTaskF->nRef = qTaskFile->nRef;
      pTaskF->level = qTaskFile->level;
      pTaskF->version = qTaskFile->version;
      pTaskF->mtime = qTaskFile->mtime;
      goto _exit;
    }
  }

  if (taosArrayInsert(pFS->aQTaskFSet, idx, qTaskFile) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

_exit:
  return code;
}