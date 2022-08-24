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

#include "tsdb.h"

// =================================================================================================
static int32_t tsdbEncodeFS(uint8_t *p, STsdbFS *pFS) {
  int32_t  n = 0;
  int8_t   hasDel = pFS->pDelFile ? 1 : 0;
  uint32_t nSet = taosArrayGetSize(pFS->aDFileSet);

  // SDelFile
  n += tPutI8(p ? p + n : p, hasDel);
  if (hasDel) {
    n += tPutDelFile(p ? p + n : p, pFS->pDelFile);
  }

  // SArray<SDFileSet>
  n += tPutU32v(p ? p + n : p, nSet);
  for (uint32_t iSet = 0; iSet < nSet; iSet++) {
    n += tPutDFileSet(p ? p + n : p, (SDFileSet *)taosArrayGet(pFS->aDFileSet, iSet));
  }

  return n;
}

static int32_t tsdbGnrtCurrent(STsdb *pTsdb, STsdbFS *pFS, char *fname) {
  int32_t   code = 0;
  int64_t   n;
  int64_t   size;
  uint8_t  *pData = NULL;
  TdFilePtr pFD = NULL;

  // to binary
  size = tsdbEncodeFS(NULL, pFS) + sizeof(TSCKSUM);
  pData = taosMemoryMalloc(size);
  if (pData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  n = tsdbEncodeFS(pData, pFS);
  ASSERT(n + sizeof(TSCKSUM) == size);
  taosCalcChecksumAppend(0, pData, size);

  // create and write
  pFD = taosOpenFile(fname, TD_FILE_WRITE | TD_FILE_CREATE);
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
  tsdbError("vgId:%d, tsdb gnrt current failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  if (pData) taosMemoryFree(pData);
  return code;
}

// static int32_t tsdbApplyDFileSetChange(STsdbFS *pFS, SDFileSet *pFrom, SDFileSet *pTo) {
//   int32_t code = 0;
//   char    fname[TSDB_FILENAME_LEN];

//   if (pFrom && pTo) {
//     bool isSameDisk = (pFrom->diskId.level == pTo->diskId.level) && (pFrom->diskId.id == pTo->diskId.id);

//     // head
//     if (isSameDisk && pFrom->pHeadF->commitID == pTo->pHeadF->commitID) {
//       ASSERT(pFrom->pHeadF->size == pTo->pHeadF->size);
//       ASSERT(pFrom->pHeadF->offset == pTo->pHeadF->offset);
//     } else {
//       tsdbHeadFileName(pFS->pTsdb, pFrom->diskId, pFrom->fid, pFrom->pHeadF, fname);
//       taosRemoveFile(fname);
//     }

//     // data
//     if (isSameDisk && pFrom->pDataF->commitID == pTo->pDataF->commitID) {
//       if (pFrom->pDataF->size > pTo->pDataF->size) {
//         code = tsdbDFileRollback(pFS->pTsdb, pTo, TSDB_DATA_FILE);
//         if (code) goto _err;
//       }
//     } else {
//       tsdbDataFileName(pFS->pTsdb, pFrom->diskId, pFrom->fid, pFrom->pDataF, fname);
//       taosRemoveFile(fname);
//     }

//     // last
//     if (isSameDisk && pFrom->pLastF->commitID == pTo->pLastF->commitID) {
//       if (pFrom->pLastF->size > pTo->pLastF->size) {
//         code = tsdbDFileRollback(pFS->pTsdb, pTo, TSDB_LAST_FILE);
//         if (code) goto _err;
//       }
//     } else {
//       tsdbLastFileName(pFS->pTsdb, pFrom->diskId, pFrom->fid, pFrom->pLastF, fname);
//       taosRemoveFile(fname);
//     }

//     // sma
//     if (isSameDisk && pFrom->pSmaF->commitID == pTo->pSmaF->commitID) {
//       if (pFrom->pSmaF->size > pTo->pSmaF->size) {
//         code = tsdbDFileRollback(pFS->pTsdb, pTo, TSDB_SMA_FILE);
//         if (code) goto _err;
//       }
//     } else {
//       tsdbSmaFileName(pFS->pTsdb, pFrom->diskId, pFrom->fid, pFrom->pSmaF, fname);
//       taosRemoveFile(fname);
//     }
//   } else if (pFrom) {
//     // head
//     tsdbHeadFileName(pFS->pTsdb, pFrom->diskId, pFrom->fid, pFrom->pHeadF, fname);
//     taosRemoveFile(fname);

//     // data
//     tsdbDataFileName(pFS->pTsdb, pFrom->diskId, pFrom->fid, pFrom->pDataF, fname);
//     taosRemoveFile(fname);

//     // last
//     tsdbLastFileName(pFS->pTsdb, pFrom->diskId, pFrom->fid, pFrom->pLastF, fname);
//     taosRemoveFile(fname);

//     // fsm
//     tsdbSmaFileName(pFS->pTsdb, pFrom->diskId, pFrom->fid, pFrom->pSmaF, fname);
//     taosRemoveFile(fname);
//   }

//   return code;

// _err:
//   tsdbError("vgId:%d, tsdb apply disk file set change failed since %s", TD_VID(pFS->pTsdb->pVnode), tstrerror(code));
//   return code;
// }

// static int32_t tsdbApplyDelFileChange(STsdbFS *pFS, SDelFile *pFrom, SDelFile *pTo) {
//   int32_t code = 0;
//   char    fname[TSDB_FILENAME_LEN];

//   if (pFrom && pTo) {
//     if (!tsdbDelFileIsSame(pFrom, pTo)) {
//       tsdbDelFileName(pFS->pTsdb, pFrom, fname);
//       if (taosRemoveFile(fname) < 0) {
//         code = TAOS_SYSTEM_ERROR(errno);
//         goto _err;
//       }
//     }
//   } else if (pFrom) {
//     tsdbDelFileName(pFS->pTsdb, pFrom, fname);
//     if (taosRemoveFile(fname) < 0) {
//       code = TAOS_SYSTEM_ERROR(errno);
//       goto _err;
//     }
//   } else {
//     // do nothing
//   }

//   return code;

// _err:
//   tsdbError("vgId:%d, tsdb apply del file change failed since %s", TD_VID(pFS->pTsdb->pVnode), tstrerror(code));
//   return code;
// }

// static int32_t tsdbFSApplyDiskChange(STsdbFS *pFS, STsdbFSState *pFrom, STsdbFSState *pTo) {
//   int32_t    code = 0;
//   int32_t    iFrom = 0;
//   int32_t    nFrom = taosArrayGetSize(pFrom->aDFileSet);
//   int32_t    iTo = 0;
//   int32_t    nTo = taosArrayGetSize(pTo->aDFileSet);
//   SDFileSet *pDFileSetFrom;
//   SDFileSet *pDFileSetTo;

//   // SDelFile
//   code = tsdbApplyDelFileChange(pFS, pFrom->pDelFile, pTo->pDelFile);
//   if (code) goto _err;

//   // SDFileSet
//   while (iFrom < nFrom && iTo < nTo) {
//     pDFileSetFrom = (SDFileSet *)taosArrayGet(pFrom->aDFileSet, iFrom);
//     pDFileSetTo = (SDFileSet *)taosArrayGet(pTo->aDFileSet, iTo);

//     if (pDFileSetFrom->fid == pDFileSetTo->fid) {
//       code = tsdbApplyDFileSetChange(pFS, pDFileSetFrom, pDFileSetTo);
//       if (code) goto _err;

//       iFrom++;
//       iTo++;
//     } else if (pDFileSetFrom->fid < pDFileSetTo->fid) {
//       code = tsdbApplyDFileSetChange(pFS, pDFileSetFrom, NULL);
//       if (code) goto _err;

//       iFrom++;
//     } else {
//       iTo++;
//     }
//   }

//   while (iFrom < nFrom) {
//     pDFileSetFrom = (SDFileSet *)taosArrayGet(pFrom->aDFileSet, iFrom);
//     code = tsdbApplyDFileSetChange(pFS, pDFileSetFrom, NULL);
//     if (code) goto _err;

//     iFrom++;
//   }

// #if 0
//   // do noting
//   while (iTo < nTo) {
//     pDFileSetTo = (SDFileSet *)taosArrayGetP(pTo->aDFileSet, iTo);
//     code = tsdbApplyDFileSetChange(pFS, NULL, pDFileSetTo);
//     if (code) goto _err;

//     iTo++;
//   }
// #endif

//   return code;

// _err:
//   tsdbError("vgId:%d, tsdb fs apply disk change failed sicne %s", TD_VID(pFS->pTsdb->pVnode), tstrerror(code));
//   return code;
// }

void tsdbFSDestroy(STsdbFS *pFS) {
  if (pFS->pDelFile) {
    taosMemoryFree(pFS->pDelFile);
  }

  for (int32_t iSet = 0; iSet < taosArrayGetSize(pFS->aDFileSet); iSet++) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(pFS->aDFileSet, iSet);
    taosMemoryFree(pSet->pHeadF);
    taosMemoryFree(pSet->pDataF);
    taosMemoryFree(pSet->pLastF);
    taosMemoryFree(pSet->pSmaF);
  }

  taosArrayDestroy(pFS->aDFileSet);
}

static int32_t tsdbScanAndTryFixFS(STsdb *pTsdb) {
  int32_t code = 0;
  int64_t size;
  char    fname[TSDB_FILENAME_LEN];

  // SDelFile
  if (pTsdb->fs.pDelFile) {
    tsdbDelFileName(pTsdb, pTsdb->fs.pDelFile, fname);
    if (taosStatFile(fname, &size, NULL)) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    if (size != pTsdb->fs.pDelFile->size) {
      code = TSDB_CODE_FILE_CORRUPTED;
      goto _err;
    }
  }

  // SArray<SDFileSet>
  for (int32_t iSet = 0; iSet < taosArrayGetSize(pTsdb->fs.aDFileSet); iSet++) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(pTsdb->fs.aDFileSet, iSet);

    // head =========
    tsdbHeadFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pHeadF, fname);
    if (taosStatFile(fname, &size, NULL)) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }
    if (size != pSet->pHeadF->size) {
      code = TSDB_CODE_FILE_CORRUPTED;
      goto _err;
    }

    // data =========
    tsdbDataFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pDataF, fname);
    if (taosStatFile(fname, &size, NULL)) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }
    if (size < pSet->pDataF->size) {
      code = TSDB_CODE_FILE_CORRUPTED;
      goto _err;
    } else if (size > pSet->pDataF->size) {
      code = tsdbDFileRollback(pTsdb, pSet, TSDB_DATA_FILE);
      if (code) goto _err;
    }

    // last ===========
    tsdbLastFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pLastF, fname);
    if (taosStatFile(fname, &size, NULL)) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }
    if (size != pSet->pLastF->size) {
      code = TSDB_CODE_FILE_CORRUPTED;
      goto _err;
    }

    // sma =============
    tsdbSmaFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pSmaF, fname);
    if (taosStatFile(fname, &size, NULL)) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }
    if (size < pSet->pSmaF->size) {
      code = TSDB_CODE_FILE_CORRUPTED;
      goto _err;
    } else if (size > pSet->pSmaF->size) {
      code = tsdbDFileRollback(pTsdb, pSet, TSDB_SMA_FILE);
      if (code) goto _err;
    }
  }

  {
    // remove those invalid files (todo)
  }

  return code;

_err:
  tsdbError("vgId:%d, tsdb scan and try fix fs failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tDFileSetCmprFn(const void *p1, const void *p2) {
  if (((SDFileSet *)p1)->fid < ((SDFileSet *)p2)->fid) {
    return -1;
  } else if (((SDFileSet *)p1)->fid > ((SDFileSet *)p2)->fid) {
    return 1;
  }

  return 0;
}

static int32_t tsdbRecoverFS(STsdb *pTsdb, uint8_t *pData, int64_t nData) {
  int32_t  code = 0;
  int8_t   hasDel;
  uint32_t nSet;
  int32_t  n;

  // SDelFile
  n = 0;
  n += tGetI8(pData + n, &hasDel);
  if (hasDel) {
    pTsdb->fs.pDelFile = (SDelFile *)taosMemoryMalloc(sizeof(SDelFile));
    if (pTsdb->fs.pDelFile == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }

    pTsdb->fs.pDelFile->nRef = 1;
    n += tGetDelFile(pData + n, pTsdb->fs.pDelFile);
  } else {
    pTsdb->fs.pDelFile = NULL;
  }

  // SArray<SDFileSet>
  taosArrayClear(pTsdb->fs.aDFileSet);
  n += tGetU32v(pData + n, &nSet);
  for (uint32_t iSet = 0; iSet < nSet; iSet++) {
    SDFileSet fSet;

    // head
    fSet.pHeadF = (SHeadFile *)taosMemoryCalloc(1, sizeof(SHeadFile));
    if (fSet.pHeadF == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
    fSet.pHeadF->nRef = 1;

    // data
    fSet.pDataF = (SDataFile *)taosMemoryCalloc(1, sizeof(SDataFile));
    if (fSet.pDataF == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
    fSet.pDataF->nRef = 1;

    // last
    fSet.pLastF = (SLastFile *)taosMemoryCalloc(1, sizeof(SLastFile));
    if (fSet.pLastF == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
    fSet.pLastF->nRef = 1;

    // sma
    fSet.pSmaF = (SSmaFile *)taosMemoryCalloc(1, sizeof(SSmaFile));
    if (fSet.pSmaF == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
    fSet.pSmaF->nRef = 1;

    n += tGetDFileSet(pData + n, &fSet);

    if (taosArrayPush(pTsdb->fs.aDFileSet, &fSet) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }

  ASSERT(n + sizeof(TSCKSUM) == nData);
  return code;

_err:
  return code;
}

// EXPOSED APIS ====================================================================================
int32_t tsdbFSOpen(STsdb *pTsdb) {
  int32_t code = 0;

  // open handle
  pTsdb->fs.pDelFile = NULL;
  pTsdb->fs.aDFileSet = taosArrayInit(0, sizeof(SDFileSet));
  if (pTsdb->fs.aDFileSet == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  // load fs or keep empty
  char fname[TSDB_FILENAME_LEN];

  snprintf(fname, TSDB_FILENAME_LEN - 1, "%s%s%s%sCURRENT", tfsGetPrimaryPath(pTsdb->pVnode->pTfs), TD_DIRSEP,
           pTsdb->path, TD_DIRSEP);

  if (!taosCheckExistFile(fname)) {
    // empty one
    code = tsdbGnrtCurrent(pTsdb, &pTsdb->fs, fname);
    if (code) goto _err;
  } else {
    // read
    TdFilePtr pFD = taosOpenFile(fname, TD_FILE_READ);
    if (pFD == NULL) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    int64_t size;
    if (taosFStatFile(pFD, &size, NULL) < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      taosCloseFile(&pFD);
      goto _err;
    }

    uint8_t *pData = taosMemoryMalloc(size);
    if (pData == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      taosCloseFile(&pFD);
      goto _err;
    }

    int64_t n = taosReadFile(pFD, pData, size);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      taosMemoryFree(pData);
      taosCloseFile(&pFD);
      goto _err;
    }

    if (!taosCheckChecksumWhole(pData, size)) {
      code = TSDB_CODE_FILE_CORRUPTED;
      taosMemoryFree(pData);
      taosCloseFile(&pFD);
      goto _err;
    }

    taosCloseFile(&pFD);

    // recover fs
    code = tsdbRecoverFS(pTsdb, pData, size);
    if (code) {
      taosMemoryFree(pData);
      goto _err;
    }

    taosMemoryFree(pData);
  }

  // scan and fix FS
  code = tsdbScanAndTryFixFS(pTsdb);
  if (code) goto _err;

  return code;

_err:
  tsdbError("vgId:%d, tsdb fs open failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbFSClose(STsdb *pTsdb) {
  int32_t code = 0;

  if (pTsdb->fs.pDelFile) {
    ASSERT(pTsdb->fs.pDelFile->nRef == 1);
    taosMemoryFree(pTsdb->fs.pDelFile);
  }

  for (int32_t iSet = 0; iSet < taosArrayGetSize(pTsdb->fs.aDFileSet); iSet++) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(pTsdb->fs.aDFileSet, iSet);

    // head
    ASSERT(pSet->pHeadF->nRef == 1);
    taosMemoryFree(pSet->pHeadF);

    // data
    ASSERT(pSet->pDataF->nRef == 1);
    taosMemoryFree(pSet->pDataF);

    // last
    ASSERT(pSet->pLastF->nRef == 1);
    taosMemoryFree(pSet->pLastF);

    // sma
    ASSERT(pSet->pSmaF->nRef == 1);
    taosMemoryFree(pSet->pSmaF);
  }

  taosArrayDestroy(pTsdb->fs.aDFileSet);

  return code;
}

int32_t tsdbFSCopy(STsdb *pTsdb, STsdbFS *pFS) {
  int32_t code = 0;

  pFS->pDelFile = NULL;
  pFS->aDFileSet = taosArrayInit(taosArrayGetSize(pTsdb->fs.aDFileSet), sizeof(SDFileSet));
  if (pFS->aDFileSet == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  if (pTsdb->fs.pDelFile) {
    pFS->pDelFile = (SDelFile *)taosMemoryMalloc(sizeof(SDelFile));
    if (pFS->pDelFile == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }

    *pFS->pDelFile = *pTsdb->fs.pDelFile;
  }

  for (int32_t iSet = 0; iSet < taosArrayGetSize(pTsdb->fs.aDFileSet); iSet++) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(pTsdb->fs.aDFileSet, iSet);
    SDFileSet  fSet = {.diskId = pSet->diskId, .fid = pSet->fid};

    // head
    fSet.pHeadF = (SHeadFile *)taosMemoryMalloc(sizeof(SHeadFile));
    if (fSet.pHeadF == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    *fSet.pHeadF = *pSet->pHeadF;

    // data
    fSet.pDataF = (SDataFile *)taosMemoryMalloc(sizeof(SDataFile));
    if (fSet.pDataF == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    *fSet.pDataF = *pSet->pDataF;

    // data
    fSet.pLastF = (SLastFile *)taosMemoryMalloc(sizeof(SLastFile));
    if (fSet.pLastF == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    *fSet.pLastF = *pSet->pLastF;

    // last
    fSet.pSmaF = (SSmaFile *)taosMemoryMalloc(sizeof(SSmaFile));
    if (fSet.pSmaF == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    *fSet.pSmaF = *pSet->pSmaF;

    if (taosArrayPush(pFS->aDFileSet, &fSet) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
  }

_exit:
  return code;
}

int32_t tsdbFSRollback(STsdbFS *pFS) {
  int32_t code = 0;

  ASSERT(0);

  return code;

_err:
  return code;
}

int32_t tsdbFSUpsertDelFile(STsdbFS *pFS, SDelFile *pDelFile) {
  int32_t code = 0;

  if (pFS->pDelFile == NULL) {
    pFS->pDelFile = (SDelFile *)taosMemoryMalloc(sizeof(SDelFile));
    if (pFS->pDelFile == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
  }
  *pFS->pDelFile = *pDelFile;

_exit:
  return code;
}

int32_t tsdbFSUpsertFSet(STsdbFS *pFS, SDFileSet *pSet) {
  int32_t code = 0;
  int32_t idx = taosArraySearchIdx(pFS->aDFileSet, pSet, tDFileSetCmprFn, TD_GE);

  if (idx < 0) {
    idx = taosArrayGetSize(pFS->aDFileSet);
  } else {
    SDFileSet *pDFileSet = (SDFileSet *)taosArrayGet(pFS->aDFileSet, idx);
    int32_t    c = tDFileSetCmprFn(pSet, pDFileSet);
    if (c == 0) {
      *pDFileSet->pHeadF = *pSet->pHeadF;
      *pDFileSet->pDataF = *pSet->pDataF;
      *pDFileSet->pLastF = *pSet->pLastF;
      *pDFileSet->pSmaF = *pSet->pSmaF;

      goto _exit;
    }
  }

  SDFileSet fSet = {.diskId = pSet->diskId, .fid = pSet->fid};

  // head
  fSet.pHeadF = (SHeadFile *)taosMemoryMalloc(sizeof(SHeadFile));
  if (fSet.pHeadF == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  *fSet.pHeadF = *pSet->pHeadF;

  // data
  fSet.pDataF = (SDataFile *)taosMemoryMalloc(sizeof(SDataFile));
  if (fSet.pDataF == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  *fSet.pDataF = *pSet->pDataF;

  // data
  fSet.pLastF = (SLastFile *)taosMemoryMalloc(sizeof(SLastFile));
  if (fSet.pLastF == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  *fSet.pLastF = *pSet->pLastF;

  // last
  fSet.pSmaF = (SSmaFile *)taosMemoryMalloc(sizeof(SSmaFile));
  if (fSet.pSmaF == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  *fSet.pSmaF = *pSet->pSmaF;

  if (taosArrayInsert(pFS->aDFileSet, idx, &fSet) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

_exit:
  return code;
}

int32_t tsdbFSCommit1(STsdb *pTsdb, STsdbFS *pFSNew) {
  int32_t code = 0;
  char    tfname[TSDB_FILENAME_LEN];
  char    fname[TSDB_FILENAME_LEN];

  snprintf(tfname, TSDB_FILENAME_LEN - 1, "%s%s%s%sCURRENT.t", tfsGetPrimaryPath(pTsdb->pVnode->pTfs), TD_DIRSEP,
           pTsdb->path, TD_DIRSEP);
  snprintf(fname, TSDB_FILENAME_LEN - 1, "%s%s%s%sCURRENT", tfsGetPrimaryPath(pTsdb->pVnode->pTfs), TD_DIRSEP,
           pTsdb->path, TD_DIRSEP);

  // gnrt CURRENT.t
  code = tsdbGnrtCurrent(pTsdb, pFSNew, tfname);
  if (code) goto _err;

  // rename
  code = taosRenameFile(tfname, fname);
  if (code) {
    code = TAOS_SYSTEM_ERROR(code);
    goto _err;
  }

  return code;

_err:
  tsdbError("vgId:%d, tsdb fs commit phase 1 failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbFSCommit2(STsdb *pTsdb, STsdbFS *pFSNew) {
  int32_t code = 0;
  int32_t nRef;
  char    fname[TSDB_FILENAME_LEN];

  // del
  if (pFSNew->pDelFile) {
    SDelFile *pDelFile = pTsdb->fs.pDelFile;

    if (pDelFile == NULL || (pDelFile->commitID != pFSNew->pDelFile->commitID)) {
      pTsdb->fs.pDelFile = (SDelFile *)taosMemoryMalloc(sizeof(SDelFile));
      if (pTsdb->fs.pDelFile == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _err;
      }

      *pTsdb->fs.pDelFile = *pFSNew->pDelFile;
      pTsdb->fs.pDelFile->nRef = 1;

      if (pDelFile) {
        nRef = atomic_sub_fetch_32(&pDelFile->nRef, 1);
        if (nRef == 0) {
          tsdbDelFileName(pTsdb, pDelFile, fname);
          taosRemoveFile(fname);
          taosMemoryFree(pDelFile);
        }
      }
    }
  } else {
    ASSERT(pTsdb->fs.pDelFile == NULL);
  }

  // data
  int32_t iOld = 0;
  int32_t iNew = 0;
  while (true) {
    int32_t   nOld = taosArrayGetSize(pTsdb->fs.aDFileSet);
    int32_t   nNew = taosArrayGetSize(pFSNew->aDFileSet);
    SDFileSet fSet;
    int8_t    sameDisk;

    if (iOld >= nOld && iNew >= nNew) break;

    SDFileSet *pSetOld = (iOld < nOld) ? taosArrayGet(pTsdb->fs.aDFileSet, iOld) : NULL;
    SDFileSet *pSetNew = (iNew < nNew) ? taosArrayGet(pFSNew->aDFileSet, iNew) : NULL;

    if (pSetOld && pSetNew) {
      if (pSetOld->fid == pSetNew->fid) {
        goto _merge_old_and_new;
      } else if (pSetOld->fid < pSetNew->fid) {
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
    sameDisk = ((pSetOld->diskId.level == pSetNew->diskId.level) && (pSetOld->diskId.id == pSetNew->diskId.id));

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

    // last
    fSet.pLastF = pSetOld->pLastF;
    if ((!sameDisk) || (pSetOld->pLastF->commitID != pSetNew->pLastF->commitID)) {
      pSetOld->pLastF = (SLastFile *)taosMemoryMalloc(sizeof(SLastFile));
      if (pSetOld->pLastF == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _err;
      }
      *pSetOld->pLastF = *pSetNew->pLastF;
      pSetOld->pLastF->nRef = 1;

      nRef = atomic_sub_fetch_32(&fSet.pLastF->nRef, 1);
      if (nRef == 0) {
        tsdbLastFileName(pTsdb, pSetOld->diskId, pSetOld->fid, fSet.pLastF, fname);
        taosRemoveFile(fname);
        taosMemoryFree(fSet.pLastF);
      }
    } else {
      ASSERT(pSetOld->pLastF->size == pSetNew->pLastF->size);
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

    nRef = atomic_sub_fetch_32(&pSetOld->pLastF->nRef, 1);
    if (nRef == 0) {
      tsdbLastFileName(pTsdb, pSetOld->diskId, pSetOld->fid, pSetOld->pLastF, fname);
      taosRemoveFile(fname);
      taosMemoryFree(pSetOld->pLastF);
    }

    nRef = atomic_sub_fetch_32(&pSetOld->pSmaF->nRef, 1);
    if (nRef == 0) {
      tsdbSmaFileName(pTsdb, pSetOld->diskId, pSetOld->fid, pSetOld->pSmaF, fname);
      taosRemoveFile(fname);
      taosMemoryFree(pSetOld->pSmaF);
    }

    taosArrayRemove(pTsdb->fs.aDFileSet, iOld);
    continue;

  _add_new:
    fSet.diskId = pSetNew->diskId;
    fSet.fid = pSetNew->fid;

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

    // last
    fSet.pLastF = (SLastFile *)taosMemoryMalloc(sizeof(SLastFile));
    if (fSet.pLastF == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
    *fSet.pLastF = *pSetNew->pLastF;
    fSet.pLastF->nRef = 1;

    // sma
    fSet.pSmaF = (SSmaFile *)taosMemoryMalloc(sizeof(SSmaFile));
    if (fSet.pSmaF == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
    *fSet.pSmaF = *pSetNew->pSmaF;
    fSet.pSmaF->nRef = 1;

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

int32_t tsdbFSRef(STsdb *pTsdb, STsdbFS *pFS) {
  int32_t code = 0;
  int32_t nRef;

  pFS->aDFileSet = taosArrayInit(taosArrayGetSize(pTsdb->fs.aDFileSet), sizeof(SDFileSet));
  if (pFS->aDFileSet == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  pFS->pDelFile = pTsdb->fs.pDelFile;
  if (pFS->pDelFile) {
    nRef = atomic_fetch_add_32(&pFS->pDelFile->nRef, 1);
    ASSERT(nRef > 0);
  }

  SDFileSet fSet;
  for (int32_t iSet = 0; iSet < taosArrayGetSize(pTsdb->fs.aDFileSet); iSet++) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(pTsdb->fs.aDFileSet, iSet);
    fSet = *pSet;

    nRef = atomic_fetch_add_32(&pSet->pHeadF->nRef, 1);
    ASSERT(nRef > 0);

    nRef = atomic_fetch_add_32(&pSet->pDataF->nRef, 1);
    ASSERT(nRef > 0);

    nRef = atomic_fetch_add_32(&pSet->pLastF->nRef, 1);
    ASSERT(nRef > 0);

    nRef = atomic_fetch_add_32(&pSet->pSmaF->nRef, 1);
    ASSERT(nRef > 0);

    if (taosArrayPush(pFS->aDFileSet, &fSet) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
  }

_exit:
  return code;
}

void tsdbFSUnref(STsdb *pTsdb, STsdbFS *pFS) {
  int32_t nRef;
  char    fname[TSDB_FILENAME_LEN];

  if (pFS->pDelFile) {
    nRef = atomic_sub_fetch_32(&pFS->pDelFile->nRef, 1);
    ASSERT(nRef >= 0);
    if (nRef == 0) {
      tsdbDelFileName(pTsdb, pFS->pDelFile, fname);
      taosRemoveFile(fname);
      taosMemoryFree(pFS->pDelFile);
    }
  }

  for (int32_t iSet = 0; iSet < taosArrayGetSize(pFS->aDFileSet); iSet++) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(pFS->aDFileSet, iSet);

    // head
    nRef = atomic_sub_fetch_32(&pSet->pHeadF->nRef, 1);
    ASSERT(nRef >= 0);
    if (nRef == 0) {
      tsdbHeadFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pHeadF, fname);
      taosRemoveFile(fname);
      taosMemoryFree(pSet->pHeadF);
    }

    // data
    nRef = atomic_sub_fetch_32(&pSet->pDataF->nRef, 1);
    ASSERT(nRef >= 0);
    if (nRef == 0) {
      tsdbDataFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pDataF, fname);
      taosRemoveFile(fname);
      taosMemoryFree(pSet->pDataF);
    }

    // last
    nRef = atomic_sub_fetch_32(&pSet->pLastF->nRef, 1);
    ASSERT(nRef >= 0);
    if (nRef == 0) {
      tsdbLastFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pLastF, fname);
      taosRemoveFile(fname);
      taosMemoryFree(pSet->pLastF);
    }

    // sma
    nRef = atomic_sub_fetch_32(&pSet->pSmaF->nRef, 1);
    ASSERT(nRef >= 0);
    if (nRef == 0) {
      tsdbSmaFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pSmaF, fname);
      taosRemoveFile(fname);
      taosMemoryFree(pSet->pSmaF);
    }
  }

  taosArrayDestroy(pFS->aDFileSet);
}