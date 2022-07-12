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
static int32_t tPutFSState(uint8_t *p, STsdbFSState *pState) {
  int32_t  n = 0;
  int8_t   hasDel = pState->pDelFile ? 1 : 0;
  uint32_t nDFileSet = taosArrayGetSize(pState->aDFileSet);

  // SDelFile
  n += tPutI8(p ? p + n : p, hasDel);
  if (hasDel) {
    n += tPutDelFile(p ? p + n : p, pState->pDelFile);
  }

  // SArray<SDFileSet>
  n += tPutU32v(p ? p + n : p, nDFileSet);
  for (uint32_t iDFileSet = 0; iDFileSet < nDFileSet; iDFileSet++) {
    n += tPutDFileSet(p ? p + n : p, (SDFileSet *)taosArrayGet(pState->aDFileSet, iDFileSet));
  }

  return n;
}

static int32_t tGetFSState(uint8_t *p, STsdbFSState *pState) {
  int32_t    n = 0;
  int8_t     hasDel;
  uint32_t   nDFileSet;
  SDFileSet *pSet = &(SDFileSet){0};

  // SDelFile
  n += tGetI8(p + n, &hasDel);
  if (hasDel) {
    pState->pDelFile = &pState->delFile;
    n += tGetDelFile(p + n, pState->pDelFile);
  } else {
    pState->pDelFile = NULL;
  }

  // SArray<SDFileSet>
  taosArrayClear(pState->aDFileSet);
  n += tGetU32v(p + n, &nDFileSet);
  for (uint32_t iDFileSet = 0; iDFileSet < nDFileSet; iDFileSet++) {
    n += tGetDFileSet(p + n, pSet);
    taosArrayPush(pState->aDFileSet, pSet);
  }

  return n;
}

static int32_t tsdbGnrtCurrent(const char *fname, STsdbFSState *pState) {
  int32_t   code = 0;
  int64_t   n;
  int64_t   size;
  uint8_t  *pData;
  TdFilePtr pFD = NULL;

  // to binary
  size = tPutFSState(NULL, pState) + sizeof(TSCKSUM);
  pData = taosMemoryMalloc(size);
  if (pData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  n = tPutFSState(pData, pState);
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
  tsdbError("tsdb gnrt current failed since %s", tstrerror(code));
  if (pData) taosMemoryFree(pData);
  return code;
}

static int32_t tsdbLoadCurrentState(STsdbFS *pFS, STsdbFSState *pState) {
  int32_t   code = 0;
  int64_t   size;
  int64_t   n;
  char      fname[TSDB_FILENAME_LEN];
  uint8_t  *pData = NULL;
  TdFilePtr pFD;

  snprintf(fname, TSDB_FILENAME_LEN - 1, "%s%s%s%sCURRENT", tfsGetPrimaryPath(pFS->pTsdb->pVnode->pTfs), TD_DIRSEP,
           pFS->pTsdb->path, TD_DIRSEP);

  if (!taosCheckExistFile(fname)) {
    // create an empry CURRENT file if not exists
    code = tsdbGnrtCurrent(fname, pState);
    if (code) goto _err;
  } else {
    // open the file and load
    pFD = taosOpenFile(fname, TD_FILE_READ);
    if (pFD == NULL) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    if (taosFStatFile(pFD, &size, NULL) < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    pData = taosMemoryMalloc(size);
    if (pData == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }

    n = taosReadFile(pFD, pData, size);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    if (!taosCheckChecksumWhole(pData, size)) {
      code = TSDB_CODE_FILE_CORRUPTED;
      goto _err;
    }

    taosCloseFile(&pFD);

    // decode
    tGetFSState(pData, pState);
  }

  if (pData) taosMemoryFree(pData);
  return code;

_err:
  tsdbError("vgId:%d tsdb load current state failed since %s", TD_VID(pFS->pTsdb->pVnode), tstrerror(code));
  if (pData) taosMemoryFree(pData);
  return code;
}

static int32_t tsdbApplyDFileSetChange(STsdbFS *pFS, SDFileSet *pFrom, SDFileSet *pTo) {
  int32_t code = 0;
  char    fname[TSDB_FILENAME_LEN];

  if (pFrom && pTo) {
    // head
    if (tsdbFileIsSame(pFrom, pTo, TSDB_HEAD_FILE)) {
      ASSERT(pFrom->fHead.size == pTo->fHead.size);
      ASSERT(pFrom->fHead.offset == pTo->fHead.offset);
    } else {
      tsdbDataFileName(pFS->pTsdb, pFrom, TSDB_HEAD_FILE, fname);
      taosRemoveFile(fname);
    }

    // data
    if (tsdbFileIsSame(pFrom, pTo, TSDB_DATA_FILE)) {
      if (pFrom->fData.size > pTo->fData.size) {
        code = tsdbDFileRollback(pFS->pTsdb, pTo, TSDB_DATA_FILE);
        if (code) goto _err;
      }
    } else {
      tsdbDataFileName(pFS->pTsdb, pFrom, TSDB_DATA_FILE, fname);
      taosRemoveFile(fname);
    }

    // last
    if (tsdbFileIsSame(pFrom, pTo, TSDB_LAST_FILE)) {
      if (pFrom->fLast.size > pTo->fLast.size) {
        code = tsdbDFileRollback(pFS->pTsdb, pTo, TSDB_LAST_FILE);
        if (code) goto _err;
      }
    } else {
      tsdbDataFileName(pFS->pTsdb, pFrom, TSDB_LAST_FILE, fname);
      taosRemoveFile(fname);
    }

    // sma
    if (tsdbFileIsSame(pFrom, pTo, TSDB_SMA_FILE)) {
      if (pFrom->fSma.size > pTo->fSma.size) {
        code = tsdbDFileRollback(pFS->pTsdb, pTo, TSDB_SMA_FILE);
        if (code) goto _err;
      }
    } else {
      tsdbDataFileName(pFS->pTsdb, pFrom, TSDB_SMA_FILE, fname);
      taosRemoveFile(fname);
    }
  } else if (pFrom) {
    // head
    tsdbDataFileName(pFS->pTsdb, pFrom, TSDB_HEAD_FILE, fname);
    taosRemoveFile(fname);

    // data
    tsdbDataFileName(pFS->pTsdb, pFrom, TSDB_DATA_FILE, fname);
    taosRemoveFile(fname);

    // last
    tsdbDataFileName(pFS->pTsdb, pFrom, TSDB_LAST_FILE, fname);
    taosRemoveFile(fname);

    // fsm
    tsdbDataFileName(pFS->pTsdb, pFrom, TSDB_SMA_FILE, fname);
    taosRemoveFile(fname);
  }

  return code;

_err:
  tsdbError("vgId:%d tsdb apply disk file set change failed since %s", TD_VID(pFS->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbApplyDelFileChange(STsdbFS *pFS, SDelFile *pFrom, SDelFile *pTo) {
  int32_t code = 0;
  char    fname[TSDB_FILENAME_LEN];

  if (pFrom && pTo) {
    if (pFrom != pTo) {
      tsdbDelFileName(pFS->pTsdb, pFrom, fname);
      if (taosRemoveFile(fname) < 0) {
        code = TAOS_SYSTEM_ERROR(errno);
        goto _err;
      }
    }
  } else if (pFrom) {
    tsdbDelFileName(pFS->pTsdb, pFrom, fname);
    if (taosRemoveFile(fname) < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }
  } else {
    // do nothing
  }

  return code;

_err:
  tsdbError("vgId:%d tsdb apply del file change failed since %s", TD_VID(pFS->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbFSApplyDiskChange(STsdbFS *pFS, STsdbFSState *pFrom, STsdbFSState *pTo) {
  int32_t    code = 0;
  int32_t    iFrom = 0;
  int32_t    nFrom = taosArrayGetSize(pFrom->aDFileSet);
  int32_t    iTo = 0;
  int32_t    nTo = taosArrayGetSize(pTo->aDFileSet);
  SDFileSet *pDFileSetFrom;
  SDFileSet *pDFileSetTo;

  // SDelFile
  code = tsdbApplyDelFileChange(pFS, pFrom->pDelFile, pTo->pDelFile);
  if (code) goto _err;

  // SDFileSet
  while (iFrom < nFrom && iTo < nTo) {
    pDFileSetFrom = (SDFileSet *)taosArrayGet(pFrom->aDFileSet, iFrom);
    pDFileSetTo = (SDFileSet *)taosArrayGet(pTo->aDFileSet, iTo);

    if (pDFileSetFrom->fid == pDFileSetTo->fid) {
      code = tsdbApplyDFileSetChange(pFS, pDFileSetFrom, pDFileSetTo);
      if (code) goto _err;

      iFrom++;
      iTo++;
    } else if (pDFileSetFrom->fid < pDFileSetTo->fid) {
      code = tsdbApplyDFileSetChange(pFS, pDFileSetFrom, NULL);
      if (code) goto _err;

      iFrom++;
    } else {
      iTo++;
    }
  }

  while (iFrom < nFrom) {
    pDFileSetFrom = (SDFileSet *)taosArrayGet(pFrom->aDFileSet, iFrom);
    code = tsdbApplyDFileSetChange(pFS, pDFileSetFrom, NULL);
    if (code) goto _err;

    iFrom++;
  }

#if 0
  // do noting
  while (iTo < nTo) {
    pDFileSetTo = (SDFileSet *)taosArrayGetP(pTo->aDFileSet, iTo);
    code = tsdbApplyDFileSetChange(pFS, NULL, pDFileSetTo);
    if (code) goto _err;

    iTo++;
  }
#endif

  return code;

_err:
  tsdbError("vgId:%d tsdb fs apply disk change failed sicne %s", TD_VID(pFS->pTsdb->pVnode), tstrerror(code));
  return code;
}

static void tsdbFSDestroy(STsdbFS *pFS) {
  if (pFS) {
    if (pFS->nState) {
      taosArrayDestroy(pFS->nState->aDFileSet);
      taosMemoryFree(pFS->nState);
    }

    if (pFS->cState) {
      taosArrayDestroy(pFS->cState->aDFileSet);
      taosMemoryFree(pFS->cState);
    }

    taosThreadRwlockDestroy(&pFS->lock);
    taosMemoryFree(pFS);
  }
  // TODO
}

static int32_t tsdbFSCreate(STsdb *pTsdb, STsdbFS **ppFS) {
  int32_t  code = 0;
  STsdbFS *pFS = NULL;

  pFS = (STsdbFS *)taosMemoryCalloc(1, sizeof(*pFS));
  if (pFS == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pFS->pTsdb = pTsdb;

  code = taosThreadRwlockInit(&pFS->lock, NULL);
  if (code) {
    taosMemoryFree(pFS);
    code = TAOS_SYSTEM_ERROR(code);
    goto _err;
  }

  pFS->inTxn = 0;

  pFS->cState = (STsdbFSState *)taosMemoryCalloc(1, sizeof(STsdbFSState));
  if (pFS->cState == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pFS->cState->aDFileSet = taosArrayInit(0, sizeof(SDFileSet));
  if (pFS->cState->aDFileSet == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  pFS->nState = (STsdbFSState *)taosMemoryCalloc(1, sizeof(STsdbFSState));
  if (pFS->nState == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pFS->nState->aDFileSet = taosArrayInit(0, sizeof(SDFileSet));
  if (pFS->nState->aDFileSet == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  *ppFS = pFS;
  return code;

_err:
  tsdbError("vgId:%d tsdb fs create failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  tsdbFSDestroy(pFS);
  *ppFS = NULL;
  return code;
}

static int32_t tsdbScanAndTryFixFS(STsdbFS *pFS, int8_t deepScan) {
  int32_t   code = 0;
  STsdb    *pTsdb = pFS->pTsdb;
  STfs     *pTfs = pTsdb->pVnode->pTfs;
  int64_t   size;
  char      fname[TSDB_FILENAME_LEN];
  char      pHdr[TSDB_FHDR_SIZE];
  TdFilePtr pFD;

  // SDelFile
  if (pFS->cState->pDelFile) {
    tsdbDelFileName(pTsdb, pFS->cState->pDelFile, fname);
    if (taosStatFile(fname, &size, NULL)) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    if (size != pFS->cState->pDelFile->size) {
      code = TSDB_CODE_FILE_CORRUPTED;
      goto _err;
    }

    if (deepScan) {
      // TODO
    }
  }

  // SArray<SDFileSet>
  for (int32_t iSet = 0; iSet < taosArrayGetSize(pFS->cState->aDFileSet); iSet++) {
    SDFileSet *pDFileSet = (SDFileSet *)taosArrayGet(pFS->cState->aDFileSet, iSet);

    // head =========
    tsdbDataFileName(pTsdb, pDFileSet, TSDB_HEAD_FILE, fname);
    if (taosStatFile(fname, &size, NULL)) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    if (deepScan) {
      // TODO
    }

    // data =========
    tsdbDataFileName(pTsdb, pDFileSet, TSDB_DATA_FILE, fname);
    if (taosStatFile(fname, &size, NULL)) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    if (size < pDFileSet->fData.size) {
      code = TSDB_CODE_FILE_CORRUPTED;
      goto _err;
    } else if (size > pDFileSet->fData.size) {
      ASSERT(0);
      // need to rollback the file
    }

    if (deepScan) {
      // TODO
    }

    // last ===========
    tsdbDataFileName(pTsdb, pDFileSet, TSDB_LAST_FILE, fname);
    if (taosStatFile(fname, &size, NULL)) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    if (size < pDFileSet->fLast.size) {
      code = TSDB_CODE_FILE_CORRUPTED;
      goto _err;
    } else if (size > pDFileSet->fLast.size) {
      ASSERT(0);
      // need to rollback the file
    }

    if (deepScan) {
      // TODO
    }

    // sma =============
    tsdbDataFileName(pTsdb, pDFileSet, TSDB_SMA_FILE, fname);
    if (taosStatFile(fname, &size, NULL)) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    if (size < pDFileSet->fSma.size) {
      code = TSDB_CODE_FILE_CORRUPTED;
      goto _err;
    } else if (size > pDFileSet->fSma.size) {
      ASSERT(0);
      // need to rollback the file
    }

    if (deepScan) {
      // TODO
    }
  }

  // remove those invalid files (todo)
#if 0
  STfsDir        *tdir;
  const STfsFile *pf;

  tdir = tfsOpendir(pTfs, pTsdb->path);
  if (tdir == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  while ((pf = tfsReaddir(tdir))) {
    tfsBasename(pf, fname);
  }

  tfsClosedir(tdir);
#endif

  return code;

_err:
  tsdbError("vgId:%d tsdb scan and try fix fs failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tDFileSetCmprFn(const void *p1, const void *p2) {
  if (((SDFileSet *)p1)->fid < ((SDFileSet *)p2)->fid) {
    return -1;
  } else if (((SDFileSet *)p1)->fid > ((SDFileSet *)p2)->fid) {
    return 1;
  }

  return 0;
}

// EXPOSED APIS ====================================================================================
int32_t tsdbFSOpen(STsdb *pTsdb, STsdbFS **ppFS) {
  int32_t code = 0;

  // create handle
  code = tsdbFSCreate(pTsdb, ppFS);
  if (code) goto _err;

  // load current state
  code = tsdbLoadCurrentState(*ppFS, (*ppFS)->cState);
  if (code) {
    tsdbFSDestroy(*ppFS);
    goto _err;
  }

  // scan and fix FS
  code = tsdbScanAndTryFixFS(*ppFS, 0);
  if (code) {
    tsdbFSDestroy(*ppFS);
    goto _err;
  }

  return code;

_err:
  *ppFS = NULL;
  tsdbError("vgId:%d tsdb fs open failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbFSClose(STsdbFS *pFS) {
  int32_t code = 0;
  tsdbFSDestroy(pFS);
  return code;
}

int32_t tsdbFSBegin(STsdbFS *pFS) {
  int32_t code = 0;

  ASSERT(!pFS->inTxn);

  // SDelFile
  pFS->nState->pDelFile = NULL;
  if (pFS->cState->pDelFile) {
    pFS->nState->delFile = pFS->cState->delFile;
    pFS->nState->pDelFile = &pFS->nState->delFile;
  }

  // SArray<aDFileSet>
  taosArrayClear(pFS->nState->aDFileSet);
  for (int32_t iSet = 0; iSet < taosArrayGetSize(pFS->cState->aDFileSet); iSet++) {
    SDFileSet *pDFileSet = (SDFileSet *)taosArrayGet(pFS->cState->aDFileSet, iSet);

    if (taosArrayPush(pFS->nState->aDFileSet, pDFileSet) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }

  pFS->inTxn = 1;
  return code;

_err:
  tsdbError("vgId:%d tsdb fs begin failed since %s", TD_VID(pFS->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbFSCommit(STsdbFS *pFS) {
  int32_t       code = 0;
  STsdbFSState *pState = pFS->nState;
  char          tfname[TSDB_FILENAME_LEN];
  char          fname[TSDB_FILENAME_LEN];

  // need lock (todo)
  pFS->nState = pFS->cState;
  pFS->cState = pState;

  snprintf(tfname, TSDB_FILENAME_LEN - 1, "%s%s%s%sCURRENT.t", tfsGetPrimaryPath(pFS->pTsdb->pVnode->pTfs), TD_DIRSEP,
           pFS->pTsdb->path, TD_DIRSEP);
  snprintf(fname, TSDB_FILENAME_LEN - 1, "%s%s%s%sCURRENT", tfsGetPrimaryPath(pFS->pTsdb->pVnode->pTfs), TD_DIRSEP,
           pFS->pTsdb->path, TD_DIRSEP);

  // gnrt CURRENT.t
  code = tsdbGnrtCurrent(tfname, pFS->cState);
  if (code) goto _err;

  // rename
  code = taosRenameFile(tfname, fname);
  if (code) {
    code = TAOS_SYSTEM_ERROR(code);
    goto _err;
  }

  // apply commit on disk
  code = tsdbFSApplyDiskChange(pFS, pFS->nState, pFS->cState);
  if (code) goto _err;

  pFS->inTxn = 0;

  return code;

_err:
  tsdbError("vgId:%d tsdb fs commit failed since %s", TD_VID(pFS->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbFSRollback(STsdbFS *pFS) {
  int32_t code = 0;

  code = tsdbFSApplyDiskChange(pFS, pFS->nState, pFS->cState);
  if (code) goto _err;

  pFS->inTxn = 0;

  return code;

_err:
  tsdbError("vgId:%d tsdb fs rollback failed since %s", TD_VID(pFS->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbFSStateUpsertDelFile(STsdbFSState *pState, SDelFile *pDelFile) {
  int32_t code = 0;
  pState->delFile = *pDelFile;
  pState->pDelFile = &pState->delFile;
  return code;
}

int32_t tsdbFSStateUpsertDFileSet(STsdbFSState *pState, SDFileSet *pSet) {
  int32_t code = 0;
  int32_t idx = taosArraySearchIdx(pState->aDFileSet, pSet, tDFileSetCmprFn, TD_GE);

  if (idx < 0) {
    if (taosArrayPush(pState->aDFileSet, pSet) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
  } else {
    SDFileSet *tDFileSet = (SDFileSet *)taosArrayGet(pState->aDFileSet, idx);
    int32_t    c = tDFileSetCmprFn(pSet, tDFileSet);
    if (c == 0) {
      taosArraySet(pState->aDFileSet, idx, pSet);
    } else {
      if (taosArrayInsert(pState->aDFileSet, idx, pSet) == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _exit;
      }
    }
  }

_exit:
  return code;
}

void tsdbFSStateDeleteDFileSet(STsdbFSState *pState, int32_t fid) {
  int32_t idx;

  idx = taosArraySearchIdx(pState->aDFileSet, &(SDFileSet){.fid = fid}, tDFileSetCmprFn, TD_EQ);
  ASSERT(idx >= 0);
  taosArrayRemove(pState->aDFileSet, idx);
}

SDelFile *tsdbFSStateGetDelFile(STsdbFSState *pState) { return pState->pDelFile; }

SDFileSet *tsdbFSStateGetDFileSet(STsdbFSState *pState, int32_t fid) {
  return (SDFileSet *)taosArraySearch(pState->aDFileSet, &(SDFileSet){.fid = fid}, tDFileSetCmprFn, TD_EQ);
}
