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

typedef struct {
  SDelFile *pDelFile;
  SArray   *aDFileSet;  // SArray<aDFileSet *>
} STsdbFSState;

struct STsdbFS {
  STsdb         *pTsdb;
  TdThreadRwlock lock;
  int8_t         inTxn;
  STsdbFSState  *cState;
  STsdbFSState  *nState;
};

// =================================================================================================
static int32_t tsdbDelFileToJson(const void *pObj, SJson *pJson) {
  int32_t   code = 0;
  SDelFile *pDelFile = (SDelFile *)pObj;

  if (tjsonAddIntegerToObject(pJson, "minKey", pDelFile->minKey) < 0) goto _err;
  if (tjsonAddIntegerToObject(pJson, "maxKey", pDelFile->maxKey) < 0) goto _err;
  if (tjsonAddIntegerToObject(pJson, "minVer", pDelFile->minVersion) < 0) goto _err;
  if (tjsonAddIntegerToObject(pJson, "maxVer", pDelFile->maxVersion) < 0) goto _err;

  return code;

_err:
  code = TSDB_CODE_OUT_OF_MEMORY;
  return code;
}

static int32_t tsdbHeadFileToJson(const void *pObj, SJson *pJson) {
  int32_t    code = 0;
  SHeadFile *pHeadFile = (SHeadFile *)pObj;

  if (tjsonAddIntegerToObject(pJson, "size", pHeadFile->size) < 0) goto _err;
  if (tjsonAddIntegerToObject(pJson, "offset", pHeadFile->offset) < 0) goto _err;

  return code;

_err:
  code = TSDB_CODE_OUT_OF_MEMORY;
  return code;
}

static int32_t tsdbDataFileToJson(const void *pObj, SJson *pJson) {
  int32_t    code = 0;
  SDataFile *pDataFile = (SDataFile *)pObj;

  if (tjsonAddIntegerToObject(pJson, "size", pDataFile->size) < 0) goto _err;

  return code;

_err:
  code = TSDB_CODE_OUT_OF_MEMORY;
  return code;
}

static int32_t tsdbLastFileToJson(const void *pObj, SJson *pJson) {
  int32_t    code = 0;
  SLastFile *pLastFile = (SLastFile *)pObj;

  if (tjsonAddIntegerToObject(pJson, "size", pLastFile->size) < 0) goto _err;

  return code;

_err:
  code = TSDB_CODE_OUT_OF_MEMORY;
  return code;
}

static int32_t tsdbSmaFileToJson(const void *pObj, SJson *pJson) {
  int32_t   code = 0;
  SSmaFile *pSmaFile = (SSmaFile *)pObj;

  if (tjsonAddIntegerToObject(pJson, "size", pSmaFile->size) < 0) goto _err;

  return code;

_err:
  code = TSDB_CODE_OUT_OF_MEMORY;
  return code;
}

static int32_t tsdbDFileSetToJson(const void *pObj, SJson *pJson) {
  int32_t    code = 0;
  SDFileSet *pDFileSet = (SDFileSet *)pObj;

  if (tjsonAddIntegerToObject(pJson, "level", pDFileSet->diskId.level) < 0) goto _err;
  if (tjsonAddIntegerToObject(pJson, "id", pDFileSet->diskId.id) < 0) goto _err;
  if (tjsonAddIntegerToObject(pJson, "fid", pDFileSet->fid) < 0) goto _err;
  if (tjsonAddObject(pJson, "head", tsdbHeadFileToJson, pDFileSet->pHeadFile) < 0) goto _err;
  if (tjsonAddObject(pJson, "data", tsdbDataFileToJson, pDFileSet->pDataFile) < 0) goto _err;
  if (tjsonAddObject(pJson, "last", tsdbLastFileToJson, pDFileSet->pLastFile) < 0) goto _err;
  if (tjsonAddObject(pJson, "sma", tsdbSmaFileToJson, pDFileSet->pSmaFile) < 0) goto _err;

  return code;

_err:
  code = TSDB_CODE_OUT_OF_MEMORY;
  return code;
}

static int32_t tsdbFSStateToJsonStr(STsdbFSState *pState, char **ppData) {
  int32_t code = 0;
  SJson  *pJson = NULL;

  pJson = tjsonCreateObject();
  if (pJson == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  if (tjsonAddObject(pJson, "DelFile", tsdbDelFileToJson, pState->pDelFile) < 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  if (tjsonAddTArray(pJson, "DFileSet", tsdbDFileSetToJson, pState->aDFileSet) < 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  *ppData = tjsonToString(pJson);
  if (*ppData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  tjsonDelete(pJson);

  return code;

_err:
  return code;
}

static int32_t tsdbJsonStrToFSState(char *pData, STsdbFSState *pState) {
  int32_t code = 0;
  SJson  *pJson = NULL;

  pJson = tjsonParse(pData);
  if (pJson == NULL) goto _err;

  // if (tjsonToObject(pJson, "DelFile", tsdbJsonToDelFile, &pState->pDelFile) < 0) goto _err;
  // if (tjsonToTArray(pJson, "DFIleSet", tsdbJsonToDFileSet, ) < 0) goto _err;
  ASSERT(0);

  tjsonDelete(pJson);

  return code;

_err:
  code = TSDB_CODE_OUT_OF_MEMORY;
  return code;
}

static int32_t tsdbCreateEmptyCurrent(const char *fname, STsdbFSState *pState) {
  int32_t   code = 0;
  int64_t   n;
  int64_t   size;
  char     *pData = NULL;
  TdFilePtr pFD = NULL;

  // to json str
  code = tsdbFSStateToJsonStr(pState, &pData);
  if (code) goto _err;

  // create and write
  pFD = taosOpenFile(fname, TD_FILE_WRITE | TD_FILE_CREATE);
  if (pFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  size = strlen(pData);
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
  tsdbError("create empry current failed since %s", tstrerror(code));
  if (pData) taosMemoryFree(pData);
  return code;
}

static int32_t tsdbSaveCurrentState(STsdbFS *pFS, STsdbFSState *pState) {
  int32_t   code = 0;
  int64_t   n;
  int64_t   size;
  char      tfname[TSDB_FILENAME_LEN];
  char      fname[TSDB_FILENAME_LEN];
  char     *pData = NULL;
  TdFilePtr pFD = NULL;

  snprintf(tfname, TSDB_FILENAME_LEN - 1, "%s/CURRENT.t", pFS->pTsdb->path);
  snprintf(fname, TSDB_FILENAME_LEN - 1, "%s/CURRENT", pFS->pTsdb->path);

  // encode
  code = tsdbFSStateToJsonStr(pState, &pData);
  if (code) goto _err;

  // create and write tfname
  pFD = taosOpenFile(tfname, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (pFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  size = strlen(pData);
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

  // rename
  code = taosRenameFile(tfname, fname);
  if (code) {
    code = TAOS_SYSTEM_ERROR(code);
    goto _err;
  }

  if (pData) taosMemoryFree(pData);
  return code;

_err:
  tsdbError("vgId:%d tsdb save current state failed since %s", TD_VID(pFS->pTsdb->pVnode), tstrerror(code));
  if (pData) taosMemoryFree(pData);
  return code;
}

static int32_t tsdbLoadCurrentState(STsdbFS *pFS, STsdbFSState *pState) {
  int32_t   code = 0;
  int64_t   size;
  int64_t   n;
  char      fname[TSDB_FILENAME_LEN];
  char     *pData = NULL;
  TdFilePtr pFD;

  snprintf(fname, TSDB_FILENAME_LEN - 1, "%s/CURRENT", pFS->pTsdb->path);

  if (!taosCheckExistFile(fname)) {
    // create an empry CURRENT file if not exists
    code = tsdbCreateEmptyCurrent(fname, pState);
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

    pData = taosMemoryMalloc(size + 1);
    if (pData == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
    pData[size] = '\0';

    n = taosReadFile(pFD, pData, size);
    if (n < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    taosCloseFile(&pFD);

    // decode
    code = tsdbJsonStrToFSState(pData, pState);
    if (code) goto _err;
  }

  if (pData) taosMemoryFree(pData);
  return code;

_err:
  tsdbError("vgId:%d tsdb load current state failed since %s", TD_VID(pFS->pTsdb->pVnode), tstrerror(code));
  if (pData) taosMemoryFree(pData);
  return code;
}

static int32_t tsdbFSOpenImpl(STsdbFS *pFS) {
  int32_t code = 0;
  int64_t size;
  int64_t n;

  // read CURRENT file
  code = tsdbLoadCurrentState(pFS, pFS->cState);
  if (code) goto _err;

  // decode the statue file
  // code = tsdbDecodeFSState(pData, pFS->cState);
  // if (code) goto _err;

  // // scan and fix invalid tsdb FS
  // code = tsdbScanAndFixFS(pFS);
  // if (code) goto _err;

  // if (pData) taosMemoryFree(pData);
  return code;

_err:
  // if (pData) taosMemoryFree(pData);
  tsdbError("vgId:%d tsdb fs open impl failed since %s", TD_VID(pFS->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbFSCloseImpl(STsdbFS *pFS) {
  int32_t code = 0;
  // TODO
  return code;
}

static int32_t tsdbApplyDFileSetChange(STsdbFS *pFS, SDFileSet *pFrom, SDFileSet *pTo) {
  int32_t code = 0;
  // TODO
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
    pDFileSetFrom = (SDFileSet *)taosArrayGetP(pFrom->aDFileSet, iFrom);
    pDFileSetTo = (SDFileSet *)taosArrayGetP(pTo->aDFileSet, iTo);

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
    pDFileSetFrom = (SDFileSet *)taosArrayGetP(pFrom->aDFileSet, iFrom);
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

// EXPOSED APIS ====================================================================================
int32_t tsdbFSOpen(STsdb *pTsdb, STsdbFS **ppFS) {
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
    code = TAOS_SYSTEM_ERROR(code);
    goto _err;
  }

  pFS->inTxn = 0;

  pFS->cState = (STsdbFSState *)taosMemoryCalloc(1, sizeof(STsdbFSState));
  if (pFS->cState == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pFS->cState->aDFileSet = taosArrayInit(0, sizeof(SDFileSet *));
  if (pFS->cState->aDFileSet == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  pFS->nState = (STsdbFSState *)taosMemoryCalloc(1, sizeof(STsdbFSState));
  if (pFS->nState == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pFS->nState->aDFileSet = taosArrayInit(0, sizeof(SDFileSet *));
  if (pFS->nState->aDFileSet == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  code = tsdbFSOpenImpl(pFS);
  if (code) goto _err;

  *ppFS = pFS;
  return code;

_err:
  *ppFS = NULL;
  tsdbError("vgId:%d tsdb fs open failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbFSClose(STsdbFS *pFS) {
  int32_t code = 0;

  if (pFS) {
    code = tsdbFSCloseImpl(pFS);
    if (code) goto _err;

    taosArrayDestroy(pFS->nState->aDFileSet);
    taosMemoryFree(pFS->nState);
    taosArrayDestroy(pFS->cState->aDFileSet);
    taosMemoryFree(pFS->cState);
    taosThreadRwlockDestroy(&pFS->lock);
    taosMemoryFree(pFS);
  }

  return code;

_err:
  tsdbError("vgId:%d tsdb fs close failed since %s", TD_VID(pFS->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbFSBegin(STsdbFS *pFS) {
  int32_t code = 0;

  ASSERT(!pFS->inTxn);

  pFS->inTxn = 1;

  pFS->nState->pDelFile = pFS->cState->pDelFile;
  taosArrayClear(pFS->nState->aDFileSet);
  for (int32_t iDFileSet = 0; iDFileSet < taosArrayGetSize(pFS->cState->aDFileSet); iDFileSet++) {
    SDFileSet *pDFileSet = (SDFileSet *)taosArrayGetP(pFS->cState->aDFileSet, iDFileSet);

    if (taosArrayPush(pFS->nState->aDFileSet, &pDFileSet) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }

  return code;

_err:
  tsdbError("vgId:%d tsdb fs begin failed since %s", TD_VID(pFS->pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbFSCommit(STsdbFS *pFS) {
  int32_t       code = 0;
  STsdbFSState *pState = pFS->nState;

  // need lock (todo)
  pFS->nState = pFS->cState;
  pFS->cState = pState;

  // save
  code = tsdbSaveCurrentState(pFS, pFS->cState);
  if (code) goto _err;

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
