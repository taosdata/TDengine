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

#define FS_SUFFIX(type) ((type) == VND_TASK_COMMIT ? "t" : "r")

// =================================================================================================
static int32_t tsdbFSToBinary(uint8_t *p, STsdbFS *pFS) {
  int32_t  n = 0;
  int8_t   hasDel = pFS->pDelFile ? 1 : 0;
  uint32_t nSet = taosArrayGetSize(pFS->aDFileSet);

  pFS->nMaxSttF = 0;

  // version
  n += tPutI8(p ? p + n : p, 1);  // 0->1

  // SDelFile
  n += tPutI8(p ? p + n : p, hasDel);
  if (hasDel) {
    n += tPutDelFile(p ? p + n : p, pFS->pDelFile);
  }

  // SArray<SDFileSet>
  n += tPutU32v(p ? p + n : p, nSet);
  for (uint32_t iSet = 0; iSet < nSet; iSet++) {
    SDFileSet *pDFileSet = (SDFileSet *)taosArrayGet(pFS->aDFileSet, iSet);
    n += tPutDFileSet(p ? p + n : p, pDFileSet);
    if (pFS->nMaxSttF < pDFileSet->nSttF) pFS->nMaxSttF = pDFileSet->nSttF;
  }

  return n;
}

static int32_t tsdbBinaryToFS(uint8_t *pData, int64_t nData, STsdbFS *pFS) {
  int32_t code = 0;
  int32_t n = 0;
  int8_t  ver = 0;

  // version
  n += tGetI8(pData + n, &ver);

  if (ver < 0 || ver > 1) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _exit;
  }

  // SDelFile
  int8_t hasDel = 0;
  n += tGetI8(pData + n, &hasDel);
  if (hasDel) {
    pFS->pDelFile = (SDelFile *)taosMemoryCalloc(1, sizeof(SDelFile));
    if (pFS->pDelFile == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }

    n += tGetDelFile(pData + n, pFS->pDelFile);
    pFS->pDelFile->nRef = 1;
  } else {
    pFS->pDelFile = NULL;
  }

  // aDFileSet
  taosArrayClear(pFS->aDFileSet);
  uint32_t nSet = 0;
  n += tGetU32v(pData + n, &nSet);
  for (uint32_t iSet = 0; iSet < nSet; iSet++) {
    SDFileSet fSet = {0};

    int32_t nt = tGetDFileSet(pData + n, &fSet, ver);
    if (nt < 0) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }

    n += nt;
    if (taosArrayPush(pFS->aDFileSet, &fSet) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
  }

  ASSERT(n + sizeof(TSCKSUM) == nData);

_exit:
  return code;
}

static int32_t tsdbSaveFSToFile(STsdbFS *pFS, const char *fname) {
  int32_t code = 0;
  int32_t lino = 0;

  // encode to binary
  int32_t  size = tsdbFSToBinary(NULL, pFS) + sizeof(TSCKSUM);
  uint8_t *pData = taosMemoryMalloc(size);
  if (pData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  tsdbFSToBinary(pData, pFS);
  taosCalcChecksumAppend(0, pData, size);

  // save to file
  TdFilePtr pFD = taosOpenFile(fname, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
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
    tsdbError("%s failed at line %d since %s, fname:%s", __func__, lino, tstrerror(code), fname);
  }
  return code;
}

int32_t tsdbFSCreate(STsdbFS *pFS) {
  int32_t code = 0;

  pFS->pDelFile = NULL;
  pFS->aDFileSet = taosArrayInit(0, sizeof(SDFileSet));
  if (pFS->aDFileSet == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

_exit:
  return code;
}

void tsdbFSDestroy(STsdbFS *pFS) {
  if (pFS->pDelFile) {
    taosMemoryFree(pFS->pDelFile);
    pFS->pDelFile = NULL;
  }

  for (int32_t iSet = 0; iSet < taosArrayGetSize(pFS->aDFileSet); iSet++) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(pFS->aDFileSet, iSet);
    taosMemoryFree(pSet->pHeadF);
    taosMemoryFree(pSet->pDataF);
    taosMemoryFree(pSet->pSmaF);
    for (int32_t iStt = 0; iStt < pSet->nSttF; iStt++) {
      taosMemoryFree(pSet->aSttF[iStt]);
    }
  }

  taosArrayDestroy(pFS->aDFileSet);
  pFS->aDFileSet = NULL;
}

static int32_t tsdbScanAndTryFixFS(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;
  int64_t size = 0;
  char    fname[TSDB_FILENAME_LEN] = {0};

  // SDelFile
  if (pTsdb->fs.pDelFile) {
    tsdbDelFileName(pTsdb, pTsdb->fs.pDelFile, fname);
    if (taosStatFile(fname, &size, NULL)) {
      code = TAOS_SYSTEM_ERROR(errno);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    if (size != tsdbLogicToFileSize(pTsdb->fs.pDelFile->size, pTsdb->pVnode->config.tsdbPageSize)) {
      code = TSDB_CODE_FILE_CORRUPTED;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  // SArray<SDFileSet>
  int32_t fid = 0;
  for (int32_t iSet = 0; iSet < taosArrayGetSize(pTsdb->fs.aDFileSet); iSet++) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(pTsdb->fs.aDFileSet, iSet);
    fid = pSet->fid;

    // head =========
    tsdbHeadFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pHeadF, fname);
    if (taosStatFile(fname, &size, NULL)) {
      code = TAOS_SYSTEM_ERROR(errno);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    if (size != tsdbLogicToFileSize(pSet->pHeadF->size, pTsdb->pVnode->config.tsdbPageSize)) {
      code = TSDB_CODE_FILE_CORRUPTED;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    // data =========
    tsdbDataFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pDataF, fname);
    if (taosStatFile(fname, &size, NULL)) {
      code = TAOS_SYSTEM_ERROR(errno);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    if (size < tsdbLogicToFileSize(pSet->pDataF->size, pTsdb->pVnode->config.tsdbPageSize)) {
      code = TSDB_CODE_FILE_CORRUPTED;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    // else if (size > tsdbLogicToFileSize(pSet->pDataF->size, pTsdb->pVnode->config.tsdbPageSize)) {
    //   code = tsdbDFileRollback(pTsdb, pSet, TSDB_DATA_FILE);
    //   TSDB_CHECK_CODE(code, lino, _exit);
    // }

    // sma =============
    tsdbSmaFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pSmaF, fname);
    if (taosStatFile(fname, &size, NULL)) {
      code = TAOS_SYSTEM_ERROR(errno);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    if (size < tsdbLogicToFileSize(pSet->pSmaF->size, pTsdb->pVnode->config.tsdbPageSize)) {
      code = TSDB_CODE_FILE_CORRUPTED;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    // else if (size > tsdbLogicToFileSize(pSet->pSmaF->size, pTsdb->pVnode->config.tsdbPageSize)) {
    //   code = tsdbDFileRollback(pTsdb, pSet, TSDB_SMA_FILE);
    //   TSDB_CHECK_CODE(code, lino, _exit);
    // }

    // stt ===========
    for (int32_t iStt = 0; iStt < pSet->nSttF; iStt++) {
      tsdbSttFileName(pTsdb, pSet->diskId, pSet->fid, pSet->aSttF[iStt], fname);
      if (taosStatFile(fname, &size, NULL)) {
        code = TAOS_SYSTEM_ERROR(errno);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
      if (size != tsdbLogicToFileSize(pSet->aSttF[iStt]->size, pTsdb->pVnode->config.tsdbPageSize)) {
        code = TSDB_CODE_FILE_CORRUPTED;
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }
  }

  {
    // remove those invalid files (todo)
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s, fid:%d", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code),
              fid);
  }
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

static void tsdbGetCurrentFName(STsdb *pTsdb, char *current, char *current_t, int8_t type) {
  SVnode *pVnode = pTsdb->pVnode;
  if (pVnode->pTfs) {
    if (current) {
      snprintf(current, TSDB_FILENAME_LEN - 1, "%s%s%s%sCURRENT", tfsGetPrimaryPath(pTsdb->pVnode->pTfs), TD_DIRSEP,
               pTsdb->path, TD_DIRSEP);
    }
    if (current_t) {
      snprintf(current_t, TSDB_FILENAME_LEN - 1, "%s%s%s%sCURRENT.%s", tfsGetPrimaryPath(pTsdb->pVnode->pTfs),
               TD_DIRSEP, pTsdb->path, TD_DIRSEP, FS_SUFFIX(type));
    }
  } else {
    if (current) {
      snprintf(current, TSDB_FILENAME_LEN - 1, "%s%sCURRENT", pTsdb->path, TD_DIRSEP);
    }
    if (current_t) {
      snprintf(current_t, TSDB_FILENAME_LEN - 1, "%s%sCURRENT.%s", pTsdb->path, TD_DIRSEP, FS_SUFFIX(type));
    }
  }
}

static int32_t tsdbLoadFSFromFile(const char *fname, STsdbFS *pFS) {
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
    tsdbError("%s failed at line %d since %s, fname:%s", __func__, lino, tstrerror(code), fname);
  }
  return code;
}

static int32_t tsdbRemoveFileSet(STsdb *pTsdb, SDFileSet *pSet) {
  int32_t code = 0;
  char    fname[TSDB_FILENAME_LEN] = {0};

  int32_t nRef = atomic_sub_fetch_32(&pSet->pHeadF->nRef, 1);
  if (nRef == 0) {
    tsdbHeadFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pHeadF, fname);
    (void)taosRemoveFile(fname);
    taosMemoryFree(pSet->pHeadF);
  }

  nRef = atomic_sub_fetch_32(&pSet->pDataF->nRef, 1);
  if (nRef == 0) {
    tsdbDataFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pDataF, fname);
    (void)taosRemoveFile(fname);
    taosMemoryFree(pSet->pDataF);
  }

  nRef = atomic_sub_fetch_32(&pSet->pSmaF->nRef, 1);
  if (nRef == 0) {
    tsdbSmaFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pSmaF, fname);
    (void)taosRemoveFile(fname);
    taosMemoryFree(pSet->pSmaF);
  }

  for (int8_t iStt = 0; iStt < pSet->nSttF; iStt++) {
    nRef = atomic_sub_fetch_32(&pSet->aSttF[iStt]->nRef, 1);
    if (nRef == 0) {
      tsdbSttFileName(pTsdb, pSet->diskId, pSet->fid, pSet->aSttF[iStt], fname);
      (void)taosRemoveFile(fname);
      taosMemoryFree(pSet->aSttF[iStt]);
    }
  }

_exit:
  return code;
}

static int32_t tsdbNewFileSet(STsdb *pTsdb, SDFileSet *pSetTo, SDFileSet *pSetFrom) {
  int32_t code = 0;
  int32_t lino = 0;

  *pSetTo = (SDFileSet){.diskId = pSetFrom->diskId, .fid = pSetFrom->fid, .nSttF = 0};

  // head
  pSetTo->pHeadF = (SHeadFile *)taosMemoryMalloc(sizeof(SHeadFile));
  if (pSetTo->pHeadF == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  *pSetTo->pHeadF = *pSetFrom->pHeadF;
  pSetTo->pHeadF->nRef = 1;

  // data
  pSetTo->pDataF = (SDataFile *)taosMemoryMalloc(sizeof(SDataFile));
  if (pSetTo->pDataF == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  *pSetTo->pDataF = *pSetFrom->pDataF;
  pSetTo->pDataF->nRef = 1;

  // sma
  pSetTo->pSmaF = (SSmaFile *)taosMemoryMalloc(sizeof(SSmaFile));
  if (pSetTo->pSmaF == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  *pSetTo->pSmaF = *pSetFrom->pSmaF;
  pSetTo->pSmaF->nRef = 1;

  // stt
  for (int32_t iStt = 0; iStt < pSetFrom->nSttF; iStt++) {
    pSetTo->aSttF[iStt] = (SSttFile *)taosMemoryMalloc(sizeof(SSttFile));
    if (pSetTo->aSttF[iStt] == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    pSetTo->nSttF++;
    *pSetTo->aSttF[iStt] = *pSetFrom->aSttF[iStt];
    pSetTo->aSttF[iStt]->nRef = 1;
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}
#if 0
static int32_t tsdbRemoveHeadSttF(STsdb *pTsdb, SDFileSet *pSet, bool unRef) {
  int32_t code = 0;
  int32_t nRef = 0;
  char    fname[TSDB_FILENAME_LEN] = {0};

  ASSERT(pSet->nSttF > 0);

  SSttFile *pSttF = pSet->aSttF[0];
  int64_t   commitId = pSttF->commitID;
  int32_t   n = 0;

  for (n = 0; n < pSet->nSttF; ++n) {
    pSttF = pSet->aSttF[n];
    ASSERT(pSttF);
    if (pSttF->commitID != commitId) {
      break;
    }
    if (unRef) {
      nRef = atomic_sub_fetch_32(&pSttF->nRef, 1);
      if (nRef == 0) {
        tsdbSttFileName(pTsdb, pSttF->diskId, pSet->fid, pSttF, fname);
        (void)taosRemoveFile(fname);
      }
    }

    taosMemoryFree(pSttF);
  }
  memmove(&pSet->aSttF, &pSet->aSttF[n], POINTER_BYTES * n);
  int32_t j = n;
  while (j++ < pSet->nSttF) {
    pSet->aSttF[j] = NULL;
  }
  pSet->nSttF -= n;

  return code;
}
#endif
static int32_t tsdbRemoveHeadSttF(STsdb *pTsdb, SDFileSet *pSet, bool unRef, int32_t *index) {
  int32_t code = 0;
  int32_t nRef = 0;
  char    fname[TSDB_FILENAME_LEN] = {0};

  ASSERT(pSet->nSttF > 0);

  SSttFile *pSttF = pSet->aSttF[0];
  int64_t   commitId = pSttF->commitID;
  int32_t   n = 0;

  for (n = 0; n < pSet->nSttF; ++n) {
    pSttF = pSet->aSttF[n];
    ASSERT(pSttF);
    if (pSttF->commitID != commitId) {
      break;
    }
    if (unRef) {
      nRef = atomic_sub_fetch_32(&pSttF->nRef, 1);
      if (nRef == 0) {
        tsdbSttFileName(pTsdb, pSttF->diskId, pSet->fid, pSttF, fname);
        (void)taosRemoveFile(fname);
      }
    }

    taosMemoryFree(pSttF);
  }
  if (index) *index = n;

  return code;
}

static int32_t tsdbMergeFileSet(STsdb *pTsdb, SDFileSet *pSetOld, SDFileSet *pSetNew) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t nRef = 0;
  char    fname[TSDB_FILENAME_LEN] = {0};

  // head
  SHeadFile *pHeadF = pSetOld->pHeadF;
  if (pHeadF->commitID == pSetNew->pHeadF->commitID) {
    if (pSetNew->action == TD_ACT_ADD) {  // from migrate
      pSetOld->pHeadF = (SHeadFile *)taosMemoryMalloc(sizeof(SHeadFile));
      if (pSetOld->pHeadF == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        TSDB_CHECK_CODE(code, lino, _exit);
      }
      *pSetOld->pHeadF = *pSetNew->pHeadF;
      pSetOld->pHeadF->nRef = 1;

      nRef = atomic_sub_fetch_32(&pHeadF->nRef, 1);
      if (nRef == 0) {
        tsdbHeadFileName(pTsdb, pSetOld->diskId, pSetOld->fid, pHeadF, fname);
        (void)taosRemoveFile(fname);
        taosMemoryFree(pHeadF);
      }
    } else if (pSetNew->action == TD_ACT_NO) {
      ASSERTS(pHeadF->offset == pSetNew->pHeadF->offset,
              "headf: fid %d commitID %" PRIi64 " offset %" PRIi64 " != %" PRIi64, pSetNew->fid, pHeadF->commitID,
              pHeadF->offset, pSetNew->pHeadF->offset);
      ASSERTS(pHeadF->size == pSetNew->pHeadF->size, "headf: fid %d commitID %" PRIi64 " size %" PRIi64 " != %" PRIi64,
              pSetNew->fid, pHeadF->commitID, pHeadF->size, pSetNew->pHeadF->size);
    } else {
      ASSERTS(0, "headf: fid %d commitID %" PRIi64 " invalid action %" PRIi8, pSetNew->fid, pHeadF->commitID,
              pSetNew->action);
    }
  } else if (pHeadF->commitID < pSetNew->pHeadF->commitID) {  // from compact/merge
    pSetOld->pHeadF = (SHeadFile *)taosMemoryMalloc(sizeof(SHeadFile));
    if (pSetOld->pHeadF == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    *pSetOld->pHeadF = *pSetNew->pHeadF;
    pSetOld->pHeadF->nRef = 1;

    nRef = atomic_sub_fetch_32(&pHeadF->nRef, 1);
    if (nRef == 0) {
      tsdbHeadFileName(pTsdb, pSetOld->diskId, pSetOld->fid, pHeadF, fname);
      (void)taosRemoveFile(fname);
      taosMemoryFree(pHeadF);
    }
  } else {
    ASSERTS(pSetNew->action == TD_ACT_NO, "headf: fid %d commitID %" PRIi64 " invalid action %" PRIi8, pSetNew->fid,
            pHeadF->commitID, pSetNew->action);
  }

  // data
  SDataFile *pDataF = pSetOld->pDataF;
  if (pDataF->commitID == pSetNew->pDataF->commitID) {
    if (pSetNew->action == TD_ACT_ADD) {  // from migrate
      pSetOld->pDataF = (SDataFile *)taosMemoryMalloc(sizeof(SDataFile));
      if (pSetOld->pDataF == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        TSDB_CHECK_CODE(code, lino, _exit);
      }
      *pSetOld->pDataF = *pSetNew->pDataF;
      pSetOld->pDataF->nRef = 1;

      nRef = atomic_sub_fetch_32(&pDataF->nRef, 1);
      if (nRef == 0) {
        tsdbDataFileName(pTsdb, pSetOld->diskId, pSetOld->fid, pDataF, fname);
        (void)taosRemoveFile(fname);
        taosMemoryFree(pDataF);
      }
    } else if (pSetNew->action == TD_ACT_NO) {
      ASSERTS(pDataF->size == pSetNew->pDataF->size, "dataf: fid %d commitID %" PRIi64 " size %" PRIi64 " != %" PRIi64,
              pSetNew->fid, pDataF->commitID, pDataF->size, pSetNew->pDataF->size);
    } else {
      ASSERTS(0, "dataf: fid %d commitID %" PRIi64 " invalid action %" PRIi8, pSetNew->fid, pDataF->commitID,
              pSetNew->action);
    }
  } else if (pDataF->commitID < pSetNew->pDataF->commitID) {
    pSetOld->pDataF = (SDataFile *)taosMemoryMalloc(sizeof(SDataFile));
    if (pSetOld->pDataF == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    *pSetOld->pDataF = *pSetNew->pDataF;
    pSetOld->pDataF->nRef = 1;

    nRef = atomic_sub_fetch_32(&pDataF->nRef, 1);
    if (nRef == 0) {
      tsdbDataFileName(pTsdb, pSetOld->diskId, pSetOld->fid, pDataF, fname);
      (void)taosRemoveFile(fname);
      taosMemoryFree(pDataF);
    }
  } else {
    ASSERTS(pSetNew->action == TD_ACT_NO, "dataf: fid %d commitID %" PRIi64 " invalid action %" PRIi8, pSetNew->fid,
            pDataF->commitID, pSetNew->action);
  }

  // sma
  SSmaFile *pSmaF = pSetOld->pSmaF;
  if (pSmaF->commitID == pSetNew->pSmaF->commitID) {
    if (pSetNew->action == TD_ACT_ADD) {  // from migrate
      pSetOld->pSmaF = (SSmaFile *)taosMemoryMalloc(sizeof(SSmaFile));
      if (pSetOld->pSmaF == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        TSDB_CHECK_CODE(code, lino, _exit);
      }
      *pSetOld->pSmaF = *pSetNew->pSmaF;
      pSetOld->pSmaF->nRef = 1;

      nRef = atomic_sub_fetch_32(&pSmaF->nRef, 1);
      if (nRef == 0) {
        tsdbSmaFileName(pTsdb, pSetOld->diskId, pSetOld->fid, pSmaF, fname);
        (void)taosRemoveFile(fname);
        taosMemoryFree(pSmaF);
      }
    } else if (pSetNew->action == TD_ACT_NO) {
      ASSERTS(pSmaF->size == pSetNew->pSmaF->size, "smaf: fid %d commitID %" PRIi64 " size %" PRIi64 " != %" PRIi64,
              pSetNew->fid, pSmaF->commitID, pSmaF->size, pSetNew->pSmaF->size);
    } else {
      ASSERTS(0, "smaf: fid %d commitID %" PRIi64 " invalid action %" PRIi8, pSetNew->fid, pSmaF->commitID,
              pSetNew->action);
    }
  } else if (pSmaF->commitID < pSetNew->pSmaF->commitID) {
    pSetOld->pSmaF = (SSmaFile *)taosMemoryMalloc(sizeof(SSmaFile));
    if (pSetOld->pSmaF == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    *pSetOld->pSmaF = *pSetNew->pSmaF;
    pSetOld->pSmaF->nRef = 1;

    nRef = atomic_sub_fetch_32(&pSmaF->nRef, 1);
    if (nRef == 0) {
      tsdbSmaFileName(pTsdb, pSetOld->diskId, pSetOld->fid, pSmaF, fname);
      (void)taosRemoveFile(fname);
      taosMemoryFree(pSmaF);
    }
  } else {
    ASSERTS(pSetNew->action == TD_ACT_NO, "smaf: fid %d commitID %" PRIi64 " invalid action %" PRIi8, pSetNew->fid,
            pSmaF->commitID, pSetNew->action);
  }

  // stt
#if 0
  if (sameDisk) {
    if (pSetNew->nSttF > pSetOld->nSttF) {
      ASSERT(pSetNew->nSttF == pSetOld->nSttF + 1);
      pSetOld->aSttF[pSetOld->nSttF] = (SSttFile *)taosMemoryMalloc(sizeof(SSttFile));
      if (pSetOld->aSttF[pSetOld->nSttF] == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        TSDB_CHECK_CODE(code, lino, _exit);
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
          (void)taosRemoveFile(fname);
          taosMemoryFree(pSttFile);
        }
        pSetOld->aSttF[iStt] = NULL;
      }

      pSetOld->nSttF = 1;
      pSetOld->aSttF[0] = (SSttFile *)taosMemoryMalloc(sizeof(SSttFile));
      if (pSetOld->aSttF[0] == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        TSDB_CHECK_CODE(code, lino, _exit);
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
            (void)taosRemoveFile(fname);
            taosMemoryFree(pSttFile);
          }

          pSetOld->aSttF[iStt] = (SSttFile *)taosMemoryMalloc(sizeof(SSttFile));
          if (pSetOld->aSttF[iStt] == NULL) {
            code = TSDB_CODE_OUT_OF_MEMORY;
            TSDB_CHECK_CODE(code, lino, _exit);
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
    for (int32_t iStt = 0; iStt < pSetOld->nSttF; iStt++) {
      SSttFile *pSttFile = pSetOld->aSttF[iStt];
      nRef = atomic_sub_fetch_32(&pSttFile->nRef, 1);
      if (nRef == 0) {
        tsdbSttFileName(pTsdb, pSetOld->diskId, pSetOld->fid, pSttFile, fname);
        (void)taosRemoveFile(fname);
        taosMemoryFree(pSttFile);
      }
    }

    pSetOld->nSttF = 0;
    for (int32_t iStt = 0; iStt < pSetNew->nSttF; iStt++) {
      pSetOld->aSttF[iStt] = (SSttFile *)taosMemoryMalloc(sizeof(SSttFile));
      if (pSetOld->aSttF[iStt] == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      *pSetOld->aSttF[iStt] = *pSetNew->aSttF[iStt];
      pSetOld->aSttF[iStt]->nRef = 1;

      pSetOld->nSttF++;
    }
  }
#endif
  ASSERT(pSetNew->nSttF > 0 && pSetNew->nSttF <= TSDB_MAX_STT_TRIGGER);
  ASSERT(pSetOld->nSttF > 0 && pSetOld->nSttF <= TSDB_MAX_STT_TRIGGER);
  int8_t newSttAct = pSetNew->aSttF[pSetNew->nSttF - 1]->action;
  int8_t oldSttAct = pSetOld->aSttF[pSetOld->nSttF - 1]->action;
  ASSERT(newSttAct >= TD_ACT_NO && newSttAct < TD_ACT_MAX);
  ASSERT(oldSttAct >= TD_ACT_NO && oldSttAct < TD_ACT_MAX);

  SSttFile *aSttMerge[TSDB_MAX_STT_TRIGGER] = {0};

  SSttFile *pSttFOld = pSetOld->aSttF[0];
  SSttFile *pSttFNew = pSetNew->aSttF[0];

  int32_t iOld = 0;
  int32_t iNew = 0;
  int32_t iMerge = 0;

  if (pSttFOld->commitID < pSttFNew->commitID) {
    // migrate/commit, then compact/merge, remove the 1st group with the same commitID in pSttFOld
    // compact/merge, then compact, remove the 1st  group with the same commitID in pSttFOld
    ASSERT(pSetNew->action == TD_ACT_ADD);
    tsdbRemoveHeadSttF(pTsdb, pSetOld, true, &iOld);
  } else if (pSttFOld->commitID > pSttFNew->commitID) {
    // compact/merge, then commit, remove the 1st group with the same commitId in pSttFNew
    // compact/merge,
    ASSERT(pSetNew->action == TD_ACT_NO);
    tsdbRemoveHeadSttF(pTsdb, pSetNew, false, &iNew);
  }

  while (true) {
    int32_t nOld = pSetOld->nSttF;
    int32_t nNew = pSetNew->nSttF;

    if (iOld >= nOld && iNew >= nNew) break;

    SSttFile *pSttFOld = (iOld < nOld) ? pSetOld->aSttF[iOld] : NULL;
    SSttFile *pSttFNew = (iNew < nNew) ? pSetNew->aSttF[iNew] : NULL;

    if (pSttFOld && pSttFNew && pSttFOld->commitID == pSttFNew->commitID) {
      if (pSttFNew->action == TD_ACT_ADD) {  // migrate
        nRef = atomic_sub_fetch_32(&pSttFOld->nRef, 1);
        if (nRef == 0) {
          tsdbSttFileName(pTsdb, pSetOld->diskId, pSetOld->fid, pSttFOld, fname);
          (void)taosRemoveFile(fname);
        }
        taosMemoryFree(pSttFOld);
        ++iOld;
        ++iNew;
        // save to merge
        aSttMerge[iMerge++] = pSttFNew;
        aSttMerge[iMerge]->nRef = 1;
        aSttMerge[iMerge]->action = TD_ACT_NO;
      } else if (pSttFNew->action == TD_ACT_NO) {  // commit
        // keep old, free new
        taosMemoryFree(pSttFNew);
        // save to merge
        aSttMerge[iMerge++] = pSttFOld;
        aSttMerge[iMerge]->action = TD_ACT_NO;
      } else {
        ASSERT(0);
      }
      continue;
    }

    if (pSttFOld && pSttFNew) {
      if (pSttFOld->commitID < pSttFNew->commitID) {
        aSttMerge[iMerge++] = pSttFOld;
        aSttMerge[iMerge]->action = TD_ACT_NO;
        ++iOld;
      } else if (pSttFOld->commitID > pSttFNew->commitID) {
        aSttMerge[iMerge++] = pSttFNew;
        aSttMerge[iMerge]->nRef = 1;
        aSttMerge[iMerge]->action = TD_ACT_NO;
        ++iNew;
      } else {
        ASSERT(0);
      }
    } else if (pSttFOld) {
      aSttMerge[iMerge++] = pSttFOld;
      aSttMerge[iMerge]->action = TD_ACT_NO;
      ++iOld;
    } else if (pSttFNew) {
      aSttMerge[iMerge++] = pSttFNew;
      aSttMerge[iMerge]->nRef = 1;
      aSttMerge[iMerge]->action = TD_ACT_NO;
      ++iNew;
    }
  }

  // assign the value
  pSetOld->nSttF = iMerge;
  ASSERT(iMerge > 0 && iMerge <= TSDB_MAX_STT_TRIGGER);
  memcpy(pSetOld->aSttF, aSttMerge, POINTER_BYTES * TSDB_MAX_STT_TRIGGER);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbFSApplyChange(STsdb *pTsdb, STsdbFS *pFS) {
  int32_t code = 0;
  int32_t lino = 0;

  int32_t nRef = 0;
  char    fname[TSDB_FILENAME_LEN] = {0};

  // SDelFile
  if (pFS->pDelFile) {
    SDelFile *pDelFile = pTsdb->fs.pDelFile;

    if (pDelFile == NULL || (pDelFile->commitID != pFS->pDelFile->commitID)) {
      pTsdb->fs.pDelFile = (SDelFile *)taosMemoryMalloc(sizeof(SDelFile));
      if (pTsdb->fs.pDelFile == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      *pTsdb->fs.pDelFile = *pFS->pDelFile;
      pTsdb->fs.pDelFile->nRef = 1;

      if (pDelFile) {
        nRef = atomic_sub_fetch_32(&pDelFile->nRef, 1);
        if (nRef == 0) {
          tsdbDelFileName(pTsdb, pDelFile, fname);
          (void)taosRemoveFile(fname);
          taosMemoryFree(pDelFile);
        }
      }
    }
  } else {
    if (pTsdb->fs.pDelFile) {
      nRef = atomic_sub_fetch_32(&pTsdb->fs.pDelFile->nRef, 1);
      if (nRef == 0) {
        tsdbDelFileName(pTsdb, pTsdb->fs.pDelFile, fname);
        (void)taosRemoveFile(fname);
        taosMemoryFree(pTsdb->fs.pDelFile);
      }
      pTsdb->fs.pDelFile = NULL;
    }
  }

  // aDFileSet
  int32_t iOld = 0;
  int32_t iNew = 0;
  while (true) {
    int32_t   nOld = taosArrayGetSize(pTsdb->fs.aDFileSet);
    int32_t   nNew = taosArrayGetSize(pFS->aDFileSet);
    SDFileSet fSet = {0};
    int8_t    sameDisk = 0;

    if (iOld >= nOld && iNew >= nNew) break;

    SDFileSet *pSetOld = (iOld < nOld) ? taosArrayGet(pTsdb->fs.aDFileSet, iOld) : NULL;
    SDFileSet *pSetNew = (iNew < nNew) ? taosArrayGet(pFS->aDFileSet, iNew) : NULL;

    if (pSetOld && pSetNew) {
      if (pSetOld->fid == pSetNew->fid) {
        code = tsdbMergeFileSet(pTsdb, pSetOld, pSetNew);
        TSDB_CHECK_CODE(code, lino, _exit);

        iOld++;
        iNew++;
      } else if (pSetOld->fid < pSetNew->fid) {
        code = tsdbRemoveFileSet(pTsdb, pSetOld);
        TSDB_CHECK_CODE(code, lino, _exit);
        taosArrayRemove(pTsdb->fs.aDFileSet, iOld);
      } else if (pSetNew->action == TD_ACT_ADD) {
        code = tsdbNewFileSet(pTsdb, &fSet, pSetNew);
        TSDB_CHECK_CODE(code, lino, _exit)

        if (taosArrayInsert(pTsdb->fs.aDFileSet, iOld, &fSet) == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          TSDB_CHECK_CODE(code, lino, _exit);
        }

        iOld++;
        iNew++;
      } else if (pSetNew->action == TD_ACT_NO) {
        // skip the pSetNew since already removed
        iNew++;
      } else {
        ASSERTS(0, "%d:%d invalid pSetNew action %" PRIi8, pSetOld->fid, pSetNew->fid, pSetNew->action);
        iNew++;
      }
    } else if (pSetOld) {
      code = tsdbRemoveFileSet(pTsdb, pSetOld);
      TSDB_CHECK_CODE(code, lino, _exit);
      taosArrayRemove(pTsdb->fs.aDFileSet, iOld);
    } else if (pSetNew->action == TD_ACT_ADD) {
      code = tsdbNewFileSet(pTsdb, &fSet, pSetNew);
      TSDB_CHECK_CODE(code, lino, _exit)

      if (taosArrayInsert(pTsdb->fs.aDFileSet, iOld, &fSet) == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      iOld++;
      iNew++;
    } else if (pSetNew->action == TD_ACT_NO) {
      // skip the pSetNew since already removed
      iNew++;
    } else {
      ASSERTS(0, "%d:%d invalid pSetNew action %" PRIi8, INT32_MIN, pSetNew->fid, pSetNew->action);
      iNew++;
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

// EXPOSED APIS ====================================================================================
int32_t tsdbFSCommit(STsdb *pTsdb, int8_t type) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdbFS fs = {0};

  char current[TSDB_FILENAME_LEN] = {0};
  char current_t[TSDB_FILENAME_LEN] = {0};
  tsdbGetCurrentFName(pTsdb, current, current_t, type);

  if (!taosCheckExistFile(current_t)) goto _exit;

  // rename the file
  if (taosRenameFile(current_t, current) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // Load the new FS
  code = tsdbFSCreate(&fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbLoadFSFromFile(current, &fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  // apply file change
  code = tsdbFSApplyChange(pTsdb, &fs);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  tsdbFSDestroy(&fs);
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbFSRollback(STsdb *pTsdb, int8_t type) {
  int32_t code = 0;
  int32_t lino = 0;

  char current_t[TSDB_FILENAME_LEN] = {0};
  tsdbGetCurrentFName(pTsdb, NULL, current_t, type);
  (void)taosRemoveFile(current_t);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(errno));
  }
  return code;
}

int32_t tsdbFSOpen(STsdb *pTsdb, int8_t rollback) {
  int32_t code = 0;
  int32_t lino = 0;
  SVnode *pVnode = pTsdb->pVnode;

  // open handle
  code = tsdbFSCreate(&pTsdb->fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  // open impl
  char current[TSDB_FILENAME_LEN] = {0};
  char current_t[TSDB_FILENAME_LEN] = {0};
  tsdbGetCurrentFName(pTsdb, current, current_t, VND_TASK_MAX);

  if (taosCheckExistFile(current)) {
    code = tsdbLoadFSFromFile(current, &pTsdb->fs);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (taosCheckExistFile(current_t)) {
      code = tsdbFSCommit(pTsdb, VND_TASK_MAX);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    tsdbGetCurrentFName(pTsdb, current, current_t, VND_TASK_COMMIT);
    if (taosCheckExistFile(current_t)) {
      if (rollback) {
        code = tsdbFSRollback(pTsdb, VND_TASK_COMMIT);
        TSDB_CHECK_CODE(code, lino, _exit);
      } else {
        code = tsdbFSCommit(pTsdb, VND_TASK_COMMIT);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }
  } else {
    // empty one
    code = tsdbSaveFSToFile(&pTsdb->fs, current);
    TSDB_CHECK_CODE(code, lino, _exit);

    ASSERT(!rollback);
  }

  // scan and fix FS
  code = tsdbScanAndTryFixFS(pTsdb);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
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

    // sma
    ASSERT(pSet->pSmaF->nRef == 1);
    taosMemoryFree(pSet->pSmaF);

    // stt
    for (int32_t iStt = 0; iStt < pSet->nSttF; iStt++) {
      ASSERT(pSet->aSttF[iStt]->nRef == 1);
      taosMemoryFree(pSet->aSttF[iStt]);
    }
  }

  taosArrayDestroy(pTsdb->fs.aDFileSet);

  return code;
}

int32_t tsdbFSCopy(STsdb *pTsdb, STsdbFS *pFS) {
  int32_t code = 0;
  int32_t lino = 0;
  uint8_t nMaxSttF = 0;

  // lock
  taosThreadRwlockRdlock(&pTsdb->rwLock);

  pFS->pDelFile = NULL;
  if (pFS->aDFileSet) {
    taosArrayClear(pFS->aDFileSet);
  } else {
    pFS->aDFileSet = taosArrayInit(taosArrayGetSize(pTsdb->fs.aDFileSet), sizeof(SDFileSet));
    if (pFS->aDFileSet == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  if (pTsdb->fs.pDelFile) {
    pFS->pDelFile = (SDelFile *)taosMemoryMalloc(sizeof(SDelFile));
    if (pFS->pDelFile == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    *pFS->pDelFile = *pTsdb->fs.pDelFile;
  }

  for (int32_t iSet = 0; iSet < taosArrayGetSize(pTsdb->fs.aDFileSet); iSet++) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(pTsdb->fs.aDFileSet, iSet);
    SDFileSet  fSet = {.diskId = pSet->diskId, .fid = pSet->fid, .action = pSet->action};

    // head
    fSet.pHeadF = (SHeadFile *)taosMemoryMalloc(sizeof(SHeadFile));
    if (fSet.pHeadF == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    *fSet.pHeadF = *pSet->pHeadF;

    // data
    fSet.pDataF = (SDataFile *)taosMemoryMalloc(sizeof(SDataFile));
    if (fSet.pDataF == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    *fSet.pDataF = *pSet->pDataF;

    // sma
    fSet.pSmaF = (SSmaFile *)taosMemoryMalloc(sizeof(SSmaFile));
    if (fSet.pSmaF == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    *fSet.pSmaF = *pSet->pSmaF;

    // stt
    for (fSet.nSttF = 0; fSet.nSttF < pSet->nSttF; fSet.nSttF++) {
      fSet.aSttF[fSet.nSttF] = (SSttFile *)taosMemoryMalloc(sizeof(SSttFile));
      if (fSet.aSttF[fSet.nSttF] == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        TSDB_CHECK_CODE(code, lino, _exit);
      }
      *fSet.aSttF[fSet.nSttF] = *pSet->aSttF[fSet.nSttF];
    }
    if (pSet->nSttF > nMaxSttF) nMaxSttF = pSet->nSttF;

    if (taosArrayPush(pFS->aDFileSet, &fSet) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  taosThreadRwlockUnlock(&pTsdb->rwLock);
  pFS->nMaxSttF = nMaxSttF;
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbFSUpsertDelFile(STsdbFS *pFS, SDelFile *pDelFile) {
  int32_t code = 0;

  if (pDelFile) {
    if (pFS->pDelFile == NULL) {
      pFS->pDelFile = (SDelFile *)taosMemoryMalloc(sizeof(SDelFile));
      if (pFS->pDelFile == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _exit;
      }
    }
    *pFS->pDelFile = *pDelFile;
  } else {
    if (pFS->pDelFile) {
      taosMemoryFree(pFS->pDelFile);
      pFS->pDelFile = NULL;
    }
  }

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
      *pDFileSet->pSmaF = *pSet->pSmaF;
      // stt
      if (pSet->nSttF > pDFileSet->nSttF) {
        ASSERT(pSet->nSttF == pDFileSet->nSttF + 1);

        pDFileSet->aSttF[pDFileSet->nSttF] = (SSttFile *)taosMemoryMalloc(sizeof(SSttFile));
        if (pDFileSet->aSttF[pDFileSet->nSttF] == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          goto _exit;
        }
        *pDFileSet->aSttF[pDFileSet->nSttF] = *pSet->aSttF[pSet->nSttF - 1];
        pDFileSet->nSttF++;
      } else if (pSet->nSttF < pDFileSet->nSttF) {
        // ASSERT(pSet->nSttF == 1);
        for (int32_t iStt = 1; iStt < pDFileSet->nSttF; iStt++) {
          taosMemoryFree(pDFileSet->aSttF[iStt]);
        }

        *pDFileSet->aSttF[0] = *pSet->aSttF[0];
        pDFileSet->nSttF = 1;
      } else {
        for (int32_t iStt = 0; iStt < pSet->nSttF; iStt++) {
          *pDFileSet->aSttF[iStt] = *pSet->aSttF[iStt];
        }
      }

      pDFileSet->diskId = pSet->diskId;
      goto _exit;
    }
  }

  ASSERT(pSet->nSttF == 1);
  SDFileSet fSet = {.diskId = pSet->diskId, .fid = pSet->fid, .nSttF = 1};

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

  // sma
  fSet.pSmaF = (SSmaFile *)taosMemoryMalloc(sizeof(SSmaFile));
  if (fSet.pSmaF == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  *fSet.pSmaF = *pSet->pSmaF;

  // stt
  fSet.aSttF[0] = (SSttFile *)taosMemoryMalloc(sizeof(SSttFile));
  if (fSet.aSttF[0] == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  *fSet.aSttF[0] = *pSet->aSttF[0];

  if (taosArrayInsert(pFS->aDFileSet, idx, &fSet) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

_exit:
  return code;
}

int32_t tsdbFSPrepareCommit(STsdb *pTsdb, STsdbFS *pFSNew, int8_t type) {
  int32_t code = 0;
  int32_t lino = 0;
  char    tfname[TSDB_FILENAME_LEN];

  tsdbGetCurrentFName(pTsdb, NULL, tfname, type);

  // gnrt CURRENT.t
  code = tsdbSaveFSToFile(pFSNew, tfname);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
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

    nRef = atomic_fetch_add_32(&pSet->pSmaF->nRef, 1);
    ASSERT(nRef > 0);

    for (int32_t iStt = 0; iStt < pSet->nSttF; iStt++) {
      nRef = atomic_fetch_add_32(&pSet->aSttF[iStt]->nRef, 1);
      ASSERT(nRef > 0);
    }

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
      (void)taosRemoveFile(fname);
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
      (void)taosRemoveFile(fname);
      taosMemoryFree(pSet->pHeadF);
    }

    // data
    nRef = atomic_sub_fetch_32(&pSet->pDataF->nRef, 1);
    ASSERT(nRef >= 0);
    if (nRef == 0) {
      tsdbDataFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pDataF, fname);
      (void)taosRemoveFile(fname);
      taosMemoryFree(pSet->pDataF);
    }

    // sma
    nRef = atomic_sub_fetch_32(&pSet->pSmaF->nRef, 1);
    ASSERT(nRef >= 0);
    if (nRef == 0) {
      tsdbSmaFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pSmaF, fname);
      (void)taosRemoveFile(fname);
      taosMemoryFree(pSet->pSmaF);
    }

    // stt
    for (int32_t iStt = 0; iStt < pSet->nSttF; iStt++) {
      nRef = atomic_sub_fetch_32(&pSet->aSttF[iStt]->nRef, 1);
      ASSERT(nRef >= 0);
      if (nRef == 0) {
        tsdbSttFileName(pTsdb, pSet->diskId, pSet->fid, pSet->aSttF[iStt], fname);
        (void)taosRemoveFile(fname);
        taosMemoryFree(pSet->aSttF[iStt]);
        /* code */
      }
    }
  }

  taosArrayDestroy(pFS->aDFileSet);
}