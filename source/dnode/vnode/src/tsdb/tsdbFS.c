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
#include "vnd.h"

// =================================================================================================
static int32_t tsdbFSToBinary(uint8_t *p, STsdbFS *pFS) {
  int32_t  n = 0;
  int8_t   hasDel = pFS->pDelFile ? 1 : 0;
  uint32_t nSet = taosArrayGetSize(pFS->aDFileSet);

  // version
  n += tPutI8(p ? p + n : p, 0);

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

static int32_t tsdbBinaryToFS(uint8_t *pData, int64_t nData, STsdbFS *pFS) {
  int32_t code = 0;
  int32_t n = 0;

  // version
  n += tGetI8(pData + n, NULL);

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

    int32_t nt = tGetDFileSet(pData + n, &fSet);
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
  TdFilePtr pFD = taosOpenFile(fname, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
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
    if (taosStatFile(fname, &size, NULL, NULL)) {
      code = TAOS_SYSTEM_ERROR(errno);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    // if (size != tsdbLogicToFileSize(pTsdb->fs.pDelFile->size, pTsdb->pVnode->config.tsdbPageSize)) {
    //   code = TSDB_CODE_FILE_CORRUPTED;
    //   TSDB_CHECK_CODE(code, lino, _exit);
    // }
  }

  // SArray<SDFileSet>
  int32_t fid = 0;
  for (int32_t iSet = 0; iSet < taosArrayGetSize(pTsdb->fs.aDFileSet); iSet++) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(pTsdb->fs.aDFileSet, iSet);
    fid = pSet->fid;

    // head =========
    tsdbHeadFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pHeadF, fname);
    if (taosStatFile(fname, &size, NULL, NULL)) {
      code = TAOS_SYSTEM_ERROR(errno);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    // if (size != tsdbLogicToFileSize(pSet->pHeadF->size, pTsdb->pVnode->config.tsdbPageSize)) {
    //   code = TSDB_CODE_FILE_CORRUPTED;
    //   TSDB_CHECK_CODE(code, lino, _exit);
    // }

    // data =========
    tsdbDataFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pDataF, fname);
    if (taosStatFile(fname, &size, NULL, NULL)) {
      code = TAOS_SYSTEM_ERROR(errno);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    // if (size < tsdbLogicToFileSize(pSet->pDataF->size, pTsdb->pVnode->config.tsdbPageSize)) {
    //   code = TSDB_CODE_FILE_CORRUPTED;
    //   TSDB_CHECK_CODE(code, lino, _exit);
    // }
    // else if (size > tsdbLogicToFileSize(pSet->pDataF->size, pTsdb->pVnode->config.tsdbPageSize)) {
    //   code = tsdbDFileRollback(pTsdb, pSet, TSDB_DATA_FILE);
    //   TSDB_CHECK_CODE(code, lino, _exit);
    // }

    // sma =============
    tsdbSmaFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pSmaF, fname);
    if (taosStatFile(fname, &size, NULL, NULL)) {
      code = TAOS_SYSTEM_ERROR(errno);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    // if (size < tsdbLogicToFileSize(pSet->pSmaF->size, pTsdb->pVnode->config.tsdbPageSize)) {
    //   code = TSDB_CODE_FILE_CORRUPTED;
    //   TSDB_CHECK_CODE(code, lino, _exit);
    // }
    // else if (size > tsdbLogicToFileSize(pSet->pSmaF->size, pTsdb->pVnode->config.tsdbPageSize)) {
    //   code = tsdbDFileRollback(pTsdb, pSet, TSDB_SMA_FILE);
    //   TSDB_CHECK_CODE(code, lino, _exit);
    // }

    // stt ===========
    for (int32_t iStt = 0; iStt < pSet->nSttF; iStt++) {
      tsdbSttFileName(pTsdb, pSet->diskId, pSet->fid, pSet->aSttF[iStt], fname);
      if (taosStatFile(fname, &size, NULL, NULL)) {
        code = TAOS_SYSTEM_ERROR(errno);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
      // if (size != tsdbLogicToFileSize(pSet->aSttF[iStt]->size, pTsdb->pVnode->config.tsdbPageSize)) {
      //   code = TSDB_CODE_FILE_CORRUPTED;
      //   TSDB_CHECK_CODE(code, lino, _exit);
      // }
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

void tsdbGetCurrentFName(STsdb *pTsdb, char *current, char *current_t) {
  SVnode *pVnode = pTsdb->pVnode;
  int32_t offset = 0;

  // CURRENT
  if (current) {
    vnodeGetPrimaryDir(pTsdb->path, pVnode->diskPrimary, pVnode->pTfs, current, TSDB_FILENAME_LEN);
    offset = strlen(current);
    snprintf(current + offset, TSDB_FILENAME_LEN - offset - 1, "%sCURRENT", TD_DIRSEP);
  }

  // CURRENT.t
  if (current_t) {
    vnodeGetPrimaryDir(pTsdb->path, pVnode->diskPrimary, pVnode->pTfs, current_t, TSDB_FILENAME_LEN);
    offset = strlen(current_t);
    snprintf(current_t + offset, TSDB_FILENAME_LEN - offset - 1, "%sCURRENT.t", TD_DIRSEP);
  }
}

static int32_t load_fs(const char *fname, STsdbFS *pFS) {
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

static int32_t tsdbMergeFileSet(STsdb *pTsdb, SDFileSet *pSetOld, SDFileSet *pSetNew) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t nRef = 0;
  bool    sameDisk = ((pSetOld->diskId.level == pSetNew->diskId.level) && (pSetOld->diskId.id == pSetNew->diskId.id));
  char    fname[TSDB_FILENAME_LEN] = {0};

  // head
  SHeadFile *pHeadF = pSetOld->pHeadF;
  if ((!sameDisk) || (pHeadF->commitID != pSetNew->pHeadF->commitID)) {
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
    ASSERT(pHeadF->offset == pSetNew->pHeadF->offset);
    ASSERT(pHeadF->size == pSetNew->pHeadF->size);
  }

  // data
  SDataFile *pDataF = pSetOld->pDataF;
  if ((!sameDisk) || (pDataF->commitID != pSetNew->pDataF->commitID)) {
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
    pDataF->size = pSetNew->pDataF->size;
  }

  // sma
  SSmaFile *pSmaF = pSetOld->pSmaF;
  if ((!sameDisk) || (pSmaF->commitID != pSetNew->pSmaF->commitID)) {
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
    pSmaF->size = pSetNew->pSmaF->size;
  }

  // stt
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

  if (!sameDisk) {
    pSetOld->diskId = pSetNew->diskId;
  }

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
      } else {
        code = tsdbNewFileSet(pTsdb, &fSet, pSetNew);
        TSDB_CHECK_CODE(code, lino, _exit);

        if (taosArrayInsert(pTsdb->fs.aDFileSet, iOld, &fSet) == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          TSDB_CHECK_CODE(code, lino, _exit);
        }

        iOld++;
        iNew++;
      }
    } else if (pSetOld) {
      code = tsdbRemoveFileSet(pTsdb, pSetOld);
      TSDB_CHECK_CODE(code, lino, _exit);
      taosArrayRemove(pTsdb->fs.aDFileSet, iOld);
    } else {
      code = tsdbNewFileSet(pTsdb, &fSet, pSetNew);
      TSDB_CHECK_CODE(code, lino, _exit);

      if (taosArrayInsert(pTsdb->fs.aDFileSet, iOld, &fSet) == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      iOld++;
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
static int32_t tsdbFSCommit(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdbFS fs = {0};

  char current[TSDB_FILENAME_LEN] = {0};
  char current_t[TSDB_FILENAME_LEN] = {0};
  tsdbGetCurrentFName(pTsdb, current, current_t);

  if (!taosCheckExistFile(current_t)) goto _exit;

  // rename the file
  if (taosRenameFile(current_t, current) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // Load the new FS
  code = tsdbFSCreate(&fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = load_fs(current, &fs);
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

static int32_t tsdbFSRollback(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;

  char current_t[TSDB_FILENAME_LEN] = {0};
  tsdbGetCurrentFName(pTsdb, NULL, current_t);
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
  tsdbGetCurrentFName(pTsdb, current, current_t);

  if (taosCheckExistFile(current)) {
    code = load_fs(current, &pTsdb->fs);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (taosCheckExistFile(current_t)) {
      if (rollback) {
        code = tsdbFSRollback(pTsdb);
        TSDB_CHECK_CODE(code, lino, _exit);
      } else {
        code = tsdbFSCommit(pTsdb);
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
