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

static int32_t tPutHeadFile(uint8_t *p, SHeadFile *pHeadFile) {
  int32_t n = 0;

  n += tPutI64v(p ? p + n : p, pHeadFile->commitID);
  n += tPutI64v(p ? p + n : p, pHeadFile->size);
  n += tPutI64v(p ? p + n : p, pHeadFile->offset);

  return n;
}

static int32_t tGetHeadFile(uint8_t *p, SHeadFile *pHeadFile) {
  int32_t n = 0;

  n += tGetI64v(p + n, &pHeadFile->commitID);
  n += tGetI64v(p + n, &pHeadFile->size);
  n += tGetI64v(p + n, &pHeadFile->offset);

  return n;
}

static int32_t tPutDataFile(uint8_t *p, SDataFile *pDataFile) {
  int32_t n = 0;

  n += tPutI64v(p ? p + n : p, pDataFile->commitID);
  n += tPutI64v(p ? p + n : p, pDataFile->size);

  return n;
}

static int32_t tGetDataFile(uint8_t *p, SDataFile *pDataFile) {
  int32_t n = 0;

  n += tGetI64v(p + n, &pDataFile->commitID);
  n += tGetI64v(p + n, &pDataFile->size);

  return n;
}

static int32_t tPutLastFile(uint8_t *p, SLastFile *pLastFile) {
  int32_t n = 0;

  n += tPutI64v(p ? p + n : p, pLastFile->commitID);
  n += tPutI64v(p ? p + n : p, pLastFile->size);

  return n;
}

static int32_t tGetLastFile(uint8_t *p, SLastFile *pLastFile) {
  int32_t n = 0;

  n += tGetI64v(p + n, &pLastFile->commitID);
  n += tGetI64v(p + n, &pLastFile->size);

  return n;
}

static int32_t tPutSmaFile(uint8_t *p, SSmaFile *pSmaFile) {
  int32_t n = 0;

  n += tPutI64v(p ? p + n : p, pSmaFile->commitID);
  n += tPutI64v(p ? p + n : p, pSmaFile->size);

  return n;
}

static int32_t tGetSmaFile(uint8_t *p, SSmaFile *pSmaFile) {
  int32_t n = 0;

  n += tGetI64v(p + n, &pSmaFile->commitID);
  n += tGetI64v(p + n, &pSmaFile->size);

  return n;
}

// EXPOSED APIS ==================================================
void tsdbDataFileName(STsdb *pTsdb, SDFileSet *pDFileSet, EDataFileT ftype, char fname[]) {
  STfs *pTfs = pTsdb->pVnode->pTfs;

  switch (ftype) {
    case TSDB_HEAD_FILE:
      snprintf(fname, TSDB_FILENAME_LEN - 1, "%s%s%s%sv%df%dver%" PRId64 "%s", tfsGetDiskPath(pTfs, pDFileSet->diskId),
               TD_DIRSEP, pTsdb->path, TD_DIRSEP, TD_VID(pTsdb->pVnode), pDFileSet->fid, pDFileSet->fHead.commitID,
               ".head");
      break;
    case TSDB_DATA_FILE:
      snprintf(fname, TSDB_FILENAME_LEN - 1, "%s%s%s%sv%df%dver%" PRId64 "%s", tfsGetDiskPath(pTfs, pDFileSet->diskId),
               TD_DIRSEP, pTsdb->path, TD_DIRSEP, TD_VID(pTsdb->pVnode), pDFileSet->fid, pDFileSet->fData.commitID,
               ".data");
      break;
    case TSDB_LAST_FILE:
      snprintf(fname, TSDB_FILENAME_LEN - 1, "%s%s%s%sv%df%dver%" PRId64 "%s", tfsGetDiskPath(pTfs, pDFileSet->diskId),
               TD_DIRSEP, pTsdb->path, TD_DIRSEP, TD_VID(pTsdb->pVnode), pDFileSet->fid, pDFileSet->fLast.commitID,
               ".last");
      break;
    case TSDB_SMA_FILE:
      snprintf(fname, TSDB_FILENAME_LEN - 1, "%s%s%s%sv%df%dver%" PRId64 "%s", tfsGetDiskPath(pTfs, pDFileSet->diskId),
               TD_DIRSEP, pTsdb->path, TD_DIRSEP, TD_VID(pTsdb->pVnode), pDFileSet->fid, pDFileSet->fSma.commitID,
               ".sma");
      break;
    default:
      ASSERT(0);
      break;
  }
}

bool tsdbFileIsSame(SDFileSet *pDFileSet1, SDFileSet *pDFileSet2, EDataFileT ftype) {
  if (pDFileSet1->diskId.level != pDFileSet2->diskId.level || pDFileSet1->diskId.id != pDFileSet2->diskId.id) {
    return false;
  }

  switch (ftype) {
    case TSDB_HEAD_FILE:
      return pDFileSet1->fHead.commitID == pDFileSet2->fHead.commitID;
    case TSDB_DATA_FILE:
      return pDFileSet1->fData.commitID == pDFileSet2->fData.commitID;
    case TSDB_LAST_FILE:
      return pDFileSet1->fLast.commitID == pDFileSet2->fLast.commitID;
    case TSDB_SMA_FILE:
      return pDFileSet1->fSma.commitID == pDFileSet2->fSma.commitID;
    default:
      ASSERT(0);
      break;
  }
}

int32_t tsdbUpdateDFileHdr(TdFilePtr pFD, SDFileSet *pSet, EDataFileT ftype) {
  int32_t code = 0;
  int64_t n;
  char    hdr[TSDB_FHDR_SIZE];

  memset(hdr, 0, TSDB_FHDR_SIZE);
  tPutDataFileHdr(hdr, pSet, ftype);
  taosCalcChecksumAppend(0, hdr, TSDB_FHDR_SIZE);

  n = taosLSeekFile(pFD, 0, SEEK_SET);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _exit;
  }

  n = taosWriteFile(pFD, hdr, TSDB_FHDR_SIZE);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _exit;
  }

_exit:
  return code;
}

int32_t tsdbDFileRollback(STsdb *pTsdb, SDFileSet *pSet, EDataFileT ftype) {
  int32_t   code = 0;
  int64_t   size;
  TdFilePtr pFD;
  char      fname[TSDB_FILENAME_LEN];

  tsdbDataFileName(pTsdb, pSet, ftype, fname);

  // open
  pFD = taosOpenFile(fname, TD_FILE_WRITE);
  if (pFD == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // truncate
  switch (ftype) {
    case TSDB_HEAD_FILE:
      size = pSet->fHead.size;
      break;
    case TSDB_DATA_FILE:
      size = pSet->fData.size;
      break;
    case TSDB_LAST_FILE:
      size = pSet->fLast.size;
      break;
    case TSDB_SMA_FILE:
      size = pSet->fSma.size;
      break;
    default:
      ASSERT(0);
  }
  if (taosFtruncateFile(pFD, size) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // update header
  code = tsdbUpdateDFileHdr(pFD, pSet, ftype);
  if (code) goto _err;

  // sync
  if (taosFsyncFile(pFD) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  // close
  taosCloseFile(&pFD);

  return code;

_err:
  return code;
}

int32_t tPutDataFileHdr(uint8_t *p, SDFileSet *pSet, EDataFileT ftype) {
  int32_t n = 0;

  switch (ftype) {
    case TSDB_HEAD_FILE:
      n += tPutHeadFile(p ? p + n : p, &pSet->fHead);
      break;
    case TSDB_DATA_FILE:
      n += tPutDataFile(p ? p + n : p, &pSet->fData);
      break;
    case TSDB_LAST_FILE:
      n += tPutLastFile(p ? p + n : p, &pSet->fLast);
      break;
    case TSDB_SMA_FILE:
      n += tPutSmaFile(p ? p + n : p, &pSet->fSma);
      break;
    default:
      ASSERT(0);
  }

  return n;
}

int32_t tPutDFileSet(uint8_t *p, SDFileSet *pSet) {
  int32_t n = 0;

  n += tPutI32v(p ? p + n : p, pSet->diskId.level);
  n += tPutI32v(p ? p + n : p, pSet->diskId.id);
  n += tPutI32v(p ? p + n : p, pSet->fid);
  n += tPutHeadFile(p ? p + n : p, &pSet->fHead);
  n += tPutDataFile(p ? p + n : p, &pSet->fData);
  n += tPutLastFile(p ? p + n : p, &pSet->fLast);
  n += tPutSmaFile(p ? p + n : p, &pSet->fSma);

  return n;
}

int32_t tGetDFileSet(uint8_t *p, SDFileSet *pSet) {
  int32_t n = 0;

  n += tGetI32v(p + n, &pSet->diskId.level);
  n += tGetI32v(p + n, &pSet->diskId.id);
  n += tGetI32v(p + n, &pSet->fid);
  n += tGetHeadFile(p + n, &pSet->fHead);
  n += tGetDataFile(p + n, &pSet->fData);
  n += tGetLastFile(p + n, &pSet->fLast);
  n += tGetSmaFile(p + n, &pSet->fSma);

  return n;
}

// SDelFile ===============================================
void tsdbDelFileName(STsdb *pTsdb, SDelFile *pFile, char fname[]) {
  STfs *pTfs = pTsdb->pVnode->pTfs;

  snprintf(fname, TSDB_FILENAME_LEN - 1, "%s%s%s%sv%dver%" PRId64 "%s", tfsGetPrimaryPath(pTfs), TD_DIRSEP, pTsdb->path,
           TD_DIRSEP, TD_VID(pTsdb->pVnode), pFile->commitID, ".del");
}

int32_t tPutDelFile(uint8_t *p, SDelFile *pDelFile) {
  int32_t n = 0;

  n += tPutI64v(p ? p + n : p, pDelFile->commitID);
  n += tPutI64v(p ? p + n : p, pDelFile->size);
  n += tPutI64v(p ? p + n : p, pDelFile->offset);

  return n;
}

int32_t tGetDelFile(uint8_t *p, SDelFile *pDelFile) {
  int32_t n = 0;

  n += tGetI64v(p + n, &pDelFile->commitID);
  n += tGetI64v(p + n, &pDelFile->size);
  n += tGetI64v(p + n, &pDelFile->offset);

  return n;
}
