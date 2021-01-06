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

#include "tsdbMain.h"

#define TSDB_FILE_OPENED(f) ((f)->fd >= 0)
#define TSDB_FILE_SET_CLOSED(f) ((f)->fd = -1)

// ============== Operations on SMFile
void tsdbInitMFile(SMFile *pMFile, int vid, int ver, SMFInfo *pInfo) {
  TSDB_FILE_SET_CLOSED(pMFile);
  if (pInfo == NULL) {
    memset(&(pMFile->info), 0, sizeof(pMFile->info));
    pMFile->info.magic = TSDB_FILE_INIT_MAGIC;
  } else {
    pMFile->info = *pInfo;
  }
  tfsInitFile(&(pMFile->f), TFS_PRIMARY_LEVEL, TFS_PRIMARY_ID, NULL /*TODO*/);

  return pMFile;
}

int tsdbOpenMFile(SMFile *pMFile, int flags) {
  ASSERT(!TSDB_FILE_OPENED(pMFile));

  pMFile->fd = open(pMFile->f.aname, flags);
  if (pMFile->fd < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

void tsdbCloseMFile(SMFile *pMFile) {
  if (TSDB_FILE_OPENED(pMFile)) {
    close(pMFile->fd);
    TSDB_FILE_SET_CLOSED(pMFile);
  }
}

int64_t tsdbSeekMFile(SMFile *pMFile, int64_t offset, int whence) {
  ASSERT(TSDB_FILE_OPENED(pMFile));

  int64_t loffset = taosLSeek(pMFile->fd, offset, whence);
  if (loffset < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return loffset;
}

int64_t tsdbWriteMFile(SMFile *pMFile, void *buf, int64_t nbyte) {
  ASSERT(TSDB_FILE_OPENED(pMFile));

  int64_t nwrite = taosWrite(pMFile->fd, buf, nbyte);
  if (nwrite < nbyte) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  pMFile->info.size += nbyte;
  return nwrite;
}

int64_t tsdbTellMFile(SMFile *pMFile) { return tsdbSeekMFile(pMFile, 0, SEEK_CUR); }

int tsdbEncodeMFile(void **buf, SMFile *pMFile) {
  int tlen = 0;

  tlen += tsdbEncodeMFInfo(buf, &(pMFile->info));
  tlen += tfsEncodeFile(buf, &(pMFile->f));

  return tlen;
}

void *tsdbDecodeMFile(void *buf, SMFile *pMFile) {
  buf = tsdbDecodeMFInfo(buf, &(pMFile->info));
  buf = tfsDecodeFile(buf, &(pMFile->f));

  return buf;
}

static int tsdbEncodeMFInfo(void **buf, SMFInfo *pInfo) {
  int tlen = 0;

  tlen += taosEncodeVariantI64(buf, pInfo->size);
  tlen += taosEncodeVariantI64(buf, pInfo->tombSize);
  tlen += taosEncodeVariantI64(buf, pInfo->nRecords);
  tlen += taosEncodeVariantI64(buf, pInfo->nDels);
  tlen += taosEncodeFixedU32(buf, pInfo->magic);

  return tlen;
}

static void *tsdbDecodeMFInfo(void *buf, SMFInfo *pInfo) {
  buf = taosDecodeVariantI64(buf, &(pInfo->size));
  buf = taosDecodeVariantI64(buf, &(pInfo->tombSize));
  buf = taosDecodeVariantI64(buf, &(pInfo->nRecords));
  buf = taosDecodeVariantI64(buf, &(pInfo->nDels));
  buf = taosDecodeFixedU32(buf, &(pInfo->magic));

  return buf;
}

// ============== Operations on SDFile
void tsdbInitDFile(SDFile *pDFile, int vid, int fid, int ver, int level, int id, const SDFInfo *pInfo, TSDB_FILE_T ftype) {
  TSDB_FILE_SET_CLOSED(pDFile);
  if (pInfo == NULL) {
    memset(&(pDFile->info), 0, sizeof(pDFile->info));
    pDFile->info.magic = TSDB_FILE_INIT_MAGIC;
  } else {
    pDFile->info = *pInfo;
  }
  tfsInitFile(&(pDFile->f), level, id, NULL /*TODO*/);
}

void tsdbInitDFileWithOld(SDFile *pDFile, SDFile *pOldDFile) {
  *pDFile = *pOldDFile;
  TSDB_FILE_SET_CLOSED(pDFile);
}

int tsdbOpenDFile(SDFile *pDFile, int flags) {
  ASSERT(!TSDB_FILE_OPENED(pDFile));

  pDFile->fd = open(pDFile->f.aname, flags);
  if (pDFile->fd < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

void tsdbCloseDFile(SDFile *pDFile) {
  if (TSDB_FILE_OPENED(pDFile)) {
    close(pDFile->fd);
    TSDB_FILE_SET_CLOSED(pDFile);
  }
}

int64_t tsdbSeekDFile(SDFile *pDFile, int64_t offset, int whence) {
  ASSERT(TSDB_FILE_OPENED(pDFile));

  int64_t loffset = taosLSeek(pDFile->fd, offset, whence);
  if (loffset < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return loffset;
}

int64_t tsdbWriteDFile(SDFile *pDFile, void *buf, int64_t nbyte) {
  ASSERT(TSDB_FILE_OPENED(pDFile));

  int64_t nwrite = taosWrite(pDFile->fd, buf, nbyte);
  if (nwrite < nbyte) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  pDFile->info.size += nbyte;
  return nwrite;
}

int64_t tsdbReadDFile(SDFile *pDFile, void *buf, int64_t nbyte) {
  ASSERT(TSDB_FILE_OPENED(pDFile));

  int64_t nread = taosRead(pDFile->fd, buf, nbyte);
  if (nread < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return nread;
}

int64_t tsdbTellDFile(SDFile *pDFile) { return tsdbSeekDFile(pDFile, 0, SEEK_CUR); }

int tsdbEncodeDFile(void **buf, SDFile *pDFile) {
  int tlen = 0;

  tlen += tsdbEncodeDFInfo(buf, &(pDFile->info));
  tlen += tfsEncodeFile(buf, &(pDFile->f));

  return tlen;
}

void *tsdbDecodeDFile(void *buf, SDFile *pDFile) {
  buf = tsdbDecodeDFInfo(buf, &(pDFile->info));
  buf = tfsDecodeFile(buf, &(pDFile->f));

  return buf;
}

static int tsdbEncodeDFInfo(void **buf, SDFInfo *pInfo) {
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

static void *tsdbDecodeDFInfo(void *buf, SDFInfo *pInfo) {
  buf = taosDecodeFixedU32(buf, &(pInfo->magic));
  buf = taosDecodeFixedU32(buf, &(pInfo->len));
  buf = taosDecodeFixedU32(buf, &(pInfo->totalBlocks));
  buf = taosDecodeFixedU32(buf, &(pInfo->totalSubBlocks));
  buf = taosDecodeFixedU32(buf, &(pInfo->offset));
  buf = taosDecodeFixedU64(buf, &(pInfo->size));
  buf = taosDecodeFixedU64(buf, &(pInfo->tombSize));

  return buf;
}

// ============== Operations on SDFileSet
void tsdbInitDFileSet(SDFileSet *pSet, int vid, int fid, int ver, int level, int id) {
  pSet->fid = fid;
  pSet->state = 0;

  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    SDFile *pDFile = TSDB_DFILE_IN_SET(pSet, ftype);
    tsdbInitDFile(pDFile, vid, fid, ver, level, id, NULL, ftype);
  }
}

void tsdbInitDFileSetWithOld(SDFileSet *pSet, SDFileSet *pOldSet) {
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    tsdbInitDFileWithOld(TSDB_DFILE_IN_SET(pSet, ftype), TSDB_DFILE_IN_SET(pOldSet, ftype));
  }
}

int tsdbOpenDFileSet(SDFileSet *pSet, int flags) {
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    SDFile *pDFile = TSDB_DFILE_IN_SET(pSet, ftype);

    if (tsdbOpenDFile(pDFile, flags) < 0) {
      tsdbCloseDFileSet(pSet);
      return -1;
    }
  }
}

void tsdbCloseDFileSet(SDFileSet *pSet) {
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    tsdbCloseDFile(pDFile);
  }
}

int tsdbUpdateDFileSetHeader(SDFileSet *pSet) {
  // TODO
  return 0;
}

int tsdbCopyDFileSet(SDFileSet *pFromSet, SDFileSet *pToSet) {
  // return 0;
}