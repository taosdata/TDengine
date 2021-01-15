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

#ifndef _TS_TSDB_FILE_H_
#define _TS_TSDB_FILE_H_

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_FILE_HEAD_SIZE 512
#define TSDB_FILE_DELIMITER 0xF00AFA0F
#define TSDB_FILE_INIT_MAGIC 0xFFFFFFFF
#define TSDB_IVLD_FID INT_MIN

#define TSDB_FILE_INFO(tf) (&((tf)->info))
#define TSDB_FILE_F(tf) (&((tf)->f))
#define TSDB_FILE_FD(tf) ((tf)->fd)
#define TSDB_FILE_FULL_NAME(tf) TFILE_NAME(TSDB_FILE_F(tf))
#define TSDB_FILE_OPENED(tf) (TSDB_FILE_FD(tf) >= 0)
#define TSDB_FILE_CLOSED(tf) (!TSDB_FILE_OPENED(tf))
#define TSDB_FILE_SET_CLOSED(f) (TSDB_FILE_FD(f) = -1)
#define TSDB_FILE_LEVEL(tf) TFILE_LEVEL(TSDB_FILE_F(tf))
#define TSDB_FILE_ID(tf) TFILE_ID(TSDB_FILE_F(tf))

typedef enum {
  TSDB_FILE_HEAD = 0,
  TSDB_FILE_DATA,
  TSDB_FILE_LAST,
  TSDB_FILE_MAX,
  TSDB_FILE_META,
  TSDB_FILE_MANIFEST
} TSDB_FILE_T;

// =============== SMFile
typedef struct {
  int64_t  size;
  int64_t  tombSize;
  int64_t  nRecords;
  int64_t  nDels;
  uint32_t magic;
} SMFInfo;

typedef struct {
  SMFInfo info;
  TFILE   f;
  int     fd;
} SMFile;

void  tsdbInitMFile(SMFile* pMFile, SDiskID did, int vid, uint32_t ver);
void  tsdbInitMFileEx(SMFile* pMFile, SMFile* pOMFile);
int   tsdbEncodeSMFile(void** buf, SMFile* pMFile);
void* tsdbDecodeSMFile(void* buf, SMFile* pMFile);
int   tsdbApplyMFileChange(SMFile* from, SMFile* to);
int   tsdbApplyMFileChange(SMFile* from, SMFile* to);

static FORCE_INLINE int tsdbOpenMFile(SMFile* pMFile, int flags) {
  ASSERT(!TSDB_FILE_OPENED(pMFile));

  pMFile->fd = open(TSDB_FILE_FULL_NAME(pMFile), flags);
  if (pMFile->fd < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

static FORCE_INLINE void tsdbCloseMFile(SMFile* pMFile) {
  if (TSDB_FILE_OPENED(pMFile)) {
    close(pMFile->fd);
    TSDB_FILE_SET_CLOSED(pMFile);
  }
}

static FORCE_INLINE int64_t tsdbSeekMFile(SMFile* pMFile, int64_t offset, int whence) {
  ASSERT(TSDB_FILE_OPENED(pMFile));

  int64_t loffset = taosLSeek(TSDB_FILE_FD(pMFile), offset, whence);
  if (loffset < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return loffset;
}

static FORCE_INLINE int64_t tsdbWriteMFile(SMFile* pMFile, void* buf, int64_t nbyte) {
  ASSERT(TSDB_FILE_OPENED(pMFile));

  int64_t nwrite = taosWrite(pMFile->fd, buf, nbyte);
  if (nwrite < nbyte) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return nwrite;
}

static FORCE_INLINE void tsdbUpdateMFileMagic(SMFile* pMFile, void* pCksum) {
  pMFile->info.magic = taosCalcChecksum(pMFile->info.magic, (uint8_t*)(pCksum), sizeof(TSCKSUM));
}

static FORCE_INLINE int tsdbAppendMFile(SMFile* pMFile, void* buf, int64_t nbyte, int64_t* offset) {
  ASSERT(TSDB_FILE_OPENED(pMFile));

  int64_t toffset;

  if ((toffset = tsdbSeekMFile(pMFile, 0, SEEK_END)) < 0) {
    return -1;
  }

  ASSERT(pMFile->info.size == toffset);

  if (offset) {
    *offset = toffset;
  }

  if (tsdbWriteMFile(pMFile, buf, nbyte) < 0) {
    return -1;
  }

  pMFile->info.size += nbyte;

  return 0;
}

int tsdbCreateMFile(SMFile *pMFile);

static FORCE_INLINE int tsdbRemoveMFile(SMFile* pMFile) { return tfsremove(TSDB_FILE_F(pMFile)); }

int tsdbUpdateMFileHeader(SMFile* pMFile);

static FORCE_INLINE int64_t tsdbReadMFile(SMFile* pMFile, void* buf, int64_t nbyte) {
  ASSERT(TSDB_FILE_OPENED(pMFile));

  int64_t nread = taosRead(pMFile->fd, buf, nbyte);
  if (nread < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return nread;
}

// =============== SDFile
typedef struct {
  uint32_t magic;
  uint32_t len;
  uint32_t totalBlocks;
  uint32_t totalSubBlocks;
  uint32_t offset;
  uint64_t size;
  uint64_t tombSize;
} SDFInfo;

typedef struct {
  SDFInfo info;
  TFILE   f;
  int     fd;
} SDFile;

void  tsdbInitDFile(SDFile* pDFile, SDiskID did, int vid, int fid, uint32_t ver, TSDB_FILE_T ftype);
void  tsdbInitDFileEx(SDFile* pDFile, SDFile* pODFile);
int   tsdbEncodeSDFile(void** buf, SDFile* pDFile);
void* tsdbDecodeSDFile(void* buf, SDFile* pDFile);

static FORCE_INLINE int tsdbOpenDFile(SDFile* pDFile, int flags) {
  ASSERT(!TSDB_FILE_OPENED(pDFile));

  pDFile->fd = open(TSDB_FILE_FULL_NAME(pDFile), flags);
  if (pDFile->fd < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

static FORCE_INLINE void tsdbCloseDFile(SDFile* pDFile) {
  if (TSDB_FILE_OPENED(pDFile)) {
    close(pDFile->fd);
    TSDB_FILE_SET_CLOSED(pDFile);
  }
}

static FORCE_INLINE int64_t tsdbSeekDFile(SDFile *pDFile, int64_t offset, int whence) {
  ASSERT(TSDB_FILE_OPENED(pDFile));

  int64_t loffset = taosLSeek(TSDB_FILE_FD(pDFile), offset, whence);
  if (loffset < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return loffset;
}

static FORCE_INLINE int64_t tsdbWriteDFile(SDFile* pDFile, void* buf, int64_t nbyte) {
  ASSERT(TSDB_FILE_OPENED(pDFile));

  int64_t nwrite = taosWrite(pDFile->fd, buf, nbyte);
  if (nwrite < nbyte) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return nwrite;
}

static FORCE_INLINE void tsdbUpdateDFileMagic(SDFile* pDFile, void* pCksm) {
  pDFile->info.magic = taosCalcChecksum(pDFile->info.magic, (uint8_t*)(pCksm), sizeof(TSCKSUM));
}

static FORCE_INLINE int tsdbAppendDFile(SDFile* pDFile, void* buf, int64_t nbyte, int64_t* offset) {
  ASSERT(TSDB_FILE_OPENED(pDFile));

  int64_t toffset;

  if ((toffset = tsdbSeekDFile(pDFile, 0, SEEK_END)) < 0) {
    return -1;
  }

  ASSERT(pDFile->info.size == toffset);

  if (offset) {
    *offset = toffset;
  }

  if (tsdbWriteDFile(pDFile, buf, nbyte) < 0) {
    return -1;
  }

  pDFile->info.size += nbyte;

  return 0;
}

int tsdbCreateDFile(SDFile* pDFile);

static FORCE_INLINE int tsdbRemoveDFile(SDFile* pDFile) { return tfsremove(TSDB_FILE_F(pDFile)); }

int tsdbUpdateDFileHeader(SDFile* pDFile);

static FORCE_INLINE int64_t tsdbReadDFile(SDFile* pDFile, void* buf, int64_t nbyte) {
  ASSERT(TSDB_FILE_OPENED(pDFile));

  int64_t nread = taosRead(pDFile->fd, buf, nbyte);
  if (nread < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return nread;
}

static FORCE_INLINE int tsdbCopyDFile(SDFile* pSrc, SDFile* pDest) {
  if (tfscopy(TSDB_FILE_F(pSrc), TSDB_FILE_F(pDest)) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  pDest->info = pSrc->info;
  return 0;
}

// =============== SDFileSet
typedef struct {
  int    fid;
  int    state;
  SDFile files[TSDB_FILE_MAX];
} SDFileSet;

#define TSDB_FSET_FID(s) ((s)->fid)
#define TSDB_DFILE_IN_SET(s, t) ((s)->files + (t))
#define TSDB_FSET_LEVEL(s) TSDB_FILE_LEVEL(TSDB_DFILE_IN_SET(s, 0))
#define TSDB_FSET_ID(s) TSDB_FILE_ID(TSDB_DFILE_IN_SET(s, 0))
#define TSDB_FSET_SET_CLOSED(s)                                                \
  do {                                                                         \
    for (TSDB_FILE_T ftype = TSDB_FILE_HEAD; ftype < TSDB_FILE_MAX; ftype++) { \
      TSDB_FILE_SET_CLOSED(TSDB_DFILE_IN_SET(s, ftype));                       \
    }                                                                          \
  } while (0);

void tsdbInitDFileSet(SDFileSet *pSet, SDiskID did, int vid, int fid, uint32_t ver);
void  tsdbInitDFileSetEx(SDFileSet* pSet, SDFileSet* pOSet);
int   tsdbEncodeDFileSet(void** buf, SDFileSet* pSet);
void* tsdbDecodeDFileSet(void* buf, SDFileSet* pSet);
int   tsdbApplyDFileSetChange(SDFileSet* from, SDFileSet* to);

static FORCE_INLINE void tsdbCloseDFileSet(SDFileSet* pSet) {
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    tsdbCloseDFile(TSDB_DFILE_IN_SET(pSet, ftype));
  }
}

static FORCE_INLINE int tsdbOpenDFileSet(SDFileSet* pSet, int flags) {
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    if (tsdbOpenDFile(TSDB_DFILE_IN_SET(pSet, ftype), flags) < 0) {
      tsdbCloseDFileSet(pSet);
      return -1;
    }
  }
  return 0;
}

int tsdbCreateDFileSet(SDFileSet *pSet);

static FORCE_INLINE void tsdbRemoveDFileSet(SDFileSet* pSet) {
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    tsdbRemoveDFile(TSDB_DFILE_IN_SET(pSet, ftype));
  }
}

int tsdbUpdateDFileSetHeader(SDFileSet* pSet);

static FORCE_INLINE int tsdbCopyDFileSet(SDFileSet* pSrc, SDFileSet* pDest) {
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    if (tsdbCopyDFile(TSDB_DFILE_IN_SET(pSrc, ftype), TSDB_DFILE_IN_SET(pDest, ftype)) < 0) {
      tsdbRemoveDFileSet(pDest);
      return -1;
    }
  }

  return 0;
}

#ifdef __cplusplus
}
#endif

#endif /* _TS_TSDB_FILE_H_ */