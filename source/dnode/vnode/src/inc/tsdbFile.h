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

#include "tchecksum.h"
#include "tfs.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_FILE_HEAD_SIZE 512
#define TSDB_FILE_DELIMITER 0xF00AFA0F
#define TSDB_FILE_INIT_MAGIC 0xFFFFFFFF
#define TSDB_IVLD_FID INT_MIN
#define TSDB_FILE_STATE_OK 0
#define TSDB_FILE_STATE_BAD 1

#define TSDB_FILE_INFO(tf) (&((tf)->info))
#define TSDB_FILE_F(tf) (&((tf)->f))
#define TSDB_FILE_PFILE(tf) ((tf)->pFile)
#define TSDB_FILE_FULL_NAME(tf) (TSDB_FILE_F(tf)->aname)
#define TSDB_FILE_OPENED(tf) (TSDB_FILE_PFILE(tf) != NULL)
#define TSDB_FILE_CLOSED(tf) (!TSDB_FILE_OPENED(tf))
#define TSDB_FILE_SET_CLOSED(f) (TSDB_FILE_PFILE(f) = NULL)
#define TSDB_FILE_LEVEL(tf) (TSDB_FILE_F(tf)->did.level)
#define TSDB_FILE_ID(tf) (TSDB_FILE_F(tf)->did.id)
#define TSDB_FILE_DID(tf) (TSDB_FILE_F(tf)->did)
#define TSDB_FILE_REL_NAME(tf) (TSDB_FILE_F(tf)->rname)
#define TSDB_FILE_ABS_NAME(tf) (TSDB_FILE_F(tf)->aname)
#define TSDB_FILE_FSYNC(tf) taosFsyncFile(TSDB_FILE_PFILE(tf))
#define TSDB_FILE_STATE(tf) ((tf)->state)
#define TSDB_FILE_SET_STATE(tf, s) ((tf)->state = (s))
#define TSDB_FILE_IS_OK(tf) (TSDB_FILE_STATE(tf) == TSDB_FILE_STATE_OK)
#define TSDB_FILE_IS_BAD(tf) (TSDB_FILE_STATE(tf) == TSDB_FILE_STATE_BAD)

typedef enum {
  TSDB_FILE_HEAD = 0,  // .head
  TSDB_FILE_DATA,      // .data
  TSDB_FILE_LAST,      // .last
  TSDB_FILE_SMAD,      // .smad(Block-wise SMA)
  TSDB_FILE_SMAL,      // .smal(Block-wise SMA)
  TSDB_FILE_MAX,       //
  TSDB_FILE_META,      // meta
  TSDB_FILE_TSMA,      // v2t100.${sma_index_name}, Time-range-wise SMA
  TSDB_FILE_RSMA,      // v2r100.${sma_index_name}, Time-range-wise Rollup SMA
} E_TSDB_FILE_T;

typedef int32_t TSDB_FILE_T;
typedef enum {
  TSDB_FS_VER_0 = 0,
  TSDB_FS_VER_MAX,
} ETsdbFsVer;

#define TSDB_LATEST_FVER    TSDB_FS_VER_0  // latest version for DFile
#define TSDB_LATEST_SFS_VER TSDB_FS_VER_0  // latest version for 'current' file

static FORCE_INLINE uint32_t tsdbGetDFSVersion(TSDB_FILE_T fType) {  // latest version for DFile
  switch (fType) {
    case TSDB_FILE_HEAD:  // .head
    case TSDB_FILE_DATA:  // .data
    case TSDB_FILE_LAST:  // .last
    case TSDB_FILE_SMAD:  // .smad(Block-wise SMA)
    case TSDB_FILE_SMAL:  // .smal(Block-wise SMA)
    default:
      return TSDB_LATEST_FVER;
  }
}

#if 0
// =============== SMFile
typedef struct {
  int64_t  size;
  int64_t  tombSize;
  int64_t  nRecords;
  int64_t  nDels;
  uint32_t magic;
} SMFInfo;

typedef struct {
  SMFInfo  info;
  STfsFile f;
  int      fd;
  uint8_t  state;
} SMFile;

void  tsdbInitMFile(SMFile* pMFile, SDiskID did, int vid, uint32_t ver);
void  tsdbInitMFileEx(SMFile* pMFile, const SMFile* pOMFile);
int   tsdbEncodeSMFile(void** buf, SMFile* pMFile);
void* tsdbDecodeSMFile(void* buf, SMFile* pMFile);
int   tsdbEncodeSMFileEx(void** buf, SMFile* pMFile);
void* tsdbDecodeSMFileEx(void* buf, SMFile* pMFile);
int   tsdbApplyMFileChange(SMFile* from, SMFile* to);
int   tsdbCreateMFile(SMFile* pMFile, bool updateHeader);
int   tsdbUpdateMFileHeader(SMFile* pMFile);
int   tsdbLoadMFileHeader(SMFile* pMFile, SMFInfo* pInfo);
int   tsdbScanAndTryFixMFile(STsdb* pRepo);
int   tsdbEncodeMFInfo(void** buf, SMFInfo* pInfo);
void* tsdbDecodeMFInfo(void* buf, SMFInfo* pInfo);

static FORCE_INLINE void tsdbSetMFileInfo(SMFile* pMFile, SMFInfo* pInfo) { pMFile->info = *pInfo; }

static FORCE_INLINE int tsdbOpenMFile(SMFile* pMFile, int flags) {
  ASSERT(TSDB_FILE_CLOSED(pMFile));

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

  int64_t loffset = taosLSeekFile(TSDB_FILE_FD(pMFile), offset, whence);
  if (loffset < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return loffset;
}

static FORCE_INLINE int64_t tsdbWriteMFile(SMFile* pMFile, void* buf, int64_t nbyte) {
  ASSERT(TSDB_FILE_OPENED(pMFile));

  int64_t nwrite = taosWriteFile(pMFile->fd, buf, nbyte);
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

  return (int)nbyte;
}

static FORCE_INLINE int tsdbRemoveMFile(SMFile* pMFile) { return tfsremove(TSDB_FILE_F(pMFile)); }

static FORCE_INLINE int64_t tsdbReadMFile(SMFile* pMFile, void* buf, int64_t nbyte) {
  ASSERT(TSDB_FILE_OPENED(pMFile));

  int64_t nread = taosReadFile(pMFile->fd, buf, nbyte);
  if (nread < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return nread;
}

#endif

// =============== SDFile
typedef struct {
  uint32_t magic;
  uint32_t fver;
  uint32_t len;
  uint32_t totalBlocks;
  uint32_t totalSubBlocks;
  uint32_t offset;
  uint64_t size;
  uint64_t tombSize;
} SDFInfo;

typedef struct {
  SDFInfo   info;
  STfsFile  f;
  TdFilePtr pFile;
  uint8_t   state;
} SDFile;

void  tsdbInitDFile(STsdb *pRepo, SDFile* pDFile, SDiskID did, int fid, uint32_t ver, TSDB_FILE_T ftype);
void  tsdbInitDFileEx(SDFile* pDFile, SDFile* pODFile);
int   tsdbEncodeSDFile(void** buf, SDFile* pDFile);
void* tsdbDecodeSDFile(STsdb *pRepo, void* buf, SDFile* pDFile);
int   tsdbCreateDFile(STsdb *pRepo, SDFile* pDFile, bool updateHeader, TSDB_FILE_T fType);
int   tsdbUpdateDFileHeader(SDFile* pDFile);
int   tsdbLoadDFileHeader(SDFile* pDFile, SDFInfo* pInfo);
int   tsdbParseDFilename(const char* fname, int* vid, int* fid, TSDB_FILE_T* ftype, uint32_t* version);

static FORCE_INLINE void tsdbSetDFileInfo(SDFile* pDFile, SDFInfo* pInfo) { pDFile->info = *pInfo; }

static FORCE_INLINE int tsdbOpenDFile(SDFile* pDFile, int flags) {
  ASSERT(!TSDB_FILE_OPENED(pDFile));

  pDFile->pFile = taosOpenFile(TSDB_FILE_FULL_NAME(pDFile), flags);
  if (pDFile->pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

static FORCE_INLINE void tsdbCloseDFile(SDFile* pDFile) {
  if (TSDB_FILE_OPENED(pDFile)) {
    taosCloseFile(&pDFile->pFile);
    TSDB_FILE_SET_CLOSED(pDFile);
  }
}

static FORCE_INLINE int64_t tsdbSeekDFile(SDFile* pDFile, int64_t offset, int whence) {
  // ASSERT(TSDB_FILE_OPENED(pDFile));

  int64_t loffset = taosLSeekFile(TSDB_FILE_PFILE(pDFile), offset, whence);
  if (loffset < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return loffset;
}

static FORCE_INLINE int64_t tsdbWriteDFile(SDFile* pDFile, void* buf, int64_t nbyte) {
  ASSERT(TSDB_FILE_OPENED(pDFile));

  int64_t nwrite = taosWriteFile(pDFile->pFile, buf, nbyte);
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

  return (int)nbyte;
}

static FORCE_INLINE int tsdbRemoveDFile(SDFile* pDFile) { return tfsRemoveFile(TSDB_FILE_F(pDFile)); }

static FORCE_INLINE int64_t tsdbReadDFile(SDFile* pDFile, void* buf, int64_t nbyte) {
  ASSERT(TSDB_FILE_OPENED(pDFile));

  int64_t nread = taosReadFile(pDFile->pFile, buf, nbyte);
  if (nread < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return nread;
}

static FORCE_INLINE int tsdbCopyDFile(SDFile* pSrc, SDFile* pDest) {
  if (tfsCopyFile(TSDB_FILE_F(pSrc), TSDB_FILE_F(pDest)) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  tsdbSetDFileInfo(pDest, TSDB_FILE_INFO(pSrc));
  return 0;
}

// =============== SDFileSet
typedef struct {
  int      fid;
  int8_t   state;  // -128~127
  uint8_t  ver;    // 0~255, DFileSet version
  uint16_t reserve;
  SDFile   files[TSDB_FILE_MAX];
} SDFileSet;

typedef struct {
  int      fid;
  int8_t   state;
  uint8_t  ver;
  uint16_t reserve;
#if 0
  SDFInfo   info;
#endif
  STfsFile  f;
  TdFilePtr pFile;

} SSFile;  // files split by days with fid

#define TSDB_LATEST_FSET_VER 0

#define TSDB_FSET_FID(s) ((s)->fid)
#define TSDB_FSET_STATE(s) ((s)->state)
#define TSDB_FSET_VER(s) ((s)->ver)
#define TSDB_DFILE_IN_SET(s, t) ((s)->files + (t))
#define TSDB_FSET_LEVEL(s) TSDB_FILE_LEVEL(TSDB_DFILE_IN_SET(s, 0))
#define TSDB_FSET_ID(s) TSDB_FILE_ID(TSDB_DFILE_IN_SET(s, 0))
#define TSDB_FSET_SET_CLOSED(s)                                                \
  do {                                                                         \
    for (TSDB_FILE_T ftype = TSDB_FILE_HEAD; ftype < TSDB_FILE_MAX; ftype++) { \
      TSDB_FILE_SET_CLOSED(TSDB_DFILE_IN_SET(s, ftype));                       \
    }                                                                          \
  } while (0);
#define TSDB_FSET_FSYNC(s)                                                     \
  do {                                                                         \
    for (TSDB_FILE_T ftype = TSDB_FILE_HEAD; ftype < TSDB_FILE_MAX; ftype++) { \
      TSDB_FILE_FSYNC(TSDB_DFILE_IN_SET(s, ftype));                            \
    }                                                                          \
  } while (0);

void  tsdbInitDFileSet(STsdb *pRepo, SDFileSet* pSet, SDiskID did, int fid, uint32_t ver);
void  tsdbInitDFileSetEx(SDFileSet* pSet, SDFileSet* pOSet);
int   tsdbEncodeDFileSet(void** buf, SDFileSet* pSet);
void* tsdbDecodeDFileSet(STsdb *pRepo, void* buf, SDFileSet* pSet);
int   tsdbEncodeDFileSetEx(void** buf, SDFileSet* pSet);
void* tsdbDecodeDFileSetEx(void* buf, SDFileSet* pSet);
int   tsdbApplyDFileSetChange(SDFileSet* from, SDFileSet* to);
int   tsdbCreateDFileSet(STsdb *pRepo, SDFileSet* pSet, bool updateHeader);
int   tsdbUpdateDFileSetHeader(SDFileSet* pSet);
int   tsdbScanAndTryFixDFileSet(STsdb* pRepo, SDFileSet* pSet);

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

static FORCE_INLINE void tsdbRemoveDFileSet(SDFileSet* pSet) {
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    (void)tsdbRemoveDFile(TSDB_DFILE_IN_SET(pSet, ftype));
  }
}

static FORCE_INLINE int tsdbCopyDFileSet(SDFileSet* pSrc, SDFileSet* pDest) {
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    if (tsdbCopyDFile(TSDB_DFILE_IN_SET(pSrc, ftype), TSDB_DFILE_IN_SET(pDest, ftype)) < 0) {
      tsdbRemoveDFileSet(pDest);
      return -1;
    }
  }

  return 0;
}

static FORCE_INLINE void tsdbGetFidKeyRange(int days, int8_t precision, int fid, TSKEY* minKey, TSKEY* maxKey) {
  *minKey = fid * days * tsTickPerDay[precision];
  *maxKey = *minKey + days * tsTickPerDay[precision] - 1;
}

static FORCE_INLINE bool tsdbFSetIsOk(SDFileSet* pSet) {
  for (TSDB_FILE_T ftype = 0; ftype < TSDB_FILE_MAX; ftype++) {
    if (TSDB_FILE_IS_BAD(TSDB_DFILE_IN_SET(pSet, ftype))) {
      return false;
    }
  }

  return true;
}

#ifdef __cplusplus
}
#endif

#endif /* _TS_TSDB_FILE_H_ */