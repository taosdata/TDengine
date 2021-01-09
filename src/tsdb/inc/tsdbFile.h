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

typedef enum {
  TSDB_FILE_HEAD = 0,
  TSDB_FILE_DATA,
  TSDB_FILE_LAST,
  TSDB_FILE_MAX,
  TSDB_FILE_META,
  TSDB_FILE_MANIFEST
} TSDB_FILE_T;

// For meta file
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

void    tsdbInitMFile(SMFile* pMFile, int vid, int ver, SMFInfo* pInfo);
int     tsdbOpenMFile(SMFile* pMFile, int flags);
void    tsdbCloseMFile(SMFile* pMFile);
int64_t tsdbSeekMFile(SMFile* pMFile, int64_t offset, int whence);
int64_t tsdbWriteMFile(SMFile* pMFile, void* buf, int64_t nbyte);
int64_t tsdbTellMFile(SMFile *pMFile);
int     tsdbEncodeMFile(void** buf, SMFile* pMFile);
void*   tsdbDecodeMFile(void* buf, SMFile* pMFile);

// For .head/.data/.last file
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

void    tsdbInitDFile(SDFile* pDFile, int vid, int fid, int ver, int level, int id, const SDFInfo* pInfo,
                      TSDB_FILE_T ftype);
void    tsdbInitDFileWithOld(SDFile* pDFile, SDFile* pOldDFile);
int     tsdbOpenDFile(SDFile* pDFile, int flags);
void    tsdbCloseDFile(SDFile* pDFile);
int64_t tsdbSeekDFile(SDFile* pDFile, int64_t offset, int whence);
int64_t tsdbWriteDFile(SDFile* pDFile, void* buf, int64_t nbyte);
int64_t tsdbAppendDFile(SDFile* pDFile, void* buf, int64_t nbyte, int64_t* offset);
int64_t tsdbTellDFile(SDFile* pDFile);
int     tsdbEncodeDFile(void** buf, SDFile* pDFile);
void*   tsdbDecodeDFile(void* buf, SDFile* pDFile);
void    tsdbUpdateDFileMagic(SDFile* pDFile, void* pCksm);

typedef struct {
  int    fid;
  int    state;
  SDFile files[TSDB_FILE_MAX];
} SDFileSet;

#define TSDB_FILE_FULL_NAME(f) TFILE_NAME(&((f)->f))
#define TSDB_DFILE_IN_SET(s, t) ((s)->files + (t))

void tsdbInitDFileSet(SDFileSet* pSet, int vid, int fid, int ver, int level, int id);
void tsdbInitDFileSetWithOld(SDFileSet *pSet, SDFileSet *pOldSet);
int  tsdbOpenDFileSet(SDFileSet* pSet, int flags);
void tsdbCloseDFileSet(SDFileSet* pSet);
int  tsdbUpdateDFileSetHeader(SDFileSet* pSet);
int  tsdbCopyDFileSet(SDFileSet* pFromSet, SDFileSet* pToSet);


#ifdef __cplusplus
}
#endif

#endif /* _TS_TSDB_FILE_H_ */