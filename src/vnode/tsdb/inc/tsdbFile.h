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
#if !defined(_TD_TSDB_FILE_H_)
#define _TD_TSDB_FILE_H_

#include <stdint.h>

#include "dataformat.h"
#include "taosdef.h"
#include "tglobalcfg.h"

#ifdef __cplusplus
extern "C" {
#endif

#define tsdbGetKeyFileId(key, daysPerFile, precision) ((key) / tsMsPerDay[(precision)] / (daysPerFile))
#define tsdbGetMaxNumOfFiles(keep, daysPerFile) ((keep) / (daysPerFile) + 3)

typedef enum {
  TSDB_FILE_TYPE_HEAD = 0,  // .head file type
  TSDB_FILE_TYPE_DATA,      // .data file type
  TSDB_FILE_TYPE_LAST,      // .last file type
  TSDB_FILE_TYPE_MAX
} TSDB_FILE_TYPE;

#define IS_VALID_TSDB_FILE_TYPE(type) ((type) >= TSDB_FILE_TYPE_HEAD && (type) < TSDB_FILE_TYPE_MAX)

extern const char *tsdbFileSuffix[];

typedef struct {
  int8_t  type;
  int     fd;
  char    fname[128];
  int64_t size;      // total size of the file
  int64_t tombSize;  // unused file size
  int32_t totalBlocks;
  int32_t totalSubBlocks;
} SFile;

#define TSDB_IS_FILE_OPENED(f) ((f)->fd != -1)

typedef struct {
  int32_t fileId;
  SFile   files[TSDB_FILE_TYPE_MAX];
} SFileGroup;

// TSDB file handle
typedef struct {
  int maxFGroups;
  int numOfFGroups;

  SFileGroup fGroup[];
} STsdbFileH;

#define TSDB_MIN_FILE_ID(fh) (fh)->fGroup[0].fileId
#define TSDB_MAX_FILE_ID(fh) (fh)->fGroup[(fh)->numOfFGroups - 1].fileId

STsdbFileH *tsdbInitFileH(char *dataDir, int maxFiles);
void        tsdbCloseFileH(STsdbFileH *pFileH);
int         tsdbCreateFGroup(STsdbFileH *pFileH, char *dataDir, int fid, int maxTables);
int         tsdbOpenFile(SFile *pFile, int oflag);
SFileGroup *tsdbOpenFilesForCommit(STsdbFileH *pFileH, int fid);
int         tsdbRemoveFileGroup(STsdbFileH *pFile, int fid);

typedef struct {
  int32_t len;
  int32_t offset;
  int32_t hasLast : 1;
  int32_t numOfSuperBlocks : 31;
  int32_t checksum;
  TSKEY   maxKey;
} SCompIdx; /* sizeof(SCompIdx) = 24 */

/**
 * if numOfSubBlocks == 0, then the SCompBlock is a sub-block
 * if numOfSubBlocks >= 1, then the SCompBlock is a super-block
 *    - if numOfSubBlocks == 1, then the SCompBlock refers to the data block, and offset/len refer to
 *      the data block offset and length
 *    - if numOfSubBlocks > 1, then the offset/len refer to the offset of the first sub-block in the
 *      binary
 */
typedef struct {
  int64_t last : 1;          // If the block in data file or last file
  int64_t offset : 63;       // Offset of data block or sub-block index depending on numOfSubBlocks
  int32_t algorithm : 8;     // Compression algorithm
  int32_t numOfPoints : 24;  // Number of total points
  int32_t sversion;          // Schema version
  int32_t len;               // Data block length or nothing
  int16_t numOfSubBlocks;    // Number of sub-blocks;
  int16_t numOfCols;
  TSKEY   keyFirst;
  TSKEY   keyLast;
} SCompBlock;

typedef struct {
  int32_t    delimiter;  // For recovery usage
  int32_t    checksum;   // TODO: decide if checksum logic in this file or make it one API
  int64_t    uid;
  SCompBlock blocks[];
} SCompInfo;

int tsdbLoadCompIdx(SFileGroup *pGroup, void *buf, int maxTables);
int tsdbLoadCompBlocks(SFileGroup *pGroup, SCompIdx *pIdx, void *buf);

// TODO: take pre-calculation into account
typedef struct {
  int16_t colId;  // Column ID
  int16_t len;    // Column length
  int32_t type : 8;
  int32_t offset : 24;
} SCompCol;

// TODO: Take recover into account
typedef struct {
  int32_t  delimiter;  // For recovery usage
  int32_t  numOfCols;  // For recovery usage
  int64_t  uid;        // For recovery usage
  SCompCol cols[];
} SCompData;

int tsdbWriteBlockToFile(SFileGroup *pGroup, SCompInfo *pCompInfo, SCompIdx *pIdx, int isMerge, SCompBlock *pBlock, SDataCols *pCols);

void tsdbGetKeyRangeOfFileId(int32_t daysPerFile, int8_t precision, int32_t fileId, TSKEY *minKey, TSKEY *maxKey);
#ifdef __cplusplus
}
#endif

#endif  // _TD_TSDB_FILE_H_
