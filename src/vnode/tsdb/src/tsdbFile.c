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
#include <dirent.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "tglobalcfg.h"
#include "tsdbFile.h"

#define TSDB_FILE_HEAD_SIZE 512
#define TSDB_FILE_DELIMITER 0xF00AFA0F

#define tsdbGetKeyFileId(key, daysPerFile, precision) ((key) / tsMsPerDay[(precision)] / (daysPerFile))
#define tsdbGetMaxNumOfFiles(keep, daysPerFile) ((keep) / (daysPerFile) + 3)

typedef struct {
  int32_t len;
  int32_t padding;  // For padding purpose
  int64_t offset;
} SCompIdx;

/**
 * if numOfSubBlocks == -1, then the SCompBlock is a sub-block
 * if numOfSubBlocks == 1, then the SCompBlock refers to the data block, and offset/len refer to
 *                         the data block offset and length
 * if numOfSubBlocks > 1, then the offset/len refer to the offset of the first sub-block in the
 * binary
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
  int32_t    padding;      // For padding purpose
  int32_t    numOfBlocks;  // TODO: make the struct padding
  SCompBlock blocks[];
} SCompInfo;

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

const char *tsdbFileSuffix[] = {
    ".head",  // TSDB_FILE_TYPE_HEAD
    ".data",  // TSDB_FILE_TYPE_DATA
    ".last"   // TSDB_FILE_TYPE_LAST
};

static int tsdbWriteFileHead(int fd, SFile *pFile) {
  char head[TSDB_FILE_HEAD_SIZE] = "\0";

  pFile->size += TSDB_FILE_HEAD_SIZE;

  // TODO: write version and File statistic to the head
  lseek(fd, 0, SEEK_SET);
  if (write(fd, head, TSDB_FILE_HEAD_SIZE) < 0) return -1;

  return 0;
}

static int tsdbWriteHeadFileIdx(int fd, int maxTables, SFile *pFile) {
  int   size = sizeof(SCompIdx) * maxTables;
  void *buf = calloc(1, size);
  if (buf == NULL) return -1;

  if (lseek(fd, TSDB_FILE_HEAD_SIZE, SEEK_SET) < 0) {
    free(buf);
    return -1;
  }

  if (write(fd, buf, size) < 0) {
    free(buf);
    return -1;
  }

  pFile->size += size;

  return 0;
}

static int tsdbGetFileName(char *dataDir, int fileId, int8_t type, char *fname) {
  if (dataDir == NULL || fname == NULL || !IS_VALID_TSDB_FILE_TYPE(type)) return -1;

  sprintf(fname, "%s/f%d%s", dataDir, fileId, tsdbFileSuffix[type]);

  return 0;
}

/**
 * Create a file and set the SFile object
 */
static int tsdbCreateFile(char *dataDir, int fileId, int8_t type, int maxTables, SFile *pFile) {
  memset((void *)pFile, 0, sizeof(SFile));
  pFile->type = type;

  tsdbGetFileName(dataDir, fileId, type, pFile->fname);
  if (access(pFile->fname, F_OK) == 0) {
    // File already exists
    return -1;
  }

  int fd = open(pFile->fname, O_WRONLY | O_CREAT, 0755);
  if (fd < 0) return -1;

  if (type == TSDB_FILE_TYPE_HEAD) {
    if (tsdbWriteHeadFileIdx(fd, maxTables, pFile) < 0) {
      close(fd);
      return -1;
    }
  }

  if (tsdbWriteFileHead(fd, pFile) < 0) {
    close(fd);
    return -1;
  }

  close(fd);

  return 0;
}

/**
 * 
 */

// Create a file group with fileId and return a SFileGroup object
int tsdbCreateFileGroup(char *dataDir, int fileId, SFileGroup *pFGroup, int maxTables) {
  if (dataDir == NULL || pFGroup == NULL) return -1;

  memset((void *)pFGroup, 0, sizeof(SFileGroup));

  for (int type = TSDB_FILE_TYPE_HEAD; type < TSDB_FILE_TYPE_MAX; type++) {
    if (tsdbCreateFile(dataDir, fileId, type, maxTables, &(pFGroup->files[type])) < 0) {
      // TODO: deal with the error here, remove the created files
      return -1;
    }
  }

  pFGroup->fileId = fileId;

  return 0;
}

/**
 * Initialize the TSDB file handle
 */
STsdbFileH *tsdbInitFile(char *dataDir, int32_t daysPerFile, int32_t keep, int32_t minRowsPerFBlock,
                         int32_t maxRowsPerFBlock) {
  STsdbFileH *pTsdbFileH =
      (STsdbFileH *)calloc(1, sizeof(STsdbFileH) + sizeof(SFileGroup) * tsdbGetMaxNumOfFiles(keep, daysPerFile));
  if (pTsdbFileH == NULL) return NULL;

  pTsdbFileH->daysPerFile = daysPerFile;
  pTsdbFileH->keep = keep;
  pTsdbFileH->minRowPerFBlock = minRowsPerFBlock;
  pTsdbFileH->maxRowsPerFBlock = maxRowsPerFBlock;

  // Open the directory to read information of each file
  DIR *dir = opendir(dataDir);
  if (dir == NULL) {
    free(pTsdbFileH);
    return NULL;
  }

  struct dirent *dp;
  char           fname[256];
  while ((dp = readdir(dir)) != NULL) {
    if (strncmp(dp->d_name, ".", 1) == 0 || strncmp(dp->d_name, "..", 2) == 0) continue;
    if (true /* check if the file is the .head file */) {
      int fileId = 0;
      int vgId = 0;
      sscanf(dp->d_name, "v%df%d.head", &vgId, &fileId);
      // TODO

      // Open head file

      // Open data file

      // Open last file
    }
  }

  return pTsdbFileH;
}

/**
 * Closet the file handle
 */
void tsdbCloseFile(STsdbFileH *pFileH) {
  // TODO
}

static void tsdbGetKeyRangeOfFileId(int32_t daysPerFile, int8_t precision, int32_t fileId, TSKEY *minKey,
                                    TSKEY *maxKey) {
  *minKey = fileId * daysPerFile * tsMsPerDay[precision];
  *maxKey = *minKey + daysPerFile * tsMsPerDay[precision] - 1;
}