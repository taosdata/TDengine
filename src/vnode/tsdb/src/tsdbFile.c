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
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <dirent.h>

#include "tsdbFile.h"
#include "tglobalcfg.h"

// int64_t tsMsPerDay[] = {
//     86400000L,       // TSDB_PRECISION_MILLI
//     86400000000L,    // TSDB_PRECISION_MICRO
//     86400000000000L  // TSDB_PRECISION_NANO
// };

#define tsdbGetKeyFileId(key, daysPerFile, precision) ((key) / tsMsPerDay[(precision)] / (daysPerFile))
#define tsdbGetMaxNumOfFiles(keep, daysPerFile) ((keep) / (daysPerFile) + 3)

typedef struct {
  int64_t offset;
} SCompHeader;

typedef struct {
  int64_t uid;
  int64_t last : 1;
  int64_t numOfBlocks : 63;
  int32_t delimiter;
} SCompInfo;

typedef struct {
  TSKEY   keyFirst;
  TSKEY   keyLast;
  int32_t numOfBlocks;
  int32_t offset;
} SCompIdx;

typedef struct {
  TSKEY   keyFirst;
  TSKEY   keyLast;
  int64_t offset;
  int32_t len;
  int32_t sversion;
} SCompBlock;

typedef struct {
  int64_t uid;
} SBlock;

typedef struct {
  int16_t colId;
  int16_t bytes;
  int32_t nNullPoints;
  int32_t type:8;
  int32_t offset:24;
  int32_t len;
  // fields for pre-aggregate
  // TODO: pre-aggregation should be seperated
  int64_t sum;
  int64_t max;
  int64_t min;
  int16_t maxIdx;
  int16_t minIdx;
} SField;

const char *tsdbFileSuffix[] = {
    ".head",  // TSDB_FILE_TYPE_HEAD
    ".data",  // TSDB_FILE_TYPE_DATA
    ".last",  // TSDB_FILE_TYPE_LAST
    ".meta"   // TSDB_FILE_TYPE_META
};

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
  char fname[256];
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

char *tsdbGetFileName(char *dirName, char *fname, TSDB_FILE_TYPE type) {
  if (!IS_VALID_TSDB_FILE_TYPE(type)) return NULL;

  char *fileName = (char *)malloc(strlen(dirName) + strlen(fname) + strlen(tsdbFileSuffix[type]) + 5);
  if (fileName == NULL) return NULL;

  sprintf(fileName, "%s/%s%s", dirName, fname, tsdbFileSuffix[type]);
  return fileName;
}

static void tsdbGetKeyRangeOfFileId(int32_t daysPerFile, int8_t precision, int32_t fileId, TSKEY *minKey,
                                    TSKEY *maxKey) {
  *minKey = fileId * daysPerFile * tsMsPerDay[precision];
  *maxKey = *minKey + daysPerFile * tsMsPerDay[precision] - 1;
}