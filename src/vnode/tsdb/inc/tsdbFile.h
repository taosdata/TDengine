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

#include "taosdef.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  TSDB_FILE_TYPE_HEAD = 0,  // .head file type
  TSDB_FILE_TYPE_DATA,      // .data file type
  TSDB_FILE_TYPE_LAST,      // .last file type
  TSDB_FILE_TYPE_MAX
} TSDB_FILE_TYPE;

extern const char *tsdbFileSuffix[];

typedef struct {
  int64_t size;
  int64_t tombSize;
} SFileInfo;

typedef struct {
  int8_t  type;
  char    fname[128];
  int64_t size;      // total size of the file
  int64_t tombSize;  // unused file size
} SFile;

typedef struct {
  int32_t fileId;
  SFile   files[TSDB_FILE_TYPE_MAX];
} SFileGroup;

// TSDB file handle
typedef struct {
  int32_t    daysPerFile;
  int32_t    keep;
  int32_t    minRowPerFBlock;
  int32_t    maxRowsPerFBlock;
  SFileGroup fGroup[];
} STsdbFileH;

#define IS_VALID_TSDB_FILE_TYPE(type) ((type) >= TSDB_FILE_TYPE_HEAD && (type) < TSDB_FILE_TYPE_MAX)

STsdbFileH *tsdbInitFile(char *dataDir, int32_t daysPerFile, int32_t keep, int32_t minRowsPerFBlock,
                         int32_t maxRowsPerFBlock);
void        tsdbCloseFile(STsdbFileH *pFileH);
int         tsdbCreateFileGroup(char *dataDir, int fileId, SFileGroup *pFGroup, int maxTables);
#ifdef __cplusplus
}
#endif

#endif  // _TD_TSDB_FILE_H_
