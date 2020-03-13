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

typedef int32_t file_id_t;

typedef enum {
  TSDB_FILE_TYPE_HEAD,  // .head file type
  TSDB_FILE_TYPE_DATA,  // .data file type
  TSDB_FILE_TYPE_LAST,  // .last file type
  TSDB_FILE_TYPE_META   // .meta file type
} TSDB_FILE_TYPE;

extern const char *tsdbFileSuffix[];

typedef struct {
  int64_t size;
  int64_t tombSize;
} SFileInfo;

typedef struct {
  char *    fname;
  SFileInfo fInfo;
} SFILE;

// typedef struct {
//   int64_t offset;
//   int64_t skey;
//   int64_t ekey;
//   int16_t numOfBlocks;
// } SDataBlock;

#define IS_VALID_TSDB_FILE_TYPE(type) ((type) >= TSDB_FILE_TYPE_HEAD && (type) <= TSDB_FILE_TYPE_META)

char *tsdbGetFileName(char *dirName, char *fname, TSDB_FILE_TYPE type);

#ifdef __cplusplus
}
#endif

#endif  // _TD_TSDB_FILE_H_
