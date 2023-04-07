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

#ifndef _TSDB_FILE_SET_H
#define _TSDB_FILE_SET_H

#include "tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Exposed Handle */
struct SFileSet;

#define TSDB_STT_FILE_LEVEL_MAX 3

/* Exposed APIs */

/* Exposed Structs */
struct SFileSet {
  int32_t        fid;
  int64_t        nextid;
  struct STFile *fHead;  // .head
  struct STFile *fData;  // .data
  struct STFile *fSma;   // .sma
  struct STFile *fTomb;  // .tomb
  struct {
    int32_t        level;
    int32_t        nFile;
    struct STFile *fileList;
  } lStt[TSDB_STT_FILE_LEVEL_MAX];
};

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_FILE_SET_H*/