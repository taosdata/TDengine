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

#ifndef _TSDB_FILE_H
#define _TSDB_FILE_H

#include "tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Exposed Handle */
struct STFile;
struct SFileOp;

typedef enum {
  TSDB_FTYPE_HEAD = 0,  // .head
  TSDB_FTYPE_DATA,      // .data
  TSDB_FTYPE_SMA,       // .sma
  TSDB_FTYPE_TOMB,      // .tomb
  TSDB_FTYPE_STT,       // .stt
} tsdb_ftype_t;

/* Exposed APIs */
int32_t tsdbTFileInit(STsdb *pTsdb, struct STFile *pFile);
int32_t tsdbTFileClear(struct STFile *pFile);

/* Exposed Structs */
struct FStt {
  int32_t level;
  int32_t nStt;
};
struct STFile {
  char         fname[TSDB_FILENAME_LEN];
  SDiskID      diskId;
  int64_t      size;
  int64_t      cid;
  int32_t      fid;
  int32_t      ref;
  tsdb_ftype_t type;
};

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_FILE_H*/