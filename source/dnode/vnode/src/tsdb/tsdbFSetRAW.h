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

#include "tsdbDataFileRAW.h"

#ifndef _TSDB_FSET_RAW_H
#define _TSDB_FSET_RAW_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SFSetRAWWriterConfig {
  STsdb  *tsdb;
  int32_t szPage;

  SDiskID did;
  int64_t fid;
  int64_t cid;
  int32_t level;
} SFSetRAWWriterConfig;

typedef struct SFSetRAWWriter SFSetRAWWriter;

int32_t tsdbFSetRAWWriterOpen(SFSetRAWWriterConfig *config, SFSetRAWWriter **writer);
int32_t tsdbFSetRAWWriterClose(SFSetRAWWriter **writer, bool abort, TFileOpArray *fopArr);
int32_t tsdbFSetRAWWriteBlockData(SFSetRAWWriter *writer, STsdbDataRAWBlockHeader *bHdr);

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_FSET_RAW_H*/
