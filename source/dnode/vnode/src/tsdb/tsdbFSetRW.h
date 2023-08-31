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

#include "tsdbDataFileRW.h"
#include "tsdbSttFileRW.h"

#ifndef _TSDB_FSET_RW_H
#define _TSDB_FSET_RW_H

#ifdef __cplusplus
extern "C" {
#endif

//
typedef struct SFSetWriter SFSetWriter;
typedef struct {
  STsdb  *tsdb;
  bool    toSttOnly;
  int64_t compactVersion;
  int32_t minRow;
  int32_t maxRow;
  int32_t szPage;
  int8_t  cmprAlg;
  int32_t fid;
  int64_t cid;
  SDiskID did;
  int32_t level;
  struct {
    bool   exist;
    STFile file;
  } files[TSDB_FTYPE_MAX];
} SFSetWriterConfig;

int32_t tsdbFSetWriterOpen(SFSetWriterConfig *config, SFSetWriter **writer);
int32_t tsdbFSetWriterClose(SFSetWriter **writer, bool abort, TFileOpArray *fopArr);
int32_t tsdbFSetWriteRow(SFSetWriter *writer, SRowInfo *row);
int32_t tsdbFSetWriteTombRecord(SFSetWriter *writer, const STombRecord *tombRecord);

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_FSET_RW_H*/
