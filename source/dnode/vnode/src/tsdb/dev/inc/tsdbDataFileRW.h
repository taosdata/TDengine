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

#include "tsdbDef.h"
#include "tsdbFSet.h"
#include "tsdbUtil.h"

#ifndef _TSDB_DATA_FILE_RW_H
#define _TSDB_DATA_FILE_RW_H

#ifdef __cplusplus
extern "C" {
#endif

typedef TARRAY2(SBlockIdx) TBlockIdxArray;
typedef TARRAY2(SDataBlk) TDataBlkArray;

// SDataFileReader =============================================
typedef struct SDataFileReader SDataFileReader;
typedef struct SDataFileReaderConfig {
  STsdb  *tsdb;
  STFile  f[TSDB_FTYPE_MAX];
  int32_t szPage;
} SDataFileReaderConfig;

int32_t tsdbDataFileReaderOpen(const char *fname[/* TSDB_FTYPE_MAX */], const SDataFileReaderConfig *config,
                               SDataFileReader **reader);
int32_t tsdbDataFileReaderClose(SDataFileReader *reader);
int32_t tsdbDataFileReadBlockIdx(SDataFileReader *reader, const TBlockIdxArray **blockIdxArray);
int32_t tsdbDataFileReadDataBlk(SDataFileReader *reader, const SBlockIdx *blockIdx, const TDataBlkArray **dataBlkArray);

int32_t tsdbDataFileReadDataBlock(SDataFileReader *reader, const SDataBlk *dataBlk, SBlockData *bData);
int32_t tsdbDataFileReadDelData(SDataFileReader *reader, const SDelBlk *delBlk, SDelData *dData);

// SDataFileWriter =============================================
typedef struct SDataFileWriter SDataFileWriter;
typedef struct SDataFileWriterConfig {
  STsdb  *tsdb;
  STFile  f[TSDB_FTYPE_MAX];
  int32_t maxRow;
} SDataFileWriterConfig;

int32_t tsdbDataFileWriterOpen(const SDataFileWriterConfig *config, SDataFileWriter **writer);
int32_t tsdbDataFileWriterClose(SDataFileWriter **writer, bool abort, STFileOp op[/*TSDB_FTYPE_MAX*/]);
int32_t tsdbDataFileWriteTSDataBlock(SDataFileWriter *writer, SBlockData *bData);

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_DATA_FILE_RW_H*/