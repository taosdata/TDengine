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
#include "tsdbSttFileRW.h"
#include "tsdbUtil.h"

#ifndef _TSDB_DATA_FILE_RW_H
#define _TSDB_DATA_FILE_RW_H

#ifdef __cplusplus
extern "C" {
#endif

typedef TARRAY2(SBlockIdx) TBlockIdxArray;
typedef TARRAY2(SDataBlk) TDataBlkArray;
typedef TARRAY2(SColumnDataAgg) TColumnDataAggArray;

// SDataFileReader =============================================
typedef struct SDataFileReader SDataFileReader;
typedef struct SDataFileReaderConfig {
  STsdb  *tsdb;
  int32_t szPage;
  struct {
    bool   exist;
    STFile file;
  } files[TSDB_FTYPE_MAX];
  uint8_t **bufArr;
} SDataFileReaderConfig;

int32_t tsdbDataFileReaderOpen(const char *fname[/* TSDB_FTYPE_MAX */], const SDataFileReaderConfig *config,
                               SDataFileReader **reader);
int32_t tsdbDataFileReaderClose(SDataFileReader **reader);
int32_t tsdbDataFileReadBlockIdx(SDataFileReader *reader, const TBlockIdxArray **blockIdxArray);
int32_t tsdbDataFileReadDataBlk(SDataFileReader *reader, const SBlockIdx *blockIdx, const TDataBlkArray **dataBlkArray);
int32_t tsdbDataFileReadDataBlock(SDataFileReader *reader, const SDataBlk *dataBlk, SBlockData *bData);
int32_t tsdbDataFileReadTombBlk(SDataFileReader *reader, const TTombBlkArray **tombBlkArray);
int32_t tsdbDataFileReadTombBlock(SDataFileReader *reader, const STombBlk *tombBlk, STombBlock *tData);

// SDataFileWriter =============================================
typedef struct SDataFileWriter SDataFileWriter;
typedef struct SDataFileWriterConfig {
  STsdb  *tsdb;
  int8_t  cmprAlg;
  int32_t maxRow;
  int32_t szPage;
  int32_t fid;
  int64_t cid;
  SDiskID did;
  int64_t compactVersion;
  struct {
    bool   exist;
    STFile file;
  } files[TSDB_FTYPE_MAX];
  SSkmInfo *skmTb;
  SSkmInfo *skmRow;
  uint8_t **bufArr;
} SDataFileWriterConfig;

int32_t tsdbDataFileWriterOpen(const SDataFileWriterConfig *config, SDataFileWriter **writer);
int32_t tsdbDataFileWriterClose(SDataFileWriter **writer, bool abort, TFileOpArray *opArr);
int32_t tsdbDataFileWriteTSData(SDataFileWriter *writer, SRowInfo *row);
int32_t tsdbDataFileWriteTSDataBlock(SDataFileWriter *writer, SBlockData *bData);
int32_t tsdbDataFileFlushTSDataBlock(SDataFileWriter *writer);
int32_t tsdbDataFileWriteTombRecord(SDataFileWriter *writer, const STombRecord *record);

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_DATA_FILE_RW_H*/