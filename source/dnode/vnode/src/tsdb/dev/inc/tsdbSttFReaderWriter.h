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

#include "tsdbFS.h"
#include "tsdbUtil.h"

#ifndef _TSDB_STT_FILE_WRITER_H
#define _TSDB_STT_FILE_WRITER_H

#ifdef __cplusplus
extern "C" {
#endif

typedef TARRAY2(SSttBlk) TSttBlkArray;
typedef TARRAY2(SDelBlk) TDelBlkArray;
typedef TARRAY2(STbStatisBlk) TStatisBlkArray;

// SSttFileReader ==========================================
typedef struct SSttFileReader       SSttFileReader;
typedef struct SSttFileReaderConfig SSttFileReaderConfig;
typedef struct SSttSegReader        SSttSegReader;
typedef TARRAY2(SSttSegReader *) TSttSegReaderArray;

// SSttFileReader
int32_t tsdbSttFReaderOpen(const SSttFileReaderConfig *config, SSttFileReader **reader);
int32_t tsdbSttFReaderClose(SSttFileReader **reader);
int32_t tsdbSttFReaderGetSegReader(SSttFileReader *reader, const TSttSegReaderArray **segReaderArray);

// SSttSegReader
int32_t tsdbSttFReadBloomFilter(SSttSegReader *reader, const void *pFilter);

int32_t tsdbSttFReadSttBlk(SSttSegReader *reader, const TSttBlkArray **sttBlkArray);
int32_t tsdbSttFReadDelBlk(SSttSegReader *reader, const TDelBlkArray **delBlkArray);
int32_t tsdbSttFReadStatisBlk(SSttSegReader *reader, const TStatisBlkArray **statisBlkArray);

int32_t tsdbSttFReadSttBlock(SSttSegReader *reader, const SSttBlk *sttBlk, SBlockData *bData);
int32_t tsdbSttFReadDelBlock(SSttSegReader *reader, const SDelBlk *delBlk, SDelBlock *dData);
int32_t tsdbSttFReadStatisBlock(SSttSegReader *reader, const STbStatisBlk *statisBlk, STbStatisBlock *sData);

struct SSttFileReaderConfig {
  STsdb    *tsdb;
  SSkmInfo *skmTb;
  SSkmInfo *skmRow;
  uint8_t **aBuf;
  int32_t   szPage;
  STFile    file;
};

// SSttFileWriter ==========================================
typedef struct SSttFileWriter       SSttFileWriter;
typedef struct SSttFileWriterConfig SSttFileWriterConfig;

int32_t tsdbSttFWriterOpen(const SSttFileWriterConfig *config, SSttFileWriter **writer);
int32_t tsdbSttFWriterClose(SSttFileWriter **writer, int8_t abort, STFileOp *op);
int32_t tsdbSttFWriteTSData(SSttFileWriter *writer, SRowInfo *row);
int32_t tsdbSttFWriteTSDataBlock(SSttFileWriter *writer, SBlockData *pBlockData);
int32_t tsdbSttFWriteDLData(SSttFileWriter *writer, TABLEID *tbid, SDelData *pDelData);

struct SSttFileWriterConfig {
  STsdb    *tsdb;
  int32_t   maxRow;
  int32_t   szPage;
  int8_t    cmprAlg;
  int64_t   compVer;  // compact version
  SSkmInfo *skmTb;
  SSkmInfo *skmRow;
  uint8_t **aBuf;
  STFile    file;
};

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_STT_FILE_WRITER_H*/