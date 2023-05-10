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

#ifndef _TSDB_STT_FILE_WRITER_H
#define _TSDB_STT_FILE_WRITER_H

#include "tsdbFS.h"

#ifdef __cplusplus
extern "C" {
#endif

// SSttFReader ==========================================
typedef struct SSttFSegReader       SSttFSegReader;
typedef struct SSttFileReader       SSttFileReader;
typedef struct SSttFileReaderConfig SSttFileReaderConfig;

// SSttFileReader
int32_t tsdbSttFReaderOpen(const SSttFileReaderConfig *config, SSttFileReader **ppReader);
int32_t tsdbSttFReaderClose(SSttFileReader **ppReader);

// SSttFSegReader
int32_t tsdbSttFSegReaderOpen(SSttFileReader *pReader, SSttFSegReader **ppSegReader, int32_t nSegment);
int32_t tsdbSttFSegReaderClose(SSttFSegReader **ppSegReader);
int32_t tsdbSttFSegReadBloomFilter(SSttFSegReader *pSegReader, const void *pFilter);
int32_t tsdbSttFSegReadStatisBlk(SSttFSegReader *pSegReader, const SArray *pStatis);
int32_t tsdbSttFSegReadDelBlk(SSttFSegReader *pSegReader, const SArray *pDelBlk);
int32_t tsdbSttFSegReadSttBlk(SSttFSegReader *pSegReader, const SArray *pSttBlk);
int32_t tsdbSttFSegReadStatisBlock(SSttFSegReader *pSegReader, const void *pBlock);
int32_t tsdbSttFSegReadDelBlock(SSttFSegReader *pSegReader, const void *pBlock);
int32_t tsdbSttFSegReadSttBlock(SSttFSegReader *pSegReader, const void *pBlock);

// SSttFWriter ==========================================
typedef struct SSttFileWriter       SSttFileWriter;
typedef struct SSttFileWriterConfig SSttFileWriterConfig;

int32_t tsdbSttFWriterOpen(const SSttFileWriterConfig *config, SSttFileWriter **ppWriter);
int32_t tsdbSttFWriterClose(SSttFileWriter **ppWriter, int8_t abort, struct STFileOp *op);
int32_t tsdbSttFWriteTSData(SSttFileWriter *pWriter, TABLEID *tbid, TSDBROW *pRow);
int32_t tsdbSttFWriteDLData(SSttFileWriter *pWriter, TABLEID *tbid, SDelData *pDelData);

/* ------------------------------------------------- */
struct SSttFileWriterConfig {
  STsdb    *pTsdb;
  STFile    file;
  int32_t   maxRow;
  int32_t   szPage;
  int8_t    cmprAlg;
  SSkmInfo *pSkmTb;
  SSkmInfo *pSkmRow;
  uint8_t **aBuf;
};

struct SSttFileReaderConfig {
  STsdb    *pTsdb;
  SSkmInfo *pSkmTb;
  SSkmInfo *pSkmRow;
  uint8_t **aBuf;
  // TODO
};

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_STT_FILE_WRITER_H*/