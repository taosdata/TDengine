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
typedef struct SSttFSegReader    SSttFSegReader;
typedef struct SSttFReader       SSttFReader;
typedef struct SSttFReaderConfig SSttFReaderConfig;

int32_t tsdbSttFReaderOpen(const SSttFReaderConfig *config, SSttFReader **ppReader);
int32_t tsdbSttFReaderClose(SSttFReader **ppReader);

// SSttFWriter ==========================================
typedef struct SSttFWriter       SSttFWriter;
typedef struct SSttFWriterConfig SSttFWriterConfig;

int32_t tsdbSttFWriterOpen(const SSttFWriterConfig *config, SSttFWriter **ppWriter);
int32_t tsdbSttFWriterClose(SSttFWriter **ppWriter, int8_t abort, struct SFileOp *op);
int32_t tsdbSttFWriteTSData(SSttFWriter *pWriter, TABLEID *tbid, TSDBROW *pRow);
int32_t tsdbSttFWriteDLData(SSttFWriter *pWriter, TABLEID *tbid, SDelData *pDelData);

/* ------------------------------------------------- */
struct SSttFWriterConfig {
  STsdb    *pTsdb;
  STFile    file;
  int32_t   maxRow;
  int32_t   szPage;
  int8_t    cmprAlg;
  SSkmInfo *pSkmTb;
  SSkmInfo *pSkmRow;
  uint8_t **aBuf;
};

struct SSttFReaderConfig {
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