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

#ifndef _TD_TSDB_DATA_F_READER_WRITER_H_
#define _TD_TSDB_DATA_F_READER_WRITER_H_

#ifdef __cplusplus
extern "C" {
#endif

// SDataFileReader =============================================
typedef struct SDataFileReader       SDataFileReader;
typedef struct SDataFileReaderConfig SDataFileReaderConfig;

// SDataFileWriter =============================================
typedef struct SDataFileWriter       SDataFileWriter;
typedef struct SDataFileWriterConfig SDataFileWriterConfig;

int32_t tsdbDataFileWriterOpen(const SDataFileWriterConfig *config, SDataFileWriter **writer);
int32_t tsdbDataFileWriterClose(SDataFileWriter *writer);
int32_t tsdbDataFileWriteTSData(SDataFileWriter *writer, SBlockData *bData);
int32_t tsdbDataFileWriteTSDataBlock(SDataFileWriter *writer, SBlockData *bData);

struct SDataFileReaderConfig {
  STsdb *pTsdb;
  // TODO
};

struct SDataFileWriterConfig {
  STsdb *pTsdb;
  //   TODO
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_TSDB_DATA_F_READER_WRITER_H_*/