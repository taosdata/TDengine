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

#include "tarray2.h"
#include "tsdbDef.h"
#include "tsdbFSet2.h"
#include "tsdbFile2.h"
#include "tsdbUtil2.h"

#ifndef _TSDB_DATA_FILE_RAW_H
#define _TSDB_DATA_FILE_RAW_H

#ifdef __cplusplus
extern "C" {
#endif

// STsdbDataRAWBlockHeader =======================================
typedef struct STsdbDataRAWBlockHeader {
  struct {
    int32_t type;
    int64_t fid;
    int64_t cid;
    int64_t size;
    int64_t minVer;
    int64_t maxVer;
    union {
      struct {
        int32_t level;
      } stt[1];
    };
  } file;

  int64_t offset;
  int64_t dataLength;
  uint8_t data[0];
} STsdbDataRAWBlockHeader;

// SDataFileRAWReader =============================================
typedef struct SDataFileRAWReaderConfig {
  STsdb  *tsdb;
  int32_t szPage;

  STFile file;
} SDataFileRAWReaderConfig;

typedef struct SDataFileRAWReader {
  SDataFileRAWReaderConfig config[1];

  struct {
    bool    opened;
    int64_t offset;
  } ctx[1];

  STsdbFD *fd;
} SDataFileRAWReader;

typedef TARRAY2(SDataFileRAWReader *) SDataFileRAWReaderArray;

int32_t tsdbDataFileRAWReaderOpen(const char *fname, const SDataFileRAWReaderConfig *config,
                                  SDataFileRAWReader **reader);
int32_t tsdbDataFileRAWReaderClose(SDataFileRAWReader **reader);

int32_t tsdbDataFileRAWReadBlockData(SDataFileRAWReader *reader, STsdbDataRAWBlockHeader *bHdr);

// SDataFileRAWWriter =============================================
typedef struct SDataFileRAWWriterConfig {
  STsdb  *tsdb;
  int32_t szPage;

  SDiskID did;
  int64_t fid;
  int64_t cid;
  int32_t level;

  STFile file;
} SDataFileRAWWriterConfig;

typedef struct SDataFileRAWWriter {
  SDataFileRAWWriterConfig config[1];

  struct {
    bool    opened;
    int64_t offset;
  } ctx[1];

  STFile   file;
  STsdbFD *fd;
} SDataFileRAWWriter;

typedef struct SDataFileRAWWriter SDataFileRAWWriter;

int32_t tsdbDataFileRAWWriterOpen(const SDataFileRAWWriterConfig *config, SDataFileRAWWriter **writer);
int32_t tsdbDataFileRAWWriterClose(SDataFileRAWWriter **writer, bool abort, TFileOpArray *opArr);

int32_t tsdbDataFileRAWWriterDoOpen(SDataFileRAWWriter *writer);
int32_t tsdbDataFileRAWWriteBlockData(SDataFileRAWWriter *writer, const STsdbDataRAWBlockHeader *bHdr);
int32_t tsdbDataFileRAWFlush(SDataFileRAWWriter *writer);

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_DATA_FILE_RAW_H*/
