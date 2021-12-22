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
#ifndef __INDEX_TFILE_H__
#define __INDEX_TFILE_H__

#include "index.h"
#include "indexInt.h"
#include "index_fst.h"
#include "index_fst_counting_writer.h"
#include "index_tfile.h"
#include "tlockfree.h"

#ifdef __cplusplus
extern "C" {
#endif

// tfile header content
// |<---suid--->|<---version--->|<--colLen-->|<-colName->|<---type-->|
// |<-uint64_t->|<---int32_t--->|<--int32_t->|<-colLen-->|<-uint8_t->|

typedef struct TFileHeader {
  uint64_t suid;
  int32_t  version;
  char     colName[128];  //
  uint8_t  colType;
} TFileHeader;

#define TFILE_HEADER_SIZE (sizeof(TFileHeader) + sizeof(uint32_t))
#define TFILE_HADER_PRE_SIZE (sizeof(uint64_t) + sizeof(int32_t) + sizeof(int32_t))

typedef struct TFileCacheKey {
  uint64_t suid;
  uint8_t  colType;
  int32_t  version;
  char*    colName;
  int32_t  nColName;
} TFileCacheKey;

// table cache
// refactor to LRU cache later
typedef struct TFileCache {
  SHashObj* tableCache;
  int16_t   capacity;
  // add more param
} TFileCache;

typedef struct TFileWriter {
  FstBuilder* fb;
  WriterCtx*  ctx;
  TFileHeader header;
  uint32_t    offset;
} TFileWriter;

typedef struct TFileReader {
  T_REF_DECLARE()
  Fst*        fst;
  WriterCtx*  ctx;
  TFileHeader header;
} TFileReader;

typedef struct IndexTFile {
  char*        path;
  TFileCache*  cache;
  TFileWriter* tw;
} IndexTFile;

typedef struct TFileWriterOpt {
  uint64_t suid;
  int8_t   colType;
  char*    colName;
  int32_t  nColName;
  int32_t  version;
} TFileWriterOpt;

typedef struct TFileReaderOpt {
  uint64_t suid;
  char*    colName;
  int32_t  nColName;
} TFileReaderOpt;

// tfile cache, manage tindex reader
TFileCache*  tfileCacheCreate(const char* path);
void         tfileCacheDestroy(TFileCache* tcache);
TFileReader* tfileCacheGet(TFileCache* tcache, TFileCacheKey* key);
void         tfileCachePut(TFileCache* tcache, TFileCacheKey* key, TFileReader* reader);

TFileReader* tfileReaderCreate(WriterCtx* ctx);
void         tfileReaderDestroy(TFileReader* reader);
int          tfileReaderSearch(TFileReader* reader, SIndexTermQuery* query, SArray* result);

TFileWriter* tfileWriterCreate(WriterCtx* ctx, TFileHeader* header);
void         tfileWriterDestroy(TFileWriter* tw);
int          tfileWriterPut(TFileWriter* tw, void* data);
int          tfileWriterFinish(TFileWriter* tw);

//
IndexTFile* indexTFileCreate(const char* path);
int         indexTFilePut(void* tfile, SIndexTerm* term, uint64_t uid);
int         indexTFileSearch(void* tfile, SIndexTermQuery* query, SArray* result);

#ifdef __cplusplus
}

#endif

#endif
