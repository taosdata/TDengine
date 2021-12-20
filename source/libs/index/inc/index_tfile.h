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
#include "tlockfree.h"
#include "index_tfile.h"
#include "index_fst_counting_writer.h"
#include "index_fst.h"

#ifdef __cplusplus
extern "C" {
#endif

  

typedef struct TFileCacheKey {
  uint64_t   suid;
  uint8_t    colType;
  int32_t    version;
  const char *colName;
  int32_t    nColName;
} TFileCacheKey; 


// table cache
// refactor to LRU cache later
typedef struct TFileCache {
  SHashObj *tableCache;        
  int16_t  capacity;
  // add more param  
} TFileCache;


typedef struct TFileWriter {
  FstBuilder *fb;
  WriterCtx  *wc; 
} TFileWriter;

typedef struct TFileReader {
  T_REF_DECLARE() 
  Fst *fst;
  
} TFileReader; 

typedef struct IndexTFile {
  char *path;
  TFileReader *tb;
  TFileWriter *tw;      
} IndexTFile;

typedef struct TFileWriterOpt {
  uint64_t suid;
  int8_t   colType;
  char     *colName;  
  int32_t  nColName; 
  int32_t  version;
} TFileWriterOpt; 

typedef struct TFileReaderOpt {
  uint64_t suid; 
  char     *colName;
  int32_t  nColName; 
  
} TFileReaderOpt;

// tfile cache 
TFileCache *tfileCacheCreate();
void tfileCacheDestroy(TFileCache *tcache);
TFileReader* tfileCacheGet(TFileCache *tcache, TFileCacheKey *key);
void tfileCachePut(TFileCache *tcache, TFileCacheKey *key, TFileReader *reader);
  
TFileWriter *tfileWriterCreate(const char *suid, const char *colName);

IndexTFile *indexTFileCreate();

int indexTFilePut(void *tfile, SIndexTerm *term,  uint64_t uid); 

int indexTFileSearch(void *tfile, SIndexTermQuery *query, SArray *result);


#ifdef __cplusplus
}


#endif



#endif
