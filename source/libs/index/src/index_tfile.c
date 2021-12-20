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

#include "index_tfile.h"
#include "index_fst.h"


#define SERIALIZE_TO_BUF(buf, key, mem) \
  do { \
   memcpy(buf, &key->mem, sizeof(key->mem)); \
   buf += sizeof(key->mem);  \
  } while (0)

#define SERIALIZE_STR_TO_BUF(buf, key, mem, len) \
  do { \
    memcpy(buf, key->mem, len); \
    buf += len; \
  } while (0)

#define SERIALIZE_DELIMITER_TO_BUF(buf, delim) \
  do { \
    char c = delim; \
    memcpy(buf, &c, sizeof(c)); \
    buf += sizeof(c); \
  } while (0)


static void tfileSerialCacheKey(TFileCacheKey *key, char *buf) {
  SERIALIZE_TO_BUF(buf, key, suid);
  SERIALIZE_DELIMITER_TO_BUF(buf, '_'); 
  SERIALIZE_TO_BUF(buf, key, colType);
  SERIALIZE_DELIMITER_TO_BUF(buf, '_'); 
  SERIALIZE_TO_BUF(buf, key, version);
  SERIALIZE_DELIMITER_TO_BUF(buf, '_'); 
  SERIALIZE_STR_TO_BUF(buf, key, colName,  key->nColName);
}

TFileCache *tfileCacheCreate() {
  TFileCache *tcache = calloc(1, sizeof(TFileCache)); 
  if (tcache == NULL) { return NULL; }
  
  tcache->tableCache = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK); 
  tcache->capacity   = 64;
  return tcache;
}
void tfileCacheDestroy(TFileCache *tcache) {
  
}

TFileReader *tfileCacheGet(TFileCache *tcache, TFileCacheKey *key) {
  char buf[128] = {0}; 
  tfileSerialCacheKey(key, buf);
  TFileReader *reader = taosHashGet(tcache->tableCache, buf, strlen(buf)); 
  return reader; 
}
void tfileCachePut(TFileCache *tcache, TFileCacheKey *key, TFileReader *reader) {
  char buf[128] = {0};
  tfileSerialCacheKey(key, buf);
  taosHashPut(tcache->tableCache, buf, strlen(buf), &reader, sizeof(void *));     
  return;
} 




IndexTFile *indexTFileCreate() {
  IndexTFile *tfile = calloc(1, sizeof(IndexTFile));   
  return tfile;
}
void IndexTFileDestroy(IndexTFile *tfile) {
  free(tfile); 
}


int indexTFileSearch(void *tfile, SIndexTermQuery *query, SArray *result) {
  IndexTFile *ptfile = (IndexTFile *)tfile;
   
  return 0;
}
int indexTFilePut(void *tfile, SIndexTerm *term,  uint64_t uid) {
  TFileWriterOpt wOpt = {.suid    = term->suid,
                         .colType = term->colType,
                         .colName = term->colName,
                         .nColName= term->nColName,
                         .version = 1}; 

  
   
  return 0; 
}



