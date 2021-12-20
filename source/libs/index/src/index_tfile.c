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

#include <sys/types.h>
#include <dirent.h>
#include "index_tfile.h"
#include "index_fst.h"
#include "index_util.h"


// tfile name suid-colId-version.tindex
static int tfileGetFileList(const char *path, SArray *result) {
  DIR *dir = opendir(path);  
  if (NULL == dir) { return -1; } 

  struct dirent *entry;
  while ((entry = readdir(dir)) != NULL) {
    size_t len = strlen(entry->d_name);
    char *buf = calloc(1, len + 1); 
    memcpy(buf, entry->d_name, len); 
    taosArrayPush(result, &buf); 
  }
  closedir(dir);
  return 0;
} 
static int tfileCompare(const void *a, const void *b) {
  const char *aName = *(char **)a;
  const char *bName = *(char **)b;
  size_t aLen = strlen(aName);
  size_t bLen = strlen(bName);
  return strncmp(aName, bName, aLen > bLen ? aLen : bLen);
}
static int tfileParseFileName(const char *filename, uint64_t *suid, int *colId, int *version) {
  if (3 == sscanf(filename, "%" PRIu64 "-%d-%d.tindex", suid, colId, version)) {
    // read suid & colid & version  success
    return 0;
  }  
  return -1;  
} 
static void tfileSerialCacheKey(TFileCacheKey *key, char *buf) {
  SERIALIZE_MEM_TO_BUF(buf, key, suid);
  SERIALIZE_VAR_TO_BUF(buf, '_', char); 
  SERIALIZE_MEM_TO_BUF(buf, key, colType);
  SERIALIZE_VAR_TO_BUF(buf, '_', char); 
  SERIALIZE_MEM_TO_BUF(buf, key, version);
  SERIALIZE_VAR_TO_BUF(buf, '_', char); 
  SERIALIZE_STR_MEM_TO_BUF(buf, key, colName, key->nColName);
}

TFileCache *tfileCacheCreate(const char *path) {
  TFileCache *tcache = calloc(1, sizeof(TFileCache)); 
  if (tcache == NULL) { return NULL; }
  
  tcache->tableCache = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK); 
  tcache->capacity   = 64;
  
  SArray *files  = taosArrayInit(4, sizeof(void *));   
  tfileGetFileList(path, files);   
  taosArraySort(files, tfileCompare);
  for (size_t i = 0; i < taosArrayGetSize(files); i++) {
    char *file = taosArrayGetP(files, i); 
    uint64_t suid; 
    int colId, version;
    if (0 != tfileParseFileName(file, &suid, &colId, &version)) {
      // invalid file, just skip 
      continue; 
    } 
    free((void *)file);    
  }
  taosArrayDestroy(files);
  
  return tcache;
}
void tfileCacheDestroy(TFileCache *tcache) {
  
  free(tcache);    
   
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




IndexTFile *indexTFileCreate(const char *path) {
  IndexTFile *tfile = calloc(1, sizeof(IndexTFile));   
  tfile->cache = tfileCacheCreate(path);    
  
  
  return tfile;
}
void IndexTFileDestroy(IndexTFile *tfile) {
  free(tfile); 
}


int indexTFileSearch(void *tfile, SIndexTermQuery *query, SArray *result) {
  IndexTFile *pTfile = (IndexTFile *)tfile;
  
  SIndexTerm *term = query->term;
  TFileCacheKey key = {.suid    = term->suid,
                       .colType = term->colType,
                       .version = 0,
                       .colName = term->colName, 
                       .nColName= term->nColName};
  TFileReader *reader = tfileCacheGet(pTfile->cache, &key);    
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



