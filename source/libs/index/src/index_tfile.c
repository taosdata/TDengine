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

//#include <sys/types.h>
//#include <dirent.h>
#include "index_tfile.h"
#include "index.h"
#include "index_fst.h"
#include "index_fst_counting_writer.h"
#include "index_util.h"
#include "taosdef.h"

static FORCE_INLINE int tfileReadLoadHeader(TFileReader *reader) {
  // TODO simple tfile header later
  char             buf[TFILE_HADER_PRE_SIZE];
  char *           p = buf;
  TFileReadHeader *header = &reader->header;
  int64_t          nread = reader->ctx->read(reader->ctx, buf, TFILE_HADER_PRE_SIZE);
  assert(nread == TFILE_HADER_PRE_SIZE);

  memcpy(&header->suid, p, sizeof(header->suid));
  p += sizeof(header->suid);

  memcpy(&header->version, p, sizeof(header->version));
  p += sizeof(header->version);

  int32_t colLen = 0;
  memcpy(&colLen, p, sizeof(colLen));
  assert(colLen < sizeof(header->colName));
  nread = reader->ctx->read(reader->ctx, header->colName, colLen);
  assert(nread == colLen);

  nread = reader->ctx->read(reader->ctx, &header->colType, sizeof(header->colType));
  return 0;
};
static int tfileGetFileList(const char *path, SArray *result) {
  DIR *dir = opendir(path);
  if (NULL == dir) {
    return -1;
  }

  struct dirent *entry;
  while ((entry = readdir(dir)) != NULL) {
    size_t len = strlen(entry->d_name);
    char * buf = calloc(1, len + 1);
    memcpy(buf, entry->d_name, len);
    taosArrayPush(result, &buf);
  }
  closedir(dir);
  return 0;
}
static void tfileDestroyFileName(void *elem) {
  char *p = *(char **)elem;
  free(p);
}
static int tfileCompare(const void *a, const void *b) {
  const char *aName = *(char **)a;
  const char *bName = *(char **)b;
  size_t      aLen = strlen(aName);
  size_t      bLen = strlen(bName);
  return strncmp(aName, bName, aLen > bLen ? aLen : bLen);
}
// tfile name suid-colId-version.tindex
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
  if (tcache == NULL) {
    return NULL;
  }

  tcache->tableCache = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  tcache->capacity = 64;

  SArray *files = taosArrayInit(4, sizeof(void *));
  tfileGetFileList(path, files);
  taosArraySort(files, tfileCompare);
  for (size_t i = 0; i < taosArrayGetSize(files); i++) {
    char *   file = taosArrayGetP(files, i);
    uint64_t suid;
    int      colId, version;
    if (0 != tfileParseFileName(file, &suid, &colId, &version)) {
      goto End;
      continue;
    }

    WriterCtx *wc = writerCtxCreate(TFile, file, true, 1024 * 64);
    if (wc == NULL) {
      indexError("failed to open index:  %s", file);
      goto End;
    }
    TFileReader *reader = tfileReaderCreate(wc);
    if (0 != tfileReadLoadHeader(reader)) {
      TFileReaderDestroy(reader);
      indexError("failed to load index header, index Id: %s", file);
      goto End;
    }
  }
  taosArrayDestroyEx(files, tfileDestroyFileName);
  return tcache;
End:
  tfileCacheDestroy(tcache);
  taosArrayDestroyEx(files, tfileDestroyFileName);
  return NULL;
}
void tfileCacheDestroy(TFileCache *tcache) {
  if (tcache == NULL) {
    return;
  }

  // free table cache
  TFileReader **reader = taosHashIterate(tcache->tableCache, NULL);
  while (reader) {
    TFileReader *p = *reader;
    indexInfo("drop table cache suid: %" PRIu64 ", colName: %s, colType: %d", p->header.suid, p->header.colName,
        p->header.colType);
    TFileReaderDestroy(p);
    reader = taosHashIterate(tcache->tableCache, reader);
  }
  taosHashCleanup(tcache->tableCache);
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

TFileReader *tfileReaderCreate(WriterCtx *ctx) {
  TFileReader *reader = calloc(1, sizeof(TFileReader));
  if (reader == NULL) {
    return NULL;
  }
  reader->ctx = ctx;
  // T_REF_INC(reader);
  return reader;
}
void TFileReaderDestroy(TFileReader *reader) {
  if (reader == NULL) {
    return;
  }
  // T_REF_INC(reader);
  writerCtxDestroy(reader->ctx);
  free(reader);
}

TFileWriter *tfileWriterCreate(const char *suid, const char *colName);
void         tfileWriterDestroy(TFileWriter *tw);

IndexTFile *indexTFileCreate(const char *path) {
  IndexTFile *tfile = calloc(1, sizeof(IndexTFile));
  tfile->cache = tfileCacheCreate(path);

  return tfile;
}
void IndexTFileDestroy(IndexTFile *tfile) { free(tfile); }

int indexTFileSearch(void *tfile, SIndexTermQuery *query, SArray *result) {
  IndexTFile *pTfile = (IndexTFile *)tfile;

  SIndexTerm *  term = query->term;
  TFileCacheKey key = {
      .suid = term->suid, .colType = term->colType, .version = 0, .colName = term->colName, .nColName = term->nColName};
  TFileReader *reader = tfileCacheGet(pTfile->cache, &key);
  return 0;
}
int indexTFilePut(void *tfile, SIndexTerm *term, uint64_t uid) {
  TFileWriterOpt wOpt = {
      .suid = term->suid, .colType = term->colType, .colName = term->colName, .nColName = term->nColName, .version = 1};

  return 0;
}
