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
#include "tcompare.h"

#define TF_TABLE_TATOAL_SIZE(sz) (sizeof(sz) + sz * sizeof(uint64_t))

typedef struct TFileValue {
  char*   colVal;  // null terminated
  SArray* tableId;
  int32_t offset;
} TFileValue;

static int  tfileValueCompare(const void* a, const void* b, const void* param);
static void tfileSerialTableIdsToBuf(char* buf, SArray* tableIds);

static int tfileWriteFstOffset(TFileWriter* tw, int32_t offset);
static int tfileWriteHeader(TFileWriter* writer);
static int tfileWriteData(TFileWriter* write, TFileValue* tval);

static int tfileReadLoadHeader(TFileReader* reader);

static int  tfileGetFileList(const char* path, SArray* result);
static void tfileDestroyFileName(void* elem);
static int  tfileCompare(const void* a, const void* b);
static int  tfileParseFileName(const char* filename, uint64_t* suid, int* colId, int* version);
static void tfileSerialCacheKey(TFileCacheKey* key, char* buf);
// static tfileGetCompareFunc(uint8_t byte) {}

TFileCache* tfileCacheCreate(const char* path) {
  TFileCache* tcache = calloc(1, sizeof(TFileCache));
  if (tcache == NULL) { return NULL; }

  tcache->tableCache = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  tcache->capacity = 64;

  SArray* files = taosArrayInit(4, sizeof(void*));
  tfileGetFileList(path, files);
  taosArraySort(files, tfileCompare);
  uint64_t suid;
  int32_t  colId, version;
  for (size_t i = 0; i < taosArrayGetSize(files); i++) {
    char* file = taosArrayGetP(files, i);
    if (0 != tfileParseFileName(file, &suid, (int*)&colId, (int*)&version)) {
      indexInfo("try parse invalid file:  %s, skip it", file);
      continue;
    }
    WriterCtx* wc = writerCtxCreate(TFile, file, true, 1024 * 64);
    if (wc == NULL) {
      indexError("failed to open index:  %s", file);
      goto End;
    }
    TFileReader* reader = tfileReaderCreate(wc);
    if (0 != tfileReadLoadHeader(reader)) {
      tfileReaderDestroy(reader);
      indexError("failed to load index header, index Id: %s", file);
      goto End;
    }
    // loader fst and validate it

    TFileHeader*  header = &reader->header;
    TFileCacheKey key = {.suid = header->suid,
                         .version = header->version,
                         .colName = header->colName,
                         .nColName = strlen(header->colName),
                         .colType = header->colType};

    char buf[128] = {0};
    tfileSerialCacheKey(&key, buf);
    taosHashPut(tcache->tableCache, buf, strlen(buf), &reader, sizeof(void*));
  }
  taosArrayDestroyEx(files, tfileDestroyFileName);
  return tcache;
End:
  tfileCacheDestroy(tcache);
  taosArrayDestroyEx(files, tfileDestroyFileName);
  return NULL;
}
void tfileCacheDestroy(TFileCache* tcache) {
  if (tcache == NULL) { return; }

  // free table cache
  TFileReader** reader = taosHashIterate(tcache->tableCache, NULL);
  while (reader) {
    TFileReader* p = *reader;
    indexInfo("drop table cache suid: %" PRIu64 ", colName: %s, colType: %d", p->header.suid, p->header.colName, p->header.colType);

    tfileReaderDestroy(p);
    reader = taosHashIterate(tcache->tableCache, reader);
  }
  taosHashCleanup(tcache->tableCache);
  free(tcache);
}

TFileReader* tfileCacheGet(TFileCache* tcache, TFileCacheKey* key) {
  char buf[128] = {0};
  tfileSerialCacheKey(key, buf);
  TFileReader* reader = taosHashGet(tcache->tableCache, buf, strlen(buf));
  return reader;
}
void tfileCachePut(TFileCache* tcache, TFileCacheKey* key, TFileReader* reader) {
  char buf[128] = {0};
  tfileSerialCacheKey(key, buf);
  taosHashPut(tcache->tableCache, buf, strlen(buf), &reader, sizeof(void*));
  return;
}

TFileReader* tfileReaderCreate(WriterCtx* ctx) {
  TFileReader* reader = calloc(1, sizeof(TFileReader));
  if (reader == NULL) { return NULL; }

  // T_REF_INC(reader);
  reader->ctx = ctx;
  return reader;
}
void tfileReaderDestroy(TFileReader* reader) {
  if (reader == NULL) { return; }
  // T_REF_INC(reader);
  writerCtxDestroy(reader->ctx);
  free(reader);
}

int tfileReaderSearch(TFileReader* reader, SIndexTermQuery* query, SArray* result) {
  SIndexTerm* term = query->term;
  // refactor to callback later
  if (query->qType == QUERY_TERM) {
    uint64_t offset;
    FstSlice key = fstSliceCreate(term->colVal, term->nColVal);
    if (fstGet(reader->fst, &key, &offset)) {
      //
    } else {
      indexInfo("index: %" PRIu64 ", col: %s, colVal: %s, not found in tindex", term->suid, term->colName, term->colVal);
    }
    return 0;
  } else if (query->qType == QUERY_PREFIX) {
    //
    //
  }
  return 0;
}

TFileWriter* tfileWriterCreate(WriterCtx* ctx, TFileHeader* header) {
  // char pathBuf[128] = {0};
  // sprintf(pathBuf, "%s/% " PRIu64 "-%d-%d.tindex", path, suid, colId, version);
  // TFileHeader header = {.suid = suid, .version = version, .colName = {0}, colType = colType};
  // memcpy(header.colName, );

  // char buf[TFILE_HADER_PRE_SIZE];
  // int  len = TFILE_HADER_PRE_SIZE;
  // if (len != ctx->write(ctx, buf, len)) {
  //  indexError("index: %" PRIu64 " failed to write header info", header->suid);
  //  return NULL;
  //}
  TFileWriter* tw = calloc(1, sizeof(TFileWriter));
  if (tw == NULL) {
    indexError("index: %" PRIu64 " failed to alloc TFilerWriter", header->suid);
    return NULL;
  }
  tw->ctx = ctx;
  tw->header = *header;
  tfileWriteHeader(tw);
  return tw;
}

int tfileWriterPut(TFileWriter* tw, void* data) {
  // sort by coltype and write to tindex
  __compar_fn_t fn = getComparFunc(tw->header.colType, 0);
  taosArraySortPWithExt((SArray*)(data), tfileValueCompare, &fn);

  int32_t bufLimit = 4096, offset = 0;
  char*   buf = calloc(1, sizeof(bufLimit));
  char*   p = buf;
  int32_t sz = taosArrayGetSize((SArray*)data);
  int32_t fstOffset = tw->offset;

  // ugly code, refactor later
  for (size_t i = 0; i < sz; i++) {
    TFileValue* v = taosArrayGetP((SArray*)data, i);

    int32_t tbsz = taosArrayGetSize(v->tableId);
    int32_t ttsz = TF_TABLE_TATOAL_SIZE(tbsz);
    fstOffset += ttsz;
  }
  // check result or not
  tfileWriteFstOffset(tw, fstOffset);
  // tw->ctx->header.fstOffset = fstOffset;
  // tw->ctx->write(tw->ctx, &fstOffset, sizeof(fstOffset));

  for (size_t i = 0; i < sz; i++) {
    TFileValue* v = taosArrayGetP((SArray*)data, i);

    int32_t tbsz = taosArrayGetSize(v->tableId);
    // check buf has enough space or not
    int32_t ttsz = TF_TABLE_TATOAL_SIZE(tbsz);
    if (offset + ttsz > bufLimit) {
      // batch write
      tw->ctx->write(tw->ctx, buf, offset);
      offset = 0;
      memset(buf, 0, bufLimit);
      p = buf;
    }

    tfileSerialTableIdsToBuf(p, v->tableId);
    offset += ttsz;
    p = buf + offset;
    // set up value offset
    v->offset = tw->offset;
    tw->offset += ttsz;
  }
  if (offset != 0) {
    // write reversed data in buf to tindex
    tw->ctx->write(tw->ctx, buf, offset);
  }

  // write fst
  for (size_t i = 0; i < sz; i++) {
    // TODO, fst batch write later
    TFileValue* v = taosArrayGetP((SArray*)data, i);
    if (tfileWriteData(tw, v) == 0) {
      //
      //
    }
  }

  tfree(buf);
  return 0;
}
void tfileWriterDestroy(TFileWriter* tw) {
  if (tw == NULL) { return; }

  writerCtxDestroy(tw->ctx);
  free(tw);
}

IndexTFile* indexTFileCreate(const char* path) {
  IndexTFile* tfile = calloc(1, sizeof(IndexTFile));
  if (tfile == NULL) { return NULL; }

  tfile->cache = tfileCacheCreate(path);
  return tfile;
}
void IndexTFileDestroy(IndexTFile* tfile) {
  free(tfile);
}

int indexTFileSearch(void* tfile, SIndexTermQuery* query, SArray* result) {
  if (tfile == NULL) { return -1; }
  IndexTFile* pTfile = (IndexTFile*)tfile;

  SIndexTerm*   term = query->term;
  TFileCacheKey key = {.suid = term->suid, .colType = term->colType, .version = 0, .colName = term->colName, .nColName = term->nColName};

  TFileReader* reader = tfileCacheGet(pTfile->cache, &key);
  return tfileReaderSearch(reader, query, result);
}
int indexTFilePut(void* tfile, SIndexTerm* term, uint64_t uid) {
  TFileWriterOpt wOpt = {.suid = term->suid, .colType = term->colType, .colName = term->colName, .nColName = term->nColName, .version = 1};

  return 0;
}

static int tfileValueCompare(const void* a, const void* b, const void* param) {
  __compar_fn_t fn = *(__compar_fn_t*)param;

  TFileValue* av = (TFileValue*)a;
  TFileValue* bv = (TFileValue*)b;

  return fn(av->colVal, bv->colVal);
}
static void tfileSerialTableIdsToBuf(char* buf, SArray* tableIds) {
  int tbSz = taosArrayGetSize(tableIds);
  SERIALIZE_VAR_TO_BUF(buf, tbSz, int32_t);
  for (size_t i = 0; i < tbSz; i++) {
    uint64_t* v = taosArrayGet(tableIds, i);
    SERIALIZE_VAR_TO_BUF(buf, *v, uint64_t);
  }
}

static int tfileWriteFstOffset(TFileWriter* tw, int32_t offset) {
  int32_t fstOffset = offset + sizeof(tw->header.fstOffset);
  tw->header.fstOffset = fstOffset;
  if (sizeof(fstOffset) != tw->ctx->write(tw->ctx, (char*)&fstOffset, sizeof(fstOffset))) { return -1; }
  return 0;
}
static int tfileWriteHeader(TFileWriter* writer) {
  char  buf[TFILE_HEADER_NO_FST] = {0};
  char* p = buf;

  TFileHeader* header = &writer->header;
  memcpy(buf, (char*)header, sizeof(buf));

  int nwrite = writer->ctx->write(writer->ctx, buf, sizeof(buf));
  if (sizeof(buf) != nwrite) { return -1; }
  writer->offset = nwrite;
  return 0;
}
static int tfileWriteData(TFileWriter* write, TFileValue* tval) {
  TFileHeader* header = &write->header;
  uint8_t      colType = header->colType;
  if (colType == TSDB_DATA_TYPE_BINARY || colType == TSDB_DATA_TYPE_NCHAR) {
    FstSlice key = fstSliceCreate((uint8_t*)(tval->colVal), (size_t)strlen(tval->colVal));
    if (fstBuilderInsert(write->fb, key, tval->offset)) {
      fstSliceDestroy(&key);
      return 0;
    }
    fstSliceDestroy(&key);
    return -1;
  } else {
    // handle other type later
  }
}
static int tfileReadLoadHeader(TFileReader* reader) {
  // TODO simple tfile header later
  char  buf[TFILE_HEADER_SIZE] = {0};
  char* p = buf;

  int64_t nread = reader->ctx->read(reader->ctx, buf, sizeof(buf));
  assert(nread == sizeof(buf));
  memcpy(&reader->header, buf, sizeof(buf));
  return 0;
}

static int tfileGetFileList(const char* path, SArray* result) {
  DIR* dir = opendir(path);
  if (NULL == dir) { return -1; }

  struct dirent* entry;
  while ((entry = readdir(dir)) != NULL) {
    size_t len = strlen(entry->d_name);
    char*  buf = calloc(1, len + 1);
    memcpy(buf, entry->d_name, len);
    taosArrayPush(result, &buf);
  }
  closedir(dir);
  return 0;
}
static void tfileDestroyFileName(void* elem) {
  char* p = *(char**)elem;
  free(p);
}
static int tfileCompare(const void* a, const void* b) {
  const char* aName = *(char**)a;
  const char* bName = *(char**)b;

  size_t aLen = strlen(aName);
  size_t bLen = strlen(bName);

  return strncmp(aName, bName, aLen > bLen ? aLen : bLen);
}
// tfile name suid-colId-version.tindex
static int tfileParseFileName(const char* filename, uint64_t* suid, int* colId, int* version) {
  if (3 == sscanf(filename, "%" PRIu64 "-%d-%d.tindex", suid, colId, version)) {
    // read suid & colid & version  success
    return 0;
  }
  return -1;
}
static void tfileSerialCacheKey(TFileCacheKey* key, char* buf) {
  SERIALIZE_MEM_TO_BUF(buf, key, suid);
  SERIALIZE_VAR_TO_BUF(buf, '_', char);
  SERIALIZE_MEM_TO_BUF(buf, key, colType);
  SERIALIZE_VAR_TO_BUF(buf, '_', char);
  SERIALIZE_MEM_TO_BUF(buf, key, version);
  SERIALIZE_VAR_TO_BUF(buf, '_', char);
  SERIALIZE_STR_MEM_TO_BUF(buf, key, colName, key->nColName);
}
