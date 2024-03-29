/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#include "indexTfile.h"
#include "index.h"
#include "indexComm.h"
#include "indexFst.h"
#include "indexFstFile.h"
#include "indexUtil.h"
#include "taosdef.h"
#include "taoserror.h"
#include "tcoding.h"
#include "tcompare.h"

const static uint64_t FILE_MAGIC_NUMBER = 0xdb4775248b80fb57ull;

typedef struct TFileFstIter {
  FStmBuilder* fb;
  FStmSt*      st;
  FAutoCtx*    ctx;
  TFileReader* rdr;
} TFileFstIter;

#define TF_TABLE_TATOAL_SIZE(sz) (sizeof(sz) + sz * sizeof(uint64_t))

static int  tfileStrCompare(const void* a, const void* b);
static int  tfileValueCompare(const void* a, const void* b, const void* param);
static void tfileSerialTableIdsToBuf(char* buf, SArray* tableIds);

static int tfileWriteHeader(TFileWriter* writer);
static int tfileWriteFstOffset(TFileWriter* tw, int32_t offset);
static int tfileWriteData(TFileWriter* write, TFileValue* tval);
static int tfileWriteFooter(TFileWriter* write);

// handle file corrupt later
static int tfileReaderLoadHeader(TFileReader* reader);
static int tfileReaderLoadFst(TFileReader* reader);
static int tfileReaderVerify(TFileReader* reader);
static int tfileReaderLoadTableIds(TFileReader* reader, int32_t offset, SArray* result);

static SArray* tfileGetFileList(const char* path);
static int     tfileRmExpireFile(SArray* result);
static void    tfileDestroyFileName(void* elem);
static int     tfileCompare(const void* a, const void* b);
static int     tfileParseFileName(const char* filename, uint64_t* suid, char* col, int64_t* version);
static void    tfileGenFileName(char* filename, uint64_t suid, const char* col, int64_t version);
static void    tfileGenFileFullName(char* fullname, const char* path, uint64_t suid, const char* col, int64_t version);
/*
 * search from  tfile
 */
static int32_t tfSearchTerm(void* reader, SIndexTerm* tem, SIdxTRslt* tr);
static int32_t tfSearchPrefix(void* reader, SIndexTerm* tem, SIdxTRslt* tr);
static int32_t tfSearchSuffix(void* reader, SIndexTerm* tem, SIdxTRslt* tr);
static int32_t tfSearchRegex(void* reader, SIndexTerm* tem, SIdxTRslt* tr);
static int32_t tfSearchLessThan(void* reader, SIndexTerm* tem, SIdxTRslt* tr);
static int32_t tfSearchLessEqual(void* reader, SIndexTerm* tem, SIdxTRslt* tr);
static int32_t tfSearchGreaterThan(void* reader, SIndexTerm* tem, SIdxTRslt* tr);
static int32_t tfSearchGreaterEqual(void* reader, SIndexTerm* tem, SIdxTRslt* tr);
static int32_t tfSearchRange(void* reader, SIndexTerm* tem, SIdxTRslt* tr);

static int32_t tfSearchCompareFunc(void* reader, SIndexTerm* tem, SIdxTRslt* tr, RangeType ctype);

static int32_t tfSearchTerm_JSON(void* reader, SIndexTerm* tem, SIdxTRslt* tr);
static int32_t tfSearchEqual_JSON(void* reader, SIndexTerm* tem, SIdxTRslt* tr);
static int32_t tfSearchPrefix_JSON(void* reader, SIndexTerm* tem, SIdxTRslt* tr);
static int32_t tfSearchSuffix_JSON(void* reader, SIndexTerm* tem, SIdxTRslt* tr);
static int32_t tfSearchRegex_JSON(void* reader, SIndexTerm* tem, SIdxTRslt* tr);
static int32_t tfSearchLessThan_JSON(void* reader, SIndexTerm* tem, SIdxTRslt* tr);
static int32_t tfSearchLessEqual_JSON(void* reader, SIndexTerm* tem, SIdxTRslt* tr);
static int32_t tfSearchGreaterThan_JSON(void* reader, SIndexTerm* tem, SIdxTRslt* tr);
static int32_t tfSearchGreaterEqual_JSON(void* reader, SIndexTerm* tem, SIdxTRslt* tr);
static int32_t tfSearchRange_JSON(void* reader, SIndexTerm* tem, SIdxTRslt* tr);

static int32_t tfSearchCompareFunc_JSON(void* reader, SIndexTerm* tem, SIdxTRslt* tr, RangeType ctype);

static int32_t (*tfSearch[][QUERY_MAX])(void* reader, SIndexTerm* tem, SIdxTRslt* tr) = {
    {tfSearchTerm, tfSearchPrefix, tfSearchSuffix, tfSearchRegex, tfSearchLessThan, tfSearchLessEqual,
     tfSearchGreaterThan, tfSearchGreaterEqual, tfSearchRange},
    {tfSearchEqual_JSON, tfSearchPrefix_JSON, tfSearchSuffix_JSON, tfSearchRegex_JSON, tfSearchLessThan_JSON,
     tfSearchLessEqual_JSON, tfSearchGreaterThan_JSON, tfSearchGreaterEqual_JSON, tfSearchRange_JSON}};

TFileCache* tfileCacheCreate(SIndex* idx, const char* path) {
  TFileCache* tcache = taosMemoryCalloc(1, sizeof(TFileCache));
  if (tcache == NULL) {
    return NULL;
  }

  tcache->tableCache = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  tcache->capacity = 64;

  SArray* files = tfileGetFileList(path);
  for (size_t i = 0; i < taosArrayGetSize(files); i++) {
    char* file = taosArrayGetP(files, i);

    IFileCtx* ctx = idxFileCtxCreate(TFILE, file, true, 1024 * 1024 * 64);
    if (ctx == NULL) {
      indexError("failed to open index:%s", file);
      goto End;
    }
    ctx->lru = idx->lru;

    TFileReader* reader = tfileReaderCreate(ctx);
    if (reader == NULL) {
      indexInfo("skip invalid file: %s", file);
      continue;
    }
    reader->lru = idx->lru;

    TFileHeader* header = &reader->header;
    ICacheKey    key = {.suid = header->suid, .colName = header->colName, .nColName = (int32_t)strlen(header->colName)};

    char    buf[128] = {0};
    int32_t sz = idxSerialCacheKey(&key, buf);
    taosHashPut(tcache->tableCache, buf, sz, &reader, sizeof(void*));
    tfileReaderRef(reader);
  }
  taosArrayDestroyEx(files, tfileDestroyFileName);
  return tcache;
End:
  tfileCacheDestroy(tcache);
  taosArrayDestroyEx(files, tfileDestroyFileName);
  return NULL;
}
void tfileCacheDestroy(TFileCache* tcache) {
  if (tcache == NULL) {
    return;
  }
  // free table cache
  TFileReader** reader = taosHashIterate(tcache->tableCache, NULL);
  while (reader) {
    TFileReader* p = *reader;
    indexInfo("drop table cache suid:%" PRIu64 ", colName:%s, colType:%d", p->header.suid, p->header.colName,
              p->header.colType);
    tfileReaderUnRef(p);
    reader = taosHashIterate(tcache->tableCache, reader);
  }
  taosHashCleanup(tcache->tableCache);
  taosMemoryFree(tcache);
}

TFileReader* tfileCacheGet(TFileCache* tcache, ICacheKey* key) {
  char          buf[128] = {0};
  int32_t       sz = idxSerialCacheKey(key, buf);
  TFileReader** reader = taosHashGet(tcache->tableCache, buf, sz);
  if (reader == NULL || *reader == NULL) {
    return NULL;
  }
  tfileReaderRef(*reader);

  return *reader;
}
void tfileCachePut(TFileCache* tcache, ICacheKey* key, TFileReader* reader) {
  char          buf[128] = {0};
  int32_t       sz = idxSerialCacheKey(key, buf);
  TFileReader** p = taosHashGet(tcache->tableCache, buf, sz);
  if (p != NULL && *p != NULL) {
    TFileReader* oldRdr = *p;
    taosHashRemove(tcache->tableCache, buf, sz);
    indexInfo("found %s, should remove file %s", buf, oldRdr->ctx->file.buf);
    oldRdr->remove = true;
    tfileReaderUnRef(oldRdr);
  }
  taosHashPut(tcache->tableCache, buf, sz, &reader, sizeof(void*));
  tfileReaderRef(reader);
  return;
}
TFileReader* tfileReaderCreate(IFileCtx* ctx) {
  TFileReader* reader = taosMemoryCalloc(1, sizeof(TFileReader));
  if (reader == NULL) {
    return NULL;
  }
  reader->ctx = ctx;
  reader->remove = false;

  if (0 != tfileReaderVerify(reader)) {
    indexError("invalid tfile, suid:%" PRIu64 ", colName:%s", reader->header.suid, reader->header.colName);
    tfileReaderDestroy(reader);
    return NULL;
  }

  if (0 != tfileReaderLoadHeader(reader)) {
    indexError("failed to load index header, suid:%" PRIu64 ", colName:%s", reader->header.suid,
               reader->header.colName);
    tfileReaderDestroy(reader);
    return NULL;
  }

  if (0 != tfileReaderLoadFst(reader)) {
    indexError("failed to load index fst, suid:%" PRIu64 ", colName:%s, code:0x%x", reader->header.suid,
               reader->header.colName, errno);
    tfileReaderDestroy(reader);
    return NULL;
  }

  return reader;
}
void tfileReaderDestroy(TFileReader* reader) {
  if (reader == NULL) {
    return;
  }
  fstDestroy(reader->fst);
  if (reader->remove) {
    indexInfo("%s is removed", reader->ctx->file.buf);
  } else {
    indexInfo("%s is not removed", reader->ctx->file.buf);
  }
  idxFileCtxDestroy(reader->ctx, reader->remove);

  taosMemoryFree(reader);
}

static int32_t tfSearchTerm(void* reader, SIndexTerm* tem, SIdxTRslt* tr) {
  int      ret = 0;
  char*    p = tem->colVal;
  uint64_t sz = tem->nColVal;

  int64_t  st = taosGetTimestampUs();
  FstSlice key = fstSliceCreate(p, sz);
  uint64_t offset;
  if (fstGet(((TFileReader*)reader)->fst, &key, &offset)) {
    int64_t et = taosGetTimestampUs();
    int64_t cost = et - st;
    indexInfo("index: %" PRIu64 ", col: %s, colVal: %s, found table info in tindex, time cost: %" PRIu64 "us",
              tem->suid, tem->colName, tem->colVal, cost);

    ret = tfileReaderLoadTableIds((TFileReader*)reader, (int32_t)offset, tr->total);
    cost = taosGetTimestampUs() - et;
    indexInfo("index: %" PRIu64 ", col: %s, colVal: %s, load all table info, time cost: %" PRIu64 "us", tem->suid,
              tem->colName, tem->colVal, cost);
  }
  fstSliceDestroy(&key);
  return 0;
}

static int32_t tfSearchPrefix(void* reader, SIndexTerm* tem, SIdxTRslt* tr) {
  char*    p = tem->colVal;
  uint64_t sz = tem->nColVal;

  SArray* offsets = taosArrayInit(16, sizeof(uint64_t));

  FAutoCtx*    ctx = automCtxCreate((void*)p, AUTOMATION_PREFIX);
  FStmBuilder* sb = fstSearch(((TFileReader*)reader)->fst, ctx);
  FStmSt*      st = stmBuilderIntoStm(sb);
  FStmStRslt*  rt = NULL;
  while ((rt = stmStNextWith(st, NULL)) != NULL) {
    taosArrayPush(offsets, &(rt->out.out));
    swsResultDestroy(rt);
  }
  stmStDestroy(st);
  stmBuilderDestroy(sb);

  int32_t ret = 0;
  for (int i = 0; i < taosArrayGetSize(offsets); i++) {
    uint64_t offset = *(uint64_t*)taosArrayGet(offsets, i);
    ret = tfileReaderLoadTableIds((TFileReader*)reader, offset, tr->total);
    if (ret != 0) {
      taosArrayDestroy(offsets);
      indexError("failed to find target tablelist");
      return TSDB_CODE_FILE_CORRUPTED;
    }
  }
  taosArrayDestroy(offsets);
  return 0;
}
static int32_t tfSearchSuffix(void* reader, SIndexTerm* tem, SIdxTRslt* tr) {
  int      ret = 0;
  char*    p = tem->colVal;
  uint64_t sz = tem->nColVal;
  int64_t  st = taosGetTimestampUs();
  FstSlice key = fstSliceCreate(p, sz);
  fstSliceDestroy(&key);
  return 0;
}
static int32_t tfSearchRegex(void* reader, SIndexTerm* tem, SIdxTRslt* tr) {
  bool hasJson = IDX_TYPE_CONTAIN_EXTERN_TYPE(tem->colType, TSDB_DATA_TYPE_JSON);

  int      ret = 0;
  char*    p = tem->colVal;
  uint64_t sz = tem->nColVal;
  if (hasJson) {
    p = idxPackJsonData(tem);
    sz = strlen(p);
  }
  int64_t  st = taosGetTimestampUs();
  FstSlice key = fstSliceCreate(p, sz);
  /*impl later*/

  if (hasJson) {
    taosMemoryFree(p);
  }
  fstSliceDestroy(&key);
  return 0;
}

static int32_t tfSearchCompareFunc(void* reader, SIndexTerm* tem, SIdxTRslt* tr, RangeType type) {
  int                  ret = 0;
  char*                p = tem->colVal;
  int                  skip = 0;
  _cache_range_compare cmpFn = idxGetCompare(type);

  SArray* offsets = taosArrayInit(16, sizeof(uint64_t));

  FAutoCtx*    ctx = automCtxCreate((void*)p, AUTOMATION_ALWAYS);
  FStmBuilder* sb = fstSearch(((TFileReader*)reader)->fst, ctx);

  FstSlice h = fstSliceCreate((uint8_t*)p, skip);
  stmBuilderSetRange(sb, &h, type);
  fstSliceDestroy(&h);

  FStmSt*     st = stmBuilderIntoStm(sb);
  FStmStRslt* rt = NULL;
  while ((rt = stmStNextWith(st, NULL)) != NULL) {
    FstSlice* s = &rt->data;
    char*     ch = (char*)fstSliceData(s, NULL);

    TExeCond cond = cmpFn(ch, p, tem->colType);
    if (MATCH == cond) {
      tfileReaderLoadTableIds((TFileReader*)reader, rt->out.out, tr->total);
    } else if (CONTINUE == cond) {
    } else if (BREAK == cond) {
      swsResultDestroy(rt);
      break;
    }
    swsResultDestroy(rt);
  }
  stmStDestroy(st);
  stmBuilderDestroy(sb);
  taosArrayDestroy(offsets);
  return TSDB_CODE_SUCCESS;
}
static int32_t tfSearchLessThan(void* reader, SIndexTerm* tem, SIdxTRslt* tr) {
  return tfSearchCompareFunc(reader, tem, tr, LT);
}
static int32_t tfSearchLessEqual(void* reader, SIndexTerm* tem, SIdxTRslt* tr) {
  return tfSearchCompareFunc(reader, tem, tr, LE);
}
static int32_t tfSearchGreaterThan(void* reader, SIndexTerm* tem, SIdxTRslt* tr) {
  return tfSearchCompareFunc(reader, tem, tr, GT);
}
static int32_t tfSearchGreaterEqual(void* reader, SIndexTerm* tem, SIdxTRslt* tr) {
  return tfSearchCompareFunc(reader, tem, tr, GE);
}
static int32_t tfSearchRange(void* reader, SIndexTerm* tem, SIdxTRslt* tr) {
  int      ret = 0;
  char*    p = tem->colVal;
  uint64_t sz = tem->nColVal;
  int64_t  st = taosGetTimestampUs();
  FstSlice key = fstSliceCreate(p, sz);
  fstSliceDestroy(&key);
  return 0;
}
static int32_t tfSearchTerm_JSON(void* reader, SIndexTerm* tem, SIdxTRslt* tr) {
  int   ret = 0;
  char* p = idxPackJsonData(tem);
  int   sz = strlen(p);

  int64_t  st = taosGetTimestampUs();
  FstSlice key = fstSliceCreate(p, sz);
  uint64_t offset;
  if (fstGet(((TFileReader*)reader)->fst, &key, &offset)) {
    int64_t et = taosGetTimestampUs();
    int64_t cost = et - st;
    indexInfo("index: %" PRIu64 ", col: %s, colVal: %s, found table info in tindex, time cost: %" PRIu64 "us",
              tem->suid, tem->colName, tem->colVal, cost);

    ret = tfileReaderLoadTableIds((TFileReader*)reader, offset, tr->total);
    cost = taosGetTimestampUs() - et;
    indexInfo("index: %" PRIu64 ", col: %s, colVal: %s, load all table info, offset: %" PRIu64
              ", size: %d, time cost: %" PRIu64 "us",
              tem->suid, tem->colName, tem->colVal, offset, (int)taosArrayGetSize(tr->total), cost);
  }
  taosMemoryFree(p);
  fstSliceDestroy(&key);
  return 0;
}
static int32_t tfSearchEqual_JSON(void* reader, SIndexTerm* tem, SIdxTRslt* tr) {
  return tfSearchCompareFunc_JSON(reader, tem, tr, EQ);
}
static int32_t tfSearchPrefix_JSON(void* reader, SIndexTerm* tem, SIdxTRslt* tr) {
  return tfSearchCompareFunc_JSON(reader, tem, tr, CONTAINS);
}
static int32_t tfSearchSuffix_JSON(void* reader, SIndexTerm* tem, SIdxTRslt* tr) {
  // impl later
  return TSDB_CODE_SUCCESS;
}
static int32_t tfSearchRegex_JSON(void* reader, SIndexTerm* tem, SIdxTRslt* tr) {
  // impl later
  return TSDB_CODE_SUCCESS;
}
static int32_t tfSearchLessThan_JSON(void* reader, SIndexTerm* tem, SIdxTRslt* tr) {
  return tfSearchCompareFunc_JSON(reader, tem, tr, LT);
}
static int32_t tfSearchLessEqual_JSON(void* reader, SIndexTerm* tem, SIdxTRslt* tr) {
  return tfSearchCompareFunc_JSON(reader, tem, tr, LE);
}
static int32_t tfSearchGreaterThan_JSON(void* reader, SIndexTerm* tem, SIdxTRslt* tr) {
  return tfSearchCompareFunc_JSON(reader, tem, tr, GT);
}
static int32_t tfSearchGreaterEqual_JSON(void* reader, SIndexTerm* tem, SIdxTRslt* tr) {
  return tfSearchCompareFunc_JSON(reader, tem, tr, GE);
}
static int32_t tfSearchRange_JSON(void* reader, SIndexTerm* tem, SIdxTRslt* tr) {
  // impl later
  return TSDB_CODE_SUCCESS;
}

static int32_t tfSearchCompareFunc_JSON(void* reader, SIndexTerm* tem, SIdxTRslt* tr, RangeType ctype) {
  int ret = 0;
  int skip = 0;

  char* p = NULL;
  if (ctype == CONTAINS) {
    SIndexTerm tm = {.suid = tem->suid,
                     .operType = tem->operType,
                     .colType = tem->colType,
                     .colName = tem->colVal,
                     .nColName = tem->nColVal};
    p = idxPackJsonDataPrefixNoType(&tm, &skip);
  } else {
    p = idxPackJsonDataPrefix(tem, &skip);
  }

  _cache_range_compare cmpFn = idxGetCompare(ctype);

  SArray* offsets = taosArrayInit(16, sizeof(uint64_t));

  FAutoCtx*    ctx = automCtxCreate((void*)p, AUTOMATION_PREFIX);
  FStmBuilder* sb = fstSearch(((TFileReader*)reader)->fst, ctx);

  FStmSt*     st = stmBuilderIntoStm(sb);
  FStmStRslt* rt = NULL;
  while ((rt = stmStNextWith(st, NULL)) != NULL) {
    FstSlice* s = &rt->data;

    int32_t  sz = 0;
    char*    ch = (char*)fstSliceData(s, &sz);
    TExeCond cond = CONTINUE;
    if (ctype == CONTAINS) {
      if (0 == strncmp(ch, p, skip)) {
        cond = MATCH;
      }
    } else {
      if (0 != strncmp(ch, p, skip - 1)) {
        swsResultDestroy(rt);
        break;
      } else if (0 != strncmp(ch, p, skip)) {
        continue;
      }
      char* tBuf = taosMemoryCalloc(1, sz + 1);
      memcpy(tBuf, ch, sz);
      cond = cmpFn(tBuf + skip, tem->colVal, IDX_TYPE_GET_TYPE(tem->colType));
      taosMemoryFree(tBuf);
    }
    if (MATCH == cond) {
      tfileReaderLoadTableIds((TFileReader*)reader, rt->out.out, tr->total);
    } else if (CONTINUE == cond) {
    } else if (BREAK == cond) {
      swsResultDestroy(rt);
      break;
    }
    swsResultDestroy(rt);
  }
  stmStDestroy(st);
  stmBuilderDestroy(sb);
  taosArrayDestroy(offsets);
  taosMemoryFree(p);

  return TSDB_CODE_SUCCESS;
}
int tfileReaderSearch(TFileReader* reader, SIndexTermQuery* query, SIdxTRslt* tr) {
  SIndexTerm*     term = query->term;
  EIndexQueryType qtype = query->qType;
  int             ret = 0;
  if (IDX_TYPE_CONTAIN_EXTERN_TYPE(term->colType, TSDB_DATA_TYPE_JSON)) {
    ret = tfSearch[1][qtype](reader, term, tr);
  } else {
    ret = tfSearch[0][qtype](reader, term, tr);
  }

  tfileReaderUnRef(reader);
  return ret;
}

TFileWriter* tfileWriterOpen(char* path, uint64_t suid, int64_t version, const char* colName, uint8_t colType) {
  char fullname[256] = {0};
  tfileGenFileFullName(fullname, path, suid, colName, version);
  IFileCtx* wcx = idxFileCtxCreate(TFILE, fullname, false, 1024 * 1024 * 64);
  if (wcx == NULL) {
    return NULL;
  }

  TFileHeader tfh = {0};
  tfh.suid = suid;
  tfh.version = version;
  tfh.colType = colType;
  if (strlen(colName) <= sizeof(tfh.colName)) {
    memcpy(tfh.colName, colName, strlen(colName));
  }

  return tfileWriterCreate(wcx, &tfh);
}
TFileReader* tfileReaderOpen(SIndex* idx, uint64_t suid, int64_t version, const char* colName) {
  char fullname[256] = {0};
  tfileGenFileFullName(fullname, idx->path, suid, colName, version);

  IFileCtx* wc = idxFileCtxCreate(TFILE, fullname, true, 1024 * 1024 * 1024);
  if (wc == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    indexError("failed to open readonly file: %s, reason: %s", fullname, terrstr());
    return NULL;
  }
  wc->lru = idx->lru;
  indexTrace("open read file name:%s, file size: %" PRId64 "", wc->file.buf, wc->file.size);

  TFileReader* reader = tfileReaderCreate(wc);
  return reader;
}
TFileWriter* tfileWriterCreate(IFileCtx* ctx, TFileHeader* header) {
  TFileWriter* tw = taosMemoryCalloc(1, sizeof(TFileWriter));
  if (tw == NULL) {
    indexError("index: %" PRIu64 " failed to alloc TFilerWriter", header->suid);
    return NULL;
  }
  tw->ctx = ctx;
  tw->header = *header;
  tfileWriteHeader(tw);
  return tw;
}

int tfileWriterPut(TFileWriter* tw, void* data, bool order) {
  // sort by coltype and write to tindex
  if (order == false) {
    __compar_fn_t fn;

    int8_t colType = tw->header.colType;
    colType = IDX_TYPE_GET_TYPE(colType);
    if (colType == TSDB_DATA_TYPE_BINARY || colType == TSDB_DATA_TYPE_VARBINARY ||
        colType == TSDB_DATA_TYPE_NCHAR || colType == TSDB_DATA_TYPE_GEOMETRY) {
      fn = tfileStrCompare;
    } else {
      fn = getComparFunc(colType, 0);
    }
    taosArraySortPWithExt((SArray*)(data), tfileValueCompare, &fn);
  }

  int32_t sz = taosArrayGetSize((SArray*)data);
  int32_t fstOffset = tw->offset;

  // ugly code, refactor later
  for (size_t i = 0; i < sz; i++) {
    TFileValue* v = taosArrayGetP((SArray*)data, i);
    taosArraySort(v->tableId, idxUidCompare);
    taosArrayRemoveDuplicate(v->tableId, idxUidCompare, NULL);
    int32_t tbsz = taosArrayGetSize(v->tableId);
    if (tbsz == 0) continue;
    fstOffset += TF_TABLE_TATOAL_SIZE(tbsz);
  }
  tfileWriteFstOffset(tw, fstOffset);

  int32_t cap = 4 * 1024;
  char*   buf = taosMemoryCalloc(1, cap);

  for (size_t i = 0; i < sz; i++) {
    TFileValue* v = taosArrayGetP((SArray*)data, i);

    int32_t tbsz = taosArrayGetSize(v->tableId);
    if (tbsz == 0) continue;
    // check buf has enough space or not
    int32_t ttsz = TF_TABLE_TATOAL_SIZE(tbsz);

    if (cap < ttsz) {
      cap = ttsz;
      char* t = (char*)taosMemoryRealloc(buf, cap);
      if (t == NULL) {
        taosMemoryFree(buf);
        return -1;
      }
      buf = t;
    }

    char* p = buf;
    tfileSerialTableIdsToBuf(p, v->tableId);
    tw->ctx->write(tw->ctx, buf, ttsz);
    v->offset = tw->offset;
    tw->offset += ttsz;
    memset(buf, 0, cap);
  }
  taosMemoryFree(buf);

  tw->fb = fstBuilderCreate(tw->ctx, 0);
  if (tw->fb == NULL) {
    tfileWriterClose(tw);
    return -1;
  }

  // write data
  for (size_t i = 0; i < sz; i++) {
    // TODO, fst batch write later
    TFileValue* v = taosArrayGetP((SArray*)data, i);

    int32_t tbsz = taosArrayGetSize(v->tableId);
    if (tbsz == 0) continue;

    if (tfileWriteData(tw, v) != 0) {
      indexError("failed to write data: %s, offset: %d len: %d", v->colVal, v->offset,
                 (int)taosArrayGetSize(v->tableId));
    } else {
    }
  }
  fstBuilderDestroy(tw->fb);
  tfileWriteFooter(tw);
  return 0;
}
void tfileWriterClose(TFileWriter* tw) {
  if (tw == NULL) {
    return;
  }
  idxFileCtxDestroy(tw->ctx, false);
  taosMemoryFree(tw);
}
void tfileWriterDestroy(TFileWriter* tw) {
  if (tw == NULL) {
    return;
  }
  idxFileCtxDestroy(tw->ctx, false);
  taosMemoryFree(tw);
}

IndexTFile* idxTFileCreate(SIndex* idx, const char* path) {
  TFileCache* cache = tfileCacheCreate(idx, path);
  if (cache == NULL) {
    return NULL;
  }

  IndexTFile* tfile = taosMemoryCalloc(1, sizeof(IndexTFile));
  if (tfile == NULL) {
    tfileCacheDestroy(cache);
    return NULL;
  }
  taosThreadMutexInit(&tfile->mtx, NULL);
  tfile->cache = cache;
  return tfile;
}
void idxTFileDestroy(IndexTFile* tfile) {
  if (tfile == NULL) {
    return;
  }
  taosThreadMutexDestroy(&tfile->mtx);
  tfileCacheDestroy(tfile->cache);
  taosMemoryFree(tfile);
}

int idxTFileSearch(void* tfile, SIndexTermQuery* query, SIdxTRslt* result) {
  int ret = -1;
  if (tfile == NULL) {
    return ret;
  }

  int64_t     st = taosGetTimestampUs();
  IndexTFile* pTfile = tfile;

  SIndexTerm* term = query->term;
  ICacheKey key = {.suid = term->suid, .colType = term->colType, .colName = term->colName, .nColName = term->nColName};

  taosThreadMutexLock(&pTfile->mtx);
  TFileReader* reader = tfileCacheGet(pTfile->cache, &key);
  taosThreadMutexUnlock(&pTfile->mtx);
  if (reader == NULL) {
    return 0;
  }
  int64_t cost = taosGetTimestampUs() - st;
  indexInfo("index tfile stage 1 cost: %" PRId64 "", cost);

  return tfileReaderSearch(reader, query, result);
}
#ifdef BUILD_NO_CALL
int idxTFilePut(void* tfile, SIndexTerm* term, uint64_t uid) {
  // TFileWriterOpt wOpt = {.suid = term->suid, .colType = term->colType, .colName = term->colName, .nColName =
  // term->nColName, .version = 1};

  return 0;
}
#endif
static bool tfileIteratorNext(Iterate* iiter) {
  IterateValue* iv = &iiter->val;
  iterateValueDestroy(iv, false);

  char*    colVal = NULL;
  uint64_t offset = 0;

  TFileFstIter* tIter = iiter->iter;
  FStmStRslt*   rt = stmStNextWith(tIter->st, NULL);
  if (rt == NULL) {
    return false;
  }

  int32_t sz = 0;
  char*   ch = (char*)fstSliceData(&rt->data, &sz);
  colVal = taosMemoryCalloc(1, sz + 1);
  memcpy(colVal, ch, sz);

  offset = (uint64_t)(rt->out.out);
  swsResultDestroy(rt);
  // set up iterate value
  if (tfileReaderLoadTableIds(tIter->rdr, offset, iv->val) != 0) {
    return false;
  }

  iv->ver = 0;
  iv->type = ADD_VALUE;  // value in tfile always ADD_VALUE
  iv->colVal = colVal;
  return true;
}

static IterateValue* tifileIterateGetValue(Iterate* iter) { return &iter->val; }

static TFileFstIter* tfileFstIteratorCreate(TFileReader* reader) {
  TFileFstIter* iter = taosMemoryCalloc(1, sizeof(TFileFstIter));
  if (iter == NULL) {
    return NULL;
  }

  iter->ctx = automCtxCreate(NULL, AUTOMATION_ALWAYS);
  iter->fb = fstSearch(reader->fst, iter->ctx);
  iter->st = stmBuilderIntoStm(iter->fb);
  iter->rdr = reader;
  return iter;
}

Iterate* tfileIteratorCreate(TFileReader* reader) {
  if (reader == NULL) {
    return NULL;
  }

  Iterate* iter = taosMemoryCalloc(1, sizeof(Iterate));
  iter->iter = tfileFstIteratorCreate(reader);
  if (iter->iter == NULL) {
    taosMemoryFree(iter);
    return NULL;
  }
  iter->next = tfileIteratorNext;
  iter->getValue = tifileIterateGetValue;
  iter->val.val = taosArrayInit(1, sizeof(uint64_t));
  iter->val.colVal = NULL;
  return iter;
}
void tfileIteratorDestroy(Iterate* iter) {
  if (iter == NULL) {
    return;
  }

  IterateValue* iv = &iter->val;
  iterateValueDestroy(iv, true);

  TFileFstIter* tIter = iter->iter;
  stmStDestroy(tIter->st);
  stmBuilderDestroy(tIter->fb);
  automCtxDestroy(tIter->ctx);
  taosMemoryFree(tIter);

  taosMemoryFree(iter);
}

TFileReader* tfileGetReaderByCol(IndexTFile* tf, uint64_t suid, char* colName) {
  if (tf == NULL) {
    return NULL;
  }
  TFileReader* rd = NULL;
  ICacheKey    key = {.suid = suid, .colType = TSDB_DATA_TYPE_BINARY, .colName = colName, .nColName = strlen(colName)};

  taosThreadMutexLock(&tf->mtx);
  rd = tfileCacheGet(tf->cache, &key);
  taosThreadMutexUnlock(&tf->mtx);
  return rd;
}

static int tfileStrCompare(const void* a, const void* b) {
  int ret = strcmp((char*)a, (char*)b);
  if (ret == 0) {
    return ret;
  }
  return ret < 0 ? -1 : 1;
}

static int tfileValueCompare(const void* a, const void* b, const void* param) {
  __compar_fn_t fn = *(__compar_fn_t*)param;

  TFileValue* av = (TFileValue*)a;
  TFileValue* bv = (TFileValue*)b;

  return fn(av->colVal, bv->colVal);
}

TFileValue* tfileValueCreate(char* val) {
  TFileValue* tf = taosMemoryCalloc(1, sizeof(TFileValue));
  if (tf == NULL) {
    return NULL;
  }
  tf->colVal = taosStrdup(val);
  tf->tableId = taosArrayInit(32, sizeof(uint64_t));
  return tf;
}
int tfileValuePush(TFileValue* tf, uint64_t val) {
  if (tf == NULL) {
    return -1;
  }
  taosArrayPush(tf->tableId, &val);
  return 0;
}
void tfileValueDestroy(TFileValue* tf) {
  taosArrayDestroy(tf->tableId);
  taosMemoryFree(tf->colVal);
  taosMemoryFree(tf);
}
static void tfileSerialTableIdsToBuf(char* buf, SArray* ids) {
  int sz = taosArrayGetSize(ids);
  SERIALIZE_VAR_TO_BUF(buf, sz, int32_t);
  for (size_t i = 0; i < sz; i++) {
    uint64_t* v = taosArrayGet(ids, i);
    SERIALIZE_VAR_TO_BUF(buf, *v, uint64_t);
  }
}

static int tfileWriteFstOffset(TFileWriter* tw, int32_t offset) {
  int32_t fstOffset = offset + sizeof(tw->header.fstOffset);
  tw->header.fstOffset = fstOffset;

  if (sizeof(fstOffset) != tw->ctx->write(tw->ctx, (char*)&fstOffset, sizeof(fstOffset))) {
    return -1;
  }
  indexInfo("tfile write fst offset: %d", tw->ctx->size(tw->ctx));
  tw->offset += sizeof(fstOffset);
  return 0;
}
static int tfileWriteHeader(TFileWriter* writer) {
  char buf[TFILE_HEADER_NO_FST] = {0};

  TFileHeader* header = &writer->header;
  memcpy(buf, (char*)header, sizeof(buf));

  indexInfo("tfile pre write header size: %d", writer->ctx->size(writer->ctx));
  int nwrite = writer->ctx->write(writer->ctx, buf, sizeof(buf));
  if (sizeof(buf) != nwrite) {
    return -1;
  }

  indexInfo("tfile after write header size: %d", writer->ctx->size(writer->ctx));
  writer->offset = nwrite;
  return 0;
}
static int tfileWriteData(TFileWriter* write, TFileValue* tval) {
  TFileHeader* header = &write->header;
  uint8_t      colType = header->colType;

  colType = IDX_TYPE_GET_TYPE(colType);
  FstSlice key = fstSliceCreate((uint8_t*)(tval->colVal), (size_t)strlen(tval->colVal));
  if (fstBuilderInsert(write->fb, key, tval->offset)) {
    fstSliceDestroy(&key);
    return 0;
  }
  return -1;
}
static int tfileWriteFooter(TFileWriter* write) {
  char  buf[sizeof(FILE_MAGIC_NUMBER) + 1] = {0};
  void* pBuf = (void*)buf;
  taosEncodeFixedU64((void**)(void*)&pBuf, FILE_MAGIC_NUMBER);
  int nwrite = write->ctx->write(write->ctx, buf, (int32_t)strlen(buf));

  indexInfo("tfile write footer size: %d", write->ctx->size(write->ctx));
  ASSERTS(nwrite == sizeof(FILE_MAGIC_NUMBER), "index write incomplete data");
  return nwrite;
}
static int tfileReaderLoadHeader(TFileReader* reader) {
  // TODO simple tfile header later
  char buf[TFILE_HEADER_SIZE] = {0};

  int64_t nread = reader->ctx->readFrom(reader->ctx, buf, sizeof(buf), 0);

  if (nread == -1) {
    indexError("actual Read: %d, to read: %d, code:0x%x, filename: %s", (int)(nread), (int)sizeof(buf), errno,
               reader->ctx->file.buf);
  } else {
    indexInfo("actual Read: %d, to read: %d, filename: %s", (int)(nread), (int)sizeof(buf), reader->ctx->file.buf);
  }
  memcpy(&reader->header, buf, sizeof(buf));

  return 0;
}
static int tfileReaderLoadFst(TFileReader* reader) {
  IFileCtx* ctx = reader->ctx;
  int       size = ctx->size(ctx);

  // current load fst into memory, refactor it later
  int   fstSize = size - reader->header.fstOffset - sizeof(FILE_MAGIC_NUMBER);
  char* buf = taosMemoryCalloc(1, fstSize);
  if (buf == NULL) {
    return -1;
  }

  int64_t ts = taosGetTimestampUs();
  int32_t nread = ctx->readFrom(ctx, buf, fstSize, reader->header.fstOffset);
  int64_t cost = taosGetTimestampUs() - ts;
  indexInfo("nread = %d, and fst offset=%d, fst size: %d, filename: %s, file size: %d, time cost: %" PRId64 "us", nread,
            reader->header.fstOffset, fstSize, ctx->file.buf, size, cost);
  // we assuse fst size less than FST_MAX_SIZE
  ASSERTS(nread > 0 && nread <= fstSize, "index read incomplete fst");
  if (nread <= 0 || nread > fstSize) {
    return -1;
  }

  FstSlice st = fstSliceCreate((uint8_t*)buf, nread);
  reader->fst = fstCreate(&st);
  taosMemoryFree(buf);
  fstSliceDestroy(&st);

  return reader->fst != NULL ? 0 : -1;
}
static int tfileReaderLoadTableIds(TFileReader* reader, int32_t offset, SArray* result) {
  // TODO(yihao): opt later
  IFileCtx* ctx = reader->ctx;
  // add block cache
  char    block[4096] = {0};
  int32_t nread = ctx->readFrom(ctx, block, sizeof(block), offset);
  ASSERT(nread >= sizeof(uint32_t));

  char*   p = block;
  int32_t nid = *(int32_t*)p;
  p += sizeof(nid);

  while (nid > 0) {
    int32_t left = block + sizeof(block) - p;
    if (left >= sizeof(uint64_t)) {
      taosArrayPush(result, (uint64_t*)p);
      p += sizeof(uint64_t);
    } else {
      char buf[sizeof(uint64_t)] = {0};
      memcpy(buf, p, left);

      memset(block, 0, sizeof(block));
      offset += sizeof(block);
      nread = ctx->readFrom(ctx, block, sizeof(block), offset);
      memcpy(buf + left, block, sizeof(uint64_t) - left);

      taosArrayPush(result, (uint64_t*)buf);
      p = block + sizeof(uint64_t) - left;
    }
    nid -= 1;
  }
  return 0;
}
static int tfileReaderVerify(TFileReader* reader) {
  // just validate header and Footer, file corrupted also shuild be verified later
  IFileCtx* ctx = reader->ctx;

  uint64_t tMagicNumber = 0;
  char     buf[sizeof(tMagicNumber) + 1] = {0};
  int      size = ctx->size(ctx);

  if (size < sizeof(tMagicNumber) || size <= sizeof(reader->header)) {
    return -1;
  } else if (ctx->readFrom(ctx, buf, sizeof(tMagicNumber), size - sizeof(tMagicNumber)) != sizeof(tMagicNumber)) {
    return -1;
  }

  taosDecodeFixedU64(buf, &tMagicNumber);
  return tMagicNumber == FILE_MAGIC_NUMBER ? 0 : -1;
}

void tfileReaderRef(TFileReader* rd) {
  if (rd == NULL) {
    return;
  }
  int ref = T_REF_INC(rd);
  UNUSED(ref);
}

void tfileReaderUnRef(TFileReader* rd) {
  if (rd == NULL) {
    return;
  }
  int ref = T_REF_DEC(rd);
  if (ref == 0) {
    // do nothing
    tfileReaderDestroy(rd);
  }
}

static SArray* tfileGetFileList(const char* path) {
  char     buf[128] = {0};
  uint64_t suid;
  int64_t  version;
  SArray*  files = taosArrayInit(4, sizeof(void*));

  TdDirPtr pDir = taosOpenDir(path);
  if (NULL == pDir) {
    taosArrayDestroy(files);
    return NULL;
  }
  TdDirEntryPtr pDirEntry;
  while ((pDirEntry = taosReadDir(pDir)) != NULL) {
    char* file = taosGetDirEntryName(pDirEntry);
    if (0 != tfileParseFileName(file, &suid, buf, &version)) {
      continue;
    }

    size_t len = strlen(path) + 1 + strlen(file) + 1;
    char*  buf = taosMemoryCalloc(1, len);
    sprintf(buf, "%s/%s", path, file);
    taosArrayPush(files, &buf);
  }
  taosCloseDir(&pDir);

  taosArraySort(files, tfileCompare);
  tfileRmExpireFile(files);

  return files;
}
static int tfileRmExpireFile(SArray* result) {
  // TODO(yihao): remove expire tindex after restart
  return 0;
}
static void tfileDestroyFileName(void* elem) {
  char* p = *(char**)elem;
  taosMemoryFree(p);
}
static int tfileCompare(const void* a, const void* b) {
  const char* as = *(char**)a;
  const char* bs = *(char**)b;
  return strcmp(as, bs);
}

static int tfileParseFileName(const char* filename, uint64_t* suid, char* col, int64_t* version) {
  if (3 == sscanf(filename, "%" PRIu64 "-%[^-]-%" PRId64 ".tindex", suid, col, version)) {
    // read suid & colid & version  success
    return 0;
  }
  return -1;
}
// tfile name suid-colId-version.tindex
static void tfileGenFileName(char* filename, uint64_t suid, const char* col, int64_t version) {
  sprintf(filename, "%" PRIu64 "-%s-%" PRId64 ".tindex", suid, col, version);
  return;
}
static void FORCE_INLINE tfileGenFileFullName(char* fullname, const char* path, uint64_t suid, const char* col,
                                              int64_t version) {
  char filename[128] = {0};
  tfileGenFileName(filename, suid, col, version);
  sprintf(fullname, "%s/%s", path, filename);
}
