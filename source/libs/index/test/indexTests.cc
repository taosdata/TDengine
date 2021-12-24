/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include "index.h"
#include "indexInt.h"
#include "index_cache.h"
#include "index_fst.h"
#include "index_fst_counting_writer.h"
#include "index_fst_util.h"
#include "index_tfile.h"
#include "tskiplist.h"
#include "tutil.h"
using namespace std;
class FstWriter {
 public:
  FstWriter() {
    _wc = writerCtxCreate(TFile, "/tmp/tindex", false, 64 * 1024 * 1024);
    _b = fstBuilderCreate(NULL, 0);
  }
  bool Put(const std::string& key, uint64_t val) {
    FstSlice skey = fstSliceCreate((uint8_t*)key.c_str(), key.size());
    bool     ok = fstBuilderInsert(_b, skey, val);
    fstSliceDestroy(&skey);
    return ok;
  }
  ~FstWriter() {
    fstBuilderFinish(_b);
    fstBuilderDestroy(_b);

    writerCtxDestroy(_wc);
  }

 private:
  FstBuilder* _b;
  WriterCtx*  _wc;
};

class FstReadMemory {
 public:
  FstReadMemory(size_t size) {
    _wc = writerCtxCreate(TFile, "/tmp/tindex", true, 64 * 1024);
    _w = fstCountingWriterCreate(_wc);
    _size = size;
    memset((void*)&_s, 0, sizeof(_s));
  }
  bool init() {
    char* buf = (char*)calloc(1, sizeof(char) * _size);
    int   nRead = fstCountingWriterRead(_w, (uint8_t*)buf, _size);
    if (nRead <= 0) { return false; }
    _size = nRead;
    _s = fstSliceCreate((uint8_t*)buf, _size);
    _fst = fstCreate(&_s);
    free(buf);
    return _fst != NULL;
  }
  bool Get(const std::string& key, uint64_t* val) {
    FstSlice skey = fstSliceCreate((uint8_t*)key.c_str(), key.size());
    bool     ok = fstGet(_fst, &skey, val);
    fstSliceDestroy(&skey);
    return ok;
  }
  bool GetWithTimeCostUs(const std::string& key, uint64_t* val, uint64_t* elapse) {
    int64_t s = taosGetTimestampUs();
    bool    ok = this->Get(key, val);
    int64_t e = taosGetTimestampUs();
    *elapse = e - s;
    return ok;
  }
  // add later
  bool Search(AutomationCtx* ctx, std::vector<uint64_t>& result) {
    FstStreamBuilder*      sb = fstSearch(_fst, ctx);
    StreamWithState*       st = streamBuilderIntoStream(sb);
    StreamWithStateResult* rt = NULL;

    while ((rt = streamWithStateNextWith(st, NULL)) != NULL) {
      result.push_back((uint64_t)(rt->out.out));
    }
    return true;
  }
  bool SearchWithTimeCostUs(AutomationCtx* ctx, std::vector<uint64_t>& result) {
    int64_t s = taosGetTimestampUs();
    bool    ok = this->Search(ctx, result);
    int64_t e = taosGetTimestampUs();
    return ok;
  }

  ~FstReadMemory() {
    fstCountingWriterDestroy(_w);
    fstDestroy(_fst);
    fstSliceDestroy(&_s);
    writerCtxDestroy(_wc);
  }

 private:
  FstCountingWriter* _w;
  Fst*               _fst;
  FstSlice           _s;
  WriterCtx*         _wc;
  size_t             _size;
};

#define L 100
#define M 100
#define N 100

int Performance_fstWriteRecords(FstWriter* b) {
  std::string str("aa");
  for (int i = 0; i < L; i++) {
    str[0] = 'a' + i;
    str.resize(2);
    for (int j = 0; j < M; j++) {
      str[1] = 'a' + j;
      str.resize(2);
      for (int k = 0; k < N; k++) {
        str.push_back('a');
        b->Put(str, k);
        printf("(%d, %d, %d, %s)\n", i, j, k, str.c_str());
      }
    }
  }
  return L * M * N;
}

void Performance_fstReadRecords(FstReadMemory* m) {
  std::string str("aa");
  for (int i = 0; i < M; i++) {
    str[0] = 'a' + i;
    str.resize(2);
    for (int j = 0; j < N; j++) {
      str[1] = 'a' + j;
      str.resize(2);
      for (int k = 0; k < L; k++) {
        str.push_back('a');
        uint64_t val, cost;
        if (m->GetWithTimeCostUs(str, &val, &cost)) {
          printf("succes to get kv(%s, %" PRId64 "), cost: %" PRId64 "\n", str.c_str(), val, cost);
        } else {
          printf("failed to get key: %s\n", str.c_str());
        }
      }
    }
  }
}
void checkFstPerf() {
  FstWriter* fw = new FstWriter;
  int64_t    s = taosGetTimestampUs();

  int     num = Performance_fstWriteRecords(fw);
  int64_t e = taosGetTimestampUs();
  printf("write %d record cost %" PRId64 "us\n", num, e - s);
  delete fw;

  FstReadMemory* m = new FstReadMemory(1024 * 64);
  if (m->init()) { printf("success to init fst read"); }
  Performance_fstReadRecords(m);
  delete m;
}

void checkFstPrefixSearch() {
  FstWriter*  fw = new FstWriter;
  int64_t     s = taosGetTimestampUs();
  int         count = 2;
  std::string key("ab");

  for (int i = 0; i < count; i++) {
    key[1] = key[1] + i;
    fw->Put(key, i);
  }
  int64_t e = taosGetTimestampUs();

  std::cout << "insert data count :  " << count << "elapas time: " << e - s << std::endl;
  delete fw;

  FstReadMemory* m = new FstReadMemory(1024 * 64);
  if (m->init() == false) {
    std::cout << "init readMemory failed" << std::endl;
    delete m;
    return;
  }

  // prefix search
  std::vector<uint64_t> result;

  AutomationCtx* ctx = automCtxCreate((void*)"ab", AUTOMATION_PREFIX);
  m->Search(ctx, result);
  assert(result.size() == count);
  for (int i = 0; i < result.size(); i++) {
    assert(result[i] == i);  // check result
  }

  free(ctx);
  delete m;
}
void validateFst() {
  int        val = 100;
  int        count = 100;
  FstWriter* fw = new FstWriter;
  // write
  {
    std::string key("ab");
    for (int i = 0; i < count; i++) {
      key.push_back('a' + i);
      fw->Put(key, val - i);
    }
  }
  delete fw;

  // read
  FstReadMemory* m = new FstReadMemory(1024 * 64);
  if (m->init() == false) {
    std::cout << "init readMemory failed" << std::endl;
    delete m;
    return;
  }

  {
    std::string key("ab");
    uint64_t    out;
    if (m->Get(key, &out)) {
      printf("success to get (%s, %" PRId64 ")\n", key.c_str(), out);
    } else {
      printf("failed to get(%s)\n", key.c_str());
    }
    for (int i = 0; i < count; i++) {
      key.push_back('a' + i);
      if (m->Get(key, &out)) {
        assert(val - i == out);
        printf("success to get (%s, %" PRId64 ")\n", key.c_str(), out);
      } else {
        printf("failed to get(%s)\n", key.c_str());
      }
    }
  }
  delete m;
}

class IndexEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    taosRemoveDir(path);
    opts = indexOptsCreate();
    int ret = indexOpen(opts, path, &index);
    assert(ret == 0);
  }
  virtual void TearDown() {
    indexClose(index);
    indexOptsDestroy(opts);
  }

  const char* path = "/tmp/tindex";
  SIndexOpts* opts;
  SIndex*     index;
};

// TEST_F(IndexEnv, testPut) {
//  // single index column
//  {
//    std::string colName("tag1"), colVal("Hello world");
//    SIndexTerm* term = indexTermCreate(0, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(), colVal.c_str(),
//    colVal.size()); SIndexMultiTerm* terms = indexMultiTermCreate(); indexMultiTermAdd(terms, term);
//
//    for (size_t i = 0; i < 100; i++) {
//      int tableId = i;
//      int ret = indexPut(index, terms, tableId);
//      assert(ret == 0);
//    }
//    indexMultiTermDestroy(terms);
//  }
//  // multi index column
//  {
//    SIndexMultiTerm* terms = indexMultiTermCreate();
//    {
//      std::string colName("tag1"), colVal("Hello world");
//      SIndexTerm* term =
//          indexTermCreate(0, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(), colVal.c_str(), colVal.size());
//      indexMultiTermAdd(terms, term);
//    }
//    {
//      std::string colName("tag2"), colVal("Hello world");
//      SIndexTerm* term =
//          indexTermCreate(0, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(), colVal.c_str(), colVal.size());
//      indexMultiTermAdd(terms, term);
//    }
//
//    for (int i = 0; i < 100; i++) {
//      int tableId = i;
//      int ret = indexPut(index, terms, tableId);
//      assert(ret == 0);
//    }
//    indexMultiTermDestroy(terms);
//  }
//  //
//}

class TFileObj {
 public:
  TFileObj(const std::string& path = "/tmp/tindex", const std::string& colName = "voltage") : path_(path), colName_(colName) {
    colId_ = 10;
    // Do Nothing
    //
  }
  int Put(SArray* tv) {
    if (reader_ != NULL) {
      tfileReaderDestroy(reader_);
      reader_ = NULL;
    }
    if (writer_ == NULL) { InitWriter(); }
    return tfileWriterPut(writer_, tv);
  }
  bool InitWriter() {
    TFileHeader header;
    header.suid = 1;
    header.version = 1;
    memcpy(header.colName, colName_.c_str(), colName_.size());
    header.colType = TSDB_DATA_TYPE_BINARY;

    std::string path(path_);
    int         colId = 2;
    char        buf[64] = {0};
    sprintf(buf, "%" PRIu64 "-%d-%d.tindex", header.suid, colId_, header.version);
    path.append("/").append(buf);

    fileName_ = path;

    WriterCtx* ctx = writerCtxCreate(TFile, path.c_str(), false, 64 * 1024 * 1024);

    writer_ = tfileWriterCreate(ctx, &header);
    return writer_ != NULL ? true : false;
  }
  bool InitReader() {
    WriterCtx* ctx = writerCtxCreate(TFile, fileName_.c_str(), true, 64 * 1024 * 1024);
    reader_ = tfileReaderCreate(ctx);
    return reader_ != NULL ? true : false;
  }
  int Get(SIndexTermQuery* query, SArray* result) {
    if (writer_ != NULL) {
      tfileWriterDestroy(writer_);
      writer_ = NULL;
    }
    if (reader_ == NULL && InitReader()) {
      //
      //
    }
    return tfileReaderSearch(reader_, query, result);
  }
  ~TFileObj() {
    if (writer_) { tfileWriterDestroy(writer_); }
    if (reader_) { tfileReaderDestroy(reader_); }
  }

 private:
  std::string path_;
  std::string colName_;
  std::string fileName_;

  TFileWriter* writer_;
  TFileReader* reader_;

  int colId_;
};

class IndexTFileEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    taosRemoveDir(dir.c_str());
    taosMkDir(dir.c_str());
    tfInit();
    fObj = new TFileObj(dir, colName);
  }

  virtual void TearDown() {
    // indexClose(index);
    // indexeptsDestroy(opts);
    delete fObj;
    tfCleanup();
    // tfileWriterDestroy(twrite);
  }
  TFileObj*   fObj;
  std::string dir = "/tmp/tindex";
  std::string colName = "voltage";

  int coldId = 2;
  int version = 1;
  int colType = TSDB_DATA_TYPE_BINARY;
};

static TFileValue* genTFileValue(const char* val) {
  TFileValue* tv = (TFileValue*)calloc(1, sizeof(TFileValue));
  int32_t     vlen = strlen(val) + 1;
  tv->colVal = (char*)calloc(1, vlen);
  memcpy(tv->colVal, val, vlen);

  tv->tableId = (SArray*)taosArrayInit(1, sizeof(uint64_t));
  for (size_t i = 0; i < 10; i++) {
    uint64_t v = i;
    taosArrayPush(tv->tableId, &v);
  }
  return tv;
}
static void destroyTFileValue(void* val) {
  TFileValue* tv = (TFileValue*)val;
  free(tv->colVal);
  taosArrayDestroy(tv->tableId);
  free(tv);
}

TEST_F(IndexTFileEnv, test_tfile_write) {
  TFileValue* v1 = genTFileValue("c");
  TFileValue* v2 = genTFileValue("ab");
  TFileValue* v3 = genTFileValue("b");
  TFileValue* v4 = genTFileValue("d");

  SArray* data = (SArray*)taosArrayInit(4, sizeof(void*));

  taosArrayPush(data, &v1);
  taosArrayPush(data, &v2);
  taosArrayPush(data, &v3);
  taosArrayPush(data, &v4);

  fObj->Put(data);
  for (size_t i = 0; i < taosArrayGetSize(data); i++) {
    destroyTFileValue(taosArrayGetP(data, i));
  }
  taosArrayDestroy(data);

  std::string colName("voltage");
  std::string colVal("ab");
  SIndexTerm* term = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(), colVal.c_str(), colVal.size());
  SIndexTermQuery query = {.term = term, .qType = QUERY_TERM};

  SArray* result = (SArray*)taosArrayInit(1, sizeof(uint64_t));
  fObj->Get(&query, result);
  assert(taosArrayGetSize(result) == 10);
  indexTermDestroy(term);

  // tfileWriterDestroy(twrite);
}
class CacheObj {
 public:
  CacheObj() {
    // TODO
    cache = indexCacheCreate();
  }
  int Put(SIndexTerm* term, int16_t colId, int32_t version, uint64_t uid) {
    int ret = indexCachePut(cache, term, colId, version, uid);
    if (ret != 0) {
      //
      std::cout << "failed to put into cache: " << ret << std::endl;
    }
    return ret;
  }
  int Get(SIndexTermQuery* query, int16_t colId, int32_t version, SArray* result, STermValueType* s) {
    int ret = indexCacheSearch(cache, query, colId, version, result, s);
    if (ret != 0) {
      //
      std::cout << "failed to get from cache:" << ret << std::endl;
    }
    return ret;
  }
  ~CacheObj() {
    // TODO
    indexCacheDestroy(cache);
  }

 private:
  IndexCache* cache = NULL;
};

class IndexCacheEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    // TODO
    coj = new CacheObj();
  }
  virtual void TearDown() {
    delete coj;
    // formate
  }
  CacheObj* coj;
};

TEST_F(IndexCacheEnv, cache_test) {
  int count = 10;

  int16_t     colId = 1;
  int32_t     version = 10;
  uint64_t    suid = 100;
  std::string colName("voltage");
  std::string colVal("My God");
  for (size_t i = 0; i < count; i++) {
    colVal += ('a' + i);
    SIndexTerm* term = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(), colVal.c_str(), colVal.size());
    coj->Put(term, colId, version, suid);
    version++;
  }

  // coj->Get();
}

typedef struct CTerm {
  char buf[16];
  char version[8];
  int  val;
  int  other;
} CTerm;
CTerm* cTermCreate(const char* str, const char* version, int val) {
  CTerm* tm = (CTerm*)calloc(1, sizeof(CTerm));
  memcpy(tm->buf, str, strlen(str));
  memcpy(tm->version, version, strlen(version));
  tm->val = val;
  tm->other = -100;
  return tm;
}
int termCompar(const void* a, const void* b) {
  printf("a: %s \t b: %s\n", (char*)a, (char*)b);
  int ret = strncmp((char*)a, (char*)b, 16);
  if (ret == 0) {
    //
    return strncmp((char*)a + 16, (char*)b + 16, 8);
  }
  return ret;
}

int SerialTermTo(char* buf, CTerm* term) {
  char* p = buf;
  memcpy(buf, term->buf, sizeof(term->buf));
  buf += sizeof(term->buf);

  // memcpy(buf,  term->version, sizeof(term->version));
  // buf += sizeof(term->version);
  return buf - p;
}
static char* getTermKey(const void* pData) {
  CTerm* p = (CTerm*)pData;
  return (char*)p->buf;
}
#define MAX_TERM_KEY_LEN 128
class SkiplistObj {
 public:
  // max_key_len:
  //
  SkiplistObj() {
    slt = tSkipListCreate(MAX_SKIP_LIST_LEVEL, TSDB_DATA_TYPE_BINARY, MAX_TERM_KEY_LEN, termCompar, SL_ALLOW_DUP_KEY, getTermKey);
  }
  int Put(CTerm* term, uint64_t suid) {
    char buf[MAX_TERM_KEY_LEN] = {0};
    int  sz = SerialTermTo(buf, term);

    char* pBuf = (char*)calloc(1, sz + sizeof(suid));

    memcpy(pBuf, buf, sz);
    memcpy(pBuf + sz, &suid, sizeof(suid));
    // int32_t level, headsize;
    // tSkipListNewNodeInfo(slt, &level, &headsize);

    // SSkipListNode* node = (SSkipListNode*)calloc(1, headsize + strlen(buf) + sizeof(suid));
    // node->level = level;
    // char* d = (char*)SL_GET_NODE_DATA(node);
    // memcpy(d, buf, strlen(buf));
    // memcpy(d + strlen(buf), &suid, sizeof(suid));
    SSkipListNode* node = tSkipListPut(slt, pBuf);
    tSkipListPrint(slt, 1);
    free(pBuf);
    return 0;
  }

  int Get(int key, char* buf, int version) {
    // TODO
    // CTerm term;
    // term.key = key;
    //// term.version = version;
    // memcpy(term.buf, buf, strlen(buf));

    // char tbuf[128] = {0};
    // SerialTermTo(tbuf, &term);

    // SSkipListIterator* iter = tSkipListCreateIterFromVal(slt, tbuf, TSDB_DATA_TYPE_BINARY, TSDB_ORDER_ASC);
    // SSkipListNode*     node = tSkipListIterGet(iter);
    // CTerm*             ct = (CTerm*)SL_GET_NODE_DATA(node);
    // printf("key: %d\t, version: %d\t, buf: %s\n", ct->key, ct->version, ct->buf);
    // while (iter) {
    //  assert(tSkipListIterNext(iter) == true);
    //  SSkipListNode* node = tSkipListIterGet(iter);
    //  // ugly formate
    //  CTerm* t = (CTerm*)SL_GET_NODE_KEY(slt, node);
    //  printf("key: %d\t, version: %d\t, buf: %s\n", t->key, t->version, t->buf);
    //}
    return 0;
  }
  ~SkiplistObj() {
    // TODO
    // indexCacheDestroy(cache);
  }

 private:
  SSkipList* slt;
};

typedef struct KV {
  int32_t k;
  int32_t v;
} KV;
int kvCompare(const void* a, const void* b) {
  int32_t av = *(int32_t*)a;
  int32_t bv = *(int32_t*)b;
  return av - bv;
}
char* getKVkey(const void* a) {
  return (char*)(&(((KV*)a)->v));
  // KV* kv = (KV*)a;
}
int testKV() {
  SSkipList* slt = tSkipListCreate(MAX_SKIP_LIST_LEVEL, TSDB_DATA_TYPE_BINARY, MAX_TERM_KEY_LEN, kvCompare, SL_DISCARD_DUP_KEY, getKVkey);
  {
    KV t = {.k = 1, .v = 5};
    tSkipListPut(slt, (void*)&t);
  }
  {
    KV t = {.k = 2, .v = 3};
    tSkipListPut(slt, (void*)&t);
  }

  KV    value = {.k = 4, .v = 5};
  char* key = getKVkey(&value);
  // const char* key = "Hello";
  SArray* arr = tSkipListGet(slt, (SSkipListKey)&key);
  for (size_t i = 0; i < taosArrayGetSize(arr); i++) {
    SSkipListNode* node = (SSkipListNode*)taosArrayGetP(arr, i);
    int32_t*       ct = (int32_t*)SL_GET_NODE_KEY(slt, node);

    printf("Get key: %d\n", *ct);
    // SSkipListIterator* iter = tSkipListCreateIterFromVal(slt, tbuf, TSDB_DATA_TYPE_BINARY, TSDB_ORDER_ASC);
  }
  return 1;
}

int testComplicate() {
  SSkipList* slt = tSkipListCreate(MAX_SKIP_LIST_LEVEL, TSDB_DATA_TYPE_BINARY, MAX_TERM_KEY_LEN, termCompar, SL_ALLOW_DUP_KEY, getTermKey);
  {
    CTerm* tm = cTermCreate("val", "v1", 10);
    tSkipListPut(slt, (char*)tm);
  }
  {
    CTerm* tm = cTermCreate("val1", "v2", 2);
    tSkipListPut(slt, (char*)tm);
  }
  {
    CTerm* tm = cTermCreate("val3", "v3", -1);
    tSkipListPut(slt, (char*)tm);
  }
  {
    CTerm* tm = cTermCreate("val3", "v4", 2);
    tSkipListPut(slt, (char*)tm);
  }
  {
    CTerm*  tm = cTermCreate("val3", "v5", -1);
    char*   key = getTermKey(tm);
    SArray* arr = tSkipListGet(slt, (SSkipListKey)key);
    for (size_t i = 0; i < taosArrayGetSize(arr); i++) {
      SSkipListNode* node = (SSkipListNode*)taosArrayGetP(arr, i);
      CTerm*         ct = (CTerm*)SL_GET_NODE_KEY(slt, node);
      printf("other; %d\tbuf: %s\t, version: %s, val: %d\n", ct->other, ct->buf, ct->version, ct->val);
      // SSkipListIterator* iter = tSkipListCreateIterFromVal(slt, tbuf, TSDB_DATA_TYPE_BINARY, TSDB_ORDER_ASC);
    }
    free(tm);
    taosArrayDestroy(arr);
  }
  return 1;
}
int strCompare(const void* a, const void* b) {
  const char* sa = (char*)a;
  const char* sb = (char*)b;
  return strcmp(sa, sb);
}
void testString() {
  SSkipList* slt = tSkipListCreate(MAX_SKIP_LIST_LEVEL, TSDB_DATA_TYPE_BINARY, MAX_TERM_KEY_LEN, strCompare, SL_ALLOW_DUP_KEY, getTermKey);
  {
    tSkipListPut(slt, (void*)"Hello");
    tSkipListPut(slt, (void*)"World");
    tSkipListPut(slt, (void*)"YI");
  }

  const char* key = "YI";
  SArray*     arr = tSkipListGet(slt, (SSkipListKey)key);
  for (size_t i = 0; i < taosArrayGetSize(arr); i++) {
    SSkipListNode* node = (SSkipListNode*)taosArrayGetP(arr, i);
    char*          ct = (char*)SL_GET_NODE_KEY(slt, node);
    printf("Get key: %s\n", ct);
    // SSkipListIterator* iter = tSkipListCreateIterFromVal(slt, tbuf, TSDB_DATA_TYPE_BINARY, TSDB_ORDER_ASC);
  }
}
// class IndexSkip : public ::testing::Test {
// protected:
//  virtual void SetUp() {
//    // TODO
//    sObj = new SkiplistObj();
//  }
//  virtual void TearDown() {
//    delete sObj;
//    // formate
//  }
//  SkiplistObj* sObj;
//};

// TEST_F(IndexSkip, skip_test) {
//  std::string val("Hello");
//  std::string minVal = val;
//  for (size_t i = 0; i < 10; i++) {
//    CTerm* t = (CTerm*)calloc(1, sizeof(CTerm));
//    t->key = 1;
//    t->version = i;
//
//    val[val.size() - 1] = 'a' + i;
//    memcpy(t->buf, val.c_str(), val.size());
//    sObj->Put(t, 10);
//    free(t);
//  }
//  sObj->Get(1, (char*)(minVal.c_str()), 1000000);
//}
