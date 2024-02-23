
#include <gtest/gtest.h>
#include <algorithm>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include "index.h"
#include "indexCache.h"
#include "indexFst.h"
#include "indexFstUtil.h"
#include "indexInt.h"
#include "indexTfile.h"
#include "tskiplist.h"
#include "tutil.h"
void* callback(void* s) { return s; }

class FstEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {}
  virtual void TearDown() {}
};

static std::string fileName = TD_TMP_DIR_PATH "tindex.tindex";
class FstWriter {
 public:
  FstWriter() {
    taosRemoveFile(fileName.c_str());
    _wc = idxFileCtxCreate(TFILE, fileName.c_str(), false, 64 * 1024 * 1024);
    _b = fstBuilderCreate(_wc, 0);
  }
  bool Put(const std::string& key, uint64_t val) {
    // char buf[128] = {0};
    // int  len = 0;
    // taosMbsToUcs4(key.c_str(), key.size(), buf, 128, &len);
    // FstSlice skey = fstSliceCreate((uint8_t*)buf, len);
    FstSlice skey = fstSliceCreate((uint8_t*)key.c_str(), key.size());
    bool     ok = fstBuilderInsert(_b, skey, val);

    fstSliceDestroy(&skey);
    return ok;
  }
  ~FstWriter() {
    // fstBuilderFinish(_b);
    fstBuilderDestroy(_b);

    idxFileCtxDestroy(_wc, false);
  }

 private:
  FstBuilder* _b;
  IFileCtx*   _wc;
};

class FstReadMemory {
 public:
  FstReadMemory(int32_t size, const std::string& fileName = TD_TMP_DIR_PATH "tindex.tindex") {
    _wc = idxFileCtxCreate(TFILE, fileName.c_str(), true, 64 * 1024);
    _w = idxFileCreate(_wc);
    _size = size;
    memset((void*)&_s, 0, sizeof(_s));
  }
  bool init() {
    char* buf = (char*)taosMemoryCalloc(1, sizeof(char) * _size);
    int   nRead = idxFileRead(_w, (uint8_t*)buf, _size);
    if (nRead <= 0) {
      return false;
    }
    _size = nRead;
    _s = fstSliceCreate((uint8_t*)buf, _size);
    _fst = fstCreate(&_s);
    taosMemoryFree(buf);
    return _fst != NULL;
  }
  bool Get(const std::string& key, uint64_t* val) {
    // char buf[128] = {0};
    // int  len = 0;
    // taosMbsToUcs4(key.c_str(), key.size(), buf, 128, &len);
    // FstSlice skey = fstSliceCreate((uint8_t*)buf, len);

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
  bool Search(FAutoCtx* ctx, std::vector<uint64_t>& result) {
    FStmBuilder* sb = fstSearch(_fst, ctx);
    FStmSt*      st = stmBuilderIntoStm(sb);
    FStmStRslt*  rt = NULL;
    while ((rt = stmStNextWith(st, NULL)) != NULL) {
      // result.push_back((uint64_t)(rt->out.out));
      FstSlice*   s = &rt->data;
      int32_t     sz = 0;
      char*       ch = (char*)fstSliceData(s, &sz);
      std::string key(ch, sz);
      printf("key: %s, val: %" PRIu64 "\n", key.c_str(), (uint64_t)(rt->out.out));
      result.push_back(rt->out.out);
      swsResultDestroy(rt);
    }
    stmStDestroy(st);
    stmBuilderDestroy(sb);
    return true;
  }
  bool SearchRange(FAutoCtx* ctx, const std::string& low, RangeType lowType, const std::string& high,
                   RangeType highType, std::vector<uint64_t>& result) {
    FStmBuilder* sb = fstSearch(_fst, ctx);

    FstSlice l = fstSliceCreate((uint8_t*)low.c_str(), low.size());
    FstSlice h = fstSliceCreate((uint8_t*)high.c_str(), high.size());

    // range [low, high);
    stmBuilderSetRange(sb, &l, lowType);
    stmBuilderSetRange(sb, &h, highType);

    fstSliceDestroy(&l);
    fstSliceDestroy(&h);

    FStmSt*     st = stmBuilderIntoStm(sb);
    FStmStRslt* rt = NULL;
    while ((rt = stmStNextWith(st, NULL)) != NULL) {
      // result.push_back((uint64_t)(rt->out.out));
      FstSlice*   s = &rt->data;
      int32_t     sz = 0;
      char*       ch = (char*)fstSliceData(s, &sz);
      std::string key(ch, sz);
      printf("key: %s, val: %" PRIu64 "\n", key.c_str(), (uint64_t)(rt->out.out));
      result.push_back(rt->out.out);
      swsResultDestroy(rt);
    }
    stmStDestroy(st);
    stmBuilderDestroy(sb);
    return true;
  }
  bool SearchWithTimeCostUs(FAutoCtx* ctx, std::vector<uint64_t>& result) {
    int64_t s = taosGetTimestampUs();
    bool    ok = this->Search(ctx, result);
    int64_t e = taosGetTimestampUs();
    return ok;
  }

  ~FstReadMemory() {
    idxFileDestroy(_w);
    fstDestroy(_fst);
    fstSliceDestroy(&_s);
    idxFileCtxDestroy(_wc, false);
  }

 private:
  IdxFstFile* _w;
  Fst*        _fst;
  FstSlice    _s;
  IFileCtx*   _wc;
  int32_t     _size;
};

#define L 200
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

void checkMillonWriteAndReadOfFst() {
  FstWriter* fw = new FstWriter;
  Performance_fstWriteRecords(fw);
  delete fw;
  FstReadMemory* fr = new FstReadMemory(1024 * 8 * 1024);

  if (fr->init()) {
    printf("success to init fst read");
  }

  Performance_fstReadRecords(fr);
  delete fr;
}
void checkFstLongTerm() {
  FstWriter* fw = new FstWriter;
  // Performance_fstWriteRecords(fw);

  fw->Put("A B", 1);
  fw->Put("C", 2);
  fw->Put("a", 3);
  delete fw;

  FstReadMemory* m = new FstReadMemory(1024 * 64);
  if (m->init() == false) {
    std::cout << "init readMemory failed" << std::endl;
    delete m;
    return;
  }
  {
    uint64_t val = 0;
    if (m->Get("A B", &val)) {
      std::cout << "success to Get: " << val << std::endl;
    } else {
      std::cout << "failed to Get:" << val << std::endl;
    }
  }
  {
    uint64_t val = 0;
    if (m->Get("C", &val)) {
      std::cout << "success to Get: " << val << std::endl;
    } else {
      std::cout << "failed to Get:" << val << std::endl;
    }
  }
  {
    uint64_t val = 0;
    if (m->Get("a", &val)) {
      std::cout << "success to Get: " << val << std::endl;
    } else {
      std::cout << "failed to Get:" << val << std::endl;
    }
  }

  // prefix search
  // std::vector<uint64_t> result;

  // FAutoCtx* ctx = automCtxCreate((void*)"ab", AUTOMATION_ALWAYS);
  // m->Search(ctx, result);
  // std::cout << "size: " << result.size() << std::endl;
  // assert(result.size() == count);
  // for (int i = 0; i < result.size(); i++) {
  // assert(result[i] == i);  // check result
  //}
  // taosMemoryFree(ctx);
  // delete m;
}
void checkFstCheckIterator1() {
  FstWriter* fw = new FstWriter;
  int64_t    s = taosGetTimestampUs();
  int        count = 2;
  // Performance_fstWriteRecords(fw);
  int64_t e = taosGetTimestampUs();

  std::cout << "insert data count :  " << count << "elapas time: " << e - s << std::endl;

  fw->Put("test1&^D&10", 1);
  fw->Put("test2&^D&10", 2);
  delete fw;

  FstReadMemory* m = new FstReadMemory(1024 * 64);
  if (m->init() == false) {
    std::cout << "init readMemory failed" << std::endl;
    delete m;
    return;
  }

  // prefix search
  std::vector<uint64_t> result;

  FAutoCtx* ctx = automCtxCreate((void*)"He", AUTOMATION_ALWAYS);
  m->Search(ctx, result);
  std::cout << "size: " << result.size() << std::endl;
  // assert(result.size() == count);
  for (int i = 0; i < result.size(); i++) {
    // assert(result[i] == i);  // check result
  }
  automCtxDestroy(ctx);

  delete m;
}
void checkFstCheckIterator2() {
  FstWriter* fw = new FstWriter;
  int64_t    s = taosGetTimestampUs();
  int        count = 2;
  // Performance_fstWriteRecords(fw);
  int64_t e = taosGetTimestampUs();

  std::cout << "insert data count :  " << count << "elapas time: " << e - s << std::endl;

  fw->Put("a", 1);
  fw->Put("b", 2);
  fw->Put("c", 4);
  delete fw;

  FstReadMemory* m = new FstReadMemory(1024 * 64);
  if (m->init() == false) {
    std::cout << "init readMemory failed" << std::endl;
    delete m;
    return;
  }

  // prefix search
  std::vector<uint64_t> result;

  FAutoCtx* ctx = automCtxCreate((void*)"He", AUTOMATION_ALWAYS);
  m->Search(ctx, result);
  std::cout << "size: " << result.size() << std::endl;
  // assert(result.size() == count);
  for (int i = 0; i < result.size(); i++) {
    // assert(result[i] == i);  // check result
  }
  automCtxDestroy(ctx);

  delete m;
}
void checkFstCheckIteratorPrefix() {
  FstWriter* fw = new FstWriter;
  int64_t    s = taosGetTimestampUs();
  int        count = 2;
  // Performance_fstWriteRecords(fw);
  int64_t e = taosGetTimestampUs();

  std::cout << "insert data count :  " << count << "elapas time: " << e - s << std::endl;

  fw->Put("Hello world", 1);
  fw->Put("Hello worle", 2);
  fw->Put("hello worlf", 4);
  fw->Put("ja", 4);
  fw->Put("jb", 4);
  fw->Put("jc", 4);
  fw->Put("jddddddddd", 4);
  fw->Put("jefffffff", 4);
  delete fw;

  FstReadMemory* m = new FstReadMemory(1024 * 64);
  if (m->init() == false) {
    std::cout << "init readMemory failed" << std::endl;
    delete m;
    return;
  }
  {
    // prefix search
    std::vector<uint64_t> result;

    FAutoCtx* ctx = automCtxCreate((void*)"he", AUTOMATION_PREFIX);
    m->Search(ctx, result);
    assert(result.size() == 1);
    automCtxDestroy(ctx);
  }
  {
    // prefix search
    std::vector<uint64_t> result;

    FAutoCtx* ctx = automCtxCreate((void*)"Hello", AUTOMATION_PREFIX);
    m->Search(ctx, result);
    assert(result.size() == 2);
    automCtxDestroy(ctx);
  }
  {
    std::vector<uint64_t> result;

    FAutoCtx* ctx = automCtxCreate((void*)"jddd", AUTOMATION_PREFIX);
    m->Search(ctx, result);
    assert(result.size() == 1);
    automCtxDestroy(ctx);
  }
  delete m;
}
void checkFstCheckIteratorRange1() {
  FstWriter* fw = new FstWriter;
  int64_t    s = taosGetTimestampUs();
  int        count = 2;
  // Performance_fstWriteRecords(fw);
  int64_t e = taosGetTimestampUs();

  std::cout << "insert data count :  " << count << "elapas time: " << e - s << std::endl;

  fw->Put("a", 1);
  fw->Put("b", 2);
  fw->Put("c", 3);
  fw->Put("d", 4);
  fw->Put("e", 5);
  fw->Put("f", 5);
  fw->Put("G", 5);
  delete fw;

  FstReadMemory* m = new FstReadMemory(1024 * 64);
  if (m->init() == false) {
    std::cout << "init readMemory failed" << std::endl;
    delete m;
    return;
  }
  {
    // prefix search
    std::vector<uint64_t> result;
    FAutoCtx*             ctx = automCtxCreate((void*)"he", AUTOMATION_ALWAYS);
    // [b, e)
    m->SearchRange(ctx, "b", GE, "e", LT, result);
    assert(result.size() == 3);
    automCtxDestroy(ctx);
  }
  {
    // prefix search
    std::vector<uint64_t> result;
    FAutoCtx*             ctx = automCtxCreate((void*)"he", AUTOMATION_ALWAYS);
    // [b, e)
    m->SearchRange(ctx, "b", GT, "e", LT, result);
    assert(result.size() == 2);
    automCtxDestroy(ctx);
  }
  {
    // prefix search
    std::vector<uint64_t> result;
    FAutoCtx*             ctx = automCtxCreate((void*)"he", AUTOMATION_ALWAYS);
    // [b, e)
    m->SearchRange(ctx, "b", GT, "e", LE, result);
    assert(result.size() == 3);
    automCtxDestroy(ctx);
  }
  {
    // prefix search
    std::vector<uint64_t> result;
    FAutoCtx*             ctx = automCtxCreate((void*)"he", AUTOMATION_ALWAYS);
    // [b, e)
    m->SearchRange(ctx, "b", GE, "e", LE, result);
    assert(result.size() == 4);
    automCtxDestroy(ctx);
  }
  delete m;
}
void checkFstCheckIteratorRange2() {
  FstWriter* fw = new FstWriter;
  int64_t    s = taosGetTimestampUs();
  int        count = 2;
  // Performance_fstWriteRecords(fw);
  int64_t e = taosGetTimestampUs();

  std::cout << "insert data count :  " << count << "elapas time: " << e - s << std::endl;

  fw->Put("ab", 1);
  fw->Put("b", 2);
  fw->Put("cdd", 3);
  fw->Put("cde", 3);
  fw->Put("ddd", 4);
  fw->Put("ed", 5);
  delete fw;

  FstReadMemory* m = new FstReadMemory(1024 * 64);
  if (m->init() == false) {
    std::cout << "init readMemory failed" << std::endl;
    delete m;
    return;
  }
  {
    // range  search
    std::vector<uint64_t> result;
    FAutoCtx*             ctx = automCtxCreate((void*)"he", AUTOMATION_ALWAYS);
    // [b, e)
    m->SearchRange(ctx, "b", GE, "ed", LT, result);
    assert(result.size() == 4);
    automCtxDestroy(ctx);
  }
  {
    // range  search
    std::vector<uint64_t> result;
    FAutoCtx*             ctx = automCtxCreate((void*)"he", AUTOMATION_ALWAYS);
    // [b, e)
    m->SearchRange(ctx, "bb", GE, "ed", LT, result);
    assert(result.size() == 3);
    automCtxDestroy(ctx);
  }
  {
    // range  search
    std::vector<uint64_t> result;
    FAutoCtx*             ctx = automCtxCreate((void*)"he", AUTOMATION_ALWAYS);
    // [b, e)
    m->SearchRange(ctx, "b", GE, "ed", LE, result);
    assert(result.size() == 5);
    automCtxDestroy(ctx);
    // taosMemoryFree(ctx);
  }
  {
    // range  search
    std::vector<uint64_t> result;
    FAutoCtx*             ctx = automCtxCreate((void*)"he", AUTOMATION_ALWAYS);
    // [b, e)
    m->SearchRange(ctx, "b", GT, "ed", LE, result);
    assert(result.size() == 4);
    automCtxDestroy(ctx);
  }
  {
    // range  search
    std::vector<uint64_t> result;
    FAutoCtx*             ctx = automCtxCreate((void*)"he", AUTOMATION_ALWAYS);
    // [b, e)
    m->SearchRange(ctx, "b", GT, "ed", LT, result);
    assert(result.size() == 3);
    automCtxDestroy(ctx);
  }
  delete m;
}
void checkFstCheckIteratorRange3() {
  FstWriter* fw = new FstWriter;
  int64_t    s = taosGetTimestampUs();
  int        count = 2;
  // Performance_fstWriteRecords(fw);
  int64_t e = taosGetTimestampUs();

  std::cout << "insert data count :  " << count << "elapas time: " << e - s << std::endl;

  fw->Put("ab", 1);
  fw->Put("b", 2);
  fw->Put("cdd", 3);
  fw->Put("cde", 3);
  fw->Put("ddd", 4);
  fw->Put("ed", 5);
  delete fw;

  FstReadMemory* m = new FstReadMemory(1024 * 64);
  if (m->init() == false) {
    std::cout << "init readMemory failed" << std::endl;
    delete m;
    return;
  }
  {
    // range  search
    std::vector<uint64_t> result;
    FAutoCtx*             ctx = automCtxCreate((void*)"he", AUTOMATION_ALWAYS);
    // [b, e)
    m->SearchRange(ctx, "b", GE, "", (RangeType)10, result);
    assert(result.size() == 5);
    automCtxDestroy(ctx);
  }
  {
    // range  search
    std::vector<uint64_t> result;
    FAutoCtx*             ctx = automCtxCreate((void*)"he", AUTOMATION_ALWAYS);
    // [b, e)
    m->SearchRange(ctx, "", (RangeType)20, "ab", LE, result);
    assert(result.size() == 1);
    automCtxDestroy(ctx);
    // taosMemoryFree(ctx);
  }
  {
    // range  search
    std::vector<uint64_t> result;
    FAutoCtx*             ctx = automCtxCreate((void*)"he", AUTOMATION_ALWAYS);
    // [b, e)
    m->SearchRange(ctx, "", (RangeType)30, "ab", LT, result);
    assert(result.size() == 0);
    automCtxDestroy(ctx);
  }
  {
    // range  search
    std::vector<uint64_t> result;
    FAutoCtx*             ctx = automCtxCreate((void*)"he", AUTOMATION_ALWAYS);
    // [b, e)
    m->SearchRange(ctx, "ed", GT, "ed", (RangeType)40, result);
    assert(result.size() == 0);
    automCtxDestroy(ctx);
  }
  delete m;
}

void fst_get(Fst* fst) {
  for (int i = 0; i < 10000; i++) {
    std::string term = "Hello World";
    FstSlice    key = fstSliceCreate((uint8_t*)term.c_str(), term.size());
    uint64_t    offset = 0;
    bool        ret = fstGet(fst, &key, &offset);
    if (ret == false) {
      std::cout << "not found" << std::endl;
    } else {
      std::cout << "found value:" << offset << std::endl;
    }
  }
}

#define NUM_OF_THREAD 10
void validateTFile(char* arg) {
  std::thread threads[NUM_OF_THREAD];
  // std::vector<std::thread> threads;
  SIndex* index = (SIndex*)taosMemoryCalloc(1, sizeof(SIndex));
  index->path = taosStrdup(arg);
  TFileReader* reader = tfileReaderOpen(index, 0, 20000000, "tag1");

  for (int i = 0; i < NUM_OF_THREAD; i++) {
    threads[i] = std::thread(fst_get, reader->fst);
    // threads.push_back(fst_get, reader->fst);
    // std::thread t(fst_get, reader->fst);
  }
  for (int i = 0; i < NUM_OF_THREAD; i++) {
    // wait join
    threads[i].join();
  }
}

void iterTFileReader(char* path, char* uid, char* colName, char* ver) {
  // tfInit();

  uint64_t suid = atoi(uid);
  int      version = atoi(ver);

  TFileReader* reader = tfileReaderOpen(NULL, suid, version, colName);

  Iterate* iter = tfileIteratorCreate(reader);
  bool     tn = iter ? iter->next(iter) : false;
  int      count = 0;
  int      termCount = 0;
  while (tn == true) {
    count++;
    IterateValue* cv = iter->getValue(iter);
    termCount += (int)taosArrayGetSize(cv->val);
    printf("col val: %s, size: %d\n", cv->colVal, (int)taosArrayGetSize(cv->val));
    tn = iter->next(iter);
  }
  printf("total size: %d\n term count: %d\n", count, termCount);

  tfileIteratorDestroy(iter);
}

// int main(int argc, char* argv[]) {
//   // tool to check all kind of fst test
//   // if (argc > 1) { validateTFile(argv[1]); }
//   // if (argc > 4) {
//   // path suid colName ver
//   // iterTFileReader(argv[1], argv[2], argv[3], argv[4]);
//   //}
//   checkFstCheckIterator1();
//   // checkFstCheckIterator2();
//   // checkFstCheckIteratorPrefix();
//   // checkFstCheckIteratorRange1();
//   // checkFstCheckIteratorRange2();
//   // checkFstCheckIteratorRange3();
//   // checkFstLongTerm();
//   // checkFstPrefixSearch();

//   // checkMillonWriteAndReadOfFst();

//   return 1;
// }
TEST_F(FstEnv, checkIterator1) { checkFstCheckIterator1(); }
TEST_F(FstEnv, checkItertor2) { checkFstCheckIterator2(); }
TEST_F(FstEnv, checkPrefix) { checkFstCheckIteratorPrefix(); }
TEST_F(FstEnv, checkRange1) { checkFstCheckIteratorRange1(); }
TEST_F(FstEnv, checkRange2) { checkFstCheckIteratorRange2(); }
TEST_F(FstEnv, checkRange3) { checkFstCheckIteratorRange3(); }
TEST_F(FstEnv, checkLongTerm) { checkFstLongTerm(); }
TEST_F(FstEnv, checkMillonWriteData) { checkMillonWriteAndReadOfFst(); }
