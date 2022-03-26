
#include <algorithm>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include "index.h"
#include "indexInt.h"
#include "index_cache.h"
#include "index_fst.h"
#include "index_fst_counting_writer.h"
#include "index_fst_util.h"
#include "index_tfile.h"
#include "tskiplist.h"
#include "tutil.h"
void* callback(void* s) { return s; }

static std::string fileName = "/tmp/tindex.tindex";
class FstWriter {
 public:
  FstWriter() {
    taosRemoveFile(fileName.c_str());
    _wc = writerCtxCreate(TFile, fileName.c_str(), false, 64 * 1024 * 1024);
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
    fstBuilderFinish(_b);
    fstBuilderDestroy(_b);

    writerCtxDestroy(_wc, false);
  }

 private:
  FstBuilder* _b;
  WriterCtx*  _wc;
};

class FstReadMemory {
 public:
  FstReadMemory(size_t size, const std::string& fileName = "/tmp/tindex.tindex") {
    _wc = writerCtxCreate(TFile, fileName.c_str(), true, 64 * 1024);
    _w = fstCountingWriterCreate(_wc);
    _size = size;
    memset((void*)&_s, 0, sizeof(_s));
  }
  bool init() {
    char* buf = (char*)taosMemoryCalloc(1, sizeof(char) * _size);
    int   nRead = fstCountingWriterRead(_w, (uint8_t*)buf, _size);
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
  bool Search(AutomationCtx* ctx, std::vector<uint64_t>& result) {
    FstStreamBuilder*      sb = fstSearch(_fst, ctx);
    StreamWithState*       st = streamBuilderIntoStream(sb);
    StreamWithStateResult* rt = NULL;
    while ((rt = streamWithStateNextWith(st, NULL)) != NULL) {
      // result.push_back((uint64_t)(rt->out.out));
      FstSlice*   s = &rt->data;
      int32_t     sz = 0;
      char*       ch = (char*)fstSliceData(s, &sz);
      std::string key(ch, sz);
      printf("key: %s, val: %" PRIu64 "\n", key.c_str(), (uint64_t)(rt->out.out));
      swsResultDestroy(rt);
    }
    for (size_t i = 0; i < result.size(); i++) {
    }
    std::cout << std::endl;
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
    writerCtxDestroy(_wc, false);
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

void checkMillonWriteAndReadOfFst() {
  FstWriter* fw = new FstWriter;
  Performance_fstWriteRecords(fw);
  delete fw;
  FstReadMemory* fr = new FstReadMemory(1024 * 64 * 1024);

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

  // AutomationCtx* ctx = automCtxCreate((void*)"ab", AUTOMATION_ALWAYS);
  // m->Search(ctx, result);
  // std::cout << "size: " << result.size() << std::endl;
  // assert(result.size() == count);
  // for (int i = 0; i < result.size(); i++) {
  // assert(result[i] == i);  // check result
  //}
  // taosMemoryFree(ctx);
  // delete m;
}
void checkFstCheckIterator() {
  FstWriter* fw = new FstWriter;
  int64_t    s = taosGetTimestampUs();
  int        count = 2;
  // Performance_fstWriteRecords(fw);
  int64_t e = taosGetTimestampUs();

  std::cout << "insert data count :  " << count << "elapas time: " << e - s << std::endl;

  fw->Put("Hello world", 1);
  fw->Put("hello world", 2);
  fw->Put("hello worle", 3);
  fw->Put("hello worlf", 4);
  delete fw;

  FstReadMemory* m = new FstReadMemory(1024 * 64);
  if (m->init() == false) {
    std::cout << "init readMemory failed" << std::endl;
    delete m;
    return;
  }

  // prefix search
  std::vector<uint64_t> result;

  AutomationCtx* ctx = automCtxCreate((void*)"H", AUTOMATION_PREFIX);
  m->Search(ctx, result);
  std::cout << "size: " << result.size() << std::endl;
  // assert(result.size() == count);
  for (int i = 0; i < result.size(); i++) {
    // assert(result[i] == i);  // check result
  }

  taosMemoryFree(ctx);
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
  TFileReader* reader = tfileReaderOpen(arg, 0, 20000000, "tag1");

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

  TFileReader* reader = tfileReaderOpen(path, suid, version, colName);

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

int main(int argc, char* argv[]) {
  // tool to check all kind of fst test
  // if (argc > 1) { validateTFile(argv[1]); }
  // if (argc > 4) {
  // path suid colName ver
  // iterTFileReader(argv[1], argv[2], argv[3], argv[4]);
  //}
  checkFstCheckIterator();
  // checkFstLongTerm();
  // checkFstPrefixSearch();

  // checkMillonWriteAndReadOfFst();

  return 1;
}
