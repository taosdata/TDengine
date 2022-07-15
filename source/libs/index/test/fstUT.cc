
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
#include "tglobal.h"
#include "tlog.h"
#include "tskiplist.h"
#include "tutil.h"

static std::string dir = TD_TMP_DIR_PATH "index";

static char indexlog[PATH_MAX] = {0};
static char tindex[PATH_MAX] = {0};
static char tindexDir[PATH_MAX] = {0};

static void EnvInit() {
  std::string path = dir;
  taosRemoveDir(path.c_str());
  taosMkDir(path.c_str());
  // init log file
  tstrncpy(tsLogDir, path.c_str(), PATH_MAX);
  if (taosInitLog("tindex.idx", 1) != 0) {
    printf("failed to init log");
  }
  // init index file
  memset(tindex, 0, sizeof(tindex));
  snprintf(tindex, PATH_MAX, "%s/tindex.idx", path.c_str());
}
static void EnvCleanup() {}
class FstWriter {
 public:
  FstWriter() {
    _wc = idxFileCtxCreate(TFILE, tindex, false, 64 * 1024 * 1024);
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
    fstBuilderDestroy(_b);

    idxFileCtxDestroy(_wc, false);
  }

 private:
  FstBuilder* _b;
  IFileCtx*   _wc;
};

class FstReadMemory {
 public:
  FstReadMemory(size_t size) {
    _wc = idxFileCtxCreate(TFILE, tindex, true, 64 * 1024);
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
    taosMemoryFreeClear(buf);
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
      swsResultDestroy(rt);
    }
    std::cout << std::endl;
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
  size_t      _size;
};

class FstWriterEnv : public ::testing::Test {
 protected:
  virtual void SetUp() { fw = new FstWriter(); }
  virtual void TearDown() { delete fw; }
  FstWriter*   fw = NULL;
};

class FstReadEnv : public ::testing::Test {
 protected:
  virtual void   SetUp() { fr = new FstReadMemory(1024); }
  virtual void   TearDown() { delete fr; }
  FstReadMemory* fr = NULL;
};

class TFst {
 public:
  void CreateWriter() { fw = new FstWriter; }
  void ReCreateWriter() {
    if (fw != NULL) delete fw;
    fw = new FstWriter;
  }
  void DestroyWriter() {
    if (fw != NULL) delete fw;
  }
  void CreateReader() {
    fr = new FstReadMemory(1024);
    fr->init();
  }
  void ReCreateReader() {
    if (fr != NULL) delete fr;
    fr = new FstReadMemory(1024);
  }
  void DestroyReader() {
    delete fr;
    fr = NULL;
  }
  bool Put(const std::string& k, uint64_t v) {
    if (fw == NULL) {
      return false;
    }
    return fw->Put(k, v);
  }
  bool Get(const std::string& k, uint64_t* v) {
    if (fr == NULL) {
      return false;
    }
    return fr->Get(k, v);
  }
  bool Search(FAutoCtx* ctx, std::vector<uint64_t>& result) {
    // add more
    return fr->Search(ctx, result);
  }

 private:
  FstWriter*     fw;
  FstReadMemory* fr;
};
class FstEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    EnvInit();
    fst = new TFst;
  }
  virtual void TearDown() { delete fst; }
  TFst*        fst;
};

TEST_F(FstEnv, writeNormal) {
  fst->CreateWriter();
  std::string str("11");
  for (int i = 0; i < 10; i++) {
    str[0] = '1' + i;
    str.resize(2);
    assert(fst->Put(str, i) == true);
  }
  // order failed
  assert(fst->Put("11", 1) == false);

  fst->DestroyWriter();

  fst->CreateReader();
  uint64_t val;
  assert(fst->Get("1", &val) == false);
  assert(fst->Get("11", &val) == true);
  assert(val == 0);

  std::vector<uint64_t> rlt;
  FAutoCtx*             ctx = automCtxCreate((void*)"ab", AUTOMATION_ALWAYS);
  assert(fst->Search(ctx, rlt) == true);
}
TEST_F(FstEnv, WriteMillonrRecord) {}
TEST_F(FstEnv, writeAbNormal) {
  fst->CreateWriter();
  std::string str1("voltage&\b&ab");
  std::string str2("voltbge&\b&ab");

  fst->Put(str1, 1);
  fst->Put(str2, 2);

  fst->DestroyWriter();

  fst->CreateReader();
  uint64_t val;
  assert(fst->Get("1", &val) == false);
  assert(fst->Get("voltage&\b&ab", &val) == true);
  assert(val == 1);
}
