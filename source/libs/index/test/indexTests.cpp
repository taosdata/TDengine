#include <gtest/gtest.h>
#include <string>
#include <iostream>
#include "index.h"
#include "tutil.h"
#include "indexInt.h"
#include "index_fst.h"
#include "index_fst_util.h"
#include "index_fst_counting_writer.h"


class FstWriter {
  public:
    FstWriter() {
      _b = fstBuilderCreate(NULL, 0);
    }  
   bool Put(const std::string &key, uint64_t val) {
      FstSlice skey = fstSliceCreate((uint8_t *)key.c_str(), key.size());   
      bool ok = fstBuilderInsert(_b, skey, val);
      fstSliceDestroy(&skey);
      return ok;
   }
   ~FstWriter() {
     fstBuilderFinish(_b);
     fstBuilderDestroy(_b);
   }
  private:
    FstBuilder *_b; 
};

class FstReadMemory {
  public:
   FstReadMemory(size_t size) {
     _w    = fstCountingWriterCreate(NULL, true); 
     _size = size; 
     memset((void *)&_s, 0, sizeof(_s));
   }
   bool init() {
     char *buf = (char *)calloc(1,  sizeof(char) * _size);
     int nRead = fstCountingWriterRead(_w, (uint8_t *)buf, _size); 
     if (nRead <= 0) { return false; } 
      _size = nRead;
     _s   = fstSliceCreate((uint8_t *)buf, _size);  
     _fst = fstCreate(&_s); 
     free(buf);
     return _fst != NULL;
   }
   bool Get(const std::string &key, uint64_t *val) {
     FstSlice skey = fstSliceCreate((uint8_t *)key.c_str(), key.size());   
     bool ok = fstGet(_fst, &skey, val); 
     fstSliceDestroy(&skey);
     return ok;
   }
   bool GetWithTimeCostUs(const std::string &key, uint64_t *val, uint64_t *elapse) {
     int64_t s = taosGetTimestampUs();
     bool ok = this->Get(key, val); 
     int64_t e = taosGetTimestampUs();
     *elapse = e - s;
     return ok; 
   }
   // add later
   bool Search(const std::string &key, std::vector<uint64_t> &result) {
      return true;
   }
    
   ~FstReadMemory() {
    fstCountingWriterDestroy(_w);
    fstDestroy(_fst);
    fstSliceDestroy(&_s);
  } 
  
  private:
   FstCountingWriter *_w; 
   Fst *_fst;
   FstSlice _s;  
   size_t _size;
   
}; 

//TEST(IndexTest, index_create_test) {
//  SIndexOpts *opts = indexOptsCreate();
//  SIndex *index = indexOpen(opts, "./test");
//  if (index == NULL) {
//    std::cout << "index open failed" << std::endl; 
//  }
//
//  
//  // write   
//  for (int i = 0; i < 100000; i++) {
//    SIndexMultiTerm* terms = indexMultiTermCreate();
//    std::string val = "field";    
//
//    indexMultiTermAdd(terms, "tag1", strlen("tag1"), val.c_str(), val.size());
//
//    val.append(std::to_string(i)); 
//    indexMultiTermAdd(terms, "tag2", strlen("tag2"), val.c_str(), val.size());
//
//    val.insert(0, std::to_string(i));
//    indexMultiTermAdd(terms, "tag3", strlen("tag3"), val.c_str(), val.size());
//
//    val.append("const");    
//    indexMultiTermAdd(terms, "tag4", strlen("tag4"), val.c_str(), val.size());
//
//     
//    indexPut(index, terms, i);
//    indexMultiTermDestroy(terms);
//  } 
// 
//
//  // query
//  SIndexMultiTermQuery *multiQuery = indexMultiTermQueryCreate(MUST); 
//  
//  indexMultiTermQueryAdd(multiQuery, "tag1", strlen("tag1"), "field", strlen("field"), QUERY_PREFIX);
//  indexMultiTermQueryAdd(multiQuery, "tag3", strlen("tag3"), "0field0", strlen("0field0"), QUERY_TERM);
//
//  SArray *result = (SArray *)taosArrayInit(10, sizeof(int));   
//  indexSearch(index, multiQuery, result);
//
//  std::cout << "taos'size : " << taosArrayGetSize(result) << std::endl;
//  for (int i = 0;  i < taosArrayGetSize(result); i++) {
//    int *v = (int *)taosArrayGet(result, i);
//    std::cout << "value --->" << *v  << std::endl;
//  }
//  // add more test case 
//  indexMultiTermQueryDestroy(multiQuery);
//
//  indexOptsDestroy(opts); 
//  indexClose(index); 
//  //
//}


#define L 100
#define M 100
#define N 100

int Performance_fstWriteRecords(FstWriter *b) {
  std::string str("aa"); 
  for (int i = 0; i < L; i++) {
    str[0] = 'a' + i;
    str.resize(2); 
    for(int j = 0; j < M; j++) {
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

void Performance_fstReadRecords(FstReadMemory *m) {
  std::string str("aa");
  for (int i = 0; i < M; i++) {
    str[0] = 'a' + i;
    str.resize(2); 
    for(int j = 0; j < N; j++) {
      str[1] = 'a' + j;
      str.resize(2);
      for (int k = 0; k < L; k++) {
        str.push_back('a');
        uint64_t val, cost; 
        if (m->GetWithTimeCostUs(str, &val, &cost)) {
          printf("succes to get kv(%s, %" PRId64"), cost: %" PRId64"\n", str.c_str(), val, cost);
        } else {
          printf("failed to get key: %s\n", str.c_str());
        }
      }
    } 
  }
}
void checkFstPerf() {
  FstWriter *fw = new FstWriter;
  int64_t s = taosGetTimestampUs();

  int num = Performance_fstWriteRecords(fw);
  int64_t e = taosGetTimestampUs();
  printf("write %d record cost %" PRId64"us\n", num,  e - s);
  delete fw;

  FstReadMemory *m = new FstReadMemory(1024 * 64);
  if (m->init()) {
    printf("success to init fst read");  
  }  
  Performance_fstReadRecords(m); 
   
  delete m;
} 


void validateFst() {
  int val = 100;
  int count = 100;
  FstWriter *fw = new FstWriter;
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
  FstReadMemory *m = new FstReadMemory(1024 * 64);
  if (m->init() == false) { 
    std::cout << "init readMemory failed" << std::endl; 
  }

  {
   std::string key("ab");
   uint64_t out;
   if (m->Get(key, &out)) {
     printf("success to get (%s, %" PRId64")\n", key.c_str(), out);
   } else {
     printf("failed to get(%s)\n", key.c_str());
   }
   for (int i = 0; i < count; i++) {
     key.push_back('a' + i);
     if (m->Get(key, &out) ) {
       assert(val - i ==  out);
       printf("success to get (%s, %" PRId64")\n", key.c_str(), out);
     } else {
       printf("failed to get(%s)\n", key.c_str());
    }
   }
  } 
  delete m;

} 
int main(int argc, char** argv) {
  checkFstPerf(); 
  return 1;
}

//TEST(IndexFstBuilder, IndexFstInput) {
//
//}


