#include <gtest/gtest.h>
#include <string>
#include <iostream>
#include "index.h"
#include "indexInt.h"
#include "index_fst.h"
#include "index_fst_util.h"
#include "index_fst_counting_writer.h"




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

int main(int argc, char** argv) {
  // test write
  FstBuilder *b = fstBuilderCreate(NULL, 0);
  {
    std::string str("aaa");
    FstSlice key = fstSliceCreate((uint8_t *)str.c_str(), str.size());
    Output   val = 1;
    fstBuilderInsert(b, key, val); 
  }

  //std::string str1("bcd");
  //FstSlice key1 = fstSliceCreate((uint8_t *)str1.c_str(), str1.size());
  //Output   val2 = 10;
  //
   
  {
     
    for (size_t i = 1; i < 26; i++) {
      std::string str("aaa");
      str[2] = 'a' + i ;
      FstSlice key = fstSliceCreate((uint8_t *)str.c_str(), str.size());
      Output   val = 0;
      fstBuilderInsert(b, key, val); 
    }
    
  } 
  fstBuilderFinish(b);
  fstBuilderDestroy(b);


  char buf[64 * 1024] = {0};     

  FstSlice s; 

  FstCountingWriter *w = fstCountingWriterCreate(NULL, true);
  int nRead = fstCountingWriterRead(w, (uint8_t *)buf, sizeof(buf)); 
  assert(nRead <= sizeof(buf));  
  s = fstSliceCreate((uint8_t *)buf, nRead);
  fstCountingWriterDestroy(w);


  // test reader
  
      
  Fst *fst = fstCreate(&s); 
  {
    std::string str("aax"); 
    uint64_t out;
    
       
    FstSlice key = fstSliceCreate((uint8_t *)str.c_str(), str.size());
    bool ok = fstGet(fst, &key, &out); 
    if (ok == true) {
      printf("val = %d\n", out);
      //indexInfo("Get key-value success, %s, %d", str.c_str(), out); 
    } else {
      //indexError("Get key-value failed, %s", str.c_str()); 
    }
  }
  fstSliceDestroy(&s);
  

  
  return 1;
}

//TEST(IndexFstBuilder, IndexFstInput) {
//
//}


