#include <gtest/gtest.h>
#include <string>
#include <iostream>
#include "index.h"
#include "indexInt.h"




TEST(IndexTest, index_create_test) {
  SIndexOpts *opts = indexOptsCreate();
  SIndex *index = indexOpen(opts, "./test");
  if (index == NULL) {
    std::cout << "index open failed" << std::endl; 
  }

  
  SArray* terms = indexMultiTermCreate();
  indexMultiTermAdd(terms, "tag1", strlen("tag1"), "field", strlen("field"));
  for (int i = 0; i < 10; i++) {
    indexPut(index, terms, i); 
  } 
  indexMultiTermDestroy(terms);
 

  // query
  SIndexMultiTermQuery *multiQuery = indexMultiTermQueryCreate(MUST); 
  indexMultiTermQueryAdd(multiQuery, "tag1", strlen("tag1"), "field", strlen("field"), QUERY_PREFIX);

  SArray *result = (SArray *)taosArrayInit(10, sizeof(int));   
  indexSearch(index, multiQuery, result);

  std::cout << "taos'size : " << taosArrayGetSize(result) << std::endl;
  for (int i = 0;  i < taosArrayGetSize(result); i++) {
    int *v = (int *)taosArrayGet(result, i);
    std::cout << "value --->" << *v  << std::endl;
  }
  indexMultiTermQueryDestroy(multiQuery);

  indexOptsDestroy(opts); 
  indexClose(index); 
  //
}
