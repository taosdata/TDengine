#include <gtest/gtest.h>
#include <string>
#include <iostream>
#include "index.h"



TEST(IndexTest, index_create_test) {
  SIndexOpts *opts = indexOptsCreate();
  SIndex *index = indexOpen(opts, "./");
  if (index == NULL) {
    std::cout << "index open failed" << std::endl; 
  }
  indexOptsDestroy(opts); 
}
