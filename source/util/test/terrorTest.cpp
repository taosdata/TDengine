#include <gtest/gtest.h>
#include <cassert>

#include <iostream>
#include "taoserror.h"

using namespace std;

TEST(TAOS_ERROR_TEST, terror_test) {
  int32_t errSize = taosGetErrSize();
  for (int32_t i = 0; i < errSize; ++i) {
    STaosError *pInfo = &errors[i];
    std::cout << i + 1 << " " << pInfo->macro << " " << pInfo->val << std::endl;
  }
}