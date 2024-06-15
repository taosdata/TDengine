#include <gtest/gtest.h>
#include <cassert>

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unistd.h>
#include <limits.h>
#include <sys/stat.h>
#include <algorithm>
#include <unordered_map>
#include "taoserror.h"

using namespace std;

typedef struct {
  int32_t     val;
  const char* str;
  const char* macro;
} STestTaosError;




TEST(TAOS_ERROR_TEST, terror_test) {
  int32_t errSize = taosGetErrSize();
  for (int32_t i = 0; i < errSize; ++i) {
    STaosError *pInfo = &errors[i];
    std::cout << i + 1 << " " << pInfo->macro << " " << pInfo->val << std::endl;
  }
}