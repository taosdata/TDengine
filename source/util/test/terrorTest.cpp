#include <gtest/gtest.h>
#include <cassert>

#define TAOS_ERROR_INFO

#include <iostream>
#include "os.h"
#include "osTime.h"
#include "taos.h"
#include "taoserror.h"
#include "tglobal.h"

extern STaosError errors[];

using namespace std;

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

TEST(TAOS_ERROR_TEST, terror_test) {
  int32_t errSize = taosGetErrSize();
  for (int32_t i = 0; i < errSize; ++i) {
    STaosError *pInfo = &errors[i];
    std::cout << i + 1 << " " << pInfo->origin << " " << pInfo->val << std::endl;
  }
}