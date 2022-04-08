#include <gtest/gtest.h>

#include "tdbInt.h"

#include <string>

TEST(tdb_util_test, simple_test) {
  int vEncode = 5000;
  int vDecode;
  int nEncode;
  int nDecode;
  u8  buffer[128];

  nEncode = tdbPutVarInt(buffer, vEncode);

  nDecode = tdbGetVarInt(buffer, &vDecode);

  GTEST_ASSERT_EQ(nEncode, nDecode);
  GTEST_ASSERT_EQ(vEncode, vDecode);
}