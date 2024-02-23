#include <gtest/gtest.h>
#include <cassert>

#include <iostream>
#include "os.h"
#include "osTime.h"
#include "taos.h"
#include "taoserror.h"
#include "tbase58.h"
#include "tglobal.h"

using namespace std;

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

static void checkBase58Codec(uint8_t *pRaw, int32_t rawLen, int32_t index) {
  int64_t start = taosGetTimestampUs();
  char   *pEnc = base58_encode((const uint8_t *)pRaw, rawLen);
  ASSERT_NE(nullptr, pEnc);

  int32_t encLen = strlen(pEnc);
  int64_t endOfEnc = taosGetTimestampUs();
  std::cout << "index:" << index << ", encLen is " << encLen << ", cost:" << endOfEnc - start << " us" << std::endl;
  int32_t decLen = 0;
  char   *pDec = (char *)base58_decode((const char *)pEnc, encLen, &decLen);
  std::cout << "index:" << index << ", decLen is " << decLen << ", cost:" << taosGetTimestampUs() - endOfEnc << " us"
            << std::endl;
  ASSERT_NE(nullptr, pDec);
  ASSERT_EQ(rawLen, decLen);
  ASSERT_LE(rawLen, encLen);
  ASSERT_EQ(0, strncmp((char *)pRaw, pDec, rawLen));
  taosMemoryFreeClear(pDec);
  taosMemoryFreeClear(pEnc);
}

TEST(TD_BASE_CODEC_TEST, tbase58_test) {
  const int32_t TEST_LEN_MAX = TBASE_MAX_ILEN;
  const int32_t TEST_LEN_STEP = 10;
  int32_t       rawLen = 0;
  uint8_t      *pRaw = NULL;

  pRaw = (uint8_t *)taosMemoryCalloc(1, TEST_LEN_MAX);
  ASSERT_NE(nullptr, pRaw);

  // 1. normal case
  // string blend with char and '\0'
  rawLen = TEST_LEN_MAX;
  for (int32_t i = 0; i < TEST_LEN_MAX; i += 500) {
    checkBase58Codec(pRaw, rawLen, i);
    pRaw[i] = i & 127;
  }

  // string without '\0'
  for (int32_t i = 0; i < TEST_LEN_MAX; ++i) {
    pRaw[i] = i & 127;
  }
  checkBase58Codec(pRaw, TEST_LEN_MAX, 0);
  for (int32_t i = 0; i < TEST_LEN_MAX; i += 500) {
    rawLen = i;
    checkBase58Codec(pRaw, rawLen, i);
  }
  taosMemoryFreeClear(pRaw);
  ASSERT_EQ(nullptr, pRaw);

  // 2. overflow case
  char  tmp[1];
  char *pEnc = base58_encode((const uint8_t *)tmp, TBASE_MAX_ILEN + 1);
  ASSERT_EQ(nullptr, pEnc);
  char *pDec = (char *)base58_decode((const char *)tmp, TBASE_MAX_OLEN + 1, NULL);
  ASSERT_EQ(nullptr, pDec);

  taosMemoryFreeClear(pRaw);
  ASSERT_EQ(nullptr, pRaw);
}