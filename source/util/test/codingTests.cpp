#include <gtest/gtest.h>
#include <stdlib.h>
#include <time.h>
#include <random>

#include "tcoding.h"

static bool test_fixed_uint16(uint16_t value) {
  char     buf[20] = "\0";
  uint16_t value_check = 0;

  void *pBuf = (void *)buf;

  int   tlen = taosEncodeFixedU16(static_cast<void **>(&pBuf), value);
  void *ptr = taosDecodeFixedU16(static_cast<void *>(buf), &value_check);

  return ((ptr != NULL) && (value == value_check) && (pBuf == ptr) && POINTER_DISTANCE(pBuf, buf) == tlen);
}

static bool test_fixed_int16(int16_t value) {
  char    buf[20] = "\0";
  int16_t value_check = 0;

  void *pBuf = (void *)buf;

  int   tlen = taosEncodeFixedI16(static_cast<void **>(&pBuf), value);
  void *ptr = taosDecodeFixedI16(static_cast<void *>(buf), &value_check);

  return ((ptr != NULL) && (value == value_check) && (pBuf == ptr) && POINTER_DISTANCE(pBuf, buf) == tlen);
}

static bool test_fixed_uint32(uint32_t value) {
  char     buf[20] = "\0";
  uint32_t value_check = 0;

  void *pBuf = (void *)buf;

  int   tlen = taosEncodeFixedU32(static_cast<void **>(&pBuf), value);
  void *ptr = taosDecodeFixedU32(static_cast<void *>(buf), &value_check);

  return ((ptr != NULL) && (value == value_check) && (pBuf == ptr) && POINTER_DISTANCE(pBuf, buf) == tlen);
}

static bool test_fixed_int32(int32_t value) {
  char    buf[20] = "\0";
  int32_t value_check = 0;

  void *pBuf = (void *)buf;

  int   tlen = taosEncodeFixedI32(static_cast<void **>(&pBuf), value);
  void *ptr = taosDecodeFixedI32(static_cast<void *>(buf), &value_check);

  return ((ptr != NULL) && (value == value_check) && (pBuf == ptr) && POINTER_DISTANCE(pBuf, buf) == tlen);
}

static bool test_fixed_uint64(uint64_t value) {
  char     buf[20] = "\0";
  uint64_t value_check = 0;

  void *pBuf = (void *)buf;

  int   tlen = taosEncodeFixedU64(static_cast<void **>(&pBuf), value);
  void *ptr = taosDecodeFixedU64(static_cast<void *>(buf), &value_check);

  return ((ptr != NULL) && (value == value_check) && (pBuf == ptr) && POINTER_DISTANCE(pBuf, buf) == tlen);
}

static bool test_fixed_int64(int64_t value) {
  char    buf[20] = "\0";
  int64_t value_check = 0;

  void *pBuf = (void *)buf;

  int   tlen = taosEncodeFixedI64(static_cast<void **>(&pBuf), value);
  void *ptr = taosDecodeFixedI64(static_cast<void *>(buf), &value_check);

  return ((ptr != NULL) && (value == value_check) && (pBuf == ptr) && POINTER_DISTANCE(pBuf, buf) == tlen);
}

static bool test_variant_uint16(uint16_t value) {
  char     buf[20] = "\0";
  uint16_t value_check = 0;

  void *pBuf = (void *)buf;

  int   tlen = taosEncodeVariantU16(static_cast<void **>(&pBuf), value);
  void *ptr = taosDecodeVariantU16(static_cast<void *>(buf), &value_check);

  return ((ptr != NULL) && (value == value_check) && (pBuf == ptr) && POINTER_DISTANCE(pBuf, buf) == tlen);
}

static bool test_variant_int16(int16_t value) {
  char    buf[20] = "\0";
  int16_t value_check = 0;

  void *pBuf = (void *)buf;

  int   tlen = taosEncodeVariantI16(static_cast<void **>(&pBuf), value);
  void *ptr = taosDecodeVariantI16(static_cast<void *>(buf), &value_check);

  return ((ptr != NULL) && (value == value_check) && (pBuf == ptr) && POINTER_DISTANCE(pBuf, buf) == tlen);
}

static bool test_variant_uint32(uint32_t value) {
  char     buf[20] = "\0";
  uint32_t value_check = 0;

  void *pBuf = (void *)buf;

  int   tlen = taosEncodeVariantU32(static_cast<void **>(&pBuf), value);
  void *ptr = taosDecodeVariantU32(static_cast<void *>(buf), &value_check);

  return ((ptr != NULL) && (value == value_check) && (pBuf == ptr) && POINTER_DISTANCE(pBuf, buf) == tlen);
}

static bool test_variant_int32(int32_t value) {
  char    buf[20] = "\0";
  int32_t value_check = 0;

  void *pBuf = (void *)buf;

  int   tlen = taosEncodeVariantI32(static_cast<void **>(&pBuf), value);
  void *ptr = taosDecodeVariantI32(static_cast<void *>(buf), &value_check);

  return ((ptr != NULL) && (value == value_check) && (pBuf == ptr) && POINTER_DISTANCE(pBuf, buf) == tlen);
}

static bool test_variant_uint64(uint64_t value) {
  char     buf[20] = "\0";
  uint64_t value_check = 0;

  void *pBuf = (void *)buf;

  int   tlen = taosEncodeVariantU64(static_cast<void **>(&pBuf), value);
  void *ptr = taosDecodeVariantU64(static_cast<void *>(buf), &value_check);

  return ((ptr != NULL) && (value == value_check) && (pBuf == ptr) && POINTER_DISTANCE(pBuf, buf) == tlen);
}

static bool test_variant_int64(int64_t value) {
  char    buf[20] = "\0";
  int64_t value_check = 0;

  void *pBuf = (void *)buf;

  int   tlen = taosEncodeVariantI64(static_cast<void **>(&pBuf), value);
  void *ptr = taosDecodeVariantI64(static_cast<void *>(buf), &value_check);

  return ((ptr != NULL) && (value == value_check) && (pBuf == ptr) && POINTER_DISTANCE(pBuf, buf) == tlen);
}

TEST(codingTest, fixed_encode_decode) {
  taosSeedRand(taosGetTimestampSec());

  // uint16_t
  for (uint16_t value = 0; value <= UINT16_MAX; value++) {
    ASSERT_TRUE(test_fixed_uint16(value));
    if (value == UINT16_MAX) break;
  }

  // int16_t
  for (int16_t value = INT16_MIN; value <= INT16_MAX; value++) {
    ASSERT_TRUE(test_fixed_int16(value));
    if (value == INT16_MAX) break;
  }

  std::mt19937 gen32(std::random_device{}());
  // uint32_t
  ASSERT_TRUE(test_fixed_uint32(0));
  ASSERT_TRUE(test_fixed_uint32(UINT32_MAX));
  std::uniform_int_distribution<uint32_t> distr1(0, UINT32_MAX);

  for (int i = 0; i < 1000000; i++) {
    ASSERT_TRUE(test_fixed_uint32(distr1(gen32)));
  }

  // int32_t
  ASSERT_TRUE(test_fixed_int32(INT32_MIN));
  ASSERT_TRUE(test_fixed_int32(INT32_MAX));
  std::uniform_int_distribution<int32_t> distr2(INT32_MIN, INT32_MAX);

  for (int i = 0; i < 1000000; i++) {
    ASSERT_TRUE(test_fixed_int32(distr2(gen32)));
  }

  std::mt19937_64 gen64(std::random_device{}());
  // uint64_t
  std::uniform_int_distribution<uint64_t> distr3(0, UINT64_MAX);

  ASSERT_TRUE(test_fixed_uint64(0));
  ASSERT_TRUE(test_fixed_uint64(UINT64_MAX));
  for (int i = 0; i < 1000000; i++) {
    ASSERT_TRUE(test_fixed_uint64(distr3(gen64)));
  }

  // int64_t
  std::uniform_int_distribution<int64_t> distr4(INT64_MIN, INT64_MAX);

  ASSERT_TRUE(test_fixed_int64(INT64_MIN));
  ASSERT_TRUE(test_fixed_int64(INT64_MAX));
  for (int i = 0; i < 1000000; i++) {
    ASSERT_TRUE(test_fixed_int64(distr4(gen64)));
  }
}

TEST(codingTest, variant_encode_decode) {
  taosSeedRand(taosGetTimestampSec());

  // uint16_t
  for (uint16_t value = 0; value <= UINT16_MAX; value++) {
    ASSERT_TRUE(test_variant_uint16(value));
    if (value == UINT16_MAX) break;
  }

  // int16_t
  for (int16_t value = INT16_MIN; value <= INT16_MAX; value++) {
    ASSERT_TRUE(test_variant_int16(value));
    if (value == INT16_MAX) break;
  }

  std::mt19937 gen32(std::random_device{}());
  // uint32_t
  std::uniform_int_distribution<uint32_t> distr1(0, UINT32_MAX);
  ASSERT_TRUE(test_variant_uint32(0));
  ASSERT_TRUE(test_variant_uint32(UINT32_MAX));

  for (int i = 0; i < 5000000; i++) {
    ASSERT_TRUE(test_variant_uint32(distr1(gen32)));
  }

  // int32_t
  std::uniform_int_distribution<int32_t> distr2(INT32_MIN, INT32_MAX);
  ASSERT_TRUE(test_variant_int32(INT32_MIN));
  ASSERT_TRUE(test_variant_int32(INT32_MAX));

  for (int i = 0; i < 5000000; i++) {
    ASSERT_TRUE(test_variant_int32(distr2(gen32)));
  }

  std::mt19937_64 gen64(std::random_device{}());
  // uint64_t
  std::uniform_int_distribution<uint64_t> distr3(0, UINT64_MAX);

  ASSERT_TRUE(test_variant_uint64(0));
  ASSERT_TRUE(test_variant_uint64(UINT64_MAX));
  for (int i = 0; i < 5000000; i++) {
    // uint64_t value = gen();
    // printf("%ull\n", value);
    ASSERT_TRUE(test_variant_uint64(distr3(gen64)));
  }

  // int64_t
  std::uniform_int_distribution<int64_t> distr4(INT64_MIN, INT64_MAX);

  ASSERT_TRUE(test_variant_int64(INT64_MIN));
  ASSERT_TRUE(test_variant_int64(INT64_MAX));
  for (int i = 0; i < 5000000; i++) {
    // uint64_t value = gen();
    // printf("%ull\n", value);
    ASSERT_TRUE(test_variant_int64(distr4(gen64)));
  }
}