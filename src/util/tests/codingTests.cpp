#include <gtest/gtest.h>
#include <stdlib.h>
#include <time.h>
#include <random>

#include "tcoding.h"

static bool test_fixed_uint16(uint16_t value) {
  char     buf[20] = "\0";
  uint16_t value_check = 0;

  void *ptr1 = taosEncodeFixed16(static_cast<void *>(buf), value);
  void *ptr2 = taosDecodeFixed16(static_cast<void *>(buf), &value_check);

  return ((ptr2 != NULL) && (value == value_check) && (ptr1 == ptr2));
}

static bool test_fixed_uint32(uint32_t value) {
  char     buf[20] = "\0";
  uint32_t value_check = 0;

  void *ptr1 = taosEncodeFixed32(static_cast<void *>(buf), value);
  void *ptr2 = taosDecodeFixed32(static_cast<void *>(buf), &value_check);

  return ((ptr2 != NULL) && (value == value_check) && (ptr1 == ptr2));
}

static bool test_fixed_uint64(uint64_t value) {
  char     buf[20] = "\0";
  uint64_t value_check = 0;

  void *ptr1 = taosEncodeFixed64(static_cast<void *>(buf), value);
  void *ptr2 = taosDecodeFixed64(static_cast<void *>(buf), &value_check);

  return ((ptr2 != NULL) && (value == value_check) && (ptr1 == ptr2));
}

static bool test_variant_uint16(uint16_t value) {
  char     buf[20] = "\0";
  uint16_t value_check = 0;

  void *ptr1 = taosEncodeVariant16(static_cast<void *>(buf), value);
  void *ptr2 = taosDecodeVariant16(static_cast<void *>(buf), &value_check);

  return ((ptr2 != NULL) && (value == value_check) && (ptr1 == ptr2));
}

static bool test_variant_uint32(uint32_t value) {
  char     buf[20] = "\0";
  uint32_t value_check = 0;

  void *ptr1 = taosEncodeVariant32(static_cast<void *>(buf), value);
  void *ptr2 = taosDecodeVariant32(static_cast<void *>(buf), &value_check);

  return ((ptr2 != NULL) && (value == value_check) && (ptr1 == ptr2));
}

static bool test_variant_uint64(uint64_t value) {
  char     buf[20] = "\0";
  uint64_t value_check = 0;

  void *ptr1 = taosEncodeVariant64(static_cast<void *>(buf), value);
  void *ptr2 = taosDecodeVariant64(static_cast<void *>(buf), &value_check);

  return ((ptr2 != NULL) && (value == value_check) && (ptr1 == ptr2));
}

TEST(codingTest, fixed_encode_decode) {
  srand(time(0));

  for (uint16_t value = 0; value <= UINT16_MAX; value++) {
    ASSERT_TRUE(test_fixed_uint16(value));
    if (value == UINT16_MAX) break;
  }

  ASSERT_TRUE(test_fixed_uint32(0));
  ASSERT_TRUE(test_fixed_uint32(UINT32_MAX));

  for (int i = 0; i < 1000000; i++) {
    ASSERT_TRUE(test_fixed_uint32(rand()));
  }

  std::mt19937_64 gen (std::random_device{}());

  ASSERT_TRUE(test_fixed_uint64(0));
  ASSERT_TRUE(test_fixed_uint64(UINT64_MAX));
  for (int i = 0; i < 1000000; i++) {
    ASSERT_TRUE(test_fixed_uint64(gen()));
  }
}

TEST(codingTest, variant_encode_decode) {
  srand(time(0));

  for (uint16_t value = 0; value <= UINT16_MAX; value++) {
    ASSERT_TRUE(test_variant_uint16(value));
    if (value == UINT16_MAX) break;
  }

  ASSERT_TRUE(test_variant_uint32(0));
  ASSERT_TRUE(test_variant_uint32(UINT32_MAX));

  for (int i = 0; i < 5000000; i++) {
    ASSERT_TRUE(test_variant_uint32(rand()));
  }

  std::mt19937_64 gen (std::random_device{}());

  ASSERT_TRUE(test_variant_uint64(0));
  ASSERT_TRUE(test_variant_uint64(UINT64_MAX));
  for (int i = 0; i < 5000000; i++) {
    uint64_t value = gen();
    // printf("%ull\n", value);
    ASSERT_TRUE(test_variant_uint64(value));
  }
}