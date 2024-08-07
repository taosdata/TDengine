#include <gtest/gtest.h>

#include "tbuffer.h"

typedef struct {
  int8_t  value1;
  int32_t value2;
  int64_t value3;
} STestStruct;

TEST(BufferTest, simpleTest1) {
  SBuffer buffer;

  tBufferInit(&buffer);

  GTEST_ASSERT_EQ(tBufferGetSize(&buffer), 0);
  GTEST_ASSERT_EQ(tBufferGetData(&buffer), nullptr);

  tBufferDestroy(&buffer);
}

TEST(BufferTest, forwardWriteAndRead) {
  int32_t code = 0;
  bool    forward = true;
  SBuffer buffer;

  tBufferInit(&buffer);
  taosSeedRand(taosGetTimestampSec());

  // write
  /* fix-len struct */
  STestStruct testStruct = {1, 2};
  GTEST_ASSERT_EQ(tBufferPut(&buffer, &testStruct, sizeof(STestStruct)), 0);

  /* int8_t */
  int8_t i8 = taosRand() % UINT8_MAX - INT8_MAX;
  GTEST_ASSERT_EQ(tBufferPutI8(&buffer, i8), 0);

  /* int16_t */
  int8_t i16 = taosRand() % UINT16_MAX - INT16_MAX;
  GTEST_ASSERT_EQ(tBufferPutI16(&buffer, i16), 0);

  /* int32_t */
  int8_t i32 = taosRand();
  GTEST_ASSERT_EQ(tBufferPutI32(&buffer, i32), 0);

  /* int64_t */
  int64_t i64 = taosRand();
  GTEST_ASSERT_EQ(tBufferPutI64(&buffer, i64), 0);

  /* uint8_t */
  uint8_t u8 = taosRand() % UINT8_MAX;
  GTEST_ASSERT_EQ(tBufferPutU8(&buffer, u8), 0);

  /* uint16_t */
  uint16_t u16 = taosRand() % UINT16_MAX;
  GTEST_ASSERT_EQ(tBufferPutU16(&buffer, u16), 0);

  /* uint32_t */
  uint32_t u32 = taosRand();
  GTEST_ASSERT_EQ(tBufferPutU32(&buffer, u32), 0);

  /* uint64_t */
  uint64_t u64 = taosRand();
  GTEST_ASSERT_EQ(tBufferPutU64(&buffer, u64), 0);

  /* float */
  float f = (float)taosRand() / (float)taosRand();
  GTEST_ASSERT_EQ(tBufferPutF32(&buffer, f), 0);

  /* double */
  double d = (double)taosRand() / (double)taosRand();
  GTEST_ASSERT_EQ(tBufferPutF64(&buffer, d), 0);

  /* binary */
  uint8_t binary[10];
  for (int32_t i = 0; i < sizeof(binary); ++i) {
    binary[i] = taosRand() % UINT8_MAX;
  }
  GTEST_ASSERT_EQ(tBufferPutBinary(&buffer, binary, sizeof(binary)), 0);

  /* cstr */
  const char *cstr = "hello world";
  GTEST_ASSERT_EQ(tBufferPutCStr(&buffer, cstr), 0);

  /* uint16v_t */
  uint16_t u16v[] = {0, 127, 128, 129, 16384, 16385, 16386, UINT16_MAX};
  for (int32_t i = 0; i < sizeof(u16v) / sizeof(u16v[0]); ++i) {
    GTEST_ASSERT_EQ(tBufferPutU16v(&buffer, u16v[i]), 0);
  }

  /* uint32v_t */
  uint32_t u32v[] = {0,         127,           128,           129,       16384,         16385,     16386, (1 << 21) - 1,
                     (1 << 21), (1 << 21) + 1, (1 << 28) - 1, (1 << 28), (1 << 28) + 1, UINT32_MAX};
  for (int32_t i = 0; i < sizeof(u32v) / sizeof(u32v[0]); ++i) {
    GTEST_ASSERT_EQ(tBufferPutU32v(&buffer, u32v[i]), 0);
  }

  /* uint64v_t */
  uint64_t u64v[] = {0,  // 0
                     (1ul << (7 * 1)) - 1,
                     (1ul << (7 * 1)),
                     (1ul << (7 * 1)) + 1,
                     (1ul << (7 * 2)) - 1,
                     (1ul << (7 * 2)),
                     (1ul << (7 * 2)) + 1,
                     (1ul << (7 * 3)) - 1,
                     (1ul << (7 * 3)),
                     (1ul << (7 * 3)) + 1,
                     (1ul << (7 * 4)) - 1,
                     (1ul << (7 * 4)),
                     (1ul << (7 * 4)) + 1,
                     (1ul << (7 * 5)) - 1,
                     (1ul << (7 * 5)),
                     (1ul << (7 * 5)) + 1,
                     (1ul << (7 * 6)) - 1,
                     (1ul << (7 * 6)),
                     (1ul << (7 * 6)) + 1,
                     (1ul << (7 * 7)) - 1,
                     (1ul << (7 * 7)),
                     (1ul << (7 * 7)) + 1,
                     (1ul << (7 * 8)) - 1,
                     (1ul << (7 * 8)),
                     (1ul << (7 * 8)) + 1,
                     (1ul << (7 * 9)) - 1,
                     (1ul << (7 * 9)),
                     (1ul << (7 * 9)) + 1,
                     UINT64_MAX};
  for (int32_t i = 0; i < sizeof(u64v) / sizeof(u64v[0]); ++i) {
    GTEST_ASSERT_EQ(tBufferPutU64v(&buffer, u64v[i]), 0);
  }

  /* int16v_t */
  int16_t i16v[] = {
      INT16_MIN,  //
      -((1 << (7 * 1)) - 1),
      -((1 << (7 * 1))),
      -((1 << (7 * 1)) + 1),
      -((1 << (7 * 2)) - 1),
      -((1 << (7 * 2))),
      -((1 << (7 * 2)) + 1),
      (1 << (7 * 0)) - 1,
      (1 << (7 * 0)),
      (1 << (7 * 0)) + 1,
      (1 << (7 * 1)) - 1,
      (1 << (7 * 1)),
      (1 << (7 * 1)) + 1,
      (1 << (7 * 2)) - 1,
      (1 << (7 * 2)),
      (1 << (7 * 2)) + 1,
      INT16_MAX,
  };
  for (int32_t i = 0; i < sizeof(i16v) / sizeof(i16v[0]); ++i) {
    GTEST_ASSERT_EQ(tBufferPutI16v(&buffer, i16v[i]), 0);
  }

  /* int32v_t */
  int32_t i32v[] = {
      INT32_MIN,  //
      -((1 << (7 * 1)) - 1),
      -((1 << (7 * 1))),
      -((1 << (7 * 1)) + 1),
      -((1 << (7 * 2)) - 1),
      -((1 << (7 * 2))),
      -((1 << (7 * 2)) + 1),
      -((1 << (7 * 3)) - 1),
      -((1 << (7 * 3))),
      -((1 << (7 * 3)) + 1),
      -((1 << (7 * 4)) - 1),
      -((1 << (7 * 4))),
      -((1 << (7 * 4)) + 1),
      (1 << (7 * 0)) - 1,
      (1 << (7 * 0)),
      (1 << (7 * 0)) + 1,
      (1 << (7 * 1)) - 1,
      (1 << (7 * 1)),
      (1 << (7 * 1)) + 1,
      (1 << (7 * 2)) - 1,
      (1 << (7 * 2)),
      (1 << (7 * 2)) + 1,
      (1 << (7 * 3)) - 1,
      (1 << (7 * 3)),
      (1 << (7 * 3)) + 1,
      (1 << (7 * 4)) - 1,
      (1 << (7 * 4)),
      (1 << (7 * 4)) + 1,
      INT32_MAX,
  };
  for (int32_t i = 0; i < sizeof(i32v) / sizeof(i32v[0]); ++i) {
    GTEST_ASSERT_EQ(tBufferPutI32v(&buffer, i32v[i]), 0);
  }

  /* int64v_t */
  int64_t i64v[] = {
      INT64_MIN,  //
      -((1l << (7 * 1)) - 1),
      -((1l << (7 * 1))),
      -((1l << (7 * 1)) + 1),
      -((1l << (7 * 2)) - 1),
      -((1l << (7 * 2))),
      -((1l << (7 * 2)) + 1),
      -((1l << (7 * 3)) - 1),
      -((1l << (7 * 3))),
      -((1l << (7 * 3)) + 1),
      -((1l << (7 * 4)) - 1),
      -((1l << (7 * 4))),
      -((1l << (7 * 4)) + 1),
      -((1l << (7 * 5)) - 1),
      -((1l << (7 * 5))),
      -((1l << (7 * 5)) + 1),
      -((1l << (7 * 6)) - 1),
      -((1l << (7 * 6))),
      -((1l << (7 * 6)) + 1),
      -((1l << (7 * 7)) - 1),
      -((1l << (7 * 7))),
      -((1l << (7 * 7)) + 1),
      -((1l << (7 * 8)) - 1),
      -((1l << (7 * 8))),
      -((1l << (7 * 8)) + 1),
      -((1l << (7 * 9)) + 1),
      ((1l << (7 * 1)) - 1),
      ((1l << (7 * 1))),
      ((1l << (7 * 1)) + 1),
      ((1l << (7 * 2)) - 1),
      ((1l << (7 * 2))),
      ((1l << (7 * 2)) + 1),
      ((1l << (7 * 3)) - 1),
      ((1l << (7 * 3))),
      ((1l << (7 * 3)) + 1),
      ((1l << (7 * 4)) - 1),
      ((1l << (7 * 4))),
      ((1l << (7 * 4)) + 1),
      ((1l << (7 * 5)) - 1),
      ((1l << (7 * 5))),
      ((1l << (7 * 5)) + 1),
      ((1l << (7 * 6)) - 1),
      ((1l << (7 * 6))),
      ((1l << (7 * 6)) + 1),
      ((1l << (7 * 7)) - 1),
      ((1l << (7 * 7))),
      ((1l << (7 * 7)) + 1),
      ((1l << (7 * 8)) - 1),
      ((1l << (7 * 8))),
      ((1l << (7 * 8)) + 1),
      ((1l << (7 * 9)) + 1),
      INT64_MAX,
  };
  for (int32_t i = 0; i < sizeof(i64v) / sizeof(i64v[0]); ++i) {
    GTEST_ASSERT_EQ(tBufferPutI64v(&buffer, i64v[i]), 0);
  }

  // read
  SBufferReader reader;
  tBufferReaderInit(&reader, 0, &buffer);

  /* fix-len struct */
  STestStruct testStruct2 = {1, 2};
  GTEST_ASSERT_EQ(tBufferGet(&reader, sizeof(STestStruct), &testStruct2), 0);
  GTEST_ASSERT_EQ(testStruct.value1, testStruct2.value1);
  GTEST_ASSERT_EQ(testStruct.value2, testStruct2.value2);
  GTEST_ASSERT_EQ(testStruct.value3, testStruct2.value3);

  /* int8_t */
  int8_t i8_2 = 97;
  GTEST_ASSERT_EQ(tBufferGetI8(&reader, &i8_2), 0);
  GTEST_ASSERT_EQ(i8, i8_2);

  /* int16_t */
  int16_t i16_2;
  GTEST_ASSERT_EQ(tBufferGetI16(&reader, &i16_2), 0);
  GTEST_ASSERT_EQ(i16, i16_2);

  /* int32_t */
  int32_t i32_2;
  GTEST_ASSERT_EQ(tBufferGetI32(&reader, &i32_2), 0);
  GTEST_ASSERT_EQ(i32, i32_2);

  /* int64_t */
  int64_t i64_2;
  GTEST_ASSERT_EQ(tBufferGetI64(&reader, &i64_2), 0);
  GTEST_ASSERT_EQ(i64, i64_2);

  /* uint8_t */
  uint8_t u8_2;
  GTEST_ASSERT_EQ(tBufferGetU8(&reader, &u8_2), 0);
  GTEST_ASSERT_EQ(u8, u8_2);

  /* uint16_t */
  uint16_t u16_2;
  GTEST_ASSERT_EQ(tBufferGetU16(&reader, &u16_2), 0);
  GTEST_ASSERT_EQ(u16, u16_2);

  /* uint32_t */
  uint32_t u32_2;
  GTEST_ASSERT_EQ(tBufferGetU32(&reader, &u32_2), 0);
  GTEST_ASSERT_EQ(u32, u32_2);

  /* uint64_t */
  uint64_t u64_2;
  GTEST_ASSERT_EQ(tBufferGetU64(&reader, &u64_2), 0);
  GTEST_ASSERT_EQ(u64, u64_2);

  /* float */
  float f_2;
  GTEST_ASSERT_EQ(tBufferGetF32(&reader, &f_2), 0);
  GTEST_ASSERT_EQ(f, f_2);

  /* double */
  double d_2;
  GTEST_ASSERT_EQ(tBufferGetF64(&reader, &d_2), 0);
  GTEST_ASSERT_EQ(d, d_2);

  /* binary */
  const void *binary2;
  uint32_t    binarySize;
  GTEST_ASSERT_EQ(tBufferGetBinary(&reader, &binary2, &binarySize), 0);
  GTEST_ASSERT_EQ(memcmp(binary, binary2, sizeof(binary)), 0);
  GTEST_ASSERT_EQ(binarySize, sizeof(binary));

  /* cstr */
  const char *cstr2;
  GTEST_ASSERT_EQ(tBufferGetCStr(&reader, &cstr2), 0);
  GTEST_ASSERT_EQ(strcmp(cstr, cstr2), 0);

  /* uint16v_t */
  uint16_t u16v2[sizeof(u16v) / sizeof(u16v[0])];
  for (int32_t i = 0; i < sizeof(u16v) / sizeof(u16v[0]); ++i) {
    GTEST_ASSERT_EQ(tBufferGetU16v(&reader, &u16v2[i]), 0);
    GTEST_ASSERT_EQ(u16v[i], u16v2[i]);
  }

  /* uint32v_t */
  uint32_t u32v2[sizeof(u32v) / sizeof(u32v[0])];
  for (int32_t i = 0; i < sizeof(u32v) / sizeof(u32v[0]); ++i) {
    GTEST_ASSERT_EQ(tBufferGetU32v(&reader, &u32v2[i]), 0);
    GTEST_ASSERT_EQ(u32v[i], u32v2[i]);
  }

  /* uint64v_t */
  uint64_t u64v2[sizeof(u64v) / sizeof(u64v[0])];
  for (int32_t i = 0; i < sizeof(u64v) / sizeof(u64v[0]); ++i) {
    GTEST_ASSERT_EQ(tBufferGetU64v(&reader, &u64v2[i]), 0);
    GTEST_ASSERT_EQ(u64v[i], u64v2[i]);
  }

  /* int16v_t */
  int16_t i16v2[sizeof(i16v) / sizeof(i16v[0])];
  for (int32_t i = 0; i < sizeof(i16v) / sizeof(i16v[0]); ++i) {
    GTEST_ASSERT_EQ(tBufferGetI16v(&reader, &i16v2[i]), 0);
    GTEST_ASSERT_EQ(i16v[i], i16v2[i]);
  }

  /* int32v_t */
  int32_t i32v2[sizeof(i32v) / sizeof(i32v[0])];
  for (int32_t i = 0; i < sizeof(i32v) / sizeof(i32v[0]); ++i) {
    GTEST_ASSERT_EQ(tBufferGetI32v(&reader, &i32v2[i]), 0);
    GTEST_ASSERT_EQ(i32v[i], i32v2[i]);
  }

  /* int64v_t */
  int64_t i64v2[sizeof(i64v) / sizeof(i64v[0])];
  for (int32_t i = 0; i < sizeof(i64v) / sizeof(i64v[0]); ++i) {
    GTEST_ASSERT_EQ(tBufferGetI64v(&reader, &i64v2[i]), 0);
    GTEST_ASSERT_EQ(i64v[i], i64v2[i]);
  }

  tBufferReaderDestroy(&reader);

  // clear
  tBufferDestroy(&buffer);
}
