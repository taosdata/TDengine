#include <iostream>

#include "gtest/gtest.h"

#include "encode.h"

#define BUF_SIZE 64
td_endian_t endian_arr[2] = {TD_LITTLE_ENDIAN, TD_BIG_ENDIAN};

static int encode(SCoder *pCoder, int8_t val) { return tEncodeI8(pCoder, val); }
static int encode(SCoder *pCoder, uint8_t val) { return tEncodeU8(pCoder, val); }
static int encode(SCoder *pCoder, int16_t val) { return tEncodeI16(pCoder, val); }
static int encode(SCoder *pCoder, uint16_t val) { return tEncodeU16(pCoder, val); }
static int encode(SCoder *pCoder, int32_t val) { return tEncodeI32(pCoder, val); }
static int encode(SCoder *pCoder, uint32_t val) { return tEncodeU32(pCoder, val); }
static int encode(SCoder *pCoder, int64_t val) { return tEncodeI64(pCoder, val); }
static int encode(SCoder *pCoder, uint64_t val) { return tEncodeU64(pCoder, val); }

static int decode(SCoder *pCoder, int8_t *val) { return tDecodeI8(pCoder, val); }
static int decode(SCoder *pCoder, uint8_t *val) { return tDecodeU8(pCoder, val); }
static int decode(SCoder *pCoder, int16_t *val) { return tDecodeI16(pCoder, val); }
static int decode(SCoder *pCoder, uint16_t *val) { return tDecodeU16(pCoder, val); }
static int decode(SCoder *pCoder, int32_t *val) { return tDecodeI32(pCoder, val); }
static int decode(SCoder *pCoder, uint32_t *val) { return tDecodeU32(pCoder, val); }
static int decode(SCoder *pCoder, int64_t *val) { return tDecodeI64(pCoder, val); }
static int decode(SCoder *pCoder, uint64_t *val) { return tDecodeU64(pCoder, val); }

static int encodev(SCoder *pCoder, int8_t val) { return tEncodeI8(pCoder, val); }
static int encodev(SCoder *pCoder, uint8_t val) { return tEncodeU8(pCoder, val); }
static int encodev(SCoder *pCoder, int16_t val) { return tEncodeI16v(pCoder, val); }
static int encodev(SCoder *pCoder, uint16_t val) { return tEncodeU16v(pCoder, val); }
static int encodev(SCoder *pCoder, int32_t val) { return tEncodeI32v(pCoder, val); }
static int encodev(SCoder *pCoder, uint32_t val) { return tEncodeU32v(pCoder, val); }
static int encodev(SCoder *pCoder, int64_t val) { return tEncodeI64v(pCoder, val); }
static int encodev(SCoder *pCoder, uint64_t val) { return tEncodeU64v(pCoder, val); }

static int decodev(SCoder *pCoder, int8_t *val) { return tDecodeI8(pCoder, val); }
static int decodev(SCoder *pCoder, uint8_t *val) { return tDecodeU8(pCoder, val); }
static int decodev(SCoder *pCoder, int16_t *val) { return tDecodeI16v(pCoder, val); }
static int decodev(SCoder *pCoder, uint16_t *val) { return tDecodeU16v(pCoder, val); }
static int decodev(SCoder *pCoder, int32_t *val) { return tDecodeI32v(pCoder, val); }
static int decodev(SCoder *pCoder, uint32_t *val) { return tDecodeU32v(pCoder, val); }
static int decodev(SCoder *pCoder, int64_t *val) { return tDecodeI64v(pCoder, val); }
static int decodev(SCoder *pCoder, uint64_t *val) { return tDecodeU64v(pCoder, val); }

template <typename T>
static void simple_encode_decode_func(bool var_len) {
  uint8_t buf[BUF_SIZE];
  SCoder  coder;
  T       min_val, max_val;
  T       step = 1;

  if (typeid(T) == typeid(int8_t)) {
    min_val = INT8_MIN;
    max_val = INT8_MAX;
    step = 1;
  } else if (typeid(T) == typeid(uint8_t)) {
    min_val = 0;
    max_val = UINT8_MAX;
    step = 1;
  } else if (typeid(T) == typeid(int16_t)) {
    min_val = INT16_MIN;
    max_val = INT16_MAX;
    step = 1;
  } else if (typeid(T) == typeid(uint16_t)) {
    min_val = 0;
    max_val = UINT16_MAX;
    step = 1;
  } else if (typeid(T) == typeid(int32_t)) {
    min_val = INT32_MIN;
    max_val = INT32_MAX;
    step = ((T)1) << 16;
  } else if (typeid(T) == typeid(uint32_t)) {
    min_val = 0;
    max_val = UINT32_MAX;
    step = ((T)1) << 16;
  } else if (typeid(T) == typeid(int64_t)) {
    min_val = INT64_MIN;
    max_val = INT64_MAX;
    step = ((T)1) << 48;
  } else if (typeid(T) == typeid(uint64_t)) {
    min_val = 0;
    max_val = UINT64_MAX;
    step = ((T)1) << 48;
  }

  T i = min_val;
  for (;; /*T i = min_val; i <= max_val; i += step*/) {
    T dval;

    // Encode NULL
    for (td_endian_t endian : endian_arr) {
      tCoderInit(&coder, endian, NULL, 0, TD_ENCODER);

      if (var_len) {
        GTEST_ASSERT_EQ(encodev(&coder, i), 0);
      } else {
        GTEST_ASSERT_EQ(encode(&coder, i), 0);
        GTEST_ASSERT_EQ(coder.pos, sizeof(T));
      }

      tCoderClear(&coder);
    }

    // Encode and decode
    for (td_endian_t e_endian : endian_arr) {
      for (td_endian_t d_endian : endian_arr) {
        // Encode
        tCoderInit(&coder, e_endian, buf, BUF_SIZE, TD_ENCODER);

        if (var_len) {
          GTEST_ASSERT_EQ(encodev(&coder, i), 0);
        } else {
          GTEST_ASSERT_EQ(encode(&coder, i), 0);
          GTEST_ASSERT_EQ(coder.pos, sizeof(T));
        }

        int32_t epos = coder.pos;

        tCoderClear(&coder);
        // Decode
        tCoderInit(&coder, d_endian, buf, BUF_SIZE, TD_DECODER);

        if (var_len) {
          GTEST_ASSERT_EQ(decodev(&coder, &dval), 0);
        } else {
          GTEST_ASSERT_EQ(decode(&coder, &dval), 0);
          GTEST_ASSERT_EQ(coder.pos, sizeof(T));
        }

        GTEST_ASSERT_EQ(coder.pos, epos);

        if (typeid(T) == typeid(int8_t) || typeid(T) == typeid(uint8_t) || e_endian == d_endian) {
          GTEST_ASSERT_EQ(i, dval);
        }

        tCoderClear(&coder);
      }
    }

    if (i == max_val) break;

    if (max_val - i < step) {
      i = max_val;
    } else {
      i = i + step;
    }
  }
}

TEST(td_encode_test, encode_decode_fixed_len_integer) {
  simple_encode_decode_func<int8_t>(false);
  simple_encode_decode_func<uint8_t>(false);
  simple_encode_decode_func<int16_t>(false);
  simple_encode_decode_func<uint16_t>(false);
  simple_encode_decode_func<int32_t>(false);
  simple_encode_decode_func<uint32_t>(false);
  simple_encode_decode_func<int64_t>(false);
  simple_encode_decode_func<uint64_t>(false);
}

TEST(td_encode_test, encode_decode_variant_len_integer) {
  simple_encode_decode_func<int16_t>(true);
  simple_encode_decode_func<uint16_t>(true);
  simple_encode_decode_func<int32_t>(true);
  simple_encode_decode_func<uint32_t>(true);
  simple_encode_decode_func<int64_t>(true);
  simple_encode_decode_func<uint64_t>(true);
}

TEST(td_encode_test, encode_decode_cstr) {
  uint8_t *   buf = new uint8_t[1024 * 1024];
  char *      cstr = new char[1024 * 1024];
  const char *dcstr;
  SCoder      encoder;
  SCoder      decoder;

  for (size_t i = 0; i < 1024 * 2 - 1; i++) {
    memset(cstr, 'a', i);
    cstr[i] = '\0';
    for (td_endian_t endian : endian_arr) {
      // Encode
      tCoderInit(&encoder, endian, buf, 1024 * 1024, TD_ENCODER);

      GTEST_ASSERT_EQ(tEncodeCStr(&encoder, cstr), 0);

      tCoderClear(&encoder);

      // Decode
      tCoderInit(&decoder, endian, buf, 1024 * 1024, TD_DECODER);

      GTEST_ASSERT_EQ(tDecodeCStr(&decoder, &dcstr), 0);
      GTEST_ASSERT_EQ(memcmp(dcstr, cstr, i + 1), 0);

      tCoderClear(&decoder);
    }
  }

  delete buf;
  delete cstr;
}