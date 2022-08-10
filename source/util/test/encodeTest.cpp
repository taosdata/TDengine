#if 0

#include <iostream>

#include <gtest/gtest.h>

#include "tencode.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wshift-count-overflow"
#pragma GCC diagnostic ignored "-Woverflow"
#pragma GCC diagnostic ignored "-Woverflow"

#define BUF_SIZE 64
td_endian_t endian_arr[2] = {TD_LITTLE_ENDIAN, TD_BIG_ENDIAN};

static int32_t encode(SEncoder *pCoder, int8_t val) { return tEncodeI8(pCoder, val); }
static int32_t encode(SEncoder *pCoder, uint8_t val) { return tEncodeU8(pCoder, val); }
static int32_t encode(SEncoder *pCoder, int16_t val) { return tEncodeI16(pCoder, val); }
static int32_t encode(SEncoder *pCoder, uint16_t val) { return tEncodeU16(pCoder, val); }
static int32_t encode(SEncoder *pCoder, int32_t val) { return tEncodeI32(pCoder, val); }
static int32_t encode(SEncoder *pCoder, uint32_t val) { return tEncodeU32(pCoder, val); }
static int32_t encode(SEncoder *pCoder, int64_t val) { return tEncodeI64(pCoder, val); }
static int32_t encode(SEncoder *pCoder, uint64_t val) { return tEncodeU64(pCoder, val); }

static int32_t decode(SDecoder *pCoder, int8_t *val) { return tDecodeI8(pCoder, val); }
static int32_t decode(SDecoder *pCoder, uint8_t *val) { return tDecodeU8(pCoder, val); }
static int32_t decode(SDecoder *pCoder, int16_t *val) { return tDecodeI16(pCoder, val); }
static int32_t decode(SDecoder *pCoder, uint16_t *val) { return tDecodeU16(pCoder, val); }
static int32_t decode(SDecoder *pCoder, int32_t *val) { return tDecodeI32(pCoder, val); }
static int32_t decode(SDecoder *pCoder, uint32_t *val) { return tDecodeU32(pCoder, val); }
static int32_t decode(SDecoder *pCoder, int64_t *val) { return tDecodeI64(pCoder, val); }
static int32_t decode(SDecoder *pCoder, uint64_t *val) { return tDecodeU64(pCoder, val); }

static int32_t encodev(SEncoder *pCoder, int8_t val) { return tEncodeI8(pCoder, val); }
static int32_t encodev(SEncoder *pCoder, uint8_t val) { return tEncodeU8(pCoder, val); }
static int32_t encodev(SEncoder *pCoder, int16_t val) { return tEncodeI16v(pCoder, val); }
static int32_t encodev(SEncoder *pCoder, uint16_t val) { return tEncodeU16v(pCoder, val); }
static int32_t encodev(SEncoder *pCoder, int32_t val) { return tEncodeI32v(pCoder, val); }
static int32_t encodev(SEncoder *pCoder, uint32_t val) { return tEncodeU32v(pCoder, val); }
static int32_t encodev(SEncoder *pCoder, int64_t val) { return tEncodeI64v(pCoder, val); }
static int32_t encodev(SEncoder *pCoder, uint64_t val) { return tEncodeU64v(pCoder, val); }

static int32_t decodev(SDecoder *pCoder, int8_t *val) { return tDecodeI8(pCoder, val); }
static int32_t decodev(SDecoder *pCoder, uint8_t *val) { return tDecodeU8(pCoder, val); }
static int32_t decodev(SDecoder *pCoder, int16_t *val) { return tDecodeI16v(pCoder, val); }
static int32_t decodev(SDecoder *pCoder, uint16_t *val) { return tDecodeU16v(pCoder, val); }
static int32_t decodev(SDecoder *pCoder, int32_t *val) { return tDecodeI32v(pCoder, val); }
static int32_t decodev(SDecoder *pCoder, uint32_t *val) { return tDecodeU32v(pCoder, val); }
static int32_t decodev(SDecoder *pCoder, int64_t *val) { return tDecodeI64v(pCoder, val); }
static int32_t decodev(SDecoder *pCoder, uint64_t *val) { return tDecodeU64v(pCoder, val); }

template <typename T>
static void simple_encode_decode_func(bool var_len) {
  uint8_t  buf[BUF_SIZE];
  SEncoder encoder = {0};
  SDecoder decoder = {0};
  T        min_val, max_val;
  T        step = 1;

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
      tEncoderInit(&encoder, endian, NULL, 0, TD_ENCODER);

      if (var_len) {
        GTEST_ASSERT_EQ(encodev(&encoder, i), 0);
      } else {
        GTEST_ASSERT_EQ(encode(&encoder, i), 0);
        GTEST_ASSERT_EQ(encoder.pos, sizeof(T));
      }

      tCoderClear(&encoder);
    }

    // Encode and decode
    for (td_endian_t e_endian : endian_arr) {
      for (td_endian_t d_endian : endian_arr) {
        // Encode
        tCoderInit(&encoder, e_endian, buf, BUF_SIZE, TD_ENCODER);

        if (var_len) {
          GTEST_ASSERT_EQ(encodev(&encoder, i), 0);
        } else {
          GTEST_ASSERT_EQ(encode(&encoder, i), 0);
          GTEST_ASSERT_EQ(encoder.pos, sizeof(T));
        }

        int32_t epos = encoder.pos;

        tCoderClear(&encoder);
        // Decode
        tCoderInit(&encoder, d_endian, buf, BUF_SIZE, TD_DECODER);

        if (var_len) {
          GTEST_ASSERT_EQ(decodev(&encoder, &dval), 0);
        } else {
          GTEST_ASSERT_EQ(decode(&encoder, &dval), 0);
          GTEST_ASSERT_EQ(encoder.pos, sizeof(T));
        }

        GTEST_ASSERT_EQ(encoder.pos, epos);

        if (typeid(T) == typeid(int8_t) || typeid(T) == typeid(uint8_t) || e_endian == d_endian) {
          GTEST_ASSERT_EQ(i, dval);
        }

        tCoderClear(&encoder);
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
  uint8_t    *buf = new uint8_t[1024 * 1024];
  char       *cstr = new char[1024 * 1024];
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

  delete[] buf;
  delete[] cstr;
}

typedef struct {
  int32_t A_a;
  int64_t A_b;
  char   *A_c;
} SStructA_v1;

static int32_t tSStructA_v1_encode(SCoder *pCoder, const SStructA_v1 *pSAV1) {
  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeI32(pCoder, pSAV1->A_a) < 0) return -1;
  if (tEncodeI64(pCoder, pSAV1->A_b) < 0) return -1;
  if (tEncodeCStr(pCoder, pSAV1->A_c) < 0) return -1;

  tEndEncode(pCoder);
  return 0;
}

static int32_t tSStructA_v1_decode(SCoder *pCoder, SStructA_v1 *pSAV1) {
  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeI32(pCoder, &pSAV1->A_a) < 0) return -1;
  if (tDecodeI64(pCoder, &pSAV1->A_b) < 0) return -1;
  const char *tstr;
  uint64_t    len;
  if (tDecodeCStrAndLen(pCoder, &tstr, &len) < 0) return -1;
  pSAV1->A_c = (char *)tCoderMalloc(pCoder, len + 1);
  memcpy(pSAV1->A_c, tstr, len + 1);

  tEndDecode(pCoder);
  return 0;
}

typedef struct {
  int32_t A_a;
  int64_t A_b;
  char   *A_c;
  // -------------------BELOW FEILDS ARE ADDED IN A NEW VERSION--------------
  int16_t A_d;
  int16_t A_e;
} SStructA_v2;

static int32_t tSStructA_v2_encode(SCoder *pCoder, const SStructA_v2 *pSAV2) {
  if (tStartEncode(pCoder) < 0) return -1;

  if (tEncodeI32(pCoder, pSAV2->A_a) < 0) return -1;
  if (tEncodeI64(pCoder, pSAV2->A_b) < 0) return -1;
  if (tEncodeCStr(pCoder, pSAV2->A_c) < 0) return -1;

  // ------------------------NEW FIELDS ENCODE-------------------------------
  if (tEncodeI16(pCoder, pSAV2->A_d) < 0) return -1;
  if (tEncodeI16(pCoder, pSAV2->A_e) < 0) return -1;

  tEndEncode(pCoder);
  return 0;
}

static int32_t tSStructA_v2_decode(SCoder *pCoder, SStructA_v2 *pSAV2) {
  if (tStartDecode(pCoder) < 0) return -1;

  if (tDecodeI32(pCoder, &pSAV2->A_a) < 0) return -1;
  if (tDecodeI64(pCoder, &pSAV2->A_b) < 0) return -1;
  const char *tstr;
  uint64_t    len;
  if (tDecodeCStrAndLen(pCoder, &tstr, &len) < 0) return -1;
  pSAV2->A_c = (char *)tCoderMalloc(pCoder, len + 1);
  memcpy(pSAV2->A_c, tstr, len + 1);

  // ------------------------NEW FIELDS DECODE-------------------------------
  if (!tDecodeIsEnd(pCoder)) {
    if (tDecodeI16(pCoder, &pSAV2->A_d) < 0) return -1;
    if (tDecodeI16(pCoder, &pSAV2->A_e) < 0) return -1;
  } else {
    pSAV2->A_d = 0;
    pSAV2->A_e = 0;
  }

  tEndDecode(pCoder);
  return 0;
}

typedef struct {
  SStructA_v1 *pA;
  int32_t      v_a;
  int8_t       v_b;
} SFinalReq_v1;

static int32_t tSFinalReq_v1_encode(SCoder *pCoder, const SFinalReq_v1 *ps1) {
  if (tStartEncode(pCoder) < 0) return -1;

  if (tSStructA_v1_encode(pCoder, ps1->pA) < 0) return -1;
  if (tEncodeI32(pCoder, ps1->v_a) < 0) return -1;
  if (tEncodeI8(pCoder, ps1->v_b) < 0) return -1;

  tEndEncode(pCoder);
  return 0;
}

static int32_t tSFinalReq_v1_decode(SCoder *pCoder, SFinalReq_v1 *ps1) {
  if (tStartDecode(pCoder) < 0) return -1;

  ps1->pA = (SStructA_v1 *)tCoderMalloc(pCoder, sizeof(*(ps1->pA)));
  if (tSStructA_v1_decode(pCoder, ps1->pA) < 0) return -1;
  if (tDecodeI32(pCoder, &ps1->v_a) < 0) return -1;
  if (tDecodeI8(pCoder, &ps1->v_b) < 0) return -1;

  tEndDecode(pCoder);
  return 0;
}

typedef struct {
  SStructA_v2 *pA;
  int32_t      v_a;
  int8_t       v_b;
  // ----------------------- Feilds added -----------------------
  int16_t v_c;
} SFinalReq_v2;

static int32_t tSFinalReq_v2_encode(SCoder *pCoder, const SFinalReq_v2 *ps2) {
  if (tStartEncode(pCoder) < 0) return -1;

  if (tSStructA_v2_encode(pCoder, ps2->pA) < 0) return -1;
  if (tEncodeI32(pCoder, ps2->v_a) < 0) return -1;
  if (tEncodeI8(pCoder, ps2->v_b) < 0) return -1;

  // ----------------------- Feilds added encode -----------------------
  if (tEncodeI16(pCoder, ps2->v_c) < 0) return -1;

  tEndEncode(pCoder);
  return 0;
}

static int32_t tSFinalReq_v2_decode(SCoder *pCoder, SFinalReq_v2 *ps2) {
  if (tStartDecode(pCoder) < 0) return -1;

  ps2->pA = (SStructA_v2 *)tCoderMalloc(pCoder, sizeof(*(ps2->pA)));
  if (tSStructA_v2_decode(pCoder, ps2->pA) < 0) return -1;
  if (tDecodeI32(pCoder, &ps2->v_a) < 0) return -1;
  if (tDecodeI8(pCoder, &ps2->v_b) < 0) return -1;

  // ----------------------- Feilds added decode -----------------------
  if (tDecodeIsEnd(pCoder)) {
    ps2->v_c = 0;
  } else {
    if (tDecodeI16(pCoder, &ps2->v_c) < 0) return -1;
  }

  tEndDecode(pCoder);
  return 0;
}
#if 0
TEST(td_encode_test, compound_struct_encode_test) {
  SCoder       encoder, decoder;
  uint8_t *    buf1;
  int32_t      buf1size;
  uint8_t     *buf2;
  int32_t      buf2size;
  SStructA_v1  sa1 = {.A_a = 10, .A_b = 65478, .A_c = (char *)"Hello"};
  SStructA_v2  sa2 = {.A_a = 10, .A_b = 65478, .A_c = (char *)"Hello", .A_d = 67, .A_e = 13};
  SFinalReq_v1 req1 = {.pA = &sa1, .v_a = 15, .v_b = 35};
  SFinalReq_v2 req2 = {.pA = &sa2, .v_a = 15, .v_b = 32, .v_c = 37};
  SFinalReq_v1 dreq11, dreq21;
  SFinalReq_v2 dreq12, dreq22;

  // Get size
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, nullptr, 0, TD_ENCODER);
  GTEST_ASSERT_EQ(tSFinalReq_v1_encode(&encoder, &req1), 0);
  buf1size = encoder.pos;
  buf1 = new uint8_t[encoder.pos];
  tCoderClear(&encoder);

  tCoderInit(&encoder, TD_LITTLE_ENDIAN, nullptr, 0, TD_ENCODER);
  GTEST_ASSERT_EQ(tSFinalReq_v2_encode(&encoder, &req2), 0);
  buf2size = encoder.pos;
  buf2 = new uint8_t[encoder.pos];
  tCoderClear(&encoder);

  // Encode
  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf1, buf1size, TD_ENCODER);
  GTEST_ASSERT_EQ(tSFinalReq_v1_encode(&encoder, &req1), 0);
  tCoderClear(&encoder);

  tCoderInit(&encoder, TD_LITTLE_ENDIAN, buf2, buf2size, TD_ENCODER);
  GTEST_ASSERT_EQ(tSFinalReq_v2_encode(&encoder, &req2), 0);
  tCoderClear(&encoder);

  // Decode
  // buf1 -> req1
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf1, buf1size, TD_DECODER);
  GTEST_ASSERT_EQ(tSFinalReq_v1_decode(&decoder, &dreq11), 0);
  GTEST_ASSERT_EQ(dreq11.pA->A_a, req1.pA->A_a);
  GTEST_ASSERT_EQ(dreq11.pA->A_b, req1.pA->A_b);
  GTEST_ASSERT_EQ(strcmp(dreq11.pA->A_c, req1.pA->A_c), 0);
  GTEST_ASSERT_EQ(dreq11.v_a, req1.v_a);
  GTEST_ASSERT_EQ(dreq11.v_b, req1.v_b);
  tCoderClear(&decoder);

  // buf1 -> req2 (backward compatibility)
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf1, buf1size, TD_DECODER);
  GTEST_ASSERT_EQ(tSFinalReq_v2_decode(&decoder, &dreq12), 0);
  GTEST_ASSERT_EQ(dreq12.pA->A_a, req1.pA->A_a);
  GTEST_ASSERT_EQ(dreq12.pA->A_b, req1.pA->A_b);
  GTEST_ASSERT_EQ(strcmp(dreq12.pA->A_c, req1.pA->A_c), 0);
  GTEST_ASSERT_EQ(dreq12.pA->A_d, 0);
  GTEST_ASSERT_EQ(dreq12.pA->A_e, 0);
  GTEST_ASSERT_EQ(dreq12.v_a, req1.v_a);
  GTEST_ASSERT_EQ(dreq12.v_b, req1.v_b);
  GTEST_ASSERT_EQ(dreq12.v_c, 0);
  tCoderClear(&decoder);

  // buf2 -> req2
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf2, buf2size, TD_DECODER);
  GTEST_ASSERT_EQ(tSFinalReq_v2_decode(&decoder, &dreq22), 0);
  GTEST_ASSERT_EQ(dreq22.pA->A_a, req2.pA->A_a);
  GTEST_ASSERT_EQ(dreq22.pA->A_b, req2.pA->A_b);
  GTEST_ASSERT_EQ(strcmp(dreq22.pA->A_c, req2.pA->A_c), 0);
  GTEST_ASSERT_EQ(dreq22.pA->A_d, req2.pA->A_d);
  GTEST_ASSERT_EQ(dreq22.pA->A_e, req2.pA->A_e);
  GTEST_ASSERT_EQ(dreq22.v_a, req2.v_a);
  GTEST_ASSERT_EQ(dreq22.v_b, req2.v_b);
  GTEST_ASSERT_EQ(dreq22.v_c, req2.v_c);
  tCoderClear(&decoder);

  tCoderInit(&decoder, TD_LITTLE_ENDIAN, buf2, buf2size, TD_DECODER);
  GTEST_ASSERT_EQ(tSFinalReq_v1_decode(&decoder, &dreq21), 0);
  GTEST_ASSERT_EQ(dreq21.pA->A_a, req2.pA->A_a);
  GTEST_ASSERT_EQ(dreq21.pA->A_b, req2.pA->A_b);
  GTEST_ASSERT_EQ(strcmp(dreq21.pA->A_c, req2.pA->A_c), 0);
  GTEST_ASSERT_EQ(dreq21.v_a, req2.v_a);
  GTEST_ASSERT_EQ(dreq21.v_b, req2.v_b);
  tCoderClear(&decoder);
}
#endif

#pragma GCC diagnostic pop

#endif
