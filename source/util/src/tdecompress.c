/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "os.h"
#include "ttypes.h"
#include "tcompression.h"

int32_t getWordLength(char type) {
  int32_t wordLength = 0;
  switch (type) {
    case TSDB_DATA_TYPE_BIGINT:
      wordLength = LONG_BYTES;
      break;
    case TSDB_DATA_TYPE_INT:
      wordLength = INT_BYTES;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      wordLength = SHORT_BYTES;
      break;
    case TSDB_DATA_TYPE_TINYINT:
      wordLength = CHAR_BYTES;
      break;
    default:
      uError("Invalid decompress integer type:%d", type);
      return -1;
  }

  return wordLength;
}

int32_t tsDecompressIntImpl_Hw(const char *const input, const int32_t nelements, char *const output, const char type) {
  int32_t word_length = getWordLength(type);

  // Selector value:           0  1   2   3   4   5   6   7   8  9  10  11 12  13  14  15
  char    bit_per_integer[] = {0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60};
  int32_t selector_to_elems[] = {240, 120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1};

  const char *ip = input + 1;
  int32_t     count = 0;
  int32_t     _pos = 0;
  int64_t     prevValue = 0;

#if __AVX2__
  while (_pos < nelements) {
    uint64_t w = *(uint64_t*) ip;

    char    selector = (char)(w & INT64MASK(4));       // selector = 4
    char    bit = bit_per_integer[(int32_t)selector];  // bit = 3
    int32_t elems = selector_to_elems[(int32_t)selector];

    // Optimize the performance, by remove the constantly switch operation.
    int32_t  v = 4;
    uint64_t zigzag_value = 0;
    uint64_t mask = INT64MASK(bit);

    switch (type) {
      case TSDB_DATA_TYPE_BIGINT: {
        int64_t* p = (int64_t*) output;

        int32_t gRemainder = (nelements - _pos);
        int32_t num = (gRemainder > elems)? elems:gRemainder;

        int32_t batch = num >> 2;
        int32_t remain = num & 0x03;
        if (selector == 0 || selector == 1) {
          if (tsSIMDEnable && tsAVX2Enable) {
            for (int32_t i = 0; i < batch; ++i) {
              __m256i prev = _mm256_set1_epi64x(prevValue);
              _mm256_storeu_si256((__m256i *)&p[_pos], prev);
              _pos += 4;
            }

            for (int32_t i = 0; i < remain; ++i) {
              p[_pos++] = prevValue;
            }
          } else if (tsSIMDEnable && tsAVX512Enable) {
#if __AVX512F__
            // todo add avx512 impl
#endif
          } else { // alternative implementation without SIMD instructions.
            for (int32_t i = 0; i < elems && count < nelements; i++, count++) {
              p[_pos++] = prevValue;
              v += bit;
            }
          }
        } else {
          if (tsSIMDEnable && tsAVX2Enable) {
            __m256i base = _mm256_set1_epi64x(w);
            __m256i maskVal = _mm256_set1_epi64x(mask);

            __m256i shiftBits = _mm256_set_epi64x(bit * 3 + 4, bit * 2 + 4, bit + 4, 4);
            __m256i inc = _mm256_set1_epi64x(bit << 2);

            for (int32_t i = 0; i < batch; ++i) {
              __m256i after = _mm256_srlv_epi64(base, shiftBits);
              __m256i zigzagVal = _mm256_and_si256(after, maskVal);

              // ZIGZAG_DECODE(T, v) (((v) >> 1) ^ -((T)((v)&1)))
              __m256i signmask = _mm256_and_si256(_mm256_set1_epi64x(1), zigzagVal);
              signmask = _mm256_sub_epi64(_mm256_setzero_si256(), signmask);

              // get four zigzag values here
              __m256i delta = _mm256_xor_si256(_mm256_srli_epi64(zigzagVal, 1), signmask);

              // calculate the cumulative sum (prefix sum) for each number
              // decode[0] = prevValue + final[0]
              // decode[1] = decode[0] + final[1]   -----> prevValue + final[0] + final[1]
              // decode[2] = decode[1] + final[2]   -----> prevValue + final[0] + final[1] + final[2]
              // decode[3] = decode[2] + final[3]   -----> prevValue + final[0] + final[1] + final[2] + final[3]

              //  1, 2, 3, 4
              //+ 0, 1, 0, 3
              //  1, 3, 3, 7
              // shift and add for the first round
              __m128i prev = _mm_set1_epi64x(prevValue);
              __m256i x = _mm256_slli_si256(delta, 8);

              delta = _mm256_add_epi64(delta, x);
              _mm256_storeu_si256((__m256i *)&p[_pos], delta);

              //  1, 3, 3, 7
              //+ 0, 0, 3, 3
              //  1, 3, 6, 10
              // shift and add operation for the second round
              __m128i firstPart = _mm_loadu_si128((__m128i *)&p[_pos]);
              __m128i secondItem = _mm_set1_epi64x(p[_pos + 1]);
              __m128i secPart = _mm_add_epi64(_mm_loadu_si128((__m128i *)&p[_pos + 2]), secondItem);
              firstPart = _mm_add_epi64(firstPart, prev);
              secPart = _mm_add_epi64(secPart, prev);

              // save it in the memory
              _mm_storeu_si128((__m128i *)&p[_pos], firstPart);
              _mm_storeu_si128((__m128i *)&p[_pos + 2], secPart);

              shiftBits = _mm256_add_epi64(shiftBits, inc);
              prevValue = p[_pos + 3];
              _pos += 4;
            }

            // handle the remain value
            for (int32_t i = 0; i < remain; i++) {
              zigzag_value = ((w >> (v + (batch * bit * 4))) & mask);
              prevValue += ZIGZAG_DECODE(int64_t, zigzag_value);

              p[_pos++] = prevValue;
              v += bit;
            }
          } else if (tsSIMDEnable && tsAVX512Enable) {
#if __AVX512F__
            // todo add avx512 impl
#endif
          } else {  // alternative implementation without SIMD instructions.
            for (int32_t i = 0; i < elems && count < nelements; i++, count++) {
              zigzag_value = ((w >> v) & mask);
              prevValue += ZIGZAG_DECODE(int64_t, zigzag_value);

              p[_pos++] = prevValue;
              v += bit;
            }
          }
        }
      } break;
      case TSDB_DATA_TYPE_INT: {
        int32_t* p = (int32_t*) output;

        if (selector == 0 || selector == 1) {
          for (int32_t i = 0; i < elems && count < nelements; i++, count++) {
            p[_pos++] = (int32_t)prevValue;
          }
        } else {
          for (int32_t i = 0; i < elems && count < nelements; i++, count++) {
            zigzag_value = ((w >> v) & mask);
            prevValue += ZIGZAG_DECODE(int64_t, zigzag_value);

            p[_pos++] = (int32_t)prevValue;
            v += bit;
          }
        }
      } break;
      case TSDB_DATA_TYPE_SMALLINT: {
        int16_t* p = (int16_t*) output;

        if (selector == 0 || selector == 1) {
          for (int32_t i = 0; i < elems && count < nelements; i++, count++) {
            p[_pos++] = (int16_t)prevValue;
          }
        } else {
          for (int32_t i = 0; i < elems && count < nelements; i++, count++) {
            zigzag_value = ((w >> v) & mask);
            prevValue += ZIGZAG_DECODE(int64_t, zigzag_value);

            p[_pos++] = (int16_t)prevValue;
            v += bit;
          }
        }
      } break;

      case TSDB_DATA_TYPE_TINYINT: {
        int8_t *p = (int8_t *)output;

        if (selector == 0 || selector == 1) {
          for (int32_t i = 0; i < elems && count < nelements; i++, count++) {
            p[_pos++] = (int8_t)prevValue;
          }
        } else {
          for (int32_t i = 0; i < elems && count < nelements; i++, count++) {
            zigzag_value = ((w >> v) & mask);
            prevValue += ZIGZAG_DECODE(int64_t, zigzag_value);

            p[_pos++] = (int8_t)prevValue;
            v += bit;
          }
        }
      } break;
    }

    ip += LONG_BYTES;
  }

#endif
  return nelements * word_length;
}

int32_t tsDecompressFloatImplAvx512(const char *const input, const int32_t nelements, char *const output) {
#if __AVX512F__
    // todo add it
#endif
  return 0;
}

// todo add later
int32_t tsDecompressFloatImplAvx2(const char *const input, const int32_t nelements, char *const output) {
#if __AVX2__
#endif
  return 0;
}

int32_t tsDecompressTimestampAvx2(const char *const input, const int32_t nelements, char *const output,
                                  bool bigEndian) {
  int64_t *ostream = (int64_t *)output;
  int32_t  ipos = 1, opos = 0;
  __m128i  prevVal = _mm_setzero_si128();
  __m128i  prevDelta = _mm_setzero_si128();

#if __AVX2__
  int32_t batch = nelements >> 1;
  int32_t remainder = nelements & 0x01;
  __mmask16 mask2[16] = {0, 0x0001, 0x0003, 0x0007, 0x000f, 0x001f, 0x003f, 0x007f, 0x00ff};

  int32_t i = 0;
  if (batch > 1) {
    // first loop
    uint8_t flags = input[ipos++];

    int8_t nbytes1 = flags & INT8MASK(4);  // range of nbytes starts from 0 to 7
    int8_t nbytes2 = (flags >> 4) & INT8MASK(4);

    __m128i data1;
    if (nbytes1 == 0) {
      data1 = _mm_setzero_si128();
    } else {
//      _mm_shuffle_epi8()
      memcpy(&data1, (const void*) (input + ipos), nbytes1);
    }

    __m128i data2;
    if (nbytes2 == 0) {
      data2 = _mm_setzero_si128();
    } else {
      memcpy(&data2, (const void*) (input + ipos + nbytes1), nbytes2);
    }

    data2 = _mm_broadcastq_epi64(data2);
    __m128i zzVal = _mm_blend_epi32(data2, data1, 0x03);

    // ZIGZAG_DECODE(T, v) (((v) >> 1) ^ -((T)((v)&1)))
    __m128i signmask = _mm_and_si128(_mm_set1_epi64x(1), zzVal);
    signmask = _mm_sub_epi64(_mm_setzero_si128(), signmask);

    // get two zigzag values here
    __m128i deltaOfDelta = _mm_xor_si128(_mm_srli_epi64(zzVal, 1), signmask);

    __m128i deltaCurrent = _mm_add_epi64(deltaOfDelta, prevDelta);
    deltaCurrent = _mm_add_epi64(_mm_slli_si128(deltaCurrent, 8), deltaCurrent);

    __m128i val = _mm_add_epi64(deltaCurrent, prevVal);
    _mm_storeu_si128((__m128i *)&ostream[opos], val);

    // keep the previous value
    prevVal = _mm_set1_epi64x(val[1]);

    // keep the previous delta of delta, for the first item
    prevDelta = _mm_set1_epi64x(deltaOfDelta[1]);

    opos += 2;
    ipos += nbytes1 + nbytes2;
    i += 1;
  }

  // the remain
  for(; i < batch; ++i) {
    uint8_t flags = input[ipos++];

    int8_t nbytes1 = flags & INT8MASK(4);  // range of nbytes starts from 0 to 7
    int8_t nbytes2 = (flags >> 4) & INT8MASK(4);

//    __m128i data1 = _mm_maskz_loadu_epi8(mask2[nbytes1], (const void*)(input + ipos));
//    __m128i data2 = _mm_maskz_loadu_epi8(mask2[nbytes2], (const void*)(input + ipos + nbytes1));
    __m128i data1;
    if (nbytes1 == 0) {
      data1 = _mm_setzero_si128();
    } else {
      int64_t dd = 0;
      memcpy(&dd, (const void*) (input + ipos), nbytes1);
      data1 = _mm_loadu_si64(&dd);
    }

    __m128i data2;
    if (nbytes2 == 0) {
      data2 = _mm_setzero_si128();
    } else {
      int64_t dd = 0;
      memcpy(&dd, (const void*) (input + ipos + nbytes1), nbytes2);
      data2 = _mm_loadu_si64(&dd);
    }

    data2 = _mm_broadcastq_epi64(data2);

    __m128i zzVal = _mm_blend_epi32(data2, data1, 0x03);

    // ZIGZAG_DECODE(T, v) (((v) >> 1) ^ -((T)((v)&1)))
    __m128i signmask = _mm_and_si128(_mm_set1_epi64x(1), zzVal);
    signmask = _mm_sub_epi64(_mm_setzero_si128(), signmask);

    // get two zigzag values here
    __m128i deltaOfDelta = _mm_xor_si128(_mm_srli_epi64(zzVal, 1), signmask);

    __m128i deltaCurrent = _mm_add_epi64(deltaOfDelta, prevDelta);
    deltaCurrent = _mm_add_epi64(_mm_slli_si128(deltaCurrent, 8), deltaCurrent);

    __m128i val = _mm_add_epi64(deltaCurrent, prevVal);
    _mm_storeu_si128((__m128i *)&ostream[opos], val);

    // keep the previous value
    prevVal = _mm_set1_epi64x(val[1]);

    // keep the previous delta of delta
    __m128i delta = _mm_add_epi64(_mm_slli_si128(deltaOfDelta, 8), deltaOfDelta);
    prevDelta = _mm_set1_epi64x(_mm_add_epi64(delta, prevDelta)[1]);

    opos += 2;
    ipos += nbytes1 + nbytes2;
  }

  if (remainder > 0) {
    uint64_t dd = 0;
    uint8_t  flags = input[ipos++];

    int32_t nbytes = flags & INT8MASK(4);
    int64_t deltaOfDelta = 0;
    if (nbytes == 0) {
      deltaOfDelta = 0;
    } else {
      //      if (is_bigendian()) {
      //        memcpy(((char *)(&dd1)) + longBytes - nbytes, input + ipos, nbytes);
      //      } else {
      memcpy(&dd, input + ipos, nbytes);
      //      }
      deltaOfDelta = ZIGZAG_DECODE(int64_t, dd);
    }

    ipos += nbytes;
    if (opos == 0) {
      ostream[opos++] = deltaOfDelta;
    } else {
      int64_t prevDeltaX = deltaOfDelta + prevDelta[1];
      ostream[opos++] = prevVal[1] + prevDeltaX;
    }
  }
#endif

  return 0;
}

int32_t tsDecompressTimestampAvx512(const char* const input, const int32_t nelements, char *const output, bool bigEndian) {
  int64_t *ostream = (int64_t *)output;
  int32_t  ipos = 1, opos = 0;
  __m128i  prevVal = _mm_setzero_si128();
  __m128i  prevDelta = _mm_setzero_si128();

  // _mm_maskz_loadu_epi8
#if __AVX512VL__

  int32_t batch = nelements >> 1;
  int32_t remainder = nelements & 0x01;
  __mmask16 mask2[16] = {0, 0x0001, 0x0003, 0x0007, 0x000f, 0x001f, 0x003f, 0x007f, 0x00ff};

  int32_t i = 0;
  if (batch > 1) {
    // first loop
    uint8_t flags = input[ipos++];

    int8_t nbytes1 = flags & INT8MASK(4);  // range of nbytes starts from 0 to 7
    int8_t nbytes2 = (flags >> 4) & INT8MASK(4);

    __m128i data1 = _mm_maskz_loadu_epi8(mask2[nbytes1], (const void*)(input + ipos));
    __m128i data2 = _mm_maskz_loadu_epi8(mask2[nbytes2], (const void*)(input + ipos + nbytes1));
    data2 = _mm_broadcastq_epi64(data2);

    __m128i zzVal = _mm_blend_epi32(data2, data1, 0x03);

    // ZIGZAG_DECODE(T, v) (((v) >> 1) ^ -((T)((v)&1)))
    __m128i signmask = _mm_and_si128(_mm_set1_epi64x(1), zzVal);
    signmask = _mm_sub_epi64(_mm_setzero_si128(), signmask);

    // get two zigzag values here
    __m128i deltaOfDelta = _mm_xor_si128(_mm_srli_epi64(zzVal, 1), signmask);

    __m128i deltaCurrent = _mm_add_epi64(deltaOfDelta, prevDelta);
    deltaCurrent = _mm_add_epi64(_mm_slli_si128(deltaCurrent, 8), deltaCurrent);

    __m128i val = _mm_add_epi64(deltaCurrent, prevVal);
    _mm_storeu_si128((__m128i *)&ostream[opos], val);

    // keep the previous value
    prevVal = _mm_shuffle_epi32 (val, 0xEE);

    // keep the previous delta of delta, for the first item
    prevDelta = _mm_shuffle_epi32(deltaOfDelta, 0xEE);

    opos += 2;
    ipos += nbytes1 + nbytes2;
    i += 1;
  }

  // the remain
  for(; i < batch; ++i) {
    uint8_t flags = input[ipos++];

    int8_t nbytes1 = flags & INT8MASK(4);  // range of nbytes starts from 0 to 7
    int8_t nbytes2 = (flags >> 4) & INT8MASK(4);

    __m128i data1 = _mm_maskz_loadu_epi8(mask2[nbytes1], (const void*)(input + ipos));
    __m128i data2 = _mm_maskz_loadu_epi8(mask2[nbytes2], (const void*)(input + ipos + nbytes1));
    data2 = _mm_broadcastq_epi64(data2);

    __m128i zzVal = _mm_blend_epi32(data2, data1, 0x03);

    // ZIGZAG_DECODE(T, v) (((v) >> 1) ^ -((T)((v)&1)))
    __m128i signmask = _mm_and_si128(_mm_set1_epi64x(1), zzVal);
    signmask = _mm_sub_epi64(_mm_setzero_si128(), signmask);

    // get two zigzag values here
    __m128i deltaOfDelta = _mm_xor_si128(_mm_srli_epi64(zzVal, 1), signmask);

    __m128i deltaCurrent = _mm_add_epi64(deltaOfDelta, prevDelta);
    deltaCurrent = _mm_add_epi64(_mm_slli_si128(deltaCurrent, 8), deltaCurrent);

    __m128i val = _mm_add_epi64(deltaCurrent, prevVal);
    _mm_storeu_si128((__m128i *)&ostream[opos], val);

    // keep the previous value
    prevVal = _mm_shuffle_epi32 (val, 0xEE);

    // keep the previous delta of delta
    __m128i delta = _mm_add_epi64(_mm_slli_si128(deltaOfDelta, 8), deltaOfDelta);
    prevDelta = _mm_shuffle_epi32(_mm_add_epi64(delta, prevDelta), 0xEE);

    opos += 2;
    ipos += nbytes1 + nbytes2;
  }

  if (remainder > 0) {
    uint64_t dd = 0;
    uint8_t  flags = input[ipos++];

    int32_t nbytes = flags & INT8MASK(4);
    int64_t deltaOfDelta = 0;
    if (nbytes == 0) {
      deltaOfDelta = 0;
    } else {
      //      if (is_bigendian()) {
      //        memcpy(((char *)(&dd1)) + longBytes - nbytes, input + ipos, nbytes);
      //      } else {
      memcpy(&dd, input + ipos, nbytes);
      //      }
      deltaOfDelta = ZIGZAG_DECODE(int64_t, dd);
    }

    ipos += nbytes;
    if (opos == 0) {
      ostream[opos++] = deltaOfDelta;
    } else {
      int64_t prevDeltaX = deltaOfDelta + prevDelta[1];
      ostream[opos++] = prevVal[1] + prevDeltaX;
    }
  }

#endif
  return 0;
}