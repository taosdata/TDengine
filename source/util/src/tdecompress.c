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
  int64_t     prev_value = 0;

#if __AVX2__
  while (1) {
    if (_pos == nelements) break;

    uint64_t w = 0;
    memcpy(&w, ip, LONG_BYTES);

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
              __m256i prev = _mm256_set1_epi64x(prev_value);
              _mm256_storeu_si256((__m256i *)&p[_pos], prev);
              _pos += 4;
            }

            for (int32_t i = 0; i < remain; ++i) {
              p[_pos++] = prev_value;
            }
          } else if (tsSIMDEnable && tsAVX512Enable) {
#if __AVX512F__
            // todo add avx512 impl
#endif
          } else { // alternative implementation without SIMD instructions.
            for (int32_t i = 0; i < elems && count < nelements; i++, count++) {
              p[_pos++] = prev_value;
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

              // get the four zigzag values here
              __m256i delta = _mm256_xor_si256(_mm256_srli_epi64(zigzagVal, 1), signmask);

              // calculate the cumulative sum (prefix sum) for each number
              // decode[0] = prev_value + final[0]
              // decode[1] = decode[0] + final[1]   -----> prev_value + final[0] + final[1]
              // decode[2] = decode[1] + final[2]   -----> prev_value + final[0] + final[1] + final[2]
              // decode[3] = decode[2] + final[3]   -----> prev_value + final[0] + final[1] + final[2] + final[3]

              //  1, 2, 3, 4
              //+ 0, 1, 0, 3
              //  1, 3, 3, 7
              // shift and add for the first round
              __m128i prev = _mm_set1_epi64x(prev_value);
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
              prev_value = p[_pos + 3];
              _pos += 4;
            }

            // handle the remain value
            for (int32_t i = 0; i < remain; i++) {
              zigzag_value = ((w >> (v + (batch * bit * 4))) & mask);
              prev_value += ZIGZAG_DECODE(int64_t, zigzag_value);

              p[_pos++] = prev_value;
              v += bit;
            }
          } else if (tsSIMDEnable && tsAVX512Enable) {
#if __AVX512F__
            // todo add avx512 impl
#endif
          } else {  // alternative implementation without SIMD instructions.
            for (int32_t i = 0; i < elems && count < nelements; i++, count++) {
              zigzag_value = ((w >> v) & mask);
              prev_value += ZIGZAG_DECODE(int64_t, zigzag_value);

              p[_pos++] = prev_value;
              v += bit;
            }
          }
        }
      } break;
      case TSDB_DATA_TYPE_INT: {
        int32_t* p = (int32_t*) output;

        if (selector == 0 || selector == 1) {
          for (int32_t i = 0; i < elems && count < nelements; i++, count++) {
            p[_pos++] = (int32_t)prev_value;
          }
        } else {
          for (int32_t i = 0; i < elems && count < nelements; i++, count++) {
            zigzag_value = ((w >> v) & mask);
            prev_value += ZIGZAG_DECODE(int64_t, zigzag_value);

            p[_pos++] = (int32_t)prev_value;
            v += bit;
          }
        }
      } break;
      case TSDB_DATA_TYPE_SMALLINT: {
        int16_t* p = (int16_t*) output;

        if (selector == 0 || selector == 1) {
          for (int32_t i = 0; i < elems && count < nelements; i++, count++) {
            p[_pos++] = (int16_t)prev_value;
          }
        } else {
          for (int32_t i = 0; i < elems && count < nelements; i++, count++) {
            zigzag_value = ((w >> v) & mask);
            prev_value += ZIGZAG_DECODE(int64_t, zigzag_value);

            p[_pos++] = (int16_t)prev_value;
            v += bit;
          }
        }
      } break;

      case TSDB_DATA_TYPE_TINYINT: {
        int8_t *p = (int8_t *)output;

        if (selector == 0 || selector == 1) {
          for (int32_t i = 0; i < elems && count < nelements; i++, count++) {
            p[_pos++] = (int8_t)prev_value;
          }
        } else {
          for (int32_t i = 0; i < elems && count < nelements; i++, count++) {
            zigzag_value = ((w >> v) & mask);
            prev_value += ZIGZAG_DECODE(int64_t, zigzag_value);

            p[_pos++] = (int8_t)prev_value;
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