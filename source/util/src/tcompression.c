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

/* README.md   TAOS compression
 *
 * INTEGER Compression Algorithm:
 *   To compress integers (including char, short, int32_t, int64_t), the difference
 *   between two integers is calculated at first. Then the difference is
 *   transformed to positive by zig-zag encoding method
 *   (https://gist.github.com/mfuerstenau/ba870a29e16536fdbaba). Then the value is
 *   encoded using simple 8B method. For more information about simple 8B,
 *   refer to https://en.wikipedia.org/wiki/8b/10b_encoding.
 *
 *   NOTE : For bigint, only 59 bits can be used, which means data from -(2**59) to (2**59)-1
 *   are allowed.
 *
 * BOOLEAN Compression Algorithm:
 *   We provide two methods for compress boolean types. Because boolean types in C
 *   code are char bytes with 0 and 1 values only, only one bit can used to discriminate
 *   the values.
 *   1. The first method is using only 1 bit to represent the boolean value with 1 for
 *   true and 0 for false. Then the compression rate is 1/8.
 *   2. The second method is using run length encoding (RLE) methods. This method works
 *   better when there are a lot of consecutive true values or false values.
 *
 * STRING Compression Algorithm:
 *   We us LZ4 method to compress the string type.
 *
 * FLOAT Compression Algorithm:
 *   We use the same method with Akumuli to compress float and double types. The compression
 *   algorithm assumes the float/double values change slightly. So we take the XOR between two
 *   adjacent values. Then compare the number of leading zeros and trailing zeros. If the number
 *   of leading zeros are larger than the trailing zeros, then record the last serveral bytes
 *   of the XORed value with informations. If not, record the first corresponding bytes.
 *
 */

#define _DEFAULT_SOURCE
#include "tcompression.h"
#include "lz4.h"
#include "tRealloc.h"
#include "tlog.h"
#include "ttypes.h"

#ifdef TD_TSZ
#include "td_sz.h"
#endif

static const int32_t TEST_NUMBER = 1;
#define is_bigendian()     ((*(char *)&TEST_NUMBER) == 0)
#define SIMPLE8B_MAX_INT64 ((uint64_t)1152921504606846974LL)

#define safeInt64Add(a, b)  (((a >= 0) && (b <= INT64_MAX - a)) || ((a < 0) && (b >= INT64_MIN - a)))

#ifdef TD_TSZ
bool lossyFloat = false;
bool lossyDouble = false;

// init call
int32_t tsCompressInit(char* lossyColumns, float fPrecision, double dPrecision, uint32_t maxIntervals, uint32_t intervals,
                       int32_t ifAdtFse, const char* compressor) {
  // config
  lossyFloat = strstr(lossyColumns, "float") != NULL;
  lossyDouble = strstr(lossyColumns, "double") != NULL;

  tdszInit(fPrecision, dPrecision, maxIntervals, intervals, ifAdtFse, compressor);
  if (lossyFloat) uTrace("lossy compression float  is opened. ");
  if (lossyDouble) uTrace("lossy compression double is opened. ");
  return 1;
}
// exit call
void tsCompressExit() { tdszExit(); }

#endif

/*
 * Compress Integer (Simple8B).
 */
int32_t tsCompressINTImp(const char *const input, const int32_t nelements, char *const output, const char type) {
  // Selector value:              0    1   2   3   4   5   6   7   8  9  10  11
  // 12  13  14  15
  char    bit_per_integer[] = {0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60};
  int32_t selector_to_elems[] = {240, 120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1};
  char    bit_to_selector[] = {0,  2,  3,  4,  5,  6,  7,  8,  9,  10, 10, 11, 11, 12, 12, 12, 13, 13, 13, 13, 13,
                               14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
                               15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15};

  // get the byte limit.
  int32_t word_length = getWordLength(type);

  int32_t byte_limit = nelements * word_length + 1;
  int32_t opos = 1;
  int64_t prev_value = 0;

  for (int32_t i = 0; i < nelements;) {
    char    selector = 0;
    char    bit = 0;
    int32_t elems = 0;
    int64_t prev_value_tmp = prev_value;

    for (int32_t j = i; j < nelements; j++) {
      // Read data from the input stream and convert it to INT64 type.
      int64_t curr_value = 0;
      switch (type) {
        case TSDB_DATA_TYPE_TINYINT:
          curr_value = (int64_t)(*((int8_t *)input + j));
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          curr_value = (int64_t)(*((int16_t *)input + j));
          break;
        case TSDB_DATA_TYPE_INT:
          curr_value = (int64_t)(*((int32_t *)input + j));
          break;
        case TSDB_DATA_TYPE_BIGINT:
          curr_value = (int64_t)(*((int64_t *)input + j));
          break;
      }
      // Get difference.
      if (!safeInt64Add(curr_value, -prev_value_tmp)) goto _copy_and_exit;

      int64_t diff = curr_value - prev_value_tmp;
      // Zigzag encode the value.
      uint64_t zigzag_value = ZIGZAG_ENCODE(int64_t, diff);

      if (zigzag_value >= SIMPLE8B_MAX_INT64) goto _copy_and_exit;

      int64_t tmp_bit;
      if (zigzag_value == 0) {
        // Take care here, __builtin_clzl give wrong anser for value 0;
        tmp_bit = 0;
      } else {
        tmp_bit = (LONG_BYTES * BITS_PER_BYTE) - BUILDIN_CLZL(zigzag_value);
      }

      if (elems + 1 <= selector_to_elems[(int32_t)selector] &&
          elems + 1 <= selector_to_elems[(int32_t)(bit_to_selector[(int32_t)tmp_bit])]) {
        // If can hold another one.
        selector = selector > bit_to_selector[(int32_t)tmp_bit] ? selector : bit_to_selector[(int32_t)tmp_bit];
        elems++;
        bit = bit_per_integer[(int32_t)selector];
      } else {
        // if cannot hold another one.
        while (elems < selector_to_elems[(int32_t)selector]) selector++;
        elems = selector_to_elems[(int32_t)selector];
        bit = bit_per_integer[(int32_t)selector];
        break;
      }
      prev_value_tmp = curr_value;
    }

    uint64_t buffer = 0;
    buffer |= (uint64_t)selector;
    for (int32_t k = 0; k < elems; k++) {
      int64_t curr_value = 0; /* get current values */
      switch (type) {
        case TSDB_DATA_TYPE_TINYINT:
          curr_value = (int64_t)(*((int8_t *)input + i));
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          curr_value = (int64_t)(*((int16_t *)input + i));
          break;
        case TSDB_DATA_TYPE_INT:
          curr_value = (int64_t)(*((int32_t *)input + i));
          break;
        case TSDB_DATA_TYPE_BIGINT:
          curr_value = (int64_t)(*((int64_t *)input + i));
          break;
      }
      int64_t  diff = curr_value - prev_value;
      uint64_t zigzag_value = ZIGZAG_ENCODE(int64_t, diff);
      buffer |= ((zigzag_value & INT64MASK(bit)) << (bit * k + 4));
      i++;
      prev_value = curr_value;
    }

    // Output the encoded value to the output.
    if (opos + sizeof(buffer) <= byte_limit) {
      memcpy(output + opos, &buffer, sizeof(buffer));
      opos += sizeof(buffer);
    } else {
    _copy_and_exit:
      output[0] = 1;
      memcpy(output + 1, input, byte_limit - 1);
      return byte_limit;
    }
  }

  // set the indicator.
  output[0] = 0;
  return opos;
}

int32_t tsDecompressINTImp(const char *const input, const int32_t nelements, char *const output, const char type) {
  int32_t word_length = getWordLength(type);
  if (word_length == -1) {
    return word_length;
  }

  // If not compressed.
  if (input[0] == 1) {
    memcpy(output, input + 1, nelements * word_length);
    return nelements * word_length;
  }

#if __AVX2__
  tsDecompressIntImpl_Hw(input, nelements, output, type);
  return nelements * word_length;
#else
  // Selector value: 0    1   2   3   4   5   6   7   8  9  10  11 12  13  14  15
  char    bit_per_integer[] = {0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60};
  int32_t selector_to_elems[] = {240, 120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1};

  const char *ip = input + 1;
  int32_t     count = 0;
  int32_t     _pos = 0;
  int64_t     prev_value = 0;

  while (1) {
    if (count == nelements) break;

    uint64_t w = 0;
    memcpy(&w, ip, LONG_BYTES);

    char    selector = (char)(w & INT64MASK(4));       // selector = 4
    char    bit = bit_per_integer[(int32_t)selector];  // bit = 3
    int32_t elems = selector_to_elems[(int32_t)selector];

    for (int32_t i = 0; i < elems; i++) {
      uint64_t zigzag_value;

      if (selector == 0 || selector == 1) {
        zigzag_value = 0;
      } else {
        zigzag_value = ((w >> (4 + bit * i)) & INT64MASK(bit));
      }
      int64_t diff = ZIGZAG_DECODE(int64_t, zigzag_value);
      int64_t curr_value = diff + prev_value;
      prev_value = curr_value;

      switch (type) {
        case TSDB_DATA_TYPE_BIGINT:
          *((int64_t *)output + _pos) = (int64_t)curr_value;
          _pos++;
          break;
        case TSDB_DATA_TYPE_INT:
          *((int32_t *)output + _pos) = (int32_t)curr_value;
          _pos++;
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          *((int16_t *)output + _pos) = (int16_t)curr_value;
          _pos++;
          break;
        case TSDB_DATA_TYPE_TINYINT:
          *((int8_t *)output + _pos) = (int8_t)curr_value;
          _pos++;
          break;
        default:
          perror("Wrong integer types.\n");
          return -1;
      }
      count++;
      if (count == nelements) break;
    }
    ip += LONG_BYTES;
  }

  return nelements * word_length;
#endif
}

/* ----------------------------------------------Bool Compression ---------------------------------------------- */
// TODO: You can also implement it using RLE method.
int32_t tsCompressBoolImp(const char *const input, const int32_t nelements, char *const output) {
  int32_t pos = -1;
  int32_t ele_per_byte = BITS_PER_BYTE / 2;

  for (int32_t i = 0; i < nelements; i++) {
    if (i % ele_per_byte == 0) {
      pos++;
      output[pos] = 0;
    }

    uint8_t t = 0;
    if (input[i] == 1) {
      t = (((uint8_t)1) << (2 * (i % ele_per_byte)));
      output[pos] |= t;
    } else if (input[i] == 0) {
      t = ((uint8_t)1 << (2 * (i % ele_per_byte))) - 1;
      /* t = (~((( uint8_t)1) << (7-i%BITS_PER_BYTE))); */
      output[pos] &= t;
    } else if (input[i] == TSDB_DATA_BOOL_NULL) {
      t = ((uint8_t)2 << (2 * (i % ele_per_byte)));
      /* t = (~((( uint8_t)1) << (7-i%BITS_PER_BYTE))); */
      output[pos] |= t;
    } else {
      uError("Invalid compress bool value:%d", output[pos]);
      return -1;
    }
  }

  return pos + 1;
}

int32_t tsDecompressBoolImp(const char *const input, const int32_t nelements, char *const output) {
  int32_t ipos = -1, opos = 0;
  int32_t ele_per_byte = BITS_PER_BYTE / 2;

  for (int32_t i = 0; i < nelements; i++) {
    if (i % ele_per_byte == 0) {
      ipos++;
    }

    uint8_t ele = (input[ipos] >> (2 * (i % ele_per_byte))) & INT8MASK(2);
    if (ele == 1) {
      output[opos++] = 1;
    } else if (ele == 2) {
      output[opos++] = TSDB_DATA_BOOL_NULL;
    } else {
      output[opos++] = 0;
    }
  }

  return nelements;
}

#if 0
/* Run Length Encoding(RLE) Method */
int32_t tsCompressBoolRLEImp(const char *const input, const int32_t nelements, char *const output) {
  int32_t _pos = 0;

  for (int32_t i = 0; i < nelements;) {
    unsigned char counter = 1;
    char          num = input[i];

    for (++i; i < nelements; i++) {
      if (input[i] == num) {
        counter++;
        if (counter == INT8MASK(7)) {
          i++;
          break;
        }
      } else {
        break;
      }
    }

    // Encode the data.
    if (num == 1) {
      output[_pos++] = INT8MASK(1) | (counter << 1);
    } else if (num == 0) {
      output[_pos++] = (counter << 1) | INT8MASK(0);
    } else {
      uError("Invalid compress bool value:%d", output[_pos]);
      return -1;
    }
  }

  return _pos;
}

int32_t tsDecompressBoolRLEImp(const char *const input, const int32_t nelements, char *const output) {
  int32_t ipos = 0, opos = 0;
  while (1) {
    char     encode = input[ipos++];
    unsigned counter = (encode >> 1) & INT8MASK(7);
    char     value = encode & INT8MASK(1);

    memset(output + opos, value, counter);
    opos += counter;
    if (opos >= nelements) {
      return nelements;
    }
  }
}
#endif

/* ----------------------------------------------String Compression ---------------------------------------------- */
// Note: the size of the output must be larger than input_size + 1 and
// LZ4_compressBound(size) + 1;
// >= max(input_size, LZ4_compressBound(input_size)) + 1;
int32_t tsCompressStringImp(const char *const input, int32_t inputSize, char *const output, int32_t outputSize) {
  // Try to compress using LZ4 algorithm.
  const int32_t compressed_data_size = LZ4_compress_default(input, output + 1, inputSize, outputSize - 1);

  // If cannot compress or after compression, data becomes larger.
  if (compressed_data_size <= 0 || compressed_data_size > inputSize) {
    /* First byte is for indicator */
    output[0] = 0;
    memcpy(output + 1, input, inputSize);
    return inputSize + 1;
  }

  output[0] = 1;
  return compressed_data_size + 1;
}

int32_t tsDecompressStringImp(const char *const input, int32_t compressedSize, char *const output, int32_t outputSize) {
  // compressedSize is the size of data after compression.

  if (input[0] == 1) {
    /* It is compressed by LZ4 algorithm */
    const int32_t decompressed_size = LZ4_decompress_safe(input + 1, output, compressedSize - 1, outputSize);
    if (decompressed_size < 0) {
      uError("Failed to decompress string with LZ4 algorithm, decompressed size:%d", decompressed_size);
      return -1;
    }

    return decompressed_size;
  } else if (input[0] == 0) {
    /* It is not compressed by LZ4 algorithm */
    memcpy(output, input + 1, compressedSize - 1);
    return compressedSize - 1;
  } else {
    uError("Invalid decompress string indicator:%d", input[0]);
    return -1;
  }
}

/* --------------------------------------------Timestamp Compression ---------------------------------------------- */
// TODO: Take care here, we assumes little endian encoding.
int32_t tsCompressTimestampImp(const char *const input, const int32_t nelements, char *const output) {
  int32_t _pos = 1;
  int32_t longBytes = LONG_BYTES;

  ASSERTS(nelements >= 0, "nelements is negative");

  if (nelements == 0) return 0;

  int64_t *istream = (int64_t *)input;

  int64_t prev_value = istream[0];
  if (prev_value >= 0x8000000000000000) {
    uWarn("compression timestamp is over signed long long range. ts = 0x%" PRIx64 " \n", prev_value);
    goto _exit_over;
  }
  int64_t  prev_delta = -prev_value;
  uint8_t  flags = 0, flag1 = 0, flag2 = 0;
  uint64_t dd1 = 0, dd2 = 0;

  for (int32_t i = 0; i < nelements; i++) {
    int64_t curr_value = istream[i];
    if (!safeInt64Add(curr_value, -prev_value)) goto _exit_over;
    int64_t curr_delta = curr_value - prev_value;
    if (!safeInt64Add(curr_delta, -prev_delta)) goto _exit_over;
    int64_t delta_of_delta = curr_delta - prev_delta;
    // zigzag encode the value.
    uint64_t zigzag_value = ZIGZAG_ENCODE(int64_t, delta_of_delta);
    if (i % 2 == 0) {
      flags = 0;
      dd1 = zigzag_value;
      if (dd1 == 0) {
        flag1 = 0;
      } else {
        flag1 = (uint8_t)(LONG_BYTES - BUILDIN_CLZL(dd1) / BITS_PER_BYTE);
      }
    } else {
      dd2 = zigzag_value;
      if (dd2 == 0) {
        flag2 = 0;
      } else {
        flag2 = (uint8_t)(LONG_BYTES - BUILDIN_CLZL(dd2) / BITS_PER_BYTE);
      }
      flags = flag1 | (flag2 << 4);
      // Encode the flag.
      if ((_pos + CHAR_BYTES - 1) >= nelements * longBytes) goto _exit_over;
      memcpy(output + _pos, &flags, CHAR_BYTES);
      _pos += CHAR_BYTES;
      /* Here, we assume it is little endian encoding method. */
      // Encode dd1
      if (is_bigendian()) {
        if ((_pos + flag1 - 1) >= nelements * longBytes) goto _exit_over;
        memcpy(output + _pos, (char *)(&dd1) + longBytes - flag1, flag1);
      } else {
        if ((_pos + flag1 - 1) >= nelements * longBytes) goto _exit_over;
        memcpy(output + _pos, (char *)(&dd1), flag1);
      }
      _pos += flag1;
      // Encode dd2;
      if (is_bigendian()) {
        if ((_pos + flag2 - 1) >= nelements * longBytes) goto _exit_over;
        memcpy(output + _pos, (char *)(&dd2) + longBytes - flag2, flag2);
      } else {
        if ((_pos + flag2 - 1) >= nelements * longBytes) goto _exit_over;
        memcpy(output + _pos, (char *)(&dd2), flag2);
      }
      _pos += flag2;
    }
    prev_value = curr_value;
    prev_delta = curr_delta;
  }

  if (nelements % 2 == 1) {
    flag2 = 0;
    flags = flag1 | (flag2 << 4);
    // Encode the flag.
    if ((_pos + CHAR_BYTES - 1) >= nelements * longBytes) goto _exit_over;
    memcpy(output + _pos, &flags, CHAR_BYTES);
    _pos += CHAR_BYTES;
    // Encode dd1;
    if (is_bigendian()) {
      if ((_pos + flag1 - 1) >= nelements * longBytes) goto _exit_over;
      memcpy(output + _pos, (char *)(&dd1) + longBytes - flag1, flag1);
    } else {
      if ((_pos + flag1 - 1) >= nelements * longBytes) goto _exit_over;
      memcpy(output + _pos, (char *)(&dd1), flag1);
    }
    _pos += flag1;
  }

  output[0] = 1;  // Means the string is compressed
  return _pos;

_exit_over:
  output[0] = 0;  // Means the string is not compressed
  memcpy(output + 1, input, nelements * longBytes);
  return nelements * longBytes + 1;
}

int32_t tsDecompressTimestampImp(const char *const input, const int32_t nelements, char *const output) {
  int64_t longBytes = LONG_BYTES;

  ASSERTS(nelements >= 0, "nelements is negative");
  if (nelements == 0) return 0;

  if (input[0] == 0) {
    memcpy(output, input + 1, nelements * longBytes);
    return nelements * longBytes;
  } else if (input[0] == 1) {  // Decompress
    if (tsSIMDEnable && tsAVX512Enable) {
      tsDecompressTimestampAvx512(input, nelements, output, false);
    } else if (tsSIMDEnable && tsAVX2Enable) {
      tsDecompressTimestampAvx2(input, nelements, output, false);
    } else {
      int64_t *ostream = (int64_t *)output;

      int32_t ipos = 1, opos = 0;
      int8_t  nbytes = 0;
      int64_t prev_value = 0;
      int64_t prev_delta = 0;
      int64_t delta_of_delta = 0;

      while (1) {
        uint8_t flags = input[ipos++];
        // Decode dd1
        uint64_t dd1 = 0;
        nbytes = flags & INT8MASK(4);
        if (nbytes == 0) {
          delta_of_delta = 0;
        } else {
          if (is_bigendian()) {
            memcpy(((char *)(&dd1)) + longBytes - nbytes, input + ipos, nbytes);
          } else {
            memcpy(&dd1, input + ipos, nbytes);
          }
          delta_of_delta = ZIGZAG_DECODE(int64_t, dd1);
        }

        ipos += nbytes;
        if (opos == 0) {
          prev_value = delta_of_delta;
          prev_delta = 0;
          ostream[opos++] = delta_of_delta;
        } else {
          prev_delta = delta_of_delta + prev_delta;
          prev_value = prev_value + prev_delta;
          ostream[opos++] = prev_value;
        }
        if (opos == nelements) return nelements * longBytes;

        // Decode dd2
        uint64_t dd2 = 0;
        nbytes = (flags >> 4) & INT8MASK(4);
        if (nbytes == 0) {
          delta_of_delta = 0;
        } else {
          if (is_bigendian()) {
            memcpy(((char *)(&dd2)) + longBytes - nbytes, input + ipos, nbytes);
          } else {
            memcpy(&dd2, input + ipos, nbytes);
          }
          // zigzag_decoding
          delta_of_delta = ZIGZAG_DECODE(int64_t, dd2);
        }
        ipos += nbytes;
        prev_delta = delta_of_delta + prev_delta;
        prev_value = prev_value + prev_delta;
        ostream[opos++] = prev_value;
        if (opos == nelements) return nelements * longBytes;
      }
    }
  }

  return nelements * longBytes;
}

/* --------------------------------------------Double Compression ---------------------------------------------- */
void encodeDoubleValue(uint64_t diff, uint8_t flag, char *const output, int32_t *const pos) {
  int32_t longBytes = LONG_BYTES;

  uint8_t nbytes = (flag & INT8MASK(3)) + 1;
  int32_t nshift = (longBytes * BITS_PER_BYTE - nbytes * BITS_PER_BYTE) * (flag >> 3);
  diff >>= nshift;

  while (nbytes) {
    output[(*pos)++] = (int8_t)(diff & INT64MASK(8));
    diff >>= BITS_PER_BYTE;
    nbytes--;
  }
}

int32_t tsCompressDoubleImp(const char *const input, const int32_t nelements, char *const output) {
  int32_t byte_limit = nelements * DOUBLE_BYTES + 1;
  int32_t opos = 1;

  uint64_t prev_value = 0;
  uint64_t prev_diff = 0;
  uint8_t  prev_flag = 0;

  double *istream = (double *)input;

  // Main loop
  for (int32_t i = 0; i < nelements; i++) {
    union {
      double   real;
      uint64_t bits;
    } curr;

    curr.real = istream[i];

    // Here we assume the next value is the same as previous one.
    uint64_t predicted = prev_value;
    uint64_t diff = curr.bits ^ predicted;

    int32_t leading_zeros = LONG_BYTES * BITS_PER_BYTE;
    int32_t trailing_zeros = leading_zeros;

    if (diff) {
      trailing_zeros = BUILDIN_CTZL(diff);
      leading_zeros = BUILDIN_CLZL(diff);
    }

    uint8_t nbytes = 0;
    uint8_t flag;

    if (trailing_zeros > leading_zeros) {
      nbytes = (uint8_t)(LONG_BYTES - trailing_zeros / BITS_PER_BYTE);

      if (nbytes > 0) nbytes--;
      flag = ((uint8_t)1 << 3) | nbytes;
    } else {
      nbytes = (uint8_t)(LONG_BYTES - leading_zeros / BITS_PER_BYTE);
      if (nbytes > 0) nbytes--;
      flag = nbytes;
    }

    if (i % 2 == 0) {
      prev_diff = diff;
      prev_flag = flag;
    } else {
      int32_t nbyte1 = (prev_flag & INT8MASK(3)) + 1;
      int32_t nbyte2 = (flag & INT8MASK(3)) + 1;
      if (opos + 1 + nbyte1 + nbyte2 <= byte_limit) {
        uint8_t flags = prev_flag | (flag << 4);
        output[opos++] = flags;
        encodeDoubleValue(prev_diff, prev_flag, output, &opos);
        encodeDoubleValue(diff, flag, output, &opos);
      } else {
        output[0] = 1;
        memcpy(output + 1, input, byte_limit - 1);
        return byte_limit;
      }
    }
    prev_value = curr.bits;
  }

  if (nelements % 2) {
    int32_t nbyte1 = (prev_flag & INT8MASK(3)) + 1;
    int32_t nbyte2 = 1;
    if (opos + 1 + nbyte1 + nbyte2 <= byte_limit) {
      uint8_t flags = prev_flag;
      output[opos++] = flags;
      encodeDoubleValue(prev_diff, prev_flag, output, &opos);
      encodeDoubleValue(0ul, 0, output, &opos);
    } else {
      output[0] = 1;
      memcpy(output + 1, input, byte_limit - 1);
      return byte_limit;
    }
  }

  output[0] = 0;
  return opos;
}

FORCE_INLINE uint64_t decodeDoubleValue(const char *const input, int32_t *const ipos, uint8_t flag) {
  int32_t longBytes = LONG_BYTES;

  uint64_t diff = 0ul;
  int32_t  nbytes = (flag & 0x7) + 1;
  for (int32_t i = 0; i < nbytes; i++) {
    diff |= (((uint64_t)0xff & input[(*ipos)++]) << BITS_PER_BYTE * i);
  }
  int32_t shift_width = (longBytes * BITS_PER_BYTE - nbytes * BITS_PER_BYTE) * (flag >> 3);
  diff <<= shift_width;

  return diff;
}

int32_t tsDecompressDoubleImp(const char *const input, const int32_t nelements, char *const output) {
  // output stream
  double *ostream = (double *)output;

  if (input[0] == 1) {
    memcpy(output, input + 1, nelements * DOUBLE_BYTES);
    return nelements * DOUBLE_BYTES;
  }

  uint8_t  flags = 0;
  int32_t  ipos = 1;
  int32_t  opos = 0;
  uint64_t diff = 0;
  union {
    uint64_t bits;
    double   real;
  } curr;

  curr.bits = 0;

  for (int32_t i = 0; i < nelements; i++) {
    if ((i & 0x01) == 0) {
      flags = input[ipos++];
    }

    diff = decodeDoubleValue(input, &ipos, flags & 0x0f);
    flags >>= 4;
    curr.bits ^= diff;

    ostream[opos++] = curr.real;
  }

  return nelements * DOUBLE_BYTES;
}

/* --------------------------------------------Float Compression ---------------------------------------------- */
void encodeFloatValue(uint32_t diff, uint8_t flag, char *const output, int32_t *const pos) {
  uint8_t nbytes = (flag & INT8MASK(3)) + 1;
  int32_t nshift = (FLOAT_BYTES * BITS_PER_BYTE - nbytes * BITS_PER_BYTE) * (flag >> 3);
  diff >>= nshift;

  while (nbytes) {
    output[(*pos)++] = (int8_t)(diff & INT32MASK(8));
    diff >>= BITS_PER_BYTE;
    nbytes--;
  }
}

int32_t tsCompressFloatImp(const char *const input, const int32_t nelements, char *const output) {
  float  *istream = (float *)input;
  int32_t byte_limit = nelements * FLOAT_BYTES + 1;
  int32_t opos = 1;

  uint32_t prev_value = 0;
  uint32_t prev_diff = 0;
  uint8_t  prev_flag = 0;

  // Main loop
  for (int32_t i = 0; i < nelements; i++) {
    union {
      float    real;
      uint32_t bits;
    } curr;

    curr.real = istream[i];

    // Here we assume the next value is the same as previous one.
    uint32_t predicted = prev_value;
    uint32_t diff = curr.bits ^ predicted;

    int32_t clz = FLOAT_BYTES * BITS_PER_BYTE;
    int32_t ctz = clz;

    if (diff) {
      ctz = BUILDIN_CTZ(diff);
      clz = BUILDIN_CLZ(diff);
    }

    uint8_t nbytes = 0;
    uint8_t flag;

    if (ctz > clz) {
      nbytes = (uint8_t)(FLOAT_BYTES - ctz / BITS_PER_BYTE);

      if (nbytes > 0) nbytes--;
      flag = ((uint8_t)1 << 3) | nbytes;
    } else {
      nbytes = (uint8_t)(FLOAT_BYTES - clz / BITS_PER_BYTE);
      if (nbytes > 0) nbytes--;
      flag = nbytes;
    }

    if (i % 2 == 0) {
      prev_diff = diff;
      prev_flag = flag;
    } else {
      int32_t nbyte1 = (prev_flag & INT8MASK(3)) + 1;
      int32_t nbyte2 = (flag & INT8MASK(3)) + 1;
      if (opos + 1 + nbyte1 + nbyte2 <= byte_limit) {
        uint8_t flags = prev_flag | (flag << 4);
        output[opos++] = flags;
        encodeFloatValue(prev_diff, prev_flag, output, &opos);
        encodeFloatValue(diff, flag, output, &opos);
      } else {
        output[0] = 1;
        memcpy(output + 1, input, byte_limit - 1);
        return byte_limit;
      }
    }
    prev_value = curr.bits;
  }

  if (nelements % 2) {
    int32_t nbyte1 = (prev_flag & INT8MASK(3)) + 1;
    int32_t nbyte2 = 1;
    if (opos + 1 + nbyte1 + nbyte2 <= byte_limit) {
      uint8_t flags = prev_flag;
      output[opos++] = flags;
      encodeFloatValue(prev_diff, prev_flag, output, &opos);
      encodeFloatValue(0, 0, output, &opos);
    } else {
      output[0] = 1;
      memcpy(output + 1, input, byte_limit - 1);
      return byte_limit;
    }
  }

  output[0] = 0;
  return opos;
}

uint32_t decodeFloatValue(const char *const input, int32_t *const ipos, uint8_t flag) {
  uint32_t diff = 0ul;
  int32_t  nbytes = (flag & INT8MASK(3)) + 1;
  for (int32_t i = 0; i < nbytes; i++) {
    diff = diff | ((INT32MASK(8) & input[(*ipos)++]) << BITS_PER_BYTE * i);
  }
  int32_t shift_width = (FLOAT_BYTES * BITS_PER_BYTE - nbytes * BITS_PER_BYTE) * (flag >> 3);
  diff <<= shift_width;

  return diff;
}

static void tsDecompressFloatHelper(const char *const input, const int32_t nelements, float* ostream) {
  uint8_t  flags = 0;
  int32_t  ipos = 1;
  int32_t  opos = 0;
  uint32_t prev_value = 0;

  for (int32_t i = 0; i < nelements; i++) {
    if (i % 2 == 0) {
      flags = input[ipos++];
    }

    uint8_t flag = flags & INT8MASK(4);
    flags >>= 4;

    uint32_t diff = decodeFloatValue(input, &ipos, flag);
    union {
      uint32_t bits;
      float    real;
    } curr;

    uint32_t predicted = prev_value;
    curr.bits = predicted ^ diff;
    prev_value = curr.bits;

    ostream[opos++] = curr.real;
  }
}

int32_t tsDecompressFloatImp(const char *const input, const int32_t nelements, char *const output) {
  if (input[0] == 1) {
    memcpy(output, input + 1, nelements * FLOAT_BYTES);
    return nelements * FLOAT_BYTES;
  }

  if (tsSIMDEnable && tsAVX2Enable) {
    tsDecompressFloatImplAvx2(input, nelements, output);
  } else if (tsSIMDEnable && tsAVX512Enable) {
    tsDecompressFloatImplAvx512(input, nelements, output);
  } else { // alternative implementation without SIMD instructions.
    tsDecompressFloatHelper(input, nelements, (float*)output);
  }

  return nelements * FLOAT_BYTES;
}

#ifdef TD_TSZ
//
//   ----------  float double lossy  -----------
//
int32_t tsCompressFloatLossyImp(const char *input, const int32_t nelements, char *const output) {
  // compress with sz
  int32_t       compressedSize = tdszCompress(SZ_FLOAT, input, nelements, output + 1);
  unsigned char algo = ALGO_SZ_LOSSY << 1;
  if (compressedSize == 0 || compressedSize >= nelements * sizeof(float)) {
    // compressed error or large than original
    output[0] = MODE_NOCOMPRESS | algo;
    memcpy(output + 1, input, nelements * sizeof(float));
    compressedSize = 1 + nelements * sizeof(float);
  } else {
    // compressed successfully
    output[0] = MODE_COMPRESS | algo;
    compressedSize += 1;
  }

  return compressedSize;
}

int32_t tsDecompressFloatLossyImp(const char *input, int32_t compressedSize, const int32_t nelements,
                                  char *const output) {
  int32_t decompressedSize = 0;
  if (HEAD_MODE(input[0]) == MODE_NOCOMPRESS) {
    // orginal so memcpy directly
    decompressedSize = nelements * sizeof(float);
    memcpy(output, input + 1, decompressedSize);

    return decompressedSize;
  }

  // decompressed with sz
  return tdszDecompress(SZ_FLOAT, input + 1, compressedSize - 1, nelements, output);
}

int32_t tsCompressDoubleLossyImp(const char *input, const int32_t nelements, char *const output) {
  // compress with sz
  int32_t       compressedSize = tdszCompress(SZ_DOUBLE, input, nelements, output + 1);
  unsigned char algo = ALGO_SZ_LOSSY << 1;
  if (compressedSize == 0 || compressedSize >= nelements * sizeof(double)) {
    // compressed error or large than original
    output[0] = MODE_NOCOMPRESS | algo;
    memcpy(output + 1, input, nelements * sizeof(double));
    compressedSize = 1 + nelements * sizeof(double);
  } else {
    // compressed successfully
    output[0] = MODE_COMPRESS | algo;
    compressedSize += 1;
  }

  return compressedSize;
}

int32_t tsDecompressDoubleLossyImp(const char *input, int32_t compressedSize, const int32_t nelements,
                                   char *const output) {
  int32_t decompressedSize = 0;
  if (HEAD_MODE(input[0]) == MODE_NOCOMPRESS) {
    // orginal so memcpy directly
    decompressedSize = nelements * sizeof(double);
    memcpy(output, input + 1, decompressedSize);

    return decompressedSize;
  }

  // decompressed with sz
  return tdszDecompress(SZ_DOUBLE, input + 1, compressedSize - 1, nelements, output);
}
#endif

#ifdef BUILD_NO_CALL
/*************************************************************************
 *                  STREAM COMPRESSION
 *************************************************************************/
#define I64_SAFE_ADD(a, b) (((a) >= 0 && (b) <= INT64_MAX - (a)) || ((a) < 0 && (b) >= INT64_MIN - (a)))

static int32_t tCompBoolStart(SCompressor *pCmprsor, int8_t type, int8_t cmprAlg);
static int32_t tCompBool(SCompressor *pCmprsor, const void *pData, int32_t nData);
static int32_t tCompBoolEnd(SCompressor *pCmprsor, const uint8_t **ppData, int32_t *nData);

static int32_t tCompIntStart(SCompressor *pCmprsor, int8_t type, int8_t cmprAlg);
static int32_t tCompInt(SCompressor *pCmprsor, const void *pData, int32_t nData);
static int32_t tCompIntEnd(SCompressor *pCmprsor, const uint8_t **ppData, int32_t *nData);

static int32_t tCompFloatStart(SCompressor *pCmprsor, int8_t type, int8_t cmprAlg);
static int32_t tCompFloat(SCompressor *pCmprsor, const void *pData, int32_t nData);
static int32_t tCompFloatEnd(SCompressor *pCmprsor, const uint8_t **ppData, int32_t *nData);

static int32_t tCompDoubleStart(SCompressor *pCmprsor, int8_t type, int8_t cmprAlg);
static int32_t tCompDouble(SCompressor *pCmprsor, const void *pData, int32_t nData);
static int32_t tCompDoubleEnd(SCompressor *pCmprsor, const uint8_t **ppData, int32_t *nData);

static int32_t tCompTimestampStart(SCompressor *pCmprsor, int8_t type, int8_t cmprAlg);
static int32_t tCompTimestamp(SCompressor *pCmprsor, const void *pData, int32_t nData);
static int32_t tCompTimestampEnd(SCompressor *pCmprsor, const uint8_t **ppData, int32_t *nData);

static int32_t tCompBinaryStart(SCompressor *pCmprsor, int8_t type, int8_t cmprAlg);
static int32_t tCompBinary(SCompressor *pCmprsor, const void *pData, int32_t nData);
static int32_t tCompBinaryEnd(SCompressor *pCmprsor, const uint8_t **ppData, int32_t *nData);

static FORCE_INLINE int64_t tGetI64OfI8(const void *pData) { return *(int8_t *)pData; }
static FORCE_INLINE int64_t tGetI64OfI16(const void *pData) { return *(int16_t *)pData; }
static FORCE_INLINE int64_t tGetI64OfI32(const void *pData) { return *(int32_t *)pData; }
static FORCE_INLINE int64_t tGetI64OfI64(const void *pData) { return *(int64_t *)pData; }

static FORCE_INLINE void tPutI64OfI8(int64_t v, void *pData) { *(int8_t *)pData = v; }
static FORCE_INLINE void tPutI64OfI16(int64_t v, void *pData) { *(int16_t *)pData = v; }
static FORCE_INLINE void tPutI64OfI32(int64_t v, void *pData) { *(int32_t *)pData = v; }
static FORCE_INLINE void tPutI64OfI64(int64_t v, void *pData) { *(int64_t *)pData = v; }

static struct {
  int8_t  type;
  int32_t bytes;
  int8_t  isVarLen;
  int32_t (*startFn)(SCompressor *, int8_t type, int8_t cmprAlg);
  int32_t (*cmprFn)(SCompressor *, const void *, int32_t nData);
  int32_t (*endFn)(SCompressor *, const uint8_t **, int32_t *);
  int64_t (*getI64)(const void *pData);
  void (*putI64)(int64_t v, void *pData);
} DATA_TYPE_INFO[] = {
    {.type = TSDB_DATA_TYPE_NULL,
     .bytes = 0,
     .isVarLen = 0,
     .startFn = NULL,
     .cmprFn = NULL,
     .endFn = NULL,
     .getI64 = NULL,
     .putI64 = NULL},
    {.type = TSDB_DATA_TYPE_BOOL,
     .bytes = 1,
     .isVarLen = 0,
     .startFn = tCompBoolStart,
     .cmprFn = tCompBool,
     .endFn = tCompBoolEnd,
     .getI64 = NULL,
     .putI64 = NULL},
    {.type = TSDB_DATA_TYPE_TINYINT,
     .bytes = 1,
     .isVarLen = 0,
     .startFn = tCompIntStart,
     .cmprFn = tCompInt,
     .endFn = tCompIntEnd,
     .getI64 = tGetI64OfI8,
     .putI64 = tPutI64OfI8},
    {.type = TSDB_DATA_TYPE_SMALLINT,
     .bytes = 2,
     .isVarLen = 0,
     .startFn = tCompIntStart,
     .cmprFn = tCompInt,
     .endFn = tCompIntEnd,
     .getI64 = tGetI64OfI16,
     .putI64 = tPutI64OfI16},
    {.type = TSDB_DATA_TYPE_INT,
     .bytes = 4,
     .isVarLen = 0,
     .startFn = tCompIntStart,
     .cmprFn = tCompInt,
     .endFn = tCompIntEnd,
     .getI64 = tGetI64OfI32,
     .putI64 = tPutI64OfI32},
    {.type = TSDB_DATA_TYPE_BIGINT,
     .bytes = 8,
     .isVarLen = 0,
     .startFn = tCompIntStart,
     .cmprFn = tCompInt,
     .endFn = tCompIntEnd,
     .getI64 = tGetI64OfI64,
     .putI64 = tPutI64OfI64},
    {.type = TSDB_DATA_TYPE_FLOAT,
     .bytes = 4,
     .isVarLen = 0,
     .startFn = tCompFloatStart,
     .cmprFn = tCompFloat,
     .endFn = tCompFloatEnd,
     .getI64 = NULL,
     .putI64 = NULL},
    {.type = TSDB_DATA_TYPE_DOUBLE,
     .bytes = 8,
     .isVarLen = 0,
     .startFn = tCompDoubleStart,
     .cmprFn = tCompDouble,
     .endFn = tCompDoubleEnd,
     .getI64 = NULL,
     .putI64 = NULL},
    {.type = TSDB_DATA_TYPE_VARCHAR,
     .bytes = 1,
     .isVarLen = 1,
     .startFn = tCompBinaryStart,
     .cmprFn = tCompBinary,
     .endFn = tCompBinaryEnd,
     .getI64 = NULL,
     .putI64 = NULL},
    {.type = TSDB_DATA_TYPE_TIMESTAMP,
     .bytes = 8,
     .isVarLen = 0,
     .startFn = tCompTimestampStart,
     .cmprFn = tCompTimestamp,
     .endFn = tCompTimestampEnd,
     .getI64 = NULL,
     .putI64 = NULL},
    {.type = TSDB_DATA_TYPE_NCHAR,
     .bytes = 1,
     .isVarLen = 1,
     .startFn = tCompBinaryStart,
     .cmprFn = tCompBinary,
     .endFn = tCompBinaryEnd,
     .getI64 = NULL,
     .putI64 = NULL},
    {.type = TSDB_DATA_TYPE_UTINYINT,
     .bytes = 1,
     .isVarLen = 0,
     .startFn = tCompIntStart,
     .cmprFn = tCompInt,
     .endFn = tCompIntEnd,
     .getI64 = tGetI64OfI8,
     .putI64 = tPutI64OfI8},
    {.type = TSDB_DATA_TYPE_USMALLINT,
     .bytes = 2,
     .isVarLen = 0,
     .startFn = tCompIntStart,
     .cmprFn = tCompInt,
     .endFn = tCompIntEnd,
     .getI64 = tGetI64OfI16,
     .putI64 = tPutI64OfI16},
    {.type = TSDB_DATA_TYPE_UINT,
     .bytes = 4,
     .isVarLen = 0,
     .startFn = tCompIntStart,
     .cmprFn = tCompInt,
     .endFn = tCompIntEnd,
     .getI64 = tGetI64OfI32,
     .putI64 = tPutI64OfI32},
    {.type = TSDB_DATA_TYPE_UBIGINT,
     .bytes = 8,
     .isVarLen = 0,
     .startFn = tCompIntStart,
     .cmprFn = tCompInt,
     .endFn = tCompIntEnd,
     .getI64 = tGetI64OfI64,
     .putI64 = tPutI64OfI64},
    {.type = TSDB_DATA_TYPE_JSON,
     .bytes = 1,
     .isVarLen = 1,
     .startFn = tCompBinaryStart,
     .cmprFn = tCompBinary,
     .endFn = tCompBinaryEnd,
     .getI64 = NULL,
     .putI64 = NULL},
    {.type = TSDB_DATA_TYPE_VARBINARY,
     .bytes = 1,
     .isVarLen = 1,
     .startFn = tCompBinaryStart,
     .cmprFn = tCompBinary,
     .endFn = tCompBinaryEnd,
     .getI64 = NULL,
     .putI64 = NULL},
    {.type = TSDB_DATA_TYPE_DECIMAL,
     .bytes = 1,
     .isVarLen = 1,
     .startFn = tCompBinaryStart,
     .cmprFn = tCompBinary,
     .endFn = tCompBinaryEnd,
     .getI64 = NULL,
     .putI64 = NULL},
    {.type = TSDB_DATA_TYPE_BLOB,
     .bytes = 1,
     .isVarLen = 1,
     .startFn = tCompBinaryStart,
     .cmprFn = tCompBinary,
     .endFn = tCompBinaryEnd,
     .getI64 = NULL,
     .putI64 = NULL},
    {.type = TSDB_DATA_TYPE_MEDIUMBLOB,
     .bytes = 1,
     .isVarLen = 1,
     .startFn = tCompBinaryStart,
     .cmprFn = tCompBinary,
     .endFn = tCompBinaryEnd,
     .getI64 = NULL,
     .putI64 = NULL},
    {.type = TSDB_DATA_TYPE_GEOMETRY,
     .bytes = 1,
     .isVarLen = 1,
     .startFn = tCompBinaryStart,
     .cmprFn = tCompBinary,
     .endFn = tCompBinaryEnd,
     .getI64 = NULL,
     .putI64 = NULL},
};

struct SCompressor {
  int8_t   type;
  int8_t   cmprAlg;
  int8_t   autoAlloc;
  int32_t  nVal;
  uint8_t *pBuf;
  int32_t  nBuf;
  uint8_t *aBuf[1];
  union {
    // Timestamp ----
    struct {
      int64_t  ts_prev_val;
      int64_t  ts_prev_delta;
      uint8_t *ts_flag_p;
    };
    // Integer ----
    struct {
      int64_t  i_prev;
      int32_t  i_selector;
      int32_t  i_start;
      int32_t  i_end;
      int32_t  i_nEle;
      uint64_t i_aZigzag[241];
      int8_t   i_aBitN[241];
    };
    // Float ----
    struct {
      uint32_t f_prev;
      uint8_t *f_flag_p;
    };
    // Double ----
    struct {
      uint64_t d_prev;
      uint8_t *d_flag_p;
    };
  };
};

static int32_t tTwoStageComp(SCompressor *pCmprsor, int32_t *szComp) {
  int32_t code = 0;

  if (pCmprsor->autoAlloc && (code = tRealloc(&pCmprsor->aBuf[0], pCmprsor->nBuf + 1))) {
    return code;
  }

  *szComp = LZ4_compress_default(pCmprsor->pBuf, pCmprsor->aBuf[0] + 1, pCmprsor->nBuf, pCmprsor->nBuf);
  if (*szComp && *szComp < pCmprsor->nBuf) {
    pCmprsor->aBuf[0][0] = 1;
    *szComp += 1;
  } else {
    pCmprsor->aBuf[0][0] = 0;
    memcpy(pCmprsor->aBuf[0] + 1, pCmprsor->pBuf, pCmprsor->nBuf);
    *szComp = pCmprsor->nBuf + 1;
  }

  return code;
}

// Timestamp =====================================================
static int32_t tCompTimestampStart(SCompressor *pCmprsor, int8_t type, int8_t cmprAlg) {
  int32_t code = 0;

  pCmprsor->nBuf = 1;

  code = tRealloc(&pCmprsor->pBuf, pCmprsor->nBuf);
  if (code) return code;

  pCmprsor->pBuf[0] = 1;

  return code;
}

static int32_t tCompTSSwitchToCopy(SCompressor *pCmprsor) {
  int32_t code = 0;

  if (pCmprsor->nVal == 0) goto _exit;

  if (pCmprsor->autoAlloc && (code = tRealloc(&pCmprsor->aBuf[0], sizeof(int64_t) * pCmprsor->nVal + 1))) {
    return code;
  }

  int32_t n = 1;
  int32_t nBuf = 1;
  int64_t value;
  int64_t delta;
  for (int32_t iVal = 0; iVal < pCmprsor->nVal;) {
    uint8_t aN[2] = {(pCmprsor->pBuf[n] & 0xf), (pCmprsor->pBuf[n] >> 4)};

    n++;

    for (int32_t i = 0; i < 2; i++) {
      uint64_t vZigzag = 0;
      for (uint8_t j = 0; j < aN[i]; j++) {
        vZigzag |= (((uint64_t)pCmprsor->pBuf[n]) << (8 * j));
        n++;
      }

      int64_t delta_of_delta = ZIGZAG_DECODE(int64_t, vZigzag);
      if (iVal) {
        delta = delta_of_delta + delta;
        value = delta + value;
      } else {
        delta = 0;
        value = delta_of_delta;
      }

      memcpy(pCmprsor->aBuf[0] + nBuf, &value, sizeof(value));
      nBuf += sizeof(int64_t);

      iVal++;
      if (iVal >= pCmprsor->nVal) break;
    }
  }

  ASSERT(n == pCmprsor->nBuf && nBuf == sizeof(int64_t) * pCmprsor->nVal + 1);

  uint8_t *pBuf = pCmprsor->pBuf;
  pCmprsor->pBuf = pCmprsor->aBuf[0];
  pCmprsor->aBuf[0] = pBuf;
  pCmprsor->nBuf = nBuf;

_exit:
  pCmprsor->pBuf[0] = 0;
  return code;
}
static int32_t tCompTimestamp(SCompressor *pCmprsor, const void *pData, int32_t nData) {
  int32_t code = 0;

  int64_t ts = *(int64_t *)pData;
  ASSERT(nData == 8);

  if (pCmprsor->pBuf[0] == 1) {
    if (pCmprsor->nVal == 0) {
      pCmprsor->ts_prev_val = ts;
      pCmprsor->ts_prev_delta = -ts;
    }

    if (!I64_SAFE_ADD(ts, -pCmprsor->ts_prev_val)) {
      code = tCompTSSwitchToCopy(pCmprsor);
      if (code) return code;
      goto _copy_cmpr;
    }
    int64_t delta = ts - pCmprsor->ts_prev_val;

    if (!I64_SAFE_ADD(delta, -pCmprsor->ts_prev_delta)) {
      code = tCompTSSwitchToCopy(pCmprsor);
      if (code) return code;
      goto _copy_cmpr;
    }
    int64_t  delta_of_delta = delta - pCmprsor->ts_prev_delta;
    uint64_t vZigzag = ZIGZAG_ENCODE(int64_t, delta_of_delta);

    pCmprsor->ts_prev_val = ts;
    pCmprsor->ts_prev_delta = delta;

    if ((pCmprsor->nVal & 0x1) == 0) {
      if (pCmprsor->autoAlloc && (code = tRealloc(&pCmprsor->pBuf, pCmprsor->nBuf + 17))) {
        return code;
      }

      pCmprsor->ts_flag_p = pCmprsor->pBuf + pCmprsor->nBuf;
      pCmprsor->nBuf++;
      pCmprsor->ts_flag_p[0] = 0;
      while (vZigzag) {
        pCmprsor->pBuf[pCmprsor->nBuf] = (vZigzag & 0xff);
        pCmprsor->nBuf++;
        pCmprsor->ts_flag_p[0]++;
        vZigzag >>= 8;
      }
    } else {
      while (vZigzag) {
        pCmprsor->pBuf[pCmprsor->nBuf] = (vZigzag & 0xff);
        pCmprsor->nBuf++;
        pCmprsor->ts_flag_p[0] += 0x10;
        vZigzag >>= 8;
      }
    }
  } else {
  _copy_cmpr:
    if (pCmprsor->autoAlloc && (code = tRealloc(&pCmprsor->pBuf, pCmprsor->nBuf + sizeof(ts)))) {
      return code;
    }

    memcpy(pCmprsor->pBuf + pCmprsor->nBuf, &ts, sizeof(ts));
    pCmprsor->nBuf += sizeof(ts);
  }
  pCmprsor->nVal++;

  return code;
}

static int32_t tCompTimestampEnd(SCompressor *pCmprsor, const uint8_t **ppData, int32_t *nData) {
  int32_t code = 0;

  if (pCmprsor->nBuf >= sizeof(int64_t) * pCmprsor->nVal + 1 && pCmprsor->pBuf[0] == 1) {
    code = tCompTSSwitchToCopy(pCmprsor);
    if (code) return code;
  }

  if (pCmprsor->cmprAlg == TWO_STAGE_COMP) {
    code = tTwoStageComp(pCmprsor, nData);
    if (code) return code;
    *ppData = pCmprsor->aBuf[0];
  } else if (pCmprsor->cmprAlg == ONE_STAGE_COMP) {
    *ppData = pCmprsor->pBuf;
    *nData = pCmprsor->nBuf;
  } else {
    ASSERT(0);
  }

  return code;
}

// Integer =====================================================
#define SIMPLE8B_MAX ((uint64_t)1152921504606846974LL)
static const uint8_t BIT_PER_INTEGER[] = {0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60};
static const int32_t SELECTOR_TO_ELEMS[] = {240, 120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1};
static const uint8_t BIT_TO_SELECTOR[] = {0,  2,  3,  4,  5,  6,  7,  8,  9,  10, 10, 11, 11, 12, 12, 12,
                                          13, 13, 13, 13, 13, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 15,
                                          15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
                                          15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15};
static const int32_t NEXT_IDX[] = {
    1,   2,   3,   4,   5,   6,   7,   8,   9,   10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,
    23,  24,  25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,
    45,  46,  47,  48,  49,  50,  51,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  62,  63,  64,  65,  66,
    67,  68,  69,  70,  71,  72,  73,  74,  75,  76,  77,  78,  79,  80,  81,  82,  83,  84,  85,  86,  87,  88,
    89,  90,  91,  92,  93,  94,  95,  96,  97,  98,  99,  100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110,
    111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132,
    133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154,
    155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176,
    177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198,
    199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220,
    221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 0};

static int32_t tCompIntStart(SCompressor *pCmprsor, int8_t type, int8_t cmprAlg) {
  int32_t code = 0;

  pCmprsor->i_prev = 0;
  pCmprsor->i_selector = 0;
  pCmprsor->i_start = 0;
  pCmprsor->i_end = 0;
  pCmprsor->i_nEle = 0;
  pCmprsor->nBuf = 1;

  code = tRealloc(&pCmprsor->pBuf, pCmprsor->nBuf);
  if (code) return code;

  pCmprsor->pBuf[0] = 0;

  return code;
}

static int32_t tCompIntSwitchToCopy(SCompressor *pCmprsor) {
  int32_t code = 0;

  if (pCmprsor->nVal == 0) goto _exit;

  int32_t size = DATA_TYPE_INFO[pCmprsor->type].bytes * pCmprsor->nVal + 1;
  if (pCmprsor->autoAlloc && (code = tRealloc(&pCmprsor->aBuf[0], size))) {
    return code;
  }

  int32_t n = 1;
  int32_t nBuf = 1;
  int64_t vPrev = 0;
  while (n < pCmprsor->nBuf) {
    uint64_t b;
    memcpy(&b, pCmprsor->pBuf + n, sizeof(b));
    n += sizeof(b);

    int32_t  i_selector = (b & 0xf);
    int32_t  nEle = SELECTOR_TO_ELEMS[i_selector];
    uint8_t  bits = BIT_PER_INTEGER[i_selector];
    uint64_t mask = (((uint64_t)1) << bits) - 1;
    for (int32_t iEle = 0; iEle < nEle; iEle++) {
      uint64_t vZigzag = (b >> (bits * iEle + 4)) & mask;
      vPrev = ZIGZAG_DECODE(int64_t, vZigzag) + vPrev;

      DATA_TYPE_INFO[pCmprsor->type].putI64(vPrev, pCmprsor->aBuf[0] + nBuf);
      nBuf += DATA_TYPE_INFO[pCmprsor->type].bytes;
    }
  }

  while (pCmprsor->i_nEle) {
    vPrev = ZIGZAG_DECODE(int64_t, pCmprsor->i_aZigzag[pCmprsor->i_start]) + vPrev;

    memcpy(pCmprsor->aBuf[0] + nBuf, &vPrev, DATA_TYPE_INFO[pCmprsor->type].bytes);
    nBuf += DATA_TYPE_INFO[pCmprsor->type].bytes;

    pCmprsor->i_start = NEXT_IDX[pCmprsor->i_start];
    pCmprsor->i_nEle--;
  }

  ASSERT(n == pCmprsor->nBuf && nBuf == size);

  uint8_t *pBuf = pCmprsor->pBuf;
  pCmprsor->pBuf = pCmprsor->aBuf[0];
  pCmprsor->aBuf[0] = pBuf;
  pCmprsor->nBuf = size;

_exit:
  pCmprsor->pBuf[0] = 1;
  return code;
}

static int32_t tCompInt(SCompressor *pCmprsor, const void *pData, int32_t nData) {
  int32_t code = 0;

  ASSERT(nData == DATA_TYPE_INFO[pCmprsor->type].bytes);

  if (pCmprsor->pBuf[0] == 0) {
    int64_t val = DATA_TYPE_INFO[pCmprsor->type].getI64(pData);

    if (!I64_SAFE_ADD(val, -pCmprsor->i_prev)) {
      code = tCompIntSwitchToCopy(pCmprsor);
      if (code) return code;
      goto _copy_cmpr;
    }

    int64_t  diff = val - pCmprsor->i_prev;
    uint64_t vZigzag = ZIGZAG_ENCODE(int64_t, diff);
    if (vZigzag >= SIMPLE8B_MAX) {
      code = tCompIntSwitchToCopy(pCmprsor);
      if (code) return code;
      goto _copy_cmpr;
    }

    int8_t nBit = (vZigzag) ? (64 - BUILDIN_CLZL(vZigzag)) : 0;
    pCmprsor->i_prev = val;

    for (;;) {
      if (pCmprsor->i_nEle + 1 <= SELECTOR_TO_ELEMS[pCmprsor->i_selector] &&
          pCmprsor->i_nEle + 1 <= SELECTOR_TO_ELEMS[BIT_TO_SELECTOR[nBit]]) {
        if (pCmprsor->i_selector < BIT_TO_SELECTOR[nBit]) {
          pCmprsor->i_selector = BIT_TO_SELECTOR[nBit];
        }
        pCmprsor->i_aZigzag[pCmprsor->i_end] = vZigzag;
        pCmprsor->i_aBitN[pCmprsor->i_end] = nBit;
        pCmprsor->i_end = NEXT_IDX[pCmprsor->i_end];
        pCmprsor->i_nEle++;
        break;
      } else {
        if (pCmprsor->i_nEle < SELECTOR_TO_ELEMS[pCmprsor->i_selector]) {
          int32_t lidx = pCmprsor->i_selector + 1;
          int32_t ridx = 15;
          while (lidx <= ridx) {
            pCmprsor->i_selector = (lidx + ridx) >> 1;

            if (pCmprsor->i_nEle < SELECTOR_TO_ELEMS[pCmprsor->i_selector]) {
              lidx = pCmprsor->i_selector + 1;
            } else if (pCmprsor->i_nEle > SELECTOR_TO_ELEMS[pCmprsor->i_selector]) {
              ridx = pCmprsor->i_selector - 1;
            } else {
              break;
            }
          }

          if (pCmprsor->i_nEle < SELECTOR_TO_ELEMS[pCmprsor->i_selector]) pCmprsor->i_selector++;
        }
        int32_t nEle = SELECTOR_TO_ELEMS[pCmprsor->i_selector];

        if (pCmprsor->autoAlloc && (code = tRealloc(&pCmprsor->pBuf, pCmprsor->nBuf + sizeof(uint64_t)))) {
          return code;
        }

        uint64_t *bp = (uint64_t *)(pCmprsor->pBuf + pCmprsor->nBuf);
        pCmprsor->nBuf += sizeof(uint64_t);
        bp[0] = pCmprsor->i_selector;
        uint8_t bits = BIT_PER_INTEGER[pCmprsor->i_selector];
        for (int32_t iVal = 0; iVal < nEle; iVal++) {
          bp[0] |= (pCmprsor->i_aZigzag[pCmprsor->i_start] << (bits * iVal + 4));
          pCmprsor->i_start = NEXT_IDX[pCmprsor->i_start];
          pCmprsor->i_nEle--;
        }

        // reset and continue
        pCmprsor->i_selector = 0;
        for (int32_t iVal = pCmprsor->i_start; iVal < pCmprsor->i_end; iVal = NEXT_IDX[iVal]) {
          if (pCmprsor->i_selector < BIT_TO_SELECTOR[pCmprsor->i_aBitN[iVal]]) {
            pCmprsor->i_selector = BIT_TO_SELECTOR[pCmprsor->i_aBitN[iVal]];
          }
        }
      }
    }
  } else {
  _copy_cmpr:
    code = tRealloc(&pCmprsor->pBuf, pCmprsor->nBuf + nData);
    if (code) return code;

    memcpy(pCmprsor->pBuf + pCmprsor->nBuf, pData, nData);
    pCmprsor->nBuf += nData;
  }
  pCmprsor->nVal++;

  return code;
}

static int32_t tCompIntEnd(SCompressor *pCmprsor, const uint8_t **ppData, int32_t *nData) {
  int32_t code = 0;

  for (; pCmprsor->i_nEle;) {
    if (pCmprsor->i_nEle < SELECTOR_TO_ELEMS[pCmprsor->i_selector]) {
      int32_t lidx = pCmprsor->i_selector + 1;
      int32_t ridx = 15;
      while (lidx <= ridx) {
        pCmprsor->i_selector = (lidx + ridx) >> 1;

        if (pCmprsor->i_nEle < SELECTOR_TO_ELEMS[pCmprsor->i_selector]) {
          lidx = pCmprsor->i_selector + 1;
        } else if (pCmprsor->i_nEle > SELECTOR_TO_ELEMS[pCmprsor->i_selector]) {
          ridx = pCmprsor->i_selector - 1;
        } else {
          break;
        }
      }

      if (pCmprsor->i_nEle < SELECTOR_TO_ELEMS[pCmprsor->i_selector]) pCmprsor->i_selector++;
    }
    int32_t nEle = SELECTOR_TO_ELEMS[pCmprsor->i_selector];

    if (pCmprsor->autoAlloc && (code = tRealloc(&pCmprsor->pBuf, pCmprsor->nBuf + sizeof(uint64_t)))) {
      return code;
    }

    uint64_t *bp = (uint64_t *)(pCmprsor->pBuf + pCmprsor->nBuf);
    pCmprsor->nBuf += sizeof(uint64_t);
    bp[0] = pCmprsor->i_selector;
    uint8_t bits = BIT_PER_INTEGER[pCmprsor->i_selector];
    for (int32_t iVal = 0; iVal < nEle; iVal++) {
      bp[0] |= (pCmprsor->i_aZigzag[pCmprsor->i_start] << (bits * iVal + 4));
      pCmprsor->i_start = NEXT_IDX[pCmprsor->i_start];
      pCmprsor->i_nEle--;
    }

    pCmprsor->i_selector = 0;
  }

  if (pCmprsor->nBuf >= DATA_TYPE_INFO[pCmprsor->type].bytes * pCmprsor->nVal + 1 && pCmprsor->pBuf[0] == 0) {
    code = tCompIntSwitchToCopy(pCmprsor);
    if (code) return code;
  }

  if (pCmprsor->cmprAlg == TWO_STAGE_COMP) {
    code = tTwoStageComp(pCmprsor, nData);
    if (code) return code;
    *ppData = pCmprsor->aBuf[0];
  } else if (pCmprsor->cmprAlg == ONE_STAGE_COMP) {
    *ppData = pCmprsor->pBuf;
    *nData = pCmprsor->nBuf;
  } else {
    ASSERT(0);
  }

  return code;
}

// Float =====================================================
static int32_t tCompFloatStart(SCompressor *pCmprsor, int8_t type, int8_t cmprAlg) {
  int32_t code = 0;

  pCmprsor->f_prev = 0;
  pCmprsor->f_flag_p = NULL;

  pCmprsor->nBuf = 1;

  code = tRealloc(&pCmprsor->pBuf, pCmprsor->nBuf);
  if (code) return code;

  pCmprsor->pBuf[0] = 0;

  return code;
}

static int32_t tCompFloatSwitchToCopy(SCompressor *pCmprsor) {
  int32_t code = 0;

  if (pCmprsor->nVal == 0) goto _exit;

  if (pCmprsor->autoAlloc && (code = tRealloc(&pCmprsor->aBuf[0], sizeof(float) * pCmprsor->nVal + 1))) {
    return code;
  }

  int32_t n = 1;
  int32_t nBuf = 1;
  union {
    float    f;
    uint32_t u;
  } val = {.u = 0};

  for (int32_t iVal = 0; iVal < pCmprsor->nVal;) {
    uint8_t flags[2] = {(pCmprsor->pBuf[n] & 0xf), (pCmprsor->pBuf[n] >> 4)};

    n++;

    for (int8_t i = 0; i < 2; i++) {
      uint8_t flag = flags[i];

      uint32_t diff = 0;
      int8_t   nBytes = (flag & 0x7) + 1;
      for (int j = 0; j < nBytes; j++) {
        diff |= (((uint32_t)pCmprsor->pBuf[n]) << (8 * j));
        n++;
      }

      if (flag & 0x8) {
        diff <<= (32 - nBytes * 8);
      }

      val.u ^= diff;

      memcpy(pCmprsor->aBuf[0] + nBuf, &val.f, sizeof(val));
      nBuf += sizeof(val);

      iVal++;
      if (iVal >= pCmprsor->nVal) break;
    }
  }
  uint8_t *pBuf = pCmprsor->pBuf;
  pCmprsor->pBuf = pCmprsor->aBuf[0];
  pCmprsor->aBuf[0] = pBuf;
  pCmprsor->nBuf = nBuf;

_exit:
  pCmprsor->pBuf[0] = 1;
  return code;
}
static int32_t tCompFloat(SCompressor *pCmprsor, const void *pData, int32_t nData) {
  int32_t code = 0;

  ASSERT(nData == sizeof(float));

  union {
    float    f;
    uint32_t u;
  } val = {.f = *(float *)pData};

  uint32_t diff = val.u ^ pCmprsor->f_prev;
  pCmprsor->f_prev = val.u;

  int32_t clz, ctz;
  if (diff) {
    clz = BUILDIN_CLZ(diff);
    ctz = BUILDIN_CTZ(diff);
  } else {
    clz = 32;
    ctz = 32;
  }

  uint8_t nBytes;
  if (clz < ctz) {
    nBytes = sizeof(uint32_t) - ctz / BITS_PER_BYTE;
    if (nBytes) diff >>= (32 - nBytes * BITS_PER_BYTE);
  } else {
    nBytes = sizeof(uint32_t) - clz / BITS_PER_BYTE;
  }
  if (nBytes == 0) nBytes++;

  if ((pCmprsor->nVal & 0x1) == 0) {
    if (pCmprsor->autoAlloc && (code = tRealloc(&pCmprsor->pBuf, pCmprsor->nBuf + 9))) {
      return code;
    }

    pCmprsor->f_flag_p = &pCmprsor->pBuf[pCmprsor->nBuf];
    pCmprsor->nBuf++;

    if (clz < ctz) {
      pCmprsor->f_flag_p[0] = (0x08 | (nBytes - 1));
    } else {
      pCmprsor->f_flag_p[0] = nBytes - 1;
    }
  } else {
    if (clz < ctz) {
      pCmprsor->f_flag_p[0] |= ((0x08 | (nBytes - 1)) << 4);
    } else {
      pCmprsor->f_flag_p[0] |= ((nBytes - 1) << 4);
    }
  }
  for (; nBytes; nBytes--) {
    pCmprsor->pBuf[pCmprsor->nBuf] = (diff & 0xff);
    pCmprsor->nBuf++;
    diff >>= BITS_PER_BYTE;
  }
  pCmprsor->nVal++;

  return code;
}

static int32_t tCompFloatEnd(SCompressor *pCmprsor, const uint8_t **ppData, int32_t *nData) {
  int32_t code = 0;

  if (pCmprsor->nBuf >= sizeof(float) * pCmprsor->nVal + 1) {
    code = tCompFloatSwitchToCopy(pCmprsor);
    if (code) return code;
  }

  if (pCmprsor->cmprAlg == TWO_STAGE_COMP) {
    code = tTwoStageComp(pCmprsor, nData);
    if (code) return code;
    *ppData = pCmprsor->aBuf[0];
  } else if (pCmprsor->cmprAlg == ONE_STAGE_COMP) {
    *ppData = pCmprsor->pBuf;
    *nData = pCmprsor->nBuf;
  } else {
    ASSERT(0);
  }

  return code;
}

// Double =====================================================
static int32_t tCompDoubleStart(SCompressor *pCmprsor, int8_t type, int8_t cmprAlg) {
  int32_t code = 0;

  pCmprsor->d_prev = 0;
  pCmprsor->d_flag_p = NULL;

  pCmprsor->nBuf = 1;

  code = tRealloc(&pCmprsor->pBuf, pCmprsor->nBuf);
  if (code) return code;

  pCmprsor->pBuf[0] = 0;

  return code;
}

static int32_t tCompDoubleSwitchToCopy(SCompressor *pCmprsor) {
  int32_t code = 0;

  if (pCmprsor->nVal == 0) goto _exit;

  if (pCmprsor->autoAlloc && (code = tRealloc(&pCmprsor->aBuf[0], sizeof(double) * pCmprsor->nVal + 1))) {
    return code;
  }

  int32_t n = 1;
  int32_t nBuf = 1;
  union {
    double   f;
    uint64_t u;
  } val = {.u = 0};

  for (int32_t iVal = 0; iVal < pCmprsor->nVal;) {
    uint8_t flags[2] = {(pCmprsor->pBuf[n] & 0xf), (pCmprsor->pBuf[n] >> 4)};

    n++;

    for (int8_t i = 0; i < 2; i++) {
      uint8_t flag = flags[i];

      uint64_t diff = 0;
      int8_t   nBytes = (flag & 0x7) + 1;
      for (int j = 0; j < nBytes; j++) {
        diff |= (((uint64_t)pCmprsor->pBuf[n]) << (8 * j));
        n++;
      }

      if (flag & 0x8) {
        diff <<= (64 - nBytes * 8);
      }

      val.u ^= diff;

      memcpy(pCmprsor->aBuf[0] + nBuf, &val.f, sizeof(val));
      nBuf += sizeof(val);

      iVal++;
      if (iVal >= pCmprsor->nVal) break;
    }
  }
  uint8_t *pBuf = pCmprsor->pBuf;
  pCmprsor->pBuf = pCmprsor->aBuf[0];
  pCmprsor->aBuf[0] = pBuf;
  pCmprsor->nBuf = nBuf;

_exit:
  pCmprsor->pBuf[0] = 1;
  return code;
}
static int32_t tCompDouble(SCompressor *pCmprsor, const void *pData, int32_t nData) {
  int32_t code = 0;

  ASSERT(nData == sizeof(double));

  union {
    double   d;
    uint64_t u;
  } val = {.d = *(double *)pData};

  uint64_t diff = val.u ^ pCmprsor->d_prev;
  pCmprsor->d_prev = val.u;

  int32_t clz, ctz;
  if (diff) {
    clz = BUILDIN_CLZL(diff);
    ctz = BUILDIN_CTZL(diff);
  } else {
    clz = 64;
    ctz = 64;
  }

  uint8_t nBytes;
  if (clz < ctz) {
    nBytes = sizeof(uint64_t) - ctz / BITS_PER_BYTE;
    if (nBytes) diff >>= (64 - nBytes * BITS_PER_BYTE);
  } else {
    nBytes = sizeof(uint64_t) - clz / BITS_PER_BYTE;
  }
  if (nBytes == 0) nBytes++;

  if ((pCmprsor->nVal & 0x1) == 0) {
    if (pCmprsor->autoAlloc && (code = tRealloc(&pCmprsor->pBuf, pCmprsor->nBuf + 17))) {
      return code;
    }

    pCmprsor->d_flag_p = &pCmprsor->pBuf[pCmprsor->nBuf];
    pCmprsor->nBuf++;

    if (clz < ctz) {
      pCmprsor->d_flag_p[0] = (0x08 | (nBytes - 1));
    } else {
      pCmprsor->d_flag_p[0] = nBytes - 1;
    }
  } else {
    if (clz < ctz) {
      pCmprsor->d_flag_p[0] |= ((0x08 | (nBytes - 1)) << 4);
    } else {
      pCmprsor->d_flag_p[0] |= ((nBytes - 1) << 4);
    }
  }
  for (; nBytes; nBytes--) {
    pCmprsor->pBuf[pCmprsor->nBuf] = (diff & 0xff);
    pCmprsor->nBuf++;
    diff >>= BITS_PER_BYTE;
  }
  pCmprsor->nVal++;

  return code;
}

static int32_t tCompDoubleEnd(SCompressor *pCmprsor, const uint8_t **ppData, int32_t *nData) {
  int32_t code = 0;

  if (pCmprsor->nBuf >= sizeof(double) * pCmprsor->nVal + 1) {
    code = tCompDoubleSwitchToCopy(pCmprsor);
    if (code) return code;
  }

  if (pCmprsor->cmprAlg == TWO_STAGE_COMP) {
    code = tTwoStageComp(pCmprsor, nData);
    if (code) return code;
    *ppData = pCmprsor->aBuf[0];
  } else if (pCmprsor->cmprAlg == ONE_STAGE_COMP) {
    *ppData = pCmprsor->pBuf;
    *nData = pCmprsor->nBuf;
  } else {
    ASSERT(0);
  }

  return code;
}

// Binary =====================================================
static int32_t tCompBinaryStart(SCompressor *pCmprsor, int8_t type, int8_t cmprAlg) {
  pCmprsor->nBuf = 1;
  return 0;
}

static int32_t tCompBinary(SCompressor *pCmprsor, const void *pData, int32_t nData) {
  int32_t code = 0;

  if (nData) {
    if (pCmprsor->autoAlloc && (code = tRealloc(&pCmprsor->pBuf, pCmprsor->nBuf + nData))) {
      return code;
    }

    memcpy(pCmprsor->pBuf + pCmprsor->nBuf, pData, nData);
    pCmprsor->nBuf += nData;
  }
  pCmprsor->nVal++;

  return code;
}

static int32_t tCompBinaryEnd(SCompressor *pCmprsor, const uint8_t **ppData, int32_t *nData) {
  int32_t code = 0;

  if (pCmprsor->nBuf == 1) return code;

  if (pCmprsor->autoAlloc && (code = tRealloc(&pCmprsor->aBuf[0], pCmprsor->nBuf))) {
    return code;
  }

  int32_t szComp =
      LZ4_compress_default(pCmprsor->pBuf + 1, pCmprsor->aBuf[0] + 1, pCmprsor->nBuf - 1, pCmprsor->nBuf - 1);
  if (szComp && szComp < pCmprsor->nBuf - 1) {
    pCmprsor->aBuf[0][0] = 1;
    *ppData = pCmprsor->aBuf[0];
    *nData = szComp + 1;
  } else {
    pCmprsor->pBuf[0] = 0;
    *ppData = pCmprsor->pBuf;
    *nData = pCmprsor->nBuf;
  }

  return code;
}

// Bool =====================================================
static const uint8_t BOOL_CMPR_TABLE[] = {0b01, 0b0100, 0b010000, 0b01000000};

static int32_t tCompBoolStart(SCompressor *pCmprsor, int8_t type, int8_t cmprAlg) {
  pCmprsor->nBuf = 0;
  return 0;
}

static int32_t tCompBool(SCompressor *pCmprsor, const void *pData, int32_t nData) {
  int32_t code = 0;

  bool vBool = *(int8_t *)pData;

  int32_t mod4 = (pCmprsor->nVal & 3);
  if (mod4 == 0) {
    pCmprsor->nBuf++;

    if (pCmprsor->autoAlloc && (code = tRealloc(&pCmprsor->pBuf, pCmprsor->nBuf))) {
      return code;
    }

    pCmprsor->pBuf[pCmprsor->nBuf - 1] = 0;
  }
  if (vBool) {
    pCmprsor->pBuf[pCmprsor->nBuf - 1] |= BOOL_CMPR_TABLE[mod4];
  }
  pCmprsor->nVal++;

  return code;
}

static int32_t tCompBoolEnd(SCompressor *pCmprsor, const uint8_t **ppData, int32_t *nData) {
  int32_t code = 0;

  if (pCmprsor->cmprAlg == TWO_STAGE_COMP) {
    code = tTwoStageComp(pCmprsor, nData);
    if (code) return code;
    *ppData = pCmprsor->aBuf[0];
  } else if (pCmprsor->cmprAlg == ONE_STAGE_COMP) {
    *ppData = pCmprsor->pBuf;
    *nData = pCmprsor->nBuf;
  } else {
    ASSERT(0);
  }

  return code;
}

// SCompressor =====================================================
int32_t tCompressorCreate(SCompressor **ppCmprsor) {
  int32_t code = 0;

  *ppCmprsor = (SCompressor *)taosMemoryCalloc(1, sizeof(SCompressor));
  if ((*ppCmprsor) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    return code;
  }

  return code;
}

int32_t tCompressorDestroy(SCompressor *pCmprsor) {
  int32_t code = 0;

  tFree(pCmprsor->pBuf);

  int32_t nBuf = sizeof(pCmprsor->aBuf) / sizeof(pCmprsor->aBuf[0]);
  for (int32_t iBuf = 0; iBuf < nBuf; iBuf++) {
    tFree(pCmprsor->aBuf[iBuf]);
  }

  taosMemoryFree(pCmprsor);

  return code;
}

int32_t tCompressStart(SCompressor *pCmprsor, int8_t type, int8_t cmprAlg) {
  int32_t code = 0;

  pCmprsor->type = type;
  pCmprsor->cmprAlg = cmprAlg;
  pCmprsor->autoAlloc = 1;
  pCmprsor->nVal = 0;

  if (DATA_TYPE_INFO[type].startFn) {
    DATA_TYPE_INFO[type].startFn(pCmprsor, type, cmprAlg);
  }

  return code;
}

int32_t tCompressEnd(SCompressor *pCmprsor, const uint8_t **ppOut, int32_t *nOut, int32_t *nOrigin) {
  int32_t code = 0;

  *ppOut = NULL;
  *nOut = 0;
  if (nOrigin) {
    if (DATA_TYPE_INFO[pCmprsor->type].isVarLen) {
      *nOrigin = pCmprsor->nBuf - 1;
    } else {
      *nOrigin = pCmprsor->nVal * DATA_TYPE_INFO[pCmprsor->type].bytes;
    }
  }

  if (pCmprsor->nVal == 0) return code;

  if (DATA_TYPE_INFO[pCmprsor->type].endFn) {
    return DATA_TYPE_INFO[pCmprsor->type].endFn(pCmprsor, ppOut, nOut);
  }

  return code;
}

int32_t tCompress(SCompressor *pCmprsor, const void *pData, int64_t nData) {
  return DATA_TYPE_INFO[pCmprsor->type].cmprFn(pCmprsor, pData, nData);
}
#endif
/*************************************************************************
 *                  REGULAR COMPRESSION
 *************************************************************************/
// Timestamp =====================================================
int32_t tsCompressTimestamp(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                            int32_t nBuf) {
  if (cmprAlg == ONE_STAGE_COMP) {
    return tsCompressTimestampImp(pIn, nEle, pOut);
  } else if (cmprAlg == TWO_STAGE_COMP) {
    int32_t len = tsCompressTimestampImp(pIn, nEle, pBuf);
    return tsCompressStringImp(pBuf, len, pOut, nOut);
  } else {
    ASSERTS(0, "compress algo not one or two stage");
    return -1;
  }
}

int32_t tsDecompressTimestamp(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg,
                              void *pBuf, int32_t nBuf) {
  if (cmprAlg == ONE_STAGE_COMP) {
    return tsDecompressTimestampImp(pIn, nEle, pOut);
  } else if (cmprAlg == TWO_STAGE_COMP) {
    if (tsDecompressStringImp(pIn, nIn, pBuf, nBuf) < 0) return -1;
    return tsDecompressTimestampImp(pBuf, nEle, pOut);
  } else {
    ASSERTS(0, "compress algo invalid");
    return -1;
  }
}

// Float =====================================================
int32_t tsCompressFloat(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                        int32_t nBuf) {
#ifdef TD_TSZ
  // lossy mode
  if (lossyFloat) {
    return tsCompressFloatLossyImp(pIn, nEle, pOut);
    // lossless mode
  } else {
#endif
    if (cmprAlg == ONE_STAGE_COMP) {
      return tsCompressFloatImp(pIn, nEle, pOut);
    } else if (cmprAlg == TWO_STAGE_COMP) {
      int32_t len = tsCompressFloatImp(pIn, nEle, pBuf);
      return tsCompressStringImp(pBuf, len, pOut, nOut);
    } else {
      ASSERTS(0, "compress algo invalid");
      return -1;
    }
#ifdef TD_TSZ
  }
#endif
}

int32_t tsDecompressFloat(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                          int32_t nBuf) {
#ifdef TD_TSZ
  if (HEAD_ALGO(((uint8_t *)pIn)[0]) == ALGO_SZ_LOSSY) {
    // decompress lossy
    return tsDecompressFloatLossyImp(pIn, nIn, nEle, pOut);
  } else {
#endif
    // decompress lossless
    if (cmprAlg == ONE_STAGE_COMP) {
      return tsDecompressFloatImp(pIn, nEle, pOut);
    } else if (cmprAlg == TWO_STAGE_COMP) {
      if (tsDecompressStringImp(pIn, nIn, pBuf, nBuf) < 0) return -1;
      return tsDecompressFloatImp(pBuf, nEle, pOut);
    } else {
      ASSERTS(0, "compress algo invalid");
      return -1;
    }
#ifdef TD_TSZ
  }
#endif
}

// Double =====================================================
int32_t tsCompressDouble(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                         int32_t nBuf) {
#ifdef TD_TSZ
  if (lossyDouble) {
    // lossy mode
    return tsCompressDoubleLossyImp(pIn, nEle, pOut);
  } else {
#endif
    // lossless mode
    if (cmprAlg == ONE_STAGE_COMP) {
      return tsCompressDoubleImp(pIn, nEle, pOut);
    } else if (cmprAlg == TWO_STAGE_COMP) {
      int32_t len = tsCompressDoubleImp(pIn, nEle, pBuf);
      return tsCompressStringImp(pBuf, len, pOut, nOut);
    } else {
      ASSERTS(0, "compress algo invalid");
      return -1;
    }
#ifdef TD_TSZ
  }
#endif
}

int32_t tsDecompressDouble(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                           int32_t nBuf) {
#ifdef TD_TSZ
  if (HEAD_ALGO(((uint8_t *)pIn)[0]) == ALGO_SZ_LOSSY) {
    // decompress lossy
    return tsDecompressDoubleLossyImp(pIn, nIn, nEle, pOut);
  } else {
#endif
    // decompress lossless
    if (cmprAlg == ONE_STAGE_COMP) {
      return tsDecompressDoubleImp(pIn, nEle, pOut);
    } else if (cmprAlg == TWO_STAGE_COMP) {
      if (tsDecompressStringImp(pIn, nIn, pBuf, nBuf) < 0) return -1;
      return tsDecompressDoubleImp(pBuf, nEle, pOut);
    } else {
      ASSERTS(0, "compress algo invalid");
      return -1;
    }
#ifdef TD_TSZ
  }
#endif
}

// Binary =====================================================
int32_t tsCompressString(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                         int32_t nBuf) {
  return tsCompressStringImp(pIn, nIn, pOut, nOut);
}

int32_t tsDecompressString(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                           int32_t nBuf) {
  return tsDecompressStringImp(pIn, nIn, pOut, nOut);
}

// Bool =====================================================
int32_t tsCompressBool(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                       int32_t nBuf) {
  if (cmprAlg == ONE_STAGE_COMP) {
    return tsCompressBoolImp(pIn, nEle, pOut);
  } else if (cmprAlg == TWO_STAGE_COMP) {
    int32_t len = tsCompressBoolImp(pIn, nEle, pBuf);
    if (len < 0) {
      return -1;
    }
    return tsCompressStringImp(pBuf, len, pOut, nOut);
  } else {
    ASSERTS(0, "compress algo invalid");
    return -1;
  }
}

int32_t tsDecompressBool(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                         int32_t nBuf) {
  if (cmprAlg == ONE_STAGE_COMP) {
    return tsDecompressBoolImp(pIn, nEle, pOut);
  } else if (cmprAlg == TWO_STAGE_COMP) {
    if (tsDecompressStringImp(pIn, nIn, pBuf, nBuf) < 0) return -1;
    return tsDecompressBoolImp(pBuf, nEle, pOut);
  } else {
    ASSERTS(0, "compress algo invalid");
    return -1;
  }
}

// Tinyint =====================================================
int32_t tsCompressTinyint(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                          int32_t nBuf) {
  if (cmprAlg == ONE_STAGE_COMP) {
    return tsCompressINTImp(pIn, nEle, pOut, TSDB_DATA_TYPE_TINYINT);
  } else if (cmprAlg == TWO_STAGE_COMP) {
    int32_t len = tsCompressINTImp(pIn, nEle, pBuf, TSDB_DATA_TYPE_TINYINT);
    return tsCompressStringImp(pBuf, len, pOut, nOut);
  } else {
    ASSERTS(0, "compress algo invalid");
    return -1;
  }
}

int32_t tsDecompressTinyint(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                            int32_t nBuf) {
  if (cmprAlg == ONE_STAGE_COMP) {
    return tsDecompressINTImp(pIn, nEle, pOut, TSDB_DATA_TYPE_TINYINT);
  } else if (cmprAlg == TWO_STAGE_COMP) {
    if (tsDecompressStringImp(pIn, nIn, pBuf, nBuf) < 0) return -1;
    return tsDecompressINTImp(pBuf, nEle, pOut, TSDB_DATA_TYPE_TINYINT);
  } else {
    ASSERTS(0, "compress algo invalid");
    return -1;
  }
}

// Smallint =====================================================
int32_t tsCompressSmallint(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                           int32_t nBuf) {
  if (cmprAlg == ONE_STAGE_COMP) {
    return tsCompressINTImp(pIn, nEle, pOut, TSDB_DATA_TYPE_SMALLINT);
  } else if (cmprAlg == TWO_STAGE_COMP) {
    int32_t len = tsCompressINTImp(pIn, nEle, pBuf, TSDB_DATA_TYPE_SMALLINT);
    return tsCompressStringImp(pBuf, len, pOut, nOut);
  } else {
    ASSERTS(0, "compress algo invalid");
    return -1;
  }
}

int32_t tsDecompressSmallint(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg,
                             void *pBuf, int32_t nBuf) {
  if (cmprAlg == ONE_STAGE_COMP) {
    return tsDecompressINTImp(pIn, nEle, pOut, TSDB_DATA_TYPE_SMALLINT);
  } else if (cmprAlg == TWO_STAGE_COMP) {
    if (tsDecompressStringImp(pIn, nIn, pBuf, nBuf) < 0) return -1;
    return tsDecompressINTImp(pBuf, nEle, pOut, TSDB_DATA_TYPE_SMALLINT);
  } else {
    ASSERTS(0, "compress algo invalid");
    return -1;
  }
}

// Int =====================================================
int32_t tsCompressInt(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                      int32_t nBuf) {
  if (cmprAlg == ONE_STAGE_COMP) {
    return tsCompressINTImp(pIn, nEle, pOut, TSDB_DATA_TYPE_INT);
  } else if (cmprAlg == TWO_STAGE_COMP) {
    int32_t len = tsCompressINTImp(pIn, nEle, pBuf, TSDB_DATA_TYPE_INT);
    return tsCompressStringImp(pBuf, len, pOut, nOut);
  } else {
    ASSERTS(0, "compress algo invalid");
    return -1;
  }
}

int32_t tsDecompressInt(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                        int32_t nBuf) {
  if (cmprAlg == ONE_STAGE_COMP) {
    return tsDecompressINTImp(pIn, nEle, pOut, TSDB_DATA_TYPE_INT);
  } else if (cmprAlg == TWO_STAGE_COMP) {
    if (tsDecompressStringImp(pIn, nIn, pBuf, nBuf) < 0) return -1;
    return tsDecompressINTImp(pBuf, nEle, pOut, TSDB_DATA_TYPE_INT);
  } else {
    ASSERTS(0, "compress algo invalid");
    return -1;
  }
}

// Bigint =====================================================
int32_t tsCompressBigint(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                         int32_t nBuf) {
  if (cmprAlg == ONE_STAGE_COMP) {
    return tsCompressINTImp(pIn, nEle, pOut, TSDB_DATA_TYPE_BIGINT);
  } else if (cmprAlg == TWO_STAGE_COMP) {
    int32_t len = tsCompressINTImp(pIn, nEle, pBuf, TSDB_DATA_TYPE_BIGINT);
    return tsCompressStringImp(pBuf, len, pOut, nOut);
  } else {
    ASSERTS(0, "compress algo invalid");
    return -1;
  }
}

int32_t tsDecompressBigint(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                           int32_t nBuf) {
  if (cmprAlg == ONE_STAGE_COMP) {
    return tsDecompressINTImp(pIn, nEle, pOut, TSDB_DATA_TYPE_BIGINT);
  } else if (cmprAlg == TWO_STAGE_COMP) {
    if (tsDecompressStringImp(pIn, nIn, pBuf, nBuf) < 0) return -1;
    return tsDecompressINTImp(pBuf, nEle, pOut, TSDB_DATA_TYPE_BIGINT);
  } else {
    ASSERTS(0, "compress algo invalid");
    return -1;
  }
}
