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
 *   To compress integers (including char, short, int, int64_t), the difference
 *   between two integers is calculated at first. Then the difference is
 *   transformed to positive by zig-zag encoding method
 *   (https://gist.github.com/mfuerstenau/ba870a29e16536fdbaba). Then the value
 * is
 *   encoded using simple 8B method. For more information about simple 8B,
 *   refer to https://en.wikipedia.org/wiki/8b/10b_encoding.
 *
 *   NOTE : For bigint, only 59 bits can be used, which means data from -(2**59)
 * to (2**59)-1
 *   are allowed.
 *
 * BOOLEAN Compression Algorithm:
 *   We provide two methods for compress boolean types. Because boolean types in
 * C
 *   code are char bytes with 0 and 1 values only, only one bit can used to
 * discrimenate
 *   the values.
 *   1. The first method is using only 1 bit to represent the boolean value with
 * 1 for
 *   true and 0 for false. Then the compression rate is 1/8.
 *   2. The second method is using run length encoding (RLE) methods. This
 * methos works
 *   better when there are a lot of consecutive true values or false values.
 *
 * STRING Compression Algorithm:
 *   We us LZ4 method to compress the string type.
 *
 * FLOAT Compression Algorithm:
 *   We use the same method with Akumuli to compress float and double types. The
 * compression
 *   algorithm assumes the float/double values change slightly. So we take the
 * XOR between two
 *   adjacent values. Then compare the number of leading zeros and trailing
 * zeros. If the number
 *   of leading zeros are larger than the trailing zeros, then record the last
 * serveral bytes
 *   of the XORed value with informations. If not, record the first
 * corresponding bytes.
 *
 */
#include <assert.h>
#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "lz4.h"
#include "tscompression.h"
#include "tsdb.h"
#include "ttypes.h"

const int TEST_NUMBER = 1;
#define is_bigendian() ((*(char *)&TEST_NUMBER) == 0)
#define SIMPLE8B_MAX_INT64 ((uint64_t)2305843009213693951L)

// Function declarations
int tsCompressINTImp(const char *const input, const int nelements, char *const output, const char type);
int tsDecompressINTImp(const char *const input, const int nelements, char *const output, const char type);
int tsCompressBoolImp(const char *const input, const int nelements, char *const output);
int tsDecompressBoolImp(const char *const input, const int nelements, char *const output);
int tsCompressStringImp(const char *const input, int inputSize, char *const output, int outputSize);
int tsDecompressStringImp(const char *const input, int compressedSize, char *const output, int outputSize);
int tsCompressTimestampImp(const char *const input, const int nelements, char *const output);
int tsDecompressTimestampImp(const char *const input, const int nelements, char *const output);
int tsCompressDoubleImp(const char *const input, const int nelements, char *const output);
int tsDecompressDoubleImp(const char *const input, const int nelements, char *const output);
int tsCompressFloatImp(const char *const input, const int nelements, char *const output);
int tsDecompressFloatImp(const char *const input, const int nelements, char *const output);

/* ----------------------------------------------Compression function used by
 * others ---------------------------------------------- */
int tsCompressTinyint(const char *const input, int inputSize, const int nelements, char *const output, int outputSize, char algorithm,
                      char *const buffer, int bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsCompressINTImp(input, nelements, output, TSDB_DATA_TYPE_TINYINT);
  } else if (algorithm == TWO_STAGE_COMP) {
    int len = tsCompressINTImp(input, nelements, buffer, TSDB_DATA_TYPE_TINYINT);
    return tsCompressStringImp(buffer, len, output, outputSize);
  } else {
    assert(0);
  }
}

int tsDecompressTinyint(const char *const input, int compressedSize, const int nelements, char *const output,
                        int outputSize, char algorithm, char *const buffer, int bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsDecompressINTImp(input, nelements, output, TSDB_DATA_TYPE_TINYINT);
  } else if (algorithm == TWO_STAGE_COMP) {
    tsDecompressStringImp(input, compressedSize, buffer, bufferSize);
    return tsDecompressINTImp(buffer, nelements, output, TSDB_DATA_TYPE_TINYINT);
  } else {
    assert(0);
  }
}

int tsCompressSmallint(const char *const input, int inputSize, const int nelements, char *const output, int outputSize, char algorithm,
                       char *const buffer, int bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsCompressINTImp(input, nelements, output, TSDB_DATA_TYPE_SMALLINT);
  } else if (algorithm == TWO_STAGE_COMP) {
    int len = tsCompressINTImp(input, nelements, buffer, TSDB_DATA_TYPE_SMALLINT);
    return tsCompressStringImp(buffer, len, output, outputSize);
  } else {
    assert(0);
  }
}

int tsDecompressSmallint(const char *const input, int compressedSize, const int nelements, char *const output,
                         int outputSize, char algorithm, char *const buffer, int bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsDecompressINTImp(input, nelements, output, TSDB_DATA_TYPE_SMALLINT);
  } else if (algorithm == TWO_STAGE_COMP) {
    tsDecompressStringImp(input, compressedSize, buffer, bufferSize);
    return tsDecompressINTImp(buffer, nelements, output, TSDB_DATA_TYPE_SMALLINT);
  } else {
    assert(0);
  }
}

int tsCompressInt(const char *const input, int inputSize, const int nelements, char *const output, int outputSize, char algorithm,
                  char *const buffer, int bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsCompressINTImp(input, nelements, output, TSDB_DATA_TYPE_INT);
  } else if (algorithm == TWO_STAGE_COMP) {
    int len = tsCompressINTImp(input, nelements, buffer, TSDB_DATA_TYPE_INT);
    return tsCompressStringImp(buffer, len, output, outputSize);
  } else {
    assert(0);
  }
}

int tsDecompressInt(const char *const input, int compressedSize, const int nelements, char *const output,
                    int outputSize, char algorithm, char *const buffer, int bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsDecompressINTImp(input, nelements, output, TSDB_DATA_TYPE_INT);
  } else if (algorithm == TWO_STAGE_COMP) {
    tsDecompressStringImp(input, compressedSize, buffer, bufferSize);
    return tsDecompressINTImp(buffer, nelements, output, TSDB_DATA_TYPE_INT);
  } else {
    assert(0);
  }
}

int tsCompressBigint(const char *const input, int inputSize, const int nelements, char *const output, int outputSize,
                     char algorithm, char *const buffer, int bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsCompressINTImp(input, nelements, output, TSDB_DATA_TYPE_BIGINT);
  } else if (algorithm == TWO_STAGE_COMP) {
    int len = tsCompressINTImp(input, nelements, buffer, TSDB_DATA_TYPE_BIGINT);
    return tsCompressStringImp(buffer, len, output, outputSize);
  } else {
    assert(0);
  }
}

int tsDecompressBigint(const char *const input, int compressedSize, const int nelements, char *const output,
                       int outputSize, char algorithm, char *const buffer, int bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsDecompressINTImp(input, nelements, output, TSDB_DATA_TYPE_BIGINT);
  } else if (algorithm == TWO_STAGE_COMP) {
    tsDecompressStringImp(input, compressedSize, buffer, bufferSize);
    return tsDecompressINTImp(buffer, nelements, output, TSDB_DATA_TYPE_BIGINT);
  } else {
    assert(0);
  }
}

int tsCompressBool(const char *const input, int inputSize, const int nelements, char *const output, int outputSize, 
                   char algorithm, char *const buffer, int bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsCompressBoolImp(input, nelements, output);
  } else if (algorithm == TWO_STAGE_COMP) {
    int len = tsCompressBoolImp(input, nelements, buffer);
    return tsCompressStringImp(buffer, len, output, outputSize);
  } else {
    assert(0);
  }
}

int tsDecompressBool(const char *const input, int compressedSize, const int nelements, char *const output,
                     int outputSize, char algorithm, char *const buffer, int bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsDecompressBoolImp(input, nelements, output);
  } else if (algorithm == TWO_STAGE_COMP) {
    tsDecompressStringImp(input, compressedSize, buffer, bufferSize);
    return tsDecompressBoolImp(buffer, nelements, output);
  } else {
    assert(0);
  }
}

int tsCompressString(const char *const input, int inputSize, const int nelements, char *const output, int outputSize,
                     char algorithm, char *const buffer, int bufferSize) {
  return tsCompressStringImp(input, inputSize, output, outputSize);
}

int tsDecompressString(const char *const input, int compressedSize, const int nelements, char *const output,
                       int outputSize, char algorithm, char *const buffer, int bufferSize) {
  return tsDecompressStringImp(input, compressedSize, output, outputSize);
}

int tsCompressFloat(const char *const input, int inputSize, const int nelements, char *const output, int outputSize,
                    char algorithm, char *const buffer, int bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsCompressFloatImp(input, nelements, output);
  } else if (algorithm == TWO_STAGE_COMP) {
    int len = tsCompressFloatImp(input, nelements, buffer);
    return tsCompressStringImp(buffer, len, output, outputSize);
  } else {
    assert(0);
  }
}

int tsDecompressFloat(const char *const input, int compressedSize, const int nelements, char *const output,
                      int outputSize, char algorithm, char *const buffer, int bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsDecompressFloatImp(input, nelements, output);
  } else if (algorithm == TWO_STAGE_COMP) {
    tsDecompressStringImp(input, compressedSize, buffer, bufferSize);
    return tsDecompressFloatImp(buffer, nelements, output);
  } else {
    assert(0);
  }
}
int tsCompressDouble(const char *const input, int inputSize, const int nelements, char *const output, int outputSize,
                     char algorithm, char *const buffer, int bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsCompressDoubleImp(input, nelements, output);
  } else if (algorithm == TWO_STAGE_COMP) {
    int len = tsCompressDoubleImp(input, nelements, buffer);
    return tsCompressStringImp(buffer, len, output, outputSize);
  } else {
    assert(0);
  }
}

int tsDecompressDouble(const char *const input, int compressedSize, const int nelements, char *const output,
                       int outputSize, char algorithm, char *const buffer, int bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsDecompressDoubleImp(input, nelements, output);
  } else if (algorithm == TWO_STAGE_COMP) {
    tsDecompressStringImp(input, compressedSize, buffer, bufferSize);
    return tsDecompressDoubleImp(buffer, nelements, output);
  } else {
    assert(0);
  }
}

int tsCompressTimestamp(const char *const input, int inputSize, const int nelements, char *const output, int outputSize,
                        char algorithm, char *const buffer, int bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsCompressTimestampImp(input, nelements, output);
  } else if (algorithm == TWO_STAGE_COMP) {
    int len = tsCompressTimestampImp(input, nelements, buffer);
    return tsCompressStringImp(buffer, len, output, outputSize);
  } else {
    assert(0);
  }
}

int tsDecompressTimestamp(const char *const input, int compressedSize, const int nelements, char *const output,
                          int outputSize, char algorithm, char *const buffer, int bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsDecompressTimestampImp(input, nelements, output);
  } else if (algorithm == TWO_STAGE_COMP) {
    tsDecompressStringImp(input, compressedSize, buffer, bufferSize);
    return tsDecompressTimestampImp(buffer, nelements, output);
  } else {
    assert(0);
  }
}

bool safeInt64Add(int64_t a, int64_t b) {
  if ((a > 0 && b > INT64_MAX - a) || (a < 0 && b < INT64_MIN - a)) return false;
  return true;
}

/*
 * Compress Integer (Simple8B).
 */
int tsCompressINTImp(const char *const input, const int nelements, char *const output, const char type) {
  // Selector value:              0    1   2   3   4   5   6   7   8  9  10  11
  // 12  13  14  15
  char bit_per_integer[] = {0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60};
  int  selector_to_elems[] = {240, 120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1};
  char bit_to_selector[] = {0,  2,  3,  4,  5,  6,  7,  8,  9,  10, 10, 11, 11, 12, 12, 12, 13, 13, 13, 13, 13,
                            14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
                            15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15};

  // get the byte limit.
  int word_length = 0;
  switch (type) {
    case TSDB_DATA_TYPE_BIGINT:
      word_length = LONG_BYTES;
      break;
    case TSDB_DATA_TYPE_INT:
      word_length = INT_BYTES;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      word_length = SHORT_BYTES;
      break;
    case TSDB_DATA_TYPE_TINYINT:
      word_length = CHAR_BYTES;
      break;
    default:
      perror("Wrong integer types.\n");
      exit(1);
  }

  int     byte_limit = nelements * word_length + 1;
  int     opos = 1;
  int64_t prev_value = 0;

  for (int i = 0; i < nelements;) {
    char    selector = 0;
    char    bit = 0;
    int     elems = 0;
    int64_t prev_value_tmp = prev_value;

    for (int j = i; j < nelements; j++) {
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
      if (!safeInt64Add(curr_value, -prev_value)) goto _copy_and_exit;

      int64_t diff = curr_value - prev_value_tmp;
      // Zigzag encode the value.
      uint64_t zigzag_value = (diff >> (LONG_BYTES * BITS_PER_BYTE - 1)) ^ (diff << 1);

      if (zigzag_value >= SIMPLE8B_MAX_INT64) goto _copy_and_exit;

      char tmp_bit;
      if (zigzag_value == 0) {
        // Take care here, __builtin_clzl give wrong anser for value 0;
        tmp_bit = 0;
      } else {
        tmp_bit = (LONG_BYTES * BITS_PER_BYTE) - __builtin_clzl(zigzag_value);
      }

      if (elems + 1 <= selector_to_elems[selector] && elems + 1 <= selector_to_elems[bit_to_selector[tmp_bit]]) {
        // If can hold another one.
        selector = selector > bit_to_selector[tmp_bit] ? selector : bit_to_selector[tmp_bit];
        elems++;
        bit = bit_per_integer[selector];
      } else {
        // if cannot hold another one.
        while (elems < selector_to_elems[selector]) selector++;
        elems = selector_to_elems[selector];
        bit = bit_per_integer[selector];
        break;
      }
      prev_value_tmp = curr_value;
    }

    uint64_t buffer = 0;
    buffer |= (uint64_t)selector;
    for (int k = 0; k < elems; k++) {
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
      uint64_t zigzag_value = (diff >> (LONG_BYTES * BITS_PER_BYTE - 1)) ^ (diff << 1);
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

int tsDecompressINTImp(const char *const input, const int nelements, char *const output, const char type) {
  int word_length = 0;
  switch (type) {
    case TSDB_DATA_TYPE_BIGINT:
      word_length = LONG_BYTES;
      break;
    case TSDB_DATA_TYPE_INT:
      word_length = INT_BYTES;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      word_length = SHORT_BYTES;
      break;
    case TSDB_DATA_TYPE_TINYINT:
      word_length = CHAR_BYTES;
      break;
    default:
      perror("Wrong integer types.\n");
      exit(1);
  }

  // If not compressed.
  if (input[0] == 1) {
    memcpy(output, input + 1, nelements * word_length);
    return nelements * word_length;
  }

  // Selector value:              0    1   2   3   4   5   6   7   8  9  10  11
  // 12  13  14  15
  char bit_per_integer[] = {0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60};
  int  selector_to_elems[] = {240, 120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1};

  const char *ip = input + 1;
  int         count = 0;
  int         _pos = 0;
  int64_t     prev_value = 0;

  while (1) {
    if (count == nelements) break;

    uint64_t w = 0;
    memcpy(&w, ip, LONG_BYTES);

    char selector = (char)(w & INT64MASK(4));  // selector = 4
    char bit = bit_per_integer[selector];      // bit = 3
    int  elems = selector_to_elems[selector];

    for (int i = 0; i < elems; i++) {
      uint64_t zigzag_value;

      if (selector == 0 || selector == 1) {
        zigzag_value = 0;
      } else {
        zigzag_value = ((w >> (4 + bit * i)) & INT64MASK(bit));
      }
      int64_t diff = (zigzag_value >> 1) ^ -(zigzag_value & 1);
      int64_t curr_value = diff + prev_value;
      prev_value = curr_value;

      switch (type) {
        case TSDB_DATA_TYPE_BIGINT:
          *((int64_t *)output + _pos) = curr_value;
          _pos++;
          break;
        case TSDB_DATA_TYPE_INT:
          *((int32_t *)output + _pos) = curr_value;
          _pos++;
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          *((int16_t *)output + _pos) = curr_value;
          _pos++;
          break;
        case TSDB_DATA_TYPE_TINYINT:
          *((int8_t *)output + _pos) = curr_value;
          _pos++;
          break;
        default:
          perror("Wrong integer types.\n");
          exit(1);
      }
      count++;
      if (count == nelements) break;
    }
    ip += LONG_BYTES;
  }

  return nelements * word_length;
}

/* ----------------------------------------------Bool Compression
 * ---------------------------------------------- */
// TODO: You can also implement it using RLE method.
int tsCompressBoolImp(const char *const input, const int nelements, char *const output) {
  int pos = -1;
  int ele_per_byte = BITS_PER_BYTE / 2;

  for (int i = 0; i < nelements; i++) {
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
      perror("Wrong bool value.\n");
      exit(1);
    }
  }

  return pos + 1;
}

int tsDecompressBoolImp(const char *const input, const int nelements, char *const output) {
  int ipos = -1, opos = 0;
  int ele_per_byte = BITS_PER_BYTE / 2;

  for (int i = 0; i < nelements; i++) {
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

/* Run Length Encoding(RLE) Method */
int tsCompressBoolRLEImp(const char *const input, const int nelements, char *const output) {
  int _pos = 0;

  for (int i = 0; i < nelements;) {
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
      perror("Wrong bool value!\n");
      exit(1);
    }
  }

  return _pos;
}

int tsDecompressBoolRLEImp(const char *const input, const int nelements, char *const output) {
  int ipos = 0, opos = 0;
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

/* ----------------------------------------------String Compression
 * ---------------------------------------------- */
// Note: the size of the output must be larger than input_size + 1 and
// LZ4_compressBound(size) + 1;
// >= max(input_size, LZ4_compressBound(input_size)) + 1;
int tsCompressStringImp(const char *const input, int inputSize, char *const output, int outputSize) {
  // Try to compress using LZ4 algorithm.
  const int compressed_data_size = LZ4_compress_default(input, output + 1, inputSize, outputSize-1);

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

int tsDecompressStringImp(const char *const input, int compressedSize, char *const output, int outputSize) {
  // compressedSize is the size of data after compression.
  if (input[0] == 1) {
    /* It is compressed by LZ4 algorithm */
    const int decompressed_size = LZ4_decompress_safe(input + 1, output, compressedSize - 1, outputSize);
    if (decompressed_size < 0) {
      char msg[128] = {0};
      sprintf(msg, "decomp_size:%d, Error decompress in LZ4 algorithm!\n", decompressed_size);
      perror(msg);
      exit(EXIT_FAILURE);
    }

    return decompressed_size;
  } else if (input[0] == 0) {
    /* It is not compressed by LZ4 algorithm */
    memcpy(output, input + 1, compressedSize - 1);
    return compressedSize - 1;
  } else {
    perror("Wrong compressed string indicator!\n");
    exit(EXIT_FAILURE);
  }
}

/* --------------------------------------------Timestamp Compression
 * ---------------------------------------------- */
// TODO: Take care here, we assumes little endian encoding.
int tsCompressTimestampImp(const char *const input, const int nelements, char *const output) {
  int _pos = 1;
  assert(nelements >= 0);

  if (nelements == 0) return 0;

  int64_t *istream = (int64_t *)input;

  int64_t  prev_value = istream[0];
  int64_t  prev_delta = -prev_value;
  uint8_t  flags = 0, flag1 = 0, flag2 = 0;
  uint64_t dd1 = 0, dd2 = 0;

  for (int i = 0; i < nelements; i++) {
    int64_t curr_value = istream[i];
    if (!safeInt64Add(curr_value, -prev_value)) goto _exit_over;
    int64_t curr_delta = curr_value - prev_value;
    if (!safeInt64Add(curr_delta, -prev_delta)) goto _exit_over;
    int64_t delta_of_delta = curr_delta - prev_delta;
    // zigzag encode the value.
    uint64_t zigzag_value = (delta_of_delta >> (LONG_BYTES * BITS_PER_BYTE - 1)) ^ (delta_of_delta << 1);
    if (i % 2 == 0) {
      flags = 0;
      dd1 = zigzag_value;
      if (dd1 == 0) {
        flag1 = 0;
      } else {
        flag1 = LONG_BYTES - __builtin_clzl(dd1) / BITS_PER_BYTE;
      }
    } else {
      dd2 = zigzag_value;
      if (dd2 == 0) {
        flag2 = 0;
      } else {
        flag2 = LONG_BYTES - __builtin_clzl(dd2) / BITS_PER_BYTE;
      }
      flags = flag1 | (flag2 << 4);
      // Encode the flag.
      if ((_pos + CHAR_BYTES - 1) >= nelements * LONG_BYTES) goto _exit_over;
      memcpy(output + _pos, &flags, CHAR_BYTES);
      _pos += CHAR_BYTES;
      /* Here, we assume it is little endian encoding method. */
      // Encode dd1
      if (is_bigendian()) {
        if ((_pos + flag1 - 1) >= nelements * LONG_BYTES) goto _exit_over;
        memcpy(output + _pos, (char *)(&dd1) + LONG_BYTES - flag1, flag1);
      } else {
        if ((_pos + flag1 - 1) >= nelements * LONG_BYTES) goto _exit_over;
        memcpy(output + _pos, (char *)(&dd1), flag1);
      }
      _pos += flag1;
      // Encode dd2;
      if (is_bigendian()) {
        if ((_pos + flag2 - 1) >= nelements * LONG_BYTES) goto _exit_over;
        memcpy(output + _pos, (char *)(&dd2) + LONG_BYTES - flag2, flag2);
      } else {
        if ((_pos + flag2 - 1) >= nelements * LONG_BYTES) goto _exit_over;
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
    if ((_pos + CHAR_BYTES - 1) >= nelements * LONG_BYTES) goto _exit_over;
    memcpy(output + _pos, &flags, CHAR_BYTES);
    _pos += CHAR_BYTES;
    // Encode dd1;
    if (is_bigendian()) {
      if ((_pos + flag1 - 1) >= nelements * LONG_BYTES) goto _exit_over;
      memcpy(output + _pos, (char *)(&dd1) + LONG_BYTES - flag1, flag1);
    } else {
      if ((_pos + flag1 - 1) >= nelements * LONG_BYTES) goto _exit_over;
      memcpy(output + _pos, (char *)(&dd1), flag1);
    }
    _pos += flag1;
  }

  output[0] = 1;  // Means the string is compressed
  return _pos;

_exit_over:
  output[0] = 0;  // Means the string is not compressed
  memcpy(output + 1, input, nelements * LONG_BYTES);
  return nelements * LONG_BYTES + 1;
}

int tsDecompressTimestampImp(const char *const input, const int nelements, char *const output) {
  assert(nelements >= 0);
  if (nelements == 0) return 0;

  if (input[0] == 0) {
    memcpy(output, input + 1, nelements * LONG_BYTES);
    return nelements * LONG_BYTES;
  } else if (input[0] == 1) {  // Decompress
    int64_t *ostream = (int64_t *)output;

    int     ipos = 1, opos = 0;
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
          memcpy(&dd1 + LONG_BYTES - nbytes, input + ipos, nbytes);
        } else {
          memcpy(&dd1, input + ipos, nbytes);
        }
        delta_of_delta = (dd1 >> 1) ^ -(dd1 & 1);
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
      if (opos == nelements) return nelements * LONG_BYTES;

      // Decode dd2
      uint64_t dd2 = 0;
      nbytes = (flags >> 4) & INT8MASK(4);
      if (nbytes == 0) {
        delta_of_delta = 0;
      } else {
        if (is_bigendian()) {
          memcpy(&dd2 + LONG_BYTES - nbytes, input + ipos, nbytes);
        } else {
          memcpy(&dd2, input + ipos, nbytes);
        }
        // zigzag_decoding
        delta_of_delta = (dd2 >> 1) ^ -(dd2 & 1);
      }
      ipos += nbytes;
      prev_delta = delta_of_delta + prev_delta;
      prev_value = prev_value + prev_delta;
      ostream[opos++] = prev_value;
      if (opos == nelements) return nelements * LONG_BYTES;
    }

  } else {
    assert(0);
  }
}
/* --------------------------------------------Double Compression
 * ---------------------------------------------- */
void encodeDoubleValue(uint64_t diff, uint8_t flag, char *const output, int *const pos) {
  uint8_t nbytes = (flag & INT8MASK(3)) + 1;
  int     nshift = (LONG_BYTES * BITS_PER_BYTE - nbytes * BITS_PER_BYTE) * (flag >> 3);
  diff >>= nshift;

  while (nbytes) {
    output[(*pos)++] = (int8_t)(diff & INT64MASK(8));
    diff >>= BITS_PER_BYTE;
    nbytes--;
  }
}

int tsCompressDoubleImp(const char *const input, const int nelements, char *const output) {
  int byte_limit = nelements * DOUBLE_BYTES + 1;
  int opos = 1;

  uint64_t prev_value = 0;
  uint64_t prev_diff = 0;
  uint8_t  prev_flag = 0;

  double *istream = (double *)input;

  // Main loop
  for (int i = 0; i < nelements; i++) {
    union {
      double   real;
      uint64_t bits;
    } curr;

    curr.real = istream[i];

    // Here we assume the next value is the same as previous one.
    uint64_t predicted = prev_value;
    uint64_t diff = curr.bits ^ predicted;

    int leading_zeros = LONG_BYTES * BITS_PER_BYTE;
    int trailing_zeros = leading_zeros;

    if (diff) {
      trailing_zeros = __builtin_ctzl(diff);
      leading_zeros = __builtin_clzl(diff);
    }

    uint8_t nbytes = 0;
    uint8_t flag;

    if (trailing_zeros > leading_zeros) {
      nbytes = LONG_BYTES - trailing_zeros / BITS_PER_BYTE;

      if (nbytes > 0) nbytes--;
      flag = ((uint8_t)1 << 3) | nbytes;
    } else {
      nbytes = LONG_BYTES - leading_zeros / BITS_PER_BYTE;
      if (nbytes > 0) nbytes--;
      flag = nbytes;
    }

    if (i % 2 == 0) {
      prev_diff = diff;
      prev_flag = flag;
    } else {
      int nbyte1 = (prev_flag & INT8MASK(3)) + 1;
      int nbyte2 = (flag & INT8MASK(3)) + 1;
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
    int nbyte1 = (prev_flag & INT8MASK(3)) + 1;
    int nbyte2 = 1;
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

uint64_t decodeDoubleValue(const char *const input, int *const ipos, uint8_t flag) {
  uint64_t diff = 0ul;
  int      nbytes = (flag & INT8MASK(3)) + 1;
  for (int i = 0; i < nbytes; i++) {
    diff = diff | ((INT64MASK(8) & input[(*ipos)++]) << BITS_PER_BYTE * i);
  }
  int shift_width = (LONG_BYTES * BITS_PER_BYTE - nbytes * BITS_PER_BYTE) * (flag >> 3);
  diff <<= shift_width;

  return diff;
}

int tsDecompressDoubleImp(const char *const input, const int nelements, char *const output) {
  // output stream
  double *ostream = (double *)output;

  if (input[0] == 1) {
    memcpy(output, input + 1, nelements * DOUBLE_BYTES);
    return nelements * DOUBLE_BYTES;
  }

  uint8_t  flags = 0;
  int      ipos = 1;
  int      opos = 0;
  uint64_t prev_value = 0;

  for (int i = 0; i < nelements; i++) {
    if (i % 2 == 0) {
      flags = input[ipos++];
    }

    uint8_t flag = flags & INT8MASK(4);
    flags >>= 4;

    uint64_t diff = decodeDoubleValue(input, &ipos, flag);
    union {
      uint64_t bits;
      double   real;
    } curr;

    uint64_t predicted = prev_value;
    curr.bits = predicted ^ diff;
    prev_value = curr.bits;

    ostream[opos++] = curr.real;
  }

  return nelements * DOUBLE_BYTES;
}

/* --------------------------------------------Float Compression
 * ---------------------------------------------- */
void encodeFloatValue(uint32_t diff, uint8_t flag, char *const output, int *const pos) {
  uint8_t nbytes = (flag & INT8MASK(3)) + 1;
  int     nshift = (FLOAT_BYTES * BITS_PER_BYTE - nbytes * BITS_PER_BYTE) * (flag >> 3);
  diff >>= nshift;

  while (nbytes) {
    output[(*pos)++] = (int8_t)(diff & INT32MASK(8));
    diff >>= BITS_PER_BYTE;
    nbytes--;
  }
}

int tsCompressFloatImp(const char *const input, const int nelements, char *const output) {
  float *istream = (float *)input;
  int    byte_limit = nelements * FLOAT_BYTES + 1;
  int    opos = 1;

  uint32_t prev_value = 0;
  uint32_t prev_diff = 0;
  uint8_t  prev_flag = 0;

  // Main loop
  for (int i = 0; i < nelements; i++) {
    union {
      float    real;
      uint32_t bits;
    } curr;

    curr.real = istream[i];

    // Here we assume the next value is the same as previous one.
    uint32_t predicted = prev_value;
    uint32_t diff = curr.bits ^ predicted;

    int leading_zeros = FLOAT_BYTES * BITS_PER_BYTE;
    int trailing_zeros = leading_zeros;

    if (diff) {
      trailing_zeros = __builtin_ctz(diff);
      leading_zeros = __builtin_clz(diff);
    }

    uint8_t nbytes = 0;
    uint8_t flag;

    if (trailing_zeros > leading_zeros) {
      nbytes = FLOAT_BYTES - trailing_zeros / BITS_PER_BYTE;

      if (nbytes > 0) nbytes--;
      flag = ((uint8_t)1 << 3) | nbytes;
    } else {
      nbytes = FLOAT_BYTES - leading_zeros / BITS_PER_BYTE;
      if (nbytes > 0) nbytes--;
      flag = nbytes;
    }

    if (i % 2 == 0) {
      prev_diff = diff;
      prev_flag = flag;
    } else {
      int nbyte1 = (prev_flag & INT8MASK(3)) + 1;
      int nbyte2 = (flag & INT8MASK(3)) + 1;
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
    int nbyte1 = (prev_flag & INT8MASK(3)) + 1;
    int nbyte2 = 1;
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

uint32_t decodeFloatValue(const char *const input, int *const ipos, uint8_t flag) {
  uint32_t diff = 0ul;
  int      nbytes = (flag & INT8MASK(3)) + 1;
  for (int i = 0; i < nbytes; i++) {
    diff = diff | ((INT32MASK(8) & input[(*ipos)++]) << BITS_PER_BYTE * i);
  }
  int shift_width = (FLOAT_BYTES * BITS_PER_BYTE - nbytes * BITS_PER_BYTE) * (flag >> 3);
  diff <<= shift_width;

  return diff;
}

int tsDecompressFloatImp(const char *const input, const int nelements, char *const output) {
  float *ostream = (float *)output;

  if (input[0] == 1) {
    memcpy(output, input + 1, nelements * FLOAT_BYTES);
    return nelements * FLOAT_BYTES;
  }

  uint8_t  flags = 0;
  int      ipos = 1;
  int      opos = 0;
  uint32_t prev_value = 0;

  for (int i = 0; i < nelements; i++) {
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

  return nelements * FLOAT_BYTES;
}
