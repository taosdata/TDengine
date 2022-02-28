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

#ifndef _TD_UTIL_COMPRESSION_H_
#define _TD_UTIL_COMPRESSION_H_

#include "os.h"
#include "taos.h"
#include "tutil.h"

#ifdef __cplusplus
extern "C" {
#endif

#define COMP_OVERFLOW_BYTES 2
#define BITS_PER_BYTE       8
// Masks
#define INT64MASK(_x) ((((uint64_t)1) << _x) - 1)
#define INT32MASK(_x) (((uint32_t)1 << _x) - 1)
#define INT8MASK(_x)  (((uint8_t)1 << _x) - 1)
// Compression algorithm
#define NO_COMPRESSION 0
#define ONE_STAGE_COMP 1
#define TWO_STAGE_COMP 2

//
// compressed data first byte foramt
//   ------ 7 bit ---- | ---- 1 bit ----
//        algorithm           mode
//

// compression data mode save first byte lower 1 bit
#define MODE_NOCOMPRESS 0  // original data
#define MODE_COMPRESS   1  // compatible old compress

// compression algorithm save first byte higher 7 bit
#define ALGO_SZ_LOSSY 1  // SZ compress

#define HEAD_MODE(x) x % 2
#define HEAD_ALGO(x) x / 2

extern int32_t tsCompressINTImp(const char *const input, const int32_t nelements, char *const output, const char type);
extern int32_t tsDecompressINTImp(const char *const input, const int32_t nelements, char *const output,
                                  const char type);
extern int32_t tsCompressBoolImp(const char *const input, const int32_t nelements, char *const output);
extern int32_t tsDecompressBoolImp(const char *const input, const int32_t nelements, char *const output);
extern int32_t tsCompressStringImp(const char *const input, int32_t inputSize, char *const output, int32_t outputSize);
extern int32_t tsDecompressStringImp(const char *const input, int32_t compressedSize, char *const output,
                                     int32_t outputSize);
extern int32_t tsCompressTimestampImp(const char *const input, const int32_t nelements, char *const output);
extern int32_t tsDecompressTimestampImp(const char *const input, const int32_t nelements, char *const output);
extern int32_t tsCompressDoubleImp(const char *const input, const int32_t nelements, char *const output);
extern int32_t tsDecompressDoubleImp(const char *const input, const int32_t nelements, char *const output);
extern int32_t tsCompressFloatImp(const char *const input, const int32_t nelements, char *const output);
extern int32_t tsDecompressFloatImp(const char *const input, const int32_t nelements, char *const output);
// lossy
extern int32_t tsCompressFloatLossyImp(const char *input, const int32_t nelements, char *const output);
extern int32_t tsDecompressFloatLossyImp(const char *input, int32_t compressedSize, const int32_t nelements,
                                         char *const output);
extern int32_t tsCompressDoubleLossyImp(const char *input, const int32_t nelements, char *const output);
extern int32_t tsDecompressDoubleLossyImp(const char *input, int32_t compressedSize, const int32_t nelements,
                                          char *const output);

#ifdef TD_TSZ
extern bool lossyFloat;
extern bool lossyDouble;
int32_t     tsCompressInit();
void        tsCompressExit();
#endif

static FORCE_INLINE int32_t tsCompressTinyint(const char *const input, int32_t inputSize, const int32_t nelements,
                                              char *const output, int32_t outputSize, char algorithm,
                                              char *const buffer, int32_t bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsCompressINTImp(input, nelements, output, TSDB_DATA_TYPE_TINYINT);
  } else if (algorithm == TWO_STAGE_COMP) {
    int32_t len = tsCompressINTImp(input, nelements, buffer, TSDB_DATA_TYPE_TINYINT);
    return tsCompressStringImp(buffer, len, output, outputSize);
  } else {
    assert(0);
    return -1;
  }
}

static FORCE_INLINE int32_t tsDecompressTinyint(const char *const input, int32_t compressedSize,
                                                const int32_t nelements, char *const output, int32_t outputSize,
                                                char algorithm, char *const buffer, int32_t bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsDecompressINTImp(input, nelements, output, TSDB_DATA_TYPE_TINYINT);
  } else if (algorithm == TWO_STAGE_COMP) {
    if (tsDecompressStringImp(input, compressedSize, buffer, bufferSize) < 0) return -1;
    return tsDecompressINTImp(buffer, nelements, output, TSDB_DATA_TYPE_TINYINT);
  } else {
    assert(0);
    return -1;
  }
}

static FORCE_INLINE int32_t tsCompressSmallint(const char *const input, int32_t inputSize, const int32_t nelements,
                                               char *const output, int32_t outputSize, char algorithm,
                                               char *const buffer, int32_t bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsCompressINTImp(input, nelements, output, TSDB_DATA_TYPE_SMALLINT);
  } else if (algorithm == TWO_STAGE_COMP) {
    int32_t len = tsCompressINTImp(input, nelements, buffer, TSDB_DATA_TYPE_SMALLINT);
    return tsCompressStringImp(buffer, len, output, outputSize);
  } else {
    assert(0);
    return -1;
  }
}

static FORCE_INLINE int32_t tsDecompressSmallint(const char *const input, int32_t compressedSize,
                                                 const int32_t nelements, char *const output, int32_t outputSize,
                                                 char algorithm, char *const buffer, int32_t bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsDecompressINTImp(input, nelements, output, TSDB_DATA_TYPE_SMALLINT);
  } else if (algorithm == TWO_STAGE_COMP) {
    if (tsDecompressStringImp(input, compressedSize, buffer, bufferSize) < 0) return -1;
    return tsDecompressINTImp(buffer, nelements, output, TSDB_DATA_TYPE_SMALLINT);
  } else {
    assert(0);
    return -1;
  }
}

static FORCE_INLINE int32_t tsCompressInt(const char *const input, int32_t inputSize, const int32_t nelements,
                                          char *const output, int32_t outputSize, char algorithm, char *const buffer,
                                          int32_t bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsCompressINTImp(input, nelements, output, TSDB_DATA_TYPE_INT);
  } else if (algorithm == TWO_STAGE_COMP) {
    int32_t len = tsCompressINTImp(input, nelements, buffer, TSDB_DATA_TYPE_INT);
    return tsCompressStringImp(buffer, len, output, outputSize);
  } else {
    assert(0);
    return -1;
  }
}

static FORCE_INLINE int32_t tsDecompressInt(const char *const input, int32_t compressedSize, const int32_t nelements,
                                            char *const output, int32_t outputSize, char algorithm, char *const buffer,
                                            int32_t bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsDecompressINTImp(input, nelements, output, TSDB_DATA_TYPE_INT);
  } else if (algorithm == TWO_STAGE_COMP) {
    if (tsDecompressStringImp(input, compressedSize, buffer, bufferSize) < 0) return -1;
    return tsDecompressINTImp(buffer, nelements, output, TSDB_DATA_TYPE_INT);
  } else {
    assert(0);
    return -1;
  }
}

static FORCE_INLINE int32_t tsCompressBigint(const char *const input, int32_t inputSize, const int32_t nelements,
                                             char *const output, int32_t outputSize, char algorithm, char *const buffer,
                                             int32_t bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsCompressINTImp(input, nelements, output, TSDB_DATA_TYPE_BIGINT);
  } else if (algorithm == TWO_STAGE_COMP) {
    int32_t len = tsCompressINTImp(input, nelements, buffer, TSDB_DATA_TYPE_BIGINT);
    return tsCompressStringImp(buffer, len, output, outputSize);
  } else {
    assert(0);
    return -1;
  }
}

static FORCE_INLINE int32_t tsDecompressBigint(const char *const input, int32_t compressedSize, const int32_t nelements,
                                               char *const output, int32_t outputSize, char algorithm,
                                               char *const buffer, int32_t bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsDecompressINTImp(input, nelements, output, TSDB_DATA_TYPE_BIGINT);
  } else if (algorithm == TWO_STAGE_COMP) {
    if (tsDecompressStringImp(input, compressedSize, buffer, bufferSize) < 0) return -1;
    return tsDecompressINTImp(buffer, nelements, output, TSDB_DATA_TYPE_BIGINT);
  } else {
    assert(0);
    return -1;
  }
}

static FORCE_INLINE int32_t tsCompressBool(const char *const input, int32_t inputSize, const int32_t nelements,
                                           char *const output, int32_t outputSize, char algorithm, char *const buffer,
                                           int32_t bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsCompressBoolImp(input, nelements, output);
  } else if (algorithm == TWO_STAGE_COMP) {
    int32_t len = tsCompressBoolImp(input, nelements, buffer);
    return tsCompressStringImp(buffer, len, output, outputSize);
  } else {
    assert(0);
    return -1;
  }
}

static FORCE_INLINE int32_t tsDecompressBool(const char *const input, int32_t compressedSize, const int32_t nelements,
                                             char *const output, int32_t outputSize, char algorithm, char *const buffer,
                                             int32_t bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsDecompressBoolImp(input, nelements, output);
  } else if (algorithm == TWO_STAGE_COMP) {
    if (tsDecompressStringImp(input, compressedSize, buffer, bufferSize) < 0) return -1;
    return tsDecompressBoolImp(buffer, nelements, output);
  } else {
    assert(0);
    return -1;
  }
}

static FORCE_INLINE int32_t tsCompressString(const char *const input, int32_t inputSize, const int32_t nelements,
                                             char *const output, int32_t outputSize, char algorithm, char *const buffer,
                                             int32_t bufferSize) {
  return tsCompressStringImp(input, inputSize, output, outputSize);
}

static FORCE_INLINE int32_t tsDecompressString(const char *const input, int32_t compressedSize, const int32_t nelements,
                                               char *const output, int32_t outputSize, char algorithm,
                                               char *const buffer, int32_t bufferSize) {
  return tsDecompressStringImp(input, compressedSize, output, outputSize);
}

static FORCE_INLINE int32_t tsCompressFloat(const char *const input, int32_t inputSize, const int32_t nelements,
                                            char *const output, int32_t outputSize, char algorithm, char *const buffer,
                                            int32_t bufferSize) {
#ifdef TD_TSZ
  // lossy mode
  if (lossyFloat) {
    return tsCompressFloatLossyImp(input, nelements, output);
    // lossless mode
  } else {
#endif
    if (algorithm == ONE_STAGE_COMP) {
      return tsCompressFloatImp(input, nelements, output);
    } else if (algorithm == TWO_STAGE_COMP) {
      int32_t len = tsCompressFloatImp(input, nelements, buffer);
      return tsCompressStringImp(buffer, len, output, outputSize);
    } else {
      assert(0);
      return -1;
    }
#ifdef TD_TSZ
  }
#endif
}

static FORCE_INLINE int32_t tsDecompressFloat(const char *const input, int32_t compressedSize, const int32_t nelements,
                                              char *const output, int32_t outputSize, char algorithm,
                                              char *const buffer, int32_t bufferSize) {
#ifdef TD_TSZ
  if (HEAD_ALGO(input[0]) == ALGO_SZ_LOSSY) {
    // decompress lossy
    return tsDecompressFloatLossyImp(input, compressedSize, nelements, output);
  } else {
#endif
    // decompress lossless
    if (algorithm == ONE_STAGE_COMP) {
      return tsDecompressFloatImp(input, nelements, output);
    } else if (algorithm == TWO_STAGE_COMP) {
      if (tsDecompressStringImp(input, compressedSize, buffer, bufferSize) < 0) return -1;
      return tsDecompressFloatImp(buffer, nelements, output);
    } else {
      assert(0);
      return -1;
    }
#ifdef TD_TSZ
  }
#endif
}

static FORCE_INLINE int32_t tsCompressDouble(const char *const input, int32_t inputSize, const int32_t nelements,
                                             char *const output, int32_t outputSize, char algorithm, char *const buffer,
                                             int32_t bufferSize) {
#ifdef TD_TSZ
  if (lossyDouble) {
    // lossy mode
    return tsCompressDoubleLossyImp(input, nelements, output);
  } else {
#endif
    // lossless mode
    if (algorithm == ONE_STAGE_COMP) {
      return tsCompressDoubleImp(input, nelements, output);
    } else if (algorithm == TWO_STAGE_COMP) {
      int32_t len = tsCompressDoubleImp(input, nelements, buffer);
      return tsCompressStringImp(buffer, len, output, outputSize);
    } else {
      assert(0);
      return -1;
    }
#ifdef TD_TSZ
  }
#endif
}

static FORCE_INLINE int32_t tsDecompressDouble(const char *const input, int32_t compressedSize, const int32_t nelements,
                                               char *const output, int32_t outputSize, char algorithm,
                                               char *const buffer, int32_t bufferSize) {
#ifdef TD_TSZ
  if (HEAD_ALGO(input[0]) == ALGO_SZ_LOSSY) {
    // decompress lossy
    return tsDecompressDoubleLossyImp(input, compressedSize, nelements, output);
  } else {
#endif
    // decompress lossless
    if (algorithm == ONE_STAGE_COMP) {
      return tsDecompressDoubleImp(input, nelements, output);
    } else if (algorithm == TWO_STAGE_COMP) {
      if (tsDecompressStringImp(input, compressedSize, buffer, bufferSize) < 0) return -1;
      return tsDecompressDoubleImp(buffer, nelements, output);
    } else {
      assert(0);
      return -1;
    }
#ifdef TD_TSZ
  }
#endif
}

#ifdef TD_TSZ
//
//  lossy float double
//
static FORCE_INLINE int32_t tsCompressFloatLossy(const char *const input, int32_t inputSize, const int32_t nelements,
                                                 char *const output, int32_t outputSize, char algorithm,
                                                 char *const buffer, int32_t bufferSize) {
  return tsCompressFloatLossyImp(input, nelements, output);
}

static FORCE_INLINE int32_t tsDecompressFloatLossy(const char *const input, int32_t compressedSize,
                                                   const int32_t nelements, char *const output, int32_t outputSize,
                                                   char algorithm, char *const buffer, int32_t bufferSize) {
  return tsDecompressFloatLossyImp(input, compressedSize, nelements, output);
}

static FORCE_INLINE int32_t tsCompressDoubleLossy(const char *const input, int32_t inputSize, const int32_t nelements,
                                                  char *const output, int32_t outputSize, char algorithm,
                                                  char *const buffer, int32_t bufferSize) {
  return tsCompressDoubleLossyImp(input, nelements, output);
}

static FORCE_INLINE int32_t tsDecompressDoubleLossy(const char *const input, int32_t compressedSize,
                                                    const int32_t nelements, char *const output, int32_t outputSize,
                                                    char algorithm, char *const buffer, int32_t bufferSize) {
  return tsDecompressDoubleLossyImp(input, compressedSize, nelements, output);
}

#endif

static FORCE_INLINE int32_t tsCompressTimestamp(const char *const input, int32_t inputSize, const int32_t nelements,
                                                char *const output, int32_t outputSize, char algorithm,
                                                char *const buffer, int32_t bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsCompressTimestampImp(input, nelements, output);
  } else if (algorithm == TWO_STAGE_COMP) {
    int32_t len = tsCompressTimestampImp(input, nelements, buffer);
    return tsCompressStringImp(buffer, len, output, outputSize);
  } else {
    assert(0);
    return -1;
  }
}

static FORCE_INLINE int32_t tsDecompressTimestamp(const char *const input, int32_t compressedSize,
                                                  const int32_t nelements, char *const output, int32_t outputSize,
                                                  char algorithm, char *const buffer, int32_t bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsDecompressTimestampImp(input, nelements, output);
  } else if (algorithm == TWO_STAGE_COMP) {
    if (tsDecompressStringImp(input, compressedSize, buffer, bufferSize) < 0) return -1;
    return tsDecompressTimestampImp(buffer, nelements, output);
  } else {
    assert(0);
    return -1;
  }
}

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_COMPRESSION_H_*/