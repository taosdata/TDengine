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

#ifndef TDENGINE_TSCOMPRESSION_H
#define TDENGINE_TSCOMPRESSION_H

#ifdef __cplusplus
extern "C" {
#endif

#include "taosdef.h"
#include "tutil.h"

#define COMP_OVERFLOW_BYTES 2
#define BITS_PER_BYTE 8
// Masks
#define INT64MASK(_x) ((((uint64_t)1) << _x) - 1)
#define INT32MASK(_x) (((uint32_t)1 << _x) - 1)
#define INT8MASK(_x) (((uint8_t)1 << _x) - 1)
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
#define MODE_NOCOMPRESS  0  // original data
#define MODE_COMPRESS    1  // compatible old compress

// compression algorithm save first byte higher 7 bit
#define ALGO_SZ_LOSSY     1 // SZ compress 

#define HEAD_MODE(x)  x%2
#define HEAD_ALGO(x)  x/2

extern int tsCompressINTImp(const char *const input, const int nelements, char *const output, const char type);
extern int tsDecompressINTImp(const char *const input, const int nelements, char *const output, const char type);
extern int tsCompressBoolImp(const char *const input, const int nelements, char *const output);
extern int tsDecompressBoolImp(const char *const input, const int nelements, char *const output);
extern int tsCompressStringImp(const char *const input, int inputSize, char *const output, int outputSize);
extern int tsDecompressStringImp(const char *const input, int compressedSize, char *const output, int outputSize);
extern int tsCompressTimestampImp(const char *const input, const int nelements, char *const output);
extern int tsDecompressTimestampImp(const char *const input, const int nelements, char *const output);
extern int tsCompressDoubleImp(const char *const input, const int nelements, char *const output);
extern int tsDecompressDoubleImp(const char *const input, const int nelements, char *const output);
extern int tsCompressFloatImp(const char *const input, const int nelements, char *const output);
extern int tsDecompressFloatImp(const char *const input, const int nelements, char *const output);
// lossy
extern int tsCompressFloatLossyImp(const char * input, const int nelements, char *const output);
extern int tsDecompressFloatLossyImp(const char * input, int compressedSize, const int nelements, char *const output);
extern int tsCompressDoubleLossyImp(const char * input, const int nelements, char *const output);
extern int tsDecompressDoubleLossyImp(const char * input, int compressedSize, const int nelements, char *const output);

#ifdef TD_TSZ
extern bool lossyFloat;
extern bool lossyDouble;
// init call
int tsCompressInit();
// exit call
void tsCompressExit();
#endif

static FORCE_INLINE int tsCompressTinyint(const char *const input, int inputSize, const int nelements, char *const output, int outputSize, char algorithm,
                      char *const buffer, int bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsCompressINTImp(input, nelements, output, TSDB_DATA_TYPE_TINYINT);
  } else if (algorithm == TWO_STAGE_COMP) {
    int len = tsCompressINTImp(input, nelements, buffer, TSDB_DATA_TYPE_TINYINT);
    return tsCompressStringImp(buffer, len, output, outputSize);
  } else {
    assert(0);
    return -1;
  }
}

static FORCE_INLINE int tsDecompressTinyint(const char *const input, int compressedSize, const int nelements, char *const output,
                        int outputSize, char algorithm, char *const buffer, int bufferSize) {
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

static FORCE_INLINE int tsCompressSmallint(const char *const input, int inputSize, const int nelements, char *const output, int outputSize, char algorithm,
                       char *const buffer, int bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsCompressINTImp(input, nelements, output, TSDB_DATA_TYPE_SMALLINT);
  } else if (algorithm == TWO_STAGE_COMP) {
    int len = tsCompressINTImp(input, nelements, buffer, TSDB_DATA_TYPE_SMALLINT);
    return tsCompressStringImp(buffer, len, output, outputSize);
  } else {
    assert(0);
    return -1;
  }
}

static FORCE_INLINE int tsDecompressSmallint(const char *const input, int compressedSize, const int nelements, char *const output,
                         int outputSize, char algorithm, char *const buffer, int bufferSize) {
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

static FORCE_INLINE int tsCompressInt(const char *const input, int inputSize, const int nelements, char *const output, int outputSize, char algorithm,
                  char *const buffer, int bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsCompressINTImp(input, nelements, output, TSDB_DATA_TYPE_INT);
  } else if (algorithm == TWO_STAGE_COMP) {
    int len = tsCompressINTImp(input, nelements, buffer, TSDB_DATA_TYPE_INT);
    return tsCompressStringImp(buffer, len, output, outputSize);
  } else {
    assert(0);
    return -1;
  }
}

static FORCE_INLINE int tsDecompressInt(const char *const input, int compressedSize, const int nelements, char *const output,
                    int outputSize, char algorithm, char *const buffer, int bufferSize) {
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

static FORCE_INLINE int tsCompressBigint(const char *const input, int inputSize, const int nelements, char *const output, int outputSize,
                     char algorithm, char *const buffer, int bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsCompressINTImp(input, nelements, output, TSDB_DATA_TYPE_BIGINT);
  } else if (algorithm == TWO_STAGE_COMP) {
    int len = tsCompressINTImp(input, nelements, buffer, TSDB_DATA_TYPE_BIGINT);
    return tsCompressStringImp(buffer, len, output, outputSize);
  } else {
    assert(0);
    return -1;
  }
}

static FORCE_INLINE int tsDecompressBigint(const char *const input, int compressedSize, const int nelements, char *const output,
                       int outputSize, char algorithm, char *const buffer, int bufferSize) {
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

static FORCE_INLINE int tsCompressBool(const char *const input, int inputSize, const int nelements, char *const output, int outputSize, 
                   char algorithm, char *const buffer, int bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsCompressBoolImp(input, nelements, output);
  } else if (algorithm == TWO_STAGE_COMP) {
    int len = tsCompressBoolImp(input, nelements, buffer);
    return tsCompressStringImp(buffer, len, output, outputSize);
  } else {
    assert(0);
    return -1;
  }
}

static FORCE_INLINE int tsDecompressBool(const char *const input, int compressedSize, const int nelements, char *const output,
                     int outputSize, char algorithm, char *const buffer, int bufferSize) {
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

static FORCE_INLINE int tsCompressString(const char *const input, int inputSize, const int nelements, char *const output, int outputSize,
                     char algorithm, char *const buffer, int bufferSize) {
  return tsCompressStringImp(input, inputSize, output, outputSize);
}

static FORCE_INLINE int tsDecompressString(const char *const input, int compressedSize, const int nelements, char *const output,
                       int outputSize, char algorithm, char *const buffer, int bufferSize) {
  return tsDecompressStringImp(input, compressedSize, output, outputSize);
}

static FORCE_INLINE int tsCompressFloat(const char *const input, int inputSize, const int nelements, char *const output, int outputSize,
                    char algorithm, char *const buffer, int bufferSize) {
#ifdef TD_TSZ
  // lossy mode
  if(lossyFloat) {
    return tsCompressFloatLossyImp(input, nelements, output);
  // lossless mode  
  } else {
#endif    
    if (algorithm == ONE_STAGE_COMP) {
      return tsCompressFloatImp(input, nelements, output);
    } else if (algorithm == TWO_STAGE_COMP) {
      int len = tsCompressFloatImp(input, nelements, buffer);
      return tsCompressStringImp(buffer, len, output, outputSize);
    } else {
      assert(0);
      return -1;
    }    
#ifdef TD_TSZ  
  }
#endif
}

static FORCE_INLINE int tsDecompressFloat(const char *const input, int compressedSize, const int nelements, char *const output,
                      int outputSize, char algorithm, char *const buffer, int bufferSize) {
#ifdef TD_TSZ
  if(HEAD_ALGO(input[0]) == ALGO_SZ_LOSSY){
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


static FORCE_INLINE int tsCompressDouble(const char *const input, int inputSize, const int nelements, char *const output, int outputSize,
                     char algorithm, char *const buffer, int bufferSize) {
#ifdef TD_TSZ  
  if(lossyDouble){
    // lossy mode
    return tsCompressDoubleLossyImp(input, nelements, output);
  } else {
#endif    
    // lossless mode
    if (algorithm == ONE_STAGE_COMP) {
      return tsCompressDoubleImp(input, nelements, output);
    } else if (algorithm == TWO_STAGE_COMP) {
      int len = tsCompressDoubleImp(input, nelements, buffer);
      return tsCompressStringImp(buffer, len, output, outputSize);
    } else {
      assert(0);
      return -1;
    }
#ifdef TD_TSZ      
  }
#endif  
}

static FORCE_INLINE int tsDecompressDouble(const char *const input, int compressedSize, const int nelements, char *const output,
                       int outputSize, char algorithm, char *const buffer, int bufferSize) {
  #ifdef TD_TSZ  
  if(HEAD_ALGO(input[0]) == ALGO_SZ_LOSSY){
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
static FORCE_INLINE int tsCompressFloatLossy(const char *const input, int inputSize, const int nelements, char *const output, int outputSize,
                    char algorithm, char *const buffer, int bufferSize) {
  return tsCompressFloatLossyImp(input, nelements, output);
}

static FORCE_INLINE int tsDecompressFloatLossy(const char *const input, int compressedSize, const int nelements, char *const output,
                      int outputSize, char algorithm, char *const buffer, int bufferSize){
  return tsDecompressFloatLossyImp(input, compressedSize, nelements, output);
}

static FORCE_INLINE int tsCompressDoubleLossy(const char *const input, int inputSize, const int nelements, char *const output, int outputSize,
                     char algorithm, char *const buffer, int bufferSize){
  return tsCompressDoubleLossyImp(input, nelements, output);
}

static FORCE_INLINE int tsDecompressDoubleLossy(const char *const input, int compressedSize, const int nelements, char *const output,
                       int outputSize, char algorithm, char *const buffer, int bufferSize){
  return tsDecompressDoubleLossyImp(input, compressedSize, nelements, output);
}

#endif

static FORCE_INLINE int tsCompressTimestamp(const char *const input, int inputSize, const int nelements, char *const output, int outputSize,
                        char algorithm, char *const buffer, int bufferSize) {
  if (algorithm == ONE_STAGE_COMP) {
    return tsCompressTimestampImp(input, nelements, output);
  } else if (algorithm == TWO_STAGE_COMP) {
    int len = tsCompressTimestampImp(input, nelements, buffer);
    return tsCompressStringImp(buffer, len, output, outputSize);
  } else {
    assert(0);
    return -1;
  }
}

static FORCE_INLINE int tsDecompressTimestamp(const char *const input, int compressedSize, const int nelements, char *const output,
                          int outputSize, char algorithm, char *const buffer, int bufferSize) {
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

#endif  // TDENGINE_TSCOMPRESSION_H