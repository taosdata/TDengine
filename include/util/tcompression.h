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

#ifdef TD_TSZ
extern bool lossyFloat;
extern bool lossyDouble;
int32_t     tsCompressInit();
void        tsCompressExit();

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

int32_t tsCompressTimestamp(const char *const input, int32_t inputSize, const int32_t nelements, char *const output,
                            int32_t outputSize, char algorithm, char *const buffer, int32_t bufferSize);

int32_t tsDecompressTimestamp(const char *const input, int32_t compressedSize, const int32_t nelements,
                              char *const output, int32_t outputSize, char algorithm, char *const buffer,
                              int32_t bufferSize);
int32_t tsCompressFloat(const char *const input, int32_t inputSize, const int32_t nelements, char *const output,
                        int32_t outputSize, char algorithm, char *const buffer, int32_t bufferSize);
int32_t tsDecompressFloat(const char *const input, int32_t compressedSize, const int32_t nelements, char *const output,
                          int32_t outputSize, char algorithm, char *const buffer, int32_t bufferSize);
int32_t tsCompressDouble(const char *const input, int32_t inputSize, const int32_t nelements, char *const output,
                         int32_t outputSize, char algorithm, char *const buffer, int32_t bufferSize);
int32_t tsDecompressDouble(const char *const input, int32_t compressedSize, const int32_t nelements, char *const output,
                           int32_t outputSize, char algorithm, char *const buffer, int32_t bufferSize);
int32_t tsCompressString(const char *const input, int32_t inputSize, const int32_t nelements, char *const output,
                         int32_t outputSize, char algorithm, char *const buffer, int32_t bufferSize);
int32_t tsDecompressString(const char *const input, int32_t compressedSize, const int32_t nelements, char *const output,
                           int32_t outputSize, char algorithm, char *const buffer, int32_t bufferSize);
int32_t tsCompressBool(const char *const input, int32_t inputSize, const int32_t nelements, char *const output,
                       int32_t outputSize, char algorithm, char *const buffer, int32_t bufferSize);
int32_t tsDecompressBool(const char *const input, int32_t compressedSize, const int32_t nelements, char *const output,
                         int32_t outputSize, char algorithm, char *const buffer, int32_t bufferSize);
int32_t tsCompressTinyint(const char *const input, int32_t inputSize, const int32_t nelements, char *const output,
                          int32_t outputSize, char algorithm, char *const buffer, int32_t bufferSize);
int32_t tsDecompressTinyint(const char *const input, int32_t compressedSize, const int32_t nelements,
                            char *const output, int32_t outputSize, char algorithm, char *const buffer,
                            int32_t bufferSize);
int32_t tsCompressSmallint(const char *const input, int32_t inputSize, const int32_t nelements, char *const output,
                           int32_t outputSize, char algorithm, char *const buffer, int32_t bufferSize);
int32_t tsDecompressSmallint(const char *const input, int32_t compressedSize, const int32_t nelements,
                             char *const output, int32_t outputSize, char algorithm, char *const buffer,
                             int32_t bufferSize);
int32_t tsCompressInt(const char *const input, int32_t inputSize, const int32_t nelements, char *const output,
                      int32_t outputSize, char algorithm, char *const buffer, int32_t bufferSize);
int32_t tsDecompressInt(const char *const input, int32_t compressedSize, const int32_t nelements, char *const output,
                        int32_t outputSize, char algorithm, char *const buffer, int32_t bufferSize);
int32_t tsCompressBigint(const char *const input, int32_t inputSize, const int32_t nelements, char *const output,
                         int32_t outputSize, char algorithm, char *const buffer, int32_t bufferSize);
int32_t tsDecompressBigint(const char *const input, int32_t compressedSize, const int32_t nelements, char *const output,
                           int32_t outputSize, char algorithm, char *const buffer, int32_t bufferSize);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_COMPRESSION_H_*/