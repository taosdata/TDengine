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

#define ZIGZAG_ENCODE(T, v) (((u##T)((v) >> (sizeof(T) * 8 - 1))) ^ (((u##T)(v)) << 1))  // zigzag encode
#define ZIGZAG_DECODE(T, v) (((v) >> 1) ^ -((T)((v)&1)))                                 // zigzag decode

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
int32_t tsCompressInit(char* lossyColumns, float fPrecision, double dPrecision, uint32_t maxIntervals, uint32_t intervals,
                       int32_t ifAdtFse, const char* compressor);

void    tsCompressExit();

int32_t tsCompressFloatLossyImp(const char *const input, const int32_t nelements, char *const output);
int32_t tsDecompressFloatLossyImp(const char *const input, int32_t compressedSize, const int32_t nelements, char *const output);
int32_t tsCompressDoubleLossyImp(const char *const input, const int32_t nelements, char *const output);
int32_t tsDecompressDoubleLossyImp(const char *const input, int32_t compressedSize, const int32_t nelements, char *const output);

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

/*************************************************************************
 *                  REGULAR COMPRESSION
 *************************************************************************/
int32_t tsCompressTimestamp(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                            int32_t nBuf);
int32_t tsDecompressTimestamp(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg,
                              void *pBuf, int32_t nBuf);
int32_t tsCompressFloat(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                        int32_t nBuf);
int32_t tsDecompressFloat(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                          int32_t nBuf);
int32_t tsCompressDouble(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                         int32_t nBuf);
int32_t tsDecompressDouble(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                           int32_t nBuf);
int32_t tsCompressString(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                         int32_t nBuf);
int32_t tsDecompressString(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                           int32_t nBuf);
int32_t tsCompressBool(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                       int32_t nBuf);
int32_t tsDecompressBool(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                         int32_t nBuf);
int32_t tsCompressTinyint(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                          int32_t nBuf);
int32_t tsDecompressTinyint(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                            int32_t nBuf);
int32_t tsCompressSmallint(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                           int32_t nBuf);
int32_t tsDecompressSmallint(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg,
                             void *pBuf, int32_t nBuf);
int32_t tsCompressInt(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                      int32_t nBuf);
int32_t tsDecompressInt(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                        int32_t nBuf);
int32_t tsCompressBigint(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                         int32_t nBuf);
int32_t tsDecompressBigint(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                           int32_t nBuf);
// for internal usage
int32_t getWordLength(char type);

int32_t tsDecompressIntImpl_Hw(const char *const input, const int32_t nelements, char *const output, const char type);
int32_t tsDecompressFloatImplAvx512(const char *const input, const int32_t nelements, char *const output);
int32_t tsDecompressFloatImplAvx2(const char *const input, const int32_t nelements, char *const output);
int32_t tsDecompressTimestampAvx512(const char* const input, const int32_t nelements, char *const output, bool bigEndian);
int32_t tsDecompressTimestampAvx2(const char* const input, const int32_t nelements, char *const output, bool bigEndian);

/*************************************************************************
 *                  STREAM COMPRESSION
 *************************************************************************/
typedef struct SCompressor SCompressor;

int32_t tCompressorCreate(SCompressor **ppCmprsor);
int32_t tCompressorDestroy(SCompressor *pCmprsor);
int32_t tCompressStart(SCompressor *pCmprsor, int8_t type, int8_t cmprAlg);
int32_t tCompressEnd(SCompressor *pCmprsor, const uint8_t **ppOut, int32_t *nOut, int32_t *nOrigin);
int32_t tCompress(SCompressor *pCmprsor, const void *pData, int64_t nData);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_COMPRESSION_H_*/