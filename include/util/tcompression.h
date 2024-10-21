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

// start compress flag
// |----l1 compAlg----|-----l2 compAlg---|---level--|
// |------8bit--------|------16bit-------|---8bit---|
#define COMPRESS_L1_TYPE_U32(type)       (((type) >> 24) & 0xFF)
#define COMPRESS_L2_TYPE_U32(type)       (((type) >> 8) & 0xFFFF)
#define COMPRESS_L2_TYPE_LEVEL_U32(type) ((type)&0xFF)
// compress flag
// |----l2lel--|----l2Alg---|---l1Alg--|
// |----2bit---|----3bit----|---3bit---|
#define COMPRESS_L1_TYPE_U8(type)       ((type)&0x07)
#define COMPRESS_L2_TYPE_U8(type)       (((type) >> 3) & 0x07)
#define COMPRESS_L2_TYPE_LEVEL_U8(type) (((type) >> 6) & 0x03)
// end compress flag

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
void tsCompressInit(char *lossyColumns, float fPrecision, double dPrecision, uint32_t maxIntervals, uint32_t intervals,
                    int32_t ifAdtFse, const char *compressor);

void tsCompressExit();

int32_t tsCompressFloatLossyImp(const char *const input, const int32_t nelements, char *const output);
int32_t tsDecompressFloatLossyImp(const char *const input, int32_t compressedSize, const int32_t nelements,
                                  char *const output);
int32_t tsCompressDoubleLossyImp(const char *const input, const int32_t nelements, char *const output);
int32_t tsDecompressDoubleLossyImp(const char *const input, int32_t compressedSize, const int32_t nelements,
                                   char *const output);

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

#ifdef __AVX2__
int32_t tsDecompressIntImpl_Hw(const char *const input, const int32_t nelements, char *const output, const char type);
int32_t tsDecompressFloatImpAvx2(const char *input, int32_t nelements, char *output);
int32_t tsDecompressDoubleImpAvx2(const char *input, int32_t nelements, char *output);
#endif
#ifdef __AVX512VL__
void tsDecompressTimestampAvx2(const char *input, int32_t nelements, char *output, bool bigEndian);
void tsDecompressTimestampAvx512(const char *const input, const int32_t nelements, char *const output, bool bigEndian);
#endif

/*************************************************************************
 *                  REGULAR COMPRESSION 2
 *************************************************************************/
int32_t tsCompressTimestamp2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg,
                             void *pBuf, int32_t nBuf);
int32_t tsDecompressTimestamp2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg,
                               void *pBuf, int32_t nBuf);
int32_t tsCompressFloat2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg, void *pBuf,
                         int32_t nBuf);
int32_t tsDecompressFloat2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg, void *pBuf,
                           int32_t nBuf);
int32_t tsCompressDouble2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg, void *pBuf,
                          int32_t nBuf);
int32_t tsDecompressDouble2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg,
                            void *pBuf, int32_t nBuf);
int32_t tsCompressString2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg, void *pBuf,
                          int32_t nBuf);
int32_t tsDecompressString2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg,
                            void *pBuf, int32_t nBuf);
int32_t tsCompressBool2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg, void *pBuf,
                        int32_t nBuf);
int32_t tsDecompressBool2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg, void *pBuf,
                          int32_t nBuf);
int32_t tsCompressTinyint2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg, void *pBuf,
                           int32_t nBuf);
int32_t tsDecompressTinyint2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg,
                             void *pBuf, int32_t nBuf);
int32_t tsCompressSmallint2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg,
                            void *pBuf, int32_t nBuf);
int32_t tsDecompressSmallint2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg,
                              void *pBuf, int32_t nBuf);
int32_t tsCompressInt2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg, void *pBuf,
                       int32_t nBuf);
int32_t tsDecompressInt2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg, void *pBuf,
                         int32_t nBuf);
int32_t tsCompressBigint2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg, void *pBuf,
                          int32_t nBuf);
int32_t tsDecompressBigint2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg,
                            void *pBuf, int32_t nBuf);

/*************************************************************************
 *                  STREAM COMPRESSION
 *************************************************************************/
typedef struct SCompressor SCompressor;

int32_t tCompressorCreate(SCompressor **ppCmprsor);
int32_t tCompressorDestroy(SCompressor *pCmprsor);
int32_t tCompressStart(SCompressor *pCmprsor, int8_t type, int8_t cmprAlg);
int32_t tCompressEnd(SCompressor *pCmprsor, const uint8_t **ppOut, int32_t *nOut, int32_t *nOrigin);
int32_t tCompress(SCompressor *pCmprsor, const void *pData, int64_t nData);

typedef int32_t (*__data_compress_init)(char *lossyColumns, float fPrecision, double dPrecision, uint32_t maxIntervals,
                                        uint32_t intervals, int32_t ifAdtFse, const char *compressor);
typedef int32_t (*__data_compress_l1_fn_t)(const char *const input, const int32_t nelements, char *const output,
                                           const char type);
typedef int32_t (*__data_decompress_l1_fn_t)(const char *const input, int32_t ninput, const int32_t nelements,
                                             char *const output, const char type);

typedef int32_t (*__data_compress_l2_fn_t)(const char *const input, const int32_t nelements, char *const output,
                                           int32_t outputSize, const char type, int8_t level);
typedef int32_t (*__data_decompress_l2_fn_t)(const char *const input, const int32_t nelements, char *const output,
                                             int32_t outputSize, const char type);

typedef struct {
  char                     *name;
  __data_compress_init      initFn;
  __data_compress_l1_fn_t   comprFn;
  __data_decompress_l1_fn_t decomprFn;
} TCmprL1FnSet;

typedef struct {
  char                     *name;
  __data_compress_init      initFn;
  __data_compress_l2_fn_t   comprFn;
  __data_decompress_l2_fn_t decomprFn;
} TCmprL2FnSet;

typedef enum {
  L1_UNKNOWN = 0,
  L1_SIMPLE_8B,
  L1_XOR,
  L1_RLE,
  L1_DELTAD,
  L1_DISABLED = 0xFF,
} TCmprL1Type;

typedef enum {
  L2_UNKNOWN = 0,
  L2_LZ4,
  L2_ZLIB,
  L2_ZSTD,
  L2_TSZ,
  L2_XZ,
  L2_DISABLED = 0xFF,
} TCmprL2Type;

typedef enum {
  L2_LVL_NOCHANGE = 0,
  L2_LVL_LOW,
  L2_LVL_MEDIUM,
  L2_LVL_HIGH,
  L2_LVL_DISABLED = 0xFF,
} TCmprLvlType;

typedef struct {
  char   *name;
  uint8_t lvl[3];  // l[0] = 'low', l[1] = 'mid', l[2] = 'high'
} TCmprLvlSet;

void tcompressDebug(uint32_t cmprAlg, uint8_t *l1Alg, uint8_t *l2Alg, uint8_t *level);

#define DEFINE_VAR(cmprAlg)                   \
  uint8_t l1 = COMPRESS_L1_TYPE_U32(cmprAlg); \
  uint8_t l2 = COMPRESS_L2_TYPE_U32(cmprAlg); \
  uint8_t lvl = COMPRESS_L2_TYPE_LEVEL_U32(cmprAlg);

#define SET_COMPRESS(l1, l2, lvl, cmpr) \
  do {                                  \
    (cmpr) &= 0x00FFFFFF;               \
    (cmpr) |= ((l1) << 24);             \
    (cmpr) &= 0xFF0000FF;               \
    (cmpr) |= ((l2) << 8);              \
    (cmpr) &= 0xFFFFFF00;               \
    (cmpr) |= (lvl);                    \
  } while (0)
int8_t tUpdateCompress(uint32_t oldCmpr, uint32_t newCmpr, uint8_t l2Disabled, uint8_t lvlDisabled, uint8_t lvlDefault,
                       uint32_t *dst);
#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_COMPRESSION_H_*/
