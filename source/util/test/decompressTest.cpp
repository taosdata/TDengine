#define ALLOW_FORBID_FUNC
#include <gtest/gtest.h>
#include <stdlib.h>
#include <tcompression.h>
#include <chrono>
#include <random>
#include "ttypes.h"

TEST(utilTest, DISABLED_decompress_ts_test) {
  {
    tsSIMDEnable = 1;
    tsAVX2Supported = 1;
  }

  int64_t tsList[10] = {1700000000, 1700000100, 1700000200, 1700000300, 1700000400,
                        1700000500, 1700000600, 1700000700, 1700000800, 1700000900};

  char*   pOutput[10 * sizeof(int64_t)] = {0};
  int32_t len = tsCompressTimestamp(tsList, sizeof(tsList), sizeof(tsList) / sizeof(tsList[0]), pOutput, 10,
                                    ONE_STAGE_COMP, NULL, 0);

  char* decompOutput[10 * 8] = {0};

  tsDecompressTimestamp(pOutput, len, 10, decompOutput, sizeof(int64_t) * 10, ONE_STAGE_COMP, NULL, 0);

  for (int32_t i = 0; i < 10; ++i) {
    std::cout << ((int64_t*)decompOutput)[i] << std::endl;
  }

#ifdef __AVX512VL__
  memset(decompOutput, 0, 10 * 8);
  tsDecompressTimestampAvx512(reinterpret_cast<const char* const>(pOutput), 10,
                              reinterpret_cast<char* const>(decompOutput), false);

  for (int32_t i = 0; i < 10; ++i) {
    std::cout << ((int64_t*)decompOutput)[i] << std::endl;
  }
#endif

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  tsList[0] = 1286;
  tsList[1] = 1124;
  tsList[2] = 2681;
  tsList[3] = 2823;

  len = tsCompressTimestamp(tsList, sizeof(tsList), sizeof(tsList) / sizeof(tsList[0]), pOutput, 4, ONE_STAGE_COMP,
                            NULL, 0);

  decompOutput[4 * 8] = {0};
  tsDecompressTimestamp(pOutput, len, 4, decompOutput, sizeof(int64_t) * 4, ONE_STAGE_COMP, NULL, 0);

  for (int32_t i = 0; i < 4; ++i) {
    std::cout << ((int64_t*)decompOutput)[i] << std::endl;
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  int64_t tsList1[7] = {1700000000, 1700000000, 1700000000, 1700000000, 1700000000, 1700000000, 1700000900};
  int32_t len1 = tsCompressTimestamp(tsList1, sizeof(tsList1), sizeof(tsList1) / sizeof(tsList1[0]), pOutput, 7,
                                     ONE_STAGE_COMP, NULL, 0);

#ifdef __AVX512VL__
  memset(decompOutput, 0, 10 * 8);
  tsDecompressTimestampAvx512(reinterpret_cast<const char* const>(pOutput), 7,
                              reinterpret_cast<char* const>(decompOutput), false);

  for (int32_t i = 0; i < 7; ++i) {
    std::cout << ((int64_t*)decompOutput)[i] << std::endl;
  }
#endif

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  int64_t tsList2[1] = {1700000000};
  int32_t len2 = tsCompressTimestamp(tsList2, sizeof(tsList2), sizeof(tsList2) / sizeof(tsList2[0]), pOutput, 1,
                                     ONE_STAGE_COMP, NULL, 0);

#ifdef __AVX512VL__
  memset(decompOutput, 0, 10 * 8);
  tsDecompressTimestampAvx512(reinterpret_cast<const char* const>(pOutput), 1,
                              reinterpret_cast<char* const>(decompOutput), false);

  for (int32_t i = 0; i < 1; ++i) {
    std::cout << ((int64_t*)decompOutput)[i] << std::endl;
  }
#endif
}

TEST(utilTest, DISABLED_decompress_perf_test) {
  int32_t num = 10000;

  int64_t* pList = static_cast<int64_t*>(taosMemoryCalloc(num, sizeof(int64_t)));
  int64_t  iniVal = 1700000000;

  uint32_t v = 100;

  for (int32_t i = 0; i < num; ++i) {
    iniVal += taosRandR(&v) % 10;
    pList[i] = iniVal;
  }

  char*   px = static_cast<char*>(taosMemoryMalloc(num * sizeof(int64_t)));
  int32_t len = tsCompressTimestamp(pList, num * sizeof(int64_t), num, px, num, ONE_STAGE_COMP, NULL, 0);

  char* pOutput = static_cast<char*>(taosMemoryMalloc(num * sizeof(int64_t)));

  int64_t st = taosGetTimestampUs();
  for (int32_t k = 0; k < 10000; ++k) {
    tsDecompressTimestamp(px, len, num, pOutput, sizeof(int64_t) * num, ONE_STAGE_COMP, NULL, 0);
  }

  int64_t el1 = taosGetTimestampUs() - st;
  std::cout << "soft decompress elapsed time:" << el1 << " us" << std::endl;

  memset(pOutput, 0, num * sizeof(int64_t));
  st = taosGetTimestampUs();
#ifdef __AVX512VL__
  for (int32_t k = 0; k < 10000; ++k) {
    tsDecompressTimestampAvx512(px, num, pOutput, false);
  }
#endif

  int64_t el2 = taosGetTimestampUs() - st;
  std::cout << "SIMD decompress elapsed time:" << el2 << " us" << std::endl;

  taosMemoryFree(pList);
  taosMemoryFree(pOutput);
  taosMemoryFree(px);
}

void setColEncode(uint32_t* compress, uint8_t l1) {
  *compress &= 0x00FFFFFF;
  *compress |= (l1 << 24);
  return;
}
void setColCompress(uint32_t* compress, uint16_t l2) {
  *compress &= 0xFF0000FF;
  *compress |= (l2 << 8);
  return;
}
void setColLevel(uint32_t* compress, uint8_t level) {
  *compress &= 0xFFFFFF00;
  *compress |= level;
  return;
}

void compressImplTest(void* pVal, int8_t type, int32_t sz, uint32_t cmprAlg) {
  {
    int64_t* pList = (int64_t*)pVal;
    int32_t  num = sz;

    int64_t bytes = 0;  // tDataTypeDescriptor[TSDB_DATA_TYPE_TIMESTAMP].
    char*   px = static_cast<char*>(taosMemoryMalloc(num * sizeof(int64_t)));
    char*   pBuf = static_cast<char*>(taosMemoryMalloc(num * sizeof(int64_t) + 64));

    int32_t len = tsCompressTimestamp2(pList, num * sizeof(int64_t), num, px, num * sizeof(int64_t) + 64, cmprAlg, pBuf,
                                       num * sizeof(int64_t) + 64);
    printf("compresess size: %d, actual size: %d\n", len, (int32_t)(num * sizeof(int64_t)));
    char* pOutput = static_cast<char*>(taosMemoryCalloc(1, num * sizeof(int64_t) + 64));
    memset(pBuf, 0, num * sizeof(int64_t) + 64);

    int32_t size = tsDecompressTimestamp2(px, len, num, pOutput, sizeof(int64_t) * num + 64, cmprAlg, pBuf,
                                          num * sizeof(int64_t) + 64);
    for (int i = 0; i < num; i++) {
      int64_t val = *(int64_t*)(pOutput + i * sizeof(int64_t));
      ASSERT_EQ(val, pList[i]);
    }
    taosMemoryFree(px);
    taosMemoryFree(pBuf);
    taosMemoryFree(pOutput);
  }
}

const char* alg[] = {"disabled", "lz4", "zlib", "zstd", "tsz", "xz"};
const char* end[] = {"disabled", "simppe8b", "delta", "test", "test"};
void        compressImplTestByAlg(void* pVal, int8_t type, int32_t num, uint32_t cmprAlg) {
  {
    tDataTypeCompress compres = tDataCompress[type];
    int32_t           bytes = compres.bytes * num;

    int32_t externalSize = bytes + 64;
    char*   px = static_cast<char*>(taosMemoryMalloc(externalSize));
    char*   pBuf = static_cast<char*>(taosMemoryMalloc(externalSize));

    DEFINE_VAR(cmprAlg)
    int32_t len = compres.compFunc(pVal, bytes, num, px, externalSize, cmprAlg, pBuf, externalSize);
    printf("encode:%s, compress alg:%s, type:%s, compresess size: %d, actual size: %d, radio: %f\n", end[l1], alg[l2],
                  compres.name, len, bytes, (float)len / bytes);
    char* pOutput = static_cast<char*>(taosMemoryCalloc(1, externalSize));
    memset(pBuf, 0, externalSize);
    int32_t size = compres.decompFunc(px, len, num, pOutput, externalSize, cmprAlg, pBuf, externalSize);

    ASSERT_EQ(size, bytes);
    taosMemoryFree(px);
    taosMemoryFree(pBuf);
    taosMemoryFree(pOutput);
    // taosMemoryFree(pVal);
  }
}
int32_t fillDataByData(char* pBuf, void* pData, int32_t nBytes) {
  memcpy(pBuf, pData, nBytes);
  return 0;
}
void* genCompressData(int32_t type, int32_t num, bool order) {
  tDataTypeDescriptor desc = tDataTypes[type];

  int32_t cnt = num * (desc.bytes);

  char*    pBuf = (char*)taosMemoryCalloc(1, cnt);
  char*    p = pBuf;
  uint32_t v = taosGetTimestampMs();
  int64_t  iniVal = 0;
  for (int32_t i = 0; i < num; i++) {
    int64_t d = taosRandR(&v);
    if (type == TSDB_DATA_TYPE_BOOL) {
      int8_t val = d % 2;
      fillDataByData(p, &val, desc.bytes);
    } else if (type == TSDB_DATA_TYPE_TINYINT) {
      int8_t val = d % INT8_MAX;
      fillDataByData(p, &val, desc.bytes);
    } else if (type == TSDB_DATA_TYPE_SMALLINT) {
      int16_t val = d % INT8_MAX;
      fillDataByData(p, &val, desc.bytes);
    } else if (type == TSDB_DATA_TYPE_INT) {
      int32_t val = d % INT8_MAX;
      fillDataByData(p, &val, desc.bytes);
    } else if (type == TSDB_DATA_TYPE_BIGINT) {
      int64_t val = d % INT8_MAX;
      fillDataByData(p, &val, desc.bytes);
    }
    p += desc.bytes;
  }
  return pBuf;
}
void* genCompressData_float(int32_t type, int32_t num, bool order) {
  tDataTypeDescriptor desc = tDataTypes[type];

  int32_t cnt = num * (desc.bytes);

  char*    pBuf = (char*)taosMemoryCalloc(1, cnt);
  char*    p = pBuf;
  uint32_t v = taosGetTimestampMs();
  int64_t  iniVal = 0;
  for (int32_t i = 0; i < num; i++) {
    int64_t d = taosRandR(&v);
    if (type == TSDB_DATA_TYPE_FLOAT) {
      float f = d * 1.0 / 3;
      fillDataByData(p, &f, desc.bytes);
    } else if (type == TSDB_DATA_TYPE_DOUBLE) {
      double f = d * 1.0 / 3;
      fillDataByData(p, &f, desc.bytes);
    }
    // if (type == TSDB_DATA_TYPE_BOOL) {
    //   int8_t val = d % 2;
    //   fillDataByData(p, &val, desc.bytes);
    // } else if (type == TSDB_DATA_TYPE_TINYINT) {
    //   int8_t val = d % INT8_MAX;
    //   fillDataByData(p, &val, desc.bytes);
    // } else if (type == TSDB_DATA_TYPE_SMALLINT) {
    //   int16_t val = d % INT8_MAX;
    //   fillDataByData(p, &val, desc.bytes);
    // } else if (type == TSDB_DATA_TYPE_INT) {
    //   int32_t val = d % INT8_MAX;
    //   fillDataByData(p, &val, desc.bytes);
    // } else if (type == TSDB_DATA_TYPE_BIGINT) {
    //   int64_t val = d % INT8_MAX;
    //   fillDataByData(p, &val, desc.bytes);
    // }
    p += desc.bytes;
  }
  return pBuf;
}
TEST(utilTest, DISABLED_compressAlg) {
  int32_t  num = 4096;
  int64_t* pList = static_cast<int64_t*>(taosMemoryCalloc(num, sizeof(int64_t)));
  int64_t  iniVal = 17000;

  uint32_t v = 100;

  for (int32_t i = 0; i < num; ++i) {
    iniVal += i;
    pList[i] = iniVal;
  }
  printf("ordered data\n");
  {
    uint32_t cmprAlg = 0;
    setColCompress(&cmprAlg, 1);
    setColEncode(&cmprAlg, 1);

    compressImplTest((void*)pList, 0, num, cmprAlg);
  }

  {
    uint32_t cmprAlg = 0;
    setColCompress(&cmprAlg, 2);
    setColEncode(&cmprAlg, 1);

    compressImplTest((void*)pList, 0, num, cmprAlg);
  }
  {
    uint32_t cmprAlg = 0;
    setColCompress(&cmprAlg, 3);
    setColEncode(&cmprAlg, 1);

    compressImplTest((void*)pList, 0, num, cmprAlg);
  }
  {
    uint32_t cmprAlg = 0;
    setColCompress(&cmprAlg, 1);
    // setColEncode(&cmprAlg, 1);

    compressImplTest((void*)pList, 0, num, cmprAlg);
  }

  {
    uint32_t cmprAlg = 0;
    setColCompress(&cmprAlg, 2);
    // setColEncode(&cmprAlg, 1);

    compressImplTest((void*)pList, 0, num, cmprAlg);
  }
  {
    uint32_t cmprAlg = 0;
    setColCompress(&cmprAlg, 3);
    // setColEncode(&cmprAlg, 1);

    compressImplTest((void*)pList, 0, num, cmprAlg);
  }

  printf("unoreder data\n");
  for (int32_t i = 0; i < num; ++i) {
    iniVal = taosRandR(&v);
    pList[i] = iniVal;
  }
  {
    uint32_t cmprAlg = 0;
    setColCompress(&cmprAlg, 1);
    setColEncode(&cmprAlg, 1);

    compressImplTest((void*)pList, 0, num, cmprAlg);
  }
  {
    uint32_t cmprAlg = 0;
    setColCompress(&cmprAlg, 2);
    setColEncode(&cmprAlg, 1);

    compressImplTest((void*)pList, 0, num, cmprAlg);
  }
  {
    uint32_t cmprAlg = 0;
    setColCompress(&cmprAlg, 3);
    setColEncode(&cmprAlg, 1);

    compressImplTest((void*)pList, 0, num, cmprAlg);
  }
  printf("unoreder data, no encode\n");
  {
    uint32_t cmprAlg = 0;
    setColCompress(&cmprAlg, 1);
    // setColEncode(&cmprAlg, 0);

    compressImplTest((void*)pList, 0, num, cmprAlg);
  }
  {
    uint32_t cmprAlg = 0;
    setColCompress(&cmprAlg, 2);
    // setColEncode(&cmprAlg, 1);

    compressImplTest((void*)pList, 0, num, cmprAlg);
  }
  {
    uint32_t cmprAlg = 0;
    setColCompress(&cmprAlg, 3);
    // setColEncode(&cmprAlg, 1);

    compressImplTest((void*)pList, 0, num, cmprAlg);
  }
  taosMemoryFree(pList);

  {
    for (int8_t type = 2; type <= 5; type++) {
      printf("------summary, type: %s-------\n", tDataTypes[type].name);
      char* p = (char*)genCompressData(type, num, 0);
      for (int8_t i = 1; i <= 3; i++) {
        uint32_t cmprAlg = 0;
        setColCompress(&cmprAlg, i);
        setColEncode(&cmprAlg, 2);
        compressImplTestByAlg(p, type, num, cmprAlg);
      }
      {
        uint32_t cmprAlg = 0;
        setColCompress(&cmprAlg, 5);
        setColEncode(&cmprAlg, 2);
        compressImplTestByAlg(p, type, num, cmprAlg);
      }
      taosMemoryFree(p);
      printf("-------------");
    }
  }
  // bool
  for (int8_t type = 1; type <= 1; type++) {
    printf("------summary, type: %s-------\n", tDataTypes[type].name);
    char* p = (char*)genCompressData(type, num, 0);
    for (int8_t i = 1; i <= 3; i++) {
      uint32_t cmprAlg = 0;
      setColCompress(&cmprAlg, i);
      setColEncode(&cmprAlg, 4);
      compressImplTestByAlg(p, type, num, cmprAlg);
    }
    {
      uint32_t cmprAlg = 0;
      setColCompress(&cmprAlg, 5);
      setColEncode(&cmprAlg, 4);
      compressImplTestByAlg(p, type, num, cmprAlg);
    }
    taosMemoryFree(p);
    printf("-------------");
  }
  // float/double
  float    fPresion = 1E-8;
  double   dPresion = 1E-16;
  uint32_t maxRange = 500;                      // max quantization intervals
  uint32_t curRange = 100;                      // current quantization intervals
  bool     ifAdtFse = false;                    // ADT-FSE algorithom or original huffman algorithom
  char     compressor[32] = "ZSTD_COMPRESSOR";  // ZSTD_COMPRESSOR or GZIP_COMPRESSOR

  tsCompressInit((char*)"float|double", fPresion, dPresion, maxRange, curRange, ifAdtFse, compressor);
  for (int8_t type = 6; type <= 7; type++) {
    printf("------summary, type: %s-------\n", tDataTypes[type].name);
    char* p = (char*)genCompressData_float(type, num, 0);
    for (int8_t i = 1; i <= 3; i++) {
      uint32_t cmprAlg = 0;
      setColCompress(&cmprAlg, i);
      setColEncode(&cmprAlg, 3);
      setColLevel(&cmprAlg, 1);
      compressImplTestByAlg(p, type, num, cmprAlg);
    }
    {
      //   uint32_t cmprAlg = 0;
      //   setColCompress(&cmprAlg, 4);
      //   setColEncode(&cmprAlg, 3);
      //   compressImplTestByAlg(p, type, num, cmprAlg);
      // }
      // taosMemoryFree(p);
      // printf("-------------");
    }
    taosMemoryFree(p);
  }
}

static uint32_t decompressRandomSeed;

static void refreshSeed() {
  decompressRandomSeed = std::random_device()();
  std::cout << "Refresh random seed to " << decompressRandomSeed << "\n";
}

#ifdef __GNUC__
template <typename T>
static std::vector<typename std::enable_if<std::is_integral<T>::value, T>::type> utilTestRandomData(int32_t n, T min,
                                                                                                    T max) {
  std::mt19937   gen(decompressRandomSeed);
  std::vector<T> data(n);

  std::uniform_int_distribution<T> dist(min, max);
  for (auto& v : data) v = dist(gen);
  return data;
}

template <typename T>
static std::vector<typename std::enable_if<std::is_floating_point<T>::value, T>::type> utilTestRandomData(int32_t n,
                                                                                                          T       min,
                                                                                                          T       max) {
  std::mt19937   gen(decompressRandomSeed);
  std::vector<T> data(n);

  std::uniform_real_distribution<T> dist(min, max);
  for (auto& v : data) v = dist(gen);
  return data;
}
#else
template <typename T>
static std::vector<T> utilTestRandomData(int32_t n, T min, T max) {
  std::vector<T> data(n);

  for (int32_t i = 0; i < n; ++i) {
    data[i] = (i & 0x1) ? min : max;
  }
  return data;
}
#endif

template <typename F>
static double measureRunTime(const F& func, int32_t nround = 1) {
  auto start = std::chrono::high_resolution_clock::now();
  for (int32_t i = 0; i < nround; ++i) {
    func();
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  return duration / 1000.0;
}

template <typename F>
struct DataTypeSupportAvx {
  static const bool value = false;
};

template <>
struct DataTypeSupportAvx<float> {
  static const bool value = true;
};

template <>
struct DataTypeSupportAvx<double> {
  static const bool value = true;
};

template <typename T, typename CompF, typename DecompF>
static void decompressBasicTest(size_t dataSize, const CompF& compress, const DecompF& decompress, T min, T max) {
  auto              origData = utilTestRandomData(dataSize, min, max);
  std::vector<char> compData(origData.size() * sizeof(origData[0]) + 1);
  int32_t           cnt = compress(origData.data(), origData.size(), origData.size(), compData.data(), compData.size(),
                                   ONE_STAGE_COMP, nullptr, 0);
  ASSERT_LE(cnt, compData.size());
  decltype(origData) decompData(origData.size());

  // test simple implementation without SIMD instructions
  tsSIMDEnable = 0;
  cnt = decompress(compData.data(), compData.size(), decompData.size(), decompData.data(), decompData.size(),
                   ONE_STAGE_COMP, nullptr, 0);
  ASSERT_EQ(cnt, compData.size() - 1);
  EXPECT_EQ(origData, decompData);

#ifdef __AVX2__
  if (DataTypeSupportAvx<T>::value) {
    // test AVX2 implementation
    tsSIMDEnable = 1;
    tsAVX2Supported = 1;
    cnt = decompress(compData.data(), compData.size(), decompData.size(), decompData.data(), decompData.size(),
                     ONE_STAGE_COMP, nullptr, 0);
    ASSERT_EQ(cnt, compData.size() - 1);
    EXPECT_EQ(origData, decompData);
  }
#endif
}

template <typename T, typename CompF, typename DecompF>
static void decompressPerfTest(const char* typname, const CompF& compress, const DecompF& decompress, T min, T max) {
  constexpr size_t  DATA_SIZE = 1 * 1024 * 1024;
  constexpr int32_t NROUND = 1000;
  auto              origData = utilTestRandomData(DATA_SIZE, min, max);
  std::vector<char> compData(origData.size() * sizeof(origData[0]) + 1);
  int32_t           cnt = compress(origData.data(), origData.size(), origData.size(), compData.data(), compData.size(),
                                   ONE_STAGE_COMP, nullptr, 0);
  ASSERT_LE(cnt, compData.size());
  if (compData[0] == 1) std::cout << "NOT COMPRESSED!\n";
  std::cout << "Original size: " << compData.size() - 1 << "; Compressed size: " << cnt
            << "; Compression ratio: " << 1.0 * (compData.size() - 1) / cnt << "\n";
  decltype(origData) decompData(origData.size());

  tsSIMDEnable = 0;
  auto ms = measureRunTime(
      [&]() {
        decompress(compData.data(), compData.size(), decompData.size(), decompData.data(), decompData.size(),
                   ONE_STAGE_COMP, nullptr, 0);
      },
      NROUND);
  std::cout << "Decompression of " << NROUND * DATA_SIZE << " " << typname << " without SIMD costs " << ms
            << " ms, avg speed: " << NROUND * DATA_SIZE * 1000 / ms << " tuples/s\n";

#ifdef __AVX2__
  if (DataTypeSupportAvx<T>::value) {
    tsSIMDEnable = 1;
    tsAVX2Supported = 1;
    ms = measureRunTime(
        [&]() {
          decompress(compData.data(), compData.size(), decompData.size(), decompData.data(), decompData.size(),
                     ONE_STAGE_COMP, nullptr, 0);
        },
        NROUND);
    std::cout << "Decompression of " << NROUND * DATA_SIZE << " " << typname << " using AVX2 costs " << ms
              << " ms, avg speed: " << NROUND * DATA_SIZE * 1000 / ms << " tuples/s\n";
  }
#endif
}

#define RUN_PERF_TEST(typname, comp, decomp, min, max)             \
  do {                                                             \
    refreshSeed();                                                 \
    decompressPerfTest<typname>(#typname, comp, decomp, min, max); \
  } while (0)

TEST(utilTest, decompressTinyintBasic) {
  refreshSeed();
  for (int32_t r = 1; r <= 4096; ++r) {
    decompressBasicTest<int8_t>(r, tsCompressTinyint, tsDecompressTinyint, 0, 100);
  }
}

TEST(utilTest, decompressTinyintPerf) { RUN_PERF_TEST(int8_t, tsCompressTinyint, tsDecompressTinyint, 0, 100); }

TEST(utilTest, decompressSmallintBasic) {
  refreshSeed();
  for (int32_t r = 1; r <= 4096; ++r) {
    decompressBasicTest<int16_t>(r, tsCompressSmallint, tsDecompressSmallint, 0, 10000);
  }
}

TEST(utilTest, decompressSmallintPerf) { RUN_PERF_TEST(int16_t, tsCompressSmallint, tsDecompressSmallint, 0, 10000); }

TEST(utilTest, decompressIntBasic) {
  refreshSeed();
  for (int32_t r = 1; r <= 4096; ++r) {
    decompressBasicTest<int32_t>(r, tsCompressInt, tsDecompressInt, 0, 1000000);
  }
}

TEST(utilTest, decompressIntPerf) { RUN_PERF_TEST(int32_t, tsCompressInt, tsDecompressInt, 0, 1000000); }

TEST(utilTest, decompressBigintBasic) {
  refreshSeed();
  for (int32_t r = 1; r <= 4096; ++r) {
    decompressBasicTest<int64_t>(r, tsCompressBigint, tsDecompressBigint, 0, 1000000000L);
  }
}

TEST(utilTest, decompressBigintPerf) { RUN_PERF_TEST(int64_t, tsCompressBigint, tsDecompressBigint, 0, 1000000000L); }

TEST(utilTest, decompressFloatBasic) {
  refreshSeed();
  for (int32_t r = 1; r <= 4096; ++r) {
    decompressBasicTest<float>(r, tsCompressFloat, tsDecompressFloat, 0, 99999);
  }
}

TEST(utilTest, decompressFloatPerf) { RUN_PERF_TEST(float, tsCompressFloat, tsDecompressFloat, 0, 99999); }

TEST(utilTest, decompressDoubleBasic) {
  refreshSeed();
  for (int32_t r = 1; r <= 4096; ++r) {
    decompressBasicTest<double>(r, tsCompressDouble, tsDecompressDouble, 0, 9999999999);
  }
}

TEST(utilTest, decompressDoublePerf) { RUN_PERF_TEST(double, tsCompressDouble, tsDecompressDouble, 0, 9999999999); }


TEST(utilTest, decompressTimestampBasic) {
  refreshSeed();
  for (int32_t r = 1; r <= 4096; ++r) {
    decompressBasicTest<int64_t>(r, tsCompressTimestamp, tsDecompressTimestamp, 0, 1000000000L);
  }
}

TEST(utilTest, decompressTimestampPerf) {
  refreshSeed();
  decompressPerfTest<int64_t>("timestamp", tsCompressTimestamp, tsDecompressTimestamp, 0, 1000000000L);
}
