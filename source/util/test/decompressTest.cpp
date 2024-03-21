#include <gtest/gtest.h>
#include <stdlib.h>
#include <tcompression.h>
#include <random>

namespace {}  // namespace

TEST(utilTest, decompress_test) {
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

  memset(decompOutput, 0, 10 * 8);
  tsDecompressTimestampAvx512(reinterpret_cast<const char* const>(pOutput), 10,
                              reinterpret_cast<char* const>(decompOutput), false);

  for (int32_t i = 0; i < 10; ++i) {
    std::cout << ((int64_t*)decompOutput)[i] << std::endl;
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  int64_t tsList1[7] = {1700000000, 1700000000, 1700000000, 1700000000, 1700000000, 1700000000, 1700000900};
  int32_t len1 = tsCompressTimestamp(tsList1, sizeof(tsList1), sizeof(tsList1) / sizeof(tsList1[0]), pOutput, 7,
                                     ONE_STAGE_COMP, NULL, 0);

  memset(decompOutput, 0, 10 * 8);
  tsDecompressTimestampAvx512(reinterpret_cast<const char* const>(pOutput), 7,
                              reinterpret_cast<char* const>(decompOutput), false);

  for (int32_t i = 0; i < 7; ++i) {
    std::cout << ((int64_t*)decompOutput)[i] << std::endl;
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  int64_t tsList2[1] = {1700000000};
  int32_t len2 = tsCompressTimestamp(tsList2, sizeof(tsList2), sizeof(tsList2) / sizeof(tsList2[0]), pOutput, 1,
                                     ONE_STAGE_COMP, NULL, 0);

  memset(decompOutput, 0, 10 * 8);
  tsDecompressTimestampAvx512(reinterpret_cast<const char* const>(pOutput), 1,
                              reinterpret_cast<char* const>(decompOutput), false);

  for (int32_t i = 0; i < 1; ++i) {
    std::cout << ((int64_t*)decompOutput)[i] << std::endl;
  }
}

TEST(utilTest, decompress_perf_test) {
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
  for (int32_t k = 0; k < 10000; ++k) {
    tsDecompressTimestampAvx512(px, num, pOutput, false);
  }

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

    char* px = static_cast<char*>(taosMemoryMalloc(num * sizeof(int64_t)));
    char* pBuf = static_cast<char*>(taosMemoryMalloc(num * sizeof(int64_t) + 64));

    int32_t len =
        tsCompressTimestamp2(pList, num * sizeof(int64_t), num, px, num, cmprAlg, pBuf, num * sizeof(int64_t) + 64);

    char* pOutput = static_cast<char*>(taosMemoryCalloc(1, num * sizeof(int64_t)));
    memset(pBuf, 0, num * sizeof(int64_t) + 64);
    int32_t size =
        tsDecompressTimestamp2(px, len, num, pOutput, sizeof(int64_t) * num, cmprAlg, pBuf, num * sizeof(int64_t) + 64);
    printf("size: %d\n", size);
    for (int i = 0; i < num; i++) {
      int64_t val = *(int64_t*)(pOutput + i * sizeof(int64_t));
      int32_t ival = val;
      if (i < 10) printf("val = %d\n", ival);
    }
    taosMemoryFree(px);
    taosMemoryFree(pBuf);
    taosMemoryFree(pOutput);
  }
}
TEST(utilTest, zstdtest) {
  int32_t  num = 10000;
  int64_t* pList = static_cast<int64_t*>(taosMemoryCalloc(num, sizeof(int64_t)));
  int64_t  iniVal = 17000;

  uint32_t v = 100;

  for (int32_t i = 0; i < num; ++i) {
    iniVal += i;
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
}
