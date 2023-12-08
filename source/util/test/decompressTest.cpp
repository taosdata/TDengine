#include <gtest/gtest.h>
#include <stdlib.h>
#include <tcompression.h>
#include <random>

namespace {}  // namespace

TEST(utilTest, decompress_test) {
  int64_t tsList[10] = {1700000000, 1700000100, 1700000200, 1700000300, 1700000400,
                        1700000500, 1700000600, 1700000700, 1700000800, 1700000900};

  char* pOutput[10 * sizeof(int64_t)] = {0};
  int32_t len = tsCompressTimestamp(tsList, sizeof(tsList), sizeof(tsList) / sizeof(tsList[0]), pOutput, 10, ONE_STAGE_COMP, NULL, 0);

  char* decompOutput[10 * 8] = {0};
  tsDecompressTimestamp(pOutput, len, 10, decompOutput, sizeof(int64_t)*10, ONE_STAGE_COMP, NULL, 0);

  for(int32_t i = 0; i < 10; ++i) {
    std::cout<< ((int64_t*)decompOutput)[i] << std::endl;
  }

  memset(decompOutput, 0, 10*8);
  tsDecompressTimestampAvx512(reinterpret_cast<const char* const>(pOutput), 10,
                            reinterpret_cast<char* const>(decompOutput), false);

  for(int32_t i = 0; i < 10; ++i) {
    std::cout<<((int64_t*)decompOutput)[i] << std::endl;
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  int64_t tsList1[7] = {1700000000, 1700000000, 1700000000, 1700000000, 1700000000, 1700000000, 1700000900};
  int32_t len1 = tsCompressTimestamp(tsList1, sizeof(tsList1), sizeof(tsList1) / sizeof(tsList1[0]), pOutput, 7, ONE_STAGE_COMP, NULL, 0);

  memset(decompOutput, 0, 10*8);
  tsDecompressTimestampAvx512(reinterpret_cast<const char* const>(pOutput), 7,
                            reinterpret_cast<char* const>(decompOutput), false);

  for(int32_t i = 0; i < 7; ++i) {
    std::cout<<((int64_t*)decompOutput)[i] << std::endl;
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  int64_t tsList2[1] = {1700000000};
  int32_t len2 = tsCompressTimestamp(tsList2, sizeof(tsList2), sizeof(tsList2) / sizeof(tsList2[0]), pOutput, 1, ONE_STAGE_COMP, NULL, 0);

  memset(decompOutput, 0, 10*8);
  tsDecompressTimestampAvx512(reinterpret_cast<const char* const>(pOutput), 1,
                            reinterpret_cast<char* const>(decompOutput), false);

  for(int32_t i = 0; i < 1; ++i) {
    std::cout<<((int64_t*)decompOutput)[i] << std::endl;
  }
}

TEST(utilTest, decompress_perf_test) {
  int32_t num = 10000;

  int64_t* pList = static_cast<int64_t*>(taosMemoryCalloc(num, sizeof(int64_t)));
  int64_t iniVal = 1700000000;

  uint32_t v = 100;

  for(int32_t i = 0; i < num; ++i) {
    iniVal += taosRandR(&v)%10;
    pList[i] = iniVal;
  }

  char* px = static_cast<char*>(taosMemoryMalloc(num * sizeof(int64_t)));
  int32_t len = tsCompressTimestamp(pList, num * sizeof(int64_t), num, px, num, ONE_STAGE_COMP, NULL, 0);

  char* pOutput = static_cast<char*>(taosMemoryMalloc(num * sizeof(int64_t)));

  int64_t st = taosGetTimestampUs();
  for(int32_t k = 0; k < 10000; ++k) {
    tsDecompressTimestamp(px, len, num, pOutput, sizeof(int64_t) * num, ONE_STAGE_COMP, NULL, 0);
  }

  int64_t el1 = taosGetTimestampUs() - st;
  std::cout << "soft decompress elapsed time:" << el1 << " us" << std::endl;

  memset(pOutput, 0, num * sizeof(int64_t));
  st = taosGetTimestampUs();
  for(int32_t k = 0; k < 10000; ++k) {
    tsDecompressTimestampAvx512(px, num, pOutput, false);
  }

  int64_t el2 = taosGetTimestampUs() - st;
  std::cout << "SIMD decompress elapsed time:" << el2 << " us" << std::endl;

  taosMemoryFree(pList);
  taosMemoryFree(pOutput);
  taosMemoryFree(px);
}

