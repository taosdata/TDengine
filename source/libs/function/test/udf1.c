#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef LINUX
#include <unistd.h>
#endif
#ifdef WINDOWS
#include <windows.h>
#endif
#include "taosudf.h"

DLL_EXPORT int32_t udf1_init() { return 0; }

DLL_EXPORT int32_t udf1_destroy() { return 0; }

DLL_EXPORT int32_t udf1(SUdfDataBlock *block, SUdfColumn *resultCol) {
  SUdfColumnData *resultData = &resultCol->colData;
  for (int32_t i = 0; i < block->numOfRows; ++i) {
    int j = 0;
    for (; j < block->numOfCols; ++j) {
      if (udfColDataIsNull(block->udfCols[j], i)) {
        udfColDataSetNull(resultCol, i);
        break;
      }
    }
    if (j == block->numOfCols) {
      int32_t luckyNum = 1;
      udfColDataSet(resultCol, i, (char *)&luckyNum, false);
    }
  }
  // to simulate actual processing delay by udf
#ifdef LINUX
  usleep(1 * 1000);  // usleep takes sleep time in us (1 millionth of a second)
#endif
#ifdef WINDOWS
  Sleep(1);
#endif
  resultData->numOfRows = block->numOfRows;
  return 0;
}
