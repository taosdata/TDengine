#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#ifdef LINUX
#include <unistd.h>
#endif
#ifdef WINDOWS
#include <windows.h>
#endif
#include "taosudf.h"

TAOS* taos = NULL;

DLL_EXPORT int32_t gpd_init() {
  return 0;
}

DLL_EXPORT int32_t gpd_destroy() {
  return 0;
}

DLL_EXPORT int32_t gpd(SUdfDataBlock* block, SUdfColumn *resultCol) {
  SUdfColumnMeta *meta = &resultCol->colMeta;
  meta->bytes = 4;
  meta->type = TSDB_DATA_TYPE_INT;
  meta->scale = 0;
  meta->precision = 0;

  SUdfColumnData *resultData = &resultCol->colData;
  resultData->numOfRows = block->numOfRows;
  for (int32_t i = 0; i < resultData->numOfRows; ++i) {
    int64_t* calc_ts = (int64_t*)udfColDataGetData(block->udfCols[0], i);
    char* varTbname = udfColDataGetData(block->udfCols[1], i);
    char* varDbname = udfColDataGetData(block->udfCols[2], i);

    char dbName[256] = {0};
    char tblName[256] = {0};      
    memcpy(dbName, varDataVal(varDbname), varDataLen(varDbname));
    memcpy(tblName, varDataVal(varTbname), varDataLen(varTbname));
    printf("%s, %s\n", dbName, tblName);    
    int32_t result = 0;
    udfColDataSet(resultCol, i, (char*)&result, false);    
  }

  return 0;
}
