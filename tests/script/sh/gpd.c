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
    int j = 0;
    for (; j < block->numOfCols; ++j) {
      if (udfColDataIsNull(block->udfCols[j], i)) {
        udfColDataSetNull(resultCol, i);
        break;
      }
    }
    if ( j == block->numOfCols) {
      int32_t luckyNum = 88;
      udfColDataSet(resultCol, i, (char *)&luckyNum, false);
    }
  }
  taos_init();
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 7100);
  if (taos == NULL) {
    char* errstr = "can not connect";
  }
  TAOS_RES* res = taos_query(taos, "create database if not exists gpd");
  if (taos_errno(res) != 0) {
    char* errstr = taos_errstr(res);
  } 
  res = taos_query(taos, "create table gpd.st (ts timestamp, f int) tags(t int)");
  if (taos_errno(res) != 0) {
    char* errstr = taos_errstr(res);
  } 

  taos_query(taos, "insert into gpd.t using gpd.st tags(1) values(now, 1) ");
  if (taos_errno(res) != 0) {
    char* errstr = taos_errstr(res);
  } 

  taos_query(taos, "select * from gpd.t");
  if (taos_errno(res) != 0) {
    char* errstr = taos_errstr(res);
  } 

  taos_close(taos);
  taos_cleanup();
  //to simulate actual processing delay by udf
#ifdef LINUX
  usleep(1 * 1000);   // usleep takes sleep time in us (1 millionth of a second)
#endif
#ifdef WINDOWS
  Sleep(1);
#endif
  return 0;
}
