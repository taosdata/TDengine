#ifndef TDENGINE_NAME_H
#define TDENGINE_NAME_H

#include "os.h"
#include "taosmsg.h"

typedef struct SDataStatis {
  int16_t colId;
  int64_t sum;
  int64_t max;
  int64_t min;
  int16_t maxIndex;
  int16_t minIndex;
  int16_t numOfNull;
} SDataStatis;

typedef struct SColumnInfoData {
  SColumnInfo info;
  void* pData;    // the corresponding block data in memory
} SColumnInfoData;

void extractTableName(const char *tableId, char *name);

char* extractDBName(const char *tableId, char *name);


#endif  // TDENGINE_NAME_H
