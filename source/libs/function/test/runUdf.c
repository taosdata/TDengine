#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "uv.h"

#include "fnLog.h"
#include "os.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tudf.h"

static int32_t parseArgs(int32_t argc, char *argv[]) {
  for (int32_t i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-c") == 0) {
      if (i < argc - 1) {
        if (strlen(argv[++i]) >= PATH_MAX) {
          printf("config file path overflow");
          return -1;
        }
        tstrncpy(configDir, argv[i], PATH_MAX);
      } else {
        printf("'-c' requires a parameter, default is %s\n", configDir);
        return -1;
      }
    }
  }

  return 0;
}

static int32_t initLog() {
  char logName[12] = {0};
  snprintf(logName, sizeof(logName), "%slog", "udfc");
  return taosCreateLog(logName, 1, NULL, configDir, NULL, NULL, NULL, 0);
}

int main(int argc, char *argv[]) {
  parseArgs(argc, argv);
  initLog();
  if (taosInitCfg(NULL, configDir, NULL, NULL, NULL, 0) != 0) {
    fnError("failed to start since read config error");
    return -1;
  }

  udfcOpen();
  uv_sleep(1000);

  UdfcFuncHandle handle;

  setupUdf("udf1", &handle);

  SSDataBlock  block = {0};
  SSDataBlock *pBlock = &block;
  pBlock->pDataBlock = taosArrayInit(1, sizeof(SColumnInfoData));
  pBlock->info.numOfCols = 1;
  pBlock->info.rows = 4;
  char data[16] = {0};
  char bitmap[4] = {0};
  for (int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
    SColumnInfoData colInfo = {0};
    colInfo.info.type = TSDB_DATA_TYPE_INT;
    colInfo.info.bytes = sizeof(int32_t);
    colInfo.info.colId = 1;
    colInfo.pData = data;
    colInfo.nullbitmap = bitmap;
    for (int32_t j = 0; j < pBlock->info.rows; ++j) {
      colDataAppendInt32(&colInfo, j, &j);
    }
    taosArrayPush(pBlock->pDataBlock, &colInfo);
  }

  SScalarParam input = {0};
  input.numOfRows = pBlock->info.rows;
  input.columnData = taosArrayGet(pBlock->pDataBlock, 0);
  SScalarParam output = {0};
  callUdfScalarFunc(handle, &input, 1, &output);

  SColumnInfoData *col = output.columnData;
  for (int32_t i = 0; i < output.numOfRows; ++i) {
    fprintf(stderr, "%d\t%d\n", i, *(int32_t *)(col->pData + i * sizeof(int32_t)));
  }
  teardownUdf(handle);
  udfcClose();
}
