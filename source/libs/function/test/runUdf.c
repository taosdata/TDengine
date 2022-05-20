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
  return taosCreateLog(logName, 1, configDir, NULL, NULL, NULL, NULL, 0);
}

int scalarFuncTest() {
  UdfcFuncHandle handle;

  if (doSetupUdf("udf1", &handle) != 0) {
    fnError("setup udf failure");
    return -1;
  }
  int64_t beg = taosGetTimestampUs();
  for (int k = 0; k < 1; ++k) {
    SSDataBlock  block = {0};
    SSDataBlock *pBlock = &block;
    pBlock->pDataBlock = taosArrayInit(1, sizeof(SColumnInfoData));
    pBlock->info.numOfCols = 1;
    pBlock->info.rows = 1024;
    for (int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
      SColumnInfoData colInfo = {0};
      colInfo.info.type = TSDB_DATA_TYPE_INT;
      colInfo.info.bytes = sizeof(int32_t);
      colInfo.info.colId = 1;
      colInfoDataEnsureCapacity(&colInfo, 0, pBlock->info.rows);
      for (int32_t j = 0; j < pBlock->info.rows; ++j) {
        colDataAppendInt32(&colInfo, j, &j);
      }
      taosArrayPush(pBlock->pDataBlock, &colInfo);
    }

    SScalarParam input = {0};
    input.numOfRows = pBlock->info.rows;
    input.columnData = taosArrayGet(pBlock->pDataBlock, 0);
    SScalarParam output = {0};
    doCallUdfScalarFunc(handle, &input, 1, &output);
    taosArrayDestroy(pBlock->pDataBlock);
    SColumnInfoData *col = output.columnData;
    for (int32_t i = 0; i < output.numOfRows; ++i) {
      if (i % 100 == 0)
        fprintf(stderr, "%d\t%d\n", i, *(int32_t *)(col->pData + i * sizeof(int32_t)));
    }
    colDataDestroy(output.columnData);
    taosMemoryFree(output.columnData);
  }
  int64_t end = taosGetTimestampUs();
  fprintf(stderr, "time: %f\n", (end-beg)/1000.0);
  doTeardownUdf(handle);

  return 0;
}

int aggregateFuncTest() {
  UdfcFuncHandle handle;

  if (doSetupUdf("udf2", &handle) != 0) {
    fnError("setup udf failure");
    return -1;
  }

  SSDataBlock  block = {0};
  SSDataBlock *pBlock = &block;
  pBlock->pDataBlock = taosArrayInit(1, sizeof(SColumnInfoData));
  pBlock->info.numOfCols = 1;
  pBlock->info.rows = 1024;
  for (int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
    SColumnInfoData colInfo = {0};
    colInfo.info.type = TSDB_DATA_TYPE_INT;
    colInfo.info.bytes = sizeof(int32_t);
    colInfo.info.colId = 1;
    colInfoDataEnsureCapacity(&colInfo, 0, pBlock->info.rows);
    for (int32_t j = 0; j < pBlock->info.rows; ++j) {
      colDataAppendInt32(&colInfo, j, &j);
    }
    taosArrayPush(pBlock->pDataBlock, &colInfo);
  }

  SUdfInterBuf buf = {0};
  SUdfInterBuf newBuf = {0};
  SUdfInterBuf resultBuf = {0};
  doCallUdfAggInit(handle, &buf);
  doCallUdfAggProcess(handle, pBlock, &buf, &newBuf);
  taosArrayDestroy(pBlock->pDataBlock);

  doCallUdfAggFinalize(handle, &newBuf, &resultBuf);
  fprintf(stderr, "agg result: %f\n", *(double*)resultBuf.buf);

  freeUdfInterBuf(&buf);
  freeUdfInterBuf(&newBuf);
  freeUdfInterBuf(&resultBuf);
  doTeardownUdf(handle);

  return 0;
}

int main(int argc, char *argv[]) {
  parseArgs(argc, argv);
  initLog();
  if (taosInitCfg(configDir, NULL, NULL, NULL, NULL, 0) != 0) {
    fnError("failed to start since read config error");
    return -1;
  }

  udfcOpen();
  uv_sleep(1000);

  scalarFuncTest();
  aggregateFuncTest();
  udfcClose();
}
