#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "uv.h"

#include "fnLog.h"
#include "os.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tudf.h"

#define TAOSFPRINTF(stream, format, ...) ((void)fprintf(stream, format, ##__VA_ARGS__))
#define TAOSPRINTF(format, ...) ((void)printf(format, ##__VA_ARGS__))

static int32_t parseArgs(int32_t argc, char *argv[]) {
  for (int32_t i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-c") == 0) {
      if (i < argc - 1) {
        if (strlen(argv[++i]) >= PATH_MAX) {
          TAOSPRINTF("config file path overflow");
          return -1;
        }
        tstrncpy(configDir, argv[i], PATH_MAX);
      } else {
        TAOSPRINTF("'-c' requires a parameter, default is %s\n", configDir);
        return -1;
      }
    }
  }

  return 0;
}

static int32_t initLog() {
  char logName[12] = {0};
  snprintf(logName, sizeof(logName), "%slog", "udfc");
  return taosCreateLog(logName, 1, configDir, NULL, NULL, NULL, NULL, LOG_MODE_TAOSD);
}

int scalarFuncTest() {
  int32_t ret = 0;
  UdfcFuncHandle handle;

  if (doSetupUdf("udf1", &handle) != 0) {
    fnError("setup udf failure");
    return -1;
  }
  int64_t beg = taosGetTimestampUs();
  for (int k = 0; k < 1; ++k) {
    SSDataBlock  block = {0};
    SSDataBlock *pBlock = &block;
    for (int32_t i = 0; i < 1; ++i) {
      SColumnInfoData colInfo = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 1);
      ret =  blockDataAppendColInfo(pBlock, &colInfo);
      if (ret != 0) {
        fnError("failed to append column info");
        return -1;
      }
    }

    ret = blockDataEnsureCapacity(pBlock, 1024);
    if (ret != 0) {
      fnError("failed to ensure capacity");
      return -1;
    }
    pBlock->info.rows = 1024;

    SColumnInfoData *pCol = taosArrayGet(pBlock->pDataBlock, 0);
    for (int32_t j = 0; j < pBlock->info.rows; ++j) {
      colDataSetInt32(pCol, j, &j);
    }

    SScalarParam input = {0};
    input.numOfRows = pBlock->info.rows;
    input.columnData = taosArrayGet(pBlock->pDataBlock, 0);

    SScalarParam output = {0};
    ret =  doCallUdfScalarFunc(handle, &input, 1, &output);
    if (ret != 0) {
      fnError("failed to call udf scalar function");
      return -1;
    }
    taosArrayDestroy(pBlock->pDataBlock);

    SColumnInfoData *col = output.columnData;
    for (int32_t i = 0; i < output.numOfRows; ++i) {
      if (i % 100 == 0) TAOSFPRINTF(stderr, "%d\t%d\n", i, *(int32_t *)(col->pData + i * sizeof(int32_t)));
    }
    colDataDestroy(output.columnData);
    taosMemoryFree(output.columnData);
  }
  int64_t end = taosGetTimestampUs();
  TAOSFPRINTF(stderr, "time: %f\n", (end - beg) / 1000.0);
  ret = doTeardownUdf(handle);
  if (ret != 0) {
    fnError("failed to teardown udf");
    return -1;
  }

  return 0;
}

int aggregateFuncTest() {
  int32_t ret = 0;
  UdfcFuncHandle handle;

  ret = doSetupUdf("udf2", &handle);
  if (ret != 0) {
    fnError("setup udf failure, code:%d", ret);
    return -1;
  }

  SSDataBlock *pBlock = NULL;
  int32_t code = createDataBlock(&pBlock);
  if (code) {
    return code;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); ++i) {
    SColumnInfoData colInfo = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 1);
    ret = blockDataAppendColInfo(pBlock, &colInfo);
    if(ret != 0) {
      fnError( "failed to append column info. code:%d", ret);
      return -1;
    } 
  }

  ret = blockDataEnsureCapacity(pBlock, 1024);
  if (ret != 0) {
    fnError( "failed to ensure capacity. code:%d", ret);
    return -1;
  }
  pBlock->info.rows = 1024;

  SColumnInfoData *pColInfo = NULL;
  code = bdGetColumnInfoData(pBlock, 0, &pColInfo);
  if (code) {
    return code;
  }

  for (int32_t j = 0; j < pBlock->info.rows; ++j) {
    colDataSetInt32(pColInfo, j, &j);
  }

  SUdfInterBuf buf = {0};
  SUdfInterBuf newBuf = {0};
  SUdfInterBuf resultBuf = {0};
  ret  =  doCallUdfAggInit(handle, &buf);
  if (ret != 0) {
    fnError("failed to init udf. code:%d", ret);
    return -1;
  }
  ret = doCallUdfAggProcess(handle, pBlock, &buf, &newBuf);
  if (ret != 0) {
    fnError("failed to process udf. code:%d", ret);
    return -1;
  }
  taosArrayDestroy(pBlock->pDataBlock);

  ret = doCallUdfAggFinalize(handle, &newBuf, &resultBuf);
  if (ret != 0) {
    TAOSFPRINTF(stderr,"failed to finalize udf. code:%d", ret);
    return -1;
  }
  if (resultBuf.buf != NULL) {
    TAOSFPRINTF(stderr, "agg result: %f\n", *(double *)resultBuf.buf);
  } else {
    fnError("result buffer is null");
  }
  freeUdfInterBuf(&buf);
  freeUdfInterBuf(&newBuf);
  freeUdfInterBuf(&resultBuf);
  ret = doTeardownUdf(handle);
  if (ret != 0) {
    fnError("failed to teardown udf. code:%d", ret);
    return -1;
  }

  blockDataDestroy(pBlock);
  return 0;
}

int main(int argc, char *argv[]) {
  int32_t ret = 0;
  ret = parseArgs(argc, argv);
  if (ret != 0) {
    fnError("failed to parse args");
    return -1;
  }
  ret = initLog();
  if (ret != 0) {
    fnError("failed to init log");
    return -1;
  }
  if (taosInitCfg(configDir, NULL, NULL, NULL, NULL, 0) != 0) {
    fnError("failed to start since read config error");
    return -1;
  }

  if (udfcOpen() != 0) {
    fnError("failed to open udfc");
    return -1;
  }
  uv_sleep(1000);

  ret = scalarFuncTest();
  if (ret != 0) {
    fnError("failed to run scalar function test");
    return -1;
  }
  ret =  aggregateFuncTest();
  if (ret != 0) {
    fnError("failed to run aggregate function test");
    return -1;
  } 
  ret = udfcClose();
  if (ret != 0) {
    fnError("failed to close udfc");
    return -1;
  }
}
