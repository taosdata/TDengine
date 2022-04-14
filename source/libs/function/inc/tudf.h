/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef TDENGINE_TUDF_H
#define TDENGINE_TUDF_H


#include <stdint.h>
#include <stdbool.h>
#include "tmsg.h"

#ifdef __cplusplus
extern "C" {
#endif

//======================================================================================
//begin API to taosd and qworker

enum {
  UDFC_CODE_STOPPING = -1,
  UDFC_CODE_RESTARTING = -2,
};

/**
 * start udf dameon service
 * @return error code
 */
int32_t startUdfService();

/**
 * stop udf dameon service
 * @return error code
 */
int32_t stopUdfService();

typedef void *UdfHandle;

/**
 * setup udf
 * @param udf, in
 * @param handle, out
 * @return error code
 */
int32_t setupUdf(char udfName[], SEpSet epSet, UdfHandle *handle);

typedef struct SUdfColumnMeta {
  int16_t type;
  int32_t bytes; // <0 var length, others fixed length bytes
  uint8_t precision;
  uint8_t scale;
} SUdfColumnMeta;

typedef struct SUdfColumnData {
  int32_t numOfRows;
  bool varLengthColumn;
  union {
    int32_t nullBitmapLen;
    char* nullBitmap;
    int32_t dataLen;
    char* data;
  };

  union {
    int32_t varOffsetsLen;
    char* varOffsets;
    int32_t payloadLen;
    char* payload;
  };
} SUdfColumnData;


typedef struct SUdfColumn {
  SUdfColumnMeta colMeta;
  SUdfColumnData colData;
} SUdfColumn;

typedef struct SUdfDataBlock {
  int32_t numOfRows;
  int32_t numOfCols;
  SUdfColumn **udfCols;
} SUdfDataBlock;

typedef struct SUdfInterBuf {
  int32_t bufLen;
  char* buf;
} SUdfInterBuf;

// input: block, initFirst
// output: interbuf
int32_t callUdfAggProcess(SUdfDataBlock block, SUdfInterBuf *interBuf, bool initFirst);
// input: interBuf
// output: resultData
int32_t callUdfAggFinalize(SUdfInterBuf interBuf, SUdfColumnData* resultData);
// input: block
// output: resultData
int32_t callUdfScalaProcess(SUdfDataBlock block, SUdfColumnData* resultData);

/**
 * tearn down udf
 * @param handle
 * @return
 */
int32_t teardownUdf(UdfHandle handle);

// end API to taosd and qworker
//=============================================================================================================================
// begin API to UDF writer.

// dynamic lib init and destroy
typedef int32_t (*TUdfSetupFunc)();
typedef int32_t (*TUdfTeardownFunc)();

typedef int32_t (*TUdfFreeUdfColumnDataFunc)(SUdfColumnData* columnData);

typedef int32_t (*TUdfScalarProcFunc)(SUdfDataBlock block, SUdfColumnData *resultData);
typedef int32_t (*TUdfAggInitFunc)(SUdfInterBuf *buf);
typedef int32_t (*TUdfAggProcessFunc)(SUdfDataBlock block, SUdfInterBuf *interBuf);
typedef int32_t (*TUdfAggFinalizeFunc)(SUdfInterBuf buf, SUdfColumnData *resultData);


// end API to UDF writer
//=======================================================================================================================

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TUDF_H
