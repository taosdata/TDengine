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

#ifdef __cplusplus
extern "C" {
#endif

//======================================================================================
//begin API to taosd and qworker
#define TSDB_UDF_MAX_COLUMNS 4

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

enum {
  TSDB_UDF_TYPE_SCALAR = 0,
  TSDB_UDF_TYPE_AGGREGATE = 1
};

enum {
  TSDB_UDF_SCRIPT_BIN_LIB = 0,
  TSDB_UDF_SCRIPT_LUA = 1,
};

typedef struct SUdfColumnMeta {
  int16_t type;
  int32_t bytes; // <0 var length, others fixed length bytes
  uint8_t precision;
  uint8_t scale;
} SUdfColumnMeta;

typedef struct SUdfInfo {
  char   *udfName;        // function name
  int32_t udfType;    // scalar function or aggregate function
  int8_t    scriptType;
  char *path;

  // known info between qworker and udf
  //  struct SUdfColumnMeta resultMeta;
  //  int32_t bufSize;     //interbuf size

} SUdfInfo;

typedef void *UdfHandle;

/**
 * setup udf
 * @param udf, in
 * @param handle, out
 * @return error code
 */
int32_t setupUdf(SUdfInfo* udf, UdfHandle *handle);


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
  SUdfColumn udfCols[TSDB_UDF_MAX_COLUMNS];
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
typedef int32_t (*TUdfInitFunc)();
typedef int32_t (*TUdfDestroyFunc)();

typedef int32_t (*TUdfScalarProcFunc)(SUdfDataBlock block, SUdfColumnData *resultData);
typedef int32_t (*TUdfAggInit)(SUdfInterBuf *buf);
typedef int32_t (*TUdfAggProcess)(SUdfDataBlock block, SUdfInterBuf *interBuf);
typedef int32_t (*TUdfAggFinalize)(SUdfInterBuf buf, SUdfColumnData *resultData);
// end API to UDF writer
//=======================================================================================================================

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TUDF_H
