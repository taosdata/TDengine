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

typedef struct SUdfInfo {
  char   *udfName;        // function name
  int32_t udfType;    // scalar function or aggregate function
  int8_t    scriptType;
  char *path;

  int8_t  resType;     // result type
  int16_t resBytes;    // result byte
  int32_t bufSize;     //interbuf size

} SUdfInfo;

typedef void *UdfHandle;

/**
 * setup udf
 * @param udf, in
 * @param handle, out
 * @return error code
 */
int32_t setupUdf(SUdfInfo* udf, UdfHandle *handle);


enum {
  TSDB_UDF_STEP_NORMAL = 0,
  TSDB_UDF_STEP_MERGE,
  TSDb_UDF_STEP_FINALIZE,
  TSDB_UDF_STEP_MAX_NUM
};
/**
 * call udf
 * @param handle udf handle
 * @param step
 * @param state
 * @param stateSize
 * @param input
 * @param newstate
 * @param newStateSize
 * @param output
 * @return error code
 */

//TODO: must change the following after metadata flow and data flow between qworker and udfd is well defined
typedef struct SUdfDataBlock {
  char* data;
  int32_t size;
} SUdfDataBlock;

int32_t callUdf(UdfHandle handle, int8_t step, char *state, int32_t stateSize, SUdfDataBlock input, char **newstate,
                int32_t *newStateSize, SUdfDataBlock *output);

/**
 * tearn down udf
 * @param handle
 * @return
 */
int32_t teardownUdf(UdfHandle handle);

// end API to taosd and qworker
//=============================================================================================================================
// TODO: Must change
// begin API to UDF writer.

// script

//typedef int32_t (*scriptInitFunc)(void* pCtx);
//typedef void (*scriptNormalFunc)(void* pCtx, char* data, int16_t iType, int16_t iBytes, int32_t numOfRows,
//                                 int64_t* ptList, int64_t key, char* dataOutput, char* tsOutput, int32_t* numOfOutput,
//                                 int16_t oType, int16_t oBytes);
//typedef void (*scriptFinalizeFunc)(void* pCtx, int64_t key, char* dataOutput, int32_t* numOfOutput);
//typedef void (*scriptMergeFunc)(void* pCtx, char* data, int32_t numOfRows, char* dataOutput, int32_t* numOfOutput);
//typedef void (*scriptDestroyFunc)(void* pCtx);

// dynamic lib
typedef int32_t (*TUdfInitFunc)();
typedef void (*TUdfDestroyFunc)();

typedef void (*TUdfFunc)(int8_t step,
                         char *state, int32_t stateSize, SUdfDataBlock input,
                         char **newstate, int32_t *newStateSize, SUdfDataBlock *output);

//typedef void (*udfMergeFunc)(char *data, int32_t numOfRows, char *dataOutput, int32_t* numOfOutput);
//typedef void (*udfFinalizeFunc)(char* state, int32_t stateSize, SUdfDataBlock *output);

// end API to UDF writer
//=======================================================================================================================

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TUDF_H
