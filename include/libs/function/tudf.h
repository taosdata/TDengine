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

#undef malloc
#define malloc malloc
#undef free
#define free free
#undef realloc
#define alloc alloc
#include <taosudf.h>

#include <stdbool.h>
#include <stdint.h>
#include "function.h"
#include "tcommon.h"
#include "tdatablock.h"
#include "tmsg.h"

#ifdef __cplusplus
extern "C" {
#endif

#define UDF_LISTEN_PIPE_NAME_LEN 32
#ifdef _WIN32
#define UDF_LISTEN_PIPE_NAME_PREFIX "\\\\?\\pipe\\udfd.sock"
#else
#define UDF_LISTEN_PIPE_NAME_PREFIX ".udfd.sock."
#endif
#define UDF_DNODE_ID_ENV_NAME "DNODE_ID"

// low level APIs
/**
 * setup udf
 * @param udf, in
 * @param funcHandle, out
 * @return error code
 */
int32_t doSetupUdf(char udfName[], UdfcFuncHandle *funcHandle);
// output: interBuf
int32_t doCallUdfAggInit(UdfcFuncHandle handle, SUdfInterBuf *interBuf);
// input: block, state
// output: newState
int32_t doCallUdfAggProcess(UdfcFuncHandle handle, SSDataBlock *block, SUdfInterBuf *state, SUdfInterBuf *newState);
// input: interBuf
// output: resultData
int32_t doCallUdfAggFinalize(UdfcFuncHandle handle, SUdfInterBuf *interBuf, SUdfInterBuf *resultData);
// input: interbuf1, interbuf2
// output: resultBuf
int32_t doCallUdfAggMerge(UdfcFuncHandle handle, SUdfInterBuf *interBuf1, SUdfInterBuf *interBuf2,
                          SUdfInterBuf *resultBuf);
// input: block
// output: resultData
int32_t doCallUdfScalarFunc(UdfcFuncHandle handle, SScalarParam *input, int32_t numOfCols, SScalarParam *output);
/**
 * tearn down udf
 * @param handle
 * @return
 */
int32_t doTeardownUdf(UdfcFuncHandle handle);

void freeUdfInterBuf(SUdfInterBuf *buf);

// high level APIs
bool    udfAggGetEnv(struct SFunctionNode *pFunc, SFuncExecEnv *pEnv);
bool    udfAggInit(struct SqlFunctionCtx *pCtx, struct SResultRowEntryInfo *pResultCellInfo);
int32_t udfAggProcess(struct SqlFunctionCtx *pCtx);
int32_t udfAggFinalize(struct SqlFunctionCtx *pCtx, SSDataBlock *pBlock);

int32_t callUdfScalarFunc(char *udfName, SScalarParam *input, int32_t numOfCols, SScalarParam *output);

int32_t cleanUpUdfs();

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// udf api
/**
 * create udfd proxy, called once in process that call doSetupUdf/callUdfxxx/doTeardownUdf
 * @return error code
 */
int32_t udfcOpen();

/**
 * destroy udfd proxy
 * @return error code
 */
int32_t udfcClose();

/**
 * start udfd that serves udf function invocation under dnode startDnodeId
 * @param startDnodeId
 * @return
 */
int32_t udfStartUdfd(int32_t startDnodeId);
/**
 * stop udfd
 * @return
 */
int32_t udfStopUdfd();

/**
 * get udfd pid
 *
 */
 int32_t udfGetUdfdPid(int32_t* pUdfdPid);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TUDF_H
