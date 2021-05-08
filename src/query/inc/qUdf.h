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

#ifndef TDENGINE_QUDF_H
#define TDENGINE_QUDF_H

enum { TSDB_UDF_FUNC_NORMAL = 0, TSDB_UDF_FUNC_INIT, TSDB_UDF_FUNC_FINALIZE, TSDB_UDF_FUNC_MERGE, TSDB_UDF_FUNC_DESTROY, TSDB_UDF_FUNC_MAX_NUM };



typedef struct SUdfInit{
 int32_t maybe_null;       /* 1 if function can return NULL */
 uint32_t decimals;     /* for real functions */
 uint64_t length;       /* For string functions */
 char  *ptr;            /* free pointer for function data */
 int32_t const_item;       /* 0 if result is independent of arguments */

 // script like lua/javascript
 void* script_ctx;
 void (*destroyCtxFunc)(void *script_ctx);
} SUdfInit;


typedef struct SUdfInfo {
  int32_t functionId;  // system assigned function id
  int32_t funcType;    // scalar function or aggregate function
  int8_t  resType;     // result type
  int16_t resBytes;    // result byte
  int32_t contLen;     // content length
  int32_t bufSize;     //interbuf size
  char   *name;        // function name
  void   *handle;      // handle loaded in mem
  void   *funcs[TSDB_UDF_FUNC_MAX_NUM];     // function ptr

  // for script like lua/javascript only
  int    isScript;
  void   *pScriptCtx;

  SUdfInit init;
  char *content;
  char *path;
} SUdfInfo;

//script 

typedef int32_t (*scriptInitFunc)(void *pCtx);
typedef void (*scriptNormalFunc)(void *pCtx, char* data, int16_t iType, int16_t iBytes, int32_t numOfRows,
                                 int64_t* ptList, int64_t key, char* dataOutput, char* tsOutput, int32_t* numOfOutput, int16_t oType, int16_t oBytes);
typedef void (*scriptFinalizeFunc)(void *pCtx, int64_t key, char* dataOutput, int32_t* numOfOutput);
typedef void (*scriptMergeFunc)(void *pCtx, char* data, int32_t numOfRows, char* dataOutput, int32_t* numOfOutput);
typedef void (*scriptDestroyFunc)(void* pCtx);

// dynamic lib
typedef void (*udfNormalFunc)(char* data, int16_t itype, int16_t iBytes, int32_t numOfRows, int64_t* ts, char* dataOutput, char* interBuf,
  char* tsOutput, int32_t* numOfOutput, int16_t oType, int16_t oBytes, SUdfInit* buf);
typedef int32_t (*udfInitFunc)(SUdfInit* data);
typedef void (*udfFinalizeFunc)(char* dataOutput, char *interBuf, int32_t* numOfOutput, SUdfInit* buf);
typedef void (*udfMergeFunc)(char* data, int32_t numOfRows, char* dataOutput, int32_t* numOfOutput, SUdfInit* buf);
typedef void (*udfDestroyFunc)(SUdfInit* buf);


#endif  // TDENGINE_QUDF_H
