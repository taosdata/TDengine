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

enum { TSDB_UDF_FUNC_NORMAL = 0, TSDB_UDF_FUNC_INIT, TSDB_UDF_FUNC_FINALIZE, TSDB_UDF_FUNC_MAX_NUM };



typedef struct SUdfInit{
 int32_t maybe_null;       /* 1 if function can return NULL */
 uint32_t decimals;     /* for real functions */
 uint64_t length;       /* For string functions */
 char  *ptr;            /* free pointer for function data */
 int32_t const_item;       /* 0 if result is independent of arguments */
} SUdfInit;


typedef struct SUdfInfo {
  int32_t functionId;  // system assigned function id
  int32_t funcType;    // scalar function or aggregate function
  int8_t  resType;     // result type
  int16_t resBytes;    // result byte
  int32_t contLen;     // content length
  char   *name;        // function name
  void   *handle;      // handle loaded in mem
  void   *funcs[TSDB_UDF_FUNC_MAX_NUM];     // function ptr
  SUdfInit init;
  union {              // file path or [in memory] binary content
    char *content;
    char *path;
  };
} SUdfInfo;

typedef void (*udfNormalFunc)(char* data, int8_t type, int32_t numOfRows, int64_t* ts, char* dataOutput, char* tsOutput,
                        int32_t* numOfOutput, char* buf);
typedef void (*udfInitFunc)(SUdfInit* data);
typedef void (*udfFinalizeFunc)(char* data, int8_t type, int32_t numOfRows, int64_t* ts, char* dataOutput, char* tsOutput,
                        int32_t* numOfOutput, char* buf);


#endif  // TDENGINE_QUDF_H
