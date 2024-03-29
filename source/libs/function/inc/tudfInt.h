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

#ifndef TDENGINE_TUDF_INT_H
#define TDENGINE_TUDF_INT_H
#ifdef __cplusplus
extern "C" {
#endif

enum {
  UDF_TASK_SETUP = 0,
  UDF_TASK_CALL = 1,
  UDF_TASK_TEARDOWN = 2

};

enum {
  TSDB_UDF_CALL_AGG_INIT = 0,
  TSDB_UDF_CALL_AGG_PROC,
  TSDB_UDF_CALL_AGG_MERGE,
  TSDB_UDF_CALL_AGG_FIN,
  TSDB_UDF_CALL_SCALA_PROC,
};

typedef struct SUdfSetupRequest {
  char udfName[TSDB_FUNC_NAME_LEN + 1];
} SUdfSetupRequest;

typedef struct SUdfSetupResponse {
  int64_t udfHandle;
  int8_t  outputType;
  int32_t bytes;
  int32_t bufSize;
} SUdfSetupResponse;

typedef struct SUdfCallRequest {
  int64_t udfHandle;
  int8_t  callType;

  SSDataBlock  block;
  SUdfInterBuf interBuf;
  SUdfInterBuf interBuf2;
  int8_t       initFirst;
} SUdfCallRequest;

typedef struct SUdfCallResponse {
  int8_t       callType;
  SSDataBlock  resultData;
  SUdfInterBuf resultBuf;
} SUdfCallResponse;

typedef struct SUdfTeardownRequest {
  int64_t udfHandle;
} SUdfTeardownRequest;

typedef struct SUdfTeardownResponse {
#ifdef WINDOWS
  size_t avoidCompilationErrors;
#endif
} SUdfTeardownResponse;

typedef struct SUdfRequest {
  int32_t msgLen;
  int64_t seqNum;

  int8_t type;
  union {
    SUdfSetupRequest    setup;
    SUdfCallRequest     call;
    SUdfTeardownRequest teardown;
  };
} SUdfRequest;

typedef struct SUdfResponse {
  int32_t msgLen;
  int64_t seqNum;

  int8_t  type;
  int32_t code;
  union {
    SUdfSetupResponse    setupRsp;
    SUdfCallResponse     callRsp;
    SUdfTeardownResponse teardownRsp;
  };
} SUdfResponse;

int32_t encodeUdfRequest(void **buf, const SUdfRequest *request);
void   *decodeUdfRequest(const void *buf, SUdfRequest *request);

int32_t encodeUdfResponse(void **buf, const SUdfResponse *response);
void   *decodeUdfResponse(const void *buf, SUdfResponse *response);

void freeUdfColumnData(SUdfColumnData *data, SUdfColumnMeta *meta);
void freeUdfColumn(SUdfColumn *col);
void freeUdfDataDataBlock(SUdfDataBlock *block);

int32_t convertDataBlockToUdfDataBlock(SSDataBlock *block, SUdfDataBlock *udfBlock);
int32_t convertUdfColumnToDataBlock(SUdfColumn *udfCol, SSDataBlock *block);

int32_t getUdfdPipeName(char *pipeName, int32_t size);
#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TUDF_INT_H
