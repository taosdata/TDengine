//
// Created by shenglian on 28/02/22.
//

#ifndef UDF_UDF_H
#define UDF_UDF_H

#include <stdlib.h>
#define DEBUG
#ifdef DEBUG
#define debugPrint(...) fprintf(__VA_ARGS__)
#else
#define debugPrint(...) /**/
#endif

enum {
  UDF_TASK_SETUP = 0,
  UDF_TASK_CALL = 1,
  UDF_TASK_TEARDOWN = 2

};

typedef struct SSDataBlock {
  char   *data;
  int32_t size;
} SSDataBlock;

typedef struct SUdfInfo {
  char *udfName;
  char *path;
} SUdfInfo;

typedef void *UdfcFuncHandle;

int32_t createUdfdProxy();

int32_t destroyUdfdProxy();

// int32_t setupUdf(SUdfInfo *udf, int32_t numOfUdfs, UdfcFuncHandle *handles);

int32_t setupUdf(SUdfInfo *udf, UdfcFuncHandle *handle);

int32_t callUdf(UdfcFuncHandle handle, int8_t step, char *state, int32_t stateSize, SSDataBlock input, char **newstate,
                int32_t *newStateSize, SSDataBlock *output);

int32_t doTeardownUdf(UdfcFuncHandle handle);

typedef struct SUdfSetupRequest {
  char    udfName[16];  //
  int8_t  scriptType;   // 0:c, 1: lua, 2:js
  int8_t  udfType;      // udaf, udf, udtf
  int16_t pathSize;
  char   *path;
} SUdfSetupRequest;

typedef struct SUdfSetupResponse {
  int64_t udfHandle;
} SUdfSetupResponse;

typedef struct SUdfCallRequest {
  int64_t udfHandle;
  int8_t  step;

  int32_t inputBytes;
  char   *input;

  int32_t stateBytes;
  char   *state;
} SUdfCallRequest;

typedef struct SUdfCallResponse {
  int32_t outputBytes;
  char   *output;
  int32_t newStateBytes;
  char   *newState;
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
  void  *subReq;
} SUdfRequest;

typedef struct SUdfResponse {
  int32_t msgLen;
  int64_t seqNum;

  int8_t  type;
  int32_t code;
  void   *subRsp;
} SUdfResponse;

int32_t decodeRequest(char *buf, int32_t bufLen, SUdfRequest **pRequest);
int32_t encodeResponse(char **buf, int32_t *bufLen, SUdfResponse *response);
int32_t encodeRequest(char **buf, int32_t *bufLen, SUdfRequest *request);
int32_t decodeResponse(char *buf, int32_t bufLen, SUdfResponse **pResponse);
#endif  // UDF_UDF_H
