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

#ifndef TDENGINE_HTTP_INT_H
#define TDENGINE_HTTP_INT_H

#include <stdbool.h>
#include "pthread.h"
#include "semaphore.h"
#include "tmempool.h"
#include "taosdef.h"
#include "tutil.h"
#include "zlib.h"
#include "http.h"
#include "httpCode.h"
#include "httpLog.h"
#include "httpJson.h"

#define HTTP_MAX_CMD_SIZE           1024
#define HTTP_MAX_BUFFER_SIZE        1024*1024

#define HTTP_LABEL_SIZE             8
#define HTTP_MAX_EVENTS             10
#define HTTP_BUFFER_SIZE            1024*65 //65k
#define HTTP_DECOMPRESS_BUF_SIZE    1024*64
#define HTTP_STEP_SIZE              1024    //http message get process step by step
#define HTTP_MAX_URL                5       //http url stack size
#define HTTP_METHOD_SCANNER_SIZE    7       //http method fp size
#define HTTP_GC_TARGET_SIZE         512

#define HTTP_VERSION_10             0
#define HTTP_VERSION_11             1
//#define HTTP_VERSION_12           2

#define HTTP_UNCUNKED               0
#define HTTP_CHUNKED                1

#define HTTP_KEEPALIVE_NO_INPUT     0
#define HTTP_KEEPALIVE_ENABLE       1
#define HTTP_KEEPALIVE_DISABLE      2

#define HTTP_REQTYPE_OTHERS         0
#define HTTP_REQTYPE_LOGIN          1
#define HTTP_REQTYPE_HEARTBEAT      2
#define HTTP_REQTYPE_SINGLE_SQL     3
#define HTTP_REQTYPE_MULTI_SQL      4

#define HTTP_CHECK_BODY_ERROR      -1
#define HTTP_CHECK_BODY_CONTINUE    0
#define HTTP_CHECK_BODY_SUCCESS     1

#define HTTP_READ_DATA_SUCCESS      0
#define HTTP_READ_DATA_FAILED       1

#define HTTP_WRITE_RETRY_TIMES      500
#define HTTP_WRITE_WAIT_TIME_MS     5
#define HTTP_EXPIRED_TIME           60000
#define HTTP_DELAY_CLOSE_TIME_MS    500

#define HTTP_COMPRESS_IDENTITY      0
#define HTTP_COMPRESS_GZIP          2

#define HTTP_SESSION_ID_LEN         (TSDB_USER_LEN + TSDB_PASSWORD_LEN)

typedef enum {
  HTTP_SERVER_INIT,
  HTTP_SERVER_RUNNING,
  HTTP_SERVER_CLOSING,
  HTTP_SERVER_CLOSED
} HttpServerStatus;

typedef enum {
    HTTP_CONTEXT_STATE_READY,
    HTTP_CONTEXT_STATE_HANDLING,
    HTTP_CONTEXT_STATE_DROPPING,
    HTTP_CONTEXT_STATE_CLOSED
} HttpContextState;

struct HttpContext;
struct HttpThread;

typedef struct {
  char  id[HTTP_SESSION_ID_LEN];
  int   refCount;
  void *taos;
} HttpSession;

typedef enum {
  HTTP_CMD_TYPE_UN_SPECIFIED,
  HTTP_CMD_TYPE_CREATE_DB,
  HTTP_CMD_TYPE_CREATE_STBALE,
  HTTP_CMD_TYPE_INSERT
} HttpSqlCmdType;

typedef enum { HTTP_CMD_STATE_NOT_RUN_YET, HTTP_CMD_STATE_RUN_FINISHED } HttpSqlCmdState;

typedef enum { HTTP_CMD_RETURN_TYPE_WITH_RETURN, HTTP_CMD_RETURN_TYPE_NO_RETURN } HttpSqlCmdReturnType;

typedef struct {
  // used by single cmd
  char   *nativSql;
  int32_t numOfRows;
  int32_t code;

  // these are the locations in the buffer
  int32_t tagNames[TSDB_MAX_TAGS];
  int32_t tagValues[TSDB_MAX_TAGS];
  int32_t timestamp;
  int32_t metric;
  int32_t stable;
  int32_t table;
  int32_t values;
  int32_t sql;

  // used by multi-cmd
  int8_t cmdType;
  int8_t cmdReturnType;
  int8_t cmdState;
  int8_t tagNum;
} HttpSqlCmd;

typedef struct {
  HttpSqlCmd *cmds;
  int16_t     pos;
  int16_t     size;
  int16_t     maxSize;
  int32_t     bufferPos;
  int32_t     bufferSize;
  char *      buffer;
} HttpSqlCmds;

typedef struct {
  char *module;
  bool (*decodeFp)(struct HttpContext *pContext);
} HttpDecodeMethod;

typedef struct {
  void (*startJsonFp)(struct HttpContext *pContext, HttpSqlCmd *cmd, void *result);
  void (*stopJsonFp)(struct HttpContext *pContext, HttpSqlCmd *cmd);
  bool (*buildQueryJsonFp)(struct HttpContext *pContext, HttpSqlCmd *cmd, void *result, int numOfRows);
  void (*buildAffectRowJsonFp)(struct HttpContext *pContext, HttpSqlCmd *cmd, int affectRows);
  void (*initJsonFp)(struct HttpContext *pContext);
  void (*cleanJsonFp)(struct HttpContext *pContext);
  bool (*checkFinishedFp)(struct HttpContext *pContext, HttpSqlCmd *cmd, int code);
  void (*setNextCmdFp)(struct HttpContext *pContext, HttpSqlCmd *cmd, int code);
} HttpEncodeMethod;

typedef struct {
  char   *pos;
  int32_t len;
} HttpBuf;

typedef struct {
  char              buffer[HTTP_BUFFER_SIZE];
  int               bufsize;
  char             *pLast;
  char             *pCur;
  HttpBuf           method;
  HttpBuf           path[HTTP_MAX_URL];  // url: dbname/meter/query
  HttpBuf           data;                // body content
  HttpBuf           token;               // auth token
  HttpDecodeMethod *pMethod;
} HttpParser;

typedef struct HttpContext {
  int32_t      refCount;
  int          fd;
  uint32_t     accessTimes;
  uint32_t     lastAccessTime;
  int32_t      state;
  uint8_t      httpVersion;
  uint8_t      httpChunked;
  uint8_t      httpKeepAlive;  // http1.0 and not keep-alive, close connection immediately
  uint8_t      acceptEncoding;
  uint8_t      contentEncoding;
  uint8_t      reqType;
  uint8_t      parsed;
  char         ipstr[22];
  char         user[TSDB_USER_LEN];  // parsed from auth token or login message
  char         pass[TSDB_PASSWORD_LEN];
  void *       taos;
  void *       ppContext;
  HttpSession *session;
  z_stream     gzipStream;
  HttpParser   parser;
  HttpSqlCmd   singleCmd;
  HttpSqlCmds *multiCmds;
  JsonBuf *    jsonBuf;
  void *       timer;
  HttpEncodeMethod * encodeMethod;
  struct HttpThread *pThread;
} HttpContext;

typedef struct HttpThread {
  pthread_t       thread;
  HttpContext *   pHead;
  pthread_mutex_t threadMutex;
  bool            stop;
  int             pollFd;
  int             numOfContexts;
  int             threadId;
  char            label[HTTP_LABEL_SIZE];
  bool (*processData)(HttpContext *pContext);
} HttpThread;

typedef struct HttpServer {
  char              label[HTTP_LABEL_SIZE];
  uint32_t          serverIp;
  uint16_t          serverPort;
  int               fd;
  int               numOfThreads;
  int               methodScannerLen;
  int32_t           requestNum;
  int32_t           status;
  pthread_t         thread;
  HttpThread *      pThreads;
  void *            contextCache;
  void *            sessionCache;
  pthread_mutex_t   serverMutex;
  HttpDecodeMethod *methodScanner[HTTP_METHOD_SCANNER_SIZE];
  bool (*processData)(HttpContext *pContext);
} HttpServer;

extern const char *httpKeepAliveStr[];
extern const char *httpVersionStr[];
extern HttpServer  tsHttpServer;

#endif
