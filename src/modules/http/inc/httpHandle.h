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

#ifndef TDENGINE_HTTP_SERVER_H
#define TDENGINE_HTTP_SERVER_H

#include <stdbool.h>
#include "pthread.h"
#include "semaphore.h"
#include "tmempool.h"
#include "tsdb.h"
#include "tutil.h"

#include "http.h"
#include "httpJson.h"

#define HTTP_MAX_CMD_SIZE           1024*20
#define HTTP_MAX_BUFFER_SIZE        1024*1024*10

#define HTTP_LABEL_SIZE             8
#define HTTP_MAX_EVENTS             10
#define HTTP_BUFFER_SIZE            1024*100//100k
#define HTTP_STEP_SIZE              1024    //http message get process step by step
#define HTTP_MAX_URL                5       //http url stack size
#define HTTP_METHOD_SCANNER_SIZE    7       //http method fp size
#define HTTP_GC_TARGET_SIZE         128

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

#define HTTP_CLOSE_CONN             0
#define HTTP_KEEP_CONN              1

#define HTTP_PROCESS_ERROR          0
#define HTTP_PROCESS_SUCCESS        1

struct HttpContext;
struct HttpThread;

typedef struct {
  void *signature;
  int   expire;
  int   access;
  void *taos;
  char  id[TSDB_USER_LEN];
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
  char *  nativSql;
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
  char *  pos;
  int32_t len;
} HttpBuf;

typedef struct HttpContext {
  void *       signature;
  int          fd;
  uint32_t     accessTimes;
  uint8_t      httpVersion : 1;
  uint8_t      httpChunked : 1;
  uint8_t      httpKeepAlive : 2;  // http1.0 and not keep-alive, close connection immediately
  uint8_t      fromMemPool : 1;
  uint8_t      compress : 1;
  uint8_t      usedByEpoll : 1;
  uint8_t      usedByApp : 1;
  uint8_t      reqType;
  char         ipstr[22];
  char         user[TSDB_USER_LEN];  // parsed from auth token or login message
  char         pass[TSDB_PASSWORD_LEN];
  void *       taos;
  HttpSession *session;
  HttpEncodeMethod *  encodeMethod;
  HttpSqlCmd          singleCmd;
  HttpSqlCmds *       multiCmds;
  JsonBuf *           jsonBuf;
  pthread_mutex_t     mutex;
  struct HttpThread * pThread;
  struct HttpContext *prev, *next;
} HttpContext;

typedef struct {
  char *            buffer;
  int               bufsize;
  char *            pLast;
  char *            pCur;
  HttpBuf           method;
  HttpBuf           path[HTTP_MAX_URL];  // url: dbname/meter/query
  HttpBuf           data;                // body content
  HttpBuf           token;               // auth token
  HttpDecodeMethod *pMethod;
} HttpParser;

#define HTTP_MAX_FDS_LEN 65536

typedef struct HttpThread {
  pthread_t       thread;
  HttpContext *   pHead;
  pthread_mutex_t threadMutex;
  pthread_cond_t  fdReady;
  int             pollFd;
  int             numOfFds;
  int             threadId;
  char            label[HTTP_LABEL_SIZE];
  char            buffer[HTTP_BUFFER_SIZE];  // buffer to receive data
  HttpParser      parser;                    // parse from buffer
  bool (*processData)(HttpContext *pContext);
  struct _http_server_obj_ *pServer;  // handle passed by upper layer during pServer initialization
} HttpThread;

typedef struct _http_server_obj_ {
  char              label[HTTP_LABEL_SIZE];
  char              serverIp[16];
  short             serverPort;
  int               cacheContext;
  int               sessionExpire;
  int               numOfThreads;
  HttpDecodeMethod *methodScanner[HTTP_METHOD_SCANNER_SIZE];
  int               methodScannerLen;
  pthread_mutex_t   serverMutex;
  void *            pSessionHash;
  void *            pContextPool;
  void *            expireTimer;
  HttpThread *      pThreads;
  pthread_t         thread;
  bool (*processData)(HttpContext *pContext);
  int   requestNum;
  void *timerHandle;
  bool  online;
} HttpServer;

// http util method
bool httpCheckUsedbSql(char *sql);
void httpTimeToString(time_t t, char *buf, int buflen);

// http init method
void *httpInitServer(char *ip, short port, char *label, int numOfThreads, void *fp, void *shandle);
void httpCleanUpServer(HttpServer *pServer);

// http server connection
void httpCleanUpConnect(HttpServer *pServer);
bool httpInitConnect(HttpServer *pServer);

// http context for each client connection
HttpContext *httpCreateContext(HttpServer *pServer);
bool httpInitContext(HttpContext *pContext);
void httpCloseContextByApp(HttpContext *pContext);
void httpCloseContextByServer(HttpThread *pThread, HttpContext *pContext);

// http session method
void httpCreateSession(HttpContext *pContext, void *taos);
void httpAccessSession(HttpContext *pContext);
void httpFetchSession(HttpContext *pContext);
void httpRestoreSession(HttpContext *pContext);
void httpRemoveExpireSessions(HttpServer *pServer);
bool httpInitAllSessions(HttpServer *pServer);
void httpRemoveAllSessions(HttpServer *pServer);
void httpProcessSessionExpire(void *handle, void *tmrId);

// http request parser
void httpAddMethod(HttpServer *pServer, HttpDecodeMethod *pMethod);

// http token method
bool httpParseBasicAuthToken(HttpContext *pContext, char *token, int len);
bool httpParseTaosdAuthToken(HttpContext *pContext, char *token, int len);
bool httpGenTaosdAuthToken(HttpContext *pContext, char *token, int maxLen);

// util
bool httpUrlMatch(HttpContext *pContext, int pos, char *cmp);
bool httpProcessData(HttpContext *pContext);
bool httpReadDataImp(HttpContext *pContext);

// http request handler
void httpProcessRequest(HttpContext *pContext);

// http json printer
JsonBuf *httpMallocJsonBuf(HttpContext *pContext);
void httpFreeJsonBuf(HttpContext *pContext);

// http multicmds util

int32_t httpAddToSqlCmdBuffer(HttpContext *pContext, const char *const format, ...);
int32_t httpAddToSqlCmdBufferNoTerminal(HttpContext *pContext, const char *const format, ...);
int32_t httpAddToSqlCmdBufferWithSize(HttpContext *pContext, int mallocSize);
int32_t httpAddToSqlCmdBufferTerminal(HttpContext *pContext);

bool httpMallocMultiCmds(HttpContext *pContext, int cmdSize, int bufferSize);
bool httpReMallocMultiCmdsSize(HttpContext *pContext, int cmdSize);
bool httpReMallocMultiCmdsBuffer(HttpContext *pContext, int bufferSize);
void httpFreeMultiCmds(HttpContext *pContext);

HttpSqlCmd *httpNewSqlCmd(HttpContext *pContext);
HttpSqlCmd *httpCurrSqlCmd(HttpContext *pContext);
int httpCurSqlCmdPos(HttpContext *pContext);

void httpTrimTableName(char *name);
int httpShrinkTableName(HttpContext *pContext, int pos, char *name);
char *httpGetCmdsString(HttpContext *pContext, int pos);

extern const char *httpKeepAliveStr[];
extern const char *httpVersionStr[];

#endif
