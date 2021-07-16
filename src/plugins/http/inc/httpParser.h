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

#ifndef HTTP_PARSER_H
#define HTTP_PARSER_H
#include "httpGzip.h"

#define HTTP_MAX_URL 6  // http url stack size

#define HTTP_CODE_CONTINUE                 100
#define HTTP_CODE_SWITCHING_PROTOCOL       101
#define HTTP_CODE_PROCESSING               102
#define HTTP_CODE_EARLY_HINTS              103
#define HTTP_CODE_OK                       200
#define HTTP_CODE_CREATED                  201
#define HTTP_CODE_ACCEPTED                 202
#define HTTP_CODE_NON_AUTHORITATIVE_INFO   203
#define HTTP_CODE_NO_CONTENT               204
#define HTTP_CODE_RESET_CONTENT            205
#define HTTP_CODE_PARTIAL_CONTENT          206
#define HTTP_CODE_MULTI_STATUS             207
#define HTTP_CODE_ALREADY_REPORTED         208
#define HTTP_CODE_IM_USED                  226
#define HTTP_CODE_MULTIPLE_CHOICE          300
#define HTTP_CODE_MOVED_PERMANENTLY        301
#define HTTP_CODE_FOUND                    302
#define HTTP_CODE_SEE_OTHER                303
#define HTTP_CODE_NOT_MODIFIED             304
#define HTTP_CODE_USE_PROXY                305
#define HTTP_CODE_UNUSED                   306
#define HTTP_CODE_TEMPORARY_REDIRECT       307
#define HTTP_CODE_PERMANENT_REDIRECT       308
#define HTTP_CODE_BAD_REQUEST              400
#define HTTP_CODE_UNAUTHORIZED             401
#define HTTP_CODE_PAYMENT_REQUIRED         402
#define HTTP_CODE_FORBIDDEN                403
#define HTTP_CODE_NOT_FOUND                404
#define HTTP_CODE_METHOD_NOT_ALLOWED       405
#define HTTP_CODE_NOT_ACCEPTABLE           406
#define HTTP_CODE_PROXY_AUTH_REQUIRED      407
#define HTTP_CODE_REQUEST_TIMEOUT          408
#define HTTP_CODE_CONFLICT                 409
#define HTTP_CODE_GONE                     410
#define HTTP_CODE_LENGTH_REQUIRED          411
#define HTTP_CODE_PRECONDITION_FAILED      412
#define HTTP_CODE_PAYLOAD_TOO_LARGE        413
#define HTTP_CODE_URI_TOO_LARGE            414
#define HTTP_CODE_UNSUPPORTED_MEDIA_TYPE   415
#define HTTP_CODE_RANGE_NOT_SATISFIABLE    416
#define HTTP_CODE_EXPECTATION_FAILED       417
#define HTTP_CODE_IM_A_TEAPOT              418
#define HTTP_CODE_MISDIRECTED_REQUEST      421
#define HTTP_CODE_UNPROCESSABLE_ENTITY     422
#define HTTP_CODE_LOCKED                   423
#define HTTP_CODE_FAILED_DEPENDENCY        424
#define HTTP_CODE_TOO_EARLY                425
#define HTTP_CODE_UPGRADE_REQUIRED         426
#define HTTP_CODE_PRECONDITION_REQUIRED    428
#define HTTP_CODE_TOO_MANY_REQUESTS        429
#define HTTP_CODE_REQ_HDR_FIELDS_TOO_LARGE 431
#define HTTP_CODE_UNAVAIL_4_LEGAL_REASONS  451
#define HTTP_CODE_INTERNAL_SERVER_ERROR    500
#define HTTP_CODE_NOT_IMPLEMENTED          501
#define HTTP_CODE_BAD_GATEWAY              502
#define HTTP_CODE_SERVICE_UNAVAILABLE      503
#define HTTP_CODE_GATEWAY_TIMEOUT          504
#define HTTP_CODE_HTTP_VER_NOT_SUPPORTED   505
#define HTTP_CODE_VARIANT_ALSO_NEGOTIATES  506
#define HTTP_CODE_INSUFFICIENT_STORAGE     507
#define HTTP_CODE_LOOP_DETECTED            508
#define HTTP_CODE_NOT_EXTENDED             510
#define HTTP_CODE_NETWORK_AUTH_REQUIRED    511

typedef enum HTTP_PARSER_STATE {
  HTTP_PARSER_BEGIN,
  HTTP_PARSER_REQUEST_OR_RESPONSE,
  HTTP_PARSER_METHOD,
  HTTP_PARSER_TARGET,
  HTTP_PARSER_HTTP_VERSION,
  HTTP_PARSER_SP,
  HTTP_PARSER_STATUS_CODE,
  HTTP_PARSER_REASON_PHRASE,
  HTTP_PARSER_CRLF,
  HTTP_PARSER_HEADER,
  HTTP_PARSER_HEADER_KEY,
  HTTP_PARSER_HEADER_VAL,
  HTTP_PARSER_CHUNK_SIZE,
  HTTP_PARSER_CHUNK,
  HTTP_PARSER_END,
  HTTP_PARSER_ERROR,
  HTTP_PARSER_OPTIONAL_SP
} HTTP_PARSER_STATE;

typedef enum HTTP_AUTH_TYPE {
  HTTP_INVALID_AUTH,
  HTTP_BASIC_AUTH,
  HTTP_TAOSD_AUTH
} HTTP_AUTH_TYPE;

typedef enum HTTP_VERSION {
  HTTP_VERSION_10 = 0,
  HTTP_VERSION_11 = 1,
  HTTP_VERSION_12 = 2,
  HTTP_INVALID_VERSION
} HTTP_VERSION;

typedef enum HTTP_KEEPALIVE {
  HTTP_KEEPALIVE_NO_INPUT = 0,
  HTTP_KEEPALIVE_ENABLE = 1,
  HTTP_KEEPALIVE_DISABLE = 2
} HTTP_KEEPALIVE;

typedef struct HttpString {
  char *  str;
  int32_t pos;
  int32_t size;
} HttpString;

typedef struct HttpStatus {
  int32_t code;
  char *  desc;
} HttpStatus;

typedef struct HttpStack{
  int8_t *stacks;
  int32_t pos;
  int32_t size;
} HttpStack;

struct HttpContext;
typedef struct HttpParser {
  struct HttpContext *pContext;
  ehttp_gzip_t *gzip;
  HttpStack     stacks;
  HttpString    str;
  HttpString    body;
  HttpString    path[HTTP_MAX_URL];
  char *  method;
  char *  target;
  char *  version;
  char *  reasonPhrase;
  char *  key;
  char *  val;
  char *  authContent;
  int8_t  httpVersion;
  int8_t  acceptEncodingGzip;
  int8_t  acceptEncodingChunked;
  int8_t  contentLengthSpecified;
  int8_t  contentChunked;
  int8_t  transferGzip;
  int8_t  transferChunked;
  int8_t  keepAlive;
  int8_t  authType;
  int32_t contentLength;
  int32_t chunkSize;
  int32_t receivedChunkSize;
  int32_t receivedSize;
  int32_t statusCode;
  int8_t  inited;
  int8_t  parsed;
  int16_t httpCode;
  int32_t parseCode;
} HttpParser;

void        httpInitParser(HttpParser *parser);
HttpParser *httpCreateParser(struct HttpContext *pContext);
void        httpClearParser(HttpParser *parser);
void        httpDestroyParser(HttpParser *parser);
int32_t     httpParseBuf(HttpParser *parser, const char *buf, int32_t len);
char *      httpGetStatusDesc(int32_t statusCode);

#endif
