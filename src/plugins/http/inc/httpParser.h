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

#define HTTP_MAX_URL 5  // http url stack size

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
