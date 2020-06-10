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

#ifndef TDENGINE_HTTP_JSON_H
#define TDENGINE_HTTP_JSON_H

#include <stdint.h>
#include <stdbool.h>

#define JSON_BUFFER_SIZE 10240
struct HttpContext;

enum { JsonNumber, JsonString, JsonBoolean, JsonArray, JsonObject, JsonNull };

extern char JsonItmTkn;
extern char JsonObjStt;
extern char JsonObjEnd;
extern char JsonArrStt;
extern char JsonArrEnd;
extern char JsonStrStt;
extern char JsonStrEnd;
extern char JsonPairTkn;
extern char JsonNulTkn[];
extern char JsonTrueTkn[];
extern char JsonFalseTkn[];

typedef struct {
  int                 size;
  int                 total;
  char*               lst;
  char                buf[JSON_BUFFER_SIZE];
  struct HttpContext* pContext;
} JsonBuf;

// http response
int httpWriteBuf(struct HttpContext* pContext, const char* buf, int sz);
int httpWriteBufNoTrace(struct HttpContext* pContext, const char* buf, int sz);
int httpWriteBufByFd(struct HttpContext* pContext, const char* buf, int sz);

// builder callback
typedef void (*httpJsonBuilder)(JsonBuf* buf, void* jsnHandle);

// buffer
void httpInitJsonBuf(JsonBuf* buf, struct HttpContext* pContext);
void httpWriteJsonBufHead(JsonBuf* buf);
int httpWriteJsonBufBody(JsonBuf* buf, bool isTheLast);
void httpWriteJsonBufEnd(JsonBuf* buf);

// value
void httpJsonString(JsonBuf* buf, char* sVal, int len);
void httpJsonOriginString(JsonBuf* buf, char* sVal, int len);
void httpJsonStringForTransMean(JsonBuf* buf, char* SVal, int maxLen);
void httpJsonInt64(JsonBuf* buf, int64_t num);
void httpJsonTimestamp(JsonBuf* buf, int64_t t, bool us);
void httpJsonUtcTimestamp(JsonBuf* buf, int64_t t, bool us);
void httpJsonInt(JsonBuf* buf, int num);
void httpJsonFloat(JsonBuf* buf, float num);
void httpJsonDouble(JsonBuf* buf, double num);
void httpJsonNull(JsonBuf* buf);
void httpJsonBool(JsonBuf* buf, int val);

// pair
void httpJsonPair(JsonBuf* buf, char* name, int nameLen, char* sVal, int valLen);
void httpJsonPairOriginString(JsonBuf* buf, char* name, int nameLen, char* sVal, int valLen);
void httpJsonPairHead(JsonBuf* buf, char* name, int len);
void httpJsonPairIntVal(JsonBuf* buf, char* name, int nNameLen, int num);
void httpJsonPairInt64Val(JsonBuf* buf, char* name, int nNameLen, int64_t num);
void httpJsonPairBoolVal(JsonBuf* buf, char* name, int nNameLen, int num);
void httpJsonPairFloatVal(JsonBuf* buf, char* name, int nNameLen, float num);
void httpJsonPairDoubleVal(JsonBuf* buf, char* name, int nNameLen, double num);
void httpJsonPairNullVal(JsonBuf* buf, char* name, int nNameLen);

// object
void httpJsonPairArray(JsonBuf* buf, char* name, int nLen, httpJsonBuilder builder, void* dsHandle);
void httpJsonPairObject(JsonBuf* buf, char* name, int nLen, httpJsonBuilder builder, void* dsHandle);
void httpJsonObject(JsonBuf* buf, httpJsonBuilder fnBuilder, void* dsHandle);
void httpJsonArray(JsonBuf* buf, httpJsonBuilder fnBuidler, void* jsonHandle);

// print
void httpJsonTestBuf(JsonBuf* buf, int safety);
void httpJsonToken(JsonBuf* buf, char c);
void httpJsonItemToken(JsonBuf* buf);
void httpJsonPrint(JsonBuf* buf, const char* json, int len);

// quick
void httpJsonPairStatus(JsonBuf* buf, int code);

#endif
