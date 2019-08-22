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

#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

#include "http.h"
#include "httpCode.h"
#include "httpJson.h"
#include "httpResp.h"
#include "taosmsg.h"

#define MAX_NUM_STR_SZ 25

char JsonItmTkn = ',';
char JsonObjStt = '{';
char JsonObjEnd = '}';
char JsonArrStt = '[';
char JsonArrEnd = ']';
char JsonStrStt = '\"';
char JsonStrEnd = '\"';
char JsonPairTkn = ':';
char JsonNulTkn[] = "null";
char JsonTrueTkn[] = "true";
char JsonFalseTkn[] = "false";

int httpWriteBufByFd(struct HttpContext* pContext, const char* buf, int sz) {
  int       len;
  int       countWait = 0;
  int       writeLen = 0;

  do {
    if (pContext->fd > 2){
      len = (int)send(pContext->fd, buf + writeLen, (size_t)(sz - writeLen), MSG_NOSIGNAL);
    }
    else {
      return sz;
    }

    if (len < 0) {
      httpTrace("context:%p, fd:%d, ip:%s, socket write errno:%d, times:%d",
                pContext, pContext->fd, pContext->ipstr, errno, countWait);
      if (++countWait > HTTP_WRITE_RETRY_TIMES) break;
      taosMsleep(HTTP_WRITE_WAIT_TIME_MS);
      continue;
    } else if (len == 0) {
      httpTrace("context:%p, fd:%d, ip:%s, socket write errno:%d, connect already closed",
                pContext, pContext->fd, pContext->ipstr, errno);
      break;
    } else {
      countWait = 0;
      writeLen += len;
    }
  } while (writeLen < sz);

  return writeLen;
}

int httpWriteBuf(struct HttpContext *pContext, const char *buf, int sz) {
  int writeSz = httpWriteBufByFd(pContext, buf, sz);
  if (writeSz != sz) {
    httpError("context:%p, fd:%d, ip:%s, dataSize:%d, writeSize:%d, failed to send response:\n%s",
              pContext, pContext->fd, pContext->ipstr, sz, writeSz, buf);
  } else {
    httpTrace("context:%p, fd:%d, ip:%s, dataSize:%d, writeSize:%d, response:\n%s",
              pContext, pContext->fd, pContext->ipstr, sz, writeSz, buf);
  }

  return writeSz;
}

int httpWriteBufNoTrace(struct HttpContext *pContext, const char *buf, int sz) {
  int writeSz = httpWriteBufByFd(pContext, buf, sz);
  if (writeSz != sz) {
    httpError("context:%p, fd:%d, ip:%s, dataSize:%d, writeSize:%d, failed to send response",
              pContext, pContext->fd, pContext->ipstr, sz, writeSz);
  }

  return writeSz;
}

int httpWriteJsonBufBody(JsonBuf* buf, bool isTheLast) {
  int remain = 0;
  char sLen[24];
  uint64_t srcLen = (uint64_t) (buf->lst - buf->buf);

  if (buf->pContext->fd <= 0) {
    httpTrace("context:%p, fd:%d, ip:%s, write json body error", buf->pContext, buf->pContext->fd, buf->pContext->ipstr);
    buf->pContext->fd = -1;
  }

  /*
   * HTTP servers often use compression to optimize transmission, for example
   * with Content-Encoding: gzip or Content-Encoding: deflate.
   * If both compression and chunked encoding are enabled, then the content stream is first compressed, then chunked;
   * so the chunk encoding itself is not compressed, and the data in each chunk is not compressed individually.
   * The remote endpoint then decodes the stream by concatenating the chunks and uncompressing the result.
   */

  if (buf->pContext->acceptEncoding == HTTP_COMPRESS_IDENTITY) {
    if (buf->lst == buf->buf) {
      httpTrace("context:%p, fd:%d, ip:%s, no data need dump", buf->pContext, buf->pContext->fd, buf->pContext->ipstr);
      return 0;  // there is no data to dump.
    } else {
      int len = sprintf(sLen, "%lx\r\n", srcLen);
      httpTrace("context:%p, fd:%d, ip:%s, write body, chunkSize:%ld, response:\n%s",
                buf->pContext, buf->pContext->fd, buf->pContext->ipstr, srcLen, buf->buf);
      httpWriteBufNoTrace(buf->pContext, sLen, len);
      remain = httpWriteBufNoTrace(buf->pContext, buf->buf, (int) srcLen);
    }
  } else {
    char compressBuf[JSON_BUFFER_SIZE] = {0};
    int32_t compressBufLen = JSON_BUFFER_SIZE;
    int ret = httpGzipCompress(buf->pContext, buf->buf, srcLen, compressBuf, &compressBufLen, isTheLast);
    if (ret == 0) {
      if (compressBufLen > 0) {
        int len = sprintf(sLen, "%x\r\n", compressBufLen);
        httpTrace("context:%p, fd:%d, ip:%s, write body, chunkSize:%ld, compressSize:%d, last:%d, response:\n%s",
                  buf->pContext, buf->pContext->fd, buf->pContext->ipstr, srcLen, compressBufLen, isTheLast, buf->buf);
        httpWriteBufNoTrace(buf->pContext, sLen, len);
        remain = httpWriteBufNoTrace(buf->pContext, (const char *) compressBuf, (int) compressBufLen);
      } else {
        httpTrace("context:%p, fd:%d, ip:%s, last:%d, compress already dumped, response:\n%s",
                buf->pContext, buf->pContext->fd, buf->pContext->ipstr, isTheLast, buf->buf);
        return 0;  // there is no data to dump.
      }
    } else {
      httpError("context:%p, fd:%d, ip:%s, failed to compress data, chunkSize:%d, last:%d, error:%d, response:\n%s",
                buf->pContext, buf->pContext->fd, buf->pContext->ipstr, srcLen, isTheLast, ret, buf->buf);
      return 0;
    }
  }

  httpWriteBufNoTrace(buf->pContext, "\r\n", 2);
  buf->total += (int) (buf->lst - buf->buf);
  buf->lst = buf->buf;
  memset(buf->buf, 0, (size_t) buf->size);
  return remain;
}

void httpWriteJsonBufHead(JsonBuf* buf) {
  if (buf->pContext->fd <= 0) {
    buf->pContext->fd = -1;
  }

  char msg[1024] = {0};
  int  len = -1;

  if (buf->pContext->acceptEncoding == HTTP_COMPRESS_IDENTITY) {
    len = sprintf(msg, httpRespTemplate[HTTP_RESPONSE_CHUNKED_UN_COMPRESS], httpVersionStr[buf->pContext->httpVersion],
                  httpKeepAliveStr[buf->pContext->httpKeepAlive]);
  } else {
    len = sprintf(msg, httpRespTemplate[HTTP_RESPONSE_CHUNKED_COMPRESS], httpVersionStr[buf->pContext->httpVersion],
                  httpKeepAliveStr[buf->pContext->httpKeepAlive]);
  }

  httpWriteBuf(buf->pContext, (const char*)msg, len);
}

void httpWriteJsonBufEnd(JsonBuf* buf) {
  if (buf->pContext->fd <= 0) {
    httpTrace("context:%p, fd:%d, ip:%s, json buf fd is 0", buf->pContext, buf->pContext->fd, buf->pContext->ipstr);
    buf->pContext->fd = -1;
  }

  httpWriteJsonBufBody(buf, true);
  httpWriteBufNoTrace(buf->pContext, "0\r\n\r\n", 5);  // end of chunked resp
}

void httpInitJsonBuf(JsonBuf* buf, struct HttpContext* pContext) {
  buf->lst = buf->buf;
  buf->total = 0;
  buf->size = JSON_BUFFER_SIZE;  // option setting
  buf->pContext = pContext;
  memset(buf->lst, 0, JSON_BUFFER_SIZE);

  if (pContext->acceptEncoding == HTTP_COMPRESS_GZIP) {
    httpGzipCompressInit(buf->pContext);
  }

  httpTrace("context:%p, fd:%d, ip:%s, json buffer initialized", buf->pContext, buf->pContext->fd, buf->pContext->ipstr);
}

void httpJsonItemToken(JsonBuf* buf) {
  char c = *(buf->lst - 1);
  if (c == JsonArrStt || c == JsonObjStt || c == JsonPairTkn || c == JsonItmTkn) {
    return;
  }
  if (buf->lst > buf->buf) httpJsonToken(buf, JsonItmTkn);
}

void httpJsonString(JsonBuf* buf, char* sVal, int len) {
  httpJsonItemToken(buf);
  httpJsonToken(buf, JsonStrStt);
  httpJsonPrint(buf, sVal, len);
  httpJsonToken(buf, JsonStrEnd);
}

void httpJsonOriginString(JsonBuf* buf, char* sVal, int len) {
  httpJsonItemToken(buf);
  httpJsonPrint(buf, sVal, len);
}

void httpJsonStringForTransMean(JsonBuf* buf, char* sVal, int maxLen) {
  httpJsonItemToken(buf);
  httpJsonToken(buf, JsonStrStt);

  if (sVal != NULL) {
    // dispose transferred meaning byte
    char* lastPos = sVal;
    char* curPos = sVal;

    for (int i = 0; i < maxLen; ++i) {
      if (*curPos == 0) {
        break;
      }

      if (*curPos == '\"') {
        httpJsonPrint(buf, lastPos, (int)(curPos - lastPos));
        curPos++;
        lastPos = curPos;
        httpJsonPrint(buf, "\\\"", 2);
      } else if (*curPos == '\\') {
        httpJsonPrint(buf, lastPos, (int)(curPos - lastPos));
        curPos++;
        lastPos = curPos;
        httpJsonPrint(buf, "\\\\", 2);
      } else {
        curPos++;
      }
    }

    if (*lastPos) {
      httpJsonPrint(buf, lastPos, (int)(curPos - lastPos));
    }
  }

  httpJsonToken(buf, JsonStrEnd);
}

void httpJsonInt64(JsonBuf* buf, int64_t num) {
  httpJsonItemToken(buf);
  httpJsonTestBuf(buf, MAX_NUM_STR_SZ);
  buf->lst += snprintf(buf->lst, MAX_NUM_STR_SZ, "%ld", num);
}

void httpJsonTimestamp(JsonBuf* buf, int64_t t, bool us) {
  char ts[35] = {0};
  struct tm *ptm;
  int precision = 1000;
  if (us) {
    precision = 1000000;
  }

  time_t tt = t / precision;
  ptm = localtime(&tt);
  int length = (int) strftime(ts, 35, "%Y-%m-%d %H:%M:%S", ptm);
  if (us) {
    length += snprintf(ts + length, 8, ".%06ld", t % precision);
  } else {
    length += snprintf(ts + length, 5, ".%03ld", t % precision);
  }

  httpJsonString(buf, ts, length);
}

void httpJsonUtcTimestamp(JsonBuf* buf, int64_t t, bool us) {
  char ts[40] = {0};
  struct tm *ptm;
  int precision = 1000;
  if (us) {
    precision = 1000000;
  }

  time_t tt = t / precision;
  ptm = localtime(&tt);
  int length = (int) strftime(ts, 40, "%Y-%m-%dT%H:%M:%S", ptm);
  if (us) {
    length += snprintf(ts + length, 8, ".%06ld", t % precision);
  } else {
    length += snprintf(ts + length, 5, ".%03ld", t % precision);
  }
  length += (int) strftime(ts + length, 40 - length, "%z", ptm);

  httpJsonString(buf, ts, length);
}

void httpJsonInt(JsonBuf* buf, int num) {
  httpJsonItemToken(buf);
  httpJsonTestBuf(buf, MAX_NUM_STR_SZ);
  buf->lst += snprintf(buf->lst, MAX_NUM_STR_SZ, "%d", num);
}

void httpJsonFloat(JsonBuf* buf, float num) {
  httpJsonItemToken(buf);
  httpJsonTestBuf(buf, MAX_NUM_STR_SZ);
  if (num > 1E10 || num < -1E10) {
    buf->lst += snprintf(buf->lst, MAX_NUM_STR_SZ, "%.5e", num);
  } else {
    buf->lst += snprintf(buf->lst, MAX_NUM_STR_SZ, "%.5f", num);
  }
}

void httpJsonDouble(JsonBuf* buf, double num) {
  httpJsonItemToken(buf);
  httpJsonTestBuf(buf, MAX_NUM_STR_SZ);
  if (num > 1E10 || num < -1E10) {
    buf->lst += snprintf(buf->lst, MAX_NUM_STR_SZ, "%.9e", num);
  } else {
    buf->lst += snprintf(buf->lst, MAX_NUM_STR_SZ, "%.9f", num);
  }
}

void httpJsonNull(JsonBuf* buf) { httpJsonString(buf, "null", 4); }

void httpJsonBool(JsonBuf* buf, int val) {
  if (val == 0)
    httpJsonPrint(buf, JsonFalseTkn, sizeof(JsonFalseTkn));
  else
    httpJsonPrint(buf, JsonTrueTkn, sizeof(JsonTrueTkn));
}

void httpJsonPairHead(JsonBuf* buf, char* name, int len) {
  httpJsonItemToken(buf);
  httpJsonString(buf, name, len);
  httpJsonToken(buf, JsonPairTkn);
}

void httpJsonPair(JsonBuf* buf, char* name, int nameLen, char* sVal, int valLen) {
  httpJsonPairHead(buf, name, nameLen);
  httpJsonString(buf, sVal, valLen);
}

void httpJsonPairOriginString(JsonBuf* buf, char* name, int nameLen, char* sVal, int valLen) {
  httpJsonPairHead(buf, name, nameLen);
  httpJsonOriginString(buf, sVal, valLen);
}

void httpJsonPairIntVal(JsonBuf* buf, char* name, int nNameLen, int num) {
  httpJsonPairHead(buf, name, nNameLen);
  httpJsonInt(buf, num);
}

void httpJsonPairInt64Val(JsonBuf* buf, char* name, int nNameLen, int64_t num) {
  httpJsonPairHead(buf, name, nNameLen);
  httpJsonInt64(buf, num);
}

void httpJsonPairBoolVal(JsonBuf* buf, char* name, int nNameLen, int num) {
  httpJsonPairHead(buf, name, nNameLen);
  httpJsonBool(buf, num);
}

void httpJsonPairFloatVal(JsonBuf* buf, char* name, int nNameLen, float num) {
  httpJsonPairHead(buf, name, nNameLen);
  httpJsonFloat(buf, num);
}

void httpJsonPairDoubleVal(JsonBuf* buf, char* name, int nNameLen, double num) {
  httpJsonPairHead(buf, name, nNameLen);
  httpJsonDouble(buf, num);
}

void httpJsonPairNullVal(JsonBuf* buf, char* name, int nNameLen) {
  httpJsonPairHead(buf, name, nNameLen);
  httpJsonNull(buf);
}

void httpJsonPairArray(JsonBuf* buf, char* name, int len, httpJsonBuilder fnBuilder, void* dsHandle) {
  httpJsonPairHead(buf, name, len);
  httpJsonArray(buf, fnBuilder, dsHandle);
}

void httpJsonPairObject(JsonBuf* buf, char* name, int len, httpJsonBuilder fnBuilder, void* dsHandle) {
  httpJsonPairHead(buf, name, len);
  httpJsonObject(buf, fnBuilder, dsHandle);
}

void httpJsonObject(JsonBuf* buf, httpJsonBuilder fnBuilder, void* dsHandle) {
  httpJsonItemToken(buf);
  httpJsonToken(buf, JsonObjStt);
  (*fnBuilder)(buf, dsHandle);
  httpJsonToken(buf, JsonObjEnd);
}

void httpJsonArray(JsonBuf* buf, httpJsonBuilder fnBuilder, void* jsonHandle) {
  httpJsonItemToken(buf);
  httpJsonToken(buf, JsonArrStt);
  (*fnBuilder)(buf, jsonHandle);
  httpJsonToken(buf, JsonArrEnd);
}

void httpJsonTestBuf(JsonBuf* buf, int safety) {
  if ((buf->lst - buf->buf + safety) < buf->size) return;
  // buf->slot = *buf->lst;
  httpWriteJsonBufBody(buf, false);
}

void httpJsonToken(JsonBuf* buf, char c) {
  httpJsonTestBuf(buf, MAX_NUM_STR_SZ);  // maybe object stack
  *buf->lst++ = c;
}

void httpJsonPrint(JsonBuf* buf, const char* json, int len) {
  if (len == 0 || len >= JSON_BUFFER_SIZE) {
    return;
  }

  if (len > buf->size) {
    httpWriteJsonBufBody(buf, false);
    httpJsonPrint(buf, json, len);
    // buf->slot = json[len - 1];
    return;
  }
  httpJsonTestBuf(buf, len + 2);
  memcpy(buf->lst, json, (size_t)len);
  buf->lst += len;
}

void httpJsonPairStatus(JsonBuf* buf, int code) {
  if (code == 0) {
    httpJsonPair(buf, "status", 6, "succ", 4);
  } else {
    httpJsonPair(buf, "status", 6, "error", 5);
    httpJsonItemToken(buf);
    httpJsonPairIntVal(buf, "code", 4, code);
    if (code >= 0) {
      httpJsonItemToken(buf);
      if (code == TSDB_CODE_DB_NOT_SELECTED) {
        httpJsonPair(buf, "desc", 4, "failed to create database", 23);
      } else if (code == TSDB_CODE_INVALID_TABLE) {
        httpJsonPair(buf, "desc", 4, "failed to create table", 22);
      } else
        httpJsonPair(buf, "desc", 4, tsError[code], (int)strlen(tsError[code]));
    }
  }
}
