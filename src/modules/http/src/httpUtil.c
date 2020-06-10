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

#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "tmd5.h"

#include "http.h"
#include "httpCode.h"
#include "httpHandle.h"
#include "httpResp.h"

#include "shash.h"
#include "taos.h"

bool httpCheckUsedbSql(char *sql) {
  if (strstr(sql, "use ") != NULL) {
    return true;
  }
  return false;
}

void httpTimeToString(time_t t, char *buf, int buflen) {
  memset(buf, 0, (size_t)buflen);
  char ts[30] = {0};

  struct tm *ptm;
  time_t     tt = t / 1000;
  ptm = localtime(&tt);
  strftime(ts, 64, "%Y-%m-%d %H:%M:%S", ptm);
  sprintf(buf, "%s.%03ld", ts, t % 1000);
}

int32_t httpAddToSqlCmdBuffer(HttpContext *pContext, const char *const format, ...) {
  HttpSqlCmds *cmd = pContext->multiCmds;
  if (cmd->buffer == NULL) return -1;

  int remainLength = cmd->bufferSize - cmd->bufferPos;
  if (remainLength < 4096) {
    if (!httpReMallocMultiCmdsBuffer(pContext, cmd->bufferSize * 2)) return -1;
  }

  char *buffer = cmd->buffer + cmd->bufferPos;
  int   len = 0;

  va_list argpointer;
  va_start(argpointer, format);
  len += vsnprintf(buffer, (size_t)remainLength, format, argpointer);
  va_end(argpointer);

  if (cmd->bufferPos + len + 1 >= cmd->bufferSize) {
    return -1;
  }

  cmd->buffer[cmd->bufferPos + len] = 0;
  cmd->bufferPos = cmd->bufferPos + len + 1;

  remainLength = cmd->bufferSize - cmd->bufferPos;
  if (remainLength < 4096) {
    if (!httpReMallocMultiCmdsBuffer(pContext, cmd->bufferSize * 2)) return -1;
  }

  return (int32_t)(buffer - cmd->buffer);
}

int32_t httpAddToSqlCmdBufferNoTerminal(HttpContext *pContext, const char *const format, ...) {
  HttpSqlCmds *cmd = pContext->multiCmds;
  if (cmd->buffer == NULL) return -1;

  int remainLength = cmd->bufferSize - cmd->bufferPos;
  if (remainLength < 4096) {
    if (!httpReMallocMultiCmdsBuffer(pContext, cmd->bufferSize * 2)) return -1;
  }

  char *buffer = cmd->buffer + cmd->bufferPos;
  int   len = 0;

  va_list argpointer;
  va_start(argpointer, format);
  len += vsnprintf(buffer, (size_t)remainLength, format, argpointer);
  va_end(argpointer);

  if (cmd->bufferPos + len + 1 >= cmd->bufferSize) {
    return -1;
  }

  cmd->bufferPos = cmd->bufferPos + len;

  remainLength = cmd->bufferSize - cmd->bufferPos;
  if (remainLength < 4096) {
    if (!httpReMallocMultiCmdsBuffer(pContext, cmd->bufferSize * 2)) return -1;
  }

  return (int32_t)(buffer - cmd->buffer);
}

int32_t httpAddToSqlCmdBufferTerminal(HttpContext *pContext) {
  HttpSqlCmds *cmd = pContext->multiCmds;
  if (cmd->buffer == NULL) return -1;

  int remainLength = cmd->bufferSize - cmd->bufferPos;
  if (remainLength < 4096) {
    if (!httpReMallocMultiCmdsBuffer(pContext, cmd->bufferSize * 2)) return -1;
  }

  char *buffer = cmd->buffer + cmd->bufferPos;
  *buffer = 0;
  cmd->bufferPos = cmd->bufferPos + 1;

  remainLength = cmd->bufferSize - cmd->bufferPos;
  if (remainLength < 4096) {
    if (!httpReMallocMultiCmdsBuffer(pContext, cmd->bufferSize * 2)) return -1;
  }

  return (int32_t)(buffer - cmd->buffer);
}

int32_t httpAddToSqlCmdBufferWithSize(HttpContext *pContext, int mallocSize) {
  HttpSqlCmds *cmd = pContext->multiCmds;
  if (cmd->buffer == NULL) return -1;

  if (cmd->bufferPos + mallocSize >= cmd->bufferSize) {
    if (!httpReMallocMultiCmdsBuffer(pContext, cmd->bufferSize * 2)) return -1;
  }

  char *buffer = cmd->buffer + cmd->bufferPos;
  memset(cmd->buffer + cmd->bufferPos, 0, (size_t)mallocSize);
  cmd->bufferPos = cmd->bufferPos + mallocSize;

  return (int32_t)(buffer - cmd->buffer);
}

bool httpMallocMultiCmds(HttpContext *pContext, int cmdSize, int bufferSize) {
  if (cmdSize > HTTP_MAX_CMD_SIZE) {
    httpError("context:%p, fd:%d, ip:%s, user:%s, mulitcmd size:%d large then %d", pContext, pContext->fd,
              pContext->ipstr, pContext->user, cmdSize, HTTP_MAX_CMD_SIZE);
    return false;
  }

  if (pContext->multiCmds == NULL) {
    pContext->multiCmds = (HttpSqlCmds *)malloc(sizeof(HttpSqlCmds));
    if (pContext->multiCmds == NULL) {
      httpError("context:%p, fd:%d, ip:%s, user:%s, malloc multiCmds error", pContext, pContext->fd, pContext->ipstr,
                pContext->user);
      return false;
    }
    memset(pContext->multiCmds, 0, sizeof(HttpSqlCmds));
  }

  HttpSqlCmds *multiCmds = pContext->multiCmds;
  if (multiCmds->cmds == NULL || cmdSize > multiCmds->maxSize) {
    free(multiCmds->cmds);
    multiCmds->cmds = (HttpSqlCmd *)malloc((size_t)cmdSize * sizeof(HttpSqlCmd));
    if (multiCmds->cmds == NULL) {
      httpError("context:%p, fd:%d, ip:%s, user:%s, malloc cmds:%d error", pContext, pContext->fd, pContext->ipstr,
                pContext->user, cmdSize);
      return false;
    }
    multiCmds->maxSize = (int16_t)cmdSize;
  }

  if (multiCmds->buffer == NULL || bufferSize > multiCmds->bufferSize) {
    free(multiCmds->buffer);
    multiCmds->buffer = (char *)malloc((size_t)bufferSize);
    if (multiCmds->buffer == NULL) {
      httpError("context:%p, fd:%d, ip:%s, user:%s, malloc buffer:%d error", pContext, pContext->fd, pContext->ipstr,
                pContext->user, bufferSize);
      return false;
    }
    multiCmds->bufferSize = bufferSize;
  }

  multiCmds->pos = 0;
  multiCmds->size = 0;
  multiCmds->bufferPos = 0;
  memset(multiCmds->cmds, 0, (size_t)multiCmds->maxSize * sizeof(HttpSqlCmd));

  return true;
}

bool httpReMallocMultiCmdsSize(HttpContext *pContext, int cmdSize) {
  HttpSqlCmds *multiCmds = pContext->multiCmds;

  if (cmdSize > HTTP_MAX_CMD_SIZE) {
    httpError("context:%p, fd:%d, ip:%s, user:%s, mulitcmd size:%d large then %d", pContext, pContext->fd,
              pContext->ipstr, pContext->user, cmdSize, HTTP_MAX_CMD_SIZE);
    return false;
  }

  multiCmds->cmds = (HttpSqlCmd *)realloc(multiCmds->cmds, (size_t)cmdSize * sizeof(HttpSqlCmd));
  if (multiCmds->cmds == NULL) {
    httpError("context:%p, fd:%d, ip:%s, user:%s, malloc cmds:%d error", pContext, pContext->fd, pContext->ipstr,
              pContext->user, cmdSize);
    return false;
  }
  memset(multiCmds->cmds + multiCmds->maxSize * (int16_t)sizeof(HttpSqlCmd), 0,
         (size_t)(cmdSize - multiCmds->maxSize) * sizeof(HttpSqlCmd));
  multiCmds->maxSize = (int16_t)cmdSize;

  return true;
}

bool httpReMallocMultiCmdsBuffer(HttpContext *pContext, int bufferSize) {
  HttpSqlCmds *multiCmds = pContext->multiCmds;

  if (bufferSize > HTTP_MAX_BUFFER_SIZE) {
    httpError("context:%p, fd:%d, ip:%s, user:%s, mulitcmd buffer size:%d large then %d",
              pContext, pContext->fd, pContext->ipstr, pContext->user, bufferSize, HTTP_MAX_BUFFER_SIZE);
    return false;
  }

  multiCmds->buffer = (char *)realloc(multiCmds->buffer, (size_t)bufferSize);
  if (multiCmds->buffer == NULL) {
    httpError("context:%p, fd:%d, ip:%s, user:%s, malloc buffer:%d error", pContext, pContext->fd, pContext->ipstr,
              pContext->user, bufferSize);
    return false;
  }
  memset(multiCmds->buffer + multiCmds->bufferSize, 0, (size_t)(bufferSize - multiCmds->bufferSize));
  multiCmds->bufferSize = bufferSize;

  return true;
}

void httpFreeMultiCmds(HttpContext *pContext) {
  if (pContext->multiCmds != NULL) {
    if (pContext->multiCmds->buffer != NULL) free(pContext->multiCmds->buffer);
    if (pContext->multiCmds->cmds != NULL) free(pContext->multiCmds->cmds);
    free(pContext->multiCmds);
    pContext->multiCmds = NULL;
  }
}

JsonBuf *httpMallocJsonBuf(HttpContext *pContext) {
  if (pContext->jsonBuf == NULL) {
    pContext->jsonBuf = (JsonBuf *)malloc(sizeof(JsonBuf));
  }

  return pContext->jsonBuf;
}

void httpFreeJsonBuf(HttpContext *pContext) {
  if (pContext->jsonBuf != NULL) {
    free(pContext->jsonBuf);
    pContext->jsonBuf = 0;
  }
}

bool httpCompareMethod(HttpDecodeMethod *pSrc, HttpDecodeMethod *pCmp) {
  if (strcmp(pSrc->module, pCmp->module) != 0) {
    return false;
  }
  return true;
}

void httpAddMethod(HttpServer *pServer, HttpDecodeMethod *pMethod) {
  int pos = 0;
  for (pos = 0; pos < pServer->methodScannerLen; ++pos) {
    if (httpCompareMethod(pServer->methodScanner[pos], pMethod)) {
      break;
    }
  }

  if (pos == pServer->methodScannerLen && pServer->methodScannerLen < HTTP_METHOD_SCANNER_SIZE) {
    pServer->methodScanner[pos] = pMethod;
    pServer->methodScannerLen++;
  }
}

HttpSqlCmd *httpNewSqlCmd(HttpContext *pContext) {
  HttpSqlCmds *multiCmds = pContext->multiCmds;
  if (multiCmds->size >= multiCmds->maxSize) {
    if (!httpReMallocMultiCmdsSize(pContext, 2 * multiCmds->maxSize)) return NULL;
  }

  HttpSqlCmd *cmd = multiCmds->cmds + multiCmds->size++;
  cmd->cmdType = HTTP_CMD_TYPE_UN_SPECIFIED;
  cmd->cmdReturnType = HTTP_CMD_RETURN_TYPE_WITH_RETURN;
  cmd->cmdState = HTTP_CMD_STATE_NOT_RUN_YET;

  return cmd;
}

HttpSqlCmd *httpCurrSqlCmd(HttpContext *pContext) {
  HttpSqlCmds *multiCmds = pContext->multiCmds;
  if (multiCmds->size == 0) return NULL;
  if (multiCmds->size > multiCmds->maxSize) return NULL;

  return multiCmds->cmds + multiCmds->size - 1;
}

int httpNextSqlCmdPos(HttpContext *pContext) {
  HttpSqlCmds *multiCmds = pContext->multiCmds;
  return multiCmds->size;
}

void httpTrimTableName(char *name) {
  for (int i = 0; name[i] != 0; i++) {
    if (name[i] == ' ' || name[i] == ':' || name[i] == '.' || name[i] == '-' || name[i] == '/' || name[i] == '\'')
      name[i] = '_';
    if (i == TSDB_METER_NAME_LEN + 1) {
      name[i] = 0;
      break;
    }
  }
}

int httpShrinkTableName(HttpContext *pContext, int pos, char *name) {
  int len = 0;
  for (int i = 0; name[i] != 0; i++) {
    if (name[i] == ' ' || name[i] == ':' || name[i] == '.' || name[i] == '-' || name[i] == '/' || name[i] == '\'' ||
        name[i] == '\"')
      name[i] = '_';
    len++;
  }

  if (len < TSDB_METER_NAME_LEN) {
    return pos;
  }

  MD5_CTX context;
  MD5Init(&context);
  MD5Update(&context, (uint8_t *)name, (uint32_t)len);
  MD5Final(&context);

  int table_name = httpAddToSqlCmdBuffer(
      pContext, "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x", context.digest[0],
      context.digest[1], context.digest[2], context.digest[3], context.digest[4], context.digest[5], context.digest[6],
      context.digest[7], context.digest[8], context.digest[9], context.digest[10], context.digest[11],
      context.digest[12], context.digest[13], context.digest[14], context.digest[15]);

  if (table_name != -1) {
    httpGetCmdsString(pContext, table_name)[0] = 't';
  }

  return table_name;
}

char *httpGetCmdsString(HttpContext *pContext, int pos) {
  HttpSqlCmds *multiCmds = pContext->multiCmds;
  if (pos < 0 || pos >= multiCmds->bufferSize) {
    return "";
  }

  return multiCmds->buffer + pos;
}

int httpGzipDeCompress(char *srcData, int32_t nSrcData, char *destData, int32_t *nDestData) {
  int err = 0;
  z_stream gzipStream = {0};

  static char dummyHead[2] = {
          0x8 + 0x7 * 0x10,
          (((0x8 + 0x7 * 0x10) * 0x100 + 30) / 31 * 31) & 0xFF,
  };

  gzipStream.zalloc = (alloc_func) 0;
  gzipStream.zfree = (free_func) 0;
  gzipStream.opaque = (voidpf) 0;
  gzipStream.next_in = (Bytef *) srcData;
  gzipStream.avail_in = 0;
  gzipStream.next_out = (Bytef *) destData;
  if (inflateInit2(&gzipStream, 47) != Z_OK) {
    return -1;
  }

  while (gzipStream.total_out < *nDestData && gzipStream.total_in < nSrcData) {
    gzipStream.avail_in = gzipStream.avail_out = nSrcData;  //1
    if ((err = inflate(&gzipStream, Z_NO_FLUSH)) == Z_STREAM_END) {
      break;
    }

    if (err != Z_OK) {
      if (err == Z_DATA_ERROR) {
        gzipStream.next_in = (Bytef *) dummyHead;
        gzipStream.avail_in = sizeof(dummyHead);
        if ((err = inflate(&gzipStream, Z_NO_FLUSH)) != Z_OK) {
          return -2;
        }
      } else return -3;
    }
  }

  if (inflateEnd(&gzipStream) != Z_OK) {
    return -4;
  }
  *nDestData = gzipStream.total_out;

  return 0;
}

int httpGzipCompressInit(HttpContext *pContext) {
  pContext->gzipStream.zalloc = (alloc_func) 0;
  pContext->gzipStream.zfree = (free_func) 0;
  pContext->gzipStream.opaque = (voidpf) 0;
  if (deflateInit2(&pContext->gzipStream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, MAX_WBITS + 16, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
    return -1;
  }

  return 0;
}

int httpGzipCompress(HttpContext *pContext, char *srcData, int32_t nSrcData, char *destData, int32_t *nDestData, bool isTheLast) {
  int err = 0;
  pContext->gzipStream.next_in = (Bytef *) srcData;
  pContext->gzipStream.avail_in = (uLong) nSrcData;
  pContext->gzipStream.next_out = (Bytef *) destData;
  pContext->gzipStream.avail_out = (uLong) (*nDestData);

  while (pContext->gzipStream.avail_in != 0 && pContext->gzipStream.total_out < (uLong) (*nDestData)) {
    if (deflate(&pContext->gzipStream, Z_FULL_FLUSH) != Z_OK) {
      return -1;
    }
  }

  if (pContext->gzipStream.avail_in != 0) {
    return pContext->gzipStream.avail_in;
  }

  if (isTheLast) {
    for (;;) {
      if ((err = deflate(&pContext->gzipStream, Z_FINISH)) == Z_STREAM_END) {
        break;
      }
      if (err != Z_OK) {
        return -2;
      }
    }

    if (deflateEnd(&pContext->gzipStream) != Z_OK) {
      return -3;
    }
  }

  *nDestData = (int32_t) (pContext->gzipStream.total_out);
  return 0;
}
