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

#ifndef _TD_UTIL_ANAL_H_
#define _TD_UTIL_ANAL_H_

#include "os.h"
#include "tdef.h"
#include "thash.h"
#include "tjson.h"

#ifdef __cplusplus
extern "C" {
#endif

#define ANAL_FORECAST_DEFAULT_ROWS  60
#define ANAL_FORECAST_DEFAULT_CONF  80
#define ANAL_FORECAST_DEFAULT_EVERY 10000

typedef struct {
  EAnalFuncType type;
  int32_t       anode;
  int32_t       urlLen;
  char         *url;
} SAnalUrl;

typedef enum {
  ANAL_BUF_TYPE_JSON = 0,
  ANAL_BUF_TYPE_OTHERS,
} EAnalBufType;

typedef enum {
  ANAL_HTTP_GET,
  ANAL_HTTP_POST,
} EAnalHttpType;

typedef struct {
  EAnalBufType bufType;
  TdFilePtr    filePtr;
  char         fileName[PATH_MAX];
} SAnalBuf;

int32_t taosAnalInit();
void    taosAnalCleanup();
SJson  *taosAnalSendReqRetJson(const char *url, EAnalHttpType type, SAnalBuf *pBuf);

int32_t taosAnalGetFuncUrl(const char *funcName, EAnalFuncType type, char *url, int32_t urlLen);
bool    taosAnalGetParaStr(const char *option, const char *paraName, char *paraValue, int32_t paraValueMaxLen);
bool    taosAnalGetParaInt(const char *option, const char *paraName, int32_t *paraValue);
int64_t taosAnalGetVersion();
void    taosAnalUpdate(int64_t newVer, SHashObj *pHash);

int32_t tsosAnalBufOpen(SAnalBuf *pBuf);
int32_t taosAnalBufWritePara(SAnalBuf *pBuf, const char *paras, const char *timefmt, const char *timeprec);
int32_t taosAnalBufWriteMeta(SAnalBuf *pBuf, int32_t col1, int32_t col2);
int32_t taosAnalBufWriteData(SAnalBuf *pBuf, const char *data, bool isLast);
int32_t taosAnalBufWriteRows(SAnalBuf *pBuf, int32_t numOfRows);
void    taosAnalBufClose(SAnalBuf *pBuf);

const char   *taosAnalFuncStr(EAnalFuncType funcType);
EAnalFuncType taosAnalFuncInt(const char *funcName);

#ifdef __cplusplus
}
#endif
#endif /*_TD_UTIL_ANAL_H_*/
