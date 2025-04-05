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

#define ANALY_FORECAST_DEFAULT_ROWS    10
#define ANALY_FORECAST_DEFAULT_CONF    95
#define ANALY_FORECAST_DEFAULT_WNCHECK 1
#define ANALY_FORECAST_MAX_ROWS        40000
#define ANALY_FORECAST_MIN_ROWS        10
#define ANALY_FORECAST_RES_MAX_ROWS    1024
#define ANALY_ANOMALY_WINDOW_MAX_ROWS  40000
#define ANALY_DEFAULT_TIMEOUT          60
#define ANALY_MAX_TIMEOUT              600

typedef struct {
  EAnalAlgoType type;
  int32_t       anode;
  int32_t       urlLen;
  char         *url;
} SAnalyticsUrl;

typedef enum {
  ANALYTICS_BUF_TYPE_JSON = 0,
  ANALYTICS_BUF_TYPE_JSON_COL = 1,
  ANALYTICS_BUF_TYPE_OTHERS,
} EAnalBufType;

typedef enum {
  ANALYTICS_HTTP_TYPE_GET = 0,
  ANALYTICS_HTTP_TYPE_POST,
} EAnalyHttpType;

typedef struct {
  TdFilePtr filePtr;
  char      fileName[TSDB_FILENAME_LEN + 10];
  int64_t   numOfRows;
} SAnalyticsColBuf;

typedef struct {
  EAnalBufType bufType;
  TdFilePtr    filePtr;
  char         fileName[TSDB_FILENAME_LEN];
  int32_t      numOfCols;
  SAnalyticsColBuf *pCols;
} SAnalyticBuf;

int32_t taosAnalyticsInit();
void    taosAnalyticsCleanup();
SJson  *taosAnalySendReqRetJson(const char *url, EAnalyHttpType type, SAnalyticBuf *pBuf, int64_t timeout);

int32_t taosAnalyGetAlgoUrl(const char *algoName, EAnalAlgoType type, char *url, int32_t urlLen);
bool    taosAnalyGetOptStr(const char *option, const char *optName, char *optValue, int32_t optMaxLen);
bool    taosAnalyGetOptInt(const char *option, const char *optName, int64_t *optValue);
int64_t taosAnalyGetVersion();
void    taosAnalyUpdate(int64_t newVer, SHashObj *pHash);

int32_t tsosAnalyBufOpen(SAnalyticBuf *pBuf, int32_t numOfCols);
int32_t taosAnalyBufWriteOptStr(SAnalyticBuf *pBuf, const char *optName, const char *optVal);
int32_t taosAnalyBufWriteOptInt(SAnalyticBuf *pBuf, const char *optName, int64_t optVal);
int32_t taosAnalyBufWriteOptFloat(SAnalyticBuf *pBuf, const char *optName, float optVal);
int32_t taosAnalyBufWriteColMeta(SAnalyticBuf *pBuf, int32_t colIndex, int32_t colType, const char *colName);
int32_t taosAnalyBufWriteDataBegin(SAnalyticBuf *pBuf);
int32_t taosAnalyBufWriteColBegin(SAnalyticBuf *pBuf, int32_t colIndex);
int32_t taosAnalyBufWriteColData(SAnalyticBuf *pBuf, int32_t colIndex, int32_t colType, void *colValue);
int32_t taosAnalyBufWriteColEnd(SAnalyticBuf *pBuf, int32_t colIndex);
int32_t taosAnalyBufWriteDataEnd(SAnalyticBuf *pBuf);
int32_t taosAnalyBufClose(SAnalyticBuf *pBuf);
void    taosAnalyBufDestroy(SAnalyticBuf *pBuf);

const char   *taosAnalysisAlgoType(EAnalAlgoType algoType);
EAnalAlgoType taosAnalyAlgoInt(const char *algoName);
const char   *taosAnalyAlgoUrlStr(EAnalAlgoType algoType);

#ifdef __cplusplus
}
#endif
#endif /*_TD_UTIL_ANAL_H_*/