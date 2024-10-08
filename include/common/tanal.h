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

#define ANAL_FORECAST_DEFAULT_PERIOD 10
#define ANAL_FORECAST_DEFAULT_CONF   95
#define ANAL_FORECAST_DEFAULT_ALPHA  0.05
#define ANAL_FORECAST_DEFAULT_PARAM  "diff"

typedef struct {
  EAnalAlgoType type;
  int32_t       anode;
  int32_t       urlLen;
  char         *url;
} SAnalUrl;

typedef enum {
  ANAL_BUF_TYPE_JSON = 0,
  ANAL_BUF_TYPE_JSON_COL = 1,
  ANAL_BUF_TYPE_OTHERS,
} EAnalBufType;

typedef enum {
  ANAL_HTTP_TYPE_GET = 0,
  ANAL_HTTP_TYPE_POST,
} EAnalHttpType;

typedef struct {
  TdFilePtr filePtr;
  char      fileName[TSDB_FILENAME_LEN + 10];
  int64_t   numOfRows;
} SAnalColBuf;

typedef struct {
  EAnalBufType bufType;
  TdFilePtr    filePtr;
  char         fileName[TSDB_FILENAME_LEN];
  int32_t      numOfCols;
  SAnalColBuf *pCols;
} SAnalBuf;

int32_t taosAnalInit();
void    taosAnalCleanup();
SJson  *taosAnalSendReqRetJson(const char *url, EAnalHttpType type, SAnalBuf *pBuf);

int32_t taosAnalGetAlgoUrl(const char *algoName, EAnalAlgoType type, char *url, int32_t urlLen);
bool    taosAnalGetOptStr(const char *option, const char *optName, char *optValue, int32_t optMaxLen);
bool    taosAnalGetOptInt(const char *option, const char *optName, int32_t *optValue);
int64_t taosAnalGetVersion();
void    taosAnalUpdate(int64_t newVer, SHashObj *pHash);

int32_t tsosAnalBufOpen(SAnalBuf *pBuf, int32_t numOfCols);
int32_t taosAnalBufWriteOptStr(SAnalBuf *pBuf, const char *optName, const char *optVal);
int32_t taosAnalBufWriteOptInt(SAnalBuf *pBuf, const char *optName, int64_t optVal);
int32_t taosAnalBufWriteOptFloat(SAnalBuf *pBuf, const char *optName, float optVal);
int32_t taosAnalBufWriteColMeta(SAnalBuf *pBuf, int32_t colIndex, int32_t colType, const char *colName);
int32_t taosAnalBufWriteDataBegin(SAnalBuf *pBuf);
int32_t taosAnalBufWriteColBegin(SAnalBuf *pBuf, int32_t colIndex);
int32_t taosAnalBufWriteColData(SAnalBuf *pBuf, int32_t colIndex, int32_t colType, void *colValue);
int32_t taosAnalBufWriteColEnd(SAnalBuf *pBuf, int32_t colIndex);
int32_t taosAnalBufWriteDataEnd(SAnalBuf *pBuf);
int32_t taosAnalBufClose(SAnalBuf *pBuf);
void    taosAnalBufDestroy(SAnalBuf *pBuf);

const char   *taosAnalAlgoStr(EAnalAlgoType algoType);
EAnalAlgoType taosAnalAlgoInt(const char *algoName);
const char   *taosAnalAlgoUrlStr(EAnalAlgoType algoType);

#ifdef __cplusplus
}
#endif
#endif /*_TD_UTIL_ANAL_H_*/
