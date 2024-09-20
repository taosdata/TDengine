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

#ifndef _TD_UTIL_ANAL_FUNC_H_
#define _TD_UTIL_ANAL_FUNC_H_

#include "os.h"
#include "tdef.h"
#include "thash.h"
#include "tjson.h"

#ifdef __cplusplus
extern "C" {
#endif

#define ANAL_FUNC_FORECAST_DEFAULT_ROWS  60
#define ANAL_FUNC_FORECAST_DEFAULT_CONF  80
#define ANAL_FUNC_FORECAST_DEFAULT_EVERY 10000

typedef struct {
  EAnalFuncType type;
  int32_t       anode;
  int32_t       urlLen;
  char         *url;
} SAnalFuncUrl;

typedef struct {
  TdFilePtr filePtr;
  char      fileName[PATH_MAX];
} SAnalFuncJson;

int32_t taosFuncInit();
void    taosFuncCleanup();
SJson  *taosFuncGetJson(const char *url, bool isGet, const char *file);

void    taosFuncUpdate();
int32_t taosFuncGetUrl(const char *funcName, EAnalFuncType type, char *url, int32_t urlLen);
bool    taosFuncGetParaStr(const char *option, const char *paraName, char *paraValue, int32_t paraValueMaxLen);
bool    taosFuncGetParaInt(const char *option, const char *paraName, int32_t *paraValue);
int64_t taosFuncGetVersion();
void    taosFuncUpdate(int64_t newVer, SHashObj *pHash);
void    taosFuncFreeHash(SHashObj *pHash);

const char   *taosAnalFuncStr(EAnalFuncType type);
EAnalFuncType taosAnalFuncInt(const char *name);

int32_t taosFuncOpenJson(SAnalFuncJson *pFile);
int32_t taosFuncWritePara(SAnalFuncJson *pFile, const char *paras, const char *fmt, const char *prec);
int32_t taosFuncWriteMeta(SAnalFuncJson *pFile, int32_t c1, int32_t c2);
int32_t taosFuncWriteData(SAnalFuncJson *pFile, const char *data, bool isLast);
int32_t taosFuncWriteRows(SAnalFuncJson *pFile, int32_t numOfRows);
void    taosFuncCloseJson(SAnalFuncJson *pFile);

#ifdef __cplusplus
}
#endif
#endif /*_TD_UTIL_ANAL_FUNC_H_*/
