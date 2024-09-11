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

#ifndef _TD_UTIL_FUNC_H_
#define _TD_UTIL_FUNC_H_

#include "os.h"
#include "tdef.h"
#include "thash.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_AFUNC_DEFAULT_ROWS 60
#define TSDB_AFUNC_DEFAULT_CONF 80

typedef struct {
  char   *data;
  int64_t dataLen;
} SCurlResp;

typedef struct {
  EAFuncType type;
  int32_t    anode;
  int32_t    urlLen;
  char      *url;
} SAFuncUrl;

int32_t taosFuncInit();
void    taosFuncCleanup();
int32_t taosCurlGetRequest(const char *url, SCurlResp *pRsp);
int32_t taosCurlPostRequest(const char *url, SCurlResp *pRsp);

void    taosFuncUpdate();
int32_t taosFuncGetUrl(const char *funcName, EAFuncType type, char *url, int32_t urlLen);
bool    taosFuncGetParaStr(const char *option, const char *paraName, char *paraValue, int32_t paraValueMaxLen);
bool    taosFuncGetParaInt(const char *option, const char *paraName, int32_t *paraValue);
int64_t taosFuncGetVersion();
void    taosFuncUpdate(int64_t newVer, SHashObj *pHash);
void    taosFuncFreeHash(SHashObj *pHash);

const char *taosFuncStr(EAFuncType type);
EAFuncType  taosFuncInt(const char *name);

#ifdef __cplusplus
}
#endif
#endif /*_TD_UTIL_FUNC_H_*/
