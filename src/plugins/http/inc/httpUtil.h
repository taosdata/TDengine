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

#ifndef TDENGINE_HTTP_UTIL_H
#define TDENGINE_HTTP_UTIL_H

bool httpCheckUsedbSql(char *sql);
void httpTimeToString(int32_t t, char *buf, int32_t buflen);

bool    httpUrlMatch(HttpContext *pContext, int32_t pos, char *cmp);
bool    httpParseRequest(HttpContext *pContext);
int32_t httpCheckReadCompleted(HttpContext *pContext);
void    httpReadDirtyData(HttpContext *pContext);

int32_t httpGzipDeCompress(char *srcData, int32_t nSrcData, char *destData, int32_t *nDestData);
int32_t httpGzipCompressInit(HttpContext *pContext);
int32_t httpGzipCompress(HttpContext *pContext, char *inSrcData, int32_t inSrcDataLen,
                     char *outDestData, int32_t *outDestDataLen, bool isTheLast);

// http request parser
void httpAddMethod(HttpServer *pServer, HttpDecodeMethod *pMethod);

#endif
