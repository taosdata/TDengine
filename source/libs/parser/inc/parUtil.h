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

#ifndef TDENGINE_PARSER_UTIL_H
#define TDENGINE_PARSER_UTIL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "query.h"

#define parserFatal(param, ...)  qFatal("PARSER: " param, __VA_ARGS__)
#define parserError(param, ...)  qError("PARSER: " param, __VA_ARGS__)
#define parserWarn(param, ...)   qWarn("PARSER: " param, __VA_ARGS__)
#define parserInfo(param, ...)   qInfo("PARSER: " param, __VA_ARGS__)
#define parserDebug(param, ...)  qDebug("PARSER: " param, __VA_ARGS__)
#define parserTrace(param, ...)  qTrace("PARSER: " param, __VA_ARGS__)

typedef struct SMsgBuf {
  int32_t len;
  char   *buf;
} SMsgBuf;

int32_t generateSyntaxErrMsg(SMsgBuf* pBuf, int32_t errCode, ...);
int32_t buildInvalidOperationMsg(SMsgBuf* pMsgBuf, const char* msg);
int32_t buildSyntaxErrMsg(SMsgBuf* pBuf, const char* additionalInfo,  const char* sourceStr);

STableMeta* tableMetaDup(const STableMeta* pTableMeta);
SSchema *getTableColumnSchema(const STableMeta *pTableMeta);
SSchema *getTableTagSchema(const STableMeta* pTableMeta);
int32_t  getNumOfColumns(const STableMeta* pTableMeta);
int32_t  getNumOfTags(const STableMeta* pTableMeta);
STableComInfo getTableInfo(const STableMeta* pTableMeta);

int32_t trimString(const char* src, int32_t len, char* dst, int32_t dlen);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_PARSER_UTIL_H
