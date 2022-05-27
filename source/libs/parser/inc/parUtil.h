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

#include "catalog.h"
#include "os.h"
#include "query.h"

#define parserFatal(param, ...) qFatal("PARSER: " param, __VA_ARGS__)
#define parserError(param, ...) qError("PARSER: " param, __VA_ARGS__)
#define parserWarn(param, ...)  qWarn("PARSER: " param, __VA_ARGS__)
#define parserInfo(param, ...)  qInfo("PARSER: " param, __VA_ARGS__)
#define parserDebug(param, ...) qDebug("PARSER: " param, __VA_ARGS__)
#define parserTrace(param, ...) qTrace("PARSER: " param, __VA_ARGS__)

#define PK_TS_COL_INTERNAL_NAME "_rowts"

typedef struct SMsgBuf {
  int32_t len;
  char*   buf;
} SMsgBuf;

typedef struct SParseMetaCache {
  SHashObj* pTableMeta;    // key is tbFName, element is STableMeta*
  SHashObj* pDbVgroup;     // key is dbFName, element is SArray<SVgroupInfo>*
  SHashObj* pTableVgroup;  // key is tbFName, element is SVgroupInfo*
  SHashObj* pDbCfg;        // key is tbFName, element is SDbCfgInfo
} SParseMetaCache;

int32_t generateSyntaxErrMsg(SMsgBuf* pBuf, int32_t errCode, ...);
int32_t buildInvalidOperationMsg(SMsgBuf* pMsgBuf, const char* msg);
int32_t buildSyntaxErrMsg(SMsgBuf* pBuf, const char* additionalInfo, const char* sourceStr);

SSchema*      getTableColumnSchema(const STableMeta* pTableMeta);
SSchema*      getTableTagSchema(const STableMeta* pTableMeta);
int32_t       getNumOfColumns(const STableMeta* pTableMeta);
int32_t       getNumOfTags(const STableMeta* pTableMeta);
STableComInfo getTableInfo(const STableMeta* pTableMeta);
STableMeta*   tableMetaDup(const STableMeta* pTableMeta);
int32_t       parseJsontoTagData(const char* json, SKVRowBuilder* kvRowBuilder, SMsgBuf* errMsg, int16_t startColId);

int32_t trimString(const char* src, int32_t len, char* dst, int32_t dlen);

int32_t buildCatalogReq(const SParseMetaCache* pMetaCache, SCatalogReq* pCatalogReq);
int32_t putMetaDataToCache(const SCatalogReq* pCatalogReq, const SMetaData* pMetaData, SParseMetaCache* pMetaCache);
int32_t reserveTableMetaInCache(int32_t acctId, const char* pDb, const char* pTable, SParseMetaCache* pMetaCache);
int32_t getTableMetaFromCache(SParseMetaCache* pMetaCache, const SName* pName, STableMeta** pMeta);
int32_t getDBVgInfoFromCache(SParseMetaCache* pMetaCache, const char* pDbFName, SArray** pVgInfo);
int32_t getTableHashVgroupFromCache(SParseMetaCache* pMetaCache, const SName* pName, SVgroupInfo* pVgroup);
int32_t getDBVgVersionFromCache(SParseMetaCache* pMetaCache, const char* pDbFName, int32_t* pVersion, int64_t* pDbId,
                                int32_t* pTableNum);
int32_t getDBCfgFromCache(SParseMetaCache* pMetaCache, const char* pDbFName, SDbCfgInfo* pInfo);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_PARSER_UTIL_H
