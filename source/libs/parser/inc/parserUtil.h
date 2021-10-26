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

#ifndef TDENGINE_PARSERUTIL_H
#define TDENGINE_PARSERUTIL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "ttoken.h"
#include "parserInt.h"

#define UTIL_TABLE_IS_SUPER_TABLE(metaInfo) \
  (((metaInfo)->pTableMeta != NULL) && ((metaInfo)->pTableMeta->tableType == TSDB_SUPER_TABLE))

#define UTIL_TABLE_IS_CHILD_TABLE(metaInfo) \
  (((metaInfo)->pTableMeta != NULL) && ((metaInfo)->pTableMeta->tableType == TSDB_CHILD_TABLE))

#define UTIL_TABLE_IS_NORMAL_TABLE(metaInfo) \
  (!(UTIL_TABLE_IS_SUPER_TABLE(metaInfo) || UTIL_TABLE_IS_CHILD_TABLE(metaInfo)))

#define UTIL_TABLE_IS_TMP_TABLE(metaInfo) \
  (((metaInfo)->pTableMeta != NULL) && ((metaInfo)->pTableMeta->tableType == TSDB_TEMP_TABLE))

TAOS_FIELD createField(const SSchema* pSchema);
SSchema createSchema(uint8_t type, int16_t bytes, int16_t colId, const char* name);

SInternalField* insertFieldInfo(SFieldInfo* pFieldInfo, int32_t index, SSchema* field);
int32_t getNumOfFields(SFieldInfo* pFieldInfo);
SInternalField* getInternalField(SFieldInfo* pFieldInfo, int32_t index);

int32_t parserValidateIdToken(SToken* pToken);
int32_t buildInvalidOperationMsg(SMsgBuf* pMsgBuf, const char* msg);
int32_t buildSyntaxErrMsg(char* dst, int32_t dstBufLen, const char* additionalInfo,  const char* sourceStr);

int32_t createProjectionExpr(SQueryStmtInfo* pQueryInfo, STableMetaInfo* pTableMetaInfo, SExprInfo*** pExpr, int32_t* num);
STableMetaInfo* addEmptyMetaInfo(SQueryStmtInfo* pQueryInfo);

void columnListCopy(SArray* dst, const SArray* src, uint64_t tableUid);
void columnListCopyAll(SArray* dst, const SArray* src);
void columnListDestroy(SArray* pColumnList);

SColumn* columnListInsert(SArray* pColumnList, int32_t columnIndex, uint64_t uid, SSchema* pSchema);
SColumn* insertPrimaryTsColumn(SArray* pColumnList, uint64_t tableUid);

void cleanupTagCond(STagCond* pTagCond);
void cleanupColumnCond(SArray** pCond);

uint32_t convertRelationalOperator(SToken *pToken);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_PARSERUTIL_H
