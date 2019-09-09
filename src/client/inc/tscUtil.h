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

#ifndef TDENGINE_TSCUTIL_H
#define TDENGINE_TSCUTIL_H

#ifdef __cplusplus
extern "C" {
#endif

/*
 * @date   2018/09/30
 */
#include <stdio.h>
#include "tsdb.h"
#include "tsclient.h"
#include "textbuffer.h"

#define UTIL_METER_IS_METRIC(cmd) (((cmd)->pMeterMeta != NULL) && ((cmd)->pMeterMeta->meterType == TSDB_METER_METRIC))

#define UTIL_METER_IS_NOMRAL_METER(cmd) (!(UTIL_METER_IS_METRIC(cmd)))

#define UTIL_METER_IS_CREATE_FROM_METRIC(cmd) \
  (((cmd)->pMeterMeta != NULL) && ((cmd)->pMeterMeta->meterType == TSDB_METER_MTABLE))

typedef struct SParsedColElem {
  int16_t colIndex;
  int16_t offset;
} SParsedColElem;

typedef struct SParsedDataColInfo {
  int16_t        numOfCols;
  int16_t        numOfAssignedCols;
  SParsedColElem elems[TSDB_MAX_COLUMNS];
  bool           hasVal[TSDB_MAX_COLUMNS];
} SParsedDataColInfo;

STableDataBlocks* tscCreateDataBlock(int32_t size);
void tscDestroyDataBlock(STableDataBlocks* pDataBlock);
void tscAppendDataBlock(SDataBlockList* pList, STableDataBlocks* pBlocks);

SDataBlockList* tscCreateBlockArrayList();
void* tscDestroyBlockArrayList(SDataBlockList* pList);
int32_t tscCopyDataBlockToPayload(SSqlObj* pSql, STableDataBlocks* pDataBlock);
void tscFreeUnusedDataBlocks(SDataBlockList* pList);
void tscMergeTableDataBlocks(SSqlObj* pSql, SDataBlockList* pDataList);
STableDataBlocks* tscGetDataBlockFromList(void* pHashList, SDataBlockList* pDataBlockList, int64_t id, int32_t size,
                                          int32_t startOffset, int32_t rowSize, char* tableId);
STableDataBlocks* tscCreateDataBlockEx(size_t size, int32_t rowSize, int32_t startOffset, char* name);

SVnodeSidList* tscGetVnodeSidList(SMetricMeta* pMetricmeta, int32_t vnodeIdx);
SMeterSidExtInfo* tscGetMeterSidInfo(SVnodeSidList* pSidList, int32_t idx);

bool tscProjectionQueryOnMetric(SSqlObj* pSql);
bool tscIsTwoStageMergeMetricQuery(SSqlObj* pSql);

/**
 *
 * for the projection query on metric or point interpolation query on metric,
 * we iterate all the meters, instead of invoke query on all qualified meters simultaneously.
 *
 * @param pSql  sql object
 * @return
 *
 */
bool tscIsFirstProjQueryOnMetric(SSqlObj* pSql);
bool tscIsPointInterpQuery(SSqlCmd* pCmd);
void tscClearInterpInfo(SSqlCmd* pCmd);

int32_t setMeterID(SSqlObj* pSql, SSQLToken* pzTableName);

bool tscIsInsertOrImportData(char* sqlstr);

/* use for keep current db info temporarily, for handle table with db prefix */
void tscGetDBInfoFromMeterId(char* meterId, char* db);

void tscGetMetricMetaCacheKey(SSqlCmd* pCmd, char* keyStr);
bool tscQueryOnMetric(SSqlCmd* pCmd);

int tscAllocPayloadWithSize(SSqlCmd* pCmd, int size);

void tscFieldInfoSetValFromSchema(SFieldInfo* pFieldInfo, int32_t index, SSchema* pSchema);
void tscFieldInfoSetValFromField(SFieldInfo* pFieldInfo, int32_t index, TAOS_FIELD* pField);
void tscFieldInfoSetValue(SFieldInfo* pFieldInfo, int32_t index, int8_t type, char* name, int16_t bytes);

void tscFieldInfoCalOffset(SSqlCmd* pCmd);
void tscFieldInfoRenewOffsetForInterResult(SSqlCmd* pCmd);
void tscFieldInfoClone(SFieldInfo* src, SFieldInfo* dst);

TAOS_FIELD* tscFieldInfoGetField(SSqlCmd* pCmd, int32_t index);
int16_t tscFieldInfoGetOffset(SSqlCmd* pCmd, int32_t index);
int32_t tscGetResRowLength(SSqlCmd* pCmd);
void tscClearFieldInfo(SSqlCmd* pCmd);

void addExprParams(SSqlExpr* pExpr, char* argument, int32_t type, int32_t bytes);

SSqlExpr* tscSqlExprInsert(SSqlCmd* pCmd, int32_t index, int16_t functionId, int16_t srcColumnIndex, int16_t type,
                           int16_t size);
SSqlExpr* tscSqlExprUpdate(SSqlCmd* pCmd, int32_t index, int16_t functionId, int16_t srcColumnIndex, int16_t type,
                           int16_t size);

SSqlExpr* tscSqlExprGet(SSqlCmd* pCmd, int32_t index);
void tscSqlExprClone(SSqlExprInfo* src, SSqlExprInfo* dst);

SColumnBase* tscColumnInfoInsert(SSqlCmd* pCmd, int32_t colIndex);

void tscColumnInfoClone(SColumnsInfo* src, SColumnsInfo* dst);
SColumnBase* tscColumnInfoGet(SSqlCmd* pCmd, int32_t index);
void tscColumnInfoReserve(SSqlCmd* pCmd, int32_t size);

int32_t tscValidateName(SSQLToken* pToken);

void tscIncStreamExecutionCount(void* pStream);

bool tscValidateColumnId(SSqlCmd* pCmd, int32_t colId);

// get starter position of metric query condition (query on tags) in SSqlCmd.payload
char* tsGetMetricQueryCondPos(STagCond* pCond);
void tscTagCondAssign(STagCond* pDst, STagCond* pSrc);
void tscTagCondRelease(STagCond* pCond);
void tscTagCondSetQueryCondType(STagCond* pCond, int16_t type);

void tscGetSrcColumnInfo(SSrcColumnInfo* pColInfo, SSqlCmd* pCmd);

void tscSetFreeHeatBeat(STscObj* pObj);
bool tscShouldFreeHeatBeat(SSqlObj* pHb);
void tscCleanSqlCmd(SSqlCmd* pCmd);
bool tscShouldFreeAsyncSqlObj(SSqlObj* pSql);
void tscDoQuery(SSqlObj* pSql);

void sortRemoveDuplicates(STableDataBlocks* dataBuf);
#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSCUTIL_H
