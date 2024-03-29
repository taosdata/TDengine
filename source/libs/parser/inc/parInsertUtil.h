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

#ifndef TDENGINE_PAR_INSERT_UTIL_H
#define TDENGINE_PAR_INSERT_UTIL_H

#include "parUtil.h"

struct SToken;

#define NEXT_TOKEN(pSql, sToken)                      \
  do {                                                \
    int32_t index = 0;                                \
    sToken = tStrGetToken(pSql, &index, false, NULL); \
    pSql += index;                                    \
  } while (0)

#define CHECK_CODE(expr)             \
  do {                               \
    int32_t code = expr;             \
    if (TSDB_CODE_SUCCESS != code) { \
      return code;                   \
    }                                \
  } while (0)

typedef struct SVgroupDataCxt {
  int32_t      vgId;
  SSubmitReq2 *pData;
} SVgroupDataCxt;

int32_t insCreateSName(SName *pName, struct SToken *pTableName, int32_t acctId, const char *dbName, SMsgBuf *pMsgBuf);
int16_t insFindCol(struct SToken *pColname, int16_t start, int16_t end, SSchema *pSchema);
void    insBuildCreateTbReq(SVCreateTbReq *pTbReq, const char *tname, STag *pTag, int64_t suid, const char *sname,
                            SArray *tagName, uint8_t tagNum, int32_t ttl);
int32_t insInitBoundColsInfo(int32_t numOfBound, SBoundColInfo *pInfo);
void    insInitColValues(STableMeta* pTableMeta, SArray* aColValues);
void    insCheckTableDataOrder(STableDataCxt *pTableCxt, TSKEY tsKey);
int32_t insGetTableDataCxt(SHashObj *pHash, void *id, int32_t idLen, STableMeta *pTableMeta,
                           SVCreateTbReq **pCreateTbReq, STableDataCxt **pTableCxt, bool colMode, bool ignoreColVals);
int32_t initTableColSubmitData(STableDataCxt *pTableCxt);
int32_t insMergeTableDataCxt(SHashObj *pTableHash, SArray **pVgDataBlocks, bool isRebuild);
int32_t insBuildVgDataBlocks(SHashObj *pVgroupsHashObj, SArray *pVgDataBlocks, SArray **pDataBlocks);
void    insDestroyTableDataCxtHashMap(SHashObj *pTableCxtHash);
void    insDestroyVgroupDataCxt(SVgroupDataCxt *pVgCxt);
void    insDestroyVgroupDataCxtList(SArray *pVgCxtList);
void    insDestroyVgroupDataCxtHashMap(SHashObj *pVgCxtHash);
void    insDestroyTableDataCxt(STableDataCxt *pTableCxt);
void    insDestroyBoundColInfo(SBoundColInfo *pInfo);

#endif  // TDENGINE_PAR_INSERT_UTIL_H
