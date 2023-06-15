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

#include "geomFuncTestUtil.h"

void setColumnInfo(SColumnInfo *info, int32_t colId, int32_t type, int32_t bytes) {
  memset(info, 0, sizeof(SColumnInfo));
  info->colId = colId;
  info->type = type;
  info->bytes = bytes;
}

void setScalarParam(SScalarParam *sclParam, int32_t type, void *valueArray, TDRowValT valTypeArray[], int32_t rowNum) {
  int32_t bytes = 0;
  switch (type) {
    case TSDB_DATA_TYPE_NULL: {
      bytes = -1;
      break;
    }
    case TSDB_DATA_TYPE_BOOL: {
      bytes = sizeof(int8_t);
      break;
    }
    case TSDB_DATA_TYPE_TINYINT: {
      bytes = sizeof(int8_t);
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      bytes = sizeof(int16_t);
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      bytes = sizeof(int32_t);
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      bytes = sizeof(int64_t);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      bytes = sizeof(float);
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      bytes = sizeof(double);
      break;
    }
    case TSDB_DATA_TYPE_VARCHAR: {
      bytes = TSDB_MAX_BINARY_LEN;
      break;
    }
    case TSDB_DATA_TYPE_GEOMETRY: {
      bytes = TSDB_MAX_GEOMETRY_LEN;
      break;
    }
    default: {
      ASSERT(0);
      break;
    }
  }

  sclParam->columnData = (SColumnInfoData *)taosMemoryCalloc(1, sizeof(SColumnInfoData));
  sclParam->numOfRows = rowNum;

  setColumnInfo(&sclParam->columnData->info, 0, type, bytes);
  colInfoDataEnsureCapacity(sclParam->columnData, rowNum, false);

  if (type != TSDB_DATA_TYPE_NULL && valueArray) {
    for (int32_t i = 0; i < rowNum; ++i) {
      if (tdValTypeIsNull(valTypeArray[i])) {
        colDataSetNULL(sclParam->columnData, i);
      }
      else {
        const char *val = (const char *)valueArray + (i * bytes);
        colDataSetVal(sclParam->columnData, i, val, false);
      }
    }
  }
}

void destroyScalarParam(SScalarParam *sclParam, int32_t colNum) {
  for (int32_t i = 0; i < colNum; ++i) {
    colDataDestroy((sclParam + i)->columnData);
    taosMemoryFree((sclParam + i)->columnData);
  }
  taosMemoryFree(sclParam);
}

void makeOneScalarParam(SScalarParam **pSclParam, int32_t type, void *valueArray, TDRowValT valTypeArray[], int32_t rowNum) {
  *pSclParam = (SScalarParam *)taosMemoryCalloc(1, sizeof(SScalarParam));
  setScalarParam(*pSclParam, type, valueArray, valTypeArray, rowNum);
}

bool compareVarData(unsigned char *varData1, unsigned char *varData2) {
  if (varDataLen(varData1) == 0 || varDataLen(varData2) == 0) {
    return false;
  }
  if(varDataLen(varData1) != varDataLen(varData2)) {
    return false;
  }

  return (memcmp(varDataVal(varData1), varDataVal(varData2), varDataLen(varData1)) == 0);
}

void compareVarDataColumn(SColumnInfoData *columnData1, SColumnInfoData *columnData2, int32_t rowNum) {
  for (int32_t i = 0; i < rowNum; ++i) {
    bool isNull1 = colDataIsNull_s(columnData1, i);
    bool isNull2 = colDataIsNull_s(columnData2, i);
    ASSERT_EQ((isNull1 == isNull2), true);

    if (!isNull1) {
      bool res = compareVarData((unsigned char *)colDataGetData(columnData1, i),
                                (unsigned char *)colDataGetData(columnData2, i));
      ASSERT_EQ(res, true);
    }
  }
}
