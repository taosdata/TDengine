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

#include <gtest/gtest.h>

#include "tdatablock.h"
#include "geomFunc.h"

void setScalarParam(SScalarParam *sclParam, int32_t type, void *valueArray, TDRowValT valTypeArray[], int32_t rowNum);
void destroyScalarParam(SScalarParam *sclParam, int32_t colNum);

void makeOneScalarParam(SScalarParam **pSclParam, int32_t type, void *valueArray, TDRowValT valTypeArray[], int32_t rowNum);

void compareVarDataColumn(SColumnInfoData *columnData1, SColumnInfoData *columnData2, int32_t rowNum);

void callGeomFromTextWrapper5(void *strArray, TDRowValT valTypeArray[], int32_t rowNum, SScalarParam *pOutputGeomFromText);
