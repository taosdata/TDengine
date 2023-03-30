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

#ifndef TDENGINE_GEOM_FUNC_H
#define TDENGINE_GEOM_FUNC_H

#ifdef __cplusplus
extern "C" {
#endif

#include "function.h"

int32_t makePointFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);

int32_t geomFromTextFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t asTextFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);

int32_t intersectsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t equalsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t touchesFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t coversFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t containsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t containsProperlyFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_GEOM_FUNC_H
