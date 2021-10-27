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
#ifndef TDENGINE_TSCALARFUNCTION_H
#define TDENGINE_TSCALARFUNCTION_H

#ifdef __cplusplus
extern "C" {
#endif

#include "function.h"

extern struct SScalarFunctionInfo scalarFunc[1];

#define FUNCTION_CEIL         38
#define FUNCTION_FLOOR        39
#define FUNCTION_ROUND        40
#define FUNCTION_MAVG         41
#define FUNCTION_CSUM         42

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSCALARFUNCTION_H
