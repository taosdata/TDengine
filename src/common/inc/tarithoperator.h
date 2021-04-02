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

#ifndef TDENGINE_QARITHMETICOPERATOR_H
#define TDENGINE_QARITHMETICOPERATOR_H

#ifdef __cplusplus
extern "C" {
#endif

typedef void (*_arithmetic_operator_fn_t)(void *left, int32_t numLeft, int32_t leftType, void *right, int32_t numRight,
                                          int32_t rightType, void *output, int32_t order);

_arithmetic_operator_fn_t getArithmeticOperatorFn(int32_t arithmeticOptr);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSYNTAXTREEFUNCTION_H
