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

#ifndef TDENGINE_TSYNTAXTREEFUNCTION_H
#define TDENGINE_TSYNTAXTREEFUNCTION_H

#ifdef __cplusplus
extern "C" {
#endif

typedef void (*_bi_consumer_fn_t)(void *left, void *right, int32_t numOfLeft, int32_t numOfRight, void *output,
                                  int32_t order);

_bi_consumer_fn_t getArithmeticOperatorFn(int32_t leftType, int32_t rightType, int32_t optr);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSYNTAXTREEFUNCTION_H
