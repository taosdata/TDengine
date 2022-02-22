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

#ifndef _TD_COMMON_BIN_SCALAR_OPERATOR_H_
#define _TD_COMMON_BIN_SCALAR_OPERATOR_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "sclfunc.h"

typedef void (*_bin_scalar_fn_t)(SScalarParam* pLeft, SScalarParam* pRight, void *output, int32_t order);
_bin_scalar_fn_t getBinScalarOperatorFn(int32_t binOperator);
bool isBinaryStringOp(int32_t op);

#ifdef __cplusplus
}
#endif

#endif  /*_TD_COMMON_BIN_SCALAR_OPERATOR_H_*/
