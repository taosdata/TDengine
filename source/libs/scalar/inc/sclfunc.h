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
#include "scalar.h"

typedef struct SScalarFunctionSupport {
  struct SExprInfo   *pExprInfo;
  int32_t      numOfCols;
  SColumnInfo *colList;
  void        *exprList;   // client side used
  int32_t      offset;
  char**       data;
} SScalarFunctionSupport;

extern struct SScalarFunctionInfo scalarFunc[8];

int32_t evaluateExprNodeTree(tExprNode* pExprs, int32_t numOfRows, SScalarParam* pOutput,
                          void* param, char* (*getSourceDataBlock)(void*, const char*, int32_t));


#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSCALARFUNCTION_H
