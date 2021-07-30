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

#include "tfunctional.h"
#include "tarray.h"


tGenericSavedFunc* genericSavedFuncInit(GenericVaFunc func, int numOfArgs) {
  tGenericSavedFunc* pSavedFunc = malloc(sizeof(tGenericSavedFunc) + numOfArgs * (sizeof(void*)));
  pSavedFunc->func = func;
  return pSavedFunc;
}

tI32SavedFunc* i32SavedFuncInit(I32VaFunc func, int numOfArgs) {
  tI32SavedFunc* pSavedFunc = malloc(sizeof(tI32SavedFunc) + numOfArgs * sizeof(void *));
  pSavedFunc->func = func;
  return pSavedFunc;
}

tVoidSavedFunc* voidSavedFuncInit(VoidVaFunc func, int numOfArgs) {
  tVoidSavedFunc* pSavedFunc = malloc(sizeof(tVoidSavedFunc) + numOfArgs * sizeof(void*));
  pSavedFunc->func = func;
  return pSavedFunc;
}

FORCE_INLINE void* genericInvoke(tGenericSavedFunc* const pSavedFunc) {
  return pSavedFunc->func(pSavedFunc->args);
}

FORCE_INLINE int32_t i32Invoke(tI32SavedFunc* const pSavedFunc) {
  return pSavedFunc->func(pSavedFunc->args);
}

FORCE_INLINE void voidInvoke(tVoidSavedFunc* const pSavedFunc) {
  if(pSavedFunc) pSavedFunc->func(pSavedFunc->args);
}
