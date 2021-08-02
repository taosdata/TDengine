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
#ifndef TD_TFUNCTIONAL_H
#define TD_TFUNCTIONAL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"

//TODO: hard to use, trying to rewrite it using va_list

typedef void* (*GenericVaFunc)(void* args[]);
typedef int32_t (*I32VaFunc) (void* args[]);
typedef void (*VoidVaFunc) (void* args[]);

typedef struct GenericSavedFunc {
  GenericVaFunc func;
  void * args[];
} tGenericSavedFunc;

typedef struct I32SavedFunc {
  I32VaFunc func;
  void * args[];
} tI32SavedFunc;

typedef struct VoidSavedFunc {
  VoidVaFunc func;
  void * args[];
} tVoidSavedFunc;

tGenericSavedFunc* genericSavedFuncInit(GenericVaFunc func, int numOfArgs);
tI32SavedFunc* i32SavedFuncInit(I32VaFunc func, int numOfArgs);
tVoidSavedFunc* voidSavedFuncInit(VoidVaFunc func, int numOfArgs);
void* genericInvoke(tGenericSavedFunc* const pSavedFunc);
int32_t i32Invoke(tI32SavedFunc* const pSavedFunc);
void voidInvoke(tVoidSavedFunc* const pSavedFunc);

#ifdef __cplusplus
}
#endif

#endif
