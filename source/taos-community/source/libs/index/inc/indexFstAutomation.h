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
#ifndef __INDEX_FST_AUTAOMATION_H__
#define __INDEX_FST_AUTAOMATION_H__

#ifdef __cplusplus
extern "C" {
#endif

#include "indexFstUtil.h"
#include "indexInt.h"

typedef struct FAutoCtx FAutoCtx;

typedef enum AutomationType { AUTOMATION_ALWAYS, AUTOMATION_PREFIX, AUTMMATION_MATCH } AutomationType;

typedef struct StartWith {
  FAutoCtx* autoSelf;
} StartWith;

typedef struct Complement {
  FAutoCtx* autoSelf;
} Complement;

// automation
typedef struct FAutoCtx {
  AutomationType type;
  void*          stdata;
  char*          data;
} FAutoCtx;

typedef enum ValueType { FST_INT, FST_CHAR, FST_ARRAY } ValueType;
typedef enum StartWithStateKind { Done, Running } StartWithStateKind;

typedef struct StartWithStateValue {
  StartWithStateKind kind;
  ValueType          type;
  union {
    int     val;
    char*   ptr;
    SArray* arr;
    // add more type
  };
} StartWithStateValue;

StartWithStateValue* startWithStateValueCreate(StartWithStateKind kind, ValueType ty, void* val);
StartWithStateValue* startWithStateValueDump(StartWithStateValue* sv);
void                 startWithStateValueDestroy(void* sv);

typedef struct AutomationFunc {
  void* (*start)(FAutoCtx* ctx);
  bool (*isMatch)(FAutoCtx* ctx, void*);
  bool (*canMatch)(FAutoCtx* ctx, void* data);
  bool (*willAlwaysMatch)(FAutoCtx* ctx, void* state);
  void* (*accept)(FAutoCtx* ctx, void* state, uint8_t byte);
  void* (*acceptEof)(FAutoCtx* ct, void* state);
} AutomationFunc;

FAutoCtx* automCtxCreate(void* data, AutomationType atype);
void      automCtxDestroy(FAutoCtx* ctx);

extern AutomationFunc automFuncs[];
#ifdef __cplusplus
}
#endif

#endif
