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

#include "index_fst_util.h"
typedef struct AutomationCtx AutomationCtx;

typedef enum AutomationType {
  AUTOMATION_PREFIX,
  AUTMMATION_MATCH 
} AutomationType;

typedef struct StartWith {
  AutomationCtx  *autoSelf;
} StartWith;

typedef struct Complement {
  AutomationCtx *autoSelf;
} Complement;

// automation 
typedef struct AutomationCtx {
  AutomationType type; 
  void *data;
} AutomationCtx;



typedef enum StartWithStateKind { Done, Running } StartWithStateKind; 

typedef struct StartWithStateValue {
  StartWithStateKind kind;
  void *value;
} StartWithStateValue;

typedef struct AutomationFunc {
  void* (*start)(AutomationCtx *ctx) ; 
  bool (*isMatch)(AutomationCtx *ctx, void *);
  bool (*canMatch)(AutomationCtx *ctx, void *data);
  bool (*willAlwaysMatch)(AutomationCtx *ctx, void *state); 
  void* (*accept)(AutomationCtx *ctx, void *state, uint8_t byte);
  void* (*acceptEof)(AutomationCtx *ct, void *state);
} AutomationFunc; 

AutomationCtx *automCtxCreate(void *data, AutomationType type);
void automCtxDestroy(AutomationCtx *ctx);

extern AutomationFunc automFuncs[]; 
#ifdef __cplusplus
}
#endif

#endif
