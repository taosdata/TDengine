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

#ifndef TDENGINE_QSCRIPT_H
#define TDENGINE_QSCRIPT_H

#if 0
#include <lauxlib.h>
#include <lua.h>
#include <lualib.h>

#include "hash.h"
#include "tlist.h"
#include "tudf.h"
#include "tutil.h"

#define MAX_FUNC_NAME 64

#define USER_FUNC_NAME       "funcName"
#define USER_FUNC_NAME_LIMIT 48

enum ScriptState {
  SCRIPT_STATE_INIT,
  SCRIPT_STATE_ADD,
  SCRIPT_STATE_MERGE,
  SCRIPT_STATE_FINALIZE 
};


typedef struct {
  SHashObj  *funcId;    //func already registed in lua_env, may be no use
  lua_State *lua_state; // lua env 
} ScriptEnv;   

typedef struct ScriptCtx {
  char        funcName[USER_FUNC_NAME_LIMIT]; 
  int8_t      state; 
  ScriptEnv  *pEnv;
  int8_t      isAgg; // agg function or not
  
  // init value of udf script
  int8_t      resType;
  int16_t     resBytes; 

  int32_t     numOfOutput; 
  int32_t     offset;
  
} ScriptCtx;

int taosLoadScriptInit(void *pInit);
void taosLoadScriptNormal(void *pInit, char *pInput, int16_t iType, int16_t iBytes, int32_t numOfRows, 
    int64_t *ptsList, int64_t key, char* pOutput, char *ptsOutput, int32_t *numOfOutput, int16_t oType, int16_t oBytes);
void taosLoadScriptFinalize(void *pInit, int64_t key, char *pOutput, int32_t *output);
void taosLoadScriptMerge(void *pCtx, char* data, int32_t numOfRows, char* dataOutput, int32_t* numOfOutput);
void taosLoadScriptDestroy(void *pInit);

typedef struct {
  SList     *scriptEnvs; //  
  int32_t   mSize;  // pool limit
  int32_t   cSize;  // current available size
  TdThreadMutex mutex;
} ScriptEnvPool;

ScriptCtx* createScriptCtx(char *str, int8_t resType, int16_t resBytes);
void       destroyScriptCtx(void *pScriptCtx);

int32_t scriptEnvPoolInit();
void    scriptEnvPoolCleanup();
bool    isValidScript(char *script, int32_t len);
#endif

#endif  // TDENGINE_QSCRIPT_H
