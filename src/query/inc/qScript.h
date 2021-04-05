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

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#include <openssl/sha.h>

#include "tutil.h"
#include "hash.h"
#include "tlist.h"
#include "qUdf.h"

#define MAX_FUNC_NAME 64

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
  char        funcName[MAX_FUNC_NAME]; 
  int8_t      state; 
  ScriptEnv  *pEnv;
  int8_t      isAgg; // agg function or not
  //void(*callback)(struct ScriptCtx*ctx,  char *input, int16_t iType, int16_t iBytes, int32_t numOfInput, int64_t* ts, char* dataOutput, 
  //  char *tsOutput, int32_t* numOfOutput, char *interbuf, int16_t oType, int16_t oBytes);  
  
  // init value of udf script
  int8_t      type;
  union       {int64_t i; double d;} initValue;
  int32_t     numOfOutput; 
  int32_t     offset;
  
} ScriptCtx;

int taosLoadScriptInit(SUdfInit* pSUdfInit);
int taosLoadScriptNormal(char *pInput, int16_t iType, int16_t iBytes, int32_t numOfRows, 
    int64_t *ptsList, char* pOutput, char *ptsOutput, int32_t *numOfOutput, 
    int16_t oType, int16_t oBytes, SUdfInit *init);
int taosLoadScriptFinalize(char *pOutput, int32_t output, SUdfInit *init);
int taosLoadScriptDestroy(SUdfInit* pSUdfInit);

typedef struct {
  SList     *scriptEnvs; //  
  int32_t   mSize;  // pool limit
  int32_t   cSize;  // current available size
  pthread_mutex_t mutex;
} ScriptEnvPool;

ScriptCtx* createScriptCtx(const char *str);
void       destroyScriptCtx(void *pScriptCtx);

int32_t scriptEnvPoolInit();
void    scriptEnvPoolCleanup();
bool    isValidScript(const char *sript);


void execUdf(struct ScriptCtx*ctx, char *input, int16_t iType, int16_t iBytes, int32_t numOfInput, 
    int64_t* ts, char* dataOutput, char *tsOutput, int32_t* numOfOutput, char *interbuf, int16_t oType, int16_t oBytes);  

#endif //TDENGINE_QSCRIPT_H 
