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

#include "tscript.h"
#include "os.h"
#include "tstrbuild.h"
#include "ttypes.h"
//#include "queryLog.h"
#include "ttokendef.h"
#if 0
static ScriptEnvPool *pool = NULL;  

static ScriptEnv* getScriptEnvFromPool();
static void addScriptEnvToPool(ScriptEnv *pEnv);

static lua_State* createLuaEnv();
static void destroyLuaEnv(lua_State *state);

static void destroyScriptEnv(ScriptEnv *pEnv);

static void luaValueToTaosType(lua_State *lua, char *interBuf, int32_t *numOfOutput, int16_t oType, int16_t oBytes);
static void taosValueToLuaType(lua_State *lua, int32_t type, char *val);

static bool hasBaseFuncDefinedInScript(lua_State *lua, const char *funcPrefix, int32_t len);

static int userlib_exampleFunc(lua_State *lua) {
  double op1 = luaL_checknumber(lua,1);
  double op2 = luaL_checknumber(lua,2);
  lua_pushnumber(lua, op1 * op2); 
  return 1; 
}  
void luaRegisterLibFunc(lua_State *lua) {
  lua_register(lua, "exampleFunc", userlib_exampleFunc);
}

void luaLoadLib(lua_State *lua, const char *libname, lua_CFunction luafunc) {
  lua_pushcfunction(lua, luafunc);
  lua_pushstring(lua, libname);
  lua_call(lua, 1, 0);
}

LUALIB_API int (luaopen_cjson) (lua_State *L);
LUALIB_API int (luaopen_struct) (lua_State *L);
LUALIB_API int (luaopen_cmsgpack) (lua_State *L);
LUALIB_API int (luaopen_bit) (lua_State *L);


static void luaLoadLibraries(lua_State *lua) {
  luaLoadLib(lua, "", luaopen_base);
  luaLoadLib(lua, LUA_TABLIBNAME, luaopen_table);
  luaLoadLib(lua, LUA_STRLIBNAME, luaopen_string);
  luaLoadLib(lua, LUA_MATHLIBNAME, luaopen_math);
  luaLoadLib(lua, LUA_DBLIBNAME, luaopen_debug);
}
static void luaRemoveUnsupportedFunctions(lua_State *lua) {
  lua_pushnil(lua);
  lua_setglobal(lua,"loadfile");
  lua_pushnil(lua);
  lua_setglobal(lua,"dofile");
}
void taosValueToLuaType(lua_State *lua, int32_t type, char *val) {
  //TODO(dengyihao): handle more data type
  if (IS_SIGNED_NUMERIC_TYPE(type) || type == TSDB_DATA_TYPE_BOOL) {
    int64_t v;
    GET_TYPED_DATA(v, int64_t, type, val);
    lua_pushnumber(lua, (lua_Number)v);  
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    uint64_t v;
    GET_TYPED_DATA(v, uint64_t, type, val);
    lua_pushnumber(lua,(lua_Number)v);  
  } else if (IS_FLOAT_TYPE(type)) {
    double v; 
    GET_TYPED_DATA(v, double, type, val);
    lua_pushnumber(lua,v); 
  } else if (type == TSDB_DATA_TYPE_BINARY) {
    lua_pushlstring(lua, val, varDataLen(val));       
  } else if (type == TSDB_DATA_TYPE_NCHAR) {
  } 
} 
int taosLoadScriptInit(void* pInit) {
  ScriptCtx *pCtx = pInit;   
  char funcName[MAX_FUNC_NAME] = {0};
  sprintf(funcName, "%s_init", pCtx->funcName);

  lua_State* lua = pCtx->pEnv->lua_state;   
  lua_getglobal(lua, funcName);
  if (lua_pcall(lua, 0, -1, 0)) {
    lua_pop(lua, -1);
  }
  lua_setglobal(lua, "global"); 
  return 0;
}
void taosLoadScriptNormal(void *pInit, char *pInput, int16_t iType, int16_t iBytes, int32_t numOfRows, 
    int64_t *ptsList, int64_t key, char* pOutput, char *ptsOutput, int32_t *numOfOutput, int16_t oType, int16_t oBytes) { 
  ScriptCtx* pCtx = pInit;
  char funcName[MAX_FUNC_NAME] = {0};
  sprintf(funcName, "%s_add", pCtx->funcName);

  lua_State* lua  = pCtx->pEnv->lua_state;   
  lua_getglobal(lua, funcName);

  // first param of script;
  lua_newtable(lua); 
  int32_t offset = 0;
  for (int32_t i = 0; i < numOfRows; i++) {
    taosValueToLuaType(lua, iType, pInput + offset); 
    lua_rawseti(lua, -2, i+1);
    offset += iBytes;
  } 
  int isGlobalState = false; 
  lua_getglobal(lua, "global"); 
  if (lua_istable(lua, -1)) {
    isGlobalState = true; 
  }
  lua_pushnumber(lua, (lua_Number)key);
  // do call lua script 
  if (lua_pcall(lua, 3, 1, 0) != 0) {
    qError("SCRIPT ERROR: %s", lua_tostring(lua, -1)); 
    lua_pop(lua, -1);
    return;
  }
  int tNumOfOutput = 0; 
  if (isGlobalState == false) {
    luaValueToTaosType(lua, pOutput, &tNumOfOutput, oType, oBytes);
  } else {
    lua_setglobal(lua, "global");
  }
  *numOfOutput = tNumOfOutput;
}

void taosLoadScriptMerge(void *pInit, char* data, int32_t numOfRows, char* pOutput, int32_t* numOfOutput) {
  ScriptCtx *pCtx = pInit;
  char funcName[MAX_FUNC_NAME] = {0};
  sprintf(funcName, "%s_merge", pCtx->funcName);

  lua_State* lua  = pCtx->pEnv->lua_state;   
  lua_getglobal(lua, funcName);
  if (!lua_isfunction(lua, -1)) {
    qError("SCRIPT ERROR: %s", lua_tostring(lua, -1)); 
    return;
  }

  lua_getglobal(lua, "global"); 
  if (lua_pcall(lua, 1, 1, 0) != 0) {
    qError("SCRIPT ERROR: %s", lua_tostring(lua, -1)); 
    lua_pop(lua, -1);
    return;
  }
  int tNumOfOutput = 0; 
  luaValueToTaosType(lua, pOutput, &tNumOfOutput, pCtx->resType, pCtx->resBytes);
  *numOfOutput = tNumOfOutput;
}

//do not support agg now
void taosLoadScriptFinalize(void *pInit,int64_t key, char *pOutput, int32_t* numOfOutput) {
  ScriptCtx *pCtx = pInit;
  char funcName[MAX_FUNC_NAME] = {0};
  sprintf(funcName, "%s_finalize", pCtx->funcName);
  
  lua_State* lua  = pCtx->pEnv->lua_state;   
  lua_getglobal(lua, funcName);
  if (!lua_isfunction(lua, -1)) {
    qError("SCRIPT ERROR: %s", lua_tostring(lua, -1)); 
    return;
  }

  lua_getglobal(lua, "global"); 

  lua_pushnumber(lua, (lua_Number)key);
  if (lua_pcall(lua, 2, 2, 0) != 0) {
    qError("SCRIPT ERROR: %s", lua_tostring(lua, -1)); 
    lua_pop(lua, -1);
    return;
  }
  lua_setglobal(lua, "global"); 
  int tNumOfOutput = 0; 
  luaValueToTaosType(lua, pOutput, &tNumOfOutput, pCtx->resType, pCtx->resBytes);
  *numOfOutput = tNumOfOutput;
}

void taosLoadScriptDestroy(void *pInit) {
  destroyScriptCtx(pInit);
}

ScriptCtx* createScriptCtx(char *script, int8_t resType, int16_t resBytes) {
  ScriptCtx *pCtx = (ScriptCtx *)taosMemoryCalloc(1, sizeof(ScriptCtx)); 
  pCtx->state = SCRIPT_STATE_INIT; 
  pCtx->pEnv  = getScriptEnvFromPool();  //  
  pCtx->resType  = resType;
  pCtx->resBytes = resBytes; 
  
  if (pCtx->pEnv == NULL) {
    destroyScriptCtx(pCtx);
    return NULL;
  } 

  lua_State *lua = pCtx->pEnv->lua_state;
  if (luaL_dostring(lua, script)) {
    lua_pop(lua, 1);
    qError("dynamic load script failed");
    destroyScriptCtx(pCtx);
    return NULL;
  } 
  lua_getglobal(lua, USER_FUNC_NAME);
  const char *name = lua_tostring(lua, -1); 
  if (name == NULL) {
    lua_pop(lua, 1);
    qError("SCRIPT ERROR: invalid script");
    destroyScriptCtx(pCtx);
    return NULL; 
  }
  memcpy(pCtx->funcName, name, strlen(name));
  lua_pop(lua, 1);
  
  return pCtx;
}
void destroyScriptCtx(void *pCtx) {
  if (pCtx == NULL) return;
  addScriptEnvToPool(((ScriptCtx *)pCtx)->pEnv);
  taosMemoryFree(pCtx);
}

void luaValueToTaosType(lua_State *lua, char *interBuf, int32_t *numOfOutput, int16_t oType, int16_t oBytes) {
  int t = lua_type(lua,-1); 
  int32_t sz = 0;
  switch (t) {
    case LUA_TSTRING:
      //TODO(yihaodeng): handle str type
      {
        const char *v = lua_tostring(lua, -1);
        memcpy(interBuf, v, strlen(v));
        sz = 1; 
      }
      break;
    case LUA_TBOOLEAN: 
      {
        double v = lua_tonumber(lua, -1);
        memcpy(interBuf, (char *)&v, oBytes);
        sz = 1;
      }
      break;
    case LUA_TNUMBER:
      {
        if (oType == TSDB_DATA_TYPE_FLOAT) {
          float v = (float)lua_tonumber(lua, -1);
          memcpy(interBuf, (char *)&v, oBytes);
          sz = 1;
        } else if (oType == TSDB_DATA_TYPE_DOUBLE) {
          double v = (double)lua_tonumber(lua, -1);
          memcpy(interBuf, (char *)&v, oBytes);
          sz = 1;
        } else if (oType == TSDB_DATA_TYPE_BIGINT) {
          int64_t v = (int64_t)lua_tonumber(lua, -1);
          memcpy(interBuf, (char *)&v, oBytes);
          sz = 1;
        } else if (oType <= TSDB_DATA_TYPE_INT) {
          int32_t v = (int32_t)lua_tonumber(lua, -1);
          memcpy(interBuf, (char *)&v, oBytes);
          sz = 1;
        }
      }
      break;
    case LUA_TTABLE: 
      {
        lua_pushnil(lua);
        int32_t offset = 0;
        while(lua_next(lua, -2)) {
          int32_t v = (int32_t)lua_tonumber(lua, -1); 
          memcpy(interBuf + offset, (char *)&v, oBytes);
          offset += oBytes;
          lua_pop(lua, 1);
          sz += 1;
        }
      }
      break;
    default:
      setNull(interBuf, oType, oBytes);
      sz = 1;
      break; 
  }
  lua_pop(lua,1); // pop ret value from script  
  *numOfOutput = sz; 
}


/*
*Initialize the scripting environment.
*/  
lua_State* createLuaEnv() {
  lua_State *lua = lua_open(); 
  luaLoadLibraries(lua);
  luaRemoveUnsupportedFunctions(lua);

  // register func in external lib 
  luaRegisterLibFunc(lua); 

  {
    char *errh_func = "local dbg = debug\n"
                      "function __taos__err__handler(err)\n"
                      "  local i = dbg.getinfo(2,'nSl')\n"
                      "  if i and i.what == 'C' then\n"
                      "    i = dbg.getinfo(3,'nSl')\n"
                      "  end\n"
                      "  if i then\n"
                      "    return i.source .. ':' .. i.currentline .. ': ' .. err\n"
                      "  else\n"
                      "    return err\n"
                      "  end\n"
                      "end\n";
    luaL_loadbuffer(lua,errh_func,strlen(errh_func),"@err_handler_def");
    lua_pcall(lua,0,0,0);
  }
    
  return lua;
}

void destroyLuaEnv(lua_State *lua) {
  lua_close(lua); 
}

int32_t scriptEnvPoolInit() {
  const int size = 10; // configure or not 
  pool = taosMemoryMalloc(sizeof(ScriptEnvPool));  
  taosThreadMutexInit(&pool->mutex, NULL);

  pool->scriptEnvs = tdListNew(sizeof(ScriptEnv *));
  for (int i = 0; i < size; i++) {
    ScriptEnv *env = taosMemoryMalloc(sizeof(ScriptEnv));
    env->funcId = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);;  
    env->lua_state = createLuaEnv(); 
    tdListAppend(pool->scriptEnvs, (void *)(&env));  
  }

  pool->mSize = size;
  pool->cSize = size;
  return 0;  
}

void scriptEnvPoolCleanup() {
  if (pool == NULL) return;
  
  SListNode *pNode = NULL; 
  while ((pNode = tdListPopHead(pool->scriptEnvs)) != NULL) {
    ScriptEnv *pEnv = NULL;
    tdListNodeGetData(pool->scriptEnvs, pNode, (void *)(&pEnv));
    destroyScriptEnv(pEnv);
    listNodeFree(pNode); 
  }  
  tdListFree(pool->scriptEnvs);
  taosThreadMutexDestroy(&pool->mutex);
  taosMemoryFree(pool);
}

void destroyScriptEnv(ScriptEnv *pEnv) {
  destroyLuaEnv(pEnv->lua_state);       
  taosHashCleanup(pEnv->funcId); 
  taosMemoryFree(pEnv);
} 

ScriptEnv* getScriptEnvFromPool() {
  ScriptEnv *pEnv = NULL;

  taosThreadMutexLock(&pool->mutex); 
  if (pool->cSize <= 0) {
    taosThreadMutexUnlock(&pool->mutex); 
    return NULL;
  }  
  SListNode *pNode = tdListPopHead(pool->scriptEnvs);
  if (pNode){
    tdListNodeGetData(pool->scriptEnvs, pNode, (void *)(&pEnv));
    listNodeFree(pNode);
  }

  pool->cSize--;
  taosThreadMutexUnlock(&pool->mutex); 
  return pEnv; 
}

void addScriptEnvToPool(ScriptEnv *pEnv) {
  if (pEnv == NULL) {
    return;
  }
  taosThreadMutexLock(&pool->mutex); 
  lua_settop(pEnv->lua_state, 0);
  tdListAppend(pool->scriptEnvs, (void *)(&pEnv));  
  pool->cSize++;
  taosThreadMutexUnlock(&pool->mutex); 
}

bool hasBaseFuncDefinedInScript(lua_State *lua, const char *funcPrefix, int32_t len) {
  bool ret = true;
  char funcName[MAX_FUNC_NAME]; 
  memcpy(funcName, funcPrefix, len); 

  const char *base[] = {"_init", "_add"};
  for (int i = 0; (i < sizeof(base)/sizeof(base[0])) && (ret == true); i++) {
    memcpy(funcName + len, base[i], strlen(base[i])); 
    memset(funcName + len + strlen(base[i]), 0, MAX_FUNC_NAME - len - strlen(base[i]));
    lua_getglobal(lua, funcName);   
    ret = lua_isfunction(lua, -1); // exsit function or not 
    lua_pop(lua, 1);
  }
  return ret;
} 

bool isValidScript(char *script, int32_t len) {
  ScriptEnv *pEnv = getScriptEnvFromPool();  //  
  if (pEnv == NULL) {
    return false;
  }  
  lua_State *lua = pEnv->lua_state; 
  if (len < strlen(script)) {
    script[len] = 0; 
  }  
  if (luaL_dostring(lua, script)) {
    lua_pop(lua, 1);
    addScriptEnvToPool(pEnv); 
    qError("error at %s and %d", script, (int)(strlen(script)));
    return false;
  }
  lua_getglobal(lua, USER_FUNC_NAME);
  const char *name = lua_tostring(lua, -1);
  if (name == NULL || strlen(name) >= USER_FUNC_NAME_LIMIT) {
    lua_pop(lua, 1);
    addScriptEnvToPool(pEnv); 
    qError("error at %s name: %s, len = %d", script, name, (int)(strlen(name)));
    return false;
  } 
  bool ret = hasBaseFuncDefinedInScript(lua, name, (int32_t)strlen(name));
  lua_pop(lua, 1); // pop  
  addScriptEnvToPool(pEnv); 
  return ret;
}

#endif
