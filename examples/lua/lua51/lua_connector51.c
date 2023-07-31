#include <math.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include "taos.h"
#include "lauxlib.h"
#include "lua.h"
#include "lualib.h"

struct cb_param{
  lua_State* state;
  int callback;
  void * stream;
};

struct async_query_callback_param{
  lua_State* state;
  int callback;
};

static int l_connect(lua_State *L){
  TAOS *    taos=NULL;
  const char* host;
  const char* database;
  const char* user;
  const char* password;
  int port;

  luaL_checktype(L, 1, LUA_TTABLE);

  lua_getfield(L, 1,"host");
  if (lua_isstring(L, -1)){
    host = lua_tostring(L, -1);
    // printf("host = %s\n", host);
  }
  
  lua_getfield(L, 1, "port");
  if (lua_isnumber(L, -1)){
    port = lua_tonumber(L, -1);
    //printf("port = %d\n", port);
  }
  
  lua_getfield(L, 1, "database");
  if (lua_isstring(L, -1)){
    database = lua_tostring(L, -1);
    //printf("database = %s\n", database);
  }
  
  lua_getfield(L, 1, "user");
  if (lua_isstring(L, -1)){
    user = lua_tostring(L, -1);
    //printf("user = %s\n", user);
  }

  lua_getfield(L, 1, "password");
  if (lua_isstring(L, -1)){
    password = lua_tostring(L, -1);
    //printf("password = %s\n", password);
  }

  lua_settop(L, 0);

  taos_init();
  
  lua_newtable(L);
  int table_index = lua_gettop(L);

  taos = taos_connect(host, user,password,database, port);
  if (taos == NULL) {
    //printf("failed to connect server, reason:%s\n", taos_errstr(taos));
    
    lua_pushinteger(L, -1);
    lua_setfield(L, table_index, "code");
    lua_pushstring(L, taos_errstr(taos));
    lua_setfield(L, table_index, "error");    
    lua_pushlightuserdata(L,NULL);
    lua_setfield(L, table_index, "conn");
  }else{
    // printf("success to connect server\n");
    lua_pushinteger(L, 0);
    lua_setfield(L, table_index, "code");
    lua_pushstring(L, "success");
    lua_setfield(L, table_index, "error");    
    lua_pushlightuserdata(L,taos);
    lua_setfield(L, table_index, "conn");
  }
  
  return 1;
}

static int l_query(lua_State *L){
  TAOS *taos= (TAOS*)lua_topointer(L,1);
  const char* s = lua_tostring(L, 2);
  TAOS_RES *result;
  lua_newtable(L);
  int table_index = lua_gettop(L);

  //  printf("receive command:%s\r\n",s);
  result = taos_query(taos, s);
  int32_t code = taos_errno(result);
  if( code != 0){
    printf("failed, reason:%s\n", taos_errstr(result));
    lua_pushinteger(L, -1);
    lua_setfield(L, table_index, "code");    
    lua_pushstring(L, taos_errstr(result));
    lua_setfield(L, table_index, "error");    
   
    return 1;
    
  }else{
    //printf("success to query.\n");
    TAOS_ROW    row;
    int         rows = 0;
    int         num_fields = taos_field_count(result);
    const TAOS_FIELD *fields = taos_fetch_fields(result);

    const int affectRows = taos_affected_rows(result);
    //    printf(" affect rows:%d\r\n", affectRows);
    lua_pushinteger(L, 0);
    lua_setfield(L, table_index, "code");
    lua_pushinteger(L, affectRows);
    lua_setfield(L, table_index, "affected");
    lua_newtable(L);

    while ((row = taos_fetch_row(result))) {
      //printf("row index:%d\n",rows);
      rows++;

      lua_pushnumber(L, rows);
      lua_newtable(L);

      for (int i = 0; i < num_fields; ++i) {
	if (row[i] == NULL) {
	  continue;
	}

	lua_pushstring(L,fields[i].name);
	int32_t* length = taos_fetch_lengths(result);
	switch (fields[i].type) {
	case TSDB_DATA_TYPE_UTINYINT:
	case TSDB_DATA_TYPE_TINYINT:
	  lua_pushinteger(L,*((char *)row[i]));
	  break;
	case TSDB_DATA_TYPE_USMALLINT:
	case TSDB_DATA_TYPE_SMALLINT:
	  lua_pushinteger(L,*((short *)row[i]));
	  break;
	case TSDB_DATA_TYPE_UINT:
	case TSDB_DATA_TYPE_INT:
	  lua_pushinteger(L,*((int *)row[i]));
	  break;
	case TSDB_DATA_TYPE_UBIGINT:
	case TSDB_DATA_TYPE_BIGINT:
	  lua_pushinteger(L,*((int64_t *)row[i]));
	  break;
	case TSDB_DATA_TYPE_FLOAT:
	  lua_pushnumber(L,*((float *)row[i]));
	  break;
	case TSDB_DATA_TYPE_DOUBLE:
	  lua_pushnumber(L,*((double *)row[i]));
	  break;
	case TSDB_DATA_TYPE_JSON:
	case TSDB_DATA_TYPE_BINARY:
	case TSDB_DATA_TYPE_NCHAR:
	case TSDB_DATA_TYPE_GEOMETRY:
	  //printf("type:%d, max len:%d, current len:%d\n",fields[i].type, fields[i].bytes, length[i]);
	  lua_pushlstring(L,(char *)row[i], length[i]);
	  break;
	case TSDB_DATA_TYPE_TIMESTAMP:
	  lua_pushinteger(L,*((int64_t *)row[i]));
	  break;
	case TSDB_DATA_TYPE_BOOL:
	  lua_pushinteger(L,*((char *)row[i]));
	  break;
	case TSDB_DATA_TYPE_NULL:
	default:
	  lua_pushnil(L);
	  break;
	}

	lua_settable(L,-3);
      }

      lua_settable(L,-3);
    }
    taos_free_result(result);    
  }

  lua_setfield(L, table_index, "item");
  return 1;
}

void async_query_callback(void *param, TAOS_RES *result, int code){
  struct async_query_callback_param* p = (struct async_query_callback_param*) param;

  //printf("\nin c,numfields:%d\n", numFields);
  //printf("\nin c, code:%d\n", code);

  lua_State *L = p->state;
  lua_rawgeti(L, LUA_REGISTRYINDEX, p->callback);
  lua_newtable(L);
  int table_index = lua_gettop(L);
  if( code < 0){
    printf("failed, reason:%s\n", taos_errstr(result));
    lua_pushinteger(L, -1);
    lua_setfield(L, table_index, "code");    
    lua_pushstring(L, taos_errstr(result));
    lua_setfield(L, table_index, "error");    
  }else{
    //printf("success to async query.\n");
    const int affectRows = taos_affected_rows(result);
    //printf(" affect rows:%d\r\n", affectRows);
    lua_pushinteger(L, 0);
    lua_setfield(L, table_index, "code");
    lua_pushinteger(L, affectRows);
    lua_setfield(L, table_index, "affected");
  }
  
  lua_call(L, 1, 0);
}

static int l_async_query(lua_State *L){
  int r = luaL_ref(L, LUA_REGISTRYINDEX);
  TAOS *    taos = (TAOS*)lua_topointer(L, 1);
  const char * sqlstr = lua_tostring(L, 2);
  // int stime = luaL_checknumber(L, 3);

  lua_newtable(L);
  int table_index = lua_gettop(L);

  struct async_query_callback_param *p = malloc(sizeof(struct async_query_callback_param));
  p->state = L;
  p->callback=r;
  // printf("r:%d, L:%d\n", r, L);
  taos_query_a(taos,sqlstr,async_query_callback,p);

  lua_pushnumber(L, 0);
  lua_setfield(L, table_index, "code");
  lua_pushstring(L, "ok");
  lua_setfield(L, table_index, "error");
  
  return 1;
}


static int l_close(lua_State *L){
  TAOS *taos= (TAOS*)lua_topointer(L,1);
  lua_newtable(L);
  int table_index = lua_gettop(L);
  
  if(taos == NULL){
    lua_pushnumber(L, -1);
    lua_setfield(L, table_index, "code");    
    lua_pushstring(L, "null pointer.");
    lua_setfield(L, table_index, "error"); 
  }else{
    taos_close(taos);
    lua_pushnumber(L, 0);
    lua_setfield(L, table_index, "code");    
    lua_pushstring(L, "done.");
    lua_setfield(L, table_index, "error");    
  }
  return 1;
}

static const struct luaL_Reg lib[] = {
    {"connect", l_connect},
    {"query", l_query},
    {"query_a",l_async_query},
    {"close", l_close},
    {NULL, NULL}
};

extern int luaopen_luaconnector51(lua_State* L)
{
  //  luaL_register(L, "luaconnector51", lib);
  lua_newtable(L);
  luaL_setfuncs(L,lib,0);
  return 1;
}
