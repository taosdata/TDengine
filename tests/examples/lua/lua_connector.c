#include <stdio.h>
#include <math.h>
#include <stdarg.h>
#include <stdlib.h>
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#include <taos.h>

struct cb_param{
  lua_State* state;
  int callback;
  void * stream;
};

static int l_connect(lua_State *L){
  TAOS *    taos=NULL;
  char* host;
  char* database;
  char* user;
  char* password;
  int port;

  luaL_checktype(L, 1, LUA_TTABLE);

  lua_getfield(L,-1,"host");
  if (lua_isstring(L,-1)){
    host = lua_tostring(L, -1);
    // printf("host = %s\n", host);
  }
  
  lua_getfield(L, 1, "port");
  if (lua_isinteger(L,-1)){
    port = lua_tointeger(L, -1);
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

  lua_settop(L,0);

  taos_init();
  lua_newtable(L);
  int table_index = lua_gettop(L);

  taos = taos_connect(host, user,password,database, port);
  if (taos == NULL) {
    printf("failed to connect server, reason:%s\n", taos_errstr(taos));
    
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
    lua_pushstring(L, taos_errstr(taos));
    lua_setfield(L, table_index, "error");    
    lua_pushlightuserdata(L,taos);
    lua_setfield(L, table_index, "conn");
  }
  
  return 1;
}

static int l_query(lua_State *L){
  TAOS *    taos= lua_topointer(L,1);
  char *s = lua_tostring(L, 2);
  TAOS_RES *result;
  lua_newtable(L);
  int table_index = lua_gettop(L);

  //  printf("receive command:%s\r\n",s);
    result = taos_query(taos,s);
    int32_t code = taos_errno(result);
  if( code != 0){
    printf("failed, reason:%s\n", taos_errstr(result));
    lua_pushinteger(L, -1);
    lua_setfield(L, table_index, "code");    
    lua_pushstring(L, taos_errstr(taos));
    lua_setfield(L, table_index, "error");    
   
    return 1;
    
  }else{
    //printf("success to query.\n");
    TAOS_ROW    row;
    int         rows = 0;
    int         num_fields = taos_field_count(result);
    TAOS_FIELD *fields = taos_fetch_fields(result);
    char        temp[256];

    int affectRows = taos_affected_rows(result);
    //    printf(" affect rows:%d\r\n", affectRows);
    lua_pushinteger(L, 0);
    lua_setfield(L, table_index, "code");
    lua_pushinteger(L, affectRows);
    lua_setfield(L, table_index, "affected");
    lua_newtable(L);
    
    while ((row = taos_fetch_row(result))) {
      //printf("row index:%d\n",rows);
      rows++;

      lua_pushnumber(L,rows);
      lua_newtable(L);

      for (int i = 0; i < num_fields; ++i) {
	if (row[i] == NULL) {
	  continue;
	}

	lua_pushstring(L,fields[i].name);

	switch (fields[i].type) {
	case TSDB_DATA_TYPE_TINYINT:
	  lua_pushinteger(L,*((char *)row[i]));
	  break;
	case TSDB_DATA_TYPE_SMALLINT:
	  lua_pushinteger(L,*((short *)row[i]));
	  break;
	case TSDB_DATA_TYPE_INT:
	  lua_pushinteger(L,*((int *)row[i]));
	  break;
	case TSDB_DATA_TYPE_BIGINT:
	  lua_pushinteger(L,*((int64_t *)row[i]));
	  break;
	case TSDB_DATA_TYPE_FLOAT:
	  lua_pushnumber(L,*((float *)row[i]));
	  break;
	case TSDB_DATA_TYPE_DOUBLE:
	  lua_pushnumber(L,*((double *)row[i]));
	  break;
	case TSDB_DATA_TYPE_BINARY:
	case TSDB_DATA_TYPE_NCHAR:
	  lua_pushstring(L,(char *)row[i]);
	  break;
	case TSDB_DATA_TYPE_TIMESTAMP:
	  lua_pushinteger(L,*((int64_t *)row[i]));
	  break;
	case TSDB_DATA_TYPE_BOOL:
	  lua_pushinteger(L,*((char *)row[i]));
	  break;
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

void stream_cb(void *param, TAOS_RES *result, TAOS_ROW row){
  struct cb_param* p = (struct cb_param*) param;
  TAOS_FIELD *fields = taos_fetch_fields(result);
  int         numFields = taos_num_fields(result);

  // printf("\nnumfields:%d\n", numFields);
  //printf("\n\r-----------------------------------------------------------------------------------\n");

  lua_State *L = p->state;
  lua_rawgeti(L, LUA_REGISTRYINDEX, p->callback);

  lua_newtable(L);

  for (int i = 0; i < numFields; ++i) {
    if (row[i] == NULL) {
      continue;
    }

    lua_pushstring(L,fields[i].name);

    switch (fields[i].type) {
    case TSDB_DATA_TYPE_TINYINT:
      lua_pushinteger(L,*((char *)row[i]));
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      lua_pushinteger(L,*((short *)row[i]));
      break;
    case TSDB_DATA_TYPE_INT:
      lua_pushinteger(L,*((int *)row[i]));
      break;
    case TSDB_DATA_TYPE_BIGINT:
      lua_pushinteger(L,*((int64_t *)row[i]));
      break;
    case TSDB_DATA_TYPE_FLOAT:
      lua_pushnumber(L,*((float *)row[i]));
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      lua_pushnumber(L,*((double *)row[i]));
      break;
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
      lua_pushstring(L,(char *)row[i]);
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      lua_pushinteger(L,*((int64_t *)row[i]));
      break;
    case TSDB_DATA_TYPE_BOOL:
      lua_pushinteger(L,*((char *)row[i]));
      break;
    default:
      lua_pushnil(L);
      break;
    }

    lua_settable(L, -3);
  }

  lua_call(L, 1, 0);

  //  printf("-----------------------------------------------------------------------------------\n\r");
}

static int l_open_stream(lua_State *L){
  int r = luaL_ref(L, LUA_REGISTRYINDEX);
  TAOS *    taos = lua_topointer(L,1);
  char * sqlstr = lua_tostring(L,2);
  int stime = luaL_checknumber(L,3);

  lua_newtable(L);
  int table_index = lua_gettop(L);

  struct cb_param *p = malloc(sizeof(struct cb_param));
  p->state = L;
  p->callback=r;
  //  printf("r:%d, L:%d\n",r,L);
  void * s = taos_open_stream(taos,sqlstr,stream_cb,stime,p,NULL);
  if (s == NULL) {
    printf("failed to open stream, reason:%s\n", taos_errstr(taos));
    free(p);
    lua_pushnumber(L, -1);
    lua_setfield(L, table_index, "code");
    lua_pushstring(L, taos_errstr(taos));
    lua_setfield(L, table_index, "error");
    lua_pushlightuserdata(L,NULL);
    lua_setfield(L, table_index, "stream");
  }else{
    //    printf("success to open stream\n");
    lua_pushnumber(L, 0);
    lua_setfield(L, table_index, "code");
    lua_pushstring(L, taos_errstr(taos));
    lua_setfield(L, table_index, "error");
    p->stream = s;
    lua_pushlightuserdata(L,p);
    lua_setfield(L, table_index, "stream");//stream has different content in lua and c.
  }

  return 1;
}

static int l_close_stream(lua_State *L){
  //TODO:get stream and free cb_param
  struct cb_param *p = lua_touserdata(L,1);
  taos_close_stream(p->stream);
  free(p);
  return 0;
}

static int l_close(lua_State *L){
  TAOS *    taos= lua_topointer(L,1);
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
    {"close", l_close},
    {"open_stream", l_open_stream},
    {"close_stream", l_close_stream},
    {NULL, NULL}
};

extern int luaopen_luaconnector(lua_State* L)
{
    luaL_newlib(L, lib);

    return 1;
}
