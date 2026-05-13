#include "MathFunctions.hpp"

// Square wave function implementation
double MathFunctions::square_wave(int call_index, double min, double max, int period, int offset) {
    if (period <= 0) return min;
    
    int pos = (call_index + offset) % period;
    return (pos < period / 2) ? max : min;
}

// Lua binding function
int MathFunctions::lua_square_wave(lua_State* L) {
    lua_getglobal(L, "_i");
    int call_index = lua_tointeger(L, -1);
    lua_pop(L, 1);
    
    double min = luaL_checknumber(L, 1);
    double max = luaL_checknumber(L, 2);
    int period = luaL_checkinteger(L, 3);
    int offset = luaL_checkinteger(L, 4);
    
    double result = MathFunctions::square_wave(call_index, min, max, period, offset);
    lua_pushnumber(L, result);
    return 1;
}

// Registration function implementation
void register_MathFunctions(lua_State* L) {
    lua_pushcfunction(L, MathFunctions::lua_square_wave);
    lua_setglobal(L, "square");
}