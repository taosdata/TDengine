#pragma once
#include "FunctionRegistry.hpp"
#include <string>

class NetworkFunctions {
    public:
    static std::string random_ipv4();

    static int lua_random_ipv4(lua_State* L);
};

// Declare registration function
void register_NetworkFunctions(lua_State* L);
