#include "NetworkFunctions.hpp"
#include "pcg_random.hpp"
#include <random>
#include <sstream>

// Random IPv4 generator
std::string NetworkFunctions::random_ipv4() {
    // thread_local std::mt19937 gen(std::random_device{}());
    thread_local pcg32_fast gen(pcg_extras::seed_seq_from<std::random_device>{});
    std::uniform_int_distribution<int> dist(0, 255);
    
    std::ostringstream oss;
    oss << dist(gen) << '.' << dist(gen) << '.' 
        << dist(gen) << '.' << dist(gen);
    return oss.str();
}

// Lua binding function - random IPv4
int NetworkFunctions::lua_random_ipv4(lua_State* L) {
    std::string ip = random_ipv4();
    lua_pushstring(L, ip.c_str());
    return 1;
}


// Registration function implementation
void register_NetworkFunctions(lua_State* L) {
    // Register random IPv4 function
    lua_pushcfunction(L, NetworkFunctions::lua_random_ipv4);
    lua_setglobal(L, "rand_ipv4");
}