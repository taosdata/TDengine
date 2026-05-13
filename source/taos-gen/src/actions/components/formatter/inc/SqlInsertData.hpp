#pragma once
#include "SqlData.hpp"
#include <cstdint>
#include <string>
#include <typeindex>

using SqlInsertData = SqlData;
inline std::type_index SQL_TYPE_ID = std::type_index(typeid(SqlInsertData));
inline uint64_t SQL_TYPE_HASH = SQL_TYPE_ID.hash_code();