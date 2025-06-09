#pragma once

#include <string>
#include <algorithm>
#include <cctype>
#include <locale>


class StringUtils {
public:
    static std::string to_lower(const std::string& str);
    static std::string to_upper(const std::string& str);
    static void trim(std::string& str);
};