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
    static void remove_all_spaces(std::string& str);

    static std::u16string utf8_to_u16string(const std::string& str);
    static std::string u16string_to_utf8(const std::u16string& str);
};