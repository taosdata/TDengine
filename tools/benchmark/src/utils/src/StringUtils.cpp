#include "StringUtils.h"
#include <algorithm>
#include <cctype>
#include <string>
#include <iterator>


std::string StringUtils::to_lower(const std::string& str) {
    std::string lower_str = str;
    std::transform(lower_str.begin(), lower_str.end(), lower_str.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return lower_str;
}

std::string StringUtils::to_upper(const std::string& str) {
    std::string upper_str = str;
    std::transform(upper_str.begin(), upper_str.end(), upper_str.begin(),
                   [](unsigned char c) { return std::toupper(c); });
    return upper_str;
}

void StringUtils::trim(std::string& str) {
    str.erase(str.begin(), std::find_if(str.begin(), str.end(), [](unsigned char ch) {
        return !std::isspace(ch);
    }));
    
    str.erase(std::find_if(str.rbegin(), str.rend(), [](unsigned char ch) {
        return !std::isspace(ch);
    }).base(), str.end());
}