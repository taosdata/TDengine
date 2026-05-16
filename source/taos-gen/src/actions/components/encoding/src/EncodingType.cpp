#include "EncodingType.hpp"
#include "StringUtils.hpp"
#include <algorithm>
#include <string>
#include <stdexcept>

const char* encoding_to_string(EncodingType type) {
    switch (type) {
        case EncodingType::NONE:     return "NONE";
        case EncodingType::GBK:      return "GBK";
        case EncodingType::GB18030:  return "GB18030";
        case EncodingType::BIG5:     return "BIG5";
        case EncodingType::UTF8:     return "UTF-8";
        default:                     return "UNKNOWN";
    }
}

EncodingType string_to_encoding(const std::string& str) {
    std::string s = StringUtils::to_upper(str);
    if (s == "" || s == "NONE")         return EncodingType::UTF8; // NONE defaults to UTF-8
    if (s == "GBK")                     return EncodingType::GBK;
    if (s == "GB18030")                 return EncodingType::GB18030;
    if (s == "BIG5")                    return EncodingType::BIG5;
    if (s == "UTF-8" || s == "UTF8")    return EncodingType::UTF8;
    throw std::invalid_argument("Unknown encoding type: " + str);
}