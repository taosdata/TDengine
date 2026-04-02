#pragma once

#include <string>

enum class EncodingType {
    NONE,
    GBK,
    GB18030,
    BIG5,
    UTF8
};

const char* encoding_to_string(EncodingType type);

EncodingType string_to_encoding(const std::string& str);