#pragma once
#include "EncodingType.hpp"
#include <string>
#include <stdexcept>
#include <iconv.h>
#include <cerrno>
#include <cstring>

class EncodingConverter {
public:
    static std::string convert(const std::string& data, 
                               EncodingType from, 
                               EncodingType to);
    
private:
    static const char* encoding_to_iconv(EncodingType type);
    static void throw_iconv_error(const char* operation);
};
