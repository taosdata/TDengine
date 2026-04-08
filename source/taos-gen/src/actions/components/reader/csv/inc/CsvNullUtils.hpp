#pragma once

#include "StringUtils.hpp"
#include <string_view>

namespace CsvNullUtils {
    inline bool is_null_text(std::string_view sv) {
        std::string_view trimmed = StringUtils::trim_view(sv);
        if (trimmed.empty()) {
            return true;
        }
        return StringUtils::iequals_ascii(trimmed, "null")
            || StringUtils::iequals_ascii(trimmed, "na")
            || StringUtils::iequals_ascii(trimmed, "n/a")
            || StringUtils::iequals_ascii(trimmed, "nan");
    }
}
