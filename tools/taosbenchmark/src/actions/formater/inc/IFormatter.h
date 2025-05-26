#pragma once
#include <variant>
#include "ActionConfigVariant.h"


// 通用格式化结果类型
using FormatResult = std::variant<std::string, int>;

class IFormatter {
public:
    virtual ~IFormatter() = default;
    virtual FormatResult format(const CreateDatabaseConfig&, bool is_drop) const = 0;
    // virtual FormatResult format(const CreateSuperTableConfig&) const = 0;
    // virtual FormatResult format(const InsertDataConfig&) const = 0;
    // 其他Action的format方法...
};
