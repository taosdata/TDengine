#pragma once
#include <variant>
#include "ActionConfigVariant.h"


// 通用格式化结果类型
using FormatResult = std::variant<std::string, int>;

class IFormatter {
public:
    virtual ~IFormatter() = default;
};


class IDatabaseFormatter : public IFormatter {
public:
    virtual FormatResult format(const CreateDatabaseConfig&, bool is_drop) const = 0;
};


class ISuperTableFormatter : public IFormatter {
public:
    virtual FormatResult format(const CreateSuperTableConfig&) const = 0;
};


class IChildTableFormatter : public IFormatter {
    public:
        virtual FormatResult format(const CreateChildTableConfig& config, std::vector<std::string> table_names, std::vector<RowType> tags) const = 0;
    };


class IInsertDataFormatter : public IFormatter {
public:
    virtual FormatResult format(const InsertDataConfig&) const = 0;
};

