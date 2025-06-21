#pragma once
#include <variant>
#include "ActionConfigVariant.h"
#include "ColumnConfigInstance.h"
#include "TableData.h"
#include "FormatResult.h"


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
        virtual FormatResult format(const CreateChildTableConfig& config, 
                                    const std::vector<std::string>& table_names, 
                                    const std::vector<RowType>& tags) const = 0;
    };


class IInsertDataFormatter : public IFormatter {
public:
    virtual FormatResult format(const InsertDataConfig&, const ColumnConfigInstanceVector& col_instances, MultiBatch&& batch) const = 0;
};

