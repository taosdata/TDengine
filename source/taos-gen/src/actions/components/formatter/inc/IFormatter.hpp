#pragma once
#include "ActionConfigVariant.hpp"
#include "ColumnConfigInstance.hpp"
#include "TableData.hpp"
#include "FormatResult.hpp"
#include "ISinkContext.hpp"
#include <variant>
#include <memory>

class IFormatter {
public:
    virtual ~IFormatter() = default;
};


class IDatabaseFormatter : public IFormatter {
public:
    virtual FormatResult format(const CreateDatabaseConfig&) const = 0;
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


enum class InsertMode {
    SubTable,           // INSERT INTO ? VALUES(?,cols-qmark)
    SuperTable,         // INSERT INTO `db_name`.`stb_name`(tbname,ts,cols-name) VALUES(?,?,col-qmark)
    AutoCreateTable     // INSERT INTO ? USING `db_name`.`stb_name` TAGS (tags-qmark) VALUES(?,cols-qmark)
};

class IInsertDataFormatter : public IFormatter {
public:
    virtual std::unique_ptr<ISinkContext> init(const InsertDataConfig& config,
                                               const ColumnConfigInstanceVector& col_instances,
                                               const ColumnConfigInstanceVector& tag_instances
    ) {
        config_ = &config;
        col_instances_ = &col_instances;
        tag_instances_ = &tag_instances;
        return nullptr;
    }

    virtual FormatResult format(MemoryPool::MemoryBlock* batch,
	                            bool is_checkpoint_recover = false) const = 0;

protected:
    InsertMode mode_;

    const InsertDataConfig* config_ = nullptr;
    const ColumnConfigInstanceVector* col_instances_ = nullptr;
    const ColumnConfigInstanceVector* tag_instances_ = nullptr;

    const InsertDataConfig& config() const {
        if (!config_) throw std::logic_error("config_ not set");
        return *config_;
    }

    const ColumnConfigInstanceVector& cols() const {
         if (!col_instances_) throw std::logic_error("col_instances_ not set");
         return *col_instances_;
    }

    const ColumnConfigInstanceVector& tags() const {
        if (!tag_instances_) throw std::logic_error("tag_instances_ not set");
        return *tag_instances_;
    }
};
