#include "CreateSuperTableAction.hpp"
#include "LogUtils.hpp"
#include "FormatterRegistrar.hpp"
#include "ConnectorFactory.hpp"
#include <iostream>

void CreateSuperTableAction::prepare_connector() {
    connector_ = ConnectorFactory::create(
        config_.tdengine
    );
}

void CreateSuperTableAction::execute() {
    LogUtils::info("Creating super table: {}.{}", config_.tdengine.database, config_.schema.name);

    try {
        prepare_connector();

        auto formatter = FormatterFactory::create_formatter<CreateSuperTableConfig>(DataFormat());
        FormatResult formatted_result = formatter->format(config_);
        auto sql = std::get<std::string>(formatted_result);
        if (!connector_->execute(sql)) {
            throw std::runtime_error("Failed to execute SQL: " + sql);
        }

    } catch (const std::exception& e) {
        LogUtils::error("An error occurred: {}", e.what());
        throw;
    }

    connector_->close();
}
