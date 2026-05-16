#include "CreateDatabaseAction.hpp"
#include "LogUtils.hpp"
#include "FormatterRegistrar.hpp"
#include "ConnectorFactory.hpp"
#include "CheckpointAction.hpp"
#include <iostream>

void CreateDatabaseAction::prepare_connector() {
    auto tdengine = config_.tdengine;
    tdengine.database.clear();
    connector_ = ConnectorFactory::create(
        tdengine
    );
}

void CreateDatabaseAction::execute() {
    LogUtils::info("Creating database: {}", config_.tdengine.database);

    try {
        prepare_connector();

        auto formatter = FormatterFactory::create_formatter<CreateDatabaseConfig>(DataFormat());

        if (CheckpointAction::is_recover(global_, config_.checkpoint_info)) {
            config_.tdengine.drop_if_exists = false;
            LogUtils::info("Checkpoint file exists. Skipping database drop");
        }

        FormatResult result = formatter->format(config_);
        const auto& stmts = std::get<std::vector<std::string>>(result);

        for (const auto& stmt : stmts) {
            if (!connector_->execute(stmt)) {
                throw std::runtime_error("Failed to execute SQL: " + stmt);
            }
        }
    } catch (const std::exception& e) {
        LogUtils::error("An error occurred: {}", e.what());
        throw;
    }

    connector_->close();
}
