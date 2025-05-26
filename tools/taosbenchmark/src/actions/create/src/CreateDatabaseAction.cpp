#include "CreateDatabaseAction.h"
#include <iostream>
#include "FormatterFactory.h"

CreateDatabaseAction::CreateDatabaseAction(const CreateDatabaseConfig& config) 
    : config_(config) {}

void CreateDatabaseAction::execute() {
    std::cout << "Creating database: " << config_.database_info.name << std::endl;

    try {
        prepare_connector();
        
        FormatResult formatted_result;
        auto formatter = FormatterFactory::instance().create_formatter<CreateDatabaseConfig>(config_.data_format);
        if (config_.database_info.drop_if_exists) {
            formatted_result = formatter->format(config_, true);
            connector_->execute(std::get<std::string>(formatted_result));
        }

        formatted_result = formatter->format(config_, false);
        connector_->execute(std::get<std::string>(formatted_result));
        
    } catch (const std::exception& e) {
        std::cerr << "An error occurred: " << e.what() << std::endl;
    }

    connector_->close();
}
