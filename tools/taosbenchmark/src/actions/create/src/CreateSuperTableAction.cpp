#include "CreateSuperTableAction.h"
#include <iostream>
#include "FormatterRegistrar.h"


void CreateSuperTableAction::prepare_connector() {
    connector_ = DatabaseConnector::create(
        config_.data_channel, 
        config_.connection_info
    );
}

void CreateSuperTableAction::execute() {
    std::cout << "Creating super table: " << config_.database_info.name << "." << config_.super_table_info.name << std::endl;

    try {
        prepare_connector();
        
        auto formatter = FormatterFactory::instance().create_formatter<CreateSuperTableConfig>(config_.data_format);
        FormatResult formatted_result = formatter->format(config_);
        connector_->execute(std::get<std::string>(formatted_result));

    } catch (const std::exception& e) {
        std::cerr << "An error occurred: " << e.what() << std::endl;
        throw;
    }

    connector_->close();
}
