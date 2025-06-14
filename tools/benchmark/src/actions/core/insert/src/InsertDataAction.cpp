#include "InsertDataAction.h"
#include <iostream>


void InsertDataAction::execute() {
    std::cout << "Inserting data into table: " << config_.database_info.name << "." << config_.super_table_info.name << std::endl;

}