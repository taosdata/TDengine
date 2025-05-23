#include "CreateDatabaseAction.h"
#include <iostream>

CreateDatabaseAction::CreateDatabaseAction(const CreateDatabaseConfig& config) 
    : config_(config) {}

void CreateDatabaseAction::execute() {
    std::cout << "Creating database: " << config_.database_info.name << std::endl;
    // 在此处实现具体的创建数据库逻辑
}

