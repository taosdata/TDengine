#ifndef CREATE_DATABASE_CONFIG_H
#define CREATE_DATABASE_CONFIG_H

#include "ConnectionInfo.h"
#include "DatabaseInfo.h"

struct CreateDatabaseConfig {
    ConnectionInfo connection_info;
    DatabaseInfo database_info;
};

#endif // CREATE_DATABASE_CONFIG_H