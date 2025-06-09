#ifndef CREATE_DATABASE_CONFIG_H
#define CREATE_DATABASE_CONFIG_H

#include "ConnectionInfo.h"
#include "DataFormat.h"
#include "DataChannel.h"
#include "DatabaseInfo.h"

struct CreateDatabaseConfig {
    ConnectionInfo connection_info;
    DataFormat data_format;
    DataChannel data_channel;
    DatabaseInfo database_info;
};

#endif // CREATE_DATABASE_CONFIG_H