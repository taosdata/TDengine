#pragma once
#include <string>

struct SchemalessFormatOptions {
    std::string protocol = "line";  // line protocol (InfluxDB)
    std::string tbname_key = "";    // tag key for child table name in line protocol
};
