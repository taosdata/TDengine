#pragma once

#include <string>

struct TableNameConfig {
    bool enabled = false;
    std::string source_type = "generator";

    struct Generator {
        std::string prefix = "d";
        int count = 10000;
        int from = 0; // Default start index is 0
    } generator;

    struct CSV {
        std::string file_path;
        bool has_header = true;
        std::string delimiter = ",";
        int tbname_index = 0;
    } csv;
};