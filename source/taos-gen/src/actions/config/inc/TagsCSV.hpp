#pragma once

#include "StringUtils.hpp"
#include "ColumnConfig.hpp"
#include <string>
#include <sstream>
#include <set>

struct TagsCSV {
    bool enabled = false;
    ColumnConfigVector schema;

    std::string file_path;
    bool has_header = true;
    std::string delimiter = ",";
    int tbname_index = -1;
    std::string exclude_indices_str;
    std::set<size_t> exclude_indices;

    void parse_exclude_indices() {
        exclude_indices.clear();

        if (!exclude_indices_str.empty()) {
            std::istringstream ss(exclude_indices_str);
            std::string token;

            while (std::getline(ss, token, ',')) {
                StringUtils::remove_all_spaces(token);

                if (!token.empty()) {
                    try {
                        size_t idx = std::stoull(token);
                        exclude_indices.insert(idx);
                    } catch (const std::exception&) {
                        throw std::runtime_error("Invalid element in exclude_indices: " + token + ".");
                    }
                } else {
                    throw std::runtime_error("There is an empty element in exclude_indices: " + exclude_indices_str + ".");
                }
            }
        }

        if (tbname_index >= 0) {
            exclude_indices.insert(static_cast<size_t>(tbname_index));
        }
    }
};
