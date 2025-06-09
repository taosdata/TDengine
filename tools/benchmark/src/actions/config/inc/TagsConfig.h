#pragma once

#include <vector>
#include <string>
#include <sstream>
#include <algorithm>
#include "ColumnConfig.h"

struct TagsConfig {
    std::string source_type; // 数据来源类型：generator 或 csv

    struct Generator {
        ColumnConfigVector schema;
    } generator;

    struct CSV {
        ColumnConfigVector schema;
        std::string file_path;
        bool has_header = true;
        std::string delimiter = ",";
        std::string exclude_indices_str = "";
        std::vector<size_t> exclude_indices;

        void parse_exclude_indices() {
            if (exclude_indices_str.empty()) {
                exclude_indices.clear();
                return;
            }

            std::istringstream ss(exclude_indices_str);
            std::string token;

            while (std::getline(ss, token, ',')) {
                token.erase(token.begin(), std::find_if(token.begin(), token.end(), [](unsigned char ch) {
                    return !std::isspace(ch);
                }));
                token.erase(std::find_if(token.rbegin(), token.rend(), [](unsigned char ch) {
                    return !std::isspace(ch);
                }).base(), token.end());

                if (!token.empty()) {
                    try {
                        size_t idx = std::stoull(token);
                        exclude_indices.push_back(idx);
                    } catch (const std::exception& e) {
                        throw std::runtime_error("Invalid element in exclude_indices: " + token + ".");
                    }
                } else {
                    throw std::runtime_error("There is an empty element in exclude_indices: " + exclude_indices_str + ".");
                }
            }
        }

    } csv;
};
