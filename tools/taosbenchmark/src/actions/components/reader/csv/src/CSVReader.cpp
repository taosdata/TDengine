#include "CSVReader.h"
#include <sstream>
#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstring>


CSVReader::CSVReader(const std::string& file_path, bool has_header, char delimiter)
    : file_path_(file_path), has_header_(has_header), delimiter_(delimiter) {
    
    // Open the file
    file_stream_.open(file_path);
    if (!file_stream_.is_open()) {
        throw std::runtime_error("Failed to open CSV file: " + file_path + " - " + std::strerror(errno));
    }
    
    // Skip the header row if necessary
    if (has_header_) {
        skip_header();
    }
    
    // Determine the number of columns
    if (auto first_row = read_next()) {
        column_count_ = first_row->size();
        reset();
    }
}

std::vector<CSVRow> CSVReader::read_all() {
    reset();
    std::vector<CSVRow> rows;
    while (auto row = read_next()) {
        rows.push_back(std::move(*row));
    }
    return rows;
}

std::optional<CSVRow> CSVReader::read_next() {
    std::string line;
    if (!std::getline(file_stream_, line)) {
        return std::nullopt;
    }
    
    // Skip empty lines
    while (line.empty() && std::getline(file_stream_, line)) {}
    
    if (line.empty()) {
        return std::nullopt;
    }
    
    return parse_line(line);
}

size_t CSVReader::column_count() const {
    return column_count_;
}

void CSVReader::reset() {
    // Reset the file stream to the beginning
    file_stream_.clear();
    file_stream_.seekg(0);
    if (has_header_) {
        skip_header();
    }
}

void CSVReader::skip_header() {
    // Skip the header row if the file pointer is at the beginning
    if (file_stream_.tellg() == 0) {
        std::string header;
        std::getline(file_stream_, header);
    }
}

CSVRow CSVReader::parse_line(const std::string& line) {
    CSVRow fields;
    std::string current_field;
    ParseState state = ParseState::START_FIELD;
    bool escape_next = false;
    
    for (char c : line) {
        switch (state) {
            case ParseState::START_FIELD:
                if (c == '"') {
                    state = ParseState::IN_QUOTED_FIELD;
                } else if (c == delimiter_) {
                    fields.push_back("");
                    current_field.clear();
                } else {
                    current_field += c;
                    state = ParseState::IN_UNQUOTED_FIELD;
                }
                break;
                
            case ParseState::IN_UNQUOTED_FIELD:
                if (c == delimiter_) {
                    fields.push_back(std::move(current_field));
                    current_field.clear();
                    state = ParseState::START_FIELD;
                } else {
                    current_field += c;
                }
                break;
                
            case ParseState::IN_QUOTED_FIELD:
                if (escape_next) {
                    current_field += c;
                    escape_next = false;
                } else if (c == '"') {
                    state = ParseState::QUOTE_IN_QUOTED_FIELD;
                } else if (c == '\\') {
                    escape_next = true;
                } else {
                    current_field += c;
                }
                break;
                
            case ParseState::QUOTE_IN_QUOTED_FIELD:
                if (c == '"') {
                    // Two consecutive quotes represent an escaped quote
                    current_field += '"';
                    state = ParseState::IN_QUOTED_FIELD;
                } else if (c == delimiter_) {
                    fields.push_back(std::move(current_field));
                    current_field.clear();
                    state = ParseState::START_FIELD;
                } else {
                    // A non-quote/non-delimiter character follows a quote - end the quoted field
                    fields.push_back(std::move(current_field));
                    current_field = std::string(1, c);
                    state = ParseState::IN_UNQUOTED_FIELD;
                }
                break;
                
            case ParseState::END_OF_ROW:
                break;
        }
    }
    
    // Handle the state at the end of the line
    switch (state) {
        case ParseState::START_FIELD:
            fields.push_back("");
            break;
        case ParseState::IN_UNQUOTED_FIELD:
            fields.push_back(std::move(current_field));
            break;
        case ParseState::IN_QUOTED_FIELD:
            // Unclosed quote - treat as normal end
            fields.push_back(std::move(current_field));
            break;
        case ParseState::QUOTE_IN_QUOTED_FIELD:
            fields.push_back(std::move(current_field));
            break;
        case ParseState::END_OF_ROW:
            break;
    }
    
    return fields;
}