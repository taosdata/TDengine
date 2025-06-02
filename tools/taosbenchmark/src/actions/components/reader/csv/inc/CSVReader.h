#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <optional>
#include <stdexcept>
#include <cctype>
#include <utility>

using CSVRow = std::vector<std::string>;

class CSVReader {
public:
    // Parsing state
    enum class ParseState {
        START_FIELD,
        IN_UNQUOTED_FIELD,
        IN_QUOTED_FIELD,
        QUOTE_IN_QUOTED_FIELD,
        END_OF_ROW
    };

    CSVReader(const std::string& file_path, bool has_header = true, char delimiter = ',');

    CSVReader(const CSVReader&) = delete;
    CSVReader& operator=(const CSVReader&) = delete;
    CSVReader(CSVReader&&) = delete;
    CSVReader& operator=(CSVReader&&) = delete;
    
    ~CSVReader() = default;

    // Read the entire file
    std::vector<CSVRow> read_all();
    
    // Read line by line
    std::optional<CSVRow> read_next();
    
    // Get the number of columns
    size_t column_count() const;
    
    // Reset the reading position to the beginning of the file
    void reset();

private:    
    // Parse a single line
    CSVRow parse_line(const std::string& line);

    // Skip the file header
    void skip_header();
    
    // File path
    std::string file_path_;
    
    // Whether the file has a header
    bool has_header_;
    
    // Delimiter
    char delimiter_;
    
    // File stream
    std::ifstream file_stream_;
    
    // Number of columns
    size_t column_count_ = 0;
};