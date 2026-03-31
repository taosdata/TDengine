#pragma once

#include <string>
#include <vector>
#include <optional>
#include <stdexcept>
#include <memory>

using CSVRow = std::vector<std::string>;

class CSVReader {
public:
    CSVReader(const std::string& file_path, bool has_header = true, char delimiter = ',');
    CSVReader(const std::vector<std::string>& file_paths, bool has_header = true, char delimiter = ',');

    CSVReader(const CSVReader&) = delete;
    CSVReader& operator=(const CSVReader&) = delete;
    CSVReader(CSVReader&&) noexcept;
    CSVReader& operator=(CSVReader&&) noexcept;

    ~CSVReader();

    // Read the entire file (all files if multi-file)
    std::vector<CSVRow> read_all();

    // Read line by line (auto-advances across files)
    std::optional<CSVRow> read_next();

    // Get the number of columns (based on first file)
    size_t column_count() const;

    // Reset the reading position to the beginning
    void reset();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};