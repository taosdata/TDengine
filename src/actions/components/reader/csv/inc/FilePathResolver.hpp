#pragma once

#include <string>
#include <vector>

class FilePathResolver {
public:
    // Resolve file_path to an ordered list of files:
    // - Plain file path → [file_path]
    // - Directory path → all .csv files sorted by name
    // - Glob pattern (*, ?) → matched files sorted by name
    static std::vector<std::string> resolve(const std::string& file_path);

private:
    static bool is_directory(const std::string& path);
    static bool has_glob_pattern(const std::string& path);
    static std::vector<std::string> resolve_directory(const std::string& dir_path);
    static std::vector<std::string> resolve_glob(const std::string& pattern);
    static bool glob_match(const std::string& pattern, const std::string& str);
};
