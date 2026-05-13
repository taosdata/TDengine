class WildcardMatcher {
public:
    static bool match(const std::string& pattern, const std::string& path) {
        std::vector<std::string> pattern_parts = split(pattern, '.');
        std::vector<std::string> path_parts = split(path, '.');
        
        if (pattern_parts.size() != path_parts.size()) return false;
        
        for (size_t i = 0; i < pattern_parts.size(); ++i) {
            if (pattern_parts[i] == "*") continue;
            if (pattern_parts[i] != path_parts[i]) return false;
        }
        return true;
    }

private:
    static std::vector<std::string> split(const std::string& s, char delim) {
        std::vector<std::string> tokens;
        // Implement split logic
        return tokens;
    }
};