
class ConfigError : public std::runtime_error {
public:
    std::string param_path;
    std::vector<std::string> suggestions;
    
    ConfigError(const std::string& msg, const std::string& path = "")
        : runtime_error(msg), param_path(path) {}
};
