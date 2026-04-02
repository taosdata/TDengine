#include <iostream>
#include <fstream>
#include <yaml-cpp/yaml.h>


int main() {
    // Open YAML file
    std::cout << "Opening config.yaml: " << CONFIG_PATH << std::endl;
    std::ifstream file(CONFIG_PATH);
    if (!file.is_open()) {
        std::cerr << "Failed to open config.yaml" << std::endl;
        return 1;
    }

    // Parse YAML file
    YAML::Node config;
    try {
        config = YAML::Load(file);
    } catch (const YAML::ParserException& e) {
        std::cerr << "YAML parse error: " << e.what() << std::endl;
        return 1;
    }

    // Output parsed result
    std::cout << "Parsed YAML content:" << std::endl;
    std::cout << config << std::endl;

    // Example: access YAML data
    if (config["global"]["host"]) {
        std::cout << "Host: " << config["host"].as<std::string>() << std::endl;
    }

    std::cout << "test_parse_yaml passed\n";

    return 0;
}