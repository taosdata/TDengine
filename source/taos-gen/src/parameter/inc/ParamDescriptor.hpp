#pragma once

#include <string>
#include <vector>
#include <set>
#include <functional>
#include <variant>

// Parameter value type definition
using ParamValue = std::variant<
    int, 
    double, 
    std::string, 
    bool, 
    std::vector<DatabaseMeta>,
    ColumnMeta,
    TagMeta
>;

// Structure encapsulating parameter value and source
struct ParamEntry {
    ParamValue value;
    SourceType source;
};

// Config scope type
enum class ConfigScopeType {
    GLOBAL,        // Global parameter
    DATABASE,      // Database level
    SUPER_TABLE,   // Super table level
    CHILD_TABLE,   // Child table level
    COLUMN,        // Column level
    TAG            // Tag level
};

// Config scope structure
struct ConfigScope {
    ConfigScopeType type;
    int index;  // Index in the hierarchy

    bool operator==(const ConfigScope& other) const {
        return type == other.type && index == other.index;
    }
};

using json = nlohmann::json;
namespace nlohmann {
  template <>
  struct adl_serializer<CustomStruct> {
      static void from_json(const json& j, CustomStruct& cs) {
          // Custom parsing logic
      }
  };
}

// Parameter descriptor
struct ParamDescriptor {
    std::vector<std::string> cli_flags;  // Multiple supported flags
    std::string env_var; // Environment variable field
    std::string json_path;     // Path in JSON (dot notation supported)
    ParamValue default_value;
    std::set<std::string> allowed_scopes;   // Allowed config scopes
    std::function<bool(const ParamValue&)> validator;   // Custom validation logic
    std::set<std::string> dependencies;     // Dependent parameters
    std::set<std::string> conflict_with;    // Conflicting parameters
    std::string description;   // Parameter description
    ConfigScopeType scope_type; // Scope type
};
