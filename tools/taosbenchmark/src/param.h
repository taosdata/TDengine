#ifndef PARAM_H
#define PARAM_H

#include <string>
#include <vector>
#include <variant>
#include <optional>
#include <unordered_map>
#include <set>
#include <functional>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

// Forward declarations
struct ColumnMeta;
struct TagMeta;
struct DatabaseMeta;
struct TaskMeta;
class ParameterContext;

// Parameter value type definition
using ParamValue = std::variant<int, double, std::string, bool, std::vector<DatabaseMeta>, ColumnMeta, TagMeta>;

// Column metadata structure
struct ColumnMeta {
    std::string type;
    std::string name;
    int count;
    double max;
    double min;
};

// Tag metadata structure
struct TagMeta {
    std::string type;
    std::string name;
    std::optional<int> length;
    std::optional<std::vector<std::string>> values;
    std::optional<double> max;
    std::optional<double> min;
    std::optional<int> count;

    void validate() const;
};

// Database metadata structure
struct DatabaseMeta {
    struct {
        std::string name;
        bool drop;
        int vgroups;
        std::string precision;
    } dbinfo;

    struct SuperTableMeta {
        std::string name;
        bool child_table_exists;
        int childtable_count;
        std::string childtable_prefix;
        bool auto_create_table;
        int batch_create_tbl_num;
        std::string data_source;
        std::string insert_mode;
        bool non_stop_mode;
        std::string line_protocol;
        int insert_rows;
        std::vector<ColumnMeta> columns;
        std::vector<TagMeta> tags;
    };

    std::vector<SuperTableMeta> super_tables;
};

// Parameter context class
class ParameterContext {
public:
    ParameterContext();
    void merge_json(const json& config);
    void merge_commandline(int argc, char* argv[]);
    void validate();
    template <typename T>
    T get(const std::string& path) const;

private:
    void register_param(const ParamDescriptor& desc);
    void load_defaults();
    void set_param(const std::string& path, const ParamValue& value);
    void handle_unknown_param(const std::string& path);
    ParamValue parse_json_value(const json& j, const std::string& path);
    std::vector<ColumnMeta> parse_columns(const json& j);
    std::vector<TagMeta> parse_tags(const json& j);
    bool parse_bool(const json& j);
};

// Function declarations
std::vector<ColumnMeta> parse_columns(const json& j);
std::vector<TagMeta> parse_tags(const json& j);
bool parse_bool(const json& j);

#endif // PARAM_H