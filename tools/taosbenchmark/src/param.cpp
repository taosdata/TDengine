#include <variant>
#include <unordered_map>
#include <nlohmann/json.hpp>
#include <vector>
#include <set>
#include <functional>


class ParameterValidator {
  public:
      // 未识别参数检测
      static void check_unknown_params(
          const std::set<std::string>& input_params,
          const std::map<std::string, ParamDescriptor>& descriptors);
      
      // 条件依赖校验
      static void check_conditional_params(
          const ParameterContext& context,
          const std::string& trigger_param,
          const std::set<std::string>& required_params);
      
      // 类型兼容性检查
      static bool check_type_compatibility(
          const std::string& param_name,
          const ParamValue& actual_value,
          const ParamDescriptor& descriptor);
};

// void validate_filetype_dependent(const ParameterContext& ctx) {
//   if (ctx.get<std::string>("filetype") == "csvfile") {
//       if (!ctx.has("super_tables.csv_ts_format")) {
//           throw ConfigError("csv_ts_format required when filetype=csvfile");
//       }
//   }
// }



class SuggestionEngine {
  static constexpr size_t MAX_EDIT_DISTANCE = 2;
  
public:
  static std::vector<std::string> suggest_corrections(
      const std::string& input,
      const std::map<std::string, ParamDescriptor>& descriptors)
  {
      std::vector<std::string> candidates;
      for (const auto& [name, _] : descriptors) {
          if (edit_distance(input, name) <= MAX_EDIT_DISTANCE) {
              candidates.push_back(name);
          }
      }
      return candidates;
  }
};


class ConfigError : public std::runtime_error {
public:
    std::string param_path;
    std::vector<std::string> suggestions;
    
    ConfigError(const std::string& msg, const std::string& path = "")
        : runtime_error(msg), param_path(path) {}
};


using json = nlohmann::json;
namespace nlohmann {
  template <>
  struct adl_serializer<CustomStruct> {
      static void from_json(const json& j, CustomStruct& cs) {
          // 自定义解析逻辑
      }
  };
}




// 自定义结构体定义
struct ColumnMeta {
    std::string type;
    std::string name;
    int count;
    double max;
    double min;
};



struct TagMeta {
    std::string type;
    std::string name;
    
    // 字符串类型专用字段
    std::optional<int> length;
    std::optional<std::vector<std::string>> values;
    
    // 数值类型专用字段
    std::optional<double> max;
    std::optional<double> min;
    std::optional<int> count;
  
    // 类型校验方法
    void validate() const {
        const std::string lower_type = to_lower(type);
        
        if (is_numeric_type(lower_type)) {
            if (!max || !min) {
                throw ConfigError("Numeric tag requires max/min values");
            }
            if (length || values) {
                throw ConfigError("Numeric tag cannot have string attributes");
            }
        } else if (is_string_type(lower_type)) {
            if (!length && !values) {
                throw ConfigError("String tag requires length or values");
            }
            if (max || min || count) {
                throw ConfigError("String tag cannot have numeric attributes");
            }
        }
    }
  
  private:
    static bool is_numeric_type(const std::string& t) {
        return t.find("int") != std::string::npos || t == "float" || t == "double";
    }
  
    static bool is_string_type(const std::string& t) {
        return t == "binary" || t == "nchar" || t == "json";
    }
  };



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
        // ...其他字段
        std::vector<ColumnMeta> columns;
        std::vector<TagMeta> tags;
    };
    
    std::vector<SuperTableMeta> super_tables;
};


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
        // 实现分割逻辑
        return tokens;
    }
};


// 参数值类型定义
using ParamValue = std::variant<
    int, 
    double, 
    std::string, 
    bool, 
    std::vector<DatabaseMeta>,
    ColumnMeta,
    TagMeta
>;


// 新增来源类型枚举
enum class SourceType {
    DEFAULT,      // 默认值（最低优先级）
    JSON,         // JSON文件
    ENVIRONMENT,  // 环境变量
    COMMAND_LINE  // 命令行参数（最高优先级）
};

// 封装参数值和来源的结构体
struct ParamEntry {
    ParamValue value;
    SourceType source;
};



// 增强型作用域描述体系
enum class ConfigScopeType {
    GLOBAL,        // 全局参数
    DATABASE,      // 数据库层级
    SUPER_TABLE,   // 超级表层级 
    CHILD_TABLE,   // 子表层级
    COLUMN,        // 列定义层级
    TAG            // 标签定义层级
  };
  
  struct ConfigScope {
    ConfigScopeType type;
    int index;  // 层级中的位置索引
    
    bool operator==(const ConfigScope& other) const {
        return type == other.type && index == other.index;
    }
  };


// 参数描述符
struct ParamDescriptor {
    std::vector<std::string> cli_flags;  // 支持多个标识
    std::string env_var; // 新增环境变量字段
    std::string json_path;     // JSON中的路径（支持点分表示法）
    ParamValue default_value;
    std::set<std::string> allowed_scopes;   // 允许出现的配置域
    std::function<bool(const ParamValue&)> validator;   // 自定义校验逻辑
    std::set<std::string> dependencies;     // 依赖参数
    std::set<std::string> conflict_with;    // 冲突参数
    std::string description;   // 参数描述
    ConfigScopeType scope_type; // 作用域类型
};



class ParameterContext {
private:
    std::unordered_map<std::string, ParamEntry> current_params; // 修改存储结构
    std::unordered_map<std::string, ParamDescriptor> descriptors;
    std::vector<DatabaseMeta> databases;


    // 参数描述符注册示例
    void register_core_params() {
        // 全局参数
        register_param({
            .cli_flags = {"--host"},
            .json_path = "host",
            .default_value = std::string("localhost"),
            .allowed_scopes = {"global"},
            .description = "Server hostname"
        });
      
        register_param({
            .cli_flags = {"-T"},
            .json_path = "thread_count",
            .default_value = 8,
            .validator = [](const ParamValue& v) {
                return std::get<int>(v) > 0 && std::get<int>(v) <= 256;
            },
            .description = "Number of worker threads"
        });

        register_param({
            .cli_flags = {"-c", "--config-dir"},
            .json_path = "cfgdir",
            .allowed_scopes = {"global"},
            .default_value = std::string("/etc/taos"),
            .validator = validate_path
        });
    
        register_param({
            .cli_flags = {"--file"},
            .json_path = "filetype",
            .default_value = std::string("insert"),
            .allowed_scopes = {"global"},
            .description = "Set JSON configuration file"
        });

        // 数据库级参数
        register_param({
            .cli_flags = {"-d", "--database"},
            .json_path = "databases.*.dbinfo.name",
            .default_value = std::string("test"),
            .allowed_scopes = {"database"},
            .validator = [](const ParamValue& v) {
                return std::holds_alternative<std::string>(v) &&
                    !std::get<std::string>(v).empty();
            }
        });

        register_param({
            .cli_flags = {"-v"},
            .json_path = "databases.*.dbinfo.vgroups",
            .allowed_scopes = {"database"},
            .default_value = 4,
            .validator = [](int v) { return v >= 1 && v <= 64; },
            // .validator = [](const ParamValue& v) {
            //     int val = std::get<int>(v);
            //     return val >= 1 && val <= 64;
            // },
            .description = "Number of vgroups"
        });
    

        // 超级表级参数
        register_param({
            .json_path = "databases.*.super_tables.*.childtable_count",
            .default_value = 5,
            .allowed_scopes = {"super_table"},
            .dependencies = {"filetype=insert"},
            .validator = [](const ParamValue& v) {
                return std::get<int>(v) >= 0;
            }
        });

        // 列级参数
        register_param({
            .json_path = "databases.*.super_tables.*.columns.*.max",
            .default_value = 100.0,
            .allowed_scopes = {"column"},
            .validator = [](const ParamValue& v) {
                return std::get<double>(v) >= std::get<double>(v.min);
            }
        });

        auto validator = [](const ParamValue& v) {
            static const std::set<std::string> valid_types = {"int", "uint", "float", "double", "binary", "nchar"};
            std::string type = to_lower(std::get<std::string>(v));
            return valid_types.count(type) > 0;
        };

        register_param({
            .json_path = "databases.*.super_tables.*.columns.*.type",
            .default_value = std::string("float"),
            .allowed_scopes = {"column"},
            .validator = validator
        });

        // 标签级参数
        register_param({
            .json_path = "databases.*.super_tables.*.tags.*.type",
            .default_value = std::string("int"),
            .allowed_scopes = {"tag"},
            .validator = validator
        });
    }
  

public:
    ParameterContext() {
        register_core_params();
        load_defaults();
    }

    void register_param(const ParamDescriptor& desc) {
        descriptors[desc.json_path] = desc;
        if (!desc.cli_flag.empty()) {
            cli_mapping[desc.cli_flag] = desc.json_path;
        }
    }

    // 修改默认值加载方法
    void load_defaults() {
        for (const auto& [path, desc] : descriptors) {
            set_param(path, desc.default_value, SourceType::DEFAULT);
        }
    }

    void merge_json(const json& config) {
        // 处理嵌套结构的JSON配置
        parse_nested_json(config);
    }

    // 命令行参数映射策略
    void merge_commandline(int argc, char* argv[]) {
        auto cli_params = parse_cli_args(argc, argv);
        for (const auto& [flag, value] : cli_params) {
            if (cli_mapping.count(flag)) {
                const auto& path_template = cli_mapping[flag];
                
                // 动态确定目标路径
                std::string target_path = path_template;
                if (path_template.find("*") != std::string::npos) {
                    // 自动定位到第一个数据库实例
                    target_path = replace_wildcard(path_template, 0); 
                }
                
                set_param(target_path, parse_cli_value(value, descriptors[path_template]), SourceType::COMMAND_LINE);
            }
        }
    }


    // 新增方法：合并环境变量参数
    void merge_environment_vars() {
        for (const auto& [json_path, desc] : descriptors) {
            if (!desc.env_var.empty()) {
                const char* env_value = std::getenv(desc.env_var.c_str());
                if (env_value != nullptr) {
                    try {
                        ParamValue parsed_value = parse_env_value(env_value, desc);
                        if (desc.validator && !desc.validator(parsed_value)) {
                            // 验证失败，跳过设置
                            continue;
                        }
                        std::string target_path = desc.json_path;
                        if (target_path.find('*') != std::string::npos) {
                            target_path = replace_wildcard(target_path, 0); // 替换通配符为第一个实例
                        }
                        // current_params[target_path] = parsed_value;
                        set_param(target_path, parsed_value, SourceType::ENVIRONMENT);
    
                    } catch (const std::exception& e) {
                        // 处理解析错误，例如记录日志
                        std::cerr << "Error parsing environment variable " << desc.env_var << ": " << e.what() << std::endl;
                    }
                }
            }
        }
    }


    // 新增方法：获取参数来源（调试用）
    SourceType get_param_source(const std::string& path) const {
        if (auto it = current_params.find(path); it != current_params.end()) {
            return it->second.source;
        }
        return SourceType::DEFAULT;
    }


    template <typename T>
    T get(const std::string& path) const {
        return std::get<T>(current_params.at(path));
    }

private:
    std::unordered_map<std::string, std::string> cli_mapping;

    // 辅助函数：替换通配符为指定索引
    std::string replace_wildcard(const std::string& path_template, int index) {
        std::string result = path_template;
        size_t pos = result.find('*');
        while (pos != std::string::npos) {
            result.replace(pos, 1, std::to_string(index));
            pos = result.find('*', pos + 1);
        }
        return result;
    }


    // 辅助函数：解析环境变量值为ParamValue
    ParamValue parse_env_value(const std::string& value_str, const ParamDescriptor& desc) {
        if (std::holds_alternative<int>(desc.default_value)) {
            try {
                return std::stoi(value_str);
            } catch (const std::exception&) {
                throw std::runtime_error("Invalid integer value for environment variable: " + desc.env_var);
            }
        } else if (std::holds_alternative<double>(desc.default_value)) {
            try {
                return std::stod(value_str);
            } catch (const std::exception&) {
                throw std::runtime_error("Invalid double value for environment variable: " + desc.env_var);
            }
        } else if (std::holds_alternative<std::string>(desc.default_value)) {
            return value_str;
        } else {
            throw std::runtime_error("Unsupported parameter type for environment variable: " + desc.env_var);
        }
    }


    // 辅助函数：比较来源优先级
    bool has_higher_priority(SourceType new_source, SourceType existing_source) {
        return static_cast<int>(new_source) > static_cast<int>(existing_source);
    }



    void parse_nested_json(const json& j, const std::string& prefix = "") {
        for (auto& el : j.items()) {
            std::string current_path = prefix.empty() ? el.key() : prefix + "." + el.key();

            if (el.value().is_object()) {
                parse_nested_json(el.value(), current_path);
            } else if (el.value().is_array()) {
                handle_json_array(el.value(), current_path);
            } else {
                set_param(current_path, parse_json_value(el.value(), current_path), SourceType::JSON);
            }
        }
    }

    void handle_json_array(const json& arr, const std::string& path) {
        size_t index = 0;
        for (const auto& item : arr) {
            std::string elem_path = path + "." + std::to_string(index);
            if (item.is_object()) {
                parse_nested_json(item, elem_path);
            } else {
                set_param(elem_path, parse_json_value(item, elem_path), SourceType::JSON);
            }
            ++index;
        }
    }

    void set_param(const std::string& path, const ParamValue& value, SourceType source) {

        if (descriptors.count(path)) {
            current_params[path] = value;
        } else {
            handle_unknown_param(path);
        }
    }

    // 修改后的参数设置方法
    void set_param(const std::string& path, const ParamValue& value, SourceType source) {
        auto it = current_params.find(path);
        if (it == current_params.end()) {
            // 不存在则直接插入
            current_params[path] = {value, source};
        } else {
            // 存在则比较优先级
            if (has_higher_priority(source, it->second.source)) {
                it->second = {value, source};
            }
        }
    }

    // 在set_param中改进查找逻辑
    void set_param(const std::string& path, const ParamValue& value) {
        bool found = false;
        for (const auto& [pattern, desc] : descriptors) {
            if (WildcardMatcher::match(pattern, path)) {
                // 执行类型转换和验证
                current_params[path] = value;
                found = true;
                break;
            }
        }
        
        if (!found) {
            handle_unknown_param(path);
        }
    }


    ParamValue parse_json_value(const json& j, const std::string& path) {
        const auto& desc = descriptors.at(path);
        return std::visit([&](auto&& default_val) -> ParamValue {
            using T = std::decay_t<decltype(default_val)>;
            
            try {
                if constexpr (std::is_same_v<T, std::vector<DatabaseMeta>>) {
                    return parse_databases(j);
                }
                else if constexpr (std::is_same_v<T, ColumnMeta>) {
                    return parse_column_meta(j);
                }
                else if constexpr (std::is_same_v<T, TagMeta>) {
                    return parse_tag_meta(j);
                }
                else {
                    return j.get<T>();
                }
            } catch (const json::exception& e) {
                throw ConfigError("Type mismatch for parameter: " + path);
            }
        }, desc.default_value);
    }

    std::vector<DatabaseMeta> parse_databases(const json& j) {
        std::vector<DatabaseMeta> dbs;
        for (const auto& db_item : j) {
            DatabaseMeta db;
            // 解析dbinfo
            db.dbinfo = {
                .name = db_item["dbinfo"]["name"],
                .drop = parse_bool(db_item["dbinfo"]["drop"]),
                .vgroups = db_item["dbinfo"]["vgroups"],
                .precision = db_item["dbinfo"]["precision"]
            };
            
            // 解析super tables
            for (const auto& stb_item : db_item["super_tables"]) {
                DatabaseMeta::SuperTableMeta stb;
                stb.name = stb_item["name"];
                stb.columns = parse_columns(stb_item["columns"]);
                stb.tags = parse_tags(stb_item["tags"]);
                // ...解析其他字段
                db.super_tables.push_back(stb);
            }
            dbs.push_back(db);
        }
        return dbs;
    }

    std::vector<ColumnMeta> parse_columns(const json& j) {
        std::vector<ColumnMeta> cols;
        for (const auto& col : j) {
            cols.push_back({
                .type = col["type"],
                .name = col["name"],
                .count = col.value("count", 1),
                .max = col["max"],
                .min = col["min"]
            });
        }
        return cols;
    }

    std::vector<TagMeta> parse_tags(const json& j) {
        std::vector<TagMeta> tags;
        for (const auto& tag : j) {
            TagMeta t;
            t.type = tag.value("type", "varchar");
            t.name = tag.value("name", "");
            
            // 安全获取字段
            if (tag.contains("length")) t.length = tag["length"];
            if (tag.contains("values")) t.values = tag["values"].get<std::vector<std::string>>();
            if (tag.contains("max")) t.max = tag["max"];
            if (tag.contains("min")) t.min = tag["min"];
            if (tag.contains("count")) t.count = tag["count"];
            
            try {
                t.validate();
            } catch (const ConfigError& e) {
                throw ConfigError(e.what() + " in tag: " + t.name);
            }
            
            tags.push_back(t);
        }
        return tags;
    }


    bool parse_bool(const json& j) {
        if (j.is_boolean()) return j.get<bool>();
        const std::string s = j.get<std::string>();
        return (s == "yes" || s == "true");
    }


    void handle_unknown_param(const std::string& path) {
        auto suggestions = SuggestionEngine::suggest(path, descriptors);
        throw ConfigError("Unknown parameter", path).with_suggestions(suggestions);
    }


    void validate() {
        // 层级维度校验
        validate_scope_constraints();
        
        // 类型维度校验
        validate_type_compatibility();
        
        // 依赖维度校验
        validate_dependencies();
        
        // 冲突维度校验
        validate_conflicts();
        
        // 自定义业务规则
        validate_business_rules();
      }


    // 其他辅助方法...
};



//////////////////////////////////////////////////////////////////////////////////////


int main(int argc, char* argv[]) {

    ParameterContext ctx;
    // 自动注册所有预定义参数
    

    try {
        // 从配置文件加载JSON
        json config = load_json_file("config.json");

        // 解析JSON并合并到上下文中
        ctx.merge_json(config);

        // 解析命令行参数并合并到上下文中
        ctx.merge_commandline(argc, argv);
    } catch (const ConfigError& e) {
        std::cerr << "Error in parameter '" << e.param() << "': " << e.what();
        if (!e.suggestions().empty()) {
            std::cerr << "\nDid you mean: ";
            for (const auto& s : e.suggestions()) {
                std::cerr << s << " ";
            }
        }
    }

    try {
        ctx.validate();
    } catch (const ConfigError& e) {
        std::cerr << "Configuration error: " << e.what();
        auto suggestions = SuggestionEngine::suggest(e.param_name());
        if (!suggestions.empty()) {
            std::cerr << " Did you mean: " << join(suggestions, ", ");
        }
        return EXIT_FAILURE;
    }
    
    // 进入运行阶段...
}


// 参数上下文增强方法
class ParameterContext {
public:
    template<typename T>
    T parse_object(const std::string& json_path) const {
        const json& j = get_json_value(json_path);
        return parse_from_json<T>(j);
    }

    std::vector<DatabaseMeta> get_databases() const {
        return parse_object<std::vector<DatabaseMeta>>("databases");
    }
};

// 数据库元数据解析
DatabaseMeta parse_from_json(const json& j) {
    DatabaseMeta db;
    db.dbinfo.name = j["dbinfo"]["name"];
    db.dbinfo.vgroups = j["dbinfo"]["vgroups"];
    
    for (const auto& stb_j : j["super_tables"]) {
        SuperTableMeta stb;
        stb.name = stb_j["name"];
        stb.childtable_count = stb_j["childtable_count"];
        // 解析列和标签...
        db.super_tables.push_back(stb);
    }
    return db;
}


ParameterContext params;
params.merge_json_file("config.json");
params.merge_commandline(argc, argv);
