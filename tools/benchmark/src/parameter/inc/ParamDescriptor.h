#ifndef PARAM_DESCRIPTOR_H
#define PARAM_DESCRIPTOR_H

#include <string>
#include <vector>
#include <set>
#include <functional>
#include <variant>


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



// 封装参数值和来源的结构体
struct ParamEntry {
    ParamValue value;
    SourceType source;
};


// 配置作用域类型
enum class ConfigScopeType {
    GLOBAL,        // 全局参数
    DATABASE,      // 数据库层级
    SUPER_TABLE,   // 超级表层级 
    CHILD_TABLE,   // 子表层级
    COLUMN,        // 列定义层级
    TAG            // 标签定义层级
};

// 配置作用域结构体
struct ConfigScope {
    ConfigScopeType type;
    int index;  // 层级中的位置索引

    bool operator==(const ConfigScope& other) const {
        return type == other.type && index == other.index;
    }
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

#endif // PARAM_DESCRIPTOR_H



