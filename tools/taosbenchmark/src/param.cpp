#include <variant>
#include <unordered_map>
#include <nlohmann/json.hpp>
#include <vector>
#include <set>
#include <functional>










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
