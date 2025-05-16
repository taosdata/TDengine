#include "ParameterContext.h"
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <sstream>


ParameterContext::ParameterContext() {}

// 解析全局配
void ParameterContext::parse_columns_or_tags(const YAML::Node& node, std::vector<SuperTableInfo::Column>& target) {
    for (const auto& item : node) {
        SuperTableInfo::Column column;
        column.name = item["name"].as<std::string>();
        column.type = item["type"].as<std::string>();
        if (item["len"]) column.len = item["len"].as<int>();
        if (item["count"]) column.count = item["count"].as<int>();
        if (item["precision"]) column.precision = item["precision"].as<int>();
        if (item["scale"]) column.scale = item["scale"].as<int>();
        if (item["properties"]) column.properties = item["properties"].as<std::string>();
        if (item["null_ratio"]) column.null_ratio = item["null_ratio"].as<float>();
        if (item["gen_type"]) {
            column.gen_type = item["gen_type"].as<std::string>();
            if (*column.gen_type == "random") {
                if (item["min"]) column.min = item["min"].as<double>();
                if (item["max"]) column.max = item["max"].as<double>();
                if (item["dec_min"]) column.dec_min = item["dec_min"].as<std::string>();
                if (item["dec_max"]) column.dec_max = item["dec_max"].as<std::string>();
                if (item["corpus"]) column.corpus = item["corpus"].as<std::string>();
                if (item["chinese"]) column.chinese = item["chinese"].as<bool>();
                if (item["values"]) column.values = item["values"].as<std::vector<std::string>>();
            } else if (*column.gen_type == "order") {
                if (item["min"]) column.order_min = item["min"].as<double>();
                if (item["max"]) column.order_max = item["max"].as<double>();
            } else if (*column.gen_type == "function") {
                SuperTableInfo::Column::FunctionConfig func_config;
                func_config.expression = item["function"].as<std::string>(); // 解析完整表达式
                // 解析函数表达式的各部分
                // 假设函数表达式格式为：<multiple> * <function>(<args>) + <addend> * random(<random>) + <base>
                std::istringstream expr_stream(func_config.expression);
                std::string token;
                while (std::getline(expr_stream, token, '*')) {
                    if (token.find("sinusoid") != std::string::npos ||
                        token.find("counter") != std::string::npos ||
                        token.find("sawtooth") != std::string::npos ||
                        token.find("square") != std::string::npos ||
                        token.find("triangle") != std::string::npos) {
                        func_config.function = token.substr(0, token.find('('));
                        // 解析函数参数
                        auto args_start = token.find('(') + 1;
                        auto args_end = token.find(')');
                        auto args = token.substr(args_start, args_end - args_start);
                        std::istringstream args_stream(args);
                        std::string arg;
                        while (std::getline(args_stream, arg, ',')) {
                            if (arg.find("min") != std::string::npos) func_config.min = std::stod(arg.substr(arg.find('=') + 1));
                            if (arg.find("max") != std::string::npos) func_config.max = std::stod(arg.substr(arg.find('=') + 1));
                            if (arg.find("period") != std::string::npos) func_config.period = std::stoi(arg.substr(arg.find('=') + 1));
                            if (arg.find("offset") != std::string::npos) func_config.offset = std::stoi(arg.substr(arg.find('=') + 1));
                        }
                    } else if (token.find("random") != std::string::npos) {
                        func_config.random = std::stoi(token.substr(token.find('(') + 1, token.find(')') - token.find('(') - 1));
                    } else if (token.find('+') != std::string::npos) {
                        func_config.addend = std::stod(token.substr(0, token.find('+')));
                        func_config.base = std::stod(token.substr(token.find('+') + 1));
                    } else {
                        func_config.multiple = std::stod(token);
                    }
                }
                column.function_config = func_config;
            }
        }
        target.push_back(column);
    }
}


void ParameterContext::parse_global(const YAML::Node& global_yaml) {
    auto& global_config = config_data.global;
    if (global_yaml["confirm_prompt"]) {
        global_config.confirm_prompt = global_yaml["confirm_prompt"].as<bool>();
    }
    if (global_yaml["log_dir"]) {
        global_config.log_dir = global_yaml["log_dir"].as<std::string>();
    }
    if (global_yaml["cfg_dir"]) {
        global_config.cfg_dir = global_yaml["cfg_dir"].as<std::string>();
    }
    if (global_yaml["connection_info"]) {
        const auto& conn = global_yaml["connection_info"];
        auto& conn_info = global_config.connection_info;
        if (conn["host"]) conn_info.host = conn["host"].as<std::string>();
        if (conn["port"]) conn_info.port = conn["port"].as<int>();
        if (conn["user"]) conn_info.user = conn["user"].as<std::string>();
        if (conn["password"]) conn_info.password = conn["password"].as<std::string>();
        if (conn["dsn"]) conn_info.dsn = conn["dsn"].as<std::string>();
    }
    if (global_yaml["database_info"]) {
        const auto& db = global_yaml["database_info"];
        auto& db_info = global_config.database_info;
        if (db["name"]) db_info.name = db["name"].as<std::string>();
        if (db["drop_if_exists"]) db_info.drop_if_exists = db["drop_if_exists"].as<bool>();
        if (db["properties"]) db_info.properties = db["properties"].as<std::string>();
    }
    if (global_yaml["super_table_info"]) {
        const auto& stb = global_yaml["super_table_info"];
        auto& stb_info = global_config.super_table_info;
        if (stb["name"]) stb_info.name = stb["name"].as<std::string>();
        if (stb["columns"]) {
            parse_columns_or_tags(stb["columns"], stb_info.columns);
        }
        if (stb["tags"]) {
            parse_columns_or_tags(stb["tags"], stb_info.tags);
        }
    }
}


void ParameterContext::parse_jobs(const YAML::Node& jobs_yaml) {
    for (const auto& job_node : jobs_yaml) {
        Job job;
        job.key = job_node.first.as<std::string>(); // 获取作业标识符
        const auto& job_content = job_node.second;

        if (job_content["name"]) {
            job.name = job_content["name"].as<std::string>();
        }
        if (job_content["needs"]) {
            job.needs = job_content["needs"].as<std::vector<std::string>>();
        }
        if (job_content["steps"]) {
            parse_steps(job_content["steps"], job.steps);
        }
        config_data.jobs.push_back(job);
    }
}



void ParameterContext::parse_steps(const YAML::Node& steps_yaml, std::vector<Step>& steps) {
    for (const auto& step_node : steps_yaml) {
        Step step;
        step.name = step_node["name"].as<std::string>();
        step.uses = step_node["uses"].as<std::string>();
        if (step_node["with"]) {
            step.with = step_node["with"]; // 保留原始 YAML 节点

            // 根据 uses 字段解析具体行动
            if (step.uses == "actions/create-database") {
                parse_create_database_action(step);
            } else if (step.uses == "actions/create-super-table") {
                parse_create_super_table_action(step);
            } else if (step.uses == "actions/create-child-table") {
                parse_create_child_table_action(step);
            } else if (step.uses == "actions/insert-data") {
                parse_insert_data_action(step);
            }
            // 其他行动解析逻辑可以在此扩展
        }
        steps.push_back(step);
    }
}


void ParameterContext::parse_create_database_action(Step& step) {
    if (!step.with["database_info"]) {
        throw std::runtime_error("Missing required 'database_info' for create-database action.");
    }

    CreateDatabaseConfig create_db_config;

    // 解析 connection_info（可选）
    if (step.with["connection_info"]) {
        const auto& conn_info = step.with["connection_info"];
        if (conn_info["host"]) create_db_config.connection_info.host = conn_info["host"].as<std::string>();
        if (conn_info["port"]) create_db_config.connection_info.port = conn_info["port"].as<int>();
        if (conn_info["user"]) create_db_config.connection_info.user = conn_info["user"].as<std::string>();
        if (conn_info["password"]) create_db_config.connection_info.password = conn_info["password"].as<std::string>();
        if (conn_info["dsn"]) create_db_config.connection_info.dsn = conn_info["dsn"].as<std::string>();
    } else {
        // 如果未指定 connection_info，则使用全局配置
        create_db_config.connection_info = config_data.global.connection_info;
    }

    // 解析 database_info（必需）
    const auto& db_info = step.with["database_info"];
    if (db_info["name"]) {
        create_db_config.database_info.name = db_info["name"].as<std::string>();
    } else {
        throw std::runtime_error("Missing required 'name' in database_info.");
    }

    if (db_info["drop_if_exists"]) {
        create_db_config.database_info.drop_if_exists = db_info["drop_if_exists"].as<bool>();
    }

    if (db_info["precision"]) {
        if (!db_info["precision"].IsScalar()) {
            throw std::runtime_error("Invalid type for 'precision' in database_info. Expected a string.");
        }
        // 验证时间精度是否为合法值
        std::string precision = db_info["precision"].as<std::string>();
        if (precision != "ms" && precision != "us" && precision != "ns") {
            throw std::runtime_error("Invalid precision value: " + precision);
        }
        create_db_config.database_info.properties = "precision " + precision;
    }


    if (db_info["precision"]) {
        // 验证时间精度是否为合法值
        std::string precision = db_info["precision"].as<std::string>();
        if (precision != "ms" && precision != "us" && precision != "ns") {
            throw std::runtime_error("Invalid precision value: " + precision);
        }
        create_db_config.database_info.precision = precision;
    }
    if (db_info["properties"]) {
        if (create_db_config.database_info.properties) {
            create_db_config.database_info.properties.value() += " " + db_info["properties"].as<std::string>();
        } else {
            create_db_config.database_info.properties = db_info["properties"].as<std::string>();
        }
    }

    // 将解析结果保存到 Step 的 action_config 字段
    step.action_config = std::move(create_db_config);

    // 打印解析结果（可选）
    std::cout << "Parsed create-database action: " << create_db_config.database_info.name << std::endl;
}


void ParameterContext::parse_create_super_table_action(Step& step) {
    if (!step.with["database_info"]) {
        throw std::runtime_error("Missing required 'database_info' for create-super-table action.");
    }
    if (!step.with["super_table_info"]) {
        throw std::runtime_error("Missing required 'super_table_info' for create-super-table action.");
    }

    CreateSuperTableConfig create_stb_config;

    // 解析 connection_info（可选）
    if (step.with["connection_info"]) {
        const auto& conn_info = step.with["connection_info"];
        if (conn_info["host"]) create_stb_config.connection_info.host = conn_info["host"].as<std::string>();
        if (conn_info["port"]) create_stb_config.connection_info.port = conn_info["port"].as<int>();
        if (conn_info["user"]) create_stb_config.connection_info.user = conn_info["user"].as<std::string>();
        if (conn_info["password"]) create_stb_config.connection_info.password = conn_info["password"].as<std::string>();
        if (conn_info["dsn"]) create_stb_config.connection_info.dsn = conn_info["dsn"].as<std::string>();
    } else {
        // 如果未指定 connection_info，则使用全局配置
        create_stb_config.connection_info = config_data.global.connection_info;
    }

    // 解析 database_info（必需）
    const auto& db_info = step.with["database_info"];
    if (db_info["name"]) {
        create_stb_config.database_info.name = db_info["name"].as<std::string>();
    } else {
        throw std::runtime_error("Missing required 'name' in database_info.");
    }

    // 解析 super_table_info（必需）
    const auto& stb_info = step.with["super_table_info"];
    if (stb_info["name"]) {
        create_stb_config.super_table_info.name = stb_info["name"].as<std::string>();
    } else {
        throw std::runtime_error("Missing required 'name' in super_table_info.");
    }
    if (stb_info["columns"]) {
        parse_columns_or_tags(stb_info["columns"], create_stb_config.super_table_info.columns);
    } else {
        throw std::runtime_error("Missing required 'columns' in super_table_info.");
    }
    if (stb_info["tags"]) {
        parse_columns_or_tags(stb_info["tags"], create_stb_config.super_table_info.tags);
    } else {
        throw std::runtime_error("Missing required 'tags' in super_table_info.");
    }

    // 将解析结果保存到 Step 的 action_config 字段
    step.action_config = std::move(create_stb_config);

    // 打印解析结果（可选）
    std::cout << "Parsed create-super-table action: " << create_stb_config.super_table_info.name << std::endl;
}


void ParameterContext::parse_create_child_table_action(Step& step) {
    if (!step.with["database_info"]) {
        throw std::runtime_error("Missing required 'database_info' for create-child-table action.");
    }
    if (!step.with["super_table_info"]) {
        throw std::runtime_error("Missing required 'super_table_info' for create-child-table action.");
    }
    if (!step.with["child_table_info"]) {
        throw std::runtime_error("Missing required 'child_table_info' for create-child-table action.");
    }

    CreateChildTableConfig create_child_config;

    // 解析 connection_info（可选）
    if (step.with["connection_info"]) {
        const auto& conn_info = step.with["connection_info"];
        if (conn_info["host"]) create_child_config.connection_info.host = conn_info["host"].as<std::string>();
        if (conn_info["port"]) create_child_config.connection_info.port = conn_info["port"].as<int>();
        if (conn_info["user"]) create_child_config.connection_info.user = conn_info["user"].as<std::string>();
        if (conn_info["password"]) create_child_config.connection_info.password = conn_info["password"].as<std::string>();
        if (conn_info["dsn"]) create_child_config.connection_info.dsn = conn_info["dsn"].as<std::string>();
    } else {
        // 如果未指定 connection_info，则使用全局配置
        create_child_config.connection_info = config_data.global.connection_info;
    }

    // 解析 database_info（必需）
    const auto& db_info = step.with["database_info"];
    if (db_info["name"]) {
        create_child_config.database_info.name = db_info["name"].as<std::string>();
    } else {
        throw std::runtime_error("Missing required 'name' in database_info.");
    }

    // 解析 super_table_info（必需）
    const auto& stb_info = step.with["super_table_info"];
    if (stb_info["name"]) {
        create_child_config.super_table_info.name = stb_info["name"].as<std::string>();
    } else {
        throw std::runtime_error("Missing required 'name' in super_table_info.");
    }

    // 解析 child_table_info（必需）
    const auto& child_info = step.with["child_table_info"];
    const auto& table_name = child_info["table_name"];
    create_child_config.child_table_info.table_name.source_type = table_name["source_type"].as<std::string>();
    if (create_child_config.child_table_info.table_name.source_type == "generator") {
        const auto& generator = table_name["generator"];
        create_child_config.child_table_info.table_name.generator.prefix = generator["prefix"].as<std::string>();
        create_child_config.child_table_info.table_name.generator.count = generator["count"].as<int>();
        if (generator["from"]) {
            create_child_config.child_table_info.table_name.generator.from = generator["from"].as<int>();
        }
    } else if (create_child_config.child_table_info.table_name.source_type == "csv") {
        const auto& csv = table_name["csv"];
        create_child_config.child_table_info.table_name.csv.file_path = csv["file_path"].as<std::string>();
        if (csv["has_header"]) create_child_config.child_table_info.table_name.csv.has_header = csv["has_header"].as<bool>();
        if (csv["delimiter"]) create_child_config.child_table_info.table_name.csv.delimiter = csv["delimiter"].as<std::string>();
        if (csv["column_index"]) create_child_config.child_table_info.table_name.csv.column_index = csv["column_index"].as<int>();
    } else {
        throw std::runtime_error("Invalid source_type for table_name in child_table_info.");
    }

    // 解析 tags
    const auto& tags = child_info["tags"];
    create_child_config.child_table_info.tags.source_type = tags["source_type"].as<std::string>();
    if (create_child_config.child_table_info.tags.source_type == "generator") {
        if (tags["generator"]["schema"]) {
            parse_columns_or_tags(tags["generator"]["schema"], create_child_config.child_table_info.tags.generator.schema);
        }
    } else if (create_child_config.child_table_info.tags.source_type == "csv") {
        const auto& csv = tags["csv"];
        create_child_config.child_table_info.tags.csv.file_path = csv["file_path"].as<std::string>();
        if (csv["has_header"]) create_child_config.child_table_info.tags.csv.has_header = csv["has_header"].as<bool>();
        if (csv["delimiter"]) create_child_config.child_table_info.tags.csv.delimiter = csv["delimiter"].as<std::string>();
        if (csv["exclude_index"]) create_child_config.child_table_info.tags.csv.exclude_index = csv["exclude_index"].as<int>();
    } else {
        throw std::runtime_error("Invalid source_type for tags in child_table_info.");
    }

    // 解析 batch（可选）
    if (step.with["batch"]) {
        const auto& batch = step.with["batch"];
        if (batch["size"]) create_child_config.batch.size = batch["size"].as<int>();
        if (batch["concurrency"]) create_child_config.batch.concurrency = batch["concurrency"].as<int>();
    }

    // 将解析结果保存到 Step 的 action_config 字段
    step.action_config = std::move(create_child_config);

    // 打印解析结果（可选）
    std::cout << "Parsed create-child-table action for super table: " << create_child_config.super_table_info.name << std::endl;
}


void ParameterContext::parse_insert_data_action(Step& step) {
    InsertDataConfig insert_config;

    return;

    if (step.with["source"]) {
        const auto& source = step.with["source"];
        insert_config.source.table_name = source["table_name"].as<std::string>();
        insert_config.source.source_type = source["source_type"].as<std::string>();
        // 解析其他字段...
    }
    if (step.with["target"]) {
        const auto& target = step.with["target"];
        insert_config.target.database_name = target["database_name"].as<std::string>();
        insert_config.target.super_table_name = target["super_table_name"].as<std::string>();
        // 解析其他字段...
    }
    if (step.with["control"]) {
        const auto& control = step.with["control"];
        insert_config.control.concurrency = control["concurrency"].as<int>();
        insert_config.control.batch_size = control["batch_size"].as<int>();
        // 解析其他字段...
    }

    // 将解析结果保存到 Step 的 action_config 字段
    step.action_config = insert_config;

    // 打印解析结果（可选）
    std::cout << "Parsed insert-data action for table: " << insert_config.source.table_name << std::endl;
}




void ParameterContext::merge_yaml(const YAML::Node& config) {
    // 解析全局配置
    if (config["global"]) {
        parse_global(config["global"]);
    }

    // 解析作业并发数
    if (config["concurrency"]) {
        config_data.concurrency = config["concurrency"].as<int>();
    }

    // 解析作业列表
    if (config["jobs"]) {
        parse_jobs(config["jobs"]);
    }
}









void ParameterContext::merge_commandline(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg.find('=') != std::string::npos) {
            auto pos = arg.find('=');
            std::string key = arg.substr(0, pos);
            std::string value = arg.substr(pos + 1);
            cli_params[key] = value;
        }
    }

    // 映射命令行参数到全局配置
    auto& conn_info = config_data.global.connection_info;
    if (cli_params.count("--host")) conn_info.host = cli_params["--host"];
    if (cli_params.count("--port")) conn_info.port = std::stoi(cli_params["--port"]);
    if (cli_params.count("--user")) conn_info.user = cli_params["--user"];
    if (cli_params.count("--password")) conn_info.password = cli_params["--password"];
}


void ParameterContext::merge_environment_vars() {
    // 定义需要读取的环境变量
    std::vector<std::pair<std::string, std::string>> env_mappings = {
        {"TAOS_HOST", "host"},
        {"TAOS_PORT", "port"},
        {"TAOS_USER", "user"},
        {"TAOS_PASSWORD", "password"}
    };

    auto& conn_info = config_data.global.connection_info;
    // 遍历环境变量并更新连接信息
    for (const auto& [env_var, key] : env_mappings) {
        const char* env_value = std::getenv(env_var.c_str());
        if (env_value) {
            if (key == "host") conn_info.host = env_value;
            else if (key == "port") conn_info.port = std::stoi(env_value);
            else if (key == "user") conn_info.user = env_value;
            else if (key == "password") conn_info.password = env_value;
        }
    }
}


const ConfigData& ParameterContext::get_config_data() const {
    return config_data;
}

const GlobalConfig& ParameterContext::get_global_config() const {
    return config_data.global;
}

const ConnectionInfo& ParameterContext::get_connection_info() const {
    return config_data.global.connection_info;
}

const DatabaseInfo& ParameterContext::get_database_info() const {
    return config_data.global.database_info;
}

const SuperTableInfo& ParameterContext::get_super_table_info() const {
    return config_data.global.super_table_info;
}

// template <typename T>
// T ParameterContext::get(const std::string& path) const {
//     // 根据优先级获取参数值
//     if (cli_params.count(path)) {
//         return cli_params.at(path);
//     }
//     if (env_params.count(path)) {
//         return env_params.at(path);
//     }
//     if (json_config.contains(path)) {
//         return json_config.at(path).get<T>();
//     }
//     throw std::runtime_error("Parameter not found: " + path);
// }


// void validate() {
//     // 层级维度校验
//     validate_scope_constraints();
    
//     // 类型维度校验
//     validate_type_compatibility();
    
//     // 依赖维度校验
//     validate_dependencies();
    
//     // 冲突维度校验
//     validate_conflicts();
    
//     // 自定义业务规则
//     validate_business_rules();
//   }