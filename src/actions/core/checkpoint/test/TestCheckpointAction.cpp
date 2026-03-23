#include <iostream>
#include <cassert>
#include <fstream>
#include "CheckpointAction.hpp"
#include "GlobalConfig.hpp"
#include "CheckpointActionConfig.hpp"
#include "InsertDataConfig.hpp"
#include "CheckpointData.hpp"

#include <nlohmann/json.hpp>

TDengineConfig* get_tdengine_config(GlobalConfig& config) {
    set_plugin_config(config.extensions, "tdengine", TDengineConfig{});
    return get_plugin_config_mut<TDengineConfig>(config.extensions, "tdengine");
}

void test_checkpoint_recover() {
    std::cout << "\n--- Running test_checkpoint_recover ---" << std::endl;
    GlobalConfig global;
    global.yaml_cfg_dir = "./b";

    auto* tc = get_tdengine_config(global);
    assert(tc != nullptr);
    tc->database = "test_db";
    global.schema.name = "test_super_table_recover"; // Use a different name to avoid conflict

    std::string timestamp_precision = "ms";
    int64_t start_timestamp = TimestampUtils::parse_timestamp("2021-05-03 08:00:00", timestamp_precision);
    int64_t timestamp_step = 1000;
    int64_t rows_per_table = 100;
    int64_t recovered_rows = 5;
    int64_t expected_start_ts = start_timestamp + recovered_rows * timestamp_step;
    int64_t expected_rows = 100 - recovered_rows;

    // 1. Manually create a checkpoint file
    std::string checkpoint_file = global.yaml_cfg_dir + "_" + tc->database + "_" + global.schema.name + "_checkpoints.json";
    std::cout << "Creating dummy checkpoint file: " << checkpoint_file << std::endl;
    nlohmann::json json_data;
    json_data["table_name"] = "t1";
    json_data["last_checkpoint_time"] = expected_start_ts;

    std::ofstream ofs(checkpoint_file);
    ofs << json_data.dump(4);
    ofs.close();

    // 2. Check if is_recover works
    CheckpointInfo chk_info;
    chk_info.enabled = true;
    assert(CheckpointAction::is_recover(global, chk_info));
    std::cout << "is_recover check passed." << std::endl;

    // 3. Create InsertDataConfig and test recover
    InsertDataConfig insert_config;
    insert_config.target_type = "tdengine";
    insert_config.checkpoint_info.enabled = true;
    TimestampGeneratorConfig ts_gen_cfg;
    ts_gen_cfg.start_timestamp = start_timestamp;
    ts_gen_cfg.timestamp_precision = timestamp_precision;
    ts_gen_cfg.timestamp_step = timestamp_step;
    insert_config.schema.columns_cfg.generator.timestamp_strategy.timestamp_config = ts_gen_cfg;
    insert_config.schema.generation.rows_per_table = rows_per_table;

    CheckpointAction::checkpoint_recover(global, insert_config);
    std::cout << "checkpoint_recover executed." << insert_config.schema.generation.rows_per_table << std::endl;

    // 4. Assert config changes
    std::cout << "Recovered start_timestamp: " << std::get<int64_t>(insert_config.schema.columns_cfg.generator.timestamp_strategy.timestamp_config.start_timestamp) << std::endl;
    std::cout << "expected_start_ts: " << expected_start_ts << std::endl;
    std::cout << "Adjusted rows_per_table: " << insert_config.schema.generation.rows_per_table << std::endl;
    std::cout << "expected_rows: " << expected_rows << std::endl;
    assert(std::get<int64_t>(insert_config.schema.columns_cfg.generator.timestamp_strategy.timestamp_config.start_timestamp) == expected_start_ts);
    assert(insert_config.schema.generation.rows_per_table == expected_rows);

    // 5. Clean up
    std::remove(checkpoint_file.c_str());
    std::cout << "Cleaned up dummy checkpoint file." << std::endl;

    std::cout << "test_checkpoint_recover passed\n";
}

void test_checkpoint_interrupt() {
    std::cout << "\n--- Running test_checkpoint_interrupt ---" << std::endl;
    GlobalConfig global;
    global.yaml_cfg_dir = "./c";

    auto* tc = get_tdengine_config(global);
    assert(tc != nullptr);
    tc->database = "test_db";
    global.schema.name = "test_super_table_interrupt"; // Use a different name

    CheckpointActionConfig config;
    config.enabled = true;
    config.interval_sec = 1;
    config.tableCount = 1;

    // Create and execute action
    auto action = std::make_unique<CheckpointAction>(global, config);
    action->execute();

    // Provide data and wait for checkpoint file
    std::vector<CheckpointData> data_list = {{"table1", 1700000000000, 1}};
    action->update_checkpoint(data_list);
    std::cout << "Waiting for checkpoint file to be created..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(4));

    // Trigger an interrupt
    std::cout << "Triggering interrupt..." << std::endl;
    CheckpointAction::stop_all(true); // true for interrupt

    // Check that the file was created
    std::string checkpoint_file = global.yaml_cfg_dir + "_" + tc->database + "_" + global.schema.name + "_checkpoints.json";

    // Check that the file was NOT deleted
    std::cout << "Checking if checkpoint file still exists after interrupt..." << std::endl;
    std::ifstream ifs_after_interrupt(checkpoint_file);
    assert(ifs_after_interrupt.is_open());
    ifs_after_interrupt.close();
    std::cout << "Checkpoint file correctly preserved after interrupt." << std::endl;

    // Clean up the file
    std::remove(checkpoint_file.c_str());
    std::cout << "Cleaned up dummy checkpoint file." << std::endl;

    std::cout << "test_checkpoint_interrupt passed\n";
}

int main() {
    test_checkpoint_interrupt();
    test_checkpoint_recover();

    std::cout << "All Checkpoint tests passed!\n";
    return 0;
}
