#ifndef TDENGINE_STORAGE_ENGINE_H
#define TDENGINE_STORAGE_ENGINE_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// TDengine TMQ 存储引擎工厂函数
struct SStorageEngineInterface* tdengine_storage_engine_create(void);

// 便捷函数：注册TDengine存储引擎
int32_t register_tdengine_storage_engine(void);

// 配置 TMQ 参数的高级接口
int32_t tdengine_set_tmq_config(const char* server_ip, int32_t server_port,
                                const char* username, const char* password,
                                const char* database, const char* topic_name,
                                const char* group_id);

// 设置 offset 提交策略
int32_t tdengine_set_commit_strategy(bool auto_commit, bool at_least_once, int64_t commit_interval_ms);

// 获取详细的统计信息
int32_t tdengine_get_detailed_stats(uint64_t* events_processed, uint64_t* events_dropped,
                                   uint64_t* messages_consumed, uint64_t* offset_commits,
                                   int64_t* last_commit_time);

#ifdef __cplusplus
}
#endif

#endif // TDENGINE_STORAGE_ENGINE_H
