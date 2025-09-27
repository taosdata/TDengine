#include "storage_engine_interface.h"
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

// 存储引擎接口注册表
typedef struct {
    char* name;
    StorageEngineInterfaceFactory factory;
} SStorageEngineRegistry;

// 全局注册表
static SStorageEngineRegistry* g_registry = NULL;
static uint32_t g_registry_size = 0;
static uint32_t g_registry_capacity = 0;
static pthread_mutex_t g_registry_mutex = PTHREAD_MUTEX_INITIALIZER;

// 默认存储引擎接口（空实现）
static int32_t default_init(const SStorageEngineConfig* config) {
    (void)config; // 避免未使用参数警告
    return 0;
}

static void default_destroy(void) {
    // 空实现
}

static int32_t default_install_interception(void) {
    return 0; // 成功但不做任何事
}

static int32_t default_uninstall_interception(void) {
    return 0; // 成功但不做任何事
}

static int32_t default_trigger_event(const SStorageEvent* event) {
    (void)event; // 避免未使用参数警告
    return 0;
}

static int32_t default_get_stats(uint64_t* events_processed, uint64_t* events_dropped) {
    if (events_processed) *events_processed = 0;
    if (events_dropped) *events_dropped = 0;
    return 0;
}

static bool default_is_supported(void) {
    return false; // 默认不支持
}

static const char* default_get_engine_name(void) {
    return "default";
}

// 默认存储引擎接口
static SStorageEngineInterface g_default_interface = {
    .init = default_init,
    .destroy = default_destroy,
    .install_interception = default_install_interception,
    .uninstall_interception = default_uninstall_interception,
    .trigger_event = default_trigger_event,
    .get_stats = default_get_stats,
    .is_supported = default_is_supported,
    .get_engine_name = default_get_engine_name
};

int32_t register_storage_engine_interface(const char* name, StorageEngineInterfaceFactory factory) {
    if (!name || !factory) {
        return -1;
    }
    
    pthread_mutex_lock(&g_registry_mutex);
    
    // 检查是否已存在
    for (uint32_t i = 0; i < g_registry_size; i++) {
        if (strcmp(g_registry[i].name, name) == 0) {
            // 更新现有注册
            g_registry[i].factory = factory;
            pthread_mutex_unlock(&g_registry_mutex);
            return 0;
        }
    }
    
    // 扩展注册表
    if (g_registry_size >= g_registry_capacity) {
        uint32_t new_capacity = g_registry_capacity == 0 ? 4 : g_registry_capacity * 2;
        SStorageEngineRegistry* new_registry = realloc(g_registry, 
                                                      new_capacity * sizeof(SStorageEngineRegistry));
        if (!new_registry) {
            pthread_mutex_unlock(&g_registry_mutex);
            return -1;
        }
        g_registry = new_registry;
        g_registry_capacity = new_capacity;
    }
    
    // 添加新注册
    g_registry[g_registry_size].name = strdup(name);
    g_registry[g_registry_size].factory = factory;
    g_registry_size++;
    
    pthread_mutex_unlock(&g_registry_mutex);
    return 0;
}

// 内部: 按名称从注册表获取（不存在则返回NULL）
static SStorageEngineInterface* get_by_name_or_null(const char* name) {
    if (!name) return NULL;
    pthread_mutex_lock(&g_registry_mutex);
    for (uint32_t i = 0; i < g_registry_size; i++) {
        if (strcmp(g_registry[i].name, name) == 0) {
            SStorageEngineInterface* interface = g_registry[i].factory();
            pthread_mutex_unlock(&g_registry_mutex);
            return interface;
        }
    }
    pthread_mutex_unlock(&g_registry_mutex);
    return NULL;
}

// 自动选择逻辑：
// 1) 若环境变量 USE_MOCK=1 或 STORAGE_ENGINE=mock，则返回 mock
// 2) 否则优先 tdengine_tmq，若未注册则回退 mock
// 3) 再回退 default
static SStorageEngineInterface* get_auto_selected_interface(void) {
    const char* use_mock = getenv("USE_MOCK");
    const char* engine_env = getenv("STORAGE_ENGINE");
    if ((use_mock && strcmp(use_mock, "1") == 0) || (engine_env && strcmp(engine_env, "mock") == 0)) {
        SStorageEngineInterface* mock = get_by_name_or_null("mock");
        return mock ? mock : get_default_storage_engine_interface();
    }
    // 优先 TMQ
    SStorageEngineInterface* tmq = get_by_name_or_null("tdengine_tmq");
    if (tmq) return tmq;
    // 回退 mock
    SStorageEngineInterface* mock = get_by_name_or_null("mock");
    if (mock) return mock;
    return get_default_storage_engine_interface();
}

SStorageEngineInterface* get_storage_engine_interface(const char* name) {
    if (!name || strcmp(name, "auto") == 0) {
        return get_auto_selected_interface();
    }
    
    SStorageEngineInterface* found = get_by_name_or_null(name);
    if (found) return found;
    return get_auto_selected_interface();
}

SStorageEngineInterface* get_default_storage_engine_interface(void) {
    return &g_default_interface;
}

int32_t list_storage_engine_interfaces(char** names, uint32_t max_count, uint32_t* actual_count) {
    if (!names || !actual_count) {
        return -1;
    }
    
    pthread_mutex_lock(&g_registry_mutex);
    
    uint32_t count = g_registry_size < max_count ? g_registry_size : max_count;
    
    for (uint32_t i = 0; i < count; i++) {
        names[i] = strdup(g_registry[i].name);
        if (!names[i]) {
            // 清理已分配的内存
            for (uint32_t j = 0; j < i; j++) {
                free(names[j]);
            }
            pthread_mutex_unlock(&g_registry_mutex);
            return -1;
        }
    }
    
    *actual_count = count;
    pthread_mutex_unlock(&g_registry_mutex);
    return 0;
}

// 清理函数（用于程序退出时）
void cleanup_storage_engine_registry(void) {
    pthread_mutex_lock(&g_registry_mutex);
    
    if (g_registry) {
        for (uint32_t i = 0; i < g_registry_size; i++) {
            free(g_registry[i].name);
        }
        free(g_registry);
        g_registry = NULL;
        g_registry_size = 0;
        g_registry_capacity = 0;
    }
    
    pthread_mutex_unlock(&g_registry_mutex);
}

