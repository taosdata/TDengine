#include "storage_engine_interface.h"
#include "event_interceptor.h"
#include "bitmap_engine.h"
#include "ring_buffer.h"
#include "skiplist.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>

// TMQ 头文件 - 需要链接 TDengine 客户端库
#include <taos.h>

// 本地时间戳工具，避免依赖 TDengine 的时间精度宏
static inline int64_t now_monotonic_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;
}

static inline int64_t now_monotonic_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000LL + (int64_t)(ts.tv_nsec / 1000000LL);
}

// 前向声明，避免隐式声明导致的编译错误
static int32_t tdengine_uninstall_interception(void);

// TMQ 配置和状态管理
typedef struct {
    // TMQ 连接相关
    tmq_t* consumer;
    tmq_conf_t* config;
    char* topic_name;
    char* group_id;
    
    // 消费者线程
    pthread_t consumer_thread;
    pthread_mutex_t thread_mutex;
    bool thread_running;
    bool thread_should_stop;
    
    // 配置参数
    char* server_ip;
    int32_t server_port;
    char* username;
    char* password;
    char* database;
    
    // Offset 提交策略
    bool auto_commit;
    bool at_least_once;  // true=至少一次, false=至多一次
    int64_t commit_interval_ms;
    
    // 统计信息
    uint64_t events_processed;
    uint64_t events_dropped;
    uint64_t messages_consumed;
    uint64_t offset_commits;
    int64_t last_commit_time;
    
    // 可观测性指标
    SObservabilityMetrics observability_metrics;
    int64_t start_time;              // 启动时间
    int64_t last_metrics_update;     // 最后指标更新时间
    uint64_t last_events_count;      // 上次事件计数
    uint64_t last_messages_count;    // 上次消息计数
    uint64_t last_bytes_count;       // 上次字节计数
    
    // 重试统计
    uint64_t connection_retries;
    uint64_t subscription_retries;
    uint64_t commit_retries;
    
    // 错误统计
    uint64_t parse_errors;
    
    // 事件处理相关
    int64_t last_event_timestamp;    // 最后事件时间戳
    SEventInterceptor* event_interceptor;  // 事件拦截器
    SBitmapEngine* bitmap_engine;    // 位图引擎
    
    // 事件回调
    StorageEventCallback event_callback;
    void* callback_user_data;
    
    // 互斥锁保护
    pthread_mutex_t mutex;
    pthread_rwlock_t config_rwlock;
    
    // 状态标志
    bool initialized;
    bool interception_installed;
} STmqContext;

static STmqContext g_tmq_context = {
    .consumer = NULL,
    .config = NULL,
    .topic_name = NULL,
    .group_id = NULL,
    .consumer_thread = 0,
    .thread_mutex = PTHREAD_MUTEX_INITIALIZER,
    .thread_running = false,
    .thread_should_stop = false,
    .server_ip = NULL,
    .server_port = 0,
    .username = NULL,
    .password = NULL,
    .database = NULL,
    .auto_commit = false,
    .at_least_once = true,
    .commit_interval_ms = 0,
    .events_processed = 0,
    .events_dropped = 0,
    .messages_consumed = 0,
    .offset_commits = 0,
    .last_commit_time = 0,
    .observability_metrics = {0},
    .start_time = 0,
    .last_metrics_update = 0,
    .last_events_count = 0,
    .last_messages_count = 0,
    .last_bytes_count = 0,
    .connection_retries = 0,
    .subscription_retries = 0,
    .commit_retries = 0,
    .parse_errors = 0,
    .last_event_timestamp = 0,
    .event_interceptor = NULL,
    .bitmap_engine = NULL,
    .event_callback = NULL,
    .callback_user_data = NULL,
    .mutex = PTHREAD_MUTEX_INITIALIZER,
    .config_rwlock = PTHREAD_RWLOCK_INITIALIZER,
    .initialized = false,
    .interception_installed = false
};

// 默认配置值
#define DEFAULT_SERVER_IP "localhost"
#define DEFAULT_SERVER_PORT 6030
#define DEFAULT_USERNAME "root"
#define DEFAULT_PASSWORD "taosdata"
#define DEFAULT_DATABASE "test_bitmap_db"
#define DEFAULT_TOPIC_NAME "incremental_backup_topic"
#define DEFAULT_GROUP_ID "incremental_backup_group"
#define DEFAULT_COMMIT_INTERVAL_MS 1000
#define DEFAULT_POLL_TIMEOUT_MS 100
#define DEFAULT_CONNECT_TIMEOUT_MS 5000

// 重试控制
#define TMQ_CREATE_RETRY_MAX 5
#define TMQ_SUBSCRIBE_RETRY_MAX 5

// TMQ 配置初始化
static int32_t init_tmq_config(void) {
    g_tmq_context.config = tmq_conf_new();
    if (!g_tmq_context.config) {
        printf("[TMQ] 创建配置失败\n");
        return -1;
    }
    
    // 允许通过环境变量覆盖基础连接参数（提升部署兼容性）
    // TD_CONNECT_IP, TD_CONNECT_PORT, TD_USERNAME, TD_PASSWORD, TD_DATABASE, TD_TOPIC_NAME, TD_GROUP_ID
    const char* env_ip = getenv("TD_CONNECT_IP");
    if (env_ip && *env_ip) {
        snprintf(g_tmq_context.server_ip, sizeof(g_tmq_context.server_ip), "%s", env_ip);
    }
    const char* env_port = getenv("TD_CONNECT_PORT");
    if (env_port && *env_port) {
        int p = atoi(env_port);
        if (p > 0 && p < 65536) g_tmq_context.server_port = p;
    }
    const char* env_user = getenv("TD_USERNAME");
    if (env_user && *env_user) {
        snprintf(g_tmq_context.username, sizeof(g_tmq_context.username), "%s", env_user);
    }
    const char* env_pass = getenv("TD_PASSWORD");
    if (env_pass && *env_pass) {
        snprintf(g_tmq_context.password, sizeof(g_tmq_context.password), "%s", env_pass);
    }
    const char* env_db = getenv("TD_DATABASE");
    if (env_db && *env_db) {
        snprintf(g_tmq_context.database, sizeof(g_tmq_context.database), "%s", env_db);
    }
    const char* env_topic = getenv("TD_TOPIC_NAME");
    if (env_topic && *env_topic) {
        snprintf(g_tmq_context.topic_name, sizeof(g_tmq_context.topic_name), "%s", env_topic);
    }
    const char* env_group = getenv("TD_GROUP_ID");
    if (env_group && *env_group) {
        snprintf(g_tmq_context.group_id, sizeof(g_tmq_context.group_id), "%s", env_group);
    }
    
    // 设置基本连接参数
    if (tmq_conf_set(g_tmq_context.config, "td.connect.ip", g_tmq_context.server_ip) != TMQ_CONF_OK) {
        printf("[TMQ] 设置服务器IP失败\n");
        return -1;
    }
    
    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", g_tmq_context.server_port);
    if (tmq_conf_set(g_tmq_context.config, "td.connect.port", port_str) != TMQ_CONF_OK) {
        printf("[TMQ] 设置服务器端口失败\n");
        return -1;
    }
    
    if (tmq_conf_set(g_tmq_context.config, "td.connect.user", g_tmq_context.username) != TMQ_CONF_OK) {
        printf("[TMQ] 设置用户名失败\n");
        return -1;
    }
    
    if (tmq_conf_set(g_tmq_context.config, "td.connect.pass", g_tmq_context.password) != TMQ_CONF_OK) {
        printf("[TMQ] 设置密码失败\n");
        return -1;
    }
    
    // database 在不同版本中可能无此键；尝试多种键名，失败则仅警告不致命
    if (g_tmq_context.database && strlen(g_tmq_context.database) > 0) {
        int db_set_ok = 0;
        if (tmq_conf_set(g_tmq_context.config, "td.connect.database", g_tmq_context.database) == TMQ_CONF_OK) {
            db_set_ok = 1;
        } else if (tmq_conf_set(g_tmq_context.config, "td.connect.db", g_tmq_context.database) == TMQ_CONF_OK) {
            db_set_ok = 1;
        }
        if (!db_set_ok) {
            printf("[TMQ] 警告: 数据库配置键不被支持，跳过设置 (value=%s)\n", g_tmq_context.database);
        }
    }
    
    // 设置消费者组参数
    if (tmq_conf_set(g_tmq_context.config, "group.id", g_tmq_context.group_id) != TMQ_CONF_OK) {
        printf("[TMQ] 设置消费者组ID失败\n");
        return -1;
    }
    
    // 设置 offset 提交策略
    if (g_tmq_context.auto_commit) {
        if (tmq_conf_set(g_tmq_context.config, "enable.auto.commit", "true") != TMQ_CONF_OK) {
            printf("[TMQ] 启用自动提交失败\n");
            return -1;
        }
    } else {
        if (tmq_conf_set(g_tmq_context.config, "enable.auto.commit", "false") != TMQ_CONF_OK) {
            printf("[TMQ] 禁用自动提交失败\n");
            return -1;
        }
    }
    
    // 设置 offset 重置策略
    const char* offset_reset = g_tmq_context.at_least_once ? "earliest" : "latest";
    if (tmq_conf_set(g_tmq_context.config, "auto.offset.reset", offset_reset) != TMQ_CONF_OK) {
        printf("[TMQ] 设置offset重置策略失败\n");
        return -1;
    }

    // 连接与请求超时（尝试多种可能的配置键名），支持环境变量覆盖键名
    char timeout_buf[16];
    snprintf(timeout_buf, sizeof(timeout_buf), "%d", DEFAULT_CONNECT_TIMEOUT_MS);
    
    const char* key_conn_timeout = getenv("TMQ_KEY_CONNECT_TIMEOUT");
    int timeout_set_ok = 0;
    if (key_conn_timeout && *key_conn_timeout) {
        // 显式指定键名
        if (tmq_conf_set(g_tmq_context.config, key_conn_timeout, timeout_buf) == TMQ_CONF_OK) {
            timeout_set_ok = 1;
        }
    } else {
        // 尝试常见键名集合
        if (tmq_conf_set(g_tmq_context.config, "connect.timeout", timeout_buf) == TMQ_CONF_OK) {
            timeout_set_ok = 1;
        } else if (tmq_conf_set(g_tmq_context.config, "td.connect.timeout", timeout_buf) == TMQ_CONF_OK) {
            timeout_set_ok = 1;
        } else if (tmq_conf_set(g_tmq_context.config, "connection.timeout", timeout_buf) == TMQ_CONF_OK) {
            timeout_set_ok = 1;
        }
    }
    if (!timeout_set_ok) {
        printf("[TMQ] 警告: 未能设置连接超时配置 (尝试键=%s, 值=%s)\n", key_conn_timeout ? key_conn_timeout : "auto", timeout_buf);
    }
    
    // 请求超时配置
    const char* key_req_timeout = getenv("TMQ_KEY_REQUEST_TIMEOUT");
    int req_ok = 0;
    if (key_req_timeout && *key_req_timeout) {
        if (tmq_conf_set(g_tmq_context.config, key_req_timeout, timeout_buf) == TMQ_CONF_OK) req_ok = 1;
    } else {
        if (tmq_conf_set(g_tmq_context.config, "request.timeout.ms", timeout_buf) == TMQ_CONF_OK) {
            req_ok = 1;
        } else if (tmq_conf_set(g_tmq_context.config, "td.request.timeout", timeout_buf) == TMQ_CONF_OK) {
            req_ok = 1;
        } else if (tmq_conf_set(g_tmq_context.config, "request.timeout", timeout_buf) == TMQ_CONF_OK) {
            req_ok = 1;
        }
    }
    if (!req_ok) {
        // 非致命，仅记录
        printf("[TMQ] 警告: 未能设置请求超时配置 (尝试键=%s, 值=%s)\n", key_req_timeout ? key_req_timeout : "auto", timeout_buf);
    }
    // 设定客户端ID（便于排查）
    if (tmq_conf_set(g_tmq_context.config, "client.id", "backup-consumer-1") != TMQ_CONF_OK) {
        // 非致命
    }
    
    printf("[TMQ] 配置初始化成功\n");
    return 0;
}

// TMQ 消费者创建和连接
static int32_t create_tmq_consumer(void) {
    char errstr[512];

    // 带重试创建消费者
    int attempt = 0;
    while (attempt < TMQ_CREATE_RETRY_MAX) {
        g_tmq_context.consumer = tmq_consumer_new(g_tmq_context.config, errstr, sizeof(errstr));
        if (g_tmq_context.consumer) break;
        printf("[TMQ] 创建消费者失败(%d/%d): %s\n", attempt + 1, TMQ_CREATE_RETRY_MAX, errstr);
        int backoff_ms = 200 * (1 << attempt);
        struct timespec ts = { .tv_sec = backoff_ms / 1000, .tv_nsec = (backoff_ms % 1000) * 1000000L };
        nanosleep(&ts, NULL);
        attempt++;
    }
    if (!g_tmq_context.consumer) {
        printf("[TMQ] 创建消费者失败: %s\n", errstr);
        return -1;
    }

    // 创建主题列表
    tmq_list_t* topic_list = tmq_list_new();
    if (!topic_list) {
        printf("[TMQ] 创建主题列表失败\n");
        return -1;
    }
    if (tmq_list_append(topic_list, g_tmq_context.topic_name) != 0) {
        printf("[TMQ] 添加主题到列表失败\n");
        tmq_list_destroy(topic_list);
        return -1;
    }

    // 带重试订阅
    attempt = 0;
    int sub_ok = 0;
    while (attempt < TMQ_SUBSCRIBE_RETRY_MAX) {
        int sub_result = tmq_subscribe(g_tmq_context.consumer, topic_list);
        if (sub_result == 0) { 
            sub_ok = 1; 
            break; 
        }
        printf("[TMQ] 订阅主题失败(%d/%d): 错误码=%d, 主题=%s\n", 
               attempt + 1, TMQ_SUBSCRIBE_RETRY_MAX, sub_result, g_tmq_context.topic_name);
        int backoff_ms = 200 * (1 << attempt);
        struct timespec ts = { .tv_sec = backoff_ms / 1000, .tv_nsec = (backoff_ms % 1000) * 1000000L };
        nanosleep(&ts, NULL);
        attempt++;
    }
    tmq_list_destroy(topic_list);
    if (!sub_ok) {
        printf("[TMQ] 订阅主题失败\n");
        return -1;
    }

    printf("[TMQ] 消费者创建和订阅成功\n");
    return 0;
}

// TMQ 消息解析为存储事件
static SStorageEvent* parse_tmq_message(TAOS_RES* msg) {
    if (!msg) {
        return NULL;
    }
    
    SStorageEvent* event = malloc(sizeof(SStorageEvent));
    if (!event) {
        printf("[TMQ] 分配事件内存失败\n");
        return NULL;
    }
    
    // 获取消息类型
    tmq_res_t res_type = tmq_get_res_type(msg);
    
    // 根据消息类型映射事件类型
    switch (res_type) {
        case TMQ_RES_DATA:
            event->event_type = STORAGE_EVENT_BLOCK_UPDATE;
            break;
        case TMQ_RES_TABLE_META:
            event->event_type = STORAGE_EVENT_BLOCK_CREATE;
            break;
        case TMQ_RES_METADATA:
            event->event_type = STORAGE_EVENT_BLOCK_FLUSH;
            break;
        case TMQ_RES_RAWDATA:
            event->event_type = STORAGE_EVENT_BLOCK_UPDATE;
            break;
        default:
            event->event_type = STORAGE_EVENT_BLOCK_UPDATE;
            break;
    }
    
    // 获取块ID（从表名或其他标识符生成）
    const char* table_name = tmq_get_table_name(msg);
    if (table_name) {
        // 简单的哈希算法生成块ID
        uint64_t hash = 0;
        for (int i = 0; table_name[i]; i++) {
            hash = hash * 31 + table_name[i];
        }
        event->block_id = hash;
    } else {
        event->block_id = 0;
    }
    
    // 获取 WAL offset
    event->wal_offset = tmq_get_vgroup_offset(msg);
    
    // 获取时间戳
    int64_t current_time = now_monotonic_ns();
    event->timestamp = current_time;
    
    event->user_data = NULL;
    
    return event;
}

// Offset 提交策略实现
static int32_t commit_offset(const char* topic_name, int32_t vg_id, int64_t offset, bool sync) {
    if (!g_tmq_context.consumer) {
        return -1;
    }
    
    int32_t result;
    if (sync) {
        result = tmq_commit_offset_sync(g_tmq_context.consumer, topic_name, vg_id, offset);
    } else {
        // 异步提交
        tmq_commit_offset_async(g_tmq_context.consumer, topic_name, vg_id, offset, NULL, NULL);
        result = 0; // 异步提交总是返回成功
    }
    
    if (result == 0) {
        pthread_mutex_lock(&g_tmq_context.mutex);
        g_tmq_context.offset_commits++;
        g_tmq_context.last_commit_time = now_monotonic_ms();
        pthread_mutex_unlock(&g_tmq_context.mutex);
        
        if (sync) {
            printf("[TMQ] Offset 同步提交成功: topic=%s, vg_id=%d, offset=%ld\n", 
                   topic_name, vg_id, offset);
        } else {
            printf("[TMQ] Offset 异步提交成功: topic=%s, vg_id=%d, offset=%ld\n", 
                   topic_name, vg_id, offset);
        }
    } else {
        printf("[TMQ] Offset 提交失败: topic=%s, vg_id=%d, offset=%ld, error=%d\n", 
               topic_name, vg_id, offset, result);
    }
    
    return result;
}

// TMQ 消费线程主函数
static void* tmq_consumer_thread(void* arg) {
    (void)arg; // 避免未使用参数警告
    
    printf("[TMQ] 消费线程启动\n");
    
    while (g_tmq_context.thread_running && !g_tmq_context.thread_should_stop) {
        // 轮询消息
        TAOS_RES* msg = tmq_consumer_poll(g_tmq_context.consumer, DEFAULT_POLL_TIMEOUT_MS);
        
        if (!msg) {
            // 超时，继续轮询
            continue;
        }
        
        // 解析消息为存储事件
        SStorageEvent* event = parse_tmq_message(msg);
        if (event) {
            // 调用事件回调
            if (g_tmq_context.event_callback) {
                g_tmq_context.event_callback(event, g_tmq_context.callback_user_data);
            }
            
            // 更新统计信息
            pthread_mutex_lock(&g_tmq_context.mutex);
            g_tmq_context.events_processed++;
            g_tmq_context.messages_consumed++;
            pthread_mutex_unlock(&g_tmq_context.mutex);
            
            // 根据策略提交 offset
            if (!g_tmq_context.auto_commit) {
                const char* topic_name = tmq_get_topic_name(msg);
                int32_t vg_id = tmq_get_vgroup_id(msg);
                int64_t offset = tmq_get_vgroup_offset(msg);
                
                if (g_tmq_context.at_least_once) {
                    // 至少一次：同步提交
                    commit_offset(topic_name, vg_id, offset, true);
                } else {
                    // 至多一次：异步提交
                    commit_offset(topic_name, vg_id, offset, false);
                }
            }
            
            free(event);
        } else {
            pthread_mutex_lock(&g_tmq_context.mutex);
            g_tmq_context.events_dropped++;
            pthread_mutex_unlock(&g_tmq_context.mutex);
            printf("[TMQ] 消息解析失败\n");
        }
        
        // 释放消息资源
        taos_free_result(msg);
        
        // 检查是否需要定期提交 offset
        if (g_tmq_context.auto_commit && g_tmq_context.commit_interval_ms > 0) {
            int64_t current_time = now_monotonic_ms();
            if (current_time - g_tmq_context.last_commit_time >= g_tmq_context.commit_interval_ms) {
                // 这里可以实现批量提交逻辑
                g_tmq_context.last_commit_time = current_time;
            }
        }
    }
    
    printf("[TMQ] 消费线程退出\n");
    return NULL;
}

// TDengine存储引擎实现函数
static int32_t tdengine_init(const SStorageEngineConfig* config) {
    if (!config) {
        return -1;
    }
    
    pthread_mutex_lock(&g_tmq_context.mutex);
    
    // 保存回调函数
    g_tmq_context.event_callback = config->event_callback;
    g_tmq_context.callback_user_data = config->callback_user_data;
    
    // 初始化默认配置
    g_tmq_context.server_ip = strdup(DEFAULT_SERVER_IP);
    g_tmq_context.server_port = DEFAULT_SERVER_PORT;
    g_tmq_context.username = strdup(DEFAULT_USERNAME);
    g_tmq_context.password = strdup(DEFAULT_PASSWORD);
    g_tmq_context.database = strdup(DEFAULT_DATABASE);
    g_tmq_context.topic_name = strdup(DEFAULT_TOPIC_NAME);
    g_tmq_context.group_id = strdup(DEFAULT_GROUP_ID);
    
    // 从环境变量或配置中获取自定义设置
    const char* env_topic = getenv("TMQ_TOPIC_NAME");
    const char* env_group = getenv("TMQ_GROUP_ID");
    const char* env_server = getenv("TMQ_SERVER_ADDR");
    
    if (env_topic && strlen(env_topic) > 0) {
        free(g_tmq_context.topic_name);
        g_tmq_context.topic_name = strdup(env_topic);
        printf("[TMQ] 使用环境变量主题: %s\n", g_tmq_context.topic_name);
    }
    
    if (env_group && strlen(env_group) > 0) {
        free(g_tmq_context.group_id);
        g_tmq_context.group_id = strdup(env_group);
        printf("[TMQ] 使用环境变量组ID: %s\n", g_tmq_context.group_id);
    }
    
    if (env_server && strlen(env_server) > 0) {
        // 解析服务器地址 (格式: ip:port)
        char* colon = strchr(env_server, ':');
        if (colon) {
            *colon = '\0';
            free(g_tmq_context.server_ip);
            g_tmq_context.server_ip = strdup(env_server);
            g_tmq_context.server_port = atoi(colon + 1);
            printf("[TMQ] 使用环境变量服务器: %s:%d\n", g_tmq_context.server_ip, g_tmq_context.server_port);
        }
    }
    
    // 设置 offset 提交策略
    g_tmq_context.auto_commit = false;  // 默认手动提交
    g_tmq_context.at_least_once = true; // 默认至少一次
    g_tmq_context.commit_interval_ms = DEFAULT_COMMIT_INTERVAL_MS;
    
    // 初始化统计信息
    g_tmq_context.events_processed = 0;
    g_tmq_context.events_dropped = 0;
    g_tmq_context.messages_consumed = 0;
    g_tmq_context.offset_commits = 0;
    g_tmq_context.last_commit_time = 0;
    
    // 初始化线程状态
    g_tmq_context.thread_running = false;
    g_tmq_context.thread_should_stop = false;
    
    g_tmq_context.initialized = true;
    g_tmq_context.start_time = now_monotonic_ms();
    g_tmq_context.last_metrics_update = g_tmq_context.start_time;
    pthread_mutex_unlock(&g_tmq_context.mutex);
    
    printf("[TMQ] TDengine存储引擎初始化成功\n");
    return 0;
}

static void tdengine_destroy(void) {
    // 先停止消费线程
    if (g_tmq_context.thread_running) {
        tdengine_uninstall_interception();
    }
    
    pthread_mutex_lock(&g_tmq_context.mutex);
    
    // 清理 TMQ 资源
    if (g_tmq_context.consumer) {
        tmq_consumer_close(g_tmq_context.consumer);
        g_tmq_context.consumer = NULL;
    }
    
    if (g_tmq_context.config) {
        tmq_conf_destroy(g_tmq_context.config);
        g_tmq_context.config = NULL;
    }
    
    // 释放字符串资源
    if (g_tmq_context.server_ip) {
        free(g_tmq_context.server_ip);
        g_tmq_context.server_ip = NULL;
    }
    if (g_tmq_context.username) {
        free(g_tmq_context.username);
        g_tmq_context.username = NULL;
    }
    if (g_tmq_context.password) {
        free(g_tmq_context.password);
        g_tmq_context.password = NULL;
    }
    if (g_tmq_context.database) {
        free(g_tmq_context.database);
        g_tmq_context.database = NULL;
    }
    if (g_tmq_context.topic_name) {
        free(g_tmq_context.topic_name);
        g_tmq_context.topic_name = NULL;
    }
    if (g_tmq_context.group_id) {
        free(g_tmq_context.group_id);
        g_tmq_context.group_id = NULL;
    }
    
    g_tmq_context.initialized = false;
    pthread_mutex_unlock(&g_tmq_context.mutex);
    
    printf("[TMQ] TDengine存储引擎销毁完成\n");
}

static int32_t tdengine_install_interception(void) {
    pthread_mutex_lock(&g_tmq_context.mutex);
    if (!g_tmq_context.initialized) {
        pthread_mutex_unlock(&g_tmq_context.mutex);
        return -1;
    }
    
    // 初始化 TMQ 配置
    if (init_tmq_config() != 0) {
        pthread_mutex_unlock(&g_tmq_context.mutex);
        return -1;
    }
    
    // 创建 TMQ 消费者
    if (create_tmq_consumer() != 0) {
        pthread_mutex_unlock(&g_tmq_context.mutex);
        return -1;
    }
    
    // 启动消费线程
    g_tmq_context.thread_running = true;
    g_tmq_context.thread_should_stop = false;
    
    if (pthread_create(&g_tmq_context.consumer_thread, NULL, tmq_consumer_thread, NULL) != 0) {
        printf("[TMQ] 创建消费线程失败\n");
        g_tmq_context.thread_running = false;
        pthread_mutex_unlock(&g_tmq_context.mutex);
        return -1;
    }
    
    g_tmq_context.interception_installed = true;
    pthread_mutex_unlock(&g_tmq_context.mutex);
    
    printf("[TMQ] 事件拦截安装成功，消费线程已启动\n");
    return 0;
}

static int32_t tdengine_uninstall_interception(void) {
    pthread_mutex_lock(&g_tmq_context.mutex);
    
    if (g_tmq_context.thread_running) {
        // 通知线程停止
        g_tmq_context.thread_should_stop = true;
        pthread_mutex_unlock(&g_tmq_context.mutex);
        
        // 等待线程退出
        if (pthread_join(g_tmq_context.consumer_thread, NULL) != 0) {
            printf("[TMQ] 等待消费线程退出失败\n");
        }
        
        pthread_mutex_lock(&g_tmq_context.mutex);
        g_tmq_context.thread_running = false;
    }
    
    g_tmq_context.interception_installed = false;
    pthread_mutex_unlock(&g_tmq_context.mutex);
    
    printf("[TMQ] 事件拦截卸载成功\n");
    return 0;
}

static int32_t tdengine_trigger_event(const SStorageEvent* event) {
    if (!event) {
        return -1;
    }
    
    pthread_mutex_lock(&g_tmq_context.mutex);
    if (!g_tmq_context.interception_installed) {
        pthread_mutex_unlock(&g_tmq_context.mutex);
        return -1;
    }
    
    g_tmq_context.events_processed++;
    pthread_mutex_unlock(&g_tmq_context.mutex);
    
    printf("[TMQ] 触发事件: 类型=%d, 块ID=%lu, WAL偏移量=%lu, 时间戳=%ld\n",
           event->event_type, event->block_id, event->wal_offset, event->timestamp);
    
    // 调用回调函数
    if (g_tmq_context.event_callback) {
        g_tmq_context.event_callback(event, g_tmq_context.callback_user_data);
    }
    
    return 0;
}

static int32_t tdengine_get_stats(uint64_t* events_processed, uint64_t* events_dropped) {
    pthread_mutex_lock(&g_tmq_context.mutex);
    
    if (events_processed) {
        *events_processed = g_tmq_context.events_processed;
    }
    if (events_dropped) {
        *events_dropped = g_tmq_context.events_dropped;
    }
    
    pthread_mutex_unlock(&g_tmq_context.mutex);
    return 0;
}

static bool tdengine_is_supported(void) {
    // 检查 TDengine 客户端库是否可用
    // 这里可以添加更详细的检查逻辑
        return true;
}

static const char* tdengine_get_engine_name(void) {
    return "tdengine_tmq";
}

// 前向声明
static int32_t tdengine_get_observability_metrics(void* engine, SObservabilityMetrics* metrics);
static int32_t tdengine_reset_stats(void* engine);

// TDengine存储引擎接口
static SStorageEngineInterface g_tdengine_interface = {
    .init = tdengine_init,
    .destroy = tdengine_destroy,
    .install_interception = tdengine_install_interception,
    .uninstall_interception = tdengine_uninstall_interception,
    .trigger_event = tdengine_trigger_event,
    .get_stats = tdengine_get_stats,
    .is_supported = tdengine_is_supported,
    .get_engine_name = tdengine_get_engine_name,
    .get_observability_metrics = tdengine_get_observability_metrics,
    .reset_stats = tdengine_reset_stats
};

// TDengine存储引擎工厂函数
SStorageEngineInterface* tdengine_storage_engine_create(void) {
    return &g_tdengine_interface;
}

// 便捷函数：注册TDengine存储引擎
int32_t register_tdengine_storage_engine(void) {
    extern int32_t register_storage_engine_interface(const char* name, StorageEngineInterfaceFactory factory);
    return register_storage_engine_interface("tdengine_tmq", tdengine_storage_engine_create);
}

// 配置 TMQ 参数的高级接口
int32_t tdengine_set_tmq_config(const char* server_ip, int32_t server_port,
                                const char* username, const char* password,
                                const char* database, const char* topic_name,
                                const char* group_id) {
    if (!g_tmq_context.initialized) {
        return -1;
    }
    
    pthread_rwlock_wrlock(&g_tmq_context.config_rwlock);
    
    if (server_ip) {
        free(g_tmq_context.server_ip);
        g_tmq_context.server_ip = strdup(server_ip);
    }
    if (server_port > 0) {
        g_tmq_context.server_port = server_port;
    }
    if (username) {
        free(g_tmq_context.username);
        g_tmq_context.username = strdup(username);
    }
    if (password) {
        free(g_tmq_context.password);
        g_tmq_context.password = strdup(password);
    }
    if (database) {
        free(g_tmq_context.database);
        g_tmq_context.database = strdup(database);
    }
    if (topic_name) {
        free(g_tmq_context.topic_name);
        g_tmq_context.topic_name = strdup(topic_name);
    }
    if (group_id) {
        free(g_tmq_context.group_id);
        g_tmq_context.group_id = strdup(group_id);
    }
    
    pthread_rwlock_unlock(&g_tmq_context.config_rwlock);
    
    printf("[TMQ] 配置更新成功\n");
    return 0;
}

// 设置 offset 提交策略
int32_t tdengine_set_commit_strategy(bool auto_commit, bool at_least_once, int64_t commit_interval_ms) {
    if (!g_tmq_context.initialized) {
    return -1;
}

    pthread_mutex_lock(&g_tmq_context.mutex);
    g_tmq_context.auto_commit = auto_commit;
    g_tmq_context.at_least_once = at_least_once;
    g_tmq_context.commit_interval_ms = commit_interval_ms;
    pthread_mutex_unlock(&g_tmq_context.mutex);
    
    printf("[TMQ] 提交策略设置: auto_commit=%s, at_least_once=%s, interval=%ldms\n",
           auto_commit ? "true" : "false", at_least_once ? "true" : "false", commit_interval_ms);
    return 0;
}

// 获取详细的统计信息
int32_t tdengine_get_detailed_stats(uint64_t* events_processed, uint64_t* events_dropped,
                                   uint64_t* messages_consumed, uint64_t* offset_commits,
                                   int64_t* last_commit_time) {
    // 未初始化时返回错误，符合错误处理测试预期
    pthread_mutex_lock(&g_tmq_context.mutex);
    if (!g_tmq_context.initialized) {
        pthread_mutex_unlock(&g_tmq_context.mutex);
        return -1;
    }
    
    if (events_processed) *events_processed = g_tmq_context.events_processed;
    if (events_dropped) *events_dropped = g_tmq_context.events_dropped;
    if (messages_consumed) *messages_consumed = g_tmq_context.messages_consumed;
    if (offset_commits) *offset_commits = g_tmq_context.offset_commits;
    if (last_commit_time) *last_commit_time = g_tmq_context.last_commit_time;
    
    pthread_mutex_unlock(&g_tmq_context.mutex);
    return 0;
}

// 可观测性指标收集和更新函数
static void update_observability_metrics_internal(void) {
    if (!g_tmq_context.initialized) {
        return;
    }
    
    pthread_mutex_lock(&g_tmq_context.mutex);
    
    int64_t current_time = now_monotonic_ms();
    int64_t time_diff_ms = current_time - g_tmq_context.last_metrics_update;
    
    if (time_diff_ms > 0) {
        // 计算速率指标
        uint64_t events_diff = g_tmq_context.events_processed - g_tmq_context.last_events_count;
        uint64_t messages_diff = g_tmq_context.messages_consumed - g_tmq_context.last_messages_count;
        uint64_t bytes_diff = 0; // TODO: 实现字节计数跟踪
        
        g_tmq_context.observability_metrics.events_per_second = 
            (events_diff * 1000) / time_diff_ms;
        g_tmq_context.observability_metrics.messages_per_second = 
            (messages_diff * 1000) / time_diff_ms;
        g_tmq_context.observability_metrics.bytes_per_second = 
            (bytes_diff * 1000) / time_diff_ms;
        
        // 更新计数
        g_tmq_context.last_events_count = g_tmq_context.events_processed;
        g_tmq_context.last_messages_count = g_tmq_context.messages_consumed;
        g_tmq_context.last_metrics_update = current_time;
    }
    
    // 更新丢弃指标
    g_tmq_context.observability_metrics.events_dropped = g_tmq_context.events_dropped;
    g_tmq_context.observability_metrics.messages_dropped = g_tmq_context.events_dropped;
    g_tmq_context.observability_metrics.parse_errors = g_tmq_context.parse_errors;
    
    // 更新重试指标
    g_tmq_context.observability_metrics.connection_retries = g_tmq_context.connection_retries;
    g_tmq_context.observability_metrics.subscription_retries = g_tmq_context.subscription_retries;
    g_tmq_context.observability_metrics.commit_retries = g_tmq_context.commit_retries;
    
    // 更新队列水位 - 从事件拦截器获取真实数据
    if (g_tmq_context.event_interceptor && g_tmq_context.event_interceptor->event_buffer) {
        SRingBuffer* ring_buffer = (SRingBuffer*)g_tmq_context.event_interceptor->event_buffer;
        uint32_t current_size = ring_buffer_get_size(ring_buffer);
        uint32_t capacity = ring_buffer_get_capacity(ring_buffer);
        
        g_tmq_context.observability_metrics.ring_buffer_usage = 
            capacity > 0 ? (current_size * 100) / capacity : 0;
        g_tmq_context.observability_metrics.ring_buffer_capacity = capacity;
        g_tmq_context.observability_metrics.event_queue_size = current_size;
    } else {
        g_tmq_context.observability_metrics.ring_buffer_usage = 0;
        g_tmq_context.observability_metrics.ring_buffer_capacity = 0;
        g_tmq_context.observability_metrics.event_queue_size = 0;
    }
    
    // 更新内存指标 - 从位图引擎获取真实数据
    size_t total_memory = sizeof(STmqContext);
    size_t bitmap_memory = 0;
    size_t metadata_memory = 0;
    
    // 计算位图引擎内存使用
    if (g_tmq_context.bitmap_engine) {
        // 计算位图内存使用
        if (g_tmq_context.bitmap_engine->dirty_blocks) {
            bitmap_memory += g_tmq_context.bitmap_engine->dirty_blocks->memory_usage(
                g_tmq_context.bitmap_engine->dirty_blocks->bitmap);
        }
        if (g_tmq_context.bitmap_engine->new_blocks) {
            bitmap_memory += g_tmq_context.bitmap_engine->new_blocks->memory_usage(
                g_tmq_context.bitmap_engine->new_blocks->bitmap);
        }
        if (g_tmq_context.bitmap_engine->deleted_blocks) {
            bitmap_memory += g_tmq_context.bitmap_engine->deleted_blocks->memory_usage(
                g_tmq_context.bitmap_engine->deleted_blocks->bitmap);
        }
        
        // 计算元数据内存使用
        metadata_memory = g_tmq_context.bitmap_engine->metadata_map_size * sizeof(SBlockMetadataNode*);
        metadata_memory += g_tmq_context.bitmap_engine->metadata_count * sizeof(SBlockMetadataNode);
        
        // 计算跳表索引内存使用
        if (g_tmq_context.bitmap_engine->time_index) {
            metadata_memory += skiplist_get_memory_usage(g_tmq_context.bitmap_engine->time_index);
        }
        if (g_tmq_context.bitmap_engine->wal_index) {
            metadata_memory += skiplist_get_memory_usage(g_tmq_context.bitmap_engine->wal_index);
        }
    }
    
    total_memory += bitmap_memory + metadata_memory;
    total_memory += g_tmq_context.events_processed * sizeof(SStorageEvent);
    
    g_tmq_context.observability_metrics.memory_usage_bytes = total_memory;
    g_tmq_context.observability_metrics.bitmap_memory_bytes = bitmap_memory;
    g_tmq_context.observability_metrics.metadata_memory_bytes = metadata_memory;
    
    // 更新时间戳
    g_tmq_context.observability_metrics.last_update_time = current_time;
    g_tmq_context.observability_metrics.uptime_seconds = 
        (current_time - g_tmq_context.start_time) / 1000;
    
    // 计算滞后指标 - 基于事件处理延迟估算
    if (g_tmq_context.events_processed > 0) {
        // 估算消费者滞后时间（基于事件处理速率）
        int64_t avg_processing_time = time_diff_ms > 0 ? 
            (g_tmq_context.events_processed * 1000) / time_diff_ms : 0;
        g_tmq_context.observability_metrics.consumer_lag_ms = avg_processing_time;
        
        // 估算Offset滞后数量（基于队列大小）
        g_tmq_context.observability_metrics.offset_lag = 
            g_tmq_context.observability_metrics.event_queue_size;
        
        // 计算处理延迟（基于事件时间戳）
        if (g_tmq_context.last_event_timestamp > 0) {
            g_tmq_context.observability_metrics.processing_delay_ms = 
                current_time - g_tmq_context.last_event_timestamp;
        } else {
            g_tmq_context.observability_metrics.processing_delay_ms = 0;
        }
    } else {
        g_tmq_context.observability_metrics.consumer_lag_ms = 0;
        g_tmq_context.observability_metrics.offset_lag = 0;
        g_tmq_context.observability_metrics.processing_delay_ms = 0;
    }
    
    pthread_mutex_unlock(&g_tmq_context.mutex);
}

// 获取可观测性指标
static int32_t tdengine_get_observability_metrics(void* engine, SObservabilityMetrics* metrics) {
    if (!g_tmq_context.initialized || !metrics) {
        return -1;
    }
    
    // 更新指标
    update_observability_metrics_internal();
    
    // 复制指标数据
    pthread_mutex_lock(&g_tmq_context.mutex);
    memcpy(metrics, &g_tmq_context.observability_metrics, sizeof(SObservabilityMetrics));
    pthread_mutex_unlock(&g_tmq_context.mutex);
    
    return 0;
}

// 重置统计信息
static int32_t tdengine_reset_stats(void* engine) {
    if (!g_tmq_context.initialized) {
        return -1;
    }
    
    pthread_mutex_lock(&g_tmq_context.mutex);
    
    // 重置基本统计
    g_tmq_context.events_processed = 0;
    g_tmq_context.events_dropped = 0;
    g_tmq_context.messages_consumed = 0;
    g_tmq_context.offset_commits = 0;
    
    // 重置重试统计
    g_tmq_context.connection_retries = 0;
    g_tmq_context.subscription_retries = 0;
    g_tmq_context.commit_retries = 0;
    
    // 重置错误统计
    g_tmq_context.parse_errors = 0;
    
    // 重置可观测性指标
    memset(&g_tmq_context.observability_metrics, 0, sizeof(SObservabilityMetrics));
    
    // 重置计数
    g_tmq_context.last_events_count = 0;
    g_tmq_context.last_messages_count = 0;
    g_tmq_context.last_bytes_count = 0;
    g_tmq_context.last_metrics_update = now_monotonic_ms();
    
    pthread_mutex_unlock(&g_tmq_context.mutex);
    
    return 0;
}

