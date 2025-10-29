#ifndef TAOSX_PLUGIN_INTERFACE_H
#define TAOSX_PLUGIN_INTERFACE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

// Tech Preview: Minimal, self-contained C ABI for future taosX integration.
// No dependency on enterprise headers; kept binary-stable and optional.

typedef struct {
    const char *key;
    const char *value;
} TaosX_KVPair;

typedef struct {
    const TaosX_KVPair *items;
    size_t count;
} TaosX_Config;

typedef struct {
    // Basic lifecycle/health
    uint64_t uptime_seconds;
    uint64_t error_count;
    // Basic throughput
    uint64_t events_processed;
    uint64_t events_dropped;
    // Memory and buffering
    size_t   memory_usage_bytes;
    uint32_t ring_buffer_usage_percent;
} TaosX_PluginStats;

typedef enum {
    TAOSX_PLUGIN_OK = 0,
    TAOSX_PLUGIN_ERR_INVALID_ARG = -1,
    TAOSX_PLUGIN_ERR_NOT_INITIALIZED = -2,
    TAOSX_PLUGIN_ERR_ALREADY_RUNNING = -3,
    TAOSX_PLUGIN_ERR_INTERNAL = -100
} TaosX_PluginCode;

// Version/capabilities are simple strings for forward compatibility.
// Example: "1.0.0" and "bitmap-index,wal-follow".
const char *taosx_plugin_get_name(void);
const char *taosx_plugin_get_version(void);
const char *taosx_plugin_get_capabilities(void);

// Lifecycle
int taosx_plugin_init(void);
int taosx_plugin_shutdown(void);

// Configuration and run controls
int taosx_plugin_configure(const TaosX_Config *config);
int taosx_plugin_start(void);
int taosx_plugin_stop(void);

// Event bridge (placeholder: taosX will pass storage events here)
typedef enum {
    TAOSX_EVENT_BLOCK_CREATE = 1,
    TAOSX_EVENT_BLOCK_UPDATE = 2,
    TAOSX_EVENT_BLOCK_FLUSH  = 3
} TaosX_EventType;

typedef struct {
    uint64_t block_id;
    int64_t  wal_offset;
    int64_t  timestamp_ns;
    TaosX_EventType event_type;
} TaosX_BlockEvent;

int taosx_plugin_on_block_event(const TaosX_BlockEvent *event);

// Stats
int taosx_plugin_get_stats(TaosX_PluginStats *out_stats);

#ifdef __cplusplus
}
#endif

#endif // TAOSX_PLUGIN_INTERFACE_H



