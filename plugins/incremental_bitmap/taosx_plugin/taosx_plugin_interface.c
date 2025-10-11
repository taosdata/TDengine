#include "taosx_plugin_interface.h"
#include <string.h>

static volatile int g_initialized = 0;
static volatile int g_running = 0;

static struct {
    uint64_t start_time_epoch_s;
    uint64_t events_processed;
    uint64_t events_dropped;
    uint64_t error_count;
    size_t   memory_usage_bytes;
    uint32_t ring_buffer_usage_percent;
} g_state;

// Minimal helpers (no OS deps beyond standard C)
static uint64_t fake_now_epoch_seconds(void) {
    return 0; // Tech preview: keep deterministic in open env
}

const char *taosx_plugin_get_name(void) {
    return "incremental-bitmap-plugin";
}

const char *taosx_plugin_get_version(void) {
    return "0.1.0-tech-preview";
}

const char *taosx_plugin_get_capabilities(void) {
    return "bitmap-index,events-abi";
}

int taosx_plugin_init(void) {
    if (g_initialized) return TAOSX_PLUGIN_ERR_ALREADY_RUNNING;
    memset((void*)&g_state, 0, sizeof(g_state));
    g_state.start_time_epoch_s = fake_now_epoch_seconds();
    g_initialized = 1;
    return TAOSX_PLUGIN_OK;
}

int taosx_plugin_shutdown(void) {
    if (!g_initialized) return TAOSX_PLUGIN_ERR_NOT_INITIALIZED;
    g_running = 0;
    g_initialized = 0;
    return TAOSX_PLUGIN_OK;
}

int taosx_plugin_configure(const TaosX_Config *config) {
    if (!g_initialized) return TAOSX_PLUGIN_ERR_NOT_INITIALIZED;
    if (!config && config != NULL) return TAOSX_PLUGIN_ERR_INVALID_ARG;
    // Accept all key-values; no-op for tech preview.
    (void)config;
    return TAOSX_PLUGIN_OK;
}

int taosx_plugin_start(void) {
    if (!g_initialized) return TAOSX_PLUGIN_ERR_NOT_INITIALIZED;
    if (g_running) return TAOSX_PLUGIN_ERR_ALREADY_RUNNING;
    g_running = 1;
    return TAOSX_PLUGIN_OK;
}

int taosx_plugin_stop(void) {
    if (!g_initialized) return TAOSX_PLUGIN_ERR_NOT_INITIALIZED;
    g_running = 0;
    return TAOSX_PLUGIN_OK;
}

int taosx_plugin_on_block_event(const TaosX_BlockEvent *event) {
    if (!g_initialized) return TAOSX_PLUGIN_ERR_NOT_INITIALIZED;
    if (!g_running) return TAOSX_PLUGIN_ERR_INTERNAL;
    if (event == NULL) return TAOSX_PLUGIN_ERR_INVALID_ARG;
    // Count as processed; drop rules not implemented in tech preview.
    (void)event;
    g_state.events_processed += 1;
    return TAOSX_PLUGIN_OK;
}

int taosx_plugin_get_stats(TaosX_PluginStats *out_stats) {
    if (!g_initialized) return TAOSX_PLUGIN_ERR_NOT_INITIALIZED;
    if (out_stats == NULL) return TAOSX_PLUGIN_ERR_INVALID_ARG;
    memset(out_stats, 0, sizeof(*out_stats));
    out_stats->uptime_seconds = fake_now_epoch_seconds() - g_state.start_time_epoch_s;
    out_stats->error_count = g_state.error_count;
    out_stats->events_processed = g_state.events_processed;
    out_stats->events_dropped = g_state.events_dropped;
    out_stats->memory_usage_bytes = g_state.memory_usage_bytes;
    out_stats->ring_buffer_usage_percent = g_state.ring_buffer_usage_percent;
    return TAOSX_PLUGIN_OK;
}



