#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "taosx_plugin_interface.h"

// Test helper macros
#define TEST_ASSERT(condition, message) \
    do { \
        if (!(condition)) { \
            fprintf(stderr, "❌ Test failed: %s\n", message); \
            return -1; \
        } else { \
            printf("✅ %s\n", message); \
        } \
    } while(0)

#define TEST_SUCCESS(message) \
    printf("✅ %s\n", message)

// Test functions
static int test_plugin_info(void);
static int test_plugin_lifecycle(void);
static int test_plugin_configuration(void);
static int test_plugin_events(void);
static int test_plugin_stats(void);

int main() {
    printf("==========================================\n");
    printf("   taosX Plugin Interface Test Suite\n");
    printf("==========================================\n\n");
    
    int total_tests = 0;
    int passed_tests = 0;
    int failed_tests = 0;
    
    // Test list
    struct {
        const char* name;
        int (*test_func)(void);
    } tests[] = {
        {"Plugin Information", test_plugin_info},
        {"Plugin Lifecycle", test_plugin_lifecycle},
        {"Plugin Configuration", test_plugin_configuration},
        {"Plugin Events", test_plugin_events},
        {"Plugin Statistics", test_plugin_stats}
    };
    
    total_tests = sizeof(tests) / sizeof(tests[0]);
    
    // Run tests
    for (int i = 0; i < total_tests; i++) {
        printf("Running test: %s\n", tests[i].name);
        printf("------------------------------------------\n");
        
        int result = tests[i].test_func();
        if (result == 0) {
            passed_tests++;
            TEST_SUCCESS(tests[i].name);
        } else {
            failed_tests++;
            printf("❌ Test failed: %s\n", tests[i].name);
        }
        printf("\n");
    }
    
    // Print results
    printf("==========================================\n");
    printf("           Test Results\n");
    printf("==========================================\n");
    printf("Total tests: %d\n", total_tests);
    printf("Passed: %d\n", passed_tests);
    printf("Failed: %d\n", failed_tests);
    printf("Success rate: %.1f%%\n", (double)passed_tests / total_tests * 100);
    
    if (failed_tests == 0) {
        printf("\n🎉 All tests passed!\n");
        return 0;
    } else {
        printf("\n❌ Some tests failed!\n");
        return 1;
    }
}

static int test_plugin_info(void) {
    const char* name = taosx_plugin_get_name();
    const char* version = taosx_plugin_get_version();
    const char* capabilities = taosx_plugin_get_capabilities();
    
    TEST_ASSERT(name != NULL, "Plugin name is not null");
    TEST_ASSERT(version != NULL, "Plugin version is not null");
    TEST_ASSERT(capabilities != NULL, "Plugin capabilities is not null");
    
    printf("  Plugin name: %s\n", name);
    printf("  Plugin version: %s\n", version);
    printf("  Plugin capabilities: %s\n", capabilities);
    
    return 0;
}

static int test_plugin_lifecycle(void) {
    // Test initialization
    int rc = taosx_plugin_init();
    TEST_ASSERT(rc == TAOSX_PLUGIN_OK, "Plugin initialization succeeds");
    
    // Test double initialization (should fail)
    rc = taosx_plugin_init();
    TEST_ASSERT(rc == TAOSX_PLUGIN_ERR_ALREADY_RUNNING, "Double initialization fails correctly");
    
    // Test shutdown
    rc = taosx_plugin_shutdown();
    TEST_ASSERT(rc == TAOSX_PLUGIN_OK, "Plugin shutdown succeeds");
    
    // Test shutdown after shutdown (should fail)
    rc = taosx_plugin_shutdown();
    TEST_ASSERT(rc == TAOSX_PLUGIN_ERR_NOT_INITIALIZED, "Shutdown after shutdown fails correctly");
    
    return 0;
}

static int test_plugin_configuration(void) {
    // Initialize plugin first
    int rc = taosx_plugin_init();
    TEST_ASSERT(rc == TAOSX_PLUGIN_OK, "Plugin initialization for config test");
    
    // Test configuration with NULL config (should succeed)
    rc = taosx_plugin_configure(NULL);
    TEST_ASSERT(rc == TAOSX_PLUGIN_OK, "NULL configuration is accepted");
    
    // Test configuration with valid config
    TaosX_KVPair pairs[] = {
        {"key1", "value1"},
        {"key2", "value2"}
    };
    TaosX_Config config = {
        .items = pairs,
        .count = 2
    };
    
    rc = taosx_plugin_configure(&config);
    TEST_ASSERT(rc == TAOSX_PLUGIN_OK, "Valid configuration is accepted");
    
    // Cleanup
    taosx_plugin_shutdown();
    
    return 0;
}

static int test_plugin_events(void) {
    // Initialize plugin
    int rc = taosx_plugin_init();
    TEST_ASSERT(rc == TAOSX_PLUGIN_OK, "Plugin initialization for event test");
    
    // Test event handling without starting (should fail)
    TaosX_BlockEvent event = {
        .block_id = 123,
        .wal_offset = 456,
        .timestamp_ns = 789,
        .event_type = TAOSX_EVENT_BLOCK_CREATE
    };
    
    rc = taosx_plugin_on_block_event(&event);
    TEST_ASSERT(rc == TAOSX_PLUGIN_ERR_INTERNAL, "Event handling without start fails correctly");
    
    // Start plugin
    rc = taosx_plugin_start();
    TEST_ASSERT(rc == TAOSX_PLUGIN_OK, "Plugin start succeeds");
    
    // Test event handling
    rc = taosx_plugin_on_block_event(&event);
    TEST_ASSERT(rc == TAOSX_PLUGIN_OK, "Event handling succeeds");
    
    // Test NULL event (should fail)
    rc = taosx_plugin_on_block_event(NULL);
    TEST_ASSERT(rc == TAOSX_PLUGIN_ERR_INVALID_ARG, "NULL event handling fails correctly");
    
    // Stop plugin
    rc = taosx_plugin_stop();
    TEST_ASSERT(rc == TAOSX_PLUGIN_OK, "Plugin stop succeeds");
    
    // Cleanup
    taosx_plugin_shutdown();
    
    return 0;
}

static int test_plugin_stats(void) {
    // Initialize and start plugin
    int rc = taosx_plugin_init();
    TEST_ASSERT(rc == TAOSX_PLUGIN_OK, "Plugin initialization for stats test");
    
    rc = taosx_plugin_start();
    TEST_ASSERT(rc == TAOSX_PLUGIN_OK, "Plugin start for stats test");
    
    // Process some events to generate stats
    TaosX_BlockEvent event = {
        .block_id = 1,
        .wal_offset = 100,
        .timestamp_ns = 1000,
        .event_type = TAOSX_EVENT_BLOCK_CREATE
    };
    
    for (int i = 0; i < 5; i++) {
        rc = taosx_plugin_on_block_event(&event);
        TEST_ASSERT(rc == TAOSX_PLUGIN_OK, "Event processing for stats");
    }
    
    // Get stats
    TaosX_PluginStats stats;
    rc = taosx_plugin_get_stats(&stats);
    TEST_ASSERT(rc == TAOSX_PLUGIN_OK, "Stats retrieval succeeds");
    
    printf("  Events processed: %lu\n", stats.events_processed);
    printf("  Events dropped: %lu\n", stats.events_dropped);
    printf("  Error count: %lu\n", stats.error_count);
    printf("  Memory usage: %zu bytes\n", stats.memory_usage_bytes);
    printf("  Ring buffer usage: %u%%\n", stats.ring_buffer_usage_percent);
    
    // Test NULL stats (should fail)
    rc = taosx_plugin_get_stats(NULL);
    TEST_ASSERT(rc == TAOSX_PLUGIN_ERR_INVALID_ARG, "NULL stats retrieval fails correctly");
    
    // Cleanup
    taosx_plugin_stop();
    taosx_plugin_shutdown();
    
    return 0;
}
