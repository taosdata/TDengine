#ifndef INCREMENTAL_BITMAP_E2E_CONSISTENCY_H
#define INCREMENTAL_BITMAP_E2E_CONSISTENCY_H

#include <stddef.h>
#include <stdint.h>

typedef struct E2EConsistencyConfig {
    const char *data_path;
    const char *snapshot_path;
    const char *recovery_path;
} E2EConsistencyConfig;

typedef struct E2EConsistencyReport {
    uint64_t snapshots_checked;
    uint64_t recovery_points_checked;
    uint64_t data_blocks_compared;
    uint64_t consistency_errors;
    uint64_t missing_files;
    double validation_time_ms;
    char details[512];
} E2EConsistencyReport;

int e2e_validate_consistency(const E2EConsistencyConfig *config,
                             char *error_buffer,
                             size_t error_buffer_length);

int e2e_validate_consistency_detailed(const E2EConsistencyConfig *config,
                                      E2EConsistencyReport *report);

int e2e_compare_snapshot_data(const char *snapshot_file,
                              const char *reference_file,
                              uint64_t *blocks_compared,
                              uint64_t *mismatches);

void e2e_print_consistency_report(const E2EConsistencyReport *report);

#endif /* INCREMENTAL_BITMAP_E2E_CONSISTENCY_H */


