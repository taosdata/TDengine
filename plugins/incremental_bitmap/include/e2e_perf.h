#ifndef INCREMENTAL_BITMAP_E2E_PERF_H
#define INCREMENTAL_BITMAP_E2E_PERF_H

#include <stdint.h>

typedef struct E2EPerfTimer {
    uint64_t start_ms;
    uint64_t end_ms;
} E2EPerfTimer;

void e2e_perf_timer_start(E2EPerfTimer *timer);
void e2e_perf_timer_stop(E2EPerfTimer *timer);
double e2e_perf_elapsed_ms(const E2EPerfTimer *timer);
double e2e_perf_throughput_per_sec(uint64_t items, const E2EPerfTimer *timer);
void e2e_perf_print_summary(const char *label, uint64_t items, const E2EPerfTimer *timer);

#endif /* INCREMENTAL_BITMAP_E2E_PERF_H */



