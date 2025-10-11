#include "e2e_perf.h"

#include <stdio.h>
#include <time.h>

static uint64_t now_ms(void) {
#if defined(CLOCK_MONOTONIC)
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
#else
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
#endif
}

void e2e_perf_timer_start(E2EPerfTimer *timer) {
    if (!timer) return;
    timer->start_ms = now_ms();
    timer->end_ms = timer->start_ms;
}

void e2e_perf_timer_stop(E2EPerfTimer *timer) {
    if (!timer) return;
    timer->end_ms = now_ms();
}

double e2e_perf_elapsed_ms(const E2EPerfTimer *timer) {
    if (!timer) return 0.0;
    if (timer->end_ms < timer->start_ms) return 0.0;
    return (double)(timer->end_ms - timer->start_ms);
}

double e2e_perf_throughput_per_sec(uint64_t items, const E2EPerfTimer *timer) {
    double ms = e2e_perf_elapsed_ms(timer);
    if (ms <= 0.0) return 0.0;
    return (double)items / (ms / 1000.0);
}

void e2e_perf_print_summary(const char *label, uint64_t items, const E2EPerfTimer *timer) {
    double ms = e2e_perf_elapsed_ms(timer);
    double tps = e2e_perf_throughput_per_sec(items, timer);
    printf("[PERF] %s: items=%llu, elapsed=%.2f ms, throughput=%.2f ops/s\n",
           label ? label : "E2E", (unsigned long long)items, ms, tps);
}



