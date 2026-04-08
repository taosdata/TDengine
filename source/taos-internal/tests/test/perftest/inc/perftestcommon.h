#ifndef TBASE_PERFTEST_H
#define TBASE_PERFTEST_H

#include <stdint.h>

#if defined(__cplusplus)
extern "C" {
#endif

/**
 * entry for insert to database
 */
typedef struct entry {
    char ts[26];//2017-01-02 12:22:12.123
    char tag[80];
    int16_t direction;
    float lat;
    float lon;
    struct entry *next;
} entry;

typedef struct {
    int32_t cur_len;
    entry *tail;
    entry *data;
} entry_list;

typedef struct {
    int32_t rec_count;
    int64_t start_time;
    int64_t end_time;

    int32_t rec_size;
} sampling_ele;

extern const int32_t MAX_SAMPLING_CNT;
extern int64_t start_ts;

int64_t get_ts_in_ms();

sampling_ele *record_sample_start();

void record_sampling_end(sampling_ele *s, int32_t recnum, int32_t recsize);

entry_list *load_all_data_into_mem(char *root_dir);

void release_entries(entry_list *entries);

void check_ts_inc(entry_list *entries);

void dump_sampling_record_to_file(sampling_ele **el, int32_t cnt,
                                  char *output_file_path);

int32_t rec_size(entry *el);

char *rec_to_string(sampling_ele *el, char *buf);

void new_timestamp(char *);

void get_current_ts(char *buf, int32_t len);

entry_list* load_all_data_into_mem_rv(int32_t count);


#if defined(__cplusplus)
}
#endif

#endif //TBASE_PERFTEST_H_H
