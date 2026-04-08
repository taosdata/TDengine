#include <stdint.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <string.h>
#include <sys/time.h>

#include "perftestcommon.h"

#if defined(__cplusplus)
extern "C" {
#endif

int64_t start_ts = 1433955000000;
const int32_t MAX_SAMPLING_CNT = 5000;

/**
 * record current time in ms
 * @return
 */
int64_t get_ts_in_ms() {
    struct timeval st;
    gettimeofday(&st, NULL);
    int64_t sts = st.tv_sec * 1000 + st.tv_usec / 1000;
    return sts;
}

/**
 * record the start of sampling operation
 * @param s
 */
sampling_ele *record_sample_start() {
    sampling_ele *s = (sampling_ele* )taosMemoryMalloc(sizeof(sampling_ele));
    if (s == 0) {
        perror("failed to allocation memory");
    }

    s->start_time = get_ts_in_ms();
    return s;
}

void record_sampling_end(sampling_ele *s, int32_t recnum, int32_t recsize) {
    s->end_time = get_ts_in_ms();
    s->rec_count = recnum;
    s->rec_size = s->rec_count * recsize;
}

void dump_sampling_record_to_file(sampling_ele **el, int32_t cnt,
                                  char *output_file_path);

void new_timestamp(char *ts) {
    time_t t = start_ts / 1000; //获取目前秒时间
    struct tm *local = localtime(&t); //转为本地时间

    char buf[26] = {0};
    strftime(buf, 26, "%Y-%m-%d %H:%M:%S", local);

    sprintf(ts, "%s.%03d", buf, (int32_t)(start_ts % 1000));
    start_ts++;
}

entry *extract_entry(char *line) {
    int32_t len = strlen(line);
    int32_t start = 0;
    int32_t end = 0;

    int32_t part = 0;

    entry *e = (entry *) taosMemoryMalloc(sizeof(entry));
    if (e == 0) {
        perror("out of memory in loading raw data into memory");
        return 0;
    }

    char t[128] = {0};
    for (int32_t i = 0; i < len; ++i) {
        if (line[i] == ',' || line[i] == '\n') {
            end = i;
            strncpy(t, &line[start], end - start);
            t[end - start] = 0;
            start = end + 1;

            if (part == 1) {
//                strcpy(e->ts, t);
                new_timestamp(e->ts);
            } else if (part == 2) {
                e->lat = atof(t);
            } else if (part == 3) {
                e->lon = atof(t);
            } else if (part == 4) {
                e->direction = atoi(t);
            } else {
                strncpy(e->tag, t, 7);
            }
            part++;
        }
    }
    e->next = 0;
    return e;
}

void load_data_from_file(char *file_path, entry_list *lists) {
    FILE *f = fopen(file_path, "r");
    if (f == 0) {
        return;
    }

    char line[256] = {0};
    char *ret = 0;

    while ((ret = fgets(line, 256, f)) != 0) {
        entry *el = extract_entry(line);
        if (lists->data == 0) {
            lists->data = el;
            lists->tail = el;
        } else {
            lists->tail->next = el;
            lists->tail = el;
        }
        lists->cur_len++;
    }
    fclose(f);
}

void load_data_into_mem_impl(char *root_dir, entry_list *entries) {
    DIR *dir;
    if ((dir = opendir(root_dir)) == 0) {
        perror("Open dir error...");
        exit(1);
    }

    struct dirent *ptr;
    char full_path[1024] = {0};

    while ((ptr = readdir(dir)) != 0) {
        //ignore current directory denote
        if (strncmp(ptr->d_name, ".", 1) == 0) {
            continue;
        }

        if (ptr->d_type == 8) {
            //load data into memory
            strcpy(full_path, root_dir);
            strcat(full_path, "/");
            strcat(full_path, ptr->d_name);
            printf("%s\n", full_path);

            load_data_from_file(full_path, entries);
        } else if (ptr->d_type == 4) {
            // recursive seek to directory
            strcpy(full_path, root_dir);
            strcat(full_path, "/");
            strcat(full_path, ptr->d_name);

            load_data_into_mem_impl(full_path, entries);
        } else {
            continue;
        }
    }
}

entry_list *load_all_data_into_mem(char *root_dir) {
    if (root_dir == 0) {
        printf("error in load data from directory");
        exit(1);
    }

    entry_list *entries = (entry_list *) taosMemoryMalloc(sizeof(entry_list));
    entries->cur_len = 0;
    entries->data = 0;
    entries->tail = 0;

    load_data_into_mem_impl(root_dir, entries);

    return entries;
}

void release_entries(entry_list *entries) {
    if (entries == 0) {
        return;
    }

    for (int32_t i = 0; i < entries->cur_len; ++i) {
        if (entries->data != 0) {
            entry *el = entries->data;
            entries->data = entries->data->next;
            taosMemoryFree(el);
            entries->cur_len--;
        }
    }

    taosMemoryFree(entries);
}

/**
 * check timestamp to make sure that the prev entry is smaller than
 * the later one
 * @param entries
 */
void check_ts_inc(entry_list *entries) {
    entry *st = 0;
    entry *cur = entries->data;

    for (int32_t i = 1; i < entries->cur_len; ++i) {
        st = cur;
        cur = cur->next;
        if (strncmp(st->ts, cur->ts, strlen(st->ts)) < 0) {
            printf("prev: %s - cur: %s\n", st->ts, cur->ts);
        }

    }
}

void dump_sampling_record_to_file(sampling_ele **el, int32_t cnt,
                                  char *output_file_path) {
    FILE *fout = fopen(output_file_path, "w");
    if (fout == 0) {
        char ret[128] = {0};
        sprintf(ret, "open dump file %s failed", output_file_path);
        perror(ret);
    }

    char buf[1024] = {0};
    for (int32_t i = 0; i < cnt; ++i) {
        char *ret = rec_to_string(el[i], buf);
        fputs(ret, fout);
    }

    fclose(fout);
}

int32_t rec_size(entry *el) {
    if (el == NULL) {
        return 0;
    }

    return sizeof(el->direction) + strlen(el->ts) + sizeof(el->lat)
           + sizeof(el->lon) + strlen(el->tag);
}

/**
 * [start_time, record, elapsed time, rec/sec.]
 * @param el
 * @param buf
 * @return
 */
char *rec_to_string(sampling_ele *el, char *buf) {
    char szBuffer[64] = {0};
    const char *pFormat = "%Y-%m-%d %H:%M:%S";
    time_t t = el->start_time / 1000;
    struct tm *local = localtime(&t);

    strftime(szBuffer, 64, pFormat, local);

    double throughput = ((double) el->rec_count * 1000) / (el->end_time - el->start_time);
    sprintf(buf, "%s,%d,%.4f,%.4f,%.4f ms\n", szBuffer, el->rec_count,
            ((double)(el->end_time - el->start_time)) / 1000,
            throughput,
            1000/throughput);
    return buf;
}

void get_current_ts(char* buf, int32_t len) {
    time_t t = time(NULL);
    struct tm *local = localtime(&t);
    strftime(buf, len, "%Y_%m_%d_%H_%M_%S", local);
};

entry_list* load_all_data_into_mem_rv(int32_t count) {
    entry_list* el = (entry_list*)taosMemoryMalloc(sizeof(entry_list));
    el->cur_len = count;

    el->tail = NULL;
    el->data = NULL;

    srand(time(NULL));

    for(int32_t i=0; i<el->cur_len; ++i) {
        entry* ele = (entry*) taosMemoryMalloc(sizeof(entry));
        ele->direction = i;
        ele->next = NULL;
        ele->lat = (float)rand()/1000;
        ele->lon = (float)rand()/1000;
        sprintf(ele->tag, "%d_@!", i);

        sprintf(ele->ts, "2017-01-%02d 19:%02d:%02d", i%30, i%24, i%54);

        if (el->data == NULL) {
            el->data = ele;
            el->tail = ele;
        } else {
            el->tail->next = ele;
            el->tail = ele;
        }
    }

    return el;
}

#if defined(__cplusplus)
}
#endif
