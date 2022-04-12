//gcc -g -Wall -fPIC -Wno-char-subscripts -malign-double -malign-stringops -D_REENTRANT -DLINUX -Iinc -I../inc -I../util/inc -I../taos/inc -I../client/inc -DLZ4_DISABLE_DEPRECATE_WARNINGS -I./perftest/inc perftest/taosperf.c perftest/perftestcommon.c -o ../../build/bin/taosperf ../../build/lib/libtaos.a -lpthread -lm
#include <stdint.h>
#include <stdbool.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>
#include <sys/syscall.h>
#include <assert.h>

#include "taos.h"
#include "perftestcommon.h"

typedef struct {
    int id;
    int64_t timeStamp;
    int insert_cnt;
    int total_cnt;
    TAOS *conn;
    int32_t interval;
    sampling_ele **samples;
    int32_t sample_cnt;
    char outputdir[128];
    bool isDone;
    char tbname[64];
} STable;

char s48[49] = {0};
char s96[97] = {0};
char s192[193] = {0};
char s384[385] = {0};

void check_env(TAOS *conn, const char *db_name, const char *table_name,
                bool clean_env) {
    char sql[128] = {0};
    // make sure db and table exists
    // create without check its existence.
    snprintf(sql, sizeof(sql) / sizeof(sql[0]), "create database %s", db_name);
    taos_query(conn, sql);

    snprintf(sql, sizeof(sql) / sizeof(sql[0]), "use %s", db_name);
    int32_t ret = taos_query(conn, sql);
    if (ret != 0) {
        fprintf(stderr, "%s\n", taos_errstr(conn));
        exit(-1);
    }

    sprintf(sql, "create table %s(tk timestamp, direction int, a bigint, b bigint, c bigint, t binary(100))", table_name);

    if (taos_query(conn, sql)!= 0) {
        fprintf(stderr, "%s\n", taos_errstr(conn));
    }
}

void get_error(TAOS *con) {
    fprintf(stderr, "TSDB error: %s\n", taos_errstr(con));
    taos_close(con);
}

/**
 * synchronized insert record into db in one-by-one means
 * @param conn
 * @param tname
 * @param entries
 * @param sample_interval : 0=no sampling
 */
void sync_insert_operation(TAOS *conn, const char *tname, entry_list *entries,
                           int32_t total_len, int32_t sample_interval,
                           const char *sample_output_dir) {
    char qstr[256] = {0};

    double st = get_ts_in_ms();

    // total sampling count during execution
    int32_t total_sample_rec_cnt =
            (total_len + sample_interval - 1) / sample_interval;

    // prepare sampling record array
    sampling_ele **sample_recs = taosMemoryMalloc(sizeof(void*) * total_sample_rec_cnt);
    int32_t sample_cnt = 0;

    sampling_ele *r1 = record_sample_start();
    sample_recs[sample_cnt++] = r1;

    for(int32_t i=0; i<entries->cur_len;++i) {
        sprintf(qstr, "insert into %s values(%ld, %d, %ld, %ld, %ld, %s)", tname, start_ts++, i,
        i+10L, i+1000L, i+9999999L, "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz");
        if (taos_query(conn, qstr) < 0) {
            get_error(conn);
        }
    }

    double ed = get_ts_in_ms();

    printf("total consumed time is: %.4f sec.\n", ((double) (ed - st)) / 1000);
    printf("average throughput is:%.4f qps\nlatency is: %.4f\n",
           ((double) total_len) * 1000 / (ed - st),
           ((double) (ed - st)) / (total_len));

    char szBuffer[64] = {0};
    get_current_ts(szBuffer, sizeof(szBuffer) / sizeof(szBuffer[0]));

    int32_t id = syscall(SYS_gettid);

    char full_path[512] = {0};
    sprintf(full_path, "%s/simple_insert_%d_%s.txt",
            sample_output_dir, id, szBuffer);

    dump_sampling_record_to_file(sample_recs, sample_cnt, full_path);
}

/**
 * execution arguments:
 * [operation] = syn
 *
 * @param argc
 * @param argv
 */
//
//typedef struct {
//    int32_t insert_cnt;
//    int32_t total_cnt;
//    TAOS *conn;
//
//    int64_t ts;
//    char tag[8];
//    float lat;
//    float lon;
//    int16_t direction;
//
//    void *table;
//    int64_t start;
//} async_status;

void get_sql(int32_t blk_size, char *sql_str) {
    char *tq = "values (%s, B1234, 12.1, 1.1, 10)";
    char sql[256] = {0};

    char tts[30] = {0};
    new_timestamp(tts);

    snprintf(sql, 256, tq, tts);

    int32_t len = strlen(sql);

    char *end = sql_str + blk_size;

    while (sql_str - end > len) {
        strcpy(sql_str, sql);
        memset(sql, 0, sizeof(sql));
        sql_str += len;

        new_timestamp(tts);
        snprintf(sql, 256, tq, tts);
    }

//    return start;
}

void get_sql_s(char *sql) {
    char *tq = "values (%s, B1234, 12.1, 1.1, 9) values (%s, AAAA, 12.1, 99, 10)";
//    char sql[256] = {0};

    char tts[30] = {0};
    new_timestamp(tts);

    char tte[30] = {0};
    new_timestamp(tte);

    sprintf(sql, tq, tts, tte);
}

void get_sql_t(char *sql, char* tbName) {
    char *tq = "insert into %s values (%lld, B1234, 12.1, 1.1, 9) "
            "values(%lld, AAAA, 11, 9.1, 72) values(%lld BBBB, 71.1, 42.1, 88) "
            "values(%lld, CCCC, 4.678,12.11,32);";

    sprintf(sql,  tq, tbName, start_ts, start_ts + 1, start_ts + 2,
            start_ts + 3);
    start_ts += 4;
}

void ts_async_insert_cb(void *param, TAOS_RES* tres, int code) {
    STable *p = (STable *) param;
    if (code > 0) {
        p->insert_cnt += 4;
        if (p->insert_cnt < p->total_cnt) {

            int64_t seg = get_ts_in_ms();
            if (p->insert_cnt % p->interval == 0) {
                record_sampling_end(p->samples[p->sample_cnt - 1],
                                    p->interval, 16);

                p->samples[p->sample_cnt++] = record_sample_start();
            }

            char *tq = "insert into %s values (%lld, first, %d, 1.1, 10) "
                    "values(%lld, second, %d, 8.7, 44) "
                    "values(%lld, third, %d, 4.09, 2) "
                    "values(%lld, fourth, %d, 1.1, 1) ";

            char sql[1024] = {0};
            sprintf(sql, tq, p->tbname, start_ts, p->insert_cnt, start_ts + 1,
                    p->insert_cnt + 1, start_ts + 2, p->insert_cnt + 2,
                    start_ts + 3, p->insert_cnt + 3);//,
            start_ts += 4;
            taos_query_a(p->conn, sql, ts_async_insert_cb, param);
        } else {
            int64_t et = get_ts_in_ms();

            char ret_msg[1024] = {0};
            sprintf(ret_msg, "%.6f seconds to insert %d data points\n", ((double) et - p->timeStamp) / 1000,
                    p->total_cnt);

            sprintf(ret_msg + strlen(ret_msg), "average throughput is:%.4f qps\nlatency is: %.4f\n",
                    ((double) p->total_cnt) * 1000 / (et - p->timeStamp),
                    ((double) (et - p->timeStamp)) / (p->total_cnt));

            printf("%s", ret_msg);
            record_sampling_end(p->samples[p->sample_cnt - 1], p->interval, 16);

            char szBuffer[64] = {0};
            get_current_ts(szBuffer, (int32_t)sizeof(szBuffer)/sizeof(szBuffer[0]));

            char full_path[512] = {0};
            int32_t id = syscall(SYS_gettid);

            sprintf(full_path, "%s/async_insert_%d_%s.txt", p->outputdir, id, szBuffer);
            dump_sampling_record_to_file(p->samples, p->sample_cnt, full_path);

            FILE *f = fopen(full_path, "r+");
            if (f != NULL) {
                fseek(f, 0, SEEK_END);
                fwrite(ret_msg, 1, strlen(ret_msg), f);
                fclose(f);
            }

            p->isDone = true;
        }
    } else if (code == 0) {
        const char *errmsg = taos_errstr(p->conn);
        printf("failed to insert, code:%d, %s, tableid: %d\n", code, errmsg, p->id);
        p->isDone = true;
    } else { // code < 0
        const char *errmsg = taos_errstr(p->conn);
        printf("failed to insert, code:%d, %s, rowsInserted:%d\n", code, errmsg, p->insert_cnt);
        p->isDone = true;
    }
}

/**
 *
 * @param conn
 * @param tname_prefix
 * @param sample_interval
 * @param rec_size
 * @param one_insertion_row_cnt 每次插入数据使用的bulk count block size
 * @param table_num
 * @param rec_each_table
 */
void async_multi_table_insert_operation(
        TAOS *conn, const char *db_name, char *tname_prefix,
        int32_t table_num, int32_t rec_each_table, const char *outputdir) {
    char qstr[1024] = {0};
    sprintf(qstr, "create database %s", db_name);
    taos_query(conn, qstr);

    sprintf(qstr, "use %s", db_name);
    if (taos_query(conn, qstr) != 0) {
        get_error(conn);
        exit(-1);
    }

    for (int32_t i = 0; i < table_num; ++i) {
        sprintf(qstr, "drop table %s%d", tname_prefix, i);
        taos_query(conn, qstr);

        sprintf(qstr, "create table %s%d(ts timestamp, tag binary(12), lat float, "
                        "lon float, direction int)",
                tname_prefix, i);
        if (taos_query(conn, qstr) != 0) {
            get_error(conn);
            exit(-1);
        }
    }

    printf("create %d tables completed!\n", table_num);

    STable *tableList = (STable *) taosMemoryMalloc(sizeof(STable) * table_num);
    int32_t total_sample_rec_cnt = rec_each_table / 1000;
    assert(total_sample_rec_cnt > 0);

    char table_name[64] = {0};
    for (int32_t i = 0; i < table_num; ++i) {
        sprintf(table_name, "%s%d", tname_prefix, i);
        strcpy(tableList[i].tbname, table_name);
        tableList[i].id = i;
        tableList[i].timeStamp = get_ts_in_ms();
        tableList[i].insert_cnt = 0;
        tableList[i].sample_cnt = 0;
        tableList[i].total_cnt = rec_each_table;
        tableList[i].interval = 10000;
        tableList[i].conn = conn;
        tableList[i].isDone = false;
        strcpy(tableList[i].outputdir, outputdir);

        tableList[i].samples = taosMemoryMalloc(sizeof(void*) * total_sample_rec_cnt);

        sampling_ele *r1 = record_sample_start();
        tableList[i].samples[tableList[i].sample_cnt] = r1;

        tableList[i].sample_cnt++;
    }

    //load from file
    char buf[512] = {0};

    for (int32_t i = 0; i < table_num; ++i) {
        get_sql_t(buf, table_name);
        taos_query_a(conn, buf, ts_async_insert_cb, &tableList[i]);
    }

    printf("wait for the finish of insert...\n");

    while(1) {
        bool all_done = true;
        for(int32_t i=0; i<table_num; ++i) {
            all_done &= tableList[i].isDone;
        }

        if (all_done) {
            break;
        }

        sleep(1);
    }
    printf("all insertion done!\n");
    taosMemoryFree(tableList);
}

/**
 * random read test
 * 1. load client count from config
 * 2. read the number of cpu cores on current pc
 * 3. waiting for the latest start time to launch all threads
 * 4. record total elapsed time
 */
void simple_read_operation(TAOS *conn, char *tname, char *outputdir) {
    char qstr[128] = {0};
    sprintf(qstr, "select * from %s", tname);

    int32_t numOfRows;
    int64_t st = get_ts_in_ms();

    if (taos_query(conn, qstr)) {
        printf("failed to select, reason:%s\n", taos_errstr(conn));
    }

    TAOS_RES* result = taos_use_result(conn);
    if (result == NULL) {
        printf("failed to get result, reason:%s\n", taos_errstr(conn));
    }

    TAOS_ROW row;
    numOfRows = 0;
    char temp[256] = {0};
    int num_fields = taos_field_count(conn);
    TAOS_FIELD *fields = taos_fetch_fields(result);

    int32_t ct = 100000;

    sampling_ele **sample_recs = taosMemoryMalloc(sizeof(void*) * 1000);
    int32_t sample_cnt = 0;

    sampling_ele *r1 = record_sample_start();
    sample_recs[sample_cnt++] = r1;

    while ((row = taos_fetch_row(result))) {
        numOfRows++;
        temp[0] = 0;
        if (numOfRows % ct == 0 && sample_cnt <= 1000) {
            printf("record %d\n", numOfRows);
            record_sampling_end(sample_recs[sample_cnt - 1],
                                ct, 16);
            sample_cnt++;
            if (sample_cnt <= 1000) {
                sample_recs[sample_cnt - 1] = record_sample_start();
            } else {
                sample_cnt = 1000;
            }
        }
    }

    if (numOfRows == 0) {
        record_sampling_end(sample_recs[sample_cnt - 1],
                            numOfRows, 16);
    }

    int64_t et = get_ts_in_ms();
    printf("%ld mseconds to retrieve %d data points\n", (et - st), numOfRows);

    char szBuffer[64] = {0};
    get_current_ts(szBuffer, sizeof(szBuffer) / sizeof(szBuffer[0]));


    int32_t id = syscall(SYS_gettid);
    char full_path[512] = {0};
    sprintf(full_path, "%s/simple_read_%d_%s.txt",
            outputdir, id, szBuffer);

    dump_sampling_record_to_file(sample_recs, sample_cnt, full_path);
    taos_free_result(result);
}

int executeSQL(TAOS* conn, char* sql, int32_t c) {
    printf("execution sql: \n%s\n", sql);

    if (taos_query(conn, sql)) {
        printf("failed to execute \"%s\", reason:%s\n", sql, taos_errstr(conn));
        return 0;
    }

    printf("taos_query end!\n");
    void* result = taos_use_result(conn);
    if (result == NULL) {
        printf("failed to get result, reason:%s\n", taos_errstr(conn));
    }
    int32_t numOfRows = 0;

    TAOS_ROW row;
    char temp[256] = {0};
    int num_fields = taos_field_count(conn);
    printf("waiting for enter\n");

    TAOS_FIELD *fields = taos_fetch_fields(result);

    printf("taos_fetch_fields\n");
    while ((row = taos_fetch_row(result))) {
        printf("taos_fetch_row success!\n");

        numOfRows++;
        temp[0] = 0;
        for (int i = 0; i < num_fields; i++) {
            switch (fields[i].type) {
                case TSDB_DATA_TYPE_TINYINT:
                    sprintf(temp + strlen(temp), "%d ", *((char *) row[i]));
                    break;
                case TSDB_DATA_TYPE_SMALLINT:
                    sprintf(temp + strlen(temp), "%d ", *((short *) row[i]));
                    break;
                case TSDB_DATA_TYPE_INT:
                    sprintf(temp + strlen(temp), "%d ", *((int *) row[i]));
                    break;
                case TSDB_DATA_TYPE_BIGINT:
                    sprintf(temp + strlen(temp), "%ld ", *((long *) row[i]));
                    break;
                case TSDB_DATA_TYPE_FLOAT:
                    sprintf(temp + strlen(temp), "%f ", *((float *) row[i]));
                    break;
                case TSDB_DATA_TYPE_DOUBLE:
                    sprintf(temp + strlen(temp), "%lf ", *((double *) row[i]));
                    break;
                case TSDB_DATA_TYPE_BINARY:
                    sprintf(temp + strlen(temp), "%s ", (char *) row[i]);
                    break;
                case TSDB_DATA_TYPE_TIMESTAMP:
                    sprintf(temp + strlen(temp), "%ld ", *((long *) row[i]));
                    break;
                default:
                    break;
            }
        }
        printf("%d: %s\n", numOfRows, temp);
    }

    taos_free_result(result);

    return 0;
}

int main(int argc, char **argv) {
    if (argc < 5) {
        printf("usage: \ncfg async db_name table_name table_cnt recs_per_table outputdir\n"
                       "cfg sync db_name table_name recs_of_table outputdir\n"
                       "cfg read db_name table_name outputdir\n");
        exit(-1);
    }

    // check running environment
    taos_options(TSDB_OPTION_CONFIGDIR, argv[1]);
    taos_init();

    TAOS* conn = taos_connect("192.168.0.1", "root", "taosdata", NULL, 0);
    if (conn == NULL) {
        printf("Failed to connect to DB, reason:%s\n", taos_errstr(conn));
        exit(-1);
    }

    char *table_name = argv[4];
    char *db_name = argv[3];

    check_env(conn, db_name, table_name, false);

    printf("check environment complete!\n");
    char *output_dir = ".";

    if (strncasecmp(argv[2], "async", strlen("async")) == 0) {
        int32_t table_cnt = atoi(argv[5]);
        if (table_cnt == 0) {
            table_cnt = 100;
            printf("set default tables 100.\n");
        }

        int32_t recs = atoi(argv[6]);
        if (recs < 1000) {
            printf("no less than 1000 per table\n");
            exit(-1);
        }
        printf("total inserted records: %d\n", table_cnt * recs);

        db_name = argv[3];
        output_dir = argv[7];

        async_multi_table_insert_operation(conn, db_name, table_name, table_cnt, recs, output_dir);
    } else {
        table_name = argv[4];

        if (strncasecmp(argv[2], "sync", strlen("sync")) == 0) {

            entry_list *el = load_all_data_into_mem_rv(atoi(argv[5]));
            output_dir = argv[6];

            sync_insert_operation(conn, table_name, el, el->cur_len, 10000, output_dir);
        } else if (strncasecmp(argv[2], "read", strlen("read")) == 0) {
            output_dir = argv[5];
            simple_read_operation(conn, table_name, output_dir);
        }
    }
    taos_close(conn);
    return 0;
}