#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <float.h>
#include <wordexp.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <strings.h>
#include <sys/time.h>
#include <pthread.h>
#include <stdbool.h>

#include "taos.h"

typedef struct {
    int     threadid;
    int     start;
    int     end;
    char*   startTime;
    char*   endTime;
    char*   db;
    bool   writeToFile;
} TableRange;

int64_t getTimestamp() {
    struct timeval systemTime;
    gettimeofday(&systemTime, NULL);
    return (int64_t)systemTime.tv_sec * 1000L + (uint64_t)systemTime.tv_usec / 1000;
}

int32_t fetchData(void* result, int32_t num_fields, TAOS_FIELD* fields) {
    TAOS_ROW row = NULL;

    int64_t numOfRows = 0;
    int64_t start = getTimestamp();
    char temp[1024] = {0};

    while ((row = taos_fetch_row(result))) {
        temp[0] = 0;
        numOfRows++;

        if (numOfRows == 20) {//output the 20-th row
            taos_print_row(temp, row, fields, num_fields);
            printf("%s\n", temp);
        }
    }
    printf("total elapsed time:%ld ms ,%d\n", getTimestamp() - start, numOfRows);
}

int32_t executeSQL(TAOS* conn, char* sql) {
    int32_t ret = taos_query(conn, sql);
    if (ret != 0) {
        printf("failed to execute %s, reason:%s\n", sql, taos_errstr(conn));
        return -1;
    }

    void *result = taos_use_result(conn);
    if (result == NULL) {
        printf("failed to get result, reason:%s\n", taos_errstr(conn));
        return -1;
    }

    int num_fields = taos_field_count(conn);
    TAOS_FIELD *fields = taos_fetch_fields(result);

    fetchData(result, num_fields, fields);

    taos_free_result(result);
    return 0;
}

int oneLoader(void* param) {
    TAOS *conn = taos_connect("127.0.0.1", "root", "taosdata", 0, 0);
    if (conn == NULL) {
        printf("Failed to connect to DB, reason:%s\n", taos_errstr(conn));
        exit(-1);
    }

    char sql[1024] = {0};

    TableRange *range = (TableRange *) param;

    sprintf(sql, "use %s", range->db);
    executeSQL(conn, sql);
    memset(sql, 0, sizeof(sql)/sizeof(sql[0]));

    int64_t start = getTimestamp();

    for (int32_t i = range->start; i < range->end; ++i) {
        sprintf(sql, "select * from device%d where receive_time>= '%s 0:0:0' and receive_time<'%s 0:0:0'",
                i, range->startTime, range->endTime);

        printf("%s\n", sql);
        executeSQL(conn, sql);

        memset(sql, 0, sizeof(sql) / sizeof(sql[0]));
    }

    int64_t elapsed = getTimestamp() - start;
    printf("Total elapsed time: %ld ms\n", elapsed);

    taos_close(conn);
    return 0;
}

int main(int argc, char **argv) {
    if (argc < 5) {
        return -1;
    }

    taos_options(TSDB_OPTION_CONFIGDIR, argv[1]);
    taos_init();

    char* db = argv[2];
    int numOfThreads = atoi(argv[3]);
    char* startTime = argv[4];
    char* endTime = argv[5];

    pthread_attr_t thattr;
    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

    pthread_t* threadId = malloc(sizeof(pthread_t)*numOfThreads);
    TableRange* params = calloc(1, sizeof(TableRange)*numOfThreads);

    //172.21.0.4
    int32_t startTableIdArray[5] = {0, 3, 9, 6, 12};

    //172.21.0.14
//    int32_t startTableIdArray[5] = {1, 4, 7, 10, 13};

    //172.21.0.2
//    int32_t startTableIdArray[5] = {2, 5, 8, 11, 14};

    for (int i = 0; i < numOfThreads; ++i) {
        params[i].threadid = i;
        params[i].start = startTableIdArray[i]*10000+1;
        params[i].end = params[i].start + 10000;

        params[i].startTime = startTime;
        params[i].endTime = endTime;
        params[i].db = db;

        pthread_create(&threadId[i], NULL, oneLoader, &params[i]);
    }

    for (int32_t i = 0; i < numOfThreads; ++i) {
        pthread_join(threadId[i], NULL);
    }

    pthread_attr_destroy(&thattr);

    free(threadId);
    free(params);

    return 0;
}