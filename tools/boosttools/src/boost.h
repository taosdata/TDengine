/*
 * Copyright (c) 2024 TDengine contributors
 *
 * boost.h -- Core header for boosttools cluster-to-cluster sync utility.
 *
 * High-performance data migration between TDengine 3.x clusters using
 * taos_fetch_raw_block() + taos_write_raw_block() for zero-serialization
 * block transfer.
 */

#ifndef BOOST_H_
#define BOOST_H_

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "taos.h"

/* ---------------------------------------------------------------------------
 *  Constants
 * -------------------------------------------------------------------------*/

#define BOOST_MAX_WORKERS       64
#define BOOST_DEFAULT_WORKERS   16
#define BOOST_MAX_CONNS         128
#define BOOST_DEFAULT_BATCH_SIZE 10000      /* rows per fetch              */
#define BOOST_TABLE_BATCH       1000        /* tables per CREATE batch     */
#define BOOST_MAX_SQL_LEN       1048576     /* 1 MB                        */
#define BOOST_MAX_DB_NAME       64
#define BOOST_MAX_TB_NAME       256
#define BOOST_MAX_HOST_LEN      256
#define BOOST_PROGRESS_FILE     "boost_progress.json"
#define BOOST_RETRY_MAX         3
#define BOOST_RETRY_DELAY_MS    1000

/* ---------------------------------------------------------------------------
 *  Logging macros
 * -------------------------------------------------------------------------*/

#define BOOST_LOG(fmt, ...)                                                    \
    do {                                                                        \
        time_t _t = time(NULL);                                                \
        struct tm _tm;                                                         \
        localtime_r(&_t, &_tm);                                               \
        fprintf(stdout, "[%04d-%02d-%02d %02d:%02d:%02d] " fmt "\n",           \
                _tm.tm_year + 1900, _tm.tm_mon + 1, _tm.tm_mday,              \
                _tm.tm_hour, _tm.tm_min, _tm.tm_sec, ##__VA_ARGS__);          \
        fflush(stdout);                                                        \
    } while (0)

#define BOOST_ERR(fmt, ...)                                                    \
    do {                                                                        \
        time_t _t = time(NULL);                                                \
        struct tm _tm;                                                         \
        localtime_r(&_t, &_tm);                                               \
        fprintf(stderr, "[%04d-%02d-%02d %02d:%02d:%02d] ERROR: " fmt "\n",    \
                _tm.tm_year + 1900, _tm.tm_mon + 1, _tm.tm_mday,              \
                _tm.tm_hour, _tm.tm_min, _tm.tm_sec, ##__VA_ARGS__);          \
        fflush(stderr);                                                        \
    } while (0)

#define BOOST_DEBUG(fmt, ...)                                                  \
    do {                                                                        \
        if (config && config->verbose) {                                       \
            time_t _t = time(NULL);                                            \
            struct tm _tm;                                                     \
            localtime_r(&_t, &_tm);                                           \
            fprintf(stdout, "[%04d-%02d-%02d %02d:%02d:%02d] DEBUG: " fmt "\n",\
                    _tm.tm_year + 1900, _tm.tm_mon + 1, _tm.tm_mday,          \
                    _tm.tm_hour, _tm.tm_min, _tm.tm_sec, ##__VA_ARGS__);      \
            fflush(stdout);                                                    \
        }                                                                      \
    } while (0)

/* ---------------------------------------------------------------------------
 *  Data structures
 * -------------------------------------------------------------------------*/

/* Cluster endpoint configuration */
typedef struct BoostEndpoint {
    char     host[BOOST_MAX_HOST_LEN];
    uint16_t port;
    char     user[64];
    char     pass[64];
} BoostEndpoint;

/* Connection pool */
typedef struct BoostConnPool {
    TAOS          **conns;
    bool           *in_use;
    int             size;
    int             capacity;
    pthread_mutex_t lock;
    pthread_cond_t  avail;
    BoostEndpoint   ep;
} BoostConnPool;

/* Table info (for work queue) */
typedef struct BoostTableInfo {
    char db_name[BOOST_MAX_DB_NAME];
    char stable_name[BOOST_MAX_TB_NAME];
    char table_name[BOOST_MAX_TB_NAME];
    bool is_synced;     /* for progress tracking */
} BoostTableInfo;

/* Work queue (thread-safe) */
typedef struct BoostWorkQueue {
    BoostTableInfo *tables;
    int             total;
    int             next_idx;       /* next table to process */
    pthread_mutex_t lock;
} BoostWorkQueue;

/* Progress / stats tracking */
typedef struct BoostProgress {
    atomic_llong    tables_done;
    atomic_llong    tables_total;
    atomic_llong    rows_transferred;
    atomic_llong    bytes_transferred;
    atomic_llong    errors;
    time_t          start_time;
    char            progress_file[256];
    pthread_mutex_t file_lock;
} BoostProgress;

/* Main configuration */
typedef struct BoostConfig {
    BoostEndpoint src;
    BoostEndpoint dst;
    char          database[BOOST_MAX_DB_NAME];  /* specific db, or empty for all   */
    char          stable[BOOST_MAX_TB_NAME];    /* specific stable, or empty for all */
    int           num_workers;
    int           conn_pool_size;
    int           batch_size;
    bool          schema_only;
    bool          data_only;
    bool          resume;           /* resume from progress file */
    bool          verbose;
    bool          dry_run;
    char          time_start[64];   /* optional time range filter */
    char          time_end[64];
} BoostConfig;

/* Worker context */
typedef struct BoostWorker {
    int              id;
    pthread_t        thread;
    BoostConnPool   *src_pool;
    BoostConnPool   *dst_pool;
    BoostWorkQueue  *queue;
    BoostProgress   *progress;
    BoostConfig     *config;
    bool             running;
} BoostWorker;

/* ---------------------------------------------------------------------------
 *  Function declarations -- conn_pool.c
 * -------------------------------------------------------------------------*/

int   boost_pool_init(BoostConnPool *pool, BoostEndpoint *ep, int capacity);
void  boost_pool_destroy(BoostConnPool *pool);
TAOS *boost_pool_get(BoostConnPool *pool);
void  boost_pool_put(BoostConnPool *pool, TAOS *conn);

/* ---------------------------------------------------------------------------
 *  Function declarations -- schema_sync.c
 * -------------------------------------------------------------------------*/

int boost_sync_databases(TAOS *src, TAOS *dst, BoostConfig *cfg);
int boost_sync_stables(TAOS *src, TAOS *dst, const char *db, BoostConfig *cfg);
int boost_sync_child_tables(TAOS *src, TAOS *dst, const char *db,
                            const char *stable, BoostConfig *cfg,
                            BoostProgress *prog);
int boost_schema_sync(BoostConnPool *src_pool, BoostConnPool *dst_pool,
                      BoostConfig *cfg, BoostProgress *prog);

/* ---------------------------------------------------------------------------
 *  Function declarations -- data_sync.c
 * -------------------------------------------------------------------------*/

int boost_data_sync(BoostConnPool *src_pool, BoostConnPool *dst_pool,
                    BoostConfig *cfg, BoostProgress *prog);
int boost_sync_table_data(TAOS *src, TAOS *dst, const char *db,
                          const char *table, BoostConfig *cfg,
                          BoostProgress *prog);

/* ---------------------------------------------------------------------------
 *  Function declarations -- progress.c
 * -------------------------------------------------------------------------*/

int  boost_progress_init(BoostProgress *prog, const char *file);
void boost_progress_destroy(BoostProgress *prog);
int  boost_progress_save(BoostProgress *prog, BoostWorkQueue *queue);
int  boost_progress_load(BoostProgress *prog, BoostWorkQueue *queue);
void boost_progress_print(BoostProgress *prog);

#endif /* BOOST_H_ */
