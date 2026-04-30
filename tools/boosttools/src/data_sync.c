/*
 * data_sync.c -- Parallel data transfer engine using raw block API.
 *
 * Core: taos_fetch_raw_block() -> taos_write_raw_block() zero-copy transfer.
 */

#include "boost.h"

#include <errno.h>
#include <unistd.h>

/* ========================================================================== */
/*  Internal helpers                                                          */
/* ========================================================================== */

static TAOS_RES *query_sql(TAOS *taos, const char *sql)
{
    TAOS_RES *res = taos_query(taos, sql);
    int code = taos_errno(res);
    if (code != 0) {
        BOOST_ERR("SQL error %d: %s\n  SQL: %.200s", code, taos_errstr(res), sql);
        taos_free_result(res);
        return NULL;
    }
    return res;
}

static int exec_sql(TAOS *taos, const char *sql)
{
    TAOS_RES *res = taos_query(taos, sql);
    int code = taos_errno(res);
    if (code != 0) {
        taos_free_result(res);
        return -1;
    }
    taos_free_result(res);
    return 0;
}

/* ========================================================================== */
/*  boost_sync_table_data -- transfer data for a single child table           */
/* ========================================================================== */

int boost_sync_table_data(TAOS *src, TAOS *dst, const char *db,
                          const char *table, BoostConfig *cfg,
                          BoostProgress *prog)
{
    char sql[BOOST_MAX_SQL_LEN];
    int total_rows = 0;
    int64_t total_bytes = 0;

    /* Switch both connections to the target database. */
    snprintf(sql, sizeof(sql), "USE `%s`", db);
    exec_sql(src, sql);
    exec_sql(dst, sql);

    /* Build SELECT query with optional time range. */
    if (cfg->time_start[0] != '\0' && cfg->time_end[0] != '\0') {
        snprintf(sql, sizeof(sql),
                 "SELECT * FROM `%s` WHERE ts >= '%s' AND ts < '%s'",
                 table, cfg->time_start, cfg->time_end);
    } else if (cfg->time_start[0] != '\0') {
        snprintf(sql, sizeof(sql),
                 "SELECT * FROM `%s` WHERE ts >= '%s'",
                 table, cfg->time_start);
    } else if (cfg->time_end[0] != '\0') {
        snprintf(sql, sizeof(sql),
                 "SELECT * FROM `%s` WHERE ts < '%s'",
                 table, cfg->time_end);
    } else {
        snprintf(sql, sizeof(sql), "SELECT * FROM `%s`", table);
    }

    TAOS_RES *res = query_sql(src, sql);
    if (!res) return -1;

    /* Get field info for potential fallback. */
    int num_fields = taos_num_fields(res);
    TAOS_FIELD *fields = taos_fetch_fields(res);

    /* Fetch raw blocks and write directly to destination. */
    int  num_rows = 0;
    void *pData = NULL;
    int  write_errors = 0;

    while (taos_fetch_raw_block(res, &num_rows, &pData) == 0) {
        if (num_rows <= 0) break;

        /* Try write_raw_block first (simpler, more compatible). */
        int rc = taos_write_raw_block(dst, num_rows, pData, table);

        if (rc != 0) {
            /* Fallback: try with fields metadata. */
            rc = taos_write_raw_block_with_fields(
                dst, num_rows, pData, table, fields, num_fields);
        }

        if (rc != 0) {
            /* Last resort: retry after brief pause. */
            usleep(50000); /* 50ms */
            exec_sql(dst, sql); /* re-USE db */
            snprintf(sql, sizeof(sql), "USE `%s`", db);
            exec_sql(dst, sql);
            rc = taos_write_raw_block(dst, num_rows, pData, table);
        }

        if (rc != 0) {
            write_errors++;
            if (write_errors <= 2) {
                /* Log first few errors per table, continue trying. */
                BOOST_ERR("write_raw_block failed for %s.%s: %s",
                          db, table, taos_errstr(NULL));
            }
            /* Don't abort -- continue with next block. */
        } else {
            total_rows += num_rows;
            total_bytes += (int64_t)num_rows * num_fields * 8;
        }
    }

    taos_free_result(res);

    if (write_errors > 0) {
        atomic_fetch_add(&prog->errors, write_errors);
    }

    if (total_rows > 0) {
        atomic_fetch_add(&prog->rows_transferred, total_rows);
        atomic_fetch_add(&prog->bytes_transferred, total_bytes);
    }

    /* Return success if we transferred any data, or if table was empty. */
    return (write_errors > 0 && total_rows == 0) ? -1 : 0;
}

/* ========================================================================== */
/*  Worker thread function                                                    */
/* ========================================================================== */

static void *worker_thread(void *arg)
{
    BoostWorker *w = (BoostWorker *)arg;
    BoostWorkQueue *q = w->queue;

    while (w->running) {
        /* Get next table from the work queue. */
        pthread_mutex_lock(&q->lock);

        int idx = -1;
        while (q->next_idx < q->total) {
            if (!q->tables[q->next_idx].is_synced) {
                idx = q->next_idx;
                q->next_idx++;
                break;
            }
            q->next_idx++;
        }

        pthread_mutex_unlock(&q->lock);

        if (idx < 0) break;

        BoostTableInfo *ti = &q->tables[idx];

        TAOS *src_conn = boost_pool_get(w->src_pool);
        TAOS *dst_conn = boost_pool_get(w->dst_pool);

        if (!src_conn || !dst_conn) {
            if (src_conn) boost_pool_put(w->src_pool, src_conn);
            if (dst_conn) boost_pool_put(w->dst_pool, dst_conn);
            atomic_fetch_add(&w->progress->errors, 1);
            usleep(100000);
            continue;
        }

        int rc = boost_sync_table_data(src_conn, dst_conn,
                                       ti->db_name, ti->table_name,
                                       w->config, w->progress);

        boost_pool_put(w->src_pool, src_conn);
        boost_pool_put(w->dst_pool, dst_conn);

        if (rc == 0) {
            pthread_mutex_lock(&q->lock);
            ti->is_synced = true;
            pthread_mutex_unlock(&q->lock);
            atomic_fetch_add(&w->progress->tables_done, 1);
        }
        /* errors already counted inside sync_table_data */
    }

    return NULL;
}

/* ========================================================================== */
/*  Progress reporter thread                                                  */
/* ========================================================================== */

typedef struct {
    BoostProgress *prog;
    BoostWorkQueue *queue;
    bool *running;
} ProgressReporterCtx;

static void *progress_reporter_thread(void *arg)
{
    ProgressReporterCtx *ctx = (ProgressReporterCtx *)arg;
    int tick = 0;

    while (*(ctx->running)) {
        sleep(5);
        boost_progress_print(ctx->prog);
        if (++tick % 6 == 0) {
            boost_progress_save(ctx->prog, ctx->queue);
        }
    }
    return NULL;
}

/* ========================================================================== */
/*  boost_data_sync -- top-level data sync orchestrator                       */
/* ========================================================================== */

int boost_data_sync(BoostConnPool *src_pool, BoostConnPool *dst_pool,
                    BoostConfig *cfg, BoostProgress *prog)
{
    BOOST_LOG("========== Data Sync Start ==========");

    TAOS *src = boost_pool_get(src_pool);
    if (!src) {
        BOOST_ERR("Cannot get source connection for table discovery");
        return -1;
    }

    /* Determine databases. */
    char db_list[256][BOOST_MAX_DB_NAME];
    int db_count = 0;

    if (cfg->database[0] != '\0') {
        strncpy(db_list[0], cfg->database, BOOST_MAX_DB_NAME - 1);
        db_list[0][BOOST_MAX_DB_NAME - 1] = '\0';
        db_count = 1;
    } else {
        TAOS_RES *res = query_sql(src, "SHOW DATABASES");
        if (res) {
            TAOS_ROW row;
            while ((row = taos_fetch_row(res)) != NULL && db_count < 256) {
                int *lengths = taos_fetch_lengths(res);
                char name[BOOST_MAX_DB_NAME];
                int nl = lengths[0] < (BOOST_MAX_DB_NAME - 1) ? lengths[0] : (BOOST_MAX_DB_NAME - 1);
                memcpy(name, row[0], nl);
                name[nl] = '\0';
                if (strcmp(name, "information_schema") == 0 ||
                    strcmp(name, "performance_schema") == 0 ||
                    strcmp(name, "log") == 0) continue;
                strncpy(db_list[db_count], name, BOOST_MAX_DB_NAME - 1);
                db_list[db_count][BOOST_MAX_DB_NAME - 1] = '\0';
                db_count++;
            }
            taos_free_result(res);
        }
    }

    /* Build work queue from all child tables. */
    BoostWorkQueue queue;
    memset(&queue, 0, sizeof(queue));
    pthread_mutex_init(&queue.lock, NULL);

    int cap = 100000;
    queue.tables = (BoostTableInfo *)malloc(cap * sizeof(BoostTableInfo));
    if (!queue.tables) {
        boost_pool_put(src_pool, src);
        return -1;
    }

    for (int d = 0; d < db_count; d++) {
        char sql[BOOST_MAX_SQL_LEN];

        /* Use SHOW TABLES which is more reliable than information_schema
         * for getting the complete list of child/normal tables. */
        snprintf(sql, sizeof(sql), "USE `%s`", db_list[d]);
        exec_sql(src, sql);

        if (cfg->stable[0] != '\0') {
            snprintf(sql, sizeof(sql),
                     "SELECT table_name FROM information_schema.ins_tables "
                     "WHERE db_name = '%s' AND stable_name = '%s'",
                     db_list[d], cfg->stable);
        } else {
            snprintf(sql, sizeof(sql),
                     "SELECT table_name, stable_name FROM information_schema.ins_tables "
                     "WHERE db_name = '%s'",
                     db_list[d]);
        }

        TAOS_RES *res = query_sql(src, sql);
        if (!res) continue;

        TAOS_ROW row;
        while ((row = taos_fetch_row(res)) != NULL) {
            int *lengths = taos_fetch_lengths(res);

            if (queue.total >= cap) {
                cap *= 2;
                BoostTableInfo *tmp = (BoostTableInfo *)realloc(
                    queue.tables, cap * sizeof(BoostTableInfo));
                if (!tmp) break;
                queue.tables = tmp;
            }

            BoostTableInfo *ti = &queue.tables[queue.total];
            memset(ti, 0, sizeof(*ti));

            strncpy(ti->db_name, db_list[d], BOOST_MAX_DB_NAME - 1);

            int nl = lengths[0] < (BOOST_MAX_TB_NAME - 1) ? lengths[0] : (BOOST_MAX_TB_NAME - 1);
            memcpy(ti->table_name, row[0], nl);
            ti->table_name[nl] = '\0';

            if (row[1] && lengths[1] > 0) {
                nl = lengths[1] < (BOOST_MAX_TB_NAME - 1) ? lengths[1] : (BOOST_MAX_TB_NAME - 1);
                memcpy(ti->stable_name, row[1], nl);
                ti->stable_name[nl] = '\0';
            }

            ti->is_synced = false;
            queue.total++;
        }
        taos_free_result(res);
    }

    boost_pool_put(src_pool, src);

    BOOST_LOG("Work queue: %d tables to sync", queue.total);
    atomic_store(&prog->tables_total, queue.total);

    /* Resume from checkpoint if requested. */
    if (cfg->resume) {
        if (boost_progress_load(prog, &queue) == 0) {
            long long done = atomic_load(&prog->tables_done);
            BOOST_LOG("Resumed: %lld tables already done", done);
        }
    }

    /* Launch workers. */
    int num_workers = cfg->num_workers;
    if (num_workers > queue.total) {
        num_workers = queue.total > 0 ? queue.total : 1;
    }

    BoostWorker *workers = (BoostWorker *)calloc(num_workers, sizeof(BoostWorker));
    if (!workers) {
        free(queue.tables);
        return -1;
    }

    bool reporter_running = true;
    pthread_t reporter_tid;
    ProgressReporterCtx reporter_ctx = {
        .prog = prog, .queue = &queue, .running = &reporter_running
    };
    pthread_create(&reporter_tid, NULL, progress_reporter_thread, &reporter_ctx);

    BOOST_LOG("Launching %d workers", num_workers);

    for (int i = 0; i < num_workers; i++) {
        workers[i].id = i;
        workers[i].src_pool = src_pool;
        workers[i].dst_pool = dst_pool;
        workers[i].queue = &queue;
        workers[i].progress = prog;
        workers[i].config = cfg;
        workers[i].running = true;
        if (pthread_create(&workers[i].thread, NULL, worker_thread, &workers[i]) != 0) {
            workers[i].running = false;
        }
    }

    for (int i = 0; i < num_workers; i++) {
        if (workers[i].running) pthread_join(workers[i].thread, NULL);
    }

    reporter_running = false;
    pthread_join(reporter_tid, NULL);

    boost_progress_print(prog);
    boost_progress_save(prog, &queue);

    free(workers);
    free(queue.tables);
    pthread_mutex_destroy(&queue.lock);

    BOOST_LOG("========== Data Sync Complete ==========");
    return 0;
}
