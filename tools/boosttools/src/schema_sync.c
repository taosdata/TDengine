/*
 * schema_sync.c -- Schema synchronization between TDengine clusters.
 *
 * Replicates databases, supertables, and child tables (with tags) from
 * the source cluster to the destination cluster.  Child tables are created
 * in batches of BOOST_TABLE_BATCH to handle 600K+ tables efficiently.
 */

#include "boost.h"

#include <errno.h>
#include <unistd.h>

/* ========================================================================== */
/*  Internal helpers                                                          */
/* ========================================================================== */

/* Execute a SQL statement, log on error, free the result. Returns 0 / -1.
 * If ignore_exists is true, "Table already exists" errors are silently ignored. */
static int exec_sql_ex(TAOS *taos, const char *sql, bool ignore_exists)
{
    TAOS_RES *res = taos_query(taos, sql);
    int code = taos_errno(res);
    if (code != 0) {
        /* 0x80002603 = Table already exists (-2147482109) */
        if (ignore_exists && (code == 0x2603 || code == -2147482109 ||
            (taos_errstr(res) && strstr(taos_errstr(res), "already exists")))) {
            taos_free_result(res);
            return 0; /* silently ignore */
        }
        BOOST_ERR("SQL error %d: %s\n  SQL: %.200s", code, taos_errstr(res), sql);
        taos_free_result(res);
        return -1;
    }
    taos_free_result(res);
    return 0;
}

static int exec_sql(TAOS *taos, const char *sql)
{
    return exec_sql_ex(taos, sql, false);
}

/* Execute a SQL statement, return the result (caller must free). */
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

/* ========================================================================== */
/*  boost_sync_databases                                                      */
/* ========================================================================== */

int boost_sync_databases(TAOS *src, TAOS *dst, BoostConfig *cfg)
{
    BOOST_LOG("=== Syncing databases ===");

    /* If a specific database is given, just sync that one. */
    if (cfg->database[0] != '\0') {
        char sql[BOOST_MAX_SQL_LEN];

        /* Get the CREATE DATABASE statement from source. */
        snprintf(sql, sizeof(sql), "SHOW CREATE DATABASE `%s`", cfg->database);
        TAOS_RES *res = query_sql(src, sql);
        if (!res) return -1;

        TAOS_ROW row = taos_fetch_row(res);
        if (!row) {
            BOOST_ERR("database '%s' not found on source", cfg->database);
            taos_free_result(res);
            return -1;
        }

        /* row[1] is the CREATE DATABASE statement. */
        int *lengths = taos_fetch_lengths(res);
        char *create_sql = (char *)malloc(lengths[1] + 1);
        if (!create_sql) {
            taos_free_result(res);
            return -1;
        }
        memcpy(create_sql, row[1], lengths[1]);
        create_sql[lengths[1]] = '\0';
        taos_free_result(res);

        BOOST_LOG("Creating database '%s' on destination", cfg->database);
        if (cfg->dry_run) {
            BOOST_LOG("[DRY RUN] %s", create_sql);
            free(create_sql);
            return 0;
        }

        /* Try to create; if it already exists, just USE it. */
        int rc = exec_sql(dst, create_sql);
        free(create_sql);

        if (rc != 0) {
            /* Might already exist -- try USE */
            snprintf(sql, sizeof(sql), "USE `%s`", cfg->database);
            if (exec_sql(dst, sql) != 0) {
                return -1;
            }
        }

        BOOST_LOG("Database '%s' synced", cfg->database);
        return 0;
    }

    /* Sync all databases. */
    TAOS_RES *res = query_sql(src, "SHOW DATABASES");
    if (!res) return -1;

    TAOS_ROW row;
    int count = 0;
    char db_names[256][BOOST_MAX_DB_NAME];

    while ((row = taos_fetch_row(res)) != NULL) {
        int *lengths = taos_fetch_lengths(res);
        if (lengths[0] >= BOOST_MAX_DB_NAME) continue;

        char db_name[BOOST_MAX_DB_NAME];
        memcpy(db_name, row[0], lengths[0]);
        db_name[lengths[0]] = '\0';

        /* Skip system databases. */
        if (strcmp(db_name, "information_schema") == 0 ||
            strcmp(db_name, "performance_schema") == 0 ||
            strcmp(db_name, "log") == 0) {
            continue;
        }

        if (count < 256) {
            strncpy(db_names[count], db_name, BOOST_MAX_DB_NAME - 1);
            db_names[count][BOOST_MAX_DB_NAME - 1] = '\0';
            count++;
        }
    }
    taos_free_result(res);

    BOOST_LOG("Found %d user databases to sync", count);

    for (int i = 0; i < count; i++) {
        char sql[BOOST_MAX_SQL_LEN];
        snprintf(sql, sizeof(sql), "SHOW CREATE DATABASE `%s`", db_names[i]);

        res = query_sql(src, sql);
        if (!res) continue;

        row = taos_fetch_row(res);
        if (row) {
            int *lengths = taos_fetch_lengths(res);
            char *create_sql = (char *)malloc(lengths[1] + 1);
            if (create_sql) {
                memcpy(create_sql, row[1], lengths[1]);
                create_sql[lengths[1]] = '\0';

                BOOST_LOG("Creating database '%s'", db_names[i]);
                if (!cfg->dry_run) {
                    exec_sql(dst, create_sql);
                }
                free(create_sql);
            }
        }
        taos_free_result(res);
    }

    return 0;
}

/* ========================================================================== */
/*  boost_sync_stables                                                        */
/* ========================================================================== */

int boost_sync_stables(TAOS *src, TAOS *dst, const char *db, BoostConfig *cfg)
{
    BOOST_LOG("--- Syncing supertables for database '%s' ---", db);

    char sql[BOOST_MAX_SQL_LEN];

    /* Switch to the database on both connections. */
    snprintf(sql, sizeof(sql), "USE `%s`", db);
    if (exec_sql(src, sql) != 0) return -1;
    if (exec_sql(dst, sql) != 0) return -1;

    /* List all supertables. */
    if (cfg->stable[0] != '\0') {
        /* Sync just one supertable. */
        snprintf(sql, sizeof(sql), "SHOW CREATE STABLE `%s`.`%s`", db, cfg->stable);
        TAOS_RES *res = query_sql(src, sql);
        if (!res) return -1;

        TAOS_ROW row = taos_fetch_row(res);
        if (row) {
            int *lengths = taos_fetch_lengths(res);
            char *create_sql = (char *)malloc(lengths[1] + 1);
            if (create_sql) {
                memcpy(create_sql, row[1], lengths[1]);
                create_sql[lengths[1]] = '\0';
                BOOST_LOG("Creating supertable '%s'.'%s'", db, cfg->stable);
                if (!cfg->dry_run) {
                    exec_sql(dst, create_sql);
                }
                free(create_sql);
            }
        }
        taos_free_result(res);
        return 0;
    }

    /* Get all supertables in this database. */
    snprintf(sql, sizeof(sql),
             "SELECT stable_name FROM information_schema.ins_stables "
             "WHERE db_name = '%s'", db);
    TAOS_RES *res = query_sql(src, sql);
    if (!res) return -1;

    /* Collect stable names first (can't nest queries on same connection). */
    typedef struct { char name[BOOST_MAX_TB_NAME]; } StableName;
    StableName *stables = NULL;
    int scount = 0;
    int scap = 128;
    stables = (StableName *)malloc(scap * sizeof(StableName));
    if (!stables) {
        taos_free_result(res);
        return -1;
    }

    TAOS_ROW row;
    while ((row = taos_fetch_row(res)) != NULL) {
        int *lengths = taos_fetch_lengths(res);
        if (lengths[0] >= BOOST_MAX_TB_NAME) continue;
        if (scount >= scap) {
            scap *= 2;
            StableName *tmp = (StableName *)realloc(stables, scap * sizeof(StableName));
            if (!tmp) break;
            stables = tmp;
        }
        memcpy(stables[scount].name, row[0], lengths[0]);
        stables[scount].name[lengths[0]] = '\0';
        scount++;
    }
    taos_free_result(res);

    BOOST_LOG("Found %d supertables in '%s'", scount, db);

    /* Create each supertable on destination. */
    for (int i = 0; i < scount; i++) {
        snprintf(sql, sizeof(sql), "SHOW CREATE STABLE `%s`.`%s`", db, stables[i].name);
        res = query_sql(src, sql);
        if (!res) continue;

        row = taos_fetch_row(res);
        if (row) {
            int *lengths = taos_fetch_lengths(res);
            char *create_sql = (char *)malloc(lengths[1] + 1);
            if (create_sql) {
                memcpy(create_sql, row[1], lengths[1]);
                create_sql[lengths[1]] = '\0';
                BOOST_LOG("Creating supertable '%s' (%d/%d)", stables[i].name, i + 1, scount);
                if (!cfg->dry_run) {
                    exec_sql(dst, create_sql);
                }
                free(create_sql);
            }
        }
        taos_free_result(res);
    }

    free(stables);
    return 0;
}

/* ========================================================================== */
/*  boost_sync_child_tables                                                   */
/* ========================================================================== */

/*
 * Sync child tables for a given supertable.
 * Uses batched CREATE TABLE ... USING ... TAGS(...) for efficiency.
 *
 * Strategy:
 *   1. DESCRIBE stable to get tag column definitions
 *   2. Query child table names + tag values in pages from information_schema
 *   3. Batch CREATE TABLE statements (BOOST_TABLE_BATCH per batch)
 */
int boost_sync_child_tables(TAOS *src, TAOS *dst, const char *db,
                            const char *stable, BoostConfig *cfg,
                            BoostProgress *prog)
{
    char sql[BOOST_MAX_SQL_LEN];
    int total_created = 0;

    BOOST_LOG("--- Syncing child tables: %s.%s ---", db, stable);

    /* Get tag columns from DESCRIBE stable. */
    snprintf(sql, sizeof(sql), "DESCRIBE `%s`.`%s`", db, stable);
    TAOS_RES *res = query_sql(src, sql);
    if (!res) return -1;

    /* Collect tag column names. */
    typedef struct { char name[256]; char type[64]; int is_tag; } ColInfo;
    ColInfo *cols = NULL;
    int ncols = 0, ncap = 64;
    cols = (ColInfo *)malloc(ncap * sizeof(ColInfo));
    if (!cols) { taos_free_result(res); return -1; }

    TAOS_ROW row;
    while ((row = taos_fetch_row(res)) != NULL) {
        int *lengths = taos_fetch_lengths(res);
        if (ncols >= ncap) {
            ncap *= 2;
            ColInfo *tmp = (ColInfo *)realloc(cols, ncap * sizeof(ColInfo));
            if (!tmp) break;
            cols = tmp;
        }
        /* col[0]=Field, col[1]=Type, col[2]=Length, col[3]=Note */
        memcpy(cols[ncols].name, row[0], lengths[0]);
        cols[ncols].name[lengths[0]] = '\0';

        memcpy(cols[ncols].type, row[1], lengths[1]);
        cols[ncols].type[lengths[1]] = '\0';

        /* In TDengine 3.x, DESCRIBE shows "TAG" in the 4th column for tags. */
        cols[ncols].is_tag = 0;
        if (row[3]) {
            char note[32] = {0};
            int note_len = lengths[3] < 31 ? lengths[3] : 31;
            memcpy(note, row[3], note_len);
            if (strcasecmp(note, "TAG") == 0) {
                cols[ncols].is_tag = 1;
            }
        }
        ncols++;
    }
    taos_free_result(res);

    /* Build tag column list for the SELECT query. */
    char tag_select[4096] = {0};
    char tag_names[128][256];
    int ntags = 0;

    for (int i = 0; i < ncols; i++) {
        if (cols[i].is_tag) {
            strncpy(tag_names[ntags], cols[i].name, 255);
            tag_names[ntags][255] = '\0';
            if (ntags > 0) strcat(tag_select, ", ");
            strcat(tag_select, "`");
            strcat(tag_select, cols[i].name);
            strcat(tag_select, "`");
            ntags++;
            if (ntags >= 128) break;
        }
    }
    free(cols);

    if (ntags == 0) {
        BOOST_LOG("No tags found for '%s' -- syncing as normal tables", stable);
    }

    /* Page through child tables using information_schema (reliable pagination). */
    int offset = 0;
    int page_size = BOOST_TABLE_BATCH;

    snprintf(sql, sizeof(sql), "USE `%s`", db);
    exec_sql(src, sql);
    exec_sql(dst, sql);

    for (;;) {
        /* Use information_schema for reliable child table listing + tags. */
        if (ntags > 0) {
            snprintf(sql, sizeof(sql),
                     "SELECT tbname, %s FROM `%s` WHERE tbname IN "
                     "(SELECT table_name FROM information_schema.ins_tables "
                     "WHERE db_name='%s' AND stable_name='%s' "
                     "ORDER BY table_name LIMIT %d OFFSET %d) "
                     "GROUP BY tbname",
                     tag_select, stable, db, stable, page_size, offset);
        } else {
            snprintf(sql, sizeof(sql),
                     "SELECT table_name FROM information_schema.ins_tables "
                     "WHERE db_name='%s' AND stable_name='%s' "
                     "ORDER BY table_name LIMIT %d OFFSET %d",
                     db, stable, page_size, offset);
        }

        res = query_sql(src, sql);
        if (!res) break;

        int num_fields = taos_num_fields(res);
        TAOS_FIELD *fields = taos_fetch_fields(res);

        /* Build batch CREATE TABLE statement. */
        char *batch_sql = (char *)malloc(BOOST_MAX_SQL_LEN);
        if (!batch_sql) { taos_free_result(res); break; }

        int batch_count = 0;
        int sql_len = 0;
        int page_rows = 0;

        sql_len = snprintf(batch_sql, BOOST_MAX_SQL_LEN, "CREATE TABLE IF NOT EXISTS ");

        while ((row = taos_fetch_row(res)) != NULL) {
            int *lengths = taos_fetch_lengths(res);
            page_rows++;

            /* row[0] = tbname */
            char tbname[BOOST_MAX_TB_NAME];
            int name_len = lengths[0] < (BOOST_MAX_TB_NAME - 1) ? lengths[0] : (BOOST_MAX_TB_NAME - 1);
            memcpy(tbname, row[0], name_len);
            tbname[name_len] = '\0';

            /* Build: `tbname` USING `stable` TAGS(val1, val2, ...) */
            char one_table[4096];
            int tlen = 0;

            if (ntags > 0) {
                tlen = snprintf(one_table, sizeof(one_table),
                                "`%s` USING `%s` TAGS(", tbname, stable);

                for (int t = 0; t < ntags; t++) {
                    int fi = t + 1; /* field index: 0=tbname, 1..n=tags */
                    if (t > 0) {
                        tlen += snprintf(one_table + tlen, sizeof(one_table) - tlen, ", ");
                    }

                    if (row[fi] == NULL) {
                        tlen += snprintf(one_table + tlen, sizeof(one_table) - tlen, "NULL");
                    } else if (fi < num_fields) {
                        int ftype = fields[fi].type;
                        /* String/binary/nchar types need quoting. */
                        if (ftype == TSDB_DATA_TYPE_BINARY ||
                            ftype == TSDB_DATA_TYPE_VARCHAR ||
                            ftype == TSDB_DATA_TYPE_NCHAR ||
                            ftype == TSDB_DATA_TYPE_VARBINARY ||
                            ftype == TSDB_DATA_TYPE_JSON ||
                            ftype == TSDB_DATA_TYPE_GEOMETRY) {
                            /* Escape single quotes in value. */
                            char escaped[2048];
                            int ei = 0;
                            char *val = (char *)row[fi];
                            for (int k = 0; k < lengths[fi] && ei < 2040; k++) {
                                if (val[k] == '\'') {
                                    escaped[ei++] = '\\';
                                    escaped[ei++] = '\'';
                                } else {
                                    escaped[ei++] = val[k];
                                }
                            }
                            escaped[ei] = '\0';
                            tlen += snprintf(one_table + tlen, sizeof(one_table) - tlen,
                                             "'%s'", escaped);
                        } else if (ftype == TSDB_DATA_TYPE_BOOL) {
                            tlen += snprintf(one_table + tlen, sizeof(one_table) - tlen,
                                             "%s", (*(int8_t *)row[fi]) ? "true" : "false");
                        } else if (ftype == TSDB_DATA_TYPE_TINYINT) {
                            tlen += snprintf(one_table + tlen, sizeof(one_table) - tlen,
                                             "%d", *(int8_t *)row[fi]);
                        } else if (ftype == TSDB_DATA_TYPE_SMALLINT) {
                            tlen += snprintf(one_table + tlen, sizeof(one_table) - tlen,
                                             "%d", *(int16_t *)row[fi]);
                        } else if (ftype == TSDB_DATA_TYPE_INT) {
                            tlen += snprintf(one_table + tlen, sizeof(one_table) - tlen,
                                             "%d", *(int32_t *)row[fi]);
                        } else if (ftype == TSDB_DATA_TYPE_BIGINT ||
                                   ftype == TSDB_DATA_TYPE_TIMESTAMP) {
                            tlen += snprintf(one_table + tlen, sizeof(one_table) - tlen,
                                             "%lld", (long long)*(int64_t *)row[fi]);
                        } else if (ftype == TSDB_DATA_TYPE_UTINYINT) {
                            tlen += snprintf(one_table + tlen, sizeof(one_table) - tlen,
                                             "%u", *(uint8_t *)row[fi]);
                        } else if (ftype == TSDB_DATA_TYPE_USMALLINT) {
                            tlen += snprintf(one_table + tlen, sizeof(one_table) - tlen,
                                             "%u", *(uint16_t *)row[fi]);
                        } else if (ftype == TSDB_DATA_TYPE_UINT) {
                            tlen += snprintf(one_table + tlen, sizeof(one_table) - tlen,
                                             "%u", *(uint32_t *)row[fi]);
                        } else if (ftype == TSDB_DATA_TYPE_UBIGINT) {
                            tlen += snprintf(one_table + tlen, sizeof(one_table) - tlen,
                                             "%llu", (unsigned long long)*(uint64_t *)row[fi]);
                        } else if (ftype == TSDB_DATA_TYPE_FLOAT) {
                            tlen += snprintf(one_table + tlen, sizeof(one_table) - tlen,
                                             "%f", *(float *)row[fi]);
                        } else if (ftype == TSDB_DATA_TYPE_DOUBLE) {
                            tlen += snprintf(one_table + tlen, sizeof(one_table) - tlen,
                                             "%f", *(double *)row[fi]);
                        } else {
                            /* Fallback: treat as string. */
                            char val_buf[1024] = {0};
                            int vl = lengths[fi] < 1023 ? lengths[fi] : 1023;
                            memcpy(val_buf, row[fi], vl);
                            tlen += snprintf(one_table + tlen, sizeof(one_table) - tlen,
                                             "'%s'", val_buf);
                        }
                    }
                }
                tlen += snprintf(one_table + tlen, sizeof(one_table) - tlen, ") ");
            } else {
                /* No tags -- normal child table (edge case). */
                tlen = snprintf(one_table, sizeof(one_table),
                                "`%s` USING `%s` TAGS() ", tbname, stable);
            }

            /* Check if adding this table would overflow the batch SQL. */
            if (sql_len + tlen + 10 >= BOOST_MAX_SQL_LEN) {
                /* Execute current batch. */
                if (batch_count > 0 && !cfg->dry_run) {
                    exec_sql_ex(dst, batch_sql, true);
                }
                total_created += batch_count;
                batch_count = 0;
                sql_len = snprintf(batch_sql, BOOST_MAX_SQL_LEN,
                                   "CREATE TABLE IF NOT EXISTS ");
            }

            memcpy(batch_sql + sql_len, one_table, tlen);
            sql_len += tlen;
            batch_sql[sql_len] = '\0';
            batch_count++;
        }

        taos_free_result(res);

        /* Execute remaining batch. */
        if (batch_count > 0) {
            if (!cfg->dry_run) {
                exec_sql_ex(dst, batch_sql, true);
            }
            total_created += batch_count;
        }

        free(batch_sql);

        /* If we got fewer rows than page_size, we're done. */
        if (page_rows < page_size) break;
        offset += page_size;

        if (total_created % 10000 == 0 && total_created > 0) {
            BOOST_LOG("  ... created %d child tables so far for %s.%s",
                      total_created, db, stable);
        }
    }

    BOOST_LOG("Synced %d child tables for %s.%s", total_created, db, stable);
    return 0;
}

/* ========================================================================== */
/*  boost_schema_sync  (top-level orchestrator)                               */
/* ========================================================================== */

int boost_schema_sync(BoostConnPool *src_pool, BoostConnPool *dst_pool,
                      BoostConfig *cfg, BoostProgress *prog)
{
    BOOST_LOG("========== Schema Sync Start ==========");

    TAOS *src = boost_pool_get(src_pool);
    TAOS *dst = boost_pool_get(dst_pool);
    if (!src || !dst) {
        BOOST_ERR("Failed to get connections for schema sync");
        if (src) boost_pool_put(src_pool, src);
        if (dst) boost_pool_put(dst_pool, dst);
        return -1;
    }

    int rc = 0;

    /* Step 1: Sync databases. */
    rc = boost_sync_databases(src, dst, cfg);
    if (rc != 0) {
        BOOST_ERR("Database sync failed");
        goto done;
    }

    /* Step 2: Determine which databases to process. */
    char db_list[256][BOOST_MAX_DB_NAME];
    int db_count = 0;

    if (cfg->database[0] != '\0') {
        strncpy(db_list[0], cfg->database, BOOST_MAX_DB_NAME - 1);
        db_list[0][BOOST_MAX_DB_NAME - 1] = '\0';
        db_count = 1;
    } else {
        /* Get all user databases. */
        TAOS_RES *res = query_sql(src, "SHOW DATABASES");
        if (res) {
            TAOS_ROW row;
            while ((row = taos_fetch_row(res)) != NULL) {
                int *lengths = taos_fetch_lengths(res);
                char name[BOOST_MAX_DB_NAME];
                int nl = lengths[0] < (BOOST_MAX_DB_NAME - 1) ? lengths[0] : (BOOST_MAX_DB_NAME - 1);
                memcpy(name, row[0], nl);
                name[nl] = '\0';

                if (strcmp(name, "information_schema") == 0 ||
                    strcmp(name, "performance_schema") == 0 ||
                    strcmp(name, "log") == 0) {
                    continue;
                }
                if (db_count < 256) {
                    strncpy(db_list[db_count], name, BOOST_MAX_DB_NAME - 1);
                    db_list[db_count][BOOST_MAX_DB_NAME - 1] = '\0';
                    db_count++;
                }
            }
            taos_free_result(res);
        }
    }

    /* Step 3: For each database, sync stables and child tables. */
    for (int d = 0; d < db_count; d++) {
        /* Sync supertables. */
        rc = boost_sync_stables(src, dst, db_list[d], cfg);
        if (rc != 0) {
            BOOST_ERR("Supertable sync failed for '%s'", db_list[d]);
            continue;
        }

        /* Get list of supertables for child table sync. */
        char sql[BOOST_MAX_SQL_LEN];
        char stable_list[1024][BOOST_MAX_TB_NAME];
        int stable_count = 0;

        if (cfg->stable[0] != '\0') {
            strncpy(stable_list[0], cfg->stable, BOOST_MAX_TB_NAME - 1);
            stable_list[0][BOOST_MAX_TB_NAME - 1] = '\0';
            stable_count = 1;
        } else {
            snprintf(sql, sizeof(sql),
                     "SELECT stable_name FROM information_schema.ins_stables "
                     "WHERE db_name = '%s'", db_list[d]);
            TAOS_RES *res = query_sql(src, sql);
            if (res) {
                TAOS_ROW row;
                while ((row = taos_fetch_row(res)) != NULL && stable_count < 1024) {
                    int *lengths = taos_fetch_lengths(res);
                    int nl = lengths[0] < (BOOST_MAX_TB_NAME - 1) ? lengths[0] : (BOOST_MAX_TB_NAME - 1);
                    memcpy(stable_list[stable_count], row[0], nl);
                    stable_list[stable_count][nl] = '\0';
                    stable_count++;
                }
                taos_free_result(res);
            }
        }

        /* Sync child tables for each supertable. */
        for (int s = 0; s < stable_count; s++) {
            boost_sync_child_tables(src, dst, db_list[d], stable_list[s],
                                    cfg, prog);
        }
    }

done:
    boost_pool_put(src_pool, src);
    boost_pool_put(dst_pool, dst);

    BOOST_LOG("========== Schema Sync Complete ==========");
    return rc;
}
