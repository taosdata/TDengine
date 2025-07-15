/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software
 * Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <locale.h>

#include "benchLog.h"
#include "benchData.h"
#include "benchDataMix.h"
#include "benchCsv.h"

//
// main etry
//

#define SHOW_CNT 100000
#define GEN_ROW_FIELDS_TAG  0
#define GEN_ROW_FIELDS_COL  1



static int csvValidateParamTsFormat(const char* csv_ts_format) {
    if (!csv_ts_format) return 0;

    struct tm test_tm = {
        .tm_year    = 70,
        .tm_mon     = 0,
        .tm_mday    = 1,
        .tm_hour    = 0,
        .tm_min     = 0,
        .tm_sec     = 0,
        .tm_isdst   = -1
    };
    mktime(&test_tm);

    char buffer[1024];
    size_t len = strftime(buffer, sizeof(buffer), csv_ts_format, &test_tm);
    if (len == 0) {
        return -1;
    }

#ifdef _WIN32
    const char* invalid_chars = "/\\:*?\"<>|";
#else
    const char* invalid_chars = "/\\?\"<>|";
#endif
    if (strpbrk(buffer, invalid_chars) != NULL) {
        return -1;
    }

    int has_Y = 0, has_m = 0, has_d = 0;
    const char* p = csv_ts_format;
    while (*p) {
        if (*p == '%') {
            p++;
            switch (*p) {
                case 'Y': has_Y = 1; break;
                case 'm': has_m = 1; break;
                case 'd': has_d = 1; break;
            }
        }
        p++;
    }
 
    if (has_Y == 0 || has_m == 0 || has_d == 0) {
        return -1;
    }

    return 0;
}


static long csvValidateParamTsInterval(const char* csv_ts_interval) {
    if (!csv_ts_interval || *csv_ts_interval == '\0') return -1;

    char* endptr;
    errno = 0;
    const long num = strtol(csv_ts_interval, &endptr, 10);
    
    if (errno == ERANGE ||
        endptr == csv_ts_interval ||
        num <= 0) {
        return -1;
    }

    if (*endptr == '\0' ||
        *(endptr + 1) != '\0') {
        return -1;
    }

    switch (tolower(*endptr)) {
        case 's': return num;
        case 'm': return num * 60;
        case 'h': return num * 60 * 60;
        case 'd': return num * 60 * 60 * 24;
        default : return -1;
    }
}


static int csvParseParameter() {
    // csv_output_path
    size_t len = strlen(g_arguments->output_path);
    if (len == 0) {
        errorPrint("Failed to generate csv files, the specified output path is empty. Please provide a valid path.\n");
        return -1;
    }
    if (g_arguments->output_path[len - 1] != '/') {
        int n = snprintf(g_arguments->output_path_buf, sizeof(g_arguments->output_path_buf), "%s/", g_arguments->output_path);
        if (n < 0 || n >= sizeof(g_arguments->output_path_buf)) {
            errorPrint("Failed to generate csv files, path buffer overflow risk when appending '/'. path: %s.\n",
                    g_arguments->output_path);
            return -1;
        }
        g_arguments->output_path = g_arguments->output_path_buf;
    }

    return 0;
}


static int csvParseStbParameter(SSuperTable* stb) {
    // csv_ts_format
    if (stb->csv_ts_format) {
        if (csvValidateParamTsFormat(stb->csv_ts_format) != 0) {
            errorPrint("Failed to generate csv files, the parameter `csv_ts_format` is invalid. csv_ts_format: %s.\n",
                    stb->csv_ts_format);
            return -1;
        }
    }

    // csv_ts_interval
    long csv_ts_intv_secs = csvValidateParamTsInterval(stb->csv_ts_interval);
    if (csv_ts_intv_secs <= 0) {
        errorPrint("Failed to generate csv files, the parameter `csv_ts_interval` is invalid. csv_ts_interval: %s.\n",
                stb->csv_ts_interval);
        return -1;
    }
    stb->csv_ts_intv_secs = csv_ts_intv_secs;

    return 0;
}


static time_t csvAlignTimestamp(time_t seconds, const char* ts_format) {
    struct tm aligned_tm;
#ifdef _WIN32
    localtime_s(&aligned_tm, &seconds);
#else
    localtime_r(&seconds, &aligned_tm);
#endif
 
    int has_Y = 0, has_m = 0, has_d = 0, has_H = 0, has_M = 0, has_S = 0;
    const char* p = ts_format;
    while (*p) {
        if (*p == '%') {
            p++;
            switch (*p) {
                case 'Y': has_Y = 1; break;
                case 'm': has_m = 1; break;
                case 'd': has_d = 1; break;
                case 'H': has_H = 1; break;
                case 'M': has_M = 1; break;
                case 'S': has_S = 1; break;
            }
        }
        p++;
    }
 
    if (!has_S) aligned_tm.tm_sec  = 0;
    if (!has_M) aligned_tm.tm_min  = 0;
    if (!has_H) aligned_tm.tm_hour = 0;
    if (!has_d) aligned_tm.tm_mday = 1;
    if (!has_m) aligned_tm.tm_mon  = 0;
    if (!has_Y) aligned_tm.tm_year = 0;
 
    return mktime(&aligned_tm);
}


static time_t csvGetStartSeconds(int precision, int64_t start_ts, const char* csv_ts_format) {
    time_t start_seconds = 0;

    if (precision == TSDB_TIME_PRECISION_MICRO) {
        start_seconds = start_ts / 1000000L;
    } else if (precision == TSDB_TIME_PRECISION_NANO) {
        start_seconds = start_ts / 1000000000L;
    } else {
        start_seconds = start_ts / 1000L;
    }
    return csvAlignTimestamp(start_seconds, csv_ts_format);
}


static void csvConvertTime2String(time_t time_value, char* ts_format, char* time_buf, size_t buf_size) {
    struct tm tm_result;
    char* old_locale = setlocale(LC_TIME, "C");
#ifdef _WIN32
    localtime_s(&tm_result, &time_value);
#else
    localtime_r(&time_value, &tm_result);
#endif
    strftime(time_buf, buf_size, ts_format, &tm_result);
    if (old_locale) {
        setlocale(LC_TIME, old_locale);
    }
    return;
}


static CsvNamingType csvGetFileNamingType(SSuperTable* stb) {
    if (stb->interlaceRows > 0) {
        if (stb->csv_ts_format) {
            return CSV_NAMING_I_TIME_SLICE;
        } else {
            return CSV_NAMING_I_SINGLE;
        }
    } else {
        if (stb->csv_ts_format) {
            return CSV_NAMING_B_THREAD_TIME_SLICE;
        } else {
            return CSV_NAMING_B_THREAD;
        }
    }
}


static time_t csvCalcTimestampFromSeconds(int precision, time_t secs) {
    time_t ts = 0;

    if (precision == TSDB_TIME_PRECISION_MICRO) {
        ts = secs * 1000000L;
    } else if (precision == TSDB_TIME_PRECISION_NANO) {
        ts = secs * 1000000000L;
    } else {
        ts = secs * 1000L;
    }
    return ts;
}


static void csvCalcTimestampStep(CsvWriteMeta* write_meta) {
    write_meta->ts_step = csvCalcTimestampFromSeconds(write_meta->db->precision, write_meta->stb->csv_ts_intv_secs);
    return;
}


static void csvCalcSliceTimestamp(CsvWriteMeta* write_meta, CsvThreadMeta* thread_meta) {
    thread_meta->start_ts   = csvCalcTimestampFromSeconds(write_meta->db->precision, thread_meta->start_secs);
    thread_meta->end_ts     = csvCalcTimestampFromSeconds(write_meta->db->precision, thread_meta->end_secs);
    return;
}


static void csvCalcCtbRange(CsvThreadMeta* thread_meta, size_t total_threads, int64_t ctb_offset, int64_t ctb_count) {
    uint64_t  ctb_start_idx = 0;
    uint64_t  ctb_end_idx   = 0;
    size_t    tid_idx       = thread_meta->thread_id - 1;
    size_t    base          = ctb_count / total_threads;
    size_t    remainder     = ctb_count % total_threads;
 
    if (tid_idx < remainder) {
        ctb_start_idx = ctb_offset + tid_idx * (base + 1);
        ctb_end_idx   = ctb_start_idx  + (base + 1);
    } else {
        ctb_start_idx = ctb_offset + remainder * (base + 1) + (tid_idx - remainder) * base;
        ctb_end_idx   = ctb_start_idx  + base;
    }
 
    if (ctb_end_idx  > ctb_offset + ctb_count) {
        ctb_end_idx  = ctb_offset + ctb_count;
    }

    thread_meta->ctb_start_idx = ctb_start_idx;
    thread_meta->ctb_end_idx   = ctb_end_idx;
    thread_meta->ctb_count     = ctb_end_idx - ctb_start_idx;
    return;
}


static void csvGenThreadFormatter(CsvWriteMeta* write_meta) {
    int digits = 0;

    if (write_meta->total_threads == 0) {
        digits = 1;
    } else {
        for (int n = write_meta->total_threads; n > 0; n /= 10) {
            digits++;
        }
    }

    if (digits <= 1) {
        (void)snprintf(write_meta->thread_formatter, sizeof(write_meta->thread_formatter), "%%d");
    } else {
        (void)snprintf(write_meta->thread_formatter, sizeof(write_meta->thread_formatter), "%%0%dd", digits);
    }
    return;
}


static int csvGenCsvHeader(CsvWriteMeta* write_meta) {
    SSuperTable*  stb  = write_meta->stb;
    char*         buf  = write_meta->csv_header;
    int           pos  = 0;
    int           size = sizeof(write_meta->csv_header);

    if (!write_meta->stb->csv_output_header) {
        return 0;
    }

    // ts
    pos += snprintf(buf + pos, size - pos, "ts");

    // columns
    for (size_t i = 0; i < stb->cols->size; ++i) {
        Field* col = benchArrayGet(stb->cols, i);
        pos += snprintf(buf + pos, size - pos, ",%s", col->name);
    }

    // tbname
    pos += snprintf(buf + pos, size - pos, ",%s", write_meta->stb->csv_tbname_alias);

    // tags
    for (size_t i = 0; i < stb->tags->size; ++i) {
        Field* tag = benchArrayGet(stb->tags, i);
        pos += snprintf(buf + pos, size - pos, ",%s", tag->name);
    }

    // line break
    pos += snprintf(buf + pos, size - pos, "\n");

    write_meta->csv_header_length = (pos > 0 && pos < size) ? pos : 0;
    return (pos > 0 && pos < size) ? 0 : -1;
}


int csvGenCreateDbSql(SDataBase* db, char* buf, int size) {
    int pos = 0;

    pos += snprintf(buf + pos, size - pos, "CREATE DATABASE IF NOT EXISTS ");
    if (pos <= 0 || pos >= size) return -1;

    pos += snprintf(buf + pos, size - pos, g_arguments->escape_character ? "`%s`" : "%s", db->dbName);
    if (pos <= 0 || pos >= size) return -1;

    if (db->cfgs) {
        for (size_t i = 0; i < db->cfgs->size; ++i) {
            SDbCfg* cfg = benchArrayGet(db->cfgs, i);
            if (cfg->valuestring) {
                pos += snprintf(buf + pos, size - pos, " %s %s", cfg->name, cfg->valuestring);
            } else {
                pos += snprintf(buf + pos, size - pos, " %s %d", cfg->name, cfg->valueint);
            }
            if (pos <= 0 || pos >= size) return -1;
        }
    }

    switch (db->precision) {
        case TSDB_TIME_PRECISION_MILLI:
            pos += snprintf(buf + pos, size - pos, " PRECISION 'ms';\n");
            break;
        case TSDB_TIME_PRECISION_MICRO:
            pos += snprintf(buf + pos, size - pos, " PRECISION 'us';\n");
            break;
        case TSDB_TIME_PRECISION_NANO:
            pos += snprintf(buf + pos, size - pos, " PRECISION 'ns';\n");
            break;
    }

    return (pos > 0 && pos < size) ? pos : -1;
}


static int csvExportCreateDbSql(CsvWriteMeta* write_meta, FILE* fp) {
    char buf[LARGE_BUFF_LEN]    = {0};
    int  ret    = 0;
    int  length = 0;

    length = csvGenCreateDbSql(write_meta->db, buf, sizeof(buf));
    if (length < 0) {
        errorPrint("Failed to generate create db sql, maybe buffer[%zu] not enough.\n", sizeof(buf));
        return -1;
    }

    ret = fwrite(buf, 1, length, fp);
    if (ret != length) {
        errorPrint("Failed to write create db sql: %s. expected written %d but %d.\n",
                buf, length, ret);
        if (ferror(fp)) {
            perror("error");
        }
        return -1;
    }

    return 0;
}


int csvGenCreateStbSql(SDataBase* db, SSuperTable* stb, char* buf, int size) {
    int pos = 0;

    pos += snprintf(buf + pos, size - pos, "CREATE TABLE IF NOT EXISTS ");
    if (pos <= 0 || pos >= size) return -1;

    pos += snprintf(buf + pos, size - pos, g_arguments->escape_character ? "`%s`.`%s`" : "%s.%s", db->dbName, stb->stbName);
    if (pos <= 0 || pos >= size) return -1;

    pos += snprintf(buf + pos, size - pos, " (ts TIMESTAMP");
    if (pos <= 0 || pos >= size) return -1;


    // columns
    for (size_t i = 0; i < stb->cols->size; ++i) {
        Field* col = benchArrayGet(stb->cols, i);

        if (col->type == TSDB_DATA_TYPE_BINARY || col->type == TSDB_DATA_TYPE_NCHAR ||
            col->type == TSDB_DATA_TYPE_VARBINARY || col->type == TSDB_DATA_TYPE_BLOB ||
            col->type == TSDB_DATA_TYPE_GEOMETRY) {
            if (col->type == TSDB_DATA_TYPE_GEOMETRY && col->length < 21) {
                errorPrint("%s() LN%d, geometry filed len must be greater than 21 on %zu.\n", __func__, __LINE__, i);
                return -1;
            }

            pos += snprintf(buf + pos, size - pos, ",%s %s(%d)", col->name, convertDatatypeToString(col->type), col->length);
        } else {
            pos += snprintf(buf + pos, size - pos, ",%s %s", col->name, convertDatatypeToString(col->type));
        }
        if (pos <= 0 || pos >= size) return -1;

        // primary key
        if (stb->primary_key && i == 0) {
            pos += snprintf(buf + pos, size - pos, " %s", PRIMARY_KEY);
            if (pos <= 0 || pos >= size) return -1;
        }

        // compress key
        if (strlen(col->encode) > 0) {
            pos += snprintf(buf + pos, size - pos, " encode '%s'", col->encode);
            if (pos <= 0 || pos >= size) return -1;
        }
        if (strlen(col->compress) > 0) {
            pos += snprintf(buf + pos, size - pos, " compress '%s'", col->compress);
            if (pos <= 0 || pos >= size) return -1;
        }
        if (strlen(col->level) > 0) {
            pos += snprintf(buf + pos, size - pos, " level '%s'", col->level);
            if (pos <= 0 || pos >= size) return -1;
        }
    }

    pos += snprintf(buf + pos, size - pos, ") TAGS (");
    if (pos <= 0 || pos >= size) return -1;


    // tags
    for (size_t i = 0; i < stb->tags->size; ++i) {
        Field* tag = benchArrayGet(stb->tags, i);

        if (i > 0) {
            pos += snprintf(buf + pos, size - pos, ",");
            if (pos <= 0 || pos >= size) return -1;
        }

        if (tag->type == TSDB_DATA_TYPE_BINARY
            || tag->type == TSDB_DATA_TYPE_NCHAR
            || tag->type == TSDB_DATA_TYPE_VARBINARY
            || tag->type == TSDB_DATA_TYPE_GEOMETRY) {

            if (tag->type == TSDB_DATA_TYPE_GEOMETRY && tag->length < 21) {
                errorPrint("%s() LN%d, geometry filed len must be greater than 21 on %zu.\n", __func__, __LINE__, i);
                return -1;
            }

            pos += snprintf(buf + pos, size - pos, "%s %s(%d)", tag->name, convertDatatypeToString(tag->type), tag->length);

        } else {
            pos += snprintf(buf + pos, size - pos, "%s %s", tag->name, convertDatatypeToString(tag->type));
        }
        if (pos <= 0 || pos >= size) return -1;
    }

    pos += snprintf(buf + pos, size - pos, ")");
    if (pos <= 0 || pos >= size) return -1;


    // comment
    if (stb->comment != NULL) {
        pos += snprintf(buf + pos, size - pos," COMMENT '%s'", stb->comment);
        if (pos <= 0 || pos >= size) return -1;
    }

    // delay
    if (stb->delay >= 0) {
        pos += snprintf(buf + pos, size - pos, " DELAY %d", stb->delay);
        if (pos <= 0 || pos >= size) return -1;
    }

    // file factor
    if (stb->file_factor >= 0) {
        pos += snprintf(buf + pos, size - pos, " FILE_FACTOR %f", stb->file_factor / 100.0);
        if (pos <= 0 || pos >= size) return -1;
    }

    // rollup
    if (stb->rollup != NULL) {
        pos += snprintf(buf + pos, size - pos, " ROLLUP(%s)", stb->rollup);
        if (pos <= 0 || pos >= size) return -1;
    }

    // max delay
    if (stb->max_delay != NULL) {
        pos += snprintf(buf + pos, size - pos, " MAX_DELAY %s", stb->max_delay);
        if (pos <= 0 || pos >= size) return -1;
    }

    // watermark
    if (stb->watermark != NULL) {
        pos += snprintf(buf + pos, size - pos, " WATERMARK %s", stb->watermark);
        if (pos <= 0 || pos >= size) return -1;
    }

    bool first_sma = true;
    for (size_t i = 0; i < stb->cols->size; ++i) {
        Field* col = benchArrayGet(stb->cols, i);
        if (col->sma) {
            if (first_sma) {
                pos += snprintf(buf + pos, size - pos, " SMA(%s", col->name);
                first_sma = false;
            } else {
                pos += snprintf(buf + pos, size - pos, ",%s", col->name);
            }
            if (pos <= 0 || pos >= size) return -1;
        }
    }
    if (!first_sma) {
        pos += snprintf(buf + pos, size - pos, ")");
        if (pos <= 0 || pos >= size) return -1;
    }

    pos += snprintf(buf + pos, size - pos, ";\n");
    if (pos <= 0 || pos >= size) return -1;

    // infoPrint("create stable: <%s>.\n", buf);
    return (pos > 0 && pos < size) ? pos : -1;
}


static int csvExportCreateStbSql(CsvWriteMeta* write_meta, FILE* fp) {
    char buf[4096] = {0};
    int  ret    = 0;
    int  length = 0;

    length = csvGenCreateStbSql(write_meta->db, write_meta->stb, buf, sizeof(buf));
    if (length < 0) {
        errorPrint("Failed to generate create stb sql, maybe buffer[%zu] not enough.\n", sizeof(buf));
        return -1;
    }

    ret = fwrite(buf, 1, length, fp);
    if (ret != length) {
        errorPrint("Failed to write create stb sql: %s. expected written %d but %d.\n",
                buf, length, ret);
        if (ferror(fp)) {
            perror("error");
        }
        return -1;
    }

    return 0;
}


static int csvExportCreateSql(CsvWriteMeta* write_meta) {
    char fullname[MAX_PATH_LEN] = {0};
    int   ret       = 0;
    int   length    = 0;
    FILE* fp        = NULL;


    length = snprintf(fullname, sizeof(fullname), "%s%s.txt", g_arguments->output_path, "create_stmt");
    if (length <= 0 || length >= sizeof(fullname)) {
        return -1;
    }

    fp = fopen(fullname, "w");
    if (!fp) {
        return -1;
    }


    // export db
    ret = csvExportCreateDbSql(write_meta, fp);
    if (ret < 0) {
        goto end;
    }

    // export stb
    ret = csvExportCreateStbSql(write_meta, fp);
    if (ret < 0) {
        goto end;
    }

    succPrint("Export create sql to file: %s successfully.\n", fullname);

end:
    if (fp) {
        fclose(fp);
    }

    return ret;
}


static int csvInitWriteMeta(SDataBase* db, SSuperTable* stb, CsvWriteMeta* write_meta) {
    write_meta->naming_type         = csvGetFileNamingType(stb);
    write_meta->total_threads       = 1;
    write_meta->csv_header_length   = 0;
    write_meta->db                  = db;
    write_meta->stb                 = stb;
    write_meta->start_ts            = stb->startTimestamp;
    write_meta->end_ts              = stb->startTimestamp + stb->timestamp_step * stb->insertRows;
    write_meta->ts_step             = stb->timestamp_step * stb->insertRows;
    write_meta->interlace_step      = stb->timestamp_step * stb->interlaceRows;

    int ret = csvGenCsvHeader(write_meta);
    if (ret < 0) {
        errorPrint("Failed to generate csv header data. database: %s, super table: %s, naming type: %d.\n",
                db->dbName, stb->stbName, write_meta->naming_type);
        return -1;
    }

    switch (write_meta->naming_type) {
        case CSV_NAMING_I_SINGLE: {
            (void)snprintf(write_meta->mode, sizeof(write_meta->mode), "interlace::normal");
            break;
        }
        case CSV_NAMING_I_TIME_SLICE: {
            (void)snprintf(write_meta->mode, sizeof(write_meta->mode), "interlace::time-slice");
            csvCalcTimestampStep(write_meta);
            break;
        }
        case CSV_NAMING_B_THREAD: {
            write_meta->total_threads = MIN(g_arguments->nthreads, stb->childTblCount);
            (void)snprintf(write_meta->mode, sizeof(write_meta->mode), "batch[%zu]::normal", write_meta->total_threads);
            csvGenThreadFormatter(write_meta);
            break;
        }
        case CSV_NAMING_B_THREAD_TIME_SLICE: {
            write_meta->total_threads = MIN(g_arguments->nthreads, stb->childTblCount);
            (void)snprintf(write_meta->mode, sizeof(write_meta->mode), "batch[%zu]::time-slice", write_meta->total_threads);
            csvGenThreadFormatter(write_meta);
            csvCalcTimestampStep(write_meta);
            break;
        }
        default: {
            write_meta->naming_type = CSV_NAMING_I_SINGLE;
            break;
        }
    }

    return 0;
}


static void csvInitThreadMeta(CsvWriteMeta* write_meta, uint32_t thread_id, CsvThreadMeta* thread_meta) {
    SDataBase*      db      = write_meta->db;
    SSuperTable*    stb     = write_meta->stb;

    thread_meta->ctb_start_idx      = 0;
    thread_meta->ctb_end_idx        = 0;
    thread_meta->ctb_count          = 0;
    thread_meta->start_secs         = 0;
    thread_meta->end_secs           = 0;
    thread_meta->start_ts           = write_meta->start_ts;
    thread_meta->end_ts             = write_meta->end_ts;
    thread_meta->thread_id          = thread_id;
    thread_meta->output_header      = false;
    thread_meta->tags_buf_size      = 0;
    thread_meta->tags_buf_array     = NULL;
    thread_meta->cols_buf           = NULL;

    csvCalcCtbRange(thread_meta, write_meta->total_threads, stb->childTblFrom, stb->childTblCount);

    switch (write_meta->naming_type) {
        case CSV_NAMING_I_SINGLE:
        case CSV_NAMING_B_THREAD: {
            break;
        }
        case CSV_NAMING_I_TIME_SLICE:
        case CSV_NAMING_B_THREAD_TIME_SLICE: {
            thread_meta->start_secs = csvGetStartSeconds(db->precision, stb->startTimestamp, stb->csv_ts_format);
            thread_meta->end_secs   = thread_meta->start_secs + write_meta->stb->csv_ts_intv_secs;
            csvCalcSliceTimestamp(write_meta, thread_meta);
            break;
        }
        default: {
            break;
        }
    }

    return;
}


static void csvUpdateSliceRange(CsvWriteMeta* write_meta, CsvThreadMeta* thread_meta, int64_t last_end_ts) {
    SDataBase*      db      = write_meta->db;
    SSuperTable*    stb     = write_meta->stb;

    switch (write_meta->naming_type) {
        case CSV_NAMING_I_SINGLE:
        case CSV_NAMING_B_THREAD: {
            break;
        }
        case CSV_NAMING_I_TIME_SLICE:
        case CSV_NAMING_B_THREAD_TIME_SLICE: {
            thread_meta->start_secs = csvGetStartSeconds(db->precision, last_end_ts, stb->csv_ts_format);
            thread_meta->end_secs   = thread_meta->start_secs + write_meta->stb->csv_ts_intv_secs;
            csvCalcSliceTimestamp(write_meta, thread_meta);
            break;
        }
        default: {
            break;
        }
    }

    return;
}


static const char* csvGetGzipFilePrefix(CsvCompressionLevel csv_compress_level) {
    if (csv_compress_level == CSV_COMPRESS_NONE) {
        return "";
    } else {
        return ".gz";
    }
}


static int csvGetFileFullname(CsvWriteMeta* write_meta, CsvThreadMeta* thread_meta, char* fullname, size_t size) {
    char thread_buf[SMALL_BUFF_LEN];
    char start_time_buf[MIDDLE_BUFF_LEN];
    char end_time_buf[MIDDLE_BUFF_LEN];
    int ret = -1;
    const char* base_path   = g_arguments->output_path;
    const char* file_prefix = write_meta->stb->csv_file_prefix;
    const char* gzip_suffix = csvGetGzipFilePrefix(write_meta->stb->csv_compress_level);

    switch (write_meta->naming_type) {
        case CSV_NAMING_I_SINGLE: {
            ret = snprintf(fullname, size, "%s%s.csv%s", base_path, file_prefix, gzip_suffix);
            break;
        }
        case CSV_NAMING_I_TIME_SLICE: {
            csvConvertTime2String(thread_meta->start_secs, write_meta->stb->csv_ts_format, start_time_buf, sizeof(start_time_buf));
            csvConvertTime2String(thread_meta->end_secs, write_meta->stb->csv_ts_format, end_time_buf, sizeof(end_time_buf));
            ret = snprintf(fullname, size, "%s%s_%s_%s.csv%s", base_path, file_prefix, start_time_buf, end_time_buf, gzip_suffix);
            break;
        }
        case CSV_NAMING_B_THREAD: {
            (void)snprintf(thread_buf, sizeof(thread_buf), write_meta->thread_formatter, thread_meta->thread_id);
            ret = snprintf(fullname, size, "%s%s_%s.csv%s", base_path, file_prefix, thread_buf, gzip_suffix);
            break;
        }
        case CSV_NAMING_B_THREAD_TIME_SLICE: {
            (void)snprintf(thread_buf, sizeof(thread_buf), write_meta->thread_formatter, thread_meta->thread_id);
            csvConvertTime2String(thread_meta->start_secs, write_meta->stb->csv_ts_format, start_time_buf, sizeof(start_time_buf));
            csvConvertTime2String(thread_meta->end_secs, write_meta->stb->csv_ts_format, end_time_buf, sizeof(end_time_buf));
            ret = snprintf(fullname, size, "%s%s_%s_%s_%s.csv%s", base_path, file_prefix, thread_buf, start_time_buf, end_time_buf, gzip_suffix);
            break;
        }
        default: {
            ret = -1;
            break;
        }
    }

    return (ret > 0 && (size_t)ret < size) ? 0 : -1;
}


static int64_t csvCalcSliceBatchTimestamp(CsvWriteMeta* write_meta, int64_t slice_cur_ts, int64_t slice_end_ts) {
    int64_t slice_batch_ts = 0;

    switch (write_meta->naming_type) {
        case CSV_NAMING_I_SINGLE:
        case CSV_NAMING_I_TIME_SLICE: {
            slice_batch_ts = MIN(slice_cur_ts + write_meta->interlace_step, slice_end_ts);
            break;
        }
        case CSV_NAMING_B_THREAD:
        case CSV_NAMING_B_THREAD_TIME_SLICE: {
            slice_batch_ts = slice_end_ts;
            break;
        }
        default: {
            break;
        }
    }

    return slice_batch_ts;
}


static int csvGenRowFields(char* buf, int size, SSuperTable* stb, int fields_cate, int64_t* k) {
    int     pos             = 0;
    BArray* fields          = NULL;
    int16_t field_count     = 0;
    char*   binanry_prefix  = stb->binaryPrefex ? stb->binaryPrefex : "";
    char*   nchar_prefix    = stb->ncharPrefex ? stb->ncharPrefex : "";

    if (!buf || !stb || !k || size <= 0) {
        return -1;
    }

    if (fields_cate == GEN_ROW_FIELDS_TAG) {
        fields      = stb->tags;
        field_count = stb->tags->size;
    } else {
        fields      = stb->cols;
        field_count = stb->cols->size;
    }

    for (uint16_t i = 0; i < field_count; ++i) {
        Field* field = benchArrayGet(fields, i);
        char* prefix = "";
        if (field->type == TSDB_DATA_TYPE_BINARY || field->type == TSDB_DATA_TYPE_VARBINARY ||
            field->type == TSDB_DATA_TYPE_BLOB) {
            prefix = binanry_prefix;
        } else if (field->type == TSDB_DATA_TYPE_NCHAR) {
            prefix = nchar_prefix;
        }
        pos += dataGenByField(field, buf, pos, prefix, k, "");
    }

    return pos;
}


static int csvGenRowTagData(char* buf, int size, SSuperTable* stb, int64_t index, int64_t* k) {
    if (!buf || !stb || !k || size <= 0) {
        return -1;
    }

    // tbname
    int pos = snprintf(buf, size, ",'%s%"PRId64"'", stb->childTblPrefix, index);

    // tags
    pos += csvGenRowFields(buf + pos, size - pos, stb, GEN_ROW_FIELDS_TAG, k);

    return (pos > 0 && pos < size) ? pos : -1;
}


static int csvGenRowColData(char* buf, int size, SSuperTable* stb, int64_t ts, int32_t precision, int64_t *k) {
    char ts_fmt[128] = {0};
    toolsFormatTimestamp(ts_fmt, ts, precision);
    int pos = snprintf(buf, size, "\'%s\'", ts_fmt);

    // columns
    pos += csvGenRowFields(buf + pos, size - pos, stb, GEN_ROW_FIELDS_COL, k);
    return (pos > 0 && pos < size) ? pos : -1;
}


static void csvFreeCtbTagData(CsvThreadMeta* thread_meta, CsvRowTagsBuf* tags_buf_array) {
    if (!thread_meta || !tags_buf_array) {
        return;
    }

    for (uint64_t i = 0 ; i < thread_meta->ctb_count; ++i) {
        char* tags_buf = tags_buf_array[i].buf;
        if (tags_buf) {
            tmfree(tags_buf);
        } else {
            break;
        }
    }
    tmfree(tags_buf_array);
    return;
}


static CsvRowTagsBuf* csvGenCtbTagData(CsvWriteMeta* write_meta, CsvThreadMeta* thread_meta) {
    SSuperTable* stb = write_meta->stb;
    int ret    = 0;
    int64_t tk = 0;

    if (!write_meta || !thread_meta) {
        return NULL;
    }

    CsvRowTagsBuf* tags_buf_array = (CsvRowTagsBuf*)benchCalloc(thread_meta->ctb_count, sizeof(CsvRowTagsBuf), true);
    if (!tags_buf_array) {
        return NULL;
    }

    char* tags_buf      = NULL; 
    int   tags_buf_size = TSDB_TABLE_NAME_LEN + stb->lenOfTags + stb->tags->size; 
    for (uint64_t i = 0; i < thread_meta->ctb_count; ++i) {
        tags_buf = benchCalloc(1, tags_buf_size, true);
        if (!tags_buf) {
            goto error;
        }

        tags_buf_array[i].buf      = tags_buf;

        ret = csvGenRowTagData(tags_buf, tags_buf_size, stb, thread_meta->ctb_start_idx + i, &tk);
        if (ret <= 0) {
            goto error;
        }

        tags_buf_array[i].length  = ret;
    }
    thread_meta->tags_buf_size  = tags_buf_size;

    return tags_buf_array;

error:
    csvFreeCtbTagData(thread_meta, tags_buf_array);
    return NULL;
}


static CsvFileHandle* csvOpen(const char* filename, CsvCompressionLevel compress_level) {
    CsvFileHandle* fhdl = NULL;
    bool failed = false;
    
    fhdl = (CsvFileHandle*)benchCalloc(1, sizeof(CsvFileHandle), false);
    if (!fhdl) {
        errorPrint("Failed to malloc csv file handle. filename: %s, compress level: %d.\n",
                filename, compress_level);
        return NULL;
    }

    if (compress_level == CSV_COMPRESS_NONE) {
        fhdl->handle.fp = fopen(filename, "w");
        failed = (!fhdl->handle.fp);
    } else {
        char mode[TINY_BUFF_LEN];
        (void)snprintf(mode, sizeof(mode), "wb%d", compress_level);
        fhdl->handle.gf = gzopen(filename, mode);
        failed = (!fhdl->handle.gf);
    }

    if (failed) {
        tmfree(fhdl);
        errorPrint("Failed to open csv file handle. filename: %s, compress level: %d.\n",
                filename, compress_level);
        return NULL;
    } else {
        fhdl->filename = filename;
        fhdl->compress_level = compress_level;
        fhdl->result = CSV_ERR_OK;
        return fhdl;
    }
}


static CsvIoError csvWrite(CsvFileHandle* fhdl, const char* buf, size_t size) {
    if (fhdl->compress_level == CSV_COMPRESS_NONE) {
        size_t ret = fwrite(buf, 1, size, fhdl->handle.fp);
        if (ret != size) {
            errorPrint("Failed to write csv file: %s. expected written %zu but %zu.\n",
                    fhdl->filename, size, ret);
            if (ferror(fhdl->handle.fp)) {
                perror("error");
            }
            fhdl->result = CSV_ERR_WRITE_FAILED;
            return CSV_ERR_WRITE_FAILED;
        }
    } else {
        int ret = gzwrite(fhdl->handle.gf, buf, size);
        if (ret != size) {
            errorPrint("Failed to write csv file: %s. expected written %zu but %d.\n",
                    fhdl->filename, size, ret);
            int errnum;
            const char* errmsg = gzerror(fhdl->handle.gf, &errnum);
            errorPrint("gzwrite error: %s\n", errmsg);
            fhdl->result = CSV_ERR_WRITE_FAILED;
            return CSV_ERR_WRITE_FAILED;
        }
    }

    return CSV_ERR_OK;
}


static void csvClose(CsvFileHandle* fhdl) {
    if (!fhdl) {
        return;
    }

    if (fhdl->compress_level == CSV_COMPRESS_NONE) {
        if (fhdl->handle.fp) {
            fclose(fhdl->handle.fp);
            fhdl->handle.fp = NULL;
        }
    } else {
        if (fhdl->handle.gf) {
            gzclose(fhdl->handle.gf);
            fhdl->handle.gf = NULL;
        }
    }

    tmfree(fhdl);
}


static int csvWriteFile(CsvFileHandle* fhdl, uint64_t ctb_idx, int64_t cur_ts, int64_t* ck, CsvWriteMeta* write_meta, CsvThreadMeta* thread_meta) {
    SDataBase*   db  = write_meta->db;
    SSuperTable* stb = write_meta->stb;
    CsvRowTagsBuf* tags_buf_array  = thread_meta->tags_buf_array;
    CsvRowTagsBuf* tags_buf        = &tags_buf_array[ctb_idx];
    CsvRowColsBuf* cols_buf        = thread_meta->cols_buf;
    int ret = 0;


    ret = csvGenRowColData(cols_buf->buf, cols_buf->buf_size, stb, cur_ts, db->precision, ck);
    if (ret <= 0) {
        errorPrint("Failed to generate csv column data. database: %s, super table: %s, naming type: %d, thread index: %zu, ctb index: %" PRIu64 ".\n",
                db->dbName, stb->stbName, write_meta->naming_type, thread_meta->thread_id, ctb_idx);
        return -1;
    }

    cols_buf->length = ret;

    // write header
    if (thread_meta->output_header) {
        ret = csvWrite(fhdl, write_meta->csv_header, write_meta->csv_header_length);
        if (ret != CSV_ERR_OK) {
            errorPrint("Failed to write csv header data. database: %s, super table: %s, naming type: %d, thread index: %zu, ctb index: %" PRIu64 ".\n",
                    db->dbName, stb->stbName, write_meta->naming_type, thread_meta->thread_id, ctb_idx);
            return -1;
        }

        thread_meta->output_header = false;
    }

    // write columns
    ret = csvWrite(fhdl, cols_buf->buf, cols_buf->length);
    if (ret != CSV_ERR_OK) {
        errorPrint("Failed to write csv column data. database: %s, super table: %s, naming type: %d, thread index: %zu, ctb index: %" PRIu64 ".\n",
                db->dbName, stb->stbName, write_meta->naming_type, thread_meta->thread_id, ctb_idx);
        return -1;
    }

    // write tags
    ret = csvWrite(fhdl, tags_buf->buf, tags_buf->length);
    if (ret != CSV_ERR_OK) {
        errorPrint("Failed to write csv tag data. database: %s, super table: %s, naming type: %d, thread index: %zu, ctb index: %" PRIu64 ".\n",
                db->dbName, stb->stbName, write_meta->naming_type, thread_meta->thread_id, ctb_idx);
        return -1;
    }

    // write line break
    ret = csvWrite(fhdl, "\n", 1);
    if (ret != CSV_ERR_OK) {
        errorPrint("Failed to write csv line break data. database: %s, super table: %s, naming type: %d, thread index: %zu, ctb index: %" PRIu64 ".\n",
                db->dbName, stb->stbName, write_meta->naming_type, thread_meta->thread_id, ctb_idx);
        return -1;
    }

    return 0;
}


static void* csvGenStbThread(void* arg) {
    CsvThreadArgs*  thread_arg  = (CsvThreadArgs*)arg;
    CsvWriteMeta*   write_meta  = thread_arg->write_meta;
    CsvThreadMeta*  thread_meta = &thread_arg->thread_meta;
    SDataBase*      db          = write_meta->db;
    SSuperTable*    stb         = write_meta->stb;

    int64_t  cur_ts             = 0;
    int64_t  slice_cur_ts       = 0;
    int64_t  slice_end_ts       = 0;
    int64_t  slice_batch_ts     = 0;
    int64_t  slice_ctb_cur_ts   = 0;
    int64_t  ck                 = 0;
    uint64_t ctb_idx            = 0;
    int      ret                = 0;
    CsvFileHandle*    fhdl      = NULL;
    char fullname[MAX_PATH_LEN] = {0};

    uint64_t total_rows         = 0;
    uint64_t pre_total_rows     = 0;
    uint64_t file_rows          = 0;
    int64_t  start_print_ts     = 0;
    int64_t  pre_print_ts       = 0;
    int64_t  cur_print_ts       = 0;
    int64_t  print_ts_elapse    = 0;


    // tags buffer
    CsvRowTagsBuf* tags_buf_array = csvGenCtbTagData(write_meta, thread_meta);
    if (!tags_buf_array) {
        errorPrint("Failed to generate csv tag data. database: %s, super table: %s, naming type: %d, thread index: %zu.\n",
                db->dbName, stb->stbName, write_meta->naming_type, thread_meta->thread_id);
        return NULL;
    }

    // column buffer
    int    buf_size = stb->lenOfCols + stb->cols->size;
    char*  buf = (char*)benchCalloc(1, buf_size, true);
    if (!buf) {
        errorPrint("Failed to malloc csv column buffer. database: %s, super table: %s, naming type: %d, thread index: %zu.\n",
                db->dbName, stb->stbName, write_meta->naming_type, thread_meta->thread_id);
        goto end;
    }

    CsvRowColsBuf cols_buf = {
        .buf        = buf,
        .buf_size   = buf_size,
        .length     = 0
    };

    thread_meta->tags_buf_array  = tags_buf_array;
    thread_meta->cols_buf        = &cols_buf;
    start_print_ts = toolsGetTimestampMs();

    cur_ts = write_meta->start_ts;
    while (cur_ts < write_meta->end_ts) {
        // get filename
        ret = csvGetFileFullname(write_meta, thread_meta, fullname, sizeof(fullname));
        if (ret < 0) {
            errorPrint("Failed to generate csv filename. database: %s, super table: %s, naming type: %d, thread index: %zu.\n",
                    db->dbName, stb->stbName, write_meta->naming_type, thread_meta->thread_id);
            goto end;
        }

        // create fd
        fhdl = csvOpen(fullname, stb->csv_compress_level);
        if (fhdl == NULL) {
            errorPrint("Failed to create csv file. thread index: %zu, file: %s, errno: %d, strerror: %s.\n",
                    thread_meta->thread_id, fullname, errno, strerror(errno));
            goto end;
        }


        thread_meta->output_header = stb->csv_output_header;
        slice_cur_ts = cur_ts;
        slice_end_ts = MIN(thread_meta->end_ts, write_meta->end_ts);
        file_rows    = 0;
        pre_print_ts = toolsGetTimestampMs();

        infoPrint("thread[%zu] begin to write csv file: %s.\n", thread_meta->thread_id, fullname);

        // write data
        while (slice_cur_ts < slice_end_ts) {
            slice_batch_ts = csvCalcSliceBatchTimestamp(write_meta, slice_cur_ts, slice_end_ts);

            for (ctb_idx = 0; ctb_idx < thread_meta->ctb_count; ++ctb_idx) {
                for (slice_ctb_cur_ts = slice_cur_ts; slice_ctb_cur_ts < slice_batch_ts; slice_ctb_cur_ts += write_meta->stb->timestamp_step) {
                    ret = csvWriteFile(fhdl, ctb_idx, slice_ctb_cur_ts, &ck, write_meta, thread_meta);
                    if (ret) {
                        errorPrint("Failed to write csv file. thread index: %zu, file: %s, errno: %d, strerror: %s.\n",
                                thread_meta->thread_id, fullname, errno, strerror(errno));
                        csvClose(fhdl);
                        goto end;
                    }

                    ck          += 1;
                    total_rows  += 1;
                    file_rows   += 1;

                    cur_print_ts    = toolsGetTimestampMs();
                    print_ts_elapse = cur_print_ts - pre_print_ts;
                    if (print_ts_elapse > 30000) {
                        infoPrint("thread[%zu] has currently inserted rows: %" PRIu64 ", period insert rate: %.2f rows/s.\n",
                                thread_meta->thread_id, total_rows, (total_rows - pre_total_rows) * 1000.0 / print_ts_elapse);

                        pre_print_ts    = cur_print_ts;
                        pre_total_rows  = total_rows;
                    }


                    if (g_arguments->terminate) {
                        csvClose(fhdl);
                        goto end;
                    }
                }
            }
            
            slice_cur_ts = slice_batch_ts;
        }

        csvClose(fhdl);
        cur_ts = thread_meta->end_ts;
        csvUpdateSliceRange(write_meta, thread_meta, slice_end_ts);
    }

    cur_print_ts    = toolsGetTimestampMs();
    print_ts_elapse = cur_print_ts - start_print_ts;

    succPrint("thread [%zu] has completed inserting rows: %" PRIu64 ", insert rate %.2f rows/s.\n",
            thread_meta->thread_id, total_rows, total_rows * 1000.0 / print_ts_elapse);

end:
    thread_meta->total_rows = total_rows;
    csvFreeCtbTagData(thread_meta, tags_buf_array);
    tmfree(buf);
    return NULL;
}


static int csvGenStbProcess(SDataBase* db, SSuperTable* stb) {
    int      ret        = 0;
    bool     prompt     = true;
    uint64_t total_rows = 0;
    int64_t  start_ts   = 0;
    int64_t  ts_elapse  = 0;

    CsvWriteMeta*   write_meta  = NULL;
    CsvThreadArgs*  args        = NULL;
    pthread_t*      pids        = NULL;


    write_meta = benchCalloc(1, sizeof(CsvWriteMeta), false);
    if (!write_meta) {
        ret = -1;
        goto end;
    }

    ret = csvInitWriteMeta(db, stb, write_meta);
    if (ret < 0) {
        ret = -1;
        goto end;
    }

    infoPrint("export csv mode: %s.\n", write_meta->mode);

    args = benchCalloc(write_meta->total_threads, sizeof(CsvThreadArgs), false);
    if (!args) {
        ret = -1;
        goto end;
    }

    pids = benchCalloc(write_meta->total_threads, sizeof(pthread_t), false);
    if (!pids) {
        ret = -1;
        goto end;
    }

    start_ts = toolsGetTimestampMs();
    for (uint32_t i = 0; (i < write_meta->total_threads && !g_arguments->terminate); ++i) {
        CsvThreadArgs* arg = &args[i];
        arg->write_meta  = write_meta;
        csvInitThreadMeta(write_meta, i + 1, &arg->thread_meta);

        ret = pthread_create(&pids[i], NULL, csvGenStbThread, arg);
        if (ret) {
            perror("Failed to create thread"); 
            goto end;
        }
    }

    // wait threads
    for (uint32_t i = 0; i < write_meta->total_threads; ++i) {
        if (g_arguments->terminate && prompt) {
            infoPrint("Operation cancelled by user, exiting gracefully...\n");
            prompt = false;
        }

        infoPrint("pthread_join %u ...\n", i);
        pthread_join(pids[i], NULL);
    }

    // statistics
    total_rows = 0;
    for (uint32_t i = 0; i < write_meta->total_threads; ++i) {
        CsvThreadArgs* arg = &args[i];
        CsvThreadMeta* thread_meta = &arg->thread_meta;
        total_rows += thread_meta->total_rows;
    }

    ts_elapse = toolsGetTimestampMs() - start_ts;
    if (ts_elapse > 0) {
        succPrint("Spent %.6f seconds to insert rows: %" PRIu64 " with %zu thread(s) into %s, at a rate of %.2f rows/s.\n",
                ts_elapse / 1000.0, total_rows, write_meta->total_threads, g_arguments->output_path, total_rows * 1000.0 / ts_elapse);
    }

    // export create db/stb sql
    ret = csvExportCreateSql(write_meta);

end:
    tmfree(pids);
    tmfree(args);
    tmfree(write_meta);
    return ret;
}


static void csvGenPrepare(SDataBase* db, SSuperTable* stb) {
    stb->lenOfTags = accumulateRowLen(stb->tags, stb->iface);
    stb->lenOfCols = accumulateRowLen(stb->cols, stb->iface);

    if (stb->childTblTo) {
        stb->childTblCount = stb->childTblTo - stb->childTblFrom;
    }

    return;
}


static int csvGenStb(SDataBase* db, SSuperTable* stb) {
    // prepare
    csvGenPrepare(db, stb);

    return csvGenStbProcess(db, stb);
}


static int csvWriteThread() {
    for (size_t i = 0; i < g_arguments->databases->size && !g_arguments->terminate; ++i) {
        // database
        SDataBase* db = benchArrayGet(g_arguments->databases, i);
        if (db->superTbls) {
            for (size_t j = 0; j < db->superTbls->size && !g_arguments->terminate; ++j) {
                // stb
                SSuperTable* stb = benchArrayGet(db->superTbls, j);
                if (stb->insertRows == 0) {
                    continue;
                }

                // parsing parameters
                int ret = csvParseStbParameter(stb);
                if (ret != 0) {
                    errorPrint("Failed to parse csv parameter. database: %s, super table: %s, error code: %d.\n",
                            db->dbName, stb->stbName, ret);
                    return -1;
                }

                // gen csv
                ret = csvGenStb(db, stb);
                if(ret != 0) {
                    errorPrint("Failed to generate csv files. database: %s, super table: %s, error code: %d.\n",
                            db->dbName, stb->stbName, ret);
                    return -1;
                }
            }
        }
    }

    return 0;
}


int csvTestProcess() {
    // parsing parameters
    if (csvParseParameter() != 0) {
        return -1;
    }

    infoPrint("Starting to output data to csv files in directory: %s ...\n", g_arguments->output_path);
    int64_t start = toolsGetTimestampMs();
    int ret = csvWriteThread();
    if (ret != 0) {
        return -1;
    }
    int64_t elapse = toolsGetTimestampMs() - start;
    infoPrint("Generating csv files in directory: %s has been completed. Time elapsed: %.3f seconds\n",
            g_arguments->output_path, elapse / 1000.0);
    return 0;
}
