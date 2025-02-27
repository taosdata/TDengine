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



static time_t csvGetStartSeconds(int precision, int64_t start_ts) {
    time_t start_seconds = 0;

    if (precision == TSDB_TIME_PRECISION_MICRO) {
        start_seconds = start_ts / 1000000L;
    } else if (precision == TSDB_TIME_PRECISION_NANO) {
        start_seconds = start_ts / 1000000000L;
    } else {
        start_seconds = start_ts / 1000L;
    }
    return start_seconds;
}


static void csvConvertTime2String(time_t time_value, char* time_buf, size_t buf_size) {
    struct tm tm_result;
    char *old_locale = setlocale(LC_TIME, "C");
#ifdef _WIN32
    gmtime_s(&tm_result, &time_value);
#else
    gmtime_r(&time_value, &tm_result);
#endif
    strftime(time_buf, buf_size, g_arguments->csv_ts_format, &tm_result);
    if (old_locale) {
        setlocale(LC_TIME, old_locale);
    }
    return;
}


static CsvNamingType csvGetFileNamingType(SSuperTable* stb) {
    if (stb->interlaceRows > 0) {
        if (g_arguments->csv_ts_format) {
            return CSV_NAMING_I_TIME_SLICE;
        } else {
            return CSV_NAMING_I_SINGLE;
        }
    } else {
        if (g_arguments->csv_ts_format) {
            return CSV_NAMING_B_THREAD_TIME_SLICE;
        } else {
            return CSV_NAMING_B_THREAD;
        }
    }
}


static void csvCalcTimestampStep(CsvWriteMeta* meta) {
    time_t ts_step = 0;

    if (meta->db->precision == TSDB_TIME_PRECISION_MICRO) {
        ts_step = g_arguments->csv_ts_intv_secs * 1000000L;
    } else if (db->precision == TSDB_TIME_PRECISION_NANO) {
        ts_step = g_arguments->csv_ts_intv_secs * 1000000000L;
    } else {
        ts_step = g_arguments->csv_ts_intv_secs * 1000L;
    }
    meta->ts_step = ts_step;
    return;
}


static void csvCalcCtbRange(CsvThreadMeta* meta, size_t total_threads, int64_t ctb_offset, int64_t ctb_count) {
    uint64_t  ctb_start_idx = 0;
    uint64_t  ctb_end_idx   = 0;
    size_t    tid_idx       = meta->thread_id - 1;
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

    meta->ctb_start_idx = ctb_start_idx;
    meta->ctb_end_idx   = ctb_end_idx;
    meta->ctb_count     = ctb_count;
    return;
}


static void csvGenThreadFormatter(CsvWriteMeta* meta) {
    int digits = 0;

    if (meta->total_threads == 0) {
        digits = 1;
    } else {
        for (int n = meta->total_threads; n > 0; n /= 10) {
            digits++;
        }
    }

    if (digits <= 1) {
        (void)snprintf(meta->thread_formatter, sizeof(meta->thread_formatter), "%%d");
    } else {
        (void)snprintf(meta->thread_formatter, sizeof(meta->thread_formatter), "%%0%dd", digits);
    }
    return;
}


static int csvGenCsvHeader(CsvWriteMeta* write_meta) {
    SDataBase*    db   = write_meta->db;
    SSuperTable*  stb  = write_meta->stb;
    char*         buf  = write_meta->csv_header;
    int           pos  = 0;
    int           size = sizeof(write_meta->csv_header);

    if (!g_arguments->csv_output_header) {
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
    pos += snprintf(buf + pos, size - pos, ",%s", g_arguments->csv_tbname_alias);

    // tags
    for (size_t i = 0; i < stb->tags->size; ++i) {
        Field* tag = benchArrayGet(stb->tags, i);
        pos += snprintf(buf + pos, size - pos, ",%s", tag->name);
    }

    write_meta->csv_header_length = (pos > 0 && pos < size) ? pos : 0;
    return (pos > 0 && pos < size) ? 0 : -1;
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

    switch (meta.naming_type) {
        case CSV_NAMING_I_SINGLE: {
            break;
        }
        case CSV_NAMING_I_TIME_SLICE: {
            csvCalcTimestampStep(write_meta);
            break;
        }
        case CSV_NAMING_B_THREAD: {
            meta.total_threads = g_arguments->nthreads;
            csvGenThreadFormatter(write_meta);
            break;
        }
        case CSV_NAMING_B_THREAD_TIME_SLICE: {
            meta.total_threads = g_arguments->nthreads;
            csvGenThreadFormatter(write_meta);
            csvCalcTimestampStep(write_meta);
            break;
        }
        default: {
            meta.naming_type = CSV_NAMING_I_SINGLE;
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
    thread_meta->thread_id          = thread_id;
    thread_meta->output_header      = false;
    thread_meta->tags_buf_size      = 0;
    thread_meta->tags_buf_bucket    = NULL;
    thread_meta->cols_buf           = NULL;

    csvCalcCtbRange(write_meta, write_meta->total_threads, stb->childTblFrom, stb->childTblCount);

    switch (write_meta->naming_type) {
        case CSV_NAMING_I_SINGLE:
        case CSV_NAMING_B_THREAD: {
            break;
        }
        case CSV_NAMING_I_TIME_SLICE:
        case CSV_NAMING_B_THREAD_TIME_SLICE: {
            thread_meta->start_secs = csvGetStartSeconds(db->precision, stb->startTimestamp);
            thread_meta->end_secs   = thread_meta->start_secs + g_arguments->csv_ts_intv_secs;
            break;
        }
        default: {
            thread_meta->naming_type = CSV_NAMING_I_SINGLE;
            break;
        }
    }

    return;
}


static void csvUpdateSliceRange(CsvWriteMeta* write_meta, CsvThreadMeta* thread_meta, int64_t last_end_ts) {
    SDataBase*      db      = write_meta->db;

    switch (write_meta->naming_type) {
        case CSV_NAMING_I_SINGLE:
        case CSV_NAMING_B_THREAD: {
            break;
        }
        case CSV_NAMING_I_TIME_SLICE:
        case CSV_NAMING_B_THREAD_TIME_SLICE: {
            thread_meta->start_secs = csvGetStartSeconds(db->precision, last_end_ts);
            thread_meta->end_secs   = thread_meta.start_secs + g_arguments->csv_ts_intv_secs;
            break;
        }
        default: {
            break;
        }
    }

    return;
}


static int csvGetFileFullname(CsvWriteMeta* write_meta, CsvThreadMeta* thread_meta, char* fullname, size_t size) {
    char thread_buf[SMALL_BUFF_LEN];
    char start_time_buf[MIDDLE_BUFF_LEN];
    char end_time_buf[MIDDLE_BUFF_LEN];
    int ret = -1;
    const char* base_path   = g_arguments->output_path;
    const char* file_prefix = g_arguments->csv_file_prefix;

    switch (meta->naming_type) {
        case CSV_NAMING_I_SINGLE: {
            ret = snprintf(fullname, size, "%s%s.csv", base_path, file_prefix);
            break;
        }
        case CSV_NAMING_I_TIME_SLICE: {
            csvConvertTime2String(meta->start_secs, start_time_buf, sizeof(start_time_buf));
            csvConvertTime2String(meta->end_secs, end_time_buf, sizeof(end_time_buf));
            ret = snprintf(fullname, size, "%s%s_%s_%s.csv", base_path, file_prefix, start_time_buf, end_time_buf);
            break;
        }
        case CSV_NAMING_B_THREAD: {
            (void)snprintf(thread_buf, sizeof(thread_buf), meta->thread_formatter, meta->thread_id);
            ret = snprintf(fullname, size, "%s%s_%s.csv", base_path, file_prefix, thread_buf);
            break;
        }
        case CSV_NAMING_B_THREAD_TIME_SLICE: {
            (void)snprintf(thread_buf, sizeof(thread_buf), meta->thread_formatter, meta->thread_id);
            csvConvertTime2String(meta->start_secs, start_time_buf, sizeof(start_time_buf));
            csvConvertTime2String(meta->end_secs, end_time_buf, sizeof(end_time_buf));
            ret = snprintf(fullname, size, "%s%s_%s_%s_%s.csv", base_path, file_prefix, thread_buf, start_time_buf, end_time_buf);
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
        if(field->type == TSDB_DATA_TYPE_BINARY || field->type == TSDB_DATA_TYPE_VARBINARY) {
            prefix = binanry_prefix;
        } else if(field->type == TSDB_DATA_TYPE_NCHAR) {
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
    int pos = snprintf(buf, size, "\'%s%"PRId64"\'", stb->childTblPrefix, index);

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


static CsvRowTagsBuf* csvGenCtbTagData(CsvWriteMeta* write_meta, CsvThreadMeta* thread_meta) {
    SSuperTable* stb = write_meta->stb;
    int ret    = 0;
    int64_t tk = 0;

    if (!write_meta || !thread_meta) {
        return NULL;
    }

    CsvRowTagsBuf* tags_buf_bucket = (CsvRowTagsBuf*)benchCalloc(thread_meta->ctb_count, sizeof(CsvRowTagsBuf), true);
    if (!tags_buf_bucket) {
        return NULL;
    }

    char* tags_buf      = NULL; 
    int   tags_buf_size = TSDB_TABLE_NAME_LEN + stb->lenOfTags + stb->tags->size; 
    for (uint64_t i = 0; i < thread_meta->ctb_count; ++i) {
        tags_buf = benchCalloc(1, tags_buf_size, true);
        if (!tags_buf) {
            goto error;
        }

        tags_buf_bucket[i].buf      = tags_buf;
        write_meta->tags_buf_size   = tags_buf_size;

        ret = csvGenRowTagData(tags_buf, tags_buf_size, stb, thread_meta->ctb_start_idx + i, &tk);
        if (ret <= 0) {
            goto error;
        }

        tags_buf_bucket[i].length  = ret;
    }

    return tags_buf_bucket;

error:
    csvFreeCtbTagData(thread_meta, tags_buf_bucket);
    return NULL;
}


static void csvFreeCtbTagData(CsvThreadMeta* thread_meta, CsvRowTagsBuf* tags_buf_bucket) {
    if (!thread_meta || !tags_buf_bucket) {
        return;
    }

    for (uint64_t i = 0 ; i < thread_meta->ctb_count; ++i) {
        char* tags_buf = tags_buf_bucket[i].buf;
        if (tags_buf) {
            tmfree(tags_buf_bucket);
        } else {
            break;
        }
    }
    tmfree(tags_buf_bucket);
    return;
}


static int csvWriteFile(FILE* fp, uint64_t ctb_idx, int64_t cur_ts, int64_t* ck, CsvWriteMeta* write_meta, CsvThreadMeta* thread_meta) {
    SDataBase*   db  = write_meta->db;
    SSuperTable* stb = write_meta->stb;
    CsvRowTagsBuf* tags_buf_bucket = thread_meta->tags_buf_bucket;
    CsvRowColsBuf* tags_buf        = &tags_buf_bucket[ctb_idx];
    CsvRowColsBuf* cols_buf        = thread_meta->cols_buf;
    int ret         = 0;
    size_t written  = 0;


    ret = csvGenRowColData(cols_buf->buf, cols_buf->buf_size, stb, cur_ts, db->precision, ck);
    if (ret <= 0) {
        errorPrint("Failed to generate csv column data. database: %s, super table: %s, naming type: %d, thread index: %d, ctb index: %" PRIu64 ".\n",
                db->dbName, stb->stbName, write_meta.naming_type, thread_meta->thread_id, ctb_idx);
        return -1;
    }

    cols_buf->length = ret;

    // write header
    if (thread_meta->output_header) {
        written = fwrite(write_meta->csv_header, 1, write_meta->csv_header_length, fp);
        thread_meta->output_header = false;
    }

    // write columns
    written = fwrite(cols_buf->buf, 1, cols_buf->length, fp);
    if (written != cols_buf->length) {
        errorPrint("Failed to write csv column data, expected written %d but got %zu. database: %s, super table: %s, naming type: %d, thread index: %d, ctb index: %" PRIu64 ".\n",
                cols_buf->length, written, db->dbName, stb->stbName, write_meta.naming_type, thread_meta->thread_id, ctb_idx);
        return -1;
    }

    // write tags
    written = fwrite(tags_buf->buf, 1, tags_buf->length, fp);
    if (written != tags_buf->length) {
        errorPrint("Failed to write csv tag data, expected written %d but got %zu. database: %s, super table: %s, naming type: %d, thread index: %d, ctb index: %" PRIu64 ".\n",
                tags_buf->length, written, db->dbName, stb->stbName, write_meta.naming_type, thread_meta->thread_id, ctb_idx);
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
    FILE*    fp                 = NULL;
    char fullname[MAX_PATH_LEN] = {};


    // tags buffer
    CsvRowTagsBuf* tags_buf_bucket = csvGenCtbTagData(write_meta, thread_meta);
    if (!tags_buf_bucket) {
        errorPrint("Failed to generate csv tag data. database: %s, super table: %s, naming type: %d, thread index: %d.\n",
                db->dbName, stb->stbName, write_meta.naming_type, thread_meta->thread_id);
        return NULL;
    }

    // column buffer
    int    buf_size = stb->lenOfCols + stb->cols->size;
    char*  buf = (char*)benchCalloc(1, buf_size, true);
    if (!buf) {
        errorPrint("Failed to malloc csv column buffer. database: %s, super table: %s, naming type: %d, thread index: %d.\n",
                db->dbName, stb->stbName, write_meta.naming_type, thread_meta->thread_id);
        goto end;
    }

    CsvRowColsBuf cols_buf = {
        .buf        = buf,
        .buf_size   = buf_size,
        .length     = 0
    };

    thread_meta->tags_buf_bucket = tags_buf_bucket;
    thread_meta->cols_buf        = &cols_buf;


    for (cur_ts = write_meta->start_ts; cur_ts < write_meta->end_ts; cur_ts += write_meta->ts_step) {
        // get filename
        fullname[MAX_PATH_LEN] = {};
        ret = csvGetFileFullname(write_meta, thread_meta, fullname, sizeof(fullname));
        if (ret < 0) {
            errorPrint("Failed to generate csv filename. database: %s, super table: %s, naming type: %d, thread index: %d.\n",
                    db->dbName, stb->stbName, write_meta.naming_type, thread_meta->thread_id);
            goto end;
        }

        // create fd
        fp = fopen(fullname, "w");
        if (fp == NULL) {
            errorPrint("Failed to create csv file. thread index: %d, file: %s, errno: %d, strerror: %s.\n",
                    thread_meta->thread_id, fullname, errno, strerror(errno));
            goto end;
        }


        thread_meta->output_header = g_arguments->csv_output_header;
        slice_cur_ts = cur_ts;
        slice_end_ts = MIN(cur_ts + write_meta->ts_step, write_meta->end_ts);

        // write data
        while (slice_cur_ts < slice_end_ts) {
            slice_batch_ts = csvCalcSliceBatchTimestamp(write_meta, slice_cur_ts, slice_end_ts);

            for (ctb_idx = 0; ctb_idx < thread_meta->ctb_count; ++ctb_idx) {
                for (slice_ctb_cur_ts = slice_cur_ts; slice_ctb_cur_ts < slice_batch_ts; slice_ctb_cur_ts += write_meta->stb->timestamp_step) {
                    ret = csvWriteFile(fp, ctb_idx, slice_ctb_cur_ts, &ck, write_meta, thread_meta);
                    if (!ret) {
                        errorPrint("Failed to write csv file. thread index: %d, file: %s, errno: %d, strerror: %s.\n",
                                thread_meta->thread_id, fullname, errno, strerror(errno));
                        fclose(fp);
                        goto end;
                    }

                    ck += 1;
                }
            }
            
            slice_cur_ts = slice_batch_ts;
        }

        fclose(fp);
        csvUpdateSliceRange(write_meta, thread_meta, last_end_ts);
    }

end:
    csvFreeCtbTagData(tags_buf_bucket);
    tmfree(cols_buf);
    return NULL;
}


static int csvGenStbProcess(SDataBase* db, SSuperTable* stb) {
    int ret = 0;

    CsvWriteMeta* write_meta = benchCalloc(1, sizeof(CsvWriteMeta), false);
    if (!write_meta) {
        ret = -1;
        goto end;
    }

    ret = csvInitWriteMeta(db, stb, write_meta);
    if (ret < 0) {
        ret = -1;
        goto end;
    }

    CsvThreadArgs* args = benchCalloc(write_meta->total_threads, sizeof(CsvThreadArgs), false);
    if (!args) {
        ret = -1;
        goto end;
    }

    pthread_t* pids = benchCalloc(write_meta.total_threads, sizeof(pthread_t), false);
    if (!pids) {
        ret = -1;
        goto end;
    }

    for (uint32_t i = 0; (i < write_meta->total_threads && !g_arguments->terminate); ++i) {
        CsvThreadArgs* arg = &args[i];
        arg->write_meta  = write_meta;
        csvInitThreadMeta(write_meta, i + 1, &arg->thread_meta);

        ret = pthread_create(&pids[i], NULL, csvGenStbThread, arg);
        if (!ret) {
            perror("Failed to create thread"); 
            goto end;
        }
    }

    // wait threads
    for (uint32_t i = 0; i < write_meta->total_threads; ++i) {
        infoPrint("pthread_join %d ...\n", i);
        pthread_join(pids[i], NULL);
    }

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
        errorPrint("Failed to generate CSV files, the specified output path is empty. Please provide a valid path. database: %s, super table: %s.\n",
                db->dbName, stb->stbName);
        return -1;
    }
    if (g_arguments->output_path[len - 1] != '/') {
        int n = snprintf(g_arguments->output_path_buf, sizeof(g_arguments->output_path_buf), "%s/", g_arguments->output_path);
        if (n < 0 || n >= sizeof(g_arguments->output_path_buf)) {
            errorPrint("Failed to generate CSV files, path buffer overflow risk when appending '/'. path: %s, database: %s, super table: %s.\n",
                    g_arguments->csv_output_path, db->dbName, stb->stbName);
            return -1;
        }
        g_arguments->output_path = g_arguments->output_path_buf;
    }

    // csv_ts_format
    if (g_arguments->csv_ts_format) {
        if (csvValidateParamTsFormat(g_arguments->csv_ts_format) != 0) {
            errorPrint("Failed to generate CSV files, the parameter `csv_ts_format` is invalid. csv_ts_format: %s, database: %s, super table: %s.\n",
                    g_arguments->csv_ts_format, db->dbName, stb->stbName);
            return -1;
        }
    }

    // csv_ts_interval
    long csv_ts_intv_secs = csvValidateParamTsInterval(g_arguments->csv_ts_interval);
    if (csv_ts_intv_secs <= 0) {
        errorPrint("Failed to generate CSV files, the parameter `csv_ts_interval` is invalid. csv_ts_interval: %s, database: %s, super table: %s.\n",
                g_arguments->csv_ts_interval, db->dbName, stb->stbName);
        return -1;
    }
    g_arguments->csv_ts_intv_secs = csv_ts_intv_secs;

    return 0;
}


static int csvWriteThread() {
    for (size_t i = 0; i < g_arguments->databases->size && !g_arguments->terminate; ++i) {
        // database
        SDataBase* db = benchArrayGet(g_arguments->databases, i);
        if (database->superTbls) {
            for (size_t j = 0; j < db->superTbls->size && !g_arguments->terminate; ++j) {
                // stb
                SSuperTable* stb = benchArrayGet(db->superTbls, j);
                if (stb->insertRows == 0) {
                    continue;
                }

                // gen csv
                int ret = csvGenStb(db, stb);
                if(ret != 0) {
                    errorPrint("Failed to generate CSV files. database: %s, super table: %s, error code: %d.\n",
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

    infoPrint("Starting to output data to CSV files in directory: %s ...\n", g_arguments->output_path);
    int64_t start = toolsGetTimestampMs();
    int ret = csvWriteThread();
    if (ret != 0) {
        return -1;
    }
    int64_t delay = toolsGetTimestampMs() - start;
    infoPrint("Generating CSV files in directory: %s has been completed. Time elapsed: %.3f seconds\n",
            g_arguments->output_path, delay / 1000.0);
    return 0;
}
