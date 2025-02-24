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





int32_t writeCsvFile(FILE* f, char * buf, int32_t len) {
    size_t size = fwrite(buf, 1, len, f);
    if(size != len) {
        errorPrint("failed to write csv file. expect write length:%d real write length:%d \n", len, (int32_t)size);
        return -1;
    }
    return 0;
}

int batchWriteCsv(SDataBase* db, SSuperTable* stb, FILE* fs, char* buf, int rows_buf_len, int minRemain) {
    int ret    = 0;
    int pos    = 0;
    int64_t tk = 0;
    int64_t show = 0;


    uint32_t tags_length = accumulateRowLen(stbInfo->tags, stbInfo->iface);
    uint32_t cols_length = accumulateRowLen(stbInfo->cols, stbInfo->iface);

    size_t tags_csv_length = tags_length + stb->tags->size;
    size_t cols_csv_length = cols_length + stb->cols->size;
    char*  tags_csv_buf    = (char*)benchCalloc(1, tags_csv_length, true);    
    char*  cols_csv_buf    = (char*)benchCalloc(1, cols_csv_length, true);

    // gen child name
    for (int64_t i = 0; i < stb->childTblCount; ++i) {
        int64_t ts = stb->startTimestamp;
        int64_t ck = 0;

        // child table

        // tags
        csvGenRowTagData(tags_csv_buf, stb, i, &tk);
        // insert child column data
        for(int64_t j = 0; j < stb->insertRows; j++) {
            genColumnData(cols_csv_buf, stb, ts, db->precision, &ck);
            // combine
            pos += sprintf(buf + pos, "%s,%s.\n", tags_csv_buf, cols_csv_buf);
            if (rows_buf_len - pos < minRemain) {
                // submit 
                ret = writeCsvFile(fs, buf, pos);
                if (ret != 0) {
                    goto END;
                }

                pos = 0;
            }

            // ts move next
            ts += stb->timestamp_step;

            // check cancel
            if(g_arguments->terminate) {
                infoPrint("%s", "You are cancel, exiting ...\n");
                ret = -1;
                goto END;
            }

            // print show
            if (++show % SHOW_CNT == 0) {
                infoPrint("batch write child table cnt = %"PRId64 " all rows = %" PRId64 "\n", i+1, show);
            }

        }
    }

    if (pos > 0) {
        ret = writeCsvFile(fs, buf, pos);
        pos = 0;
    }

END:
    // free
    tmfree(tags_csv_buf);
    tmfree(cols_csv_buf);
    return ret;
}


static time_t csvGetStartSeconds(SDataBase* db, SSuperTable* stb) {
    time_t start_seconds = 0;

    if (db->precision == TSDB_TIME_PRECISION_MICRO) {
        start_seconds = stb->startTimestamp / 1000000L;
    } else if (db->precision == TSDB_TIME_PRECISION_NANO) {
        start_seconds = stb->startTimestamp / 1000000000L;
    } else {
        start_seconds = stb->startTimestamp / 1000L;
    }
    return start_seconds;
}


void csvConvertTime2String(time_t time_value, char* time_buf, size_t buf_size) {
    struct tm tm_result;
    char *old_locale = setlocale(LC_TIME, "C");
#ifdef _WIN32
    gmtime_s(&tm_result, &time_value);
#else
    gmtime_r(&time_value, &tm_result);
#endif
    strftime(time_buf, buf_size, g_arguments->csv_ts_format, &tm_result);
    if (old_locale) {
        (LC_TIME, old_locale);
    }
}


static CsvNamingType csvGetFileNamingType(SSuperTable* stb) {
    if (stb->interlaceRows > 0) {
        if (g_arguments->csv_ts_format) {
            return CSV_NAMING_TIME_SLICE;
        } else {
            return CSV_NAMING_SINGLE;
        }
    } else {
        if (g_arguments->csv_ts_format) {
            return CSV_NAMING_THREAD_TIME_SLICE;
        } else {
            return CSV_NAMING_THREAD;
        }
    }
}


static void csvGenEndTimestamp(CsvWriteMeta* meta, SDataBase* db) {
    time_t end_ts = 0;

    if (db->precision == TSDB_TIME_PRECISION_MICRO) {
        end_ts = meta->end_secs * 1000000L;
    } else if (db->precision == TSDB_TIME_PRECISION_NANO) {
        end_ts = meta->end_secs * 1000000000L;
    } else {
        end_ts = meta->end_secs * 1000L;
    }
    meta->end_ts = end_ts;
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
        (void)sprintf(meta->thread_formatter, "%%d");
    } else {
        (void)snprintf(meta->thread_formatter, sizeof(meta->thread_formatter), "%%0%dd", digits);
    }
}


static CsvWriteMeta csvInitFileNamingMeta(SDataBase* db, SSuperTable* stb) {
    CsvWriteMeta meta      = {
        .naming_type        = CSV_NAMING_SINGLE,
        .start_secs         = 0,
        .end_secs           = 0,
        .thread_id          = 0,
        .total_threads      = 1,
        .thread_formatter   = {}
    };

    meta.naming_type = csvGetFileNamingType(stb);

    switch (meta.naming_type) {
        case CSV_NAMING_SINGLE: {
            break;
        }
        case CSV_NAMING_TIME_SLICE: {
            meta.start_secs = csvGetStartSeconds(db, stb);
            meta.end_secs   = meta.start_secs + g_arguments->csv_ts_intv_secs;
            csvGenEndTimestamp(&meta, db);
            break;
        }
        case CSV_NAMING_THREAD: {
            meta.thread_id = 1;
            meta.total_threads = g_arguments->nthreads;
            csvGenThreadFormatter(&meta);
            break;
        }
        case CSV_NAMING_THREAD_TIME_SLICE: {
            meta.thread_id = 1;
            meta.total_threads = g_arguments->nthreads;
            csvGenThreadFormatter(&meta);
            meta.start_secs = csvGetStartSeconds(db, stb);
            meta.end_secs   = meta.start_secs + g_arguments->csv_ts_intv_secs;
            csvGenEndTimestamp(&meta, db);
            break;
        }
        default: {
            meta.naming_type = CSV_NAMING_SINGLE;
            break;
        }
    }

    return meta;
}


int csvGetFileFullname(CsvWriteMeta* meta, char* fullname, size_t size) {
    char thread_buf[SMALL_BUFF_LEN];
    char start_time_buf[MIDDLE_BUFF_LEN];
    char end_time_buf[MIDDLE_BUFF_LEN];
    int ret = -1;
    const char* base_path   = g_arguments->output_path;
    const char* file_prefix = g_arguments->csv_file_prefix;

    switch (meta->naming_type) {
        case CSV_NAMING_SINGLE: {
            ret = snprintf(fullname, size, "%s%s.csv", base_path, file_prefix);
            break;
        }
        case CSV_NAMING_TIME_SLICE: {
            csvConvertTime2String(meta->start_secs, start_time_buf, sizeof(start_time_buf));
            csvConvertTime2String(meta->end_secs, end_time_buf, sizeof(end_time_buf));
            ret = snprintf(fullname, size, "%s%s_%s_%s.csv", base_path, file_prefix, start_time_buf, end_time_buf);
            break;
        }
        case CSV_NAMING_THREAD: {
            (void)snprintf(thread_buf, sizeof(thread_buf), meta->thread_formatter, meta->thread_id);
            ret = snprintf(fullname, size, "%s%s_%s.csv", base_path, file_prefix, thread_buf);
            break;
        }
        case CSV_NAMING_THREAD_TIME_SLICE: {
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


uint32_t csvCalcInterlaceRows(CsvWriteMeta* meta, SSuperTable* stb, int64_t ts) {
    uint32_t need_rows = 0;


    switch (meta->naming_type) {
        case CSV_NAMING_SINGLE: {
            need_rows = stb->interlaceRows;
            break;
        }
        case CSV_NAMING_TIME_SLICE: {
            (meta->end_ts - ts) / stb->timestamp_step
            need_rows = stb->interlaceRows;

            break;
        }
        case CSV_NAMING_THREAD: {
            (void)snprintf(thread_buf, sizeof(thread_buf), meta->thread_formatter, meta->thread_id);
            ret = snprintf(fullname, size, "%s%s_%s.csv", base_path, file_prefix, thread_buf);
            break;
        }
        case CSV_NAMING_THREAD_TIME_SLICE: {
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
}




static int interlaceWriteCsv(SDataBase* db, SSuperTable* stb, FILE* fp, char* rows_buf, int rows_buf_len) {
    char fullname[MAX_PATH_LEN] = {};
    CsvWriteMeta meta = csvInitFileNamingMeta();

    int ret = csvGetFileFullname(&meta, fullname, sizeof(fullname));
    if (ret < 0) {
        errorPrint("Failed to generate csv filename. database: %s, super table: %s, naming type: %d.\n",
                db->dbName, stb->stbName, meta.naming_type);
        return -1;
    }

    int ret    = 0;
    int pos    = 0;
    int64_t n  = 0; // already inserted rows for one child table
    int64_t tk = 0;
    int64_t show = 0;
    int64_t ts   = 0;
    int64_t last_ts = stb->startTimestamp;

    // init buffer
    char** tags_buf_bucket = (char **)benchCalloc(stb->childTblCount, sizeof(char *), true);
    int    cols_buf_length = stb->lenOfCols + stb->cols->size;
    char*  cols_buf = (char *)benchCalloc(1, cols_buf_length, true);

    for (int64_t i = 0; i < stb->childTblCount; ++i) {
        int tags_buf_length = TSDB_TABLE_NAME_LEN +  stb->lenOfTags + stb->tags->size;
        tags_buf_bucket[i] = benchCalloc(1, tags_buf_length, true);
        if (!tags_buf_bucket[i]) {
            ret = -1;
            goto end;
        }

        ret = csvGenRowTagData(tags_buf_bucket[i], tags_buf_length, stb, i, &tk);
        if (!ret) {
            goto end;
        }
    }

    while (n < stb->insertRows ) {
        for (int64_t i = 0; i < stb->childTblCount; ++i) {
            ts = last_ts;
            int64_t ck = 0;

            
            // calc need insert rows
            uint32_t need_rows = csvCalcInterlaceRows(&meta, stb, ts)

            int64_t needInserts = stb->interlaceRows;
            if(needInserts > stb->insertRows - n) {
                needInserts = stb->insertRows - n;
            } 

            for (int64_t j = 0; j < needInserts; j++) {
                genColumnData(cols_buf, stb, ts, db->precision, &ck);
                // combine tags,cols
                pos += sprintf(buf + pos, "%s,%s\n", tags_buf_bucket[i], cols_buf);
                if (rows_buf_len - pos <  minRemain) {
                    // submit 
                    ret = writeCsvFile(fp, buf, pos);
                    if (ret != 0) {
                        goto end;
                    }
                    pos = 0;
                }

                // ts move next
                ts += stb->timestamp_step;

                // check cancel
                if(g_arguments->terminate) {
                    infoPrint("%s", "You are cancel, exiting ... \n");
                    ret = -1;
                    goto end;
                }

                // print show
                if (++show % SHOW_CNT == 0) {
                    infoPrint("interlace write child table index = %"PRId64 " all rows = %"PRId64 "\n", i+1, show);
                }
            }

            // if last child table
            if (i + 1 == stb->childTblCount ) {
                n += needInserts;
                last_ts = ts;
            }
        }
    }

    if (pos > 0) {
        ret = writeCsvFile(fp, buf, pos);
        pos = 0;
    }

end:
    // free
    for (int64_t m = 0 ; m < stb->childTblCount; m ++) {
        tmfree(tags_buf_bucket[m]);
    }
    tmfree(tags_buf_bucket);
    tmfree(cols_buf);
    return ret;
}


// gen tag data 
int csvGenRowTagData(char* buf, size_t size, SSuperTable* stb, int64_t index, int64_t* k) {
    // tbname
    int pos = snprintf(buf, size, "\'%s%"PRId64"\'", stb->childTblPrefix, index);
    // tags
    pos += csvGenRowFields(buf + pos, stb->tags, stb->tags->size, stb->binaryPrefex, stb->ncharPrefex, k);

    return (pos > 0 && (size_t)pos < size) ? 0 : -1;
}

// gen column data 
char * genColumnData(char* cols_csv_buf, SSuperTable* stb, int64_t ts, int32_t precision, int64_t *k) {
    char szTime[128] = {0};
    toolsFormatTimestamp(szTime, ts, precision);
    int pos = sprintf(cols_csv_buf, "\'%s\'", szTime);

    // columns
    csvGenRowFields(cols_csv_buf + pos, stb->cols, stb->cols->size, stb->binaryPrefex, stb->ncharPrefex, k);
    return cols_csv_buf;
}


int32_t csvGenRowFields(char* buf, BArray* fields, int16_t field_count, char* binanry_prefix, char* nchar_prefix, int64_t* k) {
    int32_t pos = 0;

    for (uint16_t i = 0; i < field_count; ++i) {
        Field* field = benchArrayGet(fields, i);
        char* prefix = "";
        if(field->type == TSDB_DATA_TYPE_BINARY || field->type == TSDB_DATA_TYPE_VARBINARY) {
            if (binanry_prefix) {
                prefix = binanry_prefix;
            }
        } else if(field->type == TSDB_DATA_TYPE_NCHAR) {
            if (nchar_prefix) {
                prefix = nchar_prefix;
            }
        }
        pos += dataGenByField(field, buf, pos, prefix, k, "");
    }

    return pos;
}



int csvGenStbInterlace(SDataBase* db, SSuperTable* stb) {


    int ret = 0;
    char outFile[MAX_FILE_NAME_LEN] = {0};
    obtainCsvFile(outFile, db, stb, outDir);
    FILE* fp = fopen(outFile, "w");
    if(fp == NULL) {
        errorPrint("failed create csv file. file=%s, last errno=%d strerror=%s \n", outFile, errno, strerror(errno));
        return -1;
    }

    int row_buf_len   = TSDB_TABLE_NAME_LEN + stb->lenOfTags + stb->lenOfCols + stb->tags->size + stb->cols->size;
    int rows_buf_len  = row_buf_len * g_arguments->interlaceRows;
    char* rows_buf    = benchCalloc(1, rows_buf_len, true);

    infoPrint("start write csv file: %s \n", outFile);

    // interlace mode
    ret = interlaceWriteCsv(db, stb, fp, rows_buf, rows_buf_len);


    tmfree(rows_buf);
    fclose(fp);

    succPrint("end write csv file: %s \n", outFile);


    // wait threads
    for (int i = 0; i < threadCnt; i++) {
        infoPrint("pthread_join %d ...\n", i);
        pthread_join(pids[i], NULL);
    }


    return ret;
}


void csvGenPrepare(SDataBase* db, SSuperTable* stb) {
    stbInfo->lenOfTags = accumulateRowLen(stbInfo->tags, stbInfo->iface);
    stbInfo->lenOfCols = accumulateRowLen(stbInfo->cols, stbInfo->iface);
    return;
}


int csvGenStb(SDataBase* db, SSuperTable* stb) {
    // prepare
    csvGenPrepare(db, stb);


    int ret = 0;
    if (stb->interlaceRows > 0) {
        // interlace mode
        ret = csvGenStbInterlace(db, stb);
    } else {
        // batch mode
        ret = csvGenStbBatch(db, stb);
    }

    return ret;
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
