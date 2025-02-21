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

#include <bench.h>
#include "benchLog.h"
#include <benchDataMix.h>
#include <benchCsv.h>


//
// main etry
//

#define SHOW_CNT 100000



void obtainCsvFile(char * outFile, SDataBase* db, SSuperTable* stb, char* outDir) {
    sprintf(outFile, "%s%s-%s.csv", outDir, db->dbName, stb->stbName);
}

int32_t writeCsvFile(FILE* f, char * buf, int32_t len) {
    size_t size = fwrite(buf, 1, len, f);
    if(size != len) {
        errorPrint("failed to write csv file. expect write length:%d real write length:%d \n", len, (int32_t)size);
        return -1;
    }
    return 0;
}

int batchWriteCsv(SDataBase* db, SSuperTable* stb, FILE* fs, char* buf, int bufLen, int minRemain) {
    int ret    = 0;
    int pos    = 0;
    int64_t tk = 0;
    int64_t show = 0;

    int    tagDataLen = stb->lenOfTags + stb->tags->size + 256;
    char * tagData    = (char *) benchCalloc(1, tagDataLen, true);    
    int    colDataLen = stb->lenOfCols + stb->cols->size + 256;
    char * colData    = (char *) benchCalloc(1, colDataLen, true);

    // gen child name
    for (int64_t i = 0; i < stb->childTblCount; i++) {
        int64_t ts = stb->startTimestamp;
        int64_t ck = 0;
        // tags
        genTagData(tagData, stb, i, &tk);
        // insert child column data
        for(int64_t j = 0; j < stb->insertRows; j++) {
            genColumnData(colData, stb, ts, db->precision, &ck);
            // combine
            pos += sprintf(buf + pos, "%s,%s.\n", tagData, colData);
            if (bufLen - pos < minRemain) {
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
    tmfree(tagData);
    tmfree(colData);
    return ret;
}

int interlaceWriteCsv(SDataBase* db, SSuperTable* stb, FILE* fs, char* buf, int bufLen, int minRemain) {
    int ret    = 0;
    int pos    = 0;
    int64_t n  = 0; // already inserted rows for one child table
    int64_t tk = 0;
    int64_t show = 0;

    char **tagDatas   = (char **)benchCalloc(stb->childTblCount, sizeof(char *), true);
    int    colDataLen = stb->lenOfCols + stb->cols->size + 256;
    char * colData    = (char *) benchCalloc(1, colDataLen, true);
    int64_t last_ts   = stb->startTimestamp;
    
    while (n < stb->insertRows ) {
        for (int64_t i = 0; i < stb->childTblCount; i++) {
            // start one table
            int64_t ts = last_ts;
            int64_t ck = 0;
            // tags
            if (tagDatas[i] == NULL) {
                tagDatas[i] = genTagData(NULL, stb, i, &tk);
            }
            
            // calc need insert rows
            int64_t needInserts = stb->interlaceRows;
            if(needInserts > stb->insertRows - n) {
                needInserts = stb->insertRows - n;
            }

            for (int64_t j = 0; j < needInserts; j++) {
                genColumnData(colData, stb, ts, db->precision, &ck);
                // combine tags,cols
                pos += sprintf(buf + pos, "%s,%s.\n", tagDatas[i], colData);
                if (bufLen - pos <  minRemain) {
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
                    infoPrint("%s", "You are cancel, exiting ... \n");
                    ret = -1;
                    goto END;
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
        ret = writeCsvFile(fs, buf, pos);
        pos = 0;
    }

END:
    // free
    for (int64_t m = 0 ; m < stb->childTblCount; m ++) {
        tmfree(tagDatas[m]);
    }
    tmfree(tagDatas);
    tmfree(colData);
    return ret;
}

// gen tag data 
char * genTagData(char* buf, SSuperTable* stb, int64_t i, int64_t *k) {
    // malloc
    char* tagData;
    if (buf == NULL) {
        int tagDataLen = TSDB_TABLE_NAME_LEN +  stb->lenOfTags + stb->tags->size + 32;
        tagData = benchCalloc(1, tagDataLen, true);
    } else {
        tagData = buf;
    }

    int pos = 0;
    // tbname
    pos += sprintf(tagData, "\'%s%"PRId64"\'", stb->childTblPrefix, i);
    // tags
    pos += genRowByField(tagData + pos, stb->tags, stb->tags->size, stb->binaryPrefex, stb->ncharPrefex, k);

    return tagData;
}

// gen column data 
char * genColumnData(char* colData, SSuperTable* stb, int64_t ts, int32_t precision, int64_t *k) {
    char szTime[128] = {0};
    toolsFormatTimestamp(szTime, ts, precision);
    int pos = sprintf(colData, "\'%s\'", szTime);

    // columns
    genRowByField(colData + pos, stb->cols, stb->cols->size, stb->binaryPrefex, stb->ncharPrefex, k);
    return colData;
}


int32_t genRowByField(char* buf, BArray* fields, int16_t fieldCnt, char* binanryPrefix, char* ncharPrefix, int64_t *k) {

  // other cols data
  int32_t pos1 = 0;
  for(uint16_t i = 0; i < fieldCnt; i++) {
    Field* fd = benchArrayGet(fields, i);
    char* prefix = "";
    if(fd->type == TSDB_DATA_TYPE_BINARY || fd->type == TSDB_DATA_TYPE_VARBINARY) {
      if(binanryPrefix) {
        prefix = binanryPrefix;
      }
    } else if(fd->type == TSDB_DATA_TYPE_NCHAR) {
      if(ncharPrefix) {
        prefix = ncharPrefix;
      }
    }

    pos1 += dataGenByField(fd, buf, pos1, prefix, k, "");
  }
  
  return pos1;
}


int genWithSTable(SDataBase* db, SSuperTable* stb) {




    int ret = 0;
    char outFile[MAX_FILE_NAME_LEN] = {0};
    obtainCsvFile(outFile, db, stb, outDir);
    FILE * fs = fopen(outFile, "w");
    if(fs == NULL) {
        errorPrint("failed create csv file. file=%s, last errno=%d strerror=%s \n", outFile, errno, strerror(errno));
        return -1;
    }

    int rowLen = TSDB_TABLE_NAME_LEN + stb->lenOfTags + stb->lenOfCols + stb->tags->size + stb->cols->size;
    int bufLen = rowLen * g_arguments->reqPerReq;
    char* buf  = benchCalloc(1, bufLen, true);

    infoPrint("start write csv file: %s \n", outFile);

    if (stb->interlaceRows > 0) {
        // interlace mode
        ret = interlaceWriteCsv(db, stb, fs, buf, bufLen, rowLen * 2);
    } else {
        // batch mode 
        ret = batchWriteCsv(db, stb, fs, buf, bufLen, rowLen * 2);
    }

    tmfree(buf);
    fclose(fs);

    succPrint("end write csv file: %s \n", outFile);
    return ret;
}


static int is_valid_csv_ts_format(const char* csv_ts_format) {
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

    const char* invalid_chars = "/\\:*?\"<>|";
    if (strpbrk(buffer, invalid_chars) != NULL) {
        return -1;
    }

    return 0;
}


static long validate_csv_ts_interval(const char* csv_ts_interval) {
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
    size_t len = strlen(g_arguments->csv_output_path);
    if (len == 0) {
        errorPrint("Failed to generate CSV, the specified output path is empty. Please provide a valid path. database: %s, super table: %s.\n",
                db->dbName, stb->stbName);
        return -1;
    }
    if (g_arguments->csv_output_path[len - 1] != '/') {
        int n = snprintf(g_arguments->csv_output_path_buf, sizeof(g_arguments->csv_output_path_buf), "%s/", g_arguments->csv_output_path);
        if (n < 0 || n >= sizeof(g_arguments->csv_output_path_buf)) {
            errorPrint("Failed to generate CSV, path buffer overflow risk when appending '/'. path: %s, database: %s, super table: %s.\n",
                    g_arguments->csv_output_path， db->dbName, stb->stbName);
            return -1;
        }
        g_arguments->csv_output_path = g_arguments->csv_output_path_buf;
    }

    // csv_ts_format
    if (g_arguments->csv_ts_format) {
        if (is_valid_csv_ts_format(g_arguments->csv_ts_format) != 0) {
            errorPrint("Failed to generate CSV, the parameter `csv_ts_format` is invalid. csv_ts_format: %s, database: %s, super table: %s.\n",
                    g_arguments->csv_ts_format, db->dbName, stb->stbName);
            return -1;
        }
    }

    // csv_ts_interval
    long csv_ts_intv_secs = validate_csv_ts_interval(g_arguments->csv_ts_interval);
    if (csv_ts_intv_secs <= 0) {
        errorPrint("Failed to generate CSV, the parameter `csv_ts_interval` is invalid. csv_ts_interval: %s, database: %s, super table: %s.\n",
                g_arguments->csv_ts_interval, db->dbName, stb->stbName);
        return -1;
    }
    g_arguments->csv_ts_intv_secs = csv_ts_intv_secs;

    return 0;
}


static void csvWriteThread() {
    for (size_t i = 0; i < g_arguments->databases->size; ++i) {
        // database
        SDataBase* db = benchArrayGet(g_arguments->databases, i);
        if (database->superTbls) {
            for (size_t j = 0; j < db->superTbls->size; ++j) {
                // stb
                SSuperTable* stb = benchArrayGet(db->superTbls, j);
                if (stb->insertRows == 0) {
                    continue;
                }

                // gen csv
                int ret = genWithSTable(db, stb);
                if(ret != 0) {
                    errorPrint("Failed to generate CSV files. database: %s, super table: %s, error code: %d.\n",
                            db->dbName, stb->stbName, ret);
                    return;
                }
            }
        }
    }

    return;
}



int csvTestProcess() {
    // parse parameter
    if (csvParseParameter() != 0) {
        errorPrint("Failed to generate CSV files. database: %s, super table: %s, error code: %d.\n",
                db->dbName, stb->stbName, ret);
        return -1;
    }




    infoPrint("Starting to output data to CSV files in directory: %s ...\n", g_arguments->csv_output_path);
    int64_t start = toolsGetTimestampMs();
    csvWriteThread();
    int64_t delay = toolsGetTimestampMs() - start;
    infoPrint("Data export to CSV files in directory: %s has been completed. Time elapsed: %.3f seconds\n",
            g_arguments->csv_output_path, delay / 1000.0);
    return 0;
}
