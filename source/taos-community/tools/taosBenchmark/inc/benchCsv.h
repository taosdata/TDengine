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
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef INC_BENCHCSV_H_
#define INC_BENCHCSV_H_

#include <zlib.h>

#include "bench.h"


typedef enum {
    CSV_NAMING_I_SINGLE,
    CSV_NAMING_I_TIME_SLICE,
    CSV_NAMING_B_THREAD,
    CSV_NAMING_B_THREAD_TIME_SLICE
} CsvNamingType;

typedef enum {
    CSV_ERR_OK = 0,
    CSV_ERR_OPEN_FAILED,
    CSV_ERR_WRITE_FAILED
} CsvIoError;

typedef struct {
    const char* filename;
    CsvCompressionLevel compress_level;
    CsvIoError result;
    union {
        gzFile  gf;
        FILE*   fp;
    } handle;
} CsvFileHandle;

typedef struct {
    char*           buf;
    int             length;
} CsvRowTagsBuf;

typedef struct {
    char*           buf;
    int             buf_size;
    int             length;
} CsvRowColsBuf;

typedef struct {
    CsvNamingType   naming_type;
    size_t          total_threads;
    char            mode[MIDDLE_BUFF_LEN];
    char            thread_formatter[SMALL_BUFF_LEN];
    char            csv_header[LARGE_BUFF_LEN];
    int             csv_header_length;
    SDataBase*      db;
    SSuperTable*    stb;
    int64_t         start_ts;
    int64_t         end_ts;
    int64_t         ts_step;
    int64_t         interlace_step;
} CsvWriteMeta;

typedef struct {
    uint64_t        ctb_start_idx;
    uint64_t        ctb_end_idx;
    uint64_t        ctb_count;
    uint64_t        total_rows;
    time_t          start_secs;
    time_t          end_secs;
    int64_t         start_ts;
    int64_t         end_ts;
    size_t          thread_id;
    bool            output_header;
    int             tags_buf_size;
    CsvRowTagsBuf*  tags_buf_array;
    CsvRowColsBuf*  cols_buf;
} CsvThreadMeta;

typedef struct {
    CsvWriteMeta*   write_meta;
    CsvThreadMeta   thread_meta;
} CsvThreadArgs;


int csvTestProcess();

#endif  // INC_BENCHCSV_H_
