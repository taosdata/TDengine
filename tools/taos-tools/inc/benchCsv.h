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

#include <bench.h>


typedef enum {
    CSV_NAMING_SINGLE,
    CSV_NAMING_TIME_SLICE,
    CSV_NAMING_THREAD,
    CSV_NAMING_THREAD_TIME_SLICE
} CsvNamingType;

typedef struct {
    CsvNamingType   naming_type;
    time_t          start_secs;
    time_t          end_secs;
    time_t          end_ts;
    size_t          thread_id;
    size_t          total_threads;
    char            thread_formatter[TINY_BUFF_LEN];
} CsvWriteMeta;



int csvTestProcess();

#endif  // INC_BENCHCSV_H_
