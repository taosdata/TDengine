/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _tsdb_global_header_
#define _tsdb_global_header_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include "tglobalcfg.h"

#define TSDB__packed

#ifdef TSKEY32
#define TSKEY int32_t;
#else
#define TSKEY int64_t
#endif

#define TSDB_TRUE   1
#define TSDB_FALSE  0
#define TSDB_OK     0
#define TSDB_ERR    -1

#define TS_PATH_DELIMITER "."

#define TSDB_TIME_PRECISION_MILLI 0
#define TSDB_TIME_PRECISION_MICRO 1

#define TSDB_TIME_PRECISION_MILLI_STR "ms"
#define TSDB_TIME_PRECISION_MICRO_STR "us"

enum _status {
  TSDB_STATUS_OFFLINE,
  TSDB_STATUS_CREATING,
  TSDB_STATUS_UNSYNCED,
  TSDB_STATUS_SLAVE,
  TSDB_STATUS_MASTER,
  TSDB_STATUS_READY,
};

enum _syncstatus {
  STDB_SSTATUS_INIT,
  TSDB_SSTATUS_SYNCING,
  TSDB_SSTATUS_SYNC_CACHE,
  TSDB_SSTATUS_SYNC_FILE,
};

#define TSDB_DATA_TYPE_BOOL       1       // 1 bytes
#define TSDB_DATA_TYPE_TINYINT    2       // 1 byte
#define TSDB_DATA_TYPE_SMALLINT   3       // 2 bytes
#define TSDB_DATA_TYPE_INT        4       // 4 bytes
#define TSDB_DATA_TYPE_BIGINT     5       // 8 bytes
#define TSDB_DATA_TYPE_FLOAT      6       // 4 bytes
#define TSDB_DATA_TYPE_DOUBLE     7       // 8 bytes
#define TSDB_DATA_TYPE_BINARY     8       // string
#define TSDB_DATA_TYPE_TIMESTAMP  9       // 8 bytes
#define TSDB_DATA_TYPE_NCHAR      10      // wide string

#define TSDB_KEYSIZE              sizeof(TSKEY)
#define TSDB_NCHAR_SIZE           sizeof(wchar_t)

#define TSDB_RELATION_INVALID     0
#define TSDB_RELATION_LESS        1
#define TSDB_RELATION_LARGE       2
#define TSDB_RELATION_EQUAL       3
#define TSDB_RELATION_LESS_EQUAL  4
#define TSDB_RELATION_LARGE_EQUAL 5
#define TSDB_RELATION_NOT_EQUAL   6
#define TSDB_RELATION_LIKE        7

#define TSDB_RELATION_AND         8
#define TSDB_RELATION_OR          9
#define TSDB_RELATION_NOT         10

#define TSDB_BINARY_OP_ADD        11
#define TSDB_BINARY_OP_SUBTRACT   12
#define TSDB_BINARY_OP_MULTIPLY   13
#define TSDB_BINARY_OP_DIVIDE     14
#define TSDB_BINARY_OP_REMAINDER  15

#define TSDB_USERID_LEN           9
#define TS_PATH_DELIMITER_LEN     1

#define TSDB_METER_ID_LEN_MARGIN 10
#define TSDB_METER_ID_LEN                                                                 \
  (TSDB_DB_NAME_LEN + TSDB_METER_NAME_LEN + 2 * TS_PATH_DELIMITER_LEN + TSDB_USERID_LEN + \
   TSDB_METER_ID_LEN_MARGIN)  // TSDB_DB_NAME_LEN+TSDB_METER_NAME_LEN+2*strlen(TS_PATH_DELIMITER)+strlen(USERID)
#define TSDB_UNI_LEN              24
#define TSDB_USER_LEN             TSDB_UNI_LEN
#define TSDB_ACCT_LEN             TSDB_UNI_LEN
#define TSDB_PASSWORD_LEN         TSDB_UNI_LEN

#define TSDB_MAX_COLUMNS          256
#define TSDB_MIN_COLUMNS          2  // PRIMARY COLUMN(timestamp) + other columns

#define TSDB_METER_NAME_LEN       64
#define TSDB_DB_NAME_LEN          32
#define TSDB_COL_NAME_LEN         64
#define TSDB_MAX_SAVED_SQL_LEN TSDB_MAX_COLUMNS * 16
#define TSDB_MAX_SQL_LEN TSDB_PAYLOAD_SIZE

#define TSDB_MAX_BYTES_PER_ROW TSDB_MAX_COLUMNS * 16
#define TSDB_MAX_TAGS_LEN         512
#define TSDB_MAX_TAGS             6

#define TSDB_AUTH_LEN             16
#define TSDB_KEY_LEN              16
#define TSDB_VERSION_LEN          12
#define TSDB_STREET_LEN           64
#define TSDB_CITY_LEN             20
#define TSDB_STATE_LEN            20
#define TSDB_COUNTRY_LEN          20
#define TSDB_VNODES_SUPPORT       6
#define TSDB_MGMT_SUPPORT         4
#define TSDB_LOCALE_LEN           64
#define TSDB_TIMEZONE_LEN         64

#define TSDB_IPv4ADDR_LEN         16
#define TSDB_FILENAME_LEN         128
#define TSDB_METER_VNODE_BITS     20
#define TSDB_METER_SID_MASK       0xFFFFF
#define TSDB_SHELL_VNODE_BITS     24
#define TSDB_SHELL_SID_MASK       0xFF
#define TSDB_HTTP_TOKEN_LEN       20
#define TSDB_SHOW_SQL_LEN         32

#define TSDB_METER_STATE_OFFLINE  0
#define TSDB_METER_STATE_ONLLINE  1

#define TSDB_DEFAULT_PKT_SIZE     65480  // same as RPC_MAX_UDP_SIZE

#define TSDB_PAYLOAD_SIZE (TSDB_DEFAULT_PKT_SIZE - 100)
#define TSDB_DEFAULT_PAYLOAD_SIZE 1024  // default payload size
#define TSDB_EXTRA_PAYLOAD_SIZE   128   // extra bytes for auth
#define TSDB_SQLCMD_SIZE          1024
#define TSDB_MAX_VNODES           256
#define TSDB_MIN_VNODES           50
#define TSDB_INVALID_VNODE_NUM    0

#define TSDB_DNODE_ROLE_ANY       0
#define TSDB_DNODE_ROLE_MGMT      1
#define TSDB_DNODE_ROLE_VNODE     2

#define TSDB_MAX_MPEERS           5
#define TSDB_MAX_MGMT_IPS (TSDB_MAX_MPEERS + 1)

#define TSDB_REPLICA_MAX_NUM 3
#define TSDB_REPLICA_MIN_NUM 1

// default value == 10
#define TSDB_FILE_MIN_PARTITION_RANGE 1     // minimum partition range of vnode file in days
#define TSDB_FILE_MAX_PARTITION_RANGE 3650  // max partition range of vnode file in days

#define TSDB_DATA_MIN_RESERVE_DAY     1     // data in db to be reserved.
#define TSDB_DATA_DEFAULT_RESERVE_DAY 3650  // ten years

#define TSDB_MIN_COMPRESSION_LEVEL        0
#define TSDB_MAX_COMPRESSION_LEVEL        2

#define TSDB_MIN_CACHE_BLOCKS_PER_METER   32
#define TSDB_MAX_CACHE_BLOCKS_PER_METER   40960

#define TSDB_MIN_COMMIT_TIME_INTERVAL     30
#define TSDB_MAX_COMMIT_TIME_INTERVAL     40960

#define TSDB_MIN_ROWS_IN_FILEBLOCK        200
#define TSDB_MAX_ROWS_IN_FILEBLOCK        500000

#define TSDB_MIN_CACHE_BLOCK_SIZE         100
#define TSDB_MAX_CACHE_BLOCK_SIZE         104857600

#define TSDB_MIN_CACHE_BLOCKS             100
#define TSDB_MAX_CACHE_BLOCKS             409600

#define TSDB_MAX_AVG_BLOCKS               2048

#define TSDB_MIN_TABLES_PER_VNODE         1
#define TSDB_MAX_TABLES_PER_VNODE         220000

#define TSDB_MAX_BINARY_LEN               (TSDB_MAX_BYTES_PER_ROW - TSDB_KEYSIZE)
#define TSDB_MAX_NCHAR_LEN                (TSDB_MAX_BYTES_PER_ROW - TSDB_KEYSIZE)
#define PRIMARYKEY_TIMESTAMP_COL_INDEX    0

#define TSDB_DATA_BOOL_NULL       0x02
#define TSDB_DATA_TINYINT_NULL    0x80
#define TSDB_DATA_SMALLINT_NULL   0x8000
#define TSDB_DATA_INT_NULL        0x80000000
#define TSDB_DATA_BIGINT_NULL     0x8000000000000000L

#define TSDB_DATA_FLOAT_NULL      0x7FF00000            // it is an NAN
#define TSDB_DATA_DOUBLE_NULL     0x7FFFFF0000000000L  // an NAN
#define TSDB_DATA_NCHAR_NULL      0xFFFFFFFF
#define TSDB_DATA_BINARY_NULL     0xFF

#define TSDB_DATA_NULL_STR        "NULL"
#define TSDB_DATA_NULL_STR_L      "null"

#define TSDB_MAX_RPC_THREADS      5

#ifdef __cplusplus
}
#endif

#endif
