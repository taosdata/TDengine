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

#ifndef TDENGINE_TAOS_DEF_H
#define TDENGINE_TAOS_DEF_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include "osDef.h"
#include "taos.h"

#define TSDB__packed

#ifdef TSKEY32
#define TSKEY int32_t;
#else
#define TSKEY int64_t
#endif

#define TSWINDOW_INITIALIZER ((STimeWindow) {INT64_MIN, INT64_MAX})
#define TSKEY_INITIAL_VAL    INT64_MIN

// Bytes for each type.
extern const int32_t TYPE_BYTES[15];

// TODO: replace and remove code below
#define CHAR_BYTES    sizeof(char)
#define SHORT_BYTES   sizeof(int16_t)
#define INT_BYTES     sizeof(int32_t)
#define LONG_BYTES    sizeof(int64_t)
#define FLOAT_BYTES   sizeof(float)
#define DOUBLE_BYTES  sizeof(double)
#define POINTER_BYTES sizeof(void *)  // 8 by default  assert(sizeof(ptrdiff_t) == sizseof(void*)

#define TSDB_KEYSIZE            sizeof(TSKEY)

#if LINUX
#define TSDB_NCHAR_SIZE         sizeof(wchar_t)
#else
#define TSDB_NCHAR_SIZE         sizeof(int32_t)
#endif

// NULL definition
#define TSDB_DATA_BOOL_NULL             0x02
#define TSDB_DATA_TINYINT_NULL          0x80
#define TSDB_DATA_SMALLINT_NULL         0x8000
#define TSDB_DATA_INT_NULL              0x80000000L
#define TSDB_DATA_BIGINT_NULL           0x8000000000000000L
#define TSDB_DATA_TIMESTAMP_NULL        TSDB_DATA_BIGINT_NULL

#define TSDB_DATA_FLOAT_NULL            0x7FF00000              // it is an NAN
#define TSDB_DATA_DOUBLE_NULL           0x7FFFFF0000000000L     // an NAN
#define TSDB_DATA_NCHAR_NULL            0xFFFFFFFF
#define TSDB_DATA_BINARY_NULL           0xFF

#define TSDB_DATA_UTINYINT_NULL         0xFF
#define TSDB_DATA_USMALLINT_NULL        0xFFFF
#define TSDB_DATA_UINT_NULL             0xFFFFFFFF
#define TSDB_DATA_UBIGINT_NULL          0xFFFFFFFFFFFFFFFFL

#define TSDB_DATA_NULL_STR              "NULL"
#define TSDB_DATA_NULL_STR_L            "null"

#define TSDB_DEFAULT_USER               "root"
#ifdef _TD_POWER_
#define TSDB_DEFAULT_PASS               "powerdb"
#else
#define TSDB_DEFAULT_PASS               "taosdata"
#endif

#define TSDB_TRUE   1
#define TSDB_FALSE  0
#define TSDB_OK     0
#define TSDB_ERR   -1

#define TS_PATH_DELIMITER "."

#define TSDB_TIME_PRECISION_MILLI 0
#define TSDB_TIME_PRECISION_MICRO 1
#define TSDB_TIME_PRECISION_NANO  2

#define TSDB_TIME_PRECISION_MILLI_STR "ms"
#define TSDB_TIME_PRECISION_MICRO_STR "us"
#define TSDB_TIME_PRECISION_NANO_STR  "ns"

#define TSDB_TICK_PER_SECOND(precision) ((precision)==TSDB_TIME_PRECISION_MILLI ? 1e3L : ((precision)==TSDB_TIME_PRECISION_MICRO ? 1e6L : 1e9L))

#define T_MEMBER_SIZE(type, member) sizeof(((type *)0)->member)
#define T_APPEND_MEMBER(dst, ptr, type, member) \
do {\
  memcpy((void *)(dst), (void *)(&((ptr)->member)), T_MEMBER_SIZE(type, member));\
  dst = (void *)((char *)(dst) + T_MEMBER_SIZE(type, member));\
} while(0)
#define T_READ_MEMBER(src, type, target) \
do { \
  (target) = *(type *)(src); \
  (src) = (void *)((char *)src + sizeof(type));\
} while(0)

#define GET_INT8_VAL(x)    (*(int8_t *)(x))
#define GET_INT16_VAL(x)   (*(int16_t *)(x))
#define GET_INT32_VAL(x)   (*(int32_t *)(x))
#define GET_INT64_VAL(x)   (*(int64_t *)(x))
#define GET_UINT8_VAL(x)   (*(uint8_t*) (x))
#define GET_UINT16_VAL(x)  (*(uint16_t *)(x))
#define GET_UINT32_VAL(x)  (*(uint32_t *)(x))
#define GET_UINT64_VAL(x)  (*(uint64_t *)(x))

#ifdef _TD_ARM_32
  float  taos_align_get_float(const char* pBuf);
  double taos_align_get_double(const char* pBuf);

  #define GET_FLOAT_VAL(x)       taos_align_get_float(x)
  #define GET_DOUBLE_VAL(x)      taos_align_get_double(x)
  #define SET_FLOAT_VAL(x, y)  { float z = (float)(y);   (*(int32_t*) x = *(int32_t*)(&z)); }
  #define SET_DOUBLE_VAL(x, y) { double z = (double)(y); (*(int64_t*) x = *(int64_t*)(&z)); }
  #define SET_FLOAT_PTR(x, y)  { (*(int32_t*) x = *(int32_t*)y); }
  #define SET_DOUBLE_PTR(x, y) { (*(int64_t*) x = *(int64_t*)y); }
#else
  #define GET_FLOAT_VAL(x)       (*(float *)(x))
  #define GET_DOUBLE_VAL(x)      (*(double *)(x))
  #define SET_FLOAT_VAL(x, y)  { (*(float *)(x))  = (float)(y);       }
  #define SET_DOUBLE_VAL(x, y) { (*(double *)(x)) = (double)(y);      }
  #define SET_FLOAT_PTR(x, y)  { (*(float *)(x))  = (*(float *)(y));  }
  #define SET_DOUBLE_PTR(x, y) { (*(double *)(x)) = (*(double *)(y)); }
#endif

// TODO: check if below is necessary
#define TSDB_RELATION_INVALID     0
#define TSDB_RELATION_LESS        1
#define TSDB_RELATION_GREATER     2
#define TSDB_RELATION_EQUAL       3
#define TSDB_RELATION_LESS_EQUAL  4
#define TSDB_RELATION_GREATER_EQUAL 5
#define TSDB_RELATION_NOT_EQUAL   6
#define TSDB_RELATION_LIKE        7
#define TSDB_RELATION_ISNULL      8
#define TSDB_RELATION_NOTNULL     9
#define TSDB_RELATION_IN          10

#define TSDB_RELATION_AND         11
#define TSDB_RELATION_OR          12
#define TSDB_RELATION_NOT         13

#define TSDB_BINARY_OP_ADD        30
#define TSDB_BINARY_OP_SUBTRACT   31
#define TSDB_BINARY_OP_MULTIPLY   32
#define TSDB_BINARY_OP_DIVIDE     33
#define TSDB_BINARY_OP_REMAINDER  34


#define IS_RELATION_OPTR(op) (((op) >= TSDB_RELATION_LESS) && ((op) <= TSDB_RELATION_IN))
#define IS_ARITHMETIC_OPTR(op) (((op) >= TSDB_BINARY_OP_ADD) && ((op) <= TSDB_BINARY_OP_REMAINDER))

#define TS_PATH_DELIMITER_LEN     1

#define TSDB_UNI_LEN              24
#define TSDB_USER_LEN             TSDB_UNI_LEN

// ACCOUNT is a 32 bit positive integer
// this is the length of its string representation, including the terminator zero
#define TSDB_ACCT_ID_LEN          11

#define TSDB_MAX_COLUMNS          1024
#define TSDB_MIN_COLUMNS          2       //PRIMARY COLUMN(timestamp) + other columns

#define TSDB_NODE_NAME_LEN        64
#define TSDB_TABLE_NAME_LEN       193     // it is a null-terminated string
#define TSDB_DB_NAME_LEN          33
#define TSDB_FUNC_NAME_LEN        65
#define TSDB_FUNC_CODE_LEN        (65535 - 512)
#define TSDB_TYPE_STR_MAX_LEN     32
#define TSDB_TABLE_FNAME_LEN      (TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN)
#define TSDB_COL_NAME_LEN         65
#define TSDB_MAX_SAVED_SQL_LEN    TSDB_MAX_COLUMNS * 64
#define TSDB_MAX_SQL_LEN          TSDB_PAYLOAD_SIZE
#define TSDB_MAX_SQL_SHOW_LEN     512
#define TSDB_MAX_ALLOWED_SQL_LEN  (1*1024*1024u)          // sql length should be less than 1mb

#define TSDB_APPNAME_LEN          TSDB_UNI_LEN

#define TSDB_MAX_BYTES_PER_ROW    16384
#define TSDB_MAX_TAGS_LEN         16384
#define TSDB_MAX_TAGS             128
#define TSDB_MAX_TAG_CONDITIONS   1024

#define TSDB_AUTH_LEN             16
#define TSDB_KEY_LEN              16
#define TSDB_VERSION_LEN          12
#define TSDB_LOCALE_LEN           64
#define TSDB_TIMEZONE_LEN         96
#define TSDB_LABEL_LEN            8 

#define TSDB_CLUSTER_ID_LEN       40
#define TSDB_FQDN_LEN             128
#define TSDB_EP_LEN               (TSDB_FQDN_LEN+6)
#define TSDB_IPv4ADDR_LEN      	  16
#define TSDB_FILENAME_LEN         128
#define TSDB_SHOW_SQL_LEN         512
#define TSDB_SLOW_QUERY_SQL_LEN   512

#define TSDB_STEP_NAME_LEN        32
#define TSDB_STEP_DESC_LEN        128

#define TSDB_MQTT_HOSTNAME_LEN    64
#define TSDB_MQTT_PORT_LEN        8
#define TSDB_MQTT_USER_LEN        24
#define TSDB_MQTT_PASS_LEN        24
#define TSDB_MQTT_TOPIC_LEN       64
#define TSDB_MQTT_CLIENT_ID_LEN   32

#define TSDB_DB_TYPE_DEFAULT      0
#define TSDB_DB_TYPE_TOPIC        1

#define TSDB_DEFAULT_PKT_SIZE     65480  //same as RPC_MAX_UDP_SIZE

#define TSDB_PAYLOAD_SIZE         TSDB_DEFAULT_PKT_SIZE
#define TSDB_DEFAULT_PAYLOAD_SIZE 5120   // default payload size, greater than PATH_MAX value
#define TSDB_EXTRA_PAYLOAD_SIZE   128    // extra bytes for auth
#define TSDB_CQ_SQL_SIZE          1024
#define TSDB_MIN_VNODES           64
#define TSDB_MAX_VNODES           2048
#define TSDB_MIN_VNODES_PER_DB    2
#define TSDB_MAX_VNODES_PER_DB    64

#define TSDB_DNODE_ROLE_ANY       0
#define TSDB_DNODE_ROLE_MGMT      1
#define TSDB_DNODE_ROLE_VNODE     2

#define TSDB_MAX_REPLICA          5

#define TSDB_TBNAME_COLUMN_INDEX        (-1)
#define TSDB_BLOCK_DIST_COLUMN_INDEX     (-2)
#define TSDB_UD_COLUMN_INDEX            (-100)

#define TSDB_MULTI_TABLEMETA_MAX_NUM    100000  // maximum batch size allowed to load table meta

#define TSDB_MIN_CACHE_BLOCK_SIZE       1
#define TSDB_MAX_CACHE_BLOCK_SIZE       128     // 128MB for each vnode
#define TSDB_DEFAULT_CACHE_BLOCK_SIZE   16

#define TSDB_MIN_TOTAL_BLOCKS           3
#define TSDB_MAX_TOTAL_BLOCKS           10000
#define TSDB_DEFAULT_TOTAL_BLOCKS       6

#define TSDB_MIN_TABLES                 4
#define TSDB_MAX_TABLES                 10000000
#define TSDB_DEFAULT_TABLES             1000000
#define TSDB_TABLES_STEP                1000

#define TSDB_MIN_DAYS_PER_FILE          1
#define TSDB_MAX_DAYS_PER_FILE          3650 
#define TSDB_DEFAULT_DAYS_PER_FILE      10

#define TSDB_MIN_KEEP                   1        // data in db to be reserved.
#define TSDB_MAX_KEEP                   365000   // data in db to be reserved.
#define TSDB_DEFAULT_KEEP               3650     // ten years

#define TSDB_DEFAULT_MIN_ROW_FBLOCK     100
#define TSDB_MIN_MIN_ROW_FBLOCK         10
#define TSDB_MAX_MIN_ROW_FBLOCK         1000

#define TSDB_DEFAULT_MAX_ROW_FBLOCK     4096
#define TSDB_MIN_MAX_ROW_FBLOCK         200
#define TSDB_MAX_MAX_ROW_FBLOCK         10000

#define TSDB_MIN_COMMIT_TIME            30
#define TSDB_MAX_COMMIT_TIME            40960
#define TSDB_DEFAULT_COMMIT_TIME        3600

#define TSDB_MIN_PRECISION              TSDB_TIME_PRECISION_MILLI
#define TSDB_MAX_PRECISION              TSDB_TIME_PRECISION_NANO
#define TSDB_DEFAULT_PRECISION          TSDB_TIME_PRECISION_MILLI

#define TSDB_MIN_COMP_LEVEL             0
#define TSDB_MAX_COMP_LEVEL             2
#define TSDB_DEFAULT_COMP_LEVEL         2

#define TSDB_MIN_WAL_LEVEL              0
#define TSDB_MAX_WAL_LEVEL              2
#define TSDB_DEFAULT_WAL_LEVEL          1

#define TSDB_MIN_DB_UPDATE              0
#define TSDB_MAX_DB_UPDATE              1
#define TSDB_DEFAULT_DB_UPDATE_OPTION   0

#define TSDB_MIN_DB_CACHE_LAST_ROW      0
#define TSDB_MAX_DB_CACHE_LAST_ROW      1
#define TSDB_DEFAULT_CACHE_LAST_ROW     0

#define TSDB_MIN_FSYNC_PERIOD           0
#define TSDB_MAX_FSYNC_PERIOD           180000   // millisecond
#define TSDB_DEFAULT_FSYNC_PERIOD       3000     // three second

#define TSDB_MIN_DB_REPLICA_OPTION      1
#define TSDB_MAX_DB_REPLICA_OPTION      3
#define TSDB_DEFAULT_DB_REPLICA_OPTION  1

#define TSDB_MIN_DB_PARTITON_OPTION     0
#define TSDB_MAX_DB_PARTITON_OPTION     1000
#define TSDB_DEFAULT_DB_PARTITON_OPTION 4

#define TSDB_MIN_DB_QUORUM_OPTION       1
#define TSDB_MAX_DB_QUORUM_OPTION       2
#define TSDB_DEFAULT_DB_QUORUM_OPTION   1

#define TSDB_MAX_JOIN_TABLE_NUM         5
#define TSDB_MAX_UNION_CLAUSE           5

#define TSDB_MAX_BINARY_LEN            (TSDB_MAX_BYTES_PER_ROW-TSDB_KEYSIZE)
#define TSDB_MAX_NCHAR_LEN             (TSDB_MAX_BYTES_PER_ROW-TSDB_KEYSIZE)
#define PRIMARYKEY_TIMESTAMP_COL_INDEX  0

#define TSDB_MAX_RPC_THREADS            5

#define TSDB_QUERY_TYPE_NON_TYPE               0x00u     // none type
#define TSDB_QUERY_TYPE_FREE_RESOURCE          0x01u     // free qhandle at vnode

#define TSDB_UDF_TYPE_SCALAR       1
#define TSDB_UDF_TYPE_AGGREGATE    2


/*
 * 1. ordinary sub query for select * from super_table
 * 2. all sqlobj generated by createSubqueryObj with this flag
 */
#define TSDB_QUERY_TYPE_SUBQUERY               0x02u
#define TSDB_QUERY_TYPE_STABLE_SUBQUERY        0x04u     // two-stage subquery for super table

#define TSDB_QUERY_TYPE_TABLE_QUERY            0x08u    // query ordinary table; below only apply to client side
#define TSDB_QUERY_TYPE_STABLE_QUERY           0x10u    // query on super table
#define TSDB_QUERY_TYPE_JOIN_QUERY             0x20u    // join query
#define TSDB_QUERY_TYPE_PROJECTION_QUERY       0x40u    // select *,columns... query
#define TSDB_QUERY_TYPE_JOIN_SEC_STAGE         0x80u    // join sub query at the second stage

#define TSDB_QUERY_TYPE_TAG_FILTER_QUERY       0x400u
#define TSDB_QUERY_TYPE_INSERT                 0x100u    // insert type
#define TSDB_QUERY_TYPE_MULTITABLE_QUERY       0x200u
#define TSDB_QUERY_TYPE_STMT_INSERT            0x800u    // stmt insert type

#define TSDB_QUERY_HAS_TYPE(x, _type)          (((x) & (_type)) != 0)
#define TSDB_QUERY_SET_TYPE(x, _type)          ((x) |= (_type))
#define TSDB_QUERY_CLEAR_TYPE(x, _type)        ((x) &= (~_type))
#define TSDB_QUERY_RESET_TYPE(x)               ((x) = TSDB_QUERY_TYPE_NON_TYPE)

#define TSDB_ORDER_ASC                         1
#define TSDB_ORDER_DESC                        2
                                               
#define TSDB_DEFAULT_CLUSTER_HASH_SIZE         1
#define TSDB_DEFAULT_MNODES_HASH_SIZE          5
#define TSDB_DEFAULT_DNODES_HASH_SIZE          10
#define TSDB_DEFAULT_ACCOUNTS_HASH_SIZE        10
#define TSDB_DEFAULT_USERS_HASH_SIZE           20
#define TSDB_DEFAULT_DBS_HASH_SIZE             100
#define TSDB_DEFAULT_VGROUPS_HASH_SIZE         100
#define TSDB_DEFAULT_STABLES_HASH_SIZE         100
#define TSDB_DEFAULT_CTABLES_HASH_SIZE         20000

#define TSDB_PORT_DNODESHELL                   0
#define TSDB_PORT_DNODEDNODE                   5
#define TSDB_PORT_SYNC                         10
#define TSDB_PORT_HTTP                         11
#define TSDB_PORT_ARBITRATOR                   12

#define TSDB_MAX_WAL_SIZE    (1024*1024*3)

typedef enum {
  TAOS_QTYPE_RPC   = 0,
  TAOS_QTYPE_FWD   = 1,
  TAOS_QTYPE_WAL   = 2,
  TAOS_QTYPE_CQ    = 3,
  TAOS_QTYPE_QUERY = 4
} EQType;

#define TSDB_MAX_TIERS           3
#define TSDB_MAX_DISKS_PER_TIER 16
#define TSDB_MAX_DISKS           (TSDB_MAX_TIERS * TSDB_MAX_DISKS_PER_TIER)

typedef enum {
  TSDB_SUPER_TABLE  = 0,   // super table
  TSDB_CHILD_TABLE  = 1,   // table created from super table
  TSDB_NORMAL_TABLE = 2,  // ordinary table
  TSDB_STREAM_TABLE = 3,  // table created from stream computing
  TSDB_TABLE_MAX    = 4
} ETableType;

typedef enum {
  TSDB_MOD_MNODE   = 0,
  TSDB_MOD_HTTP    = 1,
  TSDB_MOD_MONITOR = 2,
  TSDB_MOD_MQTT    = 3,
  TSDB_MOD_MAX     = 4
} EModuleType;

typedef enum {
  TSDB_CHECK_ITEM_NETWORK,
  TSDB_CHECK_ITEM_MEM,
  TSDB_CHECK_ITEM_CPU,
  TSDB_CHECK_ITEM_DISK,
  TSDB_CHECK_ITEM_OS,
  TSDB_CHECK_ITEM_ACCESS,
  TSDB_CHECK_ITEM_VERSION,
  TSDB_CHECK_ITEM_DATAFILE,
  TSDB_CHECK_ITEM_MAX
} ECheckItemType;

extern char *qtypeStr[];

#ifdef __cplusplus
}
#endif

#endif
