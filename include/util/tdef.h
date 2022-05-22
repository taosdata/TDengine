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

#ifndef _TD_UTIL_DEF_H_
#define _TD_UTIL_DEF_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB__packed

#define TSKEY             int64_t
#define TSKEY_MIN         INT64_MIN
#define TSKEY_MAX         (INT64_MAX - 1)
#define TSKEY_INITIAL_VAL TSKEY_MIN

#define TD_VER_MAX UINT64_MAX  // TODO: use the real max version from query handle

// Bytes for each type.
extern const int32_t TYPE_BYTES[15];

// TODO: replace and remove code below
#define CHAR_BYTES      sizeof(char)
#define SHORT_BYTES     sizeof(int16_t)
#define INT_BYTES       sizeof(int32_t)
#define LONG_BYTES      sizeof(int64_t)
#define FLOAT_BYTES     sizeof(float)
#define DOUBLE_BYTES    sizeof(double)
#define POINTER_BYTES   sizeof(void *)  // 8 by default  assert(sizeof(ptrdiff_t) == sizseof(void*)
#define TSDB_KEYSIZE    sizeof(TSKEY)
#define TSDB_NCHAR_SIZE sizeof(TdUcs4)

// NULL definition
#define TSDB_DATA_BOOL_NULL      0x02
#define TSDB_DATA_TINYINT_NULL   0x80
#define TSDB_DATA_SMALLINT_NULL  0x8000
#define TSDB_DATA_INT_NULL       0x80000000L
#define TSDB_DATA_BIGINT_NULL    0x8000000000000000L
#define TSDB_DATA_TIMESTAMP_NULL TSDB_DATA_BIGINT_NULL

#define TSDB_DATA_FLOAT_NULL  0x7FF00000           // it is an NAN
#define TSDB_DATA_DOUBLE_NULL 0x7FFFFF0000000000L  // an NAN
#define TSDB_DATA_NCHAR_NULL  0xFFFFFFFF
#define TSDB_DATA_BINARY_NULL 0xFF

#define TSDB_DATA_UTINYINT_NULL  0xFF
#define TSDB_DATA_USMALLINT_NULL 0xFFFF
#define TSDB_DATA_UINT_NULL      0xFFFFFFFF
#define TSDB_DATA_UBIGINT_NULL   0xFFFFFFFFFFFFFFFFL

#define TSDB_DATA_NULL_STR   "NULL"
#define TSDB_DATA_NULL_STR_L "null"

#define TSDB_NETTEST_USER "nettestinternal"
#define TSDB_DEFAULT_USER "root"
#ifdef _TD_POWER_
#define TSDB_DEFAULT_PASS "powerdb"
#elif (_TD_TQ_ == true)
#define TSDB_DEFAULT_PASS "tqueue"
#elif (_TD_PRO_ == true)
#define TSDB_DEFAULT_PASS "prodb"
#else
#define TSDB_DEFAULT_PASS "taosdata"
#endif

#define TSDB_TRUE  1
#define TSDB_FALSE 0
#define TSDB_OK    0
#define TSDB_ERR   -1

#define TS_PATH_DELIMITER "."
#define TS_ESCAPE_CHAR    '`'

#define TSDB_TIME_PRECISION_MILLI   0
#define TSDB_TIME_PRECISION_MICRO   1
#define TSDB_TIME_PRECISION_NANO    2
#define TSDB_TIME_PRECISION_HOURS   3
#define TSDB_TIME_PRECISION_MINUTES 4
#define TSDB_TIME_PRECISION_SECONDS 5

#define TSDB_TIME_PRECISION_MILLI_STR "ms"
#define TSDB_TIME_PRECISION_MICRO_STR "us"
#define TSDB_TIME_PRECISION_NANO_STR  "ns"

#define TSDB_TIME_PRECISION_SEC_DIGITS   10
#define TSDB_TIME_PRECISION_MILLI_DIGITS 13
#define TSDB_TIME_PRECISION_MICRO_DIGITS 16
#define TSDB_TIME_PRECISION_NANO_DIGITS  19

#define TSDB_INDEX_TYPE_SMA      "SMA"
#define TSDB_INDEX_TYPE_FULLTEXT "FULLTEXT"

#define TSDB_INS_USER_STABLES_DBNAME_COLID 2

#define TSDB_TICK_PER_SECOND(precision)                      \
  ((int64_t)((precision) == TSDB_TIME_PRECISION_MILLI ? 1e3L \
                                                      : ((precision) == TSDB_TIME_PRECISION_MICRO ? 1e6L : 1e9L)))

#define T_MEMBER_SIZE(type, member) sizeof(((type *)0)->member)
#define T_APPEND_MEMBER(dst, ptr, type, member)                                     \
  do {                                                                              \
    memcpy((void *)(dst), (void *)(&((ptr)->member)), T_MEMBER_SIZE(type, member)); \
    dst = (void *)((char *)(dst) + T_MEMBER_SIZE(type, member));                    \
  } while (0)
#define T_READ_MEMBER(src, type, target)          \
  do {                                            \
    (target) = *(type *)(src);                    \
    (src) = (void *)((char *)src + sizeof(type)); \
  } while (0)

typedef enum EOperatorType {
  // binary arithmetic operator
  OP_TYPE_ADD = 1,
  OP_TYPE_SUB,
  OP_TYPE_MULTI,
  OP_TYPE_DIV,
  OP_TYPE_MOD,
  // unary arithmetic operator
  OP_TYPE_MINUS,
  OP_TYPE_ASSIGN,

  // bit operator
  OP_TYPE_BIT_AND,
  OP_TYPE_BIT_OR,

  // binary comparison operator
  OP_TYPE_GREATER_THAN,
  OP_TYPE_GREATER_EQUAL,
  OP_TYPE_LOWER_THAN,
  OP_TYPE_LOWER_EQUAL,
  OP_TYPE_EQUAL,
  OP_TYPE_NOT_EQUAL,
  OP_TYPE_IN,
  OP_TYPE_NOT_IN,
  OP_TYPE_LIKE,
  OP_TYPE_NOT_LIKE,
  OP_TYPE_MATCH,
  OP_TYPE_NMATCH,
  // unary comparison operator
  OP_TYPE_IS_NULL,
  OP_TYPE_IS_NOT_NULL,
  OP_TYPE_IS_TRUE,
  OP_TYPE_IS_FALSE,
  OP_TYPE_IS_UNKNOWN,
  OP_TYPE_IS_NOT_TRUE,
  OP_TYPE_IS_NOT_FALSE,
  OP_TYPE_IS_NOT_UNKNOWN,

  // json operator
  OP_TYPE_JSON_GET_VALUE,
  OP_TYPE_JSON_CONTAINS
} EOperatorType;

#define OP_TYPE_CALC_MAX OP_TYPE_BIT_OR

typedef enum ELogicConditionType {
  LOGIC_COND_TYPE_AND = 1,
  LOGIC_COND_TYPE_OR,
  LOGIC_COND_TYPE_NOT,
} ELogicConditionType;

#define TSDB_NAME_DELIMITER_LEN 1

#define TSDB_UNI_LEN  24
#define TSDB_USER_LEN TSDB_UNI_LEN

// ACCOUNT is a 32 bit positive integer
// this is the length of its string representation, including the terminator zero
#define TSDB_ACCT_ID_LEN 11

#define TSDB_MAX_COLUMNS 4096
#define TSDB_MIN_COLUMNS 2  // PRIMARY COLUMN(timestamp) + other columns

#define TSDB_NODE_NAME_LEN  64
#define TSDB_TABLE_NAME_LEN 193  // it is a null-terminated string
#define TSDB_TOPIC_NAME_LEN 193  // it is a null-terminated string
#define TSDB_CGROUP_LEN     193  // it is a null-terminated string
#define TSDB_DB_NAME_LEN    65
#define TSDB_DB_FNAME_LEN   (TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN + TSDB_NAME_DELIMITER_LEN)

#define TSDB_FUNC_NAME_LEN       65
#define TSDB_FUNC_COMMENT_LEN    1024 * 1024
#define TSDB_FUNC_CODE_LEN       10 * 1024 * 1024
#define TSDB_FUNC_BUF_SIZE       512
#define TSDB_FUNC_TYPE_SCALAR    1
#define TSDB_FUNC_TYPE_AGGREGATE 2
#define TSDB_FUNC_SCRIPT_BIN_LIB 0
#define TSDB_FUNC_SCRIPT_LUA     1
#define TSDB_FUNC_MAX_RETRIEVE   1024

#define TSDB_INDEX_NAME_LEN      65  // 64 + 1 '\0'
#define TSDB_INDEX_TYPE_LEN      10
#define TSDB_INDEX_EXTS_LEN      256
#define TSDB_INDEX_FNAME_LEN     (TSDB_DB_FNAME_LEN + TSDB_INDEX_NAME_LEN + TSDB_NAME_DELIMITER_LEN)
#define TSDB_TYPE_STR_MAX_LEN    32
#define TSDB_TABLE_FNAME_LEN     (TSDB_DB_FNAME_LEN + TSDB_TABLE_NAME_LEN + TSDB_NAME_DELIMITER_LEN)
#define TSDB_TOPIC_FNAME_LEN     TSDB_TABLE_FNAME_LEN
#define TSDB_STREAM_FNAME_LEN    TSDB_TABLE_FNAME_LEN
#define TSDB_SUBSCRIBE_KEY_LEN   (TSDB_CGROUP_LEN + TSDB_TOPIC_FNAME_LEN + 2)
#define TSDB_PARTITION_KEY_LEN   (TSDB_SUBSCRIBE_KEY_LEN + 20)
#define TSDB_COL_NAME_LEN        65
#define TSDB_MAX_SAVED_SQL_LEN   TSDB_MAX_COLUMNS * 64
#define TSDB_MAX_SQL_LEN         TSDB_PAYLOAD_SIZE
#define TSDB_MAX_SQL_SHOW_LEN    1024
#define TSDB_MAX_ALLOWED_SQL_LEN (1 * 1024 * 1024u)  // sql length should be less than 1mb

#define TSDB_APP_NAME_LEN   TSDB_UNI_LEN
#define TSDB_TB_COMMENT_LEN 1025

/**
 *  In some scenarios uint16_t (0~65535) is used to store the row len.
 *  - Firstly, we use 65531(65535 - 4), as the SDataRow/SKVRow contains 4 bits header.
 *  - Secondly, if all cols are VarDataT type except primary key, we need 4 bits to store the offset, thus
 *    the final value is 65531-(4096-1)*4 = 49151.
 */
#define TSDB_MAX_BYTES_PER_ROW  49151
#define TSDB_MAX_TAGS_LEN       16384
#define TSDB_MAX_TAGS           128
#define TSDB_MAX_TAG_CONDITIONS 1024

#define TSDB_MAX_JSON_TAG_LEN 16384
#define TSDB_MAX_JSON_KEY_LEN 256

#define TSDB_AUTH_LEN          16
#define TSDB_PASSWORD_LEN      32
#define TSDB_USET_PASSWORD_LEN 129
#define TSDB_VERSION_LEN       12
#define TSDB_LABEL_LEN         8

#define TSDB_CLUSTER_ID_LEN     40
#define TSDB_FQDN_LEN           128
#define TSDB_EP_LEN             (TSDB_FQDN_LEN + 6)
#define TSDB_IPv4ADDR_LEN       16
#define TSDB_FILENAME_LEN       128
#define TSDB_SHOW_SQL_LEN       1024
#define TSDB_SLOW_QUERY_SQL_LEN 512
#define TSDB_SHOW_SUBQUERY_LEN  1000

#define TSDB_TRANS_STAGE_LEN 12
#define TSDB_TRANS_TYPE_LEN  16
#define TSDB_TRANS_ERROR_LEN 64

#define TSDB_STEP_NAME_LEN 32
#define TSDB_STEP_DESC_LEN 128

#define TSDB_ERROR_MSG_LEN    1024
#define TSDB_DNODE_CONFIG_LEN 128
#define TSDB_DNODE_VALUE_LEN  256

#define TSDB_DEFAULT_PKT_SIZE 65480  // same as RPC_MAX_UDP_SIZE

#define TSDB_PAYLOAD_SIZE         TSDB_DEFAULT_PKT_SIZE
#define TSDB_DEFAULT_PAYLOAD_SIZE 5120  // default payload size, greater than PATH_MAX value
#define TSDB_EXTRA_PAYLOAD_SIZE   128   // extra bytes for auth
#define TSDB_CQ_SQL_SIZE          1024
#define TSDB_MIN_VNODES           16
#define TSDB_MAX_VNODES           512

#define TSDB_DNODE_ROLE_ANY   0
#define TSDB_DNODE_ROLE_MGMT  1
#define TSDB_DNODE_ROLE_VNODE 2

#define TSDB_MAX_REPLICA 5

#define TSDB_TBNAME_COLUMN_INDEX (-1)
#define TSDB_UD_COLUMN_INDEX     (-1000)
#define TSDB_RES_COL_ID          (-5000)

#define TSDB_MULTI_TABLEMETA_MAX_NUM 100000  // maximum batch size allowed to load table meta

#define TSDB_MIN_VNODES_PER_DB          1
#define TSDB_MAX_VNODES_PER_DB          4096
#define TSDB_DEFAULT_VN_PER_DB          2
#define TSDB_MIN_BUFFER_PER_VNODE       3      // unit MB
#define TSDB_MAX_BUFFER_PER_VNODE       16384  // unit MB
#define TSDB_DEFAULT_BUFFER_PER_VNODE   96
#define TSDB_MIN_PAGES_PER_VNODE        64
#define TSDB_MAX_PAGES_PER_VNODE        16384
#define TSDB_DEFAULT_PAGES_PER_VNODE    256
#define TSDB_MIN_PAGESIZE_PER_VNODE     1  // unit KB
#define TSDB_MAX_PAGESIZE_PER_VNODE     16384
#define TSDB_DEFAULT_PAGESIZE_PER_VNODE 4
#define TSDB_MIN_DAYS_PER_FILE          60  // unit minute
#define TSDB_MAX_DAYS_PER_FILE          (3650 * 1440)
#define TSDB_DEFAULT_DAYS_PER_FILE      (10 * 1440)
#define TSDB_MIN_DURATION_PER_FILE      60  // unit minute
#define TSDB_MAX_DURATION_PER_FILE      (3650 * 1440)
#define TSDB_DEFAULT_DURATION_PER_FILE  (10 * 1440)
#define TSDB_MIN_KEEP                   (1 * 1440)       // data in db to be reserved. unit minute
#define TSDB_MAX_KEEP                   (365000 * 1440)  // data in db to be reserved.
#define TSDB_DEFAULT_KEEP               (3650 * 1440)    // ten years
#define TSDB_MIN_MINROWS_FBLOCK         10
#define TSDB_MAX_MINROWS_FBLOCK         1000
#define TSDB_DEFAULT_MINROWS_FBLOCK     100
#define TSDB_MIN_MAXROWS_FBLOCK         200
#define TSDB_MAX_MAXROWS_FBLOCK         10000
#define TSDB_DEFAULT_MAXROWS_FBLOCK     4096
#define TSDB_MIN_FSYNC_PERIOD           0
#define TSDB_MAX_FSYNC_PERIOD           180000  // millisecond
#define TSDB_DEFAULT_FSYNC_PERIOD       3000    // three second
#define TSDB_MIN_WAL_LEVEL              1
#define TSDB_MAX_WAL_LEVEL              2
#define TSDB_DEFAULT_WAL_LEVEL          1
#define TSDB_MIN_PRECISION              TSDB_TIME_PRECISION_MILLI
#define TSDB_MAX_PRECISION              TSDB_TIME_PRECISION_NANO
#define TSDB_DEFAULT_PRECISION          TSDB_TIME_PRECISION_MILLI
#define TSDB_MIN_COMP_LEVEL             0
#define TSDB_MAX_COMP_LEVEL             2
#define TSDB_DEFAULT_COMP_LEVEL         2
#define TSDB_MIN_DB_REPLICA             1
#define TSDB_MAX_DB_REPLICA             3
#define TSDB_DEFAULT_DB_REPLICA         1
#define TSDB_DB_STRICT_OFF              0
#define TSDB_DB_STRICT_ON               1
#define TSDB_DEFAULT_DB_STRICT          0
#define TSDB_MIN_DB_CACHE_LAST_ROW      0
#define TSDB_MAX_DB_CACHE_LAST_ROW      3
#define TSDB_DEFAULT_CACHE_LAST_ROW     0
#define TSDB_DB_STREAM_MODE_OFF         0
#define TSDB_DB_STREAM_MODE_ON          1
#define TSDB_DEFAULT_DB_STREAM_MODE     0
#define TSDB_DB_SINGLE_STABLE_ON        1
#define TSDB_DB_SINGLE_STABLE_OFF       0
#define TSDB_DEFAULT_DB_SINGLE_STABLE   TSDB_DB_SINGLE_STABLE_OFF
#define TSDB_DB_SCHEMALESS_ON           1
#define TSDB_DB_SCHEMALESS_OFF          0
#define TSDB_DEFAULT_DB_SCHEMALESS      TSDB_DB_SCHEMALESS_OFF

#define TSDB_MIN_ROLLUP_FILE_FACTOR     0
#define TSDB_MAX_ROLLUP_FILE_FACTOR     1
#define TSDB_DEFAULT_ROLLUP_FILE_FACTOR 0.1
#define TSDB_MIN_ROLLUP_DELAY           1
#define TSDB_MAX_ROLLUP_DELAY           10
#define TSDB_DEFAULT_ROLLUP_DELAY       2
#define TSDB_MIN_TABLE_TTL              0
#define TSDB_DEFAULT_TABLE_TTL          0

#define TSDB_MIN_EXPLAIN_RATIO     0
#define TSDB_MAX_EXPLAIN_RATIO     1
#define TSDB_DEFAULT_EXPLAIN_RATIO 0.001

#define TSDB_MAX_JOIN_TABLE_NUM 10
#define TSDB_MAX_UNION_CLAUSE   5

#define TSDB_DEFAULT_EXPLAIN_VERBOSE false

#define TSDB_EXPLAIN_RESULT_ROW_SIZE    1024
#define TSDB_EXPLAIN_RESULT_COLUMN_NAME "QUERY PLAN"

#define TSDB_MAX_FIELD_LEN             16384
#define TSDB_MAX_BINARY_LEN            (TSDB_MAX_FIELD_LEN - TSDB_KEYSIZE)  // keep 16384
#define TSDB_MAX_NCHAR_LEN             (TSDB_MAX_FIELD_LEN - TSDB_KEYSIZE)  // keep 16384
#define PRIMARYKEY_TIMESTAMP_COL_ID    1
#define COL_REACH_END(colId, maxColId) ((colId) > (maxColId))

#define TSDB_MAX_RPC_THREADS 5

#define TSDB_QUERY_TYPE_NON_TYPE      0x00u  // none type
#define TSDB_QUERY_TYPE_FREE_RESOURCE 0x01u  // free qhandle at vnode

#define TSDB_META_COMPACT_RATIO 0  // disable tsdb meta compact by default

/*
 * 1. ordinary sub query for select * from super_table
 * 2. all sqlobj generated by createSubqueryObj with this flag
 */
#define TSDB_QUERY_TYPE_INSERT      0x100u  // insert type
#define TSDB_QUERY_TYPE_FILE_INSERT 0x400u  // insert data from file
#define TSDB_QUERY_TYPE_STMT_INSERT 0x800u  // stmt insert type

#define TSDB_QUERY_HAS_TYPE(x, _type)   (((x) & (_type)) != 0)
#define TSDB_QUERY_SET_TYPE(x, _type)   ((x) |= (_type))
#define TSDB_QUERY_CLEAR_TYPE(x, _type) ((x) &= (~_type))
#define TSDB_QUERY_RESET_TYPE(x)        ((x) = TSDB_QUERY_TYPE_NON_TYPE)

#define TSDB_ORDER_ASC  1
#define TSDB_ORDER_DESC 2

#define TSDB_DEFAULT_CLUSTER_HASH_SIZE  1
#define TSDB_DEFAULT_MNODES_HASH_SIZE   5
#define TSDB_DEFAULT_DNODES_HASH_SIZE   10
#define TSDB_DEFAULT_ACCOUNTS_HASH_SIZE 10
#define TSDB_DEFAULT_USERS_HASH_SIZE    20
#define TSDB_DEFAULT_DBS_HASH_SIZE      100
#define TSDB_DEFAULT_VGROUPS_HASH_SIZE  100
#define TSDB_DEFAULT_STABLES_HASH_SIZE  100
#define TSDB_DEFAULT_CTABLES_HASH_SIZE  20000

#define TSDB_MAX_WAL_SIZE (1024 * 1024 * 3)

#define TSDB_ARB_DUMMY_TIME 4765104000000  // 2121-01-01 00:00:00.000, :P

#define TFS_MAX_TIERS          3
#define TFS_MAX_DISKS_PER_TIER 16
#define TFS_MAX_DISKS          (TFS_MAX_TIERS * TFS_MAX_DISKS_PER_TIER)
#define TFS_MIN_LEVEL          0
#define TFS_MAX_LEVEL          (TFS_MAX_TIERS - 1)
#define TFS_PRIMARY_LEVEL      0
#define TFS_PRIMARY_ID         0
#define TFS_MIN_DISK_FREE_SIZE 50 * 1024 * 1024

enum { TRANS_STAT_INIT = 0, TRANS_STAT_EXECUTING, TRANS_STAT_EXECUTED, TRANS_STAT_ROLLBACKING, TRANS_STAT_ROLLBACKED };
enum { TRANS_OPER_INIT = 0, TRANS_OPER_EXECUTE, TRANS_OPER_ROLLBACK };

typedef struct {
  char    dir[TSDB_FILENAME_LEN];
  int32_t level;
  int32_t primary;
} SDiskCfg;

#define TMQ_SEPARATOR ':'

enum {
  SND_WORKER_TYPE__SHARED = 1,
  SND_WORKER_TYPE__UNIQUE,
};

#define DEFAULT_HANDLE 0
#define MNODE_HANDLE   1
#define QNODE_HANDLE   -1
#define SNODE_HANDLE   -2
#define VNODE_HANDLE   -3
#define BNODE_HANDLE   -4

#define TSDB_CONFIG_OPTION_LEN 16
#define TSDB_CONIIG_VALUE_LEN  48
#define TSDB_CONFIG_NUMBER     8

#define QUERY_ID_SIZE      20
#define QUERY_OBJ_ID_SIZE  18
#define SUBQUERY_INFO_SIZE 6
#define QUERY_SAVE_SIZE    20

#define MAX_NUM_STR_SIZE 40

#ifdef __cplusplus
}
#endif

#endif
