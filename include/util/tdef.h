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

#include "cus_name.h"
#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB__packed

#if defined(TD_ASTRA_32)
#define PACK_PUSH_MIN _Pragma("pack(push, 4)")
#elif defined(WINDOWS)
#define PACK_PUSH_MIN __pragma(pack(push, 1))
#else
#define PACK_PUSH_MIN _Pragma("pack(push, 1)")
#endif

#if defined(WINDOWS)
#define PACK_POP __pragma(pack(pop))
#else
#define PACK_POP _Pragma("pack(pop)")
#endif

#define TSKEY             int64_t
#define TSKEY_MIN         INT64_MIN
#define TSKEY_MAX         INT64_MAX
#define TSKEY_INITIAL_VAL TSKEY_MIN

#define TD_VER_MAX UINT64_MAX  // TODO: use the real max version from query handle

// Bytes for each type.
extern const int32_t TYPE_BYTES[22];

#define CHAR_BYTES      sizeof(char)
#define SHORT_BYTES     sizeof(int16_t)
#define INT_BYTES       sizeof(int32_t)
#define LONG_BYTES      sizeof(int64_t)
#define FLOAT_BYTES     sizeof(float)
#define DOUBLE_BYTES    sizeof(double)
#define POINTER_BYTES   sizeof(void *)
#define M256_BYTES      32
#define TSDB_KEYSIZE    sizeof(TSKEY)
#define TSDB_NCHAR_SIZE sizeof(TdUcs4)

#define DECIMAL64_BYTES  8
#define DECIMAL128_BYTES 16

// NULL definition
#define TSDB_DATA_BOOL_NULL     0x02
#define TSDB_DATA_TINYINT_NULL  0x80
#define TSDB_DATA_SMALLINT_NULL 0x8000

#ifndef TSDB_DATA_INT_NULL
#define TSDB_DATA_INT_NULL 0x80000000LL
#endif

#ifndef TSDB_DATA_BIGINT_NULL
#define TSDB_DATA_BIGINT_NULL 0x8000000000000000LL
#endif

#define TSDB_DATA_TIMESTAMP_NULL TSDB_DATA_BIGINT_NULL

#ifndef TSDB_DATA_FLOAT_NULL
#define TSDB_DATA_FLOAT_NULL 0x7FF00000
#endif

#ifndef TSDB_DATA_DOUBLE_NULL
#define TSDB_DATA_DOUBLE_NULL 0x7FFFFF0000000000LL
#endif

#define TSDB_DATA_NCHAR_NULL    0xFFFFFFFF
#define TSDB_DATA_BINARY_NULL   0xFF
#define TSDB_DATA_GEOMETRY_NULL 0xFF

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
#define TSDB_INDEX_TYPE_NORMAL   "NORMAL"

#define TSDB_INS_USER_STABLES_DBNAME_COLID 2

static const int64_t TICK_PER_SECOND[] = {
    1000LL,        // MILLISECOND
    1000000LL,     // MICROSECOND
    1000000000LL,  // NANOSECOND
    0LL,           // HOUR
    0LL,           // MINUTE
    1LL            // SECOND
};

#define TSDB_TICK_PER_SECOND(precision)               \
  ((int64_t)((precision) == TSDB_TIME_PRECISION_MILLI \
                 ? 1000LL                             \
                 : ((precision) == TSDB_TIME_PRECISION_MICRO ? 1000000LL : 1000000000LL)))

#define T_MEMBER_SIZE(type, member) sizeof(((type *)0)->member)
#define T_APPEND_MEMBER(dst, ptr, type, member)                                           \
  do {                                                                                    \
    (void)memcpy((void *)(dst), (void *)(&((ptr)->member)), T_MEMBER_SIZE(type, member)); \
    dst = (void *)((char *)(dst) + T_MEMBER_SIZE(type, member));                          \
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
  OP_TYPE_REM,
  // unary arithmetic operator
  OP_TYPE_MINUS = 20,

  // bitwise operator
  OP_TYPE_BIT_AND = 30,
  OP_TYPE_BIT_OR,

  // binary comparison operator
  OP_TYPE_GREATER_THAN = 40,  // MUST KEEP IT FIRST AT COMPARE SECTION
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
  OP_TYPE_IS_NULL = 100,
  OP_TYPE_IS_NOT_NULL,
  OP_TYPE_IS_TRUE,
  OP_TYPE_IS_FALSE,
  OP_TYPE_IS_UNKNOWN,
  OP_TYPE_IS_NOT_TRUE,
  OP_TYPE_IS_NOT_FALSE,
  OP_TYPE_IS_NOT_UNKNOWN,
  OP_TYPE_COMPARE_MAX_VALUE = 149,  // MUST KEEP IT LAST AT COMPARE SECTION

  // json operator
  OP_TYPE_JSON_GET_VALUE = 150,
  OP_TYPE_JSON_CONTAINS,

  // internal operator
  OP_TYPE_ASSIGN = 200
} EOperatorType;

static const EOperatorType OPERATOR_ARRAY[] = {
    OP_TYPE_ADD, OP_TYPE_SUB, OP_TYPE_MULTI, OP_TYPE_DIV, OP_TYPE_REM,

    OP_TYPE_MINUS,

    OP_TYPE_BIT_AND, OP_TYPE_BIT_OR,

    OP_TYPE_GREATER_THAN, OP_TYPE_GREATER_EQUAL, OP_TYPE_LOWER_THAN, OP_TYPE_LOWER_EQUAL, OP_TYPE_EQUAL,
    OP_TYPE_NOT_EQUAL, OP_TYPE_IN, OP_TYPE_NOT_IN, OP_TYPE_LIKE, OP_TYPE_NOT_LIKE, OP_TYPE_MATCH, OP_TYPE_NMATCH,

    OP_TYPE_IS_NULL, OP_TYPE_IS_NOT_NULL, OP_TYPE_IS_TRUE, OP_TYPE_IS_FALSE, OP_TYPE_IS_UNKNOWN, OP_TYPE_IS_NOT_TRUE,
    OP_TYPE_IS_NOT_FALSE, OP_TYPE_IS_NOT_UNKNOWN,
    // OP_TYPE_COMPARE_MAX_VALUE,

    OP_TYPE_JSON_GET_VALUE, OP_TYPE_JSON_CONTAINS,

    OP_TYPE_ASSIGN};

#define OP_TYPE_CALC_MAX OP_TYPE_BIT_OR

typedef enum ELogicConditionType {
  LOGIC_COND_TYPE_AND = 1,
  LOGIC_COND_TYPE_OR,
  LOGIC_COND_TYPE_NOT,
} ELogicConditionType;

#define ENCRYPTED_LEN(len)  (len / 16) * 16 + (len % 16 ? 1 : 0) * 16
#define ENCRYPT_KEY_LEN     16
#define ENCRYPT_KEY_LEN_MIN 8

#define TSDB_INT32_ID_LEN 11

#define TSDB_NAME_DELIMITER_LEN 1

#define TSDB_UNI_LEN  24
#define TSDB_USER_LEN TSDB_UNI_LEN
#define TSDB_ROLE_LEN 64

#define TSDB_POINTER_PRINT_BYTES 18  // 0x1122334455667788
// ACCOUNT is a 32 bit positive integer
// this is the length of its string representation, including the terminator zero
#define TSDB_ACCT_ID_LEN   11
#define TSDB_NODE_ID_LEN   11
#define TSDB_VGROUP_ID_LEN 11

#define TSDB_MAX_COLUMNS 4096
#define TSDB_MIN_COLUMNS 2  // PRIMARY COLUMN(timestamp) + other columns

#define TSDB_NODE_NAME_LEN            64
#define TSDB_OBJ_NAME_LEN             193                                // it is a null-terminated string
#define TSDB_TABLE_NAME_LEN           193                                // it is a null-terminated string
#define TSDB_TOPIC_NAME_LEN           193                                // it is a null-terminated string
#define TSDB_CGROUP_LEN               193                                // it is a null-terminated string
#define TSDB_CLIENT_ID_LEN            256                                // it is a null-terminated string
#define TSDB_CONSUMER_ID_LEN          32                                 // it is a null-terminated string
#define TSDB_OFFSET_LEN               64                                 // it is a null-terminated string
#define TSDB_USER_CGROUP_LEN          (TSDB_USER_LEN + TSDB_CGROUP_LEN)  // it is a null-terminated string
#define TSDB_STREAM_NAME_LEN          193                                // it is a null-terminated string
#define TSDB_STREAM_NOTIFY_URL_LEN    128                                // it includes the terminating '\0'
#define TSDB_STREAM_NOTIFY_STAT_LEN   350                                // it includes the terminating '\0'
#define TSDB_DB_NAME_LEN              65
#define TSDB_DB_FNAME_LEN             (TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN + TSDB_NAME_DELIMITER_LEN)
#define TSDB_PRIVILEDGE_CONDITION_LEN 48 * 1024
#define TSDB_PRIVILEDGE_HOST_LEN      48 * 1024

#define TSDB_FUNC_NAME_LEN       65
#define TSDB_FUNC_COMMENT_LEN    1024 * 1024
#define TSDB_FUNC_CODE_LEN       10 * 1024 * 1024
#define TSDB_FUNC_BUF_SIZE       4096 * 64
#define TSDB_FUNC_TYPE_SCALAR    1
#define TSDB_FUNC_TYPE_AGGREGATE 2
#define TSDB_FUNC_SCRIPT_BIN_LIB 0
#define TSDB_FUNC_SCRIPT_PYTHON  1
#define TSDB_FUNC_MAX_RETRIEVE   1024

#define TSDB_INDEX_NAME_LEN      65  // 64 + 1 '\0'
#define TSDB_INDEX_TYPE_LEN      10
#define TSDB_INDEX_EXTS_LEN      256
#define TSDB_INDEX_FNAME_LEN     (TSDB_DB_FNAME_LEN + TSDB_INDEX_NAME_LEN + TSDB_NAME_DELIMITER_LEN)
#define TSDB_TYPE_STR_MAX_LEN    32
#define TSDB_TABLE_FNAME_LEN     (TSDB_DB_FNAME_LEN + TSDB_TABLE_NAME_LEN + TSDB_NAME_DELIMITER_LEN)
#define TSDB_TOPIC_FNAME_LEN     (TSDB_ACCT_ID_LEN + TSDB_TOPIC_NAME_LEN + TSDB_NAME_DELIMITER_LEN)
#define TSDB_STREAM_FNAME_LEN    (TSDB_DB_FNAME_LEN + TSDB_STREAM_NAME_LEN + TSDB_NAME_DELIMITER_LEN)
#define TSDB_SUBSCRIBE_KEY_LEN   (TSDB_CGROUP_LEN + TSDB_TOPIC_FNAME_LEN + 2)
#define TSDB_PARTITION_KEY_LEN   (TSDB_SUBSCRIBE_KEY_LEN + 20)
#define TSDB_COL_NAME_LEN        65
#define TSDB_COL_NAME_EXLEN      8
#define TSDB_COL_FNAME_LEN       (TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN + TSDB_NAME_DELIMITER_LEN)
#define TSDB_COL_FNAME_EX_LEN    (TSDB_DB_NAME_LEN + TSDB_NAME_DELIMITER_LEN + TSDB_TABLE_NAME_LEN + TSDB_NAME_DELIMITER_LEN + TSDB_COL_NAME_LEN)
#define TSDB_MAX_SAVED_SQL_LEN   TSDB_MAX_COLUMNS * 64
#define TSDB_MAX_SQL_LEN         TSDB_PAYLOAD_SIZE
#define TSDB_MAX_SQL_SHOW_LEN    1024
#define TSDB_MAX_ALLOWED_SQL_LEN (1 * 1024 * 1024u)  // sql length should be less than 1mb

#define TSDB_VIEW_NAME_LEN  193
#define TSDB_VIEW_FNAME_LEN (TSDB_DB_FNAME_LEN + TSDB_VIEW_NAME_LEN + TSDB_NAME_DELIMITER_LEN)

#define TSDB_APP_NAME_LEN   TSDB_UNI_LEN
#define TSDB_TB_COMMENT_LEN 1025

#define TSDB_QUERY_ID_LEN   26
#define TSDB_TRANS_OPER_LEN 16

#define TSDB_MAX_BYTES_PER_ROW 65531  // 49151:65531
#define TSDB_MAX_TAGS_LEN      16384
#define TSDB_MAX_TAGS          128

#define TSDB_MAX_COL_TAG_NUM  (TSDB_MAX_COLUMNS + TSDB_MAX_TAGS)
#define TSDB_MAX_JSON_TAG_LEN 16384
#define TSDB_MAX_JSON_KEY_LEN 256

#define TSDB_AUTH_LEN              16
#define TSDB_PASSWORD_MIN_LEN      8
#define TSDB_PASSWORD_MAX_LEN      255
#define TSDB_PASSWORD_LEN          32
#define TSDB_USET_PASSWORD_LEN     129
#define TSDB_USET_PASSWORD_LONGLEN 256
#define TSDB_VERSION_LEN           32
#define TSDB_LABEL_LEN             16
#define TSDB_JOB_STATUS_LEN        32

#define TSDB_CLUSTER_ID_LEN       40
#define TSDB_MACHINE_ID_LEN       24
#define TSDB_FQDN_LEN             TD_FQDN_LEN
#define TSDB_EP_LEN               (TSDB_FQDN_LEN + 6)
#define TSDB_IPv4ADDR_LEN         16
#define TSDB_FILENAME_LEN         128
#define TSDB_SHOW_SQL_LEN         2048
#define TSDB_SHOW_SCHEMA_JSON_LEN TSDB_MAX_COLUMNS * 256
#define TSDB_SLOW_QUERY_SQL_LEN   512
#define TSDB_SHOW_SUBQUERY_LEN    1000
#define TSDB_LOG_VAR_LEN          32

#define TSDB_ANALYTIC_ANODE_URL_LEN   128
#define TSDB_ANALYTIC_ALGO_NAME_LEN   64
#define TSDB_ANALYTIC_ALGO_TYPE_LEN   24
#define TSDB_ANALYTIC_ALGO_KEY_LEN    (TSDB_ANALYTIC_ALGO_NAME_LEN + 9)
#define TSDB_ANALYTIC_ALGO_URL_LEN    (TSDB_ANALYTIC_ANODE_URL_LEN + TSDB_ANALYTIC_ALGO_TYPE_LEN + 1)
#define TSDB_ANALYTIC_ALGO_OPTION_LEN (512)

#define TSDB_MOUNT_NAME_LEN TSDB_DB_FNAME_LEN
#define TSDB_MOUNT_PATH_LEN TSDB_FILENAME_LEN
#define TSDB_MOUNT_FPATH_LEN (TSDB_MOUNT_PATH_LEN + 32)
#define TSDB_BNODE_OPT_PROTO_STR_MQTT "mqtt"
#define TSDB_BNODE_OPT_PROTO_MQTT     1
#define TSDB_BNODE_OPT_PROTO_STR_LEN  sizeof(TSDB_BNODE_OPT_PROTO_STR_MQTT)
#define TSDB_BNODE_OPT_PROTO_DEFAULT  TSDB_BNODE_OPT_PROTO_MQTT
#define TSDB_BNODE_OPT_PROTO_DFT_STR  TSDB_BNODE_OPT_PROTO_STR_MQTT

#define TSDB_MAX_EP_NUM 10

#define TSDB_ARB_GROUP_MEMBER_NUM 2
#define TSDB_ARB_TOKEN_SIZE       32

#define TSDB_TRANS_STAGE_LEN   12
#define TSDB_TRANS_TYPE_LEN    16
#define TSDB_TRANS_ERROR_LEN   512
#define TSDB_TRANS_OBJTYPE_LEN 40
#define TSDB_TRANS_RESULT_LEN  100
#define TSDB_TRANS_TARGET_LEN  300
#define TSDB_TRANS_DETAIL_LEN  100

#define TSDB_STEP_NAME_LEN 32
#define TSDB_STEP_DESC_LEN 128

#define TSDB_ERROR_MSG_LEN    1024
#define TSDB_DNODE_CONFIG_LEN 128
#define TSDB_DNODE_VALUE_LEN  256

#define TSDB_RESERVE_VALUE_LEN 256

#define TSDB_CLUSTER_VALUE_LEN 1000
#define TSDB_GRANT_LOG_COL_LEN 15600

#define TSDB_ACTIVE_KEY_LEN      109
#define TSDB_CONN_ACTIVE_KEY_LEN 255

#define TSDB_DEFAULT_PKT_SIZE       65480  // same as RPC_MAX_UDP_SIZE
#define TSDB_SNAP_DATA_PAYLOAD_SIZE (1 * 1024 * 1024)
#define TSDB_SNAP_MAX_ROWS_PER_DATA (64 * 1024)

#define TSDB_PAYLOAD_SIZE         TSDB_DEFAULT_PKT_SIZE
#define TSDB_DEFAULT_PAYLOAD_SIZE 5120  // default payload size, greater than PATH_MAX value
#define TSDB_MIN_VNODES           16
#define TSDB_MAX_VNODES           512

#define TSDB_DNODE_ROLE_ANY   0
#define TSDB_DNODE_ROLE_MGMT  1
#define TSDB_DNODE_ROLE_VNODE 2

#define TSDB_MAX_REPLICA               5
#define TSDB_MAX_LEARNER_REPLICA       10
#define TSDB_SYNC_RESTORE_lEN          20
#define TSDB_SYNC_APPLY_COMMIT_LEN     41
#define TSDB_SYNC_LOG_BUFFER_SIZE      4096
#define TSDB_SYNC_LOG_BUFFER_RETENTION 256
#define TSDB_SYNC_LOG_BUFFER_THRESHOLD (1024 * 1024 * 5)
#define TSDB_SYNC_NEGOTIATION_WIN      512

#define TSDB_SYNC_SNAP_BUFFER_SIZE 1024

#define TSDB_TBNAME_COLUMN_INDEX     (-1)
#define TSDB_MULTI_TABLEMETA_MAX_NUM 100000  // maximum batch size allowed to load table meta

#define TSDB_MIN_VNODES_PER_DB          1
#define TSDB_MAX_VNODES_PER_DB          1024
#define TSDB_DEFAULT_VN_PER_DB          2
#define TSDB_MIN_BUFFER_PER_VNODE       3      // unit MB
#define TSDB_MAX_BUFFER_PER_VNODE       16384  // unit MB
#define TSDB_DEFAULT_BUFFER_PER_VNODE   256
#define TSDB_MIN_PAGES_PER_VNODE        64
#define TSDB_MAX_PAGES_PER_VNODE        (INT32_MAX - 1)
#define TSDB_DEFAULT_PAGES_PER_VNODE    256
#define TSDB_MIN_PAGESIZE_PER_VNODE     1  // unit KB
#define TSDB_MAX_PAGESIZE_PER_VNODE     16384
#define TSDB_DEFAULT_TSDB_PAGESIZE      4
#define TSDB_MIN_TSDB_PAGESIZE          1  // unit KB
#define TSDB_MAX_TSDB_PAGESIZE          16384
#define TSDB_DEFAULT_PAGESIZE_PER_VNODE 4
#define TSDB_MIN_DAYS_PER_FILE          60  // unit minute
#define TSDB_MAX_DAYS_PER_FILE          (3650 * 1440)
#define TSDB_DEFAULT_DAYS_PER_FILE      (10 * 1440)
#define TSDB_MIN_DURATION_PER_FILE      60  // unit minute
#define TSDB_MAX_DURATION_PER_FILE      (90 * 1440)
#define TSDB_DEFAULT_DURATION_PER_FILE  (10 * 1440)
#define TSDB_MIN_KEEP                   (1 * 1440)          // data in db to be reserved. unit minute
#define TSDB_MAX_KEEP                   (365000 * 1440)     // data in db to be reserved.
#define TSDB_MAX_KEEP_NS                (365 * 292 * 1440)  // data in db to be reserved.
#define TSDB_DEFAULT_KEEP               (3650 * 1440)       // ten years
#define TSDB_MIN_KEEP_TIME_OFFSET       0
#define TSDB_MAX_KEEP_TIME_OFFSET       23
#define TSDB_DEFAULT_KEEP_TIME_OFFSET   0
#define TSDB_MIN_MINROWS_FBLOCK         10
#define TSDB_MAX_MINROWS_FBLOCK         1000000
#define TSDB_DEFAULT_MINROWS_FBLOCK     100
#define TSDB_MIN_MAXROWS_FBLOCK         200
#define TSDB_MAX_MAXROWS_FBLOCK         10000000
#define TSDB_DEFAULT_MAXROWS_FBLOCK     4096
#define TSDB_MIN_FSYNC_PERIOD           0
#define TSDB_MAX_FSYNC_PERIOD           180000  // millisecond
#define TSDB_DEFAULT_FSYNC_PERIOD       3000    // three second
#define TSDB_MIN_WAL_LEVEL              0
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
#define TSDB_DB_STRICT_STR_LEN          sizeof(TSDB_DB_STRICT_OFF_STR)
#define TSDB_DB_STRICT_OFF_STR          "off"
#define TSDB_DB_STRICT_ON_STR           "on"
#define TSDB_DB_STRICT_OFF              0
#define TSDB_DB_STRICT_ON               1
#define TSDB_DEFAULT_DB_STRICT          TSDB_DB_STRICT_ON
#define TSDB_CACHE_MODEL_STR_LEN        sizeof(TSDB_CACHE_MODEL_LAST_VALUE_STR)
#define TSDB_CACHE_MODEL_NONE_STR       "none"
#define TSDB_CACHE_MODEL_LAST_ROW_STR   "last_row"
#define TSDB_CACHE_MODEL_LAST_VALUE_STR "last_value"
#define TSDB_CACHE_MODEL_BOTH_STR       "both"
#define TSDB_CACHE_MODEL_NONE           0
#define TSDB_CACHE_MODEL_LAST_ROW       1
#define TSDB_CACHE_MODEL_LAST_VALUE     2
#define TSDB_CACHE_MODEL_BOTH           3
#define TSDB_DNODE_LIST_LEN             256
#define TSDB_ENCRYPT_ALGO_STR_LEN       16
#define TSDB_ENCRYPT_ALGO_NONE_STR      "none"
#define TSDB_ENCRYPT_ALGO_SM4_STR       "sm4"
#define TSDB_ENCRYPT_ALGO_NONE          0
#define TSDB_ENCRYPT_ALGO_SM4           1
#define TSDB_DEFAULT_ENCRYPT_ALGO       TSDB_ENCRYPT_ALGO_NONE
#define TSDB_MIN_ENCRYPT_ALGO           TSDB_ENCRYPT_ALGO_NONE
#define TSDB_MAX_ENCRYPT_ALGO           TSDB_ENCRYPT_ALGO_SM4
#define TSDB_DEFAULT_CACHE_MODEL        TSDB_CACHE_MODEL_NONE
#define TSDB_MIN_DB_CACHE_SIZE          1  // MB
#define TSDB_MAX_DB_CACHE_SIZE          65536
#define TSDB_DEFAULT_CACHE_SIZE         1
#define TSDB_DB_STREAM_MODE_OFF         0
#define TSDB_DB_STREAM_MODE_ON          1
#define TSDB_DEFAULT_DB_STREAM_MODE     0
#define TSDB_DB_SINGLE_STABLE_ON        1
#define TSDB_DB_SINGLE_STABLE_OFF       0
#define TSDB_DEFAULT_DB_SINGLE_STABLE   TSDB_DB_SINGLE_STABLE_OFF
#define TSDB_DB_SCHEMALESS_ON           1
#define TSDB_DB_SCHEMALESS_OFF          0
#define TSDB_DEFAULT_DB_SCHEMALESS      TSDB_DB_SCHEMALESS_OFF
#define TSDB_MIN_STT_TRIGGER            1
// #ifdef TD_ENTERPRISE
#define TSDB_MAX_STT_TRIGGER     16
#define TSDB_DEFAULT_SST_TRIGGER 2
// #else
// #define TSDB_MAX_STT_TRIGGER     1
// #define TSDB_DEFAULT_SST_TRIGGER 1
// #endif
#define TSDB_STT_TRIGGER_ARRAY_SIZE 16  // maximum of TSDB_MAX_STT_TRIGGER of TD_ENTERPRISE and TD_COMMUNITY
#define TSDB_MIN_HASH_PREFIX        (2 - TSDB_TABLE_NAME_LEN)
#define TSDB_MAX_HASH_PREFIX        (TSDB_TABLE_NAME_LEN - 2)
#define TSDB_DEFAULT_HASH_PREFIX    0
#define TSDB_MIN_HASH_SUFFIX        (2 - TSDB_TABLE_NAME_LEN)
#define TSDB_MAX_HASH_SUFFIX        (TSDB_TABLE_NAME_LEN - 2)
#define TSDB_DEFAULT_HASH_SUFFIX    0

#define TSDB_MIN_SS_CHUNK_SIZE     (128 * 1024)
#define TSDB_MAX_SS_CHUNK_SIZE     (1024 * 1024)
#define TSDB_DEFAULT_SS_CHUNK_SIZE (128 * 1024)
#define TSDB_MIN_SS_KEEP_LOCAL     (1 * 1440)  // unit minute
#define TSDB_MAX_SS_KEEP_LOCAL     (365000 * 1440)
#define TSDB_DEFAULT_SS_KEEP_LOCAL (365 * 1440)
#define TSDB_MIN_SS_COMPACT        0
#define TSDB_MAX_SS_COMPACT        1
#define TSDB_DEFAULT_SS_COMPACT    1

#define TSDB_DB_MIN_WAL_RETENTION_PERIOD -1
#define TSDB_REP_DEF_DB_WAL_RET_PERIOD   3600
#define TSDB_REPS_DEF_DB_WAL_RET_PERIOD  3600
#define TSDB_DB_MIN_WAL_RETENTION_SIZE   -1
#define TSDB_REP_DEF_DB_WAL_RET_SIZE     0
#define TSDB_REPS_DEF_DB_WAL_RET_SIZE    0
#define TSDB_DB_MIN_WAL_ROLL_PERIOD      0
#define TSDB_REP_DEF_DB_WAL_ROLL_PERIOD  0
#define TSDB_REPS_DEF_DB_WAL_ROLL_PERIOD 0
#define TSDB_DB_MIN_WAL_SEGMENT_SIZE     0
#define TSDB_DEFAULT_DB_WAL_SEGMENT_SIZE 0

#define TSDB_DEFAULT_DB_WITH_ARBITRATOR 0
#define TSDB_MIN_DB_WITH_ARBITRATOR     0
#define TSDB_MAX_DB_WITH_ARBITRATOR     1

#define TSDB_MIN_ROLLUP_MAX_DELAY       1  // unit millisecond
#define TSDB_MAX_ROLLUP_MAX_DELAY       (15 * 60 * 1000)
#define TSDB_MIN_ROLLUP_WATERMARK       0  // unit millisecond
#define TSDB_MAX_ROLLUP_WATERMARK       (15 * 60 * 1000)
#define TSDB_DEFAULT_ROLLUP_WATERMARK   5000
#define TSDB_MIN_ROLLUP_DELETE_MARK     0  // unit millisecond
#define TSDB_MAX_ROLLUP_DELETE_MARK     INT64_MAX
#define TSDB_DEFAULT_ROLLUP_DELETE_MARK 900000  // 900s
#define TSDB_MIN_TABLE_TTL              0
#define TSDB_DEFAULT_TABLE_TTL          0

#define TSDB_DEFAULT_COMPACT_INTERVAL    0
#define TSDB_MIN_COMPACT_INTERVAL        10             // unit minute
#define TSDB_MAX_COMPACT_INTERVAL        TSDB_MAX_KEEP  // unit minute
#define TSDB_DEFAULT_COMPACT_START_TIME  0
#define TSDB_DEFAULT_COMPACT_END_TIME    0
#define TSDB_MIN_COMPACT_TIME_OFFSET     0
#define TSDB_MAX_COMPACT_TIME_OFFSET     23
#define TSDB_DEFAULT_COMPACT_TIME_OFFSET 0

#define TSDB_MIN_EXPLAIN_RATIO     0
#define TSDB_MAX_EXPLAIN_RATIO     1
#define TSDB_DEFAULT_EXPLAIN_RATIO 0.001

#define TSDB_DEFAULT_EXPLAIN_VERBOSE false

#define TSDB_EXPLAIN_RESULT_ROW_SIZE    (16 * 1024)
#define TSDB_EXPLAIN_RESULT_COLUMN_NAME "QUERY_PLAN"

#define TSDB_MAX_FIELD_LEN     65519               // 16384:65519
#define TSDB_MAX_BINARY_LEN    TSDB_MAX_FIELD_LEN  // 16384-8:65519
#define TSDB_MAX_NCHAR_LEN     TSDB_MAX_FIELD_LEN  // 16384-8:65519
#define TSDB_MAX_GEOMETRY_LEN  TSDB_MAX_FIELD_LEN  // 16384-8:65519
#define TSDB_MAX_VARBINARY_LEN TSDB_MAX_FIELD_LEN  // 16384-8:65519

#define TSDB_MAX_BLOB_LEN (4 << 20)

#define TSDB_MAX_SUBROLE 32

#define PRIMARYKEY_TIMESTAMP_COL_ID    1
#define COL_REACH_END(colId, maxColId) ((colId) > (maxColId))

#ifdef WINDOWS
#define TSDB_MAX_RPC_THREADS 4  // windows pipe only support 4 connections.
#else
#define TSDB_MAX_RPC_THREADS 100
#endif

#define TSDB_QUERY_TYPE_NON_TYPE 0x00u  // none type

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

#define TSDB_ORDER_NONE 0
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

#define TSDB_MAX_MSG_SIZE (1024 * 1024 * 10)

#define TSDB_BLOB_MEMORY_LIMIT (1024 * 1024 * 1024 * 1)  // 1GB

#define TSDB_ARB_DUMMY_TIME 4765104000000  // 2121-01-01 00:00:00.000, :P

#define TFS_MAX_TIERS              3
#define TFS_MAX_DISKS_PER_TIER     128
#define TFS_MAX_DISKS              (TFS_MAX_TIERS * TFS_MAX_DISKS_PER_TIER)
#define TFS_MIN_LEVEL              0
#define TFS_MAX_LEVEL              (TFS_MAX_TIERS - 1)
#define TFS_PRIMARY_LEVEL          0
#define TFS_PRIMARY_ID             0
#define TFS_MIN_DISK_FREE_SIZE     50 * 1024 * 1024                    // 50MB
#define TFS_MIN_DISK_FREE_SIZE_MAX (2ULL * 1024 * 1024 * 1024 * 1024)  // 2TB

enum { TRANS_STAT_INIT = 0, TRANS_STAT_EXECUTING, TRANS_STAT_EXECUTED, TRANS_STAT_ROLLBACKING, TRANS_STAT_ROLLBACKED };
enum { TRANS_OPER_INIT = 0, TRANS_OPER_EXECUTE, TRANS_OPER_ROLLBACK };
enum { ENCRYPT_KEY_STAT_UNKNOWN = 0, ENCRYPT_KEY_STAT_UNSET, ENCRYPT_KEY_STAT_SET, ENCRYPT_KEY_STAT_LOADED };

typedef struct {
  char    dir[TSDB_FILENAME_LEN];
  int32_t level;
  int64_t diskId;
  int32_t primary;
  int8_t  disable;  // disable create new file
} SDiskCfg;

typedef struct {
  char name[TSDB_LOG_VAR_LEN];
} SLogVar;

#define TMQ_SEPARATOR      ":"
#define TMQ_SEPARATOR_CHAR ':'

enum {
  SND_WORKER_TYPE__SHARED = 1,
  SND_WORKER_TYPE__UNIQUE,
};

enum { RAND_ERR_MEMORY = 1, RAND_ERR_FILE = 2, RAND_ERR_NETWORK = 4 };

/**
 * RB: return before
 * RA: return after
 * NR: not return, skip and go on following steps
 */
#define TSDB_BYPASS_RB_RPC_SEND_SUBMIT 0x01u
#define TSDB_BYPASS_RA_RPC_RECV_SUBMIT 0x02u
#define TSDB_BYPASS_RB_TSDB_WRITE_MEM  0x04u
#define TSDB_BYPASS_RB_TSDB_COMMIT     0x08u

#define DEFAULT_HANDLE 0
#define MNODE_HANDLE   1
#define QNODE_HANDLE   -1
#define SNODE_HANDLE   -2
#define VNODE_HANDLE   -3
#define CLIENT_HANDLE  -5

#define TSDB_CONFIG_OPTION_LEN   32
#define TSDB_CONFIG_VALUE_LEN    64
#define TSDB_CONFIG_SCOPE_LEN    8
#define TSDB_CONFIG_NUMBER       16
#define TSDB_CONFIG_PATH_LEN     4096
#define TSDB_CONFIG_INFO_LEN     64
#define TSDB_CONFIG_CATEGORY_LEN 8

#define QUERY_ID_SIZE      20
#define QUERY_OBJ_ID_SIZE  18
#define SUBQUERY_INFO_SIZE 6
#define QUERY_SAVE_SIZE    20

#define MAX_NUM_STR_SIZE 40

#define MAX_META_MSG_IN_BATCH   1048576
#define MAX_META_BATCH_RSP_SIZE (1 * 1048576 * 1024)

// sort page size by default
#define DEFAULT_PAGESIZE 4096

#define VNODE_TIMEOUT_SEC 60
#define MNODE_TIMEOUT_SEC 60

#define MONITOR_TABLENAME_LEN   200
#define MONITOR_TAG_NAME_LEN    100
#define MONITOR_TAG_VALUE_LEN   300
#define MONITOR_METRIC_NAME_LEN 100

#define AUDIT_OPERATION_LEN 20
#define CONNECTOR_INFO_LEN 256

typedef enum {
  ANALY_ALGO_TYPE_ANOMALY_DETECT = 0,
  ANALY_ALGO_TYPE_FORECAST = 1,
  ANALY_ALGO_TYPE_IMPUTATION = 2,
  ANALY_ALGO_TYPE_CORREL = 3,
  ANALY_ALGO_TYPE_CLASSIFI = 4,
  ANALY_ALGO_TYPE_MOTIF = 5,
  ANALY_ALGO_TYPE_END = 10,
} EAnalyAlgoType;

typedef enum {
  TSDB_VERSION_UNKNOWN = 0,
  TSDB_VERSION_OSS,
  TSDB_VERSION_ENTERPRISE,
  TSDB_VERSION_CLOUD,
  TSDB_VERSION_END,
} EVersionType;

#define MIN_RESERVE_MEM_SIZE 1024  // MB

// Decimal
#define TSDB_DECIMAL64_MAX_PRECISION 18
#define TSDB_DECIMAL64_MAX_SCALE     TSDB_DECIMAL64_MAX_PRECISION

#define TSDB_DECIMAL128_MAX_PRECISION 38
#define TSDB_DECIMAL128_MAX_SCALE     TSDB_DECIMAL128_MAX_PRECISION

#define TSDB_DECIMAL_MIN_PRECISION 1
#define TSDB_DECIMAL_MAX_PRECISION TSDB_DECIMAL128_MAX_PRECISION
#define TSDB_DECIMAL_MIN_SCALE     0
#define TSDB_DECIMAL_MAX_SCALE     TSDB_DECIMAL_MAX_PRECISION
#define GET_DEICMAL_MAX_PRECISION(type) \
  (type) == TSDB_DATA_TYPE_DECIMAL64 ? TSDB_DECIMAL64_MAX_PRECISION : TSDB_DECIMAL_MAX_SCALE

typedef uint64_t DecimalWord;
#define DECIMAL_WORD_NUM(TYPE) (sizeof(TYPE) / sizeof(DecimalWord))

#define COMPILE_TIME_ASSERT(pred) \
  switch (0) {                    \
    case 0:                       \
    case pred:;                   \
  }

typedef enum {
  // ==================== Legacy Privilege ====================
  PRIV_TYPE_ALL = 1,        // ALL PRIVILEGES
  PRIV_TYPE_READ = 2,       // READ PRIVILEGE
  PRIV_TYPE_WRITE = 3,      // WRITE PRIVILEGE
  PRIV_TYPE_SUBSCRIBE = 4,  // SUBSCRIBE PRIVILEGE
  PRIV_TYPE_ALTER = 5,      // ALTER PRIVILEGE
  // ==================== DB Privileges(6~49) ====================
  PRIV_DB_CREATE = 6,      // CREATE DATABASE
  PRIV_DB_ALTER,           // ALTER DATABASE
  PRIV_DB_DROP,            // DROP DATABASE
  PRIV_DB_USE,             // USE DATABASE
  PRIV_DB_FLUSH,           // FLUSH DATABASE
  PRIV_DB_COMPACT,         // COMPACT DATABASE
  PRIV_DB_TRIM,            // TRIM DATABASE
  PRIV_DB_ROLLUP,          // ROLLUP DATABASE
  PRIV_DB_SCAN,            // SCAN DATABASE
  PRIV_DB_SSMIGRATE = 15,  // SSMIGRATE DATABASE

  // VGroup Operations
  PRIV_VG_BALANCE = 26,  // BALANCE VGROUP
  PRIV_VG_MERGER,        // MERGER VGROUP
  PRIV_VG_REDISTRIBUTE,  // REDISTRIBUTE VGROUP
  PRIV_VG_SPLIT = 29,    // SPLIT VGROUP

  // DB Show Operations
  PRIV_SHOW_DATABASES = 30,   // SHOW DATABASES
  PRIV_SHOW_VNODES,           // SHOW VNODES
  PRIV_SHOW_VGROUPS,          // SHOW VGROUPS
  PRIV_SHOW_COMPACTS,         // SHOW COMPACTS
  PRIV_SHOW_RETENTIONS,       // SHOW RETENTIONS
  PRIV_SHOW_SCANS,            // SHOW SCANS
  PRIV_SHOW_SSMIGRATES = 36,  // SHOW SSMIGRATES

  // ==================== Table Privileges(50-69)  ================
  PRIV_TBL_CREATE = 50,  // CREATE TABLE
  PRIV_TBL_DROP,         // DROP TABLE
  PRIV_TBL_ALTER,        // ALTER TABLE
  PRIV_TBL_SHOW,         // SHOW TABLES
  PRIV_TBL_SHOW_CREATE,  // SHOW CREATE TABLE
  PRIV_TBL_READ,         // READ TABLE(equivalent to SELECT TABLE)
  PRIV_TBL_WRITE,        // WRITE TABLE(equivalent to INSERT TABLE)
  PRIV_TBL_UPDATE,       // UPDATE TABLE(reserved)
  PRIV_TBL_DELETE = 58,  // DELETE TABLE

  PRIV_COL_READ_DATA = 60,  // READ COLUMN DATA
  PRIV_COL_WRITE_DATA,      // WRITE COLUMN DATA
  PRIV_ROW_READ_DATA,       // READ ROW DATA
  PRIV_ROW_WRITE_DATA,      // WRITE ROW DATA

  // ==================== Other Privileges ================
  // function management
  PRIV_FUNC_CREATE = 70,  // CREATE FUNCTION
  PRIV_FUNC_DROP,         // DROP FUNCTION
  PRIV_FUNC_SHOW,         // SHOW FUNCTIONS

  // index management
  PRIV_IDX_CREATE = 73,  // CREATE INDEX
  PRIV_IDX_DROP,         //  DROP INDEX
  PRIV_IDX_SHOW,         //  SHOW INDEXES

  // view management
  PRIV_VIEW_CREATE = 76,  // CREATE VIEW
  PRIV_VIEW_DROP,         // DROP VIEW
  PRIV_VIEW_SHOW,         // SHOW VIEWS
  PRIV_VIEW_READ,         // READ VIEW

  // SMA management
  PRIV_RSMA_CREATE = 80,  // CREATE RSMA
  PRIV_RSMA_DROP,         // DROP RSMA
  PRIV_RSMA_ALTER,        // ALTER RSMA
  PRIV_RSMA_SHOW,         // SHOW RSMAS
  PRIV_RSMA_SHOW_CREATE,  // SHOW CREATE RSMA
  PRIV_TSMA_CREATE,       // CREATE TSMA
  PRIV_TSMA_DROP,         // DROP TSMA
  PRIV_TSMA_SHOW,         // SHOW TSMAS

  // mount management
  PRIV_MOUNT_CREATE = 90,  // CREATE MOUNT
  PRIV_MOUNT_DROP,         // DROP MOUNT
  PRIV_MOUNT_SHOW,         // SHOW MOUNTS

  // password management
  PRIV_PASS_ALTER = 93,  // ALTER PASS
  PRIV_PASS_ALTER_SELF,  // ALTER SELF PASS

  // role management
  PRIV_ROLE_CREATE = 100,  // CREATE ROLE
  PRIV_ROLE_DROP,          // DROP ROLE
  PRIV_ROLE_SHOW,          // SHOW ROLES

  // user management
  PRIV_USER_CREATE = 110,  // CREATE USER
  PRIV_USER_DROP,          // DROP USER
  PRIV_USER_SET_SECURITY,  // SET USER SECURITY INFO
  PRIV_USER_SET_AUDIT,     // SET USER AUDIT INFO
  PRIV_USER_SET_BASIC,     // SET USER BASIC INFO
  PRIV_USER_ENABLE,        // ENABLE USER
  PRIV_USER_DISABLE,       // DISABLE USER
  PRIV_USER_SHOW,          // SHOW USERS

  // audit management
  PRIV_AUDIT_DB_CREATE = 120,  // CREATE AUDIT DATABASE
  PRIV_AUDIT_DB_DROP,          // DROP AUDIT DATABASE
  PRIV_AUDIT_DB_ALTER,         // ALTER AUDIT DATABASE
  PRIV_AUDIT_DB_READ,          // READ AUDIT DATABASE
  PRIV_AUDIT_DB_WRITE,         // WRITE AUDIT DATABASE

  // token management
  PRIV_TOKEN_CREATE = 130,  // CREATE TOKEN
  PRIV_TOKEN_DROP,          // DROP TOKEN
  PRIV_TOKEN_ALTER,         // ALTER TOKEN
  PRIV_TOKEN_SHOW,          // SHOW TOKENS

  // key management
  PRIV_KEY_UPDATE = 140,  // UPDATE KEY
  PRIV_TOTP_CREATE,       // CREATE TOTP
  PRIV_TOTP_DROP,         // DROP TOTP
  PRIV_TOTP_UPDATE,       // UPDATE TOTP

  // grant/revoke privileges
  PRIV_GRANT_PRIVILEGE = 150,  // GRANT PRIVILEGE
  PRIV_REVOKE_PRIVILEGE,       // REVOKE PRIVILEGE
  PRIV_GRANT_SYSDBA,           // GRANT SYSDBA PRIVILEGE
  PRIV_REVOKE_SYSDBA,          // REVOKE SYSDBA PRIVILEGE
  PRIV_GRANT_SYSSEC,           // GRANT SYSSEC PRIVILEGE
  PRIV_REVOKE_SYSSEC,          // REVOKE SYSSEC PRIVILEGE
  PRIV_GRANT_SYSAUDIT,         // GRANT SYSAUDIT PRIVILEGE
  PRIV_REVOKE_SYSAUDIT,        // REVOKE SYSAUDIT PRIVILEGE

  // node management
  PRIV_NODE_CREATE = 160,  // CREATE NODE
  PRIV_NODE_DROP,          // DROP NODE
  PRIV_NODES_SHOW,          // SHOW NODES

  // system variables
  PRIV_VAR_SECURITY_ALTER = 190,  // ALTER SECURITY VARIABLE
  PRIV_VAR_AUDIT_ALTER,           // ALTER AUDIT VARIABLE
  PRIV_VAR_SYSTEM_ALTER,          // ALTER SYSTEM VARIABLE
  PRIV_VAR_DEBUG_ALTER,           // ALTER DEBUG VARIABLE
  PRIV_VAR_SECURITY_SHOW,         // SHOW SECURITY VARIABLES
  PRIV_VAR_AUDIT_SHOW,            // SHOW AUDIT VARIABLES
  PRIV_VAR_SYSTEM_SHOW,           // SHOW SYSTEM VARIABLES
  PRIV_VAR_DEBUG_SHOW,            // SHOW DEBUG VARIABLES

  // topic management
  PRIV_TOPIC_CREATE = 200,  // CREATE TOPIC
  PRIV_TOPIC_DROP,          // DROP TOPIC
  PRIV_TOPIC_SHOW,          // SHOW TOPICS
  PRIV_CONSUMER_SHOW,       // SHOW CONSUMERS
  PRIV_SUBSCRIPTION_SHOW,   // SHOW SUBSCRIPTIONS

  // stream management
  PRIV_STREAM_CREATE = 210,  // CREATE STREAM
  PRIV_STREAM_DROP,          // DROP STREAM
  PRIV_STREAM_SHOW,          // SHOW STREAMS
  PRIV_STREAM_START,         // START STREAM
  PRIV_STREAM_STOP,          // STOP STREAM
  PRIV_STREAM_RECALC,        // RECALC STREAM

  // system operation management
  PRIV_TRANS_SHOW = 220,  // SHOW TRANS
  PRIV_TRANS_KILL,        // KILL TRANS
  PRIV_CONNECTION_SHOW,   // SHOW CONNECTIONS
  PRIV_CONNECTION_KILL,   // KILL CONNECTION
  PRIV_QUERY_SHOW,        // SHOW QUERIES
  PRIV_QUERY_KILL,        // KILL QUERY

  // system info
  PRIV_INFO_SCHEMA_USE = 230,   // USE INFORMATION_SCHEMA
  PRIV_PERF_SCHEMA_USE,         // USE PERFORMANCE_SCHEMA
  PRIV_INFO_SCHEMA_READ_LIMIT,  // READ INFORMATION_SCHEMA LIMIT
  PRIV_INFO_SCHEMA_READ_SEC,    // READ INFORMATION_SCHEMA SECURITY
  PRIV_INFO_SCHEMA_READ_AUDIT,  // READ INFORMATION_SCHEMA AUDIT
  PRIV_INFO_SCHEMA_READ_BASIC,  // READ INFORMATION_SCHEMA BASIC
  PRIV_PERF_SCHEMA_READ_LIMIT,  // READ PERFORMANCE_SCHEMA LIMIT
  PRIV_PERF_SCHEMA_READ_BASIC,  // READ PERFORMANCE_SCHEMA BASIC
  PRIV_GRANTS_SHOW,             // SHOW GRANTS
  PRIV_CLUSTER_SHOW,            // SHOW CLUSTER
  PRIV_APPS_SHOW,               // SHOW APPS

  // extended privileges can be defined here (255 bits reserved in total)
  // ==================== Maximum Privilege Bit ====================
  MAX_PRIV_TYPE = 255
} EPrivType;

#define PRIV_GROUP_CNT ((MAX_PRIV_TYPE + 63) / 64)
typedef struct {
  uint64_t set[PRIV_GROUP_CNT];
} SPrivSet;

#define PRIV_GROUP(type)  ((type) / 64)
#define PRIV_OFFSET(type) ((type) & 63)
#define PRIV_TYPE(type) \
  (SPrivSet) { .set[PRIV_GROUP(type)] = 1ULL << PRIV_OFFSET(type) }

#define PRIV_HAS(privSet, type) (((privSet)->set[PRIV_GROUP(type)] & (1ULL << PRIV_OFFSET(type))) != 0)

static FORCE_INLINE SPrivSet privAdd(SPrivSet privSet1, SPrivSet privSet2) {
  SPrivSet merged = privSet1;
  for (int32_t i = 0; i < PRIV_GROUP_CNT; ++i) {
    if (privSet2.set[i]) {
      merged.set[i] |= privSet2.set[i];
    }
  }
  return merged;
}

#ifdef __cplusplus
}
#endif

#endif
